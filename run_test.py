"""
run_test.py ── 日志模板挖掘实验入口

修复要点（相对于 experiment/run_drain.py）：
  1. 只解析 CSV 的 `value` 字段，不解析整行
  2. 按 `log_name` 区分 Envoy Gateway / Service Application 两条路径
  3. Envoy → 正则结构化抽取 → 模式统计
  4. Service → Drain 算法解析 → 模板生成
  5. 对接 testdata/ 下全部 6 个测试文件

用法：
  python run_test.py

输出：
  result/envoy_patterns.txt     — Envoy 调用模式频次
  result/service_templates.txt  — Service 日志模板及计数
"""

import csv
import os
import re
import sys
from collections import defaultdict
from typing import Dict, List, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from drain import Drain

# ═══════════════════════════════════════════════════════════════
#  Envoy Gateway Access Log ── 结构化字段抽取
# ═══════════════════════════════════════════════════════════════

# Envoy HTTP 请求行：METHOD /path HTTP/2
#   \S* 而非 \S+：兼容根路径 / (如 GET / HTTP/1.1)
_RE_ENVOY_REQUEST = re.compile(
    r'"([A-Z]+)\s+(/\S*?)\s+(HTTP/\d(?:\.\d)?)"'
)

# Envoy TCP 请求行（非 HTTP 流量，如 TiDB 数据库连接）
_RE_ENVOY_TCP = re.compile(
    r'"-\s+-\s+-"'
)

# 响应状态码（HTTP 请求行后的数字）
_RE_ENVOY_STATUS = re.compile(
    r'"HTTP/\d(?:\.\d)?"\s+(\d{3})'
)

# Trace ID（UUID 格式）
_RE_ENVOY_TRACE = re.compile(
    r'"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})"'
)

# 目标服务名（"服务名:端口" 格式）
_RE_ENVOY_SERVICE = re.compile(
    r'"(\w[\w.-]*):\d{2,5}"'
)

# TCP 流量目标 IP:Port
_RE_ENVOY_TCP_TARGET = re.compile(
    r'"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})"\s+outbound\|(\d+)'
)

# 路径中动态 ID 的检测：全大写+数字、无小写字母 → 视为参数应被合并
_RE_PATH_ID = re.compile(r'^[A-Z0-9_\-]+$')

# Envoy 自身运行日志（不以引号开头 + 以日志级别开头）
_RE_ENVOY_INTERNAL = re.compile(
    r'^(warning|error|info|debug|trace)\s', re.IGNORECASE
)


def parse_envoy_value(value: str) -> Dict[str, str]:
    """从 Envoy 访问日志 value 中提取结构化字段"""
    result = {
        "method": "",
        "path": "",
        "protocol": "",
        "status": "",
        "trace_id": "",
        "service": "",
        "is_tcp": False,
        "tcp_target": "",
        "is_internal": False,
    }

    # 判断是否为 TCP 流量（请求行 "- - -"）
    if _RE_ENVOY_TCP.match(value):
        result["is_tcp"] = True
        m = _RE_ENVOY_TCP_TARGET.search(value)
        if m:
            result["tcp_target"] = f"{m.group(1)}:{m.group(3)}"
        return result

    # 判断是否为 Envoy 自身运行日志（无引号请求行）
    if _RE_ENVOY_INTERNAL.match(value):
        result["is_internal"] = True
        return result

    # HTTP 流量解析
    m = _RE_ENVOY_REQUEST.search(value)
    if m:
        result["method"] = m.group(1)
        result["path"] = m.group(2)
        result["protocol"] = m.group(3)

    m = _RE_ENVOY_STATUS.search(value)
    if m:
        result["status"] = m.group(1)

    m = _RE_ENVOY_TRACE.search(value)
    if m:
        result["trace_id"] = m.group(1)

    m = _RE_ENVOY_SERVICE.search(value)
    if m:
        result["service"] = m.group(1)

    return result


def make_envoy_pattern(parsed: Dict[str, str]) -> str:
    """将 Envoy 字段组装为模式字符串，用于聚类统计"""
    # TCP 流量 — 按目标 port 聚合成模式
    if parsed["is_tcp"]:
        tcp = parsed["tcp_target"]
        if tcp:
            parts = tcp.split(":")
            return f"TCP → port:{parts[1]}" if len(parts) == 2 else f"TCP → {tcp}"
        return "TCP"

    # Envoy 自身运行日志 — 取首 token 作为模式
    if parsed["is_internal"]:
        return "envoy internal"

    method = parsed["method"] or "?"
    status = parsed["status"]

    # 路径归一化 + 去 query string
    path = parsed["path"]
    if "?" in path:
        path = path.split("?")[0]

    # 按 / 拆分路径，逐段检查是否为动态 ID
    segments = path.strip("/").split("/")
    normalized_segments = []
    for seg in segments:
        if not seg:
            continue
        # 全大写+数字、无小写字母 → 动态参数 (产品ID等)
        if _RE_PATH_ID.match(seg):
            normalized_segments.append("<*>")
        else:
            # 缩短命名空间前缀: hipstershop.CartService → CartService
            seg = seg.split(".")[-1] if "." in seg else seg
            normalized_segments.append(seg)

    path_pattern = "/".join(normalized_segments) if normalized_segments else path

    if status:
        return f"{method} {path_pattern} → {status}"
    return f"{method} {path_pattern}"


# ═══════════════════════════════════════════════════════════════
#  Service Application Log ── Drain 预处理规则
# ═══════════════════════════════════════════════════════════════

# 规则按顺序应用，注意依赖关系：
#   - 先清 ANSI 转义码（否则会破坏所有后续规则）
#   - 再替换框架日志时间戳前缀
#   - 再替换 UUID、IP
#   - 最后替换残留纯数字

SERVICE_RULES: List[Dict[str, str]] = [
    # 0. ANSI 转义码 — 必须最先清除
    #    \x1b[32m (绿色) \x1b[39m (重置) \x1b[22m (正常) \x1b[49m (背景重置)
    {"pattern": r"\x1b\[[\d;]*[a-zA-Z]", "replacement": ""},
    # 1. 框架日志时间戳前缀 [40m → [<TIME>m
    {"pattern": r"\[\d{2,3}m", "replacement": "[<TIME>m"},
    # 2. UUID
    {"pattern": r"\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b",
     "replacement": "<UUID>"},
    # 3. IPv4
    {"pattern": r"\b\d{1,3}(\.\d{1,3}){3}\b", "replacement": "<IP>"},
    # 4. 剩余纯数字
    {"pattern": r"\b\d+\b", "replacement": "<NUM>"},
]


# ═══════════════════════════════════════════════════════════════
#  CSV 处理
# ═══════════════════════════════════════════════════════════════

def process_file(
    filepath: str,
    envoy_stats: Dict[str, int],
    service_drain: Drain,
) -> Tuple[int, int]:
    """读取一个 CSV 文件，按 log_name 分流处理。"""
    envoy_count = 0
    service_count = 0

    with open(filepath, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # 跳过表头

        for row in reader:
            if len(row) < 5:
                continue

            # CSV 列: log_id, timestamp, cmdb_id, log_name, value
            log_name = row[3]
            value = row[4]

            if "envoy_gateway" in log_name:
                parsed = parse_envoy_value(value)
                pattern = make_envoy_pattern(parsed)
                envoy_stats[pattern] += 1
                envoy_count += 1

            elif "service_application" in log_name:
                service_drain.parse(value)
                service_count += 1

    return envoy_count, service_count


# ═══════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════

def main():
    testdata_dir = os.path.join(BASE_DIR, "testdata")
    result_dir = os.path.join(BASE_DIR, "result")
    os.makedirs(result_dir, exist_ok=True)

    test_files = [
        "cloudbed1_envoy_sample.csv",
        "cloudbed1_service_sample.csv",
        "cloudbed2_envoy_sample.csv",
        "cloudbed2_service_sample.csv",
        "cloudbed3_envoy_sample.csv",
        "cloudbed3_service_sample.csv",
    ]

    envoy_stats: Dict[str, int] = defaultdict(int)
    service_drain = Drain(
        max_depth=4,        # 论文推荐值
        max_children=100,   # 论文推荐值，防止树过宽
        st=0.4,             # 相似度阈值
        rules=SERVICE_RULES,
    )

    total_envoy = 0
    total_service = 0

    print("=" * 55)
    print("  日志模板挖掘 — Drain + Envoy 结构化抽取")
    print("=" * 55)

    for filename in test_files:
        filepath = os.path.join(testdata_dir, filename)
        if not os.path.exists(filepath):
            print(f"  [SKIP] {filename} — not found")
            continue

        print(f"  {filename} ...", end=" ", flush=True)
        e_cnt, s_cnt = process_file(filepath, envoy_stats, service_drain)
        total_envoy += e_cnt
        total_service += s_cnt
        print(f"✓  ({e_cnt} envoy, {s_cnt} service)")

    total = total_envoy + total_service
    print(f"\n  Total: {total_envoy} envoy + {total_service} service = {total} logs")

    # ── Envoy 结果 ───────────────────────────────────────────
    print(f"\n{'─' * 55}")
    print(f"  Envoy 调用模式 — Top 20  (共 {len(envoy_stats)} 种)")
    print(f"{'─' * 55}")
    sorted_envoy = sorted(envoy_stats.items(), key=lambda x: x[1], reverse=True)
    for i, (pattern, count) in enumerate(sorted_envoy[:20], 1):
        bar = "█" * max(1, count // 200)
        print(f"  {i:2d}. [{count:5d}] {pattern} {bar}")

    envoy_path = os.path.join(result_dir, "envoy_patterns.txt")
    with open(envoy_path, "w", encoding="utf-8") as f:
        f.write("count\tpattern\n")
        for pattern, count in sorted_envoy:
            f.write(f"{count}\t{pattern}\n")

    # ── Service 结果 ─────────────────────────────────────────
    templates = service_drain.export_templates()
    sorted_tpl = sorted(templates, key=lambda x: x["count"], reverse=True)

    print(f"\n{'─' * 55}")
    print(f"  Service 日志模板 — Top 20  (共 {len(templates)} 种)")
    print(f"{'─' * 55}")
    for i, tpl in enumerate(sorted_tpl[:20], 1):
        bar = "█" * max(1, tpl["count"] // 200)
        print(f"  {i:2d}. [{tpl['count']:5d}] {tpl['template']} {bar}")

    service_path = os.path.join(result_dir, "service_templates.txt")
    with open(service_path, "w", encoding="utf-8") as f:
        f.write("count\ttemplate\n")
        for tpl in sorted_tpl:
            f.write(f"{tpl['count']}\t{tpl['template']}\n")

    # ── 摘要 ─────────────────────────────────────────────────
    print(f"\n{'═' * 55}")
    print(f"  输出文件")
    print(f"{'═' * 55}")
    print(f"  Envoy patterns   → {envoy_path}")
    print(f"  Service templates → {service_path}")
    print(f"  Envoy unique patterns  : {len(envoy_stats)}")
    print(f"  Service unique templates: {len(templates)}")

    if sorted_tpl:
        top5_count = sum(t["count"] for t in sorted_tpl[:5])
        total_count = sum(t["count"] for t in sorted_tpl)
        print(f"  Service Top-5 template coverage: "
              f"{top5_count}/{total_count} "
              f"({100 * top5_count / total_count:.1f}%)")


if __name__ == "__main__":
    main()
