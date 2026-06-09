"""
日志预处理规则。

针对数据集中的两类日志（Envoy 网关日志 / Service 应用日志）分别提供
正则规则，把动态变量（IP、端口、Trace ID、数字、ANSI 颜色码等）归一化，
便于 Drain 提取稳定的日志模板。

规则格式与 drain.Drain 接受的一致：[{"pattern": ..., "replacement": ...}, ...]
按列表顺序依次 re.sub，因此更具体的规则要放在更前面。
"""

ENVOY = "envoy"
SERVICE = "service"

# UUID 形式的 Trace ID：8-4-4-4-12 十六进制
_TRACE_ID = r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
# IP:端口
_IP_PORT = r"\b\d{1,3}(?:\.\d{1,3}){3}:\d+\b"
# 纯 IP
_IP = r"\b\d{1,3}(?:\.\d{1,3}){3}\b"
# 整数
_NUM = r"\b\d+\b"
# ANSI 颜色/控制码（数据中首个 [40m 可能缺少 ESC 前缀，故 ESC 可选）
_ANSI = r"\x1b?\[[0-9;]*m"

ENVOY_RULES = [
    {"pattern": _TRACE_ID, "replacement": "<TID>"},
    {"pattern": _IP_PORT, "replacement": "<ADDR>"},
    {"pattern": _IP, "replacement": "<IP>"},
    {"pattern": _NUM, "replacement": "<NUM>"},
]

SERVICE_RULES = [
    {"pattern": _ANSI, "replacement": ""},
    {"pattern": _TRACE_ID, "replacement": "<TID>"},
    {"pattern": _NUM, "replacement": "<NUM>"},
]


def get_rules(log_type: str):
    """根据日志类型返回对应的预处理正则规则列表。"""
    if log_type == ENVOY:
        return ENVOY_RULES
    if log_type == SERVICE:
        return SERVICE_RULES
    raise ValueError(f"未知日志类型: {log_type}")


def classify(log_name: str) -> str:
    """根据 log_name 字段判断日志类型。"""
    if log_name and log_name.endswith("envoy_gateway"):
        return ENVOY
    return SERVICE
