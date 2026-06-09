# -*- coding: utf-8 -*-

"""
Drain 参数实验：扫描相似度阈值 st 与树深度 depth，
观察它们对「模板数量」「压缩比」「过度合并程度」的影响。

用途：为答辩提供参数调优分析（呼应 Envoy 高频模板被过度合并为全 <*> 的问题）。

运行（本地小样本）：
    python experiment/run_param_sweep.py --input testdata --output experiment/result/param_sweep

输出：
    - param_sweep.csv         各参数组合的指标表
    - param_sweep_envoy.png   Envoy：模板数 / 通配率 随 st 变化
    - param_sweep_service.png Service：同上
"""
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 复用 run_spark 中的环境设置、Spark 构建与读取逻辑
from run_spark import build_spark, read_logs  # noqa: E402

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

import pyspark.sql.functions as F  # noqa: E402
import pandas as pd  # noqa: E402
import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from preprocess import ENVOY, SERVICE  # noqa: E402
from spark_drain import mine_templates  # noqa: E402

PARAM_TOKEN = "<*>"


def parse_args():
    p = argparse.ArgumentParser(description="Drain 参数扫描实验")
    p.add_argument("--input", default="testdata")
    p.add_argument("--output", default="experiment/result/param_sweep")
    p.add_argument("--master", default="local[*]")
    p.add_argument("--st-list", default="0.3,0.4,0.5,0.6,0.7",
                   help="逗号分隔的相似度阈值列表")
    p.add_argument("--depth-list", default="5",
                   help="逗号分隔的树深度列表")
    p.add_argument("--max-children", type=int, default=100)
    p.add_argument("--limit", type=int, default=0,
                   help="每类日志仅取前 N 条加速实验，0 表示全部")
    return p.parse_args()


def wildcard_ratio(templates):
    """计算 <*> 通配符占比（按出现次数加权）。

    该值越高，说明模板把越多本应是常量的 token 合并成了通配符，
    即「过度合并」越严重。仅统计通用通配符 <*>，
    不含 <NUM>/<TID>/<ADDR> 这类有语义的归一化标记。
    """
    total_tokens = 0
    wild_tokens = 0
    for template, count in templates:
        tokens = template.split()
        total_tokens += len(tokens) * count
        wild_tokens += sum(1 for t in tokens if t == PARAM_TOKEN) * count
    if total_tokens == 0:
        return 0.0
    return wild_tokens / total_tokens


def get_value_rdd(df, suffix, limit):
    sub = df.filter(F.col("log_name").endswith(suffix)).select("value")
    if limit > 0:
        sub = sub.limit(limit)
    return sub.rdd.map(lambda r: r["value"])


def main():
    args = parse_args()
    st_list = [float(x) for x in args.st_list.split(",")]
    depth_list = [int(x) for x in args.depth_list.split(",")]
    os.makedirs(args.output, exist_ok=True)

    spark = build_spark(args.master)
    df = read_logs(spark, args.input).cache()

    targets = [(ENVOY, "envoy_gateway"), (SERVICE, "service_application")]
    rdds = {}
    totals = {}
    for log_type, suffix in targets:
        rdd = get_value_rdd(df, suffix, args.limit).cache()
        rdds[log_type] = rdd
        totals[log_type] = rdd.count()
        print(f"[{log_type}] 总日志 {totals[log_type]} 条")

    records = []
    for depth in depth_list:
        for st in st_list:
            for log_type, _ in targets:
                templates = mine_templates(
                    rdds[log_type],
                    log_type=log_type,
                    max_depth=depth,
                    max_children=args.max_children,
                    st=st,
                )
                n_tpl = len(templates)
                total = totals[log_type]
                wr = wildcard_ratio(templates)
                ratio = total / n_tpl if n_tpl else 0
                records.append({
                    "log_type": log_type,
                    "depth": depth,
                    "st": st,
                    "num_templates": n_tpl,
                    "compression_ratio": round(ratio, 1),
                    "wildcard_ratio": round(wr, 4),
                })
                print(f"depth={depth} st={st} [{log_type}] "
                      f"模板={n_tpl} 压缩比={ratio:.1f}:1 通配率={wr:.3f}")

    spark.stop()

    sweep = pd.DataFrame(records)
    csv_path = os.path.join(args.output, "param_sweep.csv")
    sweep.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n指标表写入 {csv_path}")

    # 每类日志画一张图：固定第一个 depth，横轴 st，双纵轴（模板数 / 通配率）
    base_depth = depth_list[0]
    for log_type, _ in targets:
        sub = sweep[(sweep.log_type == log_type) & (sweep.depth == base_depth)]
        sub = sub.sort_values("st")
        fig, ax1 = plt.subplots(figsize=(7, 4.5))
        ax1.plot(sub["st"], sub["num_templates"], "o-", color="tab:blue",
                 label="num_templates")
        ax1.set_xlabel("similarity threshold st")
        ax1.set_ylabel("num templates", color="tab:blue")
        ax1.tick_params(axis="y", labelcolor="tab:blue")

        ax2 = ax1.twinx()
        ax2.plot(sub["st"], sub["wildcard_ratio"], "s--", color="tab:red",
                 label="wildcard ratio")
        ax2.set_ylabel("wildcard <*> ratio", color="tab:red")
        ax2.tick_params(axis="y", labelcolor="tab:red")

        plt.title(f"Drain param sweep ({log_type}, depth={base_depth})")
        fig.tight_layout()
        png_path = os.path.join(args.output, f"param_sweep_{log_type}.png")
        fig.savefig(png_path, dpi=120)
        plt.close(fig)
        print(f"图表写入 {png_path}")

    print("\n=== 汇总 ===")
    print(sweep.to_string(index=False))


if __name__ == "__main__":
    main()
