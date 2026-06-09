"""
Spark 分布式 Drain 日志模板挖掘 —— 实验入口。

设计为参数化，本地与集群仅需切换命令行参数，代码无需改动：

  本地调试（小样本）：
    python experiment/run_spark.py \
        --input testdata \
        --output experiment/result/spark \
        --master "local[*]"

  集群全量（12GB）：
    spark-submit --master yarn experiment/run_spark.py \
        --input 3-log \
        --output experiment/result/spark

输出：按日志类型分别生成 templates_envoy.txt / templates_service.txt，
每行为「次数<TAB>模板」，并打印汇总统计（总条数、模板数、压缩比）。
"""
import os
import sys
import time
import argparse

# Windows 本地模式下，Python worker 默认可能连不上（worker failed to connect back），
# 显式指定 driver/worker 使用当前 Python 解释器即可解决。对 Linux 集群无副作用。
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from preprocess import ENVOY, SERVICE
from spark_drain import mine_templates


def parse_args():
    p = argparse.ArgumentParser(description="Spark 分布式 Drain 日志模板挖掘")
    p.add_argument("--input", default="testdata",
                   help="输入目录（递归读取其中所有 .csv），本地默认 testdata")
    p.add_argument("--output", default="experiment/result/spark",
                   help="结果输出目录")
    p.add_argument("--master", default="local[*]",
                   help="Spark master：本地 local[*]，集群 yarn 或 spark://host:7077。"
                        "若用 spark-submit --master 提交，可忽略此参数")
    p.add_argument("--max-depth", type=int, default=5, help="Drain 树最大深度")
    p.add_argument("--max-children", type=int, default=100, help="每层最大子节点数")
    p.add_argument("--st", type=float, default=0.5, help="相似度阈值")
    p.add_argument("--auto-st", type=bool, default=False, help="是否启用自适应相似度阈值")
    p.add_argument("--limit", type=int, default=0,
                   help="仅取前 N 条用于快速调试，0 表示全部")
    return p.parse_args()


def build_spark(master: str) -> SparkSession:
    builder = SparkSession.builder.appName("DrainLogMining")
    # 若通过 spark-submit 指定了 master，则不在代码里覆盖
    if master:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _list_local_csv(input_dir: str):
    """递归枚举本地目录下的所有 .csv 文件路径。"""
    files = []
    for root, _, names in os.walk(input_dir):
        for name in names:
            if name.lower().endswith(".csv"):
                files.append(os.path.join(root, name))
    return files


def read_logs(spark: SparkSession, input_dir: str):
    """读取目录下所有 CSV，返回带 (log_name, value) 的 DataFrame。

    兼容 testdata/ 与 3-log/<env>/ 两级目录结构。

    说明：在 Windows 本地（无 winutils/hadoop.dll）直接用 recursiveFileLookup
    读目录会触发 NativeIO 报错，故优先用 os.walk 枚举出显式文件清单交给 Spark；
    若传入的是 HDFS 等非本地路径（os.walk 找不到文件），则回退为按目录递归读取。
    """
    reader = (
        spark.read
        .option("header", True)
        .option("multiLine", False)     # value 均为单行，关闭可切分提升并行
        .option("escape", '"')          # 正确处理 RFC4180 双引号转义
    )

    local_files = _list_local_csv(input_dir)
    if local_files:
        df = reader.csv(local_files)
    else:
        # HDFS / 集群路径：直接按目录递归读
        df = reader.option("recursiveFileLookup", True).csv(input_dir)

    return df.select("log_name", "value").na.drop(subset=["value"])


def write_templates(output_dir: str, name: str, templates):
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"templates_{name}.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        for template, count in templates:
            f.write(f"{count}\t{template}\n")
    return out_path

def write_templates_hdfs(spark: SparkSession, output_dir: str, name: str, templates):
    ts = int(time.time())  # 秒级时间戳
    dir_name = f"templates_{name}_{ts}"

    rdd = spark.sparkContext.parallelize(templates)
    rdd.map(lambda x: f"{x[1]}\t{x[0]}") \
        .saveAsTextFile(os.path.join(output_dir, dir_name))

    return os.path.join(output_dir, dir_name)

def run_one_type(spark: SparkSession, df, log_type: str, suffix: str, args):
    """对某一类日志执行模板挖掘并输出结果，返回 (日志条数, 模板数)。"""
    sub = df.filter(F.col("log_name").endswith(suffix)).select("value")
    if args.limit > 0:
        sub = sub.limit(args.limit)

    total = sub.count()
    if total == 0:
        print(f"[{log_type}] 没有匹配的日志，跳过")
        return 0, 0

    value_rdd = sub.rdd.map(lambda r: r["value"])
    templates = mine_templates(
        value_rdd,
        log_type=log_type,
        max_depth=args.max_depth,
        max_children=args.max_children,
        st=args.st,
        auto_st=args.auto_st
    )

    out_path = write_templates(args.output, log_type, templates)
    # out_path = write_templates_hdfs(spark, args.output, log_type, templates)
    n_tpl = len(templates)
    ratio = total / n_tpl if n_tpl else 0
    print(f"[{log_type}] 日志 {total} 条 -> 模板 {n_tpl} 个（压缩比 {ratio:.1f}:1），"
          f"结果写入 {out_path}")
    print(f"[{log_type}] Top5 模板：")
    for template, count in templates[:5]:
        print(f"    {count:>7}  {template[:120]}")
    return total, n_tpl


def main():
    args = parse_args()
    spark = build_spark(args.master)

    t0 = time.time()
    df = read_logs(spark, args.input).cache()

    envoy_total, envoy_tpl = run_one_type(spark, df, ENVOY, "envoy_gateway", args)
    service_total, service_tpl = run_one_type(spark, df, SERVICE, "service_application", args)

    elapsed = time.time() - t0
    print("=" * 60)
    print(f"输入目录      : {args.input}")
    print(f"Envoy   日志  : {envoy_total} 条 -> {envoy_tpl} 模板")
    print(f"Service 日志  : {service_total} 条 -> {service_tpl} 模板")
    print(f"总耗时        : {elapsed:.1f} 秒")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
