"""
基于 Spark 的分布式 Drain 日志模板挖掘。

Drain 原算法是有状态的串行在线算法，无法直接分布式执行。这里采用
经典的「两阶段 MapReduce」思路把它并行化：

    阶段 1（Map / mapPartitions）：
        每个分区的数据独立构建一棵 Drain 树并行解析，
        产出该分区的局部模板 (template, count)。

    阶段 2（Reduce / merge）：
        汇总所有分区的局部模板，按「日志长度分桶 + 相似度」
        二次聚类合并，得到全局模板并累加计数。

这样既复用了单机 drain.py 的算法逻辑，又能借助 Spark 横向扩展处理大数据。
"""
from typing import Iterable, List, Tuple

from drain import Drain
from util import similarity
from preprocess import get_rules

PARAM_TOKEN = "<*>"

Template = Tuple[str, int]  # (模板字符串, 出现次数)


def parse_partition(
    lines: Iterable[str],
    log_type: str,
    max_depth: int,
    max_children: int,
    st: float,
    auto_st: bool,
) -> Iterable[Template]:
    """阶段 1：对单个分区的日志构建 Drain 树并导出局部模板。

    作为 RDD.mapPartitions 的处理函数使用。
    """
    drain = Drain(
        max_depth=max_depth,
        max_children=max_children,
        rules=get_rules(log_type),
        st=st,
        auto_st=auto_st,
    )
    has_data = False
    for line in lines:
        if line is None:
            continue
        line = line.strip()
        if line:
            has_data = True
            drain.parse(line)

    if not has_data:
        return

    for tpl in drain.export_templates():
        yield (tpl["template"], tpl["count"], tpl["st"])


def merge_templates(partials: List[Template]) -> List[Template]:
    """阶段 2：把各分区的局部模板按相似度二次聚类合并，累加计数。

    :param partials: 所有分区产出的 (模板字符串, 次数) 列表
    :param st: 相似度阈值，与解析阶段保持一致
    :return: 合并去重后的全局模板列表，按出现次数降序
    """
    # 按 token 数分桶（相似度只在等长模板间计算）
    buckets = {}  # token_len -> List[[template_tokens, count]]

    for template, count, st in partials:
        tokens = template.split()
        length = len(tokens)
        bucket = buckets.setdefault(length, [])

        best = None
        best_sim = -1.0
        for entry in bucket:
            sim, _ = similarity(tokens, entry[0])
            if sim > best_sim:
                best_sim = sim
                best = entry

        if best is not None and best_sim >= st:
            # 命中已有模板：不同位置 token 合并为通配符，计数累加
            best[0] = [
                t1 if t1 == t2 else PARAM_TOKEN
                for t1, t2 in zip(best[0], tokens)
            ]
            best[1] += count
        else:
            bucket.append([tokens[:], count])

    result: List[Template] = []
    for bucket in buckets.values():
        for tokens, count in bucket:
            result.append((" ".join(tokens), count))

    result.sort(key=lambda x: x[1], reverse=True)
    return result


def mine_templates(
    value_rdd,
    log_type: str,
    max_depth: int,
    max_children: int,
    st: float,
    auto_st: bool,
) -> List[Template]:
    """对一个仅含日志 value 文本的 RDD 执行完整的两阶段模板挖掘。

    :param value_rdd: 元素为日志 value 字符串的 RDD
    :return: 全局模板列表 (模板, 次数)，按次数降序
    """
    partials = value_rdd.mapPartitions(
        lambda it: parse_partition(it, log_type, max_depth, max_children, st, auto_st)
    ).collect()

    return merge_templates(partials)
