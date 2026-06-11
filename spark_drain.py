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
from typing import Dict, Iterable, List, Tuple

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
) -> Iterable[Template]:
    """阶段 1：对单个分区的日志构建 Drain 树并导出局部模板。

    作为 RDD.mapPartitions 的处理函数使用。
    """
    drain = Drain(
        max_depth=max_depth,
        max_children=max_children,
        rules=get_rules(log_type),
        st=st,
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
        yield (tpl["template"], tpl["count"])


def merge_templates(partials: List[Template], st: float) -> List[Template]:
    """阶段 2：把各分区的局部模板按相似度二次聚类合并，累加计数。

    :param partials: 所有分区产出的 (模板字符串, 次数) 列表
    :param st: 相似度阈值，与解析阶段保持一致
    :return: 合并去重后的全局模板列表，按出现次数降序
    """
    # 按 token 数分桶（相似度只在等长模板间计算）
    buckets = {}  # token_len -> List[[template_tokens, count]]

    for template, count in partials:
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


# ---------------------------------------------------------------------------
# 参数树构建（二次遍历）
# ---------------------------------------------------------------------------

def _match_template(
    tokens: List[str],
    templates_by_len: dict,
    st: float,
):
    """将一条已预处理的日志 token 列表匹配到最佳模板。

    :param tokens: 预处理后的日志 token 列表
    :param templates_by_len: {token_len: [(idx, template_tokens), ...]}
    :param st: 相似度阈值
    :return: (template_idx, param_values) 或 (None, []) 无匹配时
    """
    length = len(tokens)
    if length not in templates_by_len:
        return None, []

    candidates = templates_by_len[length]
    best_idx = None
    best_sim = -1.0
    best_par_count = -1

    for idx, tpl_tokens in candidates:
        sim, par_count = similarity(tokens, tpl_tokens)
        if sim >= st:
            if sim > best_sim or (sim == best_sim and par_count > best_par_count):
                best_sim = sim
                best_idx = idx
                best_par_count = par_count

    if best_idx is not None:
        # 找到最佳匹配，提取 <*> 位置的参数值
        tpl_tokens = None
        for idx2, t in candidates:
            if idx2 == best_idx:
                tpl_tokens = t
                break
        if tpl_tokens is None:
            return None, []
        param_values = [
            tok for tok, tpl in zip(tokens, tpl_tokens) if tpl == PARAM_TOKEN
        ]
        return best_idx, param_values

    return None, []


def _build_trees_on_partition(
    lines: Iterable[str],
    templates_by_len_bc,
    log_type: str,
    st: float,
) -> Iterable[Dict[int, "ParamTree"]]:
    """在单个分区上：匹配日志→模板，构建该分区的参数树字典。

    作为 RDD.mapPartitions 的处理函数使用。
    """
    from preprocess import get_rules
    from param_tree import ParamTree

    rules = get_rules(log_type)
    templates_by_len = templates_by_len_bc.value

    trees: Dict[int, ParamTree] = {}

    for line in lines:
        if line is None:
            continue
        line = line.strip()
        if not line:
            continue

        # 预处理（与 Drain 解析阶段一致）
        for rule in rules:
            line = __import__("re").sub(rule["pattern"], rule["replacement"], line)

        tokens = line.split()
        result = _match_template(tokens, templates_by_len, st)
        if result[0] is None:
            continue

        idx, param_values = result
        if idx not in trees:
            trees[idx] = ParamTree("")
        trees[idx].add_params(param_values)

    yield trees


def build_param_trees(
    value_rdd,
    templates: List[Template],
    log_type: str,
    st: float,
):
    """二次遍历日志，为每个最终模板构建参数分布树。

    :param value_rdd: 元素为日志 value 字符串的 RDD
    :param templates: merge_templates 返回的最终全局模板列表 [(模板, 次数), ...]
    :param log_type: 日志类型（envoy / service）
    :param st: 相似度阈值
    :return: dict[int, ParamTree]，键为模板在 templates 列表中的索引
    """
    from param_tree import ParamTree

    spark = value_rdd.context

    # 构建查找结构：按 token 长度分桶
    templates_by_len: dict = {}
    for idx, (tpl_str, _count) in enumerate(templates):
        tokens = tpl_str.split()
        length = len(tokens)
        templates_by_len.setdefault(length, []).append((idx, tokens))

    templates_by_len_bc = spark.broadcast(templates_by_len)

    # 每个分区构建局部 ParamTree 字典，然后 collect 到 driver 合并
    partial_dicts = value_rdd.mapPartitions(
        lambda it: _build_trees_on_partition(
            it, templates_by_len_bc, log_type, st
        )
    ).collect()

    templates_by_len_bc.unpersist()

    # 在 driver 端合并所有分区的树 + 填充模板字符串
    merged: Dict[int, ParamTree] = {}
    for pdict in partial_dicts:
        for idx, tree in pdict.items():
            if idx in merged:
                merged[idx].merge(tree)
            else:
                tree.template = templates[idx][0]
                merged[idx] = tree

    return merged


def mine_templates(
    value_rdd,
    log_type: str,
    max_depth: int,
    max_children: int,
    st: float,
) -> List[Template]:
    """对一个仅含日志 value 文本的 RDD 执行完整的两阶段模板挖掘。

    :param value_rdd: 元素为日志 value 字符串的 RDD
    :return: 全局模板列表 (模板, 次数)，按次数降序
    """
    partials = value_rdd.mapPartitions(
        lambda it: parse_partition(it, log_type, max_depth, max_children, st)
    ).collect()

    # record_partials(log_type, partials)

    return merge_templates(partials, st)
