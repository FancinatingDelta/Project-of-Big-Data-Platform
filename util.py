from typing import List, Tuple

PARAM_TOKEN = "<*>"

def routing_token(tokens: List[str]) -> Tuple[str, str]:
    """
    返回当前层的路由 token 和剩余 token
    """
    return tokens[0], tokens[1:]


def similarity(new: List[str], old: List[str]) -> Tuple[float, int]:
    """
    计算两个日志模板之间的相似度。

    :param new: 新日志的 token 列表
    :param old: 已有模板的 token 列表
    :return: (相似度, 模板中通配符数量)
    :rtype: Tuple[float, int]
    """
    assert len(new) == len(old)
    if len(new) == 0:
        return 1.0, 0

    same_count, param_count = 0, 0
    for t1, t2 in zip(new, old):
        if t1 == PARAM_TOKEN:
            param_count += 1
            continue
        if t1 == t2:
            same_count += 1

    sim = float(same_count) / len(new)
    return sim, param_count
