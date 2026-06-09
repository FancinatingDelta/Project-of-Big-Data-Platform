from typing import List, Tuple
import re

PARAM_TOKEN = "<*>"

def routing_token(tokens: List[str]) -> Tuple[str, str]:
    """
    返回当前层的路由 token 和剩余 token
    """
    def has_digit(token: str):
        return any(c.isdigit() for c in token)

    def has_punct(token: str):
        return bool(re.search(r"[^\w\s]", token))

    first = tokens[0]
    last = tokens[-1]

    if has_digit(first) and has_digit(last):
        return PARAM_TOKEN, tokens[1:]
    elif has_digit(first) and not has_digit(last):
        return last, tokens[:-1]
    elif not has_digit(first) and has_digit(last):
        return first, tokens[1:]

    if has_punct(first) and has_punct(last):
        return PARAM_TOKEN, tokens[1:]
    elif has_punct(first) and not has_punct(last):
        return last, tokens[:-1]
    elif not has_punct(first) and has_punct(last):
        return first, tokens[1:]

    return first, tokens[1:]


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

    same_count, total = 0, 0
    param_count = 0

    for t1, t2 in zip(new, old):
        if t2 == PARAM_TOKEN:
            param_count += 1
            continue
        total += 1
        if t1 == t2:
            same_count += 1

    sim = float(same_count) / total
    return sim, param_count
