# -*- coding: utf-8 -*-
"""
参数分布树 —— 为每个模板构建其 <*> 位置上的参数值层次分布。

用于在 Drain 模板挖掘完成后，二次遍历日志，将每条日志匹配到最终模板，
提取通配符位置的具体参数值，聚合成层次树，支持树状可视化输出。
"""

from typing import Dict, List, Tuple

PARAM_TOKEN = "<*>"


class ParamNode:
    """参数值树的节点。

    每个节点记录：该路径出现的总次数 + 下一层参数值的分布。
    """

    __slots__ = ("count", "children")

    def __init__(self):
        self.count: int = 0
        self.children: Dict[str, "ParamNode"] = {}

    def add(self, values: List[str]) -> None:
        """沿参数值序列向下生长树，沿途累加计数。"""
        self.count += 1
        if not values:
            return
        key = values[0]
        child = self.children.get(key)
        if child is None:
            child = ParamNode()
            self.children[key] = child
        child.add(values[1:])

    def merge(self, other: "ParamNode") -> None:
        """合并另一个 ParamNode 到当前节点（累加计数，递归合并子节点）。"""
        self.count += other.count
        for key, other_child in other.children.items():
            if key in self.children:
                self.children[key].merge(other_child)
            else:
                self.children[key] = other_child


class ParamTree:
    """单个模板的完整参数分布树。"""

    def __init__(self, template: str):
        self.template: str = template
        self.root: ParamNode = ParamNode()

    def add_params(self, values: List[str]) -> None:
        """添加一组参数值（按 <*> 位置顺序）。"""
        self.root.add(values)

    def merge(self, other: "ParamTree") -> None:
        """合并另一个 ParamTree（用于跨分区聚合）。"""
        self.root.merge(other.root)


# ---------------------------------------------------------------------------
# 树状渲染
# ---------------------------------------------------------------------------

def render_param_tree(
    tree: "ParamTree",
    max_branches: int = 5,
) -> str:
    """将一棵参数树渲染为带树状连接线的多行字符串。

    :param tree: 参数分布树
    :param max_branches: 每层最多展示几个分支（超出部分折叠）
    :return: 格式化后的树状文本
    """
    total = tree.root.count
    lines: List[str] = [f"[{total}] {tree.template}"]

    if not tree.root.children:
        return "\n".join(lines)

    children = sorted(tree.root.children.items(), key=lambda x: -x[1].count)
    _render_children(children, "", lines, max_branches)

    return "\n".join(lines)


def _render_children(
    children: List[Tuple[str, "ParamNode"]],
    indent: str,
    lines: List[str],
    max_branches: int,
) -> None:
    """递归渲染子节点列表。"""
    visible = children[:max_branches]
    hidden_count = len(children) - len(visible)

    for i, (key, child) in enumerate(visible):
        is_last = i == len(visible) - 1 and hidden_count == 0
        connector = "└─ " if is_last else "├─ "
        lines.append(f"  {indent}{connector}{key} ({child.count})")

        if child.children:
            child_indent = indent + ("   " if is_last else "│  ")
            child_children = sorted(child.children.items(), key=lambda x: -x[1].count)
            _render_children(child_children, child_indent, lines, max_branches)

    if hidden_count > 0:
        hidden_total = sum(c.count for _, c in children[len(visible):])
        lines.append(
            f"  {indent}└─ ... 共 {len(children)} 种参数组合（剩余 {hidden_total} 条）"
        )
