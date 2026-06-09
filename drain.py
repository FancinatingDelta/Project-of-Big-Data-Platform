import math
import re
from typing import List, Dict, Union, Optional

from util import routing_token, similarity

PARAM_TOKEN = "<*>"

class Cluster:
    def __init__(
        self,
        tokens: Optional[List[str]] = None,
        st: float = 0.4,
        auto_st: bool = False,
    ):
        self.template = tokens or []
        self.count = 1 if tokens else 0

        self.auto_st = auto_st
        self.eta = 0

        self.dig_len = 0
        self.seq_len = 0
        self.st_init = st
        self.base = 2

        self._init_st(st, auto_st)

    def _init_st(self, st: float, auto_st: bool) -> None:
        if not auto_st:
            self.st = st
            return

        tokens = self.template
        self.seq_len = len(tokens)
        self.dig_len = sum(t.isdigit() for t in tokens)

        if self.seq_len == 0:
            self.st_init = 0.5
        else:
            self.st_init = 0.5 * (self.seq_len - self.dig_len) / self.seq_len

        self.st = self.st_init
        self.base = max(2, self.dig_len + 1)

    def update(self, tokens: List[str]) -> None:
        new_template = []

        for t1, t2 in zip(tokens, self.template):
            if t1 == t2:
                new_template.append(t1)
            else:
                new_template.append(PARAM_TOKEN)
                self.eta += 1

        self.template = new_template
        self.count += 1

        if self.auto_st:
            self.st = min(1.0, self.st_init + 0.5 * math.log(self.eta + 1, self.base))

    def __str__(self):
        return f"Cluster(size={self.count}): {' '.join(self.template)}"


class Node:
    def __init__(self, depth: int = 0):
        self.depth = depth
        self.children: Dict[Union[str, int], "Node"] = dict()
        self.clusters: List["Cluster"] = list()

    def __str__(self):
        return (
            f"Node(depth={self.depth}, "
            f"children={len(self.children)}, "
            f"clusters={len(self.clusters)})"
        )

class Drain:
    def __init__(
        self,
        max_depth: int = 4,
        max_children: int = 100,
        rules: Optional[List[Dict[str, str]]] = None,
        st: float = 0.4,
        auto_st: bool = False,
    ):
        """
        初始化 Drain 解析器。

        :param max_depth: 树的最大深度

            该定义与原始论文不同，
            此处包含根节点与叶节点，与 logparser 实现保持一致。
        
        :param max_children: 每层最大子节点数

        :param rules: 预处理使用的正则规则列表
        
        :param st: 日志相似度阈值
        """
        self.root = Node()
        self.max_depth = max_depth
        self.max_children = max_children
        self.rules = rules or []
        self.st = st
        self.auto_st = auto_st

    def preprocess(self, line: str) -> str:
        """
        日志预处理，使用正则规则替换 IP、数字、日期等动态内容
        """
        for rule in self.rules:
            line = re.sub(rule["pattern"], rule["replacement"], line)
        return line

    def insert_template(self, tokens: List[str]) -> None:
        """
        将一条日志模板插入 Drain 树
        """
        token_len = len(tokens)
        if token_len == 0:
            return

        node = self.root

        # 第一层：按日志长度区分
        node = node.children.setdefault(token_len, Node(depth=1))

        # 第二层开始：按 token 区分
        depth = 2
        remaining = tokens

        for idx in range(token_len + 1):
            # 到达叶子层，直接创建 Cluster
            if depth + 1 >= self.max_depth or idx == token_len:
                node.clusters.append(Cluster(tokens, st=self.st, auto_st=self.auto_st))
                return

            router, remaining = routing_token(remaining[:])

            # 子节点过多，使用通配符路由
            if len(node.children) >= self.max_children:
                router = PARAM_TOKEN

            # 预留通配符位置，防止溢出
            if PARAM_TOKEN not in node.children and len(node.children) + 1 == self.max_children:
                router = PARAM_TOKEN

            node = node.children.setdefault(router, Node(depth))
            depth += 1

    def select_cluster(self, tokens: List[str]) -> Optional[Cluster]:
        """
        根据日志 token 查找最匹配的 Cluster
        """
        node = self.root
        token_len = len(tokens)
        
        # 第一层：按日志长度区分
        if token_len not in node.children:
            return None

        node = node.children[token_len]

        # 第二层开始：按 token 区分
        depth = 2
        remaining = tokens

        for _ in range(token_len):
            if depth + 1 >= self.max_depth:
                break

            router, remaining = routing_token(remaining[:])

            if router in node.children:
                node = node.children[router]
            elif PARAM_TOKEN in node.children:
                node = node.children[PARAM_TOKEN]
            else:
                return None

            depth += 1

        best = None
        max_sim = 0
        max_par_count = 0

        # 在所有候选 Cluster 中选择最优
        for cluster in node.clusters:
            sim, par_count = similarity(tokens, cluster.template)
            if sim >= cluster.st:
                if (sim > max_sim or
                   (sim == max_sim and par_count > max_par_count)):
                    best = cluster
                    max_sim = sim
                    max_par_count = par_count

        return best

    def parse(self, log: str) -> None:
        """
        解析单条日志
        """
        tokens = self.preprocess(log).strip().split()
        cluster = self.select_cluster(tokens)

        # 若找到相似模板则合并，否则新建模板
        if cluster is None:
            self.insert_template(tokens)
        else:
            cluster.update(tokens)

    def export_templates(self) -> List[Dict]:
        """
        导出所有日志模板
        """
        templates = []

        def dfs(node: Node):
            for cluster in node.clusters:
                templates.append({
                    "template": " ".join(cluster.template),
                    "count": cluster.count,
                    "st": cluster.st,
                })
            for child in node.children.values():
                dfs(child)

        dfs(self.root)
        return templates

    def print_tree(self, node=None, depth=0) -> None:
        """
        打印 Drain 树结构
        """
        indent = "  " * depth

        if node is None:
            node = self.root
            print(f"{indent}{node}")

        for cluster in node.clusters:
            print(f"{indent}  {cluster}")

        for key, child in node.children.items():
            print(f"{indent}[{key}] {child}")
            self.print_tree(child, depth + 1)