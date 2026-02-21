from dataclasses import dataclass, field
from typing import List, Dict, Any
from graphviz import Digraph

AstDict = Dict[str, Any]


@dataclass
class ExprNode:
    label: str 
    children: List["ExprNode"] = field(default_factory=list)


def _find_child(ast: AstDict, node_type: str) -> AstDict | None:
    for ch in ast.get("children", []):
        if ch.get("node_type") == node_type:
            return ch
    return None


def _find_children(ast: AstDict, node_type: str) -> List[AstDict]:
    return [ch for ch in ast.get("children", []) if ch.get("node_type") == node_type]


def _get_table_name(table_node: AstDict) -> str:
    meta = table_node.get("metadata", {}) or {}
    return meta.get("table_name", table_node.get("value", "table"))


def _get_where_condition(where_node: AstDict) -> str:
    if not where_node:
        return ""
    conds = _find_children(where_node, "CONDITION")
    if not conds:
        conds = where_node.get("children", [])
    if not conds:
        return ""
    meta = conds[0].get("metadata", {}) or {}
    return meta.get("condition_text", conds[0].get("value", "")).strip()


def build_expression_tree(sql: str, ast_root: AstDict) -> ExprNode:
    sql_l = sql.strip()
    lower = sql_l.lower()

    proj = "*"
    if "select" in lower and "from" in lower:
        start = lower.index("select") + len("select")
        end = lower.index("from")
        proj = sql_l[start:end].strip()

    select_node = ExprNode(f"SELECT {proj}")

    where_ast = _find_child(ast_root, "WHERE")
    where_cond = _get_where_condition(where_ast)

    current_parent = select_node
    if where_cond:
        where_node = ExprNode(f"WHERE {where_cond}")
        current_parent.children.append(where_node)
        current_parent = where_node

    from_ast = _find_child(ast_root, "FROM")
    if not from_ast:
        return select_node

    table_nodes = _find_children(from_ast, "TABLE")
    if not table_nodes and from_ast.get("children"):
        table_nodes = from_ast["children"]

    tables = [_get_table_name(t) for t in table_nodes]

    join_tree = _build_join_tree(tables)
    current_parent.children.append(join_tree)

    return select_node


def _build_join_tree(tables: List[str]) -> ExprNode:
    if not tables:
        return ExprNode("")

    if len(tables) == 1:
        return ExprNode(tables[0])

    left = ExprNode(tables[0])
    right = _build_join_tree(tables[1:])
    join = ExprNode("JOIN", children=[left, right])
    return join


def render_expression_tree(root: ExprNode,
                           filename: str = "expression_tree",
                           fmt: str = "svg") -> str:
    g = Digraph(format=fmt)
    g.attr(rankdir="TB")
    g.attr("node", shape="plaintext")

    counter = {"i": 0}

    def dfs(node: ExprNode, parent_id: str | None = None):
        node_id = f"n{counter['i']}"
        counter["i"] += 1

        g.node(node_id, node.label)
        if parent_id is not None:
            g.edge(parent_id, node_id)

        for ch in node.children:
            dfs(ch, node_id)

    dfs(root)
    out_path = g.render(filename=filename, cleanup=True)
    return out_path
