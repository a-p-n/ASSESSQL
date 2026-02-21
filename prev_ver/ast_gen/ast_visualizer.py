from graphviz import Digraph
from typing import Dict, Any, List

AstDict = Dict[str, Any]


def _get_children(node: AstDict) -> List[AstDict]:
    return node.get("children", []) or []


def _get_label(node: AstDict) -> str:
    node_type = node.get("node_type", "")
    value = node.get("value", "")
    meta = node.get("metadata", {}) or {}

    if node_type == "SELECT":
        return "SELECT"

    if node_type in {"FROM", "WHERE", "HAVING", "GROUP_BY", "ORDER_BY"}:
        return node_type

    if node_type == "TABLE":
        return meta.get("table_name", value or "TABLE")

    if node_type == "CONDITION":
        return meta.get("condition_text", value or "CONDITION")

    if value and value != "QUERY":
        return f"{node_type}\n{value}"
    return node_type or value or "?"


def _build_graph(node: AstDict,
                 g: Digraph,
                 parent_id: str | None,
                 counter: Dict[str, int]) -> None:
    node_id = f"n{counter['i']}"
    counter["i"] += 1

    label = _get_label(node)
    g.node(node_id, label)

    if parent_id is not None:
        g.edge(parent_id, node_id)

    for child in _get_children(node):
        _build_graph(child, g, node_id, counter)


def ast_to_graph(ast_root: AstDict,
                 filename: str = "ast_tree",
                 fmt: str = "svg") -> str:
    g = Digraph(format=fmt)
    g.attr(rankdir="TB")
    g.attr("node", shape="plaintext")

    counter = {"i": 0}
    _build_graph(ast_root, g, parent_id=None, counter=counter)

    out_path = g.render(filename=filename, cleanup=True)
    return out_path
