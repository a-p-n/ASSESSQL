from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Any
from graphviz import Digraph


@dataclass
class RelOpNode:
    op_type: str
    label: str = ""
    children: List["RelOpNode"] = field(default_factory=list)


AstDict = Dict[str, Any]

def _find_child(ast: AstDict, node_type: str) -> AstDict | None:
    for child in ast.get("children", []):
        if child.get("node_type") == node_type:
            return child
    return None


def _build_from_subtree(from_ast: AstDict) -> RelOpNode:
    table_nodes: List[RelOpNode] = []
    for child in from_ast.get("children", []):
        if child.get("node_type") == "TABLE":
            table_name = child.get("metadata", {}).get("table_name", child.get("value", ""))
            table_nodes.append(RelOpNode("TABLE", label=table_name))

    if not table_nodes:
        return RelOpNode("TABLE", label="(unknown)")

    if len(table_nodes) == 1:
        return table_nodes[0]

    cur = table_nodes[0]
    for t in table_nodes[1:]:
        cur = RelOpNode("JOIN", label="", children=[cur, t])
    return cur


def _build_where_node(where_ast: AstDict, child: RelOpNode) -> RelOpNode:
    cond_ast_list = where_ast.get("children", [])
    cond_text = ""
    if cond_ast_list:
        cond_ast = cond_ast_list[0]
        cond_text = cond_ast.get("metadata", {}).get("condition_text", cond_ast.get("value", ""))
    return RelOpNode("WHERE", label=cond_text, children=[child])


def _build_having_node(having_ast: AstDict, child: RelOpNode) -> RelOpNode:
    cond_ast_list = having_ast.get("children", [])
    cond_text = ""
    if cond_ast_list:
        cond_ast = cond_ast_list[0]
        cond_text = cond_ast.get("metadata", {}).get("condition_text", cond_ast.get("value", ""))
    return RelOpNode("HAVING", label=cond_text, children=[child])


def build_relational_tree(select_ast: AstDict) -> RelOpNode:
    if select_ast.get("node_type") != "SELECT":
        raise ValueError("Expected root node_type 'SELECT'")

    from_ast = _find_child(select_ast, "FROM")
    if not from_ast:
        base = RelOpNode("TABLE", label="(no FROM)")
    else:
        base = _build_from_subtree(from_ast)

    where_ast = _find_child(select_ast, "WHERE")
    if where_ast:
        base = _build_where_node(where_ast, base)

    having_ast = _find_child(select_ast, "HAVING")
    if having_ast:
        base = _build_having_node(having_ast, base)

    proj_label = select_ast.get("metadata", {}).get("projection", "")
    if not proj_label:
        proj_label = "*"

    root = RelOpNode("SELECT", label=proj_label, children=[base])
    return root


def render_relational_tree(root: RelOpNode,
                           filename: str = "rel_tree",
                           fmt: str = "svg") -> str:
    g = Digraph(format=fmt)
    g.attr(rankdir="TB")
    g.attr("node", shape="plaintext")

    counter = {"i": 0}

    def walk(node: RelOpNode, parent_id: str | None):
        node_id = f"n{counter['i']}"
        counter["i"] += 1

        if node.label:
            label = f"{node.op_type}\n{node.label}"
        else:
            label = node.op_type

        g.node(node_id, label)

        if parent_id is not None:
            g.edge(parent_id, node_id)

        for child in node.children:
            walk(child, node_id)

    walk(root, None)
    out_path = g.render(filename=filename, cleanup=True)
    return out_path
