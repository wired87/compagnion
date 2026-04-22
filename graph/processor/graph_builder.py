"""
GraphBuilder: Adds CONTENT nodes and modular edges to GUtils from chunk rows.
"""
from typing import List, Dict, Any, TYPE_CHECKING
from collections import defaultdict

if TYPE_CHECKING:
    from qbrain.graph.local_graph_utils import GUtils


def build_graph(
    rows: List[Dict[str, Any]],
    g: "GUtils",
    add_file_nodes: bool = False,
) -> int:
    """
    Add CONTENT nodes and edges to GUtils from KnowledgeNode rows.

    Args:
        rows: List of KnowledgeNode.to_dict() outputs (or equivalent dicts).
        g: GUtils instance.
        add_file_nodes: If True, create FILE nodes and part_of_file edges.

    Returns:
        Number of CONTENT nodes added.
    """
    if not rows:
        return 0

    # Filter empty content
    rows = [r for r in rows if r.get("content", "").strip()]
    if not rows:
        return 0

    # Build set of all node ids for parent validation
    node_ids = {r["id"] for r in rows}

    # 1. Add CONTENT nodes
    for row in rows:
        attrs = {**row, "type": "CONTENT"}
        g.add_node(attrs)

    # 2. Add parent_of edges
    for row in rows:
        parent_id = row.get("parent_id")
        if parent_id and parent_id in node_ids:
            g.add_edge(
                parent_id,
                row["id"],
                attrs={
                    "rel": "parent_of",
                    "src_layer": "CONTENT",
                    "trgt_layer": "CONTENT",
                },
            )

    # 3. Add follows edges (sequential within same parent)
    by_parent: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        pid = row.get("parent_id") if row.get("parent_id") else f"__root_{row['source_file']}"
        by_parent[pid].append(row)

    for _parent_key, group in by_parent.items():
        # Sort by id for deterministic order
        group.sort(key=lambda r: r["id"])
        for i in range(len(group) - 1):
            src_id = group[i]["id"]
            trg_id = group[i + 1]["id"]
            g.add_edge(
                src_id,
                trg_id,
                attrs={
                    "rel": "follows",
                    "src_layer": "CONTENT",
                    "trgt_layer": "CONTENT",
                },
            )

    # 4. Optional: FILE nodes and part_of_file edges
    if add_file_nodes:
        source_files = {r["source_file"] for r in rows}
        for sf in source_files:
            # Sanitize: use basename as FILE id (avoid path chars)
            file_id = sf.replace("/", "_").replace("\\", "_")
            g.add_node(attrs={"id": file_id, "type": "FILE", "source_file": sf})
            for row in rows:
                if row["source_file"] == sf:
                    g.add_edge(
                        row["id"],
                        file_id,
                        attrs={
                            "rel": "part_of_file",
                            "src_layer": "CONTENT",
                            "trgt_layer": "FILE",
                        },
                    )

    return len(rows)
