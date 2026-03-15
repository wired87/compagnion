"""
Graph traversal helpers for Brain nodes. Shared by Brain and SimOrchestrator.
Avoids duplicate SUB_GOAL / REQUIRES edge logic.
"""
from __future__ import annotations

from typing import Any, List

from brain_schema import BrainEdgeRel, BrainNodeType


def get_sub_goal_ids_for_goal(G: Any, goal_node_id: str) -> List[str]:
    """
    Collect SUB_GOAL node ids linked from goal via REQUIRES relation.
    MultiGraph is undirected; neighbor is the non-goal endpoint.
    """
    out: List[str] = []
    if not goal_node_id or not G.has_node(goal_node_id):
        return out
    try:
        for src, trt, attrs in G.edges(goal_node_id, data=True):
            rel = str(attrs.get("rel") or "").lower()
            if rel != BrainEdgeRel.REQUIRES:
                continue
            neighbor = trt if src == goal_node_id else src
            if G.has_node(neighbor):
                ntype = str(G.nodes[neighbor].get("type") or "").upper()
                if ntype == BrainNodeType.SUB_GOAL:
                    out.append(neighbor)
    except Exception:
        pass
    return out


if __name__ == "__main__":
    # Minimal workflow: get_sub_goal_ids_for_goal on empty graph
    import networkx as nx
    G = nx.MultiGraph()
    G.add_node("g1", type="GOAL")
    G.add_node("s1", type="SUB_GOAL")
    G.add_edge("g1", "s1", rel=BrainEdgeRel.REQUIRES)
    out = get_sub_goal_ids_for_goal(G, "g1")
    assert "s1" in out
    print("[brain_graph_utils] ok")
