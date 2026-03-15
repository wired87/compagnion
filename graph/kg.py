"""
Single Brain knowledge graph (KG) instance for all users' entries.

File-manager and Brain use this shared graph so FILE, EQUATION, OBJECT, METHOD
and long-term storage refs live in one place.
"""

from __future__ import annotations

from typing import Optional

import networkx as nx

_kg: Optional[nx.MultiGraph] = None


def get_knowledge_graph() -> nx.MultiGraph:
    """Return the singleton Brain KG (one MultiGraph for all users)."""
    global _kg
    if _kg is None:
        _kg = nx.MultiGraph()
    return _kg


def reset_knowledge_graph(G: Optional[nx.MultiGraph] = None) -> nx.MultiGraph:
    """Reset the singleton (e.g. for tests). Returns the new or existing G."""
    global _kg
    _kg = G if G is not None else nx.MultiGraph()
    return _kg


if __name__ == "__main__":
    # Minimal workflow: get_knowledge_graph, reset_knowledge_graph
    G = get_knowledge_graph()
    assert G is not None and hasattr(G, "nodes")
    reset_knowledge_graph()
    print("[graph.kg] ok")
