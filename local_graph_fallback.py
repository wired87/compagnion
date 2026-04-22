"""
local_graph_fallback.py - Minimal qbrain-free graph helpers for Brain.

Prompt:
verwende ollama mit gemma4 e2b als lokales modell (integrate in brain)
"""
from __future__ import annotations

from typing import Any, Dict

import networkx as nx


class GUtils:
    """Small subset of the graph helper API that Brain currently needs."""

    def __init__(self, G: nx.MultiGraph | None = None, nx_only: bool = True, enable_data_store: bool = False) -> None:
        self.G = G or nx.MultiGraph()
        self.nx_only = nx_only
        self.enable_data_store = enable_data_store

    def add_node(self, attrs: Dict[str, Any]) -> bool:
        node_id = str(attrs.get("id") or "")
        if not node_id:
            return False
        payload = {k: v for k, v in attrs.items() if k != "id"}
        if self.G.has_node(node_id):
            self.G.nodes[node_id].update(payload)
        else:
            self.G.add_node(node_id, **payload)
        return True

    def add_edge(self, src: str | None = None, trt: str | None = None, attrs: Dict[str, Any] | None = None) -> bool:
        edge_attrs = dict(attrs or {})
        source = str(src or edge_attrs.get("src") or "")
        target = str(trt or edge_attrs.get("trt") or "")
        if not source or not target:
            return False
        if not self.G.has_node(source):
            self.add_node({"id": source, "type": edge_attrs.get("src_layer") or "NODE"})
        if not self.G.has_node(target):
            self.add_node({"id": target, "type": edge_attrs.get("trgt_layer") or "NODE"})
        self.G.add_edge(source, target, **edge_attrs)
        return True

    def get_node(self, nid: str) -> Dict[str, Any]:
        return self.G.nodes[nid]
