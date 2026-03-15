from __future__ import annotations

import networkx as nx

from qbrain.graph.local_graph_utils import GUtils
from qbrain.graph.cpu_model import CpuGraphScorer


def test_cpu_model_ctlr_returns_node_id_and_score():
    G = nx.MultiGraph()
    # Nodes
    G.add_node("GOAL::1", type="GOAL", message="find relevant method for gauge")
    G.add_node("M1", type="METHOD", equation="y=x+1")
    G.add_node("M2", type="METHOD", equation="z=x*y")
    G.add_node("P1", type="PARAM", name="alpha")
    G.add_node("C1", type="CONTENT", content="Gauge field strength tensor F_{mu nu}")

    # Edges (undirected, MultiGraph)
    G.add_edge("GOAL::1", "M1", rel="derived_from", src_layer="GOAL", trgt_layer="METHOD")
    G.add_edge("M1", "P1", rel="has_param", src_layer="METHOD", trgt_layer="PARAM")
    G.add_edge("M2", "P1", rel="has_param", src_layer="METHOD", trgt_layer="PARAM")
    G.add_edge("C1", "M2", rel="mentions", src_layer="CONTENT", trgt_layer="METHOD")

    g = GUtils(G=G, nx_only=True, enable_data_store=False)

    scorer = CpuGraphScorer(
        gutils=g,
        node_types=["GOAL", "METHOD", "PARAM", "CONTENT"],
        rng_seed=0,
    )

    out = scorer.ctlr(
        {"goal_text": "create equation from goal: gauge field strength", "top_k": 3},
        type="create_eq_from_goal",
    )

    assert isinstance(out, dict)
    assert out["type"] == "create_eq_from_goal"
    assert isinstance(out["results"], list)
    assert len(out["results"]) == 3
    for item in out["results"]:
        assert "node_id" in item and isinstance(item["node_id"], str)
        assert "score" in item and isinstance(item["score"], float)

