from __future__ import annotations

from typing import Any, Dict

import networkx as nx

from qbrain.graph.brn.brain_schema import BrainEdgeRel, BrainNodeType
from qbrain.graph.brn.think_manager import ThinkManager


class _DummyQB:
    """Minimal stand-in for QBrain manager; not used directly in tests."""

    def __init__(self) -> None:
        self.db = None


def _build_simple_graph(user_id: str = "test_user_1") -> nx.MultiGraph:
    G = nx.MultiGraph()

    user_nid = f"USER::{user_id}"
    G.add_node(user_nid, type=BrainNodeType.USER, user_id=user_id)

    # One PARAM node
    param_nid = "LTS::params::123"
    G.add_node(
        param_nid,
        type=BrainNodeType.LONG_TERM_STORAGE,
        user_id=user_id,
        table_name="params",
        row_id="123",
        description="learning rate parameter alpha",
    )
    G.add_edge(
        user_nid,
        param_nid,
        rel=BrainEdgeRel.REFERENCES_TABLE_ROW,
        src_layer=BrainNodeType.USER,
        trgt_layer=BrainNodeType.LONG_TERM_STORAGE,
    )

    # One FIELD node
    field_nid = "LTS::fields::456"
    G.add_node(
        field_nid,
        type=BrainNodeType.LONG_TERM_STORAGE,
        user_id=user_id,
        table_name="fields",
        row_id="456",
        description="output field for loss curve",
    )
    G.add_edge(
        user_nid,
        field_nid,
        rel=BrainEdgeRel.REFERENCES_TABLE_ROW,
        src_layer=BrainNodeType.USER,
        trgt_layer=BrainNodeType.LONG_TERM_STORAGE,
    )

    return G


def test_suggest_missing_fields_param() -> None:
    G = _build_simple_graph()
    tm = ThinkManager(G=G, qb=_DummyQB(), user_id="test_user_1")

    case: Dict[str, Any] = {
        "case": "SET_PARAM",
        "desc": "Create or update a simulation parameter.",
    }
    suggestions = tm.suggest_missing_fields(case, ["param_id"])

    assert "param_id" in suggestions
    assert suggestions["param_id"], "Expected at least one suggestion for param_id"
    first = suggestions["param_id"][0]
    assert first["node_id"].startswith("LTS::params::")
    assert first["table_name"] == "params"


def test_summarize_component_includes_neighbors() -> None:
    G = _build_simple_graph()
    tm = ThinkManager(G=G, qb=_DummyQB(), user_id="test_user_1")

    summary = tm.summarize_component("params", "123")
    assert summary["node_id"] == "LTS::params::123"
    assert summary["attrs"]["table_name"] == "params"
    assert any(n["node_id"].startswith("USER::") for n in summary["neighbors"])


def test_analyze_case_context_uses_graph_stats_and_suggestions() -> None:
    G = _build_simple_graph()
    tm = ThinkManager(G=G, qb=_DummyQB(), user_id="test_user_1")

    case: Dict[str, Any] = {
        "case": "SET_PARAM",
        "desc": "Create or update a simulation parameter.",
    }
    result = tm.analyze_case_context(
        case_item=case,
        resolved_fields={"name": "alpha"},
        missing_fields=["param_id"],
    )

    assert result["case"] == "SET_PARAM"
    assert "param_id" in result["missing_keys"]
    assert result["graph_stats"]["params"] >= 1
    assert "param_id" in result["suggestions"]
    assert result["suggestions"]["param_id"]

