"""
Tests for graph/pathway.py (Issue #1 – Pathways Creation, Issue #2 – Pathway Execution).

Run with: python graph/test_pathway.py
or:       PYTHONPATH=. pytest graph/test_pathway.py -v
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from typing import Any, Dict, List

import networkx as nx

# ---------------------------------------------------------------------------
# Bootstrap: load graph.pathway without triggering graph/__init__.py
# (graph/__init__.py imports from qbrain which may not be available here).
# ---------------------------------------------------------------------------
if "graph" not in sys.modules:
    _pkg = types.ModuleType("graph")
    sys.modules["graph"] = _pkg

if "graph.pathway" not in sys.modules:
    import os as _os
    _here = _os.path.dirname(_os.path.abspath(__file__))
    _spec = importlib.util.spec_from_file_location(
        "graph.pathway",
        _os.path.join(_here, "pathway.py"),
    )
    _mod = importlib.util.module_from_spec(_spec)
    sys.modules["graph.pathway"] = _mod
    _spec.loader.exec_module(_mod)

import pytest

from brain_schema import BrainNodeType, BrainEdgeRel, PathwayNode, PathwayParam
from graph.pathway import (
    PathwayBuilder,
    PathwayRunner,
    _count_pathway_nodes,
    _extract_params_from_node,
    _param_src,
    build_and_run_pathways,
    should_build_pathways,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_graph_with_nodes(*nodes: Dict[str, Any]) -> nx.MultiGraph:
    G = nx.MultiGraph()
    for n in nodes:
        nid = n.get("id") or n.get("action_name") or "unknown"
        G.add_node(str(nid), **{k: v for k, v in n.items() if k != "id"})
    return G


def _add_node_fn(G: nx.MultiGraph):
    def fn(attrs: Dict[str, Any]) -> None:
        nid = attrs["id"]
        G.add_node(nid, **{k: v for k, v in attrs.items() if k != "id"})
    return fn


def _add_edge_fn(G: nx.MultiGraph):
    def fn(src: str, trt: str, attrs: Dict[str, Any]) -> None:
        G.add_edge(src, trt, **attrs)
    return fn


def _method_node(
    node_id: str,
    params: List[Dict[str, Any]],
    *,
    user_id: str = "u1",
    func=None,
) -> Dict[str, Any]:
    n: Dict[str, Any] = {
        "id": node_id,
        "type": BrainNodeType.METHOD,
        "user_id": user_id,
        "action_name": node_id.split("::")[-1],
        "description": f"Method {node_id}",
        "params": json.dumps(params),
    }
    if func is not None:
        n["func"] = func
    return n


def _action_node(
    node_id: str,
    params: List[Dict[str, Any]],
    *,
    user_id: str = "u1",
) -> Dict[str, Any]:
    return {
        "id": node_id,
        "type": BrainNodeType.ACTION,
        "user_id": user_id,
        "action_name": node_id.split("::")[-1],
        "description": f"MCP action {node_id}",
        "params": json.dumps(params),
    }


# ---------------------------------------------------------------------------
# Unit tests – helpers
# ---------------------------------------------------------------------------


def test_extract_params_from_node_json_list():
    attrs = {
        "params": json.dumps([{"key": "module_ids"}, {"key": "param_ids"}])
    }
    result = _extract_params_from_node(attrs)
    assert len(result) == 2
    assert result[0]["key"] == "module_ids"


def test_extract_params_from_node_req_struct():
    attrs = {
        "req_struct": {"data": {"name": "string", "value": "float"}}
    }
    result = _extract_params_from_node(attrs)
    keys = {r["key"] for r in result}
    assert "name" in keys
    assert "value" in keys


def test_extract_params_from_node_empty():
    assert _extract_params_from_node({}) == []


def test_param_src_user_when_no_default():
    p = {"key": "module_ids"}
    assert _param_src(p, {}) == "USER"


def test_param_src_method_when_default_present():
    p = {"key": "timeout", "default": 30}
    assert _param_src(p, {}) == "METHOD"


def test_param_src_method_when_node_has_value():
    p = {"key": "alpha"}
    attrs = {"alpha": 0.01}
    assert _param_src(p, attrs) == "METHOD"


def test_param_src_explicit_override():
    p = {"key": "x", "src": "USER"}
    assert _param_src(p, {"x": "value"}) == "USER"


def test_count_pathway_nodes_empty():
    G = nx.MultiGraph()
    assert _count_pathway_nodes(G, "u1") == 0


def test_count_pathway_nodes_with_node():
    G = nx.MultiGraph()
    G.add_node("PATHWAY::u1::0::1", type=BrainNodeType.PATHWAY, user_id="u1")
    assert _count_pathway_nodes(G, "u1") == 1
    assert _count_pathway_nodes(G, "u2") == 0


def test_should_build_pathways_true_when_empty():
    G = nx.MultiGraph()
    assert should_build_pathways(G, "u1") is True


def test_should_build_pathways_false_after_build():
    G = nx.MultiGraph()
    G.add_node("PATHWAY::u1::0::1", type=BrainNodeType.PATHWAY, user_id="u1")
    assert should_build_pathways(G, "u1") is False


# ---------------------------------------------------------------------------
# Issue #1 – PathwayBuilder tests
# ---------------------------------------------------------------------------


class TestPathwayBuilder:

    def _build_graph(self) -> nx.MultiGraph:
        G = nx.MultiGraph()
        n1 = _method_node(
            "METHOD::set_cfg",
            [{"key": "module_ids"}, {"key": "method_ids"}, {"key": "param_ids"}],
        )
        G.add_node(n1["id"], **{k: v for k, v in n1.items() if k != "id"})
        n2 = _action_node(
            "ACTION::create_env",
            [{"key": "env_name"}, {"key": "description", "default": ""}],
        )
        G.add_node(n2["id"], **{k: v for k, v in n2.items() if k != "id"})
        return G

    def test_build_returns_pathway_nodes(self):
        G = self._build_graph()
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        nodes = builder.build()
        assert len(nodes) == 2

    def test_build_assigns_monotonic_path_idx(self):
        G = self._build_graph()
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        nodes = builder.build()
        indices = [pn.path_idx for pn in nodes]
        assert indices == sorted(indices)
        assert indices[0] == 0

    def test_build_pathway_nodes_added_to_graph(self):
        G = self._build_graph()
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        nodes = builder.build()
        for pn in nodes:
            assert G.has_node(pn.node_id), f"PATHWAY node {pn.node_id!r} missing from graph"
            attrs = dict(G.nodes[pn.node_id])
            assert attrs.get("type") == BrainNodeType.PATHWAY
            assert attrs.get("user_id") == "u1"

    def test_build_pathway_step_edges_created(self):
        G = self._build_graph()
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        nodes = builder.build()
        for pn in nodes:
            edges = list(G.edges(pn.node_id, data=True))
            rels = [e[2].get("rel") for e in edges]
            assert BrainEdgeRel.PATHWAY_STEP in rels, (
                f"No PATHWAY_STEP edge for node {pn.node_id!r}"
            )

    def test_build_params_classified_correctly(self):
        """module_ids/method_ids/param_ids have no default → src=USER."""
        G = self._build_graph()
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        nodes = builder.build()
        set_cfg_node = next(
            pn for pn in nodes if "set_cfg" in pn.method_or_action_id
        )
        for pp in set_cfg_node.params:
            assert pp.src == "USER", f"Expected USER src for {pp.key!r}"

    def test_build_params_with_defaults_method_src(self):
        """'description' has default='' → src=METHOD."""
        G = self._build_graph()
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        nodes = builder.build()
        create_env = next(
            pn for pn in nodes if "create_env" in pn.method_or_action_id
        )
        by_key = {pp.key: pp.src for pp in create_env.params}
        assert by_key["description"] == "METHOD"
        assert by_key["env_name"] == "USER"

    def test_build_with_target_node_ids(self):
        """Only the specified target node should be built into a pathway."""
        G = self._build_graph()
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        nodes = builder.build(target_node_ids=["METHOD::set_cfg"])
        assert len(nodes) == 1
        assert nodes[0].method_or_action_id == "METHOD::set_cfg"

    def test_should_build_pathways_false_after_build(self):
        G = self._build_graph()
        assert should_build_pathways(G, "u1") is True
        builder = PathwayBuilder(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
        )
        builder.build()
        assert should_build_pathways(G, "u1") is False


# ---------------------------------------------------------------------------
# Issue #2 – PathwayRunner tests
# ---------------------------------------------------------------------------


class TestPathwayRunner:

    def _simple_pathway_setup(self, with_callable: bool = False):
        G = nx.MultiGraph()

        call_log: List[Any] = []

        def _fn(payload):
            call_log.append(payload)
            return {"cfg_id": "cfg_001"}

        method_attrs: Dict[str, Any] = {
            "type": BrainNodeType.METHOD,
            "user_id": "u1",
            "action_name": "set_cfg",
            "params": json.dumps([
                {"key": "module_ids"},
                {"key": "param_ids"},
            ]),
        }
        if with_callable:
            method_attrs["func"] = _fn

        G.add_node("METHOD::set_cfg", **method_attrs)
        G.add_node(
            "PATHWAY::u1::0",
            type=BrainNodeType.PATHWAY,
            user_id="u1",
            path_idx=0,
            method_or_action_id="METHOD::set_cfg",
            params=json.dumps([
                {"key": "module_ids", "src": "USER"},
                {"key": "param_ids", "src": "USER"},
            ]),
        )

        pnode = PathwayNode(
            path_idx=0,
            node_id="PATHWAY::u1::0",
            method_or_action_id="METHOD::set_cfg",
            params=[
                PathwayParam(key="module_ids", src="USER"),
                PathwayParam(key="param_ids", src="USER"),
            ],
        )
        return G, pnode, call_log

    def test_run_need_data_when_user_params_missing(self):
        G, pnode, _ = self._simple_pathway_setup()
        runner = PathwayRunner(G=G)
        results = runner.run([pnode], resolved_fields={})
        assert len(results) == 1
        assert results[0]["status"] == "need_data"
        assert "module_ids" in results[0]["missing_params"]
        assert "param_ids" in results[0]["missing_params"]

    def test_run_executed_when_all_params_provided(self):
        G, pnode, _ = self._simple_pathway_setup()
        runner = PathwayRunner(G=G)
        results = runner.run(
            [pnode],
            resolved_fields={"module_ids": ["m1"], "param_ids": ["p1"]},
        )
        assert results[0]["status"] == "executed"

    def test_run_calls_callable_when_present(self):
        G, pnode, call_log = self._simple_pathway_setup(with_callable=True)
        runner = PathwayRunner(G=G)
        runner.run(
            [pnode],
            resolved_fields={"module_ids": ["m1"], "param_ids": ["p1"]},
        )
        assert len(call_log) == 1
        assert call_log[0]["data"]["module_ids"] == ["m1"]

    def test_run_step_output_propagated_to_next_step(self):
        """Output of step 0 should be available as resolved params for step 1."""
        G = nx.MultiGraph()

        def _step0_fn(payload):
            return {"cfg_id": "cfg_abc"}

        G.add_node(
            "METHOD::create_cfg",
            type=BrainNodeType.METHOD,
            user_id="u1",
            action_name="create_cfg",
            params=json.dumps([{"key": "name"}]),
            func=_step0_fn,
        )
        G.add_node(
            "METHOD::start_sim",
            type=BrainNodeType.METHOD,
            user_id="u1",
            action_name="start_sim",
            params=json.dumps([{"key": "cfg_id"}]),
        )

        pnode0 = PathwayNode(
            path_idx=0,
            node_id="PATHWAY::u1::0",
            method_or_action_id="METHOD::create_cfg",
            params=[PathwayParam(key="name", src="USER")],
        )
        pnode1 = PathwayNode(
            path_idx=1,
            node_id="PATHWAY::u1::1",
            method_or_action_id="METHOD::start_sim",
            params=[PathwayParam(key="cfg_id", src="USER")],
        )

        runner = PathwayRunner(G=G)
        results = runner.run(
            [pnode0, pnode1],
            resolved_fields={"name": "sim_1"},
        )
        # step 0 executed; step 1 should pick up cfg_id from step 0's output
        assert results[0]["status"] == "executed"
        assert results[1]["status"] == "executed"
        assert results[1]["resolved_params"]["cfg_id"] == "cfg_abc"

    def test_run_error_when_node_missing(self):
        G = nx.MultiGraph()
        pnode = PathwayNode(
            path_idx=0,
            node_id="PATHWAY::u1::0",
            method_or_action_id="METHOD::ghost",
            params=[],
        )
        runner = PathwayRunner(G=G)
        results = runner.run([pnode], resolved_fields={})
        assert results[0]["status"] == "error"


# ---------------------------------------------------------------------------
# Issue #1 & #2 – build_and_run_pathways integration test
# ---------------------------------------------------------------------------


class TestBuildAndRunPathways:

    def test_full_pipeline_no_method_nodes(self):
        """With no METHOD/ACTION nodes, expect need_data and empty pathway_nodes."""
        G = nx.MultiGraph()
        result = build_and_run_pathways(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
            resolved_fields={},
            query="create sim cfg",
        )
        assert result["goal_case"] == "PATHWAY"
        assert result["pathway_nodes"] == []
        assert result["status"] == "need_data"

    def test_full_pipeline_with_set_cfg(self):
        G = nx.MultiGraph()
        G.add_node(
            "METHOD::set_cfg",
            type=BrainNodeType.METHOD,
            user_id="u1",
            action_name="set_cfg",
            description="Create sim cfg with components",
            params=json.dumps([
                {"key": "module_ids"},
                {"key": "method_ids"},
                {"key": "param_ids"},
            ]),
        )

        result = build_and_run_pathways(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
            resolved_fields={
                "module_ids": ["m1"],
                "method_ids": ["met1"],
                "param_ids": ["p1"],
            },
            query="create sim cfg",
        )

        assert result["goal_case"] == "PATHWAY"
        assert len(result["pathway_nodes"]) == 1
        pn = result["pathway_nodes"][0]
        assert pn["path_idx"] == 0
        assert pn["method_or_action_id"] == "METHOD::set_cfg"
        param_keys = {p["key"] for p in pn["params"]}
        assert "module_ids" in param_keys
        assert result["status"] == "executed"

    def test_full_pipeline_missing_user_params(self):
        G = nx.MultiGraph()
        G.add_node(
            "METHOD::set_cfg",
            type=BrainNodeType.METHOD,
            user_id="u1",
            action_name="set_cfg",
            params=json.dumps([
                {"key": "module_ids"},
                {"key": "param_ids"},
            ]),
        )
        result = build_and_run_pathways(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
            resolved_fields={},  # nothing provided
            query="create sim cfg",
        )
        assert result["status"] == "need_data"
        assert "module_ids" in result["missing_fields"]
        assert "param_ids" in result["missing_fields"]
        assert "need the following user-provided params" in result["next_message"]

    def test_pathway_nodes_added_to_graph(self):
        G = nx.MultiGraph()
        G.add_node(
            "METHOD::set_cfg",
            type=BrainNodeType.METHOD,
            user_id="u1",
            action_name="set_cfg",
            params=json.dumps([{"key": "module_ids"}]),
        )
        assert should_build_pathways(G, "u1") is True
        build_and_run_pathways(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
            resolved_fields={"module_ids": ["m1"]},
        )
        assert should_build_pathways(G, "u1") is False

    def test_path_idx_structure(self):
        """Each pathway_node entry has correct path_idx in result dict."""
        G = nx.MultiGraph()
        for i, name in enumerate(["step_a", "step_b", "step_c"]):
            G.add_node(
                f"METHOD::{name}",
                type=BrainNodeType.METHOD,
                user_id="u1",
                action_name=name,
                params=json.dumps([{"key": f"arg_{name}"}]),
            )
        result = build_and_run_pathways(
            G=G,
            user_id="u1",
            add_node_fn=_add_node_fn(G),
            add_edge_fn=_add_edge_fn(G),
            resolved_fields={f"arg_{n}": f"val_{n}" for n in ["step_a", "step_b", "step_c"]},
        )
        indices = [pn["path_idx"] for pn in result["pathway_nodes"]]
        assert indices == sorted(indices)
        assert indices[0] == 0


if __name__ == "__main__":
    # Run tests directly without pytest.
    test_classes = [
        TestPathwayBuilder,
        TestPathwayRunner,
        TestBuildAndRunPathways,
    ]

    standalone_fns = [
        test_extract_params_from_node_json_list,
        test_extract_params_from_node_req_struct,
        test_extract_params_from_node_empty,
        test_param_src_user_when_no_default,
        test_param_src_method_when_default_present,
        test_param_src_method_when_node_has_value,
        test_param_src_explicit_override,
        test_count_pathway_nodes_empty,
        test_count_pathway_nodes_with_node,
        test_should_build_pathways_true_when_empty,
        test_should_build_pathways_false_after_build,
    ]

    failed = 0
    for fn in standalone_fns:
        try:
            fn()
            print(f"  [ok] {fn.__name__}")
        except Exception as exc:
            print(f"  [FAIL] {fn.__name__}: {exc}")
            failed += 1

    for cls in test_classes:
        inst = cls()
        for name in dir(cls):
            if not name.startswith("test_"):
                continue
            try:
                getattr(inst, name)()
                print(f"  [ok] {cls.__name__}.{name}")
            except Exception as exc:
                print(f"  [FAIL] {cls.__name__}.{name}: {exc}")
                failed += 1

    if failed:
        print(f"\n{failed} test(s) FAILED")
        sys.exit(1)
    else:
        print("\nAll pathway tests passed.")
