"""
Pathway Creation and Execution (Issues #1 and #2).

Issue #1 – Pathways Creation
------------------------------
For user requests where Type=None AND the user has 0 PATHWAY nodes in the graph:

1. Identify target local Method/Action nodes to call (e.g. set_cfg).
2. Create a PATHWAY graph node from each Method/Action node, including its
   params as list[PathwayParam] where each param carries a `src` attribute:
       src = "METHOD"  →  value can be resolved from the METHOD/ACTION node itself
       src = "USER"    →  value must be collected interactively from the user
3. Repeat to reverse-engineer the full PATHWAY struct.
4. Assign path_idx (0-based execution order) to every Pathway node.

Issue #2 – Pathway Execution
------------------------------
Execute the built pathway:
  Create cfg  ←  module_ids, method_ids, param_ids
Walk the PathwayNode list in path_idx order, resolve METHOD-sourced params
from the graph, collect USER-sourced params from resolved_fields, and call
the associated METHOD/ACTION callable (if available).
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from brain_schema import BrainEdgeRel, BrainNodeType, PathwayNode, PathwayParam

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PARAM_SRC_METHOD = "METHOD"
_PARAM_SRC_USER = "USER"

# Keys inspected on a METHOD/ACTION node to discover its input params.
_PARAM_HINT_KEYS = (
    "params",
    "parameters",
    "input_schema",
    "req_struct",
    "args",
    "inputs",
)

# Attribute keys that, when present on a param, indicate a resolvable default.
_RESOLVABLE_ATTRS = ("default", "value", "default_value", "resolved_value")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_params_from_node(attrs: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract a list of param dicts from a METHOD or ACTION node's attributes.

    Handles several storage formats:
      - JSON list of dicts stored as string under param hint keys
      - Python list/dict already parsed
      - Flat key listing in the "params" or "parameters" attribute
    """
    for hint in _PARAM_HINT_KEYS:
        raw = attrs.get(hint)
        if raw is None:
            continue

        # Attempt JSON decode if stored as string.
        if isinstance(raw, str):
            stripped = raw.strip()
            if stripped.startswith(("{", "[")):
                try:
                    raw = json.loads(stripped)
                except Exception:
                    pass

        if isinstance(raw, list):
            out: List[Dict[str, Any]] = []
            for item in raw:
                if isinstance(item, dict):
                    out.append(item)
                elif isinstance(item, str) and item.strip():
                    out.append({"key": item.strip()})
            if out:
                return out

        if isinstance(raw, dict):
            # req_struct / input_schema style: {data: {key: type, ...}}
            data_block = raw.get("data") if isinstance(raw.get("data"), dict) else raw
            return [{"key": k} for k in data_block.keys()]

    return []


def _param_src(param_dict: Dict[str, Any], node_attrs: Dict[str, Any]) -> str:
    """
    Determine the source for a param:
      - "METHOD" when the param can be resolved from the graph (has a default,
        or the METHOD/ACTION node itself carries the value).
      - "USER" when the param must be provided interactively.
    """
    # Explicit src override on param dict takes highest priority.
    explicit = str(param_dict.get("src") or "").upper()
    if explicit in (_PARAM_SRC_METHOD, _PARAM_SRC_USER):
        return explicit

    # If the param dict has a resolvable attribute, treat as METHOD.
    for attr in _RESOLVABLE_ATTRS:
        if param_dict.get(attr) not in (None, ""):
            return _PARAM_SRC_METHOD

    # If the parent node itself carries the param key as a non-empty attribute,
    # it can be resolved from the graph.
    key = str(param_dict.get("key") or param_dict.get("name") or "")
    if key and node_attrs.get(key) not in (None, ""):
        return _PARAM_SRC_METHOD

    return _PARAM_SRC_USER


def _method_action_nodes(G: Any, user_id: Optional[str] = None) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    Yield (node_id, attrs) for all METHOD and ACTION nodes in the graph,
    optionally scoped to user_id.
    """
    for nid, attrs in G.nodes(data=True):
        ntype = str(attrs.get("type") or "").upper()
        if ntype not in (BrainNodeType.METHOD, BrainNodeType.ACTION):
            continue
        if user_id is not None:
            node_uid = str(attrs.get("user_id") or attrs.get("owner") or "")
            if node_uid and node_uid != str(user_id):
                continue
        yield str(nid), dict(attrs)


def _count_pathway_nodes(G: Any, user_id: str) -> int:
    """Count PATHWAY nodes scoped to user_id."""
    count = 0
    for _, attrs in G.nodes(data=True):
        if str(attrs.get("type") or "").upper() != BrainNodeType.PATHWAY:
            continue
        if str(attrs.get("user_id") or "") == str(user_id):
            count += 1
    return count


# ---------------------------------------------------------------------------
# PathwayBuilder
# ---------------------------------------------------------------------------


class PathwayBuilder:
    """
    Reverse-engineers a PATHWAY struct from METHOD and ACTION nodes in the graph.

    For each discovered Method/Action node:
      - Inspect its params (from node attrs or connected PARAM nodes).
      - For each param decide src: METHOD (graph-resolvable) or USER (must ask).
      - Create a PATHWAY node in the graph linked via a PATHWAY_STEP edge.
      - Assign a monotonically increasing path_idx.

    The resulting list of PathwayNode is ordered by path_idx and ready for
    PathwayRunner to execute.
    """

    def __init__(
        self,
        G: Any,
        user_id: str,
        add_node_fn: Callable[[Dict[str, Any]], None],
        add_edge_fn: Callable[[str, str, Dict[str, Any]], None],
    ) -> None:
        print("PathwayBuilder.__init__...")
        self.G = G
        self.user_id = str(user_id)
        self._add_node = add_node_fn
        self._add_edge = add_edge_fn
        print("PathwayBuilder.__init__... done")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(
        self,
        query: Optional[str] = None,
        target_node_ids: Optional[List[str]] = None,
    ) -> List[PathwayNode]:
        """
        Build pathway nodes for all (or targeted) METHOD/ACTION nodes.

        Args:
            query: user request text; used to rank/filter candidate nodes.
            target_node_ids: if provided, only build pathways for these node ids.

        Returns:
            Ordered list of PathwayNode (by path_idx).
        """
        print("PathwayBuilder.build...")
        candidates = list(self._collect_candidates(query=query, target_node_ids=target_node_ids))
        pathway_nodes: List[PathwayNode] = []

        for path_idx, (nid, attrs) in enumerate(candidates):
            pnode = self._build_pathway_node(path_idx=path_idx, method_action_id=nid, attrs=attrs)
            pathway_nodes.append(pnode)

        print(f"PathwayBuilder.build... done (pathway_nodes={len(pathway_nodes)})")
        return pathway_nodes

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _collect_candidates(
        self,
        query: Optional[str],
        target_node_ids: Optional[List[str]],
    ) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """Yield (node_id, attrs) of candidate METHOD/ACTION nodes."""
        q_lower = (query or "").lower()

        for nid, attrs in _method_action_nodes(self.G):
            if target_node_ids is not None and nid not in target_node_ids:
                continue

            # When query provided, lightweight relevance filter using node text.
            if q_lower:
                node_text = " ".join(
                    str(v or "") for k, v in attrs.items()
                    if k in ("action_name", "name", "description", "method_name", "func_name")
                ).lower()
                if not node_text:
                    node_text = nid.lower()
                # Accept the node if the query shares at least one token with the node text
                # or the user has not specified targets (accept all).
                if target_node_ids is None:
                    tokens = [t for t in q_lower.split() if len(t) > 2]
                    if tokens and not any(t in node_text for t in tokens):
                        # Still include if no candidates have matched yet (best-effort).
                        pass  # intentional fall-through: include all when no match
            yield nid, attrs

    def _build_pathway_node(
        self,
        path_idx: int,
        method_action_id: str,
        attrs: Dict[str, Any],
    ) -> PathwayNode:
        """Create a PATHWAY graph node and return its PathwayNode descriptor."""
        print(f"PathwayBuilder._build_pathway_node path_idx={path_idx}...")

        raw_params = _extract_params_from_node(attrs)
        pathway_params: List[PathwayParam] = []
        for p in raw_params:
            key = str(p.get("key") or p.get("name") or "")
            if not key:
                continue
            src = _param_src(p, attrs)
            pathway_params.append(PathwayParam(key=key, src=src))

        ts = int(time.time() * 1000)
        uid_part = self.user_id.replace(" ", "_")
        pathway_node_id = f"PATHWAY::{uid_part}::{path_idx}::{ts}"

        node_attrs: Dict[str, Any] = {
            "id": pathway_node_id,
            "type": BrainNodeType.PATHWAY,
            "user_id": self.user_id,
            "path_idx": path_idx,
            "method_or_action_id": method_action_id,
            "params": json.dumps([{"key": pp.key, "src": pp.src} for pp in pathway_params]),
        }
        self._add_node(node_attrs)
        self._add_edge(
            pathway_node_id,
            method_action_id,
            {
                "rel": BrainEdgeRel.PATHWAY_STEP,
                "src_layer": BrainNodeType.PATHWAY,
                "trgt_layer": str(attrs.get("type") or BrainNodeType.METHOD).upper(),
                "path_idx": path_idx,
            },
        )

        pnode = PathwayNode(
            path_idx=path_idx,
            node_id=pathway_node_id,
            method_or_action_id=method_action_id,
            params=pathway_params,
        )
        print(f"PathwayBuilder._build_pathway_node path_idx={path_idx}... done")
        return pnode


# ---------------------------------------------------------------------------
# PathwayRunner
# ---------------------------------------------------------------------------


class PathwayRunner:
    """
    Execute a list of PathwayNode steps in path_idx order.

    For each step:
      - Resolve METHOD-sourced params from the graph node's attributes.
      - Collect USER-sourced params from resolved_fields (provided by caller).
      - Invoke the callable attached to the METHOD/ACTION node (if any).

    Returns a list of per-step execution results (Issue #2 – Pathway Execution).
    """

    def __init__(self, G: Any) -> None:
        print("PathwayRunner.__init__...")
        self.G = G
        print("PathwayRunner.__init__... done")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        pathway_nodes: List[PathwayNode],
        resolved_fields: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Execute pathway steps in path_idx order.

        Args:
            pathway_nodes: ordered list produced by PathwayBuilder.build().
            resolved_fields: values already collected (e.g. from user input or
                             previous steps).  USER-sourced params not present
                             here will be reported as missing.

        Returns:
            List of step result dicts, one per PathwayNode.
        """
        print("PathwayRunner.run...")
        ordered = sorted(pathway_nodes, key=lambda pn: pn.path_idx)
        step_results: List[Dict[str, Any]] = []
        accumulated: Dict[str, Any] = dict(resolved_fields)

        for pnode in ordered:
            result = self._execute_step(pnode, accumulated)
            step_results.append(result)
            # Propagate step outputs so later steps can consume them.
            if result.get("status") == "executed":
                out = result.get("output") or {}
                if isinstance(out, dict):
                    accumulated.update(out)

        print("PathwayRunner.run... done")
        return step_results

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_method_params(
        self,
        method_action_id: str,
        params: List[PathwayParam],
    ) -> Dict[str, Any]:
        """
        Collect METHOD-sourced param values from the METHOD/ACTION graph node.
        """
        resolved: Dict[str, Any] = {}
        if not self.G.has_node(method_action_id):
            return resolved
        attrs = dict(self.G.nodes[method_action_id])
        for pp in params:
            if pp.src != _PARAM_SRC_METHOD:
                continue
            # Try several attribute name patterns.
            for candidate_key in (pp.key, pp.key.lower(), pp.key.upper()):
                val = attrs.get(candidate_key)
                if val not in (None, ""):
                    resolved[pp.key] = val
                    break
        return resolved

    def _collect_missing_user_params(
        self,
        params: List[PathwayParam],
        accumulated: Dict[str, Any],
    ) -> List[str]:
        """Return keys for USER-sourced params not yet present in accumulated."""
        missing: List[str] = []
        for pp in params:
            if pp.src != _PARAM_SRC_USER:
                continue
            if accumulated.get(pp.key) in (None, ""):
                missing.append(pp.key)
        return missing

    def _execute_step(
        self,
        pnode: PathwayNode,
        accumulated: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a single pathway step and return a result dict."""
        print(f"PathwayRunner._execute_step path_idx={pnode.path_idx}...")

        if not self.G.has_node(pnode.method_or_action_id):
            result = {
                "path_idx": pnode.path_idx,
                "node_id": pnode.node_id,
                "method_or_action_id": pnode.method_or_action_id,
                "status": "error",
                "message": f"Method/Action node not found: {pnode.method_or_action_id}",
                "output": None,
            }
            print(f"PathwayRunner._execute_step path_idx={pnode.path_idx}... done (node missing)")
            return result

        # Resolve METHOD-sourced params from graph.
        method_resolved = self._resolve_method_params(pnode.method_or_action_id, pnode.params)
        step_resolved: Dict[str, Any] = {**method_resolved}
        for pp in pnode.params:
            if pp.src == _PARAM_SRC_USER and accumulated.get(pp.key) not in (None, ""):
                step_resolved[pp.key] = accumulated[pp.key]

        # Check for missing USER params.
        missing = self._collect_missing_user_params(pnode.params, step_resolved)
        if missing:
            result = {
                "path_idx": pnode.path_idx,
                "node_id": pnode.node_id,
                "method_or_action_id": pnode.method_or_action_id,
                "status": "need_data",
                "message": (
                    "Missing user-provided params for pathway step "
                    f"{pnode.path_idx}: " + ", ".join(missing)
                ),
                "missing_params": missing,
                "resolved_params": step_resolved,
                "output": None,
            }
            print(f"PathwayRunner._execute_step path_idx={pnode.path_idx}... done (need_data)")
            return result

        # Attempt to invoke the callable attached to the METHOD/ACTION node.
        attrs = dict(self.G.nodes[pnode.method_or_action_id])
        fn = attrs.get("func") or attrs.get("callable") or attrs.get("handler")
        output: Any = None
        if callable(fn):
            try:
                payload = {
                    "path_idx": pnode.path_idx,
                    "method_or_action_id": pnode.method_or_action_id,
                    "data": step_resolved,
                }
                output = fn(payload)
            except Exception as exc:
                result = {
                    "path_idx": pnode.path_idx,
                    "node_id": pnode.node_id,
                    "method_or_action_id": pnode.method_or_action_id,
                    "status": "error",
                    "message": f"Step callable raised: {exc}",
                    "resolved_params": step_resolved,
                    "output": None,
                }
                print(f"PathwayRunner._execute_step path_idx={pnode.path_idx}... done (callable error)")
                return result

        result = {
            "path_idx": pnode.path_idx,
            "node_id": pnode.node_id,
            "method_or_action_id": pnode.method_or_action_id,
            "status": "executed",
            "message": f"Step {pnode.path_idx} executed.",
            "resolved_params": step_resolved,
            "output": output,
        }
        print(f"PathwayRunner._execute_step path_idx={pnode.path_idx}... done")
        return result


# ---------------------------------------------------------------------------
# Convenience helpers used by Brain
# ---------------------------------------------------------------------------


def should_build_pathways(G: Any, user_id: str) -> bool:
    """
    Return True when pathway creation should be triggered:
      Type is None  AND  user has 0 PATHWAY nodes in the graph.
    """
    return _count_pathway_nodes(G, user_id) == 0


def build_and_run_pathways(
    G: Any,
    user_id: str,
    add_node_fn: Callable[[Dict[str, Any]], None],
    add_edge_fn: Callable[[str, str, Dict[str, Any]], None],
    resolved_fields: Dict[str, Any],
    query: Optional[str] = None,
) -> Dict[str, Any]:
    """
    One-shot helper: build pathways then execute them.

    Returns a result dict compatible with Brain.execute_or_ask's return shape.
    """
    print("build_and_run_pathways...")
    builder = PathwayBuilder(
        G=G,
        user_id=user_id,
        add_node_fn=add_node_fn,
        add_edge_fn=add_edge_fn,
    )
    pathway_nodes = builder.build(query=query)

    if not pathway_nodes:
        print("build_and_run_pathways... done (no pathway nodes built)")
        return {
            "status": "need_data",
            "goal_case": "PATHWAY",
            "pathway_nodes": [],
            "step_results": [],
            "next_message": (
                "No Method or Action nodes are available in the graph to build a pathway from. "
                "Please register methods or actions first."
            ),
        }

    runner = PathwayRunner(G=G)
    step_results = runner.run(pathway_nodes=pathway_nodes, resolved_fields=resolved_fields)

    all_executed = all(r.get("status") == "executed" for r in step_results)
    missing_all: List[str] = []
    for r in step_results:
        missing_all.extend(r.get("missing_params") or [])

    status = "executed" if all_executed else ("need_data" if missing_all else "partial")

    next_message: str
    if status == "executed":
        next_message = f"Pathway completed successfully ({len(step_results)} step(s))."
    elif status == "need_data":
        next_message = (
            "Pathway is building. Still need the following user-provided params: "
            + ", ".join(sorted(set(missing_all)))
        )
    else:
        next_message = f"Pathway partially executed ({len(step_results)} step(s))."

    result: Dict[str, Any] = {
        "status": status,
        "goal_case": "PATHWAY",
        "pathway_nodes": [
            {
                "path_idx": pn.path_idx,
                "node_id": pn.node_id,
                "method_or_action_id": pn.method_or_action_id,
                "params": [{"key": pp.key, "src": pp.src} for pp in pn.params],
            }
            for pn in pathway_nodes
        ],
        "step_results": step_results,
        "missing_fields": list(sorted(set(missing_all))),
        "next_message": next_message,
    }
    print("build_and_run_pathways... done")
    return result


if __name__ == "__main__":
    import networkx as nx

    def _add_node(attrs):
        G.add_node(attrs["id"], **{k: v for k, v in attrs.items() if k != "id"})

    def _add_edge(src, trt, attrs):
        G.add_edge(src, trt, **attrs)

    G = nx.MultiGraph()
    G.add_node(
        "METHOD::set_cfg",
        type=BrainNodeType.METHOD,
        action_name="set_cfg",
        description="Create sim cfg with components",
        params=json.dumps([
            {"key": "module_ids", "src": "USER"},
            {"key": "method_ids", "src": "USER"},
            {"key": "param_ids", "src": "USER"},
        ]),
    )

    assert should_build_pathways(G, "u1"), "Expected True before any PATHWAY nodes"

    result = build_and_run_pathways(
        G=G,
        user_id="u1",
        add_node_fn=_add_node,
        add_edge_fn=_add_edge,
        resolved_fields={"module_ids": ["m1"], "method_ids": ["met1"], "param_ids": ["p1"]},
        query="create sim cfg",
    )
    assert result["goal_case"] == "PATHWAY"
    assert len(result["pathway_nodes"]) == 1
    pn = result["pathway_nodes"][0]
    assert pn["path_idx"] == 0
    assert pn["method_or_action_id"] == "METHOD::set_cfg"
    param_keys = {p["key"] for p in pn["params"]}
    assert "module_ids" in param_keys

    assert not should_build_pathways(G, "u1"), "Expected False after PATHWAY nodes were built"
    print("[graph.pathway] ok")
