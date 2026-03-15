from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import networkx as nx

from brain_schema import BrainEdgeRel, BrainNodeType


class ThinkManager:
    """
    Lightweight helper around the Brain MultiGraph.

    Provides graph- and case-aware analysis utilities to help fill missing
    case fields and to inspect component-level context for a given user.
    """

    def __init__(self, G: nx.MultiGraph, qb: Any, *, user_id: str) -> None:
        print("ThinkManager.__init__...")
        self.G = G
        self.qb = qb
        self.user_id = str(user_id)
        print("ThinkManager.__init__... done")

    # -------------------------
    # Internal graph helpers
    # -------------------------

    def _iter_user_nodes(self) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """Yield all nodes that belong to this user_id."""
        if self.G is None:
            return
        for nid, attrs in self.G.nodes(data=True):
            try:
                if str(attrs.get("user_id") or "") == self.user_id:
                    yield nid, dict(attrs)
            except Exception:
                continue

    def _iter_user_long_term_nodes(
        self,
        table_name: Optional[str] = None,
    ) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """
        Yield user-scoped long-term nodes, optionally filtered by table_name.

        This includes LONG_TERM_STORAGE plus related FILE / METHOD / OBJECT nodes
        that are used by the file-manager GraphProcessor.
        """
        table_name_l = table_name.lower() if isinstance(table_name, str) else None

        for nid, attrs in self._iter_user_nodes():
            ntype = str(attrs.get("type") or "").upper()
            if ntype not in {
                getattr(BrainNodeType, "LONG_TERM_STORAGE", "LONG_TERM_STORAGE"),
                getattr(BrainNodeType, "FILE", "FILE"),
                getattr(BrainNodeType, "METHOD", "METHOD"),
                getattr(BrainNodeType, "OBJECT", "OBJECT"),
                getattr(BrainNodeType, "EQUATION", "EQUATION"),
            }:
                continue

            if table_name_l is not None:
                tname = str(attrs.get("table_name") or "").lower()
                if tname != table_name_l:
                    continue

            yield nid, attrs

    @staticmethod
    def _component_node_id(component_type: str, component_id: str) -> str:
        """Map a logical component type/id pair to the canonical LTS node id."""
        ctype = (component_type or "").strip().lower()
        cid = str(component_id)

        if ctype.endswith("s"):
            ctype = ctype[:-1]

        if "param" in ctype:
            prefix = "params"
        elif "field" in ctype:
            prefix = "fields"
        elif "method" in ctype or "equation" in ctype or "object" in ctype:
            prefix = "methods"
        elif "file" in ctype or "module" in ctype or "script" in ctype:
            prefix = "files"
        else:
            prefix = ctype or "items"

        return f"LTS::{prefix}::{cid}"

    @staticmethod
    def _tokenize_key(key: str) -> List[str]:
        """Split a field key into stable tokens (lowercased, alnum only)."""
        s = re.sub(r"[^a-z0-9]+", " ", (key or "").lower())
        return [t for t in s.split() if t]

    @staticmethod
    def _infer_table_for_key(field_key: str, case_item: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Heuristically infer a manager/table domain for a missing field key."""
        k = (field_key or "").lower()

        if "param" in k:
            return "params"
        if "field" in k:
            return "fields"
        if "method" in k or "equation" in k or "func" in k or "callable" in k:
            return "methods"
        if "file" in k or "path" in k or "module" in k or "script" in k:
            return "files"
        if "env" in k:
            return "envs"

        # Fallback: optionally look at case name/description for hints.
        if case_item:
            text = f"{case_item.get('case') or ''} {case_item.get('desc') or ''}".lower()
            if "param" in text:
                return "params"
            if "field" in text:
                return "fields"
            if "method" in text or "equation" in text:
                return "methods"
            if "file" in text or "module" in text:
                return "files"

        return None

    @staticmethod
    def _infer_domains_for_case(case_item: Dict[str, Any]) -> List[str]:
        """
        Infer which domains (tables) are most relevant for a given case.

        Used for higher-level context tracing; defaults to a small, broad set
        when no strong hints are present.
        """
        text = f"{case_item.get('case') or ''} {case_item.get('desc') or ''}".lower()
        domains: List[str] = []

        mapping = [
            ("param", "params"),
            ("field", "fields"),
            ("method", "methods"),
            ("equation", "methods"),
            ("file", "files"),
            ("module", "files"),
        ]
        for token, domain in mapping:
            if token in text and domain not in domains:
                domains.append(domain)

        if not domains:
            domains = ["params", "fields", "methods", "files"]
        return domains

    @staticmethod
    def _score_node_for_key(
        key_tokens: List[str],
        nid: str,
        attrs: Dict[str, Any],
    ) -> int:
        """Deterministic overlap score between key tokens and node attributes."""
        haystack_parts: List[str] = [
            str(nid),
            str(attrs.get("row_id") or ""),
            str(attrs.get("table_name") or ""),
            str(attrs.get("description") or ""),
            str(attrs.get("name") or ""),
            str(attrs.get("title") or ""),
        ]
        haystack = " ".join(haystack_parts).lower()

        score = 0
        for tok in key_tokens:
            if tok and tok in haystack:
                score += 1
        return score

    def _count_long_term(self, table_name: str) -> int:
        """Count user long-term nodes for a specific table_name."""
        return sum(1 for _ in self._iter_user_long_term_nodes(table_name))

    # -------------------------
    # Public component helpers
    # -------------------------

    def summarize_component(self, component_type: str, component_id: str) -> Dict[str, Any]:
        """
        Summarize a component (param/field/method/file) and its immediate neighbors.
        """
        print("summarize_component...")
        summary: Dict[str, Any] = {
            "component_type": component_type,
            "component_id": str(component_id),
            "node_id": None,
            "attrs": {},
            "neighbors": [],
        }

        if self.G is None:
            print("summarize_component... done")
            return summary

        nid = self._component_node_id(component_type, component_id)
        if not self.G.has_node(nid):
            # Fallback: search by table_name + row_id under this user.
            ctype = (component_type or "").strip().lower()
            logical = "params"
            if "field" in ctype:
                logical = "fields"
            elif "method" in ctype or "equation" in ctype or "object" in ctype:
                logical = "methods"
            elif "file" in ctype or "module" in ctype or "script" in ctype:
                logical = "files"

            for cand_id, attrs in self._iter_user_long_term_nodes():
                if str(attrs.get("table_name") or "").lower() != logical:
                    continue
                if str(attrs.get("row_id") or "") == str(component_id):
                    nid = cand_id
                    break

            if not self.G.has_node(nid):
                print("summarize_component... done")
                return summary

        attrs = dict(self.G.nodes[nid])
        summary["node_id"] = nid
        summary["attrs"] = {k: v for k, v in attrs.items()}

        neighbors: List[Dict[str, Any]] = []
        try:
            for neighbor in self.G.neighbors(nid):
                n_attrs = dict(self.G.nodes[neighbor])
                edge_data = self.G.get_edge_data(nid, neighbor) or {}
                edge_items: Iterable[Dict[str, Any]]
                if isinstance(self.G, (nx.MultiGraph, nx.MultiDiGraph)):
                    edge_items = [edata for _, edata in getattr(edge_data, "items", lambda: [])()]
                else:
                    edge_items = [edge_data]

                for eattrs in edge_items:
                    if not isinstance(eattrs, dict):
                        continue
                    neighbors.append(
                        {
                            "node_id": neighbor,
                            "node_type": n_attrs.get("type"),
                            "rel": eattrs.get("rel"),
                            "src_layer": eattrs.get("src_layer"),
                            "trgt_layer": eattrs.get("trgt_layer"),
                        }
                    )
        except Exception as exc:
            print(f"summarize_component: warning: {exc}")

        summary["neighbors"] = neighbors
        print("summarize_component... done")
        return summary

    def trace_case_graph_context(self, case_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Provide a coarse view of how a case maps onto the graph for this user.

        Returns per-domain node ids and (when possible) shortest paths from the
        user node to those long-term nodes.
        """
        print("trace_case_graph_context...")
        out: Dict[str, Any] = {
            "case": str(case_item.get("case") or ""),
            "domains": {},
        }

        if self.G is None:
            print("trace_case_graph_context... done")
            return out

        user_node_id = f"USER::{self.user_id}"
        domains = self._infer_domains_for_case(case_item)

        for domain in domains:
            node_ids = [nid for nid, _ in self._iter_user_long_term_nodes(domain)]
            domain_info: Dict[str, Any] = {
                "table_name": domain,
                "node_ids": node_ids,
                "paths_from_user": {},
            }

            if self.G.has_node(user_node_id):
                for nid in node_ids[:25]:
                    try:
                        path = nx.shortest_path(self.G, user_node_id, nid)
                    except Exception:
                        path = None
                    if path:
                        domain_info["paths_from_user"][nid] = path

            out["domains"][domain] = domain_info

        print("trace_case_graph_context... done")
        return out

    # -------------------------
    # Case-aware helpers
    # -------------------------

    def suggest_missing_fields(
        self,
        case_item: Dict[str, Any],
        missing_fields: Iterable[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Suggest candidate graph-backed values for each missing field key.

        Uses deterministic string heuristics (no new embeddings) over user
        long-term nodes, filtered by inferred manager/table domain.
        """
        print("suggest_missing_fields...")
        missing_list = [str(k) for k in missing_fields or []]
        suggestions: Dict[str, List[Dict[str, Any]]] = {}

        for key in missing_list:
            table_name = self._infer_table_for_key(key, case_item)
            key_tokens = self._tokenize_key(key)

            candidates: List[Tuple[int, str, Dict[str, Any]]] = []
            for nid, attrs in self._iter_user_long_term_nodes(table_name=table_name):
                score = self._score_node_for_key(key_tokens, nid, attrs)
                if score <= 0:
                    continue
                candidates.append((score, nid, attrs))

            candidates.sort(key=lambda t: (-t[0], str(t[1])))

            limited: List[Dict[str, Any]] = []
            for score, nid, attrs in candidates[:10]:
                limited.append(
                    {
                        "node_id": nid,
                        "table_name": attrs.get("table_name"),
                        "row_id": attrs.get("row_id"),
                        "description": attrs.get("description") or "",
                        "score": score,
                    }
                )

            suggestions[key] = limited

        print("suggest_missing_fields... done")
        return suggestions

    def analyze_case_context(
        self,
        case_item: Dict[str, Any],
        resolved_fields: Optional[Dict[str, Any]],
        missing_fields: Iterable[str],
    ) -> Dict[str, Any]:
        """
        Build a structured summary of a case and its graph context.

        Includes required/resolved/missing keys plus per-domain graph stats and
        ThinkManager's missing-field suggestions.
        """
        print("analyze_case_context...")
        resolved_fields = resolved_fields or {}
        missing_list = [str(k) for k in missing_fields or []]

        resolved_keys = sorted(str(k) for k in resolved_fields.keys())
        required_keys = sorted(set(resolved_keys + missing_list))

        suggestions: Dict[str, List[Dict[str, Any]]] = {}
        if missing_list:
            suggestions = self.suggest_missing_fields(case_item, missing_list)

        graph_stats = {
            "params": self._count_long_term("params"),
            "fields": self._count_long_term("fields"),
            "methods": self._count_long_term("methods"),
            "files": self._count_long_term("files"),
            "all_long_term": sum(1 for _ in self._iter_user_long_term_nodes(None)),
        }

        result = {
            "case": str(case_item.get("case") or ""),
            "desc": str(case_item.get("desc") or ""),
            "required_keys": required_keys,
            "resolved_keys": resolved_keys,
            "missing_keys": missing_list,
            "graph_stats": graph_stats,
            "suggestions": suggestions,
        }
        print("analyze_case_context... done")
        return result


if __name__ == "__main__":
    # Minimal workflow: ThinkManager._iter_user_nodes on empty graph
    G = nx.MultiGraph()
    tm = ThinkManager(G=G, qb=None, user_id="public")
    out = list(tm._iter_user_nodes())
    assert out == []
    print("[think_manager] ok")
