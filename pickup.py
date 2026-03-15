"""
MCP Pickup: collects tools from all /mcp endpoints and consumes them into a GUtils instance.
All received routes (tools) are upserted as ACTION nodes with full metadata.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from brain_schema import BrainNodeType
from brain_utils import normalize_user_id
from mcp_client import get_json, post_json_rpc

if TYPE_CHECKING:
    from graph.local_graph_utils import GUtils


def _normalize_endpoint_url(endpoint: str) -> str:
    return str(endpoint or "").strip().rstrip("/")


def _split_endpoint_value(raw: str) -> List[str]:
    """Parse env value: JSON list or comma/semicolon/newline delimited."""
    text = (raw or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
    return [part.strip() for part in re.split(r"[,\n;]+", text) if part and part.strip()]


class McpPickup:
    """
    Fetches MCP tools from all configured /mcp endpoints and consumes them
    into the given GUtils instance as ACTION nodes with full metadata.
    """

    def __init__(
        self,
        gutils: GUtils,
        user_id: str = "public",
        graph_lock: Optional[threading.Lock] = None,
        poll_interval_sec: float = 20.0,
        http_timeout_sec: float = 8.0,
        log_throttle_sec: float = 30.0,
    ):
        self.gutils = gutils
        self.user_id = normalize_user_id(user_id)
        self._graph_lock = graph_lock or threading.Lock()
        self._poll_interval_sec = poll_interval_sec
        self._http_timeout_sec = http_timeout_sec
        self._log_throttle_sec = log_throttle_sec
        self._log_last_ts: Dict[str, float] = {}
        print("[pickup] Initializing MCP tool collector for user scope.")

    def _log_warning(self, key: str, message: str) -> None:
        now = time.time()
        last = self._log_last_ts.get(key, 0.0)
        if now - last < self._log_throttle_sec:
            return
        self._log_last_ts[key] = now
        print(f"[pickup] ⚠ {message}")

    def collect_endpoint_urls(self) -> List[str]:
        """Collect all /mcp endpoint URLs from env (BRAIN_MCP_ENDPOINTS, MCP_EP*, etc)."""
        print("[pickup] Scanning environment for MCP endpoint URLs...")
        out: List[str] = []
        explicit_keys = ("BRAIN_MCP_ENDPOINTS", "MCP_EP")
        for key in explicit_keys:
            raw = os.environ.get(key)
            if raw is None:
                continue
            out.extend(_split_endpoint_value(str(raw)))
        for key, raw in os.environ.items():
            if not str(key).upper().startswith("MCP_EP_"):
                continue
            out.extend(_split_endpoint_value(str(raw or "")))
        if not out:
            for key, raw in os.environ.items():
                if "MCP_EP" not in str(key).upper():
                    continue
                out.extend(_split_endpoint_value(str(raw or "")))
        seen: Set[str] = set()
        unique: List[str] = []
        for ep in out:
            ep = _normalize_endpoint_url(ep)
            if ep in seen:
                continue
            low = ep.lower()
            if not (low.startswith("http://") or low.startswith("https://")):
                continue
            seen.add(ep)
            unique.append(ep)
        print(f"[pickup] Found {len(unique)} unique endpoint(s) to query.")
        return unique

    def _extract_tools_from_payload(self, payload: Any) -> List[Dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        candidates: List[Any] = []
        result = payload.get("result")
        if isinstance(result, dict):
            candidates.append(result.get("tools"))
        candidates.append(payload.get("tools"))
        data = payload.get("data")
        if isinstance(data, dict):
            candidates.append(data.get("tools"))
        if "name" in payload and ("inputSchema" in payload or "description" in payload):
            candidates.append([payload])
        for candidate in candidates:
            if isinstance(candidate, list):
                tools = [item for item in candidate if isinstance(item, dict)]
                if tools:
                    return tools
        return []

    def fetch_tools(self, endpoint_url: str) -> Tuple[List[Dict[str, Any]], bool]:
        """Fetch tools from one endpoint. Returns (tools, had_response)."""
        print(f"[pickup] Fetching tools from endpoint...")
        had_response = False
        payload = get_json(endpoint_url, timeout_sec=self._http_timeout_sec)
        if payload is None:
            self._log_warning(f"http_get::{endpoint_url}", "GET request failed.")
        else:
            had_response = True
            tools = self._extract_tools_from_payload(payload)
            if tools:
                print(f"[pickup] Retrieved {len(tools)} tool(s) via GET.")
                return tools, True
        print("[pickup] GET returned no tools, trying JSON-RPC tools/list...")
        rpc_payload = post_json_rpc(
            endpoint_url, "tools/list", {}, timeout_sec=self._http_timeout_sec
        )
        if rpc_payload is None:
            self._log_warning(f"http_post::{endpoint_url}", "POST tools/list failed.")
        else:
            had_response = True
            tools = self._extract_tools_from_payload(rpc_payload)
            if tools:
                print(f"[pickup] Retrieved {len(tools)} tool(s) via POST.")
                return tools, True
        if had_response:
            self._log_warning(f"parse::{endpoint_url}", "No MCP tools found in response.")
        print("[pickup] Endpoint yielded 0 tools.")
        return [], had_response

    def _make_action_node(self, endpoint_url: str, tool: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build ACTION node dict with all necessary info for GUtils.add_node."""
        action_name = str(tool.get("name") or tool.get("tool") or "").strip()
        if not action_name:
            return None
        source = f"{endpoint_url}::{action_name}"
        action_id = f"ACTION::{hashlib.sha1(source.encode('utf-8')).hexdigest()[:20]}"
        input_schema = (
            tool.get("inputSchema") or tool.get("input_schema") or tool.get("schema") or {}
        )
        if not isinstance(input_schema, (dict, list)):
            input_schema = {}
        return {
            "id": action_id,
            "type": BrainNodeType.ACTION,
            "user_id": self.user_id,
            "action_name": action_name,
            "title": str(tool.get("title") or action_name),
            "description": str(tool.get("description") or ""),
            "input_schema": input_schema,
            "source_endpoint": endpoint_url,
            "updated_at": int(time.time() * 1000),
        }

    def _collect_action_ids_for_endpoint(self, endpoint_url: str) -> Set[str]:
        """Ids of ACTION nodes from this endpoint (for stale cleanup)."""
        out: Set[str] = set()
        ep_norm = _normalize_endpoint_url(endpoint_url)
        for node_id, attrs in list(self.gutils.G.nodes(data=True)):
            ntype = str(attrs.get("type") or "").upper()
            src_ep = _normalize_endpoint_url(str(attrs.get("source_endpoint") or ""))
            if ntype == BrainNodeType.ACTION and src_ep == ep_norm:
                out.add(str(node_id))
        return out

    def _remove_stale_nodes(self, node_ids: Set[str]) -> int:
        removed = 0
        if node_ids:
            print(f"[pickup] Removing {len(node_ids)} stale ACTION node(s) from graph.")
        for nid in node_ids:
            if self.gutils.G.has_node(nid):
                self.gutils.G.remove_node(nid)
                removed += 1
        return removed

    def refresh(self) -> int:
        """
        Fetch tools from all endpoints, upsert ACTION nodes into GUtils, remove stale.
        Returns count of upserted nodes.
        """
        print("[pickup] Starting refresh cycle...")
        endpoints = self.collect_endpoint_urls()
        if not endpoints:
            print("[pickup] No endpoints configured; skipping.")
            return 0
        endpoint_nodes: Dict[str, List[Dict[str, Any]]] = {}
        successful: Set[str] = set()
        max_workers = min(4, max(1, len(endpoints)))
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mcp-pickup") as pool:
            fut_to_ep = {pool.submit(self.fetch_tools, ep): ep for ep in endpoints}
            for fut in as_completed(fut_to_ep):
                ep = fut_to_ep[fut]
                try:
                    tools, ok = fut.result()
                except Exception as exc:
                    self._log_warning(f"fetch::{ep}", f"Unexpected error: {type(exc).__name__}")
                    continue
                if ok:
                    successful.add(ep)
                nodes = []
                for tool in tools:
                    node = self._make_action_node(ep, tool)
                    if node:
                        nodes.append(node)
                endpoint_nodes[ep] = nodes
        if not successful:
            print("[pickup] No endpoints responded successfully.")
            return 0
        print(f"[pickup] Upserting ACTION nodes from {len(successful)} endpoint(s)...")
        upserted = 0
        removed = 0
        with self._graph_lock:
            for ep in successful:
                nodes = endpoint_nodes.get(ep, [])
                for node in nodes:
                    self.gutils.add_node(node)
                    upserted += 1
                expected_ids = {str(n.get("id")) for n in nodes if n.get("id")}
                existing = self._collect_action_ids_for_endpoint(ep)
                stale = existing - expected_ids
                if stale:
                    removed += self._remove_stale_nodes(stale)
        if removed:
            self._log_warning("stale_cleanup", f"Removed {removed} stale node(s).")
        print(f"[pickup] Refresh complete: {upserted} added, {removed} removed.")
        return upserted


if __name__ == "__main__":
    # Minimal workflow: McpPickup.refresh() with no endpoints -> 0
    import networkx as nx
    class _StubGUtils:
        def __init__(self):
            self.G = nx.MultiGraph()

        def add_node(self, attrs):
            nid = attrs.get("id")
            if nid:
                self.G.add_node(nid, **{k: v for k, v in attrs.items() if k != "id"})

    g = _StubGUtils()
    p = McpPickup(gutils=g, poll_interval_sec=999.0)
    n = p.refresh()
    assert n == 0
    print("[pickup] Test passed.")
