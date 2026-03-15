"""
ReceiveUserData: scans ACTION nodes (from McpPickup), calls user-entries routes via MCP tools/call,
and consumes returned data as COMPONENT nodes with sub_type = table_id.
"""
from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from brain_schema import BrainNodeType
from brain_utils import normalize_user_id
from mcp_client import post_json_rpc

if TYPE_CHECKING:
    from graph.local_graph_utils import GUtils

_USER_ENTRIES_PATTERNS = ("user-entries", "user_entries")


def _is_user_entries_route(attrs: dict) -> bool:
    """True if action_name, description, or source_endpoint contains user-entries path."""
    text = " ".join(
        str(v or "") for k, v in attrs.items() if k in ("action_name", "description", "source_endpoint")
    ).lower()
    return any(p in text for p in _USER_ENTRIES_PATTERNS)


class ReceiveUserData:
    """
    Scans ACTION nodes for user-entries routes, invokes via MCP tools/call with user_id,
    and adds response data as COMPONENT nodes with sub_type = table_id.
    """

    def __init__(
        self,
        gutils: "GUtils",
        user_id: str = "public",
        graph_lock: Optional[Any] = None,
        http_timeout_sec: float = 8.0,
    ):
        self.gutils = gutils
        self.user_id = normalize_user_id(user_id)
        self._graph_lock = graph_lock
        self._http_timeout_sec = http_timeout_sec

    def _collect_user_entries_action_nodes(self) -> List[Tuple[str, Dict[str, Any]]]:
        """Return [(node_id, attrs), ...] for ACTION nodes matching user-entries path."""
        out: List[Tuple[str, Dict[str, Any]]] = []
        for node_id, attrs in list(self.gutils.G.nodes(data=True)):
            ntype = str(attrs.get("type") or "").upper()
            if ntype != BrainNodeType.ACTION:
                continue
            if _is_user_entries_route(attrs):
                out.append((str(node_id), dict(attrs)))
        return out

    def _call_mcp_tool(
        self, endpoint: str, tool_name: str, arguments: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """POST tools/call to endpoint. Returns full response dict or None."""
        return post_json_rpc(
            endpoint,
            "tools/call",
            {"name": tool_name, "arguments": arguments},
            timeout_sec=self._http_timeout_sec,
        )

    def _parse_response(self, payload: Optional[Dict[str, Any]]) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        Extract table_id and rows from MCP tools/call response.
        Handles result.table_id/rows or content[].text as JSON.
        """
        if not payload or not isinstance(payload, dict):
            return None, []
        result = payload.get("result")
        if isinstance(result, dict):
            table_id = result.get("table_id")
            rows = result.get("rows")
            if isinstance(rows, list):
                return str(table_id) if table_id else None, rows
            if table_id:
                return str(table_id), []
        content = payload.get("content")
        if isinstance(content, list) and content:
            first = content[0]
            if isinstance(first, dict) and first.get("type") == "text":
                text = first.get("text")
                if text:
                    try:
                        parsed = json.loads(text)
                        if isinstance(parsed, dict):
                            return (
                                str(parsed.get("table_id", "") or ""),
                                parsed.get("rows") if isinstance(parsed.get("rows"), list) else [],
                            )
                    except json.JSONDecodeError:
                        pass
        return None, []

    def _make_component_nodes(
        self, table_id: str, rows: List[Dict[str, Any]], source_action_id: str
    ) -> List[Dict[str, Any]]:
        """Build COMPONENT node dicts for gutils.add_node."""
        nodes: List[Dict[str, Any]] = []
        for i, row in enumerate(rows):
            row_id = row.get("id") if isinstance(row, dict) else None
            if not row_id:
                row_id = f"row_{i}"
            node_id = f"COMPONENT::{table_id}::{row_id}"
            node = {
                "id": node_id,
                "type": BrainNodeType.COMPONENT,
                "sub_type": table_id,
                "user_id": self.user_id,
                "source_action_id": source_action_id,
                "row_id": str(row_id),
                "updated_at": int(time.time() * 1000),
            }
            if isinstance(row, dict):
                node["data"] = row
            nodes.append(node)
        if not rows:
            node_id = f"COMPONENT::{table_id}::empty"
            nodes.append({
                "id": node_id,
                "type": BrainNodeType.COMPONENT,
                "sub_type": table_id,
                "user_id": self.user_id,
                "source_action_id": source_action_id,
                "updated_at": int(time.time() * 1000),
            })
        return nodes

    def receive(self) -> int:
        """
        Scan user-entries ACTION nodes, call each via MCP tools/call with user_id,
        add COMPONENT nodes to gutils. Returns count of nodes added.
        """
        actions = self._collect_user_entries_action_nodes()
        if not actions:
            return 0
        all_nodes: List[Dict[str, Any]] = []
        for node_id, attrs in actions:
            endpoint = str(attrs.get("source_endpoint") or "").strip()
            action_name = str(attrs.get("action_name") or "").strip()
            if not endpoint or not action_name:
                continue
            payload = self._call_mcp_tool(endpoint, action_name, {"user_id": self.user_id})
            table_id, rows = self._parse_response(payload)
            if not table_id:
                table_id = action_name
            all_nodes.extend(self._make_component_nodes(table_id, rows, node_id))
        lock = self._graph_lock
        if lock:
            with lock:
                for node in all_nodes:
                    self.gutils.add_node(node)
        else:
            for node in all_nodes:
                self.gutils.add_node(node)
        return len(all_nodes)


if __name__ == "__main__":
    # Minimal workflow: ReceiveUserData.receive() with no user-entries -> 0
    import networkx as nx

    class _StubGUtils:
        def __init__(self):
            self.G = nx.MultiGraph()

        def add_node(self, attrs):
            nid = attrs.get("id")
            if nid:
                self.G.add_node(nid, **{k: v for k, v in attrs.items() if k != "id"})

    g = _StubGUtils()
    r = ReceiveUserData(gutils=g)
    n = r.receive()
    assert n == 0
    print("[receive_user_data] ok")
