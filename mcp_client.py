"""
Shared MCP HTTP client: JSON-RPC POST and GET helpers for tools/list and tools/call.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib import request as urllib_request


def post_json_rpc(
    url: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    timeout_sec: float = 8.0,
) -> Optional[Dict[str, Any]]:
    """
    POST JSON-RPC request to MCP endpoint. Returns full response dict or None on failure.
    """
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or {},
    }
    try:
        raw = json.dumps(body).encode("utf-8")
        req = urllib_request.Request(
            url=url,
            data=raw,
            method="POST",
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        with urllib_request.urlopen(req, timeout=timeout_sec) as resp:
            payload = resp.read().decode("utf-8", errors="ignore")
        return json.loads(payload) if payload else None
    except Exception:
        return None


def get_json(url: str, *, timeout_sec: float = 8.0) -> Optional[Dict[str, Any]]:
    """GET JSON from URL. Returns parsed dict or None on failure."""
    try:
        req = urllib_request.Request(url=url, method="GET", headers={"Accept": "application/json"})
        with urllib_request.urlopen(req, timeout=timeout_sec) as resp:
            payload = resp.read().decode("utf-8", errors="ignore")
        return json.loads(payload) if payload else None
    except Exception:
        return None


if __name__ == "__main__":
    # Minimal workflow: get_json/post_json_rpc return None on unreachable URL
    r = get_json("http://127.0.0.1:61999/nonexistent", timeout_sec=0.5)
    assert r is None
    r2 = post_json_rpc("http://127.0.0.1:61999/mcp", "tools/list", timeout_sec=0.5)
    assert r2 is None
    print("[mcp_client] ok")
