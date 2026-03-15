"""
main.py - Entry point for Brain/Thalamus workflows.

Simplified: dict-based request list, loop execution, metrics tracking.
Run with "serve" arg to start uvicorn; otherwise runs automated relay loop.
"""
from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, List

# ---- Hardcoded request list (matches routes.RelayPayload) ----
REQUEST_LIST: List[Dict[str, Any]] = [
    {"auth": {"user_id": "public", "session_id": None}, "data": {"text": "Hello", "files": []}, "type": "CHAT"},
    {"auth": {"user_id": "public", "session_id": None}, "data": {"text": "", "files": []}, "type": "GET_USERS_ENVS"},
    {"auth": {"user_id": "demo", "session_id": "s1"}, "data": {"text": "Run simulation", "files": []}, "type": "CHAT"},
]


def _run_relay(payload: Dict[str, Any]) -> tuple[Dict[str, Any], float]:
    """POST /relay via TestClient. Returns (response_dict, elapsed_sec)."""
    from fastapi.testclient import TestClient

    from routes import app

    client = TestClient(app)
    t0 = time.perf_counter()
    resp = client.post("/relay", json=payload)
    elapsed = time.perf_counter() - t0

    if resp.status_code != 200:
        body = resp.json() if "application/json" in (resp.headers.get("content-type") or "") else {"text": resp.text}
        return {"error": resp.status_code, "detail": body}, elapsed
    return resp.json(), elapsed


def run_loop() -> List[Dict[str, Any]]:
    """Execute all requests in REQUEST_LIST. Return metrics per request."""
    metrics: List[Dict[str, Any]] = []
    for i, payload in enumerate(REQUEST_LIST):
        result, elapsed = _run_relay(payload)
        m = {
            "idx": i,
            "type": payload.get("type", "?"),
            "elapsed_ms": round(elapsed * 1000, 2),
            "ok": "error" not in result,
            "status": result.get("status", {}).get("state", "unknown") if "error" not in result else "error",
        }
        metrics.append(m)
        print(f"[{i}] {m['type']} | {m['elapsed_ms']}ms | ok={m['ok']} | {m['status']}")
    return metrics


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        import uvicorn
        print("[main] Starting MCP app on 127.0.0.1:8000...")
        uvicorn.run("routes:app", host="127.0.0.1", port=8000, reload=False)
        return 0

    print("[main] Running relay loop...")
    metrics = run_loop()
    ok_count = sum(1 for m in metrics if m["ok"])
    total_ms = sum(m["elapsed_ms"] for m in metrics)
    print(f"\n--- metrics: {ok_count}/{len(metrics)} ok, total {total_ms:.0f}ms ---")
    return 0 if ok_count == len(metrics) else 1


if __name__ == "__main__":
    sys.exit(main())
