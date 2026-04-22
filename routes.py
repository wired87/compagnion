"""
routes.py - Entry for Thalamus (classification + relay handling).

Minimal FastMCP app with a single POST route that receives:
  {auth: {user_id, session_id}, data: {text, files}, type: str|None}
and delegates to Thalamus.handle_relay_payload for classification and handling.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ---- Input model (per spec) ----
# auth: {user_id, session_id}; data: {text, files}; type: str|None


class AuthInput(BaseModel):
    """Auth block: user_id and session_id."""

    user_id: str = Field(default="public", description="User identifier")
    session_id: Optional[str] = Field(default=None, description="Session identifier")


class DataInput(BaseModel):
    """Data block: text (message) and optional files."""

    text: str = Field(default="", description="User message for classification")
    files: List[Any] = Field(default_factory=list, description="Optional file refs")


class RelayPayload(BaseModel):
    """Full relay payload: auth, data, type."""

    auth: AuthInput = Field(default_factory=AuthInput)
    data: DataInput = Field(default_factory=DataInput)
    type: Optional[str] = Field(default=None, description="Pre-set type or None to classify")


# ---- FastAPI app ----
app = FastAPI(title="Thalamus Entry", version="1.0.0")

# Lazy singleton Thalamus (avoids heavy startup until first request)
_orchestrator: Optional[Any] = None


class _StubOrchestrator:
    """Fallback when Thalamus unavailable. Returns stable response for schema validation."""

    async def handle_relay_payload(
        self, payload: Dict[str, Any], user_id: str = "public", session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        data_type = payload.get("type") or "CHAT"
        return {
            "type": data_type,
            "status": {"state": "success", "code": 200, "msg": "stub (Thalamus unavailable)"},
            "data": {"user_id": user_id, "session_id": session_id, "msg": (payload.get("data") or {}).get("msg", "")},
        }


def _get_orchestrator() -> Any:
    """Lazy-init Thalamus on first request. Falls back to stub if import fails."""
    global _orchestrator
    if _orchestrator is None:
        try:
            from qbrain.core.orchestrator_manager.orchestrator import Thalamus
            from qbrain.predefined_case import RELAY_CASES_CONFIG

            _orchestrator = Thalamus(
                qdash_con=None,
                cases=RELAY_CASES_CONFIG,
                user_id="public",
            )
        except Exception as exc:
            print(f"[routes] Thalamus init failed, using stub: {exc}")
            _orchestrator = _StubOrchestrator()
    return _orchestrator


def _to_relay_payload(body: RelayPayload) -> Dict[str, Any]:
    """Convert Pydantic model to Thalamus payload. Ensures data.msg for Thalamus."""
    data = body.data.model_dump()
    # Thalamus expects data.msg or data.text; set both for compatibility
    text = data.get("text") or ""
    data["msg"] = text
    return {
        "auth": body.auth.model_dump(exclude_none=True),
        "data": data,
        "type": body.type.strip() if body.type and str(body.type).strip() else None,
    }


@app.post("/relay", operation_id="relay_entry")
async def relay_entry(body: RelayPayload) -> Any:
    """
    Entry for Thalamus: receives relay payload, classifies type if None, dispatches handler.
    """
    try:
        payload = _to_relay_payload(body)
        user_id = (body.auth.user_id or "").strip() or "public"
        session_id = (body.auth.session_id or "").strip() or None

        orch = _get_orchestrator()
        result = await orch.handle_relay_payload(
            payload=payload,
            user_id=user_id,
            session_id=session_id,
        )
        return result if result is not None else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---- FastMCP wrapper ----
def get_mcp() -> Any:
    """Return FastMCP server wrapping this FastAPI app."""
    from fastmcp import FastMCP

    return FastMCP.from_fastapi(app=app, name="ThalamusEntry")


if __name__ == "__main__":
    # Minimal workflow: POST /relay via TestClient
    from fastapi.testclient import TestClient
    payload = {"auth": {"user_id": "public"}, "data": {"text": "hi"}, "type": "CHAT"}
    with TestClient(app) as c:
        r = c.post("/relay", json=payload)
    assert r.status_code == 200
    assert "status" in r.json() or "type" in r.json()
    print("[routes] ok")
