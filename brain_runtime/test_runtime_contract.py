import asyncio

from qbrain.brain_runtime.prompt_policy import (
    build_extraction_policy_block,
    resolve_prompt_policy_mode,
)
from qbrain.brain_runtime.runtime import BrainRuntime


class _DummyThalamus:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def handle_relay_payload(self, payload, user_id=None, session_id=None):
        return {
            "type": payload.get("type", "CHAT"),
            "status": {"state": "success", "code": 200, "msg": ""},
            "data": {"user_id": user_id, "session_id": session_id},
        }


def test_runtime_mode_resolution_defaults_to_legacy(monkeypatch):
    monkeypatch.setattr("qbrain.brain_runtime.runtime.Thalamus", _DummyThalamus)
    runtime = BrainRuntime(qdash_con=None, cases=[], runtime_mode="unknown")
    assert runtime.runtime_mode == "legacy"


def test_legacy_and_unified_delegate_equally(monkeypatch):
    monkeypatch.setattr("qbrain.brain_runtime.runtime.Thalamus", _DummyThalamus)
    payload = {"type": "CHAT", "data": {"msg": "hello"}}

    legacy = BrainRuntime(qdash_con=None, cases=[], runtime_mode="legacy")
    unified = BrainRuntime(qdash_con=None, cases=[], runtime_mode="unified")

    legacy_res = asyncio.run(legacy.handle_relay_payload(payload, user_id="u1", session_id="s1"))
    unified_res = asyncio.run(unified.handle_relay_payload(payload, user_id="u1", session_id="s1"))

    assert legacy_res == unified_res
    assert legacy_res["status"]["state"] == "success"


def test_prompt_policy_mode_resolution(monkeypatch):
    monkeypatch.setenv("QBRAIN_PROMPT_POLICY", "strict")
    assert resolve_prompt_policy_mode() == "strict"
    monkeypatch.setenv("QBRAIN_PROMPT_POLICY", "invalid")
    assert resolve_prompt_policy_mode() == "legacy"


def test_strict_policy_block_contains_shape_rules():
    block = build_extraction_policy_block("SET_FIELD", "strict")
    assert "Preserve scalar/list/object type shape" in block
    assert "Do NOT include any keys not present in req_struct." in block
