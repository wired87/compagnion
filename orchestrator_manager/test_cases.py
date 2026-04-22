"""
Test case struct for `py -m core.orchestrator_manager.orchestrator`.
Each item: name, payload (input to handle_relay_payload), user_id, session_id, and optional expected_type / description.
Covers classification, follow-ups, all action kinds, and tricky requests.
"""
from typing import Dict, Any, List, Optional

# Default auth context for test run
DEFAULT_USER_ID = "test_user"
DEFAULT_SESSION_ID = "1"


def make_payload(
    msg: Optional[str] = None,
    data_type: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
    auth: Optional[Dict[str, Any]] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """Build payload with data.msg and type; merge data and auth."""
    payload = {"type": data_type}
    if data is None:
        data = {}
    if msg is not None:
        data = {**data, "msg": msg}
    if data:
        payload["data"] = data
    if auth is not None:
        payload["auth"] = auth
    payload.update(extra)
    return payload


# Struct: list of test cases for the orchestrator __main__
ORCHESTRATOR_TEST_CASES: List[Dict[str, Any]] = [
    # ---- Classification (no type): should classify and then follow-up or dispatch ----
    {
        "name": "classify_set_field_minimal",
        "description": "No type; message triggers SET_FIELD classification; expect follow-up for missing fields",
        "payload": make_payload(msg="please create a field", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "classify_set_param",
        "description": "No type; message asks to set param",
        "payload": make_payload(msg="I want to add a parameter alpha with value 0.5", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "classify_list_envs",
        "description": "No type; list envs",
        "payload": make_payload(msg="show me all my environments", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "classify_list_params",
        "description": "No type; list params",
        "payload": make_payload(msg="list my parameters", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "classify_chat_vague",
        "description": "No type; vague message may stay CHAT or classify",
        "payload": make_payload(msg="hello what can you do?", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    # ---- Follow-up flow (same session): first request then follow-up ----
    {
        "name": "followup_field_name_and_id",
        "description": "Follow-up: provide name and id (simulates second message after 'create a field')",
        "payload": make_payload(msg="name and id are hello", data_type="SET_FIELD"),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "followup_field_params",
        "description": "Follow-up: include params in field",
        "payload": make_payload(
            msg="include params dmuG, h and psi into the field. Provide them all a matrix point",
            data_type="SET_FIELD",
        ),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    # ---- Explicit type with minimal data (should trigger follow-up or handler) ----
    {
        "name": "explicit_set_field_empty_data",
        "description": "Explicit SET_FIELD with no data.field; expect follow-up",
        "payload": make_payload(msg="", data_type="SET_FIELD", data={"field": None}),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "explicit_list_users_params",
        "description": "Explicit LIST_USERS_PARAMS; only needs auth.user_id",
        "payload": make_payload(data_type="LIST_USERS_PARAMS", data={}, auth={"user_id": DEFAULT_USER_ID}),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "explicit_get_users_envs",
        "description": "Explicit GET_USERS_ENVS",
        "payload": make_payload(data_type="GET_USERS_ENVS", data={}, auth={"user_id": DEFAULT_USER_ID}),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "explicit_chat",
        "description": "Explicit CHAT with message",
        "payload": make_payload(msg="Just a chat message", data_type="CHAT"),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    # ---- START_SIM (may fail without full config but tests path) ----
    {
        "name": "start_sim_minimal",
        "description": "START_SIM with minimal config",
        "payload": {
            "type": "START_SIM",
            "data": {"config": {}},
            "auth": {"user_id": DEFAULT_USER_ID},
        },
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    # ---- Edge / tricky ----
    {
        "name": "empty_msg_no_type",
        "description": "No type and empty msg; expect early return None or CHAT",
        "payload": make_payload(msg="", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "set_param_natural",
        "description": "Natural language set param",
        "payload": make_payload(
            msg="Add a parameter called beta, type float, value 1.0",
            data_type=None,
        ),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "delete_param",
        "description": "Delete param intent",
        "payload": make_payload(msg="delete the parameter alpha", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "set_module_intent",
        "description": "Set module intent",
        "payload": make_payload(msg="create a new module for my simulation", data_type=None),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    {
        "name": "list_modules_fields",
        "description": "List modules fields (needs module_id)",
        "payload": make_payload(
            msg="show fields of module x",
            data_type=None,
        ),
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
    # ---- Payload with files (struct only; actual files may be empty for unit run) ----
    {
        "name": "payload_with_files_key",
        "description": "Payload has data.files key (empty list); tests file path without real upload",
        "payload": {
            "type": None,
            "data": {"msg": "upload these documents", "files": []},
            "auth": {"user_id": DEFAULT_USER_ID, "session_id": DEFAULT_SESSION_ID},
        },
        "user_id": DEFAULT_USER_ID,
        "session_id": DEFAULT_SESSION_ID,
    },
]


def run_test_cases(
    handle_relay_payload,
    *,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    cases: Optional[List[Dict[str, Any]]] = None,
    verbose: bool = True,
) -> List[Dict[str, Any]]:
    """
    Run ORCHESTRATOR_TEST_CASES (or given cases) through handle_relay_payload.
    handle_relay_payload must be async: await handle_relay_payload(payload, user_id=..., session_id=...).
    Returns list of {name, payload, response, error?}.
    """
    import asyncio

    cases = cases or ORCHESTRATOR_TEST_CASES
    uid = user_id or DEFAULT_USER_ID
    sid = session_id or DEFAULT_SESSION_ID
    results = []

    async def run_one(tc: Dict[str, Any]) -> Dict[str, Any]:
        name = tc.get("name", "unnamed")
        payload = tc.get("payload", {})
        u = tc.get("user_id", uid)
        s = tc.get("session_id", sid)
        try:
            out = await handle_relay_payload(payload=payload, user_id=u, session_id=s)
            return {"name": name, "payload": payload, "response": out, "error": None}
        except Exception as e:
            return {"name": name, "payload": payload, "response": None, "error": str(e)}

    async def run_all():
        for tc in cases:
            r = await run_one(tc)
            results.append(r)
            if verbose:
                desc = tc.get("description", "")
                err = r.get("error")
                resp = r.get("response")
                print(f"[{r['name']}] {desc}")
                if err:
                    print(f"  ERROR: {err}")
                else:
                    typ = resp.get("type") if isinstance(resp, dict) else type(resp).__name__
                    print(f"  response type: {typ}")

    asyncio.run(run_all())
    return results
