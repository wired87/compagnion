from __future__ import annotations

import inspect
import json
import time
import traceback
import uuid
from typing import Any, Dict, List


def _flatten_required_keys(req_struct: Dict[str, Any]) -> List[str]:
    required: List[str] = []
    data = req_struct.get("data") if isinstance(req_struct, dict) else None
    base = data if isinstance(data, dict) else req_struct

    if not isinstance(base, dict):
        return required

    def _walk(prefix: str, obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                next_prefix = f"{prefix}.{k}" if prefix else k
                if isinstance(v, dict):
                    _walk(next_prefix, v)
                else:
                    required.append(next_prefix)
        else:
            if prefix:
                required.append(prefix)

    _walk("", base)
    return required


class BrainExecutor:
    async def execute_or_request_more(
        self,
        case_item: Dict[str, Any],
        resolved_fields: Dict[str, Any],
        missing_fields: List[str],
    ) -> Dict[str, Any]:
        print("execute_or_request_more...")
        trace_id = f"brain_exec_{uuid.uuid4().hex[:10]}"
        started_ms = int(time.time() * 1000)
        case_name = str(case_item.get("case") or "")
        fn = case_item.get("func")
        req_struct = case_item.get("req_struct") or {}
        required_keys = _flatten_required_keys(req_struct)

        print(
            f"[BrainExecutor] trace_id={trace_id} case={case_name} "
            f"required_keys={len(required_keys)} resolved_keys={len(resolved_fields)} "
            f"missing_keys={len(missing_fields)}"
        )

        if missing_fields:
            result = {
                "status": "need_data",
                "goal_case": case_name,
                "resolved_fields": resolved_fields,
                "missing_fields": missing_fields,
                "next_message": (
                    "I need more data to continue. Missing keys: "
                    + ", ".join(missing_fields)
                ),
                "required_structure": req_struct,
                "execution_debug": {
                    "trace_id": trace_id,
                    "started_ms": started_ms,
                    "finished_ms": int(time.time() * 1000),
                    "duration_ms": int(time.time() * 1000) - started_ms,
                    "required_keys": required_keys,
                    "resolved_keys": sorted(list(resolved_fields.keys())),
                    "missing_keys": missing_fields,
                    "callable_ready": False,
                },
            }
            print("execute_or_request_more... done")
            return result

        if not callable(fn):
            result = {
                "status": "error",
                "goal_case": case_name,
                "resolved_fields": resolved_fields,
                "missing_fields": [],
                "next_message": "No callable is configured for the selected case.",
                "required_structure": req_struct,
                "execution_debug": {
                    "trace_id": trace_id,
                    "started_ms": started_ms,
                    "finished_ms": int(time.time() * 1000),
                    "duration_ms": int(time.time() * 1000) - started_ms,
                    "required_keys": required_keys,
                    "resolved_keys": sorted(list(resolved_fields.keys())),
                    "missing_keys": [],
                    "callable_ready": False,
                },
            }
            print("execute_or_request_more... done")
            return result

        payload = {"type": case_name, "data": resolved_fields}

        # Stability guard: ensure payload is JSON-serializable before function call.
        try:
            _ = json.dumps(payload, default=str)
        except Exception as exc:
            result = {
                "status": "error",
                "goal_case": case_name,
                "resolved_fields": resolved_fields,
                "missing_fields": [],
                "next_message": f"Payload serialization check failed: {exc}",
                "required_structure": req_struct,
                "execution_debug": {
                    "trace_id": trace_id,
                    "started_ms": started_ms,
                    "finished_ms": int(time.time() * 1000),
                    "duration_ms": int(time.time() * 1000) - started_ms,
                    "required_keys": required_keys,
                    "resolved_keys": sorted(list(resolved_fields.keys())),
                    "missing_keys": [],
                    "callable_ready": True,
                    "stage": "pre_call_payload_validation",
                },
            }
            print("execute_or_request_more... done")
            return result

        try:
            print(f"[BrainExecutor] trace_id={trace_id} invoking callable...")
            if inspect.iscoroutinefunction(fn):
                out = await fn(payload)
            else:
                out = fn(payload)
                if inspect.isawaitable(out):
                    out = await out

            result = {
                "status": "executed",
                "goal_case": case_name,
                "resolved_fields": resolved_fields,
                "missing_fields": [],
                "next_message": "Execution completed.",
                "result": out,
                "required_structure": req_struct,
                "execution_debug": {
                    "trace_id": trace_id,
                    "started_ms": started_ms,
                    "finished_ms": int(time.time() * 1000),
                    "duration_ms": int(time.time() * 1000) - started_ms,
                    "required_keys": required_keys,
                    "resolved_keys": sorted(list(resolved_fields.keys())),
                    "missing_keys": [],
                    "callable_ready": True,
                    "stage": "post_call_success",
                },
            }
            print("execute_or_request_more... done")
            return result
        except Exception as exc:
            result = {
                "status": "error",
                "goal_case": case_name,
                "resolved_fields": resolved_fields,
                "missing_fields": [],
                "next_message": f"Callable execution failed: {exc}",
                "required_structure": req_struct,
                "execution_debug": {
                    "trace_id": trace_id,
                    "started_ms": started_ms,
                    "finished_ms": int(time.time() * 1000),
                    "duration_ms": int(time.time() * 1000) - started_ms,
                    "required_keys": required_keys,
                    "resolved_keys": sorted(list(resolved_fields.keys())),
                    "missing_keys": [],
                    "callable_ready": True,
                    "stage": "post_call_error",
                    "error_traceback": traceback.format_exc(),
                },
            }
            print("execute_or_request_more... done")
            return result


if __name__ == "__main__":
    # Minimal workflow: execute_or_request_more with missing_fields
    import asyncio
    ex = BrainExecutor()
    case = {"case": "CHAT", "req_struct": {"data": {"text": ""}}}
    out = asyncio.run(ex.execute_or_request_more(case, {}, ["data.text"]))
    assert out["status"] == "need_data"
    assert "data.text" in out["missing_fields"]
    print("[brain_executor] ok")
