import os
from typing import Any, Dict, Optional

from qbrain.core.orchestrator_manager.orchestrator import Thalamus


class BrainRuntime:
    """
    Canonical runtime entrypoint for request handling.

    This facade preserves current behavior by delegating to Thalamus while
    exposing a stable root-level API for future internal consolidation.
    """

    VALID_RUNTIME_MODES = {"legacy", "unified"}

    def __init__(
        self,
        qdash_con,
        cases,
        user_id: str = "public",
        relay=None,
        *,
        collect_cases_into_graph: bool = True,
        build_component_graph: bool = True,
        parse_equations: bool = False,
        runtime_mode: Optional[str] = None,
    ) -> None:
        self.runtime_mode = self._resolve_runtime_mode(runtime_mode)
        self._orchestrator = Thalamus(
            qdash_con=qdash_con,
            cases=cases,
            user_id=user_id,
            relay=relay,
            collect_cases_into_graph=collect_cases_into_graph,
            build_component_graph=build_component_graph,
            parse_equations=parse_equations,
        )
        self.user_id = user_id

    def _resolve_runtime_mode(self, raw_mode: Optional[str]) -> str:
        mode = (raw_mode or os.getenv("QBRAIN_RUNTIME_MODE", "legacy")).strip().lower()
        if mode not in self.VALID_RUNTIME_MODES:
            return "legacy"
        return mode

    async def handle_relay_payload(
        self,
        payload: Dict[str, Any],
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ):
        """
        Unified runtime request entry preserving current websocket contracts.
        Normalizes payload (dict), user_id, and session_id, then delegates to Thalamus.
        """
        normalized_payload = payload if isinstance(payload, dict) else {}
        resolved_user_id = (str(user_id).strip() if user_id is not None else "") or self.user_id
        resolved_session_id = (str(session_id).strip() if session_id is not None else "") or None
        return await self._orchestrator.handle_relay_payload(
            payload=normalized_payload,
            user_id=resolved_user_id,
            session_id=resolved_session_id,
        )

    async def dispatch_relay_handler(self, data_type: str, payload: Dict[str, Any]):
        return await self._orchestrator._dispatch_relay_handler(data_type, payload)

    def ensure_data_type_from_classifier(
        self, payload: Dict[str, Any], msg: str, user_id: Optional[str]
    ) -> str:
        return self._orchestrator._ensure_data_type_from_classifier(payload, msg, user_id)

    def extract_goal_values_from_text(
        self,
        case_name: str,
        message: str,
        req_struct: Dict[str, Any],
        current_payload: Dict[str, Any],
        conversation_history: Optional[list[Dict[str, str]]] = None,
    ):
        return self._orchestrator._extract_goal_values_from_text(
            case_name=case_name,
            message=message,
            req_struct=req_struct,
            current_payload=current_payload,
            conversation_history=conversation_history,
        )

    def collect_missing_values(self, goal_struct: Dict[str, Any], prefix: str = "") -> list[str]:
        return self._orchestrator._collect_missing_values(goal_struct, prefix=prefix)

    def return_follow_up_chat(self, follow_up_msg: str, session_key: str) -> Dict[str, Any]:
        return self._orchestrator._return_follow_up_chat(follow_up_msg, session_key)

    @property
    def orchestrator(self) -> Thalamus:
        return self._orchestrator

    def __getattr__(self, item):
        # Keep compatibility with existing callers expecting Thalamus attributes.
        return getattr(self._orchestrator, item)
