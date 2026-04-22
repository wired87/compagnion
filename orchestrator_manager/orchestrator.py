"""
bsp task:
entwickel und teste ein ravity modell.
zeig mir alle files an

"""
import ast

from brn import Brain
from brn import SimOrchestrator
from qbrain.utils.math.operator_handler import EqExtractor

"""
Orchestrator Manager

This module implements the Thalamus, responsible for high-level coordination
and optimization of simulation entries using advanced neural architectures.

HOPFIELD NETWORK ORCHESTRATION EXPLANATION:
-------------------------------------------
The Orchestrator employs a continuous-state Hopfield Network (or modern dense associative memory)
to manage and orchestrate over existing simulation entries. 

1. Conceptualization:
   Each simulation entry (comprising environment configurations, module states, field parameters, 
   and injection patterns) is treated as a "memory pattern" or "attractor state" within the 
   network's energy landscape.

2. Process:
   - The "State Space" is defined by the aggregate parameters of all active simulations.
   - The Orbit of the simulation execution is viewed as a trajectory through this high-dimensional energy landscape.
   - Using the Hopfield energy function E = -1/2 * x^T * W * x (where W is the weight matrix learned 
     from successful historic simulations), the Orchestrator directs the current simulation flow 
     towards local minima which represent stable, coherent, or "optimal" simulation states.

3. Utility:
   - **Error Correction**: If a user provides a partial or incoherent simulation setup (noisy input), 
     the network relaxes the state to the nearest stored valid configuration (pattern retrieval).
   - **Convergence**: It ensures that dynamic interactions between modules and fields settle into 
     stable equilibrium points rather than oscillating chaotically, effectively "orchestrating" 
     transient dynamics into meaningful steady states.
   - **Interpolation**: It can plausibly interpolate between two known stable simulation states 
     to generate novel but valid transitional configurations.

This approach transforms the role of the Orchestrator from a simple task scheduler to a 
dynamic stability controller, ensuring global coherence across distributed simulation components.
"""
import asyncio
import json
from typing import List, Dict, Optional, Any, TypedDict

from qbrain.gem_core.gem import Gem

from qbrain.core.fields_manager.fields_lib import FieldsManager
from qbrain.core.method_manager.method_lib import MethodManager

from qbrain.core.injection_manager import InjectionManager
from qbrain.core.module_manager.ws_modules_manager import ModuleWsManager
from qbrain.core.param_manager.params_lib import ParamsManager
from qbrain.core.user_manager import UserManager
from qbrain.core.file_manager.file_lib import FileManager
from qbrain.chat_manger.main import AIChatClassifier
from brn.thalamic_classifier import ThalamicEventClassifier
from qbrain.core.env_manager.env_lib import EnvManager
from qbrain.core.guard import Guard, ComponentGraphCreator
from qbrain.core.model_manager.model_lib import ModelManager

from qbrain.core.session_manager.session import SessionManager
from qbrain.core.researcher2.researcher2.core import ResearchAgent
from qbrain.core.collector_manager import CaseCollectorManager
from qbrain.predefined_case import RELAY_CASES_CONFIG
from qbrain.qf_utils.qf_utils import QFUtils
from qbrain.brain_runtime.prompt_policy import (
    build_extraction_policy_block,
    resolve_prompt_policy_mode,
)

from qbrain.core.managers_context import set_orchestrator, reset_orchestrator
from qbrain.core.workflows.create_env_from_components import validate_env_components
from qbrain.core.sim_analyzer.sim_result_analyzer import SimResultAnalyzer


class StartSimInput(TypedDict):
    """
    Structure for gathering information required to start a simulation.
    """
    simulation_name: str
    target_env_id: str
    duration_seconds: int
    time_step: float
    description: Optional[str]

class Thalamus:
    """
    Manages the orchestration of various processes and serves as the central manager
    for simulation workflows and coordination tasks.

    Serves as a primary integration point that connects multiple subsystems and allows
    efficient handling of user commands, simulation execution, and communication handling.
    """
    DATASET_ID = "QBRAIN"
    
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
    ):

        # Use the shared/global QBrainTableManager (DuckDB by default unless configured otherwise).
        from qbrain.core.qbrain_manager import get_qbrain_table_manager
        self._qb = get_qbrain_table_manager()

        # Core model interface used for classification and follow-up prompts.
        self.gem = Gem()

        self.env_manager = EnvManager(self._qb)
        self.session_manager = SessionManager(self._qb)
        self.injection_manager = InjectionManager(self._qb)
        self.file_manager = FileManager(self._qb)
        self.model_manager = ModelManager(qb=self._qb, gem=self.gem)
        self.module_db_manager = ModuleWsManager(self._qb)
        self.field_manager = FieldsManager(self._qb)
        self.params_manager = ParamsManager(self._qb)
        self.method_manager = MethodManager(self._qb, self.params_manager)
        self.user_manager = UserManager(self._qb)

        self.research_agent = ResearchAgent(
            self.file_manager,
            self.gem,
            deep_research_backend=qdash_con,
        )

        # init Brain empty
        self.g = Brain(
            qdash_con,
            user_id,
        )

        self.qfu = QFUtils(g=self.g)

        # Discover all manager relay case structs and mirror them into the Brain graph.
        # This is read‑only and does not affect handler wiring; it only adds CASE nodes
        # for introspection and tooling.
        if collect_cases_into_graph:
            try:
                self.case_collector = CaseCollectorManager(gutils=self.g)
                self.case_collector.collect_cases_into_graph()
            except Exception as e:
                print(f"[Thalamus] CaseCollectorManager error during init: {e}")

        # fill Brain with data
        self.data_handler = ComponentGraphCreator(
            qb=self._qb,
            qfu=self.qfu,
            g=self.g,
            env_manager=self.env_manager,
            module_db_manager=self.module_db_manager,
            field_manager=self.field_manager,
            method_manager=self.method_manager,
            injection_manager=self.injection_manager,
            params_manager=self.params_manager,
            user_id=user_id,
        )
        if build_component_graph:
            self.data_handler.main()

        # Optional: split equations into parts (p -> op -> p). Keep disabled by default
        # for fast interactive/chat test startup.
        if parse_equations:
            try:
                for _, v in self.g.G.nodes(data=True):
                    ntype = str(v.get("type") or "").upper()
                    if ntype not in {"METHOD", "METHODS"}:
                        continue
                    eq = v.get("equation")
                    if not eq:
                        continue
                    self.eq_processor = EqExtractor(self.g)
                    self.eq_processor.visit(ast.parse(str(eq), mode="eval"))
                    _ = getattr(self.eq_processor, "batches", None)
            except Exception as e:
                print(f"[Thalamus] equation parse warning: {e}")

        self.guard = Guard(
            qfu=self.qfu,
            g=self.g,
            user_id=user_id,
            field_manager=self.field_manager,
            method_manager=self.method_manager,
            injection_manager=self.injection_manager,
            env_manager=self.env_manager,
            module_db_manager=self.module_db_manager,
            params_manager=self.params_manager,
        )

        self.sim_orchestrator = SimOrchestrator(
            brain=self.g,
            guard=self.guard,
            env_manager=self.env_manager,
            sim_analyzer=SimResultAnalyzer(env_manager=self.env_manager),
            injection_manager=self.injection_manager,
            params_manager=self.params_manager,
            qb=self._qb,
            relay=None,
            session_manager=self.session_manager,
        )

        ai_chat = AIChatClassifier(cases, gem=self.gem)
        self.chat_classifier = ThalamicEventClassifier(
            relay_cases=cases,
            embed_fn=self.g._embed_text,
            ai_chat_classifier=ai_chat,
        )
        self.user_id = user_id

        self.relay = relay
        self.sim_orchestrator.relay = relay
        self.cases = cases
        self.last_files = []
        self.history = []
        # Key: session_key (str), Value: List[Dict[role, content]]
        self.chat_contexts: Dict[str, List[Dict[str, str]]] = {}
        self.simulation_drafts: Dict[str, Dict[str, Any]] = {}

        self.goal_request_struct: Dict[str, Dict[str, Any]] = {}

        # Background data_handler/main threading
        import threading
        self._data_handler_lock = threading.Lock()
        self._data_handler_thread: Optional[threading.Thread] = None

    def _normalize_relay_payload(self, payload: Any) -> Dict[str, Any]:
        """Normalize relay payload shape so downstream handlers can rely on dict sections."""
        raw = payload if isinstance(payload, dict) else {}
        out = dict(raw)
        auth = out.get("auth")
        data = out.get("data")
        out["auth"] = auth if isinstance(auth, dict) else {}
        out["data"] = data if isinstance(data, dict) else {}
        data_type = out.get("type")
        out["type"] = str(data_type).strip() if data_type is not None and str(data_type).strip() else None
        return out

    @staticmethod
    def _normalize_user_id(user_id: Optional[Any], fallback: str = "public") -> str:
        candidate = str(user_id).strip() if user_id is not None else ""
        return candidate or fallback

    @staticmethod
    def _normalize_session_id(session_id: Optional[Any]) -> Optional[str]:
        if session_id is None:
            return None
        text = str(session_id).strip()
        return text or None

    async def handle_relay_payload(
            self,
            payload: Dict[str, Any],
            user_id: Optional[str] = None,
            session_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any] or List[Dict[str, Any]]]:
        """
        Central entry point for Relay WebSocket messages.
        Workflow: normalize payload -> ensure data_type (classify if missing) -> files -> ensure goal struct for case -> START_SIM | auto-fill -> missing? follow-up | dispatch handler.
        """
        payload = self._normalize_relay_payload(payload)
        user_id = self._normalize_user_id(user_id, fallback=getattr(self, "user_id", "public"))
        session_id = self._normalize_session_id(session_id)
        payload_auth = payload.get("auth") or {}
        if not payload_auth.get("user_id"):
            payload_auth["user_id"] = user_id
        if session_id is not None and not payload_auth.get("session_id"):
            payload_auth["session_id"] = session_id
        payload["auth"] = payload_auth

        print(f"handle_relay_payload: IN payload keys={list(payload.keys())}, user_id={user_id}, session_id={session_id}")

        # Handle typed request (payload already has type)
        data_type = payload.get("type", None)
        if data_type:
            if data_type == "START_SIM":
                return await self._dispatch_start_sim(payload, user_id, session_id)
            if data_type == "CHAT":
                return await self._dispatch_chat(payload)
            result = await self._dispatch_relay_handler(data_type, payload)
            if result is not None:
                if not isinstance(result, dict) or result.get("status", {}).get("state", "").lower() != "error":
                    self._schedule_data_handler_refresh()
                return result

        # Handle untyped request (classify from message)
        data_block = payload.get("data") or {}
        if not isinstance(data_block, dict):
            data_block = {}

        msg = data_block.get("msg") or data_block.get("text") or payload.get("msg") or payload.get("text") or ""
        session_key = self._session_key(session_id, user_id)

        if msg:
            self.chat_contexts.setdefault(session_key, []).append({"role": "user", "content": msg})

        #
        self.upsert_files(session_id, data_block, user_id, msg)

        if not payload.get("type") and not msg:
            print("handle_relay_payload: no type and no msg -> return None")
            return None

        # GET AUTH
        auth = payload.get("auth")

        # STEP: Ensure data_type (run classifier if none or CHAT)
        data_type = self._ensure_data_type_from_classifier(payload, msg, user_id)
        print(f"handle_relay_payload: resolved data_type={data_type}")

        current_case = self._resolve_case(data_type)
        if current_case is None:
            if data_type == "CHAT":
                return await self._dispatch_chat(payload)
            return None
        print("casse struct identifed", current_case)

        req_struct = current_case.get("req_struct") or {}
        goal_struct = self._ensure_goal_struct_for_case(data_type, req_struct)

        # Pre-fill goal_struct from payload so auth.user_id/session_id are not reported missing
        if payload.get("auth") and isinstance(goal_struct.get("auth"), dict):
            self._deep_merge_into(goal_struct["auth"], payload["auth"])

        if payload.get("data") and isinstance(goal_struct.get("data"), dict):
            self._deep_merge_into(goal_struct["data"], payload["data"])

        if data_type == "START_SIM":
            return await self._dispatch_start_sim(payload, user_id, session_id)
        if data_type == "CHAT":
            return await self._dispatch_chat(payload)

        conversation_history = self._get_conversation_history(session_key)

        try:
            self._run_auto_fill(data_type, payload, data_block, req_struct, goal_struct, conversation_history)
        except Exception as e:
            print(f"auto-fill from text error: {e}")

        missing = self._collect_missing_values(goal_struct)
        if missing:
            missing_str = ", ".join(missing)
            conversation_context_str = ""
            if conversation_history:
                recent = conversation_history[-3:]
                conversation_context_str = "\n\nRecent conversation:\n" + "\n".join(
                    f"{m.get('role', '')}: {m.get('content', '')}" for m in recent
                )
            prompt = f"""You are the Orchestrator Assistant for a simulation platform.
            We need more information from the user to complete the request.
            CASE: {data_type}
            MISSING FIELDS: {missing_str}
            Current progression: {json.dumps(goal_struct, ensure_ascii=False)}
            {conversation_context_str}
            Current user input: {json.dumps(payload.get("data") or {}, ensure_ascii=False)}
            Ask a concise, natural follow-up question in the user's language. Only the question."""
            try:
                follow_up = self.gem.ask(content=prompt) or ""
            except Exception as e:
                follow_up = ""
                print(f"follow-up question error: {e}")
            if not follow_up:
                follow_up = f"Please provide: {missing_str}?"
            return self._return_follow_up_chat(follow_up.strip(), session_key)

        print(f"handle_relay_payload: no handler matched type={data_type}, returning None")
        return None

    def _schedule_data_handler_refresh(self) -> None:
        """
        Run self.data_handler.main in a background thread, with a simple
        lock/flag so we never run two graph builds concurrently.

        This avoids race conditions when multiple handle_relay_payload
        calls arrive in parallel while still keeping request handling fast.
        """
        import threading

        with self._data_handler_lock:
            # If a previous refresh is still running, skip – latest state will be picked up there.
            if self._data_handler_thread and self._data_handler_thread.is_alive():
                print("[Thalamus] data_handler.main already running, skipping new refresh request")
                return

            def _worker():
                try:
                    self.data_handler.main()
                except Exception as e:
                    print(f"[Thalamus] data_handler.main error: {e}")
                finally:
                    with self._data_handler_lock:
                        self._data_handler_thread = None

            t = threading.Thread(target=_worker, name="qbrain-data-handler-main", daemon=True)
            self._data_handler_thread = t
            t.start()






    def _sanitize_req_struct_for_json(self, req_struct: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Python type objects (dict, str, etc.) to string representations
        so req_struct can be JSON serialized.
        """
        if not isinstance(req_struct, dict):
            return req_struct
        
        sanitized = {}
        for key, value in req_struct.items():
            if isinstance(value, dict):
                sanitized[key] = self._sanitize_req_struct_for_json(value)
            elif isinstance(value, type):
                # Convert Python type objects to string descriptions
                sanitized[key] = value.__name__ if hasattr(value, "__name__") else str(value)
            elif isinstance(value, str) and value in ("dict", "str", "list", "int", "float", "bool"):
                # Already a string type hint, keep as-is
                sanitized[key] = value
            else:
                sanitized[key] = value
        return sanitized

    def _session_key(self, session_id: Optional[Any], user_id: Optional[str]) -> str:
        """Return key for conversation/history lookup."""
        normalized_sid = self._normalize_session_id(session_id)
        if normalized_sid is not None:
            return normalized_sid
        normalized_uid = self._normalize_user_id(user_id, fallback="")
        return f"user_{normalized_uid}" if normalized_uid else "default"

    def _get_conversation_history(self, session_key: str) -> List[Dict[str, str]]:
        """Return conversation history for session (for context-aware extraction)."""
        return self.chat_contexts.get(session_key, [])

    def _ensure_data_type_from_classifier(
        self, payload: Dict[str, Any], msg: str, user_id: Optional[str]
    ) -> str:
        """
        If payload has no type or type is CHAT, run classifier and set payload['type'].
        Returns resolved data_type (never None after this).
        """
        data_type = payload.get("type")
        if (data_type is None or data_type == "CHAT") and msg and self.chat_classifier and user_id:
            print(f"handle_relay_payload: running classifier for msg={msg!r}")
            classified = self.chat_classifier.main(user_id, msg)
            print(f"handle_relay_payload: classifier_result={classified!r}")
            if classified:
                data_type = classified
                payload["type"] = data_type
            elif data_type is None:
                data_type = "CHAT"
        if not data_type:
            data_type = "CHAT"
        return data_type

    def _resolve_case(self, data_type: str) -> Optional[Dict[str, Any]]:
        """Return case dict for data_type (with case, func, req_struct)."""
        for c in self.cases or []:
            case_name = c.get("case") if isinstance(c, dict) else getattr(c, "case", None)
            if case_name == data_type:
                if isinstance(c, dict):
                    return c
                return {
                    "case": case_name,
                    "func": getattr(c, "callable", None),
                    "req_struct": getattr(c, "required_data_structure", None) or {},
                }
        return None

    def _ensure_goal_struct_for_case(self, data_type: str, req_struct: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure goal_request_struct[data_type] exists. Each new data_type gets a fresh struct.
        Returns the current goal struct for this data_type (mutated as we collect values).
        """
        if data_type not in self.goal_request_struct:
            self.goal_request_struct[data_type] = self._template_to_empty(req_struct)
            print(f"new goal_request_struct for data_type={data_type}")
        return self.goal_request_struct[data_type]

    def _template_to_empty(self, template: Dict[str, Any]) -> Dict[str, Any]:
        """Deep copy req_struct template, replacing type hints with None for leaf values."""
        if not isinstance(template, dict):
            return None
        out = {}
        for k, v in template.items():
            if isinstance(v, dict):
                out[k] = self._template_to_empty(v)
            else:
                out[k] = None  # leaf
        return out

    def _deep_merge_into(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """Merge source into target in place (only set non-None values)."""
        for k, v in source.items():
            if v is None:
                continue
            if isinstance(v, dict) and isinstance(target.get(k), dict):
                self._deep_merge_into(target[k], v)
            else:
                target[k] = v

    def _run_auto_fill(
        self,
        data_type: str,
        payload: Dict[str, Any],
        data_block: Dict[str, Any],
        req_struct_template: Dict[str, Any],
        goal_struct: Dict[str, Any],
        conversation_history: List[Dict[str, str]],
    ) -> None:
        """Merge inferred values from user message into payload and goal_struct."""
        msg_text = data_block.get("msg") or data_block.get("text") or payload.get("msg") or payload.get("text") or ""
        if not msg_text:
            return
        inferred = self._extract_goal_values_from_text(
            case_name=data_type,
            message=msg_text,
            req_struct=req_struct_template,
            current_payload=payload,
            conversation_history=conversation_history,
        )
        if not isinstance(inferred, dict):
            return
        for section, section_vals in inferred.items():
            if not isinstance(section_vals, dict):
                continue
            base = payload.get(section) or {}
            if not isinstance(base, dict):
                base = {}
            for key, val in section_vals.items():
                if val is not None:
                    if isinstance(val, dict) and isinstance(base.get(key), dict):
                        base[key] = {**base[key], **val}
                    elif key not in base or base.get(key) is None:
                        base[key] = val
            payload[section] = base
        # Keep goal_struct in sync so _collect_missing_values sees progress
        for section in ("auth", "data"):
            if section in inferred and isinstance(inferred[section], dict) and section in goal_struct:
                if goal_struct[section] is None:
                    goal_struct[section] = {}
                if isinstance(goal_struct[section], dict):
                    self._deep_merge_into(goal_struct[section], inferred[section])

    def _collect_missing_values(self, goal_struct: Dict[str, Any], prefix: str = "") -> List[str]:
        """Recursively collect keys whose value is None. Returns paths like 'data.field', 'auth.user_id'."""
        missing = []

        def walk(d: Any, path: str) -> None:
            if isinstance(d, dict):
                for k, v in d.items():
                    p = f"{path}.{k}" if path else k
                    walk(v, p)
            elif d is None or d == "":
                if path:
                    missing.append(path)

        walk(goal_struct, prefix)
        return missing

    def _return_follow_up_chat(
        self,
        follow_up_msg: str,
        session_key: str,
    ) -> Dict[str, Any]:
        """Store assistant message in history and return CHAT-shaped response."""
        if session_key:
            self.chat_contexts.setdefault(session_key, []).append({"role": "assistant", "content": follow_up_msg})
        for c in self.cases or []:
            if isinstance(c, dict) and c.get("case") == "CHAT" and isinstance(c.get("out_struct"), dict):
                out_struct = c["out_struct"]
                data_template = out_struct.get("data", {}) or {}
                data_out = {k: (follow_up_msg if k.lower() in ("msg", "message", "text") else None) for k in data_template}
                return {
                    "type": out_struct.get("type", "CHAT"),
                    "status": {"state": "success", "code": 200, "msg": ""},
                    "data": data_out,
                }
        return {
            "type": "CHAT",
            "status": {"state": "success", "code": 200, "msg": ""},
            "data": {"msg": follow_up_msg},
        }

    async def _dispatch_relay_handler(
        self,
        data_type: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Find and invoke the relay handler for data_type. Passes only data and auth from payload. Returns result dict or None."""
        data = payload.get("data")
        auth = payload.get("auth")
        if not isinstance(data, dict):
            data = {}
        if not isinstance(auth, dict):
            auth = {}

        for relay_case in self.cases or []:
            if isinstance(relay_case, dict):
                case_name = relay_case.get("case")
                handler = relay_case.get("func") if case_name == data_type else None
            else:
                case_name = getattr(relay_case, "case", None)
                handler = getattr(relay_case, "callable", None) if case_name == data_type else None

            if case_name != data_type or handler is None:
                continue

            try:
                token = set_orchestrator(self)
                try:
                    if asyncio.iscoroutinefunction(handler):
                        result = await handler(data=data, auth=auth)
                    else:
                        result = handler(data=data, auth=auth)
                    if isinstance(result, dict):
                        return result
                finally:
                    reset_orchestrator(token)
            except Exception as e:
                print(f"Error handling relay payload type={data_type}: {e}")
                import traceback
                traceback.print_exc()
                return {"type": data_type, "status": {"state": "error", "code": 500, "msg": str(e)}, "data": {}}
        return None

    async def _dispatch_start_sim(
        self,
        payload: Dict[str, Any],
        user_id: str,
        session_id: Optional[str] = None,
    ):
        """Run START_SIM workflow and schedule graph refresh."""
        result = await asyncio.to_thread(
            self._run_start_sim,
            payload,
            user_id,
            session_id=session_id,
        )
        self._schedule_data_handler_refresh()
        return result

    async def _dispatch_chat(self, payload: Dict[str, Any]):
        """Dispatch CHAT handler; return fallback message if no handler matched."""
        chat_result = await self._dispatch_relay_handler("CHAT", payload)
        if chat_result is not None:
            return chat_result
        return {
            "type": "CHAT",
            "status": {"state": "success", "code": 200, "msg": ""},
            "data": {"msg": "Couldn't identify your request. Please try rephrasing."},
        }

    def _extract_goal_values_from_text(
        self,
        case_name: str,
        message: str,
        req_struct: Dict[str, Any],
        current_payload: Dict[str, Any],
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Use Gemini to map a free-form user message into the current
        goal_request_struct for this case (auth/data/...).

        This allows natural language like:
            "create field with param 1,2 and 3. provide them values 4,5 and 6"
        to be converted into a SET_FIELD-shaped payload.
        
        conversation_history: Optional list of previous messages for context.
        """
        if not message.strip() or not isinstance(req_struct, dict):
            return None

        # Sanitize req_struct to handle Python type objects
        sanitized_req_struct = self._sanitize_req_struct_for_json(
            req_struct)

        # Only keep sections that appear in req_struct for context.
        context_payload = {
            k: v for k, v in (current_payload or {}).items() if k in req_struct
        }

        # Build conversation context string
        conversation_context = ""
        if conversation_history:
            recent_messages = conversation_history[-5:]  # Last 5 messages for context
            conversation_context = "\n\nPrevious conversation:\n"
            for msg in recent_messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role == "user" and content:
                    conversation_context += f"User: {content}\n"
                elif role == "assistant" and content:
                    conversation_context += f"Assistant: {content}\n"

        policy_mode = resolve_prompt_policy_mode()
        policy_block = build_extraction_policy_block(
            case_name=case_name,
            policy_mode=policy_mode,
        )

        prompt = f"""
        You are a structured data extraction assistant for the Orchestrator.
        
        CASE: {case_name}
        
        We have the following request template (req_struct):
        {json.dumps(sanitized_req_struct, ensure_ascii=False, indent=2)}
        
        {conversation_context}
        
        Current user message:
        {message}
        
        Existing payload (may already contain some values):
        {json.dumps(context_payload, ensure_ascii=False, indent=2)}
        
        Your task:
        - Infer values for as many fields in req_struct as possible from the user message AND conversation history.
        - If the user refers to previous messages (e.g., "name and id are hello" referring to a previous question), use that context.
        {policy_block}
        """

        if case_name == "SET_FIELD":
            prompt += """
            Additional rules for CASE=SET_FIELD:
            - The object under data.field describes a new field.
            - When the user mentions parameters (e.g., "include params dmuG, h and psi"), create data.field.params as a list or array of those parameter names/IDs.
            - When the user mentions parameter values or types (e.g., "provide them all a matrix point"), include that information in data.field.params or data.field structure.
            - If the user gives a field name (e.g., "name is hello"), populate data.field.name with that value.
            - If the user gives an ID (e.g., "id is hello"), populate auth.original_id with that value.
            - Example: User says "name and id are hello" -> data.field.name = "hello", auth.original_id = "hello"
            - Example: User says "include params dmuG, h and psi" -> data.field.params = ["dmuG", "h", "psi"] or data.field.params = {{"dmuG": null, "h": null, "psi": null}}
            """

        prompt += """
        Return ONLY the JSON, with no markdown, backticks, or explanations.
        """

        try:
            raw = self.gem.ask(content=prompt)
            text = raw.strip()
            # Strip accidental markdown fences if present.
            text = text.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception as e:
            print(f"_extract_goal_values_from_text error: {e}")
            import traceback
            traceback.print_exc()
        return None



    def upsert_files(self, session_id, data_block, user_id, msg):
        # If files are present in the payload, immediately run the FileManager
        # pipeline so that modules/params/fields are extracted and upserted (via qb).
        files = data_block.get("files") or []
        if files and user_id and session_id:
            try:
                print(
                    f"handle_relay_payload: detected files -> invoking FileManager for session_id={session_id}")
                fm_data = {
                    "id": f"session_{session_id}",
                    "files": files,
                    "prompt": msg or "",
                    "msg": msg or "",
                }
                fm_result = self.file_manager.process_and_upload_file_config(
                    user_id=user_id,
                    data=fm_data,
                    testing=False,
                    mock_extraction=False,
                )
                print(f"handle_relay_payload: FileManager finished type={fm_result.get('type')}")

            except Exception as e:
                print(f"handle_relay_payload: FileManager error: {e}")


    def _run_start_sim(
            self,
            payload: dict,
            user_id: str,
            session_id: Optional[str] = None,
    ):
        """
        Delegate to SimOrchestrator when configured; fall back to legacy _handle_start_sim_process.
        """
        if self.sim_orchestrator is not None:
            result = self.sim_orchestrator.run(
                payload=payload,
                user_id=user_id or self.user_id,
                session_id=session_id,
            )
            if result:
                return result
        return self._handle_start_sim_process(
            payload=payload,
            user_id=user_id,
            session_id=session_id,
        )

    def _validate_start_sim_config(
        self,
        config: Dict[str, Any],
        session_id: Optional[str],
        user_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Validate START_SIM config. Returns follow-up chat response if invalid, else None.
        """
        if not config:
            session_key = self._session_key(session_id, user_id)
            msg = (
                "To start a simulation, provide an env with modules and fields. "
                "Use create_env_from_components with module_id, field_ids, and method_ids."
            )
            return self._return_follow_up_chat(msg, session_key)
        for env_id, env_cfg in config.items():
            is_valid, missing = validate_env_components(
                env_cfg if isinstance(env_cfg, dict) else None
            )
            if not is_valid:
                session_key = self._session_key(session_id, user_id)
                missing_str = ", ".join(missing)
                msg = (
                    f"Env {env_id} is missing: {missing_str}. "
                    "Please add modules with fields, or use create_env_from_components."
                )
                return self._return_follow_up_chat(msg, session_key)
        return None

    def _finalize_session_rollover(self, session_id: Optional[str], user_id: str) -> None:
        """Deactivate current session and create new active session."""
        sid = session_id or (getattr(self.relay, "session_id", None) if self.relay else None)
        if sid is None:
            return
        try:
            print(f"_handle_start_sim_process: deactivating session {sid}")
            self.session_manager.deactivate_session(sid)
            new_sid = self.session_manager.get_or_create_active_session(user_id)
            print(f"_handle_start_sim_process: new session_id={new_sid}")
            if self.relay is not None:
                self.relay.session_id = new_sid
        except Exception as sess_err:
            print(f"_handle_start_sim_process: session deactivate/create error: {sess_err}")
            import traceback
            traceback.print_exc()

    def _handle_start_sim_process(
            self,
            payload: dict,
            user_id: str,
            session_id: Optional[str] = None,
    ):
        """
        Handles the START_SIM case.
        Pre-check: validate env_data has modules/fields. If missing, return need_data.
        Delegates to Guard.main to fetch data, build graph, and compile pattern.
        Deactivates session upon success.
        """
        print("_handle_start_sim_process...")
        try:
            response_items = []
            config = payload.get("data", {}).get("config", {})
            validation_err = self._validate_start_sim_config(config, session_id, user_id)
            if validation_err is not None:
                return validation_err

            for k, v in config.items():
                try:
                    grid_streamer = getattr(self.relay, "_grid_streamer", None) if self.relay else None
                    # qdash controller engine runs without matplotlib-based animation recording.
                    grid_animation_recorder = None
                    print(f"_handle_start_sim_process: running guard.main for env_id={k}")
                    components = self.guard.main(
                        env_id=k,
                        env_data=v,
                        grid_streamer=grid_streamer,
                        grid_animation_recorder=grid_animation_recorder,
                    )
                    print(f"_handle_start_sim_process: guard.main done for env_id={k}")
                except Exception as guard_err:
                    print(f"_handle_start_sim_process: guard.main error for env_id={k}: {guard_err}")
                    import traceback
                    traceback.print_exc()
                    raise

                # send update user env
                try:
                    print(f"_handle_start_sim_process: retrieving env table rows for user_id={user_id}")
                    updated_env = self.env_manager.retrieve_send_user_specific_env_table_rows(user_id)
                    response_items.append(
                        {
                            "type": "GET_USERS_ENVS",
                            "data": updated_env
                        }
                    )
                    print(f"_handle_start_sim_process: sent GET_USERS_ENVS for env_id={k}")
                except Exception as ex:
                    print(f"_handle_start_sim_process: error updating env status (continuing): {ex}")
                    import traceback
                    traceback.print_exc()





            self._finalize_session_rollover(session_id, user_id)

            response = {
                "type": "START_SIM",
                "status": {"state": "success", "code": 200, "msg": "Simulation started and session completed."},
                "data": {}
            }
            response_items.append(response)
            print(f"_handle_start_sim_process: completed successfully")
        except Exception as e:
            print(f"_handle_start_sim_process: error: {e}")
            import traceback
            traceback.print_exc()
            response_items.append({
                "type": "START_SIM",
                "status": {"state": "error", "code": 500, "msg": str(e)},
                "data": {}
            })
        print("_handle_start_sim_process... done")
        return response_items




if __name__ == "__main__":
    import sys
    from qbrain.core.orchestrator_manager.test_cases import (
        ORCHESTRATOR_TEST_CASES,
        run_test_cases,
    )

    orchestrator = Thalamus(RELAY_CASES_CONFIG)

    # --test [index ...]  Run test case struct (all or given indices)
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        indices = []
        for a in sys.argv[2:]:
            try:
                indices.append(int(a))
            except ValueError:
                pass
        cases = (
            [ORCHESTRATOR_TEST_CASES[i] for i in indices if 0 <= i < len(ORCHESTRATOR_TEST_CASES)]
            if indices
            else ORCHESTRATOR_TEST_CASES
        )
        print(f"Running {len(cases)} orchestrator test case(s)...")
        results = run_test_cases(
            orchestrator.handle_relay_payload,
            user_id="user_id",
            session_id="1",
            cases=cases,
            verbose=True,
        )
        for r in results:
            if r.get("error"):
                print(f"  FAIL {r['name']}: {r['error']}")
            else:
                resp = r.get("response")
                typ = resp.get("type") if isinstance(resp, dict) else type(resp).__name__
                print(f"  OK   {r['name']} -> {typ}")
        sys.exit(0)

    # Interactive loop
    try:
        while True:
            _in = input("> ")
            payload = {"data": {"msg": _in}, "type": None}
            orchestrator_response = asyncio.run(
                orchestrator.handle_relay_payload(
                    payload=payload,
                    user_id="user_id",
                    session_id="1",
                )
            )
            print("iter finished:", orchestrator_response)
    except Exception as orch_err:
        print(f"receive: orchestrator error: {orch_err}")
        import traceback
        traceback.print_exc()