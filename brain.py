from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from collections import deque
from typing import Any, Deque, Dict, List, Optional

import networkx as nx
import numpy as np

from brain_classifier import BrainClassifier
from brain_executor import BrainExecutor, _flatten_required_keys
from brain_graph_utils import get_sub_goal_ids_for_goal
from brain_hydrator import BrainHydrator
from brain_schema import BrainEdgeRel, BrainNodeType, DataCollectionResult, GoalDecision
from brain_utils import normalize_user_id
from brain_workers import BrainWorkers
from graph.local_graph_utils import GUtils
from graph.pathway import build_and_run_pathways, should_build_pathways
from pickup import McpPickup
from receive_user_data import ReceiveUserData
from think_manager import ThinkManager





def _get_brain_graph(use_shared_kg: bool = True) -> nx.MultiGraph:
    """Return the graph for Brain: shared KG (single instance for all users) or a new graph."""
    if use_shared_kg:
        try:
            from graph.kg import get_knowledge_graph
            return get_knowledge_graph()
        except Exception:
            pass
    return nx.MultiGraph()


class Brain(GUtils):
    """Hybrid graph+db brain that classifies goals and executes relay cases. Uses single shared KG by default."""

    def __init__(
        self,
        _dr_backend,
        user_id: str,

        max_short_term: int = 30,
        use_shared_kg: bool = True,
    ):
        print("__init__...")
        user_id = normalize_user_id(user_id)
        max_short_term = self._normalize_max_short_term(max_short_term)
        G = _get_brain_graph(use_shared_kg=use_shared_kg)
        super().__init__(G=G, nx_only=True, enable_data_store=False)
        self.user_id = user_id
        self._dr_backend=_dr_backend
        self.max_short_term = max_short_term
        self.short_term_ids: Deque[str] = deque(maxlen=max_short_term)
        self.long_term_ids: List[str] = []
        self.last_goal_node_id: Optional[str] = None
        try:
            self.think_manager: Optional[ThinkManager] = ThinkManager(G=self.G, qb=self._qb, user_id=self.user_id)
        except Exception as exc:
            print(f"Brain.__init__: ThinkManager init warning: {exc}")
            self.think_manager = None
        self.workers = BrainWorkers(max_workers=4)
        self.hydrator = BrainHydrator(self._qb)
        self.executor = BrainExecutor()
        self.classifier = BrainClassifier(
            relay_cases=_get_relay_cases_config(),
            embed_fn=self._embed_text,
            vector_db_path="brain_cases.duckdb",
            use_vector=True,
        )
        self._graph_update_lock = threading.Lock()
        poll = self._resolve_float_env("BRAIN_MCP_POLL_INTERVAL_SEC", 20.0)
        timeout = self._resolve_float_env("BRAIN_MCP_HTTP_TIMEOUT_SEC", 8.0)
        self._mcp_pickup = McpPickup(
            gutils=self,
            user_id=self.user_id,
            graph_lock=self._graph_update_lock,
            poll_interval_sec=poll,
            http_timeout_sec=timeout,
        )
        self._mcp_poll_interval_sec = poll
        self._mcp_http_timeout_sec = timeout
        self._mcp_stop_event = threading.Event()
        self._mcp_loop_thread: Optional[threading.Thread] = None
        self._receive_user_data = ReceiveUserData(
            gutils=self,
            user_id=self.user_id,
            graph_lock=self._graph_update_lock,
            http_timeout_sec=timeout,
        )

        self._ensure_content_chunk_table()
        self._init_user_node()
        self._start_background_mcp_loop()
        print("__init__... done")

    @staticmethod
    def _normalize_max_short_term(value: Any, default: int = 30) -> int:
        try:
            parsed = int(value)
            if parsed > 0:
                return parsed
        except Exception:
            pass
        return default

    @staticmethod
    def _resolve_float_env(key: str, default: float) -> float:
        raw = str(os.environ.get(key) or "").strip()
        if not raw:
            return default
        try:
            parsed = float(raw)
            return parsed if parsed > 0 else default
        except Exception:
            return default

    def _run_background_mcp_loop(self) -> None:
        print("_run_background_mcp_loop...")
        while not self._mcp_stop_event.is_set():
            try:
                inserted = self._mcp_pickup.refresh()
                if inserted:
                    print(f"_run_background_mcp_loop: upserted ACTION nodes={inserted}")
            except Exception as exc:
                print(f"_run_background_mcp_loop: warning: {exc}")
            self._mcp_stop_event.wait(self._mcp_poll_interval_sec)
        print("_run_background_mcp_loop... done")

    def _start_background_mcp_loop(self) -> None:
        if self._mcp_loop_thread and self._mcp_loop_thread.is_alive():
            return
        self._mcp_loop_thread = threading.Thread(
            target=self._run_background_mcp_loop,
            name="qbrain-mcp-action-loop",
            daemon=True,
        )
        self._mcp_loop_thread.start()

    def close(self) -> None:
        self._mcp_stop_event.set()
        if self._mcp_loop_thread and self._mcp_loop_thread.is_alive():
            join_timeout = max(1.0, self._mcp_http_timeout_sec + 1.0)
            self._mcp_loop_thread.join(timeout=join_timeout)
            if self._mcp_loop_thread.is_alive():
                print(f"[Brain] MCP loop thread still alive after {join_timeout:.1f}s timeout")
        self.workers.close()

    def _init_user_node(self) -> None:
        print("_init_user_node...")
        self.add_node({"id": f"USER::{self.user_id}", "type": BrainNodeType.USER, "user_id": self.user_id})
        print("_init_user_node... done")

    def _ensure_content_chunk_table(self) -> None:
        print("_ensure_content_chunk_table...")
        try:
            db = getattr(self.qb, "db", None)
            if db is None:
                print("_ensure_content_chunk_table... done")
                return
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS brain_content_chunks (
                    id VARCHAR PRIMARY KEY,
                    user_id VARCHAR,
                    source_file VARCHAR,
                    chunk_type VARCHAR,
                    parent_id VARCHAR,
                    description VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        except Exception as exc:
            print(f"_ensure_content_chunk_table: warning: {exc}")
        print("_ensure_content_chunk_table... done")

    def _embed_text(self, text: str) -> List[float]:
        print("_embed_text...")
        # Preferred embedding path via QBrain manager.
        if hasattr(self.qb, "_generate_embedding"):
            vec = self.qb._generate_embedding(text)  # existing internal helper
            if vec:
                print("_embed_text... done")
                return [float(x) for x in vec]

        # Deterministic local fallback embedding for robust execution without external APIs.
        digest = hashlib.sha256((text or "").encode("utf-8")).digest()
        seed = int.from_bytes(digest[:8], byteorder="big", signed=False)
        rng = np.random.default_rng(seed)
        vec = rng.standard_normal(128).astype(np.float32)
        norm = np.linalg.norm(vec)
        if norm > 1e-8:
            vec = vec / norm
        out = vec.tolist()
        print("_embed_text... done")
        return out

    def _add_edge(self, src: str, trt: str, rel: str, src_layer: str, trgt_layer: str) -> None:
        self.add_edge(
            src=src,
            trt=trt,
            attrs={"rel": rel, "src_layer": src_layer, "trgt_layer": trgt_layer},
        )

    def hydrate_user_context(self) -> int:
        print("hydrate_user_context...")
        nodes = self.workers.run_sync(self.hydrator.hydrate_user_long_term, self.user_id)
        user_node_id = f"USER::{self.user_id}"
        inserted = 0
        for n in nodes:
            self.add_node(n)
            self._add_edge(
                src=user_node_id,
                trt=n["id"],
                rel=BrainEdgeRel.REFERENCES_TABLE_ROW,
                src_layer=BrainNodeType.USER,
                trgt_layer=BrainNodeType.LONG_TERM_STORAGE,
            )
            self.long_term_ids.append(n["id"])
            inserted += 1
        inserted += self._receive_user_data.receive()
        print("hydrate_user_context... done")
        return inserted

    def _add_short_term(self, role: str, message: str, request_id: Optional[str] = None) -> str:
        print("_add_short_term...")
        ts = int(time.time() * 1000)
        rid = request_id or str(uuid.uuid4())
        node_id = f"STS::{self.user_id}::{ts}::{rid[:8]}"
        node = {
            "id": node_id,
            "type": BrainNodeType.SHORT_TERM_STORAGE,
            "role": role,
            "message": message,
            "user_id": self.user_id,
            "request_id": rid,
            "created_at": ts,
        }
        self.add_node(node)
        self.short_term_ids.append(node_id)

        user_node_id = f"USER::{self.user_id}"
        self._add_edge(
            src=user_node_id,
            trt=node_id,
            rel=BrainEdgeRel.DERIVED_FROM,
            src_layer=BrainNodeType.USER,
            trgt_layer=BrainNodeType.SHORT_TERM_STORAGE,
        )

        # Preserve temporal chain in short-term memory.
        if len(self.short_term_ids) >= 2:
            ids = list(self.short_term_ids)
            self._add_edge(
                src=ids[-2],
                trt=ids[-1],
                rel=BrainEdgeRel.FOLLOWS,
                src_layer=BrainNodeType.SHORT_TERM_STORAGE,
                trgt_layer=BrainNodeType.SHORT_TERM_STORAGE,
            )
        print("_add_short_term... done")
        return node_id

    def _persist_content_ref(self, node: Dict[str, Any]) -> None:
        print("_persist_content_ref...")
        try:
            db = getattr(self.qb, "db", None)
            if db is None:
                print("_persist_content_ref... done")
                return
            db.execute(
                """
                INSERT INTO brain_content_chunks (id, user_id, source_file, chunk_type, parent_id, description)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    user_id=excluded.user_id,
                    source_file=excluded.source_file,
                    chunk_type=excluded.chunk_type,
                    parent_id=excluded.parent_id,
                    description=excluded.description
                """,
                [
                    str(node.get("id")),
                    self.user_id,
                    str(node.get("source_file") or ""),
                    str(node.get("chunk_type") or ""),
                    str(node.get("parent_id") or ""),
                    str(node.get("content") or "")[:512],
                ],
            )
        except Exception as exc:
            print(f"_persist_content_ref: warning: {exc}")
        print("_persist_content_ref... done")

    def ingest_input(
        self,
        content: str,
        *,
        content_type: str = "text",
        source_file: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        print("ingest_input...")
        if content_type == "text" and not source_file:
            node_id = self._add_short_term(role="user", message=content, request_id=request_id)
            print("ingest_input... done")
            return {"node_id": node_id, "kind": "short_term"}

        # File-like content path: create CONTENT parent/child chunks and persist compact refs.
        source = source_file or f"inline_{uuid.uuid4().hex[:8]}.txt"
        text = content if isinstance(content, str) else str(content)
        parent_chunks = [text[i : i + 1000] for i in range(0, len(text), 1000)] or [text]
        created_ids: List[str] = []
        for i, parent_text in enumerate(parent_chunks):
            parent_id = f"CONTENT::{source}::p{i}"
            parent_node = {
                "id": parent_id,
                "type": BrainNodeType.CONTENT,
                "user_id": self.user_id,
                "source_file": source,
                "chunk_type": "large",
                "parent_id": None,
                "content": parent_text,
            }
            self.add_node(parent_node)
            self._persist_content_ref(parent_node)
            created_ids.append(parent_id)

            child_chunks = [parent_text[j : j + 200] for j in range(0, len(parent_text), 200)] or [parent_text]
            prev_child_id: Optional[str] = None
            for j, child_text in enumerate(child_chunks):
                child_id = f"{parent_id}::c{j}"
                child_node = {
                    "id": child_id,
                    "type": BrainNodeType.CONTENT,
                    "user_id": self.user_id,
                    "source_file": source,
                    "chunk_type": "small",
                    "parent_id": parent_id,
                    "content": child_text,
                }
                self.add_node(child_node)
                self._persist_content_ref(child_node)
                created_ids.append(child_id)

                self._add_edge(
                    src=parent_id,
                    trt=child_id,
                    rel=BrainEdgeRel.PARENT_OF,
                    src_layer=BrainNodeType.CONTENT,
                    trgt_layer=BrainNodeType.CONTENT,
                )
                if prev_child_id:
                    self._add_edge(
                        src=prev_child_id,
                        trt=child_id,
                        rel=BrainEdgeRel.FOLLOWS,
                        src_layer=BrainNodeType.CONTENT,
                        trgt_layer=BrainNodeType.CONTENT,
                    )
                prev_child_id = child_id

        print("ingest_input... done")
        return {"node_ids": created_ids, "kind": "content"}

    def _get_long_term_nodes(self) -> List[Dict[str, Any]]:
        print("_get_long_term_nodes...")
        nodes: List[Dict[str, Any]] = []
        for nid in self.long_term_ids:
            if self.G.has_node(nid):
                attrs = dict(self.get_node(nid))
                attrs["id"] = nid
                nodes.append(attrs)
        print("_get_long_term_nodes... done")
        return nodes

    def classify_goal(self, user_query: str) -> GoalDecision:
        print("classify_goal...")
        long_term_nodes = self._get_long_term_nodes()
        decision = self.classifier.classify(user_query, long_term_nodes=long_term_nodes)

        goal_node_id = f"GOAL::{self.user_id}::{decision.case_name or 'UNKNOWN'}::{int(time.time() * 1000)}"
        goal_node = {
            "id": goal_node_id,
            "type": BrainNodeType.GOAL,
            "user_id": self.user_id,
            "case_name": decision.case_name,
            "confidence": decision.confidence,
            "source": decision.source,
            "reason": decision.reason,
        }
        self.add_node(goal_node)
        self.last_goal_node_id = goal_node_id
        self._add_edge(
            src=f"USER::{self.user_id}",
            trt=goal_node_id,
            rel=BrainEdgeRel.DERIVED_FROM,
            src_layer=BrainNodeType.USER,
            trgt_layer=BrainNodeType.GOAL,
        )
        print("classify_goal... done")
        return decision

    def _extract_from_short_term(self, key: str) -> Optional[str]:
        print("_extract_from_short_term...")
        pattern = key.split(".")[-1].lower()
        for nid in reversed(list(self.short_term_ids)):
            if not self.G.has_node(nid):
                continue
            msg = str(self.get_node(nid).get("message") or "")
            low = msg.lower()
            marker = f"{pattern}:"
            if marker in low:
                idx = low.find(marker)
                value = msg[idx + len(marker) :].strip().split("\n")[0]
                print("_extract_from_short_term... done")
                return value
        print("_extract_from_short_term... done")
        return None

    def _extract_from_long_term(self, key: str) -> Optional[str]:
        print("_extract_from_long_term...")
        k = key.split(".")[-1].lower()
        for nid in self.long_term_ids:
            if not self.G.has_node(nid):
                continue
            n = self.get_node(nid)
            if k in n:
                print("_extract_from_long_term... done")
                return str(n.get(k))
            desc = str(n.get("description") or "")
            if k in desc.lower():
                print("_extract_from_long_term... done")
                return desc
        print("_extract_from_long_term... done")
        return None

    def collect_required_data(
        self,
        decision: GoalDecision,
        user_payload: Optional[Dict[str, Any]] = None,
    ) -> DataCollectionResult:
        print("collect_required_data...")
        user_payload = user_payload or {}
        required_keys = _flatten_required_keys(decision.req_struct)

        resolved: Dict[str, Any] = {}
        missing: List[str] = []
        for key in required_keys:
            if key in user_payload and user_payload[key] not in (None, ""):
                resolved[key] = user_payload[key]
                continue
            v_short = self._extract_from_short_term(key)
            if v_short not in (None, ""):
                resolved[key] = v_short
                continue
            v_long = self._extract_from_long_term(key)
            if v_long not in (None, ""):
                resolved[key] = v_long
                continue
            missing.append(key)

        print("collect_required_data... done")
        return DataCollectionResult(resolved=resolved, missing=missing)

    def _cleanup_goal_and_subgoals(self, goal_node_id: Optional[str]) -> None:
        print("_cleanup_goal_and_subgoals...")
        if not goal_node_id or not self.G.has_node(goal_node_id):
            print("_cleanup_goal_and_subgoals... done")
            return

        sub_goal_ids = get_sub_goal_ids_for_goal(self.G, goal_node_id)
        # Remove sub-goals first, then goal.
        for sid in set(sub_goal_ids):
            if self.G.has_node(sid):
                self.G.remove_node(sid)
                print(f"_cleanup_goal_and_subgoals: removed sub_goal={sid}")

        if self.G.has_node(goal_node_id):
            self.G.remove_node(goal_node_id)
            print(f"_cleanup_goal_and_subgoals: removed goal={goal_node_id}")

        if self.last_goal_node_id == goal_node_id:
            self.last_goal_node_id = None
        print("_cleanup_goal_and_subgoals... done")

    async def execute_or_ask(
        self,
        user_query: str,
        user_payload: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
        data_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        print("execute_or_ask...")
        self.ingest_input(user_query, content_type="text", request_id=request_id)

        # ------------------------------------------------------------------
        # Issue #1 & #2: Pathway Creation + Execution
        # Trigger when type=None AND no PATHWAY nodes exist for this user.
        # ------------------------------------------------------------------
        if data_type is None and should_build_pathways(self.G, self.user_id):
            try:
                resolved_fields: Dict[str, Any] = dict(user_payload or {})
                pathway_result = build_and_run_pathways(
                    G=self.G,
                    user_id=self.user_id,
                    add_node_fn=self.add_node,
                    add_edge_fn=lambda src, trt, attrs: self.add_edge(src=src, trt=trt, attrs=attrs),
                    resolved_fields=resolved_fields,
                    query=user_query,
                )
                if pathway_result.get("pathway_nodes"):
                    self._add_short_term(
                        role="assistant",
                        message=str(pathway_result.get("next_message") or ""),
                        request_id=request_id,
                    )
                    print("execute_or_ask... done (pathway)")
                    return pathway_result
            except Exception as exc:
                print(f"execute_or_ask: pathway build/run warning: {exc}")

        decision = self.classify_goal(user_query)
        collect = self.collect_required_data(decision, user_payload=user_payload)
        suggestions: Dict[str, Any] = {}
        if getattr(self, "think_manager", None) is not None and collect.missing:
            try:
                case_item: Dict[str, Any] = decision.case_item or {}
                suggestions = self.think_manager.suggest_missing_fields(case_item, collect.missing)
            except Exception as exc:
                print(f"execute_or_ask: ThinkManager.suggest_missing_fields warning: {exc}")
        created_sub_goal_id: Optional[str] = None

        if collect.missing:
            sub_goal_id = f"SUBGOAL::{self.user_id}::{decision.case_name}::{int(time.time() * 1000)}"
            self.add_node(
                {
                    "id": sub_goal_id,
                    "type": BrainNodeType.SUB_GOAL,
                    "user_id": self.user_id,
                    "goal_case": decision.case_name,
                    "missing_fields": json.dumps(collect.missing),
                }
            )
            created_sub_goal_id = sub_goal_id
            if self.last_goal_node_id:
                self._add_edge(
                    src=self.last_goal_node_id,
                    trt=sub_goal_id,
                    rel=BrainEdgeRel.REQUIRES,
                    src_layer=BrainNodeType.GOAL,
                    trgt_layer=BrainNodeType.SUB_GOAL,
                )

        case_item = decision.case_item or {}
        result = await self.executor.execute_or_request_more(
            case_item=case_item,
            resolved_fields=collect.resolved,
            missing_fields=collect.missing,
        )

        if suggestions:
            try:
                # Attach ThinkManager suggestions without mutating core executor semantics.
                if isinstance(result, dict):
                    result.setdefault("suggestions", suggestions)
            except Exception as exc:
                print(f"execute_or_ask: attach suggestions warning: {exc}")

        # If execution succeeded, remove active GOAL and related SUB_GOAL nodes.
        if str(result.get("status") or "").lower() == "executed":
            self._cleanup_goal_and_subgoals(self.last_goal_node_id)
            if created_sub_goal_id and self.G.has_node(created_sub_goal_id):
                self.G.remove_node(created_sub_goal_id)

        self._add_short_term(role="assistant", message=str(result.get("next_message") or ""), request_id=request_id)
        print("execute_or_ask... done")
        return result


    def process_file_result(
        self,
        user_id: str,
        file_result: Dict[str, Any],
        module_id: Optional[str] = None,
        classification: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Merge file-manager result into this Brain's graph (seamless workflow).
        Uses GraphProcessor to add FILE, EQUATION, OBJECT, METHOD nodes; all users in single KG.
        """
        from core.file_manager.graph_processor import get_graph_processor
        proc = get_graph_processor(self.G)
        data = file_result.get("data") or {}
        mid = module_id or data.get("module_id") or f"file_{int(time.time())}"
        return proc.merge(
            user_id=user_id,
            module_id=mid,
            file_result=file_result,
            created_components=file_result.get("created_components"),
            classification=classification,
        )