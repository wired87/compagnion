from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

from qbrain._db.vector_store import VectorStore
from brain_schema import GoalDecision


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _tokens(text: str) -> List[str]:
    return [t for t in re.findall(r"[a-zA-Z0-9_]+", _normalize_text(text)) if t]


class BrainClassifier:
    """Hybrid rule/vector classifier for relay-case goals."""

    def __init__(
        self,
        relay_cases: List[Dict[str, Any]],
        embed_fn: Callable[[str], List[float]],
        vector_db_path: str = "brain_cases.duckdb",
        use_vector: bool = True,
    ):
        self.relay_cases = relay_cases or []
        self.embed_fn = embed_fn
        self.use_vector = use_vector
        self._case_by_name: Dict[str, Dict[str, Any]] = {}

        self._vector_store = VectorStore(
            store_name="brain_relay_cases",
            db_path=vector_db_path,
            normalize_embeddings=True,
        )
        self._vector_ready = False

        self._build_case_map()
        self._init_vector_index()

    def _build_case_map(self) -> None:
        for item in self.relay_cases:
            case_name = str(item.get("case") or "").strip()
            if case_name:
                self._case_by_name[case_name] = item

    def _rule_score(self, query: str, case_item: Dict[str, Any]) -> float:
        q = _normalize_text(query)
        q_tokens = set(_tokens(q))
        cname = str(case_item.get("case") or "")
        cdesc = str(case_item.get("desc") or case_item.get("description") or "")
        name_tokens = set(_tokens(cname.replace("_", " ")))
        desc_tokens = set(_tokens(cdesc))

        if cname.lower() == q:
            return 1.0
        if cname.lower() in q:
            return 0.95

        overlap_name = len(q_tokens & name_tokens)
        overlap_desc = len(q_tokens & desc_tokens)
        if not q_tokens:
            return 0.0
        return min(0.90, (2.0 * overlap_name + overlap_desc) / max(1.0, len(q_tokens)))

    def _init_vector_index(self) -> None:
        if not self.use_vector:
            return
        if not self.relay_cases:
            return
        print("_init_vector_index...")
        try:
            self._vector_store.create_store()
            ids: List[str] = []
            vecs: List[List[float]] = []
            metas: List[Dict[str, Any]] = []
            for case_item in self.relay_cases:
                case_name = str(case_item.get("case") or "")
                if not case_name:
                    continue
                desc = str(case_item.get("desc") or case_item.get("description") or "")
                text = f"{case_name} {desc}".strip()
                emb = self.embed_fn(text)
                if not emb:
                    continue
                ids.append(case_name)
                vecs.append(emb)
                metas.append(
                    {
                        "case": case_name,
                        "desc": desc,
                        "req_struct": case_item.get("req_struct") or {},
                        "out_struct": case_item.get("out_struct") or {},
                    }
                )
            if ids:
                self._vector_store.upsert_vectors(ids=ids, vectors=vecs, metadata=metas)
                self._vector_ready = True
        finally:
            print("_init_vector_index... done")

    def classify(
        self,
        query: str,
        long_term_nodes: Optional[List[Dict[str, Any]]] = None,
    ) -> GoalDecision:
        print("classify...")
        long_term_nodes = long_term_nodes or []
        # Stage 1: deterministic rule scoring.
        best_rule_case: Optional[Dict[str, Any]] = None
        best_rule_score = -1.0
        for item in self.relay_cases:
            s = self._rule_score(query, item)
            if s > best_rule_score:
                best_rule_score = s
                best_rule_case = item

        if best_rule_case and best_rule_score >= 0.80:
            case_name = str(best_rule_case.get("case") or "")
            result = GoalDecision(
                case_name=case_name,
                confidence=float(best_rule_score),
                source="rule",
                reason="high deterministic overlap",
                req_struct=best_rule_case.get("req_struct") or {},
                out_struct=best_rule_case.get("out_struct") or {},
                case_item=best_rule_case,
            )
            print("classify... done")
            return result

        # Stage 2: vector retrieval.
        if self._vector_ready:
            q_aug = query
            if long_term_nodes:
                context_hint = " ".join((n.get("description") or "") for n in long_term_nodes[:10])
                if context_hint:
                    q_aug = f"{query}\n{context_hint}"
            q_emb = self.embed_fn(q_aug)
            if q_emb:
                hits = self._vector_store.similarity_search(q_emb, top_k=1)
                if hits:
                    top = hits[0]
                    case_name = str((top.get("metadata") or {}).get("case") or "")
                    case_item = self._case_by_name.get(case_name, {})
                    result = GoalDecision(
                        case_name=case_name,
                        confidence=float(top.get("score") or 0.0),
                        source="vector",
                        reason="top vector similarity",
                        req_struct=(top.get("metadata") or {}).get("req_struct") or case_item.get("req_struct") or {},
                        out_struct=(top.get("metadata") or {}).get("out_struct") or case_item.get("out_struct") or {},
                        case_item=case_item if case_item else None,
                    )
                    print("classify... done")
                    return result

        # Stage 3 fallback: best rule even if low confidence.
        fallback = best_rule_case or (self.relay_cases[0] if self.relay_cases else {})
        result = GoalDecision(
            case_name=str(fallback.get("case") or ""),
            confidence=max(0.0, float(best_rule_score)),
            source="fallback",
            reason="fallback to best available case",
            req_struct=fallback.get("req_struct") or {},
            out_struct=fallback.get("out_struct") or {},
            case_item=fallback if fallback else None,
        )
        print("classify... done")
        return result

    def close(self) -> None:
        print("close...")
        self._vector_store.close()
        print("close... done")


if __name__ == "__main__":
    # Minimal workflow: BrainClassifier.classify (requires qbrain)
    cases = [{"case": "CHAT", "desc": "chat"}]
    bc = BrainClassifier(cases, embed_fn=lambda t: [0.0] * 8, use_vector=False)
    d = bc.classify("hi")
    assert d is not None and d.case_name == "CHAT"
    bc.close()
    print("[brain_classifier] ok")
