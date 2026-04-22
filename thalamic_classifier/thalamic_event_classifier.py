"""
ThalamicEventClassifier: Unified classification facade.

Tries BrainClassifier (rule + vector) first for speed and determinism.
Falls back to AIChatClassifier (Gem LLM) when confidence is low.

Every method follows: print("method_name...") at start, print("method_name... done") at end.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from brain_classifier import BrainClassifier
from brain_schema import GoalDecision


# ---- Configuration ----

# Minimum confidence for rule/vector result to be accepted without LLM fallback.
DEFAULT_MIN_CONFIDENCE = float(os.environ.get("THALAMIC_MIN_CONFIDENCE", "0.65"))

# Vector store path for BrainClassifier.
DEFAULT_VECTOR_DB_PATH = os.environ.get("THALAMIC_VECTOR_DB_PATH", "brain_cases.duckdb")


@dataclass
class ThalamicEvent:
    """Structured result from thalamic event classification."""

    case_name: str
    confidence: float
    source: str  # "rule" | "vector" | "fallback" | "llm"
    reason: str
    req_struct: Dict[str, Any]
    out_struct: Dict[str, Any]


# ---- ThalamicEventClassifier ----

class ThalamicEventClassifier:
    """
    Facade that unifies BrainClassifier (rule + vector) with AIChatClassifier (LLM).
    Uses rule/vector first for speed; falls back to LLM when confidence is low.
    """

    def __init__(
        self,
        relay_cases: List[Dict[str, Any]],
        embed_fn: Callable[[str], List[float]],
        ai_chat_classifier: Any,
        *,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        vector_db_path: str = DEFAULT_VECTOR_DB_PATH,
        use_vector: bool = True,
    ):
        """
        Args:
            relay_cases: List of relay case dicts (RELAY_CASES_CONFIG).
            embed_fn: Function to embed text to vector (e.g. Brain._embed_text).
            ai_chat_classifier: AIChatClassifier instance for LLM fallback.
            min_confidence: Minimum confidence to accept rule/vector result.
            vector_db_path: Path for BrainClassifier vector store.
            use_vector: Whether BrainClassifier uses vector search.
        """
        print("ThalamicEventClassifier.__init__...")
        self.relay_cases = relay_cases or []
        self.embed_fn = embed_fn
        self.ai_chat_classifier = ai_chat_classifier
        self.min_confidence = min_confidence
        self._brain_classifier: Optional[BrainClassifier] = None
        self._vector_db_path = vector_db_path
        self._use_vector = use_vector
        print("ThalamicEventClassifier.__init__... done")

    def _get_brain_classifier(self) -> BrainClassifier:
        """Lazy init BrainClassifier to avoid startup cost."""
        print("_get_brain_classifier...")
        if self._brain_classifier is None:
            self._brain_classifier = BrainClassifier(
                relay_cases=self.relay_cases,
                embed_fn=self.embed_fn,
                vector_db_path=self._vector_db_path,
                use_vector=self._use_vector,
            )
        print("_get_brain_classifier... done")
        return self._brain_classifier

    def classify(
        self,
        user_id: str,
        user_input: str,
        long_term_nodes: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[ThalamicEvent]:
        """
        Classify user input to a thalamic event (relay case).

        Tries BrainClassifier first; falls back to AIChatClassifier when
        confidence is below min_confidence.

        Returns:
            ThalamicEvent with case_name, confidence, source, etc., or None if classification fails.
        """
        print("classify...")
        if not (user_input or "").strip():
            print("classify... done")
            return None

        # ---- Stage 1: BrainClassifier (rule + vector) ----
        try:
            bc = self._get_brain_classifier()
            decision = bc.classify(
                query=(user_input or "").strip(),
                long_term_nodes=long_term_nodes or [],
            )
            if decision and decision.confidence >= self.min_confidence:
                event = ThalamicEvent(
                    case_name=decision.case_name,
                    confidence=decision.confidence,
                    source=decision.source,
                    reason=decision.reason,
                    req_struct=decision.req_struct or {},
                    out_struct=decision.out_struct or {},
                )
                print("classify... done")
                return event
        except Exception as e:
            print(f"[ThalamicEventClassifier] BrainClassifier error: {e}")

        # ---- Stage 2: AIChatClassifier (LLM) fallback ----
        try:
            case_name = self.ai_chat_classifier.main(user_id, user_input)
            if case_name:
                # Normalize: extract case name if LLM returned extra text.
                case_name = self._extract_case_name(case_name)
                if case_name:
                    case_item = next(
                        (c for c in self.relay_cases if (c.get("case") or "") == case_name),
                        {},
                    )
                    event = ThalamicEvent(
                        case_name=case_name,
                        confidence=0.0,
                        source="llm",
                        reason="AIChatClassifier fallback",
                        req_struct=case_item.get("req_struct") or {},
                        out_struct=case_item.get("out_struct") or {},
                    )
                    print("classify... done")
                    return event
        except Exception as e:
            print(f"[ThalamicEventClassifier] AIChatClassifier error: {e}")

        # ---- Fallback: best rule result even if low confidence ----
        try:
            bc = self._get_brain_classifier()
            decision = bc.classify(
                query=(user_input or "").strip(),
                long_term_nodes=long_term_nodes or [],
            )
            if decision and decision.case_name:
                event = ThalamicEvent(
                    case_name=decision.case_name,
                    confidence=decision.confidence,
                    source="fallback",
                    reason="low-confidence rule/vector result",
                    req_struct=decision.req_struct or {},
                    out_struct=decision.out_struct or {},
                )
                print("classify... done")
                return event
        except Exception:
            pass

        print("classify... done")
        return None

    def _extract_case_name(self, raw: str) -> Optional[str]:
        """Extract relay case name from LLM output (may include extra text)."""
        print("_extract_case_name...")
        raw = (raw or "").strip()
        if not raw:
            print("_extract_case_name... done")
            return None
        case_names = {str(c.get("case") or "").strip() for c in self.relay_cases if c.get("case")}
        # Exact match.
        if raw in case_names:
            print("_extract_case_name... done")
            return raw
        # Case name at start of string.
        for cn in case_names:
            if raw.startswith(cn) or cn in raw.split():
                print("_extract_case_name... done")
                return cn
        # First token might be case name.
        first = raw.split()[0] if raw.split() else raw
        if first in case_names:
            print("_extract_case_name... done")
            return first
        # Substring match: raw may contain case name.
        for cn in case_names:
            if cn in raw:
                print("_extract_case_name... done")
                return cn
        print("_extract_case_name... done")
        return None

    def main(self, user_id: str, user_input: str) -> Optional[str]:
        """
        Thalamus-compatible entry: returns case_name string or None.
        """
        print("main...")
        event = self.classify(user_id=user_id, user_input=user_input)
        result = event.case_name if event else None
        print("main... done")
        return result

    def close(self) -> None:
        """Release resources (e.g. vector store)."""
        print("close...")
        if self._brain_classifier is not None:
            try:
                self._brain_classifier.close()
            except Exception:
                pass
            self._brain_classifier = None
        print("close... done")
