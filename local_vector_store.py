"""
local_vector_store.py - Lightweight in-memory vector store fallback for BrainClassifier.

Prompt:
verwende ollama mit gemma4 e2b als lokales modell (integrate in brain)
"""
from __future__ import annotations

from typing import Any, Dict, List


class LocalVectorStore:
    """Small cosine-similarity store used when qbrain.VectorStore is unavailable."""

    def __init__(self, store_name: str, db_path: str, normalize_embeddings: bool = True) -> None:
        self.store_name = store_name
        self.db_path = db_path
        self.normalize_embeddings = normalize_embeddings
        self._rows: Dict[str, Dict[str, Any]] = {}

    def create_store(self) -> None:
        return

    def upsert_vectors(
        self,
        *,
        ids: List[str],
        vectors: List[List[float]],
        metadata: List[Dict[str, Any]],
    ) -> None:
        for row_id, vector, meta in zip(ids, vectors, metadata):
            vec = self._normalize(vector)
            self._rows[str(row_id)] = {"id": str(row_id), "vector": vec, "metadata": dict(meta or {})}

    def similarity_search(self, query_vector: List[float], top_k: int = 1) -> List[Dict[str, Any]]:
        q = self._normalize(query_vector)
        if not q:
            return []
        scored: List[Dict[str, Any]] = []
        for row in self._rows.values():
            score = self._dot(q, row["vector"])
            scored.append({"id": row["id"], "score": score, "metadata": dict(row["metadata"])})
        scored.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
        return scored[: max(1, int(top_k))]

    def close(self) -> None:
        return

    def _normalize(self, vector: List[float]) -> List[float]:
        if not vector:
            return []
        values = [float(x) for x in vector]
        if not self.normalize_embeddings:
            return values
        norm = sum(v * v for v in values) ** 0.5
        if norm <= 1e-12:
            return values
        return [v / norm for v in values]

    @staticmethod
    def _dot(a: List[float], b: List[float]) -> float:
        return float(sum(x * y for x, y in zip(a, b)))
