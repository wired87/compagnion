"""
local_brain_backend.py - Local Ollama-backed Brain runtime helpers.

Prompt:
verwende ollama mit gemma4 e2b als lokales modell (integrate in brain)
"""
from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Tuple

import numpy as np


class LocalBrainBackend:
    """
    Minimal local backend for Brain when qbrain is unavailable.

    It exposes the small surface area Brain already expects:
    embeddings, optional text generation, and empty table-manager hooks.
    """

    def __init__(self) -> None:
        self.base_url = str(os.environ.get("BRAIN_OLLAMA_BASE_URL") or "http://127.0.0.1:11434").rstrip("/")
        raw_model = str(os.environ.get("BRAIN_OLLAMA_MODEL") or "gemma4:e2b").strip()
        self.model = self._normalize_model_name(raw_model)
        self.timeout_sec = self._resolve_timeout("BRAIN_OLLAMA_TIMEOUT_SEC", 20.0)
        self.db = None
        self._availability_checked = False
        self._is_available = False
        self._availability_message = "Ollama availability not checked yet."

    @staticmethod
    def _normalize_model_name(value: str) -> str:
        text = (value or "").strip()
        if " " in text and ":" not in text:
            parts = [part for part in text.split() if part]
            if len(parts) == 2:
                return f"{parts[0]}:{parts[1]}"
        return text or "gemma4:e2b"

    @staticmethod
    def _resolve_timeout(env_key: str, default: float) -> float:
        raw = str(os.environ.get(env_key) or "").strip()
        if not raw:
            return default
        try:
            parsed = float(raw)
            return parsed if parsed > 0 else default
        except Exception:
            return default

    def _request(self, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = None
        headers = {"Content-Type": "application/json"}
        method = "GET"
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            method = "POST"
        req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as response:
            raw = response.read().decode("utf-8")
        return json.loads(raw) if raw else {}

    def availability(self) -> Tuple[bool, str]:
        if self._availability_checked:
            return self._is_available, self._availability_message
        try:
            payload = self._request("/api/tags")
            models = payload.get("models") if isinstance(payload, dict) else None
            self._is_available = True
            self._availability_message = (
                f"Ollama reachable at {self.base_url} with model {self.model}."
                if isinstance(models, list)
                else f"Ollama reachable at {self.base_url}."
            )
        except urllib.error.URLError as exc:
            self._is_available = False
            self._availability_message = f"Ollama unavailable at {self.base_url}: {exc.reason}"
        except Exception as exc:
            self._is_available = False
            self._availability_message = f"Ollama check failed: {exc}"
        self._availability_checked = True
        return self._is_available, self._availability_message

    def _fallback_embedding(self, text: str, dim: int = 128) -> List[float]:
        digest = hashlib.sha256((text or "").encode("utf-8")).digest()
        seed = int.from_bytes(digest[:8], byteorder="big", signed=False)
        rng = np.random.default_rng(seed)
        vec = rng.standard_normal(dim).astype(np.float32)
        norm = np.linalg.norm(vec)
        if norm > 1e-8:
            vec = vec / norm
        return vec.tolist()

    def _generate_embedding(self, text: str) -> List[float]:
        ok, _ = self.availability()
        if not ok:
            return self._fallback_embedding(text)
        try:
            payload = self._request("/api/embed", {"model": self.model, "input": text or ""})
            embeddings = payload.get("embeddings") if isinstance(payload, dict) else None
            if isinstance(embeddings, list) and embeddings:
                first = embeddings[0] if isinstance(embeddings[0], list) else embeddings
                if isinstance(first, list) and first:
                    return [float(x) for x in first]
        except Exception:
            pass
        return self._fallback_embedding(text)

    def generate_text(self, prompt: str, *, system: str = "") -> str:
        ok, message = self.availability()
        if not ok:
            return f"Local model unavailable. {message}"
        try:
            payload = {
                "model": self.model,
                "prompt": prompt,
                "system": system,
                "stream": False,
            }
            response = self._request("/api/generate", payload)
            text = str(response.get("response") or "").strip()
            return text or f"Model {self.model} returned an empty response."
        except Exception as exc:
            return f"Local model call failed: {exc}"

    # ----- qbrain-compatible no-op hooks -----

    def get_managers_info(self) -> List[Dict[str, Any]]:
        return []

    def run_query(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        return []

    def _table_ref(self, table_name: str) -> str:
        return str(table_name)
