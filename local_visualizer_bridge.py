"""
local_visualizer_bridge.py - Persist incoming Brain data in DuckDB and trigger local visualizer renders.

Prompt:
connect to visualizer py package and save the incomming data in a local duckdb
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Dict

try:
    import duckdb
except Exception:
    duckdb = None


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT / "brain_incoming.duckdb"
DEFAULT_VISUAL_DIR = ROOT / "docs" / "runtime_visuals"


class LocalVisualizerBridge:
    """Store runtime payloads locally and render visualizer artifacts when possible."""

    def __init__(self, *, gutils: Any, user_id: str) -> None:
        self.gutils = gutils
        self.user_id = str(user_id)
        self.db_path = Path(str(os.environ.get("BRAIN_INCOMING_DB_PATH") or DEFAULT_DB_PATH))
        self.visual_dir = Path(str(os.environ.get("BRAIN_VISUALIZER_OUTPUT_DIR") or DEFAULT_VISUAL_DIR))
        self._db: Any | None = None
        self._visual_module: ModuleType | None = None
        self._visual_import_error: str = ""
        self._ensure_db()

    def close(self) -> None:
        if self._db is None:
            return
        try:
            self._db.close()
        except Exception:
            pass
        finally:
            self._db = None

    def persist_event(
        self,
        *,
        source_kind: str,
        payload: Any,
        request_id: str | None = None,
        content_type: str = "application/json",
        render_visual: bool = False,
    ) -> Dict[str, Any]:
        """Persist one incoming payload and return storage/render metadata."""
        created_at_ms = int(time.time() * 1000)
        payload_json = self._to_json(payload)
        summary_json = self._build_summary_json(payload=payload, source_kind=source_kind)
        event_id = self._build_event_id(
            source_kind=source_kind,
            request_id=request_id,
            created_at_ms=created_at_ms,
            payload_json=payload_json,
        )

        visualizer_status = "skipped"
        visualizer_artifact_path = ""
        if render_visual:
            visualizer_status, visualizer_artifact_path = self._render_visual_snapshot(
                source_kind=source_kind,
                event_id=event_id,
            )

        if self._db is not None:
            self._db.execute(
                """
                INSERT INTO brain_incoming_events (
                    event_id,
                    created_at_ms,
                    user_id,
                    source_kind,
                    content_type,
                    request_id,
                    summary_json,
                    payload_json,
                    visualizer_status,
                    visualizer_artifact_path
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    event_id,
                    created_at_ms,
                    self.user_id,
                    str(source_kind),
                    str(content_type),
                    str(request_id or ""),
                    summary_json,
                    payload_json,
                    visualizer_status,
                    visualizer_artifact_path,
                ],
            )

        return {
            "event_id": event_id,
            "db_path": str(self.db_path),
            "source_kind": str(source_kind),
            "content_type": str(content_type),
            "visualizer_status": visualizer_status,
            "visualizer_artifact_path": visualizer_artifact_path,
        }

    def _ensure_db(self) -> None:
        if duckdb is None:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = duckdb.connect(str(self.db_path))
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS brain_incoming_events (
                event_id TEXT PRIMARY KEY,
                created_at_ms BIGINT,
                user_id TEXT,
                source_kind TEXT,
                content_type TEXT,
                request_id TEXT,
                summary_json TEXT,
                payload_json TEXT,
                visualizer_status TEXT,
                visualizer_artifact_path TEXT
            )
            """
        )

    def _build_event_id(
        self,
        *,
        source_kind: str,
        request_id: str | None,
        created_at_ms: int,
        payload_json: str,
    ) -> str:
        digest = hashlib.sha256(
            "|".join(
                [
                    self.user_id,
                    str(source_kind),
                    str(request_id or ""),
                    str(created_at_ms),
                    payload_json,
                ]
            ).encode("utf-8")
        ).hexdigest()
        return f"evt_{digest[:24]}"

    def _build_summary_json(self, *, payload: Any, source_kind: str) -> str:
        summary: Dict[str, Any] = {
            "source_kind": str(source_kind),
            "payload_type": type(payload).__name__,
        }
        if isinstance(payload, dict):
            summary["top_level_keys"] = sorted(str(key) for key in payload.keys())[:24]
            for key in ("status", "goal_case", "kind", "module_id", "content_type", "source_file"):
                value = payload.get(key)
                if value not in (None, "", [], {}):
                    summary[key] = value
        elif isinstance(payload, list):
            summary["items"] = len(payload)
        else:
            summary["preview"] = str(payload)[:240]
        return self._to_json(summary)

    def _render_visual_snapshot(self, *, source_kind: str, event_id: str) -> tuple[str, str]:
        module = self._load_visual_module()
        if module is None:
            reason = self._visual_import_error or "visual.py unavailable"
            return (f"unavailable: {reason}", "")

        render_live_graph = getattr(module, "render_live_graph", None)
        if not callable(render_live_graph):
            return ("unavailable: render_live_graph missing", "")

        try:
            self.visual_dir.mkdir(parents=True, exist_ok=True)
            out_path = self.visual_dir / f"{source_kind}_{event_id}.png"
            saved_path = render_live_graph(self.gutils, out_path=out_path)
            return ("rendered", str(saved_path))
        except Exception as exc:
            return (f"render_error: {exc}", "")

    def _load_visual_module(self) -> ModuleType | None:
        if self._visual_module is not None:
            return self._visual_module

        visual_path = ROOT / "visual.py"
        if not visual_path.exists():
            self._visual_import_error = f"missing file: {visual_path}"
            return None

        try:
            spec = importlib.util.spec_from_file_location("dr_tens_runtime_visual", visual_path)
            if spec is None or spec.loader is None:
                self._visual_import_error = "import spec could not be created"
                return None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self._visual_module = module
            return module
        except Exception as exc:
            self._visual_import_error = str(exc)
            return None

    @staticmethod
    def _to_json(value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=True, default=str)
        except Exception:
            return json.dumps({"unserializable_repr": str(value)}, ensure_ascii=True)
