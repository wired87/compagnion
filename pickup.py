"""
Prompt: remove the collect_local_case_actions from the brain. keep logic to
use firegraph to collect the entire local codebase in the graph as defined
within the existing class StructInspector.

MCP Pickup:
  - fetches MCP tools from all configured /mcp endpoints and upserts them as
    ACTION nodes on the given GUtils graph (remote pipeline, unchanged).
  - collects the entire local codebase into the same graph by delegating to
    the firegraph StructInspector (MODULE / CLASS / METHOD / PARAM / CLASS_VAR
    nodes and their edges), so the Brain sees the live code structure without
    the previous case-accessor scanner.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from brain_schema import BrainNodeType
from brain_utils import normalize_user_id
from mcp_client import get_json, post_json_rpc

if TYPE_CHECKING:
    from graph.local_graph_utils import GUtils


# Directories skipped when walking the local codebase for StructInspector ingestion.
_SCAN_SKIP_DIRS = {
    ".git",
    ".idea",
    ".pytest_cache",
    ".venv",
    "__pycache__",
    "agent-transcripts",
    "build",
    "dist",
    "htmlcov",
    "node_modules",
    "site-packages",
    "venv",
}

# Location of firegraph.StructInspector relative to the repository root.
_FIREGRAPH_DIR = "firegraph"
_FIREGRAPH_MODULE_FILE = "graph_creator.py"


def _normalize_endpoint_url(endpoint: str) -> str:
    return str(endpoint or "").strip().rstrip("/")


def _split_endpoint_value(raw: str) -> List[str]:
    """Parse env value: JSON list or comma/semicolon/newline delimited."""
    text = (raw or "").strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            pass
    return [part.strip() for part in re.split(r"[,\n;]+", text) if part and part.strip()]


class _StructInspectorGraphAdapter:
    """
    Adapter between firegraph.StructInspector (expects `add_edge(trgt=...)`)
    and the brain-side GUtils implementation (which uses `add_edge(trt=...)`).
    Forwards every call one-to-one so StructInspector populates the live graph
    without needing any modification.
    """

    def __init__(self, gutils: "GUtils") -> None:
        self._gutils = gutils

    @property
    def G(self):
        return self._gutils.G

    def add_node(self, attrs: Dict[str, Any], flatten: bool = False) -> Any:
        return self._gutils.add_node(attrs, flatten=flatten)

    def add_edge(
        self,
        src: Any = None,
        trgt: Any = None,
        attrs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        # Translate the `trgt=` keyword to GUtils' `trt=` signature.
        return self._gutils.add_edge(src=src, trt=trgt, attrs=attrs or {}, **kwargs)


class McpPickup:
    """
    Fetches MCP tools from all configured /mcp endpoints and consumes them
    into the given GUtils instance as ACTION nodes, and uses the firegraph
    StructInspector to ingest the entire local codebase as structural nodes
    on the same graph.
    """

    def __init__(
        self,
        gutils: "GUtils",
        user_id: str = "public",
        graph_lock: Optional[threading.Lock] = None,
        poll_interval_sec: float = 20.0,
        http_timeout_sec: float = 8.0,
        log_throttle_sec: float = 30.0,
    ):
        self.gutils = gutils
        self.user_id = normalize_user_id(user_id)
        self._graph_lock = graph_lock or threading.Lock()
        self._poll_interval_sec = poll_interval_sec
        self._http_timeout_sec = http_timeout_sec
        self._log_throttle_sec = log_throttle_sec
        self._log_last_ts: Dict[str, float] = {}
        self._code_scan_root = os.path.abspath(
            os.environ.get("BRAIN_CODE_PICKUP_ROOT")
            or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        self._struct_inspector_cls: Optional[Any] = None
        print("[pickup] Initializing MCP tool collector for user scope.")

    def _log_warning(self, key: str, message: str) -> None:
        now = time.time()
        last = self._log_last_ts.get(key, 0.0)
        if now - last < self._log_throttle_sec:
            return
        self._log_last_ts[key] = now
        print(f"[pickup] ⚠ {message}")

    def collect_endpoint_urls(self) -> List[str]:
        """Collect all /mcp endpoint URLs from env (BRAIN_MCP_ENDPOINTS, MCP_EP*, etc)."""
        print("[pickup] Scanning environment for MCP endpoint URLs...")
        out: List[str] = []
        explicit_keys = ("BRAIN_MCP_ENDPOINTS", "MCP_EP")
        for key in explicit_keys:
            raw = os.environ.get(key)
            if raw is None:
                continue
            out.extend(_split_endpoint_value(str(raw)))
        for key, raw in os.environ.items():
            if not str(key).upper().startswith("MCP_EP_"):
                continue
            out.extend(_split_endpoint_value(str(raw or "")))
        if not out:
            for key, raw in os.environ.items():
                if "MCP_EP" not in str(key).upper():
                    continue
                out.extend(_split_endpoint_value(str(raw or "")))
        seen: Set[str] = set()
        unique: List[str] = []
        for ep in out:
            ep = _normalize_endpoint_url(ep)
            if ep in seen:
                continue
            low = ep.lower()
            if not (low.startswith("http://") or low.startswith("https://")):
                continue
            seen.add(ep)
            unique.append(ep)
        print(f"[pickup] Found {len(unique)} unique endpoint(s) to query.")
        return unique

    def _extract_tools_from_payload(self, payload: Any) -> List[Dict[str, Any]]:
        if payload is None:
            return []
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        candidates: List[Any] = []
        result = payload.get("result")
        if isinstance(result, dict):
            candidates.append(result.get("tools"))
        candidates.append(payload.get("tools"))
        data = payload.get("data")
        if isinstance(data, dict):
            candidates.append(data.get("tools"))
        if "name" in payload and ("inputSchema" in payload or "description" in payload):
            candidates.append([payload])
        for candidate in candidates:
            if isinstance(candidate, list):
                tools = [item for item in candidate if isinstance(item, dict)]
                if tools:
                    return tools
        return []

    def fetch_tools(self, endpoint_url: str) -> Tuple[List[Dict[str, Any]], bool]:
        """Fetch tools from one endpoint. Returns (tools, had_response)."""
        print("[pickup] Fetching tools from endpoint...")
        had_response = False
        payload = get_json(endpoint_url, timeout_sec=self._http_timeout_sec)
        if payload is None:
            self._log_warning(f"http_get::{endpoint_url}", "GET request failed.")
        else:
            had_response = True
            tools = self._extract_tools_from_payload(payload)
            if tools:
                print(f"[pickup] Retrieved {len(tools)} tool(s) via GET.")
                return tools, True
        print("[pickup] GET returned no tools, trying JSON-RPC tools/list...")
        rpc_payload = post_json_rpc(
            endpoint_url, "tools/list", {}, timeout_sec=self._http_timeout_sec
        )
        if rpc_payload is None:
            self._log_warning(f"http_post::{endpoint_url}", "POST tools/list failed.")
        else:
            had_response = True
            tools = self._extract_tools_from_payload(rpc_payload)
            if tools:
                print(f"[pickup] Retrieved {len(tools)} tool(s) via POST.")
                return tools, True
        if had_response:
            self._log_warning(f"parse::{endpoint_url}", "No MCP tools found in response.")
        print("[pickup] Endpoint yielded 0 tools.")
        return [], had_response

    # ------------------------------------------------------------------
    # Local codebase ingestion via firegraph.StructInspector
    # ------------------------------------------------------------------

    def _iter_code_scan_files(self) -> List[str]:
        """Walk the configured code root and return every .py file path."""
        files: List[str] = []
        if not os.path.isdir(self._code_scan_root):
            return files
        for root, dirs, filenames in os.walk(self._code_scan_root):
            dirs[:] = [d for d in dirs if d not in _SCAN_SKIP_DIRS and not d.startswith(".")]
            for filename in filenames:
                if filename.endswith(".py"):
                    files.append(os.path.join(root, filename))
        return files

    def _module_name_from_path(self, py_path: str) -> str:
        """Stable unique module id from a path (relative dotted notation)."""
        rel = os.path.relpath(py_path, self._code_scan_root).replace("\\", "/")
        return rel.replace("/", ".").replace(".py", "") or "root"

    def _get_struct_inspector_cls(self) -> Optional[Any]:
        """
        Load firegraph.StructInspector once and cache it. The firegraph package
        is imported via importlib to avoid mutating sys.path globally; we add
        its directory to sys.path only for the duration of the import so the
        internal `from graph.local_graph_utils import GUtils` can resolve via
        the brain-side `graph` package that is already on sys.path.
        """
        if self._struct_inspector_cls is not None:
            return self._struct_inspector_cls
        firegraph_dir = os.path.join(self._code_scan_root, _FIREGRAPH_DIR)
        module_path = os.path.join(firegraph_dir, _FIREGRAPH_MODULE_FILE)
        if not os.path.isfile(module_path):
            self._log_warning(
                "struct_inspector_missing",
                f"StructInspector not found at {module_path}; local codebase scan disabled.",
            )
            return None
        added = firegraph_dir not in sys.path
        if added:
            sys.path.insert(0, firegraph_dir)
        try:
            spec = importlib.util.spec_from_file_location(
                "brain_firegraph_graph_creator", module_path
            )
            if spec is None or spec.loader is None:
                return None
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            self._struct_inspector_cls = getattr(module, "StructInspector", None)
        except Exception as exc:
            self._log_warning(
                "struct_inspector_import",
                f"Failed to import StructInspector: {type(exc).__name__}: {exc}",
            )
            self._struct_inspector_cls = None
        finally:
            if added:
                try:
                    sys.path.remove(firegraph_dir)
                except ValueError:
                    pass
        return self._struct_inspector_cls

    def collect_local_codebase(self) -> int:
        """
        Seamless integration: delegate to firegraph.StructInspector so every
        .py file under the code root is parsed and merged into the existing
        brain graph (MODULE / CLASS / METHOD / PARAM / CLASS_VAR nodes with
        their edges). Returns the number of successfully processed modules.
        """
        StructInspector = self._get_struct_inspector_cls()
        if StructInspector is None:
            return 0
        adapter = _StructInspectorGraphAdapter(self.gutils)
        inspector = StructInspector(G=self.gutils.G, g=adapter)
        processed = 0
        for py_path in self._iter_code_scan_files():
            try:
                with open(py_path, "r", encoding="utf-8") as handle:
                    source = handle.read()
            except Exception as exc:
                self._log_warning(
                    f"code_read::{py_path}",
                    f"Skip {py_path}: read error {type(exc).__name__}",
                )
                continue
            if not source.strip():
                continue
            module_name = self._module_name_from_path(py_path)
            try:
                with self._graph_lock:
                    inspector.convert_module_to_graph(source, module_name)
                processed += 1
            except Exception as exc:
                # StructInspector is defensive internally, but any bubbled-up
                # error for a single file must not stop the full codebase scan.
                self._log_warning(
                    f"code_parse::{py_path}",
                    f"Skip {module_name}: {type(exc).__name__}",
                )
        return processed

    # ------------------------------------------------------------------
    # Remote MCP action-node pipeline
    # ------------------------------------------------------------------

    def _make_action_node(self, endpoint_url: str, tool: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build ACTION node dict with all necessary info for GUtils.add_node."""
        action_name = str(tool.get("name") or tool.get("tool") or "").strip()
        if not action_name:
            return None
        source = f"{endpoint_url}::{action_name}"
        action_id = f"ACTION::{hashlib.sha1(source.encode('utf-8')).hexdigest()[:20]}"
        input_schema = (
            tool.get("inputSchema") or tool.get("input_schema") or tool.get("schema") or {}
        )
        if not isinstance(input_schema, (dict, list)):
            input_schema = {}
        return {
            "id": action_id,
            "type": BrainNodeType.ACTION,
            "user_id": self.user_id,
            "action_name": action_name,
            "title": str(tool.get("title") or action_name),
            "description": str(tool.get("description") or ""),
            "input_schema": input_schema,
            "source_endpoint": endpoint_url,
            "updated_at": int(time.time() * 1000),
        }

    def _collect_action_ids_for_endpoint(self, endpoint_url: str) -> Set[str]:
        """Ids of ACTION nodes from this endpoint (for stale cleanup)."""
        out: Set[str] = set()
        ep_norm = _normalize_endpoint_url(endpoint_url)
        for node_id, attrs in list(self.gutils.G.nodes(data=True)):
            ntype = str(attrs.get("type") or "").upper()
            src_ep = _normalize_endpoint_url(str(attrs.get("source_endpoint") or ""))
            if ntype == BrainNodeType.ACTION and src_ep == ep_norm:
                out.add(str(node_id))
        return out

    def _remove_stale_nodes(self, node_ids: Set[str]) -> int:
        removed = 0
        if node_ids:
            print(f"[pickup] Removing {len(node_ids)} stale ACTION node(s) from graph.")
        for nid in node_ids:
            if self.gutils.G.has_node(nid):
                self.gutils.G.remove_node(nid)
                removed += 1
        return removed

    def refresh(self) -> int:
        """
        Fetch tools from all remote endpoints, upsert ACTION nodes into GUtils
        and remove stale ones. Additionally walk the local codebase through
        StructInspector to keep the code-structure subgraph in sync.
        Returns the count of upserted remote ACTION nodes.
        """
        print("[pickup] Starting refresh cycle...")
        endpoints = self.collect_endpoint_urls()
        endpoint_nodes: Dict[str, List[Dict[str, Any]]] = {}
        successful: Set[str] = set()
        if endpoints:
            max_workers = min(4, max(1, len(endpoints)))
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mcp-pickup") as pool:
                fut_to_ep = {pool.submit(self.fetch_tools, ep): ep for ep in endpoints}
                for fut in as_completed(fut_to_ep):
                    ep = fut_to_ep[fut]
                    try:
                        tools, ok = fut.result()
                    except Exception as exc:
                        self._log_warning(f"fetch::{ep}", f"Unexpected error: {type(exc).__name__}")
                        continue
                    if ok:
                        successful.add(ep)
                    nodes = []
                    for tool in tools:
                        node = self._make_action_node(ep, tool)
                        if node:
                            nodes.append(node)
                    endpoint_nodes[ep] = nodes
        else:
            print("[pickup] No MCP endpoints configured.")

        upserted = 0
        removed = 0
        with self._graph_lock:
            for ep in successful:
                nodes = endpoint_nodes.get(ep, [])
                for node in nodes:
                    self.gutils.add_node(node)
                    upserted += 1
                expected_ids = {str(n.get("id")) for n in nodes if n.get("id")}
                existing = self._collect_action_ids_for_endpoint(ep)
                stale = existing - expected_ids
                if stale:
                    removed += self._remove_stale_nodes(stale)
        if removed:
            self._log_warning("stale_cleanup", f"Removed {removed} stale ACTION node(s).")

        # Local codebase subgraph via firegraph.StructInspector.
        try:
            processed = self.collect_local_codebase()
            if processed:
                print(f"[pickup] Local codebase: {processed} module(s) ingested via StructInspector.")
        except Exception as exc:
            self._log_warning(
                "codebase_scan",
                f"Local codebase scan skipped: {type(exc).__name__}: {exc}",
            )

        print(f"[pickup] Refresh complete: {upserted} ACTION added, {removed} removed.")
        return upserted


if __name__ == "__main__":
    # Minimal workflow: refresh runs without crashing on a stub graph.
    import networkx as nx

    class _StubGUtils:
        def __init__(self):
            self.G = nx.MultiGraph()

        def add_node(self, attrs, flatten=False):
            nid = attrs.get("id")
            if nid:
                self.G.add_node(nid, **{k: v for k, v in attrs.items() if k != "id"})

        def add_edge(self, src=None, trt=None, attrs=None, **kwargs):
            attrs = attrs or {}
            if src and trt:
                self.G.add_edge(src, trt, **{k: v for k, v in attrs.items()})

    g = _StubGUtils()
    p = McpPickup(gutils=g, poll_interval_sec=999.0)
    n = p.refresh()
    assert isinstance(n, int) and n >= 0
    print(f"[pickup] Test passed. ACTION upserts={n}, total graph nodes={g.G.number_of_nodes()}")
