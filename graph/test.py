from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Ensure project root is importable when running "python graph/test.py".
_THIS_FILE = Path(__file__).resolve()
_PROJECT_ROOT = _THIS_FILE.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from qbrain.graph.brn.brain import Brain
from qbrain.graph.brn.brain_classifier import BrainClassifier


console = Console()


@dataclass
class QueryRun:
    index: int
    query: str
    payload: Dict[str, Any]
    status: str
    goal_case: str
    duration_ms: int
    missing_fields: List[str]
    next_message: str
    execution_debug: Dict[str, Any]
    error: str = ""


def _mock_case_callable(case_name: str):
    async def _runner(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ok": True,
            "case": case_name,
            "received_payload": payload,
            "message": f"Callable for {case_name} executed.",
        }

    return _runner


def _default_case_struct() -> List[Dict[str, Any]]:
    return [
        {
            "case": "SET_PARAM",
            "desc": "Create or update a simulation parameter.",
            "req_struct": {
                "data": {
                    "name": "string",
                    "param_type": "string",
                    "description": "string",
                }
            },
            "out_struct": {"status": "string", "param_id": "string"},
            "func": _mock_case_callable("SET_PARAM"),
        },
        {
            "case": "SET_ENV",
            "desc": "Create or update environment settings.",
            "req_struct": {"data": {"id": "string", "description": "string"}},
            "out_struct": {"status": "string"},
            "func": _mock_case_callable("SET_ENV"),
        },
        {
            "case": "SET_METHOD",
            "desc": "Register a computational method.",
            "req_struct": {"data": {"equation": "string", "description": "string"}},
            "out_struct": {"status": "string", "method_id": "string"},
            "func": _mock_case_callable("SET_METHOD"),
        },
    ]


def _load_case_struct(path: Optional[str]) -> List[Dict[str, Any]]:
    if not path:
        return _default_case_struct()
    p = Path(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("case_struct file must contain a JSON list.")
    out: List[Dict[str, Any]] = []
    for item in raw:
        ci = dict(item)
        if not callable(ci.get("func")):
            ci["func"] = _mock_case_callable(str(ci.get("case") or "UNKNOWN_CASE"))
        out.append(ci)
    return out


def _default_suite() -> List[Dict[str, Any]]:
    return [
        {"query": "create a parameter", "payload": {}},
        {"query": "name: alpha", "payload": {}},
        {"query": "param_type: float", "payload": {}},
        {"query": "description: learning rate for optimizer", "payload": {}},
        {"query": "create env", "payload": {"id": "env_test_1", "description": "test environment"}},
        {"query": "add a method", "payload": {"equation": "y=x+1", "description": "simple linear rule"}},
    ]


def _render_result_table(results: List[QueryRun]) -> None:
    table = Table(title="Brain Test Results", show_lines=False)
    table.add_column("#", style="cyan", justify="right")
    table.add_column("Status", style="bold")
    table.add_column("Goal", style="magenta")
    table.add_column("Duration (ms)", justify="right")
    table.add_column("Missing")
    table.add_column("Query")

    for r in results:
        status_color = "green" if r.status == "executed" else ("yellow" if r.status == "need_data" else "red")
        missing = ", ".join(r.missing_fields) if r.missing_fields else "-"
        table.add_row(
            str(r.index),
            f"[{status_color}]{r.status}[/{status_color}]",
            r.goal_case or "-",
            str(r.duration_ms),
            missing,
            r.query,
        )
    console.print(table)


async def _run_single(brain: Brain, query: str, payload: Dict[str, Any], idx: int) -> QueryRun:
    started = time.perf_counter()
    try:
        result = await brain.execute_or_ask(query, user_payload=payload)
        duration_ms = int((time.perf_counter() - started) * 1000)
        return QueryRun(
            index=idx,
            query=query,
            payload=payload,
            status=str(result.get("status") or "unknown"),
            goal_case=str(result.get("goal_case") or ""),
            duration_ms=duration_ms,
            missing_fields=list(result.get("missing_fields") or []),
            next_message=str(result.get("next_message") or ""),
            execution_debug=dict(result.get("execution_debug") or {}),
        )
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return QueryRun(
            index=idx,
            query=query,
            payload=payload,
            status="error",
            goal_case="",
            duration_ms=duration_ms,
            missing_fields=[],
            next_message="Execution failed.",
            execution_debug={},
            error=f"{exc}\n{traceback.format_exc()}",
        )


def _save_report(
    output_dir: Path,
    user_id: str,
    case_struct: List[Dict[str, Any]],
    results: List[QueryRun],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"brain_test_{user_id}_{ts}.json"

    success = sum(1 for r in results if r.status == "executed")
    need_data = sum(1 for r in results if r.status == "need_data")
    errors = sum(1 for r in results if r.status == "error")
    avg_ms = int(sum(r.duration_ms for r in results) / max(1, len(results)))

    report = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "user_id": user_id,
        "summary": {
            "total_queries": len(results),
            "executed": success,
            "need_data": need_data,
            "errors": errors,
            "avg_duration_ms": avg_ms,
            "overall_status": "success" if errors == 0 else "failure",
        },
        "case_struct": [
            {k: v for k, v in c.items() if k != "func"} for c in case_struct
        ],
        "qa_pairs": [asdict(r) for r in results],
    }
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report_path


async def _run_suite(brain: Brain, suite: List[Dict[str, Any]]) -> List[QueryRun]:
    results: List[QueryRun] = []
    for i, item in enumerate(suite, start=1):
        q = str(item.get("query") or "")
        p = dict(item.get("payload") or {})
        run = await _run_single(brain, q, p, i)
        results.append(run)
        console.print(
            f"[bold]Q{i}[/bold] {q}\n"
            f" -> status=[{('green' if run.status == 'executed' else 'yellow' if run.status == 'need_data' else 'red')}]"
            f"{run.status}[/], goal={run.goal_case or '-'}, duration={run.duration_ms}ms"
        )
        if run.status == "error":
            console.print(Panel(run.error or "Unknown error", title="Error", border_style="red"))
    return results


async def _run_interactive(brain: Brain, max_turns: int = 30) -> List[QueryRun]:
    results: List[QueryRun] = []
    for idx in range(1, max_turns + 1):
        query = console.input("[bold cyan]You[/bold cyan] (or 'exit'): ").strip()
        if query.lower() in {"exit", "quit"}:
            break

        raw_payload = console.input("[dim]Payload JSON (optional)[/dim]: ").strip()
        payload: Dict[str, Any] = {}
        if raw_payload:
            try:
                payload = json.loads(raw_payload)
            except Exception as exc:
                console.print(f"[red]Invalid payload JSON:[/red] {exc}")
                payload = {}

        run = await _run_single(brain, query, payload, idx)
        results.append(run)
        color = "green" if run.status == "executed" else ("yellow" if run.status == "need_data" else "red")
        console.print(
            f"[bold]{idx}[/bold] status=[{color}]{run.status}[/] "
            f"goal={run.goal_case or '-'} duration={run.duration_ms}ms"
        )
        console.print(Panel(run.next_message or "(no message)", title="Brain"))
        if run.status == "error" and run.error:
            console.print(Panel(run.error, title="Error", border_style="red"))
    return results


async def main() -> None:
    parser = argparse.ArgumentParser(description="Terminal-based Brain test harness.")
    parser.add_argument("--user-id", default="test_user_1")
    parser.add_argument("--mode", choices=["suite", "interactive", "both"], default="suite")
    parser.add_argument("--case-struct", default="", help="Path to relay case struct JSON file.")
    parser.add_argument(
        "--output-dir",
        default="graph/test_runs",
        help="Where JSON test reports are saved and vector DB is stored.",
    )
    parser.add_argument(
        "--expensive",
        action="store_true",
        help="Run an expensive Brain test: build vector index over cases and run an extended query suite.",
    )
    args = parser.parse_args()

    case_struct = _load_case_struct(args.case_struct or None)
    output_dir = Path(args.output_dir)

    results: List[QueryRun] = []

    # --- Brain creation + model (classifier) generation ---
    # Use configured deep-research backend (if any) just as an identifier.
    dr_backend = os.environ.get("DEEP_RESEARCH_BACKEND", "chatgpt").strip().lower()
    brain = Brain(
        dr_backend,
        args.user_id,
    )

    # Attach a BrainClassifier instance to the Brain. This is intentionally
    # "expensive": it builds a vector index over all relay cases using the
    # Brain's embedding function and a DuckDB-backed VectorStore.
    vector_db_path = str(output_dir / "brain_cases_test.duckdb")
    brain.classifier = BrainClassifier(
        relay_cases=case_struct,
        embed_fn=brain._embed_text,
        vector_db_path=vector_db_path,
        use_vector=True,
    )

    try:
        try:
            hydrated = brain.hydrate_user_context()
            console.print(f"[dim]Hydrated long-term nodes: {hydrated}[/dim]")
        except Exception as exc:
            # Do not abort test on hydration issues; continue with stable local graph flow.
            console.print(f"[yellow]Hydration skipped due to warning:[/yellow] {exc}")

        if args.mode in {"suite", "both"}:
            console.print(Panel("Running suite mode", style="bold blue"))
            suite = _default_suite()
            # In "expensive" mode, scale the suite by repeating it several times
            # to stress-test Brain creation + model + query pipeline.
            if args.expensive:
                console.print(Panel("Expensive mode enabled: repeating suite 5x", style="bold red"))
                suite = suite * 5
            results.extend(await _run_suite(brain, suite))

        if args.mode in {"interactive", "both"}:
            console.print(Panel("Running interactive mode", style="bold blue"))
            results.extend(await _run_interactive(brain))

    finally:
        brain.close()

    _render_result_table(results)
    report_path = _save_report(output_dir, args.user_id, case_struct, results)
    console.print(f"[bold green]Report saved:[/bold green] {report_path}")


if __name__ == "__main__":
    asyncio.run(main())

