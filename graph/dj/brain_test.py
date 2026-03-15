import json
import traceback
from copy import deepcopy
from typing import Any, Dict, List

from asgiref.sync import async_to_sync
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views import View

from qbrain.graph.brn.brain import Brain


def _mock_case_callable(case_name: str):
    async def _runner(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ok": True,
            "case": case_name,
            "received_payload": payload,
            "message": f"Callable for {case_name} executed.",
        }

    return _runner


def _ensure_callable_case_struct(case_struct: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in case_struct:
        ci = dict(item)
        case_name = str(ci.get("case") or "UNKNOWN_CASE")
        if not callable(ci.get("func")):
            ci["func"] = _mock_case_callable(case_name)
        out.append(ci)
    return out


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
        },
        {
            "case": "SET_ENV",
            "desc": "Create or update environment settings.",
            "req_struct": {"data": {"id": "string", "description": "string"}},
            "out_struct": {"status": "string"},
        },
        {
            "case": "SET_METHOD",
            "desc": "Register a computational method.",
            "req_struct": {"data": {"equation": "string", "description": "string"}},
            "out_struct": {"status": "string", "method_id": "string"},
        },
    ]


class BrainTestView(View):
    """
    Minimal test chat interface for Brain.

    GET  -> HTML page
    POST -> JSON API for single-turn run or suite run
    """

    def get(self, request: HttpRequest) -> HttpResponse:
        html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Brain Test Chat</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; max-width: 980px; }
    textarea, input { width: 100%; box-sizing: border-box; margin-bottom: 10px; }
    textarea { min-height: 120px; }
    .chat { border: 1px solid #ddd; border-radius: 8px; padding: 12px; min-height: 220px; background: #fafafa; }
    .msg { margin: 8px 0; padding: 8px; border-radius: 6px; }
    .user { background: #e8f4ff; }
    .assistant { background: #f2f2f2; }
    .row { display: flex; gap: 10px; }
    .row > * { flex: 1; }
    button { margin-right: 8px; padding: 8px 12px; }
    .small { font-size: 12px; color: #666; }
  </style>
</head>
<body>
  <h2>Brain Test Chat (Minimal)</h2>
  <p class="small">Provide relay_case_struct JSON, then chat. The backend replays history each request so classification + req_struct filling can be tested end-to-end.</p>

  <label>User ID</label>
  <input id="userId" value="test_user_1" />

  <label>Relay Case Struct JSON</label>
  <textarea id="caseStruct"></textarea>

  <div class="row">
    <div>
      <label>User Message</label>
      <input id="message" placeholder="e.g. create a parameter" />
    </div>
    <div>
      <label>Payload JSON (optional)</label>
      <input id="payload" placeholder='{"name":"alpha"}' />
    </div>
  </div>

  <div style="margin-bottom: 12px;">
    <button onclick="sendMsg()">Send</button>
    <button onclick="runSuite()">Run Wide Test Suite</button>
    <button onclick="clearChat()">Clear</button>
  </div>

  <div id="chat" class="chat"></div>
  <pre id="debug"></pre>

<script>
const defaultCaseStruct = [
  {
    case: "SET_PARAM",
    desc: "Create or update a simulation parameter.",
    req_struct: { data: { name: "string", param_type: "string", description: "string" } },
    out_struct: { status: "string", param_id: "string" }
  },
  {
    case: "SET_ENV",
    desc: "Create or update environment settings.",
    req_struct: { data: { id: "string", description: "string" } },
    out_struct: { status: "string" }
  },
  {
    case: "SET_METHOD",
    desc: "Register a computational method.",
    req_struct: { data: { equation: "string", description: "string" } },
    out_struct: { status: "string", method_id: "string" }
  }
];

let history = [];
document.getElementById("caseStruct").value = JSON.stringify(defaultCaseStruct, null, 2);

function render(role, text) {
  const el = document.createElement("div");
  el.className = "msg " + (role === "user" ? "user" : "assistant");
  el.textContent = (role === "user" ? "You: " : "Brain: ") + text;
  document.getElementById("chat").appendChild(el);
}

function clearChat() {
  history = [];
  document.getElementById("chat").innerHTML = "";
  document.getElementById("debug").textContent = "";
}

async function sendMsg() {
  const userId = document.getElementById("userId").value.trim();
  const message = document.getElementById("message").value.trim();
  if (!message) return;

  let caseStruct;
  try { caseStruct = JSON.parse(document.getElementById("caseStruct").value); }
  catch (e) { alert("Invalid case struct JSON"); return; }

  let payload = {};
  const rawPayload = document.getElementById("payload").value.trim();
  if (rawPayload) {
    try { payload = JSON.parse(rawPayload); }
    catch (e) { alert("Invalid payload JSON"); return; }
  }

  render("user", message);
  history.push(message);

  const resp = await fetch(window.location.pathname, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      action: "chat",
      user_id: userId,
      case_struct: caseStruct,
      history: history.slice(0, -1),
      query: message,
      payload
    })
  });
  const data = await resp.json();
  render("assistant", data.result?.next_message || JSON.stringify(data.result || data));
  document.getElementById("debug").textContent = JSON.stringify(data, null, 2);
}

async function runSuite() {
  const userId = document.getElementById("userId").value.trim();
  let caseStruct;
  try { caseStruct = JSON.parse(document.getElementById("caseStruct").value); }
  catch (e) { alert("Invalid case struct JSON"); return; }

  const resp = await fetch(window.location.pathname, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      action: "suite",
      user_id: userId,
      case_struct: caseStruct
    })
  });
  const data = await resp.json();
  clearChat();
  (data.suite_results || []).forEach(item => {
    render("user", item.query);
    render("assistant", (item.result && item.result.next_message) || JSON.stringify(item.result || {}));
  });
  document.getElementById("debug").textContent = JSON.stringify(data, null, 2);
}
</script>
</body>
</html>
        """
        return HttpResponse(html)

    def post(self, request: HttpRequest) -> JsonResponse:
        try:
            body = json.loads(request.body.decode("utf-8") or "{}")
            action = str(body.get("action") or "chat").lower()
            user_id = str(body.get("user_id") or "test_user_1")
            input_case_struct = body.get("case_struct")
            if not isinstance(input_case_struct, list):
                input_case_struct = _default_case_struct()

            case_struct = _ensure_callable_case_struct(deepcopy(input_case_struct))
            history = body.get("history") or []
            query = str(body.get("query") or "")
            payload = body.get("payload") or {}

            brain = Brain(
                user_id=user_id,
                case_struct=case_struct,
                use_vector=False,
            )
            try:
                # Optional hydration; keep robust if DB is not ready.
                try:
                    brain.hydrate_user_context()
                except Exception:
                    pass

                if action == "suite":
                    suite_inputs = [
                        ("create a parameter", {}),
                        ("name: alpha", {}),
                        ("param_type: float", {}),
                        ("description: learning rate for optimizer", {}),
                        ("create env", {"id": "env_test_1", "description": "test environment"}),
                        ("add a method", {"equation": "y=x+1", "description": "simple linear rule"}),
                    ]
                    suite_results = []
                    for q, p in suite_inputs:
                        result = async_to_sync(brain.execute_or_ask)(q, user_payload=p)
                        suite_results.append({"query": q, "payload": p, "result": result})
                    return JsonResponse({"ok": True, "suite_results": suite_results})

                for old_q in history:
                    async_to_sync(brain.execute_or_ask)(str(old_q), user_payload={})

                result = async_to_sync(brain.execute_or_ask)(query, user_payload=payload)
                return JsonResponse({"ok": True, "result": result})
            finally:
                brain.close()

        except Exception as exc:
            return JsonResponse(
                {
                    "ok": False,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                },
                status=500,
            )

