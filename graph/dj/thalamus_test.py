import json
import traceback
from typing import Any, Dict, Tuple

from asgiref.sync import async_to_sync
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views import View

# In-memory cache so a single browser session behaves like a chat window.
# Key: (user_id, session_id) -> Thalamus instance
_ORCH_CACHE: Dict[Tuple[str, str], Any] = {}


def _session_key(user_id: str, session_id: str) -> Tuple[str, str]:
    return str(user_id or "public"), str(session_id or "1")


class ThalamusTestView(View):
    """
    Minimal chat window for Thalamus (Orchestrator).

    Each user message is sent to Thalamus.handle_relay_payload as a relay payload.
    We keep an in-memory Thalamus instance per (user_id, session_id) so state,
    goal structs, and conversation history persist across messages.
    """

    def get(self, request: HttpRequest) -> HttpResponse:
        html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Thalamus Test Chat</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; max-width: 980px; }
    textarea, input { width: 100%; box-sizing: border-box; margin-bottom: 10px; }
    .chat { border: 1px solid #ddd; border-radius: 8px; padding: 12px; min-height: 260px; background: #fafafa; }
    .msg { margin: 8px 0; padding: 8px; border-radius: 6px; white-space: pre-wrap; }
    .user { background: #e8f4ff; }
    .assistant { background: #f2f2f2; }
    .row { display: flex; gap: 10px; }
    .row > * { flex: 1; }
    button { margin-right: 8px; padding: 8px 12px; }
    .small { font-size: 12px; color: #666; }
  </style>
</head>
<body>
  <h2>Thalamus Test Chat (Interactive)</h2>
  <p class="small">
    Each message is sent to <code>Thalamus.handle_relay_payload</code> as a Relay payload.
    The backend keeps a Thalamus instance per (user_id, session_id) so this behaves like a chat window.
  </p>

  <div class="row">
    <div>
      <label>User ID</label>
      <input id="userId" value="test_user_1" />
    </div>
    <div>
      <label>Session ID</label>
      <input id="sessionId" value="1" />
    </div>
  </div>

  <label>User Message</label>
  <input id="message" placeholder="e.g. create field with param 1,2,3 ..." />

  <div style="margin-bottom: 12px;">
    <button onclick="sendMsg()">Send</button>
    <button onclick="resetChat()">Reset (new Thalamus)</button>
    <button onclick="clearUi()">Clear UI</button>
  </div>

  <div id="chat" class="chat"></div>
  <pre id="debug"></pre>

<script>
function render(role, text) {
  const el = document.createElement("div");
  el.className = "msg " + (role === "user" ? "user" : "assistant");
  el.textContent = (role === "user" ? "You: " : "Thalamus: ") + (text || "");
  document.getElementById("chat").appendChild(el);
}

function clearUi() {
  document.getElementById("chat").innerHTML = "";
  document.getElementById("debug").textContent = "";
}

async function resetChat() {
  const userId = document.getElementById("userId").value.trim() || "public";
  const sessionId = document.getElementById("sessionId").value.trim() || "1";

  const resp = await fetch(window.location.pathname, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "reset", user_id: userId, session_id: sessionId })
  });
  const data = await resp.json();
  render("assistant", data?.message || "reset done");
  document.getElementById("debug").textContent = JSON.stringify(data, null, 2);
}

async function sendMsg() {
  const userId = document.getElementById("userId").value.trim() || "public";
  const sessionId = document.getElementById("sessionId").value.trim() || "1";
  const message = document.getElementById("message").value.trim();
  if (!message) return;

  render("user", message);

  const resp = await fetch(window.location.pathname, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "chat", user_id: userId, session_id: sessionId, query: message })
  });
  const data = await resp.json();
  const textOut = data?.assistant_message || JSON.stringify(data?.result || data);
  render("assistant", textOut);
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
            user_id = str(body.get("user_id") or "public")
            session_id = str(body.get("session_id") or "1")
            query = str(body.get("query") or "")

            key = _session_key(user_id, session_id)

            if action == "reset":
                if key in _ORCH_CACHE:
                    _ORCH_CACHE.pop(key, None)
                return JsonResponse({"ok": True, "message": f"Reset Thalamus for user_id={user_id}, session_id={session_id}."})

            if action != "chat":
                return JsonResponse({"ok": False, "error": f"Unknown action: {action}"}, status=400)

            if key not in _ORCH_CACHE:
                # For interactive testing, keep init lightweight: skip full graph build and equation parsing.
                from qbrain.core.orchestrator_manager.orchestrator import Thalamus
                from qbrain.predefined_case import RELAY_CASES_CONFIG

                _ORCH_CACHE[key] = Thalamus(
                    RELAY_CASES_CONFIG,
                    user_id=user_id,
                    relay=None,
                    collect_cases_into_graph=True,
                    build_component_graph=False,
                    parse_equations=False,
                )

            orchestrator = _ORCH_CACHE[key]

            payload: Dict[str, Any] = {
                "type": None,
                "auth": {"user_id": user_id, "session_id": session_id},
                "data": {"msg": query},
            }
            result = async_to_sync(orchestrator.handle_relay_payload)(
                payload=payload,
                user_id=user_id,
                session_id=session_id,
            )

            assistant_message = None
            try:
                if isinstance(result, dict):
                    data = result.get("data") or {}
                    if isinstance(data, dict):
                        assistant_message = data.get("msg") or data.get("message") or data.get("text")
                elif isinstance(result, list) and result:
                    # START_SIM can return a list of response items
                    last = result[-1]
                    if isinstance(last, dict):
                        data = last.get("data") or {}
                        if isinstance(data, dict):
                            assistant_message = data.get("msg") or data.get("message") or data.get("text")
            except Exception:
                assistant_message = None

            return JsonResponse({"ok": True, "result": result, "assistant_message": assistant_message})

        except Exception as exc:
            return JsonResponse(
                {"ok": False, "error": str(exc), "traceback": traceback.format_exc()},
                status=500,
            )

