import asyncio
import json

from qbrain.core.orchestrator_manager.orchestrator import Thalamus
from qbrain.predefined_case import RELAY_CASES_CONFIG
from qbrain.qf_utils.qf_utils import QFUtils
from qbrain.graph.local_graph_utils import GUtils
from qbrain.core.guard import Guard


async def main() -> None:
    """Simple smoke test for Thalamus.handle_relay_payload."""
    g = GUtils()
    qfu = QFUtils()
    guard = Guard(qfu, g, "public")
    orchestrator = Thalamus(
        RELAY_CASES_CONFIG,
        user_id="public",
        g=g,
        qfu=qfu,
        guard=guard,
    )

    payload = {
        "type": "CHAT",
        "auth": {"user_id": "test_user", "session_id": "1"},
        "data": {
            "msg": "create field with param 1,2 and 3 and provide them values 4,5 and 6"
        },
    }

    print("[test_run] sending payload:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    response = await orchestrator.handle_relay_payload(
        payload=payload,
        user_id="test_user",
        session_id="1",
    )

    print("[test_run] response:")
    print(json.dumps(response, indent=2, default=str, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())

