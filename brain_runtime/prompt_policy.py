import os


def resolve_prompt_policy_mode(raw_mode: str | None = None) -> str:
    """
    Resolve prompt policy mode from explicit value or environment.
    Supported values: legacy, strict.
    """
    mode = (raw_mode or os.getenv("QBRAIN_PROMPT_POLICY", "legacy")).strip().lower()
    if mode not in {"legacy", "strict"}:
        return "legacy"
    return mode


def build_extraction_policy_block(case_name: str, policy_mode: str) -> str:
    """
    Build extraction policy instructions appended to orchestrator prompts.
    """
    base_rules = [
        "- Infer values from the current user message and relevant conversation context only.",
        "- Output a SINGLE JSON object.",
        '- Top-level keys MUST exactly match req_struct (for example: "auth", "data").',
        "- Do NOT include any keys not present in req_struct.",
        "- If a value is not present in message or conversation, set it explicitly to null.",
        "- Keep nested object structure shape aligned with req_struct.",
    ]
    if policy_mode == "strict":
        strict_rules = [
            "- Preserve scalar/list/object type shape implied by req_struct; do not change field types.",
            "- Never coerce unknown values into guessed defaults; prefer null for uncertain fields.",
            "- For arrays, keep item semantics consistent with user language and existing payload context.",
            "- Only backfill from conversation when references are explicit or unambiguous.",
            "- Maintain all required parameter paths so missing-value detection remains accurate.",
        ]
        base_rules.extend(strict_rules)

    rules = "\n".join(base_rules)
    return f"""
Policy mode: {policy_mode}
Case: {case_name}
Rules:
{rules}
"""
