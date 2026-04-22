"""
Shared Brain utilities: user_id normalization and other common helpers.
Single source for logic used across pickup, receive_user_data, Brain.
"""
from __future__ import annotations

from typing import Any


def normalize_user_id(user_id: Any, fallback: str = "public") -> str:
    """Normalize user_id to non-empty string; use fallback when empty."""
    candidate = str(user_id).strip() if user_id is not None else ""
    return candidate or fallback


if __name__ == "__main__":
    # Minimal workflow: normalize_user_id
    assert normalize_user_id("alice") == "alice"
    assert normalize_user_id("  bob  ") == "bob"
    assert normalize_user_id(None) == "public"
    assert normalize_user_id("") == "public"
    print("[brain_utils] ok")
