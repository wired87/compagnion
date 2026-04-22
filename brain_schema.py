from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class BrainNodeType:
    USER = "USER"
    GOAL = "GOAL"
    SUB_GOAL = "SUB_GOAL"
    SHORT_TERM_STORAGE = "SHORT_TERM_STORAGE"
    LONG_TERM_STORAGE = "LONG_TERM_STORAGE"
    CONTENT = "CONTENT"
    # File-manager / KG: files, equations (handwritten→method), objects (infer equation from behaviour)
    FILE = "FILE"
    EQUATION = "EQUATION"
    OBJECT = "OBJECT"
    METHOD = "METHOD"
    ACTION = "ACTION"
    COMPONENT = "COMPONENT"


class BrainEdgeRel:
    DERIVED_FROM = "derived_from"
    REQUIRES = "requires"
    SATISFIES = "satisfies"
    REFERENCES_TABLE_ROW = "references_table_row"
    FOLLOWS = "follows"
    PARENT_OF = "parent_of"
    HISTORY = "history"


@dataclass
class GoalDecision:
    case_name: str
    confidence: float
    source: str
    reason: str = ""
    req_struct: Dict[str, Any] = field(default_factory=dict)
    out_struct: Dict[str, Any] = field(default_factory=dict)
    case_item: Optional[Dict[str, Any]] = None


@dataclass
class DataCollectionResult:
    resolved: Dict[str, Any]
    missing: List[str]


if __name__ == "__main__":
    # Minimal workflow: GoalDecision, DataCollectionResult
    d = GoalDecision(case_name="CHAT", confidence=0.9, source="rule")
    assert d.case_name == "CHAT"
    r = DataCollectionResult(resolved={"text": "hi"}, missing=[])
    assert r.resolved["text"] == "hi"
    print("[brain_schema] ok")
