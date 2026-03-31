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
    # Pathway planning: reverse-engineered execution plan for a user request
    PATHWAY = "PATHWAY"


class BrainEdgeRel:
    DERIVED_FROM = "derived_from"
    REQUIRES = "requires"
    SATISFIES = "satisfies"
    REFERENCES_TABLE_ROW = "references_table_row"
    FOLLOWS = "follows"
    PARENT_OF = "parent_of"
    HISTORY = "history"
    # Pathway edges: connect PATHWAY node to its METHOD/ACTION steps in order
    PATHWAY_STEP = "pathway_step"


@dataclass
class PathwayParam:
    """
    A single parameter in a Pathway step.

    src == "METHOD"  -> value can be resolved from the graph (METHOD/ACTION node attrs).
    src == "USER"    -> value must be collected interactively from the user.
    """

    key: str
    src: str  # "METHOD" | "USER"


@dataclass
class PathwayNode:
    """
    One step in an ordered Pathway plan.

    path_idx          : zero-based execution order
    node_id           : ID of the PATHWAY graph node created for this step
    method_or_action_id: ID of the source METHOD or ACTION graph node
    params            : ordered list of PathwayParam items for this step
    """

    path_idx: int
    node_id: str
    method_or_action_id: str
    params: List[PathwayParam] = field(default_factory=list)


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
    # Minimal workflow: GoalDecision, DataCollectionResult, PathwayParam, PathwayNode
    d = GoalDecision(case_name="CHAT", confidence=0.9, source="rule")
    assert d.case_name == "CHAT"
    r = DataCollectionResult(resolved={"text": "hi"}, missing=[])
    assert r.resolved["text"] == "hi"
    pp = PathwayParam(key="module_ids", src="USER")
    assert pp.src == "USER"
    pn = PathwayNode(path_idx=0, node_id="PATHWAY::u1::0", method_or_action_id="METHOD::set_cfg", params=[pp])
    assert pn.path_idx == 0
    assert pn.params[0].key == "module_ids"
    print("[brain_schema] ok")
