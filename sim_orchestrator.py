"""
SimOrchestrator: Brain sub-module for start-sim workflow.

Orchestrates goal-driven env cfg creation, sim execution, param evolution scoring,
history-based adjustment, and sub_goal inference from milestones.

Every method follows: print("method_name...") at start, print("method_name... done") at end.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

from qbrain.core.env_manager.env_lib import EnvManager
from qbrain.core.guard import Guard
from qbrain.core.injection_manager import InjectionManager
from qbrain.core.param_manager.params_lib import ParamsManager
from qbrain.core.sim_analyzer.sim_result_analyzer import AnalysisResult, SimResultAnalyzer
from qbrain.core.workflows.create_env_from_components import validate_env_components
from brain_graph_utils import get_sub_goal_ids_for_goal
from brain_schema import BrainEdgeRel, BrainNodeType


# ---- SimOrchestrator ----


class SimOrchestrator:
    """
    Orchestrates the start-sim process as a Brain sub-module.
    Resolves GOAL, creates env cfg, runs sim, scores results, adjusts params, infers sub_goals.
    """

    def __init__(
        self,
        brain: Any,
        guard: Guard,
        env_manager: EnvManager,
        sim_analyzer: Optional[SimResultAnalyzer] = None,
        injection_manager: Optional[InjectionManager] = None,
        params_manager: Optional[ParamsManager] = None,
        qb: Optional[Any] = None,
        relay: Optional[Any] = None,
        session_manager: Optional[Any] = None,
    ):
        print("SimOrchestrator.__init__...")
        self.brain = brain
        self.guard = guard
        self.env_manager = env_manager
        self.sim_analyzer = sim_analyzer or SimResultAnalyzer(env_manager=env_manager)
        self.injection_manager = injection_manager
        self.params_manager = params_manager
        self.relay = relay
        self.session_manager = session_manager
        if qb is None:
            from qbrain.core.qbrain_manager import get_qbrain_table_manager
            self._qb = get_qbrain_table_manager()
        else:
            self._qb = qb
        self._goal_score_threshold = 0.99
        self._milestone_markers = [
            "milestone:",
            "checkpoint:",
            "process finished:",
            "step completed:",
            "process finish:",
        ]
        print("SimOrchestrator.__init__... done")

    def run(
        self,
        payload: Dict[str, Any],
        user_id: str,
        session_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Orchestrate full start-sim workflow.
        Returns list of response items (GET_USERS_ENVS, START_SIM, etc.).
        """
        print("run...")
        response_items: List[Dict[str, Any]] = []

        try:
            # ---- 1. Resolve GOAL and goal_cfg ----
            goal_node_id, goal_cfg, sub_goals = self._resolve_goal_and_cfg(
                payload=payload,
                user_id=user_id,
            )

            # ---- 2. Create env cfg (adjust injection properties) ----
            config = self._create_env_cfg_from_goal(
                payload=payload,
                user_id=user_id,
                goal_node_id=goal_node_id,
                goal_cfg=goal_cfg,
                sub_goals=sub_goals,
            )
            if not config:
                print("run... done (no config)")
                return response_items

            # Validate env components
            for env_id, env_data in config.items():
                is_valid, missing = validate_env_components(
                    env_data if isinstance(env_data, dict) else None
                )
                if not is_valid:
                    msg = f"Env {env_id} is missing: {', '.join(missing)}."
                    response_items.append({
                        "type": "CHAT",
                        "status": {"state": "error", "code": 400, "msg": msg},
                        "data": {"msg": msg},
                    })
                    print("run... done (validation failed)")
                    return response_items

            # ---- 3. Start sim with all envs ----
            relay = getattr(self, "relay", None)
            grid_streamer = getattr(relay, "_grid_streamer", None) if relay else None
            grid_animation_recorder = None

            goal_id = goal_node_id or payload.get("data", {}).get("goal_id", "")

            for env_id, env_data in config.items():
                print(f"run: starting guard.main for env_id={env_id}")
                _set_env_vars(env_id=env_id, user_id=user_id, goal_id=goal_id)
                try:
                    self.guard.main(
                        env_id=env_id,
                        env_data=env_data,
                        grid_streamer=grid_streamer,
                        grid_animation_recorder=grid_animation_recorder,
                    )
                except Exception as guard_err:
                    print(f"run: guard.main error for env_id={env_id}: {guard_err}")
                    import traceback
                    traceback.print_exc()
                    raise

                # Send GET_USERS_ENVS update
                try:
                    updated_env = self.env_manager.retrieve_send_user_specific_env_table_rows(
                        user_id
                    )
                    response_items.append({
                        "type": "GET_USERS_ENVS",
                        "data": updated_env,
                    })
                except Exception as ex:
                    print(f"run: error updating env status (continuing): {ex}")

            # ---- 4. Retrieve param evolutions ----
            analyses = self.sim_analyzer.analyze_envs_for_user_goal(
                user_id=user_id,
                goal_id=goal_id or None,
            )

            # ---- 5. Score vs goal ----
            goal_reached = False
            if goal_cfg and analyses:
                for ar in analyses:
                    score = self.sim_analyzer.compute_goal_score(
                        analysis=ar,
                        goal_cfg=goal_cfg,
                        tolerance=0.1,
                    )
                    if score >= self._goal_score_threshold:
                        goal_reached = True
                        break

            # ---- 6. Goal reached or adjust ----
            if goal_reached:
                self._mark_goal_reached(goal_node_id, user_id, goal_id)
                response_items.append({
                    "type": "START_SIM",
                    "status": {"state": "success", "code": 200, "msg": "Goal reached."},
                    "data": {"goal_reached": True},
                })
            else:
                # Adjust params based on history envs, link env to goal with HISTORY
                for ar in analyses:
                    self.sim_analyzer.suggest_param_adjustments(
                        analysis=ar,
                        goal_cfg=goal_cfg,
                    )
                    self._link_env_to_goal_history(
                        env_id=ar.env_id,
                        goal_node_id=goal_node_id,
                        user_id=user_id,
                    )
                response_items.append({
                    "type": "START_SIM",
                    "status": {"state": "success", "code": 200, "msg": "Simulation started."},
                    "data": {"goal_reached": False},
                })

            # ---- 7. Infer sub_goals from milestones ----
            self._infer_sub_goals_from_milestones(
                goal_node_id=goal_node_id,
                user_id=user_id,
            )

            # Session deactivation (mirror original flow)
            sid = session_id
            session_mgr = getattr(self, "session_manager", None)
            if sid is not None and session_mgr is not None:
                try:
                    session_mgr.deactivate_session(sid)
                    new_sid = session_mgr.get_or_create_active_session(user_id)
                    if self.relay is not None:
                        self.relay.session_id = new_sid
                except Exception as sess_err:
                    print(f"run: session deactivate/create error: {sess_err}")

        except Exception as e:
            print(f"run: error: {e}")
            import traceback
            traceback.print_exc()
            response_items.append({
                "type": "START_SIM",
                "status": {"state": "error", "code": 500, "msg": str(e)},
                "data": {},
            })

        print("run... done")
        return response_items

    def _resolve_goal_and_cfg(
        self,
        payload: Dict[str, Any],
        user_id: str,
    ) -> tuple[Optional[str], Optional[Dict[str, Any]], List[str]]:
        """Resolve GOAL node id, goal_cfg, and sub_goals from Brain or payload."""
        print("_resolve_goal_and_cfg...")
        goal_node_id = getattr(self.brain, "last_goal_node_id", None)
        goal_id_from_payload = (payload.get("data") or {}).get("goal_id")
        if goal_id_from_payload:
            goal_node_id = goal_id_from_payload

        goal_cfg = None
        goal_cfg_raw = (payload.get("data") or {}).get("goal_cfg")
        if goal_cfg_raw:
            try:
                goal_cfg = (
                    json.loads(goal_cfg_raw)
                    if isinstance(goal_cfg_raw, str)
                    else goal_cfg_raw
                )
            except Exception:
                pass

        # Query goals table for target_cfg if not in payload
        if goal_cfg is None and goal_node_id:
            try:
                rows = self._qb.run_query(
                    sql="SELECT target_cfg FROM goals WHERE user_id = @user_id AND goal_id = @goal_id",
                    params={"user_id": user_id, "goal_id": goal_node_id},
                    conv_to_dict=True,
                ) or []
                if rows and rows[0].get("target_cfg"):
                    goal_cfg = json.loads(rows[0]["target_cfg"])
            except Exception:
                pass

        sub_goals = get_sub_goal_ids_for_goal(self.brain.G, goal_node_id or "") if goal_node_id else []

        print("_resolve_goal_and_cfg... done")
        return goal_node_id, goal_cfg, sub_goals

    def _create_env_cfg_from_goal(
        self,
        payload: Dict[str, Any],
        user_id: str,
        goal_node_id: Optional[str],
        goal_cfg: Optional[Dict[str, Any]],
        sub_goals: List[str],
    ) -> Dict[str, Any]:
        """
        Create env config. If payload has config, use it.
        Else build from goal + param evolution from envs table.
        """
        print("_create_env_cfg_from_goal...")
        config = (payload.get("data") or {}).get("config", {})
        if config:
            print("_create_env_cfg_from_goal... done (using payload config)")
            return config

        # Goal-driven creation: retrieve envs for user, adjust injection from goal_cfg
        rows = self.env_manager.retrieve_envs_by_user_goal(
            user_id=user_id,
            goal_id=goal_node_id,
            select="*",
        )
        if not rows:
            print("_create_env_cfg_from_goal... done (no envs)")
            return {}

        out: Dict[str, Any] = {}
        for row in rows:
            env_id = row.get("id", "")
            data_str = row.get("data")
            if not data_str:
                continue
            try:
                env_data = json.loads(data_str) if isinstance(data_str, str) else data_str
            except Exception:
                continue
            if not isinstance(env_data, dict):
                continue

            # Adjust injection properties based on goal_cfg vs param evolution
            if goal_cfg:
                env_data = self._adjust_injections_from_goal(
                    env_data=env_data,
                    row=row,
                    goal_cfg=goal_cfg,
                )
            out[env_id] = env_data

        print("_create_env_cfg_from_goal... done")
        return out

    def _adjust_injections_from_goal(
        self,
        env_data: Dict[str, Any],
        row: Dict[str, Any],
        goal_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Adjust injection properties per param by comparing goal_cfg to param evolution."""
        # Simplified: merge goal_cfg targets into env_data for downstream use
        env_data = dict(env_data)
        if "goal_targets" not in env_data:
            env_data["goal_targets"] = goal_cfg
        return env_data

    def _mark_goal_reached(
        self,
        goal_node_id: Optional[str],
        user_id: str,
        goal_id: str,
    ) -> None:
        """Mark goal as reached in goals table and optionally GOAL node."""
        print("_mark_goal_reached...")
        gid = goal_id or goal_node_id
        if not gid:
            print("_mark_goal_reached... done")
            return
        try:
            self._qb.run_db(
                "UPDATE goals SET status = 'reached' WHERE user_id = @user_id AND goal_id = @goal_id",
                params={"user_id": user_id, "goal_id": gid},
            )
        except Exception as e:
            print(f"_mark_goal_reached: db update warning: {e}")
        print("_mark_goal_reached... done")

    def _link_env_to_goal_history(
        self,
        env_id: str,
        goal_node_id: Optional[str],
        user_id: str,
    ) -> None:
        """Create ENV node if needed, link to GOAL with HISTORY relation."""
        print("_link_env_to_goal_history...")
        if not goal_node_id or not self.brain.G.has_node(goal_node_id):
            print("_link_env_to_goal_history... done (no goal)")
            return

        env_node_id = f"ENV::{user_id}::{env_id}"
        if not self.brain.G.has_node(env_node_id):
            self.brain.add_node({
                "id": env_node_id,
                "type": "ENV",
                "user_id": user_id,
                "env_id": env_id,
            })
        self.brain.add_edge(
            src=env_node_id,
            trt=goal_node_id,
            attrs={
                "rel": BrainEdgeRel.HISTORY,
                "src_layer": "ENV",
                "trgt_layer": BrainNodeType.GOAL,
            },
        )
        print("_link_env_to_goal_history... done")

    def _infer_sub_goals_from_milestones(
        self,
        goal_node_id: Optional[str],
        user_id: str,
    ) -> None:
        """Scan short-term messages for milestones; create SUB_GOAL nodes."""
        print("_infer_sub_goals_from_milestones...")
        if not goal_node_id or not hasattr(self.brain, "short_term_ids"):
            print("_infer_sub_goals_from_milestones... done (no goal or short_term)")
            return

        for nid in reversed(list(self.brain.short_term_ids)):
            if not self.brain.G.has_node(nid):
                continue
            msg = str(self.brain.get_node(nid).get("message") or "")
            low = msg.lower()
            for marker in self._milestone_markers:
                if marker.lower() in low:
                    idx = low.find(marker.lower())
                    desc = msg[idx + len(marker) :].strip().split("\n")[0][:200]
                    sub_goal_id = (
                        f"SUBGOAL::{user_id}::milestone::{int(time.time() * 1000)}"
                    )
                    if not self.brain.G.has_node(sub_goal_id):
                        self.brain.add_node({
                            "id": sub_goal_id,
                            "type": BrainNodeType.SUB_GOAL,
                            "user_id": user_id,
                            "goal_case": "milestone",
                            "milestone_description": desc,
                        })
                        self.brain.add_edge(
                            src=goal_node_id,
                            trt=sub_goal_id,
                            attrs={
                                "rel": BrainEdgeRel.REQUIRES,
                                "src_layer": BrainNodeType.GOAL,
                                "trgt_layer": BrainNodeType.SUB_GOAL,
                            },
                        )
                    break
        print("_infer_sub_goals_from_milestones... done")


# ---- Helpers ----


def _set_env_vars(
    env_id: str,
    user_id: str,
    goal_id: str,
) -> None:
    """Set ENV_ID, USER_ID, GOAL_ID for jax_test guard persistence."""
    os.environ["ENV_ID"] = str(env_id)
    os.environ["USER_ID"] = str(user_id)
    os.environ["GOAL_ID"] = str(goal_id or "")
