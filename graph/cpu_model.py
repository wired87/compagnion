from __future__ import annotations

import dataclasses
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import numpy as np

import jax
import jax.numpy as jnp

try:
    from flax import linen as nn
except Exception as _exc:  # pragma: no cover
    nn = None  # type: ignore[assignment]


@dataclass(frozen=True)
class CpuModelConfig:
    hidden_dim: int = 32
    num_layers: int = 2
    dim_vector_size: int = 7
    goal_in_dim: int = 32
    top_k_default: int = 25
    max_nodes: Optional[int] = None  # deterministic cap; None means all


@dataclass(frozen=True)
class CpuModelRequest:
    """
    Well-described request struct for ctlr queries.

    Goal is accepted in multiple deterministic ways:
    - goal_vec: numeric vector (preferred if caller can provide).
    - goal_node_id: derive a deterministic vector from that node's attrs (stringified).
    - goal_text: deterministic hash -> vector.
    """

    goal_vec: Optional[Sequence[float]] = None
    goal_node_id: Optional[str] = None
    goal_text: Optional[str] = None
    top_k: Optional[int] = None
    allowed_node_types: Optional[Sequence[str]] = None
    candidate_node_ids: Optional[Sequence[str]] = None


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int, np.integer)):
            return int(x)
        if isinstance(x, (float, np.floating)):
            return int(x)
        return int(str(x).strip())
    except Exception:
        return default


def _to_float_list(x: Any, n: int, default: float = 0.0) -> List[float]:
    if x is None:
        return [default] * n
    if isinstance(x, (list, tuple, np.ndarray)):
        arr = [float(v) for v in list(x)]
    else:
        # Support CSV-like string "1,2,3"
        try:
            s = str(x)
            parts = [p.strip() for p in s.replace("[", "").replace("]", "").split(",") if p.strip()]
            arr = [float(p) for p in parts]
        except Exception:
            arr = []
    if len(arr) >= n:
        return arr[:n]
    return arr + [default] * (n - len(arr))


def _stable_hash_seed(text: str) -> int:
    digest = hashlib.sha256((text or "").encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def goal_text_to_vec(text: str, dim: int) -> np.ndarray:
    """
    Deterministic goal embedding seed -> vector. Fully offline.
    """
    seed = _stable_hash_seed(text or "")
    rng = np.random.default_rng(seed)
    vec = rng.standard_normal(dim).astype(np.float32)
    n = float(np.linalg.norm(vec))
    if n > 1e-8:
        vec = vec / n
    return vec


def _goal_node_to_text(gutils: Any, goal_node_id: str) -> str:
    try:
        if gutils is None or getattr(gutils, "G", None) is None:
            return str(goal_node_id)
        if not gutils.G.has_node(goal_node_id):
            return str(goal_node_id)
        attrs = dict(gutils.G.nodes[goal_node_id])
        # Keep deterministic ordering.
        items = sorted((str(k), str(v)) for k, v in attrs.items())
        return f"{goal_node_id}::" + "|".join([f"{k}={v}" for k, v in items])
    except Exception:
        return str(goal_node_id)


def _pick_first(attrs: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for k in keys:
        if k in attrs:
            return attrs.get(k)
    return None


def extract_node_feature_vector(
    node_id: str,
    attrs: Mapping[str, Any],
    *,
    node_types: Sequence[str],
    config: CpuModelConfig,
) -> np.ndarray:
    """
    Minimal, deterministic feature design.
    Missing fields always default to 0.
    """
    t = str(attrs.get("type") or "").upper()
    type_map = {str(x).upper(): i for i, x in enumerate(node_types)}
    type_id = type_map.get(t, -1)
    type_onehot = np.zeros((len(node_types),), dtype=np.float32)
    if 0 <= type_id < len(node_types):
        type_onehot[type_id] = 1.0

    # Accept multiple alias keys; if none found, default to 0.
    tensor_rank = _to_int(
        _pick_first(attrs, ["tensor_rank", "tensorRank", "rank", "tensor_rank_id", "tensor"]),
        default=0,
    )
    derivative_order = _to_int(
        _pick_first(attrs, ["derivative_order", "derivativeOrder", "deriv_order", "d_order", "derivative"]),
        default=0,
    )
    semantic_class_id = _to_int(
        _pick_first(attrs, ["semantic_class_id", "semanticClassId", "semantic_class", "class_id", "semantic_id"]),
        default=(type_id if type_id >= 0 else 0),
    )
    dim_vec = _to_float_list(
        _pick_first(attrs, ["dimensional_vector", "dimensionalVector", "dim_vector", "dim_vec", "dims"]),
        n=int(config.dim_vector_size),
        default=0.0,
    )

    scalars = np.array(
        [float(tensor_rank), float(derivative_order), float(semantic_class_id)],
        dtype=np.float32,
    )
    out = np.concatenate([type_onehot, scalars, np.asarray(dim_vec, dtype=np.float32)], axis=0)
    return out


def build_graph_tensors(
    G: Any,
    node_id_to_idx: Mapping[str, int],
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build (senders, receivers) as int32 numpy arrays.
    For undirected graphs, we include both directions.
    Preserves parallel edges for MultiGraph/MultiDiGraph.
    """
    senders: List[int] = []
    receivers: List[int] = []
    is_multi = "Multi" in str(type(G))
    is_directed = bool(getattr(G, "is_directed", lambda: False)())

    if is_multi:
        edges_iter = G.edges(keys=True, data=False)
        for u, v, _k in edges_iter:
            su = node_id_to_idx.get(str(u))
            sv = node_id_to_idx.get(str(v))
            if su is None or sv is None:
                continue
            senders.append(su)
            receivers.append(sv)
            if not is_directed:
                senders.append(sv)
                receivers.append(su)
    else:
        edges_iter = G.edges(data=False)
        for u, v in edges_iter:
            su = node_id_to_idx.get(str(u))
            sv = node_id_to_idx.get(str(v))
            if su is None or sv is None:
                continue
            senders.append(su)
            receivers.append(sv)
            if not is_directed:
                senders.append(sv)
                receivers.append(su)

    return np.asarray(senders, dtype=np.int32), np.asarray(receivers, dtype=np.int32)


if nn is not None:

    class _SageLayer(nn.Module):
        hidden_dim: int

        @nn.compact
        def __call__(self, h: jnp.ndarray, senders: jnp.ndarray, receivers: jnp.ndarray) -> jnp.ndarray:
            # mean aggregation with scatter-add (handles duplicate edges)
            n = h.shape[0]
            msg = h[senders]  # (E, H)
            agg = jnp.zeros((n, h.shape[1]), dtype=h.dtype).at[receivers].add(msg)
            deg = jnp.zeros((n,), dtype=h.dtype).at[receivers].add(1.0)
            mean = agg / jnp.maximum(deg, 1.0)[:, None]

            h_self = nn.Dense(self.hidden_dim, use_bias=True)(h)
            h_nei = nn.Dense(self.hidden_dim, use_bias=False)(mean)
            out = jax.nn.relu(h_self + h_nei)
            return out


    class _GoalMLP(nn.Module):
        hidden_dim: int

        @nn.compact
        def __call__(self, x: jnp.ndarray) -> jnp.ndarray:
            x = nn.Dense(self.hidden_dim)(x)
            x = jax.nn.relu(x)
            x = nn.Dense(self.hidden_dim)(x)
            return x


    class _CpuGraphSageScorer(nn.Module):
        hidden_dim: int
        num_layers: int

        @nn.compact
        def __call__(
            self,
            node_x: jnp.ndarray,
            senders: jnp.ndarray,
            receivers: jnp.ndarray,
            goal_x: jnp.ndarray,
        ) -> Tuple[jnp.ndarray, jnp.ndarray]:
            # Project node features once
            h = nn.Dense(self.hidden_dim)(node_x)
            h = jax.nn.relu(h)
            for _ in range(int(self.num_layers)):
                h = _SageLayer(self.hidden_dim)(h, senders=senders, receivers=receivers)
            goal_emb = _GoalMLP(self.hidden_dim)(goal_x)
            # Dot-product scoring
            logits = jnp.einsum("nh,h->n", h, goal_emb)
            scores = jax.nn.sigmoid(logits)
            return scores, logits


class CpuGraphScorer:
    """
    Tiny goal-conditioned GNN scorer. Designed for CPU (<100k params, <3 layers).
    The model is only a relevance scorer: it does not synthesize equations.
    """

    def __init__(
        self,
        gutils: Any,
        node_types: Sequence[str],
        *,
        config: Optional[CpuModelConfig] = None,
        rng_seed: int = 0,
    ):
        if nn is None:  # pragma: no cover
            raise ImportError("flax is required for CpuGraphScorer (flax.linen import failed).")
        if gutils is None or getattr(gutils, "G", None) is None:
            raise ValueError("CpuGraphScorer requires a GUtils-like instance with .G (networkx graph).")
        self.gutils = gutils
        self.node_types = [str(t).upper() for t in (node_types or [])]
        self.config = config or CpuModelConfig()

        # Deterministic node list & cap
        all_node_ids = [str(nid) for nid in list(self.gutils.G.nodes())]
        all_node_ids = sorted(all_node_ids)
        if self.config.max_nodes is not None:
            all_node_ids = all_node_ids[: int(self.config.max_nodes)]
        self.node_ids: List[str] = all_node_ids
        self.node_id_to_idx: Dict[str, int] = {nid: i for i, nid in enumerate(self.node_ids)}

        # Build node feature matrix
        feats: List[np.ndarray] = []
        for nid in self.node_ids:
            attrs = dict(self.gutils.G.nodes[nid]) if self.gutils.G.has_node(nid) else {}
            feats.append(
                extract_node_feature_vector(
                    nid,
                    attrs,
                    node_types=self.node_types,
                    config=self.config,
                )
            )
        self.node_x_np = np.stack(feats, axis=0).astype(np.float32) if feats else np.zeros((0, 1), np.float32)

        # Graph edge tensors
        s_np, r_np = build_graph_tensors(self.gutils.G, self.node_id_to_idx)
        self.senders_np = s_np
        self.receivers_np = r_np

        # Model init
        self._model = _CpuGraphSageScorer(hidden_dim=self.config.hidden_dim, num_layers=self.config.num_layers)
        self._rng = jax.random.PRNGKey(int(rng_seed))
        node_x = jnp.asarray(self.node_x_np)
        senders = jnp.asarray(self.senders_np)
        receivers = jnp.asarray(self.receivers_np)
        goal_x = jnp.zeros((int(self.config.goal_in_dim),), dtype=jnp.float32)
        self._params = self._model.init(self._rng, node_x=node_x, senders=senders, receivers=receivers, goal_x=goal_x)

    def _resolve_request(self, req: Union[CpuModelRequest, Mapping[str, Any]]) -> CpuModelRequest:
        if isinstance(req, CpuModelRequest):
            return req
        if not isinstance(req, Mapping):
            raise TypeError("ctlr req must be a dict-like mapping or CpuModelRequest.")
        return CpuModelRequest(
            goal_vec=req.get("goal_vec"),
            goal_node_id=req.get("goal_node_id"),
            goal_text=req.get("goal_text"),
            top_k=req.get("top_k"),
            allowed_node_types=req.get("allowed_node_types"),
            candidate_node_ids=req.get("candidate_node_ids"),
        )

    def _build_goal_x(self, req: CpuModelRequest) -> np.ndarray:
        if req.goal_vec is not None:
            v = np.asarray(list(req.goal_vec), dtype=np.float32).ravel()
            if v.size >= int(self.config.goal_in_dim):
                return v[: int(self.config.goal_in_dim)]
            if v.size == 0:
                return np.zeros((int(self.config.goal_in_dim),), dtype=np.float32)
            return np.pad(v, (0, int(self.config.goal_in_dim) - v.size))

        if req.goal_node_id:
            txt = _goal_node_to_text(self.gutils, str(req.goal_node_id))
            return goal_text_to_vec(txt, int(self.config.goal_in_dim))

        txt = str(req.goal_text or "")
        return goal_text_to_vec(txt, int(self.config.goal_in_dim))

    def score_nodes(self, req: Union[CpuModelRequest, Mapping[str, Any]]) -> List[Dict[str, Any]]:
        req_obj = self._resolve_request(req)
        if len(self.node_ids) == 0:
            return []

        goal_x_np = self._build_goal_x(req_obj)
        node_x = jnp.asarray(self.node_x_np)
        senders = jnp.asarray(self.senders_np)
        receivers = jnp.asarray(self.receivers_np)
        goal_x = jnp.asarray(goal_x_np)

        scores, _logits = self._model.apply(self._params, node_x=node_x, senders=senders, receivers=receivers, goal_x=goal_x)
        scores_np = np.asarray(scores, dtype=np.float32)

        # Candidate filtering
        candidate_idx = np.arange(len(self.node_ids), dtype=np.int32)
        if req_obj.allowed_node_types:
            allow = {str(t).upper() for t in req_obj.allowed_node_types}
            mask = []
            for nid in self.node_ids:
                t = str(self.gutils.G.nodes[nid].get("type") or "").upper() if self.gutils.G.has_node(nid) else ""
                mask.append(t in allow)
            candidate_idx = candidate_idx[np.asarray(mask, dtype=bool)]

        if req_obj.candidate_node_ids:
            cand_set = {str(x) for x in req_obj.candidate_node_ids}
            mask = np.asarray([nid in cand_set for nid in self.node_ids], dtype=bool)
            candidate_idx = candidate_idx[mask[candidate_idx]]

        if candidate_idx.size == 0:
            return []

        top_k = int(req_obj.top_k or self.config.top_k_default)
        top_k = max(1, min(top_k, int(candidate_idx.size)))

        # Deterministic top-k with stable tie-break on node_id.
        cand_scores = scores_np[candidate_idx]
        order = np.lexsort((np.asarray([self.node_ids[i] for i in candidate_idx]), -cand_scores))
        top_local = order[:top_k]
        top_idx = candidate_idx[top_local]

        return [
            {"node_id": self.node_ids[int(i)], "score": float(scores_np[int(i)])}
            for i in top_idx
        ]

    def ctlr(self, req: Union[CpuModelRequest, Mapping[str, Any]], type: str) -> Dict[str, Any]:
        """
        Inbuilt controller-style query entrypoint.

        Must always return node_id & score for top-k (no closure/resolution here).
        """
        qtype = str(type or "")
        if qtype not in {"create_eq_from_goal", "score_nodes_for_goal"}:
            raise ValueError(f"Unsupported ctlr type: {qtype}")
        req_obj = self._resolve_request(req)
        top_k = int(req_obj.top_k or self.config.top_k_default)
        results = self.score_nodes(req_obj)
        return {"type": qtype, "top_k": min(top_k, len(results)), "results": results}

    # Optional: small training helper (caller provides samples).
    def fit(
        self,
        samples: Sequence[Mapping[str, Any]],
        *,
        epochs: int = 10,
        lr: float = 1e-2,
        seed: int = 0,
    ) -> Dict[str, Any]:
        """
        Train with tiny binary relevance targets.

        Each sample:
          - goal_* fields (same as CpuModelRequest)
          - positive_node_ids: list[str]
          - optionally negative_node_ids: list[str] (else random negatives)
        """
        try:
            import optax
        except Exception as exc:  # pragma: no cover
            raise ImportError("optax is required for fit()") from exc

        if not samples:
            return {"epochs": 0, "loss": None}

        rng = np.random.default_rng(int(seed))
        opt = optax.adam(float(lr))
        opt_state = opt.init(self._params)

        node_x = jnp.asarray(self.node_x_np)
        senders = jnp.asarray(self.senders_np)
        receivers = jnp.asarray(self.receivers_np)

        def _loss_fn(params, goal_x, y):
            scores, logits = self._model.apply(params, node_x=node_x, senders=senders, receivers=receivers, goal_x=goal_x)
            # sigmoid BCE on logits for numerical stability
            loss = optax.sigmoid_binary_cross_entropy(logits, y).mean()
            return loss, scores

        @jax.jit
        def _step(params, opt_state, goal_x, y):
            (loss, _scores), grads = jax.value_and_grad(_loss_fn, has_aux=True)(params, goal_x, y)
            updates, opt_state2 = opt.update(grads, opt_state, params)
            params2 = optax.apply_updates(params, updates)
            return params2, opt_state2, loss

        last_loss = None
        for _ep in range(int(epochs)):
            for s in samples:
                req = self._resolve_request(s)
                goal_x_np = self._build_goal_x(req)
                goal_x = jnp.asarray(goal_x_np)

                pos = {str(x) for x in (s.get("positive_node_ids") or [])}
                neg = {str(x) for x in (s.get("negative_node_ids") or [])}

                # If no explicit negatives, sample deterministically from non-positives.
                if not neg:
                    pool = [nid for nid in self.node_ids if nid not in pos]
                    k = min(len(pool), max(1, len(pos) * 2))
                    if k > 0:
                        neg = set(rng.choice(pool, size=k, replace=False).tolist())

                y_np = np.zeros((len(self.node_ids),), dtype=np.float32)
                for nid in pos:
                    idx = self.node_id_to_idx.get(nid)
                    if idx is not None:
                        y_np[int(idx)] = 1.0
                # Keep labels only on pos/neg nodes to keep objective tiny.
                mask = np.zeros_like(y_np)
                for nid in pos.union(neg):
                    idx = self.node_id_to_idx.get(nid)
                    if idx is not None:
                        mask[int(idx)] = 1.0
                if float(mask.sum()) < 1.0:
                    continue
                y = jnp.asarray(y_np)
                # Apply mask by zeroing loss contribution outside labeled nodes (avoid branching).
                y_mask = jnp.asarray(mask)

                def _masked_step(params, opt_state):
                    def _masked_loss_fn(params2):
                        scores, logits = self._model.apply(params2, node_x=node_x, senders=senders, receivers=receivers, goal_x=goal_x)
                        per = optax.sigmoid_binary_cross_entropy(logits, y) * y_mask
                        loss = per.sum() / jnp.maximum(y_mask.sum(), 1.0)
                        return loss

                    loss, grads = jax.value_and_grad(_masked_loss_fn)(params)
                    updates, opt_state2 = opt.update(grads, opt_state, params)
                    params2 = optax.apply_updates(params, updates)
                    return params2, opt_state2, loss

                self._params, opt_state, last_loss = _masked_step(self._params, opt_state)

        return {"epochs": int(epochs), "loss": (float(last_loss) if last_loss is not None else None)}


def build_cpu_graph_scorer(gutils: Any, node_types: Sequence[str], **kwargs: Any) -> CpuGraphScorer:
    return CpuGraphScorer(gutils=gutils, node_types=node_types, **kwargs)

