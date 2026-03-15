from qbrain.graph.local_graph_utils import GUtils

# Brain is optional/heavy; keep graph importable even if its deps are missing.
try:
    from qbrain.graph.brn.brain import Brain
except Exception:  # pragma: no cover
    Brain = None  # type: ignore[assignment]

# CPU graph scorer depends on JAX; guard against environments where jax isn't installed.
try:
    from qbrain.graph.cpu_model import (
        CpuGraphScorer,
        CpuModelConfig,
        CpuModelRequest,
        build_cpu_graph_scorer,
    )
except Exception:  # pragma: no cover
    CpuGraphScorer = None  # type: ignore[assignment]
    CpuModelConfig = None  # type: ignore[assignment]
    CpuModelRequest = None  # type: ignore[assignment]
    build_cpu_graph_scorer = None  # type: ignore[assignment]

__all__ = [
    "GUtils",
    "CpuGraphScorer",
    "CpuModelConfig",
    "CpuModelRequest",
    "build_cpu_graph_scorer",
]

if Brain is not None:
    __all__.append("Brain")

