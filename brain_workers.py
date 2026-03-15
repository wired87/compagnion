from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Optional


class BrainWorkers:
    """Small bounded worker pool for non-graph heavy tasks."""

    def __init__(self, max_workers: int = 4):
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="brain")

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
        return self._executor.submit(fn, *args, **kwargs)

    def run_sync(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return self.submit(fn, *args, **kwargs).result()

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)

    def __enter__(self) -> "BrainWorkers":
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Any) -> None:
        self.close()


if __name__ == "__main__":
    # Minimal workflow: submit + run_sync
    w = BrainWorkers(max_workers=2)
    r = w.run_sync(lambda a, b: a + b, 1, 2)
    assert r == 3
    w.close()
    print("[brain_workers] ok")
