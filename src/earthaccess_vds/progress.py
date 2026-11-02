"""Progress reporting and cancellation for virtualization jobs.

``earthaccess_plugin_spec.Job`` already tracks a background future. This module
adds the per-granule detail: how many granules have been opened, whether the
job is still virtualizing, combining, or writing, and the cluster dashboard
when a distributed ``Client`` is active.

The executor is a plain thread pool. It runs *in this process*, so the obstore
registry (which carries the EDL token) never crosses the Dask wire. Worker-side
credential resolution is the prerequisite for shipping the open step to a
cluster; until then, an active Client is only used for the dashboard link.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from earthaccess_plugin_spec import Job, JobStatus

__all__ = [
    "ProgressExecutor",
    "ProgressState",
    "VirtualJob",
    "dashboard_link",
    "is_interactive",
]


def is_interactive() -> bool:
    """Whether to show a progress bar by default (notebook or TTY)."""

    if "ipykernel" in sys.modules:
        return True
    try:
        return bool(sys.stderr.isatty())
    except Exception:  # noqa: BLE001 - a closed stderr must not break a job
        return False


def dashboard_link() -> str | None:
    """The active distributed Client's dashboard URL, if there is one."""

    try:
        from distributed import default_client

        client = default_client()
    except Exception:  # noqa: BLE001 - distributed is optional / no client
        return None
    return getattr(client, "dashboard_link", None)


@dataclass
class ProgressState:
    """Thread-safe counters shared by the executor, the job, and the writer."""

    total: int = 0
    opened: int = 0
    failed: int = 0
    phase: str = "pending"
    dashboard: str | None = field(default_factory=dashboard_link)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def start(self, phase: str, total: int | None = None) -> None:
        with self._lock:
            self.phase = phase
            if total is not None:
                self.total = total
            self.opened = 0
            self.failed = 0
        self.update_dashboard()

    def set_phase(self, phase: str) -> None:
        with self._lock:
            self.phase = phase
        self.update_dashboard()

    def update_dashboard(self) -> None:
        if self.dashboard is not None:
            return
        dashboard = dashboard_link()
        if dashboard is not None:
            with self._lock:
                self.dashboard = dashboard

    def opened_one(self) -> None:
        with self._lock:
            self.opened += 1

    def failed_one(self) -> None:
        with self._lock:
            self.failed += 1

    def percent(self) -> float:
        with self._lock:
            return self._percent_locked()

    def _percent_locked(self) -> float:
        if not self.total:
            return 100.0 if self.phase == "done" else 0.0
        return round(100.0 * (self.opened + self.failed) / self.total, 1)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "opened": self.opened,
                "total": self.total,
                "failed": self.failed,
                "phase": self.phase,
                "percent": self._percent_locked(),
                "dashboard": self.dashboard,
            }


class ProgressExecutor(Executor):
    """A thread-pool executor that counts completed granules.

    VirtualiZarr drives it through ``map``; the counters update as each granule
    finishes, so ``job.info()`` shows live progress. The executor is not shut
    down by VirtualiZarr (caller-supplied executors never are), so the job owns
    its lifecycle and can cancel pending work.
    """

    def __init__(
        self,
        state: ProgressState | None = None,
        *,
        max_workers: int | None = None,
        show_progress: bool | None = None,
        client: Any = None,
        desc: str = "virtualize",
    ) -> None:
        self.state = state if state is not None else ProgressState()
        self._client = client
        self._pool = (
            None if client is not None else ThreadPoolExecutor(max_workers=max_workers)
        )
        if client is not None:
            dashboard = getattr(client, "dashboard_link", None)
            if dashboard:
                self.state.dashboard = dashboard
        self._futures: list[Future] = []
        self._lock = threading.Lock()
        self._desc = desc
        self._show = is_interactive() if show_progress is None else show_progress
        self._bar: Any = None
        self._bar_ready = False
        self._closed = False

    # -- Executor protocol ---------------------------------------------------

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        if self._client is not None:
            future = self._client.submit(fn, *args, **kwargs)
        else:
            assert self._pool is not None
            future = self._pool.submit(fn, *args, **kwargs)
        with self._lock:
            self._futures.append(future)
        future.add_done_callback(self._done)
        self._ensure_bar()
        return future

    def map(
        self,
        fn: Callable[..., Any],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[Any]:
        del chunksize  # accepted for API compatibility; threads have no chunks
        futures = [self.submit(fn, *args) for args in zip(*iterables, strict=False)]

        def results() -> Iterator[Any]:
            for future in futures:
                yield future.result(timeout)

        return results()

    # -- progress / cancellation --------------------------------------------

    def _done(self, future: Future) -> None:
        if future.cancelled():
            return
        if future.exception() is None:
            self.state.opened_one()
        else:
            self.state.failed_one()
        if self._bar is not None:
            self._bar.update(1)

    def _ensure_bar(self) -> None:
        if not self._show or self._bar_ready:
            return
        self._bar_ready = True
        try:
            from tqdm.auto import tqdm
        except ImportError:  # pragma: no cover - tqdm ships with dask
            return
        self._bar = tqdm(
            total=self.state.total or None, desc=self._desc, unit="granule"
        )

    def cancel(self) -> bool:
        """Cancel pending tasks; return whether any cancellation was requested."""

        with self._lock:
            futures = list(self._futures)
        if self._client is not None:
            if not futures:
                return False
            self._client.cancel(futures)
            return True
        return any(future.cancel() for future in futures)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        if self._bar is not None:
            self._bar.close()
        if self._pool is not None:
            self._pool.shutdown(wait=wait, cancel_futures=cancel_futures)
        # A caller-supplied Client is never closed by the executor.


class VirtualJob(Job):
    """A Job whose ``info()`` carries the virtualization counters.

    ``info()`` is the stable shape::

        {"status": "running", "opened": 4, "total": 10, "failed": 0,
         "percent": 40.0, "phase": "virtualize"|"combine"|"write"|"done",
         "dashboard": "http://…" | None}
    """

    def __init__(
        self,
        fn: Callable[[], Any],
        *,
        plugin: str,
        output: str,
        state: ProgressState,
        executor: ProgressExecutor | None = None,
    ) -> None:
        self._state = state
        self._executor = executor
        super().__init__(fn, plugin=plugin, output=output)

    def _poll(self) -> tuple[JobStatus, dict[str, Any]]:
        status, info = super()._poll()
        merged = {**info, **self._state.snapshot()}
        if status is JobStatus.SUCCEEDED:
            merged["phase"] = "done"
        return status, merged

    def _cancel(self) -> bool:
        if self._executor is None:
            return False
        return self._executor.cancel()
