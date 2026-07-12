"""Speculative ordered Athena query prefetching."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from math import ceil
from time import monotonic

from aws.athena_models import AthenaQueryExecution
from aws.athena_settings import AthenaSettings
from aws.athena_sql import AthenaQueryWindow


@dataclass(slots=True)
class PendingAthenaQuery:
    """Athena query submitted for ordered speculative consumption."""

    future: Future[AthenaQueryExecution]
    submitted_at: float
    completed_at: float | None = None

    @property
    def elapsed_seconds(self) -> float:
        """Return elapsed time from submission to completion or now."""
        return (self.completed_at or monotonic()) - self.submitted_at


class AthenaPrefetchPolicy:
    """Adapt the prefetch target from query wait time and row consumption speed."""

    def __init__(self, settings: AthenaSettings) -> None:
        self.settings = settings

    def target(
        self,
        *,
        current_target: int,
        query_elapsed: float,
        consumption_elapsed: float | None,
        wait_elapsed: float,
    ) -> int:
        """Return the next bounded speculative prefetch target."""
        minimum = self.settings.query_prefetch_min_windows
        maximum = self.settings.query_prefetch_max_windows
        desired = current_target

        if consumption_elapsed is not None:
            if consumption_elapsed <= 0:
                desired = maximum
            else:
                desired = ceil(query_elapsed / consumption_elapsed) + 1

        if wait_elapsed > self.settings.query_prefetch_wait_target_seconds:
            desired = max(desired, current_target + 1)

        if desired > current_target:
            adjusted = desired
        elif desired < current_target:
            adjusted = current_target - 1
        else:
            adjusted = current_target

        return max(minimum, min(maximum, adjusted))


class AthenaQueryPrefetcher:
    """Execute Athena windows speculatively while yielding results in order."""

    def __init__(
        self,
        *,
        settings: AthenaSettings,
        executor: Callable[[str], AthenaQueryExecution],
        policy: AthenaPrefetchPolicy,
    ) -> None:
        self.settings = settings
        self.executor = executor
        self.policy = policy

    def executions(
        self,
        *,
        windows: Iterable[AthenaQueryWindow],
        query_builder: Callable[[AthenaQueryWindow], str],
    ) -> Iterable[AthenaQueryExecution]:
        """Yield completed query executions in window order."""
        window_iterator = iter(windows)
        try:
            first_window = next(window_iterator)
        except StopIteration:
            return

        max_workers = min(
            self.settings.query_prefetch_workers,
            self.settings.query_prefetch_max_windows,
        )
        executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="athena-prefetch",
        )
        pending: deque[PendingAthenaQuery] = deque()
        target_prefetch = self.settings.query_prefetch_min_windows
        last_consumption_elapsed: float | None = None
        windows_exhausted = False

        def submit_window(window: AthenaQueryWindow) -> None:
            pending.append(self.submit(executor, query=query_builder(window)))

        def fill_prefetch() -> None:
            nonlocal windows_exhausted
            while not windows_exhausted and len(pending) < target_prefetch:
                try:
                    submit_window(next(window_iterator))
                except StopIteration:
                    windows_exhausted = True

        try:
            submit_window(first_window)
            fill_prefetch()
            while pending:
                current = pending.popleft()
                wait_started = monotonic()
                execution = current.future.result()
                wait_elapsed = monotonic() - wait_started
                target_prefetch = self.policy.target(
                    current_target=target_prefetch,
                    query_elapsed=current.elapsed_seconds,
                    consumption_elapsed=last_consumption_elapsed,
                    wait_elapsed=wait_elapsed,
                )
                fill_prefetch()
                consumption_started = monotonic()
                yield execution
                last_consumption_elapsed = monotonic() - consumption_started
                target_prefetch = self.policy.target(
                    current_target=target_prefetch,
                    query_elapsed=current.elapsed_seconds,
                    consumption_elapsed=last_consumption_elapsed,
                    wait_elapsed=0.0,
                )
                fill_prefetch()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def submit(
        self,
        executor: ThreadPoolExecutor,
        *,
        query: str,
    ) -> PendingAthenaQuery:
        """Submit SQL to the worker pool and attach timing metadata."""
        pending = PendingAthenaQuery(
            future=executor.submit(self.executor, query),
            submitted_at=monotonic(),
        )
        pending.future.add_done_callback(lambda _: setattr(pending, "completed_at", monotonic()))
        return pending
