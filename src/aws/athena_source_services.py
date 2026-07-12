"""Support services for Athena-backed data sources."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

from aws.athena_models import AthenaQueryExecution
from aws.athena_prefetch import AthenaQueryPrefetcher
from aws.athena_settings import AthenaSettings
from aws.athena_sql import AthenaQueryWindow, AthenaQueryWindowBuilder


class AthenaSettingsResolver:
    """Resolve Athena settings from environment values and constructor overrides."""

    @classmethod
    def resolve(
        cls,
        *,
        settings: AthenaSettings | None,
        overrides: dict[str, Any],
    ) -> AthenaSettings:
        """Return resolved settings."""
        resolved = settings or AthenaSettings()
        updates = {key: value for key, value in overrides.items() if value is not None}
        return AthenaSettings.model_validate({**resolved.model_dump(), **updates})


class AthenaDataQueryService:
    """Execute Athena queries over time windows."""

    def __init__(
        self,
        *,
        window_builder: AthenaQueryWindowBuilder,
        prefetcher: AthenaQueryPrefetcher,
    ) -> None:
        self.window_builder = window_builder
        self.prefetcher = prefetcher

    def executions(
        self,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
        chunk_days: int,
        query_builder: Callable[[AthenaQueryWindow], str],
    ) -> Iterable[AthenaQueryExecution]:
        """Yield query executions for a windowed market-data request."""
        windows = self.window_builder.windows(
            start_at=start_at,
            end_at=end_at,
            chunk_days=chunk_days,
        )
        yield from self.prefetcher.executions(windows=windows, query_builder=query_builder)
