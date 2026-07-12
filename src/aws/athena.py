"""Athena-backed market data source."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from core.models import CurrencyPair
from core.sources import Candle, CandleGranularity, DataSource, Tick

from aws.athena_execution import AthenaAwsClientProvider, AthenaQueryExecutor
from aws.athena_mapping import AthenaMarketDataMapper
from aws.athena_models import AthenaDataSourceError, AthenaQueryExecution
from aws.athena_prefetch import AthenaPrefetchPolicy, AthenaQueryPrefetcher
from aws.athena_results import AthenaResultReader
from aws.athena_settings import AthenaSettings
from aws.athena_source_services import AthenaDataQueryService, AthenaSettingsResolver
from aws.athena_sql import AthenaQueryWindowBuilder, AthenaSqlBuilder


class AthenaDataSource(DataSource):
    """Read Polygon-style forex quotes from AWS Athena and yield Core models."""

    def __init__(
        self,
        *,
        settings: AthenaSettings | None = None,
        profile_name: str | None = None,
        region_name: str | None = None,
        account_id: str | None = None,
        database: str | None = None,
        table: str | None = None,
        minute_aggs_table: str | None = None,
        day_aggs_table: str | None = None,
        output_bucket: str | None = None,
        output_prefix: str | None = None,
        work_group: str | None = None,
        poll_interval_seconds: float | None = None,
        timeout_seconds: float | None = None,
        query_chunk_days: int | None = None,
        candle_query_chunk_days: int | None = None,
        query_prefetch_min_windows: int | None = None,
        query_prefetch_max_windows: int | None = None,
        query_prefetch_workers: int | None = None,
        query_prefetch_wait_target_seconds: float | None = None,
        athena_client: Any | None = None,
        s3_client: Any | None = None,
    ) -> None:
        self.settings = AthenaSettingsResolver.resolve(
            settings=settings,
            overrides={
                "profile_name": profile_name,
                "region_name": region_name,
                "account_id": account_id,
                "database": database,
                "table": table,
                "minute_aggs_table": minute_aggs_table,
                "day_aggs_table": day_aggs_table,
                "output_bucket": output_bucket,
                "output_prefix": output_prefix,
                "work_group": work_group,
                "poll_interval_seconds": poll_interval_seconds,
                "timeout_seconds": timeout_seconds,
                "query_chunk_days": query_chunk_days,
                "candle_query_chunk_days": candle_query_chunk_days,
                "query_prefetch_min_windows": query_prefetch_min_windows,
                "query_prefetch_max_windows": query_prefetch_max_windows,
                "query_prefetch_workers": query_prefetch_workers,
                "query_prefetch_wait_target_seconds": query_prefetch_wait_target_seconds,
            },
        )
        self.clients = AthenaAwsClientProvider(
            settings=self.settings,
            athena_client=athena_client,
            s3_client=s3_client,
        )
        self.sql = AthenaSqlBuilder(self.settings)
        self.window_builder = AthenaQueryWindowBuilder()
        self.query_executor = AthenaQueryExecutor(settings=self.settings, clients=self.clients)
        self.result_reader = AthenaResultReader(self.clients)
        self.mapper = AthenaMarketDataMapper(settings=self.settings, sql=self.sql)
        self.prefetch_policy = AthenaPrefetchPolicy(self.settings)
        self.prefetcher = AthenaQueryPrefetcher(
            settings=self.settings,
            executor=self.query_executor.execute,
            policy=self.prefetch_policy,
        )
        self.query_service = AthenaDataQueryService(
            window_builder=self.window_builder,
            prefetcher=self.prefetcher,
        )

    @classmethod
    def from_env(cls, **overrides: Any) -> AthenaDataSource:
        """Create a data source from ``.env`` and environment variables."""
        return cls(settings=AthenaSettings(), **overrides)

    def _raw_ticks(
        self,
        *,
        instrument: CurrencyPair,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> Iterable[Tick]:
        requested_instrument = CurrencyPair.of(instrument)
        executions = self.query_service.executions(
            start_at=start_at,
            end_at=end_at,
            chunk_days=self.settings.query_chunk_days,
            query_builder=lambda window: self.query_for_ticks(
                instrument=requested_instrument,
                start_at=window.start_at,
                end_at=window.end_at,
            ),
        )
        for execution in executions:
            for row in self.result_reader.rows(execution.output_location):
                yield self.mapper.tick(
                    row,
                    requested_instrument,
                    execution_id=execution.execution_id,
                )

    def candles(
        self,
        *,
        instrument: CurrencyPair,
        granularity: CandleGranularity,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> Iterable[Candle]:
        """Yield candles from Athena aggregate tables."""
        requested_instrument = CurrencyPair.of(instrument)
        requested_granularity = CandleGranularity(granularity)
        executions = self.query_service.executions(
            start_at=start_at,
            end_at=end_at,
            chunk_days=self.settings.candle_query_chunk_days,
            query_builder=lambda window: self.query_for_candles(
                instrument=requested_instrument,
                granularity=requested_granularity,
                start_at=window.start_at,
                end_at=window.end_at,
            ),
        )
        for execution in executions:
            for row in self.result_reader.rows(execution.output_location):
                yield self.mapper.candle(
                    row,
                    requested_instrument,
                    granularity=requested_granularity,
                    execution_id=execution.execution_id,
                )

    def query_for_ticks(
        self,
        *,
        instrument: CurrencyPair,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> str:
        """Build the Athena SQL used to fetch ticks."""
        return self.sql.ticks(instrument=instrument, start_at=start_at, end_at=end_at)

    def query_for_candles(
        self,
        *,
        instrument: CurrencyPair,
        granularity: CandleGranularity,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> str:
        """Build the Athena SQL used to fetch aggregate candles."""
        return self.sql.candles(
            instrument=instrument,
            granularity=granularity,
            start_at=start_at,
            end_at=end_at,
        )

    def close(self) -> None:
        """Release lazily-created AWS clients when possible."""
        self.clients.close()

    @property
    def client(self) -> Any:
        """Return the Athena client."""
        return self.clients.athena

    @property
    def s3_client(self) -> Any:
        """Return the S3 client used to stream Athena result objects."""
        return self.clients.s3


__all__ = [
    "AthenaDataSource",
    "AthenaDataSourceError",
    "AthenaQueryExecution",
    "AthenaSettings",
]
