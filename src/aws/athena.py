"""Athena-backed market data source."""

from __future__ import annotations

import csv
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from math import ceil
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from urllib.parse import urlparse

import boto3
from core.models import CurrencyPair, Metadata, Money
from core.sources import Candle, CandleGranularity, DataSource, Tick
from pydantic import AliasChoices, Field, PositiveFloat, PositiveInt, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PACKAGE_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class AthenaDataSourceError(RuntimeError):
    """Raised when Athena cannot return a valid tick stream."""


@dataclass(frozen=True, slots=True)
class AthenaQueryExecution:
    """Completed Athena query and the S3 object containing its results."""

    execution_id: str
    output_location: str


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


class AthenaSettings(BaseSettings):
    """Configuration for querying Polygon-style forex quotes in Athena."""

    model_config = SettingsConfigDict(
        env_file=(_PACKAGE_ENV_FILE, ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    profile_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AWS_PROFILE", "AWS_PROFILE_NAME"),
    )
    region_name: str = Field(
        default="us-west-2",
        validation_alias=AliasChoices("AWS_REGION", "AWS_DEFAULT_REGION"),
    )
    account_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AWS_ACCOUNT_ID"),
    )
    database: str = Field(
        default="forex_hist_data_db",
        validation_alias=AliasChoices("AWS_ATHENA_DATABASE", "ATHENA_DATABASE"),
    )
    table: str = Field(
        default="quotes",
        validation_alias=AliasChoices("AWS_ATHENA_TABLE", "ATHENA_TABLE"),
    )
    minute_aggs_table: str = Field(
        default="minute_aggs",
        validation_alias=AliasChoices(
            "AWS_ATHENA_MINUTE_AGGS_TABLE",
            "ATHENA_MINUTE_AGGS_TABLE",
        ),
    )
    day_aggs_table: str = Field(
        default="day_aggs",
        validation_alias=AliasChoices(
            "AWS_ATHENA_DAY_AGGS_TABLE",
            "ATHENA_DAY_AGGS_TABLE",
        ),
    )
    output_bucket: str = Field(
        default="aws-athena-query-results-789121567207-us-west-2",
        validation_alias=AliasChoices(
            "AWS_ATHENA_OUTPUT_BUCKET",
            "ATHENA_OUTPUT_BUCKET",
        ),
    )
    output_prefix: str = Field(
        default="athena-query-results/",
        validation_alias=AliasChoices(
            "AWS_ATHENA_OUTPUT_PREFIX",
            "ATHENA_OUTPUT_PREFIX",
        ),
    )
    work_group: str | None = Field(
        default=None,
        validation_alias=AliasChoices("AWS_ATHENA_WORK_GROUP", "ATHENA_WORK_GROUP"),
    )
    poll_interval_seconds: PositiveFloat = Field(
        default=1.0,
        validation_alias=AliasChoices(
            "AWS_ATHENA_POLL_INTERVAL_SECONDS",
            "ATHENA_POLL_INTERVAL_SECONDS",
        ),
    )
    timeout_seconds: PositiveFloat = Field(
        default=300.0,
        validation_alias=AliasChoices("AWS_ATHENA_TIMEOUT_SECONDS", "ATHENA_TIMEOUT_SECONDS"),
    )
    query_chunk_days: PositiveInt = Field(
        default=1,
        validation_alias=AliasChoices("AWS_ATHENA_QUERY_CHUNK_DAYS", "ATHENA_QUERY_CHUNK_DAYS"),
    )
    candle_query_chunk_days: PositiveInt = Field(
        default=31,
        validation_alias=AliasChoices(
            "AWS_ATHENA_CANDLE_QUERY_CHUNK_DAYS",
            "ATHENA_CANDLE_QUERY_CHUNK_DAYS",
        ),
    )
    query_prefetch_min_windows: PositiveInt = Field(
        default=3,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_MIN_WINDOWS",
            "ATHENA_QUERY_PREFETCH_MIN_WINDOWS",
        ),
    )
    query_prefetch_max_windows: PositiveInt = Field(
        default=6,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_MAX_WINDOWS",
            "ATHENA_QUERY_PREFETCH_MAX_WINDOWS",
        ),
    )
    query_prefetch_workers: PositiveInt = Field(
        default=4,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_WORKERS",
            "ATHENA_QUERY_PREFETCH_WORKERS",
        ),
    )
    query_prefetch_wait_target_seconds: PositiveFloat = Field(
        default=0.5,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_WAIT_TARGET_SECONDS",
            "ATHENA_QUERY_PREFETCH_WAIT_TARGET_SECONDS",
        ),
    )

    @model_validator(mode="after")
    def _normalize_and_validate(self) -> AthenaSettings:
        prefix = self.output_prefix.strip("/")
        if prefix:
            prefix = f"{prefix}/"
        object.__setattr__(self, "output_prefix", prefix)
        if self.query_prefetch_min_windows > self.query_prefetch_max_windows:
            raise ValueError(
                "query_prefetch_min_windows must be less than or equal to "
                "query_prefetch_max_windows"
            )
        return self

    @property
    def output_location(self) -> str:
        """Return the S3 location Athena writes query results to."""
        return f"s3://{self.output_bucket}/{self.output_prefix}"


class AthenaDataSource(DataSource):
    """Read Polygon-style forex quotes from AWS Athena and yield Core ticks."""

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
        resolved = settings or AthenaSettings()
        updates = {
            key: value
            for key, value in {
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
            }.items()
            if value is not None
        }
        self.settings = AthenaSettings.model_validate({**resolved.model_dump(), **updates})
        self._client = athena_client
        self._s3_client = s3_client

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
        executions = self._query_executions(
            start_at=start_at,
            end_at=end_at,
            chunk_days=self.settings.query_chunk_days,
            query_builder=lambda window: self.query_for_ticks(
                instrument=requested_instrument,
                start_at=window[0],
                end_at=window[1],
            ),
        )
        for execution in executions:
            for row in self._query_rows(execution.output_location):
                yield self._tick_from_row(
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
        executions = self._query_executions(
            start_at=start_at,
            end_at=end_at,
            chunk_days=self.settings.candle_query_chunk_days,
            query_builder=lambda window: self.query_for_candles(
                instrument=requested_instrument,
                granularity=requested_granularity,
                start_at=window[0],
                end_at=window[1],
            ),
        )
        for execution in executions:
            for row in self._query_rows(execution.output_location):
                yield self._candle_from_row(
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
        ticker = self._ticker_for(instrument)
        predicates = self._predicates(
            ticker=ticker,
            start_at=start_at,
            end_at=end_at,
            timestamp_column="participant_timestamp",
        )
        where = " AND ".join(predicates)
        return (
            "SELECT ticker, bid_price, ask_price, participant_timestamp "
            f"FROM {self._qualified_table(self.settings.table)} "
            f"WHERE {where} "
            "ORDER BY participant_timestamp ASC"
        )

    def query_for_candles(
        self,
        *,
        instrument: CurrencyPair,
        granularity: CandleGranularity,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> str:
        """Build the Athena SQL used to fetch aggregate candles."""
        requested_granularity = CandleGranularity(granularity)
        table = self._table_for_candle_granularity(requested_granularity)
        ticker = self._ticker_for(instrument)
        predicates = self._predicates(
            ticker=ticker,
            start_at=start_at,
            end_at=end_at,
            timestamp_column="window_start",
        )
        where = " AND ".join(predicates)
        return (
            'SELECT ticker, volume, "open" AS "open", "close" AS "close", '
            "high, low, window_start, transactions "
            f"FROM {self._qualified_table(table)} "
            f"WHERE {where} "
            "ORDER BY window_start ASC"
        )

    def close(self) -> None:
        """Release lazily-created AWS clients when possible."""
        for client in (self._client, self._s3_client):
            close = getattr(client, "close", None)
            if callable(close):
                close()

    @property
    def client(self) -> Any:
        """Return the Athena client, creating it lazily."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    @property
    def s3_client(self) -> Any:
        """Return the S3 client used to stream Athena result objects."""
        if self._s3_client is None:
            self._s3_client = self._create_s3_client()
        return self._s3_client

    def _create_client(self) -> Any:
        return self._create_session().client("athena")

    def _create_s3_client(self) -> Any:
        return self._create_session().client("s3")

    def _create_session(self) -> Any:
        session = boto3.Session(
            profile_name=self.settings.profile_name,
            region_name=self.settings.region_name,
        )
        return session

    def _query_executions(
        self,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
        chunk_days: int,
        query_builder: Callable[[tuple[datetime | None, datetime | None]], str],
    ) -> Iterable[AthenaQueryExecution]:
        windows = iter(
            self._query_windows(start_at=start_at, end_at=end_at, chunk_days=chunk_days)
        )
        try:
            first_window = next(windows)
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

        def submit_window(window: tuple[datetime | None, datetime | None]) -> None:
            pending.append(self._submit_query(executor, query=query_builder(window)))

        def fill_prefetch() -> None:
            nonlocal windows_exhausted
            while not windows_exhausted and len(pending) < target_prefetch:
                try:
                    submit_window(next(windows))
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
                target_prefetch = self._adaptive_prefetch_target(
                    current_target=target_prefetch,
                    query_elapsed=current.elapsed_seconds,
                    consumption_elapsed=last_consumption_elapsed,
                    wait_elapsed=wait_elapsed,
                )
                fill_prefetch()
                consumption_started = monotonic()
                yield execution
                last_consumption_elapsed = monotonic() - consumption_started
                target_prefetch = self._adaptive_prefetch_target(
                    current_target=target_prefetch,
                    query_elapsed=current.elapsed_seconds,
                    consumption_elapsed=last_consumption_elapsed,
                    wait_elapsed=0.0,
                )
                fill_prefetch()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _submit_query(
        self,
        executor: ThreadPoolExecutor,
        *,
        query: str,
    ) -> PendingAthenaQuery:
        pending = PendingAthenaQuery(
            future=executor.submit(self._execute_query, query),
            submitted_at=monotonic(),
        )
        pending.future.add_done_callback(lambda _: setattr(pending, "completed_at", monotonic()))
        return pending

    def _adaptive_prefetch_target(
        self,
        *,
        current_target: int,
        query_elapsed: float,
        consumption_elapsed: float | None,
        wait_elapsed: float,
    ) -> int:
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

    def _execute_query(self, query: str) -> AthenaQueryExecution:
        execution_id = self._start_query(query)
        output_location = self._wait_for_query(execution_id)
        return AthenaQueryExecution(
            execution_id=execution_id,
            output_location=output_location,
        )

    def _start_query(self, query: str) -> str:
        request: dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": self.settings.database},
            "ResultConfiguration": {"OutputLocation": self.settings.output_location},
        }
        if self.settings.work_group is not None:
            request["WorkGroup"] = self.settings.work_group
        response = self.client.start_query_execution(**request)
        execution_id = response.get("QueryExecutionId")
        if not isinstance(execution_id, str) or not execution_id:
            raise AthenaDataSourceError("Athena did not return a query execution id")
        return execution_id

    def _wait_for_query(self, execution_id: str) -> str:
        deadline = monotonic() + self.settings.timeout_seconds
        while True:
            response = self.client.get_query_execution(QueryExecutionId=execution_id)
            query_execution = response.get("QueryExecution", {})
            status = query_execution.get("Status", {})
            state = status.get("State")
            if state == "SUCCEEDED":
                return self._query_output_location(query_execution, execution_id=execution_id)
            if state in {"FAILED", "CANCELLED"}:
                reason = status.get("StateChangeReason", "")
                message = f"Athena query {execution_id} {state.lower()}"
                if reason:
                    message = f"{message}: {reason}"
                raise AthenaDataSourceError(message)
            if monotonic() >= deadline:
                raise AthenaDataSourceError(f"Athena query timed out: {execution_id}")
            sleep(self.settings.poll_interval_seconds)

    def _query_rows(self, output_location: str) -> Iterable[dict[str, str]]:
        bucket, key = self._s3_location(output_location)
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        if body is None:
            raise AthenaDataSourceError(f"Athena result object has no body: {output_location}")
        lines = self._decoded_lines(body)
        reader = csv.DictReader(lines)
        yield from reader

    def _tick_from_row(
        self,
        row: Mapping[str, str],
        instrument: CurrencyPair,
        *,
        execution_id: str,
    ) -> Tick:
        timestamp = self._participant_timestamp(row["participant_timestamp"])
        metadata = {
            "source": "athena",
            "ticker": row.get("ticker", ""),
            "query_execution_id": execution_id,
        }
        if self.settings.account_id is not None:
            metadata["aws_account_id"] = self.settings.account_id
        return Tick(
            instrument=instrument,
            timestamp=timestamp,
            bid=Money.of(row["bid_price"], instrument.quote),
            ask=Money.of(row["ask_price"], instrument.quote),
            metadata=Metadata.of(**metadata),
        )

    def _candle_from_row(
        self,
        row: Mapping[str, str],
        instrument: CurrencyPair,
        *,
        granularity: CandleGranularity,
        execution_id: str,
    ) -> Candle:
        timestamp = self._participant_timestamp(row["window_start"])
        metadata = {
            "source": "athena",
            "ticker": row.get("ticker", ""),
            "query_execution_id": execution_id,
            "athena_table": self._table_for_candle_granularity(granularity),
            "transactions": row.get("transactions", ""),
        }
        if self.settings.account_id is not None:
            metadata["aws_account_id"] = self.settings.account_id
        return Candle(
            instrument=instrument,
            timestamp=timestamp,
            granularity=granularity,
            open=Money.of(row["open"], instrument.quote),
            high=Money.of(row["high"], instrument.quote),
            low=Money.of(row["low"], instrument.quote),
            close=Money.of(row["close"], instrument.quote),
            volume=self._optional_int(row.get("volume")),
            metadata=Metadata.of(**metadata),
        )

    def _predicates(
        self,
        *,
        ticker: str,
        start_at: datetime | None,
        end_at: datetime | None,
        timestamp_column: str,
    ) -> tuple[str, ...]:
        predicates: list[str] = []
        partition_filter = self._partition_filter(start_at=start_at, end_at=end_at)
        if partition_filter:
            predicates.append(partition_filter)
        predicates.append(f"ticker = {self._sql_string(ticker)}")
        if start_at is not None:
            predicates.append(
                f"{self._identifier(timestamp_column)} >= {self._epoch_nanoseconds(start_at)}"
            )
        if end_at is not None:
            predicates.append(
                f"{self._identifier(timestamp_column)} <= {self._epoch_nanoseconds(end_at)}"
            )
        return tuple(predicates)

    def _query_windows(
        self,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
        chunk_days: int | None = None,
    ) -> Iterable[tuple[datetime | None, datetime | None]]:
        if start_at is None or end_at is None:
            yield start_at, end_at
            return

        start = self._utc(start_at)
        end = self._utc(end_at)
        if start > end:
            raise ValueError("start_at must be earlier than or equal to end_at")

        chunk_start = start
        while chunk_start <= end:
            next_chunk_start = self._next_chunk_start(
                chunk_start,
                chunk_days=chunk_days or self.settings.query_chunk_days,
            )
            chunk_end = min(end, next_chunk_start - timedelta(microseconds=1))
            yield chunk_start, chunk_end
            chunk_start = next_chunk_start

    def _next_chunk_start(self, value: datetime, *, chunk_days: int) -> datetime:
        next_date = value.date() + timedelta(days=chunk_days)
        return datetime.combine(next_date, datetime.min.time(), tzinfo=UTC)

    def _partition_filter(
        self,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
    ) -> str:
        if start_at is None and end_at is None:
            return ""
        start_date = self._date_for(start_at or end_at)
        end_date = self._date_for(end_at or start_at)
        if start_date > end_date:
            raise ValueError("start_at must be earlier than or equal to end_at")
        filters = [
            (
                f"(year = {self._sql_string(f'{current.year:04d}')} "
                f"AND month = {self._sql_string(f'{current.month:02d}')} "
                f"AND day = {self._sql_string(f'{current.day:02d}')})"
            )
            for current in self._dates(start_date, end_date)
        ]
        return f"({' OR '.join(filters)})"

    def _qualified_table(self, table: str) -> str:
        return f"{self._identifier(self.settings.database)}.{self._identifier(table)}"

    def _table_for_candle_granularity(self, granularity: CandleGranularity) -> str:
        match CandleGranularity(granularity):
            case CandleGranularity.MINUTE_1:
                return self.settings.minute_aggs_table
            case CandleGranularity.DAY:
                return self.settings.day_aggs_table
            case _:
                raise AthenaDataSourceError(
                    f"unsupported Athena candle granularity: {granularity}"
                )

    @staticmethod
    def _ticker_for(instrument: CurrencyPair) -> str:
        return f"C:{instrument.base}-{instrument.quote}"

    @staticmethod
    def _participant_timestamp(value: str) -> datetime:
        text = str(value).strip()
        try:
            raw_epoch = int(text)
        except ValueError:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        seconds, nanoseconds = divmod(raw_epoch, 1_000_000_000)
        return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=nanoseconds // 1_000)

    @staticmethod
    def _epoch_nanoseconds(value: datetime) -> int:
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = AthenaDataSource._utc(value) - epoch
        return (
            delta.days * 86_400_000_000_000
            + delta.seconds * 1_000_000_000
            + delta.microseconds * 1_000
        )

    @staticmethod
    def _date_for(value: datetime | None) -> date:
        if value is None:
            raise ValueError("date value is required")
        return AthenaDataSource._utc(value).date()

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @staticmethod
    def _dates(start: date, end: date) -> Iterable[date]:
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)

    @staticmethod
    def _query_output_location(
        query_execution: Mapping[str, Any],
        *,
        execution_id: str,
    ) -> str:
        result_configuration = query_execution.get("ResultConfiguration", {})
        output_location = result_configuration.get("OutputLocation")
        if isinstance(output_location, str) and output_location:
            return output_location
        raise AthenaDataSourceError(
            f"Athena query {execution_id} did not return an output location"
        )

    @staticmethod
    def _s3_location(output_location: str) -> tuple[str, str]:
        parsed = urlparse(output_location)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
            raise AthenaDataSourceError(f"invalid Athena S3 output location: {output_location}")
        return parsed.netloc, parsed.path.lstrip("/")

    @staticmethod
    def _decoded_lines(body: Any) -> Iterator[str]:
        for line in body.iter_lines():
            yield line.decode("utf-8")

    @staticmethod
    def _identifier(value: str) -> str:
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    @staticmethod
    def _sql_string(value: str) -> str:
        return f"'{value.replace(chr(39), chr(39) * 2)}'"

    @staticmethod
    def _optional_int(value: str | None) -> int | None:
        if value is None or value == "":
            return None
        return int(value)
