"""Athena-backed market data source."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import Any

import boto3
from core.models import CurrencyPair, Metadata, Money
from core.sources import DataSource, Tick
from pydantic import AliasChoices, Field, PositiveFloat, PositiveInt, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PACKAGE_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class AthenaDataSourceError(RuntimeError):
    """Raised when Athena cannot return a valid tick stream."""


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
    page_size: PositiveInt = Field(
        default=1000,
        le=1000,
        validation_alias=AliasChoices("AWS_ATHENA_PAGE_SIZE", "ATHENA_PAGE_SIZE"),
    )
    query_chunk_days: PositiveInt = Field(
        default=1,
        validation_alias=AliasChoices("AWS_ATHENA_QUERY_CHUNK_DAYS", "ATHENA_QUERY_CHUNK_DAYS"),
    )

    @model_validator(mode="after")
    def _normalize_output_prefix(self) -> AthenaSettings:
        prefix = self.output_prefix.strip("/")
        if prefix:
            prefix = f"{prefix}/"
        object.__setattr__(self, "output_prefix", prefix)
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
        output_bucket: str | None = None,
        output_prefix: str | None = None,
        work_group: str | None = None,
        poll_interval_seconds: float | None = None,
        timeout_seconds: float | None = None,
        page_size: int | None = None,
        query_chunk_days: int | None = None,
        athena_client: Any | None = None,
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
                "output_bucket": output_bucket,
                "output_prefix": output_prefix,
                "work_group": work_group,
                "poll_interval_seconds": poll_interval_seconds,
                "timeout_seconds": timeout_seconds,
                "page_size": page_size,
                "query_chunk_days": query_chunk_days,
            }.items()
            if value is not None
        }
        self.settings = AthenaSettings.model_validate({**resolved.model_dump(), **updates})
        self._client = athena_client

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
        for window_start, window_end in self._query_windows(start_at=start_at, end_at=end_at):
            query = self.query_for_ticks(
                instrument=requested_instrument,
                start_at=window_start,
                end_at=window_end,
            )
            execution_id = self._start_query(query)
            self._wait_for_query(execution_id)
            for row in self._query_rows(execution_id):
                yield self._tick_from_row(row, requested_instrument, execution_id=execution_id)

    def query_for_ticks(
        self,
        *,
        instrument: CurrencyPair,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> str:
        """Build the Athena SQL used to fetch ticks."""
        ticker = self._ticker_for(instrument)
        predicates = self._predicates(ticker=ticker, start_at=start_at, end_at=end_at)
        where = " AND ".join(predicates)
        return (
            "SELECT ticker, bid_price, ask_price, participant_timestamp "
            f"FROM {self._qualified_table()} "
            f"WHERE {where} "
            "ORDER BY participant_timestamp ASC"
        )

    def close(self) -> None:
        """Release the lazily-created Athena client when possible."""
        close = getattr(self._client, "close", None)
        if callable(close):
            close()

    @property
    def client(self) -> Any:
        """Return the Athena client, creating it lazily."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def _create_client(self) -> Any:
        session = boto3.Session(
            profile_name=self.settings.profile_name,
            region_name=self.settings.region_name,
        )
        return session.client("athena")

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

    def _wait_for_query(self, execution_id: str) -> None:
        deadline = monotonic() + self.settings.timeout_seconds
        while True:
            response = self.client.get_query_execution(QueryExecutionId=execution_id)
            status = response.get("QueryExecution", {}).get("Status", {})
            state = status.get("State")
            if state == "SUCCEEDED":
                return
            if state in {"FAILED", "CANCELLED"}:
                reason = status.get("StateChangeReason", "")
                message = f"Athena query {execution_id} {state.lower()}"
                if reason:
                    message = f"{message}: {reason}"
                raise AthenaDataSourceError(message)
            if monotonic() >= deadline:
                raise AthenaDataSourceError(f"Athena query timed out: {execution_id}")
            sleep(self.settings.poll_interval_seconds)

    def _query_rows(self, execution_id: str) -> Iterable[dict[str, str]]:
        request: dict[str, Any] = {
            "QueryExecutionId": execution_id,
            "MaxResults": self.settings.page_size,
        }
        while True:
            response = self.client.get_query_results(**request)
            columns = self._result_columns(response)
            for row in response.get("ResultSet", {}).get("Rows", ()):
                values = self._row_values(row)
                if values == columns:
                    continue
                yield dict(zip(columns, values, strict=False))
            next_token = response.get("NextToken")
            if not next_token:
                return
            request["NextToken"] = next_token

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

    def _predicates(
        self,
        *,
        ticker: str,
        start_at: datetime | None,
        end_at: datetime | None,
    ) -> tuple[str, ...]:
        predicates: list[str] = []
        partition_filter = self._partition_filter(start_at=start_at, end_at=end_at)
        if partition_filter:
            predicates.append(partition_filter)
        predicates.append(f"ticker = {self._sql_string(ticker)}")
        if start_at is not None:
            predicates.append(f"participant_timestamp >= {self._epoch_nanoseconds(start_at)}")
        if end_at is not None:
            predicates.append(f"participant_timestamp <= {self._epoch_nanoseconds(end_at)}")
        return tuple(predicates)

    def _query_windows(
        self,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
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
            next_chunk_start = self._next_chunk_start(chunk_start)
            chunk_end = min(end, next_chunk_start - timedelta(microseconds=1))
            yield chunk_start, chunk_end
            chunk_start = next_chunk_start

    def _next_chunk_start(self, value: datetime) -> datetime:
        next_date = value.date() + timedelta(days=self.settings.query_chunk_days)
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

    def _qualified_table(self) -> str:
        return f"{self._identifier(self.settings.database)}.{self._identifier(self.settings.table)}"

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
    def _result_columns(response: Mapping[str, Any]) -> list[str]:
        columns = (
            response.get("ResultSet", {})
            .get("ResultSetMetadata", {})
            .get(
                "ColumnInfo",
                (),
            )
        )
        names = [str(column.get("Name", "")) for column in columns]
        if not names or any(not name for name in names):
            raise AthenaDataSourceError("Athena result is missing column metadata")
        return names

    @staticmethod
    def _row_values(row: Mapping[str, Any]) -> list[str]:
        return [str(cell.get("VarCharValue", "")) for cell in row.get("Data", ())]

    @staticmethod
    def _identifier(value: str) -> str:
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    @staticmethod
    def _sql_string(value: str) -> str:
        return f"'{value.replace(chr(39), chr(39) * 2)}'"
