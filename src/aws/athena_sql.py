"""SQL construction for Athena market data queries."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from core.models import CurrencyPair
from core.sources import CandleGranularity

from aws.athena_models import AthenaDataSourceError
from aws.athena_settings import AthenaSettings


@dataclass(frozen=True, slots=True)
class AthenaQueryWindow:
    """Inclusive time window for a single Athena query."""

    start_at: datetime | None
    end_at: datetime | None


class AthenaTimeCodec:
    """Convert between Core datetimes and Athena timestamp fields."""

    @classmethod
    def participant_timestamp(cls, value: str) -> datetime:
        """Parse Athena nanosecond timestamps or ISO timestamps as UTC datetimes."""
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

    @classmethod
    def epoch_nanoseconds(cls, value: datetime) -> int:
        """Convert a datetime to Athena's nanosecond epoch representation."""
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = cls.utc(value) - epoch
        return (
            delta.days * 86_400_000_000_000
            + delta.seconds * 1_000_000_000
            + delta.microseconds * 1_000
        )

    @classmethod
    def date_for(cls, value: datetime | None) -> date:
        """Return the UTC date for a required datetime."""
        if value is None:
            raise ValueError("date value is required")
        return cls.utc(value).date()

    @classmethod
    def utc(cls, value: datetime) -> datetime:
        """Normalize naive or aware datetimes to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class AthenaSqlSyntax:
    """Escape SQL identifiers and literal strings."""

    @classmethod
    def identifier(cls, value: str) -> str:
        """Quote an Athena identifier."""
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    @classmethod
    def string(cls, value: str) -> str:
        """Quote a SQL string literal."""
        return f"'{value.replace(chr(39), chr(39) * 2)}'"


class AthenaQueryWindowBuilder:
    """Create ordered query windows for a backtest period."""

    def windows(
        self,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
        chunk_days: int,
    ) -> Iterable[AthenaQueryWindow]:
        """Yield inclusive windows split at UTC day boundaries."""
        if start_at is None or end_at is None:
            yield AthenaQueryWindow(start_at=start_at, end_at=end_at)
            return

        start = AthenaTimeCodec.utc(start_at)
        end = AthenaTimeCodec.utc(end_at)
        if start > end:
            raise ValueError("start_at must be earlier than or equal to end_at")

        chunk_start = start
        while chunk_start <= end:
            next_chunk_start = self.next_chunk_start(chunk_start, chunk_days=chunk_days)
            chunk_end = min(end, next_chunk_start - timedelta(microseconds=1))
            yield AthenaQueryWindow(start_at=chunk_start, end_at=chunk_end)
            chunk_start = next_chunk_start

    @classmethod
    def next_chunk_start(cls, value: datetime, *, chunk_days: int) -> datetime:
        """Return the next UTC day boundary after a chunk."""
        next_date = value.date() + timedelta(days=chunk_days)
        return datetime.combine(next_date, datetime.min.time(), tzinfo=UTC)


class AthenaSqlBuilder:
    """Build Athena SQL for tick and aggregate candle queries."""

    def __init__(self, settings: AthenaSettings) -> None:
        self.settings = settings

    def ticks(
        self,
        *,
        instrument: CurrencyPair,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> str:
        """Build the Athena SQL used to fetch ticks."""
        ticker = self.ticker_for(instrument)
        predicates = self.predicates(
            ticker=ticker,
            start_at=start_at,
            end_at=end_at,
            timestamp_column="participant_timestamp",
        )
        where = " AND ".join(predicates)
        return (
            "SELECT ticker, bid_price, ask_price, participant_timestamp "
            f"FROM {self.qualified_table(self.settings.table)} "
            f"WHERE {where} "
            "ORDER BY participant_timestamp ASC"
        )

    def candles(
        self,
        *,
        instrument: CurrencyPair,
        granularity: CandleGranularity,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> str:
        """Build the Athena SQL used to fetch aggregate candles."""
        requested_granularity = CandleGranularity(granularity)
        table = self.table_for_candle_granularity(requested_granularity)
        ticker = self.ticker_for(instrument)
        predicates = self.predicates(
            ticker=ticker,
            start_at=start_at,
            end_at=end_at,
            timestamp_column="window_start",
        )
        where = " AND ".join(predicates)
        return (
            'SELECT ticker, volume, "open" AS "open", "close" AS "close", '
            "high, low, window_start, transactions "
            f"FROM {self.qualified_table(table)} "
            f"WHERE {where} "
            "ORDER BY window_start ASC"
        )

    def predicates(
        self,
        *,
        ticker: str,
        start_at: datetime | None,
        end_at: datetime | None,
        timestamp_column: str,
    ) -> tuple[str, ...]:
        """Return ordered SQL predicates for partition, ticker, and time range."""
        predicates: list[str] = []
        partition_filter = self.partition_filter(start_at=start_at, end_at=end_at)
        if partition_filter:
            predicates.append(partition_filter)
        predicates.append(f"ticker = {AthenaSqlSyntax.string(ticker)}")
        if start_at is not None:
            predicates.append(
                f"{AthenaSqlSyntax.identifier(timestamp_column)} >= "
                f"{AthenaTimeCodec.epoch_nanoseconds(start_at)}"
            )
        if end_at is not None:
            predicates.append(
                f"{AthenaSqlSyntax.identifier(timestamp_column)} <= "
                f"{AthenaTimeCodec.epoch_nanoseconds(end_at)}"
            )
        return tuple(predicates)

    def partition_filter(
        self,
        *,
        start_at: datetime | None,
        end_at: datetime | None,
    ) -> str:
        """Build the partition filter for the inclusive date range."""
        if start_at is None and end_at is None:
            return ""
        start_date = AthenaTimeCodec.date_for(start_at or end_at)
        end_date = AthenaTimeCodec.date_for(end_at or start_at)
        if start_date > end_date:
            raise ValueError("start_at must be earlier than or equal to end_at")
        filters = [
            (
                f"(year = {AthenaSqlSyntax.string(f'{current.year:04d}')} "
                f"AND month = {AthenaSqlSyntax.string(f'{current.month:02d}')} "
                f"AND day = {AthenaSqlSyntax.string(f'{current.day:02d}')})"
            )
            for current in self.dates(start_date, end_date)
        ]
        return f"({' OR '.join(filters)})"

    def qualified_table(self, table: str) -> str:
        """Return a fully-qualified Athena table identifier."""
        database = AthenaSqlSyntax.identifier(self.settings.database)
        table_name = AthenaSqlSyntax.identifier(table)
        return f"{database}.{table_name}"

    def table_for_candle_granularity(self, granularity: CandleGranularity) -> str:
        """Map Core candle granularity to Athena aggregate tables."""
        match CandleGranularity(granularity):
            case CandleGranularity.MINUTE_1:
                return self.settings.minute_aggs_table
            case CandleGranularity.DAY:
                return self.settings.day_aggs_table
            case _:
                raise AthenaDataSourceError(f"unsupported Athena candle granularity: {granularity}")

    @classmethod
    def ticker_for(cls, instrument: CurrencyPair) -> str:
        """Return the Polygon-style ticker for a Core currency pair."""
        return f"C:{instrument.base}-{instrument.quote}"

    @classmethod
    def dates(cls, start: date, end: date) -> Iterable[date]:
        """Yield each date in an inclusive date range."""
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)
