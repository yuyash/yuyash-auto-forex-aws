"""Map Athena result rows to Core market data models."""

from __future__ import annotations

from collections.abc import Mapping

from core.models import CurrencyPair, Metadata, Money
from core.sources import Candle, CandleGranularity, Tick

from aws.athena_settings import AthenaSettings
from aws.athena_sql import AthenaSqlBuilder, AthenaTimeCodec


class AthenaMarketDataMapper:
    """Convert Athena CSV rows into Core market data objects."""

    def __init__(
        self,
        *,
        settings: AthenaSettings,
        sql: AthenaSqlBuilder,
    ) -> None:
        self.settings = settings
        self.sql = sql

    def tick(
        self,
        row: Mapping[str, str],
        instrument: CurrencyPair,
        *,
        execution_id: str,
    ) -> Tick:
        """Map an Athena quote row to a Core tick."""
        timestamp = AthenaTimeCodec.participant_timestamp(row["participant_timestamp"])
        metadata = self.metadata(
            ticker=row.get("ticker", ""),
            execution_id=execution_id,
        )
        return Tick(
            instrument=instrument,
            timestamp=timestamp,
            bid=Money.of(row["bid_price"], instrument.quote),
            ask=Money.of(row["ask_price"], instrument.quote),
            metadata=metadata,
        )

    def candle(
        self,
        row: Mapping[str, str],
        instrument: CurrencyPair,
        *,
        granularity: CandleGranularity,
        execution_id: str,
    ) -> Candle:
        """Map an Athena aggregate row to a Core candle."""
        timestamp = AthenaTimeCodec.participant_timestamp(row["window_start"])
        metadata = self.metadata(
            ticker=row.get("ticker", ""),
            execution_id=execution_id,
            extra={
                "athena_table": self.sql.table_for_candle_granularity(granularity),
                "transactions": row.get("transactions", ""),
            },
        )
        return Candle(
            instrument=instrument,
            timestamp=timestamp,
            granularity=granularity,
            open=Money.of(row["open"], instrument.quote),
            high=Money.of(row["high"], instrument.quote),
            low=Money.of(row["low"], instrument.quote),
            close=Money.of(row["close"], instrument.quote),
            volume=self.optional_int(row.get("volume")),
            metadata=metadata,
        )

    def metadata(
        self,
        *,
        ticker: str,
        execution_id: str,
        extra: Mapping[str, str] | None = None,
    ) -> Metadata:
        """Build shared metadata attached to Athena-sourced models."""
        values = {
            "source": "athena",
            "ticker": ticker,
            "query_execution_id": execution_id,
        }
        if self.settings.account_id is not None:
            values["aws_account_id"] = self.settings.account_id
        if extra:
            values.update(extra)
        return Metadata.of(**values)

    @classmethod
    def optional_int(cls, value: str | None) -> int | None:
        """Parse optional integer fields emitted by aggregate tables."""
        if value is None or value == "":
            return None
        return int(value)
