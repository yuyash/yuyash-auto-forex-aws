from __future__ import annotations

from datetime import UTC, datetime
from threading import Lock
from typing import Any

import pytest
from core.models import CurrencyPair, Money
from core.sources import CandleGranularity

from aws import AthenaDataSource, AthenaDataSourceError, AthenaSettings


class FakeAthenaClient:
    def __init__(self) -> None:
        self.started: dict[str, Any] | None = None
        self.started_queries: list[dict[str, Any]] = []
        self._lock = Lock()

    def start_query_execution(self, **kwargs: Any) -> dict[str, str]:
        with self._lock:
            self.started = kwargs
            self.started_queries.append(kwargs)
            execution_id = f"query-{len(self.started_queries)}"
        return {"QueryExecutionId": execution_id}

    def get_query_execution(self, *, QueryExecutionId: str) -> dict[str, Any]:
        assert QueryExecutionId in {f"query-{index}" for index in range(1, 10)}
        return {
            "QueryExecution": {
                "Status": {"State": "SUCCEEDED"},
                "ResultConfiguration": {
                    "OutputLocation": (
                        "s3://aws-athena-query-results-789121567207-us-west-2/"
                        f"athena-query-results/{QueryExecutionId}.csv"
                    )
                },
            }
        }


class FakeStreamingBody:
    def __init__(self, content: str) -> None:
        self.content = content

    def iter_lines(self) -> list[bytes]:
        return [line.encode("utf-8") for line in self.content.splitlines()]


class FakeS3Client:
    def __init__(self, *, result_kind: str = "tick") -> None:
        self.objects_requested: list[dict[str, str]] = []
        self.result_kind = result_kind

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, FakeStreamingBody]:
        self.objects_requested.append({"Bucket": Bucket, "Key": Key})
        execution_id = Key.rsplit("/", maxsplit=1)[-1].removesuffix(".csv")
        execution_index = int(execution_id.rsplit("-", maxsplit=1)[-1])
        participant_timestamp = str(1783555200000000000 + execution_index)
        if self.result_kind == "candle":
            return {
                "Body": FakeStreamingBody(
                    "\n".join(
                        (
                            "ticker,volume,open,close,high,low,window_start,transactions",
                            (
                                "C:USD-JPY,120,150.100,150.200,150.300,150.000,"
                                f"{participant_timestamp},42"
                            ),
                        )
                    )
                )
            }
        return {
            "Body": FakeStreamingBody(
                "\n".join(
                    (
                        "ticker,bid_price,ask_price,participant_timestamp",
                        f"C:USD-JPY,150.100,150.120,{participant_timestamp}",
                    )
                )
            )
        }


def test_settings_read_environment(monkeypatch: Any) -> None:
    monkeypatch.setenv("AWS_PROFILE", "yuyash-personal")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("AWS_ACCOUNT_ID", "789121567207")
    monkeypatch.setenv("AWS_ATHENA_DATABASE", "forex_hist_data_db")
    monkeypatch.setenv("AWS_ATHENA_TABLE", "quotes")
    monkeypatch.setenv("AWS_ATHENA_MINUTE_AGGS_TABLE", "minute_aggs")
    monkeypatch.setenv("AWS_ATHENA_DAY_AGGS_TABLE", "day_aggs")
    monkeypatch.setenv(
        "AWS_ATHENA_OUTPUT_BUCKET",
        "aws-athena-query-results-789121567207-us-west-2",
    )

    settings = AthenaSettings()

    assert settings.profile_name == "yuyash-personal"
    assert settings.region_name == "us-west-2"
    assert settings.account_id == "789121567207"
    assert settings.database == "forex_hist_data_db"
    assert settings.table == "quotes"
    assert settings.minute_aggs_table == "minute_aggs"
    assert settings.day_aggs_table == "day_aggs"
    assert settings.query_chunk_days == 1
    assert settings.candle_query_chunk_days == 31
    assert settings.query_prefetch_min_windows == 3
    assert settings.query_prefetch_max_windows == 6
    assert settings.query_prefetch_workers == 4
    assert (
        settings.output_location
        == "s3://aws-athena-query-results-789121567207-us-west-2/athena-query-results/"
    )


def test_query_for_ticks_uses_partitions_and_ticker() -> None:
    source = AthenaDataSource(
        settings=AthenaSettings(
            profile_name="yuyash-personal",
            region_name="us-west-2",
            account_id="789121567207",
            database="forex_hist_data_db",
            table="quotes",
            output_bucket="aws-athena-query-results-789121567207-us-west-2",
        )
    )

    query = source.query_for_ticks(
        instrument=CurrencyPair.of("USD_JPY"),
        start_at=datetime(2026, 7, 9, tzinfo=UTC),
        end_at=datetime(2026, 7, 9, 23, 59, 59, tzinfo=UTC),
    )

    assert 'FROM "forex_hist_data_db"."quotes"' in query
    assert "((year = '2026' AND month = '07' AND day = '09'))" in query
    assert "ticker = 'C:USD-JPY'" in query
    assert "ORDER BY participant_timestamp ASC" in query


def test_query_for_candles_uses_aggregate_table_and_window_start() -> None:
    source = AthenaDataSource(
        settings=AthenaSettings(
            profile_name="yuyash-personal",
            region_name="us-west-2",
            account_id="789121567207",
            database="forex_hist_data_db",
            table="quotes",
            minute_aggs_table="minute_aggs",
            day_aggs_table="day_aggs",
            output_bucket="aws-athena-query-results-789121567207-us-west-2",
        )
    )

    minute_query = source.query_for_candles(
        instrument=CurrencyPair.of("USD_JPY"),
        granularity=CandleGranularity.MINUTE_1,
        start_at=datetime(2026, 7, 9, tzinfo=UTC),
        end_at=datetime(2026, 7, 9, 23, 59, 59, tzinfo=UTC),
    )
    day_query = source.query_for_candles(
        instrument=CurrencyPair.of("USD_JPY"),
        granularity=CandleGranularity.DAY,
        start_at=datetime(2026, 7, 1, tzinfo=UTC),
        end_at=datetime(2026, 7, 31, 23, 59, 59, tzinfo=UTC),
    )

    assert 'FROM "forex_hist_data_db"."minute_aggs"' in minute_query
    assert 'FROM "forex_hist_data_db"."day_aggs"' in day_query
    assert '"window_start" >=' in minute_query
    assert '"window_start" <=' in minute_query
    assert 'SELECT ticker, volume, "open" AS "open", "close" AS "close"' in minute_query
    assert "ORDER BY window_start ASC" in minute_query


def test_ticks_execute_athena_query_and_yield_core_ticks() -> None:
    client = FakeAthenaClient()
    s3_client = FakeS3Client()
    source = AthenaDataSource(
        settings=AthenaSettings(
            profile_name="yuyash-personal",
            region_name="us-west-2",
            account_id="789121567207",
            database="forex_hist_data_db",
            table="quotes",
            output_bucket="aws-athena-query-results-789121567207-us-west-2",
            poll_interval_seconds=0.001,
            query_prefetch_workers=1,
        ),
        athena_client=client,
        s3_client=s3_client,
    )

    ticks = tuple(
        source.ticks(
            instrument=CurrencyPair.of("USD_JPY"),
            start_at=datetime(2026, 7, 9, tzinfo=UTC),
            end_at=datetime(2026, 7, 9, 23, 59, 59, tzinfo=UTC),
        )
    )

    assert len(ticks) == 1
    assert ticks[0].instrument == CurrencyPair.of("USD_JPY")
    assert ticks[0].bid == Money.of("150.100", "JPY")
    assert ticks[0].ask == Money.of("150.120", "JPY")
    assert ticks[0].metadata.get("source") == "athena"
    assert ticks[0].metadata.get("aws_account_id") == "789121567207"
    assert client.started is not None
    assert client.started["QueryExecutionContext"] == {"Database": "forex_hist_data_db"}
    assert client.started["ResultConfiguration"] == {
        "OutputLocation": "s3://aws-athena-query-results-789121567207-us-west-2/athena-query-results/"
    }
    assert s3_client.objects_requested == [
        {
            "Bucket": "aws-athena-query-results-789121567207-us-west-2",
            "Key": "athena-query-results/query-1.csv",
        }
    ]


def test_candles_execute_athena_query_and_yield_core_candles() -> None:
    client = FakeAthenaClient()
    s3_client = FakeS3Client(result_kind="candle")
    source = AthenaDataSource(
        settings=AthenaSettings(
            profile_name="yuyash-personal",
            region_name="us-west-2",
            account_id="789121567207",
            database="forex_hist_data_db",
            table="quotes",
            minute_aggs_table="minute_aggs",
            output_bucket="aws-athena-query-results-789121567207-us-west-2",
            poll_interval_seconds=0.001,
            query_prefetch_workers=1,
        ),
        athena_client=client,
        s3_client=s3_client,
    )

    candles = tuple(
        source.candles(
            instrument=CurrencyPair.of("USD_JPY"),
            granularity=CandleGranularity.MINUTE_1,
            start_at=datetime(2026, 7, 9, tzinfo=UTC),
            end_at=datetime(2026, 7, 9, 23, 59, 59, tzinfo=UTC),
        )
    )

    assert len(candles) == 1
    assert candles[0].instrument == CurrencyPair.of("USD_JPY")
    assert candles[0].granularity == CandleGranularity.MINUTE_1
    assert candles[0].open == Money.of("150.100", "JPY")
    assert candles[0].high == Money.of("150.300", "JPY")
    assert candles[0].low == Money.of("150.000", "JPY")
    assert candles[0].close == Money.of("150.200", "JPY")
    assert candles[0].volume == 120
    assert candles[0].metadata.get("source") == "athena"
    assert candles[0].metadata.get("athena_table") == "minute_aggs"
    assert candles[0].metadata.get("transactions") == "42"


def test_candles_use_candle_query_chunk_days() -> None:
    client = FakeAthenaClient()
    source = AthenaDataSource(
        settings=AthenaSettings(
            profile_name="yuyash-personal",
            region_name="us-west-2",
            account_id="789121567207",
            database="forex_hist_data_db",
            table="quotes",
            minute_aggs_table="minute_aggs",
            output_bucket="aws-athena-query-results-789121567207-us-west-2",
            poll_interval_seconds=0.001,
            candle_query_chunk_days=31,
            query_prefetch_min_windows=1,
            query_prefetch_max_windows=1,
            query_prefetch_workers=1,
        ),
        athena_client=client,
        s3_client=FakeS3Client(result_kind="candle"),
    )

    candles = tuple(
        source.candles(
            instrument=CurrencyPair.of("USD_JPY"),
            granularity=CandleGranularity.MINUTE_1,
            start_at=datetime(2026, 7, 1, tzinfo=UTC),
            end_at=datetime(2026, 7, 31, 23, 59, 59, tzinfo=UTC),
        )
    )

    assert len(candles) == 1
    assert len(client.started_queries) == 1
    query = client.started_queries[0]["QueryString"]
    assert "(year = '2026' AND month = '07' AND day = '01')" in query
    assert "(year = '2026' AND month = '07' AND day = '31')" in query


def test_candles_reject_unsupported_granularity() -> None:
    source = AthenaDataSource(settings=AthenaSettings())

    with pytest.raises(AthenaDataSourceError, match="unsupported Athena candle granularity"):
        source.query_for_candles(
            instrument=CurrencyPair.of("USD_JPY"),
            granularity=CandleGranularity.MINUTE_5,
        )


def test_ticks_query_each_day_sequentially() -> None:
    client = FakeAthenaClient()
    s3_client = FakeS3Client()
    source = AthenaDataSource(
        settings=AthenaSettings(
            profile_name="yuyash-personal",
            region_name="us-west-2",
            account_id="789121567207",
            database="forex_hist_data_db",
            table="quotes",
            output_bucket="aws-athena-query-results-789121567207-us-west-2",
            poll_interval_seconds=0.001,
            query_chunk_days=1,
            query_prefetch_min_windows=1,
            query_prefetch_max_windows=1,
            query_prefetch_workers=1,
        ),
        athena_client=client,
        s3_client=s3_client,
    )

    ticks = tuple(
        source.ticks(
            instrument=CurrencyPair.of("USD_JPY"),
            start_at=datetime(2026, 7, 9, 12, tzinfo=UTC),
            end_at=datetime(2026, 7, 10, 1, tzinfo=UTC),
        )
    )

    assert len(ticks) == 2
    assert len(client.started_queries) == 2
    first_query = client.started_queries[0]["QueryString"]
    second_query = client.started_queries[1]["QueryString"]
    assert "(year = '2026' AND month = '07' AND day = '09')" in first_query
    assert "(year = '2026' AND month = '07' AND day = '10')" in second_query
    assert " OR " not in first_query
    assert " OR " not in second_query
    assert ticks[0].timestamp <= ticks[1].timestamp


def test_ticks_prefetch_multiple_daily_queries_before_first_tick() -> None:
    client = FakeAthenaClient()
    source = AthenaDataSource(
        settings=AthenaSettings(
            profile_name="yuyash-personal",
            region_name="us-west-2",
            account_id="789121567207",
            database="forex_hist_data_db",
            table="quotes",
            output_bucket="aws-athena-query-results-789121567207-us-west-2",
            poll_interval_seconds=0.001,
            query_chunk_days=1,
            query_prefetch_min_windows=3,
            query_prefetch_max_windows=3,
            query_prefetch_workers=3,
        ),
        athena_client=client,
        s3_client=FakeS3Client(),
    )

    ticks = iter(
        source.ticks(
            instrument=CurrencyPair.of("USD_JPY"),
            start_at=datetime(2026, 7, 9, tzinfo=UTC),
            end_at=datetime(2026, 7, 11, 23, 59, 59, tzinfo=UTC),
        )
    )
    try:
        first_tick = next(ticks)
    finally:
        close = getattr(ticks, "close", None)
        if callable(close):
            close()

    assert first_tick.instrument == CurrencyPair.of("USD_JPY")
    assert len(client.started_queries) == 3
    query_text = "\n".join(query["QueryString"] for query in client.started_queries)
    assert "(year = '2026' AND month = '07' AND day = '09')" in query_text
    assert "(year = '2026' AND month = '07' AND day = '10')" in query_text
    assert "(year = '2026' AND month = '07' AND day = '11')" in query_text


def test_adaptive_prefetch_target_tracks_query_and_consumption_speed() -> None:
    source = AthenaDataSource(
        settings=AthenaSettings(
            query_prefetch_min_windows=1,
            query_prefetch_max_windows=6,
            query_prefetch_workers=6,
            query_prefetch_wait_target_seconds=0.5,
        )
    )

    assert (
        source._adaptive_prefetch_target(
            current_target=2,
            query_elapsed=3.0,
            consumption_elapsed=0.5,
            wait_elapsed=0.0,
        )
        == 6
    )
    assert (
        source._adaptive_prefetch_target(
            current_target=2,
            query_elapsed=0.2,
            consumption_elapsed=3.0,
            wait_elapsed=0.8,
        )
        == 3
    )
    assert (
        source._adaptive_prefetch_target(
            current_target=4,
            query_elapsed=0.5,
            consumption_elapsed=5.0,
            wait_elapsed=0.0,
        )
        == 3
    )
