from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.models import CurrencyPair, Money

from aws import AthenaDataSource, AthenaSettings


class FakeAthenaClient:
    def __init__(self) -> None:
        self.started: dict[str, Any] | None = None
        self.started_queries: list[dict[str, Any]] = []
        self.results_requested: list[dict[str, Any]] = []

    def start_query_execution(self, **kwargs: Any) -> dict[str, str]:
        self.started = kwargs
        self.started_queries.append(kwargs)
        return {"QueryExecutionId": f"query-{len(self.started_queries)}"}

    def get_query_execution(self, *, QueryExecutionId: str) -> dict[str, Any]:
        assert QueryExecutionId in {f"query-{index}" for index in range(1, 10)}
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]:
        self.results_requested.append(kwargs)
        execution_index = int(str(kwargs["QueryExecutionId"]).rsplit("-", maxsplit=1)[-1])
        participant_timestamp = str(1783555200000000000 + execution_index)
        return {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "ticker"},
                        {"Name": "bid_price"},
                        {"Name": "ask_price"},
                        {"Name": "participant_timestamp"},
                    ]
                },
                "Rows": [
                    {
                        "Data": [
                            {"VarCharValue": "ticker"},
                            {"VarCharValue": "bid_price"},
                            {"VarCharValue": "ask_price"},
                            {"VarCharValue": "participant_timestamp"},
                        ]
                    },
                    {
                        "Data": [
                            {"VarCharValue": "C:USD-JPY"},
                            {"VarCharValue": "150.100"},
                            {"VarCharValue": "150.120"},
                            {"VarCharValue": participant_timestamp},
                        ]
                    },
                ],
            }
        }


def test_settings_read_environment(monkeypatch: Any) -> None:
    monkeypatch.setenv("AWS_PROFILE", "yuyash-personal")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("AWS_ACCOUNT_ID", "789121567207")
    monkeypatch.setenv("AWS_ATHENA_DATABASE", "forex_hist_data_db")
    monkeypatch.setenv("AWS_ATHENA_TABLE", "quotes")
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
    assert settings.query_chunk_days == 1
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


def test_ticks_execute_athena_query_and_yield_core_ticks() -> None:
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
        ),
        athena_client=client,
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


def test_ticks_query_each_day_sequentially() -> None:
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
        ),
        athena_client=client,
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
