from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from core import CurrencyPair, Metadata, Money, ProfitMetric, new_uuid

from aws import CloudWatchMetricStore


class FakeCloudWatchClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_metric_data(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


def test_cloudwatch_metric_store_publishes_profit_metrics() -> None:
    client = FakeCloudWatchClient()
    store = CloudWatchMetricStore(
        namespace="AutoForex/Test",
        dimensions={"Environment": "test"},
        cloudwatch_client=client,
    )
    task_id = new_uuid()
    metric = ProfitMetric(
        metric_id=new_uuid(),
        task_id=task_id,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        instrument=CurrencyPair.of("USD_JPY"),
        realized_pl=Money.of("100", "JPY"),
        unrealized_pl=Money.of("-20", "JPY"),
        total_pl=Money.of("80", "JPY"),
        open_trade_count=1,
        closed_trade_count=2,
        interval=timedelta(minutes=5),
        metadata=Metadata.of(cloudwatch_dimensions={"Strategy": "snowball"}),
    )

    store.save_metric(metric)

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["Namespace"] == "AutoForex/Test"
    assert {item["MetricName"] for item in call["MetricData"]} == {
        "ClosedTradeCount",
        "OpenTradeCount",
        "RealizedPL",
        "TotalPL",
        "UnrealizedPL",
    }
    realized = next(item for item in call["MetricData"] if item["MetricName"] == "RealizedPL")
    assert realized["Value"] == 100.0
    assert {"Name": "TaskId", "Value": str(task_id)} in realized["Dimensions"]
    assert {"Name": "Instrument", "Value": "USD_JPY"} in realized["Dimensions"]
    assert {"Name": "Currency", "Value": "JPY"} in realized["Dimensions"]
    assert {"Name": "Environment", "Value": "test"} in realized["Dimensions"]
    assert {"Name": "Strategy", "Value": "snowball"} in realized["Dimensions"]
