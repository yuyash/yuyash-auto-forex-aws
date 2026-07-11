"""CloudWatch metric sinks for AutoForex task metrics."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import boto3
from core import Metadata, ProfitMetric


class CloudWatchMetricStore:
    """Publish Core profit metrics to Amazon CloudWatch."""

    def __init__(
        self,
        *,
        namespace: str = "AutoForex/Task",
        dimensions: Mapping[str, str] | None = None,
        cloudwatch_client: Any | None = None,
    ) -> None:
        self.namespace = namespace
        self.dimensions = dict(dimensions or {})
        self.cloudwatch_client = cloudwatch_client or boto3.client("cloudwatch")

    def save_metric(self, metric: ProfitMetric) -> None:
        """Publish realized, unrealized, total P/L, and trade counts."""
        self.cloudwatch_client.put_metric_data(
            Namespace=self.namespace,
            MetricData=[
                self._money_metric("RealizedPL", metric.realized_pl, metric),
                self._money_metric("UnrealizedPL", metric.unrealized_pl, metric),
                self._money_metric("TotalPL", metric.total_pl, metric),
                self._count_metric("OpenTradeCount", metric.open_trade_count, metric),
                self._count_metric("ClosedTradeCount", metric.closed_trade_count, metric),
            ],
        )

    def _money_metric(
        self,
        name: str,
        value: Any,
        metric: ProfitMetric,
    ) -> dict[str, Any]:
        return {
            "MetricName": name,
            "Dimensions": self._dimensions(metric, currency=value.currency.code),
            "Timestamp": metric.timestamp,
            "Value": float(value.amount),
            "Unit": "None",
        }

    def _count_metric(
        self,
        name: str,
        value: int,
        metric: ProfitMetric,
    ) -> dict[str, Any]:
        return {
            "MetricName": name,
            "Dimensions": self._dimensions(metric),
            "Timestamp": metric.timestamp,
            "Value": value,
            "Unit": "Count",
        }

    def _dimensions(
        self,
        metric: ProfitMetric,
        *,
        currency: str = "",
    ) -> list[dict[str, str]]:
        values = {
            **self.dimensions,
            "TaskId": str(metric.task_id),
            "Instrument": str(metric.instrument),
        }
        if currency:
            values["Currency"] = currency
        values.update(self._metadata_dimensions(metric.metadata))
        return [{"Name": key, "Value": value} for key, value in values.items()]

    @staticmethod
    def _metadata_dimensions(metadata: Metadata) -> dict[str, str]:
        raw = metadata.get("cloudwatch_dimensions", {})
        if not isinstance(raw, Mapping):
            return {}
        return {str(key): str(value) for key, value in raw.items()}
