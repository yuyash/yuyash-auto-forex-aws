"""Shared Athena data source model objects."""

from __future__ import annotations

from dataclasses import dataclass


class AthenaDataSourceError(RuntimeError):
    """Raised when Athena cannot return a valid market data stream."""


@dataclass(frozen=True, slots=True)
class AthenaQueryExecution:
    """Completed Athena query and the S3 object containing its results."""

    execution_id: str
    output_location: str
