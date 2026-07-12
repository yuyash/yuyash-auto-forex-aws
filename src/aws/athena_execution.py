"""Athena query execution and AWS client ownership."""

from __future__ import annotations

from collections.abc import Mapping
from time import monotonic, sleep
from typing import Any

import boto3

from aws.athena_models import AthenaDataSourceError, AthenaQueryExecution
from aws.athena_settings import AthenaSettings


class AthenaAwsClientProvider:
    """Own lazily-created boto3 clients used by the Athena data source."""

    def __init__(
        self,
        *,
        settings: AthenaSettings,
        athena_client: Any | None = None,
        s3_client: Any | None = None,
    ) -> None:
        self.settings = settings
        self._athena_client = athena_client
        self._s3_client = s3_client

    @property
    def athena(self) -> Any:
        """Return the Athena client, creating it lazily."""
        if self._athena_client is None:
            self._athena_client = self._create_session().client("athena")
        return self._athena_client

    @property
    def s3(self) -> Any:
        """Return the S3 client, creating it lazily."""
        if self._s3_client is None:
            self._s3_client = self._create_session().client("s3")
        return self._s3_client

    def close(self) -> None:
        """Close lazily-created AWS clients when they expose a close method."""
        for client in (self._athena_client, self._s3_client):
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def _create_session(self) -> Any:
        return boto3.Session(
            profile_name=self.settings.profile_name,
            region_name=self.settings.region_name,
        )


class AthenaQueryExecutor:
    """Start Athena SQL queries and wait for terminal results."""

    def __init__(
        self,
        *,
        settings: AthenaSettings,
        clients: AthenaAwsClientProvider,
    ) -> None:
        self.settings = settings
        self.clients = clients

    def execute(self, query: str) -> AthenaQueryExecution:
        """Execute SQL and return the completed output location."""
        execution_id = self.start(query)
        output_location = self.wait(execution_id)
        return AthenaQueryExecution(
            execution_id=execution_id,
            output_location=output_location,
        )

    def start(self, query: str) -> str:
        """Submit a query to Athena."""
        request: dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": self.settings.database},
            "ResultConfiguration": {"OutputLocation": self.settings.output_location},
        }
        if self.settings.work_group is not None:
            request["WorkGroup"] = self.settings.work_group
        response = self.clients.athena.start_query_execution(**request)
        execution_id = response.get("QueryExecutionId")
        if not isinstance(execution_id, str) or not execution_id:
            raise AthenaDataSourceError("Athena did not return a query execution id")
        return execution_id

    def wait(self, execution_id: str) -> str:
        """Wait until the query completes and return its S3 output location."""
        deadline = monotonic() + self.settings.timeout_seconds
        while True:
            response = self.clients.athena.get_query_execution(QueryExecutionId=execution_id)
            query_execution = response.get("QueryExecution", {})
            status = query_execution.get("Status", {})
            state = status.get("State")
            if state == "SUCCEEDED":
                return self.output_location(query_execution, execution_id=execution_id)
            if state in {"FAILED", "CANCELLED"}:
                reason = status.get("StateChangeReason", "")
                message = f"Athena query {execution_id} {state.lower()}"
                if reason:
                    message = f"{message}: {reason}"
                raise AthenaDataSourceError(message)
            if monotonic() >= deadline:
                raise AthenaDataSourceError(f"Athena query timed out: {execution_id}")
            sleep(self.settings.poll_interval_seconds)

    @classmethod
    def output_location(
        cls,
        query_execution: Mapping[str, Any],
        *,
        execution_id: str,
    ) -> str:
        """Extract the S3 output location from a completed Athena response."""
        result_configuration = query_execution.get("ResultConfiguration", {})
        output_location = result_configuration.get("OutputLocation")
        if isinstance(output_location, str) and output_location:
            return output_location
        raise AthenaDataSourceError(
            f"Athena query {execution_id} did not return an output location"
        )
