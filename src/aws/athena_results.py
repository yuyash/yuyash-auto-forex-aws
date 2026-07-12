"""S3-backed Athena result reading."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from typing import Any
from urllib.parse import urlparse

from aws.athena_execution import AthenaAwsClientProvider
from aws.athena_models import AthenaDataSourceError


class AthenaResultReader:
    """Read Athena CSV result objects from S3."""

    def __init__(self, clients: AthenaAwsClientProvider) -> None:
        self.clients = clients

    def rows(self, output_location: str) -> Iterable[dict[str, str]]:
        """Yield CSV rows from an Athena S3 output object."""
        bucket, key = self.s3_location(output_location)
        response = self.clients.s3.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        if body is None:
            raise AthenaDataSourceError(f"Athena result object has no body: {output_location}")
        reader = csv.DictReader(self.decoded_lines(body))
        yield from reader

    @classmethod
    def s3_location(cls, output_location: str) -> tuple[str, str]:
        """Parse an Athena S3 result URI into bucket and object key."""
        parsed = urlparse(output_location)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
            raise AthenaDataSourceError(f"invalid Athena S3 output location: {output_location}")
        return parsed.netloc, parsed.path.lstrip("/")

    @classmethod
    def decoded_lines(cls, body: Any) -> Iterator[str]:
        """Decode a boto3 streaming body as UTF-8 CSV lines."""
        for line in body.iter_lines():
            yield line.decode("utf-8")
