"""Athena data source configuration."""

from __future__ import annotations

from pathlib import Path

from pydantic import AliasChoices, Field, PositiveFloat, PositiveInt, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PACKAGE_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class AthenaSettings(BaseSettings):
    """Configuration for querying Polygon-style forex data in Athena."""

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
    minute_aggs_table: str = Field(
        default="minute_aggs",
        validation_alias=AliasChoices(
            "AWS_ATHENA_MINUTE_AGGS_TABLE",
            "ATHENA_MINUTE_AGGS_TABLE",
        ),
    )
    day_aggs_table: str = Field(
        default="day_aggs",
        validation_alias=AliasChoices(
            "AWS_ATHENA_DAY_AGGS_TABLE",
            "ATHENA_DAY_AGGS_TABLE",
        ),
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
    query_chunk_days: PositiveInt = Field(
        default=1,
        validation_alias=AliasChoices("AWS_ATHENA_QUERY_CHUNK_DAYS", "ATHENA_QUERY_CHUNK_DAYS"),
    )
    candle_query_chunk_days: PositiveInt = Field(
        default=31,
        validation_alias=AliasChoices(
            "AWS_ATHENA_CANDLE_QUERY_CHUNK_DAYS",
            "ATHENA_CANDLE_QUERY_CHUNK_DAYS",
        ),
    )
    query_prefetch_min_windows: PositiveInt = Field(
        default=3,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_MIN_WINDOWS",
            "ATHENA_QUERY_PREFETCH_MIN_WINDOWS",
        ),
    )
    query_prefetch_max_windows: PositiveInt = Field(
        default=6,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_MAX_WINDOWS",
            "ATHENA_QUERY_PREFETCH_MAX_WINDOWS",
        ),
    )
    query_prefetch_workers: PositiveInt = Field(
        default=4,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_WORKERS",
            "ATHENA_QUERY_PREFETCH_WORKERS",
        ),
    )
    query_prefetch_wait_target_seconds: PositiveFloat = Field(
        default=0.5,
        validation_alias=AliasChoices(
            "AWS_ATHENA_QUERY_PREFETCH_WAIT_TARGET_SECONDS",
            "ATHENA_QUERY_PREFETCH_WAIT_TARGET_SECONDS",
        ),
    )

    @model_validator(mode="after")
    def normalize_and_validate(self) -> AthenaSettings:
        """Normalize S3 output paths and validate prefetch bounds."""
        prefix = self.output_prefix.strip("/")
        if prefix:
            prefix = f"{prefix}/"
        object.__setattr__(self, "output_prefix", prefix)
        if self.query_prefetch_min_windows > self.query_prefetch_max_windows:
            raise ValueError(
                "query_prefetch_min_windows must be less than or equal to "
                "query_prefetch_max_windows"
            )
        return self

    @property
    def output_location(self) -> str:
        """Return the S3 location Athena writes query results to."""
        return f"s3://{self.output_bucket}/{self.output_prefix}"
