# AutoForex AWS

AWS adapters for AutoForexV2.

## Athena ticks

Configure AWS and Athena through `aws/.env`, the current working directory's
`.env`, or environment variables:

```env
AWS_PROFILE=<YOUR_AWS_PROFILE_NAME>
AWS_REGION=<YOUR_AWS_REGION>
AWS_ACCOUNT_ID=<YOUR_AWS_ACCOUNT_ID>
AWS_ATHENA_DATABASE=<YOUR_ATHENA_DB_NAME>
AWS_ATHENA_TABLE=<YOUR_ATHENA_TABLE_NAME>
AWS_ATHENA_MINUTE_AGGS_TABLE=minute_aggs
AWS_ATHENA_DAY_AGGS_TABLE=day_aggs
AWS_ATHENA_OUTPUT_BUCKET=<YOUR_ATHENA_RESULT_OUTPUT_NAME>
AWS_ATHENA_OUTPUT_PREFIX=<YOUR_ATHENA_RESULT_OUTPUT_PREFIX>
AWS_ATHENA_QUERY_CHUNK_DAYS=1
AWS_ATHENA_CANDLE_QUERY_CHUNK_DAYS=31
AWS_ATHENA_QUERY_PREFETCH_MIN_WINDOWS=3
AWS_ATHENA_QUERY_PREFETCH_MAX_WINDOWS=6
AWS_ATHENA_QUERY_PREFETCH_WORKERS=4
AWS_ATHENA_QUERY_PREFETCH_WAIT_TARGET_SECONDS=0.5
```

Then use `AthenaDataSource` like any Core data source:

```python
from datetime import UTC, datetime

from aws import AthenaDataSource
from core import CurrencyPair

source = AthenaDataSource.from_env()
ticks = source.ticks(
    instrument=CurrencyPair.of("USD_JPY"),
    start_at=datetime(2026, 7, 9, tzinfo=UTC),
    end_at=datetime(2026, 7, 9, 23, 59, 59, tzinfo=UTC),
)
```

Constructor arguments can override the environment-backed settings:

```python
source = AthenaDataSource(
    profile_name="your-aws-profile",
    region_name="your-aws-region-name",
    database="your-athena-db-name",
    table="your-athena-table-name",
    minute_aggs_table="minute_aggs",
    day_aggs_table="day_aggs",
    output_bucket="your-athena-query-output-result-name",
    query_chunk_days=1,
    candle_query_chunk_days=31,
    query_prefetch_min_windows=3,
    query_prefetch_max_windows=6,
    query_prefetch_workers=4,
)
```

When both `start_at` and `end_at` are set, ticks are loaded by bounded Athena
query windows. The default window is one day, matching the `year`/`month`/`day`
partitions.

Athena query results are streamed from the S3 output object instead of using
`get_query_results` pagination. Multiple query windows are started
speculatively in the background while the current window is being consumed.
The prefetch depth adapts to the observed Athena query time and tick
consumption time: if tick processing catches up to Athena, more windows are
submitted; if consumption is slow enough to hide query latency, the depth is
reduced gradually.

`AthenaDataSource.candles()` reads one-minute candles from `minute_aggs`
for `CandleGranularity.MINUTE_1` and daily candles from `day_aggs` for
`CandleGranularity.DAY`.
