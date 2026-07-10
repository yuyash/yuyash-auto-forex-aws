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
AWS_ATHENA_OUTPUT_BUCKET=<YOUR_ATHENA_RESULT_OUTPUT_NAME>
AWS_ATHENA_OUTPUT_PREFIX=<YOUR_ATHENA_RESULT_OUTPUT_PREFIX>
AWS_ATHENA_QUERY_CHUNK_DAYS=1
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
    output_bucket="your-athena-query-output-result-name",
    query_chunk_days=1,
)
```

When both `start_at` and `end_at` are set, ticks are loaded by bounded Athena
query windows. The default window is one day, matching the `year`/`month`/`day`
partitions.
