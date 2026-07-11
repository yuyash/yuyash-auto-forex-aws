"""AWS adapters for AutoForexV2."""

from importlib.metadata import version

from aws.athena import AthenaDataSource, AthenaDataSourceError, AthenaSettings
from aws.metrics import CloudWatchMetricStore

__all__ = [
    "AthenaDataSource",
    "AthenaDataSourceError",
    "AthenaSettings",
    "CloudWatchMetricStore",
    "__version__",
]

__version__ = version("auto-forex-aws")
