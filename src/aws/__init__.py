"""AWS adapters for AutoForexV2."""

from importlib.metadata import version

from aws.athena import AthenaDataSource, AthenaDataSourceError, AthenaSettings

__all__ = [
    "AthenaDataSource",
    "AthenaDataSourceError",
    "AthenaSettings",
    "__version__",
]

__version__ = version("auto-forex-aws")
