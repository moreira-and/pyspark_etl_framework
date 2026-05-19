from __future__ import annotations

from pyspark.sql import DataFrame


def require_dataframe(value: object, *, stage: str) -> DataFrame:
    """Return value as DataFrame or fail with a clear contract error."""
    if not isinstance(value, DataFrame):
        raise TypeError(
            f"{stage} must return pyspark.sql.DataFrame, " f"got {type(value).__name__}"
        )

    return value
