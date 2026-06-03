from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from etl_framework.models.context import EtlExecutionContext

REQUIRED_OPERATIONAL_METRICS = (
    "rows_read",
    "rows_valid",
    "rows_invalid",
    "rows_written",
)


def assert_target_key_not_null(df: DataFrame, keys: Iterable[str]) -> DataFrame:
    """Fail when any target key column contains nulls.

    This is an explicit production check and runs a small Spark action.
    """
    key_tuple = _normalize_columns(keys, "target_key")
    _require_columns(df, key_tuple)

    condition = F.lit(False)
    for key in key_tuple:
        condition = condition | F.col(key).isNull()

    if df.filter(condition).take(1):
        raise ValueError(f"Null value found in target key: {key_tuple}")

    return df


def assert_target_key_unique(df: DataFrame, keys: Iterable[str]) -> DataFrame:
    """Fail when target keys are duplicated in the loaded scope.

    This is an explicit production check and runs Spark aggregation.
    For large datasets this operation may be expensive. Callers must pass
    `allow_expensive=True` to run the aggregation in production flows.
    """
    def _impl(allow_expensive: bool) -> DataFrame:
        if not allow_expensive:
            raise RuntimeError(
                "assert_target_key_unique requires allow_expensive=True to run aggregation"
            )

        key_tuple = _normalize_columns(keys, "target_key")
        _require_columns(df, key_tuple)

        duplicates = df.groupBy(*key_tuple).count().filter(F.col("count") > 1)
        if duplicates.take(1):
            raise ValueError(f"Duplicate target key found: {key_tuple}")

        return df

    return _impl


def assert_volume_between(
    df: DataFrame,
    *,
    min_rows: int | None = None,
    max_rows: int | None = None,
    context: EtlExecutionContext | None = None,
    metric_name: str = "rows_read",
    allow_expensive: bool = False,
) -> int:
    """Return row count and fail when it is outside the expected range.

    This is an explicit production check and runs `count()`. Use
    `allow_expensive=True` to enable the operation explicitly.
    Pass `context` when the same count should also become an operational metric.
    """
    if min_rows is None and max_rows is None:
        raise ValueError("min_rows or max_rows must be provided")

    if (min_rows is not None and min_rows < 0) or (max_rows is not None and max_rows < 0):
        raise ValueError("min_rows and max_rows cannot be negative")

    if min_rows is not None and max_rows is not None and min_rows > max_rows:
        raise ValueError(f"min_rows ({min_rows}) cannot be greater than max_rows ({max_rows})")

    if not allow_expensive:
        raise RuntimeError(
            "assert_volume_between performs a row count and requires allow_expensive=True to execute"
        )

    row_count = df.count()

    if context is not None:
        context.metrics[metric_name] = row_count

    if min_rows is not None and row_count < min_rows:
        raise ValueError(f"Row count {row_count} below expected minimum {min_rows}")

    if max_rows is not None and row_count > max_rows:
        raise ValueError(f"Row count {row_count} above expected maximum {max_rows}")

    return row_count



def assert_freshness_at_least(
    df: DataFrame,
    *,
    column: str,
    min_value: Any,
) -> DataFrame:
    """Fail when a freshness column is null or older than `min_value`.

    This is an explicit production check and runs a small Spark action.
    """
    _require_columns(df, (column,))

    stale = df.filter(F.col(column).isNull() | (F.col(column) < F.lit(min_value)))
    if stale.take(1):
        raise ValueError(f"Freshness check failed for column '{column}'")

    return df


def assert_reconciled_by_key(
    source_df: DataFrame,
    target_df: DataFrame,
    keys: Iterable[str],
) -> None:
    """Fail when target is missing keys present in the source scope.

    This is an explicit production check and runs a small Spark action over the
    distinct key sets.
    """
    key_tuple = _normalize_columns(keys, "target_key")
    _require_columns(source_df, key_tuple)
    _require_columns(target_df, key_tuple)

    source_keys = source_df.select(*key_tuple).distinct()
    target_keys = target_df.select(*key_tuple).distinct()
    
    if source_keys.exceptAll(target_keys).take(1):
        raise ValueError(f"Target is missing source keys: {key_tuple}")


def require_is_valid_column(
    df: DataFrame,
    *,
    column: str = "is_valid",
) -> DataFrame:
    """Fail fast when a DataFrame was not explicitly validated."""
    _require_columns(df, (column,))
    return df


def split_valid_invalid(
    df: DataFrame,
    *,
    column: str = "is_valid",
) -> tuple[DataFrame, DataFrame]:
    """Return valid and invalid DataFrames without running Spark actions."""
    require_is_valid_column(df, column=column)
    valid_condition = _is_valid_condition(column)
    return df.filter(valid_condition), df.filter(~valid_condition)


def assert_no_invalid_records(
    df: DataFrame,
    *,
    column: str = "is_valid",
) -> DataFrame:
    """Fail when validated data contains `is_valid=False` rows.

    This is an explicit production check and runs a small Spark action.
    """
    require_is_valid_column(df, column=column)
    
    if df.filter(~_is_valid_condition(column)).take(1):
        raise ValueError("Invalid records must be quarantined or fixed before load")

    return df


def require_operational_metrics(
    context: EtlExecutionContext,
    *,
    required_metrics: Iterable[str] = REQUIRED_OPERATIONAL_METRICS,
    missing_reasons: Mapping[str, str] | None = None,
) -> EtlExecutionContext:
    """Ensure required metrics exist or have explicit missing reasons.

    This does not compute metrics. It only validates `context.metrics` so a
    concrete pipeline cannot silently skip expected operational evidence.
    """
    required = _normalize_columns(required_metrics, "required_metrics")
    reasons = missing_reasons or {}

    missing = [m for m in required if m not in context.metrics]
    
    missing_without_reason = [
        m for m in missing 
        if m not in reasons or not str(reasons[m]).strip()
    ]

    if missing_without_reason:
        raise ValueError(
            f"Missing operational metrics without justification: {missing_without_reason}"
        )

    for metric in missing:
        context.metrics[f"{metric}_missing_reason"] = str(reasons[metric]).strip()

    return context


# ==================== Helpers ====================

def _is_valid_condition(column: str = "is_valid") -> Column:
    """Return Spark column expression for valid records."""
    return F.coalesce(F.col(column).cast("boolean"), F.lit(False))


def _normalize_columns(columns: Iterable[str], label: str) -> tuple[str, ...]:
    """Normalize and validate column names."""
    if isinstance(columns, str):
        raise ValueError(f"{label} must be an iterable of strings, not a single string")

    try:
        normalized = tuple(columns)
    except TypeError as exc:
        raise ValueError(f"{label} must be iterable") from exc

    if not normalized:
        raise ValueError(f"{label} cannot be empty")

    invalid = [c for c in normalized if not isinstance(c, str) or not c.strip()]
    if invalid:
        raise ValueError(f"{label} contains invalid columns: {invalid}")

    return normalized


def _require_columns(df: DataFrame, columns: tuple[str, ...]) -> None:
    """Fail if required columns are missing from DataFrame."""
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")