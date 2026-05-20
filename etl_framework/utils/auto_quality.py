from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from etl_framework.utils.production_checks import require_is_valid_column
from etl_framework.utils.validate_struct import (
    summarize_struct_checks,
    validate_schema,
    validate_struct,
)


def auto_check_source(
    df: DataFrame,
    source_struct: StructType | None,
    *,
    strict: bool = False,
    extra_columns_policy: str = "ignore",
) -> DataFrame:
    """Validate extracted data against the declared source structure."""
    if source_struct is None:
        raise ValueError("source_struct is required for automatic source check in v0.1")

    return validate_schema(
        df,
        source_struct,
        strict=strict,
        extra_columns_policy=extra_columns_policy,
    )


def auto_validate_target(
    df: DataFrame,
    target_struct: StructType | None,
    *,
    strict: bool = False,
    extra_columns_policy: str = "ignore",
) -> DataFrame:
    """Validate transformed data against the declared target structure."""
    if target_struct is None:
        raise ValueError(
            "target_struct is required for automatic target validation in v0.1"
        )

    if "is_valid" in df.columns:
        _raise_with_existing_validation_diagnostics(df)

    validated_df, _ = validate_struct(
        df,
        target_struct,
        strict=strict,
        extra_columns_policy=extra_columns_policy,
    )
    return _raise_with_validation_diagnostics(validated_df, target_struct)


def _raise_with_existing_validation_diagnostics(df: DataFrame) -> None:
    require_is_valid_column(df)
    invalid_df = df.filter(~_valid_condition())
    if invalid_df.limit(1).count() == 0:
        return

    invalid_count = invalid_df.count()
    raise ValueError(
        "Invalid records found before target validation: "
        f"invalid_count={invalid_count}; failed_checks=[none]"
    )


def _raise_with_validation_diagnostics(
    df: DataFrame,
    target_struct: StructType,
) -> DataFrame:
    require_is_valid_column(df)
    invalid_df = df.filter(~_valid_condition())
    if invalid_df.limit(1).count() == 0:
        return df

    invalid_count = invalid_df.count()
    failed_checks = _failed_check_summaries(df, target_struct)
    details = ", ".join(failed_checks) if failed_checks else "none"

    raise ValueError(
        "Invalid records found during target validation: "
        f"invalid_count={invalid_count}; failed_checks=[{details}]"
    )


def _failed_check_summaries(df: DataFrame, target_struct: StructType) -> list[str]:
    summary_df = summarize_struct_checks(df, target_struct)
    if not summary_df.columns:
        return []

    rows = (
        summary_df.filter((F.col("severity") == "error") & (F.col("failed_count") > 0))
        .select("field", "check", "failed_count")
        .collect()
    )
    return [f"{row.field}.{row.check}: failed_count={row.failed_count}" for row in rows]


def _valid_condition() -> Column:
    return F.coalesce(F.col("is_valid").cast("boolean"), F.lit(False))
