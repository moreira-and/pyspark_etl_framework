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
        _raise_if_invalid_records_exist(df, context="before target validation")

    validated_df, _ = validate_struct(
        df,
        target_struct,
        strict=strict,
        extra_columns_policy=extra_columns_policy,
    )
    
    _raise_if_invalid_records_exist(
        validated_df, 
        context="during target validation",
        target_struct=target_struct,
    )
    
    return validated_df


def _raise_if_invalid_records_exist(
    df: DataFrame,
    *,
    context: str,
    target_struct: StructType | None = None,
) -> None:
    """Check for invalid records and raise with diagnostics if found.
    
    This performs a single Spark action to check for invalid records,
    then computes full diagnostics only if needed.
    """
    require_is_valid_column(df)
    
    # Single action: check if any invalid records exist
    invalid_df = df.filter(~_is_valid_condition())
    has_invalid = invalid_df.take(1)  # More efficient than limit(1).count()
    
    if not has_invalid:
        return
    
    # Only compute expensive metrics if we know there are invalid records
    invalid_count = invalid_df.count()
    
    # Get detailed check summaries if target_struct is provided
    failed_checks = (
        _get_failed_check_summaries(df, target_struct)
        if target_struct
        else None
    )
    
    details = ", ".join(failed_checks) if failed_checks else "unknown"
    
    raise ValueError(
        f"Invalid records found {context}: "
        f"invalid_count={invalid_count}; failed_checks=[{details}]"
    )


def _get_failed_check_summaries(
    df: DataFrame,
    target_struct: StructType,
) -> list[str] | None:
    """Get human-readable summaries of failed validation checks.
    
    Returns None if no check columns are present.
    """
    summary_df = summarize_struct_checks(df, target_struct)
    
    if not summary_df.columns:
        return None
    
    failed_rows = (
        summary_df
        .filter((F.col("severity") == "error") & (F.col("failed_count") > 0))
        .select("field", "check", "failed_count")
        .collect()
    )
    
    return [
        f"{row.field}.{row.check}: failed_count={row.failed_count}"
        for row in failed_rows
    ]


def _is_valid_condition() -> Column:
    """Return a Spark column expression that safely evaluates is_valid.
    
    Treats NULL as False to handle missing values gracefully.
    """
    return F.coalesce(F.col("is_valid").cast("boolean"), F.lit(False))