from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from etl_framework.utils.production_checks import assert_no_invalid_records
from etl_framework.utils.validate_struct import validate_schema, validate_struct


def auto_check_source(
    df: DataFrame,
    source_struct: StructType | None,
    *,
    strict: bool = False,
) -> DataFrame:
    """Validate extracted data against the declared source structure."""
    if source_struct is None:
        raise ValueError("source_struct is required for automatic source check in v0.1")

    return validate_schema(df, source_struct, strict=strict)


def auto_validate_target(
    df: DataFrame,
    target_struct: StructType | None,
    *,
    strict: bool = False,
) -> DataFrame:
    """Validate transformed data against the declared target structure."""
    if target_struct is None:
        raise ValueError(
            "target_struct is required for automatic target validation in v0.1"
        )

    if "is_valid" in df.columns:
        assert_no_invalid_records(df)

    validated_df, _ = validate_struct(
        df,
        target_struct,
        strict=strict,
    )
    return assert_no_invalid_records(validated_df)
