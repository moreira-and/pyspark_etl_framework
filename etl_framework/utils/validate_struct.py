from __future__ import annotations

import re
import warnings
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from etl_framework.utils.check_metadata import normalize_check_metadata

MAX_CHECKS_PER_BATCH = 100
Check = dict[str, Any]


def validate_struct(
    df: DataFrame,
    schema: StructType,
    compute_summary: bool = False,
    strict: bool = False,
    extra_columns_policy: str = "ignore",
) -> tuple[DataFrame, DataFrame | None]:
    """Validate a DataFrame against a StructType contract.

    The function validates expected column names and data types, then executes
    simple declarative checks stored in StructField metadata. Missing columns
    and incompatible types are errors. Extra columns follow
    extra_columns_policy: ignore, warn or fail. `strict=True` is a backwards
    compatible alias for warning on extras. Error-severity checks define the
    generated is_valid technical column. The input DataFrame must not already
    contain is_valid; this prevents silent overwrite after a previous
    validation step. Summary calculation is optional because it triggers Spark
    actions.
    """
    if df is None or schema is None:
        raise ValueError("DataFrame and schema cannot be None")

    validate_schema(
        df,
        schema,
        strict=strict,
        extra_columns_policy=extra_columns_policy,
    )

    checks = _extract_all_checks(schema)
    _validate_check_rules(df, checks)

    validated_df = _add_is_valid_column(df, checks)

    summary_df = None
    if compute_summary:
        summary_df = _build_checks_summary(validated_df, checks)

    return validated_df, summary_df


def validate_schema(
    df: DataFrame,
    schema: StructType | None,
    strict: bool = False,
    extra_columns_policy: str = "ignore",
) -> DataFrame:
    """Validate only names and data types, returning the original DataFrame.

    The framework uses this for automatic source validation. It does not create
    the `is_valid` column and does not execute Spark actions.
    """
    if df is None or schema is None:
        raise ValueError("DataFrame and schema cannot be None")

    _validate_schema_match(
        df,
        schema,
        strict=strict,
        extra_columns_policy=extra_columns_policy,
    )
    return df


def summarize_struct_checks(df: DataFrame, schema: StructType) -> DataFrame:
    """Build failed-count diagnostics for checks declared in a StructType."""
    if df is None or schema is None:
        raise ValueError("DataFrame and schema cannot be None")

    checks = _extract_all_checks(schema)
    _validate_check_rules(df, checks)
    return _build_checks_summary(df, checks)


def _validate_schema_match(
    df: DataFrame,
    expected: StructType,
    *,
    strict: bool = False,
    extra_columns_policy: str = "ignore",
) -> None:
    """Validate expected columns and apply the configured extra-column policy."""
    actual = {field.name: field.dataType for field in df.schema.fields}
    expected_columns = {field.name for field in expected.fields}
    resolved_extra_columns_policy = _resolve_extra_columns_policy(
        strict=strict,
        extra_columns_policy=extra_columns_policy,
    )

    errors = []
    for field in expected.fields:
        if field.name not in actual:
            errors.append(f"ERROR: Missing column '{field.name}'")
            continue

        expected_type = field.dataType.simpleString()
        actual_type = actual[field.name].simpleString()

        if expected_type != actual_type:
            errors.append(
                f"ERROR: '{field.name}': expected {expected_type}, got {actual_type}"
            )

    extra_columns = [
        field.name for field in df.schema.fields if field.name not in expected_columns
    ]
    if extra_columns:
        message = f"Extra columns not declared in schema: {extra_columns}"
        if resolved_extra_columns_policy == "warn":
            warnings.warn(message, UserWarning, stacklevel=2)
        elif resolved_extra_columns_policy == "fail":
            errors.append(f"ERROR: {message}")

    if errors:
        raise ValueError("Schema mismatch:\n  - " + "\n  - ".join(errors))


def _resolve_extra_columns_policy(
    *,
    strict: bool,
    extra_columns_policy: str,
) -> str:
    """Resolve the explicit extra-column policy with strict_schema compatibility."""
    if not isinstance(extra_columns_policy, str):
        raise ValueError("extra_columns_policy must be a string")

    policy = extra_columns_policy.strip().lower()
    if policy not in {"ignore", "warn", "fail"}:
        raise ValueError(
            "extra_columns_policy must be one of ['fail', 'ignore', 'warn']"
        )

    if strict and policy == "ignore":
        return "warn"

    return policy


def _extract_all_checks(schema: StructType, prefix: str = "") -> list[Check]:
    """Extract check metadata recursively from a StructType."""
    checks = []

    for field in schema.fields:
        field_path = f"{prefix}.{field.name}" if prefix else field.name
        checks.extend(_extract_field_checks(field, field_path))

        if isinstance(field.dataType, StructType):
            checks.extend(_extract_all_checks(field.dataType, field_path))

    return checks


def _extract_field_checks(field: StructField, field_path: str) -> list[Check]:
    """Extract and validate all checks declared for one field."""
    raw_checks = (field.metadata or {}).get("checks")

    if raw_checks is None:
        return []

    if not isinstance(raw_checks, (list, tuple)):
        raise TypeError(f"{field_path}.checks must be list/tuple")

    return [
        _parse_check(check, field_path, index) for index, check in enumerate(raw_checks)
    ]


def _parse_check(check: dict[str, Any], field_path: str, index: int) -> Check:
    """Parse one metadata check and fail fast when it is malformed."""
    path = f"{field_path}.checks[{index}]"
    normalized = normalize_check_metadata(
        check,
        path=path,
        field_path=field_path,
    )

    return {
        **normalized,
        "alias": f"chk_{_safe_alias(field_path)}_{index}",
    }


def _add_is_valid_column(df: DataFrame, checks: list[Check]) -> DataFrame:
    """Add is_valid based on all error-severity checks."""
    if "is_valid" in df.columns:
        raise ValueError(
            "validate_struct cannot add is_valid because the DataFrame already "
            "contains an is_valid column"
        )

    error_checks = [check for check in checks if check["severity"] == "error"]

    if not error_checks:
        return df.withColumn("is_valid", F.lit(True))

    return df.withColumn("is_valid", _all_checks_pass(error_checks))


def _validate_check_rules(df: DataFrame, checks: list[Check]) -> None:
    """Fail fast on SQL rules that Spark cannot resolve without running a job."""
    for check in checks:
        rule = check["rule"]
        try:
            rule_df = df.select(F.expr(rule).cast("boolean").alias(check["alias"]))
            _ = rule_df.schema
        except Exception as exc:
            available_columns = ", ".join(df.columns)
            raise ValueError(
                "Invalid SQL check rule "
                f"'{check['name']}' for field '{check['field']}': {rule}. "
                f"Available columns: [{available_columns}]. "
                f"Spark error: {type(exc).__name__}: {exc}"
            ) from exc


def _build_checks_summary(df: DataFrame, checks: list[Check]) -> DataFrame:
    """Build a compact summary DataFrame for declared checks."""
    if not checks:
        return _empty_summary_df(df.sparkSession)

    if len(checks) > MAX_CHECKS_PER_BATCH:
        return _build_summary_batched(df, checks)

    return _build_summary_single_batch(df, checks)


def _build_summary_single_batch(
    df: DataFrame,
    checks: list[Check],
) -> DataFrame:
    """Compute failed counts for a single batch of checks."""
    df_with_flags = df.select("*", *[_failed_flag(check) for check in checks])
    counts_row = df_with_flags.agg(*[_failed_count(check) for check in checks])
    counts_long = _counts_row_to_long(counts_row, checks)

    return _join_with_metadata_python(df.sparkSession, checks, counts_long)


def _all_checks_pass(checks: list[Check]) -> Any:
    """Return the combined boolean expression for error-severity checks."""
    first_check, *remaining_checks = checks
    is_valid = _check_rule(first_check)
    for check in remaining_checks:
        is_valid = is_valid & _check_rule(check)
    return is_valid


def _check_rule(check: Check) -> Any:
    """Return one check rule as a null-safe boolean Spark expression."""
    return F.coalesce(F.expr(check["rule"]).cast("boolean"), F.lit(False))


def _failed_flag(check: Check) -> Any:
    """Return a temporary flag column where failed records are marked as 1."""
    return F.when(~_check_rule(check), 1).otherwise(0).alias(check["alias"])


def _failed_count(check: Check) -> Any:
    """Return the aggregate failed-count expression for one check."""
    return F.sum(check["alias"]).alias(check["alias"])


def _counts_row_to_long(
    counts_row: DataFrame,
    checks: list[Check],
) -> DataFrame:
    """Convert one wide aggregate row into alias/failed_count rows."""
    aliases = [check["alias"] for check in checks]
    return counts_row.select(
        F.explode(
            F.arrays_zip(
                F.array(*[F.lit(alias) for alias in aliases]).alias("alias"),
                F.array(*[F.col(alias) for alias in aliases]).alias("failed_count"),
            )
        ).alias("data")
    ).select(
        F.col("data.alias").alias("alias"),
        F.col("data.failed_count").cast("long").alias("failed_count"),
    )


def _join_with_metadata_python(
    spark: SparkSession,
    checks: list[Check],
    counts_df: DataFrame,
) -> DataFrame:
    """Join small check metadata with aggregated failed counts."""
    counts_map = {row.alias: row.failed_count for row in counts_df.collect()}

    result_rows = [
        Row(
            field=check["field"],
            check=check["name"],
            severity=check["severity"],
            message=check["message"],
            rule=check["rule"],
            failed_count=counts_map.get(check["alias"], 0),
            passed=counts_map.get(check["alias"], 0) == 0,
        )
        for check in checks
    ]

    return spark.createDataFrame(result_rows)


def _build_summary_batched(df: DataFrame, checks: list[Check]) -> DataFrame:
    """Compute validation summary in batches to limit expression size."""
    summaries = []
    for index in range(0, len(checks), MAX_CHECKS_PER_BATCH):
        batch = checks[index : index + MAX_CHECKS_PER_BATCH]
        summaries.append(_build_summary_single_batch(df, batch))

    return reduce(lambda left, right: left.union(right), summaries)


def _empty_summary_df(spark: SparkSession) -> DataFrame:
    """Create an empty summary DataFrame with the official summary schema."""
    return spark.createDataFrame(
        [],
        "field string, check string, severity string, message string, "
        "rule string, failed_count long, passed boolean",
    )


def _safe_alias(value: str) -> str:
    """Convert a field path into a SQL-safe alias."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", value)
