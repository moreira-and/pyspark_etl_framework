from __future__ import annotations

import logging
import re
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession, functions as F
from pyspark.sql.types import StructField, StructType

logger = logging.getLogger(__name__)

MAX_CHECKS_PER_BATCH = 100
ALLOWED_SEVERITIES = {"error", "warning"}


def validate_struct(
    df: DataFrame,
    schema: StructType,
    compute_summary: bool = False,
) -> tuple[DataFrame, DataFrame | None]:
    """Validate a DataFrame against a StructType contract.

    The function validates expected column names and data types, then executes
    simple declarative checks stored in StructField metadata. Error-severity
    checks define the generated is_valid technical column. The input DataFrame
    must not already contain is_valid; this prevents silent overwrite after a
    previous validation step. Summary calculation is optional because it
    triggers Spark actions.
    """
    if df is None or schema is None:
        raise ValueError("DataFrame and schema cannot be None")

    _validate_schema_match(df, schema)

    checks = _extract_all_checks(schema)
    logger.info("Validating with %s checks", len(checks))

    validated_df = _add_is_valid_column(df, checks)

    summary_df = None
    if compute_summary:
        logger.info("Computing validation summary")
        summary_df = _build_checks_summary(validated_df, checks)

    return validated_df, summary_df


def _validate_schema_match(df: DataFrame, expected: StructType) -> None:
    """Validate expected columns and data types using Spark schema metadata."""
    actual = {field.name: field.dataType for field in df.schema.fields}

    errors = []
    for field in expected.fields:
        if field.name not in actual:
            errors.append(f"Missing: '{field.name}'")
            continue

        expected_type = field.dataType.simpleString()
        actual_type = actual[field.name].simpleString()

        if expected_type != actual_type:
            errors.append(
                f"'{field.name}': expected {expected_type}, got {actual_type}"
            )

    if errors:
        raise ValueError("Schema mismatch:\n  - " + "\n  - ".join(errors))


def _extract_all_checks(schema: StructType, prefix: str = "") -> list[dict[str, Any]]:
    """Extract check metadata recursively from a StructType."""
    checks = []

    for field in schema.fields:
        field_path = f"{prefix}.{field.name}" if prefix else field.name
        checks.extend(_extract_field_checks(field, field_path))

        if isinstance(field.dataType, StructType):
            checks.extend(_extract_all_checks(field.dataType, field_path))

    return checks


def _extract_field_checks(field: StructField, field_path: str) -> list[dict[str, Any]]:
    """Extract and validate all checks declared for one field."""
    raw_checks = (field.metadata or {}).get("checks")

    if raw_checks is None:
        return []

    if not isinstance(raw_checks, (list, tuple)):
        raise TypeError(f"{field_path}.checks must be list/tuple")

    return [
        _parse_check(check, field_path, index)
        for index, check in enumerate(raw_checks)
    ]


def _parse_check(check: dict[str, Any], field_path: str, index: int) -> dict[str, Any]:
    """Parse one metadata check and fail fast when it is malformed."""
    path = f"{field_path}.checks[{index}]"

    if not isinstance(check, dict):
        raise TypeError(f"{path} must be dict")

    name = _require_string(check, "name", path)
    rule = _require_string(check, "rule", path)
    severity = check.get("severity") or "warning"

    if not isinstance(severity, str):
        raise TypeError(f"{path}.severity must be string")

    if severity not in ALLOWED_SEVERITIES:
        raise ValueError(f"{path}.severity must be one of {sorted(ALLOWED_SEVERITIES)}")

    message = check.get("message") or f"Check '{name}' failed for '{field_path}'"
    if not isinstance(message, str):
        raise TypeError(f"{path}.message must be string")

    return {
        "field": field_path,
        "name": name,
        "rule": rule,
        "severity": severity,
        "message": message,
        "alias": f"chk_{_safe_alias(field_path)}_{index}",
    }


def _require_string(check: dict[str, Any], field: str, path: str) -> str:
    """Return a required non-empty string from a metadata check."""
    value = check.get(field, "")

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}.{field} must be non-empty string")

    return value.strip()


def _add_is_valid_column(df: DataFrame, checks: list[dict[str, Any]]) -> DataFrame:
    """Add is_valid based on all error-severity checks."""
    if "is_valid" in df.columns:
        raise ValueError(
            "validate_struct cannot add is_valid because the DataFrame already "
            "contains an is_valid column"
        )

    error_checks = [check for check in checks if check["severity"] == "error"]

    if not error_checks:
        return df.withColumn("is_valid", F.lit(True))

    is_valid = None
    for check in error_checks:
        rule = F.coalesce(F.expr(check["rule"]).cast("boolean"), F.lit(False))
        is_valid = rule if is_valid is None else (is_valid & rule)

    return df.withColumn("is_valid", is_valid)


def _build_checks_summary(df: DataFrame, checks: list[dict[str, Any]]) -> DataFrame:
    """Build a compact summary DataFrame for declared checks."""
    if not checks:
        return _empty_summary_df(df.sparkSession)

    if len(checks) > MAX_CHECKS_PER_BATCH:
        return _build_summary_batched(df, checks)

    return _build_summary_single_batch(df, checks)


def _build_summary_single_batch(
    df: DataFrame,
    checks: list[dict[str, Any]],
) -> DataFrame:
    """Compute failed counts for a single batch of checks."""
    check_exprs = []
    for check in checks:
        rule = F.coalesce(F.expr(check["rule"]).cast("boolean"), F.lit(False))
        check_exprs.append(F.when(~rule, 1).otherwise(0).alias(check["alias"]))

    df_with_flags = df.select("*", *check_exprs)
    agg_exprs = [F.sum(check["alias"]).alias(check["alias"]) for check in checks]
    counts_row = df_with_flags.agg(*agg_exprs)

    aliases = [check["alias"] for check in checks]
    counts_long = counts_row.select(
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

    return _join_with_metadata_python(df.sparkSession, checks, counts_long)


def _join_with_metadata_python(
    spark: SparkSession,
    checks: list[dict[str, Any]],
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


def _build_summary_batched(df: DataFrame, checks: list[dict[str, Any]]) -> DataFrame:
    """Compute validation summary in batches to limit expression size."""
    logger.info(
        "Processing %s checks in batches of %s",
        len(checks),
        MAX_CHECKS_PER_BATCH,
    )

    summaries = []
    total_batches = (len(checks) - 1) // MAX_CHECKS_PER_BATCH + 1
    for index in range(0, len(checks), MAX_CHECKS_PER_BATCH):
        batch = checks[index:index + MAX_CHECKS_PER_BATCH]
        logger.info(
            "Processing validation batch %s/%s",
            index // MAX_CHECKS_PER_BATCH + 1,
            total_batches,
        )
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
