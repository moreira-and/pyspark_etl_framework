from __future__ import annotations

import logging

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform
from etl_framework.infra.logger import log_event
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils import (
    assert_freshness_at_least,
    assert_no_invalid_records,
    assert_reconciled_by_key,
    assert_target_key_not_null,
    assert_target_key_unique,
    assert_volume_between,
    require_is_valid_column,
    require_operational_metrics,
    split_valid_invalid,
    validate_schema,
    validate_struct,
)

pytestmark = pytest.mark.integration


class CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_validate_schema_rejects_missing_source_column(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame([(1,)], "id int")
    source_struct = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Missing column 'name'"):
        validate_schema(df, source_struct)


def test_validate_schema_rejects_incompatible_source_type(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame([("1",)], "id string")
    source_struct = StructType([StructField("id", IntegerType(), nullable=False)])

    # Act / Assert
    with pytest.raises(ValueError, match="expected int, got string"):
        validate_schema(df, source_struct)


def test_required_null_check_blocks_invalid_target_record(
    spark: SparkSession,
) -> None:
    # Arrange
    target_struct = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField(
                "name",
                StringType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "name_required",
                            "rule": "name IS NOT NULL",
                            "severity": "error",
                        }
                    ]
                },
            ),
        ]
    )
    df = spark.createDataFrame([(1, None)], "id int, name string")

    # Act
    validated_df, _ = validate_struct(df, target_struct)

    # Assert
    with pytest.raises(ValueError, match="Invalid records"):
        assert_no_invalid_records(validated_df)


def test_pipeline_auto_validation_adds_is_valid_before_write(
    spark: SparkSession,
) -> None:
    # Arrange
    class InlineExtract(Extract):
        def _extract(self, spark, config, context):
            return spark.createDataFrame([(1, "ana")], "id int, name string")

        def _check(self, df, spark, config, context):
            return validate_schema(df, config.source_struct)

    class JuniorTransform(Transform):
        def _transform(self, df, spark, config, context):
            return df

    class GuardedLoad(Load):
        def __init__(self) -> None:
            self.loaded = False

        def _load(self, df, spark, config, context):
            require_is_valid_column(df)
            self.loaded = True

        def _certify(self, df, spark, config, context):
            return None

    load = GuardedLoad()
    config = EtlRunConfig(
        pipeline_name="missing_validation",
        target_schema="silver",
        target_table="people",
        target_path="memory://people",
        target_key=("id",),
        source_struct=StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=False),
            ]
        ),
        target_struct=StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=False),
            ]
        ),
    )
    pipeline = Pipeline(
        spark,
        config,
        extract=InlineExtract(),
        transform=JuniorTransform(),
        load=load,
    )

    # Act
    pipeline.run()

    # Assert
    assert load.loaded is True


def test_target_key_checks_block_nulls_and_duplicates(
    spark: SparkSession,
) -> None:
    # Arrange
    null_df = spark.createDataFrame(
        [(1, "ana"), (None, "bruno")], "id int, name string"
    )
    duplicate_df = spark.createDataFrame(
        [(1, "ana"), (1, "ana again")],
        "id int, name string",
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Null value"):
        assert_target_key_not_null(null_df, ("id",))
    with pytest.raises(ValueError, match="Duplicate target key"):
        assert_target_key_unique(duplicate_df, ("id",))


def test_volume_freshness_and_reconciliation_checks(
    spark: SparkSession,
) -> None:
    # Arrange
    context = EtlExecutionContext(run_id="run-volume")
    df = spark.createDataFrame(
        [(1, "2026-05-17"), (2, "2026-05-17")],
        "id int, updated_at string",
    )
    stale_df = spark.createDataFrame([(1, "2026-05-01")], "id int, updated_at string")
    missing_target_df = spark.createDataFrame([(1,)], "id int")

    # Act
    row_count = assert_volume_between(
        df,
        min_rows=1,
        max_rows=3,
        context=context,
    )
    assert_freshness_at_least(df, column="updated_at", min_value="2026-05-16")

    # Assert
    assert row_count == 2
    assert context.metrics["rows_read"] == 2
    with pytest.raises(ValueError, match="above expected maximum"):
        assert_volume_between(df, max_rows=1)
    with pytest.raises(ValueError, match="Freshness check failed"):
        assert_freshness_at_least(
            stale_df,
            column="updated_at",
            min_value="2026-05-16",
        )
    with pytest.raises(ValueError, match="missing source keys"):
        assert_reconciled_by_key(df.select("id"), missing_target_df, ("id",))


def test_invalid_records_can_be_split_or_blocked(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, True), (2, False)],
        "id int, is_valid boolean",
    )

    # Act
    valid_df, invalid_df = split_valid_invalid(df)

    # Assert
    assert valid_df.select("id").collect() == [(1,)]
    assert invalid_df.select("id").collect() == [(2,)]
    with pytest.raises(ValueError, match="Invalid records"):
        assert_no_invalid_records(df)


def test_operational_metrics_are_logged_when_required_metrics_exist() -> None:
    # Arrange
    logger = logging.getLogger("tests.production_checks.metrics")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)

    context = EtlExecutionContext(
        run_id="run-metrics",
        metrics={
            "rows_read": 2,
            "rows_valid": 2,
            "rows_invalid": 0,
            "rows_written": 2,
        },
    )
    require_operational_metrics(context)
    config = EtlRunConfig(
        pipeline_name="metrics_pipeline",
        target_schema="silver",
        target_table="people",
        target_path="memory://people",
        target_key=("id",),
    )

    # Act
    log_event(logger, "certify_succeeded", config, context, stage="certify")

    # Assert
    payload = handler.records[0].msg
    assert isinstance(payload, dict)
    assert payload["metrics"] == {
        "rows_read": 2,
        "rows_valid": 2,
        "rows_invalid": 0,
        "rows_written": 2,
    }


def test_operational_metrics_require_reason_when_metric_is_missing() -> None:
    # Arrange
    logger = logging.getLogger("tests.production_checks.missing_metric_reason")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)
    context = EtlExecutionContext(
        run_id="run-missing-metrics",
        metrics={
            "rows_read": 2,
            "rows_written": 2,
        },
    )
    config = EtlRunConfig(
        pipeline_name="missing_metrics_pipeline",
        target_schema="silver",
        target_table="people",
        target_path="memory://people",
        target_key=("id",),
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Missing operational metrics"):
        require_operational_metrics(context)

    require_operational_metrics(
        context,
        missing_reasons={
            "rows_valid": "destination reports only total accepted rows",
            "rows_invalid": "pipeline blocks invalid rows before load",
        },
    )
    assert context.metrics["rows_valid_missing_reason"]
    assert context.metrics["rows_invalid_missing_reason"]

    log_event(logger, "certify_succeeded", config, context, stage="certify")
    payload = handler.records[0].msg
    assert isinstance(payload, dict)
    assert "rows_valid_missing_reason" in payload["metrics"]
    assert "rows_invalid_missing_reason" in payload["metrics"]
