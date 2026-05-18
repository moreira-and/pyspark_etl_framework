from __future__ import annotations

import logging
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_framework.contracts import Extract, Load, Transform
from etl_framework.infra.errors import CheckError, LoadError, ValidateError
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

pytestmark = pytest.mark.integration

RUN_ID = "run-template-method"

SOURCE_STRUCT = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
    ]
)
TARGET_STRUCT = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name_upper", StringType(), nullable=False),
    ]
)


class CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def config(**overrides: Any) -> EtlRunConfig:
    values = {
        "pipeline_name": "template_method_contract",
        "target_schema": "silver",
        "target_table": "people",
        "target_path": "memory://people",
        "target_key": ("id",),
        "source_struct": SOURCE_STRUCT,
        "target_struct": TARGET_STRUCT,
    }
    values.update(overrides)
    return EtlRunConfig(**values)


def context() -> EtlExecutionContext:
    return EtlExecutionContext(run_id=RUN_ID)


def capture_events(pipeline_name: str) -> CapturingHandler:
    logger = logging.getLogger(pipeline_name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)
    return handler


def event_names(handler: CapturingHandler) -> list[str]:
    return [str(record.msg["event"]) for record in handler.records]


class TemplateExtract(Extract):
    def __init__(
        self,
        events: list[str],
        *,
        rows: list[tuple[Any, ...]] | None = None,
        schema: StructType | str = SOURCE_STRUCT,
    ) -> None:
        self.events = events
        self.rows = rows or [(1, "ana")]
        self.schema = schema

    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("extract")
        return spark.createDataFrame(self.rows, self.schema)

    def _custom_check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("custom_check")
        return df


class TemplateTransform(Transform):
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def _transform(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("transform")
        return df.select("id", F.upper("name").alias("name_upper"))

    def _custom_validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("custom_validate")
        return df


class TemplateLoad(Load):
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("load")

    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("certify")


def test_extract_run_should_execute_extract_then_source_struct_check_automatically(
    spark: SparkSession,
) -> None:
    # Arrange
    handler = capture_events("template_method_contract")
    events: list[str] = []
    extract = TemplateExtract(events)

    # Act
    result = extract.run(spark=spark, config=config(), context=context())

    # Assert
    assert result.columns == ["id", "name"]
    assert events == ["extract", "custom_check"]
    assert event_names(handler) == [
        "extract_started",
        "extract_succeeded",
        "check_started",
        "check_succeeded",
    ]


def test_extract_run_should_raise_managed_error_when_source_struct_validation_fails(
    spark: SparkSession,
) -> None:
    # Arrange
    events: list[str] = []
    extract = TemplateExtract(events, rows=[(1,)], schema="id int")

    # Act / Assert
    with pytest.raises(CheckError, match="Missing column 'name'") as exc_info:
        extract.run(spark=spark, config=config(), context=context())

    assert exc_info.value.run_id == RUN_ID
    assert exc_info.value.stage == "check"
    assert events == ["extract"]


def test_transform_run_should_execute_transform_then_target_struct_validation_automatically(
    spark: SparkSession,
) -> None:
    # Arrange
    handler = capture_events("template_method_contract")
    events: list[str] = []
    source_df = spark.createDataFrame([(1, "ana")], SOURCE_STRUCT)
    transform = TemplateTransform(events)

    # Act
    result = transform.run(
        df=source_df,
        spark=spark,
        config=config(),
        context=context(),
    )

    # Assert
    assert result.columns == ["id", "name_upper", "is_valid"]
    assert events == ["transform", "custom_validate"]
    assert event_names(handler) == [
        "transform_started",
        "transform_succeeded",
        "validate_started",
        "validate_succeeded",
    ]


def test_transform_run_should_raise_managed_error_when_target_struct_validation_fails(
    spark: SparkSession,
) -> None:
    # Arrange
    class MissingTargetColumnTransform(Transform):
        def _transform(self, df, spark, config, context):
            return df.select("id")

    source_df = spark.createDataFrame([(1, "ana")], SOURCE_STRUCT)

    # Act / Assert
    with pytest.raises(ValidateError, match="Missing column 'name_upper'") as exc_info:
        MissingTargetColumnTransform().run(
            df=source_df,
            spark=spark,
            config=config(),
            context=context(),
        )

    assert exc_info.value.run_id == RUN_ID
    assert exc_info.value.stage == "validate"


def test_load_run_should_skip_real_load_when_dry_run_is_enabled(
    spark: SparkSession,
) -> None:
    # Arrange
    handler = capture_events("template_method_contract")
    events: list[str] = []
    load = TemplateLoad(events)
    df = spark.createDataFrame(
        [(1, "ANA", True)], "id int, name_upper string, is_valid boolean"
    )

    # Act
    load.run(
        df=df,
        spark=spark,
        config=config(dry_run=True),
        context=context(),
    )

    # Assert
    assert events == []
    assert event_names(handler) == [
        "load_started",
        "dry_run_load_skipped",
        "dry_run_evidence",
        "dry_run_load_completed",
    ]


def test_load_run_should_execute_load_then_certify_when_dry_run_is_disabled(
    spark: SparkSession,
) -> None:
    # Arrange
    handler = capture_events("template_method_contract")
    events: list[str] = []
    load = TemplateLoad(events)
    df = spark.createDataFrame(
        [(1, "ANA", True)], "id int, name_upper string, is_valid boolean"
    )

    # Act
    load.run(df=df, spark=spark, config=config(), context=context())

    # Assert
    assert events == ["load", "certify"]
    assert event_names(handler) == [
        "load_started",
        "load_succeeded",
        "certify_started",
        "certify_succeeded",
    ]


def test_load_run_should_raise_managed_error_when_input_is_not_dataframe(
    spark: SparkSession,
) -> None:
    # Arrange
    events: list[str] = []
    load = TemplateLoad(events)

    # Act / Assert
    with pytest.raises(
        LoadError, match="must return pyspark.sql.DataFrame"
    ) as exc_info:
        load.run(
            df=["not", "a", "dataframe"],  # type: ignore[arg-type]
            spark=spark,
            config=config(),
            context=context(),
        )

    assert exc_info.value.run_id == RUN_ID
    assert exc_info.value.stage == "load"
    assert events == []
