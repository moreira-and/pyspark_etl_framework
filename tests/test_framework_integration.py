from __future__ import annotations

from typing import cast

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_framework.contracts.extract import Extract
from etl_framework.contracts.load import Load
from etl_framework.contracts.pipeline import Pipeline
from etl_framework.contracts.transform import Transform
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.validate_struct import validate_struct

pytestmark = pytest.mark.integration

SOURCE_STRUCT = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
    ]
)

TARGET_STRUCT = StructType(
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
        StructField("name_upper", StringType(), nullable=False),
    ]
)


class InlineExtract(Extract):
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("extract")
        return spark.createDataFrame([(1, "ana"), (2, "bruno")], SOURCE_STRUCT)

    def _check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("check")
        return df


class InlineTransform(Transform):
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
        return df.withColumn("name_upper", F.upper("name"))

    def _validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("validate")
        target_struct = cast(StructType, config.target_struct)
        validated_df, _ = validate_struct(df, target_struct)
        return validated_df


class InlineLoad(Load):
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.loaded_rows: list[tuple[int, str, str, bool]] = []

    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("load")
        self.loaded_rows = [
            (row.id, row.name, row.name_upper, row.is_valid)
            for row in df.orderBy("id").collect()
        ]

    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("certify")


@pytest.mark.integration
def test_inline_framework_integration_runs_full_contract(
    spark: SparkSession,
) -> None:
    # Arrange
    events: list[str] = []
    load = InlineLoad(events)
    pipeline = Pipeline(
        spark=spark,
        config=EtlRunConfig(
            pipeline_name="inline_integration",
            target_schema="test",
            target_table="people",
            target_path="memory://people",
            target_key=("id",),
            source_struct=SOURCE_STRUCT,
            target_struct=TARGET_STRUCT,
            write_mode="overwrite",
        ),
        extract=InlineExtract(events),
        transform=InlineTransform(events),
        load=load,
    )

    # Act
    result = pipeline.run()

    # Assert
    assert events == ["extract", "check", "transform", "validate", "load", "certify"]
    assert result.columns == ["id", "name", "name_upper", "is_valid"]
    assert load.loaded_rows == [
        (1, "ana", "ANA", True),
        (2, "bruno", "BRUNO", True),
    ]


def test_spark_integration_smoke_finishes_with_inline_dataframe(
    spark: SparkSession,
) -> None:
    # Arrange
    events: list[str] = []
    load = InlineLoad(events)
    pipeline = Pipeline(
        spark=spark,
        config=EtlRunConfig(
            pipeline_name="inline_smoke",
            target_schema="test",
            target_table="people_smoke",
            target_path="memory://people_smoke",
            target_key=("id",),
            source_struct=SOURCE_STRUCT,
            target_struct=TARGET_STRUCT,
        ),
        extract=InlineExtract(events),
        transform=InlineTransform(events),
        load=load,
    )

    # Act
    result = pipeline.run()

    # Assert
    assert events == ["extract", "check", "transform", "validate", "load", "certify"]
    assert result.schema["id"].dataType == IntegerType()
    assert load.loaded_rows == [
        (1, "ana", "ANA", True),
        (2, "bruno", "BRUNO", True),
    ]
