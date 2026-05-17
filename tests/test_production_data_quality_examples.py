from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_framework.utils.validate_struct import validate_struct

pytestmark = pytest.mark.integration


def assert_not_null_keys(df: DataFrame, keys: tuple[str, ...]) -> None:
    condition = F.lit(False)
    for key in keys:
        condition = condition | F.col(key).isNull()
    if df.filter(condition).limit(1).count() > 0:
        raise ValueError(f"Null value found in target key: {keys}")


def assert_unique_keys(df: DataFrame, keys: tuple[str, ...]) -> None:
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1)
    if duplicates.limit(1).count() > 0:
        raise ValueError(f"Duplicate target key found: {keys}")


def assert_volume_between(df: DataFrame, *, min_rows: int, max_rows: int) -> None:
    row_count = df.count()
    if row_count < min_rows or row_count > max_rows:
        raise ValueError(
            f"Row count {row_count} outside expected range [{min_rows}, {max_rows}]"
        )


def split_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    return df.filter(F.col("is_valid")), df.filter(~F.col("is_valid"))


def block_invalid_records(df: DataFrame) -> None:
    if df.filter(~F.col("is_valid")).limit(1).count() > 0:
        raise ValueError("Invalid records must be quarantined or fixed before load")


def test_quality_check_blocks_null_target_key(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "ana"), (None, "bruno")],
        "id int, name string",
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Null value found"):
        assert_not_null_keys(df, ("id",))


def test_quality_check_blocks_duplicate_target_key(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "ana"), (1, "ana duplicate")],
        "id int, name string",
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Duplicate target key"):
        assert_unique_keys(df, ("id",))


def test_quality_check_blocks_volume_outside_expected_range(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame([(1, "ana")], "id int, name string")

    # Act / Assert
    with pytest.raises(ValueError, match="outside expected range"):
        assert_volume_between(df, min_rows=2, max_rows=10)


def test_quality_policy_quarantines_invalid_records(
    spark: SparkSession,
) -> None:
    # Arrange
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField(
                "name",
                StringType(),
                nullable=True,
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
    df = spark.createDataFrame([(1, "ana"), (2, None)], schema)
    validated_df, _ = validate_struct(df, schema)

    # Act
    valid_df, quarantine_df = split_quarantine(validated_df)

    # Assert
    assert valid_df.select("id").collect() == [(1,)]
    assert quarantine_df.select("id").collect() == [(2,)]


def test_quality_policy_can_block_invalid_records(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, True), (2, False)],
        "id int, is_valid boolean",
    )

    # Act / Assert
    with pytest.raises(ValueError, match="Invalid records"):
        block_invalid_records(df)
