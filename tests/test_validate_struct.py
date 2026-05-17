from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from etl_framework.utils.validate_struct import validate_struct

pytestmark = pytest.mark.integration


def test_validate_struct_rejects_missing_required_column(
    spark: SparkSession,
) -> None:
    # Arrange
    # Protects against silent schema drift in production pipelines.
    df = spark.createDataFrame(
        [(1,)],
        StructType([StructField("id", IntegerType(), nullable=False)]),
    )
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("amount", IntegerType(), nullable=True),
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError) as exc_info:
        validate_struct(df, expected_schema)

    message = str(exc_info.value)
    assert "Schema mismatch" in message
    assert "Missing column 'amount'" in message


def test_validate_struct_rejects_type_mismatch(
    spark: SparkSession,
) -> None:
    # Arrange
    # Protects against a source changing a column type without failing fast.
    df = spark.createDataFrame(
        [("1",)],
        StructType([StructField("id", StringType(), nullable=False)]),
    )
    expected_schema = StructType([StructField("id", IntegerType(), nullable=False)])

    # Act / Assert
    with pytest.raises(ValueError) as exc_info:
        validate_struct(df, expected_schema)

    message = str(exc_info.value)
    assert "Schema mismatch" in message
    assert "'id': expected int, got string" in message


def test_validate_struct_strict_warns_on_extra_columns(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "unexpected")],
        StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("source_system", StringType(), nullable=True),
            ]
        ),
    )
    expected_schema = StructType([StructField("id", IntegerType(), nullable=False)])

    # Act
    with pytest.warns(UserWarning, match="Extra columns not declared in schema"):
        validated_df, summary_df = validate_struct(df, expected_schema, strict=True)

    # Assert
    assert summary_df is None
    assert validated_df.columns == ["id", "source_system", "is_valid"]
    assert validated_df.select("id", "source_system", "is_valid").collect() == [
        (1, "unexpected", True)
    ]


def test_warning_severity_does_not_invalidate_record(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, 0), (2, 10)],
        StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("amount", IntegerType(), nullable=False),
            ]
        ),
    )
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField(
                "amount",
                IntegerType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "positive_amount",
                            "rule": "amount > 0",
                            "severity": "warning",
                        }
                    ]
                },
            ),
        ]
    )

    # Act
    validated_df, summary_df = validate_struct(df, schema, compute_summary=True)

    # Assert
    assert validated_df.orderBy("id").select("id", "is_valid").collect() == [
        (1, True),
        (2, True),
    ]
    assert summary_df is not None
    assert summary_df.select(
        "field", "check", "severity", "failed_count", "passed"
    ).collect() == [("amount", "positive_amount", "warning", 1, False)]


def test_validate_struct_combines_error_and_warning_checks_correctly(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [
            (1, 0, "new"),
            (2, 10, "unknown"),
            (3, 10, "paid"),
            (4, 150, "paid"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("amount", IntegerType(), nullable=False),
                StructField("status", StringType(), nullable=False),
            ]
        ),
    )
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField(
                "amount",
                IntegerType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "positive_amount",
                            "rule": "amount > 0",
                            "severity": "error",
                        },
                        {
                            "name": "amount_below_manual_review",
                            "rule": "amount < 100",
                            "severity": "warning",
                        },
                    ]
                },
            ),
            StructField(
                "status",
                StringType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "known_status",
                            "rule": "status IN ('new', 'paid')",
                            "severity": "error",
                        }
                    ]
                },
            ),
        ]
    )

    # Act
    validated_df, summary_df = validate_struct(df, schema, compute_summary=True)

    # Assert
    assert validated_df.orderBy("id").select("id", "is_valid").collect() == [
        (1, False),
        (2, False),
        (3, True),
        (4, True),
    ]
    assert summary_df is not None
    assert summary_df.select(
        "field", "check", "severity", "failed_count", "passed"
    ).collect() == [
        ("amount", "positive_amount", "error", 1, False),
        ("amount", "amount_below_manual_review", "warning", 1, False),
        ("status", "known_status", "error", 1, False),
    ]


def test_compute_summary_is_opt_in(spark: SparkSession) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, 0), (2, 10)],
        StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("amount", IntegerType(), nullable=False),
            ]
        ),
    )
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField(
                "amount",
                IntegerType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "positive_amount",
                            "rule": "amount > 0",
                            "severity": "error",
                        }
                    ]
                },
            ),
        ]
    )

    # Act
    validated_without_summary, summary_df = validate_struct(df, schema)
    validated_with_summary, computed_summary_df = validate_struct(
        df,
        schema,
        compute_summary=True,
    )

    # Assert
    assert summary_df is None
    assert validated_without_summary.orderBy("id").select(
        "id", "is_valid"
    ).collect() == [
        (1, False),
        (2, True),
    ]
    assert validated_with_summary.orderBy("id").select("id", "is_valid").collect() == [
        (1, False),
        (2, True),
    ]
    assert computed_summary_df is not None
    assert computed_summary_df.select(
        "check", "severity", "failed_count", "passed"
    ).collect() == [("positive_amount", "error", 1, False)]


def test_validate_struct_rejects_existing_is_valid_column(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, True)],
        StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("is_valid", BooleanType(), nullable=False),
            ]
        ),
    )
    schema = StructType([StructField("id", IntegerType(), nullable=False)])

    # Act / Assert
    with pytest.raises(ValueError, match="already contains an is_valid column"):
        validate_struct(df, schema)
