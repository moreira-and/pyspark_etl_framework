from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
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


def test_validate_struct_ignores_extra_columns_by_default(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame([(1, "extra")], "id int, source_system string")
    expected_schema = StructType([StructField("id", IntegerType(), nullable=False)])

    # Act
    validated_df, _ = validate_struct(df, expected_schema)

    # Assert
    assert validated_df.columns == ["id", "source_system", "is_valid"]


def test_validate_struct_warns_on_extra_columns_when_policy_is_warn(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame([(1, "extra")], "id int, source_system string")
    expected_schema = StructType([StructField("id", IntegerType(), nullable=False)])

    # Act / Assert
    with pytest.warns(UserWarning, match="Extra columns not declared"):
        validate_struct(df, expected_schema, extra_columns_policy="warn")


def test_validate_struct_fails_on_extra_columns_when_policy_is_fail(
    spark: SparkSession,
) -> None:
    # Arrange
    df = spark.createDataFrame([(1, "extra")], "id int, source_system string")
    expected_schema = StructType([StructField("id", IntegerType(), nullable=False)])

    # Act / Assert
    with pytest.raises(ValueError, match="Extra columns not declared"):
        validate_struct(df, expected_schema, extra_columns_policy="fail")


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


def test_validate_struct_default_does_not_collect_when_summary_is_disabled(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    collect_calls: list[str] = []

    def fail_collect(self: DataFrame) -> object:
        collect_calls.append(type(self).__name__)
        raise AssertionError("collect should not run without compute_summary=True")

    monkeypatch.setattr(DataFrame, "collect", fail_collect)
    df = spark.createDataFrame(
        [(1, 10)],
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
    validated_df, summary_df = validate_struct(df, schema)

    # Assert
    assert summary_df is None
    assert "is_valid" in validated_df.columns
    assert collect_calls == []


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


@pytest.mark.parametrize(
    ("rule", "expected_message"),
    [
        pytest.param("amount >", "Invalid SQL check rule", id="invalid-syntax"),
        pytest.param("missing_amount > 0", "Available columns", id="missing-column"),
    ],
)
def test_validate_struct_reports_invalid_sql_rules_clearly(
    spark: SparkSession,
    rule: str,
    expected_message: str,
) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1,)],
        StructType([StructField("amount", IntegerType(), nullable=False)]),
    )
    schema = StructType(
        [
            StructField(
                "amount",
                IntegerType(),
                nullable=False,
                metadata={
                    "checks": [
                        {
                            "name": "bad_amount_rule",
                            "rule": rule,
                            "severity": "error",
                        }
                    ]
                },
            )
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError, match=expected_message) as exc_info:
        validate_struct(df, schema)
    assert "bad_amount_rule" in str(exc_info.value)


def test_validate_struct_accepts_nested_field_rules(spark: SparkSession) -> None:
    # Arrange
    nested_type = StructType([StructField("amount", IntegerType(), nullable=False)])
    df = spark.createDataFrame(
        [((10,),), ((0,),)],
        StructType([StructField("payload", nested_type, nullable=False)]),
    )
    schema = StructType(
        [
            StructField(
                "payload",
                StructType(
                    [
                        StructField(
                            "amount",
                            IntegerType(),
                            nullable=False,
                            metadata={
                                "checks": [
                                    {
                                        "name": "payload_amount_positive",
                                        "rule": "payload.amount > 0",
                                        "severity": "error",
                                    }
                                ]
                            },
                        )
                    ]
                ),
                nullable=False,
            )
        ]
    )

    # Act
    validated_df, summary_df = validate_struct(df, schema)

    # Assert
    assert summary_df is None
    assert validated_df.select("is_valid").collect() == [(True,), (False,)]


def test_validate_struct_batches_many_summary_checks(spark: SparkSession) -> None:
    # Arrange
    checks = [
        {
            "name": f"amount_non_negative_{index}",
            "rule": "amount >= 0",
            "severity": "warning",
        }
        for index in range(105)
    ]
    df = spark.createDataFrame(
        [(1,), (2,)],
        StructType([StructField("amount", IntegerType(), nullable=False)]),
    )
    schema = StructType(
        [
            StructField(
                "amount",
                IntegerType(),
                nullable=False,
                metadata={"checks": checks},
            )
        ]
    )

    # Act
    validated_df, summary_df = validate_struct(df, schema, compute_summary=True)

    # Assert
    assert validated_df.select("is_valid").distinct().collect() == [(True,)]
    assert summary_df is not None
    summary_rows = summary_df.select("check", "failed_count", "passed").collect()
    assert len(summary_rows) == 105
    assert all(row.failed_count == 0 and row.passed is True for row in summary_rows)
