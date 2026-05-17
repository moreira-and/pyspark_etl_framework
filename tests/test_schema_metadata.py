from __future__ import annotations

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_framework.utils.schema_metadata import SchemaMetadataValidator


def schema_with_checks(checks: object) -> StructType:
    return StructType(
        [
            StructField(
                "amount",
                IntegerType(),
                nullable=False,
                metadata={"checks": checks},
            )
        ]
    )


def test_schema_metadata_requires_checks_to_be_a_sequence() -> None:
    # Arrange
    schema = schema_with_checks("amount > 0")

    # Act / Assert
    with pytest.raises(TypeError, match="checks must be list/tuple"):
        SchemaMetadataValidator(schema, "target_struct").validate()


def test_schema_metadata_requires_each_check_to_be_a_dict() -> None:
    # Arrange
    schema = schema_with_checks([object()])

    # Act / Assert
    with pytest.raises(TypeError, match="must be dict"):
        SchemaMetadataValidator(schema, "target_struct").validate()


@pytest.mark.parametrize(
    ("check", "expected_message"),
    [
        pytest.param(
            {"rule": "amount > 0"},
            "name must be non-empty string",
            id="missing-name",
        ),
        pytest.param(
            {"name": "positive", "rule": ""},
            "rule must be non-empty string",
            id="empty-rule",
        ),
    ],
)
def test_schema_metadata_requires_name_and_rule(
    check: dict[str, object],
    expected_message: str,
) -> None:
    # Arrange
    schema = schema_with_checks([check])

    # Act / Assert
    with pytest.raises(ValueError, match=expected_message):
        SchemaMetadataValidator(schema, "target_struct").validate()


@pytest.mark.parametrize(
    ("severity", "error_type", "expected_message"),
    [
        pytest.param(10, TypeError, "severity must be string", id="not-string"),
        pytest.param(
            "critical",
            ValueError,
            "severity must be one of",
            id="unknown-value",
        ),
    ],
)
def test_schema_metadata_validates_severity(
    severity: object,
    error_type: type[Exception],
    expected_message: str,
) -> None:
    # Arrange
    schema = schema_with_checks(
        [{"name": "positive", "rule": "amount > 0", "severity": severity}]
    )

    # Act / Assert
    with pytest.raises(error_type, match=expected_message):
        SchemaMetadataValidator(schema, "target_struct").validate()


def test_schema_metadata_requires_message_to_be_a_string() -> None:
    # Arrange
    schema = schema_with_checks(
        [{"name": "positive", "rule": "amount > 0", "message": 10}]
    )

    # Act / Assert
    with pytest.raises(TypeError, match="message must be string"):
        SchemaMetadataValidator(schema, "target_struct").validate()


@pytest.mark.parametrize(
    ("rule", "expected_message"),
    [
        pytest.param("(amount > 0", "Unbalanced parentheses", id="unbalanced"),
        pytest.param("amount >> 0", "Invalid SQL operator", id="invalid-operator"),
    ],
)
def test_schema_metadata_rejects_common_sql_rule_mistakes(
    rule: str,
    expected_message: str,
) -> None:
    # Arrange
    schema = schema_with_checks([{"name": "positive", "rule": rule}])

    # Act / Assert
    with pytest.raises(ValueError, match=expected_message):
        SchemaMetadataValidator(schema, "target_struct").validate()


def test_schema_metadata_rejects_invalid_nested_metadata() -> None:
    # Arrange
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
                                        "name": "positive_amount",
                                        "rule": "",
                                        "severity": "error",
                                    }
                                ]
                            },
                        ),
                    ]
                ),
                nullable=False,
            )
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError, match="payload.amount.*rule"):
        SchemaMetadataValidator(schema, "target_struct").validate()


def test_schema_metadata_rejects_nested_invalid_severity() -> None:
    # Arrange
    schema = StructType(
        [
            StructField(
                "payload",
                StructType(
                    [
                        StructField(
                            "status",
                            StringType(),
                            nullable=False,
                            metadata={
                                "checks": [
                                    {
                                        "name": "known_status",
                                        "rule": "status IN ('new', 'paid')",
                                        "severity": "critical",
                                    }
                                ]
                            },
                        ),
                    ]
                ),
                nullable=False,
            )
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError, match="severity must be one of"):
        SchemaMetadataValidator(schema, "target_struct").validate()


def test_schema_metadata_warns_when_rule_does_not_reference_field() -> None:
    # Arrange
    schema = schema_with_checks(
        [
            {
                "name": "other_field_positive",
                "rule": "price > 0",
            }
        ]
    )

    # Act / Assert
    with pytest.warns(UserWarning, match="Rule does not reference field 'amount'"):
        SchemaMetadataValidator(schema, "target_struct").validate()
