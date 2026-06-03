from pyspark.sql.types import StructField, StructType, StringType
from etl_framework.utils.schema_metadata import SchemaMetadataValidator
from etl_framework.utils.sanitization import sanitize_with_metadata
import pytest


def test_sanitize_with_metadata_none_preserves_text():
    message = "token=secret123 path=/tmp/data"
    sanitized = sanitize_with_metadata(message, metadata={"sanitize_level": "none"})
    assert sanitized == message


def test_sanitize_with_metadata_partial_redacts_credentials():
    message = "Bearer abcdef12345"
    sanitized = sanitize_with_metadata(message, metadata={"sanitize_level": "partial"})
    assert "Bearer <redacted>" in sanitized


def test_schema_metadata_validator_accepts_valid_sanitize_level():
    schema = StructType([
        StructField("name", StringType(), True, metadata={"sanitize_level": "partial"}),
    ])
    SchemaMetadataValidator(schema, "source_struct").validate()


def test_schema_metadata_validator_rejects_invalid_sanitize_level():
    schema = StructType([
        StructField("name", StringType(), True, metadata={"sanitize_level": "invalid"}),
    ])

    with pytest.raises(ValueError, match="sanitize_level must be one of"):
        SchemaMetadataValidator(schema, "source_struct").validate()
