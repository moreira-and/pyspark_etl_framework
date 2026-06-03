from __future__ import annotations

import re
import warnings
from typing import Any

from pyspark.sql.types import StructField, StructType

from etl_framework.utils.check_metadata import normalize_check_metadata
from etl_framework.utils.sanitization import sanitize_with_metadata


class SchemaMetadataValidator:
    """Validate declarative schema metadata used by the ETL framework.

    This validator checks only the shape of metadata attached to a StructType.
    It does not execute data-quality rules. Execution remains the
    responsibility of the ETL flow through validation utilities or concrete
    pipeline code.
    """

    def __init__(self, struct: StructType, name: str) -> None:
        """Create a validator for one named StructType."""
        self.struct = struct
        self.name = name

    def validate(self) -> None:
        """Validate field metadata and nested StructType metadata."""
        if not isinstance(self.struct, StructType):
            raise TypeError(f"{self.name} must be StructType")

        for field in self.struct.fields:
            self._validate_field(field, self.name)

    def check_reserved_columns(self, reserved: frozenset[str]) -> None:
        """Reject reserved technical columns (case-insensitive) in a declared schema.
        
        Args:
            reserved: Set of reserved column names (should be lowercase).
        """
        # Normalize reserved names to lowercase for case-insensitive comparison
        reserved_lower = {name.lower() for name in reserved}
        
        used_reserved = [
            field.name for field in self.struct.fields 
            if field.name.lower() in reserved_lower
        ]

        if used_reserved:
            raise ValueError(
                f"{self.name} uses reserved columns: {used_reserved}\n"
                f"Reserved columns: {sorted(reserved)}"
            )

    def _validate_field(self, field: StructField, parent: str) -> None:
        """Validate one StructField metadata block."""
        path = f"{parent}.{field.name}"
        metadata = field.metadata or {}

        # Validate description if present
        description = metadata.get("description")
        if description is not None:
            self._validate_string_field(description, "description", path)

        try:
            # Validate checks if present
            if "checks" in metadata and metadata.get("checks") is not None:
                checks = metadata["checks"]
                if not isinstance(checks, (list, tuple)):
                    raise TypeError(f"{path}.checks must be list/tuple")

                for index, check in enumerate(checks):
                    CheckMetadataValidator(
                        check, field.name, f"{path}.checks[{index}]"
                    ).validate()

            # Validate sanitize_level if present
            if "sanitize_level" in metadata:
                sanitize_level = metadata["sanitize_level"]
                if sanitize_level not in {"none", "partial", "full"}:
                    raise ValueError(
                        f"{path}.sanitize_level must be one of ('none', 'partial', 'full')"
                    )

            # Recursively validate nested structs
            if isinstance(field.dataType, StructType):
                SchemaMetadataValidator(field.dataType, path).validate()

        except Exception as exc:
            sanitized_message = sanitize_with_metadata(str(exc), metadata=metadata)
            raise type(exc)(sanitized_message) from exc

    @staticmethod
    def _validate_string_field(value: Any, field_name: str, path: str) -> None:
        """Validate a non-empty string metadata field."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{path}.{field_name} must be non-empty string")


class CheckMetadataValidator:
    """Validate one declarative quality check from StructField metadata."""

    SQL_INVALID_OPERATORS = {
        r">>": "> (greater than)",
        r"<<": "< (less than)",
        r"===": "= or == (equality)",
        r"!==": "!= or <> (inequality)",
    }

    def __init__(self, check: dict[str, Any], field_name: str, path: str) -> None:
        """Create a validator for one metadata check declaration."""
        self.check = check
        self.field_name = field_name
        self.path = path

    def validate(self) -> None:
        """Validate required fields, optional fields and simple SQL mistakes."""
        normalized = normalize_check_metadata(
            self.check,
            path=self.path,
            field_path=self.field_name,
        )
        self._validate_sql_rule(normalized["rule"])

    def _validate_sql_rule(self, rule: str) -> None:
        """Validate simple SQL expression mistakes without executing the rule."""
        # Check balanced parentheses
        if rule.count("(") != rule.count(")"):
            raise ValueError(f"{self.path}: Unbalanced parentheses in '{rule}'")

        # Check for invalid operators
        for pattern, suggestion in self.SQL_INVALID_OPERATORS.items():
            if re.search(pattern, rule):
                raise ValueError(
                    f"{self.path}: Invalid SQL operator in '{rule}'\n"
                    f"  Suggestion: use {suggestion}"
                )

        # Warn if rule doesn't reference the field
        field_refs = [self.field_name, f"`{self.field_name}`"]
        if not any(ref in rule for ref in field_refs):
            warnings.warn(
                f"{self.path}: Rule does not reference field '{self.field_name}'",
                UserWarning,
                stacklevel=2,  # Points to the caller of validate()
            )