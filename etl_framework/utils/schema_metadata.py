from __future__ import annotations

import re
import warnings
from typing import Any

from pyspark.sql.types import StructField, StructType


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
        """Reject reserved technical columns in a declared schema."""
        used_reserved = [
            field.name
            for field in self.struct.fields
            if field.name.lower() in reserved
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

        if description := metadata.get("description"):
            self._validate_string_field(description, "description", path)

        if "checks" in metadata and metadata.get("checks") is not None:
            checks = metadata["checks"]
            if not isinstance(checks, (list, tuple)):
                raise TypeError(f"{path}.checks must be list/tuple")

            for index, check in enumerate(checks):
                CheckMetadataValidator(check, field.name, f"{path}[{index}]").validate()

        if isinstance(field.dataType, StructType):
            SchemaMetadataValidator(field.dataType, path).validate()

    @staticmethod
    def _validate_string_field(value: Any, field_name: str, path: str) -> None:
        """Validate a non-empty string metadata field."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{path}.{field_name} must be non-empty string")


class CheckMetadataValidator:
    """Validate one declarative quality check from StructField metadata."""

    ALLOWED_SEVERITIES = {"error", "warning"}
    SQL_INVALID_OPERATORS = {
        r"\>\>": "> (greater than)",
        r"\<\<": "< (less than)",
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
        if not isinstance(self.check, dict):
            raise TypeError(f"{self.path} must be dict")

        self._require_field("name")
        rule = self._require_field("rule")
        self._validate_severity()
        self._validate_optional_string("message")
        self._validate_sql_rule(rule)

    def _require_field(self, field: str) -> str:
        """Return a required non-empty string field."""
        value = self.check.get(field, "")

        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{self.path}.{field} must be non-empty string")

        return value.strip()

    def _validate_severity(self) -> None:
        """Validate the optional severity value used by data-quality checks."""
        value = self.check.get("severity")
        if value is None:
            return

        if not isinstance(value, str):
            raise TypeError(f"{self.path}.severity must be string")

        if value not in self.ALLOWED_SEVERITIES:
            raise ValueError(
                f"{self.path}.severity must be one of "
                f"{sorted(self.ALLOWED_SEVERITIES)}"
            )

    def _validate_optional_string(self, field: str) -> None:
        """Validate an optional metadata string field."""
        if (value := self.check.get(field)) is not None:
            if not isinstance(value, str):
                raise TypeError(f"{self.path}.{field} must be string")

    def _validate_sql_rule(self, rule: str) -> None:
        """Validate simple SQL expression mistakes without executing the rule."""
        if rule.count("(") != rule.count(")"):
            raise ValueError(f"{self.path}: Unbalanced parentheses in '{rule}'")

        for pattern, suggestion in self.SQL_INVALID_OPERATORS.items():
            if re.search(pattern, rule):
                raise ValueError(
                    f"{self.path}: Invalid SQL operator in '{rule}'\n"
                    f"  Suggestion: use {suggestion}"
                )

        field_refs = [self.field_name, f"`{self.field_name}`"]
        if not any(ref in rule for ref in field_refs):
            warnings.warn(
                f"{self.path}: Rule does not reference field '{self.field_name}'",
                UserWarning,
                stacklevel=5,
            )
