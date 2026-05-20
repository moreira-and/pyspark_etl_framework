from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from pyspark.sql.types import StructType

from etl_framework.utils.schema_metadata import SchemaMetadataValidator

EXTRA_COLUMNS_POLICIES = frozenset({"ignore", "warn", "fail"})


@dataclass(frozen=True, slots=True)
class EtlRunConfig:
    """Immutable execution configuration for one ETL pipeline.

    This model describes the execution contract shared by the framework and a
    concrete pipeline. It should stay small: the framework uses it to identify
    the pipeline, target metadata, dry-run behavior and optional StructType
    declarations. It validates configuration shape and declarative check
    metadata, but does not execute extract, transform or load logic.
    start_window and end_window describe the extraction window available to the
    extract step.

    Technical columns listed in TECHNICAL_COLUMNS are reserved for framework
    runtime metadata. They must not be declared in target_struct. For example,
    validate_struct creates is_valid at runtime when structural checks run.
    """

    pipeline_name: str
    target_schema: str
    target_table: str
    target_path: str
    target_key: tuple[str, ...] = ()

    dry_run: bool = False
    dry_run_limit: int = 100
    dry_run_show_rows: int = 0
    strict_schema: bool = False
    extra_columns_policy: str = "ignore"
    keep_technical_columns: bool = True

    source_struct: StructType | None = None
    target_struct: StructType | None = None

    start_window: str | None = None
    end_window: str | None = None

    write_mode: str | None = None

    TECHNICAL_COLUMNS: ClassVar[frozenset[str]] = frozenset(
        {
            "inserted_at",
            "updated_at",
            "etl_run_at",
            "etl_run_id",
            "is_valid",
        }
    )

    def __post_init__(self) -> None:
        """Validate the configuration immediately after creation."""
        if isinstance(self.target_key, str):
            raise ValueError("target_key must contain non-empty strings, not a string")

        if self.target_key is not None:
            try:
                object.__setattr__(self, "target_key", tuple(self.target_key))
            except TypeError as exc:
                raise ValueError(
                    "target_key must contain only non-empty strings"
                ) from exc

        self._validate_basic_config()
        self._validate_schemas()

    @property
    def full_target_table_name(self) -> str:
        """Return the target table name in schema.table format."""
        return f"{self.target_schema}.{self.target_table}"

    def _validate_basic_config(self) -> None:
        """Validate the minimum fields required to orient the ETL flow."""
        if not self.pipeline_name.strip():
            raise ValueError("pipeline_name cannot be empty")

        if not self.target_schema.strip():
            raise ValueError("target_schema cannot be empty")

        if not self.target_table.strip():
            raise ValueError("target_table cannot be empty")

        if not self.target_path.strip():
            raise ValueError("target_path cannot be empty")

        if any(not isinstance(key, str) or not key.strip() for key in self.target_key):
            raise ValueError("target_key must contain only non-empty strings")

        if not isinstance(self.dry_run, bool):
            raise ValueError("dry_run must be a boolean")

        if not isinstance(self.strict_schema, bool):
            raise ValueError("strict_schema must be a boolean")

        if not isinstance(self.keep_technical_columns, bool):
            raise ValueError("keep_technical_columns must be a boolean")

        if not isinstance(self.extra_columns_policy, str):
            raise ValueError("extra_columns_policy must be a string")

        extra_columns_policy = self.extra_columns_policy.strip().lower()
        if extra_columns_policy not in EXTRA_COLUMNS_POLICIES:
            raise ValueError(
                "extra_columns_policy must be one of "
                f"{sorted(EXTRA_COLUMNS_POLICIES)}"
            )

        if self.strict_schema and extra_columns_policy == "ignore":
            extra_columns_policy = "warn"

        object.__setattr__(
            self,
            "extra_columns_policy",
            extra_columns_policy,
        )

        if self.dry_run_limit <= 0:
            raise ValueError("dry_run_limit must be greater than zero")

        if self.dry_run_show_rows < 0:
            raise ValueError("dry_run_show_rows cannot be negative")

        if not self.dry_run and self.dry_run_show_rows > 0:
            raise ValueError(
                "dry_run_show_rows can be greater than zero only when dry_run=True"
            )

        if self.start_window is not None:
            if not isinstance(self.start_window, str) or not self.start_window.strip():
                raise ValueError("start_window must be a non-empty string when set")

        if self.end_window is not None:
            if not isinstance(self.end_window, str) or not self.end_window.strip():
                raise ValueError("end_window must be a non-empty string when set")

        if self.write_mode is not None and self.write_mode not in {
            "overwrite",
            "append",
        }:
            raise ValueError(
                "write_mode must be None, 'overwrite' or 'append', "
                f"got '{self.write_mode}'"
            )

    def _validate_schemas(self) -> None:
        """Validate optional StructType declarations and check metadata.

        target_struct describes business output columns. Framework technical
        columns are rejected here because they can be added during execution.
        """
        if self.source_struct is not None:
            SchemaMetadataValidator(self.source_struct, "source_struct").validate()

        if self.target_struct is not None:
            validator = SchemaMetadataValidator(self.target_struct, "target_struct")
            validator.validate()
            validator.check_reserved_columns(self.TECHNICAL_COLUMNS)
            self._validate_target_key_in_target_struct()

    def _validate_target_key_in_target_struct(self) -> None:
        """Ensure target keys refer to declared business output columns."""
        if self.target_struct is None:
            return
        if not self.target_key:
            return

        target_columns = {field.name for field in self.target_struct.fields}
        missing_keys = [key for key in self.target_key if key not in target_columns]

        if missing_keys:
            raise ValueError(
                "target_key contains columns not present in target_struct: "
                f"{missing_keys}"
            )
