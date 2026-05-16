from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from etl_framework.utils.schema_metadata import SchemaMetadataValidator
from pyspark.sql.types import StructType


@dataclass(frozen=True, slots=True)
class EtlRunConfig:
    """Immutable execution configuration for one ETL pipeline.

    This model describes the execution contract shared by the framework and a
    concrete pipeline. It should stay small: the framework uses it to identify
    the pipeline, target metadata, dry-run behavior and optional StructType
    declarations. It validates configuration shape and declarative check
    metadata, but does not execute extract, transform or load logic.

    Technical columns listed in TECHNICAL_COLUMNS are reserved for framework
    runtime metadata. They must not be declared in target_struct. For example,
    validate_struct creates is_valid at runtime when structural checks run.
    """

    pipeline_name: str
    target_schema: str
    target_table: str
    target_path: str
    target_key: list[str]

    dry_run: bool = False
    dry_run_limit: int = 100
    dry_run_show_rows: int = 20

    source_struct: StructType | None = None
    target_struct: StructType | None = None

    gt_date: str | None = None
    lt_date: str | None = None

    write_mode: str | None = None

    TECHNICAL_COLUMNS: ClassVar[frozenset[str]] = frozenset({
        "inserted_at",
        "updated_at",
        "etl_run_at",
        "etl_run_id",
        "is_valid",
    })

    def __post_init__(self) -> None:
        """Validate the configuration immediately after creation."""
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

        if not self.target_key:
            raise ValueError("target_key cannot be empty")

        if any(not isinstance(key, str) or not key.strip() for key in self.target_key):
            raise ValueError("target_key must contain only non-empty strings")

        if self.dry_run_limit <= 0:
            raise ValueError("dry_run_limit must be greater than zero")

        if self.dry_run_show_rows < 0:
            raise ValueError("dry_run_show_rows cannot be negative")

        if self.write_mode is not None and self.write_mode not in {"overwrite", "append"}:
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
