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
    declarations.

    Technical columns listed in TECHNICAL_COLUMNS are reserved for framework
    runtime metadata. They must not be declared in target_struct.
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

    TECHNICAL_COLUMNS: ClassVar[frozenset[str]] = frozenset({
        "inserted_at",
        "updated_at",
        "etl_run_at",
        "etl_run_id",
        "is_valid",
    })

    def __post_init__(self) -> None:
        """Validate and normalize configuration after creation."""
        # Normalize target_key to tuple
        if isinstance(self.target_key, str):
            raise ValueError("target_key must be a tuple of strings, not a single string")
        
        try:
            normalized_key = tuple(self.target_key) if self.target_key else ()
        except TypeError as exc:
            raise ValueError("target_key must be iterable of strings") from exc
        
        object.__setattr__(self, "target_key", normalized_key)
        
        # Normalize extra_columns_policy
        policy = self.extra_columns_policy.strip().lower()
        if policy not in EXTRA_COLUMNS_POLICIES:
            raise ValueError(
                f"extra_columns_policy must be one of {sorted(EXTRA_COLUMNS_POLICIES)}, "
                f"got '{self.extra_columns_policy}'"
            )
        
        # Auto-upgrade policy if strict_schema is enabled
        if self.strict_schema and policy == "ignore":
            policy = "warn"
        
        object.__setattr__(self, "extra_columns_policy", policy)
        
        # Run validations
        self._validate()

    @property
    def full_target_table_name(self) -> str:
        """Return the target table name in schema.table format."""
        return f"{self.target_schema}.{self.target_table}"

    def _validate(self) -> None:
        """Validate all configuration constraints."""
        self._validate_required_strings()
        self._validate_target_key()
        self._validate_dry_run_config()
        self._validate_windows()
        self._validate_write_mode()
        self._validate_schemas()

    def _validate_required_strings(self) -> None:
        """Validate required string fields are non-empty."""
        required_fields = {
            "pipeline_name": self.pipeline_name,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "target_path": self.target_path,
        }
        
        for field_name, value in required_fields.items():
            if not value or not value.strip():
                raise ValueError(f"{field_name} cannot be empty")

    def _validate_target_key(self) -> None:
        """Validate target_key contains only non-empty strings."""
        if not all(isinstance(key, str) and key.strip() for key in self.target_key):
            raise ValueError("target_key must contain only non-empty strings")

    def _validate_dry_run_config(self) -> None:
        """Validate dry run configuration."""
        if self.dry_run_limit <= 0:
            raise ValueError("dry_run_limit must be greater than zero")

        if self.dry_run_show_rows < 0:
            raise ValueError("dry_run_show_rows cannot be negative")

        if not self.dry_run and self.dry_run_show_rows > 0:
            raise ValueError(
                "dry_run_show_rows can only be set when dry_run=True"
            )

    def _validate_windows(self) -> None:
        """Validate extraction window parameters."""
        for window_name in ("start_window", "end_window"):
            value = getattr(self, window_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{window_name} must be a non-empty string when set")

    def _validate_write_mode(self) -> None:
        """Validate write mode is a supported value."""
        if self.write_mode is not None and self.write_mode not in {"overwrite", "append"}:
            raise ValueError(
                f"write_mode must be None, 'overwrite' or 'append', got '{self.write_mode}'"
            )

    def _validate_schemas(self) -> None:
        """Validate optional StructType declarations and check metadata."""
        if self.source_struct is not None:
            SchemaMetadataValidator(self.source_struct, "source_struct").validate()

        if self.target_struct is not None:
            validator = SchemaMetadataValidator(self.target_struct, "target_struct")
            validator.validate()
            validator.check_reserved_columns(self.TECHNICAL_COLUMNS)
            self._validate_target_key_in_target_struct()

    def _validate_target_key_in_target_struct(self) -> None:
        """Ensure target keys refer to declared business output columns."""
        if not self.target_struct or not self.target_key:
            return

        target_columns = {field.name for field in self.target_struct.fields}
        missing_keys = [key for key in self.target_key if key not in target_columns]

        if missing_keys:
            raise ValueError(
                f"target_key contains columns not in target_struct: {missing_keys}"
            )
    
    def dry_run_extract_metadata(self) -> dict[str, object]:
        """Return metadata for dry run extract stage."""
        return {"dry_run_limit": self.dry_run_limit}
    
    def dry_run_sample_metadata(self) -> dict[str, object]:
        """Return metadata for dry run sample stage."""
        return {"dry_run_show_rows": self.dry_run_show_rows}
    
    def dry_run_evidence_metadata(self) -> dict[str, object]:
        """Return complete dry run evidence metadata."""
        return {
            "dry_run": self.dry_run,  # Inclui flag dry_run
            "dry_run_limit": self.dry_run_limit,
            "dry_run_show_rows": self.dry_run_show_rows,
        }