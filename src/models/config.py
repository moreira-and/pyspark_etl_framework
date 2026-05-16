from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from typing import Any
from pyspark.sql.types import StructField, StructType


@dataclass(frozen=True, slots=True)
class EtlRunConfig:
    """Configuração imutável para pipeline ETL."""
    
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

    write_mode: str = "overwrite"

    TECHNICAL_COLUMNS = frozenset({
        "inserted_at", "updated_at", "etl_run_at", "etl_run_id", "is_valid",
    })

    def __post_init__(self) -> None:
        self._validate_basic_config()
        self._validate_schemas()

    @property
    def full_target_table_name(self) -> str:
        return f"{self.target_schema}.{self.target_table}"

    def _validate_basic_config(self) -> None:
        """Valida configurações básicas."""
        if not self.pipeline_name.strip():
            raise ValueError("pipeline_name cannot be empty")
        
        if not self.target_key:
            raise ValueError("target_key cannot be empty")
        
        if self.write_mode not in {"overwrite", "append"}:
            raise ValueError(
                f"write_mode must be 'overwrite' or 'append', got '{self.write_mode}'"
            )

    def _validate_schemas(self) -> None:
        """Valida schemas source e target."""
        if self.source_struct:
            SchemaValidator(self.source_struct, "source_struct").validate()
        
        if self.target_struct:
            validator = SchemaValidator(self.target_struct, "target_struct")
            validator.validate()
            validator.check_reserved_columns(self.TECHNICAL_COLUMNS)


class SchemaValidator:
    """Validador de StructType com checks."""
    
    def __init__(self, struct: StructType, name: str):
        self.struct = struct
        self.name = name
    
    def validate(self) -> None:
        """Valida schema completo."""
        if not isinstance(self.struct, StructType):
            raise TypeError(f"{self.name} must be StructType")
        
        for field in self.struct.fields:
            self._validate_field(field, self.name)
    
    def check_reserved_columns(self, reserved: frozenset[str]) -> None:
        """Verifica se não usa colunas reservadas."""
        used_reserved = [
            f.name for f in self.struct.fields 
            if f.name.lower() in reserved
        ]
        
        if used_reserved:
            raise ValueError(
                f"{self.name} uses reserved columns: {used_reserved}\n"
                f"Reserved columns: {sorted(reserved)}"
            )
    
    def _validate_field(self, field: StructField, parent: str) -> None:
        """Valida campo completo: tipo, metadata, checks."""
        path = f"{parent}.{field.name}"
        metadata = field.metadata or {}
        
        # Valida description
        if desc := metadata.get("description"):
            self._validate_string_field(desc, "description", path)
        
        # Valida checks
        if checks := metadata.get("checks"):
            if not isinstance(checks, (list, tuple)):
                raise TypeError(f"{path}.checks must be list/tuple")
            
            for i, check in enumerate(checks):
                CheckValidator(check, field.name, f"{path}[{i}]").validate()
        
        # Recursão para structs aninhados
        if isinstance(field.dataType, StructType):
            SchemaValidator(field.dataType, path).validate()
    
    @staticmethod
    def _validate_string_field(value: Any, field_name: str, path: str) -> None:
        """Valida campo string não-vazio."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{path}.{field_name} must be non-empty string")


class CheckValidator:
    """Validador de check de qualidade."""
    
    SQL_INVALID_OPERATORS = {
        r'\>\>': '> (greater than)',
        r'\<\<': '< (less than)',
        r'===': '= or == (equality)',
        r'!==': '!= or <> (inequality)',
    }
    
    def __init__(self, check: dict, field_name: str, path: str):
        self.check = check
        self.field_name = field_name
        self.path = path
    
    def validate(self) -> None:
        """Valida estrutura e sintaxe do check."""
        if not isinstance(self.check, dict):
            raise TypeError(f"{self.path} must be dict")
        
        # Valida campos obrigatórios
        name = self._require_field("name")
        rule = self._require_field("rule")
        
        # Valida campos opcionais
        self._validate_optional_field("severity")
        self._validate_optional_field("message")
        
        # Valida sintaxe SQL
        self._validate_sql_rule(rule)
    
    def _require_field(self, field: str) -> str:
        """Valida e retorna campo obrigatório."""
        value = self.check.get(field, "")
        
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{self.path}.{field} must be non-empty string")
        
        return value.strip()
    
    def _validate_optional_field(self, field: str) -> None:
        """Valida campo opcional se presente."""
        if (value := self.check.get(field)) is not None:
            if not isinstance(value, str):
                raise TypeError(f"{self.path}.{field} must be string")
    
    def _validate_sql_rule(self, rule: str) -> None:
        """Valida sintaxe SQL da regra."""
        # Parênteses balanceados
        if rule.count("(") != rule.count(")"):
            raise ValueError(f"{self.path}: Unbalanced parentheses in '{rule}'")
        
        # Operadores inválidos
        for pattern, suggestion in self.SQL_INVALID_OPERATORS.items():
            if re.search(pattern, rule):
                raise ValueError(
                    f"{self.path}: Invalid SQL operator in '{rule}'\n"
                    f"  Suggestion: use {suggestion}"
                )
        
        # Avisa se não referencia o campo
        field_refs = [self.field_name, f"`{self.field_name}`"]
        if not any(ref in rule for ref in field_refs):
            warnings.warn(
                f"{self.path}: Rule doesn't reference field '{self.field_name}'",
                UserWarning,
                stacklevel=5
            )