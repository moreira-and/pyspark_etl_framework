from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_framework.models.config import EtlRunConfig

VALID_CONFIG = {
    "pipeline_name": "orders_pipeline",
    "target_schema": "gold",
    "target_table": "orders",
    "target_path": "/warehouse/gold/orders",
    "target_key": ("order_id",),
}


def make_config(**overrides: object) -> EtlRunConfig:
    values = dict(VALID_CONFIG)
    values.update(overrides)
    return EtlRunConfig(**values)


@pytest.mark.parametrize(
    "field",
    ["pipeline_name", "target_schema", "target_table", "target_path"],
)
@pytest.mark.parametrize("empty_value", ["", "   "])
def test_config_rejects_empty_required_text_fields(
    field: str,
    empty_value: str,
) -> None:
    # Arrange
    invalid_config = {field: empty_value}

    # Act / Assert
    with pytest.raises(ValueError, match=f"{field} cannot be empty"):
        make_config(**invalid_config)


@pytest.mark.parametrize(
    "invalid_target_key",
    [
        pytest.param("order_id", id="string-is-not-sequence"),
        pytest.param(("order_id", ""), id="empty-item"),
        pytest.param(("order_id", "   "), id="blank-item"),
        pytest.param(("order_id", 10), id="non-string-item"),
    ],
)
def test_config_rejects_invalid_target_key(invalid_target_key: object) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="target_key"):
        make_config(target_key=invalid_target_key)


@pytest.mark.parametrize("invalid_limit", [0, -1])
def test_config_rejects_non_positive_dry_run_limit(invalid_limit: int) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="dry_run_limit must be greater than zero"):
        make_config(dry_run_limit=invalid_limit)


def test_config_rejects_negative_dry_run_show_rows() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="dry_run_show_rows cannot be negative"):
        make_config(dry_run_show_rows=-1)


def test_config_rejects_show_rows_in_production_mode() -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="only when dry_run=True"):
        make_config(dry_run=False, dry_run_show_rows=1)


@pytest.mark.parametrize("field", ["start_window", "end_window"])
@pytest.mark.parametrize("invalid_window", ["", "   ", 20260101])
def test_config_rejects_invalid_window_values(
    field: str,
    invalid_window: object,
) -> None:
    # Arrange
    invalid_config = {field: invalid_window}

    # Act / Assert
    with pytest.raises(ValueError, match=f"{field} must be a non-empty string"):
        make_config(**invalid_config)


@pytest.mark.parametrize("invalid_write_mode", ["", "merge"])
def test_config_rejects_unknown_write_mode(invalid_write_mode: str) -> None:
    # Arrange / Act / Assert
    with pytest.raises(ValueError, match="write_mode must be None"):
        make_config(write_mode=invalid_write_mode)


def test_config_normalizes_target_key_and_exposes_full_table_name() -> None:
    # Arrange / Act
    config = make_config(target_key=["order_id", "line_id"])

    # Assert
    assert config.target_key == ("order_id", "line_id")
    assert config.full_target_table_name == "gold.orders"


def test_config_allows_empty_target_key_for_v01_auto_validation() -> None:
    # Arrange / Act
    config = make_config(target_key=())

    # Assert
    assert config.target_key == ()


def test_config_rejects_target_key_not_present_in_target_struct() -> None:
    # Arrange
    target_struct = StructType(
        [
            StructField("order_id", IntegerType(), nullable=False),
            StructField("amount", IntegerType(), nullable=True),
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError, match="target_key contains columns"):
        make_config(
            target_key=("order_id", "missing_line_id"),
            target_struct=target_struct,
        )


def test_config_accepts_valid_write_modes_and_windows() -> None:
    # Arrange / Act
    config = make_config(
        dry_run=True,
        dry_run_limit=5,
        dry_run_show_rows=2,
        start_window="2026-01-01",
        end_window="2026-01-31",
        write_mode="append",
    )

    # Assert
    assert config.dry_run is True
    assert config.dry_run_limit == 5
    assert config.dry_run_show_rows == 2
    assert config.start_window == "2026-01-01"
    assert config.end_window == "2026-01-31"
    assert config.write_mode == "append"


def test_config_is_immutable() -> None:
    # Arrange
    config = make_config()

    # Act / Assert
    with pytest.raises(FrozenInstanceError):
        config.pipeline_name = "other_pipeline"  # type: ignore[misc]


@pytest.mark.parametrize(
    "reserved_column",
    ["inserted_at", "updated_at", "etl_run_at", "etl_run_id", "is_valid"],
)
def test_target_struct_rejects_reserved_columns(reserved_column: str) -> None:
    # Arrange
    target_struct = StructType(
        [
            StructField("order_id", IntegerType(), nullable=False),
            StructField(reserved_column, StringType(), nullable=True),
        ]
    )

    # Act / Assert
    with pytest.raises(ValueError, match="reserved columns"):
        make_config(target_struct=target_struct)
