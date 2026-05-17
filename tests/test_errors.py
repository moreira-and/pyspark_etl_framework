from __future__ import annotations

import pytest

from etl_framework.infra.errors import (
    CertifyError,
    CheckError,
    EtlError,
    ExtractError,
    LoadError,
    TransformError,
    ValidateError,
    ensure_stage_error,
)


@pytest.mark.parametrize(
    ("error_type", "expected_stage"),
    [
        (ExtractError, "extract"),
        (CheckError, "check"),
        (TransformError, "transform"),
        (ValidateError, "validate"),
        (LoadError, "load"),
        (CertifyError, "certify"),
    ],
)
def test_stage_errors_include_trace_context_in_attributes_and_message(
    error_type: type[EtlError],
    expected_stage: str,
) -> None:
    # Arrange
    cause = RuntimeError("upstream failure")

    # Act
    error = error_type(
        "stage failed",
        pipeline_name="orders_daily",
        run_id="run-123",
        cause=cause,
    )

    # Assert
    assert error.pipeline_name == "orders_daily"
    assert error.run_id == "run-123"
    assert error.stage == expected_stage
    assert error.cause is cause

    message = str(error)
    assert "stage failed" in message
    assert "pipeline_name=orders_daily" in message
    assert "run_id=run-123" in message
    assert f"stage={expected_stage}" in message
    assert "cause_type=RuntimeError" in message
    assert "cause_message=upstream failure" in message


def test_ensure_stage_error_wraps_generic_exception_with_context() -> None:
    # Arrange
    cause = RuntimeError("source is unavailable")

    # Act
    error = ensure_stage_error(
        cause,
        ExtractError,
        pipeline_name="orders_daily",
        run_id="run-456",
    )

    # Assert
    assert isinstance(error, ExtractError)
    assert error.pipeline_name == "orders_daily"
    assert error.run_id == "run-456"
    assert error.stage == "extract"
    assert error.cause is cause
    assert "source is unavailable" in str(error)
    assert "pipeline_name=orders_daily" in str(error)
    assert "run_id=run-456" in str(error)
    assert "stage=extract" in str(error)
    assert "cause_type=RuntimeError" in str(error)


def test_ensure_stage_error_preserves_existing_context() -> None:
    # Arrange
    cause = ValueError("missing required column")
    original = ValidateError(
        "schema validation failed",
        pipeline_name="orders_daily",
        run_id="run-789",
        cause=cause,
    )

    # Act
    error = ensure_stage_error(
        original,
        ValidateError,
        pipeline_name="fallback_pipeline",
        run_id="run-789",
    )

    # Assert
    assert error is original
    assert error.pipeline_name == "orders_daily"
    assert error.run_id == "run-789"
    assert error.stage == "validate"
    assert error.cause is cause


def test_ensure_stage_error_enriches_managed_error_missing_context() -> None:
    # Arrange
    original = TransformError("business rule failed")

    # Act
    error = ensure_stage_error(
        original,
        TransformError,
        pipeline_name="orders_daily",
        run_id="run-101",
    )

    # Assert
    assert error is not original
    assert isinstance(error, TransformError)
    assert error.pipeline_name == "orders_daily"
    assert error.run_id == "run-101"
    assert error.stage == "transform"
    assert error.cause is original
