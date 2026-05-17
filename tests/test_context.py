from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from etl_framework.models.context import EtlExecutionContext


def test_execution_context_defaults_are_traceable() -> None:
    # Arrange / Act
    context = EtlExecutionContext()

    # Assert
    assert UUID(context.run_id)
    assert context.started_at.tzinfo is timezone.utc


def test_execution_context_accepts_explicit_trace_values() -> None:
    # Arrange
    started_at = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    # Act
    context = EtlExecutionContext(
        run_id="run-001",
        started_at=started_at,
    )

    # Assert
    assert context.run_id == "run-001"
    assert context.started_at == started_at
