from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timezone

import pytest

from etl_framework.infra.observability import (
    ObservabilityService,
    configure_observability_from_env,
    configure_observability_sink,
    get_observability_service,
    reset_observability_sink,
)
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.observability_events import (
    EVENT_SCHEMA_VERSION,
    build_observability_event,
)


class InMemoryObservabilitySink:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(dict(event))


class FailingObservabilitySink:
    def emit(self, event: Mapping[str, object]) -> None:
        raise RuntimeError("sink failure")


def _config(*, dry_run: bool = False) -> EtlRunConfig:
    return EtlRunConfig(
        pipeline_name="orders_daily",
        target_schema="silver",
        target_table="orders",
        target_path="/tmp/orders",
        target_key=("order_id",),
        dry_run=dry_run,
    )


def _context() -> EtlExecutionContext:
    return EtlExecutionContext(
        run_id="run-123",
        started_at=datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
    )


def test_build_observability_event_payload_contains_required_fields() -> None:
    # Act
    payload = build_observability_event(
        "validate_succeeded",
        _config(),
        _context(),
        stage="validate",
        status="success",
        row_count=3,
    )

    # Assert
    assert {
        "event_schema_version",
        "event",
        "pipeline_name",
        "run_id",
        "started_at",
        "event_at",
        "mode",
        "target_schema",
        "target_table",
        "target_path",
        "target",
        "write_mode",
        "stage",
        "status",
    }.issubset(payload)
    assert payload["event_schema_version"] == EVENT_SCHEMA_VERSION
    assert payload["event"] == "validate_succeeded"
    assert payload["pipeline_name"] == "orders_daily"
    assert payload["run_id"] == "run-123"
    assert payload["started_at"] == "2026-05-17T12:00:00+00:00"
    assert payload["mode"] == "prod"
    assert payload["target_schema"] == "silver"
    assert payload["target_table"] == "orders"
    assert payload["target_path"] == "/tmp/orders"
    assert payload["target"] == "silver.orders"
    assert payload["write_mode"] is None
    assert payload["stage"] == "validate"
    assert payload["status"] == "success"
    assert payload["row_count"] == 3

    event_at = datetime.fromisoformat(payload["event_at"])
    assert event_at.tzinfo is not None


def test_build_observability_event_returns_payload_without_emitting() -> None:
    # Act
    payload = build_observability_event(
        "validate_succeeded",
        _config(),
        _context(),
        stage="validate",
        status="succeeded",
        elapsed_ms=12.3,
    )

    # Assert
    assert payload["event_schema_version"] == EVENT_SCHEMA_VERSION
    assert payload["event"] == "validate_succeeded"
    assert payload["pipeline_name"] == "orders_daily"
    assert payload["run_id"] == "run-123"
    assert payload["stage"] == "validate"
    assert payload["status"] == "succeeded"
    assert payload["elapsed_ms"] == 12.3


def test_observability_service_delegates_to_sink_and_tolerates_sink_failure() -> None:
    # Arrange
    sink = InMemoryObservabilitySink()
    service = ObservabilityService(sink)

    # Act
    service.emit({"event": "run_started", "pipeline_name": "orders_daily"})
    ObservabilityService(FailingObservabilitySink()).emit({"event": "run_started"})

    # Assert
    assert sink.events == [
        {"event": "run_started", "pipeline_name": "orders_daily", "level": "info"}
    ]


def test_observability_service_emits_info_warning_and_exception_levels() -> None:
    # Arrange
    sink = InMemoryObservabilitySink()
    service = ObservabilityService(sink)

    # Act
    service.info(
        "run_started",
        _config(),
        _context(),
        extra={"stage": "run", "status": "started"},
    )
    service.warning(
        "dry_run_sample_requested",
        _config(dry_run=True),
        _context(),
        extra={"stage": "load", "status": "sample_requested"},
    )
    service.exception(
        "load_failed",
        _config(),
        _context(),
        error=RuntimeError("failed token=abc123"),
        extra={"stage": "load", "status": "failed"},
    )

    # Assert
    assert [event["level"] for event in sink.events] == ["info", "warning", "error"]
    assert sink.events[0]["event"] == "run_started"
    assert sink.events[1]["mode"] == "dry_run"
    assert sink.events[2]["error_type"] == "RuntimeError"
    assert "abc123" not in str(sink.events[2]["error_message"])


def test_configured_observability_sink_receives_runtime_events() -> None:
    # Arrange
    sink = InMemoryObservabilitySink()
    configure_observability_sink(sink)

    try:
        # Act
        get_observability_service().runtime_event(
            "run_started",
            _config(),
            _context(),
            extra={"stage": "run", "status": "started"},
        )
    finally:
        reset_observability_sink()

    # Assert
    assert len(sink.events) == 1
    assert sink.events[0]["event"] == "run_started"
    assert sink.events[0]["pipeline_name"] == "orders_daily"
    assert sink.events[0]["run_id"] == "run-123"


def test_configure_observability_from_env_reads_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    calls: dict[str, object] = {}

    def capture_basic_config(**kwargs: object) -> None:
        calls.update(kwargs)

    monkeypatch.setenv("ETL_LOG_LEVEL", "WARNING")
    monkeypatch.setenv("ETL_LOG_FORMAT", "%(levelname)s|%(message)s")
    monkeypatch.setenv("ETL_LOG_TO_STDOUT", "true")
    monkeypatch.setattr(logging, "basicConfig", capture_basic_config)

    # Act
    configure_observability_from_env()

    # Assert
    assert calls["level"] == logging.WARNING
    assert calls["format"] == "%(levelname)s|%(message)s"
    assert calls["force"] is False
    assert len(calls["handlers"]) == 1  # type: ignore[arg-type]


def test_build_observability_event_marks_dry_run_mode() -> None:
    # Act
    payload = build_observability_event(
        "run_started",
        _config(dry_run=True),
        _context(),
        stage="run",
        status="started",
    )

    # Assert
    assert payload["event_schema_version"] == EVENT_SCHEMA_VERSION
    assert payload["mode"] == "dry_run"
    assert payload["stage"] == "run"
    assert payload["status"] == "started"


def test_build_observability_event_includes_only_explicit_metrics() -> None:
    # Arrange
    context = _context()
    context.metrics["rows_read"] = 10
    context.metrics["rows_valid"] = 9

    # Act
    payload = build_observability_event(
        "certify_succeeded",
        _config(),
        context,
        stage="certify",
        status="succeeded",
        metrics={"rows_written": 9},
    )

    # Assert
    assert payload["event_schema_version"] == EVENT_SCHEMA_VERSION
    assert payload["metrics"] == {
        "rows_read": 10,
        "rows_valid": 9,
        "rows_written": 9,
    }


def test_build_observability_event_sanitizes_metric_payload() -> None:
    # Arrange
    context = _context()
    context.metrics["serializable"] = True
    context.metrics[10] = "ignored"  # type: ignore[index]

    # Act
    payload = build_observability_event(
        "load_succeeded",
        _config(),
        context,
        stage="load",
        status="succeeded",
        metrics={"non_scalar": {"rows": 1}},
    )

    # Assert
    assert payload["event_schema_version"] == EVENT_SCHEMA_VERSION
    assert payload["metrics"] == {
        "serializable": True,
        "non_scalar": "{'rows': 1}",
    }


def test_build_observability_event_redacts_sensitive_error_fields() -> None:
    # Act
    payload = build_observability_event(
        "load_failed",
        _config(),
        _context(),
        stage="load",
        status="failed",
        error_message=(
            "failed password=hunter2 token=abc123 "
            "payload={'cpf': '12345678900'} path=/mnt/prod/private.csv"
        ),
    )

    # Assert
    assert payload["event_schema_version"] == EVENT_SCHEMA_VERSION
    assert payload["pipeline_name"] == "orders_daily"
    assert payload["run_id"] == "run-123"
    assert payload["stage"] == "load"
    assert "hunter2" not in payload["error_message"]
    assert "abc123" not in payload["error_message"]
    assert "12345678900" not in payload["error_message"]
    assert "/mnt/prod/private.csv" not in payload["error_message"]
