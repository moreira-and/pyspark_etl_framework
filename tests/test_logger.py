from __future__ import annotations

import logging
from datetime import datetime, timezone

from etl_framework.infra.logger import get_logger, log_event
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


class CapturingHandler(logging.Handler):
    """Small in-memory handler so tests inspect the real log payload."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


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


def test_log_event_payload_contains_required_fields() -> None:
    # Arrange
    logger = logging.getLogger("tests.test_logger.payload")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)

    # Act
    log_event(
        logger,
        "validate_succeeded",
        _config(),
        _context(),
        stage="validate",
        status="success",
        row_count=3,
    )

    # Assert
    assert len(handler.records) == 1
    payload = handler.records[0].msg

    assert isinstance(payload, dict)
    assert {
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


def test_log_event_marks_dry_run_mode() -> None:
    # Arrange
    logger = logging.getLogger("tests.test_logger.dry_run")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)

    # Act
    log_event(
        logger,
        "run_started",
        _config(dry_run=True),
        _context(),
        stage="run",
        status="started",
    )

    # Assert
    payload = handler.records[0].msg

    assert isinstance(payload, dict)
    assert payload["mode"] == "dry_run"
    assert payload["stage"] == "run"
    assert payload["status"] == "started"


def test_log_event_includes_only_explicit_metrics() -> None:
    # Arrange
    logger = logging.getLogger("tests.test_logger.metrics")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)
    context = _context()
    context.metrics["rows_read"] = 10
    context.metrics["rows_valid"] = 9

    # Act
    log_event(
        logger,
        "certify_succeeded",
        _config(),
        context,
        stage="certify",
        status="succeeded",
        metrics={"rows_written": 9},
    )

    # Assert
    payload = handler.records[0].msg
    assert isinstance(payload, dict)
    assert payload["metrics"] == {
        "rows_read": 10,
        "rows_valid": 9,
        "rows_written": 9,
    }


def test_log_event_sanitizes_metric_payload() -> None:
    # Arrange
    logger = logging.getLogger("tests.test_logger.metric_sanitizing")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)
    context = _context()
    context.metrics["serializable"] = True
    context.metrics[10] = "ignored"  # type: ignore[index]

    # Act
    log_event(
        logger,
        "load_succeeded",
        _config(),
        context,
        stage="load",
        status="succeeded",
        metrics={"non_scalar": {"rows": 1}},
    )

    # Assert
    payload = handler.records[0].msg
    assert isinstance(payload, dict)
    assert payload["metrics"] == {
        "serializable": True,
        "non_scalar": "{'rows': 1}",
    }


def test_get_logger_uses_single_plain_standard_library_handler() -> None:
    # Arrange
    logger_name = "tests.test_logger.get_logger"
    existing_logger = logging.getLogger(logger_name)
    existing_logger.handlers.clear()

    # Act
    logger = get_logger(logger_name)
    logger_again = get_logger(logger_name)

    # Assert
    assert logger is logger_again
    assert logger.level == logging.INFO
    assert logger.propagate is False
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert logger.handlers[0].formatter is not None
    assert logger.handlers[0].formatter._fmt == "%(message)s"
