from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest
from pyspark.sql import DataFrame, SparkSession

from etl_framework.infra.errors import ExtractError, ValidateError
from etl_framework.infra.observability import (
    configure_observability_sink,
    reset_observability_sink,
)
from etl_framework.infra.stage import stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.observability_events import EVENT_SCHEMA_VERSION


class CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _config() -> EtlRunConfig:
    return EtlRunConfig(
        pipeline_name="stage_test_pipeline",
        target_schema="silver",
        target_table="orders",
        target_path="/tmp/orders",
    )


def _context() -> EtlExecutionContext:
    return EtlExecutionContext(
        run_id="run-stage",
        started_at=datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
    )


def _capture_pipeline_logger(
    pipeline_name: str = "stage_test_pipeline",
) -> CapturingHandler:
    logger = logging.getLogger(pipeline_name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = CapturingHandler()
    logger.addHandler(handler)
    return handler


def _payloads(handler: CapturingHandler) -> list[dict[str, object]]:
    payloads = []
    for record in handler.records:
        assert isinstance(record.msg, dict)
        assert record.msg["event_schema_version"] == EVENT_SCHEMA_VERSION
        payloads.append(record.msg)
    return payloads


class StageProbe:
    def __init__(self) -> None:
        self.config = _config()
        self.context = _context()
        self.handler = _capture_pipeline_logger(self.config.pipeline_name)

    @stage("extract", ExtractError)
    def extract(self) -> str:
        return "ok"

    @stage("validate", ValidateError)
    def validate_with_generic_failure(self) -> None:
        raise RuntimeError("failed token=abc123")

    @stage("validate", ValidateError)
    def validate_with_managed_failure(self, error: ValidateError) -> None:
        raise error

    @stage("extract", ExtractError)
    def return_dataframe(self, df: DataFrame) -> DataFrame:
        return df


def test_stage_logs_started_and_succeeded_with_trace_fields() -> None:
    # Arrange
    probe = StageProbe()

    # Act
    result = probe.extract()

    # Assert
    assert result == "ok"
    assert not hasattr(probe, "logger")
    payloads = _payloads(probe.handler)
    assert [payload["event"] for payload in payloads] == [
        "extract_started",
        "extract_succeeded",
    ]
    assert payloads[0]["pipeline_name"] == "stage_test_pipeline"
    assert payloads[0]["run_id"] == "run-stage"
    assert payloads[0]["stage"] == "extract"
    assert payloads[0]["status"] == "started"
    assert payloads[1]["stage"] == "extract"
    assert payloads[1]["status"] == "succeeded"
    assert "elapsed_ms" in payloads[1]


class FailingSink:
    def emit(self, event: object) -> None:
        raise RuntimeError("sink is down")


def test_stage_does_not_fail_when_sink_fails() -> None:
    # Arrange
    configure_observability_sink(FailingSink())
    probe = StageProbe()

    try:
        # Act
        result = probe.extract()
    finally:
        reset_observability_sink()

    # Assert
    assert result == "ok"

def test_stage_logs_failed_and_wraps_generic_exception() -> None:
    # Arrange
    probe = StageProbe()

    # Act / Assert
    with pytest.raises(ValidateError) as exc_info:
        probe.validate_with_generic_failure()

    error = exc_info.value
    assert error.pipeline_name == "stage_test_pipeline"
    assert error.run_id == "run-stage"
    assert error.stage == "validate"

    payloads = _payloads(probe.handler)
    assert [payload["event"] for payload in payloads] == [
        "validate_started",
        "validate_failed",
    ]
    assert payloads[1]["status"] == "failed"
    assert payloads[1]["error_type"] == "ValidateError"
    assert "abc123" not in str(payloads[1]["error_message"])


def test_stage_preserves_managed_error_with_context() -> None:
    # Arrange
    probe = StageProbe()
    managed_error = ValidateError(
        "already managed",
        pipeline_name="stage_test_pipeline",
        stage="validate",
        run_id="run-stage",
    )

    # Act / Assert
    with pytest.raises(ValidateError) as exc_info:
        probe.validate_with_managed_failure(managed_error)

    assert exc_info.value is managed_error
    payloads = _payloads(probe.handler)
    assert payloads[1]["event"] == "validate_failed"
    assert payloads[1]["error_type"] == "ValidateError"


@pytest.mark.integration
def test_stage_does_not_materialize_dataframe(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    probe = StageProbe()
    df = spark.createDataFrame([(1,)], ["id"])

    def fail_action(self: DataFrame, *args: object, **kwargs: object) -> object:
        raise AssertionError("stage decorator materialized a DataFrame")

    monkeypatch.setattr(DataFrame, "count", fail_action)
    monkeypatch.setattr(DataFrame, "collect", fail_action)
    monkeypatch.setattr(DataFrame, "show", fail_action)
    monkeypatch.setattr(DataFrame, "toLocalIterator", fail_action)

    # Act
    result = probe.return_dataframe(df)

    # Assert
    assert result is df
    payloads = _payloads(probe.handler)
    assert [payload["event"] for payload in payloads] == [
        "extract_started",
        "extract_succeeded",
    ]
