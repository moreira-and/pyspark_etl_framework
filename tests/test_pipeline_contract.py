from __future__ import annotations

import logging
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from etl_framework.contracts import Extract, Load, Pipeline, Transform
from etl_framework.infra.errors import (
    CertifyError,
    CheckError,
    EtlError,
    ExtractError,
    LoadError,
    TransformError,
    ValidateError,
)
from etl_framework.infra.logger import EVENT_SCHEMA_VERSION
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

RUN_ID = "run-pipeline-contract"
PIPELINE_NAME = "orders_pipeline"

pytestmark = pytest.mark.integration

SOURCE_STRUCT = StructType(
    [
        StructField("order_id", LongType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("amount", LongType(), nullable=False),
    ]
)
TARGET_STRUCT = StructType(
    [
        StructField("order_id", LongType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("amount", LongType(), nullable=False),
        StructField("amount_with_tax", LongType(), nullable=False),
    ]
)


class CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def make_config(**overrides: object) -> EtlRunConfig:
    values = {
        "pipeline_name": PIPELINE_NAME,
        "target_schema": "gold",
        "target_table": "orders",
        "target_path": "/warehouse/gold/orders",
        "target_key": ("order_id",),
        "source_struct": SOURCE_STRUCT,
        "target_struct": TARGET_STRUCT,
    }
    values.update(overrides)
    return EtlRunConfig(**values)


def make_context() -> EtlExecutionContext:
    return EtlExecutionContext(run_id=RUN_ID)


def input_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (1, "new", 10),
            (2, "paid", 20),
            (3, "shipped", 30),
        ],
        ["order_id", "status", "amount"],
    )


class RecordingExtract(Extract):
    def __init__(
        self,
        events: list[str],
        *,
        fail_stage: str | None = None,
        bad_return_stage: str | None = None,
    ) -> None:
        self.events = events
        self.fail_stage = fail_stage
        self.bad_return_stage = bad_return_stage

    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("extract")
        self._fail_if_requested("extract")
        if self.bad_return_stage == "extract":
            return ["not", "a", "dataframe"]  # type: ignore[return-value]
        return input_df(spark)

    def _check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("check")
        self._fail_if_requested("check")
        if self.bad_return_stage == "check":
            return {"not": "a dataframe"}  # type: ignore[return-value]
        return df

    def _fail_if_requested(self, stage: str) -> None:
        if self.fail_stage == stage:
            raise RuntimeError(f"{stage} failed")


class RecordingTransform(Transform):
    def __init__(
        self,
        events: list[str],
        *,
        fail_stage: str | None = None,
        bad_return_stage: str | None = None,
    ) -> None:
        self.events = events
        self.fail_stage = fail_stage
        self.bad_return_stage = bad_return_stage

    def _transform(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("transform")
        self._fail_if_requested("transform")
        if self.bad_return_stage == "transform":
            return ("not", "a", "dataframe")  # type: ignore[return-value]
        return df.withColumn("amount_with_tax", df.amount + 1)

    def _validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("validate")
        self._fail_if_requested("validate")
        if self.bad_return_stage == "validate":
            return 42  # type: ignore[return-value]
        return df

    def _fail_if_requested(self, stage: str) -> None:
        if self.fail_stage == stage:
            raise RuntimeError(f"{stage} failed")


class RecordingLoad(Load):
    def __init__(
        self,
        events: list[str],
        *,
        fail_stage: str | None = None,
    ) -> None:
        self.events = events
        self.fail_stage = fail_stage
        self.loaded_rows: list[dict[str, Any]] = []
        self.certified_row_count: int | None = None

    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("load")
        self._fail_if_requested("load")
        self.loaded_rows = [row.asDict() for row in df.orderBy("order_id").collect()]

    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("certify")
        self._fail_if_requested("certify")
        self.certified_row_count = df.count()

    def _fail_if_requested(self, stage: str) -> None:
        if self.fail_stage == stage:
            raise RuntimeError(f"{stage} failed")


def build_pipeline(
    spark: SparkSession,
    events: list[str],
    *,
    extract: Extract | None = None,
    transform: Transform | None = None,
    load: RecordingLoad | None = None,
    config: EtlRunConfig | None = None,
) -> tuple[Pipeline, RecordingLoad]:
    load_step = load or RecordingLoad(events)
    pipeline = Pipeline(
        spark=spark,
        config=config or make_config(),
        context=make_context(),
        extract=extract or RecordingExtract(events),
        transform=transform or RecordingTransform(events),
        load=load_step,
    )
    return pipeline, load_step


def assert_stage_error_has_context(
    error: EtlError,
    *,
    stage: str,
    cause_type: type[BaseException],
) -> None:
    assert error.pipeline_name == PIPELINE_NAME
    assert error.stage == stage
    assert error.run_id == RUN_ID
    assert isinstance(error.cause, cause_type)


def capture_pipeline_events(
    pipeline_name: str,
) -> tuple[logging.Logger, CapturingHandler]:
    logger = logging.getLogger(pipeline_name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = CapturingHandler()
    logger.addHandler(handler)
    return logger, handler


def event_payloads(handler: CapturingHandler) -> list[dict[str, object]]:
    payloads = []
    for record in handler.records:
        assert isinstance(record.msg, dict)
        payloads.append(record.msg)
    return payloads


def assert_payload_has_trace_fields(
    payload: dict[str, object],
    *,
    pipeline_name: str,
    run_id: str,
) -> None:
    assert {
        "event_schema_version",
        "event",
        "pipeline_name",
        "run_id",
        "started_at",
        "event_at",
        "mode",
        "stage",
        "status",
    }.issubset(payload)
    assert payload["event_schema_version"] == EVENT_SCHEMA_VERSION
    assert payload["pipeline_name"] == pipeline_name
    assert payload["run_id"] == run_id


def test_run_executes_official_order(spark: SparkSession) -> None:
    # Arrange
    events: list[str] = []
    pipeline, load_step = build_pipeline(spark, events)

    # Act
    result = pipeline.run()

    # Assert
    assert events == [
        "extract",
        "check",
        "transform",
        "validate",
        "load",
        "certify",
    ]
    assert result.columns == [
        "order_id",
        "status",
        "amount",
        "amount_with_tax",
        "is_valid",
    ]
    assert load_step.certified_row_count == 3


def test_pipeline_run_emits_required_log_events_on_success(
    spark: SparkSession,
) -> None:
    # Arrange
    pipeline_name = "orders_pipeline_log_success"
    _, handler = capture_pipeline_events(pipeline_name)
    events: list[str] = []
    pipeline, _ = build_pipeline(
        spark,
        events,
        config=make_config(pipeline_name=pipeline_name),
    )

    # Act
    pipeline.run()

    # Assert
    payloads = event_payloads(handler)
    assert [payload["event"] for payload in payloads] == [
        "run_started",
        "extract_started",
        "extract_succeeded",
        "check_started",
        "check_succeeded",
        "transform_started",
        "transform_succeeded",
        "validate_started",
        "validate_succeeded",
        "load_started",
        "load_succeeded",
        "certify_started",
        "certify_succeeded",
        "run_succeeded",
        "execution_summary",
    ]
    for payload in payloads:
        assert_payload_has_trace_fields(
            payload,
            pipeline_name=pipeline_name,
            run_id=RUN_ID,
        )
    assert payloads[0]["stage"] == "run"
    assert payloads[0]["status"] == "started"
    assert payloads[-1]["event"] == "execution_summary"
    assert payloads[-1]["stage"] == "run"
    assert payloads[-1]["status"] == "succeeded"
    assert payloads[-1]["mode"] == "prod"


def test_pipeline_run_emits_failure_log_event_with_context(
    spark: SparkSession,
) -> None:
    # Arrange
    pipeline_name = "orders_pipeline_log_failure"
    _, handler = capture_pipeline_events(pipeline_name)
    events: list[str] = []
    pipeline, _ = build_pipeline(
        spark,
        events,
        config=make_config(pipeline_name=pipeline_name),
        transform=RecordingTransform(events, fail_stage="validate"),
    )

    # Act
    with pytest.raises(ValidateError):
        pipeline.run()

    # Assert
    payloads = event_payloads(handler)
    for payload in payloads:
        assert_payload_has_trace_fields(
            payload,
            pipeline_name=pipeline_name,
            run_id=RUN_ID,
        )

    failure_events = {
        payload["event"]: payload
        for payload in payloads
        if str(payload["event"]).endswith("_failed")
    }
    assert set(failure_events) == {
        "validate_failed",
        "run_failed",
    }
    assert failure_events["validate_failed"]["stage"] == "validate"
    assert failure_events["validate_failed"]["status"] == "failed"
    assert failure_events["validate_failed"]["error_type"] == "ValidateError"
    assert failure_events["run_failed"]["stage"] == "run"
    assert failure_events["run_failed"]["status"] == "failed"
    assert failure_events["run_failed"]["error_type"] == "ValidateError"

    summary = payloads[-1]
    assert summary["event"] == "execution_summary"
    assert summary["stage"] == "run"
    assert summary["status"] == "failed"
    assert summary["error_type"] == "ValidateError"


def test_certify_failure_is_not_logged_as_load_failure(
    spark: SparkSession,
) -> None:
    # Arrange
    pipeline_name = "orders_pipeline_certify_failure"
    _, handler = capture_pipeline_events(pipeline_name)
    events: list[str] = []
    pipeline, _ = build_pipeline(
        spark,
        events,
        config=make_config(pipeline_name=pipeline_name),
        load=RecordingLoad(events, fail_stage="certify"),
    )

    # Act
    with pytest.raises(CertifyError) as error_info:
        pipeline.run()

    # Assert
    error = error_info.value
    assert error.pipeline_name == pipeline_name
    assert error.stage == "certify"
    assert error.run_id == RUN_ID
    assert isinstance(error.cause, RuntimeError)
    assert events == ["extract", "check", "transform", "validate", "load", "certify"]

    payloads = event_payloads(handler)
    event_names = [payload["event"] for payload in payloads]
    assert "load_succeeded" in event_names
    assert "certify_failed" in event_names
    assert "load_failed" not in event_names
    assert event_names[-2:] == ["run_failed", "execution_summary"]

    failure_events = {
        payload["event"]: payload
        for payload in payloads
        if str(payload["event"]).endswith("_failed")
    }
    assert set(failure_events) == {"certify_failed", "run_failed"}
    assert failure_events["certify_failed"]["stage"] == "certify"
    assert failure_events["certify_failed"]["error_type"] == "CertifyError"
    assert failure_events["run_failed"]["stage"] == "run"
    assert failure_events["run_failed"]["error_type"] == "CertifyError"

    summary = payloads[-1]
    assert summary["event"] == "execution_summary"
    assert summary["status"] == "failed"
    assert summary["error_type"] == "CertifyError"


def test_pipeline_normal_mode_does_not_trigger_show_or_collect(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    # Auto validate may run a small count to block invalid rows, but it must not
    # expose rows or collect data automatically.
    action_calls: list[str] = []

    def fail_action(self: DataFrame, *args: object, **kwargs: object) -> object:
        action_calls.append(type(self).__name__)
        raise AssertionError("Spark action was called automatically")

    monkeypatch.setattr(DataFrame, "show", fail_action)
    monkeypatch.setattr(DataFrame, "collect", fail_action)

    class NoActionExtract(Extract):
        def _extract(
            self,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            return input_df(spark)

        def _check(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            return df

    class NoActionTransform(Transform):
        def _transform(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            return df.withColumn("amount_with_tax", df.amount + 1)

        def _validate(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            return df

    class NoActionLoad(Load):
        def _load(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> None:
            return None

        def _certify(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> None:
            return None

    pipeline = Pipeline(
        spark=spark,
        config=make_config(pipeline_name="orders_pipeline_no_actions"),
        context=make_context(),
        extract=NoActionExtract(),
        transform=NoActionTransform(),
        load=NoActionLoad(),
    )

    # Act
    result = pipeline.run()

    # Assert
    assert result.columns == [
        "order_id",
        "status",
        "amount",
        "amount_with_tax",
        "is_valid",
    ]
    assert action_calls == []


def test_pipeline_stages_share_same_spark_config_and_context_instances(
    spark: SparkSession,
) -> None:
    # Arrange
    seen: list[tuple[str, SparkSession, EtlRunConfig, EtlExecutionContext]] = []

    class IdentityExtract(Extract):
        def _extract(
            self,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            seen.append(("extract", spark, config, context))
            return input_df(spark)

        def _check(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            seen.append(("check", spark, config, context))
            return df

    class IdentityTransform(Transform):
        def _transform(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            seen.append(("transform", spark, config, context))
            return df.withColumn("amount_with_tax", df.amount + 1)

        def _validate(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> DataFrame:
            seen.append(("validate", spark, config, context))
            return df

    class IdentityLoad(Load):
        def _load(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> None:
            seen.append(("load", spark, config, context))

        def _certify(
            self,
            df: DataFrame,
            spark: SparkSession,
            config: EtlRunConfig,
            context: EtlExecutionContext,
        ) -> None:
            seen.append(("certify", spark, config, context))

    config = make_config(pipeline_name="orders_pipeline_identity")
    context = make_context()
    pipeline = Pipeline(
        spark=spark,
        config=config,
        context=context,
        extract=IdentityExtract(),
        transform=IdentityTransform(),
        load=IdentityLoad(),
    )

    # Act
    pipeline.run()

    # Assert
    assert [stage for stage, _, _, _ in seen] == [
        "extract",
        "check",
        "transform",
        "validate",
        "load",
        "certify",
    ]
    assert all(seen_spark is spark for _, seen_spark, _, _ in seen)
    assert all(seen_config is config for _, _, seen_config, _ in seen)
    assert all(seen_context is context for _, _, _, seen_context in seen)


@pytest.mark.parametrize(
    ("stage", "expected_error", "expected_events"),
    [
        pytest.param("extract", ExtractError, ["extract"], id="extract"),
        pytest.param("check", CheckError, ["extract", "check"], id="check"),
        pytest.param(
            "transform",
            TransformError,
            ["extract", "check", "transform"],
            id="transform",
        ),
        pytest.param(
            "validate",
            ValidateError,
            ["extract", "check", "transform", "validate"],
            id="validate",
        ),
        pytest.param(
            "load",
            LoadError,
            ["extract", "check", "transform", "validate", "load"],
            id="load",
        ),
        pytest.param(
            "certify",
            CertifyError,
            ["extract", "check", "transform", "validate", "load", "certify"],
            id="certify",
        ),
    ],
)
def test_stage_failures_are_wrapped_with_context(
    spark: SparkSession,
    stage: str,
    expected_error: type[EtlError],
    expected_events: list[str],
) -> None:
    # Arrange
    events: list[str] = []
    extract = RecordingExtract(events, fail_stage=stage)
    transform = RecordingTransform(events, fail_stage=stage)
    load = RecordingLoad(events, fail_stage=stage)
    pipeline, _ = build_pipeline(
        spark,
        events,
        extract=extract,
        transform=transform,
        load=load,
    )

    # Act
    with pytest.raises(expected_error) as error_info:
        pipeline.run()

    # Assert
    error = error_info.value
    assert_stage_error_has_context(error, stage=stage, cause_type=RuntimeError)
    assert str(error.cause) == f"{stage} failed"
    assert events == expected_events


@pytest.mark.parametrize(
    ("stage", "expected_error", "expected_events"),
    [
        pytest.param("extract", ExtractError, ["extract"], id="extract"),
        pytest.param("check", CheckError, ["extract", "check"], id="check"),
        pytest.param(
            "transform",
            TransformError,
            ["extract", "check", "transform"],
            id="transform",
        ),
        pytest.param(
            "validate",
            ValidateError,
            ["extract", "check", "transform", "validate"],
            id="validate",
        ),
    ],
)
def test_dataframe_stages_reject_non_dataframe_return(
    spark: SparkSession,
    stage: str,
    expected_error: type[EtlError],
    expected_events: list[str],
) -> None:
    # Arrange
    events: list[str] = []
    extract = RecordingExtract(events, bad_return_stage=stage)
    transform = RecordingTransform(events, bad_return_stage=stage)
    pipeline, _ = build_pipeline(
        spark,
        events,
        extract=extract,
        transform=transform,
    )

    # Act
    with pytest.raises(expected_error) as error_info:
        pipeline.run()

    # Assert
    error = error_info.value
    assert_stage_error_has_context(error, stage=stage, cause_type=TypeError)
    assert "must return pyspark.sql.DataFrame" in str(error.cause)
    assert events == expected_events


def test_run_returns_transformed_dataframe_after_success(
    spark: SparkSession,
) -> None:
    # Arrange
    events: list[str] = []
    pipeline, load_step = build_pipeline(spark, events)

    # Act
    result = pipeline.run()
    rows = [row.asDict() for row in result.orderBy("order_id").collect()]

    # Assert
    assert rows == [
        {
            "order_id": 1,
            "status": "new",
            "amount": 10,
            "amount_with_tax": 11,
            "is_valid": True,
        },
        {
            "order_id": 2,
            "status": "paid",
            "amount": 20,
            "amount_with_tax": 21,
            "is_valid": True,
        },
        {
            "order_id": 3,
            "status": "shipped",
            "amount": 30,
            "amount_with_tax": 31,
            "is_valid": True,
        },
    ]
    assert load_step.loaded_rows == rows
    assert load_step.certified_row_count == 3


def test_run_propagates_failure_and_stops_downstream_stages(
    spark: SparkSession,
) -> None:
    # Arrange
    events: list[str] = []
    pipeline, load_step = build_pipeline(
        spark,
        events,
        transform=RecordingTransform(events, fail_stage="validate"),
    )

    # Act
    with pytest.raises(ValidateError) as error_info:
        pipeline.run()

    # Assert
    assert error_info.value.run_id == RUN_ID
    assert events == ["extract", "check", "transform", "validate"]
    assert load_step.loaded_rows == []
    assert load_step.certified_row_count is None
