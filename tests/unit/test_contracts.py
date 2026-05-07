from __future__ import annotations

import json
import logging
from io import StringIO
from typing import Any, cast

import pytest

from etlstruct import (CheckError, EtlExecutionContext, EtlRunConfig,
                       EtlStruct, ExecutionMode, ExtractError, LoadError,
                       TransformError, ValidateError)


class FakeDataFrame:
    def __init__(self) -> None:
        self.limit_calls: list[int] = []
        self.show_calls: list[tuple[int, bool]] = []

    def limit(self, rows: int) -> FakeDataFrame:
        self.limit_calls.append(rows)
        return self

    def show(self, rows: int, *, truncate: bool = True) -> None:
        self.show_calls.append((rows, truncate))


class OrderedPipeline(EtlStruct):
    def __init__(
        self,
        config: EtlRunConfig | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(
            config=config or EtlRunConfig(pipeline_name="ordered_pipeline"),
            logger=logger,
        )
        self.events: list[str] = []
        self.loaded = False
        self.df = FakeDataFrame()

    def _extract(self):
        self.events.append("extract")
        return self.df

    def _check(self, df):
        self.events.append("check")
        return df

    def _transform(self, df):
        self.events.append("transform")
        return df

    def _validate(self, df):
        self.events.append("validate")
        return df

    def _load(self, df) -> None:
        self.events.append("load")
        self.loaded = True


def test_run_executes_fixed_stage_order() -> None:
    pipeline = OrderedPipeline()

    result = pipeline.run()

    assert result is pipeline.df
    assert pipeline.events == ["extract", "check", "transform", "validate", "load"]
    assert pipeline.loaded is True


def test_dry_run_limits_extract_and_skips_persistence() -> None:
    pipeline = OrderedPipeline(
        config=EtlRunConfig(
            pipeline_name="ordered_pipeline",
            mode=ExecutionMode.DRY_RUN,
            dry_run_limit=5,
            dry_run_show_rows=2,
        ),
    )

    result = pipeline.run()

    assert result is pipeline.df
    assert pipeline.events == ["extract", "check", "transform", "validate"]
    assert pipeline.loaded is False
    assert pipeline.df.limit_calls == [5]
    assert pipeline.df.show_calls == [(2, False)]


def test_config_accepts_dry_run_mode_enum() -> None:
    config = EtlRunConfig(pipeline_name="customer_etl", mode=ExecutionMode.DRY_RUN)

    assert config.mode == ExecutionMode.DRY_RUN
    assert config.dry_run is True


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pipeline_name": " "},
        {"pipeline_name": None},
        {"pipeline_name": "customer_etl", "dry_run_limit": 0},
        {"pipeline_name": "customer_etl", "dry_run_limit": True},
        {"pipeline_name": "customer_etl", "dry_run_limit": "100"},
        {"pipeline_name": "customer_etl", "dry_run_show_rows": 0},
        {"pipeline_name": "customer_etl", "dry_run_show_rows": True},
        {"pipeline_name": "customer_etl", "dry_run_show_rows": "20"},
        {"pipeline_name": "customer_etl", "mode": "dry_run"},
        {"pipeline_name": "customer_etl", "mode": "invalid"},
    ],
)
def test_config_validates_values_with_standard_library(kwargs) -> None:
    with pytest.raises(ValueError):
        EtlRunConfig(**kwargs)


def test_execution_context_rejects_blank_trigger() -> None:
    with pytest.raises(ValueError):
        EtlExecutionContext(triggered_by=" ")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"run_id": " "},
        {"run_id": None},
        {"started_at": "2026-05-07"},
        {"triggered_by": None},
    ],
)
def test_execution_context_validates_values(kwargs) -> None:
    with pytest.raises(ValueError):
        EtlExecutionContext(**kwargs)


def test_base_contract_requires_config_object() -> None:
    with pytest.raises(TypeError):
        OrderedPipeline(config=cast(Any, "invalid"))


def test_public_stages_emit_structured_logs() -> None:
    stream = StringIO()
    logger = logging.getLogger("test_public_stages_emit_structured_logs")
    logger.handlers = [logging.StreamHandler(stream)]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    pipeline = OrderedPipeline(logger=logger)

    pipeline.run()

    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    emitted_events = {(record["stage"], record["event"]) for record in records}

    for stage in ["run", "extract", "check", "transform", "validate", "load"]:
        assert (stage, "stage_started") in emitted_events
        assert (stage, "stage_finished") in emitted_events

    assert all(record["pipeline_name"] == "ordered_pipeline" for record in records)
    assert all(record["run_id"] == pipeline.context.run_id for record in records)
    assert all("stage_started_at" in record for record in records)
    assert all(
        "stage_finished_at" in record
        for record in records
        if record["event"] == "stage_finished"
    )
    assert all(
        "duration_seconds" in record
        for record in records
        if record["event"] == "stage_finished"
    )


def test_public_methods_cannot_be_overridden() -> None:
    with pytest.raises(TypeError, match="cannot override"):

        class InvalidPipeline(OrderedPipeline):
            def run(self):
                return None


@pytest.mark.parametrize(
    ("hook_name", "method_name", "expected_error"),
    [
        ("_extract", "extract", ExtractError),
        ("_check", "check", CheckError),
        ("_transform", "transform", TransformError),
        ("_validate", "validate", ValidateError),
        ("_load", "load", LoadError),
    ],
)
def test_stage_failures_are_mapped_to_specific_errors(
    hook_name: str,
    method_name: str,
    expected_error: type[Exception],
) -> None:
    class FailingPipeline(OrderedPipeline):
        pass

    pipeline = FailingPipeline()

    def fail(*args, **kwargs):
        raise RuntimeError("boom")

    setattr(pipeline, hook_name, fail)

    with pytest.raises(expected_error) as exc_info:
        if method_name == "extract":
            getattr(pipeline, method_name)()
        else:
            getattr(pipeline, method_name)(pipeline.df)

    assert isinstance(exc_info.value.__cause__, RuntimeError)
