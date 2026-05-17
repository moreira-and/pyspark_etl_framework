from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts.extract import Extract
from etl_framework.contracts.load import Load
from etl_framework.contracts.pipeline import Pipeline
from etl_framework.contracts.transform import Transform
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

pytestmark = pytest.mark.integration


class CountingExtract(Extract):
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.check_input_count: int | None = None

    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("extract")
        return spark.createDataFrame(
            [(1, "ana"), (2, "bruno"), (3, "carla")],
            "id int, name string",
        )

    def _check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("check")
        self.check_input_count = df.count()
        return df


class CountingTransform(Transform):
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.transform_input_count: int | None = None
        self.validate_input_count: int | None = None

    def _transform(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("transform")
        self.transform_input_count = df.count()
        return df

    def _validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        self.events.append("validate")
        self.validate_input_count = df.count()
        return df


class SpyLoad(Load):
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.load_called = False
        self.certify_called = False

    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("load")
        self.load_called = True

    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.events.append("certify")
        self.certify_called = True


def _config(**overrides: Any) -> EtlRunConfig:
    values: dict[str, Any] = {
        "pipeline_name": "dry_run_contract",
        "target_schema": "test",
        "target_table": "people",
        "target_path": "memory://people",
        "target_key": ("id",),
    }
    values.update(overrides)
    return EtlRunConfig(**values)


def _pipeline(
    spark: SparkSession,
    config: EtlRunConfig,
) -> tuple[Pipeline, CountingExtract, CountingTransform, SpyLoad, list[str]]:
    events: list[str] = []
    extract = CountingExtract(events)
    transform = CountingTransform(events)
    load = SpyLoad(events)

    pipeline = Pipeline(
        spark=spark,
        config=config,
        extract=extract,
        transform=transform,
        load=load,
    )

    return pipeline, extract, transform, load, events


def capture_show_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, bool]]:
    show_calls: list[tuple[int, bool]] = []

    def capture_show(
        self: DataFrame,
        n: int = 20,
        truncate: bool | int = True,
        vertical: bool = False,
    ) -> None:
        show_calls.append((n, bool(truncate)))

    monkeypatch.setattr(DataFrame, "show", capture_show)
    return show_calls


def test_dry_run_limits_after_check_and_skips_load(spark: SparkSession) -> None:
    # Arrange
    pipeline, extract, transform, load, events = _pipeline(
        spark,
        _config(dry_run=True, dry_run_limit=2, dry_run_show_rows=0),
    )

    # Act
    result = pipeline.run()

    # Assert
    # Protects production jobs from accidental writes while preserving validation.
    assert extract.check_input_count == 3
    assert transform.transform_input_count == 2
    assert transform.validate_input_count == 2
    assert result.count() == 2

    assert load.load_called is False
    assert load.certify_called is False
    assert events == ["extract", "check", "transform", "validate"]


def test_show_is_not_called_in_production(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    show_calls = capture_show_calls(monkeypatch)
    pipeline, _, _, _, _ = _pipeline(
        spark,
        _config(dry_run=False, dry_run_limit=2),
    )

    # Act
    pipeline.run()

    # Assert
    assert show_calls == []


def test_show_is_not_called_in_dry_run_when_show_rows_is_zero(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    show_calls = capture_show_calls(monkeypatch)
    pipeline, _, _, _, _ = _pipeline(
        spark,
        _config(dry_run=True, dry_run_limit=2, dry_run_show_rows=0),
    )

    # Act
    pipeline.run()

    # Assert
    assert show_calls == []


def test_show_is_called_only_for_dry_run_with_positive_show_rows(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    show_calls = capture_show_calls(monkeypatch)
    pipeline, _, _, _, _ = _pipeline(
        spark,
        _config(dry_run=True, dry_run_limit=2, dry_run_show_rows=2),
    )

    # Act
    pipeline.run()

    # Assert
    assert show_calls == [(2, False)]
