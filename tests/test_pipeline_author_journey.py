from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform
from etl_framework.infra.errors import CheckError
from etl_framework.infra.observability import configure_observability_sink
from etl_framework.models.context import EtlExecutionContext

pytestmark = pytest.mark.integration

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "pipeline_author_journey"
SOURCE_CSV = FIXTURE_DIR / "source.csv"
EXPECTED_TARGET_CSV = FIXTURE_DIR / "expected_target.csv"
INVALID_SOURCE_CSV = FIXTURE_DIR / "invalid_source.csv"

SOURCE_STRUCT = StructType(
    [
        StructField("Rank", IntegerType(), nullable=False),
        StructField("State", StringType(), nullable=False),
        StructField("Postal", StringType(), nullable=False),
        StructField("Population", DoubleType(), nullable=False),
    ]
)
TARGET_STRUCT = StructType(
    [
        StructField("state_code", StringType(), nullable=False),
        StructField("state_name", StringType(), nullable=False),
        StructField("population", IntegerType(), nullable=False),
        StructField("population_band", StringType(), nullable=False),
    ]
)


# This is the kind of code the pipeline author writes: read, transform and load.
class ReadStatePopulationCsv(Extract):
    def __init__(self, source_path: Path) -> None:
        self.source_path = source_path

    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        return (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(self.source_path))
        )


class CreatePopulationBands(Transform):
    def _transform(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        population = F.col("Population").cast("int")
        return df.select(
            F.col("Postal").alias("state_code"),
            F.upper("State").alias("state_name"),
            population.alias("population"),
            F.when(population >= 10_000_000, "large")
            .when(population >= 5_000_000, "medium")
            .otherwise("small")
            .alias("population_band"),
        )


class RememberLoadedRows(Load):
    def __init__(self) -> None:
        self.loaded_rows: list[tuple[str, str, int, str]] = []
        self.certified = False

    @property
    def load_called(self) -> bool:
        return bool(self.loaded_rows)

    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.loaded_rows = _target_rows(df)

    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        self.certified = True


# Everything below is test support: observability capture and assertions.
class InMemoryObservabilitySink:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(dict(event))


def _state_population_config(**overrides: object) -> EtlRunConfig:
    values = {
        "pipeline_name": "csv_author_journey",
        "target_schema": "silver",
        "target_table": "state_population",
        "target_path": "memory://state_population",
        "target_key": ("state_code",),
        "source_struct": SOURCE_STRUCT,
        "target_struct": TARGET_STRUCT,
    }
    values.update(overrides)
    return EtlRunConfig(**values)


def _pipeline(
    spark: SparkSession,
    *,
    run_id: str,
    config: EtlRunConfig,
    source_path: Path,
    load: RememberLoadedRows,
) -> Pipeline:
    return Pipeline(
        spark=spark,
        config=config,
        context=EtlExecutionContext(run_id=run_id),
        extract=ReadStatePopulationCsv(source_path),
        transform=CreatePopulationBands(),
        load=load,
    )


def _expected_rows(spark: SparkSession) -> list[tuple[str, str, int, str]]:
    return _target_rows(
        spark.read.schema(TARGET_STRUCT)
        .option("header", True)
        .csv(str(EXPECTED_TARGET_CSV))
    )


def _target_rows(df: DataFrame) -> list[tuple[str, str, int, str]]:
    return [
        (
            row.state_code,
            row.state_name,
            row.population,
            row.population_band,
        )
        for row in df.orderBy("state_code").collect()
    ]


def _event(events: list[dict[str, object]], name: str) -> dict[str, object]:
    return next(event for event in events if event["event"] == name)


def _event_names(events: list[dict[str, object]]) -> list[object]:
    return [event["event"] for event in events]


def _assert_pipeline_author_wrote_only_etl(
    pipeline: Pipeline,
) -> None:
    components = [pipeline.extract_step, pipeline.transform_step, pipeline.load_step]
    for component in components:
        assert not hasattr(component, "logger")
        assert not hasattr(component, "observability")
        assert not hasattr(component, "event_logger")


def _assert_trace_fields(
    events: list[dict[str, object]],
    *,
    pipeline_name: str,
    run_id: str,
) -> None:
    for event in events:
        assert event["pipeline_name"] == pipeline_name
        assert event["run_id"] == run_id
        assert "stage" in event
        assert "status" in event


def _assert_success_duration(events: list[dict[str, object]], name: str) -> None:
    payload = _event(events, name)
    assert isinstance(payload["elapsed_ms"], float)
    assert payload["elapsed_ms"] >= 0


def _assert_framework_emitted(
    events: list[dict[str, object]],
    *expected_names: str,
) -> None:
    event_names = _event_names(events)
    for name in expected_names:
        assert name in event_names


def test_junior_developer_builds_csv_pipeline_and_gets_operational_guarantees_for_free(
    spark: SparkSession,
) -> None:
    # Step 1 - Junior implements only CSV extract, transform and load logic.
    happy_sink = InMemoryObservabilitySink()
    configure_observability_sink(happy_sink)
    happy_load = RememberLoadedRows()
    happy_config = _state_population_config(pipeline_name="csv_author_happy_path")
    happy_pipeline = _pipeline(
        spark,
        run_id="run-csv-author-happy",
        config=happy_config,
        source_path=SOURCE_CSV,
        load=happy_load,
    )

    _assert_pipeline_author_wrote_only_etl(happy_pipeline)

    # Step 2 - Framework extracts the CSV, checks source shape and validates target.
    happy_result = happy_pipeline.run()

    assert _target_rows(happy_result) == _expected_rows(spark)
    assert happy_load.loaded_rows == _expected_rows(spark)
    assert happy_load.certified is True

    # Step 3 - Framework captures observability automatically.
    _assert_framework_emitted(
        happy_sink.events,
        "run_started",
        "extract_succeeded",
        "check_succeeded",
        "transform_succeeded",
        "validate_succeeded",
        "load_succeeded",
        "certify_succeeded",
        "run_succeeded",
        "execution_summary",
    )
    _assert_trace_fields(
        happy_sink.events,
        pipeline_name="csv_author_happy_path",
        run_id="run-csv-author-happy",
    )
    _assert_success_duration(happy_sink.events, "extract_succeeded")
    _assert_success_duration(happy_sink.events, "validate_succeeded")
    _assert_success_duration(happy_sink.events, "run_succeeded")

    # Step 4 - Framework prevents unsafe writes in dry_run.
    dry_run_sink = InMemoryObservabilitySink()
    configure_observability_sink(dry_run_sink)
    dry_run_load = RememberLoadedRows()
    dry_run_pipeline = _pipeline(
        spark,
        run_id="run-csv-author-dry-run",
        config=_state_population_config(
            pipeline_name="csv_author_dry_run",
            dry_run=True,
            dry_run_limit=2,
            dry_run_show_rows=0,
        ),
        source_path=SOURCE_CSV,
        load=dry_run_load,
    )

    dry_run_result = dry_run_pipeline.run()

    assert dry_run_result.count() == 2
    assert dry_run_load.load_called is False
    assert dry_run_load.certified is False
    assert "load_succeeded" not in _event_names(dry_run_sink.events)
    assert "certify_started" not in _event_names(dry_run_sink.events)
    dry_run_evidence = _event(dry_run_sink.events, "dry_run_evidence")
    assert dry_run_evidence["mode"] == "dry_run"
    assert dry_run_evidence["stage"] == "load"
    assert dry_run_evidence["status"] == "skipped"
    assert dry_run_evidence["dry_run_limit"] == 2
    _assert_trace_fields(
        dry_run_sink.events,
        pipeline_name="csv_author_dry_run",
        run_id="run-csv-author-dry-run",
    )

    # Step 5 - Framework catches a bad CSV before transform/load.
    failure_sink = InMemoryObservabilitySink()
    configure_observability_sink(failure_sink)
    failure_load = RememberLoadedRows()
    failure_pipeline = _pipeline(
        spark,
        run_id="run-csv-author-check-failure",
        config=_state_population_config(pipeline_name="csv_author_check_failure"),
        source_path=INVALID_SOURCE_CSV,
        load=failure_load,
    )

    with pytest.raises(CheckError) as error_info:
        failure_pipeline.run()

    error = error_info.value
    assert error.pipeline_name == "csv_author_check_failure"
    assert error.run_id == "run-csv-author-check-failure"
    assert error.stage == "check"
    assert failure_load.load_called is False
    assert "transform_started" not in _event_names(failure_sink.events)
    assert "load_started" not in _event_names(failure_sink.events)

    check_failure = _event(failure_sink.events, "check_failed")
    run_failure = _event(failure_sink.events, "run_failed")
    summary = failure_sink.events[-1]
    assert check_failure["stage"] == "check"
    assert check_failure["status"] == "failed"
    assert check_failure["error_type"] == "CheckError"
    assert run_failure["stage"] == "run"
    assert run_failure["status"] == "failed"
    assert summary["event"] == "execution_summary"
    assert summary["status"] == "failed"
    _assert_success_duration(failure_sink.events, "extract_succeeded")
    assert isinstance(check_failure["elapsed_ms"], float)
    _assert_trace_fields(
        failure_sink.events,
        pipeline_name="csv_author_check_failure",
        run_id="run-csv-author-check-failure",
    )
