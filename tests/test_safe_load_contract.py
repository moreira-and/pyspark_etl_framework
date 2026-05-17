from __future__ import annotations

from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts import Extract, Load, Pipeline, Transform
from etl_framework.infra.errors import CertifyError, LoadError
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

pytestmark = pytest.mark.integration


class MemoryDestination:
    def __init__(self) -> None:
        self.committed_rows: list[dict[str, Any]] = []
        self.staged_rows: dict[str, list[dict[str, Any]]] = {}

    def stage(self, run_id: str, rows: list[dict[str, Any]]) -> None:
        self.staged_rows[run_id] = rows

    def discard_stage(self, run_id: str) -> None:
        self.staged_rows.pop(run_id, None)

    def commit_by_key(self, run_id: str, key_fields: tuple[str, ...]) -> None:
        rows = self.staged_rows.pop(run_id)
        staged_keys = {tuple(row[field] for field in key_fields) for row in rows}
        self.committed_rows = [
            row
            for row in self.committed_rows
            if tuple(row[field] for field in key_fields) not in staged_keys
        ]
        self.committed_rows.extend(rows)

    def read_committed(self) -> list[dict[str, Any]]:
        return list(self.committed_rows)


class InlineExtract(Extract):
    def __init__(self, rows: list[tuple[int, str]]) -> None:
        self.rows = rows

    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        return spark.createDataFrame(self.rows, "id int, name string")

    def _check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        return df


class IdentityTransform(Transform):
    def _transform(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        return df

    def _validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        return df


class SafeMemoryLoad(Load):
    def __init__(
        self,
        destination: MemoryDestination,
        *,
        fail_before_commit: bool = False,
        fail_certify: bool = False,
        remove_destination_before_certify: bool = False,
    ) -> None:
        self.destination = destination
        self.fail_before_commit = fail_before_commit
        self.fail_certify = fail_certify
        self.remove_destination_before_certify = remove_destination_before_certify
        self.expected_keys: set[tuple[Any, ...]] = set()
        self.certified_from_destination = False

    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        rows = [{**row.asDict(), "etl_run_id": context.run_id} for row in df.collect()]
        self.expected_keys = {
            tuple(row[field] for field in config.target_key) for row in rows
        }
        self.destination.stage(context.run_id, rows)
        if self.fail_before_commit:
            self.destination.discard_stage(context.run_id)
            raise RuntimeError("simulated load failure before commit")
        self.destination.commit_by_key(context.run_id, config.target_key)
        context.metrics["rows_written"] = len(rows)

    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        if self.fail_certify:
            raise RuntimeError("simulated certify failure")
        if self.remove_destination_before_certify:
            self.destination.committed_rows.clear()

        committed = self.destination.read_committed()
        committed_keys = {
            tuple(row[field] for field in config.target_key) for row in committed
        }
        self.certified_from_destination = True
        if not self.expected_keys.issubset(committed_keys):
            raise RuntimeError("destination does not contain all staged keys")
        context.metrics["rows_certified"] = len(self.expected_keys)


def config() -> EtlRunConfig:
    return EtlRunConfig(
        pipeline_name="safe_load_contract",
        target_schema="silver",
        target_table="people",
        target_path="memory://silver/people",
        target_key=("id",),
        write_mode="overwrite",
    )


def run_pipeline(
    spark: SparkSession,
    destination: MemoryDestination,
    *,
    load: SafeMemoryLoad | None = None,
    context: EtlExecutionContext | None = None,
) -> SafeMemoryLoad:
    load_step = load or SafeMemoryLoad(destination)
    pipeline = Pipeline(
        spark=spark,
        config=config(),
        context=context or EtlExecutionContext(),
        extract=InlineExtract([(1, "ana"), (2, "bruno")]),
        transform=IdentityTransform(),
        load=load_step,
    )
    pipeline.run()
    return load_step


def test_safe_load_failure_before_commit_leaves_no_partial_rows(
    spark: SparkSession,
) -> None:
    # Arrange
    destination = MemoryDestination()
    destination.committed_rows = [{"id": 99, "name": "existing", "etl_run_id": "old"}]
    load = SafeMemoryLoad(destination, fail_before_commit=True)

    # Act / Assert
    with pytest.raises(LoadError):
        run_pipeline(spark, destination, load=load)

    assert destination.read_committed() == [
        {"id": 99, "name": "existing", "etl_run_id": "old"}
    ]
    assert destination.staged_rows == {}


def test_safe_load_rerun_replaces_scope_without_duplicates(
    spark: SparkSession,
) -> None:
    # Arrange
    destination = MemoryDestination()

    # Act
    run_pipeline(spark, destination, context=EtlExecutionContext(run_id="run-1"))
    run_pipeline(spark, destination, context=EtlExecutionContext(run_id="run-2"))

    # Assert
    committed = sorted(destination.read_committed(), key=lambda row: row["id"])
    assert [(row["id"], row["name"]) for row in committed] == [
        (1, "ana"),
        (2, "bruno"),
    ]
    assert [row["etl_run_id"] for row in committed] == ["run-2", "run-2"]


def test_safe_load_certify_failure_is_retry_safe(
    spark: SparkSession,
) -> None:
    # Arrange
    destination = MemoryDestination()
    failing_load = SafeMemoryLoad(destination, fail_certify=True)

    # Act
    with pytest.raises(CertifyError):
        run_pipeline(spark, destination, load=failing_load)
    run_pipeline(spark, destination)

    # Assert
    committed = destination.read_committed()
    keys = [row["id"] for row in committed]
    assert sorted(keys) == [1, 2]
    assert len(keys) == len(set(keys))


def test_safe_load_certification_reads_destination(
    spark: SparkSession,
) -> None:
    # Arrange
    destination = MemoryDestination()
    load = SafeMemoryLoad(destination, remove_destination_before_certify=True)

    # Act / Assert
    with pytest.raises(CertifyError):
        run_pipeline(spark, destination, load=load)
    assert load.certified_from_destination is True
