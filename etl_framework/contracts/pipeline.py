from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts.extract import Extract
from etl_framework.contracts.load import Load
from etl_framework.contracts.transform import Transform
from etl_framework.infra.logger import get_logger, log_event
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


class Pipeline:
    """Linear ETL orchestrator owned by the framework.

    The pipeline fixes the official execution order:
    extract -> check -> transform -> validate -> load -> certify.

    Concrete pipeline classes implement the specific behavior inside the
    injected Extract, Transform and Load contracts. This class should not know
    how to read, transform or write data; it only coordinates the flow,
    context, logging and error propagation.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        *,
        extract: Extract,
        transform: Transform,
        load: Load,
        context: EtlExecutionContext | None = None,
    ) -> None:
        """Create a pipeline run with its dependencies and trace context."""
        self.spark = spark
        self.config = config
        self.context = context or EtlExecutionContext()
        self.logger = get_logger(self.config.pipeline_name)
        self.extract_step = extract
        self.transform_step = transform
        self.load_step = load

    def run(self) -> DataFrame:
        """Run the full official ETL flow and return the final DataFrame."""
        started_at = time.perf_counter()
        log_event(
            self.logger,
            "run_started",
            self.config,
            self.context,
            stage="run",
            status="started",
            dry_run=self.config.dry_run,
        )

        try:
            df = self.extract()
            df = self.transform(df)
            self.load(df)
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                self.logger,
                "run_failed",
                self.config,
                self.context,
                stage="run",
                status="failed",
                elapsed_ms=elapsed_ms,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            log_event(
                self.logger,
                "execution_summary",
                self.config,
                self.context,
                stage="run",
                status="failed",
                elapsed_ms=elapsed_ms,
                dry_run=self.config.dry_run,
                error_type=type(exc).__name__,
            )
            raise

        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        log_event(
            self.logger,
            "run_succeeded",
            self.config,
            self.context,
            stage="run",
            status="succeeded",
            elapsed_ms=elapsed_ms,
        )
        log_event(
            self.logger,
            "execution_summary",
            self.config,
            self.context,
            stage="run",
            status="succeeded",
            elapsed_ms=elapsed_ms,
            dry_run=self.config.dry_run,
        )
        return df

    def extract(self) -> DataFrame:
        """Run the extract contract and apply dry-run limiting when enabled."""
        return self._run_extract()

    def _run_extract(self) -> DataFrame:
        """Run extract/check and apply dry-run limiting when enabled."""
        df = self.extract_step.run(
            spark=self.spark,
            config=self.config,
            context=self.context,
        )
        return self._apply_dry_run_limit(df)

    def transform(self, df: DataFrame) -> DataFrame:
        """Run the transform contract for a checked DataFrame."""
        return self._run_transform(df)

    def _run_transform(self, df: DataFrame) -> DataFrame:
        """Run transform/validate for a checked DataFrame."""
        return self.transform_step.run(
            df=df,
            spark=self.spark,
            config=self.config,
            context=self.context,
        )

    def load(self, df: DataFrame) -> None:
        """Run the load contract; `Load.run` owns dry-run behavior."""
        self._run_load(df)

    def _run_load(self, df: DataFrame) -> None:
        """Run the real load path. Dry-run is handled by `load`."""
        self.load_step.run(
            df=df,
            spark=self.spark,
            config=self.config,
            context=self.context,
        )

    def _apply_dry_run_limit(self, df: DataFrame) -> DataFrame:
        """Limit extracted data when dry-run mode is enabled."""
        if not self.config.dry_run:
            return df

        log_event(
            self.logger,
            "dry_run_extract_limited",
            self.config,
            self.context,
            stage="extract",
            status="limited",
            dry_run_limit=self.config.dry_run_limit,
        )
        return df.limit(self.config.dry_run_limit)
