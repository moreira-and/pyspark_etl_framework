from __future__ import annotations

import time
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts._type_checks import require_dataframe
from etl_framework.infra.errors import CertifyError, LoadError, ensure_stage_error
from etl_framework.infra.logger import get_logger, log_event
from etl_framework.infra.stage import stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


class Load(ABC):
    """Contract for the load and certification stages.

    The framework standardizes when loading happens, how dry-run skips writes
    and how failures are reported. Concrete pipelines own the
    destination-specific write logic and the simple final evidence produced by
    certification.
    """

    def run(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Execute dry-run evidence or real load followed by certification."""
        if config.dry_run:
            self._run_dry_run(
                df=df,
                spark=spark,
                config=config,
                context=context,
            )
            return

        self._run_load(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )
        self._run_certify(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )

    @stage("load", LoadError)
    def _run_load(
        self,
        *,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Execute destination loading as its own observable stage."""
        load_df = require_dataframe(df, stage="load")
        self._load(load_df, spark, config, context)

    def _run_dry_run(
        self,
        *,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Produce dry-run evidence without calling real load or certify."""
        logger = get_logger(config.pipeline_name)
        started_at = time.perf_counter()
        log_event(
            logger,
            "load_started",
            config,
            context,
            stage="load",
            status="started",
        )

        try:
            dry_run_df = require_dataframe(df, stage="load")
            log_event(
                logger,
                "dry_run_load_skipped",
                config,
                context,
                stage="load",
                status="skipped",
                dry_run_show_rows=config.dry_run_show_rows,
            )
            if config.dry_run_show_rows > 0:
                log_event(
                    logger,
                    "dry_run_sample_requested",
                    config,
                    context,
                    stage="load",
                    status="sample_requested",
                    dry_run_show_rows=config.dry_run_show_rows,
                )
                dry_run_df.show(config.dry_run_show_rows, truncate=False)

            log_event(
                logger,
                "dry_run_evidence",
                config,
                context,
                stage="load",
                status="skipped",
                dry_run=True,
                dry_run_limit=config.dry_run_limit,
                dry_run_show_rows=config.dry_run_show_rows,
            )
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                logger,
                "dry_run_load_completed",
                config,
                context,
                stage="load",
                status="skipped",
                elapsed_ms=elapsed_ms,
                dry_run=True,
            )
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                LoadError,
                pipeline_name=config.pipeline_name,
                run_id=context.run_id,
            )
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                logger,
                "load_failed",
                config,
                context,
                stage="load",
                status="failed",
                elapsed_ms=elapsed_ms,
                error_type=type(error).__name__,
                error_message=str(error),
            )
            if error is exc:
                raise
            raise error from exc

    @stage("certify", CertifyError)
    def _run_certify(
        self,
        *,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Execute destination certification after a successful load."""
        self._certify(df, spark, config, context)

    @abstractmethod
    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Implement destination-specific loading in a concrete pipeline."""
        raise NotImplementedError

    @abstractmethod
    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Implement final evidence collection after a successful load."""
        raise NotImplementedError
