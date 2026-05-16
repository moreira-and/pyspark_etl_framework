from __future__ import annotations

import time
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.infra.errors import CertifyError, LoadError, ensure_stage_error
from etl_framework.infra.logger import get_logger, log_event
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


class Load(ABC):
    """Contract for the load and certification stages.

    The framework standardizes when loading happens and how failures are
    reported. Concrete pipelines own the destination-specific write logic and
    the simple final evidence produced by certification.
    """

    def run(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Execute load, then certify the completed load."""
        logger = get_logger(config.pipeline_name)

        try:
            self._load(df, spark, config, context)
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                LoadError,
                pipeline_name=config.pipeline_name,
                run_id=context.run_id,
            )
            if error is exc:
                raise
            raise error from exc

        started_at = time.perf_counter()
        log_event(
            logger,
            "certify_started",
            config,
            context,
            stage="certify",
            status="started",
        )
        try:
            self._certify(df, spark, config, context)
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                CertifyError,
                pipeline_name=config.pipeline_name,
                run_id=context.run_id,
            )
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                logger,
                "certify_failed",
                config,
                context,
                stage="certify",
                status="failed",
                elapsed_ms=elapsed_ms,
                error_type=type(error).__name__,
                error_message=str(error),
            )
            if error is exc:
                raise
            raise error from exc

        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        log_event(
            logger,
            "certify_succeeded",
            config,
            context,
            stage="certify",
            status="succeeded",
            elapsed_ms=elapsed_ms,
            target_schema=config.target_schema,
            target_table=config.target_table,
            target_path=config.target_path,
            write_mode=config.write_mode,
        )

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
