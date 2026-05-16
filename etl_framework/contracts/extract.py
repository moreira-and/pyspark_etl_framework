from __future__ import annotations

import time
from abc import ABC, abstractmethod

from etl_framework.contracts._type_checks import require_dataframe
from etl_framework.infra.errors import CheckError, ExtractError, ensure_stage_error
from etl_framework.infra.logger import get_logger, log_event
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from pyspark.sql import DataFrame, SparkSession


class Extract(ABC):
    """Contract for the extract and initial check stages.

    The framework owns the execution order and error/logging standardization.
    Concrete pipelines own only the source-specific extraction logic and the
    initial structural checks implemented in the protected methods.
    """

    def run(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute extract, then check, returning the checked DataFrame."""
        logger = get_logger(config.pipeline_name)

        try:
            df = self._extract(spark, config, context)
            df = require_dataframe(df, stage="extract")
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                ExtractError,
                pipeline_name=config.pipeline_name,
            )
            if error is exc:
                raise
            raise error from exc

        started_at = time.perf_counter()
        log_event(
            logger,
            "check_started",
            config,
            context,
            stage="check",
            status="started",
        )
        try:
            checked_df = self._check(df, spark, config, context)
            checked_df = require_dataframe(checked_df, stage="check")
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                CheckError,
                pipeline_name=config.pipeline_name,
            )
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                logger,
                "check_failed",
                config,
                context,
                stage="check",
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
            "check_succeeded",
            config,
            context,
            stage="check",
            status="succeeded",
            elapsed_ms=elapsed_ms,
        )
        return checked_df

    @abstractmethod
    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Implement source-specific extraction in a concrete pipeline."""
        raise NotImplementedError

    @abstractmethod
    def _check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Implement initial checks that run before transformation."""
        raise NotImplementedError
