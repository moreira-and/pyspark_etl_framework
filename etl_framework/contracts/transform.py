from __future__ import annotations

import time
from abc import ABC, abstractmethod

from etl_framework.contracts._type_checks import require_dataframe
from etl_framework.infra.errors import TransformError, ValidateError, ensure_stage_error
from etl_framework.infra.logger import get_logger, log_event
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from pyspark.sql import DataFrame, SparkSession


class Transform(ABC):
    """Contract for the transform and final validation stages.

    The framework owns sequencing, logging and error wrapping. Concrete
    pipelines own the business transformation and the validation rules applied
    to the transformed DataFrame.
    """

    def run(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute transform, then validate, returning the validated DataFrame."""
        logger = get_logger(config.pipeline_name)

        try:
            df = self._transform(df, spark, config, context)
            df = require_dataframe(df, stage="transform")
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                TransformError,
                pipeline_name=config.pipeline_name,
            )
            if error is exc:
                raise
            raise error from exc

        started_at = time.perf_counter()
        log_event(
            logger,
            "validate_started",
            config,
            context,
            stage="validate",
            status="started",
        )
        try:
            validated_df = self._validate(df, spark, config, context)
            validated_df = require_dataframe(validated_df, stage="validate")
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                ValidateError,
                pipeline_name=config.pipeline_name,
            )
            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                logger,
                "validate_failed",
                config,
                context,
                stage="validate",
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
            "validate_succeeded",
            config,
            context,
            stage="validate",
            status="succeeded",
            elapsed_ms=elapsed_ms,
        )
        return validated_df

    @abstractmethod
    def _transform(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Implement pipeline-specific business transformation."""
        raise NotImplementedError

    @abstractmethod
    def _validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Implement final validation before the load stage."""
        raise NotImplementedError
