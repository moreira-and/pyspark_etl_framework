from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts._type_checks import require_dataframe
from etl_framework.infra.errors import TransformError, ValidateError, ensure_stage_error
from etl_framework.infra.stage import stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.auto_quality import auto_validate_target


class Transform(ABC):
    """Contract for business transformation plus automatic target validation.

    The framework owns sequencing, logging and error wrapping. Concrete
    pipelines own the business transformation. The framework automatically
    validates transformed data against `config.target_struct` before load.
    Pipelines may still override `_validate` for small legacy or
    pipeline-specific checks, but basic structural validation no longer belongs
    in junior-owned pipeline code.
    """

    def run(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute transform, then automatic validate, returning a valid DataFrame."""
        try:
            df = self._transform(df, spark, config, context)
            df = require_dataframe(df, stage="transform")
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                TransformError,
                pipeline_name=config.pipeline_name,
                run_id=context.run_id,
            )
            if error is exc:
                raise
            raise error from exc

        return self._run_validate(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )

    @stage("validate", ValidateError)
    def _run_validate(
        self,
        *,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute automatic target validation and optional pipeline hook."""
        validated_df = self._auto_validate(df, config)
        validated_df = self._validate(validated_df, spark, config, context)
        return require_dataframe(validated_df, stage="validate")

    def _auto_validate(self, df: DataFrame, config: EtlRunConfig) -> DataFrame:
        """Validate transformed data against the declared target structure."""
        return auto_validate_target(
            df,
            config.target_struct,
            strict=config.strict_schema,
        )

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

    def _validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Optional legacy hook for extra checks after automatic target validation."""
        return df
