from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts._type_checks import require_dataframe
from etl_framework.infra.errors import TransformError, ValidateError
from etl_framework.infra.stage import stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.auto_quality import auto_validate_target


class Transform(ABC):
    """Contract for business transformation plus automatic target validation.

    The framework owns sequencing, logging and error wrapping. Concrete
    pipelines own the business transformation. The framework automatically
    validates transformed data against `config.target_struct` before load.
    Pipelines may override `_custom_validate` for small pipeline-specific
    checks. The legacy `_validate` hook is still called by default for
    compatibility, but structural validation is always framework owned and runs
    before any custom hook.
    """

    def run(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute transform, then automatic validate, returning a valid DataFrame."""
        df = self._run_transform(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )
        return self._run_validate(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )

    @stage("transform", TransformError)
    def _run_transform(
        self,
        *,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute the concrete transform hook and validate its return type."""
        transformed_df = self._transform(df, spark, config, context)
        return require_dataframe(transformed_df, stage="transform")

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
        validated_df = self._custom_validate(validated_df, spark, config, context)
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
        """Legacy optional hook for extra checks after automatic target validation."""
        return df

    def _custom_validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Optional custom validate hook after mandatory target validation."""
        return self._validate(df, spark, config, context)
