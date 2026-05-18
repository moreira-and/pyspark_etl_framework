from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts._type_checks import require_dataframe
from etl_framework.infra.errors import CheckError, ExtractError
from etl_framework.infra.stage import stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.auto_quality import auto_check_source


class Extract(ABC):
    """Contract for source-specific extraction plus automatic source check.

    The framework owns the execution order and error/logging standardization.
    Concrete pipelines own the source-specific extraction logic. The framework
    automatically checks the extracted DataFrame against `config.source_struct`
    before downstream stages run. Pipelines may override `_custom_check` for
    small pipeline-specific checks. The legacy `_check` hook is still called by
    default for compatibility, but structural validation is always framework
    owned and runs before any custom hook.
    """

    def run(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute extract, then automatic check, returning the checked DataFrame."""
        df = self._run_extract(
            spark=spark,
            config=config,
            context=context,
        )
        return self._run_check(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )

    @stage("extract", ExtractError)
    def _run_extract(
        self,
        *,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute the concrete extract hook and validate its return type."""
        df = self._extract(spark, config, context)
        return require_dataframe(df, stage="extract")

    @stage("check", CheckError)
    def _run_check(
        self,
        *,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute automatic source check and optional pipeline-specific hook."""
        checked_df = self._auto_check(df, config)
        checked_df = self._custom_check(checked_df, spark, config, context)
        return require_dataframe(checked_df, stage="check")

    def _auto_check(self, df: DataFrame, config: EtlRunConfig) -> DataFrame:
        """Validate extracted data against the declared source structure."""
        return auto_check_source(
            df,
            config.source_struct,
            strict=config.strict_schema,
        )

    @abstractmethod
    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Implement source-specific extraction in a concrete pipeline."""
        raise NotImplementedError

    def _check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Legacy optional hook for extra checks after automatic source check."""
        return df

    def _custom_check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Optional custom check hook after mandatory source validation."""
        return self._check(df, spark, config, context)
