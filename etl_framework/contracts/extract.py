from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.infra.errors import CheckError, ExtractError
from etl_framework.infra.stage import runtime_event, stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.auto_quality import auto_check_source
from etl_framework.utils.dataframe_checks import require_dataframe
from etl_framework.utils.stage_metadata import dry_run_extract_metadata


class Extract(ABC):
    """Contract for source-specific extraction plus automatic source check.

    The framework owns the execution order, observability and error handling.
    Concrete pipelines own the source-specific extraction logic. The framework
    automatically checks the extracted DataFrame against `config.source_struct`
    before downstream stages run. Pipelines may override `_custom_check` for
    small pipeline-specific checks. Structural validation is always framework
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
        df = self._run_check(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )
        if not config.dry_run:
            return df

        return self._limit_dry_run_extract(
            df=df,
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
        checked_df = auto_check_source(
            df,
            config.source_struct,
            strict=config.strict_schema,
        )
        checked_df = self._custom_check(checked_df, spark, config, context)
        return require_dataframe(checked_df, stage="check")

    @runtime_event(
        "dry_run_extract_limited",
        stage_name="extract",
        status="limited",
        extra=dry_run_extract_metadata,
    )
    def _limit_dry_run_extract(
        self,
        *,
        df: DataFrame,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Limit checked source data before transform in dry-run mode."""
        return df.limit(config.dry_run_limit)

    @abstractmethod
    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Implement source-specific extraction in a concrete pipeline."""
        raise NotImplementedError

    def _custom_check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Optional custom check hook after mandatory source validation."""
        return df
