from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts._type_checks import require_dataframe
from etl_framework.infra.errors import CheckError, ExtractError, ensure_stage_error
from etl_framework.infra.stage import stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.auto_quality import auto_check_source


class Extract(ABC):
    """Contract for source-specific extraction plus automatic source check.

    The framework owns the execution order and error/logging standardization.
    Concrete pipelines own the source-specific extraction logic. The framework
    automatically checks the extracted DataFrame against `config.source_struct`
    before downstream stages run. Pipelines may still override `_check` for
    small legacy or pipeline-specific checks, but basic structural validation no
    longer belongs in junior-owned pipeline code.
    """

    def run(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        """Execute extract, then automatic check, returning the checked DataFrame."""
        try:
            df = self._extract(spark, config, context)
            df = require_dataframe(df, stage="extract")
        except Exception as exc:
            error = ensure_stage_error(
                exc,
                ExtractError,
                pipeline_name=config.pipeline_name,
                run_id=context.run_id,
            )
            if error is exc:
                raise
            raise error from exc

        return self._run_check(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )

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
        checked_df = self._check(checked_df, spark, config, context)
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
        """Optional legacy hook for extra checks after automatic source check."""
        return df
