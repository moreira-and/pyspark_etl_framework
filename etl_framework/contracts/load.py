from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.infra.errors import CertifyError, LoadError, ensure_stage_error
from etl_framework.infra.stage import stage
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

        self._run_certify(
            df=df,
            spark=spark,
            config=config,
            context=context,
        )

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
