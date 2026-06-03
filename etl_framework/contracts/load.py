from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from etl_framework.infra.errors import CertifyError, LoadError
from etl_framework.infra.decorators import runtime_event, stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.dataframe_checks import require_dataframe



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
        load_df = self._persistable_df(df, config=config)
        self._load(load_df, spark, config, context)

    @stage(
        "load",
        LoadError,
        succeeded_event="dry_run_load_completed",
        succeeded_status="skipped",
    )
    @runtime_event(
        "dry_run_evidence",
        stage_name="load",
        status="skipped",
        extra=lambda config, context: config.dry_run_evidence_metadata(),
    )
    def _run_dry_run(
        self,
        *,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Produce dry-run evidence without calling real load or certify."""
        dry_run_df = self._persistable_df(df, config=config)
        if config.dry_run_show_rows > 0:
            self._show_dry_run_sample(
                dry_run_df,
                config=config,
                context=context,
            )

    @runtime_event(
        "dry_run_sample_requested",
        stage_name="load",
        status="sample_requested",
        extra=lambda config, context: config.dry_run_sample_metadata(),
        timing="started",
    )
    def _show_dry_run_sample(
        self,
        df: DataFrame,
        *,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Show a small dry-run sample when explicitly requested."""
        df.show(config.dry_run_show_rows, truncate=False)

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
        certify_df = self._persistable_df(df, config=config)
        self._certify(certify_df, spark, config, context)

    def _persistable_df(self, df: DataFrame, *, config: EtlRunConfig) -> DataFrame:
        """Return the DataFrame shape configured for load/certify hooks."""
        checked_df = require_dataframe(df, stage="load")
        if config.keep_technical_columns:
            return checked_df

        business_columns = [
            column
            for column in checked_df.columns
            if column.lower() not in config.TECHNICAL_COLUMNS
        ]
        return checked_df.select(*business_columns)

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

    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        """Optionally implement final evidence collection after a successful load."""
        return None
