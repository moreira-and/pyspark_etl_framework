from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from etl_framework.contracts.extract import Extract
from etl_framework.contracts.load import Load
from etl_framework.contracts.transform import Transform
from etl_framework.infra.errors import PreflightError
from etl_framework.infra.decorators import stage
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


class Pipeline:
    """Linear ETL orchestrator owned by the framework.

    The pipeline fixes the official execution order:
    extract -> check -> transform -> validate -> load -> certify.

    Concrete pipeline classes implement the specific behavior inside the
    injected Extract, Transform and Load contracts. This class should not know
    how to read, transform or write data; it only coordinates the flow,
    context, observability and error propagation.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        *,
        extract: Extract,
        transform: Transform,
        load: Load,
        context: EtlExecutionContext | None = None,
    ) -> None:
        """Create a pipeline run with its dependencies and trace context."""
        self.spark = spark
        self.config = config
        self.context = context or EtlExecutionContext()
        self.extract_step = extract
        self.transform_step = transform
        self.load_step = load

    @stage("run", emit_summary=True)
    def run(self) -> DataFrame:
        """Run the full official ETL flow and return the final DataFrame."""
        self._preflight()
        df = self.extract()
        df = self.transform(df)
        self.load(df)
        return df

    def _preflight(self) -> None:
        """Fail before extraction when the standard v0.1 flow cannot run."""
        errors: list[str] = []

        if self.config.source_struct is None:
            errors.append("source_struct is required before extract")

        if self.config.target_struct is None:
            errors.append("target_struct is required before extract")

        if self.config.target_struct is not None and self.config.target_key:
            target_columns = {field.name for field in self.config.target_struct.fields}
            missing_keys = [
                key for key in self.config.target_key if key not in target_columns
            ]
            if missing_keys:
                errors.append(
                    "target_key contains columns not present in target_struct: "
                    f"{missing_keys}"
                )

        if self.config.dry_run_show_rows > 0 and not self.config.dry_run:
            errors.append(
                "dry_run_show_rows can be greater than zero only when dry_run=True"
            )

        if errors:
            raise PreflightError(
                "Preflight failed before extract: " + "; ".join(errors),
                pipeline_name=self.config.pipeline_name,
                run_id=self.context.run_id,
            )

    def extract(self) -> DataFrame:
        """Run the extract contract."""
        return self.extract_step.run(
            spark=self.spark,
            config=self.config,
            context=self.context,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Run the transform contract for a checked DataFrame."""
        return self.transform_step.run(
            df=df,
            spark=self.spark,
            config=self.config,
            context=self.context,
        )

    def load(self, df: DataFrame) -> None:
        """Run the load contract; `Load.run` owns dry-run behavior."""
        self.load_step.run(
            df=df,
            spark=self.spark,
            config=self.config,
            context=self.context,
        )
