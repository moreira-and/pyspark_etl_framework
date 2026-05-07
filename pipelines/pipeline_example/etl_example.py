from __future__ import annotations

from etlstruct import EtlRunConfig, EtlStruct
from etlstruct.utils import require_columns, require_non_empty


class ExampleCustomerPipeline(EtlStruct):
    def __init__(
        self,
        spark,
        source_path: str,
        target_path: str,
        config: EtlRunConfig,
    ) -> None:
        super().__init__(config=config)
        self.spark = spark
        self.source_path = source_path
        self.target_path = target_path

    def _extract(self):
        return self.spark.read.parquet(self.source_path)

    def _check(self, df):
        return require_columns(df, ["customer_id", "name"])

    def _transform(self, df):
        return df.dropDuplicates(["customer_id"])

    def _validate(self, df):
        return require_non_empty(df)

    def _load(self, df) -> None:
        df.write.mode("overwrite").parquet(self.target_path)
