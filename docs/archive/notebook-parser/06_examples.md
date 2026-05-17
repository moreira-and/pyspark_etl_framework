# 06 - Examples

These examples are intentionally small. They show expected structure, not a full
parser implementation.

## Example Artifact Tree

```text
prompts/notebook_parser/notebook_parser_runs/orders_daily/
  raw_orders_daily.ipynb
  cln_orders_daily.ipynb
  cfg_orders_daily.py
```

## Example Raw Notebook

Original file:

```text
Orders Daily FINAL v3.ipynb
```

Inferred semantic name:

```text
orders_daily
```

Raw copy:

```text
prompts/notebook_parser/notebook_parser_runs/orders_daily/raw_orders_daily.ipynb
```

The raw copy must be exact. No outputs, metadata or cells are changed.

## Example Clean Notebook Sections

```text
# 00 - Context and Objective
# Purpose: build daily orders dataset.

# 01 - Imports
from pyspark.sql import functions as F

# 02 - Configuration Candidates
source_table = "bronze.orders"
target_path = "/warehouse/silver/orders"

# 04 - Extract Candidates
orders_df = spark.table(source_table)

# 06 - Transform Candidates
orders_daily_df = orders_df.withColumn("order_date", F.to_date("created_at"))

# 08 - Load Candidates
orders_daily_df.write.mode("overwrite").parquet(target_path)

# 10 - Ambiguities and Manual Review
# Review overwrite scope before production.
```

## Example cfg_*.py Skeleton

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform
from etl_framework.models.context import EtlExecutionContext


class OrdersExtract(Extract):
    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        return spark.table("bronze.orders")

    def _check(self, df, spark, config, context):
        return df


class OrdersTransform(Transform):
    def _transform(self, df, spark, config, context):
        return df.withColumn("order_date", F.to_date("created_at"))

    def _validate(self, df, spark, config, context):
        return df


class OrdersLoad(Load):
    def _load(self, df, spark, config, context):
        df.write.mode(config.write_mode or "overwrite").parquet(config.target_path)

    def _certify(self, df, spark, config, context):
        target_df = spark.read.parquet(config.target_path)
        context.metrics["certified_rows"] = target_df.count()


def build_config() -> EtlRunConfig:
    return EtlRunConfig(
        pipeline_name="orders_daily",
        target_schema="silver",
        target_table="orders_daily",
        target_path="/warehouse/silver/orders",
        target_key=("order_id",),
        write_mode="overwrite",
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("orders_daily").getOrCreate()
    pipeline = Pipeline(
        spark=spark,
        config=build_config(),
        extract=OrdersExtract(),
        transform=OrdersTransform(),
        load=OrdersLoad(),
    )
    pipeline.run()
```

This skeleton is only an example. A production load should follow the safer
staging pattern in [../production_readiness.md](../production_readiness.md).

## Ambiguous Case

Notebook cell:

```python
df = spark.table("bronze.orders").filter("status = 'paid'")
df = df.withColumn("amount_tax", amount * 0.1)
df.write.mode("append").saveAsTable("silver.orders")
```

If the parser cannot separate read, transform and write with confidence, the
clean notebook must keep this in manual review. It must not silently decide the
production mapping.

## Expected Failure

Notebook cell:

```python
password = "plain_text_secret"
df.write.jdbc(url, "target", properties={"password": password})
```

Expected result:

```text
failed: hardcoded credential detected. Manual remediation required.
```
