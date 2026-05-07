from __future__ import annotations

from pathlib import Path

import pytest

from etlstruct import EtlRunConfig
from pipelines.pipeline_example import ExampleCustomerPipeline

pyspark = pytest.importorskip("pyspark.sql")
errors = pytest.importorskip("pyspark.errors")
SparkSession = pyspark.SparkSession
PySparkRuntimeError = errors.PySparkRuntimeError


@pytest.fixture(scope="session")
def spark():
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("spark-etl-framework-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except PySparkRuntimeError as exc:
        pytest.skip(f"Spark local session is unavailable: {exc}")

    yield session
    session.stop()


def test_example_customer_pipeline_reads_transforms_and_writes_parquet(
    spark,
    tmp_path,
) -> None:
    input_csv = Path(__file__).parents[1] / "data" / "customer_input.csv"
    source_path = tmp_path / "source_customers"
    target_path = tmp_path / "target_customers"

    (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_csv)
        .write.mode("overwrite")
        .parquet(str(source_path))
    )

    pipeline = ExampleCustomerPipeline(
        spark=spark,
        source_path=str(source_path),
        target_path=str(target_path),
        config=EtlRunConfig(pipeline_name="example_customer_etl"),
    )

    result = pipeline.run()
    written = spark.read.parquet(str(target_path))

    assert result.count() == 3
    assert written.count() == 3
    assert set(written.columns) == {"customer_id", "name"}
    assert {row.customer_id for row in written.select("customer_id").collect()} == {
        1,
        2,
        3,
    }
