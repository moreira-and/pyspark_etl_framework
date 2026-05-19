from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from typing import Any, cast

import pytest
from pyspark.sql import SparkSession

from etl_framework.infra.observability import reset_observability_sink


@pytest.fixture(autouse=True)
def runtime_log_sink() -> Iterator[None]:
    reset_observability_sink()
    yield
    reset_observability_sink()


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = cast(Any, SparkSession.builder)
    session = (
        builder.master("local[1]")
        .appName("spark-etl-framework-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()
