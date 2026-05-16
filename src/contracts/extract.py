from __future__ import annotations

from abc import ABC, abstractmethod

from etl_framework.infra.errors import CheckError, EtlError, ExtractError
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from pyspark.sql import DataFrame, SparkSession


class Extract(ABC):
    def run(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        try:
            df = self._extract(spark, config, context)
        except EtlError:
            raise
        except Exception as exc:
            raise ExtractError() from exc

        try:
            return self._check(df, spark, config, context)
        except EtlError:
            raise
        except Exception as exc:
            raise CheckError() from exc

    @abstractmethod
    def _extract(
        self,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _check(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        raise NotImplementedError
