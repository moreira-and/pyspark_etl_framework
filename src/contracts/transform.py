from __future__ import annotations

from abc import ABC, abstractmethod

from etl_framework.infra.errors import EtlError, TransformError, ValidateError
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from pyspark.sql import DataFrame, SparkSession


class Transform(ABC):
    def run(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        try:
            df = self._transform(df, spark, config, context)
        except EtlError:
            raise
        except Exception as exc:
            raise TransformError() from exc

        try:
            return self._validate(df, spark, config, context)
        except EtlError:
            raise
        except Exception as exc:
            raise ValidateError() from exc

    @abstractmethod
    def _transform(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _validate(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> DataFrame:
        raise NotImplementedError
