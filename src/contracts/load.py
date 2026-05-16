from __future__ import annotations

from abc import ABC, abstractmethod

from etl_framework.infra.errors import CertifyError, EtlError, LoadError
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from pyspark.sql import DataFrame, SparkSession


class Load(ABC):
    def run(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        try:
            self._load(df, spark, config, context)
        except EtlError:
            raise
        except Exception as exc:
            raise LoadError() from exc

        try:
            self._certify(df, spark, config, context)
        except EtlError:
            raise
        except Exception as exc:
            raise CertifyError() from exc

    @abstractmethod
    def _load(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def _certify(
        self,
        df: DataFrame,
        spark: SparkSession,
        config: EtlRunConfig,
        context: EtlExecutionContext,
    ) -> None:
        raise NotImplementedError
