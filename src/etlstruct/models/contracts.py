from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from etlstruct.infra.errors import (CheckError, EtlStructError, ExtractError,
                                    LoadError, TransformError, ValidateError)
from etlstruct.infra.logger import get_logger, log_event, logged_stage
from etlstruct.models.config import EtlRunConfig
from etlstruct.models.context import EtlExecutionContext


class EtlStruct(ABC):
    """Base contract for Spark ETL pipelines."""

    _reserved_public_methods: ClassVar[tuple[str, ...]] = (
        "extract",
        "check",
        "transform",
        "validate",
        "load",
        "run",
    )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        overridden = [
            method for method in cls._reserved_public_methods if method in cls.__dict__
        ]
        if overridden:
            names = ", ".join(overridden)
            raise TypeError(
                f"{cls.__name__} cannot override framework-controlled methods: {names}. "
                "Implement the protected hooks instead."
            )

    def __init__(
        self,
        config: EtlRunConfig,
        *,
        context: EtlExecutionContext | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if not isinstance(config, EtlRunConfig):
            raise TypeError("config must be an EtlRunConfig instance.")

        if context is not None and not isinstance(context, EtlExecutionContext):
            raise TypeError("context must be an EtlExecutionContext instance.")

        self.config = config
        self.context = context or EtlExecutionContext()
        self.logger = logger or get_logger(self.config.pipeline_name)

    @logged_stage("run")
    def run(self) -> Any:
        df = self.extract()
        df = self.check(df)
        df = self.transform(df)
        df = self.validate(df)
        self.load(df)
        return df

    @logged_stage("extract")
    def extract(self) -> Any:
        try:
            df = self._extract()
            return self._apply_dry_run_limit(df)
        except EtlStructError:
            raise
        except Exception as exc:
            raise ExtractError() from exc

    @logged_stage("check")
    def check(self, df: Any) -> Any:
        try:
            return self._check(df)
        except EtlStructError:
            raise
        except Exception as exc:
            raise CheckError() from exc

    @logged_stage("transform")
    def transform(self, df: Any) -> Any:
        try:
            return self._transform(df)
        except EtlStructError:
            raise
        except Exception as exc:
            raise TransformError() from exc

    @logged_stage("validate")
    def validate(self, df: Any) -> Any:
        try:
            return self._validate(df)
        except EtlStructError:
            raise
        except Exception as exc:
            raise ValidateError() from exc

    @logged_stage("load")
    def load(self, df: Any) -> None:
        try:
            if self.config.dry_run:
                log_event(
                    self.logger,
                    "dry_run_load_skipped",
                    self.config,
                    self.context,
                    stage="load",
                    status="skipped",
                    dry_run_show_rows=self.config.dry_run_show_rows,
                )
                self._analyze_dry_run_result(df)
                return None
            self._load(df)
            return None
        except EtlStructError:
            raise
        except Exception as exc:
            raise LoadError() from exc

    def _apply_dry_run_limit(self, df: Any) -> Any:
        if not self.config.dry_run:
            return df

        limit = getattr(df, "limit", None)
        if callable(limit):
            log_event(
                self.logger,
                "dry_run_extract_limited",
                self.config,
                self.context,
                stage="extract",
                status="limited",
                dry_run_limit=self.config.dry_run_limit,
            )
            return limit(self.config.dry_run_limit)

        log_event(
            self.logger,
            "dry_run_extract_limit_unavailable",
            self.config,
            self.context,
            stage="extract",
            status="not_limited",
            object_type=type(df).__name__,
        )
        return df

    def _analyze_dry_run_result(self, df: Any) -> None:
        show = getattr(df, "show", None)
        if callable(show):
            show(self.config.dry_run_show_rows, truncate=False)
            return

        log_event(
            self.logger,
            "dry_run_result_show_unavailable",
            self.config,
            self.context,
            stage="load",
            status="not_analyzed",
            object_type=type(df).__name__,
        )

    @abstractmethod
    def _extract(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _check(self, df: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _transform(self, df: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _validate(self, df: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _load(self, df: Any) -> None:
        raise NotImplementedError
