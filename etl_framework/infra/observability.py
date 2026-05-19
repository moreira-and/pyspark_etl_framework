from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from typing import Literal, Protocol

from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.observability_events import build_observability_event

ObservabilityLevel = Literal["info", "warning", "error"]
_LOGGING_LEVELS = {
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class ObservabilitySink(Protocol):
    """Minimal port for structured runtime event delivery."""

    def emit(self, event: Mapping[str, object]) -> None:
        """Emit one structured operational event."""
        ...


class NoOpObservabilitySink:
    """Fallback sink that intentionally drops structured operational events."""

    def emit(self, event: Mapping[str, object]) -> None:
        """Drop the event without side effects."""
        return None


class ObservabilityService:
    """Central runtime-owned structured observability service.

    Runtime observability must never make an ETL fail in v0.1. Sinks are
    therefore treated as best-effort outputs: failures are swallowed by default.
    """

    def __init__(self, sink: ObservabilitySink | None = None) -> None:
        self._sink: ObservabilitySink = sink or NoOpObservabilitySink()

    @property
    def sink(self) -> ObservabilitySink:
        """Return the current structured observability sink."""
        return self._sink

    def set_sink(self, sink: ObservabilitySink | None) -> None:
        """Replace the structured sink used by the framework runtime."""
        self._sink = sink or NoOpObservabilitySink()

    def emit(
        self,
        event: Mapping[str, object],
        *,
        level: ObservabilityLevel = "info",
    ) -> None:
        """Emit one structured event without letting sink failures escape."""
        try:
            payload = dict(event)
            payload.setdefault("level", level)
            self._sink.emit(payload)
        except Exception:
            return None

    def info(
        self,
        event: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit one info-level runtime event."""
        self._emit_event(event, config, context, level="info", extra=extra)

    def warning(
        self,
        event: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit one warning-level runtime event."""
        self._emit_event(event, config, context, level="warning", extra=extra)

    def error(
        self,
        event: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit one error-level runtime event."""
        self._emit_event(event, config, context, level="error", extra=extra)

    def exception(
        self,
        event: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        error: BaseException,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit one error-level runtime event with exception metadata."""
        payload = dict(extra or {})
        payload.setdefault("error_type", type(error).__name__)
        payload.setdefault("error_message", str(error))
        self._emit_event(event, config, context, level="error", extra=payload)

    def runtime_event(
        self,
        event: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        level: ObservabilityLevel = "info",
        error: BaseException | None = None,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit a runtime event at an explicit level."""
        if error is not None:
            self.exception(event, config, context, error=error, extra=extra)
            return
        self._emit_event(event, config, context, level=level, extra=extra)

    def stage_started(
        self,
        stage: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit a stage start event."""
        self.info(
            f"{stage}_started",
            config,
            context,
            extra=_stage_payload(stage, "started", extra=extra),
        )

    def stage_succeeded(
        self,
        stage: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        event: str | None = None,
        status: str = "succeeded",
        elapsed_ms: float | None = None,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit a stage success event."""
        self.info(
            event or f"{stage}_succeeded",
            config,
            context,
            extra=_stage_payload(
                stage,
                status,
                elapsed_ms=elapsed_ms,
                include_elapsed_ms=True,
                extra=extra,
            ),
        )

    def stage_warning(
        self,
        event: str,
        stage: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        status: str = "warning",
        elapsed_ms: float | None = None,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit a warning-level stage event."""
        self.warning(
            event,
            config,
            context,
            extra=_stage_payload(
                stage,
                status,
                elapsed_ms=elapsed_ms,
                include_elapsed_ms=True,
                extra=extra,
            ),
        )

    def stage_failed(
        self,
        stage: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        error: BaseException,
        elapsed_ms: float | None = None,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Emit a stage failure event."""
        self.exception(
            f"{stage}_failed",
            config,
            context,
            error=error,
            extra=_stage_payload(
                stage,
                "failed",
                elapsed_ms=elapsed_ms,
                include_elapsed_ms=True,
                extra=extra,
            ),
        )

    def _emit_event(
        self,
        event: str,
        config: EtlRunConfig,
        context: EtlExecutionContext,
        *,
        level: ObservabilityLevel,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        payload = build_observability_event(
            event,
            config,
            context,
            level=level,
            **dict(extra or {}),
        )
        self.emit(payload, level=level)


def _stage_payload(
    stage: str,
    status: str,
    *,
    elapsed_ms: float | None = None,
    include_elapsed_ms: bool = False,
    extra: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {"stage": stage, "status": status}
    if include_elapsed_ms:
        payload["elapsed_ms"] = elapsed_ms
    payload.update(dict(extra or {}))
    return payload


class StdoutObservabilitySink:
    """Default vendor-agnostic sink based on Python's standard logging."""

    def emit(self, event: Mapping[str, object]) -> None:
        """Emit through a pipeline-named standard-library backend."""
        pipeline_name = event.get("pipeline_name")
        logger_name = (
            pipeline_name if isinstance(pipeline_name, str) else "etl_framework"
        )
        level = _resolve_logging_level(event.get("level"))
        _get_standard_logger(logger_name).log(level, dict(event))


_OBSERVABILITY_SERVICE = ObservabilityService(StdoutObservabilitySink())


def _get_standard_logger(name: str) -> logging.Logger:
    """Return the standard-library backend used by the default sink."""
    logger = logging.getLogger(name)

    if logger.level == logging.NOTSET:
        logger.setLevel(logging.INFO)

    if logger.handlers or logging.getLogger().handlers:
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(_default_formatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def _default_formatter() -> logging.Formatter:
    return logging.Formatter("%(message)s")


def _resolve_logging_level(level: object) -> int:
    if isinstance(level, str):
        return _LOGGING_LEVELS.get(level.lower(), logging.INFO)
    return logging.INFO


def get_observability_service() -> ObservabilityService:
    """Return the process-level service used by stage decorators."""
    return _OBSERVABILITY_SERVICE


def configure_observability_sink(sink: ObservabilitySink | None) -> None:
    """Replace the runtime sink without changing contracts or pipelines."""
    _OBSERVABILITY_SERVICE.set_sink(sink)


def reset_observability_sink() -> None:
    """Restore the default vendor-agnostic stdout sink."""
    _OBSERVABILITY_SERVICE.set_sink(StdoutObservabilitySink())


def configure_observability_from_env() -> None:
    """Configure Python logging explicitly from environment variables.

    The framework does not load `.env` files and does not call this function on
    import. Applications that want `.env` support should load it before calling
    this function.
    """
    level_name = os.getenv("ETL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    log_format = os.getenv(
        "ETL_LOG_FORMAT",
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    kwargs: dict[str, object] = {
        "level": level,
        "format": log_format,
        "force": False,
    }
    if _env_flag_enabled("ETL_LOG_TO_STDOUT", default=True):
        kwargs["handlers"] = [logging.StreamHandler(sys.stdout)]

    logging.basicConfig(**kwargs)


def _env_flag_enabled(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}
