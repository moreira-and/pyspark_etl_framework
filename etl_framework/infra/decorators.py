from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar

from etl_framework.infra.errors import EtlError, ensure_stage_error
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger(__name__)


def stage(
    stage_name: str,
    error_type: type[EtlError] | None = None,
    **_ignored,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Minimal stage decorator: timing, logging and optional error wrapping.

    Accepts legacy kwargs (ignored) for compatibility (e.g. `emit_summary`).
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            config, context = _resolve_dependencies(args, kwargs)

            logger.info(
                "%s started",
                stage_name,
                extra={
                    "event": f"{stage_name}_started",
                    "pipeline_name": config.pipeline_name,
                    "run_id": context.run_id,
                    "stage": stage_name,
                    "status": "started",
                },
            )

            started_at = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)

                logger.info(
                    "%s succeeded in %.2fms",
                    stage_name,
                    elapsed_ms,
                    extra={
                        "event": f"{stage_name}_succeeded",
                        "pipeline_name": config.pipeline_name,
                        "run_id": context.run_id,
                        "stage": stage_name,
                        "status": "succeeded",
                        "elapsed_ms": elapsed_ms,
                    },
                )
                return result

            except Exception as exc:
                elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
                error = _convert_error(exc, error_type, config, context)

                logger.error(
                    "%s failed in %.2fms: %s",
                    stage_name,
                    elapsed_ms,
                    error,
                    extra={
                        "event": f"{stage_name}_failed",
                        "pipeline_name": config.pipeline_name,
                        "run_id": context.run_id,
                        "stage": stage_name,
                        "status": "failed",
                        "elapsed_ms": elapsed_ms,
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                    },
                    exc_info=True,
                )
                raise error from exc

        return wrapper

    return decorator


def runtime_event(
    event: str,
    *,
    stage_name: str | None = None,
    status: str | None = None,
    extra: Any | None = None,
    timing: str = "succeeded",
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Emit a lightweight runtime event around a call.

    `extra` may be a callable `lambda config, context: {...}` or a dict. Exceptions
    from evaluating a callable extra are swallowed and an empty dict used.
    """

    def _resolve_extra(config: EtlRunConfig, context: EtlExecutionContext) -> dict[str, object]:
        if extra is None:
            return {}
        try:
            if callable(extra):
                return dict(extra(config, context) or {})
            if isinstance(extra, dict):
                return extra
        except Exception:
            logger.warning("runtime_event extra callable failed; ignoring extras", exc_info=True)
        return {}

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            config, context = _resolve_dependencies(args, kwargs)
            extras = _resolve_extra(config, context)

            def emit(phase: str) -> None:
                payload = {
                    "event": event,
                    "pipeline_name": config.pipeline_name,
                    "run_id": context.run_id,
                    "stage": stage_name,
                    "status": status,
                    "phase": phase,
                    **extras,
                }
                logger.info("%s %s", event, phase, extra=payload)

            if timing == "started":
                emit("started")

            result = func(*args, **kwargs)

            if timing == "succeeded":
                emit("succeeded")

            return result

        return wrapper

    return decorator


def _resolve_dependencies(args, kwargs) -> tuple[EtlRunConfig, EtlExecutionContext]:
    owner = args[0] if args else None

    config = kwargs.get("config") or getattr(owner, "config", None)
    if not isinstance(config, EtlRunConfig):
        raise AttributeError("@stage/@runtime_event requires EtlRunConfig via self.config or config=")

    context = kwargs.get("context") or getattr(owner, "context", None)
    if not isinstance(context, EtlExecutionContext):
        raise AttributeError("@stage/@runtime_event requires EtlExecutionContext via self.context or context=")

    return config, context


def _convert_error(
    exc: Exception,
    error_type: type[EtlError] | None,
    config: EtlRunConfig,
    context: EtlExecutionContext,
) -> Exception:
    if error_type is None:
        return exc

    return ensure_stage_error(
        exc,
        error_type,
        pipeline_name=config.pipeline_name,
        run_id=context.run_id,
    )
