from __future__ import annotations

import time
from collections.abc import Mapping
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar

from etl_framework.infra.errors import EtlError, ensure_stage_error
from etl_framework.infra.observability import (
    ObservabilityLevel,
    get_observability_service,
)
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

P = ParamSpec("P")
R = TypeVar("R")
RuntimeEventExtra = (
    Mapping[str, object]
    | Callable[
        [EtlRunConfig],
        Mapping[str, object],
    ]
)


def stage(
    stage_name: str,
    error_type: type[EtlError] | None = None,
    *,
    succeeded_event: str | None = None,
    succeeded_status: str = "succeeded",
    emit_summary: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Instrument one framework-managed ETL stage.

    The decorator intentionally does not inspect returned values. In particular,
    it must not materialize Spark DataFrames or consume generators.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            owner = args[0] if args else None
            config = _resolve_config(owner, kwargs)
            context = _resolve_context(owner, kwargs)

            started_at = time.perf_counter()
            observer = get_observability_service()
            observer.stage_started(
                stage_name,
                config,
                context,
            )

            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                error = _resolve_stage_error(
                    exc,
                    error_type,
                    config=config,
                    context=context,
                )
                elapsed_ms = _elapsed_ms_since(started_at)
                observer.stage_failed(
                    stage_name,
                    config,
                    context,
                    elapsed_ms=elapsed_ms,
                    error=error,
                )
                if emit_summary:
                    observer.runtime_event(
                        "execution_summary",
                        config,
                        context,
                        level="error",
                        error=error,
                        extra={
                            "stage": stage_name,
                            "status": "failed",
                            "elapsed_ms": elapsed_ms,
                        },
                    )
                if error_type is None or error is exc:
                    raise
                raise error from exc

            elapsed_ms = _elapsed_ms_since(started_at)
            observer.stage_succeeded(
                stage_name,
                config,
                context,
                event=succeeded_event,
                status=succeeded_status,
                elapsed_ms=elapsed_ms,
            )
            if emit_summary:
                observer.runtime_event(
                    "execution_summary",
                    config,
                    context,
                    extra={
                        "stage": stage_name,
                        "status": "succeeded",
                        "elapsed_ms": elapsed_ms,
                    },
                )
            return result

        return wrapper

    return decorator


def _resolve_stage_error(
    exc: Exception,
    error_type: type[EtlError] | None,
    *,
    config: EtlRunConfig,
    context: EtlExecutionContext,
) -> Exception | EtlError:
    if error_type is None:
        return exc

    return ensure_stage_error(
        exc,
        error_type,
        pipeline_name=config.pipeline_name,
        run_id=context.run_id,
    )


def _elapsed_ms_since(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 2)


def runtime_event(
    event: str,
    *,
    stage_name: str,
    status: str,
    extra: RuntimeEventExtra | None = None,
    level: ObservabilityLevel = "info",
    timing: str = "success",
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Emit one runtime-owned operational event around a method call."""
    if timing not in {"started", "success"}:
        raise ValueError("runtime_event timing must be 'started' or 'success'")

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            owner = args[0] if args else None
            config = _resolve_config(owner, kwargs)
            context = _resolve_context(owner, kwargs)

            if timing == "started":
                _emit_runtime_event(
                    event,
                    stage_name=stage_name,
                    status=status,
                    level=level,
                    extra=extra,
                    config=config,
                    context=context,
                )

            result = func(*args, **kwargs)

            if timing == "success":
                _emit_runtime_event(
                    event,
                    stage_name=stage_name,
                    status=status,
                    level=level,
                    extra=extra,
                    config=config,
                    context=context,
                )
            return result

        return wrapper

    return decorator


def _emit_runtime_event(
    event: str,
    *,
    stage_name: str,
    status: str,
    level: ObservabilityLevel,
    extra: RuntimeEventExtra | None,
    config: EtlRunConfig,
    context: EtlExecutionContext,
) -> None:
    get_observability_service().runtime_event(
        event,
        config,
        context,
        level=level,
        extra={
            "stage": stage_name,
            "status": status,
            **_resolve_extra(extra, config),
        },
    )


def _resolve_extra(
    extra: RuntimeEventExtra | None,
    config: EtlRunConfig,
) -> dict[str, object]:
    try:
        if extra is None:
            return {}
        if callable(extra):
            return dict(extra(config))
        return dict(extra)
    except Exception:
        return {}


def _resolve_config(owner: object, kwargs: object) -> EtlRunConfig:
    kwarg_config = _kwargs_get(kwargs, "config")
    if isinstance(kwarg_config, EtlRunConfig):
        return kwarg_config

    owner_config = getattr(owner, "config", None)
    if isinstance(owner_config, EtlRunConfig):
        return owner_config

    raise AttributeError("stage decorator requires EtlRunConfig via self or config=")


def _resolve_context(owner: object, kwargs: object) -> EtlExecutionContext:
    kwarg_context = _kwargs_get(kwargs, "context")
    if isinstance(kwarg_context, EtlExecutionContext):
        return kwarg_context

    owner_context = getattr(owner, "context", None)
    if isinstance(owner_context, EtlExecutionContext):
        return owner_context

    raise AttributeError(
        "stage decorator requires EtlExecutionContext via self or context="
    )


def _kwargs_get(kwargs: object, key: str) -> Any:
    if isinstance(kwargs, dict):
        return kwargs.get(key)
    return None
