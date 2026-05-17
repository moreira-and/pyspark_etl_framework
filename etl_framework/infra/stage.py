from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar

from etl_framework.infra.errors import EtlError, ensure_stage_error
from etl_framework.infra.logger import get_logger, log_event
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

P = ParamSpec("P")
R = TypeVar("R")


def stage(
    stage_name: str,
    error_type: type[EtlError],
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Log and wrap one framework-managed ETL stage.

    The decorator intentionally does not inspect returned values. In particular,
    it must not materialize Spark DataFrames or consume generators.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            owner = args[0] if args else None
            config = _resolve_config(owner, kwargs)
            context = _resolve_context(owner, kwargs)
            logger = _resolve_logger(owner, kwargs, config)

            started_at = time.perf_counter()
            log_event(
                logger,
                f"{stage_name}_started",
                config,
                context,
                stage=stage_name,
                status="started",
            )

            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                error = ensure_stage_error(
                    exc,
                    error_type,
                    pipeline_name=config.pipeline_name,
                    run_id=context.run_id,
                )
                elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
                log_event(
                    logger,
                    f"{stage_name}_failed",
                    config,
                    context,
                    stage=stage_name,
                    status="failed",
                    elapsed_ms=elapsed_ms,
                    error_type=type(error).__name__,
                    error_message=str(error),
                )
                if error is exc:
                    raise
                raise error from exc

            elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_event(
                logger,
                f"{stage_name}_succeeded",
                config,
                context,
                stage=stage_name,
                status="succeeded",
                elapsed_ms=elapsed_ms,
            )
            return result

        return wrapper

    return decorator


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


def _resolve_logger(
    owner: object,
    kwargs: object,
    config: EtlRunConfig,
) -> logging.Logger:
    kwarg_logger = _kwargs_get(kwargs, "logger")
    if isinstance(kwarg_logger, logging.Logger):
        return kwarg_logger

    owner_logger = getattr(owner, "logger", None)
    if isinstance(owner_logger, logging.Logger):
        return owner_logger

    return get_logger(config.pipeline_name)


def _kwargs_get(kwargs: object, key: str) -> Any:
    if isinstance(kwargs, dict):
        return kwargs.get(key)
    return None
