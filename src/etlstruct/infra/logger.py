from __future__ import annotations

import functools
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Concatenate, ParamSpec, TypeVar, cast

from etlstruct.models.config import EtlRunConfig
from etlstruct.models.context import EtlExecutionContext

P = ParamSpec("P")
R = TypeVar("R")


def get_logger(name: str = "etlstruct") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    event: str,
    config: EtlRunConfig,
    context: EtlExecutionContext,
    *,
    stage: str,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    payload = {
        "event": event,
        "pipeline_name": config.pipeline_name,
        "run_id": context.run_id,
        "stage": stage,
        "started_at": context.started_at.isoformat(),
        "mode": config.mode.value,
        "triggered_by": context.triggered_by,
        **fields,
    }
    logger.log(
        level, json.dumps(payload, default=str, ensure_ascii=True, sort_keys=True)
    )


def logged_stage(
    stage: str,
) -> Callable[
    [Callable[Concatenate[Any, P], R]],
    Callable[Concatenate[Any, P], R],
]:
    def decorator(
        func: Callable[Concatenate[Any, P], R],
    ) -> Callable[Concatenate[Any, P], R]:
        @functools.wraps(func)
        def wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> R:
            stage_started_at = datetime.now(timezone.utc)
            started = time.perf_counter()
            log_event(
                self.logger,
                "stage_started",
                self.config,
                self.context,
                stage=stage,
                status="started",
                stage_started_at=stage_started_at.isoformat(),
            )
            try:
                result = func(self, *args, **kwargs)
            except Exception as exc:
                stage_finished_at = datetime.now(timezone.utc)
                duration = time.perf_counter() - started
                log_event(
                    self.logger,
                    "stage_failed",
                    self.config,
                    self.context,
                    stage=stage,
                    level=logging.ERROR,
                    status="failed",
                    stage_started_at=stage_started_at.isoformat(),
                    stage_finished_at=stage_finished_at.isoformat(),
                    duration_seconds=round(duration, 6),
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                )
                raise

            stage_finished_at = datetime.now(timezone.utc)
            duration = time.perf_counter() - started
            log_event(
                self.logger,
                "stage_finished",
                self.config,
                self.context,
                stage=stage,
                status="success",
                stage_started_at=stage_started_at.isoformat(),
                stage_finished_at=stage_finished_at.isoformat(),
                duration_seconds=round(duration, 6),
            )
            return result

        return cast(Callable[Concatenate[Any, P], R], wrapper)

    return decorator
