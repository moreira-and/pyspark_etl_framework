from __future__ import annotations

import logging
from typing import Any

from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    event: str,
    config: EtlRunConfig,
    context: EtlExecutionContext,
    **extra: Any,
) -> None:
    payload = {
        "event": event,
        "pipeline_name": config.pipeline_name,
        "run_id": context.run_id,
        "mode": _resolve_mode(config),
        **extra,
    }
    logger.info(payload)


def _resolve_mode(config: EtlRunConfig) -> str:
    return "dry_run" if config.dry_run else "prod"
