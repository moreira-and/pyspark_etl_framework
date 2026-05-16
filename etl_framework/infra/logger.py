from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


def get_logger(name: str) -> logging.Logger:
    """Return the standard framework logger for a pipeline.

    The logger writes plain event payloads to stdout/stderr through the Python
    standard library. It intentionally avoids external logging dependencies.
    """
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
    """Write one structured event for an ETL run.

    All framework events include the pipeline name, run id and execution mode.
    Callers provide the stage, status and any additional diagnostic metadata.
    """
    payload = {
        "event": event,
        "pipeline_name": config.pipeline_name,
        "run_id": context.run_id,
        "started_at": context.started_at.isoformat(),
        "event_at": datetime.now(timezone.utc).isoformat(),
        "mode": _resolve_mode(config),
        **extra,
    }
    logger.info(payload)


def _resolve_mode(config: EtlRunConfig) -> str:
    """Resolve the execution mode shown in framework logs."""
    return "dry_run" if config.dry_run else "prod"
