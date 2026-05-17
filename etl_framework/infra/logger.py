from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext

_METRIC_SCALAR_TYPES = (str, int, float, bool, type(None))


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
    extra_payload = dict(extra)
    metrics = _serializable_metrics(context.metrics)
    if "metrics" in extra_payload:
        metrics.update(_serializable_metrics(extra_payload.pop("metrics")))

    payload = {
        "event": event,
        "pipeline_name": config.pipeline_name,
        "run_id": context.run_id,
        "started_at": context.started_at.isoformat(),
        "event_at": datetime.now(timezone.utc).isoformat(),
        "mode": _resolve_mode(config),
        "target_schema": config.target_schema,
        "target_table": config.target_table,
        "target_path": config.target_path,
        "target": config.full_target_table_name,
        "write_mode": config.write_mode,
        **extra_payload,
    }
    if metrics:
        payload["metrics"] = metrics
    logger.info(payload)


def _resolve_mode(config: EtlRunConfig) -> str:
    """Resolve the execution mode shown in framework logs."""
    return "dry_run" if config.dry_run else "prod"


def _serializable_metrics(metrics: object) -> dict[str, object]:
    """Return a small JSON-serializable metrics mapping for log payloads."""
    if not isinstance(metrics, dict):
        return {}

    serializable: dict[str, object] = {}
    for key, value in metrics.items():
        if not isinstance(key, str) or not key.strip():
            continue
        serializable[key] = (
            value if isinstance(value, _METRIC_SCALAR_TYPES) else str(value)
        )
    return serializable
