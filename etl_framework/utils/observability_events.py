from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext
from etl_framework.utils.sanitization import sanitize_error_message

_METRIC_SCALAR_TYPES = (str, int, float, bool, type(None))
_SENSITIVE_EXTRA_FIELDS = {"error", "error_message", "exception", "cause_message"}
EVENT_SCHEMA_VERSION = "1.0"


def build_observability_event(
    event: str,
    config: EtlRunConfig,
    context: EtlExecutionContext,
    **extra: Any,
) -> dict[str, object]:
    """Build one structured event payload for an ETL run."""
    extra_payload = dict(extra)
    metrics = _serializable_metrics(context.metrics)
    if "metrics" in extra_payload:
        metrics.update(_serializable_metrics(extra_payload.pop("metrics")))

    payload = {
        "event_schema_version": EVENT_SCHEMA_VERSION,
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
    return _sanitize_event_payload(payload)


def _resolve_mode(config: EtlRunConfig) -> str:
    """Resolve the execution mode shown in framework observability events."""
    return "dry_run" if config.dry_run else "prod"


def _serializable_metrics(metrics: object) -> dict[str, object]:
    """Return a small JSON-serializable metrics mapping for event payloads."""
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


def _sanitize_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Sanitize known error fields without hiding normal operational metadata."""
    sanitized = dict(payload)
    for field in _SENSITIVE_EXTRA_FIELDS:
        if field in sanitized and sanitized[field] is not None:
            sanitized[field] = sanitize_error_message(sanitized[field])
    return sanitized
