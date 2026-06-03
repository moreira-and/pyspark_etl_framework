from __future__ import annotations

from typing import Any

ALLOWED_CHECK_SEVERITIES = frozenset({"error", "warning"})

def normalize_check_metadata(
    check: object,
    *,
    path: str,
    field_path: str,
) -> dict[str, Any]:
    """Validate and normalize one declarative check metadata entry."""
    if not isinstance(check, dict):
        raise TypeError(f"{path} must be dict")

    name = require_check_string(check, "name", path)
    rule = require_check_string(check, "rule", path)
    severity = _normalize_severity(check.get("severity"), path)
    message = _normalize_message(check.get("message"), name, field_path, path)

    return {
        "field": field_path,
        "name": name,
        "rule": rule,
        "severity": severity,
        "message": message,
    }


def _normalize_severity(severity: Any, path: str) -> str:
    """Validate and normalize check severity."""
    severity = severity or "warning"
    
    if not isinstance(severity, str):
        raise TypeError(f"{path}.severity must be string")
    
    severity = severity.lower()
    
    if severity not in ALLOWED_CHECK_SEVERITIES:
        raise ValueError(
            f"{path}.severity must be one of {sorted(ALLOWED_CHECK_SEVERITIES)}, "
            f"got '{severity}'"
        )
    
    return severity


def _normalize_message(
    message: Any,
    name: str,
    field_path: str,
    path: str,
) -> str:
    """Validate and set default message."""
    if message is not None and not isinstance(message, str):
        raise TypeError(f"{path}.message must be string or None")
    
    return message or f"Check '{name}' failed for '{field_path}'"


def require_check_string(check: dict[str, Any], field: str, path: str) -> str:
    """Return a required non-empty string from declarative check metadata."""
    value = check.get(field, "")

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}.{field} must be non-empty string")

    return value.strip()