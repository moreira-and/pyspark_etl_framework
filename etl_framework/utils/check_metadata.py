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
    severity = check.get("severity") or "warning"

    if not isinstance(severity, str):
        raise TypeError(f"{path}.severity must be string")

    if severity not in ALLOWED_CHECK_SEVERITIES:
        raise ValueError(
            f"{path}.severity must be one of {sorted(ALLOWED_CHECK_SEVERITIES)}"
        )

    message = check.get("message") or f"Check '{name}' failed for '{field_path}'"
    if not isinstance(message, str):
        raise TypeError(f"{path}.message must be string")

    return {
        "field": field_path,
        "name": name,
        "rule": rule,
        "severity": severity,
        "message": message,
    }


def require_check_string(check: dict[str, Any], field: str, path: str) -> str:
    """Return a required non-empty string from declarative check metadata."""
    value = check.get(field, "")

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}.{field} must be non-empty string")

    return value.strip()
