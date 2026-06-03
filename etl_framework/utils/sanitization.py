from __future__ import annotations

import re

_REDACTED = "<redacted>"

# Compiled regex patterns for sensitive data
_URI_CREDENTIAL_RE = re.compile(
    r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^:/@\s]+):([^@/\s]+)@"
)
_BEARER_RE = re.compile(
    r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+"
)
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?i)\b("
    r"token|secret|password|passwd|pwd|senha|"
    r"connection[_ -]?string|access[_ -]?key|private[_ -]?key"
    r")\b\s*([:=])\s*(?:['\"]?)[^,\s;)}\]]+(?:['\"]?)"
)
_PAYLOAD_RE = re.compile(
    r"(?is)\b(payload|record|row_data|row)\b\s*[:=]\s*"
    r"(?:\{.*?\}|\[.*?\]|[^\s,;)]+)"
)
_PATH_RE = re.compile(
    r"(?i)\b(path|source_path|target_path|file_path)\b\s*[:=]\s*"
    r"(?:['\"][^'\"]*['\"]|[^\s,;)]+)"
)


def sanitize_error_message(message: object) -> str:
    """Return an error message safe enough for operational logs.
    
    This function redacts:
    - URI credentials (user:pass@host)
    - Bearer tokens
    - Sensitive key-value assignments (password=secret)
    - Payload data (payload={...})
    - File paths (path=/sensitive/location)
    
    The order of substitutions matters: URI credentials are processed first
    to avoid partial redaction of complex patterns.
    """
    text = str(message)
    
    # Order matters: process most specific patterns first
    text = _URI_CREDENTIAL_RE.sub(rf"\1{_REDACTED}:{_REDACTED}@", text)
    text = _BEARER_RE.sub(f"Bearer {_REDACTED}", text)
    text = _SENSITIVE_ASSIGNMENT_RE.sub(rf"\1\2{_REDACTED}", text)  # Preserves : or =
    text = _PAYLOAD_RE.sub(rf"\1={_REDACTED}", text)
    text = _PATH_RE.sub(rf"\1={_REDACTED}", text)
    
    return text


def sanitize_with_metadata(message: object, *, metadata: dict | None = None) -> str:
    """Sanitize an error message with optional metadata-driven policy.

    `metadata` may contain a `sanitize_level` key with values:
    - `full`: aggressive redaction (default behaviour)
    - `partial`: redact only credentials and bearer tokens
    - `none`: perform minimal sanitization (paths/payloads preserved)

    This is an opt-in helper: callers can pass per-StructField metadata to
    control how error messages that include data are redacted. It's a
    non-invasive sketch that preserves existing `sanitize_error_message`.
    """
    level = (metadata or {}).get("sanitize_level", "full")

    text = str(message)

    if level == "none":
        return text

    if level == "partial":
        # Partial: redact URIs and bearer tokens only
        text = _URI_CREDENTIAL_RE.sub(rf"\1{_REDACTED}:{_REDACTED}@", text)
        text = _BEARER_RE.sub(f"Bearer {_REDACTED}", text)
        return text

    # full (default)
    return sanitize_error_message(text)