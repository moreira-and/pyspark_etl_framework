from __future__ import annotations

import re

_REDACTED = "<redacted>"
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?i)\b("
    r"token|secret|password|passwd|pwd|senha|connection[_ -]?string|"
    r"access[_ -]?key|private[_ -]?key"
    r")\b\s*[:=]\s*(['\"]?)[^,\s;)}\]]+(['\"]?)"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_URI_CREDENTIAL_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^:/@\s]+):([^@/\s]+)@")
_PAYLOAD_RE = re.compile(
    r"(?is)\b(payload|record|row_data|row)\b\s*[:=]\s*(\{.*?\}|\[.*?\]|[^,;]+)"
)
_PATH_RE = re.compile(
    r"(?i)\b(path|source_path|target_path|file_path)\b\s*[:=]\s*([^,;\s)]+)"
)


def sanitize_error_message(message: object) -> str:
    """Return an error message safe enough for operational logs.

    The sanitizer is intentionally small and conservative. It targets common
    secret and payload shapes seen in exception messages while preserving the
    stage, pipeline and run context that operators need for debugging.
    """
    text = str(message)
    text = _URI_CREDENTIAL_RE.sub(rf"\1{_REDACTED}:{_REDACTED}@", text)
    text = _BEARER_RE.sub(f"Bearer {_REDACTED}", text)
    text = _SENSITIVE_ASSIGNMENT_RE.sub(rf"\1={_REDACTED}", text)
    text = _PAYLOAD_RE.sub(rf"\1={_REDACTED}", text)
    text = _PATH_RE.sub(rf"\1={_REDACTED}", text)
    return text


class EtlError(Exception):
    """Base exception for framework-managed ETL failures.

    The framework uses this error family to make failures traceable without
    hiding the original exception. Every managed error can carry the pipeline
    name, run id, official stage and original cause.
    """

    default_stage: str | None = None

    def __init__(
        self,
        message: str | None = None,
        *,
        pipeline_name: str | None = None,
        stage: str | None = None,
        run_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.stage = stage or self.default_stage
        self.run_id = run_id
        self.cause = cause

        error_message = self._build_message(message)
        super().__init__(error_message)

    def _build_message(self, message: str | None) -> str:
        """Build a readable error message with execution context."""
        base_message = sanitize_error_message(message or "ETL stage failed")

        details = []
        if self.pipeline_name:
            details.append(f"pipeline_name={self.pipeline_name}")
        if self.run_id:
            details.append(f"run_id={self.run_id}")
        if self.stage:
            details.append(f"stage={self.stage}")
        if self.cause:
            details.append(f"cause_type={type(self.cause).__name__}")
            cause_message = str(self.cause)
            if cause_message:
                details.append(f"cause_message={sanitize_error_message(cause_message)}")

        if not details:
            return base_message

        return f"{base_message} ({', '.join(details)})"


class ExtractError(EtlError):
    """Raised when the official extract stage fails."""

    default_stage = "extract"


class CheckError(EtlError):
    """Raised when the official check stage fails."""

    default_stage = "check"


class TransformError(EtlError):
    """Raised when the official transform stage fails."""

    default_stage = "transform"


class ValidateError(EtlError):
    """Raised when the official validate stage fails."""

    default_stage = "validate"


class LoadError(EtlError):
    """Raised when the official load stage fails."""

    default_stage = "load"


class CertifyError(EtlError):
    """Raised when the official certify stage fails."""

    default_stage = "certify"


def ensure_stage_error(
    exc: Exception,
    error_type: type[EtlError],
    *,
    pipeline_name: str,
    run_id: str | None = None,
) -> EtlError:
    """Return an EtlError that includes pipeline, run and stage context.

    Existing framework errors with context are preserved. Generic exceptions,
    or framework errors raised without context, are wrapped in the expected
    stage-specific error type.
    """
    if isinstance(exc, EtlError):
        has_trace_context = exc.pipeline_name and exc.stage
        has_run_context = run_id is None or exc.run_id
        if has_trace_context and has_run_context:
            return exc

        return type(exc)(
            message=str(exc) or None,
            pipeline_name=exc.pipeline_name or pipeline_name,
            stage=exc.stage,
            run_id=exc.run_id or run_id,
            cause=exc.cause or exc,
        )

    return error_type(
        message=str(exc) or None,
        pipeline_name=pipeline_name,
        run_id=run_id,
        cause=exc,
    )
