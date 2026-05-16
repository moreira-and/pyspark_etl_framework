from __future__ import annotations


class EtlError(Exception):
    """Base exception for framework-managed ETL failures.

    The framework uses this error family to make failures traceable without
    hiding the original exception. Every managed error can carry the pipeline
    name, the official stage and the original cause.
    """

    default_stage: str | None = None

    def __init__(
        self,
        message: str | None = None,
        *,
        pipeline_name: str | None = None,
        stage: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.stage = stage or self.default_stage
        self.cause = cause

        error_message = self._build_message(message)
        super().__init__(error_message)

    def _build_message(self, message: str | None) -> str:
        """Build a readable error message with execution context."""
        base_message = message or "ETL stage failed"

        details = []
        if self.pipeline_name:
            details.append(f"pipeline_name={self.pipeline_name}")
        if self.stage:
            details.append(f"stage={self.stage}")
        if self.cause:
            details.append(f"cause_type={type(self.cause).__name__}")
            cause_message = str(self.cause)
            if cause_message:
                details.append(f"cause_message={cause_message}")

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
) -> EtlError:
    """Return an EtlError that includes pipeline and stage context.

    Existing framework errors with context are preserved. Generic exceptions,
    or framework errors raised without context, are wrapped in the expected
    stage-specific error type.
    """
    if isinstance(exc, error_type) and exc.pipeline_name and exc.stage:
        return exc

    return error_type(
        message=str(exc) or None,
        pipeline_name=pipeline_name,
        cause=exc,
    )
