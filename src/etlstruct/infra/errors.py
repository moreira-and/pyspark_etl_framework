from __future__ import annotations


class EtlStructError(Exception):
    stage: str | None = None

    def __init__(self, message: str | None = None) -> None:
        if message is None:
            message = "ETL pipeline failed"
        super().__init__(message)


class StageError(EtlStructError):
    stage = "unknown"

    def __init__(self, message: str | None = None) -> None:
        if message is None:
            message = f"ETL stage failed: {self.stage}"
        super().__init__(message)


class ExtractError(StageError):
    stage = "extract"


class CheckError(StageError):
    stage = "check"


class TransformError(StageError):
    stage = "transform"


class ValidateError(StageError):
    stage = "validate"


class LoadError(StageError):
    stage = "load"
