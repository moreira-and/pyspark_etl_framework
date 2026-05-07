from etlstruct.models.config import EtlRunConfig, ExecutionMode
from etlstruct.models.context import EtlExecutionContext
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from etlstruct.models.contracts import EtlStruct

__all__ = [
    "EtlExecutionContext",
    "EtlRunConfig",
    "EtlStruct",
    "ExecutionMode",
]


def __getattr__(name: str):
    if name == "EtlStruct":
        from etlstruct.models.contracts import EtlStruct

        return EtlStruct
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
