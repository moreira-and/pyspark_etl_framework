from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ExecutionMode(StrEnum):
    FULL = "full"
    DRY_RUN = "dry_run"


@dataclass(frozen=True)
class EtlRunConfig:
    pipeline_name: str
    mode: ExecutionMode = ExecutionMode.FULL
    dry_run_limit: int = 100
    dry_run_show_rows: int = 20

    def __post_init__(self) -> None:
        if not isinstance(self.pipeline_name, str):
            raise ValueError("pipeline_name must be a string.")

        pipeline_name = self.pipeline_name.strip()
        if not pipeline_name:
            raise ValueError("pipeline_name must not be empty.")
        object.__setattr__(self, "pipeline_name", pipeline_name)

        if not isinstance(self.mode, ExecutionMode):
            raise ValueError("mode must be an ExecutionMode.")

        if type(self.dry_run_limit) is not int:
            raise ValueError("dry_run_limit must be an integer.")

        if self.dry_run_limit <= 0:
            raise ValueError("dry_run_limit must be greater than zero.")

        if type(self.dry_run_show_rows) is not int:
            raise ValueError("dry_run_show_rows must be an integer.")

        if self.dry_run_show_rows <= 0:
            raise ValueError("dry_run_show_rows must be greater than zero.")

    @property
    def dry_run(self) -> bool:
        return self.mode == ExecutionMode.DRY_RUN
