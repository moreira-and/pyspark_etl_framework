from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(frozen=True)
class EtlExecutionContext:
    run_id: str = field(default_factory=lambda: str(uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    triggered_by: str = "manual"

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str):
            raise ValueError("run_id must be a string.")

        run_id = self.run_id.strip()
        if not run_id:
            raise ValueError("run_id must not be empty.")
        object.__setattr__(self, "run_id", run_id)

        if not isinstance(self.started_at, datetime):
            raise ValueError("started_at must be a datetime.")

        if not isinstance(self.triggered_by, str):
            raise ValueError("triggered_by must be a string.")

        triggered_by = self.triggered_by.strip()
        if not triggered_by:
            raise ValueError("triggered_by must not be empty.")
        object.__setattr__(self, "triggered_by", triggered_by)
