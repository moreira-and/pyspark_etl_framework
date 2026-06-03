from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass(slots=True)
class EtlExecutionContext:
    """Trace metadata for one framework-managed ETL execution.

    This object is intentionally small. It represents execution traceability,
    not a generic state bag for pipeline logic. Concrete pipelines should use
    it only to read run metadata such as the run id and start timestamp.
    
    Pipelines may publish explicitly computed operational metrics here via
    direct assignment (e.g., context.metrics["rows_processed"] = 1000).
    The framework only logs these values and never computes Spark actions for them.
    """

    run_id: str = field(default_factory=lambda: str(uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metrics: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate minimum traceability fields for one execution."""
        if not self.run_id or not self.run_id.strip():
            raise ValueError("run_id must be a non-empty string")