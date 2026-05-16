from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(slots=True)
class EtlExecutionContext:
    """Trace metadata for one framework-managed ETL execution.

    This object is intentionally small. It represents execution traceability,
    not a generic state bag for pipeline logic. Concrete pipelines should use
    it only to read run metadata such as the run id and start timestamp.
    """

    run_id: str = field(default_factory=lambda: str(uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
