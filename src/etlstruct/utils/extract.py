from __future__ import annotations

from typing import Any


def limit_rows(df: Any, rows: int) -> Any:
    return df.limit(rows)
