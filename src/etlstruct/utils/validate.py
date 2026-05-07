from __future__ import annotations

from typing import Any


def require_non_empty(df: Any) -> Any:
    if df.limit(1).count() == 0:
        raise ValueError("DataFrame cannot be empty")
    return df
