from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def require_columns(df: Any, columns: Iterable[str]) -> Any:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")
    return df
