from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def select_columns(df: Any, columns: Iterable[str]) -> Any:
    return df.select(*columns)
