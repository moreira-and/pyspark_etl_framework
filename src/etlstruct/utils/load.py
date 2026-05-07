from __future__ import annotations

from typing import Any


def write_parquet(df: Any, path: str, *, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(path)
