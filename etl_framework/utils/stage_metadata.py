from __future__ import annotations

from etl_framework.models.config import EtlRunConfig
def dry_run_extract_metadata(
    config: EtlRunConfig,
) -> dict[str, object]:
    return {"dry_run_limit": config.dry_run_limit}


def dry_run_sample_metadata(
    config: EtlRunConfig,
) -> dict[str, object]:
    return {"dry_run_show_rows": config.dry_run_show_rows}


def dry_run_evidence_metadata(
    config: EtlRunConfig,
) -> dict[str, object]:
    return {
        "dry_run": True,
        "dry_run_limit": config.dry_run_limit,
        "dry_run_show_rows": config.dry_run_show_rows,
    }
