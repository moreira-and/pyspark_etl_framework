from etl_framework.models.config import EtlRunConfig
import pytest


def test_extra_columns_policy_none_raises():
    with pytest.raises(ValueError):
        EtlRunConfig(
            pipeline_name="p",
            target_schema="s",
            target_table="t",
            target_path="/tmp",
            extra_columns_policy=None,
        )


def test_extra_columns_policy_normalization():
    cfg = EtlRunConfig(
        pipeline_name="p",
        target_schema="s",
        target_table="t",
        target_path="/tmp",
        extra_columns_policy=" WARN ",
    )
    assert cfg.extra_columns_policy == "warn"
