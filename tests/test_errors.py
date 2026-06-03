from etl_framework.infra.errors import ensure_stage_error, EtlError, ExtractError
import pytest


def test_ensure_stage_error_requires_run_id():
    with pytest.raises(ValueError):
        ensure_stage_error(ValueError("boom"), ExtractError, pipeline_name="p", run_id=None)


def test_ensure_stage_error_wraps_exception_with_run_id():
    err = ensure_stage_error(ValueError("boom"), ExtractError, pipeline_name="p", run_id="run-1")
    assert isinstance(err, EtlError)
    assert err.pipeline_name == "p"
    assert err.run_id == "run-1"
    assert isinstance(err.cause, Exception)
