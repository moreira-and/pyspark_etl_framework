import pytest
from etl_framework.utils import production_checks as pc


def test_assert_volume_between_requires_opt_in():
    df = None
    with pytest.raises(RuntimeError):
        pc.assert_volume_between(df, min_rows=1)


def test_assert_volume_between_opt_in_with_invalid_df():
    class Dummy:
        def count(self):
            return 0

    df = Dummy()
    with pytest.raises(ValueError):
        pc.assert_volume_between(df, min_rows=1, allow_expensive=True)


def test_assert_target_key_unique_returns_callable():
    # Should return a callable that requires allow_expensive flag
    def fake_df():
        pass

    impl = pc.assert_target_key_unique(None, ["id"])  # type: ignore[arg-type]
    with pytest.raises(RuntimeError):
        impl(False)
