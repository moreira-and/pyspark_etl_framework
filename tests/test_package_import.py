from __future__ import annotations

from pathlib import Path

import etl_framework
from etl_framework.contracts.pipeline import Extract, Load, Pipeline, Transform
from etl_framework.models.config import EtlRunConfig
from etl_framework.models.context import EtlExecutionContext


def test_package_import_resolves_local_framework_package() -> None:
    # Assert
    assert etl_framework.__name__ == "etl_framework"
    package_path = Path(etl_framework.__file__).resolve()
    assert package_path.name == "__init__.py"
    assert package_path.parent.name == "etl_framework"


def test_public_api_exports_expected_symbols() -> None:
    # Assert
    assert etl_framework.__all__ == [
        "Pipeline",
        "EtlRunConfig",
        "EtlExecutionContext",
        "Extract",
        "Transform",
        "Load",
    ]
    assert etl_framework.Pipeline is Pipeline
    assert etl_framework.EtlRunConfig is EtlRunConfig
    assert etl_framework.EtlExecutionContext is EtlExecutionContext
    assert etl_framework.Extract is Extract
    assert etl_framework.Transform is Transform
    assert etl_framework.Load is Load
