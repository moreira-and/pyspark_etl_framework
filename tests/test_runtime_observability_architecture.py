from __future__ import annotations

import ast
from pathlib import Path

CONTRACT_FILES = [
    Path("etl_framework/contracts/extract.py"),
    Path("etl_framework/contracts/transform.py"),
    Path("etl_framework/contracts/load.py"),
    Path("etl_framework/contracts/pipeline.py"),
]
CONTRACT_PACKAGE_FILES = sorted(Path("etl_framework/contracts").glob("*.py"))


def test_contracts_and_pipeline_do_not_import_observability_infrastructure() -> None:
    for path in CONTRACT_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module != "etl_framework.infra.observability", path
            if isinstance(node, ast.Import):
                imported_names = {alias.name for alias in node.names}
                assert "logging" not in imported_names, path


def test_contracts_and_pipeline_do_not_call_observability_helpers() -> None:
    forbidden_calls = {
        "configure_observability_sink",
        "get_observability_service",
        "build_observability_event",
    }

    for path in CONTRACT_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))

        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_calls, path


def test_contracts_and_pipeline_do_not_define_metadata_builders() -> None:
    for path in CONTRACT_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert "metadata" not in node.name, path


def test_contract_package_does_not_define_top_level_helper_functions() -> None:
    for path in CONTRACT_PACKAGE_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))

        for node in tree.body:
            assert not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)), path

def test_pipeline_does_not_store_logger_attribute() -> None:
    path = Path("etl_framework/contracts/pipeline.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert node.attr != "logger"
