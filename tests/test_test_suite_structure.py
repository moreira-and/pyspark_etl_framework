from __future__ import annotations

import ast
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_ROOT = PROJECT_ROOT / "tests"


def _module_has_integration_mark(tree: ast.Module) -> bool:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "pytestmark"
            for target in node.targets
        ):
            continue
        if _node_references_integration_mark(node.value):
            return True
    return False


def _function_has_integration_mark(function: ast.FunctionDef) -> bool:
    return any(
        _node_references_integration_mark(node) for node in function.decorator_list
    )


def _node_references_integration_mark(node: ast.AST) -> bool:
    if isinstance(node, ast.Attribute):
        return node.attr == "integration"

    if isinstance(node, ast.Call):
        return _node_references_integration_mark(node.func)

    if isinstance(node, (ast.List, ast.Tuple)):
        return any(_node_references_integration_mark(item) for item in node.elts)

    return False


def _test_uses_spark_fixture(function: ast.FunctionDef) -> bool:
    args = [*function.args.args, *function.args.kwonlyargs]
    return any(arg.arg == "spark" for arg in args)


def test_integration_tests_are_marked_explicitly() -> None:
    # Protects quick unit feedback from accidentally starting a SparkSession.
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    markers = pyproject["tool"]["pytest"]["ini_options"]["markers"]
    assert any(marker.startswith("integration:") for marker in markers)

    unmarked_spark_tests = []
    for path in sorted(TEST_ROOT.glob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        module_marked = _module_has_integration_mark(tree)

        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            if not node.name.startswith("test_"):
                continue
            if not _test_uses_spark_fixture(node):
                continue
            if module_marked or _function_has_integration_mark(node):
                continue
            unmarked_spark_tests.append(f"{path.name}::{node.name}")

    assert unmarked_spark_tests == []
