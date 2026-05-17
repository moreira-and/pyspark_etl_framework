from __future__ import annotations

import ast
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "etl_framework"

FORBIDDEN_RUNTIME_IMPORTS = {
    "dynaconf",
    "great_expectations",
    "hydra",
    "loguru",
    "pandas",
    "pandera",
    "pydantic",
    "pydantic_settings",
    "pytest",
    "sklearn",
}


def load_pyproject() -> dict[str, object]:
    return tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def imported_top_level_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules.add(node.module.split(".")[0])

    return modules


def test_runtime_dependencies_are_resolved_from_project_root() -> None:
    # Arrange / Act
    pyproject = load_pyproject()

    # Assert
    assert (PROJECT_ROOT / "pyproject.toml").is_file()
    poetry_config = pyproject["tool"]["poetry"]  # type: ignore[index]
    assert poetry_config["name"] == "spark-etl-framework"


def test_runtime_dependencies_are_limited_to_python_and_pyspark() -> None:
    # Arrange / Act
    pyproject = load_pyproject()
    dependencies = pyproject["tool"]["poetry"]["dependencies"]  # type: ignore[index]

    # Assert
    assert set(dependencies) == {"python", "pyspark"}
    assert dependencies["pyspark"] == ">=3.5,<4.0"


def test_development_dependencies_do_not_leak_into_runtime() -> None:
    # Arrange / Act
    pyproject = load_pyproject()
    poetry_config = pyproject["tool"]["poetry"]  # type: ignore[index]
    runtime_dependencies = poetry_config["dependencies"]
    dev_dependencies = poetry_config["group"]["dev"]["dependencies"]

    # Assert
    assert set(runtime_dependencies).isdisjoint(set(dev_dependencies))


def test_runtime_dependencies_do_not_include_disallowed_frameworks() -> None:
    # Arrange / Act
    pyproject = load_pyproject()
    dependencies = pyproject["tool"]["poetry"]["dependencies"]  # type: ignore[index]

    # Assert
    assert not {
        "dynaconf",
        "great-expectations",
        "hydra",
        "loguru",
        "pandera",
        "pydantic",
        "pydantic-settings",
        "scikit-learn",
    }.intersection(dependencies)


def test_runtime_package_does_not_import_forbidden_dependencies() -> None:
    # Arrange / Act
    violations = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        forbidden_imports = imported_top_level_modules(path).intersection(
            FORBIDDEN_RUNTIME_IMPORTS
        )
        if forbidden_imports:
            violations.append(
                f"{path.relative_to(PROJECT_ROOT)}: {sorted(forbidden_imports)}"
            )

    # Assert
    assert violations == []
