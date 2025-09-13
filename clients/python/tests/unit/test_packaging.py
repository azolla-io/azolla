"""Packaging-related tests for developer tooling presence."""

from pathlib import Path


def test_pyproject_has_grpcio_tools_in_dev_extras() -> None:
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    content = pyproject.read_text()
    assert "grpcio-tools" in content
