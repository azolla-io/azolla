"""Docs consistency tests."""

from pathlib import Path


def test_readme_has_no_tasktester_reference() -> None:
    readme = Path(__file__).parents[2] / "README.md"
    content = readme.read_text()
    assert "azolla.testing import TaskTester" not in content


def test_incompat_doc_matches_iterator_approach() -> None:
    doc = Path(__file__).parents[2] / "GRPC_INCOMPATIBILITY.md"
    content = doc.read_text()
    assert "__aiter__()" in content
    assert "aiter(" not in content


def test_readme_mentions_checked_in_grpc_code() -> None:
    readme = Path(__file__).parents[2] / "README.md"
    content = readme.read_text()
    assert "checks in generated gRPC stubs" in content
