"""Test CLI task discovery and registration from modules."""

import sys
from pathlib import Path

import pytest

from azolla import Worker
from azolla.cli import discover_and_register_tasks


@pytest.mark.asyncio
async def test_importing_task_modules_registers_tasks(tmp_path: Path) -> None:
    # Create a temporary module with a decorated task
    module_dir = tmp_path / "modpkg"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text("")
    mod_file = module_dir / "tasks.py"
    mod_file.write_text(
        "from azolla import azolla_task\n"
        "@azolla_task\n"
        "async def hello(name: str) -> str:\n"
        "    return f'hi {name}'\n"
    )

    # Ensure module is importable
    sys.path.insert(0, str(tmp_path))
    try:
        worker = Worker.builder().build()
        # Use CLI helper to discover and register
        count = discover_and_register_tasks(worker, ["modpkg.tasks"])
        assert count == 1
        assert worker.task_count() == 1
    finally:
        sys.path.remove(str(tmp_path))
