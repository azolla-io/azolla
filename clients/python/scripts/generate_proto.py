#!/usr/bin/env python3
"""Generate gRPC code from proto files."""
import os
import re
import subprocess
import sys
from pathlib import Path

def main():
    """Generate gRPC code from proto definitions."""
    # Get paths
    script_dir = Path(__file__).parent
    python_client_dir = script_dir.parent
    proto_dir = python_client_dir.parent.parent / "proto"
    grpc_output_dir = python_client_dir / "src" / "azolla" / "_grpc"

    if not proto_dir.exists():
        print(f"Proto directory not found: {proto_dir}")
        sys.exit(1)

    # Ensure output directory exists
    grpc_output_dir.mkdir(parents=True, exist_ok=True)

    # Proto files to compile (just the filenames, not full paths)
    proto_files = ["orchestrator.proto", "common.proto", "shepherd.proto"]

    # Verify proto files exist
    for proto_file in proto_files:
        if not (proto_dir / proto_file).exists():
            print(f"Proto file not found: {proto_dir / proto_file}")
            sys.exit(1)

    # Option 2: Try copying proto files to grpc directory temporarily
    # and compiling them locally to get relative imports
    original_cwd = os.getcwd()
    copied_proto_paths = []
    try:
        for proto_file in proto_files:
            src = proto_dir / proto_file
            dst = grpc_output_dir / proto_file
            dst.write_bytes(src.read_bytes())
            copied_proto_paths.append(dst)

        os.chdir(grpc_output_dir)

        cmd = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "--proto_path=.",
            "--python_out=.",
            "--grpc_python_out=.",
            "--pyi_out=.",
        ] + proto_files

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print("gRPC code generation failed:")
            print(result.stderr)
            sys.exit(1)

        _ensure_relative_imports(grpc_output_dir)

    finally:
        os.chdir(original_cwd)
        for path in copied_proto_paths:
            if path.exists():
                path.unlink()

    # Create __init__.py in grpc module
    init_file = grpc_output_dir / "__init__.py"
    init_file.write_text('"""Generated gRPC code for Azolla protocol."""\n')

    print("✅ gRPC code generated successfully!")


def _ensure_relative_imports(grpc_output_dir: Path) -> None:
    """Convert imports in generated modules to package-relative imports."""

    import_pattern = re.compile(r"^import (\w+_pb2)(\s+as\s+\w+)?$", re.MULTILINE)

    for path in grpc_output_dir.glob("*.py"):
        text = path.read_text()

        def _replace(match: re.Match[str]) -> str:
            module = match.group(1)
            alias = match.group(2) or ""
            return f"from . import {module}{alias}"

        new_text, replacements = import_pattern.subn(_replace, text)

        if replacements > 0:
            path.write_text(new_text)

if __name__ == "__main__":
    main()
