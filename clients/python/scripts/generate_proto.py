#!/usr/bin/env python3
"""Generate gRPC code from proto files."""
import os
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
    proto_files = ["orchestrator.proto", "common.proto"]

    # Verify proto files exist
    for proto_file in proto_files:
        if not (proto_dir / proto_file).exists():
            print(f"Proto file not found: {proto_dir / proto_file}")
            sys.exit(1)

    # Option 2: Try copying proto files to grpc directory temporarily
    # and compiling them locally to get relative imports
    original_cwd = os.getcwd()
    try:
        # Copy proto files to _grpc directory temporarily
        for proto_file in proto_files:
            src = proto_dir / proto_file
            dst = grpc_output_dir / proto_file
            dst.write_bytes(src.read_bytes())

        # Change to _grpc directory and run protoc from there
        os.chdir(grpc_output_dir)

        cmd = [
            sys.executable, "-m", "grpc_tools.protoc",
            "--proto_path=.",
            "--python_out=.",
            "--grpc_python_out=.",
            "--pyi_out=.",  # Type stubs
        ] + proto_files

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print("gRPC code generation failed:")
            print(result.stderr)
            sys.exit(1)

        # Clean up copied proto files
        for proto_file in proto_files:
            (grpc_output_dir / proto_file).unlink()

    finally:
        os.chdir(original_cwd)

    # Create __init__.py in grpc module
    init_file = grpc_output_dir / "__init__.py"
    init_file.write_text('"""Generated gRPC code for Azolla protocol."""\n')

    print("✅ gRPC code generated successfully!")

if __name__ == "__main__":
    main()