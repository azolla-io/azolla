#!/usr/bin/env python3
"""Generate gRPC code from proto files."""
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
    
    # Proto files to compile
    proto_files = [
        proto_dir / "orchestrator.proto",
        proto_dir / "common.proto"
    ]
    
    for proto_file in proto_files:
        if not proto_file.exists():
            print(f"Proto file not found: {proto_file}")
            sys.exit(1)
    
    # Generate gRPC code
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"--proto_path={proto_dir}",
        f"--python_out={grpc_output_dir}",
        f"--grpc_python_out={grpc_output_dir}",
        f"--pyi_out={grpc_output_dir}",  # Type stubs
    ] + [str(f) for f in proto_files]
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("gRPC code generation failed:")
        print(result.stderr)
        sys.exit(1)
    
    # Create __init__.py in grpc module
    init_file = grpc_output_dir / "__init__.py"
    init_file.write_text('"""Generated gRPC code for Azolla protocol."""\n')
    
    print("✅ gRPC code generated successfully!")

if __name__ == "__main__":
    main()