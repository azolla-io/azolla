#!/usr/bin/env python3
"""Fix gRPC imports to use relative imports."""
import re
from pathlib import Path

def fix_imports(grpc_dir: Path):
    """Fix imports in generated gRPC files."""

    # Fix orchestrator_pb2.py
    orchestrator_pb2 = grpc_dir / "orchestrator_pb2.py"
    if orchestrator_pb2.exists():
        content = orchestrator_pb2.read_text()
        content = content.replace(
            "import common_pb2 as common__pb2",
            "from . import common_pb2 as common__pb2"
        )
        orchestrator_pb2.write_text(content)
        print("Fixed orchestrator_pb2.py imports")

    # Fix orchestrator_pb2_grpc.py
    orchestrator_grpc = grpc_dir / "orchestrator_pb2_grpc.py"
    if orchestrator_grpc.exists():
        content = orchestrator_grpc.read_text()
        content = content.replace(
            "import orchestrator_pb2 as orchestrator__pb2",
            "from . import orchestrator_pb2 as orchestrator__pb2"
        )
        orchestrator_grpc.write_text(content)
        print("Fixed orchestrator_pb2_grpc.py imports")

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    grpc_dir = script_dir.parent / "src" / "azolla" / "_grpc"
    fix_imports(grpc_dir)
    print("✅ gRPC import fixes applied!")