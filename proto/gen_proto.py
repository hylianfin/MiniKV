"""Generate minikv_pb2*.py from proto/minikv.proto. Run from the repo root."""

from __future__ import annotations

import sys
from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    code = protoc.main(
        [
            "protoc",
            "-Iproto",
            "--python_out=minikv",
            "--grpc_python_out=minikv",
            "proto/minikv.proto",
        ]
    )
    if code != 0:
        raise SystemExit(f"protoc failed with code {code}")
    # The grpc plugin emits a top-level `import minikv_pb2`, which breaks
    # once the file lives inside the minikv package. Rewrite to packaged.
    target = ROOT / "minikv" / "minikv_pb2_grpc.py"
    text = target.read_text(encoding="utf-8")
    fixed = text.replace(
        "import minikv_pb2 as minikv__pb2",
        "from minikv import minikv_pb2 as minikv__pb2",
    )
    if fixed == text:
        raise SystemExit("expected grpc import line not found; protoc output changed?")
    target.write_text(fixed, encoding="utf-8")
    print("generated minikv/minikv_pb2*.py", flush=True)


if __name__ == "__main__":
    sys.exit(main())
