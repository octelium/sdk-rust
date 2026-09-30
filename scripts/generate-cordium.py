#!/usr/bin/env python3
"""Refresh Cordium/metadata bindings using the protobuf repository's locked Rust generator.

Usage: python3 scripts/generate-cordium.py /path/to/pb [--check]
The generator does not require protoc. Other API modules are left untouched.
"""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("protobuf_root", type=Path)
parser.add_argument("--check", action="store_true", help="fail if checked-in bindings differ")
args = parser.parse_args()
repo = Path(__file__).resolve().parents[1]
pb = args.protobuf_root.resolve()
manifest = pb / "lang/rust/gen/Cargo.toml"
if not manifest.is_file():
    parser.error(f"generator not found: {manifest}")
version = re.search(r'^version = "([^"]+)"', (repo / "crates/octelium-apis/Cargo.toml").read_text(), re.M)[1]
cargo = shutil.which("cargo") or str(Path.home() / ".cargo/bin/cargo")
with tempfile.TemporaryDirectory(prefix="cordium-rust-bindings-") as tmp:
    env = dict(os.environ, OCTELIUM_APIS_OUT_DIR=tmp, OCTELIUM_APIS_VERSION=version,
               CARGO_TARGET_DIR=str(repo / "target/protobuf-generator"))
    subprocess.run([cargo, "run", "--locked", "--manifest-path", str(manifest)], env=env, check=True)
    for package in ("cordium", "meta"):
        filename = f"octelium.api.main.{package}.v1.rs"
        source = Path(tmp) / "src/gen" / filename
        destination = repo / "crates/octelium-apis/src/gen" / filename
        if args.check:
            if source.read_bytes() != destination.read_bytes():
                raise SystemExit(f"bindings differ: {destination}")
        else:
            shutil.copyfile(source, destination)
print("Cordium and metadata bindings are current.")
