#!/usr/bin/env bash
# Aligns the workspace dependency on octelium-apis with the version of the
# generated crate in crates/octelium-apis.
#
# The generated crate carries the version chosen by the release that produced
# it, so this runs after every sync of the generated code.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
apis_manifest="$repo_root/crates/octelium-apis/Cargo.toml"
workspace_manifest="$repo_root/Cargo.toml"

version="$(grep -m1 '^version = ' "$apis_manifest" | cut -d'"' -f2)"

if [ -z "$version" ]; then
  echo "Could not read the version from $apis_manifest" >&2
  exit 1
fi

python3 - "$workspace_manifest" "$version" <<'PY'
import re
import sys

manifest, version = sys.argv[1], sys.argv[2]

with open(manifest) as f:
    content = f.read()

updated, count = re.subn(
    r'(octelium-apis = \{ version = ")[^"]*(")',
    lambda m: f"{m.group(1)}{version}{m.group(2)}",
    content,
)

if count != 1:
    raise SystemExit(
        f"expected exactly one octelium-apis dependency in {manifest}, found {count}"
    )

with open(manifest, "w") as f:
    f.write(updated)
PY

echo "Workspace now depends on octelium-apis $version"
