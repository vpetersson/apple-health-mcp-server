#!/usr/bin/env bash
#
# Package an already-built apple-health-mcp binary as an MCP bundle (.mcpb) —
# the one-click install format Claude Desktop uses for local MCP servers.
#
# Usage: scripts/build-mcpb.sh <binary> <output.mcpb>
#
# The bundle version is taken from Cargo.toml, so mcpb/manifest.json never has
# to be bumped by hand.

set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <binary> <output.mcpb>" >&2
  exit 2
fi

binary="$1"
output="$2"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ ! -f "$binary" ]; then
  echo "error: no such binary: $binary" >&2
  exit 1
fi

version="$(grep -m1 '^version = ' "${repo_root}/Cargo.toml" | cut -d'"' -f2)"
if [ -z "$version" ]; then
  echo "error: could not read version from Cargo.toml" >&2
  exit 1
fi

stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT

mkdir -p "${stage}/server"
install -m 0755 "$binary" "${stage}/server/apple-health-mcp"

python3 - "$version" "${repo_root}/mcpb/manifest.json" "${stage}/manifest.json" <<'PY'
import json
import sys

version, src, dst = sys.argv[1:4]
with open(src, encoding="utf-8") as fh:
    manifest = json.load(fh)
manifest["version"] = version
with open(dst, "w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2)
    fh.write("\n")
PY

mkdir -p "$(dirname "$output")"
bunx --bun @anthropic-ai/mcpb@2 pack "$stage" "$output"

echo "built ${output} (version ${version})"
