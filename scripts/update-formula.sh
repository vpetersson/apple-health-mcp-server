#!/usr/bin/env bash
#
# Regenerate the Homebrew formula for a tagged release.
#
# Usage: scripts/update-formula.sh <tag> <checksums-file> [output-file]
#
#   <tag>             release tag, e.g. v26.9.0 (the leading "v" is stripped
#                     from the version used inside the formula, but kept in the
#                     asset names, which embed the tag verbatim)
#   <checksums-file>  the release's SHA256SUMS
#   [output-file]     defaults to Formula/apple-health-mcp.rb

set -euo pipefail

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <tag> <checksums-file> [output-file]" >&2
    exit 64
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="${repo_root}/scripts/formula.rb.tmpl"

tag="$1"
checksums="$2"
output="${3:-${repo_root}/Formula/apple-health-mcp.rb}"
version="${tag#v}"
base_url="https://github.com/vpetersson/apple-health-mcp-server/releases/download/${tag}"

if [ ! -f "$checksums" ]; then
    echo "No such checksums file: ${checksums}" >&2
    exit 66
fi

# Look up the checksum of the binary for one target triple. The asset names are
# produced by the build job in .github/workflows/release.yml. Prints nothing
# when the asset is missing from the checksums file; callers check for that.
sha_for() {
    local target="$1" asset
    asset="apple-health-mcp-${tag}-${target}"
    # sha256sum prints "<sha>  <path>"; binary mode prefixes the path with "*".
    awk -v asset="$asset" '
        { path = $2; sub(/^\*/, "", path); sub(/.*\//, "", path) }
        path == asset { print $1; exit }
    ' "$checksums"
}

# `sha_for` runs in a subshell, so its exit status cannot abort the script —
# resolve every checksum up front and bail here if one is missing.
require_sha() {
    local target="$1" sha
    sha="$(sha_for "$target")"
    if [ -z "$sha" ]; then
        echo "No checksum for apple-health-mcp-${tag}-${target} in ${checksums}" >&2
        exit 65
    fi
    printf '%s' "$sha"
}

sha_darwin_arm64="$(require_sha aarch64-apple-darwin)" || exit $?
sha_darwin_x86_64="$(require_sha x86_64-apple-darwin)" || exit $?
sha_linux_x86_64="$(require_sha x86_64-unknown-linux-gnu)" || exit $?

rendered="$(sed \
    -e "s|@@VERSION@@|${version}|g" \
    -e "s|@@TAG@@|${tag}|g" \
    -e "s|@@BASE_URL@@|${base_url}|g" \
    -e "s|@@SHA256_AARCH64_APPLE_DARWIN@@|${sha_darwin_arm64}|g" \
    -e "s|@@SHA256_X86_64_APPLE_DARWIN@@|${sha_darwin_x86_64}|g" \
    -e "s|@@SHA256_X86_64_UNKNOWN_LINUX_GNU@@|${sha_linux_x86_64}|g" \
    "$template")"

if printf '%s' "$rendered" | grep -q '@@'; then
    echo "Template placeholders left unsubstituted:" >&2
    printf '%s' "$rendered" | grep -n '@@' >&2
    exit 70
fi

mkdir -p "$(dirname "$output")"
printf '%s\n' "$rendered" > "$output"
echo "Wrote ${output} for ${tag}"
