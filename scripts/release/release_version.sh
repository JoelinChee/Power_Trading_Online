#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/../common.sh"

VERSION_FILE="$PTO_REPO_ROOT/VERSION"

usage() {
    cat <<'EOF'
Usage:
    scripts/release/release_version.sh --show
    scripts/release/release_version.sh --set <version>
    scripts/release/release_version.sh --bump <major|minor|patch>

Examples:
    scripts/release/release_version.sh --show
    scripts/release/release_version.sh --set 1.2.0
    scripts/release/release_version.sh --bump patch
EOF
}

if [[ ! -f "$VERSION_FILE" ]]; then
    echo "VERSION file not found at $VERSION_FILE" >&2
    exit 1
fi

read_current_version() {
    # VERSION is a single-line semantic version file; strip whitespace so the
    # value can be safely embedded in archive names and build metadata.
    tr -d '[:space:]' <"$VERSION_FILE"
}

validate_version() {
    local version="$1"
    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "Invalid version: $version (expected semantic version like 1.2.3)" >&2
        exit 1
    fi
}

write_version() {
    local version="$1"
    # Always end the file with a newline so shell tools and package metadata
    # readers see the same clean value.
    printf '%s\n' "$version" >"$VERSION_FILE"
    echo "$version"
}

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

case "$1" in
--show)
    printf '%s\n' "$(read_current_version)"
    ;;
--set)
    if [[ $# -ne 2 ]]; then
        usage
        exit 1
    fi
    validate_version "$2"
    write_version "$2"
    ;;
--bump)
    if [[ $# -ne 2 ]]; then
        usage
        exit 1
    fi

    current="$(read_current_version)"
    validate_version "$current"

    IFS='.' read -r major minor patch <<<"$current"
    case "$2" in
    major)
        major=$((major + 1))
        minor=0
        patch=0
        ;;
    minor)
        minor=$((minor + 1))
        patch=0
        ;;
    patch)
        patch=$((patch + 1))
        ;;
    *)
        echo "Unknown bump type: $2 (expected major, minor, or patch)" >&2
        exit 1
        ;;
    esac

    write_version "${major}.${minor}.${patch}"
    ;;
-h|--help)
    usage
    ;;
*)
    usage
    exit 1
    ;;
esac
