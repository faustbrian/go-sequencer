#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
    printf 'usage: %s <command> [arguments...]\n' "$0" >&2
    exit 2
fi

gocache="$(mktemp -d "${TMPDIR:-/tmp}/golib-gocache.XXXXXX")"
gomodcache="$(mktemp -d "${TMPDIR:-/tmp}/golib-modcache.XXXXXX")"
cleanup() {
    chmod -R u+w "${gocache}" "${gomodcache}" 2>/dev/null || true
    find "${gocache}" -depth -delete 2>/dev/null || true
    find "${gomodcache}" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

GOCACHE="${gocache}" GOMODCACHE="${gomodcache}" "$@"
