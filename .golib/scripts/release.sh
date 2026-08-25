#!/usr/bin/env bash
set -euo pipefail

dry_run=0
public=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) dry_run=1; shift ;;
        --public) public=1; shift ;;
        *) break ;;
    esac
done
if [[ "${dry_run}" -ne 1 || $# -ne 1 ]]; then
    printf 'usage: %s --dry-run [--public] <module-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
record="$(jq -ce --arg directory "${module}" \
    '.modules[] | select(.directory == $directory and .releasable == true)' \
    "${root}/modules.json")"
module_path="$(jq -r '.module_path' <<<"${record}")"
tag_prefix="$(jq -r '.tag_prefix' <<<"${record}")"
tag="${tag_prefix}1.0.0"
directory="${root}/${module}"

[[ "$(sed -n 's/^module[[:space:]]\+//p' "${directory}/go.mod")" == "${module_path}" ]]
if grep -Eq '^[[:space:]]*replace([[:space:]]|$)' "${directory}/go.mod"; then
    printf 'release module contains a replace directive: %s\n' "${module}" >&2
    exit 1
fi
if git -C "${root}" show-ref --verify --quiet "refs/tags/${tag}"; then
    printf 'release tag already exists: %s\n' "${tag}" >&2
    exit 1
fi

task="$(mktemp -d "${TMPDIR:-/tmp}/golib-release.XXXXXX")"
# shellcheck disable=SC2329 # Invoked by the release EXIT trap.
cleanup() {
    chmod -R u+w "${task}" 2>/dev/null || true
    find "${task}" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

if [[ "${public}" -eq 1 ]]; then
    GOPROXY="https://proxy.golang.org,direct" GOWORK=off \
        go list -m "${module_path}@v1.0.0" >/dev/null
else
    proxy="${task}/proxy"
    mkdir "${proxy}"
    "${root}/.golib/scripts/build-local-proxy.sh" "${proxy}" v1.0.0
    GOPROXY="file://${proxy},https://proxy.golang.org,direct" \
        GONOSUMDB="github.com/faustbrian/go-*" GOWORK=off \
        go list -m "${module_path}@v1.0.0" >/dev/null
fi

printf 'release dry-run passed: %s %s\n' "${module_path}" "${tag}"
