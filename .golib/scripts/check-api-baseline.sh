#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <module-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
# shellcheck disable=SC1091 # Canonical repository tool versions.
source "${root}/.golib/versions.env"
case "$1" in
    /*) directory="$1" ;;
    *) directory="${root}/$1" ;;
esac
if [[ ! -f "${directory}/go.mod" ]]; then
    printf 'module has no go.mod: %s\n' "$1" >&2
    exit 1
fi

baseline="${directory}/api/baseline.txt"
if [[ ! -s "${baseline}" ]]; then
    printf 'missing API baseline: %s\n' "${baseline}" >&2
    exit 1
fi
module_path="$(
    cd "${directory}"
    GOWORK=off go mod edit -json | jq -er '.Module.Path'
)"
current="$(mktemp "${TMPDIR:-/tmp}/golib-api-current.XXXXXX")"
report="$(mktemp "${TMPDIR:-/tmp}/golib-api-report.XXXXXX")"
cleanup() {
    rm -f "${current}" "${report}"
}
trap cleanup EXIT HUP INT TERM

run_apidiff() {
    if [[ -n "${GOLIB_APIDIFF:-}" ]]; then
        GOWORK=off go exec-tool "${GOLIB_APIDIFF}" "$@"
    else
        GOWORK=off go run \
            "golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" "$@"
    fi
}

(
    cd "${directory}"
    run_apidiff -m -w "${current}" "${module_path}"
)
set +e
(
    cd "${directory}"
    run_apidiff -m -incompatible "${baseline}" "${current}" >"${report}"
)
status=$?
set -e
if [[ -s "${report}" ]]; then
    printf 'incompatible exported API changes in %s:\n' \
        "${module_path}" >&2
    cat "${report}" >&2
    exit 1
fi
if [[ "${status}" -ne 0 ]]; then
    printf 'API compatibility tool failed for %s\n' "${module_path}" >&2
    exit "${status}"
fi
