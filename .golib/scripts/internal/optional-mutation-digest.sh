#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    printf 'usage: %s <resolution> <module-directory> <package-directory>\n' \
        "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
resolution="$1"
module="$2"
package="$3"
digest=""
if ! digest="$(
    GOLIB_MUTATION_DIGEST_RESOLUTION="${resolution}" \
        "${root}/.golib/scripts/gate-input-digest.sh" \
        mutation "${module}" "${package}" 2>/dev/null
)"; then
    exit 0
fi

printf '%s\n' "${digest}"
