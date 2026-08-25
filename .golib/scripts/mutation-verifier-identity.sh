#!/usr/bin/env bash
set -euo pipefail

root="${GOLIB_ROOT:-$(git rev-parse --show-toplevel)}"

# shellcheck disable=SC1091
source "${root}/.golib/versions.env"

: "${GREMLINS_VERSION:?GREMLINS_VERSION is required}"
: "${GREMLINS_SUM:?GREMLINS_SUM is required}"
: "${GREMLINS_GOMOD_SUM:?GREMLINS_GOMOD_SUM is required}"

inputs=(
    scripts/internal/mutation-command.sh
    scripts/internal/mutation-coverage.sh
    scripts/patches/gremlins-run-all-mutants.patch
    scripts/patches/gremlins-shared-coverage.patch
    scripts/patches/gremlins-module-relative-diff.patch
)

{
    printf 'gremlins-version\t%s\n' "${GREMLINS_VERSION}"
    printf 'gremlins-sum\t%s\n' "${GREMLINS_SUM}"
    printf 'gremlins-gomod-sum\t%s\n' "${GREMLINS_GOMOD_SUM}"
    for input in "${inputs[@]}"; do
        [[ -f "${root}/.golib/${input}" ]] || {
            printf 'mutation verifier input is missing: %s\n' "${input}" >&2
            exit 1
        }
        printf 'file\t%s\t%s\n' "${input}" "$(
            shasum -a 256 "${root}/.golib/${input}" | awk '{print $1}'
        )"
    done
} | shasum -a 256 | awk '{print $1}'
