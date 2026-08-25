#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <module-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
fuzz_budget="${GOLIB_FUZZ_SMOKE_BUDGET:-10000x}"
if [[ "${module}" = /* ]]; then
    directory="${module}"
else
    directory="${root}/${module}"
fi
if [[ ! -f "${directory}/go.mod" ]]; then
    printf 'module has no go.mod: %s\n' "${module}" >&2
    exit 1
fi
targets="$(mktemp)"
trap 'rm -f "${targets}"' EXIT

cd "${directory}"
while IFS= read -r -d '' file; do
    parent="$(dirname "${file#./}")"
    nested=0
    while [[ "${parent}" != "." ]]; do
        if [[ -f "${parent}/go.mod" ]]; then
            nested=1
            break
        fi
        parent="$(dirname "${parent}")"
    done
    [[ "${nested}" -eq 0 ]] || continue
    names="$(
        sed -nE \
            's/^func (Fuzz[A-Za-z0-9_]+)\([A-Za-z_][A-Za-z0-9_]* \*testing\.F\).*/\1/p' \
            "${file}"
    )"
    while IFS= read -r target; do
        [[ -n "${target}" ]] || continue
        printf '%s %s\n' "${file}" "${target}" >>"${targets}"
    done <<<"${names}"
done < <(find . -type f -name '*_test.go' -not -path './vendor/*' -print0)
sort -u -o "${targets}" "${targets}"

count=0
while read -r file target; do
    [[ -n "${file:-}" ]] || continue
    package="./$(dirname "${file#./}")"
    [[ "${package}" != "./." ]] || package=.
    GOWORK=off go test "${package}" -run '^$' -fuzz "^${target}$" \
        -fuzztime="${fuzz_budget}" -parallel=2
    count=$((count + 1))
done <"${targets}"
if [[ "${count}" -eq 0 ]]; then
    printf 'no fuzz targets were executed for %s\n' "${module}" >&2
    exit 1
fi
printf 'executed %s registered fuzz targets\n' "${count}"
