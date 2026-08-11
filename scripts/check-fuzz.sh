#!/usr/bin/env bash
set -euo pipefail

duration="${1:-10000x}"
targets="$(mktemp)"
fuzz_cache="$(mktemp -d)"
trap 'rm -f "${targets}"; rm -rf "${fuzz_cache}"' EXIT

while IFS= read -r -d '' file; do
  package="./$(dirname "${file#./}")"
  [[ "${package}" != "./." ]] || package=.
  while IFS= read -r target; do
    [[ -n "${target}" ]] || continue
    printf '%s %s\n' "${package}" "${target}" >>"${targets}"
  done < <(sed -nE 's/^func (Fuzz[A-Za-z0-9_]+)\([A-Za-z_][A-Za-z0-9_]* \*testing\.F\).*/\1/p' "${file}")
done < <(find . -type f -name '*_test.go' -not -path './vendor/*' -print0)
sort -u -o "${targets}" "${targets}"

count=0
while read -r package target; do
  [[ -n "${package:-}" ]] || continue
  GOCACHE="${fuzz_cache}" GOWORK=off go test "${package}" -run '^$' -fuzz="^${target}$" \
    -fuzztime="${duration}" -parallel=1 -cpu=1 -timeout=2m
  count=$((count + 1))
done <"${targets}"

if [[ "${count}" -eq 0 ]]; then
  printf 'no fuzz targets were executed\n' >&2
  exit 1
fi
printf 'executed %s registered fuzz targets\n' "${count}"
