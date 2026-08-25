#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    printf 'usage: %s <coverage-profile> <build-tags>\n' "$0" >&2
    exit 2
fi

profile="$1"
tags="$2"
started="$(date +%s)"
arguments=(
    test
    -count=1
    -timeout=20m
    -cover
    -coverpkg=./...
    "-coverprofile=${profile}"
)
if [[ -n "${tags}" ]]; then
    arguments+=("-tags=${tags}")
fi
arguments+=(./...)

go "${arguments[@]}" >&2
finished="$(date +%s)"
elapsed=$((finished - started))
if [[ "${elapsed}" -lt 1 ]]; then
    elapsed=1
fi
printf '%ss\n' "${elapsed}"
