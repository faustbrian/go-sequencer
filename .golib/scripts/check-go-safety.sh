#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <module-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
directory="${root}/${module}"

go run "${root}/.golib/scripts/check-go-safety.go" "${directory}"
printf 'standalone safety policy passed for %s\n' "${module}"
