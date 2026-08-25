#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <module-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
directory="${root}/${module}"

if rg -n --glob '*.go' --glob '!**/*_test.go' \
    '(^|[^[:alnum:]_])(unsafe\.|os\.Exit\(|log\.Fatal|http\.DefaultClient)' \
    "${directory}"; then
    printf 'forbidden unsafe or process-global production API detected\n' >&2
    exit 1
fi
printf 'standalone safety policy passed for %s\n' "${module}"
