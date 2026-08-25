#!/usr/bin/env bash
set -euo pipefail

root="${GOLIB_ROOT:-$(git rev-parse --show-toplevel)}"
selection="$(cat)"

jq -r --arg selection "${selection}" '
    ($selection | split("\n") | map(select(length > 0)))[] as $directory
    | .modules[]
    | select(.directory == $directory and .releasable == true)
    | .directory
' "${root}/modules.json"
