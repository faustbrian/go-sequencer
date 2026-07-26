#!/usr/bin/env bash
set -euo pipefail

duration="${1:-2s}"
for target in FuzzCompilePlanDeterminism FuzzSanitizePersistenceText; do
  GOWORK=off go test . -run '^$' -fuzz="^${target}$" \
    -fuzztime="$duration" -parallel=4 -timeout=2m
done
