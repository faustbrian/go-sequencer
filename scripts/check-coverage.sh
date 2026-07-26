#!/usr/bin/env bash
set -euo pipefail

profile="$(mktemp)"
trap 'rm -f "$profile"' EXIT
GOWORK=off go test -tags=integration -coverpkg=./... -coverprofile="$profile" ./...
coverage="$(go tool cover -func="$profile" | awk '$1 == "total:" {print $3}')"
if [[ "$coverage" != "100.0%" ]]; then
  echo "production statement coverage is $coverage, want 100.0%" >&2
  exit 1
fi
echo "production statement coverage: $coverage"
