#!/usr/bin/env bash
set -euo pipefail

report="$(mktemp)"
trap 'rm -f "$report"' EXIT

GOWORK=off go run github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0 \
	unleash . --integration --coverpkg ./... --workers 2 \
	--timeout-coefficient 10 --threshold-mcover 100 \
	--threshold-efficacy 65 --output "$report"
