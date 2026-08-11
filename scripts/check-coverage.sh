#!/usr/bin/env bash
set -euo pipefail

profile="$(mktemp)"
packages="$(mktemp)"
report="$(mktemp)"
trap 'rm -f "$profile" "$packages" "$report"' EXIT
GOWORK=off go list ./... | awk '!/\/sequencertest$/' | sort -u >"$packages"
GOWORK=off go test -tags=integration -count=1 -timeout=20m \
  -covermode=atomic -coverpkg=./... -coverprofile="$profile" ./...

set +e
awk '
  FNR == NR { expected[$1] = 1; next }
  FNR == 1 { next }
  {
    split($1, location, ":")
    package = location[1]
    sub("/[^/]+$", "", package)
    block = $1
    blockPackage[block] = package
    blockStatements[block] = $(NF - 1) + 0
    if (($NF + 0) > 0) blockCovered[block] = 1
  }
  END {
    failed = 0
    for (block in blockStatements) {
      package = blockPackage[block]
      total[package] += blockStatements[block]
      if (blockCovered[block]) covered[package] += blockStatements[block]
    }
    for (package in expected) {
      printf "%s %d/%d statements\n", package, covered[package], total[package]
      if (!(package in total) || total[package] == 0 || covered[package] != total[package]) failed = 1
    }
    exit failed
  }
' "$packages" "$profile" | sort >"$report"
status="${PIPESTATUS[0]}"
set -e
cat "$report"
if [[ "$status" -ne 0 ]]; then
  echo "one or more production packages are below exact 100% coverage" >&2
  exit 1
fi
echo "all production packages have exact 100% statement coverage"
