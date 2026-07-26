#!/usr/bin/env bash
set -euo pipefail

required=(
  README.md CHANGELOG.md SECURITY.md CONTRIBUTING.md docs/README.md
  docs/api.md docs/quickstart.md docs/lifecycle.md docs/ordering.md
  docs/transactions.md docs/async.md docs/deployment.md docs/migrations.md
  docs/retries.md docs/rollback.md docs/recovery.md docs/operations.md
  docs/security.md docs/performance.md docs/laravel-migration.md
  docs/cookbook.md docs/faq.md docs/compatibility.md
)
for path in "${required[@]}"; do
  test -s "$path"
done

while IFS=: read -r source match; do
  link="$(sed -E 's/.*\(([^)]+)\)/\1/' <<<"$match")"
  link="${link%%#*}"
  [[ -z "$link" || "$link" == http://* || "$link" == https://* ]] && continue
  target="$(dirname "$source")/$link"
  test -e "$target" || {
    echo "broken local documentation link: $source -> $link" >&2
    exit 1
  }
done < <(rg -o --with-filename '\[[^]]+\]\([^)]+\)' README.md docs)

GOWORK=off go test ./... -run '^Example' -count=1
