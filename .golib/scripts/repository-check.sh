#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
repository="github.com/faustbrian/go-sequencer"

jq -e --arg repository "${repository}" '
    .repository == $repository and
    (.modules | length > 0) and
    all(.modules[];
        (.directory == "." or (.directory | startswith("/") | not)) and
        (
            .releasable == false or
            .module_path == $repository or
            (.module_path | startswith($repository + "/"))
        )
    )
' "${root}/modules.json" >/dev/null

while IFS= read -r module; do
    directory="$(jq -r --arg module "${module}" \
        '.modules[] | select(.module_path == $module) | .directory' \
        "${root}/modules.json")"
    [[ "$(sed -n 's/^module[[:space:]]\+//p' "${root}/${directory}/go.mod")" == "${module}" ]]
    if grep -Eq '^[[:space:]]*replace([[:space:]]|$)' "${root}/${directory}/go.mod"; then
        printf 'committed replace directive in %s\n' "${directory}/go.mod" >&2
        exit 1
    fi
done < <(jq -r '.modules[].module_path' "${root}/modules.json")

if grep -REnI \
    --exclude-dir='.git' \
    --exclude-dir='.artifacts' \
    --exclude='go.sum' \
    --exclude='CHANGELOG.md' \
    --exclude='repository-check.sh' \
    'github\.com/faustbrian/golib/pkg|/Users/[^/]+/Developer|\.\./go-' \
    "${root}"; then
    printf 'monorepo or sibling-checkout reference remains\n' >&2
    exit 1
fi

git -C "${root}" ls-files --error-unmatch package-lock.json >/dev/null

git diff --check
printf 'standalone repository contract passed\n'
