#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"

# Resolved from the verified repository root rather than the caller's cwd.
# shellcheck disable=SC1091
source "${root}/.golib/versions.env"

semantic_patch="${root}/.golib/scripts/patches/gremlins-run-all-mutants.patch"
coverage_patch="${root}/.golib/scripts/patches/gremlins-shared-coverage.patch"
diff_patch="${root}/.golib/scripts/patches/gremlins-module-relative-diff.patch"
verifier_identity="$("${root}/.golib/scripts/mutation-verifier-identity.sh")"
platform_identity="$(go env GOOS GOARCH | paste -sd- -)"
artifact="${root}/.artifacts/tooling/gremlins-${verifier_identity}-${platform_identity}"
binary="${artifact}/golib-gremlins"
lock="${artifact}.lock"

if [[ -x "${binary}" ]]; then
    printf '%s\n' "${binary}"
    exit 0
fi

mkdir -p "$(dirname "${artifact}")"
attempts=0
while ! mkdir "${lock}" 2>/dev/null; do
    if [[ -x "${binary}" ]]; then
        printf '%s\n' "${binary}"
        exit 0
    fi
    attempts=$((attempts + 1))
    if [[ "${attempts}" -ge 120 ]]; then
        printf 'timed out waiting for golib-gremlins build lock: %s\n' \
            "${lock}" >&2
        exit 1
    fi
    sleep 1
done
temporary="$(mktemp -d "${TMPDIR:-/tmp}/golib-gremlins.XXXXXX")"
cleanup() {
    rm -rf "${temporary}" "${lock}"
}
trap cleanup EXIT HUP INT TERM

download="$(GOWORK=off go mod download -json \
    "github.com/go-gremlins/gremlins@${GREMLINS_VERSION}")"
source_directory="$(jq -er '.Dir' <<<"${download}")"
jq -e \
    --arg sum "${GREMLINS_SUM}" \
    --arg gomod_sum "${GREMLINS_GOMOD_SUM}" '
        .Sum == $sum and .GoModSum == $gomod_sum
    ' <<<"${download}" >/dev/null

cp -R "${source_directory}" "${temporary}/source"
chmod -R u+w "${temporary}/source"
patch --batch --forward -d "${temporary}/source" -p1 <"${semantic_patch}" >&2
patch --batch --forward -d "${temporary}/source" -p1 <"${coverage_patch}" >&2
patch --batch --forward -d "${temporary}/source" -p1 <"${diff_patch}" >&2
(
    cd "${temporary}/source"
    GOWORK=off go build -trimpath -buildvcs=false \
        -o "${temporary}/golib-gremlins" ./cmd/gremlins
)

rm -rf "${artifact}"
mkdir "${artifact}"
mv "${temporary}/golib-gremlins" "${binary}"
chmod 0755 "${binary}"
printf '%s\n' "${binary}"
