#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
    printf 'usage: %s <profile> <identity> <destination> <input-digest> <test-tags>\n' "$0" >&2
    exit 2
fi

profile="$1"
identity="$2"
destination="$3"
expected_input_digest="$4"
expected_test_tags="$5"

[[ -s "${profile}" && -s "${identity}" ]] || exit 1

profile_sha256="$(shasum -a 256 "${profile}" | awk '{print $1}')"
elapsed="$(jq -er \
    --arg input_digest "${expected_input_digest}" \
    --arg test_tags "${expected_test_tags}" \
    --arg profile_sha256 "${profile_sha256}" '
        select(
            .schema_version == 1 and
            .input_digest == $input_digest and
            .test_tags == $test_tags and
            .profile_sha256 == $profile_sha256 and
            (.elapsed | type == "string") and
            (.elapsed | test("^[1-9][0-9]*s$"))
        )
        | .elapsed
    ' "${identity}")" || exit 1

temporary="$(mktemp "${destination}.tmp.XXXXXX")"
cleanup() {
    rm -f "${temporary}"
}
trap cleanup EXIT HUP INT TERM
cp "${profile}" "${temporary}"
copied_sha256="$(shasum -a 256 "${temporary}" | awk '{print $1}')"
[[ "${copied_sha256}" == "${profile_sha256}" ]] || exit 1
mv "${temporary}" "${destination}"
printf '%s\n' "${elapsed}"
