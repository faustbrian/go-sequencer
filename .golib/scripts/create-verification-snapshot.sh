#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    printf 'usage: %s <source-repository> <snapshot-directory>\n' "$0" >&2
    exit 2
fi

source_repository="$(cd "$1" && pwd)"
snapshot_directory="$2"
if [[ -e "${snapshot_directory}" ]]; then
    printf 'snapshot destination already exists: %s\n' \
        "${snapshot_directory}" >&2
    exit 1
fi

git -C "${source_repository}" rev-parse --verify HEAD >/dev/null
git clone --shared --no-checkout --quiet \
    "${source_repository}" "${snapshot_directory}"
git -C "${snapshot_directory}" config --local core.fsmonitor false
git -C "${snapshot_directory}" checkout --detach --quiet \
    "$(git -C "${source_repository}" rev-parse HEAD)"

patch="$(mktemp "${TMPDIR:-/tmp}/golib-snapshot-patch.XXXXXX")"
cleanup() {
    rm -f "${patch}"
}
trap cleanup EXIT HUP INT TERM
git -C "${source_repository}" diff --binary --full-index HEAD -- >"${patch}"
if [[ -s "${patch}" ]]; then
    git -C "${snapshot_directory}" apply --binary "${patch}"
fi
git -C "${snapshot_directory}" ls-files --deleted -z |
    git -C "${snapshot_directory}" update-index --remove -z --stdin

# A staged addition remains tracked even when a machine-wide ignore rule
# matches its name. Mirror that index classification so verification input
# discovery sees the same files in the snapshot and source repository.
while IFS= read -r -d '' path; do
    git -C "${snapshot_directory}" add -N -f -- "${path}"
done < <(
    git -C "${source_repository}" \
        diff --cached --name-only --diff-filter=A -z
)

while IFS= read -r -d '' path; do
    mkdir -p "${snapshot_directory}/$(dirname "${path}")"
    cp -pP \
        "${source_repository}/${path}" \
        "${snapshot_directory}/${path}"
done < <(
    git -C "${source_repository}" \
        ls-files --others --exclude-standard -z
)

mkdir -p "${source_repository}/.artifacts"
ln -s "${source_repository}/.artifacts" \
    "${snapshot_directory}/.artifacts"
printf '%s\n' '/.artifacts' >>"${snapshot_directory}/.git/info/exclude"
