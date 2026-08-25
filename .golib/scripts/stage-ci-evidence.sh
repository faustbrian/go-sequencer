#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    printf 'usage: %s <module-directory> <destination> <outcome>\n' "$0" >&2
    exit 2
fi

root="${GOLIB_ROOT:-$(git rev-parse --show-toplevel)}"
module="$1"
destination="$2"
outcome="$3"
script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source="${root}/.artifacts"
if [[ "${module}" != "." ]]; then
    source="${source}/${module}"
fi
case "${outcome}" in
    success|failure|cancelled|skipped) ;;
    *)
        printf 'invalid CI contract outcome for %s: %s\n' \
            "${module}" "${outcome}" >&2
        exit 2
        ;;
esac
source_available=0
if [[ -d "${source}" && ! -L "${source}" ]]; then
    source="$(cd "${source}" && pwd -P)"
    source_available=1
elif [[ -e "${source}" || -L "${source}" ]]; then
    printf 'invalid attributable evidence source for %s\n' "${module}" >&2
    exit 1
fi
destination_name="$(basename "${destination}")"
destination_parent="$(cd "$(dirname "${destination}")" && pwd -P)" || {
    printf 'CI evidence destination parent is unavailable: %s\n' \
        "${destination}" >&2
    exit 1
}
[[ "${destination_name}" != "." && "${destination_name}" != ".." ]] || {
    printf 'invalid CI evidence destination: %s\n' "${destination}" >&2
    exit 1
}
destination="${destination_parent}/${destination_name}"
[[ ! -e "${destination}" && ! -L "${destination}" ]] || {
    printf 'CI evidence destination already exists: %s\n' \
        "${destination}" >&2
    exit 1
}
if [[ "${source_available}" -eq 1 ]]; then
    case "${destination}" in
        "${source}"|"${source}"/*)
            printf 'CI evidence destination must be outside its source: %s\n' \
                "${destination}" >&2
            exit 1
            ;;
    esac
fi

# Cancellation can bypass the mutation runner's signal trap. Reclaim only
# scratch directories whose recorded owner process is demonstrably gone.
# shellcheck disable=SC1091
. "${script_directory}/internal/mutation-scratch.sh"
if [[ "${source_available}" -eq 1 ]]; then
    mutation_scratch_recover_abandoned "${source}"
fi

mkdir -p "${destination}"
inventory="$(mktemp "${TMPDIR:-/tmp}/golib-ci-evidence.XXXXXXXX")"
stage_complete=0
cleanup() {
    local exit_code=$?
    trap - EXIT HUP INT TERM
    find "${inventory}" -delete 2>/dev/null || true
    if [[ "${stage_complete}" -eq 0 && -d "${destination}" &&
        ! -L "${destination}" ]]; then
        find "${destination}" -depth -delete 2>/dev/null || true
    fi
    exit "${exit_code}"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

files=0
if [[ "${source_available}" -eq 1 ]]; then
    find "${source}" -mindepth 1 \
        \( -type d \( \
            -name 'mutation-run-*' -o \
            -name '*.go-cache-*' -o \
            -name 'go-cache' -o \
            -name '.locks' -o \
            -name '*.lock' -o \
            -name '*.tmp.*' \
        \) -prune \) -o -print0 >"${inventory}"
else
    : >"${inventory}"
fi
while IFS= read -r -d '' candidate; do
    relative="${candidate#"${source}/"}"
    basename="${candidate##*/}"
    case "${basename}" in
        *.tmp.*|*.lock)
            continue
            ;;
    esac
    if [[ -L "${candidate}" ]]; then
        printf 'refusing to stage symbolic link: %s\n' "${relative}" >&2
        exit 1
    fi
    if [[ -d "${candidate}" ]]; then
        mkdir -p "${destination}/${relative}"
        continue
    fi
    if [[ ! -f "${candidate}" ]]; then
        printf 'refusing to stage unsupported artifact: %s\n' \
            "${relative}" >&2
        exit 1
    fi
    mkdir -p "$(dirname "${destination}/${relative}")"
    cp -p "${candidate}" "${destination}/${relative}"
    files=$((files + 1))
done <"${inventory}"

jq -n \
    --arg module "${module}" \
    --arg outcome "${outcome}" \
    --arg repository "${GITHUB_REPOSITORY:-}" \
    --arg run_id "${GITHUB_RUN_ID:-}" \
    --arg run_attempt "${GITHUB_RUN_ATTEMPT:-}" \
    --arg revision "${GITHUB_SHA:-}" \
    '{
        schema_version: 1,
        module: $module,
        outcome: $outcome,
        repository: $repository,
        run_id: $run_id,
        run_attempt: $run_attempt,
        revision: $revision
    }' >"${destination}/ci-result.json"
files=$((files + 1))
stage_complete=1
printf '[%s] staged %d durable evidence files\n' "${module}" "${files}"
