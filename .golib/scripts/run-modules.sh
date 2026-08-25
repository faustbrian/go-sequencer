#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
    printf 'usage: %s <gate> <--all|--modules LIST>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
gate="$1"
shift
case "$1" in
    --all)
        selection="$(jq -r '.modules[].directory' "${root}/modules.json")"
        ;;
    --modules)
        [[ $# -eq 2 ]] || exit 2
        selection="${2//,/\\n}"
        ;;
    *)
        printf 'unknown module selection: %s\n' "$1" >&2
        exit 2
        ;;
esac

while IFS= read -r module; do
    [[ -n "${module}" ]] || continue
    (
        task="$(mktemp -d "${TMPDIR:-/tmp}/golib-services.XXXXXX")"
        environment="${task}/environment"
        state="${task}/state"
        # shellcheck disable=SC2329 # Invoked by the subshell EXIT trap.
        cleanup() {
            "${root}/.golib/scripts/stop-services.sh" "${state}" || true
            find "${task}" -depth -delete 2>/dev/null || true
        }
        trap cleanup EXIT HUP INT TERM
        "${root}/.golib/scripts/start-services.sh" "${module}" "${environment}" "${state}"
        set -a
        # shellcheck source=/dev/null
        source "${environment}"
        set +a
        "${root}/.golib/scripts/check-module.sh" "${module}" "${gate}"
    )
done <<<"${selection}"
