#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
    printf 'usage: %s <root> <gate> <jobs> <selection>\n' "$0" >&2
    exit 2
fi

root="$1"
gate="$2"
jobs="$3"
selection="$4"
snapshot_parents=()
snapshot_pids=()
snapshot_process_groups=()
lane_files=()
snapshot_processes_complete=0
cleanup_started=0

# shellcheck disable=SC2329 # Called from the EXIT cleanup trap.
terminate_snapshot_processes() {
    local attempt group running
    for group in "${snapshot_process_groups[@]}"; do
        kill -TERM -- "-${group}" 2>/dev/null || true
    done
    attempt=0
    while [[ "${attempt}" -lt 100 ]]; do
        running=0
        for group in "${snapshot_process_groups[@]}"; do
            if kill -0 -- "-${group}" 2>/dev/null; then
                running=1
                break
            fi
        done
        [[ "${running}" -eq 1 ]] || break
        sleep 0.05
        attempt=$((attempt + 1))
    done
    for group in "${snapshot_process_groups[@]}"; do
        if kill -0 -- "-${group}" 2>/dev/null; then
            kill -KILL -- "-${group}" 2>/dev/null || true
        fi
    done
}

# shellcheck disable=SC2329 # Invoked by the EXIT trap.
cleanup_snapshots() {
    local status=$? lane_file parent pid
    if [[ "${cleanup_started}" -eq 1 ]]; then
        return "${status}"
    fi
    cleanup_started=1
    trap '' HUP INT TERM
    if [[ "${snapshot_processes_complete}" -eq 0 ]]; then
        terminate_snapshot_processes
    fi
    for pid in "${snapshot_pids[@]}"; do
        wait "${pid}" 2>/dev/null || true
    done
    for parent in "${snapshot_parents[@]}"; do
        if [[ -d "${parent}" ]]; then
            find "${parent}" -depth -delete
        fi
    done
    for lane_file in "${lane_files[@]}"; do
        rm -f "${lane_file}"
    done
    return "${status}"
}

trap cleanup_snapshots EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

lane=0
while [[ "${lane}" -lt "${jobs}" ]]; do
    lane_files+=("$(mktemp "${TMPDIR:-/tmp}/golib-lane.XXXXXX")")
    lane=$((lane + 1))
done
lane=0
while IFS= read -r module; do
    [[ -n "${module}" ]] || continue
    printf '%s\n' "${module}" >>"${lane_files[${lane}]}"
    lane=$(((lane + 1) % jobs))
done <<<"${selection}"

printf 'parallel-safe verification snapshot jobs=%s\n' "${jobs}"
# Non-interactive job control gives every background lane its own process group.
set -m
lane=0
while [[ "${lane}" -lt "${jobs}" ]]; do
    snapshot_parent="$(
        mktemp -d "${TMPDIR:-/tmp}/golib-verification.XXXXXX"
    )"
    snapshot="${snapshot_parent}/repository"
    snapshot_parents+=("${snapshot_parent}")
    "${root}/.golib/scripts/create-verification-snapshot.sh" \
        "${root}" "${snapshot}"
    selected_modules="$(paste -sd, - <"${lane_files[${lane}]}")"
    (
        cd "${snapshot}"
        GOLIB_VERIFICATION_SNAPSHOT=1 \
            ./scripts/run-modules.sh \
            "${gate}" --jobs 1 --modules "${selected_modules}"
    ) &
    snapshot_pids+=("$!")
    snapshot_process_groups+=("$!")
    lane=$((lane + 1))
done

status=0
for pid in "${snapshot_pids[@]}"; do
    if ! wait "${pid}"; then
        status=1
    fi
done
snapshot_processes_complete=1
for group in "${snapshot_process_groups[@]}"; do
    if kill -0 -- "-${group}" 2>/dev/null; then
        snapshot_processes_complete=0
        status=1
        break
    fi
done
if [[ "${status}" -eq 0 ]]; then
    while IFS= read -r module; do
        [[ -n "${module}" ]] || continue
        if [[ "${gate}" == "check" ]]; then
            "${root}/.golib/scripts/audit-goals.sh" "${module}" >/dev/null
        else
            "${root}/.golib/scripts/verify-gate-evidence.sh" \
                "${module}" "${gate}"
        fi
    done <<<"${selection}"
fi
exit "${status}"
