#!/usr/bin/env bash

mutation_scratch_process_start() {
    LC_ALL=C ps -o lstart= -p "$1" 2>/dev/null | awk '{$1=$1; print}'
}

mutation_scratch_owner_is_abandoned() {
    local candidate="$1"
    local expected_host="$2"
    local owner_file owner_version owner_host owner_pid owner_start owner_run_id
    local current_start
    owner_file="${candidate}/.mutation-owner"
    [[ -f "${owner_file}" && ! -L "${owner_file}" ]] || return 1
    IFS=$'\t' read -r owner_version owner_host owner_pid owner_start \
        owner_run_id <"${owner_file}" || return 1
    [[ "${owner_version}" == "1" &&
        "${owner_host}" == "${expected_host}" &&
        "${owner_pid}" =~ ^[1-9][0-9]*$ &&
        "${owner_start}" != "" &&
        "${owner_run_id}" == "$(basename "${candidate}")" ]] || return 1
    current_start="$(mutation_scratch_process_start "${owner_pid}")"
    if [[ -n "${current_start}" ]]; then
        [[ "${current_start}" != "${owner_start}" ]]
        return
    fi
    if kill -0 "${owner_pid}" 2>/dev/null; then
        return 1
    fi
    return 0
}

mutation_scratch_recover_abandoned() {
    local recovery_artifact="$1"
    local recovery_host candidate claim
    [[ -d "${recovery_artifact}" && ! -L "${recovery_artifact}" ]] || return 0
    recovery_host="$(hostname)"
    while IFS= read -r candidate; do
        case "${candidate}" in
            "${recovery_artifact}"/mutation-run-*) ;;
            *) continue ;;
        esac
        [[ "$(dirname "${candidate}")" == "${recovery_artifact}" &&
            -d "${candidate}" && ! -L "${candidate}" ]] || continue
        mutation_scratch_owner_is_abandoned \
            "${candidate}" "${recovery_host}" || continue
        claim="${candidate}/.mutation-recovery-claim"
        mkdir "${claim}" 2>/dev/null || continue
        if mutation_scratch_owner_is_abandoned \
            "${candidate}" "${recovery_host}"; then
            find "${candidate}" -depth -delete
        else
            rmdir "${claim}"
        fi
    done < <(
        find "${recovery_artifact}" -mindepth 1 -maxdepth 1 -type d \
            -name 'mutation-run-*' -print
    )
}

mutation_scratch_remove_owned_run() {
    local owner_file owner_version owner_host owner_pid owner_start owner_run_id
    [[ -n "${artifact:-}" && -n "${run_directory:-}" ]] || return 0
    case "${run_directory}" in
        "${artifact}"/mutation-run-*) ;;
        *)
            printf 'refusing to remove unexpected mutation run: %s\n' \
                "${run_directory}" >&2
            return 1
            ;;
    esac
    [[ "$(dirname "${run_directory}")" == "${artifact}" &&
        -d "${run_directory}" && ! -L "${run_directory}" ]] || return 0
    owner_file="${run_directory}/.mutation-owner"
    if [[ -e "${owner_file}" || -L "${owner_file}" ]]; then
        IFS=$'\t' read -r owner_version owner_host owner_pid owner_start \
            owner_run_id <"${owner_file}" || {
            printf 'refusing to remove mutation run without a valid owner: %s\n' \
                "${run_directory}" >&2
            return 1
        }
        if [[ "${owner_version}" != "1" ||
            "${owner_host}" != "${mutation_owner_host}" ||
            "${owner_pid}" != "$$" ||
            "${owner_start}" != "${mutation_owner_start}" ||
            "${owner_run_id}" != "$(basename "${run_directory}")" ]]; then
            printf 'refusing to remove mutation run with a different owner: %s\n' \
                "${run_directory}" >&2
            return 1
        fi
    elif [[ "${mutation_owner_run_id:-}" != \
        "$(basename "${run_directory}")" ]]; then
        printf 'refusing to remove unmarked mutation run: %s\n' \
            "${run_directory}" >&2
        return 1
    fi
    find "${run_directory}" -depth -delete
    run_directory=""
}

mutation_scratch_package_cache() {
    local slug="$1"
    case "${slug}" in
        ""|"."|".."|*/*)
            printf 'invalid mutation package cache slug: %s\n' "${slug}" >&2
            return 1
            ;;
    esac
    active_build_cache="$(
        mktemp -d "${run_directory}/${slug}.go-cache-XXXXXXXX"
    )"
}

mutation_scratch_cleanup_package_cache() {
    [[ -n "${active_build_cache:-}" ]] || return 0
    case "${active_build_cache}" in
        "${run_directory}"/*.go-cache-*) ;;
        *)
            printf 'refusing to remove unexpected mutation cache: %s\n' \
                "${active_build_cache}" >&2
            return 1
            ;;
    esac
    [[ "$(dirname "${active_build_cache}")" == "${run_directory}" &&
        -d "${active_build_cache}" &&
        ! -L "${active_build_cache}" ]] || return 0
    find "${active_build_cache}" -depth -delete
    active_build_cache=""
}

mutation_scratch_on_exit() {
    local status=$?
    trap - EXIT HUP INT TERM
    if ! mutation_scratch_remove_owned_run && [[ "${status}" -eq 0 ]]; then
        status=1
    fi
    exit "${status}"
}

mutation_scratch_on_signal() {
    exit "$1"
}

mutation_scratch_install_traps() {
    trap mutation_scratch_on_exit EXIT
    trap 'mutation_scratch_on_signal 129' HUP
    trap 'mutation_scratch_on_signal 130' INT
    trap 'mutation_scratch_on_signal 143' TERM
}

mutation_scratch_initialize() {
    local owner_tmp
    artifact="$1"
    mkdir -p "${artifact}"
    mutation_scratch_recover_abandoned "${artifact}"
    run_directory="$(mktemp -d "${artifact}/mutation-run-XXXXXXXX")"
    mutation_owner_run_id="$(basename "${run_directory}")"
    mutation_scratch_install_traps
    mutation_owner_host="$(hostname)"
    mutation_owner_start="$(mutation_scratch_process_start "$$")"
    [[ -n "${mutation_owner_start}" ]] || {
        printf 'cannot identify mutation scratch owner process\n' >&2
        return 1
    }
    owner_tmp="$(mktemp "${run_directory}/.mutation-owner.XXXXXXXX")"
    printf '1\t%s\t%s\t%s\t%s\n' \
        "${mutation_owner_host}" "$$" "${mutation_owner_start}" \
        "$(basename "${run_directory}")" >"${owner_tmp}"
    mv "${owner_tmp}" "${run_directory}/.mutation-owner"
}
