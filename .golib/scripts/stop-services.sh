#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <state-file>\n' "$0" >&2
    exit 2
fi

state_file="$1"
[[ -f "${state_file}" ]] || exit 0
cleanup_timeout="${GOLIB_DOCKER_CLEANUP_TIMEOUT_SECONDS:-30}"
if [[ ! "${cleanup_timeout}" =~ ^[1-9][0-9]*$ ]]; then
    printf 'GOLIB_DOCKER_CLEANUP_TIMEOUT_SECONDS must be a positive integer\n' >&2
    exit 2
fi

remove_container() {
    local container="$1"
    local started="${SECONDS}"

    docker rm --force "${container}" >/dev/null 2>&1 &
    local docker_pid=$!
    while kill -0 "${docker_pid}" >/dev/null 2>&1; do
        if ((SECONDS - started >= cleanup_timeout)); then
            kill -TERM "${docker_pid}" >/dev/null 2>&1 || true
            sleep 0.1
            kill -KILL "${docker_pid}" >/dev/null 2>&1 || true
            wait "${docker_pid}" >/dev/null 2>&1 || true
            printf 'timed out removing Docker container %s after %ss\n' \
                "${container}" "${cleanup_timeout}" >&2
            return
        fi
        sleep 0.1
    done
    wait "${docker_pid}" >/dev/null 2>&1 || true
}

remove_compose_project() {
    local directory="$1"
    local file="$2"
    local project="$3"
    local started="${SECONDS}"

    env RABBITSTREAM_USER=cleanup RABBITSTREAM_PASSWORD=cleanup \
        RABBITSTREAM_ERLANG_COOKIE=cleanup \
        docker compose --project-directory "${directory}" -f "${file}" \
        -p "${project}" down --volumes --remove-orphans >/dev/null 2>&1 &
    local docker_pid=$!
    while kill -0 "${docker_pid}" >/dev/null 2>&1; do
        if ((SECONDS - started >= cleanup_timeout)); then
            kill -TERM "${docker_pid}" >/dev/null 2>&1 || true
            sleep 0.1
            kill -KILL "${docker_pid}" >/dev/null 2>&1 || true
            wait "${docker_pid}" >/dev/null 2>&1 || true
            printf 'timed out removing Docker Compose project %s after %ss\n' \
                "${project}" "${cleanup_timeout}" >&2
            return
        fi
        sleep 0.1
    done
    wait "${docker_pid}" >/dev/null 2>&1 || true
}

remove_owned_directory() {
    local directory="$1"
    case "$(basename "${directory}")" in
        golib-rabbitstream.*) ;;
        *)
            printf 'refusing to remove unexpected service directory: %s\n' \
                "${directory}" >&2
            return 1
            ;;
    esac
    [[ -d "${directory}" ]] || return
    chmod -R u+w "${directory}" 2>/dev/null || true
    find "${directory}" -depth -delete
}

lock_path=""
lock_owner=""

while IFS=$'\t' read -r kind first second third; do
    [[ -n "${kind}" ]] || continue
    case "${kind}" in
        container) remove_container "${first}" ;;
        compose) remove_compose_project "${first}" "${second}" "${third}" ;;
        directory) remove_owned_directory "${first}" ;;
        lock)
            lock_path="${first}"
            lock_owner="${second}"
            ;;
        *) remove_container "${kind}" ;;
    esac
done <"${state_file}"

if [[ -n "${lock_path}" ]]; then
    case "$(basename "${lock_path}")" in
        golib-rabbitstream-fixture.lock) ;;
        *)
            printf 'refusing to remove unexpected service lock: %s\n' \
                "${lock_path}" >&2
            exit 1
            ;;
    esac
    current_owner="$(cat "${lock_path}/owner" 2>/dev/null || true)"
    if [[ "${current_owner}" == "${lock_owner}" ]]; then
        rm -f "${lock_path}/owner"
        rmdir "${lock_path}" 2>/dev/null || true
    fi
fi
