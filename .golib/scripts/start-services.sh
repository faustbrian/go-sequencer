#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    printf 'usage: %s <module-directory> <environment-file> <state-file>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
environment_file="$2"
state_file="$3"
slug="$(printf '%s' "${module}" | tr '/.' '--')-${RANDOM}"

# shellcheck source=/dev/null
source "${root}/.golib/versions.env"
: >"${environment_file}"
: >"${state_file}"

services="$(jq -r --arg directory "${module}" \
    '.modules[] | select(.directory == $directory) | .required_services[]' \
    "${root}/modules.json")"
[[ -n "${services}" ]] || exit 0
command -v docker >/dev/null || {
    printf 'Docker is required by %s for: %s\n' "${module}" "${services}" >&2
    exit 1
}

record() {
    printf '%s\n' "$1" >>"${state_file}"
}

record_resource() {
    local kind="$1"
    shift
    printf '%s' "${kind}" >>"${state_file}"
    printf '\t%s' "$@" >>"${state_file}"
    printf '\n' >>"${state_file}"
}

wait_for() {
    local container="$1"
    shift
    for _ in {1..90}; do
        if docker exec "${container}" "$@" >/dev/null 2>&1; then
            return 0
        fi
        if [[ "$(docker inspect --format '{{.State.Running}}' "${container}" 2>/dev/null || true)" != "true" ]]; then
            break
        fi
        sleep 1
    done
    docker logs "${container}" >&2 || true
    printf 'service did not become healthy: %s\n' "${container}" >&2
    exit 1
}

published_port() {
    docker port "$1" "$2/tcp" | tail -1 | sed 's/.*://'
}

acquire_rabbitstream_lock() {
    local lock owner started
    lock="${TMPDIR:-/tmp}/golib-rabbitstream-fixture.lock"
    started="${SECONDS}"
    while ! mkdir "${lock}" 2>/dev/null; do
        owner="$(cat "${lock}/owner" 2>/dev/null || true)"
        if [[ "${owner}" =~ ^[0-9]+$ ]] && ! kill -0 "${owner}" 2>/dev/null; then
            rm -f "${lock}/owner"
            rmdir "${lock}" 2>/dev/null || true
            continue
        fi
        if ((SECONDS - started >= 1200)); then
            printf 'timed out waiting for the RabbitStream fixture lock\n' >&2
            exit 1
        fi
        sleep 1
    done
    printf '%s\n' "${PPID}" >"${lock}/owner"
    record_resource lock "${lock}" "${PPID}"
}

write_service_environment() {
    local name="$1"
    local value="$2"
    printf '%s=%q\n' "${name}" "${value}" >>"${environment_file}"
}

start_rabbitstream() {
    local mode="$1"
    local integration fixture_id standalone_project cluster_project tls_project
    local fixture_parent tls_runtime

    command -v openssl >/dev/null || {
        printf 'OpenSSL is required by %s for RabbitStream fixtures\n' "${module}" >&2
        exit 1
    }
    acquire_rabbitstream_lock
    integration="${root}/.golib/services/rabbitstream"
    fixture_id="codex-rabbitstream-${RANDOM}-$$"
    standalone_project="${fixture_id}-single"
    cluster_project="${fixture_id}-cluster"
    tls_project="${fixture_id}-tls"

    RABBITSTREAM_USER="rabbitstream-$(openssl rand -hex 8)"
    RABBITSTREAM_PASSWORD="$(openssl rand -hex 24)"
    RABBITSTREAM_ERLANG_COOKIE="$(openssl rand -hex 32)"
    RABBITSTREAM_RESTRICTED_USER="restricted-$(openssl rand -hex 8)"
    RABBITSTREAM_RESTRICTED_PASSWORD="$(openssl rand -hex 24)"
    export RABBITSTREAM_USER RABBITSTREAM_PASSWORD RABBITSTREAM_ERLANG_COOKIE
    export RABBITSTREAM_RESTRICTED_USER RABBITSTREAM_RESTRICTED_PASSWORD

    record_resource compose "${integration}" "${integration}/standalone-compose.yaml" \
        "${standalone_project}"
    (
        cd "${integration}"
        COMPOSE_PROJECT_NAME="${standalone_project}" ./standalone-setup.sh
    )
    write_service_environment RABBITSTREAM_TEST_HOST localhost
    write_service_environment RABBITSTREAM_TEST_PORT 15552
    write_service_environment RABBITSTREAM_TEST_USER "${RABBITSTREAM_USER}"
    write_service_environment RABBITSTREAM_TEST_PASSWORD "${RABBITSTREAM_PASSWORD}"
    write_service_environment RABBITSTREAM_TEST_RESTART_CONTAINER \
        "${standalone_project}-rabbit-1"
    write_service_environment RABBITSTREAM_TEST_PROXY_API http://127.0.0.1:18474
    write_service_environment RABBITSTREAM_TEST_PROXY_NAME rabbitstream

    [[ "${mode}" == "full" ]] || return 0

    record_resource compose "${integration}" "${integration}/compose.yaml" \
        "${cluster_project}"
    (
        cd "${integration}"
        COMPOSE_PROJECT_NAME="${cluster_project}" ./setup.sh
    )
    write_service_environment RABBITSTREAM_CLUSTER_PORTS 15561,15562,15563
    write_service_environment RABBITSTREAM_CLUSTER_CONTAINERS \
        "15561=${cluster_project}-rabbit1-1,15562=${cluster_project}-rabbit2-1,15563=${cluster_project}-rabbit3-1"
    write_service_environment RABBITSTREAM_CLUSTER_PROJECT "${cluster_project}"
    write_service_environment RABBITSTREAM_ERLANG_COOKIE "${RABBITSTREAM_ERLANG_COOKIE}"
    write_service_environment RABBITSTREAM_UPGRADE_IMAGE \
        "rabbitmq@sha256:397fde82bc04522d88680b57cbf5d70caae715a76c957404e52e3f0fa056b8f3"
    write_service_environment RABBITSTREAM_UPGRADE_FROM_VERSION 4.3.4
    write_service_environment RABBITSTREAM_UPGRADE_TO_VERSION 4.3.5

    fixture_parent="$(mktemp -d "${TMPDIR:-/tmp}/golib-rabbitstream.XXXXXX")"
    tls_runtime="${fixture_parent}/tls"
    record_resource directory "${fixture_parent}"
    record_resource compose "${integration}" "${integration}/tls-compose.yaml" \
        "${tls_project}"
    (
        cd "${integration}"
        RABBITSTREAM_TLS_RUNTIME="${tls_runtime}" \
            COMPOSE_PROJECT_NAME="${tls_project}" ./tls-setup.sh
    )
    write_service_environment RABBITSTREAM_TLS_HOST localhost
    write_service_environment RABBITSTREAM_TLS_PORT 15571
    write_service_environment RABBITSTREAM_TLS_USER "${RABBITSTREAM_USER}"
    write_service_environment RABBITSTREAM_TLS_PASSWORD "${RABBITSTREAM_PASSWORD}"
    write_service_environment RABBITSTREAM_TLS_RUNTIME "${tls_runtime}"
    write_service_environment RABBITSTREAM_RESTRICTED_USER \
        "${RABBITSTREAM_RESTRICTED_USER}"
    write_service_environment RABBITSTREAM_RESTRICTED_PASSWORD \
        "${RABBITSTREAM_RESTRICTED_PASSWORD}"
}

while IFS= read -r service; do
    case "${service}" in
        postgresql)
            container="golib-postgres-${slug}"
            postgres_version="${POSTGRES_IMAGE#postgres:}"
            postgres_version="${postgres_version%-alpine}"
            docker run --detach --name "${container}" -p 127.0.0.1::5432 \
                -e POSTGRES_USER=golib -e POSTGRES_PASSWORD=golib \
                -e POSTGRES_DB=golib "${POSTGRES_IMAGE}" >/dev/null
            record "${container}"
            wait_for "${container}" pg_isready -U golib -d golib
            port="$(published_port "${container}" 5432)"
            cat >>"${environment_file}" <<EOF
POSTGRES_URL=postgres://golib:golib@127.0.0.1:${port}/golib?sslmode=disable
POSTGRES_VERSION=${postgres_version}
OUTBOX_POSTGRES_VERSION=${postgres_version}
STATE_MACHINE_POSTGRES_VERSION=${postgres_version}
FEATURE_FLAGS_POSTGRES_DSN=postgres://golib:golib@127.0.0.1:${port}/golib?sslmode=disable
TEST_DATABASE_URL=postgres://golib:golib@127.0.0.1:${port}/golib?sslmode=disable
DATABASE_URL=postgres://golib:golib@127.0.0.1:${port}/golib?sslmode=disable
TEMPORAL_POSTGRES_DSN=postgres://golib:golib@127.0.0.1:${port}/golib?sslmode=disable
EOF
            ;;
        valkey)
            container="golib-valkey-${slug}"
            docker run --detach --name "${container}" -p 127.0.0.1::6379 \
                "${VALKEY_IMAGE}" >/dev/null
            record "${container}"
            wait_for "${container}" valkey-cli ping
            port="$(published_port "${container}" 6379)"
            cat >>"${environment_file}" <<EOF
VALKEY_ADDR=127.0.0.1:${port}
VALKEY_ADDRESS=127.0.0.1:${port}
FEATURE_FLAGS_VALKEY_ADDRESS=127.0.0.1:${port}
TEST_VALKEY_ADDRESS=127.0.0.1:${port}
CACHE_VALKEY_IMAGE=${VALKEY_IMAGE}
EOF
            ;;
        redis)
            container="golib-redis-${slug}"
            docker run --detach --name "${container}" -p 127.0.0.1::6379 \
                "${REDIS_IMAGE}" >/dev/null
            record "${container}"
            wait_for "${container}" redis-cli ping
            port="$(published_port "${container}" 6379)"
            cat >>"${environment_file}" <<EOF
REDIS_ADDR=127.0.0.1:${port}
TEST_REDIS_ADDRESS=127.0.0.1:${port}
CACHE_REDIS_IMAGE=${REDIS_IMAGE}
EOF
            ;;
        nats)
            container="golib-nats-${slug}"
            docker run --detach --name "${container}" -p 127.0.0.1::4222 \
                "${NATS_IMAGE}" >/dev/null
            record "${container}"
            sleep 2
            port="$(published_port "${container}" 4222)"
            printf 'NATS_URL=nats://127.0.0.1:%s\n' "${port}" >>"${environment_file}"
            ;;
        nsq)
            container="golib-nsq-${slug}"
            docker run --detach --name "${container}" -p 127.0.0.1::4150 \
                "${NSQ_IMAGE}" /nsqd --broadcast-address=127.0.0.1 >/dev/null
            record "${container}"
            sleep 2
            port="$(published_port "${container}" 4150)"
            printf 'NSQD_TCP_ADDRESS=127.0.0.1:%s\n' "${port}" >>"${environment_file}"
            ;;
        rabbitmq)
            container="golib-rabbitmq-${slug}"
            docker run --detach --name "${container}" --hostname "${container}" \
                --user rabbitmq \
                -p 127.0.0.1::5672 \
                "${RABBITMQ_IMAGE}" >/dev/null
            record "${container}"
            wait_for "${container}" rabbitmq-diagnostics -q ping
            port="$(published_port "${container}" 5672)"
            printf 'RABBITMQ_URL=amqp://guest:guest@127.0.0.1:%s/\n' \
                "${port}" >>"${environment_file}"
            ;;
        rabbitstream)
            start_rabbitstream full
            ;;
        rabbitstream-standalone)
            start_rabbitstream standalone
            ;;
        opensearch)
            # shellcheck source=/dev/null
            source "${root}/.golib/services/opensearch/opensearch-images.env"
            container="golib-opensearch-${slug}"
            opensearch_image="${opensearch_image_repository}@${opensearch_new_digest}"
            docker run --detach --name "${container}" -p 127.0.0.1::9200 \
                --cpus=1 --memory=1g --pids-limit=512 \
                --ulimit nofile=1024:1024 \
                -e discovery.type=single-node \
                -e DISABLE_SECURITY_PLUGIN=true \
                -e OPENSEARCH_JAVA_OPTS='-Xms512m -Xmx512m' \
                "${opensearch_image}" >/dev/null
            record "${container}"
            port="$(published_port "${container}" 9200)"
            ready=0
            for _ in {1..120}; do
                if curl --connect-timeout 2 --max-time 5 --fail --silent \
                    "http://127.0.0.1:${port}/" >/dev/null; then
                    ready=1
                    break
                fi
                if [[ "$(docker inspect --format '{{.State.Running}}' "${container}" 2>/dev/null || true)" != "true" ]]; then
                    break
                fi
                sleep 1
            done
            if [[ "${ready}" -ne 1 ]]; then
                docker logs "${container}" >&2 || true
                printf 'service did not become healthy: %s\n' "${container}" >&2
                exit 1
            fi
            cat >>"${environment_file}" <<EOF
OPENSEARCH_URL=http://127.0.0.1:${port}
OPENSEARCH_EXPECTED_VERSION=${opensearch_new_version}
EOF
            ;;
        *)
            printf 'unsupported required service %s for %s\n' \
                "${service}" "${module}" >&2
            exit 1
            ;;
    esac
done <<<"${services}"
