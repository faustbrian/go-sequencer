#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    printf 'usage: %s <module-directory> <gate>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
gate="$2"
artifact="${root}/.artifacts/${module}/evidence"
lock_root="${artifact}/.locks"
lock="${lock_root}/${gate}.lock"
lock_acquired=0

cleanup() {
    if [[ "${lock_acquired}" -eq 1 ]]; then
        owner="$(readlink "${lock}" 2>/dev/null || true)"
        if [[ "${owner}" == "$$" ]]; then
            rm -f "${lock}"
        fi
    fi
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "${lock_root}"
while ! ln -s "$$" "${lock}" 2>/dev/null; do
    if [[ -L "${lock}" ]]; then
        owner="$(readlink "${lock}" 2>/dev/null || true)"
        if [[ "${owner}" =~ ^[0-9]+$ ]] && ! kill -0 "${owner}" 2>/dev/null; then
            rm -f "${lock}"
            continue
        fi
    elif [[ -d "${lock}" ]]; then
        owner="$(cat "${lock}/owner" 2>/dev/null || true)"
        if [[ "${owner}" =~ ^[0-9]+$ ]] && ! kill -0 "${owner}" 2>/dev/null; then
            rm -f "${lock}/owner"
            rmdir "${lock}" 2>/dev/null || true
            continue
        fi
        if [[ -z "${owner}" ]] && rmdir "${lock}" 2>/dev/null; then
            continue
        fi
    elif [[ -f "${lock}" ]]; then
        owner="$(cat "${lock}" 2>/dev/null || true)"
        if [[ "${owner}" =~ ^[0-9]+$ ]] && ! kill -0 "${owner}" 2>/dev/null; then
            rm -f "${lock}"
            continue
        fi
        if [[ -z "${owner}" ]]; then
            rm -f "${lock}"
            continue
        fi
    fi
    sleep 0.05
done
lock_acquired=1

input_digest="$("${root}/.golib/scripts/gate-input-digest.sh" "${gate}" "${module}")"
digest_artifact="${artifact}/by-input/${gate}"
evidence="${digest_artifact}/${input_digest}.json"
log="${digest_artifact}/${input_digest}.log"
legacy_evidence="${artifact}/${gate}.json"
legacy_log="${artifact}/${gate}.log"

if [[ ! -f "${evidence}" || ! -f "${log}" ]]; then
    evidence="${legacy_evidence}"
    log="${legacy_log}"
fi

[[ -f "${evidence}" ]] || {
    printf '[%s] missing %s evidence\n' "${module}" "${gate}" >&2
    exit 1
}
[[ -f "${log}" ]] || {
    printf '[%s] missing %s evidence log\n' "${module}" "${gate}" >&2
    exit 1
}

log_sha256="$(shasum -a 256 "${log}" | awk '{print $1}')"
jq -e \
    --arg module "${module}" \
    --arg gate "${gate}" \
    --arg input_digest "${input_digest}" \
    --arg log_sha256 "${log_sha256}" \
    '
        .schema_version == 1 and
        .module == $module and
        .gate == $gate and
        (
            .result == "passed" or
            .result == "not_applicable" or
            ($gate == "nilaway" and .result == "advisory")
        ) and
        .exit_code == 0 and
        .input_digest == $input_digest and
        .completed_input_digest == $input_digest and
        .log_sha256 == $log_sha256
    ' "${evidence}" >/dev/null || {
    printf '[%s] stale or invalid %s evidence\n' "${module}" "${gate}" >&2
    exit 1
}

if [[ "$(jq -r '.result' "${evidence}")" == "advisory" ]] &&
    ! grep -Eq 'NilAway advisory exit status: [1-9][0-9]*$' "${log}"; then
    printf '[%s] stale or invalid %s evidence\n' "${module}" "${gate}" >&2
    exit 1
fi

if [[ "$(jq -r '.result' "${evidence}")" == "not_applicable" ]] &&
    ! grep -Fq ': not applicable by catalog policy' "${log}"; then
    printf '[%s] stale or invalid %s evidence\n' "${module}" "${gate}" >&2
    exit 1
fi
