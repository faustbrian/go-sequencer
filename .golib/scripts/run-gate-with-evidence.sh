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
legacy_evidence="${artifact}/${gate}.json"
legacy_log="${artifact}/${gate}.log"
lock_root="${artifact}/.locks"
lock="${lock_root}/${gate}.lock"
temporary_evidence=""
temporary_log=""
temporary_legacy_evidence="${legacy_evidence}.tmp.$$"
temporary_legacy_log="${legacy_log}.tmp.$$"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
execution_revision="$(git rev-parse HEAD)"
lock_acquired=0

# shellcheck disable=SC2329 # Invoked by the signal and exit trap.
cleanup() {
    if [[ -n "${temporary_evidence}" ]]; then
        rm -f "${temporary_evidence}"
    fi
    if [[ -n "${temporary_log}" ]]; then
        rm -f "${temporary_log}"
    fi
    rm -f "${temporary_legacy_evidence}" "${temporary_legacy_log}"
    if [[ "${lock_acquired}" -eq 1 ]]; then
        owner="$(readlink "${lock}" 2>/dev/null || true)"
        if [[ "${owner}" == "$$" ]]; then
            rm -f "${lock}"
        fi
    fi
}

forward_gate_output() {
    local line output_open=1
    trap '' PIPE
    while IFS= read -r line || [[ -n "${line}" ]]; do
        if [[ "${output_open}" -eq 1 ]] &&
            ! printf '%s\n' "${line}" 2>/dev/null; then
            output_open=0
        fi
    done
    return 0
}

emit_evidence_status() (
    trap '' PIPE
    printf '[%s] %s evidence: %s\n' "$1" "$2" "$3" 2>/dev/null || true
)

trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [[ -x "${root}/.golib/scripts/internal/migrate-runner-isolation-evidence.sh" ]]; then
    "${root}/.golib/scripts/internal/migrate-runner-isolation-evidence.sh" \
        "${module}" "${gate}"
fi
mkdir -p "${artifact}"
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
temporary_evidence="${evidence}.tmp.$$"
temporary_log="${log}.tmp.$$"
mkdir -p "${digest_artifact}"
rm -f "${temporary_evidence}" "${temporary_log}"

source_evidence="${evidence}"
source_log="${log}"
if [[ ! -f "${source_evidence}" || ! -f "${source_log}" ]]; then
    source_evidence="${legacy_evidence}"
    source_log="${legacy_log}"
fi
if [[ -f "${source_evidence}" && -f "${source_log}" ]]; then
    recorded_log_sha256="$(jq -er '.log_sha256' "${source_evidence}" 2>/dev/null || true)"
    current_log_sha256="$(shasum -a 256 "${source_log}" | awk '{print $1}')"
    if jq -e \
        --arg module "${module}" \
        --arg gate "${gate}" \
        --arg input_digest "${input_digest}" \
        --arg log_sha256 "${current_log_sha256}" \
        '
            .schema_version == 1 and
            .module == $module and
            .gate == $gate and
            (.result == "passed" or .result == "not_applicable" or .result == "advisory") and
            .exit_code == 0 and
            .input_digest == $input_digest and
            .completed_input_digest == $input_digest and
            .log_sha256 == $log_sha256
        ' "${source_evidence}" >/dev/null 2>&1 &&
        [[ "${recorded_log_sha256}" == "${current_log_sha256}" ]]; then
        revalidated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        reused_result="$(jq -r '.result' "${source_evidence}")"
        if grep -Fq ': not applicable by catalog policy' "${source_log}"; then
            reused_result=not_applicable
        elif [[ "${gate}" == "nilaway" ]] &&
            grep -Eq 'NilAway advisory exit status: [1-9][0-9]*$' "${source_log}"; then
            reused_result=advisory
        fi
        jq \
            --arg revalidated_revision "${execution_revision}" \
            --arg revalidated_at "${revalidated_at}" \
            --arg result "${reused_result}" \
            '
                .result = $result |
                .revalidated_revision = $revalidated_revision |
                .revalidated_at = $revalidated_at |
                .reuse_count = ((.reuse_count // 0) + 1)
            ' "${source_evidence}" >"${temporary_evidence}"
        cp "${source_log}" "${temporary_log}"
        mv "${temporary_log}" "${log}"
        mv "${temporary_evidence}" "${evidence}"
        cp "${log}" "${temporary_legacy_log}"
        cp "${evidence}" "${temporary_legacy_evidence}"
        mv "${temporary_legacy_log}" "${legacy_log}"
        mv "${temporary_legacy_evidence}" "${legacy_evidence}"
        emit_evidence_status "${module}" "${gate}" reused
        exit 0
    fi
fi

set +e
"${root}/.golib/scripts/check-module.sh" "${module}" "${gate}" 2>&1 |
    tee "${temporary_log}" |
    forward_gate_output
pipeline_status=("${PIPESTATUS[@]}")
command_status=${pipeline_status[0]}
tee_status=${pipeline_status[1]}
if [[ "${command_status}" -eq 0 && "${tee_status}" -ne 0 ]]; then
    command_status="${tee_status}"
fi
set -e

completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
completed_revision="$(git rev-parse HEAD)"
completed_digest="$("${root}/.golib/scripts/gate-input-digest.sh" "${gate}" "${module}")"
log_sha256="$(shasum -a 256 "${temporary_log}" | awk '{print $1}')"
result=passed
if [[ "${command_status}" -ne 0 ]]; then
    result=failed
elif [[ "${completed_digest}" != "${input_digest}" ]]; then
    result=invalidated
    command_status=1
elif grep -Fq ': not applicable by catalog policy' "${temporary_log}"; then
    result=not_applicable
elif [[ "${gate}" == "nilaway" ]] &&
    grep -Eq 'NilAway advisory exit status: [1-9][0-9]*$' "${temporary_log}"; then
    result=advisory
fi

jq -n \
    --arg module "${module}" \
    --arg gate "${gate}" \
    --arg result "${result}" \
    --argjson exit_code "${command_status}" \
    --arg execution_revision "${execution_revision}" \
    --arg completed_revision "${completed_revision}" \
    --arg input_digest "${input_digest}" \
    --arg completed_digest "${completed_digest}" \
    --arg log_sha256 "${log_sha256}" \
    --arg started_at "${started_at}" \
    --arg completed_at "${completed_at}" \
    --arg go_version "$(go env GOVERSION)" \
    --arg goos "$(go env GOOS)" \
    --arg goarch "$(go env GOARCH)" \
    '{
        schema_version: 1,
        module: $module,
        gate: $gate,
        result: $result,
        exit_code: $exit_code,
        execution_revision: $execution_revision,
        completed_revision: $completed_revision,
        input_digest: $input_digest,
        completed_input_digest: $completed_digest,
        log_sha256: $log_sha256,
        started_at: $started_at,
        completed_at: $completed_at,
        environment: {
            go_version: $go_version,
            goos: $goos,
            goarch: $goarch
        }
    }' >"${temporary_evidence}"

mv "${temporary_log}" "${log}"
mv "${temporary_evidence}" "${evidence}"
cp "${log}" "${temporary_legacy_log}"
cp "${evidence}" "${temporary_legacy_evidence}"
mv "${temporary_legacy_log}" "${legacy_log}"
mv "${temporary_legacy_evidence}" "${legacy_evidence}"
emit_evidence_status "${module}" "${gate}" "${result}"
exit "${command_status}"
