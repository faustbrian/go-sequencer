#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <module-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
artifact="${root}/.artifacts/${module}/evidence"
validated_revision="$(git rev-parse HEAD)"
validated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
lock=""
lock_acquired=0
migration_lock="${artifact}/.locks/api-baseline-evidence-migration.lock"
migration_lock_acquired=0
temporary_evidence=""
temporary_log=""
temporary_legacy_evidence=""
temporary_legacy_log=""

cleanup_gate() {
    rm -f \
        "${temporary_evidence}" "${temporary_log}" \
        "${temporary_legacy_evidence}" "${temporary_legacy_log}"
    if [[ "${lock_acquired}" -eq 1 ]]; then
        rm -f "${lock}/owner"
        rmdir "${lock}" 2>/dev/null || true
    fi
    lock_acquired=0
    temporary_evidence=""
    temporary_log=""
    temporary_legacy_evidence=""
    temporary_legacy_log=""
}

cleanup() {
    cleanup_gate
    if [[ "${migration_lock_acquired}" -eq 1 ]]; then
        rm -f "${migration_lock}/owner"
        rmdir "${migration_lock}" 2>/dev/null || true
    fi
}
trap cleanup EXIT HUP INT TERM

[[ -f "${root}/${module}/api/baseline.txt" ]] || {
    printf 'module has no API baseline: %s\n' "${module}" >&2
    exit 1
}

mkdir -p "${artifact}/.locks"
while ! mkdir "${migration_lock}" 2>/dev/null; do
    if [[ -f "${migration_lock}/owner" ]]; then
        owner="$(cat "${migration_lock}/owner" 2>/dev/null || true)"
        if [[ "${owner}" =~ ^[0-9]+$ ]] && ! kill -0 "${owner}" 2>/dev/null; then
            rm -f "${migration_lock}/owner"
            rmdir "${migration_lock}" 2>/dev/null || true
            continue
        fi
    fi
    sleep 0.05
done
printf '%s\n' "$$" >"${migration_lock}/owner"
migration_lock_acquired=1

while IFS= read -r gate; do
    [[ -n "${gate}" ]] || continue
    source_evidence="${artifact}/${gate}.json"
    source_log="${artifact}/${gate}.log"
    [[ -f "${source_evidence}" && -f "${source_log}" ]] || continue

    lock_root="${artifact}/.locks"
    lock="${lock_root}/${gate}.lock"
    mkdir -p "${lock_root}"
    while ! mkdir "${lock}" 2>/dev/null; do
        if [[ -f "${lock}/owner" ]]; then
            owner="$(cat "${lock}/owner" 2>/dev/null || true)"
            if [[ "${owner}" =~ ^[0-9]+$ ]] && ! kill -0 "${owner}" 2>/dev/null; then
                rm -f "${lock}/owner"
                rmdir "${lock}" 2>/dev/null || true
                continue
            fi
        fi
        sleep 0.05
    done
    printf '%s\n' "$$" >"${lock}/owner"
    lock_acquired=1

    current_digest="$(
        "${root}/.golib/scripts/gate-input-digest.sh" "${gate}" "${module}"
    )"
    source_digest="$(jq -r '.input_digest // empty' "${source_evidence}")"
    if [[ "${source_digest}" == "${current_digest}" ]]; then
        cleanup_gate
        continue
    fi

    log_sha256="$(shasum -a 256 "${source_log}" | awk '{print $1}')"
    jq -e \
        --arg module "${module}" \
        --arg gate "${gate}" \
        --arg source_digest "${source_digest}" \
        --arg log_sha256 "${log_sha256}" '
            .schema_version == 1 and
            .module == $module and
            .gate == $gate and
            (.result == "passed" or .result == "not_applicable" or
                ($gate == "nilaway" and .result == "advisory")) and
            .exit_code == 0 and
            ($source_digest | length) > 0 and
            .input_digest == $source_digest and
            .completed_input_digest == $source_digest and
            .log_sha256 == $log_sha256
        ' "${source_evidence}" >/dev/null || {
            cleanup_gate
            continue
        }

    legacy_digest="$(
        GOLIB_GATE_INPUT_POLICY=legacy-api-baseline \
            "${root}/.golib/scripts/gate-input-digest.sh" "${gate}" "${module}"
    )"
    if [[ "${source_digest}" != "${legacy_digest}" ]]; then
        cleanup_gate
        continue
    fi

    destination="${artifact}/by-input/${gate}"
    destination_evidence="${destination}/${current_digest}.json"
    destination_log="${destination}/${current_digest}.log"
    temporary_evidence="${destination_evidence}.tmp.$$"
    temporary_log="${destination_log}.tmp.$$"
    temporary_legacy_evidence="${source_evidence}.tmp.$$"
    temporary_legacy_log="${source_log}.tmp.$$"
    mkdir -p "${destination}"
    jq \
        --arg current_digest "${current_digest}" \
        --arg source_digest "${source_digest}" \
        --arg validated_revision "${validated_revision}" \
        --arg validated_at "${validated_at}" '
            .identity_lineage = ((
                (.identity_lineage // []) +
                [.input_digest] +
                [(.identity_migration.previous_gate_input_digest // "")]
            ) | map(select(length > 0)) | unique)
            | .input_digest = $current_digest
            | .completed_input_digest = $current_digest
            | .revalidated_revision = $validated_revision
            | .revalidated_at = $validated_at
            | .identity_migration = {
                reason: "non-semantic-gate-input-scope-narrowing",
                previous_gate_input_digest: $source_digest
            }
        ' "${source_evidence}" >"${temporary_evidence}"
    cp "${source_log}" "${temporary_log}"
    mv "${temporary_log}" "${destination_log}"
    mv "${temporary_evidence}" "${destination_evidence}"
    cp "${destination_log}" "${temporary_legacy_log}"
    cp "${destination_evidence}" "${temporary_legacy_evidence}"
    mv "${temporary_legacy_log}" "${source_log}"
    mv "${temporary_legacy_evidence}" "${source_evidence}"
    cleanup_gate
    printf '[%s] migrated content-identical %s evidence\n' "${module}" "${gate}"
done <"${root}/.golib/scripts/check-gates.txt"
