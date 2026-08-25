#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    printf 'usage: %s <module-directory> [gate]\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
selected_gate="${2:-}"
artifact="${root}/.artifacts/${module}/evidence"
validated_revision="$(git rev-parse HEAD)"
validated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${selected_gate}" ]]; then
    gates="${selected_gate}"
else
    gates="$(cat "${root}/.golib/scripts/check-gates.txt")"
fi

while IFS= read -r gate; do
    [[ -n "${gate}" ]] || continue
    # Aggregate assurance intentionally fingerprints runner implementation.
    # Process-isolation changes therefore invalidate rather than migrate it.
    [[ "${gate}" != "operational-assurance" ]] || continue
    current_digest="$(
        "${root}/.golib/scripts/gate-input-digest.sh" "${gate}" "${module}"
    )"
    destination="${artifact}/by-input/${gate}"
    destination_evidence="${destination}/${current_digest}.json"
    destination_log="${destination}/${current_digest}.log"
    if [[ -f "${destination_evidence}" && -f "${destination_log}" ]]; then
        continue
    fi
    source_evidence=""
    source_log=""
    source_digest=""
    for legacy_policy in \
        legacy-runner-isolation-post \
        legacy-runner-isolation; do
        legacy_digest="$(
            GOLIB_GATE_INPUT_POLICY="${legacy_policy}" \
                "${root}/.golib/scripts/gate-input-digest.sh" "${gate}" "${module}"
        )"
        [[ "${current_digest}" != "${legacy_digest}" ]] || continue
        candidate_evidence="${artifact}/by-input/${gate}/${legacy_digest}.json"
        candidate_log="${artifact}/by-input/${gate}/${legacy_digest}.log"
        if [[ ! -f "${candidate_evidence}" || ! -f "${candidate_log}" ]]; then
            candidate_evidence="${artifact}/${gate}.json"
            candidate_log="${artifact}/${gate}.log"
        fi
        [[ -f "${candidate_evidence}" && -f "${candidate_log}" ]] || continue
        candidate_digest="$(jq -r '.input_digest // empty' "${candidate_evidence}")"
        [[ "${candidate_digest}" == "${legacy_digest}" ]] || continue
        source_evidence="${candidate_evidence}"
        source_log="${candidate_log}"
        source_digest="${candidate_digest}"
        break
    done
    [[ -n "${source_evidence}" ]] || continue
    log_sha256="$(shasum -a 256 "${source_log}" | awk '{print $1}')"
    jq -e \
        --arg module "${module}" \
        --arg gate "${gate}" \
        --arg source_digest "${source_digest}" \
        --arg log_sha256 "${log_sha256}" '
            .schema_version == 1 and
            .module == $module and
            .gate == $gate and
            (
                .result == "passed" or
                .result == "not_applicable" or
                ($gate == "nilaway" and .result == "advisory")
            ) and
            .exit_code == 0 and
            .input_digest == $source_digest and
            .completed_input_digest == $source_digest and
            .log_sha256 == $log_sha256
        ' "${source_evidence}" >/dev/null || continue
    if [[ "$(jq -r '.result' "${source_evidence}")" == "advisory" ]] &&
        ! grep -Eq 'NilAway advisory exit status: [1-9][0-9]*$' "${source_log}"; then
        continue
    fi
    if [[ "$(jq -r '.result' "${source_evidence}")" == "not_applicable" ]] &&
        ! grep -Fq ': not applicable by catalog policy' "${source_log}"; then
        continue
    fi

    lock_root="${artifact}/.locks"
    lock="${lock_root}/${gate}.lock"
    mkdir -p "${destination}" "${lock_root}"
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

    temporary_evidence="${destination_evidence}.tmp.$$"
    temporary_log="${destination_log}.tmp.$$"
    temporary_pointer_evidence="${artifact}/${gate}.json.tmp.$$"
    temporary_pointer_log="${artifact}/${gate}.log.tmp.$$"
    cleanup_gate() {
        rm -f \
            "${temporary_evidence}" "${temporary_log}" \
            "${temporary_pointer_evidence}" "${temporary_pointer_log}"
        if [[ "$(readlink "${lock}" 2>/dev/null || true)" == "$$" ]]; then
            rm -f "${lock}"
        fi
    }
    trap cleanup_gate EXIT
    trap 'exit 130' HUP INT TERM

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
                reason: "non-semantic-runner-isolation-scope-narrowing",
                previous_gate_input_digest: $source_digest
            }
        ' "${source_evidence}" >"${temporary_evidence}"
    cp "${source_log}" "${temporary_log}"
    mv "${temporary_log}" "${destination_log}"
    mv "${temporary_evidence}" "${destination_evidence}"
    cp "${destination_log}" "${temporary_pointer_log}"
    cp "${destination_evidence}" "${temporary_pointer_evidence}"
    mv "${temporary_pointer_log}" "${artifact}/${gate}.log"
    mv "${temporary_pointer_evidence}" "${artifact}/${gate}.json"
    cleanup_gate
    trap - EXIT HUP INT TERM
    printf '[%s] migrated content-identical %s evidence\n' "${module}" "${gate}"
done <<<"${gates}"
