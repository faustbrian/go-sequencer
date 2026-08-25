#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 9 ]]; then
    printf 'usage: %s <ledger> <checkpoint> <module> <package> <current-input> <tool-version> <verifier-identity> <validated-revision> <output>\n' "$0" >&2
    exit 2
fi

ledger="$1"
checkpoint="$2"
module="$3"
package="$4"
current_input="$5"
tool_version="$6"
verifier_identity="$7"
validated_revision="$8"
output="$9"

[[ -s "${ledger}" && -s "${checkpoint}" ]] || exit 1

report_digest="$({ jq -S -c '.report' "${checkpoint}" || exit 1; } |
    shasum -a 256 | awk '{print $1}')"
execution_revision="$(jq -er '.execution_revision' "${checkpoint}")"
previous_input="$(jq -er '.gate_input_digest' "${checkpoint}")"
checkpoint_inputs="$(jq -c '
    ([.gate_input_digest] +
    (.identity_lineage // []) +
    [(.identity_migration.previous_gate_input_digest // "")])
    | map(select(length > 0))
    | unique
' "${checkpoint}")"

jq -e \
    --arg module "${module}" \
    --arg package "${package}" \
    --arg execution_revision "${execution_revision}" \
    --arg tool_version "${tool_version}" \
    --arg verifier_identity "${verifier_identity}" \
    --arg report_digest "${report_digest}" \
    --argjson checkpoint_inputs "${checkpoint_inputs}" '
        .schema_version == 3 and
        ([.verifier_migrations[]? | select(
            .module == $module and
            .package == $package and
            .gremlins_verifier_sha256 == $verifier_identity and
            .execution_revision == $execution_revision and
            (.gate_input_digest as $approved_input |
                ($checkpoint_inputs | index($approved_input)) != null) and
            .gremlins_version == $tool_version and
            .report_sha256 == $report_digest
        )] | length == 1)
    ' "${ledger}" >/dev/null

jq -e \
    --arg module "${module}" \
    --arg package "${package}" \
    --arg tool_version "${tool_version}" \
    --arg verifier_identity "${verifier_identity}" '
        .schema_version == 3 and
        .module == $module and
        .package == $package and
        .gremlins_version == $tool_version and
        ((.gremlins_verifier_sha256 // $verifier_identity) == $verifier_identity) and
        (.report.files | type == "array") and
        ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
    ' "${checkpoint}" >/dev/null

if [[ "${previous_input}" == "${current_input}" ]]; then
    jq \
        --arg validated_revision "${validated_revision}" \
        --arg verifier_identity "${verifier_identity}" '
            .validated_revision = $validated_revision
            | .gremlins_verifier_sha256 = $verifier_identity
            | .verifier_identity_source = "approved-semantic-migration"
        ' "${checkpoint}" >"${output}"
    exit 0
fi

jq -e \
    --arg module "${module}" \
    --arg package "${package}" \
    --arg execution_revision "${execution_revision}" \
    --arg previous_input "${previous_input}" \
    --arg current_input "${current_input}" \
    --arg tool_version "${tool_version}" \
    --arg verifier_identity "${verifier_identity}" \
    --arg report_digest "${report_digest}" '
        .schema_version == 3 and
        ([.entries[] | select(
            .module == $module and
            .package == $package and
            .execution_revision == $execution_revision and
            .gate_input_digest == $previous_input and
            .replacement_gate_input_digest == $current_input and
            .gremlins_version == $tool_version and
            ((.gremlins_verifier_sha256 // $verifier_identity) == $verifier_identity) and
            .report_sha256 == $report_digest
        )] | length == 1)
    ' "${ledger}" >/dev/null

jq \
    --arg validated_revision "${validated_revision}" \
    --arg current_input "${current_input}" \
    --arg verifier_identity "${verifier_identity}" '
        .identity_lineage = ((
            (.identity_lineage // []) +
            [.gate_input_digest] +
            [(.identity_migration.previous_gate_input_digest // "")]
        ) | map(select(length > 0)) | unique)
        | .validated_revision = $validated_revision
        | .gremlins_verifier_sha256 = $verifier_identity
        | .verifier_identity_source = "approved-semantic-migration"
        | .identity_migration = {
            reason: "approved-input-identity-migration",
            previous_gate_input_digest: .gate_input_digest
        }
        | .gate_input_digest = $current_input
    ' "${checkpoint}" >"${output}"
