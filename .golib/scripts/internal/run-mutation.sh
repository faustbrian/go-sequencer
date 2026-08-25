#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    printf 'usage: %s <enforce|discover> <module-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
mode="$1"
module="$2"
if [[ -n "${GOLIB_REAL_GO:-}" ]]; then
    PATH="$(dirname "${GOLIB_REAL_GO}"):${PATH}"
    export PATH
fi
if [[ "${mode}" != "enforce" && "${mode}" != "discover" ]]; then
    printf 'mutation mode must be enforce or discover\n' >&2
    exit 2
fi
discover_only=0
if [[ "${mode}" == "discover" ]]; then
    discover_only=1
fi
directory="${root}/${module}"
artifact="${root}/.artifacts/${module}"
report="${artifact}/mutation.json"
checkpoint_directory="${artifact}/mutation-checkpoints"
history_migrations="${root}/.golib/mutation-history-migrations.json"
mkdir -p "${checkpoint_directory}"
active_build_cache=""
# shellcheck disable=SC1091
source "${root}/.golib/scripts/internal/mutation-scratch.sh"
mutation_scratch_initialize "${artifact}"
mutation_arguments=()
execution_revision="$(git -C "${root}" rev-parse HEAD)"

# shellcheck disable=SC1091
source "${root}/.golib/versions.env"
# shellcheck disable=SC1091
source "${root}/.golib/scripts/internal/mutation-command.sh"
# shellcheck disable=SC1091
source "${root}/.golib/scripts/internal/configure-mutation-workers.sh"
gremlins_binary="$("${root}/.golib/scripts/build-golib-gremlins.sh")"
gremlins_binary_sha256="$(shasum -a 256 "${gremlins_binary}" | awk '{print $1}')"
gremlins_verifier_sha256="$("${root}/.golib/scripts/mutation-verifier-identity.sh")"
environment_identity="$(go env -json GOVERSION GOOS GOARCH CGO_ENABLED)"
legacy_gate_input_digest=""
# run_directory is initialized by mutation_scratch_initialize.
# shellcheck disable=SC2154
shared_coverage="${run_directory}/integration.coverage"
shared_coverage_elapsed=""
modfile=""
owned_count="$(jq -r --arg directory "${module}" '
    .modules[] | select(.directory == $directory) | .owned_dependencies | length
' "${root}/modules.json")"
if [[ "${owned_count}" -gt 0 ]]; then
    modfile="${run_directory}/golib-mutation.mod"
    cp "${directory}/go.mod" "${modfile}"
    if [[ -f "${directory}/go.sum" ]]; then
        cp "${directory}/go.sum" "${modfile%.mod}.sum"
    fi
    while IFS=$'\t' read -r owned_path owned_directory; do
        [[ -n "${owned_path}" && -n "${owned_directory}" ]] || continue
        GOWORK=off go mod edit -modfile="${modfile}" \
            -replace="${owned_path}=${root}/${owned_directory}"
    done < <(jq -r '.modules[]
        | select(.directory != "." and .kind != "fixture")
        | [.module_path, .directory]
        | @tsv' "${root}/modules.json")
fi
cd "${directory}"

tags="$(jq -r --arg directory "${module}" \
    '.modules[] | select(.directory == $directory)
    | .test_tags | map(select(. != "interoperability")) | join(",")' \
    "${root}/modules.json")"
mutation_workers=4
if grep -Eq \
    'github\.com/testcontainers/testcontainers-go([/[:space:]])' \
    "${directory}/go.mod"; then
    mutation_workers=1
fi
mutation_environment=(env)
if [[ "${module}" == "pkg/ecma-regexp" ]]; then
    mutation_environment+=( -u TEST262_ROOT)
fi
packages=()
while IFS= read -r package_directory; do
    [[ -n "${package_directory}" ]] && packages+=("${package_directory}")
done < <(jq -r --arg directory "${module}" '
    .modules[]
    | select(.directory == $directory)
    | .packages[]
    | select(.coverage_required == true)
    | .directory
' "${root}/modules.json")
[[ "${#packages[@]}" -gt 0 ]] || {
    printf 'mutation package selection is empty for %s\n' "${module}" >&2
    exit 1
}

expected_packages_file="${run_directory}/expected-packages.json"
printf '%s\n' "${packages[@]}" | jq -R -s \
    'split("\n") | map(select(length > 0))' >"${expected_packages_file}"
reports=()

if [[ "${discover_only}" -eq 0 ]]; then
    # A new enforced campaign supersedes any older aggregate immediately.
    rm -f "${report}"
fi

ensure_shared_coverage() {
    if [[ -s "${shared_coverage}" && -n "${shared_coverage_elapsed}" ]]; then
        return
    fi
    coverage_profile="${artifact}/coverage.out"
    coverage_identity="${artifact}/coverage-profile.json"
    coverage_input_digest="$(
        "${root}/.golib/scripts/gate-input-digest.sh" coverage "${module}"
    )"
    if shared_coverage_elapsed="$(
        "${root}/.golib/scripts/internal/reuse-mutation-coverage.sh" \
            "${coverage_profile}" \
            "${coverage_identity}" \
            "${shared_coverage}" \
            "${coverage_input_digest}" \
            "${tags}" \
            2>/dev/null
    )"; then
        printf '[%s] reused content-bound coverage for mutation\n' "${module}"
        return
    fi
    shared_coverage_elapsed=""
    if [[ -n "${modfile}" ]]; then
        shared_coverage_elapsed="$(
            GOWORK=off GOFLAGS="-modfile=${modfile} -mod=mod" \
                "${root}/.golib/scripts/internal/mutation-coverage.sh" \
                "${shared_coverage}" "${tags}"
        )"
    else
        shared_coverage_elapsed="$(
            GOWORK=off "${root}/.golib/scripts/internal/mutation-coverage.sh" \
                "${shared_coverage}" "${tags}"
        )"
    fi
    [[ -s "${shared_coverage}" ]] || {
        printf 'shared mutation coverage profile is missing for %s\n' \
            "${module}" >&2
        exit 1
    }
}

write_aggregate() {
    local aggregate_tmp validated_revision
    aggregate_tmp="$(mktemp "${report}.tmp.XXXXXX")"
    validated_revision="$(git -C "${root}" rev-parse HEAD)"
    jq -s \
        --arg module "${module}" \
        --arg validated_revision "${validated_revision}" \
        --slurpfile expected "${expected_packages_file}" '
        {
            schema_version: 3,
            module: $module,
            execution_revisions: ([.[].execution_revision] | unique),
            validated_revision: $validated_revision,
            gate_input_digests: (
                map({key: .package, value: .gate_input_digest})
                | from_entries
            ),
            gremlins_versions: ([.[].gremlins_version] | unique),
            gremlins_verifier_sha256s: ([.[].gremlins_verifier_sha256] | unique),
            verifier_identity_sources: ([.[].verifier_identity_source] | unique),
            gremlins_binary_sha256s: (
                [.[].gremlins_binary_sha256 | select(. != null)] | unique
            ),
            environments: ([.[].environment] | unique),
            expected_packages: $expected[0],
            completed_packages: [.[].package],
            complete: (length == ($expected[0] | length)),
            packages: .
        }
    ' "${reports[@]}" >"${aggregate_tmp}"
    mv "${aggregate_tmp}" "${report}"
}

checkpoint_verifier_source_is_valid() {
    jq -e '
        (
            .verifier_identity_source == "executed" and
            (.gremlins_binary_sha256 | type == "string") and
            (.gremlins_binary_sha256 | test("^[0-9a-f]{64}$"))
        ) or (
            .verifier_identity_source == "approved-semantic-migration" and
            (
                (.gremlins_binary_sha256 // null) == null or
                (
                    (.gremlins_binary_sha256 | type == "string") and
                    (.gremlins_binary_sha256 | test("^[0-9a-f]{64}$"))
                )
            )
        )
    ' "$1" >/dev/null
}

for package_directory in "${packages[@]}"; do
    target="."
    slug="root"
    if [[ "${package_directory}" != "." ]]; then
        target="./${package_directory}"
        slug="${package_directory//\//-}"
    fi
    package_report="${run_directory}/${slug}.json"
    checkpoint="${checkpoint_directory}/${slug}.json"
    package_input_digest="$(
        "${root}/.golib/scripts/gate-input-digest.sh" \
            mutation "${module}" "${package_directory}"
    )"
    build_mutation_arguments \
        "${target}" "${package_report}" "${tags}" "${discover_only}"
    configure_mutation_workers "${mutation_workers}"

    reviewed_zero_mutant() {
        local source_directory source_digest reviewed
        source_directory="${module}"
        if [[ "${package_directory}" != "." ]]; then
            source_directory="${module}/${package_directory}"
        fi
        source_digest="$("${root}/.golib/scripts/package-source-digest.sh" \
            "${source_directory}")"
        reviewed="$(jq --arg module "${module}" \
            --arg package "${package_directory}" \
            --arg digest "${source_digest}" \
            --arg version "${GREMLINS_VERSION}" \
            --arg tool_identity "${gremlins_verifier_sha256}" '
                [.packages[] | select(
                    .module_directory == $module and
                    .package_directory == $package and
                    .source_digest == $digest and
                    .gremlins_version == $version and
                    .gremlins_verifier_sha256 == $tool_identity
                )] | length
            ' "${root}/.golib/mutation-zero-inventory.json")"
        [[ "${reviewed}" -eq 1 ]]
    }

    if [[ "${discover_only}" -eq 0 && -s "${checkpoint}" ]] &&
        jq -e \
            --arg module "${module}" \
            --arg package "${package_directory}" \
            --arg digest "${package_input_digest}" \
            --arg version "${GREMLINS_VERSION}" \
            --arg tool_identity "${gremlins_verifier_sha256}" '
                .schema_version == 3 and
                .module == $module and
                .package == $package and
                .gate_input_digest == $digest and
                .gremlins_version == $version and
                .gremlins_verifier_sha256 == $tool_identity and
                (.report.files | type == "array") and
                ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
            ' "${checkpoint}" >/dev/null &&
        checkpoint_verifier_source_is_valid "${checkpoint}"; then
        checkpoint_total="$(
            jq '[.report.files[].mutations[]?] | length' "${checkpoint}"
        )"
        if [[ "${checkpoint_total}" -gt 0 ]] || reviewed_zero_mutant; then
            checkpoint_tmp="$(mktemp "${checkpoint}.tmp.XXXXXX")"
            jq --arg revision "$(git -C "${root}" rev-parse HEAD)" \
                '.validated_revision = $revision' \
                "${checkpoint}" >"${checkpoint_tmp}"
            mv "${checkpoint_tmp}" "${checkpoint}"
            reports+=("${checkpoint}")
            write_aggregate
            printf '[%s] %s reused content-identical mutation evidence\n' \
                "${module}" "${target}"
            continue
        fi
    fi

    observer_v1_package_digest="$(
        "${root}/.golib/scripts/internal/optional-mutation-digest.sh" \
            observer-v1 "${module}" "${package_directory}"
    )"
    if [[ "${discover_only}" -eq 0 &&
        -n "${observer_v1_package_digest}" && -s "${checkpoint}" ]] &&
        jq -e \
            --arg module "${module}" \
            --arg package "${package_directory}" \
            --arg digest "${observer_v1_package_digest}" \
            --arg version "${GREMLINS_VERSION}" \
            --arg tool_identity "${gremlins_verifier_sha256}" '
                .schema_version == 3 and
                .module == $module and
                .package == $package and
                ((
                    [.gate_input_digest] +
                    (.identity_lineage // []) +
                    [(.identity_migration.previous_gate_input_digest // "")]
                ) | index($digest)) != null and
                .gremlins_version == $version and
                .gremlins_verifier_sha256 == $tool_identity and
                (.report.files | type == "array") and
                ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
            ' "${checkpoint}" >/dev/null &&
        checkpoint_verifier_source_is_valid "${checkpoint}"; then
        checkpoint_total="$(
            jq '[.report.files[].mutations[]?] | length' "${checkpoint}"
        )"
        if [[ "${checkpoint_total}" -gt 0 ]] || reviewed_zero_mutant; then
            checkpoint_tmp="$(mktemp "${checkpoint}.tmp.XXXXXX")"
            jq \
                --arg revision "$(git -C "${root}" rev-parse HEAD)" \
                --arg digest "${package_input_digest}" '
                .identity_lineage = ((
                    (.identity_lineage // []) +
                    [.gate_input_digest] +
                    [(.identity_migration.previous_gate_input_digest // "")]
                ) | map(select(length > 0)) | unique)
                | .validated_revision = $revision
                | .identity_migration = {
                    reason: "dependency-test-isolation",
                    previous_gate_input_digest: .gate_input_digest
                }
                | .gate_input_digest = $digest
            ' "${checkpoint}" >"${checkpoint_tmp}"
            mv "${checkpoint_tmp}" "${checkpoint}"
            reports+=("${checkpoint}")
            write_aggregate
            printf '[%s] %s migrated dependency-test-isolated mutation identity\n' \
                "${module}" "${target}"
            continue
        fi
    fi

    legacy_stable_package_digest="$(
        "${root}/.golib/scripts/internal/optional-mutation-digest.sh" \
            legacy-stable "${module}" "${package_directory}"
    )"
    if [[ "${discover_only}" -eq 0 &&
        -n "${legacy_stable_package_digest}" && -s "${checkpoint}" ]] &&
        jq -e \
            --arg module "${module}" \
            --arg package "${package_directory}" \
            --arg digest "${legacy_stable_package_digest}" \
            --arg version "${GREMLINS_VERSION}" \
            --arg tool_identity "${gremlins_verifier_sha256}" '
                .schema_version == 3 and
                .module == $module and
                .package == $package and
                ((
                    [.gate_input_digest] +
                    (.identity_lineage // []) +
                    [(.identity_migration.previous_gate_input_digest // "")]
                ) | index($digest)) != null and
                .gremlins_version == $version and
                .gremlins_verifier_sha256 == $tool_identity and
                (.report.files | type == "array") and
                ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
            ' "${checkpoint}" >/dev/null &&
        checkpoint_verifier_source_is_valid "${checkpoint}"; then
        checkpoint_total="$(
            jq '[.report.files[].mutations[]?] | length' "${checkpoint}"
        )"
        if [[ "${checkpoint_total}" -gt 0 ]] || reviewed_zero_mutant; then
            checkpoint_tmp="$(mktemp "${checkpoint}.tmp.XXXXXX")"
            jq \
                --arg revision "$(git -C "${root}" rev-parse HEAD)" \
                --arg digest "${package_input_digest}" '
                .identity_lineage = ((
                    (.identity_lineage // []) +
                    [.gate_input_digest] +
                    [(.identity_migration.previous_gate_input_digest // "")]
                ) | map(select(length > 0)) | unique)
                | .validated_revision = $revision
                | .identity_migration = {
                    reason: "target-observer-resolution",
                    previous_gate_input_digest: .gate_input_digest
                }
                | .gate_input_digest = $digest
            ' "${checkpoint}" >"${checkpoint_tmp}"
            mv "${checkpoint_tmp}" "${checkpoint}"
            reports+=("${checkpoint}")
            write_aggregate
            printf '[%s] %s migrated module-wide mutation identity\n' \
                "${module}" "${target}"
            continue
        fi
    fi

    legacy_package_digest="$(
        "${root}/.golib/scripts/internal/optional-mutation-digest.sh" \
            caller "${module}" "${package_directory}"
    )"
    if [[ "${discover_only}" -eq 0 &&
        -n "${legacy_package_digest}" && -s "${checkpoint}" ]] &&
        jq -e \
            --arg module "${module}" \
            --arg package "${package_directory}" \
            --arg digest "${legacy_package_digest}" \
            --arg version "${GREMLINS_VERSION}" \
            --arg tool_identity "${gremlins_verifier_sha256}" '
                .schema_version == 3 and
                .module == $module and
                .package == $package and
                ((
                    [.gate_input_digest] +
                    (.identity_lineage // []) +
                    [(.identity_migration.previous_gate_input_digest // "")]
                ) | index($digest)) != null and
                .gremlins_version == $version and
                .gremlins_verifier_sha256 == $tool_identity and
                (.report.files | type == "array") and
                ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
            ' "${checkpoint}" >/dev/null &&
        checkpoint_verifier_source_is_valid "${checkpoint}"; then
        checkpoint_total="$(
            jq '[.report.files[].mutations[]?] | length' "${checkpoint}"
        )"
        if [[ "${checkpoint_total}" -gt 0 ]] || reviewed_zero_mutant; then
            checkpoint_tmp="$(mktemp "${checkpoint}.tmp.XXXXXX")"
            jq \
                --arg revision "$(git -C "${root}" rev-parse HEAD)" \
                --arg digest "${package_input_digest}" '
                .identity_lineage = ((
                    (.identity_lineage // []) +
                    [.gate_input_digest] +
                    [(.identity_migration.previous_gate_input_digest // "")]
                ) | map(select(length > 0)) | unique)
                | .validated_revision = $revision
                | .identity_migration = {
                    reason: "canonical-workspace-resolution",
                    previous_gate_input_digest: .gate_input_digest
                }
                | .gate_input_digest = $digest
            ' "${checkpoint}" >"${checkpoint_tmp}"
            mv "${checkpoint_tmp}" "${checkpoint}"
            reports+=("${checkpoint}")
            write_aggregate
            printf '[%s] %s migrated caller-dependent mutation identity\n' \
                "${module}" "${target}"
            continue
        fi
    fi

    if [[ "${discover_only}" -eq 0 && -s "${checkpoint}" ]] &&
        jq -e \
            --arg module "${module}" \
            --arg package "${package_directory}" \
            --arg version "${GREMLINS_VERSION}" \
            --arg tool_identity "${gremlins_verifier_sha256}" '
                .schema_version == 2 and
                .module == $module and
                .package == $package and
                .gremlins_version == $version and
                .gremlins_verifier_sha256 == $tool_identity and
                (.report.files | type == "array") and
                ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
            ' "${checkpoint}" >/dev/null &&
        checkpoint_verifier_source_is_valid "${checkpoint}"; then
        if [[ -z "${legacy_gate_input_digest}" ]]; then
            legacy_gate_input_digest="$(
                "${root}/.golib/scripts/gate-input-digest.sh" \
                    mutation-legacy "${module}"
            )"
        fi
        if jq -e --arg digest "${legacy_gate_input_digest}" \
            '.gate_input_digest == $digest' "${checkpoint}" >/dev/null; then
            checkpoint_total="$(
                jq '[.report.files[].mutations[]?] | length' "${checkpoint}"
            )"
            if [[ "${checkpoint_total}" -gt 0 ]] || reviewed_zero_mutant; then
                checkpoint_tmp="$(mktemp "${checkpoint}.tmp.XXXXXX")"
                jq \
                    --arg revision "$(git -C "${root}" rev-parse HEAD)" \
                    --arg digest "${package_input_digest}" '
                    .schema_version = 3
                    | .validated_revision = $revision
                    | .legacy_module_gate_input_digest = .gate_input_digest
                    | .gate_input_digest = $digest
                ' "${checkpoint}" >"${checkpoint_tmp}"
                mv "${checkpoint_tmp}" "${checkpoint}"
                reports+=("${checkpoint}")
                write_aggregate
                printf '[%s] %s migrated content-identical mutation evidence\n' \
                    "${module}" "${target}"
                continue
            fi
        fi
    fi

    if [[ "${discover_only}" -eq 0 && -s "${checkpoint}" &&
        -s "${history_migrations}" ]]; then
        checkpoint_tmp="$(mktemp "${checkpoint}.tmp.XXXXXX")"
        if "${root}/.golib/scripts/internal/reuse-approved-mutation-checkpoint.sh" \
            "${history_migrations}" \
            "${checkpoint}" \
            "${module}" \
            "${package_directory}" \
            "${package_input_digest}" \
            "${GREMLINS_VERSION}" \
            "${gremlins_verifier_sha256}" \
            "$(git -C "${root}" rev-parse HEAD)" \
            "${checkpoint_tmp}"; then
            checkpoint_total="$(
                jq '[.report.files[].mutations[]?] | length' \
                    "${checkpoint_tmp}"
            )"
            if [[ "${checkpoint_total}" -gt 0 ]] || reviewed_zero_mutant; then
                mv "${checkpoint_tmp}" "${checkpoint}"
                reports+=("${checkpoint}")
                write_aggregate
                printf '[%s] %s reused approved content-identical mutation evidence\n' \
                    "${module}" "${target}"
                continue
            fi
        fi
        rm -f "${checkpoint_tmp}"
    fi

    printf '[%s] mutation package %s\n' "${module}" "${target}"
    mutation_scratch_package_cache "${slug}"
    ensure_shared_coverage
    status=0
    if [[ "${discover_only}" -eq 1 ]]; then
        if [[ -n "${modfile}" ]]; then
            "${mutation_environment[@]}" \
                GOCACHE="${active_build_cache}" GOWORK=off \
                GOLIB_GREMLINS_COVERAGE_PROFILE="${shared_coverage}" \
                GOLIB_GREMLINS_COVERAGE_ELAPSED="${shared_coverage_elapsed}" \
                GOFLAGS="-modfile=${modfile} -mod=mod" \
                "${gremlins_binary}" "${mutation_arguments[@]}" \
                >"${run_directory}/${slug}.log" 2>&1 || status=$?
        else
            "${mutation_environment[@]}" \
                GOCACHE="${active_build_cache}" GOWORK=off \
                GOLIB_GREMLINS_COVERAGE_PROFILE="${shared_coverage}" \
                GOLIB_GREMLINS_COVERAGE_ELAPSED="${shared_coverage_elapsed}" \
                "${gremlins_binary}" "${mutation_arguments[@]}" \
                >"${run_directory}/${slug}.log" 2>&1 || status=$?
        fi
    elif [[ -n "${modfile}" ]]; then
        "${mutation_environment[@]}" \
            GOCACHE="${active_build_cache}" GOWORK=off \
            GOLIB_GREMLINS_COVERAGE_PROFILE="${shared_coverage}" \
            GOLIB_GREMLINS_COVERAGE_ELAPSED="${shared_coverage_elapsed}" \
            GOFLAGS="-modfile=${modfile} -mod=mod" \
            "${gremlins_binary}" "${mutation_arguments[@]}" || status=$?
    else
        "${mutation_environment[@]}" \
            GOCACHE="${active_build_cache}" GOWORK=off \
            GOLIB_GREMLINS_COVERAGE_PROFILE="${shared_coverage}" \
            GOLIB_GREMLINS_COVERAGE_ELAPSED="${shared_coverage_elapsed}" \
            "${gremlins_binary}" "${mutation_arguments[@]}" || status=$?
    fi
    mutation_scratch_cleanup_package_cache
    if [[ "${status}" -ne 0 ]]; then
        if [[ "${discover_only}" -eq 1 ]]; then
            cat "${run_directory}/${slug}.log" >&2
        fi
        printf 'mutation tool failed for %s %s (status %s)\n' \
            "${module}" "${target}" "${status}" >&2
        exit 1
    fi
    if [[ "${discover_only}" -eq 1 && ! -s "${package_report}" ]]; then
        source_directory="${module}"
        if [[ "${package_directory}" != "." ]]; then
            source_directory="${module}/${package_directory}"
        fi
        source_digest="$("${root}/.golib/scripts/package-source-digest.sh" \
            "${source_directory}")"
        printf 'ZERO\t%s\t%s\t%s\n' \
            "${module}" "${package_directory}" "${source_digest}"
        continue
    fi
    if [[ ! -s "${package_report}" ]]; then
        if reviewed_zero_mutant; then
            printf '{"files":[]}\n' >"${package_report}"
        else
            printf 'mutation report is missing for %s %s\n' \
                "${module}" "${target}" >&2
            exit 1
        fi
    fi
    if ! jq -e '.files | type == "array"' "${package_report}" >/dev/null; then
        printf 'mutation report is malformed for %s %s\n' "${module}" "${target}" >&2
        exit 1
    fi
    total="$(jq '[.files[].mutations[]] | length' "${package_report}")"
    if [[ "${discover_only}" -eq 1 ]]; then
        printf 'MUTANTS\t%s\t%s\t%s\n' \
            "${module}" "${package_directory}" "${total}"
        continue
    fi
    if [[ "${total}" -eq 0 ]]; then
        if ! reviewed_zero_mutant; then
            printf 'mutation report unexpectedly contains no reviewed mutants for %s %s\n' \
                "${module}" "${target}" >&2
            exit 1
        fi
    fi
    unexpected="$(jq '[.files[].mutations[] | select(.status != "KILLED")] | length' \
        "${package_report}")"
    if [[ "${status}" -ne 0 || "${unexpected}" -ne 0 ]]; then
        jq -r '.files[] as $file | $file.mutations[] | select(.status != "KILLED") | "\($file.file_name):\(.line):\(.column) \(.type) \(.status)"' \
            "${package_report}" >&2
        printf 'mutation gate failed for %s %s (tool status %s)\n' \
            "${module}" "${target}" "${status}" >&2
        exit 1
    fi
    current_input_digest="$(
        "${root}/.golib/scripts/gate-input-digest.sh" \
            mutation "${module}" "${package_directory}"
    )"
    if [[ "${current_input_digest}" != "${package_input_digest}" ]]; then
        printf 'mutation inputs changed while running %s %s\n' \
            "${module}" "${target}" >&2
        exit 1
    fi
    checkpoint_tmp="$(mktemp "${checkpoint}.tmp.XXXXXX")"
    jq \
        --arg module "${module}" \
        --arg package "${package_directory}" \
        --arg execution_revision "${execution_revision}" \
        --arg validated_revision "$(git -C "${root}" rev-parse HEAD)" \
        --arg gate_input_digest "${package_input_digest}" \
        --arg gremlins_version "${GREMLINS_VERSION}" \
        --arg gremlins_verifier_sha256 "${gremlins_verifier_sha256}" \
        --arg gremlins_binary_sha256 "${gremlins_binary_sha256}" \
        --argjson environment "${environment_identity}" '
        {
            schema_version: 3,
            module: $module,
            package: $package,
            execution_revision: $execution_revision,
            validated_revision: $validated_revision,
            gate_input_digest: $gate_input_digest,
            gremlins_version: $gremlins_version,
            gremlins_verifier_sha256: $gremlins_verifier_sha256,
            verifier_identity_source: "executed",
            gremlins_binary_sha256: $gremlins_binary_sha256,
            environment: $environment,
            report: .
        }
    ' "${package_report}" >"${checkpoint_tmp}"
    mv "${checkpoint_tmp}" "${checkpoint}"
    reports+=("${checkpoint}")
    write_aggregate
    if [[ "${total}" -eq 0 ]]; then
        printf '[%s] %s has a hash-pinned zero-viable-mutant review\n' \
            "${module}" "${target}"
    else
        printf '[%s] %s killed %s/%s viable mutants\n' \
            "${module}" "${target}" "${total}" "${total}"
    fi
done

if [[ "${discover_only}" -eq 1 ]]; then
    exit 0
fi

if ! jq -e \
    '
        .schema_version == 3 and
        .complete == true and
        (.gremlins_verifier_sha256s | length == 1) and
        ([.packages[] | select(.schema_version != 3)] | length == 0) and
        all(.packages[];
            (
                .verifier_identity_source == "executed" and
                (.gremlins_binary_sha256 | type == "string") and
                (.gremlins_binary_sha256 | test("^[0-9a-f]{64}$"))
            ) or (
                .verifier_identity_source == "approved-semantic-migration" and
                (
                    (.gremlins_binary_sha256 // null) == null or
                    (
                        (.gremlins_binary_sha256 | type == "string") and
                        (.gremlins_binary_sha256 | test("^[0-9a-f]{64}$"))
                    )
                )
            )
        )
    ' "${report}" >/dev/null; then
    printf 'aggregate mutation report is incomplete for %s\n' "${module}" >&2
    exit 1
fi
