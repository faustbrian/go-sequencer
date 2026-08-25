#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    printf 'usage: %s <module-directory> [artifact-archive]\n' "$0" >&2
    exit 2
fi

root="${GOLIB_ROOT:-$(git rev-parse --show-toplevel)}"
module="$1"
archive="${2:-}"

if ! jq -e --arg directory "${module}" \
    '.modules[] | select(.directory == $directory)' \
    "${root}/modules.json" >/dev/null; then
    printf 'cannot restore mutation evidence for unknown module: %s\n' \
        "${module}" >&2
    exit 2
fi

task="$(mktemp -d "${TMPDIR:-/tmp}/golib-ci-evidence.XXXXXX")"
cleanup() {
    find "${task}" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

checkpoint_directory="${root}/.artifacts/${module}/mutation-checkpoints"
mkdir -p "${checkpoint_directory}"
packages="${task}/packages"
jq -r --arg directory "${module}" '
    .modules[]
    | select(.directory == $directory)
    | .packages[]
    | select(.coverage_required == true)
    | .directory
' "${root}/modules.json" | LC_ALL=C sort >"${packages}"
expected="$(wc -l <"${packages}" | tr -d ' ')"
restored=0
require_exact_digest=0
digest_directory="${task}/input-digests"
fallback_directory="${task}/fallback-checkpoints"
history_migrations="${root}/.golib/mutation-history-migrations.json"
current_verifier_identity=""
current_revision=""
current_gremlins_version=""

command -v unzip >/dev/null || {
    printf 'unzip is required to restore CI mutation evidence\n' >&2
    exit 1
}

restore_archive() {
    local candidate_archive="$1"
    local entries package_directory slug entry candidate destination fallback
    local expected_digest migrated
    if [[ ! -s "${candidate_archive}" ]] ||
        ! unzip -tqq "${candidate_archive}" >/dev/null; then
        return 1
    fi

    entries="${task}/entries"
    unzip -Z1 "${candidate_archive}" >"${entries}"
    while IFS= read -r package_directory; do
        [[ -n "${package_directory}" ]] || continue
        slug=root
        if [[ "${package_directory}" != "." ]]; then
            slug="${package_directory//\//-}"
        fi
        destination="${checkpoint_directory}/${slug}.json"
        [[ ! -s "${destination}" ]] || continue
        fallback="${fallback_directory}/${slug}.json"
        expected_digest=""
        if [[ "${require_exact_digest}" -eq 1 ]]; then
            [[ -s "${digest_directory}/${slug}" ]] || continue
            expected_digest="$(<"${digest_directory}/${slug}")"
        fi
        entry="mutation-checkpoints/${slug}.json"
        if [[ "$(grep -Fxc -- "${entry}" "${entries}")" -ne 1 ]]; then
            continue
        fi
        candidate="$(mktemp "${task}/checkpoint.XXXXXX")"
        if ! unzip -p "${candidate_archive}" "${entry}" >"${candidate}" ||
            ! jq -e \
                --arg module "${module}" \
                --arg package "${package_directory}" \
                '
                    .schema_version == 3 and
                    .module == $module and
                    .package == $package and
                    (.execution_revision | type == "string") and
                    (.execution_revision | test("^[0-9a-f]{40}$")) and
                    (.gate_input_digest | type == "string") and
                    (.gate_input_digest | test("^[0-9a-f]{64}$")) and
                    (.gremlins_version | type == "string") and
                    (.gremlins_version | length > 0) and
                    (
                        (.gremlins_verifier_sha256 // null) == null or
                        (
                            (.gremlins_verifier_sha256 | type == "string") and
                            (.gremlins_verifier_sha256 | test("^[0-9a-f]{64}$"))
                        )
                    ) and
                    (.report.files | type == "array") and
                    ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
                ' "${candidate}" >/dev/null; then
            find "${candidate}" -delete 2>/dev/null || true
            continue
        fi
        if [[ "${require_exact_digest}" -eq 0 ]]; then
            mv "${candidate}" "${destination}"
            restored=$((restored + 1))
            continue
        fi
        if jq -e \
            --arg digest "${expected_digest}" \
            --arg version "${current_gremlins_version}" \
            --arg verifier "${current_verifier_identity}" '
                .gate_input_digest == $digest and
                .gremlins_version == $version and
                .gremlins_verifier_sha256 == $verifier and
                (
                    (
                        .verifier_identity_source == "executed" and
                        (.gremlins_binary_sha256 | type == "string") and
                        (.gremlins_binary_sha256 | test("^[0-9a-f]{64}$"))
                    ) or
                    (
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
            ' "${candidate}" >/dev/null; then
            mv "${candidate}" "${destination}"
            restored=$((restored + 1))
            continue
        fi
        migrated="$(mktemp "${task}/migrated-checkpoint.XXXXXX")"
        if [[ -s "${history_migrations}" ]] &&
            "${root}/.golib/scripts/internal/reuse-approved-mutation-checkpoint.sh" \
                "${history_migrations}" \
                "${candidate}" \
                "${module}" \
                "${package_directory}" \
                "${expected_digest}" \
                "${current_gremlins_version}" \
                "${current_verifier_identity}" \
                "${current_revision}" \
                "${migrated}"; then
            mv "${migrated}" "${destination}"
            find "${candidate}" -delete 2>/dev/null || true
            restored=$((restored + 1))
            continue
        fi
        find "${migrated}" -delete 2>/dev/null || true
        mkdir -p "${fallback_directory}"
        if [[ ! -s "${fallback}" ]]; then
            mv "${candidate}" "${fallback}"
        else
            find "${candidate}" -delete 2>/dev/null || true
        fi
    done <"${packages}"
}

if [[ -z "${archive}" ]]; then
    repository="${GITHUB_REPOSITORY:-}"
    repository_id="${GITHUB_REPOSITORY_ID:-}"
    run_id="${GITHUB_RUN_ID:-}"
    if [[ -z "${GH_TOKEN:-}" || -z "${repository}" ||
        ! "${repository_id}" =~ ^[0-9]+$ || ! "${run_id}" =~ ^[0-9]+$ ]]; then
        printf '[%s] prior CI mutation evidence is unavailable\n' "${module}"
        exit 0
    fi
    command -v gh >/dev/null || {
        printf 'GitHub CLI is required to restore CI mutation evidence\n' >&2
        exit 1
    }

    artifact_slug="${module//\//-}"
    [[ "${artifact_slug}" != "." ]] || artifact_slug=root
    artifact_name="evidence-${artifact_slug}"
    if ! artifacts="$(
        gh api --method GET \
            "repos/${repository}/actions/artifacts" \
            -f "name=${artifact_name}" \
            -f per_page=100
    )"; then
        printf '[%s] prior CI mutation evidence lookup failed; executing fresh\n' \
            "${module}"
        exit 0
    fi
    artifact_ids="$(
        jq -r \
            --argjson repository_id "${repository_id}" \
            --argjson run_id "${run_id}" '
                [
                    .artifacts[]
                    | select(
                        .expired == false and
                        .workflow_run.head_repository_id == $repository_id and
                        .workflow_run.head_branch == "main" and
                        .workflow_run.id != $run_id
                    )
                ]
                | sort_by(.created_at)
                | reverse
                | .[].id
            ' <<<"${artifacts}"
    )"
    if [[ -z "${artifact_ids}" ]]; then
        printf '[%s] no trusted prior CI mutation evidence found\n' "${module}"
        exit 0
    fi
    mkdir -p "${digest_directory}"
    # shellcheck disable=SC1091
    source "${root}/.golib/versions.env"
    current_gremlins_version="${GREMLINS_VERSION}"
    current_verifier_identity="$(
        "${root}/.golib/scripts/mutation-verifier-identity.sh"
    )"
    current_revision="${GITHUB_SHA:-$(git -C "${root}" rev-parse HEAD)}"
    require_exact_digest=1
    while IFS= read -r package_directory; do
        [[ -n "${package_directory}" ]] || continue
        slug=root
        if [[ "${package_directory}" != "." ]]; then
            slug="${package_directory//\//-}"
        fi
        if ! "${root}/.golib/scripts/gate-input-digest.sh" \
            mutation "${module}" "${package_directory}" \
            >"${digest_directory}/${slug}"; then
            find "${digest_directory}/${slug}" -delete 2>/dev/null || true
        fi
    done <"${packages}"
    while IFS= read -r artifact_id; do
        [[ "${artifact_id}" =~ ^[0-9]+$ ]] || continue
        candidate_archive="${task}/evidence-${artifact_id}.zip"
        if ! gh api \
            "repos/${repository}/actions/artifacts/${artifact_id}/zip" \
            >"${candidate_archive}"; then
            continue
        fi
        restore_archive "${candidate_archive}" || continue
        [[ "${restored}" -lt "${expected}" ]] || break
    done <<<"${artifact_ids}"
    while IFS= read -r package_directory; do
        [[ -n "${package_directory}" ]] || continue
        slug=root
        if [[ "${package_directory}" != "." ]]; then
            slug="${package_directory//\//-}"
        fi
        destination="${checkpoint_directory}/${slug}.json"
        fallback="${fallback_directory}/${slug}.json"
        if [[ ! -s "${destination}" && -s "${fallback}" ]]; then
            mv "${fallback}" "${destination}"
            restored=$((restored + 1))
        fi
    done <"${packages}"
else
    if ! restore_archive "${archive}"; then
        printf '[%s] prior CI mutation evidence archive is invalid; executing fresh\n' \
            "${module}"
        exit 0
    fi
fi

printf '[%s] restored %d prior content-addressed mutation checkpoints\n' \
    "${module}" "${restored}"
