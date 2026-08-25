#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    printf 'usage: %s <module-directory> <gate>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
module="$1"
gate="$2"
directory="${root}/${module}"
local_proxy_owned=0
local_modcache_owned=0
isolated_modfiles_owned=0
GOLIB_GO_TOOL_PATH=""

cleanup() {
    if [[ "${local_proxy_owned}" -eq 1 ]]; then
        rm -rf "${GOLIB_LOCAL_PROXY}"
    fi
    if [[ "${local_modcache_owned}" -eq 1 ]]; then
        chmod -R u+w "${GOLIB_LOCAL_MODCACHE}"
        rm -rf "${GOLIB_LOCAL_MODCACHE}"
    fi
    if [[ "${isolated_modfiles_owned}" -eq 1 ]]; then
        rm -rf "${GOLIB_ISOLATED_MODFILES_DIRECTORY}"
    fi
}
trap cleanup EXIT HUP INT TERM

if ! jq -e --arg directory "${module}" \
    '.modules[] | select(.directory == $directory)' \
    "${root}/modules.json" >/dev/null; then
    printf 'module is absent from modules.json: %s\n' "${module}" >&2
    exit 1
fi

if [[ ! -f "${directory}/go.mod" ]]; then
    printf 'module has no go.mod: %s\n' "${module}" >&2
    exit 1
fi

enable_local_proxy() {
    local no_sum_db upstream upstream_flags

    if [[ -z "${GOLIB_REAL_GO:-}" ]]; then
        GOLIB_REAL_GO="$(command -v go)"
    fi
    export GOLIB_REAL_GO
    upstream_flags="$(
        printf '%s' "${GOLIB_UPSTREAM_GOFLAGS:-$("${GOLIB_REAL_GO}" env GOFLAGS)}"
    )"
    export GOLIB_UPSTREAM_GOFLAGS="${upstream_flags}"

    if [[ -z "${GOLIB_LOCAL_PROXY:-}" ]]; then
        GOLIB_LOCAL_PROXY="$(mktemp -d "${TMPDIR:-/tmp}/golib-proxy.XXXXXX")"
        local_proxy_owned=1
        "${root}/.golib/scripts/build-local-proxy.sh" \
            "${GOLIB_LOCAL_PROXY}" v1.0.0 "${module}"
    fi
    export GOLIB_LOCAL_PROXY

    upstream="${GOLIB_UPSTREAM_GOPROXY:-$(go env GOPROXY)}"
    no_sum_db="$(go env GONOSUMDB)"
    export GOPROXY="file://${GOLIB_LOCAL_PROXY},${upstream}"
    export GONOSUMDB="github.com/faustbrian/go-*${no_sum_db:+,${no_sum_db}}"
    if [[ -z "${GOLIB_LOCAL_MODCACHE:-}" ]]; then
        GOLIB_LOCAL_MODCACHE="$(
            mktemp -d "${TMPDIR:-/tmp}/golib-modcache.XXXXXX"
        )"
        local_modcache_owned=1
    fi
    export GOLIB_LOCAL_MODCACHE
    export GOMODCACHE="${GOLIB_LOCAL_MODCACHE}"
    if [[ -z "${GOLIB_ISOLATED_MODFILES_DIRECTORY:-}" ]]; then
        GOLIB_ISOLATED_MODFILES_DIRECTORY="$(
            mktemp -d "${TMPDIR:-/tmp}/golib-modfiles.XXXXXX"
        )"
        isolated_modfiles_owned=1
    fi
    export GOLIB_ISOLATED_MODFILES_DIRECTORY
    mkdir -p "${GOLIB_ISOLATED_MODFILES_DIRECTORY}/bin"
    ln -sf "${root}/.golib/scripts/internal/isolated-go.sh" \
        "${GOLIB_ISOLATED_MODFILES_DIRECTORY}/bin/go"
    case ":${PATH}:" in
        *":${GOLIB_ISOLATED_MODFILES_DIRECTORY}/bin:"*) ;;
        *)
            PATH="${GOLIB_ISOLATED_MODFILES_DIRECTORY}/bin:${PATH}"
            export PATH
            ;;
    esac
    export GOFLAGS="${upstream_flags}"
}

isolated() {
    enable_local_proxy
    GOWORK=off "$@"
}

install_go_tool() {
    local package="$1"
    local executable="$2"
    local tool_directory

    enable_local_proxy
    tool_directory="${GOLIB_ISOLATED_MODFILES_DIRECTORY}/tools"
    mkdir -p "${tool_directory}"
    GOBIN="${tool_directory}" GOWORK=off \
        GOFLAGS="${GOLIB_UPSTREAM_GOFLAGS}" \
        "${GOLIB_REAL_GO}" install "${package}"
    GOLIB_GO_TOOL_PATH="${tool_directory}/${executable}"
}

run_go_tool() {
    local package="$1"
    local executable="$2"
    shift 2

    install_go_tool "${package}" "${executable}"
    isolated go exec-tool "${GOLIB_GO_TOOL_PATH}" "$@"
}

refresh_owned_sums() {
    [[ -f go.sum ]] || return 0
    local temporary
    temporary="$(mktemp "${TMPDIR:-/tmp}/golib-go-sum.XXXXXX")"
    awk '$1 !~ /^github\.com\/faustbrian\/golib\// { print }' \
        go.sum >"${temporary}"
    if cmp -s go.sum "${temporary}"; then
        rm -f "${temporary}"
    else
        mv "${temporary}" go.sum
    fi
}

applicable() {
    jq -e --arg directory "${module}" --arg gate "$1" \
        '.modules[] | select(.directory == $directory) | .gates[$gate] == true' \
        "${root}/modules.json" >/dev/null
}

package_makefile="Makefile"
if [[ "${module}" == "." && -f "${root}/.golib/package.mk" ]]; then
    package_makefile="${root}/.golib/package.mk"
fi

package_make() {
    make -f "${package_makefile}" "$@"
}

make_has_target() {
    [[ -f "${package_makefile}" ]] &&
        grep -Eq "^$1([[:space:]]+[^:]*)?:" "${package_makefile}"
}

find_make_target() {
    local target
    for target in "$@"; do
        if make_has_target "${target}"; then
            printf '%s\n' "${target}"
            return 0
        fi
    done
    return 1
}

skip_not_applicable() {
    printf '[%s] %s: not applicable by catalog policy\n' "${module}" "$1"
}

test_tags() {
    jq -r --arg directory "${module}" \
        '.modules[] | select(.directory == $directory) | .test_tags | join(",")' \
        "${root}/modules.json"
}

interoperability_declared() {
    jq -e --arg directory "${module}" \
        '.modules[] | select(.directory == $directory) |
            .interoperability_tools | length > 0' \
        "${root}/modules.json" >/dev/null
}

run_benchmark() {
    local output temporary status target
    enable_local_proxy
    output="${root}/.artifacts/${module}/benchmark.txt"
    temporary="${output}.tmp.$$"
    mkdir -p "$(dirname "${output}")"
    rm -f "${output}" "${temporary}"

    set +e
    if target="$(find_make_target benchmark performance)"; then
        package_make GOWORK=off "${target}" 2>&1 | tee "${temporary}"
        status=${PIPESTATUS[0]}
    else
        GOWORK=off go test ./... -run '^$' -bench . -benchmem 2>&1 |
            tee "${temporary}"
        status=${PIPESTATUS[0]}
    fi
    set -e

    if [[ "${status}" -ne 0 ]]; then
        rm -f "${temporary}"
        return "${status}"
    fi
    if ! grep -Eq '^Benchmark[^[:space:]]*(-[0-9]+)?[[:space:]]+' \
        "${temporary}"; then
        printf '[%s] benchmark gate produced no Go benchmark results\n' \
            "${module}" >&2
        rm -f "${temporary}"
        return 1
    fi
    mv "${temporary}" "${output}"
}

run_make_evidence() {
    local selected="$1"
    local target="$2"
    local output temporary status
    output="${root}/.artifacts/${module}/${selected}.txt"
    temporary="${output}.tmp.$$"
    mkdir -p "$(dirname "${output}")"
    rm -f "${output}" "${temporary}"

    set +e
    package_make "${target}" 2>&1 | tee "${temporary}"
    status=${PIPESTATUS[0]}
    set -e

    if [[ "${status}" -ne 0 ]]; then
        rm -f "${temporary}"
        return "${status}"
    fi
    if [[ ! -s "${temporary}" ]]; then
        printf '[%s] %s gate produced no attributable output\n' \
            "${module}" "${selected}" >&2
        rm -f "${temporary}"
        return 1
    fi
    mv "${temporary}" "${output}"
}

go_test() {
    local tags
    tags="$(test_tags)"
    if [[ -n "${tags}" ]]; then
        isolated go test -tags="${tags}" "$@"
    else
        isolated go test "$@"
    fi
}

run_gate() {
    local selected="$1"
    printf '\n[%s] %s\n' "${module}" "${selected}"
    case "${selected}" in
        format)
            if target="$(find_make_target format)"; then
                package_make GOWORK=off "${target}"
            else
                local -a format_files=()
                while IFS= read -r package_directory; do
                    while IFS= read -r -d '' source_file; do
                        format_files+=("${source_file}")
                    done < <(
                        find "${package_directory}" -maxdepth 1 -type f \
                            -name '*.go' -print0
                    )
                done < <(
                    jq -r --arg directory "${module}" \
                        '.modules[] | select(.directory == $directory) |
                            .packages[].directory' "${root}/modules.json"
                )
                if [[ "${#format_files[@]}" -gt 0 ]]; then
                    gofmt -w "${format_files[@]}"
                fi
            fi
            ;;
        format-check)
            if target="$(find_make_target format-check)"; then
                package_make GOWORK=off "${target}"
            else
                local -a format_files=()
                while IFS= read -r package_directory; do
                    while IFS= read -r -d '' source_file; do
                        format_files+=("${source_file}")
                    done < <(
                        find "${package_directory}" -maxdepth 1 -type f \
                            -name '*.go' -print0
                    )
                done < <(
                    jq -r --arg directory "${module}" \
                        '.modules[] | select(.directory == $directory) |
                            .packages[].directory' "${root}/modules.json"
                )
                unformatted=""
                if [[ "${#format_files[@]}" -gt 0 ]]; then
                    unformatted="$(gofmt -l "${format_files[@]}")"
                fi
                [[ -z "${unformatted}" ]] || {
                    printf 'unformatted Go files:\n%s\n' "${unformatted}" >&2
                    exit 1
                }
            fi
            ;;
        tidy-check)
            enable_local_proxy
            GOWORK=off GOFLAGS="${GOLIB_UPSTREAM_GOFLAGS}" go mod tidy -diff
            ;;
        tidy)
            enable_local_proxy
            refresh_owned_sums
            GOWORK=off GOFLAGS="${GOLIB_UPSTREAM_GOFLAGS}" \
                "${GOLIB_REAL_GO}" mod tidy
            ;;
        test)
            applicable tests || { skip_not_applicable tests; return; }
            packages="$(isolated go list ./...)"
            [[ -n "${packages}" ]] || {
                printf '[%s] no Go packages were executed\n' "${module}" >&2
                exit 1
            }
            go_test ./... -count=1 -timeout=20m
            ;;
        workspace-test)
            applicable tests || { skip_not_applicable tests; return; }
            go test ./... -count=1 -timeout=20m
            ;;
        race)
            applicable race || { skip_not_applicable race; return; }
            go_test -race ./... -count=1 -timeout=20m
            ;;
        coverage)
            applicable coverage || { skip_not_applicable coverage; return; }
            enable_local_proxy
            "${root}/.golib/scripts/check-coverage.sh" "${module}"
            ;;
        mutation)
            applicable mutation || { skip_not_applicable mutation; return; }
            enable_local_proxy
            "${root}/.golib/scripts/check-mutation.sh" "${module}"
            ;;
        fuzz)
            applicable fuzz || { skip_not_applicable fuzz; return; }
            enable_local_proxy
            if target="$(find_make_target fuzz fuzz-smoke)"; then
                package_make \
                    FUZZ_TIME="${GOLIB_FUZZ_SMOKE_BUDGET:-10000x}" \
                    FUZZTIME="${GOLIB_FUZZ_SMOKE_BUDGET:-10000x}" \
                    "${target}"
            else
                "${root}/.golib/scripts/check-fuzz.sh" "${module}"
            fi
            ;;
        safety)
            "${root}/.golib/scripts/check-go-safety.sh" "${module}"
            ;;
        vet)
            applicable lint || { skip_not_applicable lint; return; }
            isolated go vet ./...
            ;;
        lint)
            applicable lint || { skip_not_applicable lint; return; }
            enable_local_proxy
            GOLANGCI_LINT_CACHE="${GOLIB_ISOLATED_MODFILES_DIRECTORY}/golangci-lint-cache"
            export GOLANGCI_LINT_CACHE
            mkdir -p "${GOLANGCI_LINT_CACHE}"
            run_go_tool \
                "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}" \
                golangci-lint \
                run --allow-parallel-runners --timeout=10m ./...
            ;;
        staticcheck)
            applicable lint || { skip_not_applicable lint; return; }
            run_go_tool \
                "honnef.co/go/tools/cmd/staticcheck@${STATICCHECK_VERSION}" \
                staticcheck ./...
            ;;
        nilaway)
            applicable lint || { skip_not_applicable lint; return; }
            set +e
            run_go_tool \
                "go.uber.org/nilaway/cmd/nilaway@${NILAWAY_VERSION}" \
                nilaway \
                -include-pkgs="$(go mod edit -json | jq -r '.Module.Path')" ./...
            status=$?
            set -e
            printf '[%s] NilAway advisory exit status: %s\n' "${module}" "${status}"
            ;;
        vulnerability)
            applicable security || { skip_not_applicable security; return; }
            run_go_tool \
                "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}" \
                govulncheck ./...
            ;;
        secrets)
            applicable security || { skip_not_applicable security; return; }
            run_go_tool \
                "github.com/zricethezav/gitleaks/v8@${GITLEAKS_VERSION}" \
                gitleaks \
                dir . --config "${root}/.gitleaks.toml" --no-banner --redact
            ;;
        licenses)
            applicable security || { skip_not_applicable security; return; }
            test -s "${root}/LICENSE"
            module_path="$(go mod edit -json | jq -er '.Module.Path')"
            if [[ "${module_path}" != "github.com/faustbrian/go-sequencer" &&
                "${module_path}" != github.com/faustbrian/go-sequencer/* ]]; then
                printf '[%s] refusing to ignore non-owned module license: %s\n' \
                    "${module}" "${module_path}" >&2
                exit 1
            fi
            run_go_tool \
                "github.com/google/go-licenses/v2@${GO_LICENSES_VERSION}" \
                go-licenses \
                check ./... \
                --ignore "github.com/faustbrian/go-sequencer"
            ;;
        sbom)
            applicable security || { skip_not_applicable security; return; }
            mkdir -p "${root}/.artifacts/${module}"
            run_go_tool \
                "github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@${CYCLONEDX_VERSION}" \
                cyclonedx-gomod \
                mod -json -licenses -type library -noserial -notimestamp \
                -output "${root}/.artifacts/${module}/sbom.json" .
            test -s "${root}/.artifacts/${module}/sbom.json"
            ;;
        docs)
            applicable documentation || { skip_not_applicable documentation; return; }
            enable_local_proxy
            if [[ "${module}" == "." ]]; then
                GOWORK=off "${root}/.golib/scripts/check-documentation.sh"
            fi
            if target="$(find_make_target docs documentation)"; then
                package_make "${target}"
            elif [[ "${module}" != "." ]]; then
                GOWORK=off go test ./... -run '^Example' -count=1
            fi
            ;;
        api)
            applicable api_compatibility || { skip_not_applicable api_compatibility; return; }
            install_go_tool \
                "golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" \
                apidiff
            export GOLIB_APIDIFF="${GOLIB_GO_TOOL_PATH}"
            if target="$(find_make_target api-compat api-check api compatibility)"; then
                package_make "${target}"
            elif [[ -x "./scripts/check-api.sh" ]]; then
                GOWORK=off ./scripts/check-api.sh
            else
                "${root}/.golib/scripts/check-api-baseline.sh" "${module}"
            fi
            ;;
        api-update)
            applicable api_compatibility || { skip_not_applicable api_compatibility; return; }
            install_go_tool \
                "golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" \
                apidiff
            export GOLIB_APIDIFF="${GOLIB_GO_TOOL_PATH}"
            "${root}/.golib/scripts/update-api-baseline.sh" "${module}"
            ;;
        conformance)
            applicable conformance || { skip_not_applicable conformance; return; }
            enable_local_proxy
            if target="$(find_make_target conformance specification)"; then
                run_make_evidence conformance "${target}"
            else
                printf '[%s] conformance is declared but has no command\n' \
                    "${module}" >&2
                exit 1
            fi
            ;;
        interoperability)
            enable_local_proxy
            if target="$(find_make_target interoperability integration conformance)"; then
                run_make_evidence interoperability "${target}"
            elif interoperability_declared; then
                printf '[%s] interoperability is declared but has no command\n' \
                    "${module}" >&2
                exit 1
            else
                skip_not_applicable interoperability
            fi
            ;;
        benchmark)
            applicable benchmarks || { skip_not_applicable benchmarks; return; }
            run_benchmark
            ;;
        release-dry-run)
            "${root}/.golib/scripts/release.sh" --dry-run "${module}"
            ;;
        release-public)
            "${root}/.golib/scripts/release.sh" --dry-run --public "${module}"
            ;;
        check)
            while IFS= read -r required_gate; do
                [[ -n "${required_gate}" ]] || continue
                run_gate "${required_gate}"
            done <"${root}/.golib/scripts/check-gates.txt"
            ;;
        *)
            printf 'unknown gate: %s\n' "${selected}" >&2
            exit 2
            ;;
    esac
}

set -a
# shellcheck disable=SC1091 # Repository-pinned tool versions.
source "${root}/.golib/versions.env"
set +a
cd "${directory}"
run_gate "${gate}"
