#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    printf 'usage: %s <gate> <module-directory> [package-directory]\n' "$0" >&2
    exit 2
fi

root="${GOLIB_ROOT:-$(git rev-parse --show-toplevel)}"
gate="$1"
module="$2"
package="${3:-}"
input_policy="${GOLIB_GATE_INPUT_POLICY:-current}"
case "${input_policy}" in
    current|legacy-api-baseline|legacy-runner-isolation|legacy-runner-isolation-post) ;;
    *)
        printf 'unsupported gate input policy: %s\n' "${input_policy}" >&2
        exit 2
        ;;
esac
if ! jq -e --arg directory "${module}" \
    '.modules[] | select(.directory == $directory)' \
    "${root}/modules.json" >/dev/null; then
    printf 'module is absent from modules.json: %s\n' "${module}" >&2
    exit 2
fi

manifest="$(mktemp "${TMPDIR:-/tmp}/golib-gate-inputs.XXXXXX")"
directories="$(mktemp "${TMPDIR:-/tmp}/golib-gate-directories.XXXXXX")"
input_files="$(mktemp "${TMPDIR:-/tmp}/golib-gate-files.XXXXXX")"
package_data="${manifest}.packages"
relevant_package_data="${manifest}.relevant-packages"
existing_files="${manifest}.existing"
file_hashes="${manifest}.hashes"
nested_directories="${manifest}.nested"
bounded_output="${manifest}.bounded-output"
owned_module_paths="${manifest}.owned-modules"
digest_modfile=""
cleanup() {
    rm -f \
        "${manifest}" "${directories}" "${input_files}" "${package_data}" \
        "${relevant_package_data}" \
        "${existing_files}" "${file_hashes}" "${nested_directories}" \
        "${bounded_output}" "${owned_module_paths}"
    if [[ -n "${digest_modfile}" ]]; then
        rm -f "${digest_modfile}" "${digest_modfile%.mod}.sum"
    fi
}
trap cleanup EXIT HUP INT TERM

append_value() {
    printf 'value  %s=%s\n' "$1" "$2" >>"${manifest}"
}

append_version_value() {
    local name="$1"
    local value
    if ! value="$({
        # shellcheck source=/dev/null
        source "${root}/.golib/versions.env"
        set +u
        printf '%s' "${!name}"
    })" || [[ -z "${value}" ]]; then
        printf 'required version value is missing: %s\n' "${name}" >&2
        exit 1
    fi
    append_value "version:${name}" "${value}"
}

append_required_service_versions() {
    local service variable
    while IFS= read -r service; do
        [[ -n "${service}" ]] || continue
        case "${service}" in
            postgresql) variable=POSTGRES_IMAGE ;;
            valkey) variable=VALKEY_IMAGE ;;
            redis) variable=REDIS_IMAGE ;;
            nats) variable=NATS_IMAGE ;;
            nsq) variable=NSQ_IMAGE ;;
            rabbitmq) variable=RABBITMQ_IMAGE ;;
            rabbitstream-standalone)
                append_file "${root}/.golib/services/rabbitstream/enabled_plugins"
                append_file "${root}/.golib/services/rabbitstream/standalone-compose.yaml"
                append_file "${root}/.golib/services/rabbitstream/standalone-setup.sh"
                append_file "${root}/.golib/services/rabbitstream/standalone.conf"
                continue
                ;;
            rabbitstream)
                append_file "${root}/.golib/services/rabbitstream/compose.yaml"
                append_file "${root}/.golib/services/rabbitstream/enabled_plugins"
                append_file "${root}/.golib/services/rabbitstream/rabbit1.conf"
                append_file "${root}/.golib/services/rabbitstream/rabbit2.conf"
                append_file "${root}/.golib/services/rabbitstream/rabbit3.conf"
                append_file "${root}/.golib/services/rabbitstream/setup.sh"
                append_file "${root}/.golib/services/rabbitstream/standalone-compose.yaml"
                append_file "${root}/.golib/services/rabbitstream/standalone-setup.sh"
                append_file "${root}/.golib/services/rabbitstream/standalone.conf"
                append_file "${root}/.golib/services/rabbitstream/tls-compose.yaml"
                append_file "${root}/.golib/services/rabbitstream/tls-rabbitmq.conf"
                append_file "${root}/.golib/services/rabbitstream/tls-setup.sh"
                continue
                ;;
            opensearch)
                append_file "${root}/.golib/services/opensearch/opensearch-images.env"
                continue
                ;;
            *)
                printf 'unsupported required service %s for %s\n' \
                    "${service}" "${module}" >&2
                exit 1
                ;;
        esac
        append_version_value "${variable}"
    done < <(jq -r --arg directory "${module}" '
        .modules[]
        | select(.directory == $directory)
        | (.required_services // [])[]
    ' "${root}/modules.json" | LC_ALL=C sort -u)
}

append_gate_tool_versions() {
    case "${gate}" in
        lint) append_version_value GOLANGCI_LINT_VERSION ;;
        staticcheck) append_version_value STATICCHECK_VERSION ;;
        nilaway) append_version_value NILAWAY_VERSION ;;
        vulnerability) append_version_value GOVULNCHECK_VERSION ;;
        secrets) append_version_value GITLEAKS_VERSION ;;
        licenses) append_version_value GO_LICENSES_VERSION ;;
        sbom) append_version_value CYCLONEDX_VERSION ;;
        api|api-update) append_version_value APIDIFF_VERSION ;;
        workflow) append_version_value ACTIONLINT_VERSION ;;
        # Interoperability scripts may use package-specific pinned tools that
        # are not yet represented individually in the module catalog.
        interoperability) append_file "${root}/.golib/versions.env" ;;
    esac
}

append_file() {
    local file="$1"
    local relative digest
    [[ -f "${file}" ]] || {
        printf 'gate input is missing: %s\n' "${file}" >&2
        exit 1
    }
    relative="${file#"${root}/"}"
    digest="$(shasum -a 256 "${file}" | awk '{print $1}')"
    printf 'file   %s  %s\n' "${digest}" "${relative}" >>"${manifest}"
}

append_mutation_module_manifest() {
    local file="$1"
    local relative digest
    if [[ ! -f "${owned_module_paths}" ]]; then
        jq -r '.modules[].module_path' "${root}/modules.json" |
            LC_ALL=C sort -u >"${owned_module_paths}"
    fi
    relative="${file#"${root}/"}"
    digest="$({
        GOLIB_OWNED_MODULE_PATHS="${owned_module_paths}" perl -pe '
            BEGIN {
                open my $paths, "<", $ENV{GOLIB_OWNED_MODULE_PATHS}
                    or die "open owned module paths: $!";
                chomp(@owned = <$paths>);
            }
            for my $owned (@owned) {
                s/(\Q$owned\E)([ \t]+)v[^\s]+/$1$2v0.0.0/g;
            }
        ' "${file}"
    } | shasum -a 256 | awk '{print $1}')"
    printf 'file   %s  %s\n' "${digest}" "${relative}" >>"${manifest}"
}

append_repository_files() {
    local file
    : >"${existing_files}"
    while IFS= read -r file; do
        [[ -n "${file}" ]] || continue
        if [[ -f "${root}/${file}" ]]; then
            printf '%s\n' "${file}" >>"${existing_files}"
        else
            append_value missing-file "${file}"
        fi
    done
    [[ -s "${existing_files}" ]] || return
    git -C "${root}" hash-object --stdin-paths \
        <"${existing_files}" >"${file_hashes}"
    paste "${file_hashes}" "${existing_files}" |
        awk -F '\t' '{ printf "file   %s  %s\n", $1, $2 }' >>"${manifest}"
}

append_module_files() {
    local directory="$1"
    local include_api_baseline=0
    local include_documentation=0
    local include_secret_policy=0
    local include_tests=0
    if [[ "${input_policy}" == "legacy-api-baseline" ]]; then
        include_api_baseline=1
    fi
    if [[ "${directory}" == "${module}" ]]; then
        include_tests=1
    fi
    case "${gate}" in
        api|api-update)
            include_api_baseline=1
            ;;
        docs)
            include_documentation=1
            ;;
        secrets)
            include_documentation=1
            include_secret_policy=1
            ;;
    esac
    : >"${nested_directories}"
    jq -r --arg directory "${directory}" '
        .modules[]
        | .directory
        | select(
            $directory != "." and
            . != $directory and
            startswith($directory + "/")
        )
    ' "${root}/modules.json" >"${nested_directories}"
    git -C "${root}" ls-files -co --exclude-standard -- "${directory}" |
        awk \
            -v include_api_baseline="${include_api_baseline}" \
            -v include_documentation="${include_documentation}" \
            -v include_secret_policy="${include_secret_policy}" \
            -v include_tests="${include_tests}" \
            -v module_directory="${directory}" '
            FILENAME != "-" {
                nested[++count] = $0
                next
            }
            {
                relative = tolower($0)
                module_prefix = module_directory == "." ? "" : tolower(module_directory) "/"
                if (module_prefix != "" && substr(relative, 1, length(module_prefix)) == module_prefix) {
                    relative = substr(relative, length(module_prefix) + 1)
                }
                is_markdown = relative ~ /\.(md|markdown)$/
                in_documentation = relative ~ /^(docs|\.ai)\//
                in_test_data = relative ~ /(^|\/)(testdata|fixtures|corpus)\//
                is_named_documentation = relative ~ /(^|\/)(readme|changelog|contributing|security|code_of_conduct|support)\.(md|markdown)$/
                is_generated_documentation = relative == "llms.txt" || relative == "llms-full.txt"
                is_api_baseline = relative == "api/baseline.txt"
                is_repository_catalog = relative == "modules.json" || relative == "packages.json"
                is_secret_policy = relative == ".gitleaks.toml"
                is_test_source = relative ~ /_test\.go$/
                skip_documentation = !include_documentation && (is_generated_documentation || (is_markdown && (in_documentation || (!in_test_data && is_named_documentation))))
                skip_secret_policy = !include_secret_policy && is_secret_policy
                if (is_repository_catalog) {
                    next
                }
                if (!include_api_baseline && is_api_baseline) {
                    next
                }
                if (skip_secret_policy) {
                    next
                }
                if (!include_tests && is_test_source) {
                    next
                }
                for (position = 1; position <= count; position++) {
                    prefix = nested[position] "/"
                    if ($0 == nested[position] || substr($0, 1, length(prefix)) == prefix) {
                        next
                    }
                }
                if (skip_documentation) {
                    next
                }
                print
            }
        ' "${nested_directories}" - |
        while IFS= read -r file; do
            if [[ -e "${root}/${file}" || -L "${root}/${file}" ]]; then
                printf '%s\n' "${file}"
            fi
        done >>"${input_files}"
}

append_tool_inputs() {
    append_version_value GREMLINS_VERSION
    append_required_service_versions
    append_file "${root}/.golib/scripts/internal/mutation-command.sh"
    append_file "${root}/.golib/scripts/internal/mutation-coverage.sh"
    append_file "${root}/.golib/scripts/patches/gremlins-run-all-mutants.patch"
    append_file "${root}/.golib/scripts/patches/gremlins-shared-coverage.patch"
    append_file "${root}/.golib/scripts/start-services.sh"
}

append_environment() {
    append_value go-version "$(go env GOVERSION)"
    append_value goos "$(go env GOOS)"
    append_value goarch "$(go env GOARCH)"
    append_value cgo-enabled "$(go env CGO_ENABLED)"
}

append_assurance_environment() {
    if [[ -z "${GOLIB_ASSURANCE_GO_VERSION:-}" &&
        -z "${GOLIB_ASSURANCE_GOOS:-}" &&
        -z "${GOLIB_ASSURANCE_GOARCH:-}" &&
        -z "${GOLIB_ASSURANCE_CGO_ENABLED:-}" &&
        -z "${GOLIB_ASSURANCE_KERNEL:-}" &&
        -z "${GOLIB_ASSURANCE_NODE:-}" ]]; then
        return 1
    fi
    if [[ -z "${GOLIB_ASSURANCE_GO_VERSION:-}" ||
        -z "${GOLIB_ASSURANCE_GOOS:-}" ||
        -z "${GOLIB_ASSURANCE_GOARCH:-}" ||
        -z "${GOLIB_ASSURANCE_CGO_ENABLED:-}" ||
        -z "${GOLIB_ASSURANCE_KERNEL:-}" ||
        -z "${GOLIB_ASSURANCE_NODE:-}" ]]; then
        printf 'operational-assurance environment override is incomplete\n' >&2
        exit 2
    fi
    local value
    for value in \
        "${GOLIB_ASSURANCE_GO_VERSION}" \
        "${GOLIB_ASSURANCE_GOOS}" \
        "${GOLIB_ASSURANCE_GOARCH}" \
        "${GOLIB_ASSURANCE_CGO_ENABLED}" \
        "${GOLIB_ASSURANCE_KERNEL}" \
        "${GOLIB_ASSURANCE_NODE}"; do
        if [[ "${value}" == *$'\n'* || "${value}" == *$'\r'* ]]; then
            printf 'operational-assurance environment override contains control characters\n' >&2
            exit 2
        fi
    done
    if [[ "${GOLIB_ASSURANCE_CGO_ENABLED}" != "0" &&
        "${GOLIB_ASSURANCE_CGO_ENABLED}" != "1" ]]; then
        printf 'operational-assurance cgo override must be 0 or 1\n' >&2
        exit 2
    fi
    append_value go-version "${GOLIB_ASSURANCE_GO_VERSION}"
    append_value goos "${GOLIB_ASSURANCE_GOOS}"
    append_value goarch "${GOLIB_ASSURANCE_GOARCH}"
    append_value cgo-enabled "${GOLIB_ASSURANCE_CGO_ENABLED}"
    append_value kernel "${GOLIB_ASSURANCE_KERNEL}"
    append_value node "${GOLIB_ASSURANCE_NODE}"
}

bounded_command_output() {
    local timeout_seconds="$1"
    shift
    local command_pid status timer_pid
    : >"${bounded_output}"
    "$@" >"${bounded_output}" 2>/dev/null &
    command_pid=$!
    (
        sleep "${timeout_seconds}"
        kill -TERM "${command_pid}" 2>/dev/null || true
        sleep 1
        kill -KILL "${command_pid}" 2>/dev/null || true
    ) &
    timer_pid=$!
    status=0
    wait "${command_pid}" || status=$?
    kill -TERM "${timer_pid}" 2>/dev/null || true
    wait "${timer_pid}" 2>/dev/null || true
    if [[ "${status}" -eq 0 ]]; then
        cat "${bounded_output}"
    fi
    return "${status}"
}

append_legacy_docker_environment() {
    local docker_timeout docker_value
    if ! jq -e --arg directory "${module}" '
        .modules[]
        | select(.directory == $directory)
        | select((.required_services // []) | length > 0)
    ' "${root}/modules.json" >/dev/null; then
        append_value docker not-required
        return
    fi
    if [[ -n "${GOLIB_LEGACY_DOCKER_VALUE:-}" ]]; then
        append_value docker "${GOLIB_LEGACY_DOCKER_VALUE}"
        return
    fi
    if ! command -v docker >/dev/null 2>&1; then
        append_value docker missing
        return
    fi
    docker_timeout="${GOLIB_DOCKER_VERSION_TIMEOUT_SECONDS:-5}"
    if [[ ! "${docker_timeout}" =~ ^[1-9][0-9]*$ ]]; then
        printf 'invalid Docker version timeout: %s\n' "${docker_timeout}" >&2
        exit 1
    fi
    docker_value="$({
        bounded_command_output \
            "${docker_timeout}" \
            docker version --format '{{.Server.Version}}' ||
            printf unavailable
    })"
    append_value docker "${docker_value}"
}

append_verification_environment() {
    if [[ "${gate}" == "operational-assurance" ]] && append_assurance_environment; then
        return
    fi
    append_environment
    append_value kernel "$(uname -srm)"
    # Pinned service images define the runtime contract. Live daemon
    # availability is orchestration state and must not invalidate evidence.
    # The legacy policy reproduces the old identity solely for verified,
    # one-time evidence migration.
    if [[ "${input_policy}" == "legacy-api-baseline" ]]; then
        append_legacy_docker_environment
    fi
    if command -v node >/dev/null 2>&1; then
        append_value node "$(node --version)"
    else
        append_value node missing
    fi
}

normalize_irrelevant_formatter_dispatch() {
    awk -v selected_gate="${gate}" '
        BEGIN { quote = sprintf("%c", 39) }
        selected_gate != "format" && selected_gate != "format-check" &&
        $0 == "        format)" {
            print "        format)"
            print "            find . -name " quote "*.go" quote " -not -path " quote "./.tools/*" quote " -print0 | xargs -0 gofmt -w"
            print "            ;;"
            print "        format-check)"
            print "            unformatted=\"$(find . -name " quote "*.go" quote " -not -path " quote "./.tools/*" quote " -print0 | xargs -0 gofmt -l)\""
            print "            [[ -z \"${unformatted}\" ]] || {"
            print "                printf " quote "unformatted Go files:\\n%s\\n" quote " \"${unformatted}\" >&2"
            print "                exit 1"
            print "            }"
            print "            ;;"
            skip_formatter_dispatch = 1
            next
        }
        skip_formatter_dispatch && $0 == "        tidy-check)" {
            skip_formatter_dispatch = 0
            print
            next
        }
        skip_formatter_dispatch { next }
        { print }
    '
}

append_verification_tool_files() {
    local check_module_digest legacy_runner_digest
    local paths=(
        scripts/start-services.sh
    )
    if [[ "${gate}" == "operational-assurance" && "${input_policy}" == "current" ]]; then
        paths+=(
            scripts/create-verification-snapshot.sh
            scripts/run-modules.sh
            scripts/stop-services.sh
        )
    elif [[ "${input_policy}" == "legacy-runner-isolation" ||
        "${input_policy}" == "legacy-runner-isolation-post" ]]; then
        paths+=(
            scripts/create-verification-snapshot.sh
            scripts/stop-services.sh
        )
        # These are the exact Git blob identities immediately before and after
        # process isolation, matching append_repository_files. They identify
        # retained evidence without requiring that history to remain available.
        legacy_runner_digest='cf841512fc1e48c8c7708259c878028f06a8726f'
        if [[ "${input_policy}" == "legacy-runner-isolation-post" ]]; then
            legacy_runner_digest='d30bc5a6f7e52b2080a3fe13200dfb2963a1415a'
        fi
        printf 'file   %s  %s\n' \
            "${legacy_runner_digest}" \
            'scripts/run-modules.sh' >>"${manifest}"
    else
        # Snapshot creation, module selection, and post-gate cleanup do not
        # alter a single gate's command or inputs. Their behavior belongs to
        # aggregate-run evidence rather than every package checkpoint.
        append_value verification-orchestration-contract v1
    fi
    append_gate_tool_versions
    append_required_service_versions
    case "${gate}" in
        format-check|workspace-test|safety|benchmark|release-public) ;;
        *)
            paths+=(
                scripts/build-local-proxy.sh
                scripts/internal/isolated-go.sh
            )
            ;;
    esac
    case "${gate}" in
        coverage)
            paths+=(scripts/check-coverage.sh)
            ;;
        fuzz)
            paths+=(scripts/check-fuzz.sh)
            ;;
        safety)
            paths+=(scripts/check-go-safety.sh)
            ;;
        api)
            paths+=(scripts/check-api-baseline.sh)
            ;;
        api-update)
            paths+=(scripts/update-api-baseline.sh)
            ;;
        licenses)
            paths+=(LICENSE)
            ;;
        release-dry-run|release-public)
            paths+=(
                scripts/filter-releasable-modules.sh
                scripts/release.sh
            )
            ;;
    esac
    git -C "${root}" ls-files -co --exclude-standard -- \
        "${paths[@]}" >>"${input_files}"

    # Runner concurrency changes whether a linter can start, not its findings.
    # Keep successful gate evidence bound to the analyzer contract rather than
    # invalidating every package when isolated runners are made parallel-safe.
    if [[ -f "${root}/.golib/scripts/check-module.sh" ]]; then
        if [[ "${gate}" == "docs" ]]; then
            check_module_digest="$(
                normalize_irrelevant_formatter_dispatch \
                    <"${root}/.golib/scripts/check-module.sh" |
                    sed 's/ --allow-parallel-runners//g' |
                    git hash-object --stdin
            )"
        else
            # Root-only documentation dispatch cannot alter another gate's
            # executable contract or invalidate its retained evidence.
            check_module_digest="$(
                awk '
                    $0 == "            if [[ \"${module}\" == \".\" ]]; then" {
                        print "            if target=\"$(find_make_target docs documentation)\"; then"
                        skip_root_documentation = 1
                        next
                    }
                    skip_root_documentation && $0 == "            elif target=\"$(find_make_target docs documentation)\"; then" {
                        skip_root_documentation = 0
                        next
                    }
                    skip_root_documentation { next }
                    { print }
                ' "${root}/.golib/scripts/check-module.sh" |
                    normalize_irrelevant_formatter_dispatch |
                    sed 's/ --allow-parallel-runners//g' |
                    git hash-object --stdin
            )"
        fi
        printf 'file   %s  %s\n' \
            "${check_module_digest}" \
            'scripts/check-module.sh' >>"${manifest}"
    else
        append_value missing-file scripts/check-module.sh
    fi
}

verification_module_policy() {
    local directory="$1"
    jq -S -c --arg directory "${directory}" '
        .modules[]
        | select(.directory == $directory)
        | {
            directory,
            module_path,
            owned_dependencies,
            required_services,
            test_tags,
            interoperability_tools,
            gates
        }
    ' "${root}/modules.json"
}

verification_digest() {
    local directory file
    local repository_paths=()
    append_value gate "${gate}"
    append_value module "${module}"
    append_verification_environment

    printf '%s\n' "${module}" >"${directories}"
    jq -r --arg directory "${module}" '
        . as $catalog
        | def closure($seen):
            ([
                $catalog.modules[]
                | select(.module_path as $path | $seen | index($path))
                | .owned_dependencies[]
            ] | unique) as $dependencies
            | ($seen + $dependencies | unique) as $next
            | if $next == $seen then $next else closure($next) end;
        (.modules[] | select(.directory == $directory).owned_dependencies) as $owned
        | closure($owned) as $paths
        | .modules[]
        | select(.module_path as $path | $paths | index($path))
        | .directory
    ' "${root}/modules.json" >>"${directories}"

    while IFS= read -r directory; do
        [[ -n "${directory}" ]] || continue
        append_value "module-policy:${directory}" "$(
            verification_module_policy "${directory}"
        )"
        append_value "package-policy:${directory}" "$(
            jq -S -c --arg directory "${directory}" \
                '[.packages[] | select(.module_directory == $directory)]' \
                "${root}/packages.json"
        )"
        append_module_files "${directory}"
    done < <(LC_ALL=C sort -u "${directories}")
    append_verification_tool_files
    case "${gate}" in
        secrets)
            repository_paths+=(.gitleaks.toml)
            ;;
        benchmark|workspace-test)
            repository_paths+=(go.work)
            ;;
    esac
    if [[ "${#repository_paths[@]}" -gt 0 ]]; then
        git -C "${root}" ls-files -co --exclude-standard -- \
            "${repository_paths[@]}" >>"${input_files}"
    fi

    LC_ALL=C sort -u "${input_files}" | append_repository_files
}

mutation_digest() {
    local package_directory package_input_digest
    append_value gate mutation
    append_value module "${module}"
    append_file "${root}/.golib/scripts/check-mutation.sh"
    append_file "${root}/.golib/scripts/internal/configure-mutation-workers.sh"
    append_file "${root}/.golib/scripts/internal/run-mutation.sh"
    append_file "${root}/.golib/scripts/internal/mutation-scratch.sh"
    append_file "${root}/.golib/scripts/internal/reuse-mutation-coverage.sh"
    while IFS= read -r package_directory; do
        [[ -n "${package_directory}" ]] || continue
        package_input_digest="$(
            "${root}/.golib/scripts/gate-input-digest.sh" \
                mutation "${module}" "${package_directory}"
        )"
        append_value "package:${package_directory}" "${package_input_digest}"
    done < <(
        jq -r --arg directory "${module}" '
            .modules[]
            | select(.directory == $directory)
            | .packages[]
            | select(.coverage_required == true)
            | .directory
        ' "${root}/modules.json" | LC_ALL=C sort
    )
}

legacy_digest() {
    local directory
    append_value gate mutation
    append_value module "${module}"
    append_environment
    append_value module-policy "$(
        jq -S -c --arg directory "${module}" \
            '.modules[] | select(.directory == $directory)' \
            "${root}/modules.json"
    )"
    append_value package-policy "$(
        jq -S -c --arg directory "${module}" \
            '[.packages[] | select(.module_directory == $directory)]' \
            "${root}/packages.json"
    )"
    append_value zero-mutant-policy "$(
        jq -S -c --arg directory "${module}" \
            '[.packages[] | select(.module_directory == $directory)]' \
            "${root}/.golib/mutation-zero-inventory.json"
    )"

    append_file "${root}/.golib/versions.env"
    append_file "${root}/.golib/scripts/build-golib-gremlins.sh"
    append_file "${root}/.golib/scripts/internal/mutation-command.sh"
    append_file "${root}/.golib/scripts/patches/gremlins-run-all-mutants.patch"
    append_file "${root}/.golib/scripts/patches/gremlins-shared-coverage.patch"
    append_file "${root}/.golib/scripts/start-services.sh"

    printf '%s\n' "${module}" >"${directories}"
    jq -r --arg directory "${module}" '
        . as $catalog
        | def closure($seen):
            ([
                $catalog.modules[]
                | select(.module_path as $path | $seen | index($path))
                | .owned_dependencies[]
            ] | unique) as $dependencies
            | ($seen + $dependencies | unique) as $next
            | if $next == $seen then $next else closure($next) end;
        (.modules[] | select(.directory == $directory).owned_dependencies) as $owned
        | closure($owned) as $paths
        | .modules[]
        | select(.module_path as $path | $paths | index($path))
        | .directory
    ' "${root}/modules.json" >>"${directories}"

    while IFS= read -r directory; do
        [[ -n "${directory}" ]] || continue
        while IFS= read -r -d '' file; do
            append_file "${file}"
        done < <(
            find "${root}/${directory}" -type f \
                ! -path '*/.git/*' \
                ! -path '*/.artifacts/*' \
                ! -path '*/.tools/*' \
                ! -name '*.coverprofile' \
                ! -name 'coverage.out' \
                -print0 | LC_ALL=C sort -z
        )
    done < <(LC_ALL=C sort -u "${directories}")
}

package_digest() {
    local data_name digest_go digest_go_flags digest_workspace flag owned_count
    local module_path module_root package_directory resolution tags
    local target_import_path
    module_root="${root}/${module}"
    if ! jq -e --arg directory "${module}" --arg package "${package}" '
        .modules[]
        | select(.directory == $directory)
        | .packages[]
        | select(.directory == $package and .coverage_required == true)
    ' "${root}/modules.json" >/dev/null; then
        printf 'mutation package is absent from catalog: %s %s\n' \
            "${module}" "${package}" >&2
        exit 2
    fi

    append_value gate mutation
    append_value module "${module}"
    append_value package "${package}"
    append_environment
    append_value module-policy "$(
        jq -S -c --arg directory "${module}" '
            .modules[]
            | select(.directory == $directory)
            | {
                directory,
                module_path,
                go_version,
                owned_dependencies,
                required_services,
                test_tags,
                mutation: .gates.mutation
            }
        ' "${root}/modules.json"
    )"
    append_value package-policy "$(
        jq -S -c --arg directory "${module}" --arg package "${package}" '
            .modules[]
            | select(.directory == $directory)
            | .packages[]
            | select(.directory == $package)
            | {
                module_directory,
                directory,
                name,
                import_path,
                kind,
                production,
                executable,
                coverage_required
            }
        ' "${root}/modules.json"
    )"
    append_value zero-mutant-policy "$(
        jq -S -c --arg directory "${module}" --arg package "${package}" '
            [.packages[] | select(
                .module_directory == $directory and
                .package_directory == $package
            )]
        ' "${root}/.golib/mutation-zero-inventory.json"
    )"
    if [[ "${module}" == "pkg/ecma-regexp" ]]; then
        append_value mutation-test-environment "TEST262_ROOT=unset"
    fi
    # Evidence orchestration does not affect which mutants execute or which
    # tests observe them. Campaign semantics are captured by append_tool_inputs.
    append_tool_inputs

    module_path="$(jq -r --arg directory "${module}" '
        .modules[]
        | select(.directory == $directory)
        | .module_path
    ' "${root}/modules.json")"
    tags="$(jq -r --arg directory "${module}" '
        .modules[]
        | select(.directory == $directory)
        | .test_tags
        | join(",")
    ' "${root}/modules.json")"
    resolution="${GOLIB_MUTATION_DIGEST_RESOLUTION:-stable}"
    if [[ "${resolution}" == "caller" ]]; then
        (
            cd "${module_root}"
            if [[ -n "${tags}" ]]; then
                go list -deps -test -json -tags="${tags}" ./...
            else
                go list -deps -test -json ./...
            fi
        ) >"${package_data}"
    elif [[ "${resolution}" != "stable" &&
        "${resolution}" != "observer-v1" &&
        "${resolution}" != "legacy-stable" ]]; then
        printf 'unknown mutation digest resolution: %s\n' \
            "${resolution}" >&2
        exit 2
    else
        digest_go="${GOLIB_REAL_GO:-$(command -v go)}"
        digest_workspace=off
        digest_go_flags=""
        for flag in ${GOLIB_UPSTREAM_GOFLAGS:-${GOFLAGS:-}}; do
            case "${flag}" in
                -mod=*|-modfile=*) ;;
                *)
                    digest_go_flags="$(
                        printf '%s%s' \
                            "${digest_go_flags:+${digest_go_flags} }" "${flag}"
                    )"
                    ;;
            esac
        done
        owned_count="$(jq -r --arg directory "${module}" '
            .modules[] | select(.directory == $directory) | .owned_dependencies | length
        ' "${root}/modules.json")"
        if [[ "${owned_count}" -gt 0 ]]; then
            digest_modfile="${manifest}.mutation.mod"
            cp "${module_root}/go.mod" "${digest_modfile}"
            if [[ -f "${module_root}/go.sum" ]]; then
                cp "${module_root}/go.sum" "${digest_modfile%.mod}.sum"
            fi
            while IFS=$'\t' read -r owned_path owned_directory; do
                [[ -n "${owned_path}" && -n "${owned_directory}" ]] || continue
                GOWORK=off "${digest_go}" mod edit -modfile="${digest_modfile}" \
                    -replace="${owned_path}=${root}/${owned_directory}"
            done < <(jq -r --arg directory "${module}" '
                . as $catalog
                | def closure($seen):
                    ([ $catalog.modules[]
                       | select(.module_path as $path | $seen | index($path))
                       | .owned_dependencies[] ] | unique) as $dependencies
                    | ($seen + $dependencies | unique) as $next
                    | if $next == $seen then $next else closure($next) end;
                (.modules[] | select(.directory == $directory).owned_dependencies) as $owned
                | closure($owned) as $paths
                | .modules[]
                | select(.module_path as $path | $paths | index($path))
                | [.module_path, .directory]
                | @tsv
            ' "${root}/modules.json")
            digest_go_flags="$(
                printf '%s%s' "${digest_go_flags:+${digest_go_flags} }" \
                    "-modfile=${digest_modfile} -mod=mod"
            )"
        fi
        (
            cd "${module_root}"
            if [[ -n "${tags}" ]]; then
                GOWORK="${digest_workspace}" GOFLAGS="${digest_go_flags}" \
                    "${digest_go}" list -deps -test -json \
                    -tags="${tags}" ./...
            else
                GOWORK="${digest_workspace}" GOFLAGS="${digest_go_flags}" \
                    "${digest_go}" list -deps -test -json ./...
            fi
        ) >"${package_data}"
    fi

    if [[ "${resolution}" == "legacy-stable" ]]; then
        jq -s \
            --arg root "${root}/" \
            --arg module_path "${module_path}" '
            .[]
            | select((.Dir // "") | startswith($root))
            | . + {
                GolibMutationObserver:
                    ((.Module.Path // "") == $module_path)
            }
        ' "${package_data}" >"${relevant_package_data}"
    elif [[ "${resolution}" == "observer-v1" ]]; then
        target_import_path="${module_path}"
        if [[ "${package}" != "." ]]; then
            target_import_path="${module_path}/${package}"
        fi
        jq -s \
            --arg root "${root}/" \
            --arg target "${target_import_path}" '
        def canonical_import:
            (.ImportPath // "" | sub(" \\[.*$"; ""));
        [.[] | select(
            canonical_import == $target or
            (.ForTest // "") == $target
        )] as $observers
        | ([$observers[].Dir] | unique) as $observer_directories
        | ([
            $observers[]
            | canonical_import, (.Deps // [])[]
        ] | unique) as $relevant_imports
        | .[]
        | select((.Dir // "") | startswith($root))
        | select(
            canonical_import as $import
            | ($relevant_imports | index($import)) != null
        )
        | .Dir as $directory
        | . + {
            GolibMutationObserver:
                (($observer_directories | index($directory)) != null)
        }
        ' "${package_data}" >"${relevant_package_data}"
    else
        target_import_path="${module_path}"
        if [[ "${package}" != "." ]]; then
            target_import_path="${module_path}/${package}"
        fi
        jq -s \
            --arg root "${root}/" \
            --arg target "${target_import_path}" '
        def canonical_import:
            (.ImportPath // "" | sub(" \\[.*$"; ""));
        [.[] | select(
            (canonical_import == $target and
                (.ForTest // "") == "") or
            (.ForTest // "") == $target
        )] as $observers
        | ([$observers[].Dir] | unique) as $observer_directories
        | ([
            $observers[]
            | canonical_import, (.Deps // [])[]
        ] | unique) as $relevant_imports
        | .[]
        | select((.Dir // "") | startswith($root))
        | select(
            (.ForTest // "") == "" or
            (.ForTest // "") == $target
        )
        | select(
            canonical_import as $import
            | ($relevant_imports | index($import)) != null
        )
        | .Dir as $directory
        | . + {
            GolibMutationObserver:
                (($observer_directories | index($directory)) != null)
        }
        ' "${package_data}" >"${relevant_package_data}"
    fi

    jq -r --arg root "${root}/" --arg module_path "${module_path}" '
        select(.Dir | startswith($root))
        | .Dir as $directory
        | (
            [
                .GoFiles[]?,
                .CgoFiles[]?,
                .CFiles[]?,
                .CXXFiles[]?,
                .MFiles[]?,
                .HFiles[]?,
                .FFiles[]?,
                .SFiles[]?,
                .SwigFiles[]?,
                .SwigCXXFiles[]?,
                .SysoFiles[]?,
                .EmbedFiles[]?
            ] +
            (
                if .GolibMutationObserver == true
                then [
                    .TestGoFiles[]?,
                    .XTestGoFiles[]?,
                    .TestEmbedFiles[]?,
                    .XTestEmbedFiles[]?
                ]
                else []
                end
            )
        )[]
        | if startswith("/") then . else "\($directory)/\(.)" end
        | select(startswith($root))
    ' "${relevant_package_data}" >>"${input_files}"

    while IFS= read -r file; do
        [[ -n "${file}" ]] || continue
        append_mutation_module_manifest "${file}"
    done < <(jq -r --arg root "${root}/" '
        select(
            (.Module.GoMod // "") == $root or
            ((.Module.GoMod // "") | startswith($root))
        )
        | .Module.GoMod
    ' "${relevant_package_data}" | LC_ALL=C sort -u)

    while IFS= read -r package_directory; do
        [[ -n "${package_directory}" ]] || continue
        for data_name in corpus fixtures testdata; do
            if [[ -d "${package_directory}/${data_name}" ]]; then
                find "${package_directory}/${data_name}" -type f \
                    -print >>"${input_files}"
            fi
        done
    done < <(
        jq -r --arg root "${root}/" '
            select(.Dir | startswith($root))
            | .Dir
        ' "${relevant_package_data}" | LC_ALL=C sort -u
    )

    while IFS= read -r file; do
        [[ -n "${file}" ]] || continue
        append_file "${file}"
    done < <(LC_ALL=C sort -u "${input_files}")
}

if [[ "${gate}" == "mutation" && -z "${package}" ]]; then
    mutation_digest
elif [[ "${gate}" == "mutation-legacy" ]]; then
    legacy_digest
elif [[ "${gate}" == "mutation" ]]; then
    package_digest
else
    verification_digest
fi

LC_ALL=C sort "${manifest}" | shasum -a 256 | awk '{print $1}'
