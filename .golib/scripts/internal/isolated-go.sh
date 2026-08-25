#!/usr/bin/env bash
set -euo pipefail

real_go="${GOLIB_REAL_GO:-}"
cache_root="${GOLIB_ISOLATED_MODFILES_DIRECTORY:-}"
if [[ -z "${real_go}" || -z "${cache_root}" ]]; then
    printf 'isolated Go requires GOLIB_REAL_GO and ' >&2
    printf 'GOLIB_ISOLATED_MODFILES_DIRECTORY\n' >&2
    exit 2
fi

module_root="${PWD}"
while [[ ! -f "${module_root}/go.mod" ]]; do
    parent="$(dirname "${module_root}")"
    if [[ "${parent}" == "${module_root}" ]]; then
        exec "${real_go}" "$@"
    fi
    module_root="${parent}"
done

clean_flags=""
for flag in ${GOFLAGS:-}; do
    case "${flag}" in
        -mod=*|-modfile=*) ;;
        *) clean_flags="${clean_flags:+${clean_flags} }${flag}" ;;
    esac
done

identity="$(
    {
        printf '%s\n' "${module_root}"
        cksum "${module_root}/go.mod"
        if [[ -f "${module_root}/go.sum" ]]; then
            cksum "${module_root}/go.sum"
        fi
    } | cksum | awk '{print $1 "-" $2}'
)"
state="${cache_root}/${identity}"
modfile="${state}/isolated.mod"
sumfile="${state}/isolated.sum"
ready="${state}/ready"

if [[ ! -f "${ready}" ]]; then
    lock="${state}.lock"
    if mkdir "${lock}" 2>/dev/null; then
        mkdir -p "${state}"
        cp "${module_root}/go.mod" "${modfile}"
        if [[ -f "${module_root}/go.sum" ]]; then
            awk '$1 !~ /^github\.com\/faustbrian\/golib\// { print }' \
                "${module_root}/go.sum" >"${sumfile}"
        else
            : >"${sumfile}"
        fi
        preparation_flags="${clean_flags:+${clean_flags} }-modfile=${modfile}"
        if ! GOWORK=off GOFLAGS="${preparation_flags}" \
            "${real_go}" mod download all; then
            rm -rf "${state}" "${lock}"
            exit 1
        fi
        : >"${ready}"
        rmdir "${lock}"
    else
        while [[ ! -f "${ready}" ]]; do
            sleep 0.05
        done
    fi
fi

if [[ "${1:-}" == "mod" && "${2:-}" == "tidy" &&
    " $* " == *" -diff "* ]]; then
    tidy_arguments=()
    for argument in "$@"; do
        [[ "${argument}" == "-diff" ]] || tidy_arguments+=("${argument}")
    done
    tidy_flags="${clean_flags:+${clean_flags} }-modfile=${modfile}"
    GOWORK=off GOFLAGS="${tidy_flags}" \
        "${real_go}" "${tidy_arguments[@]}"

    status=0
    if ! diff -u "${module_root}/go.mod" "${modfile}"; then
        status=1
    fi
    source_sum="${state}/source-external.sum"
    tidy_sum="${state}/tidy-external.sum"
    if [[ -f "${module_root}/go.sum" ]]; then
        awk '$1 !~ /^github\.com\/faustbrian\/golib\// { print }' \
            "${module_root}/go.sum" >"${source_sum}"
    else
        : >"${source_sum}"
    fi
    awk '$1 !~ /^github\.com\/faustbrian\/golib\// { print }' \
        "${sumfile}" >"${tidy_sum}"
    if ! diff -u "${source_sum}" "${tidy_sum}"; then
        status=1
    fi
    exit "${status}"
fi

case "${1:-}" in
    exec-tool)
        shift
        command_flags="${clean_flags:+${clean_flags} }-modfile=${modfile} -mod=readonly"
        GOLIB_ISOLATED_MODFILE="${modfile}" \
            GOWORK=off GOFLAGS="${command_flags}" \
            PATH="$(dirname "${real_go}"):${PATH}" exec "$@"
        ;;
    run|install)
        command="$1"
        shift
        if [[ " $* " == *"@"* ]]; then
            GOWORK=off GOFLAGS="${clean_flags}" \
                "${real_go}" "${command}" "$@"
        else
            GOLIB_ISOLATED_MODFILE="${modfile}" \
                GOWORK=off GOFLAGS="${clean_flags}" \
                "${real_go}" "${command}" \
                "-modfile=${modfile}" -mod=readonly "$@"
        fi
        ;;
    get)
        command_flags="${clean_flags:+${clean_flags} }-modfile=${modfile} -mod=mod"
        GOWORK=off GOFLAGS="${command_flags}" "${real_go}" "$@"
        ;;
    build|clean|fix|fmt|generate|list|test|vet)
        command="$1"
        shift
        GOLIB_ISOLATED_MODFILE="${modfile}" \
            GOWORK=off GOFLAGS="${clean_flags}" \
            "${real_go}" "${command}" \
            "-modfile=${modfile}" -mod=readonly "$@"
        ;;
    doc)
        command_flags="${clean_flags:+${clean_flags} }-modfile=${modfile} -mod=readonly"
        GOLIB_ISOLATED_MODFILE="${modfile}" \
            GOWORK=off GOFLAGS="${command_flags}" "${real_go}" "$@"
        ;;
    mod)
        command_flags="${clean_flags:+${clean_flags} }-modfile=${modfile}"
        GOWORK=off GOFLAGS="${command_flags}" "${real_go}" "$@"
        ;;
    *)
        GOWORK=off GOFLAGS="${clean_flags}" "${real_go}" "$@"
        ;;
esac
