#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
task="$(mktemp -d "${TMPDIR:-/tmp}/golib-codeql-build.XXXXXX")"
cleanup() {
    find "${task}" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

while IFS= read -r module; do
    [[ -n "${module}" ]] || continue
    module_root="${root}"
    if [[ "${module}" != "." ]]; then
        module_root="${root}/${module}"
    fi
    while IFS= read -r package; do
        [[ -n "${package}" ]] || continue
        package_tags="$(
            jq -r \
                --arg module "${module}" \
                --arg package "${package}" '
                    .modules[]
                    | select(.directory == $module)
                    | .packages[]
                    | select(.import_path == $package)
                    | .build_tags[]?
                ' "${root}/modules.json"
        )"
        slug="$(printf '%s' "${package}" | tr '/.' '--')"
        if [[ "${module}" == "benchmarks/platform" && -n "${package_tags}" ]]; then
            package_tags=benchmark_disabled
        fi
        if [[ -z "${package_tags}" ]]; then
            (cd "${module_root}" && GOWORK=off go build -o "${task}/${slug}" "${package}")
            continue
        fi
        variant=0
        while IFS= read -r tag; do
            [[ -n "${tag}" ]] || continue
            (cd "${module_root}" && GOWORK=off go build \
                -tags="${tag}" -o "${task}/${slug}-${variant}" "${package}")
            variant=$((variant + 1))
        done <<<"${package_tags}"
    done < <(
        jq -r --arg module "${module}" '
            .modules[]
            | select(.directory == $module)
            | .packages[]
            | select(.build_required == true)
            | .import_path
        ' "${root}/modules.json"
    )
done < <(jq -r '.modules[].directory' "${root}/modules.json")
