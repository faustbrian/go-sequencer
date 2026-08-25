#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <repository-relative-package-directory>\n' "$0" >&2
    exit 2
fi

root="$(git rev-parse --show-toplevel)"
package_directory="$1"
case "${package_directory}" in
    pkg/*) ;;
    *)
        printf 'package directory must be beneath pkg/: %s\n' \
            "${package_directory}" >&2
        exit 2
        ;;
esac
absolute="${root}/${package_directory}"
[[ -d "${absolute}" ]] || {
    printf 'package directory does not exist: %s\n' "${package_directory}" >&2
    exit 2
}

manifest="$(mktemp "${TMPDIR:-/tmp}/golib-source-digest.XXXXXX")"
cleanup() {
    rm -f "${manifest}"
}
trap cleanup EXIT HUP INT TERM

while IFS= read -r -d '' file; do
    relative="${file#"${root}/"}"
    digest="$(shasum -a 256 "${file}" | awk '{print $1}')"
    printf '%s  %s\n' "${digest}" "${relative}" >>"${manifest}"
done < <(find "${absolute}" -maxdepth 1 -type f -name '*.go' \
    ! -name '*_test.go' -print0 | LC_ALL=C sort -z)

[[ -s "${manifest}" ]] || {
    printf 'package has no production Go files: %s\n' "${package_directory}" >&2
    exit 1
}
shasum -a 256 "${manifest}" | awk '{print $1}'
