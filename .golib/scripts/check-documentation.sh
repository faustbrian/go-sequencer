#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
# shellcheck source=/dev/null
source "${root}/.golib/documentation-tools.env"

tool_directory="$(mktemp -d "${TMPDIR:-/tmp}/golib-documentation-tools.XXXXXX")"

cleanup() {
    chmod -R u+w "${tool_directory}" 2>/dev/null || true
    find "${tool_directory}" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

cd "${root}"
test -s README.md

configured_cspell_version="$(jq -r '.devDependencies.cspell // empty' package.json)"
if [[ "${configured_cspell_version}" != "${CSPELL_VERSION}" ]]; then
    printf 'cspell version mismatch: package.json=%s versions.env=%s\n' \
        "${configured_cspell_version}" "${CSPELL_VERSION}" >&2
    exit 1
fi

cspell="${GOLIB_CSPELL:-}"
if [[ -z "${cspell}" ]]; then
    command -v npm >/dev/null || {
        printf 'npm is required for the documentation spelling gate\n' >&2
        exit 1
    }
    node_directory="${tool_directory}/node"
    npm_cache="${tool_directory}/npm-cache"
    mkdir -p "${node_directory}" "${npm_cache}"
    cp package.json package-lock.json "${node_directory}/"
    (
        cd "${node_directory}"
        NPM_CONFIG_CACHE="${npm_cache}" npm ci \
            --ignore-scripts \
            --no-audit \
            --no-fund \
            --silent
    )
    cspell="${node_directory}/node_modules/.bin/cspell"
fi
[[ -x "${cspell}" ]] || {
    printf 'cspell executable is unavailable: %s\n' "${cspell}" >&2
    exit 1
}
"${cspell}" lint \
    --config cspell.json \
    --no-config-search \
    --validate-directives \
    --no-progress \
    --no-summary \
    README.md 'docs/**/*.md'

lychee="${GOLIB_LYCHEE:-}"
if [[ -z "${lychee}" ]]; then
    case "$(uname -s)/$(uname -m)" in
        Darwin/arm64)
            target="aarch64-apple-darwin"
            checksum="${LYCHEE_SHA256_DARWIN_ARM64}"
            ;;
        Darwin/x86_64)
            target="x86_64-apple-darwin"
            checksum="${LYCHEE_SHA256_DARWIN_AMD64}"
            ;;
        Linux/aarch64|Linux/arm64)
            target="aarch64-unknown-linux-gnu"
            checksum="${LYCHEE_SHA256_LINUX_ARM64}"
            ;;
        Linux/x86_64)
            target="x86_64-unknown-linux-gnu"
            checksum="${LYCHEE_SHA256_LINUX_AMD64}"
            ;;
        *)
            printf 'unsupported lychee platform: %s/%s\n' "$(uname -s)" "$(uname -m)" >&2
            exit 1
            ;;
    esac
    archive="lychee-${target}.tar.gz"
    archive_path="${tool_directory}/${archive}"
    curl --fail --silent --show-error --location \
        "https://github.com/lycheeverse/lychee/releases/download/lychee-v${LYCHEE_VERSION}/${archive}" \
        --output "${archive_path}"
    printf '%s  %s\n' "${checksum}" "${archive_path}" | shasum -a 256 -c -
    tar -xzf "${archive_path}" -C "${tool_directory}"
    lychee="${tool_directory}/lychee-${target}/lychee"
fi
[[ -x "${lychee}" ]] || {
    printf 'lychee executable is unavailable: %s\n' "${lychee}" >&2
    exit 1
}
"${lychee}" \
    --cache=false \
    --exclude-private \
    --exclude-loopback \
    --max-concurrency 16 \
    --max-retries 3 \
    --no-progress \
    README.md 'docs/**/*.md'
