#!/usr/bin/env bash
set -euo pipefail

if rg -n --glob '*.go' --glob '!**/*_test.go' \
  '(^|[^[:alnum:]_])(unsafe|C)\.|go:linkname|func init\s*\(' .; then
  echo "forbidden production mechanism found" >&2
  exit 1
fi
if rg -n --glob '*.go' --glob '!**/*_test.go' \
  'reflect\.|runtime\.FuncForPC|^[[:space:]]*go[[:space:]]+' .; then
  echo "reflection discovery or hidden production goroutine found" >&2
  exit 1
fi
if rg -n --glob '*.go' --glob '!**/*_test.go' \
  'http\.(DefaultServeMux|Handle|HandleFunc)[[:space:]]*\(' .; then
  echo "process-global HTTP registration found" >&2
  exit 1
fi
