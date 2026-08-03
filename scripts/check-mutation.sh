#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
exec "${root}/scripts/check-mutation.sh" pkg/sequencer
