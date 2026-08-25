#!/usr/bin/env bash

build_mutation_arguments() {
    local target="$1"
    local output="$2"
    local tags="$3"
    local discover_only="$4"

    mutation_arguments=(
        unleash "${target}"
        --integration --coverpkg "${target}"
        --exclude-files '^.+/'
        --workers 4 --test-cpu 1 --timeout-coefficient 10
        --threshold-efficacy 100 --threshold-mcover 100
        --arithmetic-base --conditionals-boundary --conditionals-negation
        --invert-assignments --invert-bitwise --invert-bwassign
        --increment-decrement --invert-logical --invert-loopctrl
        --invert-negatives --remove-self-assignments
        --output-statuses lctvsr --output "${output}"
    )
    if [[ -n "${tags}" ]]; then
        mutation_arguments+=(--tags "${tags}")
    fi
    if [[ "${discover_only}" -eq 1 ]]; then
        mutation_arguments+=(--dry-run)
    fi
}
