#!/usr/bin/env bash

configure_mutation_workers() {
    local workers="$1"
    local index

    if [[ ! "${workers}" =~ ^[1-9][0-9]*$ ]]; then
        printf 'mutation workers must be a positive integer\n' >&2
        return 2
    fi
    for index in "${!mutation_arguments[@]}"; do
        if [[ "${mutation_arguments[${index}]}" == "--workers" ]]; then
            mutation_arguments[index + 1]="${workers}"
            return
        fi
    done

    printf 'mutation command is missing --workers\n' >&2
    return 2
}
