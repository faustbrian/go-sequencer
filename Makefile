SHELL := /usr/bin/env bash

.PHONY: check ci inventory repository-check

check:
	./.golib/scripts/with-disposable-go-cache.sh ./.golib/scripts/run-modules.sh check --all

ci: repository-check check

inventory repository-check:
	./.golib/scripts/repository-check.sh
