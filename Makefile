GOLIB ?= golib

.PHONY: source-docs-check check check-full ci cohesion inventory repository-check specification-check specification-check-online

check: source-docs-check
	$(GOLIB) check --local --all

check-full: source-docs-check repository-check specification-check-online cohesion
	$(GOLIB) check --all

ci: source-docs-check repository-check specification-check cohesion check

cohesion:
	$(GOLIB) cohesion check

inventory:
	$(GOLIB) inventory

repository-check:
	$(GOLIB) repository check

specification-check:
	$(GOLIB) specification check

specification-check-online:
	$(GOLIB) specification check --online

source-docs-check:
	$(GOLIB) source-docs check
