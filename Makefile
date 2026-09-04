GOLIB ?= golib

.PHONY: check ci cohesion inventory repository-check specification-check

check:
	$(GOLIB) check --all

ci: repository-check specification-check cohesion check

cohesion:
	$(GOLIB) cohesion check

inventory:
	$(GOLIB) inventory

repository-check:
	$(GOLIB) repository check

specification-check:
	$(GOLIB) specification check --online
