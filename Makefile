GOLIB ?= golib

.PHONY: check ci cohesion inventory repository-check

check:
	$(GOLIB) check --all

ci: repository-check cohesion check

cohesion:
	$(GOLIB) cohesion check

inventory:
	$(GOLIB) inventory

repository-check:
	$(GOLIB) repository check
