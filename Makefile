GOLIB ?= golib

.PHONY: check check-full ci cohesion inventory repository-check specification-check specification-check-online

check:
	$(GOLIB) check --local --all

check-full: repository-check specification-check-online cohesion
	$(GOLIB) check --all

ci: repository-check specification-check cohesion check

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
