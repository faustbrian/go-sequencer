GOLIB ?= golib

.PHONY: check ci inventory repository-check

check:
	$(GOLIB) check --all

ci:
	$(GOLIB) repository check
	$(GOLIB) check --all

inventory repository-check:
	$(GOLIB) repository check
