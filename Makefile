# kaneo-dispatcher
#
# Include Makefile.local for service-specific setup; it survives regenerate.

SHELL := /bin/bash
.DEFAULT_GOAL := help

BINARY := bin/app

PORT ?= 8080
export PORT

.PHONY: help
help:
	@echo "kaneo-dispatcher"
	@echo
	@echo "  make build      compile"
	@echo "  make test       vet and tests (what CI runs)"
	@echo "  make run        run it on :$(PORT)"
	@echo "  make regen      regenerate code derived from the spec"
	@echo "  make clean      remove build output"
ifneq (,$(wildcard Makefile.local))
	@$(MAKE) --no-print-directory help-local 2>/dev/null || true
endif

# `go build ./...` compiles every package and throws the binary away, so
# -o and the package are load-bearing.
.PHONY: build
build:
	CGO_ENABLED=0 go build -o $(BINARY) .

# -race matches CI.
.PHONY: test
test:
	go vet ./...
	go test -race ./...

.PHONY: run
run:
	go run .

.PHONY: regen
regen:
	@command -v homelabctl >/dev/null || { \
		echo "homelabctl not found - https://github.com/ChristopherScot/homelabctl"; exit 1; }
	@homelabctl regen

.PHONY: clean
clean:
	go clean
	rm -f $(BINARY)

-include Makefile.local
