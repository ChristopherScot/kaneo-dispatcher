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
	@echo "  make build      compile dispatcher"
	@echo "  make worker     compile pod worker"
	@echo "  make test       vet and tests (what CI runs)"
	@echo "  make graph      generate the FSM Graphviz DOT and PNG"
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

.PHONY: graph
graph:
	mkdir -p docs
	go run workflow_graph.go workflow_fsm.go ticket_fsm.go > docs/workflow-fsm.dot
	dot -Tpng -Grankdir=LR -Gbgcolor=white -Gpad=0.35 -Gdpi=144 \
		-Glabel="Sequential child workflow" -Glabelloc=t -Gfontname=Helvetica -Gfontsize=20 \
		-Nfontname=Helvetica -Nfontsize=13 -Nstyle=rounded \
		-Efontname=Helvetica -Efontsize=11 \
		docs/workflow-fsm.dot -o docs/workflow-fsm.png

	for graph in ticket command operator; do \
		go run workflow_graph.go workflow_fsm.go ticket_fsm.go $$graph > docs/$$graph-fsm.dot; \
		dot -Tpng -Grankdir=LR -Gbgcolor=white -Gpad=0.35 -Gdpi=110 \
		-Nfontname=Helvetica -Nfontsize=13 -Efontname=Helvetica -Efontsize=11 \
		docs/$$graph-fsm.dot -o docs/$$graph-fsm.png || exit 1; \
	done

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

.PHONY: worker
worker:
	CGO_ENABLED=0 go build -o bin/kaneo-worker ./cmd/kaneo-worker
