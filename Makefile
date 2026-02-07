# =============================================================================
# GOQUEUE MAKEFILE
# =============================================================================
#
# WHY A MAKEFILE?
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ A Makefile is the standard entry point for building/testing Go projects.    │
# │                                                                             │
# │ Benefits:                                                                   │
# │   - Self-documenting (make help)                                            │
# │   - Reproducible builds (same commands everywhere)                          │
# │   - CI/CD consistency (CI runs same make targets as developers)             │
# │   - Dependency tracking (targets only rebuild when needed)                  │
# │                                                                             │
# │ COMPARISON:                                                                 │
# │   - Kafka: Uses Gradle (JVM build tool)                                     │
# │   - RabbitMQ: Uses GNU Make                                                 │
# │   - NATS: Uses Makefile (same approach as goqueue)                          │
# │   - CockroachDB: Uses Makefile (complex, 2000+ lines)                       │
# │   - etcd: Uses Makefile                                                     │
# │                                                                             │
# │ PATTERN: Most production Go projects use Makefile as the build entry point. │
# └─────────────────────────────────────────────────────────────────────────────┘
#
# USAGE:
#   make              # Build all binaries (default target)
#   make help         # Show all available targets
#   make test         # Run tests with race detection
#   make lint         # Run golangci-lint
#   make docker       # Build Docker image
#   make release-dry  # Test GoReleaser without publishing
#
# =============================================================================

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------
#
# WHY VARIABLES AT THE TOP?
# Centralizes configuration so changing a value (e.g., Go version, registry)
# propagates to all targets. This is the "single source of truth" pattern.
# ---------------------------------------------------------------------------

# Project metadata
PROJECT_NAME := goqueue
MODULE       := $(shell go list -m)
DESCRIPTION  := Production-grade message queue in Go

# Version information
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ VERSION INJECTION (ldflags)                                                 │
# │                                                                             │
# │ Go binaries don't know their version at runtime unless we inject it.        │
# │ We use -ldflags "-X" to set Go variables at compile time.                  │
# │                                                                             │
# │ HOW IT WORKS:                                                               │
# │   go build -ldflags "-X main.version=1.0.0" → main.version == "1.0.0"     │
# │                                                                             │
# │ THREE PIECES OF BUILD METADATA:                                             │
# │   1. version:   Semantic version (from git tag or "dev")                   │
# │   2. commit:    Git SHA (identifies exact source code)                      │
# │   3. buildTime: When the binary was built (ISO 8601)                       │
# │                                                                             │
# │ COMPARISON:                                                                 │
# │   - Kafka: Sets version in gradle.properties                               │
# │   - NATS: Uses -ldflags -X (same approach)                                │
# │   - etcd: Uses -ldflags -X (same approach)                                │
# │   - Docker/Moby: Uses -ldflags -X (same approach)                         │
# │                                                                             │
# │ USAGE IN CODE:                                                              │
# │   var version = "dev"  // overridden by ldflags at build time              │
# └─────────────────────────────────────────────────────────────────────────────┘
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

# Build settings
GO       := go
GOFLAGS  := -trimpath
LDFLAGS  := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.buildTime=$(BUILD_TIME)

# Output directory
BIN_DIR := bin

# Docker settings
REGISTRY   ?= ghcr.io
IMAGE_NAME ?= abd-ulbasit/goqueue
DOCKER_TAG ?= $(VERSION)

# Tool versions (pinned for reproducibility)
GOLANGCI_LINT_VERSION ?= v1.64.8
BUF_VERSION           ?= v1.50.0

# ---------------------------------------------------------------------------
# Default target
# ---------------------------------------------------------------------------
.DEFAULT_GOAL := build

# ---------------------------------------------------------------------------
# Build targets
# ---------------------------------------------------------------------------

## build: Build all binaries
.PHONY: build
build: $(BIN_DIR)/goqueue $(BIN_DIR)/goqueue-cli $(BIN_DIR)/goqueue-admin
	@echo "✓ All binaries built in $(BIN_DIR)/"

## build-goqueue: Build the main broker binary
$(BIN_DIR)/goqueue: $(shell find cmd/goqueue internal pkg -name '*.go' 2>/dev/null)
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $@ ./cmd/goqueue

## build-cli: Build the CLI binary
$(BIN_DIR)/goqueue-cli: $(shell find cmd/goqueue-cli internal -name '*.go' 2>/dev/null)
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $@ ./cmd/goqueue-cli

## build-admin: Build the admin CLI binary
$(BIN_DIR)/goqueue-admin: $(shell find cmd/goqueue-admin internal -name '*.go' 2>/dev/null)
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $@ ./cmd/goqueue-admin

## build-linux: Cross-compile for Linux amd64 (for Docker/K8s)
.PHONY: build-linux
build-linux:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/goqueue-linux-amd64 ./cmd/goqueue
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/goqueue-cli-linux-amd64 ./cmd/goqueue-cli
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/goqueue-admin-linux-amd64 ./cmd/goqueue-admin
	@echo "✓ Linux binaries built"

# ---------------------------------------------------------------------------
# Test targets
# ---------------------------------------------------------------------------

## test: Run all tests with race detection
.PHONY: test
test:
	$(GO) test -race -timeout 10m ./...

## test-short: Run short tests only (skip slow integration tests)
.PHONY: test-short
test-short:
	$(GO) test -short -race -timeout 5m ./...

## test-cover: Run tests with coverage report
.PHONY: test-cover
test-cover:
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic -timeout 10m ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@echo "✓ Coverage report: coverage.out"
	@echo "  View in browser: go tool cover -html=coverage.out"

## test-bench: Run benchmarks
.PHONY: test-bench
test-bench:
	$(GO) test -bench=. -benchmem -run=^$$ -timeout 5m ./internal/storage/... ./internal/broker/...

# ---------------------------------------------------------------------------
# Quality targets
# ---------------------------------------------------------------------------

## lint: Run golangci-lint
.PHONY: lint
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --timeout 5m; \
	else \
		echo "golangci-lint not found. Install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)"; \
		exit 1; \
	fi

## fmt: Format all Go source files
.PHONY: fmt
fmt:
	$(GO) fmt ./...
	@echo "✓ Code formatted"

## vet: Run go vet
.PHONY: vet
vet:
	$(GO) vet ./...

## check: Run all quality checks (fmt, vet, lint)
.PHONY: check
check: fmt vet lint
	@echo "✓ All quality checks passed"

## vuln: Run vulnerability scanner
.PHONY: vuln
vuln:
	@if command -v govulncheck >/dev/null 2>&1; then \
		govulncheck ./...; \
	else \
		echo "govulncheck not found. Install: go install golang.org/x/vuln/cmd/govulncheck@latest"; \
		exit 1; \
	fi

# ---------------------------------------------------------------------------
# Dependency targets
# ---------------------------------------------------------------------------

## deps: Download and verify dependencies
.PHONY: deps
deps:
	$(GO) mod download
	$(GO) mod verify
	@echo "✓ Dependencies verified"

## deps-tidy: Tidy go.mod and go.sum
.PHONY: deps-tidy
deps-tidy:
	$(GO) mod tidy
	@echo "✓ Dependencies tidied"

## deps-update: Update all dependencies
.PHONY: deps-update
deps-update:
	$(GO) get -u ./...
	$(GO) mod tidy
	@echo "✓ Dependencies updated"

# ---------------------------------------------------------------------------
# Proto targets
# ---------------------------------------------------------------------------

## proto: Generate protobuf code
.PHONY: proto
proto:
	@if command -v buf >/dev/null 2>&1; then \
		cd api/proto && buf generate; \
		echo "✓ Protobuf code generated"; \
	else \
		echo "buf not found. Install: https://buf.build/docs/installation"; \
		exit 1; \
	fi

## proto-lint: Lint protobuf files
.PHONY: proto-lint
proto-lint:
	@if command -v buf >/dev/null 2>&1; then \
		buf lint api/proto; \
		echo "✓ Protobuf lint passed"; \
	else \
		echo "buf not found"; \
		exit 1; \
	fi

# ---------------------------------------------------------------------------
# Docker targets
# ---------------------------------------------------------------------------

## docker: Build Docker image
.PHONY: docker
docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(REGISTRY)/$(IMAGE_NAME):$(DOCKER_TAG) \
		-f deploy/docker/Dockerfile .
	@echo "✓ Docker image built: $(REGISTRY)/$(IMAGE_NAME):$(DOCKER_TAG)"

## docker-push: Build and push Docker image
.PHONY: docker-push
docker-push: docker
	docker push $(REGISTRY)/$(IMAGE_NAME):$(DOCKER_TAG)
	@echo "✓ Docker image pushed"

## docker-debug: Build debug Docker image (with shell)
.PHONY: docker-debug
docker-debug:
	docker build \
		--target debug \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(REGISTRY)/$(IMAGE_NAME):$(DOCKER_TAG)-debug \
		-f deploy/docker/Dockerfile .
	@echo "✓ Debug Docker image built"

## docker-compose-up: Start local 3-node cluster
.PHONY: docker-compose-up
docker-compose-up:
	docker compose -f deploy/docker/docker-compose.yaml up -d
	@echo "✓ Local cluster started"

## docker-compose-down: Stop local cluster
.PHONY: docker-compose-down
docker-compose-down:
	docker compose -f deploy/docker/docker-compose.yaml down
	@echo "✓ Local cluster stopped"

# ---------------------------------------------------------------------------
# Release targets
# ---------------------------------------------------------------------------

## release-dry: Test GoReleaser without publishing
.PHONY: release-dry
release-dry:
	@if command -v goreleaser >/dev/null 2>&1; then \
		goreleaser release --snapshot --clean; \
	else \
		echo "goreleaser not found. Install: https://goreleaser.com/install/"; \
		exit 1; \
	fi

## release-check: Validate GoReleaser config
.PHONY: release-check
release-check:
	@if command -v goreleaser >/dev/null 2>&1; then \
		goreleaser check; \
		echo "✓ GoReleaser config valid"; \
	else \
		echo "goreleaser not found"; \
		exit 1; \
	fi

# ---------------------------------------------------------------------------
# Run targets
# ---------------------------------------------------------------------------

## run: Run the broker locally
.PHONY: run
run: build
	./$(BIN_DIR)/goqueue

## run-dev: Run with hot reload (requires air)
.PHONY: run-dev
run-dev:
	@if command -v air >/dev/null 2>&1; then \
		air; \
	else \
		echo "air not found. Install: go install github.com/air-verse/air@latest"; \
		echo "Falling back to regular run..."; \
		$(MAKE) run; \
	fi

# ---------------------------------------------------------------------------
# Clean targets
# ---------------------------------------------------------------------------

## clean: Remove build artifacts
.PHONY: clean
clean:
	rm -rf $(BIN_DIR)/
	rm -f coverage.out
	rm -rf dist/
	@echo "✓ Clean"

## clean-data: Remove local data directory (WARNING: deletes all messages)
.PHONY: clean-data
clean-data:
	rm -rf data/
	@echo "✓ Data directory removed"

## clean-all: Remove everything (build artifacts + data)
.PHONY: clean-all
clean-all: clean clean-data

# ---------------------------------------------------------------------------
# Install targets
# ---------------------------------------------------------------------------

## install-tools: Install required development tools
.PHONY: install-tools
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install github.com/air-verse/air@latest
	go install github.com/goreleaser/goreleaser/v2@latest
	@echo "✓ Development tools installed"

# ---------------------------------------------------------------------------
# Version info
# ---------------------------------------------------------------------------

## version: Show version information
.PHONY: version
version:
	@echo "Project:    $(PROJECT_NAME)"
	@echo "Version:    $(VERSION)"
	@echo "Commit:     $(COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(shell $(GO) version)"

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
#
# Parses this Makefile for lines starting with "## " and formats them as help.
# This is a common pattern in Go Makefiles (used by Kubernetes, Docker, etc.).
# ---------------------------------------------------------------------------

## help: Show this help message
.PHONY: help
help:
	@echo "GoQueue - $(DESCRIPTION)"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':' | sed 's/^/  /'
