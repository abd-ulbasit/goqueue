# =============================================================================
# GOQUEUE MULTI-STAGE DOCKERFILE
# =============================================================================
#
# WHY MULTI-STAGE?
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ Multi-stage builds create minimal production images:                        │
# │                                                                             │
# │ Stage 1 (Builder):                                                          │
# │   - Full Go toolchain (~800MB)                                              │
# │   - Compiles static binary with all dependencies                            │
# │   - Runs tests, linting                                                     │
# │   - Output: Single statically-linked binary                                 │
# │                                                                             │
# │ Stage 2 (Runtime):                                                          │
# │   - Minimal base image (distroless ~3MB or alpine ~5MB)                     │
# │   - Only the binary + CA certs + timezone data                              │
# │   - No shell, no package manager = smaller attack surface                   │
# │                                                                             │
# │ RESULT: ~15MB image vs ~800MB single-stage                                  │
# │                                                                             │
# │ COMPARISON:                                                                 │
# │   - Kafka: ~700MB (JVM + dependencies)                                      │
# │   - RabbitMQ: ~150MB (Erlang runtime)                                       │
# │   - NATS: ~15MB (also Go, similar approach)                                 │
# │   - goqueue: ~15MB                                                          │
# └─────────────────────────────────────────────────────────────────────────────┘
#
# BUILD COMMANDS:
#   docker build -t goqueue:latest -f deploy/docker/Dockerfile .
#   docker build --target debug -t goqueue:debug .
#   docker build --build-arg VERSION=1.0.0 -t goqueue:1.0.0 .
#
# =============================================================================

# -----------------------------------------------------------------------------
# STAGE 1: Builder - Compile the Go binary
# -----------------------------------------------------------------------------
FROM golang:1.24-alpine AS builder

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ WHY ALPINE FOR BUILD?                                                       │
# │                                                                             │
# │ Alpine Linux is small (~5MB) but has full musl libc and package manager.    │
# │ We need: git (for go mod), ca-certificates, and build essentials.           │
# │                                                                             │
# │ Alternative: golang:1.24 (Debian-based, larger but glibc)                   │
# │ We use Alpine because we're building static binaries anyway.                │
# └─────────────────────────────────────────────────────────────────────────────┘

# Install build dependencies
# - git: Required for fetching Go modules from git repos
# - ca-certificates: For HTTPS module fetches
# - tzdata: Timezone data (needed if app does time zone conversions)
RUN apk add --no-cache git ca-certificates tzdata

# Build arguments for versioning
# Pass these at build time: docker build --build-arg VERSION=1.0.0
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

# Set working directory
WORKDIR /build

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ LAYER CACHING OPTIMIZATION                                                  │
# │                                                                             │
# │ Docker caches each layer. By copying go.mod/go.sum first and running        │
# │ go mod download, we cache dependencies separately from source code.         │
# │                                                                             │
# │ Result: Rebuilds are fast when only source changes (deps cached).           │
# │                                                                             │
# │ Order matters:                                                              │
# │   1. Copy go.mod, go.sum → go mod download (cached if deps unchanged)       │
# │   2. Copy source → go build (only this layer rebuilds on code change)       │
# └─────────────────────────────────────────────────────────────────────────────┘

# Copy dependency files first (for layer caching)
COPY go.mod go.sum ./

# Download dependencies (cached unless go.mod/go.sum change)
RUN go mod download && go mod verify

# Copy source code
COPY . .

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ STATIC BINARY COMPILATION                                                   │
# │                                                                             │
# │ CGO_ENABLED=0: Disable cgo for fully static binary                          │
# │   - No dynamic linking to libc                                              │
# │   - Can run in scratch/distroless containers                                │
# │   - Portable across Linux distributions                                     │
# │                                                                             │
# │ -ldflags explained:                                                         │
# │   -s: Strip symbol table (smaller binary)                                   │
# │   -w: Strip DWARF debug info (smaller binary)                               │
# │   -X: Inject version variables at compile time                              │
# │                                                                             │
# │ -trimpath: Remove file system paths from binary (security + reproducibility)│
# │                                                                             │
# │ RESULT: ~15MB binary with version info embedded                             │
# └─────────────────────────────────────────────────────────────────────────────┘

# Build the main goqueue binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w \
        -X main.Version=${VERSION} \
        -X main.GitCommit=${GIT_COMMIT} \
        -X main.BuildTime=${BUILD_TIME}" \
    -trimpath \
    -o /build/bin/goqueue \
    ./cmd/goqueue

# Build the CLI tool
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w \
        -X main.Version=${VERSION} \
        -X main.GitCommit=${GIT_COMMIT} \
        -X main.BuildTime=${BUILD_TIME}" \
    -trimpath \
    -o /build/bin/goqueue-cli \
    ./cmd/goqueue-cli

# Build the admin tool
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /build/bin/goqueue-admin \
    ./cmd/goqueue-admin

# -----------------------------------------------------------------------------
# STAGE 2: Debug - For debugging with shell access
# -----------------------------------------------------------------------------
FROM alpine:3.23 AS debug

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ DEBUG IMAGE                                                                 │
# │                                                                             │
# │ Sometimes you need shell access for debugging:                              │
# │   - Inspect filesystem                                                      │
# │   - Check network connectivity                                              │
# │   - Run diagnostic commands                                                 │
# │                                                                             │
# │ BUILD: docker build --target debug -t goqueue:debug .                       │
# │ RUN: docker run -it goqueue:debug /bin/sh                                   │
# └─────────────────────────────────────────────────────────────────────────────┘

RUN apk add --no-cache ca-certificates tzdata curl bind-tools

# Create non-root user
RUN addgroup -g 1000 goqueue && \
    adduser -u 1000 -G goqueue -s /bin/sh -D goqueue

# Create directories
RUN mkdir -p /data /config && \
    chown -R goqueue:goqueue /data /config

# Copy binaries from builder
COPY --from=builder /build/bin/goqueue /usr/local/bin/
COPY --from=builder /build/bin/goqueue-cli /usr/local/bin/
COPY --from=builder /build/bin/goqueue-admin /usr/local/bin/

# Copy default config if exists
COPY --from=builder /build/config.example.yaml /config/config.yaml

USER goqueue
WORKDIR /data

EXPOSE 8080 9000 7000

ENTRYPOINT ["/usr/local/bin/goqueue"]

# -----------------------------------------------------------------------------
# STAGE 3: Production - Minimal distroless image
# -----------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS production

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ DISTROLESS IMAGE                                                            │
# │                                                                             │
# │ Google's distroless images contain ONLY:                                    │
# │   - Your application binary                                                 │
# │   - CA certificates (for HTTPS)                                             │
# │   - Timezone data                                                           │
# │                                                                             │
# │ NO: shell, package manager, or any other binaries                           │
# │                                                                             │
# │ SECURITY BENEFITS:                                                          │
# │   - Minimal attack surface (nothing to exploit)                             │
# │   - No shell = can't drop into container                                    │
# │   - Fewer CVEs (less software = fewer vulnerabilities)                      │
# │                                                                             │
# │ TRADEOFF:                                                                   │
# │   - Can't exec into container for debugging                                 │
# │   - Use debug image for troubleshooting                                     │
# │                                                                             │
# │ :nonroot variant runs as non-root user (UID 65532) by default               │
# └─────────────────────────────────────────────────────────────────────────────┘

# Labels for container metadata (OCI standard)
LABEL org.opencontainers.image.title="GoQueue"
LABEL org.opencontainers.image.description="Production-grade message queue in Go"
LABEL org.opencontainers.image.vendor="GoQueue"
LABEL org.opencontainers.image.source="https://github.com/abd-ulbasit/goqueue"
LABEL org.opencontainers.image.licenses="MIT"

# Copy binaries from builder stage
# Distroless runs as nonroot (UID 65532) by default
COPY --from=builder --chown=65532:65532 /build/bin/goqueue /usr/local/bin/
COPY --from=builder --chown=65532:65532 /build/bin/goqueue-cli /usr/local/bin/
COPY --from=builder --chown=65532:65532 /build/bin/goqueue-admin /usr/local/bin/

# Copy timezone data (distroless includes this, but explicit for clarity)
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ PORT MAPPING                                                                │
# │                                                                             │
# │ 8080: HTTP API (REST endpoints for produce/consume)                         │
# │ 9000: gRPC API (high-performance streaming)                                 │
# │ 7000: Internal cluster communication (replication, leader election)         │
# │ 9090: Prometheus metrics (if enabled separately)                            │
# └─────────────────────────────────────────────────────────────────────────────┘
EXPOSE 8080 9000 7000

# Data directory (mount PVC here in Kubernetes)
VOLUME ["/data"]

# Default entrypoint
ENTRYPOINT ["/usr/local/bin/goqueue"]

# Default command (can be overridden)
CMD ["--config", "/config/config.yaml"]

# -----------------------------------------------------------------------------
# DEFAULT TARGET: Production
# -----------------------------------------------------------------------------
FROM production
