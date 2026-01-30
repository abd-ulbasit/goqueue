---
layout: default
title: Installation


---

# Installation


Install GoQueue on your system.


## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Binary Installation

### Download Pre-built Binary

Download the latest release for your platform:

```bash
# Linux (amd64)
curl -LO https://github.com/abd-ulbasit/goqueue/releases/latest/download/goqueue-linux-amd64.tar.gz
tar -xzf goqueue-linux-amd64.tar.gz
sudo mv goqueue /usr/local/bin/

# macOS (Apple Silicon)
curl -LO https://github.com/abd-ulbasit/goqueue/releases/latest/download/goqueue-darwin-arm64.tar.gz
tar -xzf goqueue-darwin-arm64.tar.gz
sudo mv goqueue /usr/local/bin/

# macOS (Intel)
curl -LO https://github.com/abd-ulbasit/goqueue/releases/latest/download/goqueue-darwin-amd64.tar.gz
tar -xzf goqueue-darwin-amd64.tar.gz
sudo mv goqueue /usr/local/bin/
```

### Verify Installation

```bash
goqueue --version
# GoQueue v1.0.0
```

---

## Build from Source

### Prerequisites

- Go 1.21 or later
- Git

### Clone and Build

```bash
# Clone repository
git clone https://github.com/abd-ulbasit/goqueue.git
cd goqueue

# Build
go build -o goqueue ./cmd/goqueue

# Install to PATH (optional)
sudo mv goqueue /usr/local/bin/
```

### Build with Version Info

```bash
VERSION=$(git describe --tags --always)
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT=$(git rev-parse HEAD)

go build -ldflags "-X main.Version=$VERSION -X main.BuildTime=$BUILD_TIME -X main.GitCommit=$GIT_COMMIT" \
  -o goqueue ./cmd/goqueue
```

---

## Docker Installation

### Pull from Docker Hub

```bash
docker pull abdulbasit/goqueue:latest
```

### Run with Docker

```bash
# Basic run
docker run -d \
  --name goqueue \
  -p 8080:8080 \
  -p 9000:9000 \
  -v goqueue-data:/var/lib/goqueue \
  abdulbasit/goqueue:latest

# With custom config
docker run -d \
  --name goqueue \
  -p 8080:8080 \
  -p 9000:9000 \
  -v $(pwd)/config.yaml:/etc/goqueue/config.yaml \
  -v goqueue-data:/var/lib/goqueue \
  abdulbasit/goqueue:latest --config /etc/goqueue/config.yaml
```

### Docker Compose

Create a `docker-compose.yaml`:

```yaml
version: '3.8'

services:
  goqueue:
    image: abdulbasit/goqueue:latest
    ports:
      - "8080:8080"   # HTTP API
      - "9000:9000"   # gRPC API
      - "9090:9090"   # Metrics
    volumes:
      - goqueue-data:/var/lib/goqueue
      - ./config.yaml:/etc/goqueue/config.yaml
    environment:
      - GOQUEUE_LOG_LEVEL=info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/healthz"]
      interval: 10s
      timeout: 5s
      retries: 3
    restart: unless-stopped

volumes:
  goqueue-data:
```

Run:

```bash
docker-compose up -d
```

---

## Kubernetes Installation

### Using Helm

```bash
# Add Helm repository
helm repo add goqueue https://abd-ulbasit.github.io/goqueue/charts
helm repo update

# Install
helm install goqueue goqueue/goqueue \
  --namespace goqueue \
  --create-namespace \
  --set replicaCount=3 \
  --set persistence.enabled=true \
  --set persistence.size=10Gi
```

### Using kubectl

```bash
# Apply manifests
kubectl apply -f https://raw.githubusercontent.com/abd-ulbasit/goqueue/main/deploy/kubernetes/goqueue.yaml

# Or with kustomize
kubectl apply -k https://github.com/abd-ulbasit/goqueue/deploy/kubernetes
```

See [Kubernetes Deployment](kubernetes) for detailed configuration options.

---

## Package Managers

### Homebrew (macOS/Linux)

```bash
brew tap abd-ulbasit/goqueue
brew install goqueue
```

### APT (Debian/Ubuntu)

```bash
# Add repository
curl -fsSL https://apt.goqueue.dev/gpg | sudo gpg --dearmor -o /usr/share/keyrings/goqueue-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/goqueue-archive-keyring.gpg] https://apt.goqueue.dev stable main" | sudo tee /etc/apt/sources.list.d/goqueue.list

# Install
sudo apt update
sudo apt install goqueue
```

---

## Verify Installation

After installation, verify GoQueue is working:

```bash
# Check version
goqueue --version

# Start broker (will use default config)
goqueue &

# Check health
curl http://localhost:8080/health
# {"status":"ok","timestamp":"2025-01-30T10:00:00Z"}

# Check readiness
curl http://localhost:8080/readyz
# {"status":"pass",...}
```

---

## Next Steps

- [Quickstart Guide](quickstart) - Create your first topic and publish messages
- [Configuration Reference](../configuration/reference) - Configure GoQueue for your needs
- [Docker Setup](docker) - Detailed Docker deployment guide
