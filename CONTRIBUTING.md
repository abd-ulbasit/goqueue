# Contributing to GoQueue

First off, thank you for considering contributing to GoQueue! It's people like you that make GoQueue such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by our commitment to a welcoming and inclusive community. By participating, you are expected to uphold this standard.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When you create a bug report, include as many details as possible using our bug report template.

**Great Bug Reports** tend to have:
- A quick summary and/or background
- Steps to reproduce (be specific!)
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion:
- Use a clear and descriptive title
- Provide a detailed description of the suggested enhancement
- Explain why this enhancement would be useful
- Consider how other systems (Kafka, RabbitMQ, SQS) handle similar functionality

### Pull Requests

1. Fork the repo and create your branch from `main`
2. If you've added code that should be tested, add tests
3. If you've changed APIs, update the documentation
4. Ensure the test suite passes
5. Make sure your code follows the existing style
6. Issue that pull request!

## Development Setup

### Prerequisites

- Go 1.24 or later
- Docker (for integration tests)
- Make (optional, but recommended)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/abd-ulbasit/goqueue.git
cd goqueue

# Download dependencies
go mod download

# Run tests
go test ./...

# Run with race detection
go test -race ./...

# Build all binaries
go build ./cmd/goqueue
go build ./cmd/goqueue-cli
go build ./cmd/goqueue-admin

# Run the broker
./goqueue
```

### Project Structure

```
goqueue/
├── api/              # API definitions (OpenAPI, Protobuf)
├── cmd/              # Main applications
│   ├── goqueue/      # Main broker server
│   ├── goqueue-cli/  # User CLI
│   └── goqueue-admin/# Admin CLI
├── clients/          # Client libraries (Go, Python, JavaScript)
├── deploy/           # Deployment configs (Docker, K8s, Terraform)
├── docs/             # Documentation
├── internal/         # Private application code
│   ├── api/          # HTTP/REST API handlers
│   ├── broker/       # Core broker logic
│   ├── grpc/         # gRPC server
│   ├── metrics/      # Prometheus metrics
│   └── storage/      # Persistence layer
├── pkg/              # Public library code
└── website/          # Documentation website
```

## Style Guidelines

### Go Code

- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Run `gofmt` and `golangci-lint` before committing
- Write comprehensive comments for exported functions
- Include inline comments explaining WHY, not just WHAT

### Commit Messages

We follow conventional commits:

```
feat(broker): add priority lanes support

- Implement priority-based message delivery
- Add PriorityLane configuration option
- Update documentation

Fixes #123
```

Types:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Formatting changes
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `perf`: Performance improvement
- `test`: Adding missing tests
- `chore`: Changes to build process or auxiliary tools

### Testing

- Write table-driven tests where applicable
- Include both positive and negative test cases
- Add benchmarks for performance-critical code
- Run tests with `-race` flag

Example test structure:
```go
func TestBroker_Publish(t *testing.T) {
    tests := []struct {
        name    string
        topic   string
        msg     Message
        wantErr bool
    }{
        {
            name:  "valid message",
            topic: "orders",
            msg:   Message{Payload: []byte("test")},
        },
        {
            name:    "empty topic",
            topic:   "",
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // test implementation
        })
    }
}
```

## Release Process

Releases are automated via GitHub Actions:

1. Create a tag following semver: `git tag v1.0.0`
2. Push the tag: `git push origin v1.0.0`
3. GoReleaser creates binaries and GitHub release
4. Docker images are pushed to GHCR

## Getting Help

- 📚 [Documentation](https://abd-ulbasit.github.io/goqueue/)
- 💬 [GitHub Discussions](https://github.com/abd-ulbasit/goqueue/discussions)
- 🐛 [Issue Tracker](https://github.com/abd-ulbasit/goqueue/issues)

## Recognition

Contributors are recognized in:
- Release notes
- README contributors section
- Our eternal gratitude 🙏
