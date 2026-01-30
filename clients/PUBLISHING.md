# GoQueue Client Libraries - Publishing Guide

This guide explains how to publish the GoQueue client libraries to their respective package registries.

## Directory Structure

```
clients/
├── javascript/     # npm package: goqueue-client
├── python/         # PyPI package: goqueue-client  
└── (no Go HTTP client; Go uses gRPC client in pkg/client)
```

---

## JavaScript/TypeScript Client (npm)

### Prerequisites

1. Create an npm account at https://www.npmjs.com/signup
2. Verify your email address
3. Enable 2FA (recommended)

### First-time Setup

```bash
cd clients/javascript

# Login to npm
npm login
# Enter your username, password, and email

# Verify login
npm whoami
```

### Publishing

```bash
cd clients/javascript

# Install dependencies
npm install

# Build the package
npm run build

# Run tests (if any)
npm test

# Publish (first time - creates the package)
npm publish --access public

# For subsequent updates, bump version first:
npm version patch  # or minor, or major
npm publish
```

### Version Bumping

```bash
npm version patch  # 1.0.0 → 1.0.1 (bug fixes)
npm version minor  # 1.0.1 → 1.1.0 (new features)
npm version major  # 1.1.0 → 2.0.0 (breaking changes)
```

### Automation with GitHub Actions

Create `.github/workflows/npm-publish.yml`:

```yaml
name: Publish JavaScript Client to npm

on:
  push:
    tags:
      - 'js-v*'

jobs:
  publish:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: clients/javascript
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          registry-url: 'https://registry.npmjs.org'
      
      - run: npm ci
      - run: npm run build
      - run: npm publish --access public
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

Add your npm token as a GitHub secret named `NPM_TOKEN`.

---

## Python Client (PyPI)

### Prerequisites

1. Create a PyPI account at https://pypi.org/account/register/
2. Verify your email address
3. Enable 2FA (required for new accounts)
4. Create an API token at https://pypi.org/manage/account/#api-tokens

### First-time Setup

```bash
cd clients/python

# Install build tools
pip install build twine

# Create a .pypirc file for authentication
cat > ~/.pypirc << EOF
[pypi]
username = __token__
password = pypi-YOUR_API_TOKEN_HERE
EOF

# Set proper permissions
chmod 600 ~/.pypirc
```

### Publishing

```bash
cd clients/python

# Clean previous builds
rm -rf dist/ build/ *.egg-info/

# Build the package
python -m build

# Check the package
twine check dist/*

# Upload to PyPI
twine upload dist/*

# Or upload to TestPyPI first (for testing)
twine upload --repository testpypi dist/*
```

### Version Bumping

Edit `pyproject.toml`:

```toml
[project]
version = "1.0.1"  # Update this
```

### Automation with GitHub Actions

Create `.github/workflows/pypi-publish.yml`:

```yaml
name: Publish Python Client to PyPI

on:
  push:
    tags:
      - 'py-v*'

jobs:
  publish:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: clients/python
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install build tools
        run: pip install build twine
      
      - name: Build package
        run: python -m build
      
      - name: Publish to PyPI
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_TOKEN }}
        run: twine upload dist/*
```

Add your PyPI token as a GitHub secret named `PYPI_TOKEN`.

---

## Go Client (pkg.go.dev)

### How Go Modules Work

Go modules are automatically published when you:
1. Push code to a public GitHub repository
2. Create a git tag with a semantic version

**No separate publishing step needed!**

### Source of Truth

The official Go client is the **gRPC-based** client in `pkg/client`.
It is published from this repository (no separate repo needed).

### Recommended Publishing (same repo)

Tag the repo as a normal Go module release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

Users import:
```go
import "github.com/abd-ulbasit/goqueue/pkg/client"
```

### Version Bumping

Simply create a new tag:

```bash
git tag v1.0.1
git push origin v1.0.1
```

### Triggering pkg.go.dev Indexing

After pushing a tag, you can manually trigger indexing:

```bash
# Using curl
curl "https://proxy.golang.org/github.com/abd-ulbasit/goqueue-client-go/@v/v1.0.0.info"

# Or visit the URL in your browser
# https://pkg.go.dev/github.com/abd-ulbasit/goqueue-client-go
```

### Automation with GitHub Actions

Create `.github/workflows/go-release.yml`:

```yaml
name: Go Client Release

on:
  push:
    tags:
      - 'v*'

jobs:
  test:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: .
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
      
      - name: Build
        run: go build ./...
      
      - name: Test
        run: go test ./...

  # No publish step needed - Go modules are automatic!
```

---

## Quick Reference

### Publish All Clients

```bash
# JavaScript (npm)
cd clients/javascript
npm version patch
npm run build
npm publish --access public

# Python (PyPI)
cd clients/python
# Update version in pyproject.toml
python -m build
twine upload dist/*

# Go (automatic with git tag)
git tag v1.0.1
git push origin v1.0.1
```

### Package URLs

After publishing:

| Language | Package URL |
|----------|-------------|
| JavaScript | https://www.npmjs.com/package/goqueue-client |
| Python | https://pypi.org/project/goqueue-client/ |
| Go | https://pkg.go.dev/github.com/abd-ulbasit/goqueue/pkg/client |

### Installation Commands

```bash
# JavaScript/TypeScript
npm install goqueue-client

# Python
pip install goqueue-client

# Go
go get github.com/abd-ulbasit/goqueue/pkg/client
```

---

## Semantic Versioning

All clients should follow [Semantic Versioning](https://semver.org/):

- **MAJOR** (1.x.x → 2.0.0): Breaking API changes
- **MINOR** (1.0.x → 1.1.0): New features, backward compatible
- **PATCH** (1.0.0 → 1.0.1): Bug fixes, backward compatible

Keep all three clients in sync for major versions when possible.

---

## Checklist Before Publishing

- [ ] Update version numbers
- [ ] Update CHANGELOG (if you have one)
- [ ] Run tests
- [ ] Build succeeds
- [ ] README is up to date
- [ ] API documentation is current
- [ ] Examples work with latest version

---

## Troubleshooting

### npm: "You must be logged in to publish packages"

```bash
npm login
```

### npm: "Package name already exists"

Choose a different package name in `package.json`:
```json
{
  "name": "@your-scope/goqueue-client"
}
```

### PyPI: "HTTPError: 403 Forbidden"

- Check your API token is correct
- Ensure 2FA is enabled
- Verify email address

### Go: "module not found"

- Ensure the repository is public
- Wait a few minutes for proxy.golang.org to index
- Try forcing an update: `go get -u github.com/...@latest`

### Go: "invalid version"

Tags must follow the format `vX.Y.Z`:
- ✅ `v1.0.0`
- ❌ `1.0.0`
- ❌ `v1.0`
