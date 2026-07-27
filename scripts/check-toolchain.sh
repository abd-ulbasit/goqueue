#!/usr/bin/env bash
# Asserts the Dockerfile's golang base image can actually build this module.
#
# The official golang images set GOTOOLCHAIN=local, so they never fetch a newer
# toolchain on demand. A base image older than go.mod's `go` directive fails at
# `RUN go mod download` with
#
#   go: go.mod requires go >= X (running Y; GOTOOLCHAIN=local)
#
# This is not hypothetical here. It is what deadlocked seven Dependabot PRs:
# each bumped go.mod past the base image, so no PR could go green on its own,
# and the two PRs that would have raised the base image had been closed. The
# pair has to move together, and 625c6d7 raised go.mod alone within hours of
# 42e393e establishing that it must not.
#
# Unlike pgbranch's equivalent, this tolerates a floating minor tag. goqueue
# builds on `golang:1.25-alpine`, which always resolves to the newest 1.25.x
# and so satisfies any 1.25.x `go` directive without edits. That is a
# deliberate, working choice; what it cannot survive is go.mod moving to a
# different minor. So:
#
#   FROM golang:1.25-alpine   + go 1.25.8  -> ok      (minors match)
#   FROM golang:1.25-alpine   + go 1.26.0  -> FAIL    (minor drifted)
#   FROM golang:1.25.8-alpine + go 1.25.8  -> ok      (exact pin, satisfied)
#   FROM golang:1.25.4-alpine + go 1.25.8  -> FAIL    (exact pin, too old)
set -euo pipefail
cd "$(dirname "$0")/.."

DOCKERFILE=deploy/docker/Dockerfile

want=$(awk '/^go [0-9]/ {print $2; exit}' go.mod)
[ -n "$want" ] || { echo "FAIL: no 'go' directive in go.mod" >&2; exit 1; }

got=$(sed -n 's/^FROM golang:\([0-9][^-@ ]*\).*/\1/p' "$DOCKERFILE" | head -1)
[ -n "$got" ] || { echo "FAIL: $DOCKERFILE has no 'FROM golang:<version>' line" >&2; exit 1; }

want_minor=${want%.*}

# A tag of the form X.Y floats across patches; X.Y.Z is an exact pin.
if [ "$got" = "${got%.*.*}" ]; then
    if [ "$got" != "$want_minor" ]; then
        echo "FAIL: $DOCKERFILE builds on golang:$got but go.mod requires go $want" >&2
        echo "      A floating golang:$got tag never resolves to a $want_minor toolchain," >&2
        echo "      and the image pins GOTOOLCHAIN=local, so it cannot upgrade itself." >&2
        exit 1
    fi
    echo "ok: $DOCKERFILE golang:$got floats within go.mod's $want_minor line (go $want)"
    exit 0
fi

# Exact pin: it must be at least the `go` directive.
if [ "$(printf '%s\n%s\n' "$want" "$got" | sort -V | head -1)" != "$want" ]; then
    echo "FAIL: $DOCKERFILE pins golang:$got but go.mod requires go $want" >&2
    echo "      (the golang image pins GOTOOLCHAIN=local, so it cannot upgrade itself)" >&2
    exit 1
fi
echo "ok: $DOCKERFILE golang:$got satisfies go.mod go $want"
