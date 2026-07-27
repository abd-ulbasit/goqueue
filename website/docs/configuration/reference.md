---
layout: default
title: Configuration Reference


---

# Configuration Reference

Every environment variable the broker reads, and nothing else.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## There is no configuration file

The broker is configured entirely by environment variables. It does not parse a
YAML file, it does not accept command-line flags, and it does not look for
`config.yaml` in any directory.

Earlier versions of this page documented a large YAML schema. None of it was
ever wired up. If you drop a `config.yaml` next to the binary it is read by
nothing:

```bash
$ cat config.yaml
broker:
  dataDir: "/tmp/THIS-YAML-IS-IGNORED"
listeners:
  http: ":19999"

$ GOQUEUE_BROKER_DATADIR=./gqdata GOQUEUE_LISTENERS_HTTP=127.0.0.1:18081 goqueue
   ✓ Data directory: ./gqdata
   ✓ HTTP API listening on http://127.0.0.1:18081
```

The same applies to flags. `goqueue --config /path/to/anything` starts a broker
with defaults; the argument is never parsed, so a typo in it fails silently
rather than loudly.

The one YAML file that is real belongs to the CLI, not the broker:
`~/.goqueue/config.yaml` holds `goqueue-cli` contexts (server address, API key,
timeout). See the CLI documentation for its schema.

---

## Broker identity and storage

| Variable | Default | Effect |
|----------|---------|--------|
| `GOQUEUE_BROKER_DATADIR` | `./data` | Directory for segments, indexes and the PID file |
| `GOQUEUE_BROKER_NODEID` | `node-1` | Node identifier, reported in `/stats` and used for cluster membership |

In Kubernetes, set `GOQUEUE_BROKER_NODEID` to the pod name so a restarted pod
keeps its identity.

---

## Listeners

| Variable | Default | Effect |
|----------|---------|--------|
| `GOQUEUE_LISTENERS_HTTP` | `127.0.0.1:8080` | HTTP REST API. Also serves `/metrics` and the health endpoints |
| `GOQUEUE_LISTENERS_GRPC` | `127.0.0.1:9000` | gRPC API |
| `GOQUEUE_LISTENERS_INTERNAL` | `:7000` | Inter-node cluster HTTP. Only bound when cluster mode is on |
| `GOQUEUE_GRPC_REFLECTION` | unset | Set to `true` to enable gRPC server reflection, which `grpcurl` needs. Off by default because it exposes the API surface |

The defaults bind to loopback, so a container needs
`GOQUEUE_LISTENERS_HTTP=:8080` to be reachable from outside.

Prometheus metrics are served at `/metrics` **on the HTTP API listener**. There
is no separate metrics port, and no variable to move or disable the endpoint.
Point your scrape config at the HTTP listener.

---

## Cluster mode

Cluster mode is off unless `GOQUEUE_CLUSTER_ENABLED` is exactly `true`. The
remaining cluster variables are read only when it is.

| Variable | Default | Effect |
|----------|---------|--------|
| `GOQUEUE_CLUSTER_ENABLED` | unset | `true` enables gossip membership, controller election and replication |
| `GOQUEUE_CLUSTER_PEERS` | empty | Comma-separated peer addresses. Empty means a single-node cluster |
| `GOQUEUE_CLUSTER_ADVERTISE` | empty | Address other nodes should use to reach this one |
| `GOQUEUE_CLUSTER_CLIENT_ADVERTISE` | derived from `GOQUEUE_LISTENERS_HTTP` | Address producers are forwarded to when this node is not the partition leader |
| `GOQUEUE_CLUSTER_QUORUM` | `2` | Votes required to elect a controller |

Coordination is built in. GoQueue runs its own gossip membership and controller
election, so there is no ZooKeeper, etcd or other external coordination service
to deploy, and no endpoints to configure. Nothing in the module talks to etcd,
and there is no etcd client in `go.mod`.

Example, one pod of a three-node StatefulSet:

```bash
export GOQUEUE_CLUSTER_ENABLED=true
export GOQUEUE_BROKER_NODEID=goqueue-0
export GOQUEUE_CLUSTER_ADVERTISE=goqueue-0.goqueue-headless:7000
export GOQUEUE_CLUSTER_PEERS=goqueue-1.goqueue-headless:7000,goqueue-2.goqueue-headless:7000
export GOQUEUE_CLUSTER_QUORUM=2
```

Replication factor and minimum in-sync replicas are properties of a topic, set
when the topic is created, not process-level settings. There is no environment
variable for either.

---

## TLS

Read by the HTTP API server. The `GOQUEUE_TLS_` set secures the client-facing
listener; the `GOQUEUE_CLUSTER_TLS_` set uses identical suffixes for
inter-node mTLS.

| Variable | Default | Effect |
|----------|---------|--------|
| `GOQUEUE_TLS_ENABLED` | unset | `true` serves HTTPS instead of HTTP |
| `GOQUEUE_TLS_CERT_FILE` | empty | Server certificate (PEM) |
| `GOQUEUE_TLS_KEY_FILE` | empty | Private key (PEM) |
| `GOQUEUE_TLS_CA_FILE` | empty | CA bundle used to verify client certificates |
| `GOQUEUE_TLS_SELF_SIGNED` | unset | `true` generates a self-signed certificate at startup. Development only |
| `GOQUEUE_TLS_CLIENT_AUTH` | `none` | One of `none`, `request`, `require`, `verify`, `require-verify` |
| `GOQUEUE_TLS_MIN_VERSION` | `1.2` | `1.2` or `1.3`. Any other value leaves the default in place |

Certificate files are watched and reloaded in place, so renewal does not need a
restart.

---

## Authentication and authorization

| Variable | Default | Effect |
|----------|---------|--------|
| `GOQUEUE_AUTH_ENABLED` | unset | `true` requires an API key on API requests |
| `GOQUEUE_API_ROOT_KEY` | empty | Root admin key. Set this when auth is enabled, or nothing can mint further keys |
| `GOQUEUE_AUTH_ALLOW_HEALTH` | `true` | Set to `false` to require auth on health endpoints too. Doing so will break Kubernetes probes unless they carry a key |
| `GOQUEUE_ACL_ENABLED` | unset | `true` enforces per-key topic ACLs on top of authentication |
| `GOQUEUE_TRUSTED_PROXIES` | empty | Comma-separated CIDR blocks or bare IPs whose `X-Forwarded-For` is believed. See below |

### Client IP and `X-Forwarded-For`

The audit log records a client IP on every auth failure and ACL denial. That
field is only worth anything if the caller cannot choose it.

`X-Forwarded-For` is a request header, so anyone who can reach the API can send
one. goqueue therefore ignores it unless the TCP peer is listed in
`GOQUEUE_TRUSTED_PROXIES`. With the default empty list, the client IP is always
the TCP peer address, which cannot be forged across a TCP handshake.

If you run behind a load balancer or ingress controller, set the variable to the
addresses that balancer connects *from* — not the addresses it serves:

```
GOQUEUE_TRUSTED_PROXIES=10.0.0.0/8,192.168.1.7
```

goqueue then walks `X-Forwarded-For` from right to left and takes the first hop
outside that list: the last address one of your own proxies observed. Entries a
client prepended to the header sit further left and are never reached.

A malformed value is logged and treated as an empty list, so a typo costs you
proxy awareness rather than silently restoring the spoof.

---

## Command-line tools

`goqueue-cli` and `goqueue-admin` are separate binaries with their own
configuration, and unlike the broker they do parse flags and a config file.

| Variable | Used by | Effect |
|----------|---------|--------|
| `GOQUEUE_SERVER` | `goqueue-cli` | Broker address |
| `GOQUEUE_API_KEY` | `goqueue-cli` | API key |
| `GOQUEUE_CONTEXT` | `goqueue-cli` | Named context from `~/.goqueue/config.yaml` |
| `GOQUEUE_ADMIN_SERVER` | `goqueue-admin` | Broker address |
| `GOQUEUE_ADMIN_API_KEY` | `goqueue-admin` | API key |

---

## Validation

The broker validates its configuration before it opens a listener and exits
with a numbered list of every problem it found, rather than one at a time:

```
$ GOQUEUE_BROKER_DATADIR=/etc/hosts GOQUEUE_CLUSTER_ENABLED=true \
  GOQUEUE_CLUSTER_PEERS=a:7000,b:7000 GOQUEUE_CLUSTER_QUORUM=5 goqueue
Configuration error:
configuration validation failed:
  1. data_dir: "/etc/hosts" exists but is not a directory
  2. cluster.quorum_size: 5 exceeds total cluster size 3 (peers=2 + self=1)
```

Validation covers the data directory, node ID, listener addresses and cluster
quorum. It cannot catch a misspelled variable name: an unrecognised `GOQUEUE_*`
variable is simply never read, and the broker starts with the default. Check
the startup banner, which prints the data directory, node ID and every bound
address, to confirm the settings you meant to apply actually landed.

---

## Not configurable

These have defaults in code with no environment variable to override them.
Listing them here so that their absence above is not read as an oversight:

- Segment size, index interval and fsync behaviour in the storage engine
- Producer batch size, linger and acknowledgement mode (per-producer arguments
  in the client library, not process-level settings)
- Consumer session timeout, heartbeat interval and poll limits
- Retention, visibility timeout and retry limits (topic properties, set at
  topic creation)
- Priority scheduler weights, timer wheel resolution and tracing sampling

---

## Next Steps

- [Operations]({{ '/docs/operations' | relative_url }}) - Deploying and monitoring GoQueue
- [Benchmarks]({{ '/docs/operations/benchmarks' | relative_url }}) - What has been measured, and how
