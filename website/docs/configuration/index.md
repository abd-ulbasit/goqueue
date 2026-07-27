---
layout: default
title: Configuration


permalink: /docs/configuration
---

# Configuration

Configure GoQueue for your environment and workload.



---

## Overview

The broker is configured by environment variables, all prefixed `GOQUEUE_`.
There is no configuration file and there are no command-line flags: the broker
parses neither, so a `config.yaml` sitting next to the binary and a `--config`
argument passed to it are both read by nothing.

That makes precedence simple. A variable is either set or it is not; if it is
not, the default in code applies. Nothing overrides anything else.

Topic-level settings such as partition count, retention, replication factor and
visibility timeout are properties of a topic, set when the topic is created,
not process-level configuration.

## Quick Links

- [Configuration Reference](reference) - Every variable the broker reads
