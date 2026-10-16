---
title: Getting started
description: Install leeway, run a one-shot multi-cluster placement report, or deploy the continuous watcher daemon.
sidebar:
  order: 1
---

`leeway` can be used immediately from your workstation or an automation runner with zero in-cluster installation (`leeway report`), or deployed as a resident watcher daemon (`leeway watch`).

1. **[Install `leeway`](/getting-started/install/)** — Download a release archive, install with `go install`, or pull the distroless container image `ghcr.io/go-steer/leeway`.
2. **[Run a one-shot report (`leeway report`)](/getting-started/report/)** — Pass one or more clusters and inspect topology skews, root-cause attributions, and ComputeClass priority ranks as structured JSON.
3. **[Deploy the daemon (`leeway watch`)](/getting-started/deploy/)** — Run `leeway watch` in-cluster or across multiple clusters with Prometheus metrics and an HTTP webhook callback.
