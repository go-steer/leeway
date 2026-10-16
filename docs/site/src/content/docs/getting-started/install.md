---
title: Install
description: Install the leeway binary or verify the signed container image.
sidebar:
  order: 2
---

## Prebuilt binaries

Download the archive for your OS and architecture (`linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`, `windows/amd64`) from the [GitHub Releases](https://github.com/go-steer/leeway/releases) page and place `leeway` on your `PATH`.

Verify the binary identity:

```sh
leeway version
```

## Build from source (`go install`)

With Go 1.26+:

```sh
go install github.com/go-steer/leeway/cmd/leeway@latest
```

Or clone the repository and build with `make`:

```sh
git clone https://github.com/go-steer/leeway.git
cd leeway
make build
./bin/leeway version
```

## Container image

Multi-arch (`linux/amd64`, `linux/arm64`) distroless images are published to GitHub Container Registry at `ghcr.io/go-steer/leeway`:

```sh
docker pull ghcr.io/go-steer/leeway:latest
```

Every release image is signed keyless with Sigstore `cosign` and carries per-platform SPDX SBOM attestations:

```sh
cosign verify ghcr.io/go-steer/leeway:latest \
  --certificate-identity-regexp '^https://github.com/go-steer/leeway' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
