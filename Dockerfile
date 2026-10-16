# syntax=docker/dockerfile:1.7
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Multi-stage distroless build for the `leeway` binary.
# Mirrors k8s-lookout and core-agent's Dockerfile conventions (alpine
# builder, distroless/static final stage, version stamped via -ldflags).
#
# ENTRYPOINT: the daemon (`leeway watch`) is the container's default
# command, so this image's ENTRYPOINT is ["/leeway", "watch"] and a
# Deployment's bare-flag `args:` splice in after `watch` (no explicit
# `command:` needed). One-shot subcommands (`leeway report`, `leeway
# version`) are reachable by overriding the entrypoint (`command:` in
# k8s, `--entrypoint` in docker run).

# ---- Builder stage ----
ARG GO_VERSION=1.26.9
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine@sha256:3082400e369fa24d5fc60bca20edab3f6d604e0c5a690ec66b295eff4dd87ade AS builder

WORKDIR /src

# Cache module downloads in a separate layer so iterative source
# changes don't re-fetch dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Bring in the rest of the source.
COPY . .

ARG VERSION=v0.0.0-dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

ARG TARGETOS
ARG TARGETARCH

ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH}

RUN go build \
    -ldflags "-s -w \
      -X github.com/go-steer/leeway/internal/version.Version=${VERSION} \
      -X github.com/go-steer/leeway/internal/version.Commit=${COMMIT} \
      -X github.com/go-steer/leeway/internal/version.Date=${BUILD_DATE}" \
    -trimpath \
    -o /out/leeway \
    ./cmd/leeway

# ---- Final stage ----
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

LABEL org.opencontainers.image.source="https://github.com/go-steer/leeway" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /out/leeway /leeway

WORKDIR /workspace

USER nonroot:nonroot

ENTRYPOINT ["/leeway", "watch"]
