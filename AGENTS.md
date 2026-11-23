# AGENTS.md — leeway

Instructions for AI agents working in this repository.

## Start here

Read these in order before changing anything substantive:

1. [`README.md`](./README.md) — what `leeway` is, CLI flags for `leeway report` and `leeway watch`, the `leeway.report/v1alpha1` JSON envelope, and the incident webhook wire contract.
2. [`docs/leeway-design.md`](./docs/leeway-design.md) — the settled specification: topology placement drift scoring, tie-aligned Hamilton apportionment, intent confidence tiers (`A` / `B` / `C`), causal attribution, GKE `ComputeClass` priority rank tracking, and scale/memory budgets (§6). Do not re-propose designs rejected there without new evidence.
3. [`CONTRIBUTING.md`](./CONTRIBUTING.md) — DCO sign-off, presubmit expectations, `CHANGELOG.md` prose discipline, and rule-testing invariants.

## What this project is

One static Go binary, `leeway` (`cmd/leeway`), extracted from [`go-steer/k8s-lookout`](https://github.com/go-steer/k8s-lookout) to evaluate two Kubernetes placement and scheduling signals across single clusters or multi-cluster fleets:

- **`topology-drift`** (`pkg/sources/topologydrift`): scores pod placement skew across failure domains (`topology.kubernetes.io/zone`, `topology.kubernetes.io/region`, or `--topology-keys`), resolves declared or inferred intent (`LeewayPolicy` / `ClusterLeewayPolicy`, `topologySpreadConstraints`, pod anti-affinity, node selectors/affinity, cluster defaults), attributes root causes, and emits actionable remediation payloads.
- **`compute-class`** (`pkg/sources/computeclass`): tracks GKE `ComputeClass` (`cloud.google.com/v1`) priority ladders, accumulating pod-seconds and pod shares per rank to detect rank degradation, wedged fallback tiers, stalled scaleback, and unused preferred tiers.

Both sources run in two modes from the same binary:

1. **One-shot fleet snapshot (`leeway report`, `pkg/report`):** evaluates one or more target clusters in parallel (via `client-go`, watcher broker proxy, or `CREDENTIAL_PROXY_URL` `ShellMode`) with dwell bypassed, writing a `leeway.report/v1alpha1` JSON document to `stdout` (or `--output`).
2. **Continuous incident daemon (`leeway watch`, default container `ENTRYPOINT`):** watches clusters via shared informers, maintains dwell/hysteresis (`leeway.AlertTable`), EWMA baseline learning (`leeway.BaselineTable`), optional SQLite state (`pkg/store`), Prometheus `/metrics` + OTLP export (`internal/telemetry`), and dispatches open/resolved incident webhooks (`pkg/inject`).

## Hard rules (from the design; violations are bugs)

- **`pkg/leeway` stays pure.** `pkg/leeway` has zero transitive imports of `k8s.io/client-go`, `k8s.io/metrics`, `k8s.io/kubectl`, `pkg/sources`, or `pkg/store` — enforced by [`pkg/leeway/purity_test.go`](./pkg/leeway/purity_test.go) and [`pkg/sources/layering_test.go`](./pkg/sources/layering_test.go). Scoring, apportionment, cause attribution, rank tracking, and remediation take plain structs; informer wiring and snapshot collection belong in `pkg/sources/*` and `pkg/report`.
- **`pkg/...` never imports `internal/...`.** Enforced across transitive closures by `TestLayering_TheImportGraphMatchesTheDesign` in [`pkg/sources/layering_test.go`](./pkg/sources/layering_test.go) so `pkg/leeway` and `pkg/sources/*` remain embeddable outside `cmd/leeway`.
- **Page and trim Kubernetes objects on ingest.** Never buffer an unpaged, untrimmed cluster list in memory:
  - `leeway watch` builds informers through `kube.NewTransformingFactory` (verified at the lister boundary by [`pkg/kube/transform_wiring_test.go`](./pkg/kube/transform_wiring_test.go)).
  - `leeway report` pages `client-go` and dynamic reads through `kube.CollectPages` (applying `kube.Transform` per page) and pages `CREDENTIAL_PROXY_URL` `ShellMode` reads through `kubectlListPaged` (`kubectl get --raw` with `limit`/`continue`, page-size backoff/recovery on proxy truncation, `410 Gone` restart, and per-page `kube.Transform`).
  - Every field stripped or preserved on `Pod` and `Node` is governed by [`pkg/kube/transform_registry.go`](./pkg/kube/transform_registry.go) (`TestSharedTransform_StripsExactlyTheRegistry`). Stripping a field requires proving no informer-reachable or report reader touches it and adding a registry entry; preserving a field requires citing its `file:line` reader. Stripping container specs also enforces §6.1's security guarantee that literal secret env/arg values never sit in process memory.
- **State the claim & prove the silence.** Every detection rule or causal attribution path must ship with both a breaching fixture and a healthy counterfactual fixture asserting zero findings. Every bug fix must ship with a regression test verified to **fail on the pre-fix code**.
- **Distroless, static posture & no bare-flag secrets.** The container image ([`Dockerfile`](./Dockerfile)) runs on `gcr.io/distroless/static-debian12:nonroot` with `CGO_ENABLED=0` and `ENTRYPOINT ["/leeway", "watch"]`. Do not introduce `cgo`, runtime shell calls on the `watch` or `client-go` paths (`ShellMode` shells out to `kubectl` only when `CREDENTIAL_PROXY_URL` is set inside an external sandbox), or secret values on CLI flags (use `--sink-token-env` and `WATCHER_BROKER_TOKEN_FILE`).
- **Wire & docs parity (`docs/site/`).** CLI flags, `leeway.report/v1alpha1` JSON (`snake_case` envelope, `camelCase` findings), webhook open/resolved payloads (`pkg/inject`), and Prometheus/OTLP metric series must match [`README.md`](./README.md) and the Astro Starlight site under [`docs/site/src/content/docs/`](./docs/site/src/content/docs/). Capture JSON examples from real runs or tests — never invent flag names or wire fields in prose.

## Layout

```
cmd/leeway/          CLI entrypoint (`leeway watch`, `leeway report`, `leeway version`).
pkg/
  leeway/            Pure scoring engine: apportionment, intent tiers, cause attribution,
                     alert dwell/hysteresis, EWMA baselines, ComputeClass rank tracking,
                     deterministic remediation payloads.
  sources/
    topologydrift/   Informer + snapshot adapter for topology placement drift.
    computeclass/    Informer + snapshot adapter for GKE ComputeClass priority ladders.
  report/            One-shot multi-cluster snapshot collector (`client-go` + `ShellMode`)
                     and `leeway.report/v1alpha1` assembler.
  clusters/          Target discovery & credential routing (kubeconfig, in-cluster, GKE API,
                     Hermes profiles, watcher broker, `CREDENTIAL_PROXY_URL` `ShellMode`).
  kube/              Client construction, paged list helpers (`CollectPages`, `ListAllPages`),
                     shared object transform + field registry.
  engine/            Signal deduplication, fingerprinting, repeat suppression, recovery gate.
  inject/            Incident open/reminder/resolved payloads and HTTP webhook sink.
  store/             Pure-Go SQLite (`modernc.org/sqlite`) persistence for alerts, baselines,
                     and bound incident sessions.
internal/
  telemetry/         Prometheus `/metrics` registry and OTLP cumulative exporter.
  version/           Build-time version/commit/date ldflags + `debug.ReadBuildInfo` fallback.
deploy/              Kustomize manifests, `LeewayPolicy` CRDs (`deploy/crds/`), dashboards.
examples/            Hermetic kind e2e (`examples/e2e`, `examples/kind/`) and kwok scale
                     harnesses (`examples/kwok/leeway-scale`, `examples/kwok/leeway-report-scale`).
test/e2e/            Live kind cluster e2e suite (invoked via `examples/e2e`, not unit CI).
dev/                 Local CI runner (`dev/tools/ci`), individual presubmits (`dev/ci/presubmits/`),
                     license header tool, pinned `golangci-lint` config (`dev/tools/.golangci.yml`).
docs/                `leeway-design.md` specification, `stability-policy.md`,
                     `archive/` historical journal, + Astro Starlight site (`docs/site/`).
```

## Build & test

```bash
dev/tools/ci                                # full local presubmit sweep, fast-fail order
dev/ci/presubmits/verify-docs-lint          # docs freshness check (dev/tools/docs-lint)
dev/ci/presubmits/verify-version-fallback   # stamped + fallback version checks
dev/ci/presubmits/verify-go-format          # gofmt + goimports (fix: dev/tools/fix-go-format)
dev/ci/presubmits/vet                       # go vet ./...
dev/ci/presubmits/build                     # go build ./...
dev/ci/presubmits/lint-go                   # golangci-lint (auto-installs pinned version)
dev/ci/presubmits/verify-mod-tidy           # go.mod / go.sum cleanliness
dev/ci/presubmits/test-unit                 # go test -race -coverprofile=coverage.out ./...
dev/ci/presubmits/verify-vuln               # govulncheck ./...
```

- **Toolchain alignment:** if your workstation default `go` is newer than the version in [`go.mod`](./go.mod), prefix presubmit runs with the `go.mod` toolchain (e.g. `GOTOOLCHAIN=go1.26.11 dev/tools/ci`) so cached `golangci-lint` and `govulncheck` binaries match the module's Go release.
- **Hermetic unit presubmits:** `dev/tools/test-unit` requires no live cluster, no network, and no cloud credentials. Live-cluster tests run non-blocking in `.github/workflows/e2e-kind.yml` (`examples/e2e`) and `.github/workflows/scale-kwok.yml` (`examples/kwok/leeway-scale` and `examples/kwok/leeway-report-scale`).
- **Docs site:** user-visible CLI, metric, or schema changes update `docs/site/src/content/docs/` alongside `README.md` (verified in `.github/workflows/docs.yml` and `docs-lint.yml`).

## Releases, tags & docs

### Cutting a release & tagging (`vX.Y.Z`)

1. **Open a `chore: cut X.Y.Z` PR first (do not tag before merging):**
   - Move the entries under `## [Unreleased]` in [`CHANGELOG.md`](./CHANGELOG.md) into a new `## [X.Y.Z] - YYYY-MM-DD` section (leaving an empty `## [Unreleased]` heading above it). [`dev/release/compose-release-notes.sh`](./dev/release/compose-release-notes.sh) extracts `## [X.Y.Z]` and fails the release job if that section is missing or empty.
   - Update the default container image tag to `ghcr.io/go-steer/leeway:vX.Y.Z` in both [`deploy/51-deployment-watcher.yaml`](./deploy/51-deployment-watcher.yaml) and [`examples/watcher/up`](./examples/watcher/up). [`.github/workflows/release-images.yml`](./.github/workflows/release-images.yml) hard-fails on tag push if `deploy/51-deployment-watcher.yaml` does not match the pushed tag.
2. **Tag the merged commit on `main`:** pushing `vX.Y.Z` triggers `release-images.yml`, which builds, signs (`cosign` keyless), and attests (`syft` SPDX SBOM) multi-arch GHCR images, cross-compiles release archives, and publishes the GitHub Release.
3. **Bump `internal/version.Version` after a minor release:** [`dev/ci/presubmits/verify-version-fallback`](./dev/ci/presubmits/verify-version-fallback) enforces that `Version` in [`internal/version/version.go`](./internal/version/version.go) equals either `v<major>.<minor+1>.0-dev` or `v<major>.<minor>.<patch+1>-dev` relative to the latest `v*.*.*` tag. Patch cuts (e.g. `v0.1.6` while `Version = "v0.2.0-dev"`) stay green; after cutting `v0.2.0`, bump `Version` to `"v0.3.0-dev"` immediately or `verify-version-fallback` will fail every subsequent PR.

### Docs site (`docs/site/`) & freshness checks

- **Published site:** Astro + Starlight under [`docs/site/`](./docs/site/) (Node 22), deployed to GitHub Pages on `main` pushes by [`.github/workflows/docs.yml`](./.github/workflows/docs.yml).
- **Local verification when editing `docs/site/`:**
  ```bash
  (cd docs/site && npm ci && npm run build)
  python3 scripts/verify-internal-links.py   # checks every /leeway/... link in docs/site/dist/
  dev/tools/docs-lint                        # blocks pinned @vX.Y.Z installs & wrong-major pins
  ```
- **Never pin release versions in install snippets:** use `@latest` in `go install` / `go get` prose so docs do not rot on the next tag (`dev/tools/docs-lint` enforces this across `AGENTS.md`, `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, and `docs/site/src/content/`).

## Conventions

- Mirror `k8s-lookout` and `core-agent` conventions (package layout, `dev/` scripts, `internal/version` stamping); a maintainer of one `go-steer` repository should immediately recognize this one.
- **License headers everywhere.** Every Go, shell, YAML, and Python file carries the Apache 2.0 `Copyright 2026 Google LLC` header (`goheader` enforces `.go`; run `dev/tools/add-license-headers` for new scripts/manifests). Markdown files carry no header.
- **Conventional Commits + DCO sign-off.** Small, self-contained commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`, `ci:`) whose bodies explain *why* and what verification ran. Always sign off with `git commit -s`.
- **No AI-assistant attribution.** Never add `Co-Authored-By:` lines naming an AI model/agent, "Generated with …" footers, or assistant marketing trailers in commits, PR titles, PR bodies, or committed files. Commit under the human author's identity.
- **Prose `CHANGELOG.md` entries.** Any user-visible change adds a bullet under `## [Unreleased]` in [`CHANGELOG.md`](./CHANGELOG.md) stating what changed and *why* it is the right change (not a one-line commit subject).

## How we develop

Single long-lived branch `main`; short-lived `feat/…`, `fix/…`, `docs/…`, `test/…`, `chore/…` branches merged via PR once CI is green. Rebase onto `origin/main` rather than merging `main` into feature branches; `--force-with-lease` on your own feature branch is fine, never force-push `main`.

- **Keep the root checkout clean on `main`; use `.worktrees/` for branches.** Do feature, fix, and parallel-track work in a dedicated git worktree under `.worktrees/<slug>` (`git fetch origin --tags && git worktree add .worktrees/<slug> -b <branch> origin/main`) rather than switching branches in the root checkout. `/.worktrees/` is git-ignored.
- **Run `dev/tools/ci` before every push.** It executes the exact checks `ci.yml` and `docs-lint.yml` run.
- **Skeptic pass before opening Go PRs.** Before `gh pr create` on any change touching Go code, review the diff against real dependency and caller source (concurrency/races, paging/memory bounds, `kube.Transform` field interactions, error handling), and for bug fixes patch the bug back to confirm the new regression test fails before the fix.
- **Pin checks to `headRefOid` & clean up after merge.** After pushing or rebasing, confirm `gh pr view --json headRefOid,statusCheckRollup` shows all check runs green on the current commit SHA before merging. Once merged, remove the worktree (`git worktree remove .worktrees/<slug>`), delete the local branch, and fast-forward the root checkout (`git pull --ff-only origin main`).
