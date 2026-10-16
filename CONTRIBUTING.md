# Contributing to Leeway

Thanks for looking. This document is the short path from "I have a
change" to "it is merged": what the project will ask of a patch, and
where the answers already are.

- **Design specification** — [`docs/leeway-design.md`](./docs/leeway-design.md),
  the normative specification for placement drift scoring, confidence tiers,
  root-cause attribution, and ComputeClass priority rank tracking.

## Before you open a PR

```console
$ dev/tools/ci                  # the same checks CI runs
```

1. **Sign your commits off.** This project uses the
   [Developer Certificate of Origin](https://developercertificate.org/).
   `git commit -s` adds the trailer; a commit without one will not
   merge. That is the whole CLA story — there is no separate
   agreement to sign.
2. **Run `dev/tools/ci`.** Build, vet, lint, unit tests, gofmt, `go mod
   tidy`, docs lint, govulncheck, version fallback — the same set the
   `ci.yml` workflow runs, so a green local run is a green PR. The
   individual checks live in `dev/ci/presubmits/` if you want to run
   one.
3. **Add a `CHANGELOG.md` entry** under `## [Unreleased]` if the change
   is user-visible. Ours are prose, not one-liners: say what changed,
   and say *why* it is the right change.

## Architectural boundaries

- **`pkg/leeway` is pure.** It has zero Kubernetes (`k8s.io/*`) imports,
  enforced by `pkg/leeway/purity_test.go`. All Kubernetes informer wiring
  and snapshot collection live in `pkg/sources/topologydrift`,
  `pkg/sources/computeclass`, and `pkg/report`.
- **State the claim & prove the silence.** Every rule must be tested
  against both a breaching fixture and a healthy fixture asserting
  zero findings.

## Reporting things

- **A bug or a missed detection** — open a GitHub issue with the cluster
  state or `leeway report` output that produced it.
- **A security vulnerability** — **not** a public issue. See
  [`SECURITY.md`](./SECURITY.md).

## Licensing

Apache 2.0. Go, shell, YAML, and Python files carry the license header
(`dev/tools/add-license-headers` adds it in bulk; `goheader` in
golangci-lint enforces it for Go). Markdown files carry none. By
signing off with the DCO you certify you have the right to submit the
work under that license.
