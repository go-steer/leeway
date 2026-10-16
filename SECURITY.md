# Security policy

## Reporting a vulnerability

**Please do not open a public issue.**

Use GitHub's private vulnerability reporting —
[**Report a vulnerability**](https://github.com/go-steer/leeway/security/advisories/new)
on the Security tab. It goes only to the maintainers, and it gives us
a private fork to develop and review the fix in before anything is
published.

If that form is unavailable to you for any reason, email a maintainer
listed in [`CODEOWNERS`](./.github/CODEOWNERS) directly and say
"security" in the subject.

Please include, as far as you can:

- what an attacker gets, concretely;
- the version or commit — `leeway version` prints the version, commit
  and build date stamped at release;
- a reproduction, ideally as a cluster fixture or a `leeway` command
  line.

We will acknowledge within **3 business days** and give you an
assessment within **10**.

## What is supported

Pre-1.0: **the latest released minor only**. Fixes land on `main` and
go out in the next release; there are no backport branches.

## What we consider a vulnerability

`leeway` is a **read-only** placement and priority-rank analyzer pointed
at production clusters with credentials:

- **Credential or token disclosure.** `leeway` reads kubeconfigs,
  credential-proxy broker tokens, and webhook bearer tokens (`--sink-token-env`).
  Any path that leaks a token or secret value into stdout, logs, metrics,
  or webhook payloads is a vulnerability.
- **Privilege beyond the shipped RBAC.** The manifests in `deploy/`
  grant a minimum-necessary read-only ClusterRole. Anything that
  mutates cluster state or reaches resources the role does not name is
  in scope.
- **Supply chain.** Images are keyless-signed with cosign and carry an
  SPDX SBOM attestation per platform under the same identity; release
  binaries have a signed `SHA256SUMS`. Anything that lets an artifact
  verify that we did not build is in scope.
