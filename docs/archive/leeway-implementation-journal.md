# leeway — Implementation Journal & Pre-Extraction Archive

> **Status: Historical Archive (Non-Normative).**
> Every capability recorded in this journal shipped across Phases 0–8 (originally inside `go-steer/k8s-lookout` under [#416](https://github.com/go-steer/k8s-lookout/issues/416), with all prerequisite spikes S1–S11 closed, and was subsequently extracted into standalone `github.com/go-steer/leeway` alongside `leeway report` and deterministic `Finding.Remediation`). Duplicate in-tree `k8s-lookout` leeway sources were deprecated and dropped from `lookout watch --sources=auto` in [`go-steer/k8s-lookout#842`](https://github.com/go-steer/k8s-lookout/issues/842), and the 100k+ / 200.5k-pod `kwokctl` scale measurements were ported in [`go-steer/leeway#29`](https://github.com/go-steer/leeway/issues/29).
>
> - **Normative living specification:** [`docs/leeway-design.md`](../leeway-design.md)
> - **External wire, Go API & CLI compatibility policy:** [`docs/stability-policy.md`](../stability-policy.md)

---

## Pre-extraction context (from original header & §2)

The notes below record the original September 2026 fold-in rationale when `leeway` was developed inside `k8s-lookout` prior to standalone extraction (`github.com/go-steer/leeway`) and [`go-steer/k8s-lookout#842`](https://github.com/go-steer/k8s-lookout/issues/842).

**Status:** Implemented (shipped in `pkg/leeway`, `pkg/sources/{topologydrift,computeclass}`, and `cmd/leeway`). Both maintainer decisions taken (§15 Q3, Q4):
the higher scale targets stand and DESIGN §6.2 is edited to match; `topology-drift`
ships default-on. Spikes S9, S10 and S11 resolved against this repo; **S1, S2 and S3
resolved against a live GKE cluster** — S1 rewrote the §7.7.2 annotation model and
deleted `RankTracker.Move`, S2 confirmed §7.7.1 unchanged and added a second fallback
shape to §7.7.3, S3 measured every remaining extractor key and turned
`NodeProfile.Reservation` into a (project, name) pair. **S4 and S5 are now resolved
too** — the scheduler's `defaultConstraints` are confirmed unreadable on managed GKE,
so FR-9 is a manual sync whose mitigation caps default-derived intent below Tier A,
and §8.4's export defaults are measured rather than guessed (GMP bills per sample, the
GKE managed collector is the OTLP endpoint, temporality is cumulative). **S7 is
deferred by maintainer decision, and S6 closed without being run** — its question
turned out to be answerable from the field-selector grammar rather than from a
namespace count, and its method would have generalised an estate to a population.
**S8 resolved 2026-09-21, and every spike is now closed** — it found the pod memory
model out by 12× (§6.1, §6.6) and shipped the kwok padding it existed to size.
No spike gates a data model, and §7.7.2 carries no `UNVERIFIED` marker.
**Tracking:** [#416](https://github.com/go-steer/k8s-lookout/issues/416)
**Date:** 2026-09-16
**Home:** `k8s-lookout` — `pkg/leeway` plus two watch sources
**Language / stack:** Go, `k8s.io/client-go` shared informers

---


## 2. Where this fits in `k8s-lookout`

### 2.1 Why lookout and not a new repo

Most of the substrate this design needs already exists in lookout, built and
tested:

| What leeway needs | What lookout already has |
|---|---|
| Cluster-wide Pod + Node informers | `object-state` runs them; DESIGN §6.3 — "one informer set serves the sentinel sources *and* the graph" |
| Pod → node → zone relation | `pkg/graph` implements `Node → RunsOn(Zone)` (`derive.go:350`), with the beta-label fallback, interned `NodeID uint32`, COW snapshots |
| Alert state machine, dedup, fingerprinting | `pkg/engine` |
| Durable state, pruning, history | `pkg/store` |
| Prometheus registry, `/metrics`, cardinality capping | `internal/watch/metrics.go` (note `reasonLabelCap` — the same label-explosion problem, already solved once) |
| OTEL bootstrap | `internal/telemetry/otel.go` (traces today; metrics is an extension, §8.4) |
| GKE API access behind a provider boundary | `pkg/cloud/gke` |
| Read-only RBAC with a loud startup probe | `pkg/sources/rbac.go`, `sources.AccessDeclarer` |
| Release, signing, SBOM, Helm, docs site | already shipping |

And `pkg/sources/sources.go` provides the exact seam this plugs into:
`Name() / Scope() / Run(ctx, emit func(Signal)) error`.

There is also a natural pairing on the read path. `pkg/checks/audit/workloads.go:502`
already emits `NoTopologySpread` — *"you declared no spread intent."* Leeway is the
runtime complement — *"you declared spread intent and it isn't holding."* Those two
belong in one repo, one docs site, one finding-kind ledger.

**The decisive argument is the informer.** lookout DESIGN §11 makes one sentinel
per cluster canonical: *"one informer cache, one topology index, one credential
boundary, one failure domain."* A separate resident binary means every cluster runs
a **second cluster-wide Pod informer**. This design spends its entire cost model
(§6) minimising watch-path expense — protobuf, field selectors, object trimming, no
polling. Running a second process pays that whole budget again to decode the
identical bytes, for zero additional information. That is the design's own cost
model arguing against shipping it standalone-by-default.

### 2.2 Package layout

```
pkg/leeway/                     the scoring engine — pure, no client-go
  intent.go                     normalised Intent, precedence resolution
  eligible.go                   eligible-domain computation (§7.1)
  apportion.go                  Hamilton apportionment, water-filling caps (§7.2)
  score.go                      skew, ρ, R, Herfindahl, χ² (§7.3)
  baseline.go                   EWMA/EWMAD, freeze, maturity, invalidation (§7.5)
  rank.go                       preference axes, assignRanks, pod-seconds (§7.7)

pkg/sources/topology-drift/     resident source: topology axes
pkg/sources/compute-class/      resident source: preference axes

cmd/leeway/                     optional standalone binary (§2.4)
```

`pkg/leeway` is deliberately shaped like `pkg/graph`, which DESIGN §6 describes as
*"adopted from the k8s-graph proposal, right-sized, and demoted from product to
package: it has no CLI of its own."* Same story here, and for the same reason: three
callers justify the boundary — the two watch sources, a future read-path
`lookout state drift` one-shot, and the existing `audit` check, which can gain a
"you have intent and it is currently violated" variant once the scoring is a
library.

### 2.3 Source names, finding kinds, metric names

Three different naming systems, and they should not all get the same answer.

- **Source names stay descriptive: `topology-drift` and `compute-class`.** These
  are user-facing config (`--sources=`) and every one of the fourteen siblings is
  self-describing — `object-state`, `saturation`, `token-burn`. `--sources=leeway`
  tells an operator, or an agent reading the flag list, nothing.
- **Finding kinds are `leeway.*`.** `docs/signal-schema-v1.md` is explicit that
  *"`kind` is the detector's own (`audit.no_pdb`)"*, so the prefix tracks the
  detector, not the source; `audit.*` is already a subsystem prefix spanning
  several commands. The settled set is below.
- **Metrics are `lookout_leeway_*`**, matching the existing `lookout_*` prefix.

> **The kinds are the expensive part.** `TestSchemaV1_KindInventory` pins the
> inventory and v1 is frozen, so kind names are a durable commitment in a way that
> source names, package names and metric names are not. They should be settled
> before the first source lands, not during.

**Settled 2026-09-17.** Eight kinds, taking the v1 inventory from 49 to 57. The Go
constants land with their sources in phases 3 and 7; this table is the commitment.

| Kind | Source | Tier | Fires when |
|---|---|---|---|
| `leeway.contract_violated` | `topology-drift` | A | A declared `DoNotSchedule` TSC or required anti-affinity is being violated |
| `leeway.placement_drift` | `topology-drift` | B | Inferred intent deviation — `ρ` over threshold (§7.3) |
| `leeway.baseline_breach` | `topology-drift` | C | Deviation from the subject's own history (§7.5) |
| `leeway.domain_unavailable` | `topology-drift` | B | A domain the cluster has, or recently had, now has no *usable* node |
| `leeway.rank_wedged` | `compute-class` | A | Pods Pending against a `DoNotScaleUp` class that the autoscaler is not provisioning for (§7.7.4) |
| `leeway.rank_degraded` | `compute-class` | B/C | Rank-0 share below threshold, sustained time at the last rank, or mean achieved rank rising against its own baseline |
| `leeway.rank_no_migration` | `compute-class` | B | No `from_rank > to_rank` transitions after capacity returns |
| `leeway.rank_tier_unused` | `compute-class` | info | An entire preference tier unused in 30 d |

Five of these differ from the names this section first proposed, and the reasons are
worth keeping, because each one is an argument about what a kind is *for*.

- **`leeway.skew` became `leeway.placement_drift`.** `skew` put the vaguest name on
  the most-emitted kind; `observed_skew` and `excess_skew` are already metric names,
  so a finding called `skew` reads as though it were about that one metric rather
  than about `ρ`; and it is simply wrong for `ModeColocate`, where the complaint is
  that pods spread out when they should not have.
- **`leeway.pinned_skew` was dropped**, and pinning became a `suspectedCause` on
  `leeway.placement_drift` (§8.5). The payload already carries that field, pinning is
  one cause among several, and promoting exactly one of them to its own kind is
  inconsistent. It also costs nothing in dedup: the §8 fingerprint hashes
  `reasonClass` alongside `kind`, so a distinct cause already gets a distinct
  identity without spending a frozen name on it.
- **`leeway.rank_depth` split into `leeway.rank_degraded` and
  `leeway.rank_tier_unused`.** One kind was carrying four triggers across three
  tiers. The split is not symmetry for its own sake — a dead preference tier is a
  *cost* finding whose subject is the compute class and whose remedy is deleting the
  tier or the reservation, while the rest are availability findings about workloads.
  Different reader, different action. Mean-rank-against-baseline stays inside
  `rank_degraded` rather than routing to `leeway.baseline_breach`, because the rank
  payload is shaped around a rank histogram and sharing the Tier C kind would force
  two payload shapes through one name.
- **`leeway.domain_outage` became `leeway.domain_unavailable`.** We cannot observe an
  outage. We observe that a domain's eligible node count reached zero, which a
  cordon, a taint or a `nodeSelector` edit produces just as readily as a zone
  failure. The name should not assert a cause we did not measure.

  > **Shipped 2026-09-21, and three things about the rule are worth stating here
  > rather than only in the code.** *It counts **usable** nodes — Ready **and**
  > schedulable — not Ready ones.* That single choice is what makes one rule cover
  > both halves of Phase 7's exit criterion: a zone whose nodes are all NotReady and
  > a zone whose nodes are all cordoned are the same finding with different
  > `suspectedCause` values (`domain_outage` and `taint_exclusion`), which is
  > precisely what the rename exists to express. *It is `Known && usable == 0`, not
  > "fewer than the peak inside the window".* The ready series is retained for hours
  > and no longer, so a window rule would have resolved the finding on its own
  > evidence ageing out while the zone was still down; the source instead latches
  > which domains the cluster has, and forgets a domain only when it has neither
  > nodes nor history — at which point we can no longer tell a deliberately deleted
  > zone from a failed one, and asserting the second would be inventing the
  > distinction. *And the axis set is configuration with a narrow default
  > (`--topology-domain-unavailable-keys`, zone and region).* `kubernetes.io/hostname`
  > is a topology key on which every node is its own domain; judged there, this kind
  > would raise a finding per NotReady node, which is `objectstate`'s observation
  > with `objectstate`'s subject.
- **`leeway.baseline_breach` is new: Tier C had no kind at all.** §8.3 routes Tier C
  to metrics by default *but allows a Signal at `info`, opt-in per policy*. A kind
  that is reachable by configuration has to exist in the frozen v1 inventory before
  the source ships, or the opt-in cannot be honoured later without a schema revision.

**`leeway.placement_drift` and `audit.no_spread` are not duplicates and must not be
merged.** `audit.no_spread` is a static claim about a template — this workload
declares no spread constraint — answered from the spec at audit time. `leeway.*` is
the runtime observation that pods are not where the declared or inferred intent puts
them, which fires precisely on the workloads whose templates look correct. The same
distinction holds between `audit.rigid_scheduling` and `leeway.domain_unavailable`.

> **All eight are in the v1 ledger as of 2026-09-21.** `pkg/inject/schema` lists
> what CAN be emitted, not what has been named, and the freeze test counts it — so
> a kind enters the ledger in the change that gives it a producer.
> `leeway.contract_violated` and `leeway.placement_drift` arrived in Phase 4,
> `leeway.baseline_breach` in Phase 5 when Tier C's learned baselines turned on,
> the four `leeway.rank_*` kinds in Phase 6, and `leeway.domain_unavailable` last,
> in Phase 7, because it is raised by the source rather than by a verdict. That the
> names were settled here is what made each of those additions mechanical — the
> commitment was made once, in this table, and the ledger caught up to it eight
> times without a rename.

### 2.4 Standalone binary

`cmd/leeway` builds the same two sources against their own informer set, its own
`MeterProvider`, and an `emit` that writes metrics instead of injecting agent
sessions. This is cheap to keep working, but only if four things hold from day one —
retrofitting any of them is expensive:

1. **`pkg/leeway` never imports client-go.** Pure functions over plain structs.
   This is what makes it embeddable in something that is not lookout at all.
2. **Sources take their informers as a parameter.** Lookout injects the shared set;
   `cmd/leeway` builds its own. This is the discipline most likely to be violated
   by accident, because calling `NewSharedInformerFactory` inline is the natural
   thing to write — and the repo already has the right habit: eight sources expose
   `WithFactory(informers.SharedInformerFactory)` and fall back to constructing
   their own only when none is injected (`objectstate.go:439`, `rollout.go:247`,
   and six more). Leeway follows that shape exactly, with one deviation: it should
   accept a *narrow* interface (register handler, get lister) rather than the
   concrete client-go factory, since the concrete type is what would drag client-go
   back into anything embedding this.
3. **The meter and the store are injected too.** Standalone may run store-less —
   §9.1 establishes that current state is rebuildable, so the cost is losing dwell
   timers across a restart, not losing correctness.
4. **Sources import `pkg/*` only, never `internal/watch`.** They are in the same
   module, so nothing in the compiler stops them. This is the trap that would
   quietly weld the sources to the sentinel.

**What it is for:** clusters not running lookout; a metrics-only deployment whose
consumer is Grafana and a rotation rather than an agent; isolating the subsystem
under kwok for scale runs; vendoring `pkg/leeway` elsewhere.

**What it must not become:** a second process on a cluster already running
`lookout watch`. That reintroduces the duplicate Pod informer §2.1 exists to avoid.
This belongs in the deployment docs as an explicit constraint, because otherwise
someone will do it.

**Two honest caveats.** First, DESIGN §4.1 is "one multicall binary"; a second
`cmd/` is a narrow exception (an optional artifact, not a second user-facing CLI
surface) and folding this in should say so rather than quietly adding it. Second,
disciplines 1 and 4 are conventions unless enforced. lookout documents the
analogous provider boundary in four places — `AGENTS.md:37`, `DESIGN.md:171`,
`repo-map.md:124`, `CONTRIBUTING.md:82` — and enforces the cloud half structurally
with build tags plus `pkg/cloud/noprovider.go`, but there is no general
import-graph test. One is needed here, and `cmd/leeway` must be built in CI: a
binary nobody builds is a binary that does not work.

> **Shipped 2026-09-22** (#477). `cmd/leeway` exists, runs both sources, and is
> built and executed by `TestSmoke_*` in `cmd/leeway/smoke_test.go` — a Go test
> rather than a workflow step, so `go test ./...` covers it everywhere and there
> is no second copy of the command line to drift. It starts against an
> unreachable apiserver, binds, serves, answers `/readyz` with 503 while its
> informers cannot list, and exits 0 on SIGTERM. `/leeway` ships in the same
> image as `/lookout`; the entrypoint is unchanged.
>
> The import-graph test is `TestLayering_TheImportGraphMatchesTheDesign` in
> `pkg/sources`, and it enforces a **stronger rule than discipline 4 asks for**:
> not "`pkg/sources` never reaches `internal/watch`" but "`pkg/...` never reaches
> `internal/...` at all". That was already true when the test was written, so the
> stronger rule cost nothing, and it is the rule that actually describes the
> layering — `internal/` is the sentinel, `pkg/` is the embeddable half, and the
> dependency runs one way. A rule that names one forbidden package invites the
> next one to be argued about; a rule that names the boundary does not. Each rule
> also fails when its `go list` pattern matches nothing, so a package rename
> cannot silently retire a guard.
>
> **Discipline 2's narrow interface did not ship, deliberately.** The sources
> still take `informers.SharedInformerFactory` through `WithFactory`, and
> `cmd/leeway` gets its own set by injecting nothing and letting each source
> construct one. Everything this issue's exit criteria asked for is satisfied by
> that — the binary builds, runs and is testable — and the narrow interface buys
> exactly one thing the criteria did not ask for: `pkg/sources` ceasing to import
> client-go, which is a property **only `pkg/leeway`** is claimed to have (NFR-10
> and discipline 1) and which the guard above already checks. Sources read
> Kubernetes objects; they are going to link client-go whatever their factory
> parameter is typed as. Doing it here would have meant a new interface, eight
> `WithFactory` signatures and their call sites changed, for a boundary that
> moves no dependency. It is left as a refinement rather than folded in silently.
>
> **Discipline 3 is half-honoured and that is the design.** The meter is
> injected; the store is not injected because there is none —
> `cmd/leeway` runs store-less, which §9.1 makes a supported posture rather than
> a degraded one. What a restart costs is the in-flight dwell timers and the
> learned baselines, not correctness.
>
> One thing the binary needed that the sketch did not anticipate: **`--sources`
> defaults to both, and `compute-class` is skipped when discovery says the GKE
> CRD is not served.** Its `Run` fails loudly on a cluster that cannot serve it,
> which is right when an operator named it and wrong when it came from the
> default — the default would then fail on kind, on kwok, and on every cluster
> that is not GKE, including the kwok harness Phase 8's scale tier runs on. The
> asymmetry is the sentinel's own `--sources=auto` rule: a source you named must
> run, a source that came from the default is a candidate.

### 2.5 What folding in changes about this design

Four things, all load-bearing:

1. **We do not own the informers, so `trimPod` becomes a negotiation.** See §6.1 —
   this is the one place where the fold-in has a real cost, and it invalidates part
   of the original memory model.
2. ~~**The scale posture needs reconciling.**~~ **Resolved (2026-09-15): the higher
   targets win, and DESIGN §6.2 was edited to match.** That paragraph previously
   read *"100k is the ceiling, not the design point"* against this document's 200k
   pods at 500 pods/sec. The two were never really in conflict — they were one
   sentence covering two different questions. §6.2 now states them separately:
   pods are a **memory** question (200k target, no cliff before ~500k, dominated by
   the informer caches), events/sec are a **CPU** question (500 pods/sec ≈ 5,000
   events/s ≈ a quarter core across the whole watch path). The 50k events/s target
   stays dropped as fiction — nothing about a bigger cluster gets you there. A
   200k-pod cluster is ten times the memory of a 20k-pod one and roughly the same
   CPU.
3. **Output shape.** Lookout's watch path turns signals into agent sessions with
   warm context. Drift is a slow-moving condition, not an incident. §8 handles this
   by routing most leeway findings to the store and metrics rather than to inject,
   using the existing severity routing (DESIGN §7.7) — the same treatment
   `notifications` info-severity signals already get.
4. **Two metric APIs in one binary, deliberately.** Spike S11 established that
   `otelprom` bridges leeway's OTEL-native instruments into the registry the
   sentinel already serves, so `/metrics` stays one scrape and no migration is
   needed. The accepted asymmetry is on the push side: an OTLP backend sees
   leeway's metrics and none of the sentinel's 43 until someone files that refactor
   on its own merits. §8.4 and §13/S11 carry the numbers and the two regressions
   that argue against doing it on leeway's schedule.

---

## 13. Prerequisite spikes

Decisions here rest on assumptions only observation can settle. Each spike is a
question, a method, and the decision it unblocks — no production code, roughly four
days total. Spikes S9–S11 are new, and exist only because of the fold-in.

**S9, S10 and S11 are done** (2026-09-15) — all three were desk work against this
repo and needed no cluster. Between them they changed five things: the terminal-pod
field selector is narrower, the transform's narrowing is confirmed safe, leeway owns
its own indexes because the graph runs only under `--storm`, half the §8.2 state
machine is now `pkg/engine.RecoveryTracker` rather than new code, and the OTEL
metrics migration §8.4 called a prerequisite turns out not to be one. Every one of
the three deleted or narrowed work.

**S1 and S2 are also done** (2026-09-15), both on `std-simian-test`, and they cut in
opposite directions. S1 *added* work: it confirmed the pessimistic branch of §7.7.3 and
turned up three states the data model did not have. S2 confirmed §7.7.1 exactly as
written and changed nothing structural — which matters, because between them they were
the last two spikes that could have invalidated a data model. **No spike now gates a
data model.**

**S4 and S5 closed 2026-09-16, S7 was deferred by the maintainer, and S6 closed
2026-09-17 without being run.** S4 found what it expected (the managed control plane is
unreadable) and spent its value on the mitigation instead; S5 answered all three of
its questions, and answered the hardest one by reading the managed collector's own
pipeline rather than by asking anyone. S6 is the odd one out and the most instructive:
it was closed on its *method*, not on a finding, and the note below is as much about
which spikes are worth running as about namespaces.

**S8 closed 2026-09-21, and with it the whole spike programme.** It is the only
spike that overturned a quantitative claim rather than confirming or narrowing one:
the trimmed pod is 12× the assumed size, and the assumption had been carried, in
writing, since the standalone design. It is also the only one whose output is code
that keeps running — the padding and its check — rather than an amended paragraph.
Both facts argue the same thing, which is that the spike worth running is the one
measuring a number other numbers are derived from.

| ID | Question | Unblocks | Effort |
|---|---|---|---|
| ~~S1~~ | ~~What *is* a compute-class fallback — new node or mutated node?~~ | **RESOLVED** — §7.7.2 and §7.7.3 amended; `Move` deleted | done |
| ~~S2~~ | ~~Does `ccc_priority_index` stay an array index when `priorityScore` is set?~~ | **RESOLVED** — yes; §7.7.1 unchanged, §7.7.3 gained a second fallback shape | done |
| ~~S3~~ | ~~Reservation and accelerator node label keys~~ | **RESOLVED** — accelerator verified; reservation is three labels, and identity is (project, name) | ~~0.25 d~~ |
| ~~S4~~ | ~~Can we read the scheduler's `defaultConstraints`?~~ | **RESOLVED** — no, confirmed live; FR-9 is a manual sync, and §13 S4 is the mitigation | ~~0.25 d~~ |
| ~~S5~~ | ~~Prometheus series budget and OTLP backend~~ | **RESOLVED** — GMP (cost, not a cap), the GKE managed collector, cumulative | ~~0.25 d~~ |
| ~~S6~~ | ~~Namespace count and watch-stream headroom~~ | **CLOSED without measuring** — the answer is the selector grammar, not a count; §6.7 rewritten, #431 shipped, #407 held | ~~0.25 d~~ |
| ~~S7~~ | ~~Real pod event rate per scheduled pod~~ | **DEFERRED** by the maintainer — see below | ~~0.5 d~~ |
| ~~S8~~ | ~~Real object sizes, for kwok padding and the trimmed-pod budget~~ | **RESOLVED** — §6.1 and §6.6 corrected (a trimmed pod is 18.6 KB of heap, not 1.5 KiB); padding shipped and enforced | ~~0.5 d~~ |
| ~~S9~~ | ~~Does any source need terminal pods, or fields the transform strips?~~ | **RESOLVED** — §6.1 amended | done |
| ~~S10~~ | ~~How much of §6.2 / §8.2 does `pkg/graph` + `pkg/engine` already provide?~~ | **RESOLVED** — §6.2 and §8.2 amended | done |
| ~~S11~~ | ~~Cost of migrating the sentinel's ~30 metrics to the OTEL API~~ | **RESOLVED** — §8.4 amended; migration descoped | done |

**~~S1 — Observe a fallback.~~ RESOLVED 2026-09-15.** Provoked rather than found: no
node in any inspected cluster had ever left rank 0, so the fleet sweep was skipped in
favour of building the condition. On `std-simian-test` (1.36.4-gke.1082000,
us-central1), namespace `leeway`, a `ComputeClass` named `leeway-fallback-probe` whose
rank-0 rule asks for a **large N4 spot machine** — `machineFamily: n4, spot: true,
minCores: 128`, where N4 caps at 80 vCPU in this region — and whose rank-1 rule is a
plain `machineFamily: n2`, plus one `pause` Deployment. Making rank 0 unsatisfiable by
*shape* rather than by stockout made the fallback deterministic and free: rank 0 never
provisions, so the expensive half of the experiment cost nothing.

| | Answer |
|---|---|
| (i) rank-1 node carries `ccc_priority_index: "1"` | **Yes.** First node observed off rank 0 |
| (ii) new node or mutated node | **New node, new NAP pool.** Mutation is impossible: existing pools carry another class's `NoSchedule` taint |
| (iii) direct bind or Pending | **Pending, 4 m 27 s.** Scale-up triggered at 7 s |
| (iv) `DoNotScaleUp` vs `ScaleUpAnyway` | Both leave running pods alone. `DoNotScaleUp` refuses the scale-up and stamps `ccc_no_rule_matching`; `ScaleUpAnyway` provisions off-list and stamps `ccc_scale_up_anyway` |
| (v) migration back | **Real, 4 m 17 s, and a different pod.** See §7.7.3 |

Four results beyond the question asked:

- **The annotation has a non-numeric domain** — `ccc_no_rule_matching`,
  `ccc_scale_up_anyway`, and absent-for-43 s — and it mutates in place on a live node.
  §7.7.2 now carries the full state machine. This is the largest single change S1
  caused, and no amount of reading would have produced it.
- **`RankTracker.Move` is deleted.** Pods are born and die at a rank; they never move.
- **Spot churn is a false-positive generator.** `gcloud` recorded
  `compute.instances.preempted` on a rank-0 spot node after 7 m 30 s, followed by
  `compute.instances.repair.recreateInstance` **reusing the node name**, with the
  replacement pod landing in a different zone. Node identity keyed on name is not
  stable across preemption, and a zone-skew detector watching a spot-backed tier will
  see clean zone migrations that are not drift. §7.1's normal-deviation split has to
  absorb this; it is now the second-largest false-positive class after §7.7.6.
- **`capacityCheckWaitTimeSeconds` is not usable here** — admission rejects it with
  *"only supported for Flex Start and for multi-host TPUs"* — so fallback latency is
  GKE's and cannot be tuned down for tests.

Also confirmed in passing, because §7.7.6 rests on it: GKE **auto-injects** the
`cloud.google.com/compute-class=<name>:NoSchedule` toleration into a pod whose
manifest declares none.

The class is kept as the permanent test fixture, restored to its rank-0-unsatisfiable
form so it provisions nothing while idle. Node dumps and the 1,537-row observation
timeline seed the §16 fixtures.

> **For anyone repeating this:** do not write
> `.metadata.annotations.ccc_priority_index // "0"`. That default — which an earlier
> draft of this section used — reads every unstamped and every sentinel-stamped node
> as rank 0.

**~~S2 — `priorityScore` semantics.~~ RESOLVED 2026-09-15.** No class on any inspected
cluster uses the field, so this too was provoked. On `std-simian-test`, a class
(`leeway-scored-probe`) arranged so index order and preference order cannot coincide:

```yaml
priorities:
  - machineFamily: n2   # index 0, score 10 — LEAST preferred
    priorityScore: 10
  - machineFamily: n4   # index 1, score 50 — tied most preferred
    priorityScore: 50
  - machineFamily: c3   # index 2, score 50 — tied most preferred
    priorityScore: 50
```

| | Answer |
|---|---|
| Index or rank under scoring? | **Still an index.** The winning node was `c3` — list position 2, preference tier 0 — and was stamped `ccc_priority_index: "2"`. A rank reading would have said `"0"` |
| Is scoring honoured over list order? | **Yes.** Index 0 was skipped outright; scale-up went straight to a score-50 rule |
| Partially-scored class at admission | **Rejected.** `spec: Invalid value: PriorityScore must be set for all priorities or for none of them`. `OrderingInvalid` is unreachable via the GKE API server |

So §7.7.1 needed no change. That is the useful result — it was the last spike that
could have invalidated a data model, and it did not.

Two results beyond the question asked:

- **The peer case occurred unprompted, and it is a live false-positive source.** The
  `n4` scale-up hit a genuine stockout (*"GCE out of resources"*, `us-central1-f`) and
  GKE fell through to `c3`. Index 1 → index 2, **rank unchanged**, because the author
  scored them equal. Any implementation reading the annotation as a rank reports a
  fallback on the very first workload it sees. §7.7.1 records the timeline.
- **A second, structurally invisible fallback shape.** Because the preferred rule never
  produced a node, the *same* pod waited in `Pending` and was bound once at index 2 —
  no transition, nothing ever observable at the preferred rank. §7.7.3 now separates
  this pre-emptive shape from S1's post-hoc one and marks "unmet preference" as a
  known gap belonging to a different detector on the event stream, not to §7.7.

The absent-annotation window from S1 reproduced here on an unrelated class (`<none>`
at 19:32:41, `"2"` at 19:32:53), which is the independent confirmation §7.7.2 wanted.

**S3 — Reservation and accelerator labels. RESOLVED 2026-09-16.** The two keys are
independent; the accelerator half closed on 2026-09-15 and the reservation half a day
later, once a reservation existed to consume.

**Accelerator: verified.** A class with `gpu: {type: nvidia-tesla-t4, count: 1}` on
`machineFamily: n1` provisioned `n1-standard-2` + 1×T4 (the first attempt, L4 on `g2`,
hit *"GCE out of resources"* in `us-central1-a` — L4 capacity is scarce and `n1`/T4 is
the reliable probe). The node carries:

```
cloud.google.com/gke-accelerator        = nvidia-tesla-t4   ← the extractor key
cloud.google.com/gke-gpu-driver-version = default
```

The value is the rule's `gpu.type` verbatim, so the matcher compares without
normalising. §7.7.2's `accelerator` marker is cleared.

**Reservation: verified — and it is three labels, not one.** A single-VM
`n2-standard-2` reservation in `us-central1-a` created with
`--require-specific-reservation`, consumed by a class whose only rule was
`reservations: {affinity: Specific, specific: [{name, project, zones}]}`. The node
carries:

```
cloud.google.com/reservation-name      = leeway-probe-res        ← the extractor key
cloud.google.com/reservation-project   = gke-demos-345619        ← not modelled before
cloud.google.com/reservation-affinity  = specific                ← not modelled before
```

Consumption is proven, not assumed: the reservation reported `inUseCount: 1` while the
node was up, and `--require-specific-reservation` means the node could not have been
created on-demand instead.

**The finding that changes the model is `reservation-project`.** A reservation name is
unique only within a project, and a reservation can be shared *to* other projects, so a
node consuming a reservation from a neighbouring project would carry a name that
collides with an unrelated local one. Name alone is therefore not an identity. §7.7.2's
`NodeProfile.Reservation` becomes a `ReservationRef{Name, Project, Affinity}` and the
matcher compares the pair against the rule's `reservations.specific[].{name, project}`.
On a single-project cluster the two agree and nothing changes; the point is that the
failure is silent on the clusters where they do not.

`reservation-affinity` is recorded but not matched on. It restates the rule's
`affinity` lowercased, so as a matcher input it is circular — its value is in evidence,
distinguishing a node that *had* to take this reservation (`specific`) from one that
merely landed on it (`any`).

**Two obstacles worth writing down, because both mislead.**

*GKE Warden rejects `location` combined with specific reservations* — "compute-class …
contains priorities using location config with specific reservations enabled". The zone
must be pinned through `specific[].zones` alone. Nothing in the CRD schema hints at this;
it is an admission webhook, so it surfaces as a rejected apply rather than a status.

*A reservation's default sharing policy makes it invisible to GKE.* With
`reservationSharingPolicy.serviceShareType: DISALLOW_ALL` — the default — scale-up fails
with `ReservationNotFound`: *"does not exist or not accessible"*, on a reservation that
is `READY` and in the right project and zone. The fix is
`gcloud compute reservations update <name> --zone=<zone> --reservation-sharing-policy=ALLOW_ALL`
(the enum is underscored; `allow-all` is rejected). This matters beyond the spike: **a
user reporting that leeway's reservation axis "sees nothing" may have a policy problem,
not a detector problem**, and the GKE error text points away from the cause. Note also
that the `ComputeClass` status condition does not re-evaluate on `kubectl apply` — the
class must be deleted and recreated to retest.

The in-cluster probe (class, pod, auto-provisioned pool) was torn down immediately; the
reservation is deleted separately, since it bills whether or not anything consumes it.

**Beyond the question asked:** the GPU node carries a *second* `NoSchedule` taint
(`nvidia.com/gpu=present`) and the probe pod was admitted with tolerations for both,
having declared neither. §7.7.6 now records the consequence — eligibility must be read
off the admitted Pod, because a `PodTemplateSpec` has none of the injected tolerations
and yields an eligible-node set that is too small.

**S4 — Scheduler default constraints. RESOLVED 2026-09-16: no live read, and the
mitigation is a downgrade rather than a workaround.** The target estate is all
managed GKE, so the question had a single answer to find. On `std-simian-test`
(1.36.4-gke.1082000) the control plane is not a workload: `kubectl get pods -n
kube-system -l component=kube-scheduler` returns nothing, there is no
scheduler-shaped ConfigMap in `kube-system`, and there is no API surface exposing
`KubeSchedulerConfiguration`. FR-9 is a documented manual sync. That was the expected
answer; what the spike was really for is what to do about it, because static config
that silently desyncs from the real scheduler is a correctness hazard and "write it
down and hope" is not a mitigation.

The mitigation has four parts, and the first two shrink the problem far more than the
last two solve it.

- **The blast radius is much smaller than it looks.** `defaultConstraints` apply only
  to a Pod that declares *no* `topologySpreadConstraints` of its own — a Pod with even
  one constraint ignores the cluster defaults entirely. So a wrong assumption cannot
  corrupt intent for any workload that has actually expressed intent. It can only
  affect the population where leeway's intent was weakest anyway, which is precisely
  where it should already be cautious.
- **The defaults are soft, so they are never a contract.** The upstream system default
  set is `kubernetes.io/hostname` at `maxSkew: 3` and `topology.kubernetes.io/zone` at
  `maxSkew: 5`, both `whenUnsatisfiable: ScheduleAnyway`.
  > **Corrected 2026-09-19 while implementing FR-9: the two keys were written the
  > wrong way round here.** kube-scheduler's `systemDefaultConstraints` pairs the
  > *tighter* bound with the *finer* axis — hostname 3, zone 5 — which reads
  > backwards if you expect the coarser axis to be the stricter one, and is
  > presumably how the swap got in. It matters: under *unset* these are the numbers
  > every undeclared workload in the fleet is scored against, and swapping them is
  > wrong on both axes at once with nothing else in the system to notice.
  > `SystemDefaultConstraints` in `pkg/sources/topologydrift` is now the single
  > place they are written down, and `TestSystemDefaultConstraints_MatchUpstream`
  > pins the pairing.

  Soft constraints are a
  scheduler preference that placement is free to violate, so a breach of one is not a
  breach of anything anybody promised. **An assumed cluster default is therefore
  capped at Tier B** — it can raise a statistical finding or feed a Tier C baseline,
  and it can never raise `contract_violated`. §8.1 states that as its own rule rather
  than relying on `ScheduleAnyway` to imply it, because an operator who *declares* a
  default is free to declare a `DoNotSchedule` one, and then the assertion is theirs
  and Tier A is fair. This is the part that matters: for the assumed case it converts
  a correctness hazard into a precision one.
  (Those two values are version-dependent and are stated here as the upstream default,
  not as a measurement — confirming them per GKE version is the operator's job, which
  is exactly what the next bullet is for.)
- **Three states, and "unset" is not "empty".** The FR-9 config distinguishes *unset*
  (we have never been told), *declared empty* (an operator has asserted the cluster has
  no defaults), and *declared* (an operator has supplied them). Defaulting a missing
  config to "no constraints" is the failure this spike exists to prevent: it produces
  confident findings built on an assumption nobody made. Under *unset*, leeway assumes
  the upstream set above, marks every intent derived from it `confidence: assumed`, and
  logs once at startup naming the assumption and the version it came from.
- **The assumption is visible on the wire.** §8.4's `intent_info` already carries
  `source` and `confidence`; cluster defaults split into two sources,
  `cluster-default-declared` and `cluster-default-assumed`. A dashboard can then answer
  "how much of this fleet's intent rests on a guess?" without reading the config, and
  an estate that cares can drive it to zero by declaring. Declaring is cheap; the point
  of the design is that not declaring is *honest* rather than silently wrong.

**S5 — Metrics budget. RESOLVED 2026-09-16, all three questions, and one of them
answered itself.** The estate uses Google Managed Prometheus and the GKE managed
OpenTelemetry collector, with Google Cloud Metrics behind both interfaces.

- **The Prometheus budget is a cost, not a cap.** GMP does not reject a high-cardinality
  target; it bills per sample ingested. For a default-on subsystem that is the worse
  failure mode, because nothing breaks — the bill just grows, in a line item nobody is
  watching. At a 30 s scrape one series is 86,400 samples/month, so §8.4's ungated worst
  case of 480k series is ~41 billion samples/month against ~3.5 billion for the
  aggregate-only path. The 12× is the whole decision. **`perDomainSeriesMinDrift: 0.05`
  stays, and stays on by default**; the gating is not an opt-in tuning knob for large
  estates, it is the default posture, and a deployment that wants per-domain series for
  everything has to ask for it. Ingest is via a `PodMonitoring` CR rather than a
  self-hosted Prometheus, which is a Phase 8 deployment artifact — and it must be
  optional in the chart, since `monitoring.googleapis.com` CRDs do not exist off GKE.
  The `manifests` CI stage will require `deploy/` and the Helm chart to agree on it.
- **The OTLP endpoint exists and needs no credentials from us.** Measured on the same
  cluster: `opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317` (gRPC) and
  `:4318` (HTTP). Its receivers are plaintext on the pod IP with no TLS or auth block —
  the Google credentials live in the collector's `googleclientauth` extension on the
  *export* side, so lookout ships to a cluster-local ClusterIP with `WithInsecure()` and
  no service-account key anywhere in the metrics path. Two operational details worth
  designing against rather than discovering: it is a **single-replica Deployment**, not
  a per-node DaemonSet, so it is a shared chokepoint and §8.4's bounded-queue,
  drop-on-full requirement is load-bearing rather than theoretical; and its gRPC server
  sets `max_connection_age: 10m` with a 1 m grace, deliberately recycling connections to
  rebalance, so a clean `GOAWAY` every ten minutes is normal and must not be counted as
  an export failure in the SLIs.
- **Cumulative — and the collector proved it rather than the docs.** The managed
  pipeline's `metrics/otlp` stage runs `memory_limiter`, `k8s_attributes`, two
  `resource`/`transform` passes and `batch`, then exports straight to
  `telemetry.googleapis.com:443`. There is **no `deltatocumulative` or
  `cumulativetodelta` processor anywhere in it**, so whatever temporality lookout emits
  is what lands in Cloud Metrics, unconverted. Since S11 put both readers on one
  `MeterProvider` and the Prometheus reader is cumulative by construction, choosing
  delta for the OTLP reader would make one instrument name mean two different things
  depending on which exit it left by. **Cumulative on both readers.** The consequence is
  that §8.4's "delta improves the restart story" escape hatch is closed: the §7.7.3
  pod-seconds counter keeps its reliance on `increase()` tolerating a reset, so the
  counter must be monotonic for a process lifetime and a restart must show as a reset
  rather than be papered over.

**S7 — Real event rate. DEFERRED 2026-09-16, by maintainer decision, and recorded
here so it does not read as merely open.** §6.6.1's ~8 watch events per pod lifecycle
stays a modelled number. This is a deliberate accepted risk, and a bounded one: the
multiplier scales a CPU estimate, not a data model, so being wrong costs a resized
budget rather than a rewrite — which is why it was the right one to drop. The
measurement remains worth doing before Phase 8's scale gate, where the modelled figure
is what the soak is being judged against; until then §6.6.1 should be read as an
estimate and not quoted as a measurement.

**~~S6 — Namespace count.~~ CLOSED 2026-09-17 without being run** — by maintainer
decision, on the method rather than on the finding.

The planned method was `kubectl get ns | wc -l` across the fleet plus apiserver
watcher counts. Two things were wrong with it, and the second is the one worth
carrying forward.

*The sample was never going to generalise.* The maintainer's estate is not a random
draw from the population of clusters lookout will run in, so a namespace-count
distribution measured across it would describe that estate and be quoted as though
it described the world. The corrective is a classification worth applying to every
future spike on this design:

- **Mechanical ratios** — measure them anywhere, because estate bias is
  second-order. Events per pod lifecycle (S7), the §6.1 transform's reduction ratio
  (S8): these are properties of Kubernetes and of our code, and one cluster's answer
  is approximately every cluster's answer. **S8 later tested this split and it
  held** — the transform's pod-heap ratio came out 25.7% on GKE and 25.3% on kind,
  while the absolute pod size varied 1.9× between the same two clusters. Same spike,
  same objects: the ratio generalises, the size does not.
- **Population parameters** — never sample these, *declare* them as a support
  envelope. Namespace counts, pod-spec fatness, pod distribution across namespaces:
  these are properties of who is running the software, and a sample of the clusters
  we happen to have says nothing about the clusters we do not. State what we intend
  to support and size for it.

*And the question did not need a measurement.* S6 existed to tell §6.7 whether
server-side scoping was viable. It is: exclusion is one stream at any length,
inclusion is `10M + 1`, and **neither crosses over at a namespace count**. No value
S6 could have returned would have changed a line of the design. The thing that
actually resolved it was reading the field-selector grammar and then verifying the
one asymmetry that matters — that `metadata.namespace` is rejected outright on
cluster-scoped resources — against a live API server.

Both halves are now built or filed rather than pending: `--exclude-namespace` became
a real watch scope in [#431](https://github.com/go-steer/k8s-lookout/pull/431), and
the include-list shipped as [#407](https://github.com/go-steer/k8s-lookout/issues/407)'s
`--watch-scope=namespace --namespace=a,b,…`, an operator-stated set rather than
one inferred here. §6.7 carries the reasoning; the operator
guidance is in `operations/scoping.md`.

**S7 — Real event rate.** §6.6.1 assumes ~8 watch events per pod lifecycle, a modelled
number the whole CPU budget scales off. *Method:* a throwaway watch on the busiest
cluster counting pod events over an hour against pods scheduled in the same window.
*Done when* the multiplier is measured.

**~~S8 — Real object sizes.~~ RESOLVED 2026-09-21, and it corrected the design.**
Double duty as planned: it sized the kwok padding *and* settled the trimmed-pod
budget that §6.1's narrowed transform invalidated. *Method:* `Pod` and `Node`
sampled on two live clusters — `std-simian-test` (GKE 1.36) and a kind node —
serialised before and after the real `sharedTransform`, plus the `status.images`
length distribution, plus retained heap measured directly by holding 5,000 deep
copies. The measurement is committed as `internal/watch/objectsize_test.go`,
env-gated on `LOOKOUT_MEASURE_CONTEXT` and asserting nothing, so it re-runs against
any cluster without becoming a test that fails on somebody else's hardware.

Findings, in descending order of how much they changed:

- **A trimmed pod is 18,630 B of retained heap, not ~1.5 KiB — 12× out.** §6.1 and
  §6.6 are corrected. The error's mechanism is worth more than the number: memory
  had been derived from serialised size, and a trimmed GKE pod is 1.7× larger in the
  heap than on the wire.
- **The default `memory: 256Mi` is exhausted by the pod cache at ~14k pods**, inside
  the range DESIGN §6.2 calls typical. Not leeway's bug and not leeway's to fix, but
  measured now rather than suspected — [#480](https://github.com/go-steer/k8s-lookout/issues/480).
- **The transform's *ratios* are stable across both clusters** (pod heap 25.7% /
  25.3%) while the absolute sizes vary 1.9×. That is the S6 mechanical-ratio versus
  population-parameter split showing up again, in the one spike that measured both
  kinds of quantity at once — and it is why the padding targets are quoted from GKE
  and the tolerance is a floor rather than a band.
- **Two measurement traps**, both of which produced confidently wrong numbers first:
  `kubectl get -o json` silently strips `managedFields` (34% of a pod), and a Go
  heap measurement that only takes `len()` of the slice it allocated measures
  nothing, because the compiler proves the copies dead before the GC runs. §12.1
  carries the first; the test carries a comment about the second.

**S9 — Shared-transform safety. RESOLVED (2026-09-15), with one design change.**
Four findings, all by inspection of this repo:

- **There is no transform today.** `wiring.go:963` is a bare
  `NewSharedInformerFactory(client, 0)`. Ours would be the first, process-wide.
- **Nulling `Env[].Value` is safe.** Every informer-reachable reader — `pkg/graph`
  (`derive.go:159–173`, `changes.go:272–286`), `pkg/checks/state`
  (`edges_checks.go:390–409`, `wi.go:330`) — reads only `Name` and `ValueFrom`.
  Not one reads the literal value.
- **Container statuses must stay** (`podclearance.go:349`, `rollout.go:501`).
- **The terminal-phase field selector was wrong and is now narrowed.** Excluding
  `phase=Failed` would have silently killed objectstate's eviction-burst detector
  (`objectstate.go:1220`). §6.1 now excludes `Succeeded` only.

*Remaining work* was the deliverable, not the question — and it is **done**
(2026-09-16): `pkg/kube/transform_registry.go` plus the behavioural guard in
`transform_test.go`. The Phase 2 change it was blocking — attaching the transform
to the factory — landed on 2026-09-17; see §6.1.

**Implementing it found two things S9's own inspection missed**, which is the
case for the registry rather than an argument against the spike:

- **Ephemeral containers were not covered.** They share
  `EphemeralContainerCommon` with a regular container, so `kubectl debug --env`
  put literal secret values in the shared cache while every strip S9 specified
  still passed. A hole in the security goal, not a missing optimisation.
- **Two more informer-reachable readers.** `internal/watch/enrich.go` imports
  `pkg/checks/delta` and `pkg/checks/logs` from the same block that makes
  `pkg/checks/state` reachable. `delta` reads container statuses (already
  preserved, no change) and `logs` reads `EphemeralContainers[].Name` — which is
  exactly the field the ephemeral strip had to be careful not to take.

Both were found by enumerating readers a second time, by hand, one day after the
first enumeration. Neither would have been caught by anything failing. That is
the argument for making the mechanism a test.

**S10 — Boundary with `pkg/graph` and `pkg/engine`. RESOLVED (2026-09-15).** The
answer moved work in both directions:

- **Leeway owns every index in §6.2**, chiefly because the graph feed is built
  inside `if f.stormEnabled()` — with `--storm=off` there is no graph at all, and
  a drift detector cannot have its counters disappear with an unrelated
  correlation flag. The graph also offers only traversal (`Radius`, `PodsUnder`,
  `OwnerChain`), no aggregation, and swaps too slowly for counting.
- **But `pkg/engine.RecoveryTracker` takes over half of §8.2.** Its
  `ClearanceObserver` interface is a clean predicate seam, and it already
  implements clearing/flap-reset/resolved/reverted. Leeway owns the fire side and
  implements the observer for the resolve side. Constraint recorded in §8.2: one
  global `stableFor` per tracker versus per-policy `resolveAfter`.
- `PodsUnder` and `OwnerChain` stay useful off the hot path — verifier rebuild and
  owner resolution — with a fallback for when storm is off.

**S11 — OTEL metrics migration. RESOLVED (2026-09-15). The answer is "half a day
*and* a week — and leeway only needs the half day."** The spike asked which of the
two it was; the prototype's real finding is that they are separable, and the design
had them fused.

*Method, as specified:* three instruments were actually migrated in a throwaway
branch — `lookout_recoveries_reverted_total` (plain counter), `lookout_storms_active`
(gauge), and `lookout_events_seen_total` (labelled counter through `boundReason`) —
onto `go.opentelemetry.io/otel/metric`, with `otelprom` bridging the `MeterProvider`
into the sentinel's existing registry. It compiled, served, and was then reverted.

**The scaffolding is genuinely small.** ~45 lines: a `newMeter` helper building the
bridge and the `MeterProvider`, a generic `must` unwrapper (every OTEL instrument
constructor returns `(T, error)`, and threading 43 error returns through
`buildMetrics` is not the trade anyone wants), and passing one `otelmetric.Meter`
down through `resolveRunners` → `newRunner`. Both readers can then hang off the one
provider exactly as §8.4 draws it. **This is all leeway needs**, and it can land in
Phase 2 alongside the source rather than gating it.

**Migrating the existing instruments is a different job.** Measured, not estimated:

| | Count |
|---|---|
| Instruments in `internal/watch/metrics.go` | 43 (23 `CounterVec`, 11 `Counter`, 5 `Gauge`, 3 `GaugeVec`, 1 `Histogram`) |
| Record sites outside `metrics.go` | 85 |
| …in a function with no `context.Context` in scope | 15 |
| Test files touching instruments directly | 25 |
| `testutil.ToFloat64` assertions | 88 |
| Diff for **three** instruments | 5 files, +118/−42 |
| Test files that stopped compiling from those three | 7, with 17 errors |

There is no wrapper layer: call sites hold the `*prometheus.CounterVec` and call
`WithLabelValues(...).Inc()` directly, so every one of the 85 is edited by hand.
Fifteen need a `ctx` that is not there — `countResolved(sig)` became
`countResolved(ctx, sig)` in the prototype, and that propagates. `testutil.ToFloat64`
takes a `prometheus.Collector` and cannot accept an OTEL instrument at all, so all 88
assertions are rewritten against a gather-and-parse or the SDK's `metricdata` reader.
Extrapolating the three-instrument diff, this is **4–6 engineer-days of mechanical
edits with a large blast radius and no user-visible benefit** beyond putting the old
metrics on OTLP too.

**And it is not neutral — it loses two things.** Both surfaced only because the
prototype was built rather than reasoned about:

1. **`MetricsInventory()` stops being derived.** `internal/watch/metricsdocs.go`
   builds the docs-site metrics page by calling `Describe()` on each live collector
   and regex-parsing `prometheus.Desc.String()`, precisely so names and help strings
   "cannot drift from metrics.go". An `otelmetric.Int64Counter` is an opaque
   interface whose only method is `Add` — the concrete type is an unexported
   `*metric.int64Inst`. There is no `Describe`, no name accessor, no description
   accessor. The three migrated rows had to be hand-written back into the table,
   which is exactly the drift `TestMetricsInventoryComplete` and
   `TestDescRegexpParsesHelp` exist to prevent. Migrating all 43 deletes that
   guarantee for the whole metrics page.
2. **Zero-valued series disappear** — see §8.4. Every counter that has not yet fired
   vanishes from `/metrics` instead of reading `0`.

**Dependency cost** is modest but real, and rubs against the recorded
dependency-minimisation policy: `go.opentelemetry.io/otel/metric` is already in
`go.mod` as *indirect* and `sdk/metric` is already in the module graph, so both come
free. New direct requirements are `go.opentelemetry.io/otel/exporters/prometheus`
(**v0.66.0 — still pre-1.0**, with its own release line), which pulls
`github.com/prometheus/otlptranslator`, plus an OTLP metric exporter when the push
side is switched on. The pre-1.0 bridge is the dependency worth flagging: it is the
component whose name-mangling behaviour the table in §8.4 pins, and a v0.x module is
entitled to change it.

*Decision.* §8.4's "migrate the existing instruments first" is **descoped**, and its
"two registries" trap does not apply — there is one registry either way. Leeway's
instruments are OTEL-native from Phase 2; the 43 existing ones stay Prometheus-native
and reach OTLP only if someone later files the refactor on its own merits. Record
that as a known asymmetry rather than a surprise: until then, an OTLP backend sees
leeway's metrics and not the sentinel's.

---

## 14. Delivery plan

Sized as lookout increments rather than as a standalone product. Phases 1–4 are a
shippable `topology-drift` source; Phase 6 is the `compute-class` source and is
independent of 5.

| Phase | Scope | Exit criteria |
|---|---|---|
| **0 — Spikes** (0.5 wk) | S1–S5 and S9–S11 **done**, S7 **deferred**, S6 **closed unrun** (2026-09-17 — the selector grammar answered it), **S8 resolved 2026-09-21** — the trimmed pod is 12× the assumed size (§6.1, §6.6) and the kwok padding it existed to size is shipped and enforced. **The spike programme is complete** | Package boundaries fixed (done); OTLP scope known (done); §7.7 transition model settled (done); scoring semantics settled (done); §8.4 export defaults measured rather than guessed (done); FR-9 mitigation designed (done); transform registry written (done) |
| **1 — Engine** (2 wks) | `pkg/leeway`: intent model, eligibility, apportionment, scoring. No informers, no source | Property tests green; zero client-go imports, enforced by test |
| **2 — Source skeleton** (2 wks) | `topology-drift` source, **default-on**: delta application, indexes, domain inventory, verifier, metrics on the OTEL API. Shared-transform change per S9, gated on its preserved-field registry | Counters provably correct under property tests at 10k pods; existing source tests still green; **transform attached (done, 2026-09-17)** — `newSharedFactory` is the single construction site and a cache-boundary test fails if the option is dropped; **domain inventory + `Placement` done, 2026-09-17**; **indexes, delta rules and subject resolution done, 2026-09-17** — counters checked against a from-scratch recount after 60k mixed events over 10k pods; **source skeleton, coalescing queue and OTEL instruments done, 2026-09-17** — the source runs against a live informer set and emits nothing, and the exported Prometheus names are pinned against the real exporter; **wired into the sentinel default-on, 2026-09-17** — `--topology-keys` and `--topology-per-domain-series`, no new watch stream and no new grant, and the bridged metrics documented against a real exporter because `MetricsInventory` cannot derive them; **§6.5 verifier done, 2026-09-17** — one shard of subjects rebuilt from the pod cache every 5 minutes, `lookout_leeway_counter_mismatch_total` on disagreement, repaired in place, proven by replaying the 60k-event churn with 1 event in 12 dropped. **Phase 2 complete.** |
| **3 — Intent inference** (2 wks) | TSC, affinity/anti-affinity, node selectors, tolerations, volume pinning, cluster defaults, precedence. **FR-7's node predicate done, 2026-09-18** — `Constraints` is read from an admitted Pod (never a template, per the S3 injection finding) and decides `MatchesSelector`/`Tolerated` for `NodeViews`, so §7.1 eligibility is now real; the §7.7.6 class-pinned arrangement is a test asserting zero drift rather than maximal skew. **COMPLETE 2026-09-19** — FR-4…FR-10 all shipped, ending with the three-state cluster defaults (FR-9) and the policy CRD (FR-10), and both exit criteria are now standing tests: a 22-scenario intent corpus exhaustive per axis, and a false-positive corpus scored end to end through `Resolve` → `Apportion` → `Score` → `Breach`. Nothing emits yet — that is Phase 4, which owns the state machine, dwell, hysteresis, tiers and severity routing | Correct intent on the scenario corpus; false-positive corpus clean |
| **4 — Findings** (2 wks) | State machine, dwell, hysteresis, tiers A/B, transient suppression, severity routing, `pkg/store` persistence. **Verdict layer, §8.2 state machine, §7.6 transient suppression, the v7 `leeway_alert_state` table with §9.3 startup reconcile, §8.5 cause attribution and the finding payload all done as library code, 2026-09-19.** **The scoring pass is now wired into the source, 2026-09-19** — `Source.evaluate` runs §7.1–§7.4 for every *eligible* axis of every subject (not only the declared ones, which would stop measuring the majority of an estate) and publishes the §8.4 score gauges; the per-domain cardinality gate is the real `ρ` floor rather than Phase 2's boolean; and the false-positive corpus now scores through the shipped `ScoreAxis` instead of reassembling the pass itself. **The §8.2 machine is now live in the source, 2026-09-19** — a 30 s alert tick advances every subject-axis against one instant (see §8.2 for why not the evaluation path), episodes persist through the v7 table under the watch process's `--store`, `loadAlertState` restores them for lazy reconcile against the first fresh verdict, and `lookout_leeway_alert_state` exports where each one sits. **§7.6 suppression is now live for the three rows leeway can answer alone, 2026-09-19** — cluster warmup, node drain and domain outage; the inventory samples each domain's ready-node count on a 30 s timer so the outage test has a peak to compare against, cordons are dated from the unschedulable taint, and `lookout_leeway_transient_subjects` reports what is being held back. The zone-outage exit criterion is a standing test at the mechanism level: twenty workloads all skewed by one dead zone open **zero** episodes, and the same skew without an outage still fires. **All five §7.6 rows are now answered, 2026-09-19** — the rollout row consumes `rollout.Source.RollingOut()` through a `RolloutOracle` seam adapted at the composition root (and a deployment without the rollout source says so at startup rather than reporting no rollouts), and the recent-scale row watches `spec.replicas` on Deployments and StatefulSets, stamping only a size that differs from the one already on file so that neither the informer's resync nor a restart reads as a cluster-wide scale event. **Emission shipped, 2026-09-20 — PHASE 4 COMPLETE.** A firing episode builds the §8.5 payload from evidence the source already holds, routes it through §8.3, and reaches the wire as `leeway.contract_violated` or `leeway.placement_drift` (two kinds, not five: `baseline_breach` needs a learned baseline, which is Phase 5, and `domain_unavailable` is Phase 7's source-raised kind). The incident UID is synthetic and carries the axis — `leeway:<subject>|<topology-key>` — because one subject drifting on zone and fine on region is two findings with two dwell timers. Resolution is the clearance observer's job rather than a second signal: §7.4 owns outcome records, `PhaseResolving` reports firing, so §8.2's resolve dwell and `--recovery-stable-for` compose. Tier C is metrics-only unless `--topology-tier-c-signals` says otherwise, and the dwell is `--topology-dwell` | Restart tests pass; zone-outage scenario yields one finding, not four hundred |
| **5 — Baselines** (2 wks) | EWMA/EWMAD, freeze-while-firing, maturity gates, invalidation, Tier C. **The estimator shipped as library code, 2026-09-20** — `leeway.BaselineSet` holds one EWMA share and one EWMAD deviation per domain, matures behind both §7.5 gates, freezes while an episode fires (the clock stops, not just the arithmetic, so a week-long incident does not teach the baseline that the incident is normal), and invalidates on the events §7.5 lists. The warm-up bias is real and corrected: at a 12 h half-life a baseline matures having accumulated ~29 % of its weight, so raw EWMAD understates dispersion ~3.5× — `DevWeight` accumulates `α(1−w)` and `DeviationOf` divides by it. **Persistence shipped, 2026-09-20** — store migration v8 and `leeway_baseline`, written in one batched transaction every 30 s (§9.2's second write policy; alert state stays synchronous), with §9.3 step 7's downtime rules on restore: ≤ 2 h resume, ≤ 24 h widen ×1.5 for one half-life, beyond that mark stale. `DevWeight` persists, because a baseline restored without it would re-acquire the bias it was corrected for. **Wired into the source, 2026-09-20** — a sample timer on its own clock, independent of the evaluation coalescer, so what is learned is a placement's duration and not its edit rate. **Tier C turn-on, 2026-09-20 — PHASE 5 COMPLETE.** A mature baseline renders as an `*Intent` with `Source: LearnedBaseline`, so it reuses apportionment, scoring, the per-domain gauges and routing unchanged; the presence of `Bands` (k·max(deviation, floor)) is what switches the breach rule from ρ to the per-domain band test, and no declared source sets them. An immature baseline is nil, which means "scored against an even apportionment" and not "unmonitored". The finding reaches the wire as `leeway.baseline_breach`, the 52nd kind in the frozen schema, still metrics-only unless `--topology-tier-c-signals` says otherwise. Three flags: `--topology-learn-baselines` (default on), `--topology-baseline-half-life`, `--topology-baseline-band`. **This phase amended §5.1** — see the delta there: `SourceClusterDefaultAssumed` moved to the bottom of the precedence list, without which the whole phase would have shipped inert | Tier C detects injected drift in soak without firing on the FP corpus. **Both halves now hold as standing tests, 2026-09-20**: a Deployment that declared nothing and has always sat evenly across three zones, with all six replicas in one, produces exactly one `leeway.baseline_breach` quoting the *learned* expectation; and all seven §12 fixtures run a second time with their declarations stripped — so the baseline is what scores them — and add no findings at the narrowest (floor-width) band |
| **6 — Preference ranks** (2 wks) | `compute-class` source: dynamic ComputeClass informer, configurable extractors, rank resolution with cross-check, time-weighted pod-seconds, attribution SLIs. **COMPLETE 2026-09-20.** The node model and rule matcher (#457), rank resolution and the pod-second tracker (#458), the ComputeClass reader (#459) and the source itself (#460) shipped counters-first, the same staging Phase 2 used. **Findings shipped 2026-09-20 (#462)**: the four §7.7.4 kinds, judged over a *sampled* window rather than the cumulative counters — the tracker's totals run for the axis's life, so a share taken off them would still be reporting a bad week in March in June; the judge keeps a ring and diffs the ends over `--compute-class-window`. §8.2's machine and `leeway.Reconcile` are reused unchanged, and the persisted rows share the one `leeway_alert_state` table with `topology-drift`, told apart by subject kind at `Load` on both sides. The incident UID prefix is `leeway-rank:` with a hyphen rather than a colon, so the other source's parse fails outright instead of half-succeeding and reporting a rank incident `cleared/object_deleted` on its first sweep. **A class with fewer than two priorities is not judged at all, which takes the wedged rule with it** — deliberate, and confirmed live: the four GKE-managed Autopilot classes each declare exactly one priority, and a Pending pod on one of those is the ordinary out-of-capacity story `capacity` already reports. `wedged_pods` still counts them. One §7.7.4 row was deferred to #463: mean achieved rank against the axis's own EWMA baseline, which needs §7.5's `BaselineSet` plumbed into this source. **Shipped 2026-09-29 (#463)** as rule `rank-baseline` — see the note under §7.7.4's table | Rank shares match a hand-audited sample of a live GKE cluster; unmatched and disagreement rates 0. **Both met on `std-simian-test`, 2026-09-20**: `preference_nodes` read 2 at rank 0 for `n2-preferred` and 1 for `n4-preferred`, which is exactly what `kubectl get nodes -L cloud.google.com/compute-class` shows, and 100 % of pod-seconds on both axes sat at rank 0; `unmatched`, `disagreement`, `no_rule_matching`, `ambiguous`, `off_axis`, `out_of_range`, `axis_invalid` and `tracker_underflows_total` were **all zero** over the run |
| **7 — Nodes** (1.5 wks) | Node-group subjects, capacity weighting, `leeway.domain_unavailable`. **Capacity weighting shipped 2026-09-21 (#468)**: `Weighting` gained an `Auto` zero value so that a declared `Equal` and an undeclared weighting stop being the same thing, every `NodeView` now carries both allocatable dimensions (the trigger has to read CPU to *choose* the weighting, so projecting one resource during eligibility was a chicken-and-egg), and §5.1 resolution writes the resolved weighting back onto the intent with an evidence line quoting the ratio. **This phase amended §7.2** — see the four carve-outs there; the pod-count one was found by breaking two §12 fixtures, and the `minDomains` one would have hidden every padded subject's missing domain. **Node-group subjects shipped 2026-09-21 (#469)**: the inventory groups nodes off FR-3's precedence list, a 30 s pass scores each group against the cluster's domains with no intent (Tier C, deliberately — see FR-3's three rules), and `--topology-max-node-groups` bounds the cardinality by dropping *all* groups rather than an arbitrary subset, with `lookout_leeway_node_groups_discovered` still reporting the true count. Node groups live in their own map inside `State`, absent from `Subjects()`: §6.5's verifier rebuilds each shard from the pod cache, and a subject with no pods would be found empty, called counter drift and erased every sweep. **This shipped a second §7.2 amendment, in the opposite direction to the first** — node-group subjects were to default to `AllocatableCPU` and are instead *pinned* to `Equal`, because a node group's objects are the capacity the weighting apportions over, and the circularity does not only excuse a lopsided pool, it makes an evenly spread pool alongside it read as drifting. **`leeway.domain_unavailable` shipped 2026-09-21 (#467) — PHASE 7 COMPLETE**: the last of §2.3's eight names now has a producer and the v1 ledger is 57 kinds. The subject is the domain, `SubjectDomain`, the one subject kind that holds no objects — which is why the §8.2 machine and the clearance observer took it unchanged but two things around them did not: `judgements` has to append the domain set or `Alerts.Pass` reads every domain episode as gone, and `Clearance` has to skip its pod-index check or it reports a zone that is still down as recovered-`object_deleted` on the first observation. The rule is `Known && usable == 0` against a latch of which domains the cluster has — see §2.3 on why not a window, why *usable* rather than Ready, and why the axis set is configuration | Node-pool imbalance detected and attributed. **The §12 corpus gained a heterogeneous-capacity fixture, 2026-09-21**: ten replicas sitting `[8 1 1]` across zones that are 64, 8 and 8 cores are quiet under the trigger and breach at ρ = 0.4 without it. **The node-group half now holds as a standing test, 2026-09-21**: three zones, a six-node pool wholly in one of them and a balanced pool alongside it — the concentrated pool produces one finding naming the pool (ρ = 2/3, R = 4), the balanced pool scores ρ = 0, and a Deployment pinned to the concentrated pool by `nodeSelector` is narrowed by FR-7 to that one zone and stays quiet rather than restating its pool's imbalance. **The domain half holds as a standing test, 2026-09-21**: three zones and twenty workloads spread over them, every node in one zone lost — **exactly one** `leeway.domain_unavailable` reaches the wire and not one of the twenty workloads emits, because §7.1 drops the lost zone and the survivors are judged among themselves (corrected 2026-10-01: that test removes the nodes, which §7.6 never sees; the NotReady zone is held by §7.6 since the outage row learned to walk §7.1's `Unready` domains); cordoning every node in a zone instead produces the same kind with `taint_exclusion` where deleting them produces `consolidation`; and the zone coming back resolves through the existing clearance observer rather than as a second signal |
| **8 — Hardening** (2 wks) | Cause attribution consuming sibling sources, cardinality controls, OTLP hardening, `cmd/leeway`, docs, dashboards. **§8.4's remaining three cardinality controls shipped 2026-09-22 (#475)** — collapsed states, an axis cap taking a prefix of `--topology-keys` rather than of the drift ranking, and the namespace lists, each independently reversible, with `lookout_leeway_domain_series_withheld{reason}` counting what they drop in subjects rather than folding it into an `other` bucket that would count the same pod on every axis; the 20k-subject budget is now an executable assertion rather than a paragraph. **`cmd/leeway` shipped 2026-09-22 (#477)** — the standalone runs both sources metrics-only and store-less, `/leeway` rides in the same image, and it is built and executed by a Go smoke test so `go test ./...` covers it everywhere. §2.4's two import-graph disciplines stopped being conventions in the same change: `TestLayering_TheImportGraphMatchesTheDesign` enforces the boundary (`pkg/...` never reaches `internal/...`) rather than the one forbidden package, and fails when a rule's pattern matches nothing. Discipline 2's narrow informer interface is deliberately **not** in it — see §2.4. **OTLP hardening shipped 2026-09-22 (#476)** — with the requirement restated: the SDK has no export queue to bound, so the deliverable was proving the structural bound, making the drops legible (five `lookout_otlp_*` SLIs on the *pull* registry, because a drop counter readable only over the broken pipe is useless), and stopping a configuration from reintroducing the hazard (the per-export deadline is now derived from the interval and always strictly below it, with the retry budget bound to the same number). The recording path's inability to block is proved by releasing a wedged exporter only after the recording and the scrape have already finished. **Operator docs and dashboards shipped 2026-09-22 (#478)** — *Operations → Placement* and a Grafana board over the §8.4 SLIs. **The scale + soak harnesses shipped with them (#478)**: `examples/kwok/leeway-scale`, `examples/kwok/soak` and a weekly `Scale (kwok)` workflow, and the ladder they produced is recorded in §6.6 | Scale + soak met on the padded kwok harness — **PARTLY met 2026-09-22.** The memory model is measured and linear over three decades of pod count (§6.6: 63.3 KiB/pod resident over a 52.9 MiB intercept, at 8,814 pods; heap:wire 1.87× against S8's independent 1.73×), evaluation p99 is 5 ms under churn, and every SLI stayed at zero. **The two-hour soak holds it** (§6.6): under four rollout drivers RSS rose 847 MiB/h while the fleet grew 121%, and the marginal resident cost of a fleet object *fell* from 53.4 to 50.5 KiB between the halves of the window — which is also the run that showed the soak had been taking its verdict on raw RSS, the opposite of the test its own prose describes. Two of the three targets were initially **not** met on `kind` and were labelled as extrapolation rather than measurement: the **200k-pod tier** (reached 8,814 on kind — the apiserver was the constraint, not the process) and **500 pods/sec** (sustained 4.6 lifecycles/s), while §6.6.1's per-event decomposition stayed modelled until [#491](https://github.com/go-steer/k8s-lookout/issues/491) added the watch-event counter — **closed 2026-11-18 ([go-steer/k8s-lookout#829](https://github.com/go-steer/k8s-lookout/pull/829), [#831](https://github.com/go-steer/k8s-lookout/pull/831); ported in [#29](https://github.com/go-steer/leeway/issues/29) / [#33](https://github.com/go-steer/leeway/pull/33))**: `examples/kwok/kwokctl-run` (`kwokctl --runtime=binary` + Protobuf clients) directly measured `4,000` nodes / `200,500` S8-padded pods (`5,697.1 MiB` quiet resident = `28.80 KiB/pod`) and `1,578.5 pod lifecycles/s` (`1,893.4` watch events/s, `79.7` events folded per evaluation, `1.384 core` whole process; §6.6, §6.6.1); **`cmd/leeway` built and smoke-tested in CI — met 2026-09-22**; process survives a black-holed OTLP endpoint for 24 h with flat RSS — **met 2026-09-30**: harness, SLIs and tests shipped 2026-09-22, and the 24 h run held heap flat at 7.2 MiB and RSS in a 33–39 MiB band while dropping 34.6 M points (§8.4; #476 closed) |

Roughly 14 weeks, against ~19 for the standalone version — the difference is almost
entirely the plumbing lookout already owns.

**Phase 0 is not optional and not a formality.** S9, S10 and S11 have already earned
it: between them they caught a field selector that would have silently disabled
eviction detection, moved half the §8.2 state machine from "write it" to "implement
one interface", and took a week-long metrics refactor off the critical path by
finding that the pull exporter registers into the registry the sentinel already has.
All three were desk work — a morning's reading apiece, and S11 a short-lived
prototype. **S1 then justified the whole argument for running spikes first**: it took
an afternoon on a live cluster, it invalidated the §7.7.3 transition model outright,
and it turned up an annotation state machine that no amount of desk work would have
produced. Discovering that at week 12 would have meant rebuilding the data model
rather than adjusting it.

**S2 is the counterexample that makes the same point.** It confirmed §7.7.1 exactly
as written and changed no structure — half a day to learn nothing, on the face of it.
But it was the last open question that *could* have inverted rank ordering on every
scored class, and a confirmation is only worthless once you have it. It also caught
the tied-score fallback live, which is a false-positive class §7.7.1 predicted on
paper and nobody had seen.

**S3 is the third shape: the spike that looked like a lookup.** "Confirm two label
keys" reads as desk work you could skip, and skipping it would have shipped a
`Reservation string` that silently conflates same-named reservations in different
projects — a wrong answer on exactly the multi-project clusters least able to notice.
The keys were as predicted; the *arity* was not.

**S8 is the fourth shape, and the only one that made the design wrong rather than
incomplete.** Every other spike confirmed a claim, narrowed one, or deleted work.
S8 took a number the document had carried since the standalone design — the trimmed
pod at ~1.5 KiB — and found it 12× out, which in turn was the difference between
Phase 8's scale gate meaning something and being theatre. It had been sitting at the
bottom of the list as the soft one.

Phases 1–4 are a shippable increment at around week 9: declared and inferred intent,
findings, restart-safe, no baselines and no compute classes.

---

## 15. Open questions

1. ~~Fleet size~~ — **resolved.** 150–200k pods at the high end, 500 pods/sec
   sustained. NFR-1 is set from these; §6.6.1 shows the marginal cost inside the
   sentinel is ~0.013 core. The residual — namespace count — is **withdrawn, not
   pending**: §6.7 shows the scoping decision does not turn on it, and how many
   namespaces a cluster has is a support envelope to declare rather than a number to
   go and find (S6).
2. ~~GKE compute class label keys~~ — **resolved** against `simian-test` on
   2026-09-11. GKE records the achieved priority in the `ccc_priority_index` node
   annotation, so §7.7.2 reads ground truth and demotes inference to fallback plus
   cross-check. Undocumented and unguaranteed; risk accepted, mitigated by the
   fallback path and the attribution SLIs. **S1 closed 2026-09-15** and materially
   revised this: the annotation is not purely numeric and is not immediately
   present — see §7.7.2. **S2 closed the same day** and did not revise it: the field
   stays an index under `priorityScore`, so §7.7.1 stands. **S3 closed 2026-09-16**
   and finished the extractor config: every key in §7.7.2 is now measured, with no
   `UNVERIFIED` marker left. It also widened one — a consumed reservation is
   identified by (project, name), not name — so `NodeProfile.Reservation` became a
   `ReservationRef`. No residual.
3. ~~Scale posture~~ — **resolved 2026-09-15: the higher targets stand, and
   DESIGN §6.2 was edited.** 200k pods and 500 pods/sec are now the repo's stated
   design point, split across the two axes they were always two answers to. See
   §2.5 item 2 and DESIGN §6.2. **Residual closed 2026-09-21 by S8**, which
   remeasured the pod-cache line §6.1's narrowed transform invalidated: 18,630 B per
   trimmed pod, so the shared cache at the 200k design point is ~3.5 GiB and the
   shipped 256Mi default is undersized from ~14k pods. The §6.6 tiers are corrected;
   the sizing consequence is Phase 8's.
4. ~~Should `topology-drift` be enabled by default?~~ — **resolved 2026-09-15:
   yes, default-on.** §8.3 routes Tier C to metrics only, so the default
   deployment adds roughly zero agent sessions, and a drift detector nobody turns
   on detects nothing. The condition attached to it is the one that was always the
   real question: default-on means the §6.1 informer transform reaches *every*
   deployment, and today there is no transform at all (`wiring.go:963` is a bare
   `NewSharedInformerFactory`). **The transform must not ship before S9's remaining
   deliverable** — the preserved-field registry and the test that fails when a field
   is added to the strip list without an entry. Until that lands, the source builds
   default-on but the transform stays off, which costs only the pod-cache memory
   S8 has since measured (25.7% of a pod's heap, 40.6% of its bytes). Both landed in
   Phase 2.
5. **Is fallback depth per-workload or per-class?** §7.7 aggregates per axis by
   default. If two Deployments share a class but only one is falling back, per-axis
   metrics hide it — but per-subject labels multiply cardinality.
6. ~~Cardinality budget~~ — **resolved 2026-09-16 (S5).** The estate is Google
   Managed Prometheus, so there is no ceiling to hit: GMP bills per sample rather
   than rejecting, which for a default-on subsystem is worse, because the failure is
   a growing line item and not a broken scrape. `perDomainSeriesMinDrift: 0.05`
   therefore stays a default rather than a knob — see §8.4. The OTLP half is the
   GKE managed collector, measured, and push does make it bind harder as this item
   said. No residual.
7. ~~Cluster default constraints~~ — **resolved 2026-09-16 (S4): neither of the two
   options this question offered.** Confirmed unreadable on managed GKE, so FR-9 is a
   documented manual sync — but the answer is not a widened Tier A threshold either.
   Default-derived intent is **capped below Tier A entirely**, because the upstream
   defaults are `ScheduleAnyway` and a soft preference is not a contract to violate.
   The residual risk is precision, not correctness, and it is bounded further by the
   fact that the defaults apply only to Pods that declare no spread of their own. See
   §13 S4 for the three-state config and the `cluster-default-assumed` provenance
   label. No residual.
8. **Karpenter consolidation.** Consolidation deliberately packs pods and produces
   drift by design. Default Karpenter-managed pools to `Ignore` on zone, or alert with
   consolidation as an annotated suspected cause? Leaning the latter — intentional and
   safe are not the same thing.
   **Resolved 2026-09-22 (#474): the latter, and with no Karpenter-specific
   configuration at all.** A consolidation is recognised from the disruption taint
   the autoscaler puts on the node it is removing, so it is a `suspectedCause:
   consolidation` on the ordinary finding rather than a pool-level `Ignore` — see
   §8.5. Defaulting managed pools to `Ignore` would have required knowing which
   pools an autoscaler owns *and* would have silenced the imbalances it did not
   cause. No residual.
9. **Descheduler integration.** Emitting a
   `RemovePodsViolatingTopologySpreadConstraint` hint is a natural v2 and sits
   uncomfortably against lookout's read-only posture. Worth reserving surface now?
10. **Region-level topology.** Multi-region clusters are rare but the model supports
    them. In scope for the target fleet, or is zone the only axis that matters?

---
