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

"""Summarize a leeway false-positive soak directory.

Every `leeway watch` finding is listed once, with what is needed to judge it:
where it reached (`POST /incidents`, `POST /incidents/<id>/events`, or the
watcher stderr log), its tier and suspected cause, the churn in the 30 minutes
before it was first seen, and the workload's per-zone placement at the nearest
sample. Point-in-time `leeway report` snapshots (which bypass dwell) and
Prometheus `/metrics` scrapes are summarized alongside for context.

Usage: summarize.py <soak dir>
"""

import collections
import datetime as dt
import json
import os
import re
import sys

WINDOW = dt.timedelta(minutes=30)
LEEWAY = re.compile(r"leeway[.:]")

WIRE_LINE = re.compile(
    r"^(\S+)\s+(INCIDENT-OPEN|INCIDENT-EVENT|INJECT)\s+(?:id|sid)=(\S+)\s+kind=(\S*)\s+token=\S+\s+body=(.*)$"
)
REJECT_LINE = re.compile(
    r"^(\S+)\s+REJECT\s+(?:id|sid)=(\S+)\s+bytes=(\d+)\s+limit=(\d+)\s+kind_rejected=(\S*)"
)
WATCHER_EMIT_LINE = re.compile(
    r"^leeway:\s+(critical|warning|info)\s+(leeway\.\S+)\s+(\S+):\s+(.*)$"
)
WATCHER_SINK_ERR = re.compile(r"^leeway:\s+sink\s+.*error")


def ts(s):
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y/%m/%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def normalize_payload(obj):
    """Extract normalized leeway finding fields from an inject.Payload or raw Finding."""
    if not isinstance(obj, dict):
        return []
    # Core-agent session envelope wraps the payload JSON as a string under "message".
    if isinstance(obj.get("message"), str) and obj["message"].strip().startswith("{"):
        try:
            obj = json.loads(obj["message"])
        except ValueError:
            return []
    if not isinstance(obj, dict):
        return []

    kind = obj.get("kind", "")
    if not isinstance(kind, str) or not kind.startswith("leeway."):
        return []

    inner = obj.get("leeway_finding") or obj.get("leeway_rank_finding")
    merged = dict(obj)
    if isinstance(inner, dict):
        merged.update(inner)
    subj = merged.get("subject") if isinstance(merged.get("subject"), dict) else {}
    ns = obj.get("namespace") or subj.get("namespace") or ""
    name = obj.get("name") or subj.get("name") or merged.get("computeClass") or ""
    topo_key = merged.get("topologyKey") or ""
    if not topo_key and isinstance(obj.get("uid"), str) and "|" in obj["uid"]:
        topo_key = obj["uid"].partition("|")[2]

    intent = merged.get("intent") if isinstance(merged.get("intent"), dict) else {}
    score = merged.get("score") if isinstance(merged.get("score"), dict) else {}

    return [
        {
            "kind": kind,
            "namespace": ns,
            "name": name,
            "topologyKey": topo_key,
            "tier": merged.get("tier"),
            "severity": merged.get("severity") or obj.get("severity"),
            "suspectedCause": merged.get("suspectedCause") or obj.get("reason"),
            "intentSource": intent.get("source"),
            "source": obj.get("source"),
            "drift": score.get("drift") if "drift" in score else merged.get("drift"),
            "transient": merged.get("transient"),
        }
    ]


def wire_findings(path):
    out = []
    resolved = 0
    rejected = 0
    rejected_kinds = set()
    if not os.path.exists(path):
        return out, resolved, rejected, rejected_kinds
    for line in open(path, errors="replace"):
        rm = REJECT_LINE.match(line)
        if rm:
            rkind = rm.group(5) or ""
            if rkind.startswith("leeway.") or LEEWAY.search(line):
                rejected += 1
                if rkind.startswith("leeway."):
                    rejected_kinds.add(rkind)
            continue
        m = WIRE_LINE.match(line)
        if not m:
            continue
        when, op, inc_id, inject_kind, body = (
            ts(m.group(1)),
            m.group(2),
            m.group(3),
            m.group(4),
            m.group(5),
        )
        if inject_kind in ("resolved", "resolved.reverted"):
            if LEEWAY.search(body):
                resolved += 1
            continue
        if not LEEWAY.search(body):
            continue
        try:
            parsed = json.loads(body)
        except ValueError:
            continue
        route = "webhook" if op.startswith("INCIDENT-") else "session"
        for f in normalize_payload(parsed):
            out.append((when, route, inc_id, f))
    return out, resolved, rejected, rejected_kinds


def parse_subject_ref(ref):
    """Parse emit.go subjectOf output: '<ns>/<kind>/<name>' or '<kind>/<name>'."""
    parts = ref.split("/")
    if len(parts) >= 3:
        return parts[0], parts[2]
    if len(parts) == 2:
        return "", parts[1]
    return "", ref


def watcher_log_findings(soak, wire_keys):
    """Collect any `leeway: <sev> leeway.<kind> <subject>: ...` lines that never
    reached wire.log, plus count any sink delivery errors."""
    unseen = collections.Counter()
    sink_errors = 0
    seen_lines = set()
    for name in ("watcher.log", "watcher-final.log", "sentinel.log", "sentinel-final.log"):
        p = os.path.join(soak, name)
        if not os.path.exists(p):
            continue
        for raw in open(p, errors="replace"):
            line = raw.strip()
            if not line or line in seen_lines:
                continue
            seen_lines.add(line)
            if WATCHER_SINK_ERR.match(line):
                sink_errors += 1
                continue
            m = WATCHER_EMIT_LINE.match(line)
            if not m:
                continue
            sev, kind, subj_ref, _msg = m.groups()
            ns, obj_name = parse_subject_ref(subj_ref)
            if (kind, ns, obj_name) not in wire_keys:
                unseen[(sev, kind, ns, obj_name)] += 1
    return unseen, sink_errors


def report_snapshots(soak):
    """Summarize periodic `leeway report` snapshots (dwell bypassed)."""
    snaps = 0
    with_findings = 0
    by_kind = collections.Counter()
    p = os.path.join(soak, "reports.jsonl")
    if os.path.exists(p):
        for line in open(p, errors="replace"):
            parts = line.strip().split(" ", 1)
            if len(parts) != 2:
                continue
            try:
                doc = json.loads(parts[1])
            except ValueError:
                continue
            snaps += 1
            n_f = 0
            for cl in doc.get("clusters") or []:
                for f in (cl.get("topology_findings") or []) + (cl.get("rank_findings") or []):
                    n_f += 1
                    by_kind[f.get("kind", "?")] += 1
            if n_f > 0:
                with_findings += 1

    final_findings = None
    fp = os.path.join(soak, "report-final.json")
    if os.path.exists(fp):
        try:
            doc = json.load(open(fp))
            final_findings = []
            for cl in doc.get("clusters") or []:
                for f in (cl.get("topology_findings") or []) + (cl.get("rank_findings") or []):
                    subj = f.get("subject") or {}
                    final_findings.append(
                        "%s %s/%s (%s)"
                        % (
                            f.get("kind", "?"),
                            subj.get("namespace", ""),
                            subj.get("name") or f.get("computeClass", "?"),
                            f.get("suspectedCause") or f.get("severity", ""),
                        )
                    )
        except (ValueError, OSError):
            pass
    return snaps, with_findings, by_kind, final_findings


def churn(soak):
    acts = []
    p = os.path.join(soak, "churn.log")
    if os.path.exists(p):
        for line in open(p):
            parts = line.split()
            if parts:
                fields = dict(x.split("=", 1) for x in parts[1:] if "=" in x)
                acts.append((ts(parts[0]), fields))
    return acts


def placements(soak):
    """[(time, {node: zone}, [(ns, pod, node, phase, owner)])]"""
    p = os.path.join(soak, "placement.log")
    snaps, cur = [], None
    if not os.path.exists(p):
        return snaps
    section = None
    for line in open(p, errors="replace"):
        line = line.rstrip("\n")
        if line.startswith("### "):
            cur = (ts(line[4:]), {}, [])
            snaps.append(cur)
            section = "nodes"
        elif line == "## pods":
            section = "pods"
        elif cur and line.strip():
            f = line.split()
            if section == "nodes" and len(f) >= 2:
                cur[1][f[0]] = f[1] if f[1] != "<none>" else "-"
            elif section == "pods" and len(f) >= 5:
                cur[2].append(tuple(f[:5]))
    return snaps


def owned_by(owner, name):
    return owner == name or owner.rsplit("-", 1)[0] == name


def zone_counts(snaps, when, ns, name):
    best = None
    for s in snaps:
        if s[0] and when and s[0] <= when:
            best = s
    if best is None:
        return "no sample yet"
    counts = collections.Counter()
    for pns, _pod, node, phase, owner in best[2]:
        if pns == ns and owned_by(owner, name):
            counts[best[1].get(node, "unscheduled") if phase == "Running" else phase] += 1
    return "%s @%s" % (dict(sorted(counts.items())), best[0].strftime("%H:%M"))


def detect_build(soak, env):
    if env.get("build") and env["build"] != "?":
        return env["build"]
    mp = os.path.join(soak, "metrics.log")
    if os.path.exists(mp):
        for line in open(mp, errors="replace"):
            m = re.search(r'lookout_leeway_standalone_info\{[^}]*version="([^"]+)"', line)
            if m:
                return "leeway " + m.group(1)
    for name in ("watcher-startup.log", "watcher.log", "sentinel-startup.log", "sentinel.log"):
        p = os.path.join(soak, name)
        if os.path.exists(p):
            for line in open(p, errors="replace"):
                m = re.search(r"\b(leeway v\S+)", line)
                if m:
                    return m.group(1)
    return "?"


def main():
    soak = sys.argv[1]
    env = {}
    p = os.path.join(soak, "run.env")
    if os.path.exists(p):
        env = dict(line.strip().split("=", 1) for line in open(p) if "=" in line)
    acts = churn(soak)
    snaps = placements(soak)
    wire, resolved, rejected, rejected_kinds = wire_findings(os.path.join(soak, "wire", "wire.log"))

    print("leeway false-positive soak — %s" % soak)
    for k in ("start", "planned_end", "actual_end", "duration", "image", "seed", "churn_scale"):
        print("  %-12s %s" % (k, env.get(k, "?")))
    print("  %-12s %s" % ("build", detect_build(soak, env)))
    tally = collections.Counter(a[1].get("action") for a in acts)
    print("  churn        %s" % ", ".join("%s=%d" % kv for kv in sorted(tally.items())))
    print("  samples      %d placement snapshots" % len(snaps))
    print()

    unique = collections.OrderedDict()
    wire_subject_keys = set()
    for when, route, inc_id, f in wire:
        ns = f["namespace"]
        name = f["name"]
        key = (f["kind"], ns, name, str(f["topologyKey"] or ""), route)
        wire_subject_keys.add((f["kind"], ns, name))
        if key not in unique:
            unique[key] = dict(first=when, last=when, n=0, id=inc_id, f=f)
        u = unique[key]
        u["last"], u["n"] = when, u["n"] + 1

    log_only, sink_errors = watcher_log_findings(soak, wire_subject_keys)

    print(
        "== webhook findings (POST /incidents & /events): %d unique, %d deliveries; %d resolves, %d rejected"
        % (len(unique), len(wire), resolved, rejected)
    )
    for (kind, ns, name, key, route), u in unique.items():
        f = u["f"]
        print(
            "- %s %s %s/%s key=%s route=%s tier=%s sev=%s cause=%s intent=%s drift=%s transient=%s"
            % (
                u["first"],
                kind,
                ns,
                name,
                key,
                route,
                f.get("tier"),
                f.get("severity"),
                f.get("suspectedCause"),
                f.get("intentSource") or f.get("source"),
                f.get("drift"),
                f.get("transient"),
            )
        )
        print("    seen %d× until %s (id %s)" % (u["n"], u["last"], u["id"]))
        print("    placement %s" % zone_counts(snaps, u["first"], ns, name))
        for when, a in acts:
            if when and u["first"] and u["first"] - WINDOW <= when <= u["first"]:
                if a.get("node") or (a.get("target", "").split("/")[-1] == name):
                    print(
                        "    churn %s %s"
                        % (when.strftime("%H:%M:%S"), " ".join("%s=%s" % kv for kv in a.items()))
                    )
    print()

    print(
        "== watcher log-only findings (emitted without reaching webhook): %d (sink errors: %d)"
        % (len(log_only), sink_errors)
    )
    for (sev, kind, ns, name), count in log_only.most_common():
        print("- %s %s %s/%s ×%d" % (sev, kind, ns, name, count))
    print()

    rep_snaps, rep_with, rep_kinds, rep_final = report_snapshots(soak)
    if rep_snaps > 0 or rep_final is not None:
        print(
            "== one-shot `leeway report` snapshots (dwell bypassed): %d/%d samples saw point-in-time skew"
            % (rep_with, rep_snaps)
        )
        if rep_kinds:
            print("  sampled kinds: %s" % ", ".join("%s=%d" % kv for kv in rep_kinds.most_common()))
        if rep_final is not None:
            print("  final report:  %d finding(s)%s" % (
                len(rep_final),
                " — " + "; ".join(rep_final[:5]) if rep_final else "",
            ))
        print()

    # Metrics: what never reached the wire but was scored.
    p = os.path.join(soak, "metrics.log")
    if os.path.exists(p):
        alert, drift, last = collections.Counter(), {}, {}
        scrapes = 0
        for line in open(p, errors="replace"):
            if line.startswith("### "):
                scrapes += 1
                continue
            m = re.match(r"(\w+)(\{[^}]*\})? (\S+)", line)
            if not m:
                continue
            name, labels, val = m.group(1), m.group(2) or "", float(m.group(3))
            last[name + labels] = val
            if name == "lookout_leeway_alert_state" and val > 0:
                alert[labels] += 1
            if name == "lookout_leeway_drift":
                drift[labels] = max(drift.get(labels, 0), val)
        print("== metrics: %d scrapes" % scrapes)
        print("  alert_state>0 (series × scrapes): %d series" % len(alert))
        for labels, c in alert.most_common(15):
            print("    %3d× %s" % (c, labels))
        print("  highest max drift:")
        for labels, v in sorted(drift.items(), key=lambda kv: -kv[1])[:10]:
            print("    %.3f %s" % (v, labels))
        for k in sorted(last):
            if k.startswith(
                (
                    "lookout_leeway_standalone_signals_total",
                    "lookout_leeway_domains_unavailable",
                    "lookout_leeway_baselines",
                    "lookout_leeway_baseline_mature",
                    "process_resident_memory_bytes",
                    "go_memstats_heap_alloc_bytes",
                    "go_memstats_heap_inuse_bytes",
                )
            ):
                print("  last %s %g" % (k, last[k]))

    covered_kinds = {k for (k, _ns, _n) in wire_subject_keys} | {
        k for (_sev, k, _ns, _n) in log_only
    }
    unaccounted_rejected = len(rejected_kinds - covered_kinds)
    if rejected > 0 and not rejected_kinds and not unique and not log_only:
        unaccounted_rejected = 1
    total = len(unique) + len(log_only) + unaccounted_rejected
    print()
    print(
        "VERDICT: %d distinct leeway findings (%d webhook, %d log-only%s)"
        % (
            total,
            len(unique),
            len(log_only),
            ", %d rejected deliveries" % rejected if rejected else "",
        )
    )


if __name__ == "__main__":
    main()
