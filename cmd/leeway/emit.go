// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-steer/leeway/internal/version"
	"github.com/go-steer/leeway/pkg/engine"
	"github.com/go-steer/leeway/pkg/inject"
	"github.com/go-steer/leeway/pkg/sources"
)

const (
	defaultDedupWindow       = 15 * time.Minute
	defaultRecoveryStableFor = 2 * time.Minute
	defaultRecoveryTick      = 15 * time.Second
)

// emitterConfig configures incident dispatch for one watched cluster.
type emitterConfig struct {
	Cluster           string
	Project           string
	Region            string
	Zone              string
	Sink              inject.Sink
	DryRun            bool
	DedupWindow       time.Duration
	RecoveryStableFor time.Duration
}

// emitter handles signals raised by leeway sources: increments Prometheus
// counters, logs a human-readable line to stderr, and optionally delivers
// incidents and recovery resolutions to a configured inject.Sink (such as the
// HTTP webhook sink).
type emitter struct {
	signals  *prometheus.CounterVec
	log      io.Writer
	cfg      emitterConfig
	dedup    *engine.DedupCache
	recovery *engine.RecoveryTracker
}

func newEmitter(reg prometheus.Registerer, log io.Writer, cfg ...emitterConfig) (*emitter, error) {
	var c emitterConfig
	if len(cfg) > 0 {
		c = cfg[0]
	}
	if c.DedupWindow <= 0 {
		c.DedupWindow = defaultDedupWindow
	}
	if c.RecoveryStableFor <= 0 {
		c.RecoveryStableFor = defaultRecoveryStableFor
	}

	e := &emitter{
		log: log,
		cfg: c,
		signals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lookout_leeway_standalone_signals_total",
			Help: "Total findings the leeway sources raised in this standalone process, by source, kind and severity.",
		}, []string{"source", "kind", "severity"}),
	}
	info := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lookout_leeway_standalone_info",
		Help: "Always 1, labelled with the version of the standalone leeway binary exporting this endpoint.",
	}, []string{"version"})

	if reg != nil {
		if err := reg.Register(e.signals); err != nil {
			var are prometheus.AlreadyRegisteredError
			if errors.As(err, &are) {
				e.signals = are.ExistingCollector.(*prometheus.CounterVec)
			} else {
				return nil, fmt.Errorf("registering the signals counter: %w", err)
			}
		}
		if err := reg.Register(info); err != nil {
			var are prometheus.AlreadyRegisteredError
			if !errors.As(err, &are) {
				return nil, fmt.Errorf("registering the info gauge: %w", err)
			}
		} else {
			info.WithLabelValues(version.Semver()).Set(1)
		}
	}

	dedup, err := engine.NewDedupCache(c.DedupWindow, "")
	if err != nil {
		return nil, err
	}
	e.dedup = dedup

	e.recovery = engine.NewRecoveryTracker(c.RecoveryStableFor, func(sig engine.Signal) {
		e.dispatchResolved(context.Background(), sig)
	})
	e.dedup.SetOpenCheck(e.recovery.Open)

	return e, nil
}

// registerObservers attaches clearance observers from sources that implement
// ClearanceObserver() so that recovery tracking can emit kind=resolved when a
// placement or rank episode clears.
func (e *emitter) registerObservers(srcs []sources.Source) {
	if e.recovery == nil {
		return
	}
	type clearanceProvider interface {
		ClearanceObserver() engine.ClearanceObserver
	}
	for _, src := range srcs {
		if cp, ok := src.(clearanceProvider); ok {
			if obs := cp.ClearanceObserver(); obs != nil {
				e.recovery.AddObserver(obs)
			}
		} else if obs, ok := src.(engine.ClearanceObserver); ok {
			e.recovery.AddObserver(obs)
		}
	}
}

// runRecoveryLoop periodically ticks the recovery tracker while ctx is active.
func (e *emitter) runRecoveryLoop(ctx context.Context, interval time.Duration) {
	if e.recovery == nil || (e.cfg.Sink == nil && !e.cfg.DryRun) {
		return
	}
	if interval <= 0 {
		interval = defaultRecoveryTick
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.recovery.Tick()
		}
	}
}

// emit is the sources' emit callback.
func (e *emitter) emit(sig sources.Signal) {
	e.signals.WithLabelValues(sig.Source, sig.Kind, string(sig.Severity)).Inc()
	fmt.Fprintf(e.log, "leeway: %s %s %s: %s\n",
		sig.Severity, sig.Kind, subjectOf(sig), oneLine(sig.Message))

	if e.cfg.Sink != nil || e.cfg.DryRun {
		e.dispatch(context.Background(), sig)
	}
}

func (e *emitter) dispatch(ctx context.Context, sig sources.Signal) {
	if sig.Cluster == "" {
		sig.Cluster = e.cfg.Cluster
	}
	if sig.Project == "" {
		sig.Project = e.cfg.Project
	}
	if sig.Region == "" {
		sig.Region = e.cfg.Region
	}
	if sig.Zone == "" {
		sig.Zone = e.cfg.Zone
	}
	if sig.Fingerprint == "" {
		sig.Fingerprint = engine.Fingerprint(
			sig.Kind,
			engine.CanonicalReason(sig.Key.Reason),
			sig.KindOfObject,
			engine.FailureDomain(sig.Region, sig.Zone),
		)
	}

	res := e.dedup.ObserveSignal(sig.Key, engine.Observation{
		LastTS:     sig.LastSeen,
		Kind:       sig.Kind,
		Severity:   sig.Severity,
		Message:    sig.Message,
		Repeatable: true,
	})

	switch res.Kind {
	case engine.DedupNewIncident:
		e.dedup.NoteIncidentKind(sig.Key, sig.Kind)
		payload := buildInjectPayload(sig, 1, 0)
		if e.cfg.DryRun {
			raw, _ := json.Marshal(payload)
			id := "dryrun-" + sig.Key.UID
			fmt.Fprintf(e.log, "leeway: dry-run OpenIncident: %s\n", string(raw))
			e.dedup.BindIncident(sig.Key, id, sig.IncidentRef())
			e.recovery.Track(engine.Incident{
				Key:       sig.Key,
				SessionID: id,
				FirstSeen: sig.FirstSeen,
				Ref:       sig.IncidentRef(),
			})
			return
		}
		id, err := e.cfg.Sink.OpenIncident(ctx, payload)
		if err != nil {
			fmt.Fprintf(e.log, "leeway: sink OpenIncident error for %s: %v\n", subjectOf(sig), err)
			if id == "" {
				e.dedup.MarkUndelivered(sig.Key, engine.RepeatChangeOpen)
				return
			}
		}
		e.dedup.BindIncident(sig.Key, id, sig.IncidentRef())
		e.recovery.Track(engine.Incident{
			Key:       sig.Key,
			SessionID: id,
			FirstSeen: sig.FirstSeen,
			Ref:       sig.IncidentRef(),
		})

	case engine.DedupRepeat:
		payload := buildInjectPayload(sig, res.Count, res.RepeatCount)
		if e.cfg.DryRun {
			raw, _ := json.Marshal(payload)
			fmt.Fprintf(e.log, "leeway: dry-run Append(%s): %s\n", res.SessionID, string(raw))
			return
		}
		if err := e.cfg.Sink.Append(ctx, res.SessionID, payload); err != nil {
			fmt.Fprintf(e.log, "leeway: sink Append error for %s (incident %s): %v\n", subjectOf(sig), res.SessionID, err)
			var se *inject.StatusError
			if errors.As(err, &se) && (se.Code == 404 || se.Code == 410) {
				e.dedup.Unbind(sig.Key)
			} else {
				e.dedup.MarkUndelivered(sig.Key, res.Change)
			}
		}
	}
}

func (e *emitter) dispatchResolved(ctx context.Context, sig engine.Signal) {
	if sig.Recovery == nil {
		return
	}
	sessionID, ok := e.dedup.LookupSession(sig.Key)
	if !ok || sessionID == "" {
		return
	}
	if sig.Kind == engine.KindResolved {
		e.dedup.MarkResolved(sig.Key)
	}
	payload := inject.ResolvedPayload{
		Kind:              sig.Kind,
		Reason:            engine.CanonicalReason(sig.Key.Reason),
		Namespace:         sig.Namespace,
		KindOfObject:      sig.KindOfObject,
		Name:              sig.Name,
		Container:         sig.Container,
		UID:               sig.Key.UID,
		Fingerprint:       sig.Fingerprint,
		Cluster:           sig.Cluster,
		FirstSeen:         sig.FirstSeen,
		ResolvedAt:        sig.Recovery.ResolvedAt,
		ClearedAfter:      sig.Recovery.ClearedAfter.String(),
		ObservedStableFor: sig.Recovery.ObservedStableFor.String(),
		Resolution:        string(sig.Recovery.Resolution),
		Context: inject.PayloadContext{
			ControllerRef: sig.ControllerRef,
		},
	}
	if sig.Recovery.RevertedAfter > 0 {
		payload.RevertedAfter = sig.Recovery.RevertedAfter.String()
	}

	if e.cfg.DryRun {
		raw, _ := json.Marshal(payload)
		fmt.Fprintf(e.log, "leeway: dry-run Append(%s) resolved: %s\n", sessionID, string(raw))
		return
	}
	if err := e.cfg.Sink.Append(ctx, sessionID, payload); err != nil {
		fmt.Fprintf(e.log, "leeway: sink Append resolved error for %s (incident %s): %v\n", sig.Name, sessionID, err)
		if sig.Kind == engine.KindResolved {
			e.recovery.RequeueResolved(sig.Key)
		}
	}
}

func buildInjectPayload(sig sources.Signal, count, repeatCount int) inject.Payload {
	if count <= 0 {
		count = sig.Count
		if count <= 0 {
			count = 1
		}
	}
	return inject.Payload{
		Kind:         sig.Kind,
		Reason:       sig.Key.Reason,
		Namespace:    sig.Namespace,
		KindOfObject: sig.KindOfObject,
		Name:         sig.Name,
		Container:    sig.Container,
		UID:          sig.Key.UID,
		Message:      sig.Message,
		Count:        count,
		FirstSeen:    sig.FirstSeen,
		LastSeen:     sig.LastSeen,
		Cluster:      sig.Cluster,
		Project:      sig.Project,
		Region:       sig.Region,
		Zone:         sig.Zone,
		Source:       sig.Source,
		Severity:     string(sig.Severity),
		Fingerprint:  sig.Fingerprint,
		Context: inject.PayloadContext{
			ControllerRef: sig.ControllerRef,
			Node:          sig.Node,
			Labels:        sig.Labels,
		},
		Type:              sig.Type,
		ReasonClass:       engine.ReasonClass(sig.Kind, sig.Key.Reason, sig.Message),
		RepeatCount:       repeatCount,
		LeewayFinding:     sig.LeewayFinding,
		LeewayRankFinding: sig.LeewayRankFinding,
	}
}

func subjectOf(sig sources.Signal) string {
	kind := sig.KindOfObject
	if kind == "" {
		kind = "object"
	}
	if sig.Namespace == "" {
		return fmt.Sprintf("%s/%s", strings.ToLower(kind), sig.Name)
	}
	return fmt.Sprintf("%s/%s/%s", sig.Namespace, strings.ToLower(kind), sig.Name)
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}
