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

package leeway

import (
	"math"
	"sort"
)

// Uncapped is the DomainCaps entry meaning "this domain has no ceiling". Any
// negative value is treated the same way; the constant exists so callers can
// say what they mean.
const Uncapped int64 = -1

// Apportionment is the result of distributing n objects across domains.
type Apportionment struct {
	// Expected[i] is the number of objects domain i should hold. It is always
	// non-negative, and sums to Placed.
	Expected []int64

	// Placed is the number of objects the apportionment could actually place,
	// which is min(n, Σcaps). It equals n unless the caps cannot absorb the
	// whole workload.
	Placed int64

	// Unplaceable is n − Placed: objects that no eligible domain has room
	// for. Non-zero means the cluster cannot satisfy the workload at all, and
	// that is a different finding from drift — scoring against an expectation
	// that silently dropped them would blame placement for a capacity
	// shortfall.
	Unplaceable int64

	// Saturated[i] reports whether domain i was clamped to its cap. A
	// saturated domain raises the minimum achievable skew, which is why
	// §7.3's S* is read off the expectation rather than from n mod m.
	Saturated []bool
}

// Apportion distributes n objects across len(weights) domains by the largest
// remainder (Hamilton) method, then applies caps by water-filling.
//
// Integer apportionment is the point. The obvious thing — compare actual
// counts against n·pᵢ — manufactures drift out of nothing, because n·pᵢ is
// usually not a whole number and no placement can ever equal it. Three pods
// over two zones is [2,1], and calling that a 0.5-pod deviation from [1.5,1.5]
// would have every odd-replica workload permanently drifting.
//
// caps may be nil (no domain capped) or the same length as weights; entries
// that are negative mean uncapped. Weights that are negative, NaN or infinite
// are treated as zero, so a bad capacity reading degrades that domain to "no
// expected share" instead of poisoning every other domain's arithmetic.
//
// Determinism: fractional-remainder ties are broken by ascending domain index,
// never by map order. Callers must therefore present domains in the canonical
// order from SortDomains. Callers that already hold an observed placement
// should call ApportionAligned so equal-remainder ties align with actual counts
// rather than charging relocation distance for an arbitrary domain-name order.
func Apportion(n int64, weights []float64, caps []int64) Apportionment {
	return apportion(n, weights, caps, nil)
}

// ApportionAligned is Apportion with fractional-remainder ties broken toward
// domains holding more objects in actual (and then by ascending domain index).
//
// Why this exists: when 2 pods sit across 3 equal zones as [0,1,1] or 4 pods
// sit as [1,1,2], every permutation of that multiset is an equally valid
// Hamilton apportionment with the exact same MinAchievableSkew (S* = 1).
// Breaking ties by domain index alone picks [1,1,0] and [2,1,1], charging
// [0,1,1] with R = 1 (ρ = 0.50) and [1,1,2] with R = 1 (ρ = 0.25) — and on 4
// zones, [1,1,2,2] with R = 2 (ρ = 0.333), breaching even DefaultThresholds().
// Aligning tied remainders at the cutoff against actual selects the valid
// Hamilton expectation at minimum L1 distance from actual while preserving
// Placed, Unplaceable, and MinAchievableSkew.
func ApportionAligned(n int64, weights []float64, caps []int64, actual []int64) Apportionment {
	if len(actual) != len(weights) {
		actual = nil
	}
	return apportion(n, weights, caps, actual)
}

func apportion(n int64, weights []float64, caps []int64, actual []int64) Apportionment {
	m := len(weights)
	res := Apportionment{
		Expected:  make([]int64, m),
		Saturated: make([]bool, m),
	}
	if m == 0 || n <= 0 {
		if n > 0 {
			res.Unplaceable = n
		}
		return res
	}

	clean := sanitiseWeights(weights)

	// active[i] is true while domain i is still absorbing the remaining
	// budget. Water-filling freezes a domain the round it hits its cap and
	// re-apportions the overflow across whatever is left.
	active := make([]bool, m)
	for i := range active {
		active[i] = true
	}
	budget := n

	for {
		idx := make([]int, 0, m)
		w := make([]float64, 0, m)
		var act []int64
		if actual != nil {
			act = make([]int64, 0, m)
		}
		for i := 0; i < m; i++ {
			if active[i] {
				idx = append(idx, i)
				w = append(w, clean[i])
				if actual != nil {
					act = append(act, actual[i])
				}
			}
		}
		if len(idx) == 0 {
			// Everything is capped and there is still budget left. This is
			// the capacity-shortfall case, reported rather than absorbed.
			res.Unplaceable = budget
			break
		}

		share := hamiltonAligned(budget, w, act)

		froze := false
		for j, i := range idx {
			want := share[j]
			if caps != nil && caps[i] >= 0 && want > caps[i] {
				res.Expected[i] = caps[i]
				res.Saturated[i] = true
				active[i] = false
				budget -= caps[i]
				froze = true
			}
		}
		if !froze {
			for j, i := range idx {
				res.Expected[i] = share[j]
			}
			break
		}
	}

	for _, e := range res.Expected {
		res.Placed += e
	}
	return res
}

// remTieEps bounds floating-point noise when comparing identical fractional
// remainders derived from equal capacity weights (for example, identical
// millicore totals summed across nodes).
const remTieEps = 1e-12

// hamilton apportions n across weights by largest remainder. It assumes
// weights are already sanitised and len(weights) > 0.
//
// The returned slice always sums to exactly n. That holds because Σqᵢ == n, so
// Σfloor(qᵢ) ≤ n, and the deficit is strictly less than len(weights) — every
// unit of it is handed out below. Float error in qᵢ cannot break the sum: it
// can only move a unit between the floor phase and the remainder phase.
func hamilton(n int64, weights []float64) []int64 {
	return hamiltonAligned(n, weights, nil)
}

func hamiltonAligned(n int64, weights []float64, actual []int64) []int64 {
	m := len(weights)
	out := make([]int64, m)
	if n <= 0 {
		return out
	}

	var sum float64
	for _, w := range weights {
		sum += w
	}
	if sum <= 0 {
		// Every weight is zero (or the domains were all invalid). Falling
		// back to equal shares keeps the total correct and is the only
		// defensible reading: we know nothing that distinguishes the domains.
		eq := make([]float64, m)
		for i := range eq {
			eq[i] = 1
		}
		weights, sum = eq, float64(m)
	}

	type frac struct {
		idx int
		rem float64
	}
	rems := make([]frac, m)
	var allocated int64
	for i, w := range weights {
		q := float64(n) * w / sum
		f := math.Floor(q)
		out[i] = int64(f)
		allocated += out[i]
		rems[i] = frac{idx: i, rem: q - f}
	}

	deficit := n - allocated
	if deficit <= 0 {
		return out
	}

	// Largest remainder first; ties by ascending index so the result does not
	// depend on sort stability or on the caller's map ordering.
	sort.Slice(rems, func(a, b int) bool {
		if rems[a].rem != rems[b].rem {
			return rems[a].rem > rems[b].rem
		}
		return rems[a].idx < rems[b].idx
	})

	// When actual counts are supplied and the deficit cutoff splits a band of
	// tied fractional remainders, award those tied slots to the domains with
	// higher actual counts first (breaking remaining ties by ascending index).
	if len(actual) == m && int(deficit) < m {
		cutoff := rems[int(deficit)-1].rem
		bandStart := int(deficit) - 1
		for bandStart > 0 && math.Abs(rems[bandStart-1].rem-cutoff) <= remTieEps {
			bandStart--
		}
		bandEnd := int(deficit)
		for bandEnd < m && math.Abs(rems[bandEnd].rem-cutoff) <= remTieEps {
			bandEnd++
		}
		if bandEnd > int(deficit) {
			band := rems[bandStart:bandEnd]
			sort.Slice(band, func(a, b int) bool {
				aa, ab := actual[band[a].idx], actual[band[b].idx]
				if aa != ab {
					return aa > ab
				}
				return band[a].idx < band[b].idx
			})
		}
	}

	for k := int64(0); k < deficit; k++ {
		out[rems[int(k)%m].idx]++
	}
	return out
}

// sanitiseWeights replaces negative, NaN and infinite weights with zero.
//
// Infinities are folded to zero rather than to a large finite number on
// purpose: an infinite weight would take the entire apportionment and silently
// zero every other domain, which is a much worse failure than treating a
// corrupt capacity reading as "unknown".
func sanitiseWeights(weights []float64) []float64 {
	out := make([]float64, len(weights))
	for i, w := range weights {
		if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 {
			continue
		}
		out[i] = w
	}
	return out
}

// EqualWeights returns m equal weights, the default for workload subjects.
func EqualWeights(m int) []float64 {
	out := make([]float64, m)
	for i := range out {
		out[i] = 1
	}
	return out
}

// CapacityWeightingRatioTrigger is the max/min eligible-domain capacity ratio
// above which a workload subject switches from Equal to capacity weighting
// (§7.2, §10.2 default 1.25).
const CapacityWeightingRatioTrigger = 1.25

// NeedsCapacityWeighting reports whether eligible domains are unequal enough
// in capacity that Equal weighting would itself manufacture drift.
//
// The case this exists for: three zones where one has a quarter of the nodes.
// Equal weighting expects a third of the pods there, the scheduler cannot fit
// them, and leeway reports drift every single evaluation for a cluster that is
// behaving exactly as its shape requires.
//
// A zero-capacity domain makes the ratio infinite by definition, so it returns
// true — a domain with no capacity is precisely the case Equal gets wrong.
// Callers must exclude synthetic MinDomains padding before asking; see
// Eligibility.EffectiveWeighting for why.
func NeedsCapacityWeighting(capacities []float64) bool {
	return NeedsCapacityWeightingAt(capacities, CapacityWeightingRatioTrigger)
}

// NeedsCapacityWeightingAt is NeedsCapacityWeighting against a configured
// trigger. A trigger at or below zero means the §10.2 default; one at or below
// 1 would fire on any inequality at all, which is a legitimate thing to ask
// for and is left to the operator.
func NeedsCapacityWeightingAt(capacities []float64, trigger float64) bool {
	if trigger <= 0 {
		trigger = CapacityWeightingRatioTrigger
	}
	if len(capacities) < 2 {
		return false
	}
	minC, maxC := math.Inf(1), math.Inf(-1)
	for _, c := range capacities {
		if math.IsNaN(c) || math.IsInf(c, 0) || c < 0 {
			continue
		}
		minC = math.Min(minC, c)
		maxC = math.Max(maxC, c)
	}
	if math.IsInf(maxC, -1) {
		return false // nothing usable to compare
	}
	if minC <= 0 {
		return true
	}
	return maxC/minC > trigger
}
