// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2026 Cisco Systems, Inc. and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// FuzzReviewMetricsStateForCycleAcceptsUnmatchedProvenance stress-tests the
// contract reviewMetricsStateForCycle depends on: metrics stay cycle-local.
// A finding's provenance only contributes a ReviewMetricFindingProvenance
// event when it matches an ArtifactReceipt from the *current* cycle;
// provenance carried forward by reviewLedgerSeedFindings from a prior
// cycle's ledger -- which never matches this cycle's ArtifactReceipts --
// must be silently skipped rather than erroring or reusing a stale
// cross-cycle timestamp. Reusing a stale timestamp would corrupt this
// cycle's own Cost.StartedAt/latency accounting (see
// aggregateReviewMetricEvents), which is exactly the failure mode this
// fuzz test also guards against. Purely in-process; no git, no runner, no
// network.
func FuzzReviewMetricsStateForCycleAcceptsUnmatchedProvenance(f *testing.F) {
	f.Add(int64(1), uint8(0), uint8(0), false)
	f.Add(int64(2), uint8(1), uint8(1), true)
	f.Add(int64(3), uint8(5), uint8(3), false)
	f.Add(int64(4), uint8(6), uint8(4), true)

	lanes := []string{
		"contract",
		"callers",
		"lifecycle",
		"persistence-recovery",
		"concurrency-ordering",
		"operations-tests",
	}

	f.Fuzz(func(
		t *testing.T,
		seed int64,
		findingCount uint8,
		provenancePerFinding uint8,
		matchReceipts bool,
	) {
		rng := rand.New(rand.NewSource(seed))
		cycle, err := newReviewCycleState(
			testReviewHeadSHA,
			builtInReviewPolicy(),
		)
		if err != nil {
			t.Fatalf("newReviewCycleState() error = %v", err)
		}
		startedAt := time.Unix(1_800_000_000, 0).UTC()
		cycle.Metrics, err = newReviewMetricsState(cycle, startedAt)
		if err != nil {
			t.Fatalf("newReviewMetricsState() error = %v", err)
		}

		type expectation struct {
			findingID string
			worker    string
			lane      string
			pass      int
			matched   bool
			acceptsAt time.Time
		}
		expectations := make([]expectation, 0)

		findings := make(
			[]ReviewCanonicalFinding,
			0,
			int(findingCount)%6,
		)
		receipts := make([]ReviewArtifactReceipt, 0)
		for i := 0; i < int(findingCount)%6; i++ {
			provCount := int(provenancePerFinding)%4 + 1
			provenance := make(
				[]ReviewFindingProvenance,
				0,
				provCount,
			)
			sum := sha256.Sum256(
				[]byte(fmt.Sprintf("fuzz-finding-%d-%d", seed, i)),
			)
			findingID := "finding-" + hex.EncodeToString(sum[:])
			for p := 0; p < provCount; p++ {
				lane := lanes[rng.Intn(len(lanes))]
				pass := rng.Intn(3) + 1
				worker := fmt.Sprintf(
					"review-worker-fuzz-%d-%d",
					i,
					p,
				)
				provenance = append(
					provenance,
					ReviewFindingProvenance{
						WorkerID:    worker,
						Lane:        lane,
						Pass:        pass,
						CandidateID: fmt.Sprintf("candidate-%d-%d", i, p),
						Severity:    ReviewFindingSeverityMedium,
						Confidence:  ReviewFindingConfidenceMedium,
					},
				)
				// Deliberately leave some (or, when matchReceipts is
				// false, all) provenance entries with no corresponding
				// ArtifactReceipt -- exactly what a ledger-seeded
				// finding from a prior cycle looks like.
				matched := matchReceipts && rng.Intn(2) == 0
				acceptedAt := startedAt.Add(
					time.Duration(rng.Intn(3600)) * time.Second,
				)
				if matched {
					receipts = append(
						receipts,
						ReviewArtifactReceipt{
							WorkerID:   worker,
							Lane:       lane,
							Pass:       pass,
							Phase:      ReviewArtifactPhaseDiscovery,
							AcceptedAt: acceptedAt,
						},
					)
				}
				expectations = append(expectations, expectation{
					findingID: findingID,
					worker:    worker,
					lane:      lane,
					pass:      pass,
					matched:   matched,
					acceptsAt: acceptedAt,
				})
			}
			findings = append(findings, ReviewCanonicalFinding{
				ID:         findingID,
				ExactSHA:   cycle.HeadSHA,
				Provenance: provenance,
			})
		}
		cycle.CanonicalFindings = findings
		cycle.ArtifactReceipts = receipts

		state, err := reviewMetricsStateForCycle(cycle)
		if err != nil {
			t.Fatalf(
				"reviewMetricsStateForCycle() error = %v (findings=%d receipts=%d)",
				err,
				len(findings),
				len(receipts),
			)
		}

		for _, want := range expectations {
			var found *ReviewMetricEvent
			for index := range state.Events {
				event := &state.Events[index]
				if event.Kind != ReviewMetricFindingProvenance ||
					event.FindingID != want.findingID ||
					event.WorkerID != want.worker ||
					event.Lane != want.lane ||
					event.Pass != want.pass {
					continue
				}
				found = event
			}
			switch {
			case want.matched && found == nil:
				t.Fatalf(
					"reviewMetricsStateForCycle() dropped current-cycle provenance for finding=%s worker=%s lane=%s pass=%d",
					want.findingID,
					want.worker,
					want.lane,
					want.pass,
				)
			case want.matched && !found.ObservedAt.Equal(want.acceptsAt):
				t.Fatalf(
					"provenance observed_at mismatch for worker=%s lane=%s pass=%d: got %v want %v",
					want.worker,
					want.lane,
					want.pass,
					found.ObservedAt,
					want.acceptsAt,
				)
			case !want.matched && found != nil:
				t.Fatalf(
					"reviewMetricsStateForCycle() emitted a provenance event for "+
						"ledger-seeded (not-yet-reobserved) provenance: %#v",
					*found,
				)
			}
		}

		// The invariant that motivates skipping instead of reusing a
		// carried-forward timestamp: this cycle's own StartedAt must never
		// be pulled backward by cross-cycle provenance.
		snapshot, err := aggregateReviewMetricEvents(state.Events)
		if err != nil {
			t.Fatalf("aggregateReviewMetricEvents() error = %v", err)
		}
		if snapshot.Cost.StartedAt.Before(startedAt) {
			t.Fatalf(
				"Cost.StartedAt = %v, want not before the cycle's own start %v",
				snapshot.Cost.StartedAt,
				startedAt,
			)
		}
	})
}
