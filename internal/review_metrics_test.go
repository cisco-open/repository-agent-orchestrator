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
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRecordTrustedReviewMetricEventIsIdempotent(t *testing.T) {
	state := &ReviewMetricsState{
		SchemaVersion: reviewMetricsSchemaVersion,
		Namespace:     "review-cycle-test",
		Events:        []ReviewMetricEvent{},
	}
	observedAt := time.Unix(1_800_500_000, 0).UTC()
	event := ReviewMetricEvent{
		Kind:       ReviewMetricLaneActivity,
		ObservedAt: observedAt,
		HeadSHA:    testReviewHeadSHA,
		Role:       AgentProfileRoleDiscovery,
		Lane:       "contract",
		Pass:       1,
		Activity:   ReviewMetricActivityCompleted,
	}
	added, err := recordTrustedReviewMetricEvent(state, event)
	if err != nil {
		t.Fatalf("recordTrustedReviewMetricEvent(first) error = %v", err)
	}
	if !added || len(state.Events) != 1 {
		t.Fatalf("first metric record = (added=%v events=%d), want true/1", added, len(state.Events))
	}
	first := state.Events[0]

	event.ObservedAt = observedAt.Add(time.Hour)
	added, err = recordTrustedReviewMetricEvent(state, event)
	if err != nil {
		t.Fatalf("recordTrustedReviewMetricEvent(replay) error = %v", err)
	}
	if added || len(state.Events) != 1 {
		t.Fatalf("replayed metric record = (added=%v events=%d), want false/1", added, len(state.Events))
	}
	if !reflect.DeepEqual(state.Events[0], first) {
		t.Fatalf("replay mutated the first trusted event: got=%#v want=%#v", state.Events[0], first)
	}

	findingID := "finding-" + strings.Repeat("b", sha256.Size*2)
	verifier := ReviewMetricEvent{
		Kind:                ReviewMetricVerifierOutcome,
		ObservedAt:          observedAt.Add(2 * time.Hour),
		HeadSHA:             testReviewHeadSHA,
		Role:                AgentProfileRoleVerifier,
		Round:               1,
		WorkerID:            "review-worker-verifier",
		FindingID:           findingID,
		AssignmentID:        "verification:" + findingID + ":001",
		VerificationOutcome: ReviewVerificationRejected,
	}
	added, err = recordTrustedReviewMetricEvent(state, verifier)
	if err != nil || !added {
		t.Fatalf("recordTrustedReviewMetricEvent(verifier) = (added=%v err=%v)", added, err)
	}
	verifier.VerificationOutcome = ReviewVerificationConfirmed
	verifier.ObservedAt = verifier.ObservedAt.Add(time.Hour)
	added, err = recordTrustedReviewMetricEvent(state, verifier)
	if err != nil || added {
		t.Fatalf("recordTrustedReviewMetricEvent(verifier replay) = (added=%v err=%v)", added, err)
	}

	snapshot, err := aggregateReviewMetricEvents(append(state.Events, state.Events[0]))
	if err != nil {
		t.Fatalf("aggregateReviewMetricEvents(replay) error = %v", err)
	}
	if snapshot.EventCount != 2 ||
		snapshot.Quality.LanesCompleted != 1 ||
		snapshot.Quality.VerifierOutcomes != 1 ||
		snapshot.Quality.VerifierRejections != 1 {
		t.Fatalf("replayed snapshot double-counted an event: %#v", snapshot)
	}
}

func TestReviewMetricsSnapshotPersistsRejectedCandidateAcrossRestart(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	completeConvergenceVerification(
		t,
		harness,
		assignment,
		ReviewVerificationRejected,
	)
	if _, err := harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	); err != nil {
		t.Fatalf("completeReviewConvergenceRound() error = %v", err)
	}
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared")
	}
	before, err := current.ReviewCycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot(before restart) error = %v", err)
	}
	if before.Quality.FindingReports != 2 ||
		before.Quality.UniqueCandidateFindings != 1 ||
		before.Quality.DeduplicatedCandidateReports != 1 ||
		before.Quality.VerifierOutcomes != 1 ||
		before.Quality.VerifierRejections != 1 ||
		before.Quality.ConvergenceRoundsStarted != 1 ||
		before.Quality.ConvergenceRoundsCompleted != 1 {
		t.Fatalf("trusted quality snapshot = %#v", before.Quality)
	}
	if before.Quality.VerifierRejectionRate !=
		(ReviewMetricRate{Numerator: 1, Denominator: 1}) {
		t.Fatalf(
			"verifier rejection rate = %#v, want 1/1",
			before.Quality.VerifierRejectionRate,
		)
	}
	if before.Cost.AgentCount < 2 ||
		before.Cost.LatencyMillis < 0 ||
		before.Cost.Usage != nil {
		t.Fatalf("trusted cost snapshot = %#v", before.Cost)
	}
	if findings := current.ReviewCycle.PublishableFindings(); len(findings) != 0 {
		t.Fatalf("rejected candidate became publishable: %#v", findings)
	}

	if err := harness.bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}
	restoredAgents := NewAgentManager()
	restored := &Orchestrator{
		cfg:    harness.bot.cfg,
		agents: restoredAgents,
	}
	if err := restored.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	restarted, ok := restoredAgents.Get(harness.reviewer.ID)
	if !ok || restarted.ReviewCycle == nil ||
		restarted.ReviewCycle.Metrics == nil {
		t.Fatal("restart did not restore persisted review metrics")
	}
	after, err := restarted.ReviewCycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot(after restart) error = %v", err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("metrics changed across restart: before=%#v after=%#v", before, after)
	}

	body, err := json.Marshal(restarted.ReviewCycle.Metrics)
	if err != nil {
		t.Fatalf("json.Marshal(metrics) error = %v", err)
	}
	for _, prohibited := range []string{
		"convergence discovery",
		"Convergence behavior",
		"independent behavior verification",
		"authorization",
		"prompt",
	} {
		if strings.Contains(strings.ToLower(string(body)), strings.ToLower(prohibited)) {
			t.Fatalf("persisted metrics contain prohibited content %q: %s", prohibited, body)
		}
	}
}

func TestReviewMetricsSnapshotAggregatesDeduplicationAndSupportedUsage(
	t *testing.T,
) {
	cycle, err := newReviewCycleState(
		testReviewHeadSHA,
		builtInReviewPolicy(),
	)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	startedAt := time.Unix(1_800_600_000, 0).UTC()
	cycle.Metrics, err = newReviewMetricsState(cycle, startedAt)
	if err != nil {
		t.Fatalf("newReviewMetricsState() error = %v", err)
	}
	ownership := ReviewWorkerOwnership{
		DurableLaunchAttempt: DurableLaunchAttempt{
			Attempt:     1,
			OwnerID:     "review-worker-metrics",
			AllocatedAt: startedAt.Add(time.Second),
		},
		Identity: ReviewWorkerIdentity{
			CycleID:  cycle.ID,
			Revision: cycle.Revision,
			Role:     AgentProfileRoleDiscovery,
			Pass:     1,
			Lane:     "contract",
		},
	}
	cycle.WorkerOwnerships = []ReviewWorkerOwnership{ownership}
	inputTokens := uint64(120)
	added, err := recordTrustedReviewUsage(
		cycle,
		ownership.OwnerID,
		ownership.Attempt,
		startedAt.Add(5*time.Second),
		ReviewMetricUsage{InputTokens: &inputTokens},
	)
	if err != nil || !added {
		t.Fatalf("recordTrustedReviewUsage(first) = (added=%v err=%v)", added, err)
	}
	differentReplay := uint64(999)
	added, err = recordTrustedReviewUsage(
		cycle,
		ownership.OwnerID,
		ownership.Attempt,
		startedAt.Add(10*time.Second),
		ReviewMetricUsage{InputTokens: &differentReplay},
	)
	if err != nil || added {
		t.Fatalf("recordTrustedReviewUsage(replay) = (added=%v err=%v)", added, err)
	}

	state, err := reviewMetricsStateForCycle(cycle)
	if err != nil {
		t.Fatalf("reviewMetricsStateForCycle() error = %v", err)
	}
	findingID := "finding-" + strings.Repeat("a", sha256.Size*2)
	for ordinal, workerID := range []string{
		"review-worker-origin-one",
		"review-worker-origin-two",
	} {
		if _, err := recordTrustedReviewMetricEvent(
			state,
			ReviewMetricEvent{
				Kind:       ReviewMetricFindingProvenance,
				ObservedAt: startedAt.Add(time.Duration(ordinal+2) * time.Second),
				HeadSHA:    cycle.HeadSHA,
				Role:       AgentProfileRoleDiscovery,
				Lane:       "contract",
				Pass:       1,
				Ordinal:    1,
				WorkerID:   workerID,
				FindingID:  findingID,
			},
		); err != nil {
			t.Fatalf("recordTrustedReviewMetricEvent(provenance %d) error = %v", ordinal, err)
		}
	}
	if _, err := recordTrustedReviewMetricEvent(
		state,
		ReviewMetricEvent{
			Kind:       ReviewMetricDeduplication,
			ObservedAt: startedAt.Add(4 * time.Second),
			HeadSHA:    cycle.HeadSHA,
			FindingID:  findingID,
		},
	); err != nil {
		t.Fatalf("recordTrustedReviewMetricEvent(deduplication) error = %v", err)
	}
	snapshot, err := aggregateReviewMetricEvents(state.Events)
	if err != nil {
		t.Fatalf("aggregateReviewMetricEvents() error = %v", err)
	}
	if snapshot.Quality.FindingReports != 2 ||
		snapshot.Quality.UniqueCandidateFindings != 1 ||
		snapshot.Quality.DeduplicatedCandidateReports != 1 ||
		snapshot.Quality.DeduplicatedFindingGroups != 1 ||
		snapshot.Quality.DeduplicationRate !=
			(ReviewMetricRate{Numerator: 1, Denominator: 2}) {
		t.Fatalf("deduplication snapshot = %#v", snapshot.Quality)
	}
	if snapshot.Cost.Usage == nil ||
		snapshot.Cost.Usage.InputTokens == nil ||
		*snapshot.Cost.Usage.InputTokens != inputTokens ||
		snapshot.Cost.Usage.CachedInputTokens != nil ||
		snapshot.Cost.Usage.OutputTokens != nil ||
		snapshot.Cost.Usage.TotalTokens != nil {
		t.Fatalf("supported usage snapshot = %#v", snapshot.Cost.Usage)
	}
	usageBody, err := json.Marshal(snapshot.Cost.Usage)
	if err != nil {
		t.Fatalf("json.Marshal(usage) error = %v", err)
	}
	for _, unsupported := range []string{
		"cached_input_tokens",
		"output_tokens",
		"total_tokens",
	} {
		if strings.Contains(string(usageBody), unsupported) {
			t.Fatalf("usage snapshot invented unsupported field %q: %s", unsupported, usageBody)
		}
	}
}

func TestReviewMetricsStateForCycleSkipsSeededFindingsUntilReobservedThisCycle(
	t *testing.T,
) {
	cycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	startedAt := time.Unix(1_800_700_000, 0).UTC()
	cycle.Metrics, err = newReviewMetricsState(cycle, startedAt)
	if err != nil {
		t.Fatalf("newReviewMetricsState() error = %v", err)
	}
	// Simulate a finding carried forward from a prior review cycle's review
	// ledger (see reviewLedgerSeedFindings): its provenance references a
	// worker/lane/pass from an earlier cycle that has no corresponding entry
	// in this cycle's ArtifactReceipts, and an ObservedAt from days earlier.
	staleObservedAt := startedAt.Add(-90 * 24 * time.Hour)
	findingID := "finding-" + strings.Repeat("c", sha256.Size*2)
	cycle.CanonicalFindings = []ReviewCanonicalFinding{
		{
			ID:       findingID,
			ExactSHA: cycle.HeadSHA,
			Provenance: []ReviewFindingProvenance{
				{
					WorkerID:    "review-worker-prior-cycle",
					Lane:        "contract",
					Pass:        1,
					CandidateID: "candidate-prior-cycle",
				},
			},
		},
	}
	if len(cycle.ArtifactReceipts) != 0 {
		t.Fatalf(
			"test setup expected no artifact receipts, got %d",
			len(cycle.ArtifactReceipts),
		)
	}
	state, err := reviewMetricsStateForCycle(cycle)
	if err != nil {
		t.Fatalf("reviewMetricsStateForCycle() error = %v", err)
	}
	for _, event := range state.Events {
		if event.Kind == ReviewMetricFindingProvenance &&
			event.FindingID == findingID {
			t.Fatalf(
				"reviewMetricsStateForCycle() emitted a provenance event for a "+
					"finding not yet reobserved this cycle: %#v",
				event,
			)
		}
	}
	snapshot, err := aggregateReviewMetricEvents(state.Events)
	if err != nil {
		t.Fatalf("aggregateReviewMetricEvents() error = %v", err)
	}
	// The whole point of skipping cross-cycle provenance: it must never pull
	// this cycle's StartedAt/latency accounting back to a stale timestamp
	// from whenever the finding was originally discovered.
	if !snapshot.Cost.StartedAt.Equal(startedAt) {
		t.Fatalf(
			"skipping seeded provenance still corrupted StartedAt = %v, want %v (not the stale %v)",
			snapshot.Cost.StartedAt,
			startedAt,
			staleObservedAt,
		)
	}
}

func TestReviewMetricsSnapshotComputesLateOriginalMaterialFindingRate(
	t *testing.T,
) {
	firstFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/first.go",
		"FirstFinding",
		"first behavior remains valid",
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/first.go",
		),
		testReviewLedgerCoverageRequirements("internal/first.go"),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				firstFinding,
				ReviewLedgerFindingReported,
				"verified first finding",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("transitionReviewLedger(first) error = %v", err)
	}
	lateFinding := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		"internal/late.go",
		"LateFinding",
		"late behavior remains valid",
	)
	second, err := transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/fix.go",
		),
		testReviewLedgerCoverageRequirements("internal/fix.go"),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				lateFinding,
				ReviewLedgerFindingMissed,
				"verified finding existed at the earlier reviewed head",
				reviewLedgerBool(true),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("transitionReviewLedger(second) error = %v", err)
	}
	cycle, err := newReviewCycleState(
		testLedgerSecondSHA,
		builtInReviewPolicy(),
	)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	snapshot, err := reviewMetricsSnapshotWithLedger(
		cycle,
		&second.Ledger,
	)
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot() error = %v", err)
	}
	if snapshot.Quality.PublishedMaterialFindings != 2 ||
		snapshot.Quality.LateOriginalMaterialFindings != 1 ||
		snapshot.Quality.LateOriginalMaterialFindingRate !=
			(ReviewMetricRate{Numerator: 1, Denominator: 2}) {
		t.Fatalf("late-original snapshot = %#v", snapshot.Quality)
	}
}
