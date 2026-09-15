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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestChallengeArtifactPreservesEmptyValidatedCollections(
	t *testing.T,
) {
	payload := ReviewArtifactPayload{
		Kind: ReviewArtifactPayloadChallenge,
		Challenge: &ReviewChallengePayload{
			Outcome:      ReviewChallengeOverturned,
			Summary:      "targeted hypothesis produced no new candidate",
			AssignmentID: "challenge:001:test",
			TargetKind:   ReviewChallengeCompetingHypothesis,
			TargetID:     "finding-test",
			Candidates:   []ReviewFindingCandidate{},
			Coverage:     []ReviewCoverageClaim{},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(challenge) error = %v", err)
	}
	var decoded ReviewArtifactPayload
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(challenge) error = %v", err)
	}
	if decoded.Challenge == nil ||
		decoded.Challenge.Candidates == nil ||
		decoded.Challenge.Coverage == nil {
		t.Fatalf(
			"challenge collections were not preserved: %s",
			body,
		)
	}
}

func TestVerificationReceiptPreservesValidationDiagnostic(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	ownership, _ := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewVerificationRunning(
				harness.reviewer.ID,
				assignment.ID,
				&ownership,
			)
		},
	)
	payload := ReviewVerificationPayload{
		FindingID:        "different-finding",
		Outcome:          ReviewVerificationInconclusive,
		Summary:          "the assigned finding could not be reproduced",
		ScopeDisposition: ReviewScopeUnclear,
		PatchDisposition: ReviewPatchUnclear,
		Evidence:         []ReviewEvidence{},
		CausalEvidence:   []ReviewEvidence{},
		TestEvidence:     []ReviewEvidence{},
	}
	envelope, err := newReviewArtifactEnvelope(
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		ReviewArtifactPhaseVerification,
		ReviewArtifactPayload{
			Kind:         ReviewArtifactPayloadVerification,
			Verification: &payload,
		},
	)
	if err != nil {
		t.Fatalf("newReviewArtifactEnvelope(verification) error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	err = completeReviewVerificationFromReceipt(
		current.ReviewCycle,
		ownership,
		envelope,
		time.Now().UTC(),
	)
	artifactErr := asReviewArtifactError(err)
	if artifactErr == nil || !strings.Contains(
		artifactErr.Detail,
		"verification result does not match its assigned finding",
	) {
		t.Fatalf("verification validation diagnostic = %#v", artifactErr)
	}
}

func TestReviewArtifactIntakeCanonicalizesEquivalentRepositoryPaths(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	ownership, store := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewVerificationRunning(
				harness.reviewer.ID,
				assignment.ID,
				&ownership,
			)
		},
	)
	finding := currentReviewFinding(t, harness, assignment.FindingID)
	envelope := verificationEnvelope(
		t,
		harness,
		ownership,
		finding,
		ReviewVerificationConfirmed,
	)
	envelope.Payload.Verification.Location.Path = "./" + finding.Location.Path
	for _, evidence := range [][]ReviewEvidence{
		envelope.Payload.Verification.Evidence,
		envelope.Payload.Verification.CausalEvidence,
		envelope.Payload.Verification.TestEvidence,
	} {
		for index := range evidence {
			evidence[index].Path = "./" + evidence[index].Path
		}
	}
	path, err := store.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	)
	if err != nil {
		t.Fatalf("publish(variant paths) error = %v", err)
	}
	accepted, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		ownership.OwnerID,
		path,
		ReviewArtifactPhaseVerification,
	)
	if err != nil {
		t.Fatalf("intake(variant paths) error = %v", err)
	}
	verification := accepted.Payload.Verification
	if verification == nil || verification.Location.Path != finding.Location.Path {
		t.Fatalf("accepted location = %#v, want canonical %q", verification, finding.Location.Path)
	}
	for _, evidence := range [][]ReviewEvidence{
		verification.Evidence,
		verification.CausalEvidence,
		verification.TestEvidence,
	} {
		for _, item := range evidence {
			if item.Path != finding.Location.Path {
				t.Fatalf("accepted evidence path = %q, want canonical %q", item.Path, finding.Location.Path)
			}
		}
	}
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatalf("persisted artifact receipts = %#v", current.ReviewCycle)
	}
	var persisted *ReviewArtifactReceipt
	for index := range current.ReviewCycle.ArtifactReceipts {
		if current.ReviewCycle.ArtifactReceipts[index].WorkerID == ownership.OwnerID {
			persisted = &current.ReviewCycle.ArtifactReceipts[index]
			break
		}
	}
	if persisted == nil || persisted.Envelope.Payload.Verification == nil {
		t.Fatalf("persisted verifier artifact is missing: %#v", current.ReviewCycle.ArtifactReceipts)
	}
	if got := persisted.Envelope.Payload.Verification.Location.Path; got != finding.Location.Path {
		t.Fatalf("persisted location = %q, want canonical %q", got, finding.Location.Path)
	}
	if err := validatePersistedReviewCycleSnapshot(current.ReviewCycle); err != nil {
		t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
	}
}

func TestCurrentReviewWorkerContractCompletesEveryAssignment(t *testing.T) {
	t.Run("required discovery lanes", func(t *testing.T) {
		harness := newReviewArtifactIntakeHarness(t)
		attachDiscoveryPlanToArtifactHarness(t, harness)
		pass, err := harness.bot.beginReviewDiscoveryPass(
			harness.reviewer.ID,
			1,
		)
		if err != nil {
			t.Fatalf("beginReviewDiscoveryPass() error = %v", err)
		}
		completed := 0
		for _, lane := range pass.Lanes {
			if !lane.Required {
				continue
			}
			var ownership ReviewWorkerOwnership
			var store *reviewArtifactStore
			if lane.Lane == harness.ownership.Identity.Lane {
				ownership = harness.ownership
				store = harness.store
				if err := harness.bot.markReviewDiscoveryLaneRunning(
					harness.reviewer.ID,
					pass.Pass,
					lane.Lane,
					nil,
				); err != nil {
					t.Fatalf("markReviewDiscoveryLaneRunning(%s) error = %v", lane.Lane, err)
				}
				if err := harness.bot.markReviewDiscoveryLaneRunning(
					harness.reviewer.ID,
					pass.Pass,
					lane.Lane,
					&ownership,
				); err != nil {
					t.Fatalf("bind discovery lane %s error = %v", lane.Lane, err)
				}
			} else {
				ownership, store = bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleDiscovery,
					pass.Pass,
					lane.Lane,
					func(ownership ReviewWorkerOwnership) error {
						if err := harness.bot.markReviewDiscoveryLaneRunning(
							harness.reviewer.ID,
							pass.Pass,
							lane.Lane,
							nil,
						); err != nil {
							return err
						}
						return harness.bot.markReviewDiscoveryLaneRunning(
							harness.reviewer.ID,
							pass.Pass,
							lane.Lane,
							&ownership,
						)
					},
				)
			}
			envelope := testDiscoveryArtifactEnvelope(
				t,
				ownership,
				"current contract discovery",
			)
			envelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
			envelope.Checkout.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
			assertReviewWorkerContractCoversArtifact(
				t,
				AgentProfileRoleDiscovery,
				envelope,
			)
			path, err := store.publish(
				context.Background(),
				harness.reviewer.ReviewCycle.HeadSHA,
				ownership,
				envelope,
			)
			if err != nil {
				t.Fatalf("publish discovery lane %s error = %v", lane.Lane, err)
			}
			if _, err := harness.bot.intakeReviewWorkerArtifact(
				context.Background(),
				harness.reviewer.ID,
				ownership.OwnerID,
				path,
				ReviewArtifactPhaseDiscovery,
			); err != nil {
				t.Fatalf("intake discovery lane %s error = %v", lane.Lane, err)
			}
			completed++
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		latest, ok := latestReviewDiscoveryPassSnapshot(current.ReviewCycle)
		if !ok || completed != 4 {
			t.Fatalf("completed required discovery lanes = %d, pass = %#v", completed, latest)
		}
		for _, lane := range latest.Lanes {
			if lane.Required && lane.Status != ReviewDiscoveryLaneCompleted {
				t.Fatalf("required discovery lane %s = %#v", lane.Lane, lane)
			}
		}
	})

	t.Run("verification", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		assignment := queueOneReviewVerification(t, harness)
		ownership, store := bindConvergenceWorker(
			t,
			harness,
			AgentProfileRoleVerifier,
			assignment.Round,
			assignment.Lane,
			func(ownership ReviewWorkerOwnership) error {
				return harness.bot.markReviewVerificationRunning(
					harness.reviewer.ID,
					assignment.ID,
					&ownership,
				)
			},
		)
		finding := currentReviewFinding(t, harness, assignment.FindingID)
		envelope := verificationEnvelope(
			t,
			harness,
			ownership,
			finding,
			ReviewVerificationConfirmed,
		)
		assertReviewWorkerContractCoversArtifact(
			t,
			AgentProfileRoleVerifier,
			envelope,
		)
		path, err := store.publish(
			context.Background(),
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf("publish verification error = %v", err)
		}
		if _, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			ownership.OwnerID,
			path,
			ReviewArtifactPhaseVerification,
		); err != nil {
			t.Fatalf("intake verification error = %v", err)
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		completed, ok := scheduledReviewVerificationAssignment(
			current.ReviewCycle,
			assignment.Round,
			assignment.Lane,
		)
		if !ok || completed.Status != ReviewVerificationCompleted {
			t.Fatalf("completed verification = %#v", completed)
		}
	})

	t.Run("challenge", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 0, false)
		round, err := harness.bot.beginReviewConvergenceRound(harness.reviewer.ID)
		if err != nil {
			t.Fatalf("beginReviewConvergenceRound() error = %v", err)
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		targets := buildReviewChallengeTargets(current.ReviewCycle)
		if len(targets) == 0 || targets[0].Kind != ReviewChallengeCoverageGap {
			t.Fatalf("challenge targets = %#v", targets)
		}
		target := targets[0]
		assignment, err := harness.bot.queueTargetedReviewChallenge(
			harness.reviewer.ID,
			round.Round,
			target,
		)
		if err != nil {
			t.Fatalf("queueTargetedReviewChallenge() error = %v", err)
		}
		if err := harness.bot.markReviewChallengeRunning(
			harness.reviewer.ID,
			assignment.ID,
			nil,
		); err != nil {
			t.Fatalf("markReviewChallengeRunning(initial) error = %v", err)
		}
		ownership, store := bindConvergenceWorker(
			t,
			harness,
			AgentProfileRoleChallenge,
			assignment.Round,
			assignment.Lane,
			func(ownership ReviewWorkerOwnership) error {
				return harness.bot.markReviewChallengeRunning(
					harness.reviewer.ID,
					assignment.ID,
					&ownership,
				)
			},
		)
		payload := ReviewChallengePayload{
			Outcome:      ReviewChallengeOverturned,
			Summary:      "current contract challenge",
			AssignmentID: assignment.ID,
			TargetKind:   target.Kind,
			TargetID:     target.ID,
			Candidates:   []ReviewFindingCandidate{},
			Coverage: []ReviewCoverageClaim{{
				RequirementID: target.RequirementID,
				Kind:          targetCoverageKind(t, current.ReviewCycle, target),
				Status:        ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary: "target requirement exercised",
					Path:    "tracked.txt",
				}},
			}},
		}
		envelope, err := newReviewArtifactEnvelope(
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			ReviewArtifactPhaseChallenge,
			ReviewArtifactPayload{
				Kind:      ReviewArtifactPayloadChallenge,
				Challenge: &payload,
			},
		)
		if err != nil {
			t.Fatalf("newReviewArtifactEnvelope(challenge) error = %v", err)
		}
		assertReviewWorkerContractCoversArtifact(
			t,
			AgentProfileRoleChallenge,
			envelope,
		)
		path, err := store.publish(
			context.Background(),
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf("publish challenge error = %v", err)
		}
		if _, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			ownership.OwnerID,
			path,
			ReviewArtifactPhaseChallenge,
		); err != nil {
			t.Fatalf("intake challenge error = %v", err)
		}
		current, _ = harness.agents.Get(harness.reviewer.ID)
		completed, ok := scheduledReviewChallengeAssignment(
			current.ReviewCycle,
			assignment.Round,
			assignment.Lane,
		)
		if !ok || completed.Status != ReviewChallengeCompleted {
			t.Fatalf("completed challenge = %#v", completed)
		}
	})
}

func assertReviewWorkerContractCoversArtifact(
	t *testing.T,
	role AgentProfileRole,
	envelope ReviewArtifactEnvelope,
) {
	t.Helper()
	contract, err := reviewWorkerArtifactContract(role)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactContract(%s) error = %v", role, err)
	}
	if strings.Contains(contract, "schema_version") {
		t.Fatalf("review worker contract contains obsolete schema version: %s", contract)
	}
	body, err := marshalReviewArtifactEnvelope(envelope)
	if err != nil {
		t.Fatalf("marshalReviewArtifactEnvelope(%s) error = %v", role, err)
	}
	var artifact any
	if err := json.Unmarshal(body, &artifact); err != nil {
		t.Fatalf("json.Unmarshal(current artifact) error = %v", err)
	}
	keys := make(map[string]struct{})
	var collectKeys func(any)
	collectKeys = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			for key, nested := range value {
				keys[key] = struct{}{}
				collectKeys(nested)
			}
		case []any:
			for _, nested := range value {
				collectKeys(nested)
			}
		}
	}
	collectKeys(artifact)
	for key := range keys {
		if !strings.Contains(contract, `"`+key+`"`) {
			t.Fatalf("review worker contract for %s omits current field %q: %s", role, key, contract)
		}
	}
	if _, err := unmarshalReviewArtifactEnvelope(body); err != nil {
		t.Fatalf("unmarshalReviewArtifactEnvelope(%s) error = %v", role, err)
	}
}

func TestReviewVerificationAssignmentsAreIndependentDeterministicAndBounded(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 1, false)
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil ||
		len(current.ReviewCycle.CanonicalFindings) != 1 {
		t.Fatal("review convergence harness has no material finding")
	}
	first := cloneReviewCycle(current.ReviewCycle)
	second := cloneReviewCycle(current.ReviewCycle)
	for _, cycle := range []*ReviewCycleState{first, second} {
		cycle.Policy.Verification.MinVerifiers = 2
		cycle.Policy.Verification.MaxVerifiers = 2
		cycle.Policy.Verification.MaxParallelVerifiers = 2
	}
	queuedAt := time.Unix(1_800_100_000, 0).UTC()
	firstAssignments, err := queueReviewVerificationAssignments(
		first,
		1,
		queuedAt,
	)
	if err != nil {
		t.Fatalf("queueReviewVerificationAssignments(first) error = %v", err)
	}
	secondAssignments, err := queueReviewVerificationAssignments(
		second,
		1,
		queuedAt,
	)
	if err != nil {
		t.Fatalf("queueReviewVerificationAssignments(second) error = %v", err)
	}
	if !reflect.DeepEqual(firstAssignments, secondAssignments) {
		t.Fatalf(
			"verification assignment changed for a snapshot: first=%#v second=%#v",
			firstAssignments,
			secondAssignments,
		)
	}
	if len(firstAssignments) != 2 {
		t.Fatalf(
			"verification assignments = %d, want snapshotted minimum 2",
			len(firstAssignments),
		)
	}
	origin := current.ReviewCycle.CanonicalFindings[0].Provenance[0].WorkerID
	for _, assignment := range firstAssignments {
		if !stringSliceContains(assignment.ExcludedWorkerIDs, origin) {
			t.Fatalf(
				"assignment %q does not exclude origin %q",
				assignment.ID,
				origin,
			)
		}
	}
	first.Convergence.VerificationAssignments[0].Status =
		ReviewVerificationCompleted
	first.Convergence.VerificationAssignments[0].Outcome =
		ReviewVerificationConfirmed
	if err := syncReviewFindingVerifications(first); err != nil {
		t.Fatalf("syncReviewFindingVerifications(minimum) error = %v", err)
	}
	if first.Convergence.FindingVerifications[0].Status !=
		ReviewFindingVerificationPending {
		t.Fatal(
			"a single completed verifier bypassed the snapshotted minimum",
		)
	}
	var originOwnership ReviewWorkerOwnership
	for _, ownership := range first.WorkerOwnerships {
		if ownership.OwnerID == origin {
			originOwnership = ownership
			break
		}
	}
	if originOwnership.OwnerID == "" {
		t.Fatalf("originating ownership %q disappeared", origin)
	}
	if reviewVerificationOwnershipMatches(
		first,
		firstAssignments[0],
		originOwnership,
	) {
		t.Fatal("originating discovery worker could self-confirm")
	}

	boundedCapacity := cloneReviewCycle(current.ReviewCycle)
	boundedCapacity.Policy.Convergence.MaxReviewAgentsPerSHA =
		len(boundedCapacity.WorkerOwnerships)
	assignments, err := queueReviewVerificationAssignments(
		boundedCapacity,
		1,
		queuedAt,
	)
	if err != nil {
		t.Fatalf("queueReviewVerificationAssignments(bounded capacity) error = %v", err)
	}
	if len(assignments) != 0 ||
		reviewConvergenceRemainingAgentCapacity(boundedCapacity) != 0 {
		t.Fatalf(
			"bounded verifier capacity state = %#v",
			assignments,
		)
	}
}

func TestReviewCapacityCapsQueuedCandidatesAtConfiguredCeiling(t *testing.T) {
	const candidateCount = 13
	harness := newReviewConvergenceTestHarness(t, candidateCount, true)
	current, _ := harness.agents.Get(harness.reviewer.ID)
	cycle := cloneReviewCycle(current.ReviewCycle)
	cycle.Policy.Convergence.MaxReviewAgentsPerSHA = 12
	available := reviewConvergenceRemainingAgentCapacity(cycle)

	plan, err := buildReviewCapacityPlan(cycle)
	if err != nil {
		t.Fatalf("buildReviewCapacityPlan() error = %v", err)
	}
	if plan.RequiredMaximum <= plan.ConfiguredMaximum ||
		plan.EffectiveMaximum != plan.ConfiguredMaximum {
		t.Fatalf("capacity plan does not preserve the configured hard ceiling: %#v", plan)
	}
	assignments, err := queueReviewVerificationAssignments(
		cycle,
		1,
		time.Unix(1_800_100_050, 0).UTC(),
	)
	if err != nil {
		t.Fatalf("queueReviewVerificationAssignments() error = %v", err)
	}
	if len(assignments) != available {
		t.Fatalf("queued assignments = %d, want available capacity %d", len(assignments), available)
	}
	for _, assignment := range assignments {
		if assignment.Status != ReviewVerificationQueued ||
			assignment.FailureCode != "" {
			t.Fatalf("candidate was silently truncated: %#v", assignment)
		}
	}
}

func TestReviewVerificationConflictsRequireOutcomeSupport(t *testing.T) {
	t.Run("mixed outcomes remain pending while capacity remains", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		current, _ := harness.agents.Get(harness.reviewer.ID)
		cycle := cloneReviewCycle(current.ReviewCycle)
		cycle.Policy.Verification.MinVerifiers = 2
		cycle.Policy.Verification.MaxVerifiers = 3
		assignments, err := queueReviewVerificationAssignments(
			cycle,
			1,
			time.Unix(1_800_100_100, 0).UTC(),
		)
		if err != nil {
			t.Fatalf("queueReviewVerificationAssignments() error = %v", err)
		}
		if len(assignments) != 2 {
			t.Fatalf("initial verification assignments = %#v", assignments)
		}
		cycle.Convergence.VerificationAssignments[0].Status =
			ReviewVerificationCompleted
		cycle.Convergence.VerificationAssignments[0].Outcome =
			ReviewVerificationConfirmed
		cycle.Convergence.VerificationAssignments[1].Status =
			ReviewVerificationCompleted
		cycle.Convergence.VerificationAssignments[1].Outcome =
			ReviewVerificationRejected
		if err := syncReviewFindingVerifications(cycle); err != nil {
			t.Fatalf("syncReviewFindingVerifications(mixed) error = %v", err)
		}
		decision := cycle.Convergence.FindingVerifications[0]
		if decision.Status != ReviewFindingVerificationPending ||
			len(cycle.PublishableFindings()) != 0 {
			t.Fatalf(
				"mixed verification decision = %#v, publishable = %#v",
				decision,
				cycle.PublishableFindings(),
			)
		}
		tiebreaker, err := queueReviewVerificationAssignments(
			cycle,
			1,
			time.Unix(1_800_100_101, 0).UTC(),
		)
		if err != nil {
			t.Fatalf("queueReviewVerificationAssignments(tiebreaker) error = %v", err)
		}
		if len(tiebreaker) != 1 || tiebreaker[0].Ordinal != 3 {
			t.Fatalf("tiebreaker verification assignment = %#v", tiebreaker)
		}
		cycle.Convergence.VerificationAssignments[2].Status =
			ReviewVerificationCompleted
		cycle.Convergence.VerificationAssignments[2].Outcome =
			ReviewVerificationConfirmed
		if err := syncReviewFindingVerifications(cycle); err != nil {
			t.Fatalf("syncReviewFindingVerifications(consensus) error = %v", err)
		}
		if cycle.Convergence.FindingVerifications[0].Status !=
			ReviewFindingVerificationConfirmed {
			t.Fatalf(
				"supported verification decision = %#v",
				cycle.Convergence.FindingVerifications[0],
			)
		}
	})

	t.Run("exhausted mixed outcomes are inconclusive", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		current, _ := harness.agents.Get(harness.reviewer.ID)
		cycle := cloneReviewCycle(current.ReviewCycle)
		cycle.Policy.Verification.MinVerifiers = 2
		cycle.Policy.Verification.MaxVerifiers = 2
		assignments, err := queueReviewVerificationAssignments(
			cycle,
			1,
			time.Unix(1_800_100_200, 0).UTC(),
		)
		if err != nil {
			t.Fatalf("queueReviewVerificationAssignments() error = %v", err)
		}
		if len(assignments) != 2 {
			t.Fatalf("verification assignments = %#v", assignments)
		}
		cycle.Convergence.VerificationAssignments[0].Status =
			ReviewVerificationCompleted
		cycle.Convergence.VerificationAssignments[0].Outcome =
			ReviewVerificationConfirmed
		cycle.Convergence.VerificationAssignments[1].Status =
			ReviewVerificationCompleted
		cycle.Convergence.VerificationAssignments[1].Outcome =
			ReviewVerificationRejected
		if err := syncReviewFindingVerifications(cycle); err != nil {
			t.Fatalf("syncReviewFindingVerifications() error = %v", err)
		}
		decision := cycle.Convergence.FindingVerifications[0]
		if decision.Status != ReviewFindingVerificationInconclusive ||
			decision.Action == "" ||
			len(cycle.PublishableFindings()) != 0 {
			t.Fatalf(
				"exhausted mixed verification decision = %#v",
				decision,
			)
		}
		targets := buildReviewChallengeTargets(cycle)
		if len(targets) != 1 ||
			targets[0].Kind != ReviewChallengeCompetingHypothesis ||
			targets[0].ID != decision.FindingID {
			t.Fatalf(
				"mixed verification challenge targets = %#v",
				targets,
			)
		}
	})
}

func TestReviewVerificationOutcomesRequireEvidenceAndGatePublication(
	t *testing.T,
) {
	t.Run("rejected candidate is never publishable", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		assignment := queueOneReviewVerification(t, harness)
		ownership, store := bindConvergenceWorker(
			t,
			harness,
			AgentProfileRoleVerifier,
			assignment.Round,
			assignment.Lane,
			func(ownership ReviewWorkerOwnership) error {
				return harness.bot.markReviewVerificationRunning(
					harness.reviewer.ID,
					assignment.ID,
					&ownership,
				)
			},
		)
		finding := currentReviewFinding(t, harness, assignment.FindingID)
		envelope := verificationEnvelope(
			t,
			harness,
			ownership,
			finding,
			ReviewVerificationRejected,
		)
		path, err := store.publish(
			context.Background(),
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf("publish(rejected verification) error = %v", err)
		}
		if _, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			ownership.OwnerID,
			path,
			ReviewArtifactPhaseVerification,
		); err != nil {
			t.Fatalf("intake(rejected verification) error = %v", err)
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		decision := current.ReviewCycle.Convergence.FindingVerifications[0]
		if decision.Status != ReviewFindingVerificationRejected ||
			decision.ExactSHA != current.ReviewCycle.HeadSHA ||
			len(current.ReviewCycle.PublishableFindings()) != 0 {
			t.Fatalf(
				"rejected finding state = decision:%#v publishable:%#v",
				decision,
				current.ReviewCycle.PublishableFindings(),
			)
		}
	})

	t.Run("confirmed finding retains exact-SHA evidence", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		assignment := queueOneReviewVerification(t, harness)
		ownership, store := bindConvergenceWorker(
			t,
			harness,
			AgentProfileRoleVerifier,
			assignment.Round,
			assignment.Lane,
			func(ownership ReviewWorkerOwnership) error {
				return harness.bot.markReviewVerificationRunning(
					harness.reviewer.ID,
					assignment.ID,
					&ownership,
				)
			},
		)
		finding := currentReviewFinding(t, harness, assignment.FindingID)
		envelope := verificationEnvelope(
			t,
			harness,
			ownership,
			finding,
			ReviewVerificationConfirmed,
		)
		path, err := store.publish(
			context.Background(),
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf("publish(confirmed verification) error = %v", err)
		}
		if _, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			ownership.OwnerID,
			path,
			ReviewArtifactPhaseVerification,
		); err != nil {
			t.Fatalf("intake(confirmed verification) error = %v", err)
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		publishable := current.ReviewCycle.PublishableFindings()
		if len(publishable) != 1 ||
			publishable[0].ExactSHA != current.ReviewCycle.HeadSHA {
			t.Fatalf(
				"publishable exact-SHA findings = %#v",
				publishable,
			)
		}
		decision := current.ReviewCycle.Convergence.FindingVerifications[0]
		if decision.ExactSHA != current.ReviewCycle.HeadSHA ||
			len(decision.Evidence) == 0 {
			t.Fatalf(
				"confirmed verification evidence = %#v",
				decision,
			)
		}
	})

	t.Run("missing behavioral or reproduction evidence is rejected", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		current, _ := harness.agents.Get(harness.reviewer.ID)
		cycle := current.ReviewCycle
		assignments, err := queueReviewVerificationAssignments(
			cycle,
			1,
			time.Now().UTC(),
		)
		if err != nil {
			t.Fatalf("queueReviewVerificationAssignments() error = %v", err)
		}
		payload := ReviewVerificationPayload{
			FindingID:        assignments[0].FindingID,
			Outcome:          ReviewVerificationConfirmed,
			Summary:          "unsupported confirmation",
			ScopeDisposition: ReviewScopeInScope,
			PatchDisposition: ReviewPatchIntroduced,
		}
		if err := validateReviewVerificationEvidence(
			cycle,
			assignments[0],
			payload,
		); err == nil {
			t.Fatal("evidence-free confirmation was accepted")
		}
	})

	t.Run("equivalent verifier location is canonicalized", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		assignment := queueOneReviewVerification(t, harness)
		finding := currentReviewFinding(t, harness, assignment.FindingID)
		location := finding.Location
		location.Symbol = strings.ToUpper(location.Symbol)
		payload := ReviewVerificationPayload{
			FindingID:        finding.ID,
			Outcome:          ReviewVerificationConfirmed,
			ScopeDisposition: ReviewScopeInScope,
			PatchDisposition: ReviewPatchIntroduced,
			Location:         &location,
			BehavioralPath:   strings.ToUpper(finding.BehavioralPath),
			Evidence: []ReviewEvidence{{
				Path: finding.Location.Path,
			}},
			CausalEvidence: []ReviewEvidence{{
				Summary: "exact diff changes the assigned behavior",
				Path:    finding.Location.Path,
			}},
			TestEvidence: []ReviewEvidence{{
				Path: finding.Location.Path,
			}},
		}
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err != nil {
			t.Fatalf("equivalent verifier location rejected: %v", err)
		}

		location.EndLine++
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err == nil {
			t.Fatal("different verifier location was accepted")
		}

	})

	t.Run("test evidence supports an assigned test location", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		assignment := queueOneReviewVerification(t, harness)
		finding := currentReviewFinding(t, harness, assignment.FindingID)
		location := finding.Location
		payload := ReviewVerificationPayload{
			FindingID:        finding.ID,
			Outcome:          ReviewVerificationRejected,
			ScopeDisposition: ReviewScopeInScope,
			PatchDisposition: ReviewPatchNotReproduced,
			Location:         &location,
			BehavioralPath:   finding.BehavioralPath,
			Evidence: []ReviewEvidence{{
				Summary: "implementation behaves correctly",
				Path:    "internal/implementation.go",
			}},
			TestEvidence: []ReviewEvidence{{
				Summary: "assigned regression test passed",
				Path:    finding.Location.Path,
			}},
		}
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err != nil {
			t.Fatalf("assigned test evidence rejected: %v", err)
		}

		payload.TestEvidence[0].Path = ""
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err == nil || !strings.Contains(err.Error(), "assigned location") {
			t.Fatalf("unsupported assigned location error = %v", err)
		}
	})

	t.Run("infrastructure failure remains unresolved", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		assignment := queueOneReviewVerification(t, harness)
		if err := harness.bot.failReviewVerificationAssignment(
			harness.reviewer.ID,
			assignment.ID,
			ReviewVerificationFailureRuntime,
		); err != nil {
			t.Fatalf("failReviewVerificationAssignment() error = %v", err)
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		decision := current.ReviewCycle.Convergence.FindingVerifications[0]
		if decision.Status != ReviewFindingVerificationUnresolved ||
			decision.Action == "" ||
			current.ReviewCycle.ApprovalEligible() {
			t.Fatalf(
				"failed verifier silently advanced approval: %#v",
				decision,
			)
		}
	})

	t.Run("pre-existing and out-of-scope observations cannot confirm", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		assignment := queueOneReviewVerification(t, harness)
		finding := currentReviewFinding(t, harness, assignment.FindingID)
		location := finding.Location
		payload := ReviewVerificationPayload{
			FindingID:        finding.ID,
			Outcome:          ReviewVerificationConfirmed,
			ScopeDisposition: ReviewScopeInScope,
			PatchDisposition: ReviewPatchPreExisting,
			Location:         &location,
			BehavioralPath:   finding.BehavioralPath,
			Evidence: []ReviewEvidence{{
				Summary: "behavior exists at head",
				Path:    finding.Location.Path,
			}},
			CausalEvidence: []ReviewEvidence{{
				Summary: "changed path inspection",
				Path:    finding.Location.Path,
			}},
			TestEvidence: []ReviewEvidence{{
				Summary: "behavior reproduced",
				Path:    finding.Location.Path,
			}},
		}
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err == nil || !strings.Contains(err.Error(), "not introduced or worsened") {
			t.Fatalf("pre-existing confirmation error = %v", err)
		}

		payload.Outcome = ReviewVerificationRejected
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err != nil {
			t.Fatalf("pre-existing rejection was not accepted: %v", err)
		}

		payload.Outcome = ReviewVerificationConfirmed
		payload.PatchDisposition = ReviewPatchIntroduced
		payload.ScopeDisposition = ReviewScopeOutOfScope
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err == nil || !strings.Contains(err.Error(), "outside the immutable task intent") {
			t.Fatalf("out-of-scope confirmation error = %v", err)
		}
	})

	t.Run("confirmation requires causal evidence from the exact diff", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		harness.reviewer.ReviewCycle.Inputs.ChangedFiles[0].ChangedRanges =
			[]ReviewLineRange{{StartLine: 10, EndLine: 12}}
		assignment := queueOneReviewVerification(t, harness)
		finding := currentReviewFinding(t, harness, assignment.FindingID)
		location := finding.Location
		payload := ReviewVerificationPayload{
			FindingID:        finding.ID,
			Outcome:          ReviewVerificationConfirmed,
			ScopeDisposition: ReviewScopeInScope,
			PatchDisposition: ReviewPatchIntroduced,
			Location:         &location,
			BehavioralPath:   finding.BehavioralPath,
			Evidence: []ReviewEvidence{{
				Summary: "behavior exists at head",
				Path:    finding.Location.Path,
			}},
			CausalEvidence: []ReviewEvidence{{
				Summary:   "same file but unrelated line",
				Path:      finding.Location.Path,
				StartLine: 1,
				EndLine:   1,
			}},
			TestEvidence: []ReviewEvidence{{
				Summary: "behavior reproduced",
				Path:    finding.Location.Path,
			}},
		}
		if err := validateReviewVerificationEvidence(
			harness.reviewer.ReviewCycle,
			assignment,
			payload,
		); err == nil || !strings.Contains(err.Error(), "exact changed line") {
			t.Fatalf("unrelated causal evidence error = %v", err)
		}
	})
}

func TestReviewWorkerPromptsBindTaskIntentAndPatchCausality(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	current, _ := harness.agents.Get(harness.reviewer.ID)
	current.ReviewCycle.Plan.TaskScope = []string{
		"Route JSON handlers through the bounded decoder.",
	}
	current.ReviewCycle.Plan.NonGoals = []string{
		"Change multipart upload policy.",
	}
	current.ReviewCycle.Plan.AcceptanceCriteria = []string{
		"Oversized JSON returns the documented error.",
	}

	discoveryPrompt, err := reviewDiscoveryLanePrompt(
		current,
		harness.ownership.Identity,
	)
	if err != nil {
		t.Fatalf("reviewDiscoveryLanePrompt() error = %v", err)
	}
	for _, required := range []string{
		"Route JSON handlers through the bounded decoder.",
		"Change multipart upload policy.",
		"Oversized JSON returns the documented error.",
		"base-to-head patch introduced or worsened",
		"pre-existing behavior",
		"non-blocking observations",
		reviewPullRequestDiffRange(
			current.ReviewCycle.Plan.BaseSHA,
			current.ReviewCycle.Plan.HeadSHA,
		),
		"Do not use a direct two-endpoint diff",
	} {
		if !strings.Contains(discoveryPrompt, required) {
			t.Fatalf("discovery prompt does not contain %q: %s", required, discoveryPrompt)
		}
	}

	assignments, err := queueReviewVerificationAssignments(
		current.ReviewCycle,
		1,
		time.Now().UTC(),
	)
	if err != nil || len(assignments) == 0 {
		t.Fatalf("queueReviewVerificationAssignments() = %#v, %v", assignments, err)
	}
	verificationPrompt, err := reviewVerificationPrompt(
		current,
		assignments[0],
	)
	if err != nil {
		t.Fatalf("reviewVerificationPrompt() error = %v", err)
	}
	for _, required := range []string{
		"scope_disposition",
		"patch_disposition",
		"causal_evidence",
		"already present at base",
		"must be rejected as non-blocking",
		reviewPullRequestDiffRange(
			current.ReviewCycle.Plan.BaseSHA,
			current.ReviewCycle.Plan.HeadSHA,
		),
		"Do not use a direct two-endpoint diff",
	} {
		if !strings.Contains(verificationPrompt, required) {
			t.Fatalf("verification prompt does not contain %q: %s", required, verificationPrompt)
		}
	}
}

func TestReviewDiscoveryLanePromptsHaveDistinctCompleteScopes(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	current, _ := harness.agents.Get(harness.reviewer.ID)
	responsibilities := map[string]string{
		"contract":             "task contract and acceptance criteria",
		"callers":              "changed symbols, their callers, and downstream consumers",
		"lifecycle":            "lifecycle behavior and state transitions",
		"persistence-recovery": "durable state, restart recovery, and replay",
		"concurrency-ordering": "concurrency, ordering, cancellation, and race safety",
		"operations-tests":     "scale, operational behavior, and regression tests",
	}
	seen := make(map[string]struct{}, len(responsibilities))
	for lane, responsibility := range responsibilities {
		identity := harness.ownership.Identity
		identity.Lane = lane
		prompt, err := reviewDiscoveryLanePrompt(current, identity)
		if err != nil {
			t.Fatalf("reviewDiscoveryLanePrompt(%s) error = %v", lane, err)
		}
		for _, required := range []string{
			responsibility,
			`"hypotheses"`,
			`"checklist"`,
			`"evidence_requirements"`,
			"Do not stop after the first finding",
			"Do not repeat work assigned to other lanes",
			"unreviewed_areas",
		} {
			if !strings.Contains(prompt, required) {
				t.Fatalf("%s prompt does not contain %q: %s", lane, required, prompt)
			}
		}
		executionBoundary := "Do not run repository-wide test commands"
		if lane == "operations-tests" {
			executionBoundary = "Run each broad repository test suite at most once"
		}
		if !strings.Contains(prompt, executionBoundary) {
			t.Fatalf(
				"%s prompt does not contain execution boundary %q: %s",
				lane,
				executionBoundary,
				prompt,
			)
		}
		if _, duplicate := seen[responsibility]; duplicate {
			t.Fatalf("duplicate discovery responsibility %q", responsibility)
		}
		seen[responsibility] = struct{}{}
	}
}

func TestReviewSynthesisPromptIncludesAllDiscoveryEvidence(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 1, false)
	current, _ := harness.agents.Get(harness.reviewer.ID)
	identity := harness.ownership.Identity
	identity.Lane = reviewSynthesisLane
	prompt, err := reviewDiscoveryLanePrompt(current, identity)
	if err != nil {
		t.Fatalf("reviewDiscoveryLanePrompt(synthesis) error = %v", err)
	}
	for _, required := range []string{
		`"lane_artifacts"`,
		`"canonical_candidates"`,
		`"coverage_gaps"`,
		`"finding_verifications"`,
		"convergence discovery",
		`"failed_lanes"`,
		"Do not repeat completed lane work",
		"perform that lane's supplied brief yourself",
		"all lane disclosures and supplied evidence were reconciled",
		"unreviewed_areas",
		reviewPullRequestDiffRange(
			current.ReviewCycle.Plan.BaseSHA,
			current.ReviewCycle.Plan.HeadSHA,
		),
	} {
		if !strings.Contains(prompt, required) {
			t.Fatalf("synthesis prompt does not contain %q: %s", required, prompt)
		}
	}
}

func TestIncompleteAnalysisPublishesConfirmedFindings(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	completeConvergenceVerification(
		t,
		harness,
		assignment,
		ReviewVerificationConfirmed,
	)
	current, _ := harness.agents.Get(harness.reviewer.ID)
	current.ReviewCycle.Convergence.Status = ReviewConvergenceUnresolved
	if !reviewCycleHasPublishableReport(current.ReviewCycle) {
		t.Fatal("incomplete review with confirmed findings is not publishable")
	}
	aggregation, err := buildReviewVerdictAggregation(current.ReviewCycle)
	if err != nil {
		t.Fatalf("buildReviewVerdictAggregation() error = %v", err)
	}
	if aggregation.ResultState != ReviewCycleResultPartialWithFindings ||
		len(aggregation.Findings) != 1 {
		t.Fatalf("partial finding aggregation = %#v", aggregation)
	}
	body, verdict := formatReviewVerdictPublicationBody(
		current.ID,
		"partial-finding-publication",
		strings.Repeat("a", 64),
		aggregation,
	)
	if verdict != ReviewVerdictNeedsChanges ||
		!strings.Contains(body, "Review changes required — partial coverage") ||
		!strings.Contains(body, aggregation.Findings[0].Summary) {
		t.Fatalf("partial finding report = verdict:%q\n%s", verdict, body)
	}
}

func TestConfirmedFindingsEndReviewWithoutCoverageChallenges(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 1, false)
	assignment := queueOneReviewVerification(t, harness)
	completeConvergenceVerification(
		t,
		harness,
		assignment,
		ReviewVerificationConfirmed,
	)
	current, _ := harness.agents.Get(harness.reviewer.ID)
	if len(current.ReviewCycle.UnresolvedCoverage) == 0 ||
		!reviewCycleReadyForChangesRequiredVerdict(current.ReviewCycle) {
		t.Fatalf(
			"confirmed finding did not establish a decisive verdict: %#v",
			current.ReviewCycle,
		)
	}
	completed, err := harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRound() error = %v", err)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if completed.Outcome != ReviewConvergenceRoundChangesRequired ||
		current.ReviewCycle.Convergence.Status !=
			ReviewConvergenceChangesRequired ||
		!reviewCycleHasPublishableReport(current.ReviewCycle) {
		t.Fatalf(
			"changes-required terminal state = round:%#v convergence:%#v",
			completed,
			current.ReviewCycle.Convergence,
		)
	}
}

func TestReviewFindingRevisionTracksClaimNotDiscoveryBookkeeping(t *testing.T) {
	finding := ReviewCanonicalFinding{
		ID:                "finding-claim",
		Fingerprint:       strings.Repeat("a", 64),
		ExactSHA:          testReviewHeadSHA,
		Summary:           "changed branch loses an acknowledgement",
		Location:          ReviewFindingLocation{Path: "internal/review.go", Symbol: "publish", StartLine: 42, EndLine: 42},
		BehavioralPath:    "publish returns before acknowledgement",
		ViolatedInvariant: "publication must be acknowledged before completion",
		Severity:          ReviewFindingSeverityMedium,
		Confidence:        ReviewFindingConfidenceMedium,
		Evidence:          []ReviewEvidence{{Summary: "first observation", Path: "internal/review.go", StartLine: 42, EndLine: 42}},
		Provenance:        []ReviewFindingProvenance{{WorkerID: "worker-1", Lane: "contract", Pass: 1, CandidateID: "candidate-1"}},
	}
	initial, err := reviewFindingCandidateRevision(finding)
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision() error = %v", err)
	}
	finding.Provenance = append(finding.Provenance, ReviewFindingProvenance{WorkerID: "worker-2", Lane: "synthesis", Pass: 2, CandidateID: "candidate-2"})
	finding.Severity = ReviewFindingSeverityHigh
	finding.Confidence = ReviewFindingConfidenceHigh
	rediscovered, err := reviewFindingCandidateRevision(finding)
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision(rediscovered) error = %v", err)
	}
	if rediscovered != initial {
		t.Fatal("corroborating discovery changed the causal finding revision")
	}
	finding.Evidence = append(finding.Evidence, ReviewEvidence{Summary: "new causal evidence", Path: "internal/review.go", StartLine: 43, EndLine: 43})
	withNewEvidence, err := reviewFindingCandidateRevision(finding)
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision(new evidence) error = %v", err)
	}
	if withNewEvidence == initial {
		t.Fatal("new causal evidence did not change the finding revision")
	}
	finding.ViolatedInvariant = "publication must be durable before completion"
	changed, err := reviewFindingCandidateRevision(finding)
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision(changed claim) error = %v", err)
	}
	if changed == withNewEvidence {
		t.Fatal("a changed causal claim kept the prior finding revision")
	}
}

func TestReviewVerificationSchedulerPersistsBeforeBoundedRetriedLaunch(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 3, true)
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	var mu sync.Mutex
	active := 0
	maxActive := 0
	attempts := make(map[string]int)
	harness.bot.reviewVerificationRunner = func(
		_ context.Context,
		_ string,
		assignment ReviewVerificationAssignment,
	) error {
		current, ok := harness.agents.Get(harness.reviewer.ID)
		if !ok || current.ReviewCycle == nil {
			return errors.New("coordinator disappeared")
		}
		stored, ok := reviewVerificationAssignmentByID(
			current.ReviewCycle,
			assignment.ID,
		)
		if !ok || stored.Status != ReviewVerificationRunning {
			return errors.New(
				"verification launched before its assignment was persisted",
			)
		}
		mu.Lock()
		active++
		attempts[assignment.ID]++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		started <- struct{}{}
		<-release
		mu.Lock()
		active--
		mu.Unlock()
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureArtifact,
			newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureIO,
			),
		)
	}
	done := make(chan error, 1)
	go func() {
		_, err := harness.bot.runReviewVerificationRound(
			context.Background(),
			harness.reviewer.ID,
			round.Round,
		)
		done <- err
	}()
	parallelism :=
		harness.reviewer.ReviewCycle.Policy.Verification.MaxParallelVerifiers
	for index := 0; index < parallelism; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("verification workers did not fill bounded slots")
		}
	}
	select {
	case <-started:
		t.Fatal("verification parallelism exceeded the snapshotted limit")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("contained verifier failures escaped the batch: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("verification scheduler did not drain")
	}
	mu.Lock()
	defer mu.Unlock()
	if maxActive > parallelism || active != 0 {
		t.Fatalf(
			"verification concurrency = max:%d active:%d limit:%d",
			maxActive,
			active,
			parallelism,
		)
	}
	for assignmentID, count := range attempts {
		if count != harness.reviewer.ReviewCycle.Policy.Verification.Retries+1 {
			t.Fatalf(
				"verification attempts for %q = %d, want %d",
				assignmentID,
				count,
				harness.reviewer.ReviewCycle.Policy.Verification.Retries+1,
			)
		}
	}
}

func TestFailedVerifierDoesNotEraseIndependentVerifiedFinding(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 2, true)
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	failedFindingID := current.ReviewCycle.CanonicalFindings[0].ID
	verifiedFindingID := current.ReviewCycle.CanonicalFindings[1].ID
	acceptedBefore := len(current.ReviewCycle.ArtifactReceipts)
	var publishMu sync.Mutex
	harness.bot.reviewVerificationRunner = func(
		_ context.Context,
		_ string,
		assignment ReviewVerificationAssignment,
	) error {
		if assignment.FindingID == failedFindingID {
			return errors.New("verifier runtime failed")
		}
		publishMu.Lock()
		defer publishMu.Unlock()
		completeConvergenceVerification(
			t,
			harness,
			assignment,
			ReviewVerificationConfirmed,
		)
		return nil
	}
	assignments, err := harness.bot.runReviewVerificationRound(
		context.Background(),
		harness.reviewer.ID,
		round.Round,
	)
	if err != nil || len(assignments) == 0 {
		t.Fatalf("mixed verification batch = %#v, %v", assignments, err)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	statuses := make(map[string]ReviewFindingVerificationStatus)
	for _, decision := range current.ReviewCycle.Convergence.FindingVerifications {
		statuses[decision.FindingID] = decision.Status
	}
	if statuses[failedFindingID] != ReviewFindingVerificationUnresolved ||
		statuses[verifiedFindingID] != ReviewFindingVerificationConfirmed {
		t.Fatalf("mixed verification decisions = %#v", statuses)
	}
	publishable := current.ReviewCycle.PublishableFindings()
	if len(publishable) != 1 || publishable[0].ID != verifiedFindingID {
		t.Fatalf("publishable findings after verifier failure = %#v", publishable)
	}
	if len(current.ReviewCycle.ArtifactReceipts) <= acceptedBefore {
		t.Fatal("accepted verification receipts were erased by a failed sibling")
	}
}

func TestReviewVerificationCoordinatorDeadlineCancelsSetupWithoutRuntimeRetry(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	runner := &testIsolatedReviewWorkerRunner{}
	harness.bot.runner = runner
	harness.bot.prepareReviewWorkerWorktreeFunc = func(
		ctx context.Context,
		_ Agent,
	) error {
		<-ctx.Done()
		return ctx.Err()
	}
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared before launch timeout")
	}
	identity, err := reviewWorkerIdentityForCycle(
		current.ReviewCycle,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}
	retries := current.ReviewCycle.Policy.Verification.Retries
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelSetup()
	runErr := runReviewConvergenceWorkerWithRetries(
		retries,
		func() error {
			return harness.bot.runReviewAssignedWorker(
				setupCtx,
				harness.reviewer.ID,
				identity,
				"verify the assigned candidate",
				ReviewArtifactPhaseVerification,
				func(ReviewWorkerOwnership) error {
					t.Fatal("timed-out launch unexpectedly bound ownership")
					return nil
				},
			)
		},
	)
	if len(runner.started) != 0 || len(runner.stopCalls) != 0 {
		t.Fatalf("runtime starts/stops = %d/%d, want 0/0 for setup failure", len(runner.started), len(runner.stopCalls))
	}
	if !errors.Is(runErr, context.DeadlineExceeded) ||
		reviewAssignedWorkerFailureCode(runErr) !=
			ReviewVerificationFailureCanceled {
		t.Fatalf(
			"setup-time timeout classification = code:%q error:%v",
			reviewAssignedWorkerFailureCode(runErr),
			runErr,
		)
	}
}

func TestReviewVerificationRuntimeTimeoutIsRetryableRuntimeFailure(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	runner := &testIsolatedReviewWorkerRunner{alive: true}
	harness.bot.runner = runner
	const (
		remainingBeforeSetup = 2 * time.Second
		setupDuration        = 600 * time.Millisecond
	)
	harness.bot.prepareReviewWorkerWorktreeFunc = func(
		_ context.Context,
		worker Agent,
	) error {
		time.Sleep(setupDuration)
		return os.MkdirAll(worker.WorktreePath, 0o755)
	}
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared before runtime timeout")
	}
	maxWallTime := time.Duration(
		current.ReviewCycle.Policy.Convergence.MaxWallTimeMinutes,
	) * time.Minute
	startedAt := time.Now().UTC().Add(
		-maxWallTime + remainingBeforeSetup,
	)
	metrics, err := newReviewMetricsState(current.ReviewCycle, startedAt)
	if err != nil {
		t.Fatalf("newReviewMetricsState() error = %v", err)
	}
	harness.agents.mu.Lock()
	harness.agents.agents[harness.reviewer.ID].ReviewCycle.Metrics = metrics
	harness.agents.mu.Unlock()
	identity, err := reviewWorkerIdentityForCycle(
		current.ReviewCycle,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}
	runErr := harness.bot.runReviewAssignedWorker(
		context.Background(),
		harness.reviewer.ID,
		identity,
		"verify the assigned candidate",
		ReviewArtifactPhaseVerification,
		func(ReviewWorkerOwnership) error { return nil },
	)
	if reviewAssignedWorkerFailureCode(runErr) != ReviewVerificationFailureRuntime {
		t.Fatalf("runtime timeout code = %q, error=%v", reviewAssignedWorkerFailureCode(runErr), runErr)
	}
	var runtimeTimeout *reviewWorkerRuntimeTimeoutError
	if !errors.As(runErr, &runtimeTimeout) ||
		!reviewConvergenceWorkerFailureIsRetryable(runErr) {
		t.Fatalf("runtime timeout retry classification = %v", runErr)
	}
	if runtimeTimeout.timeout <= 0 ||
		runtimeTimeout.timeout >= remainingBeforeSetup-setupDuration+100*time.Millisecond {
		t.Fatalf(
			"runtime allowance after %s setup = %s, want setup time deducted from %s remaining cycle budget",
			setupDuration,
			runtimeTimeout.timeout,
			remainingBeforeSetup,
		)
	}
	if len(runner.started) != 1 || len(runner.stopCalls) != 1 {
		t.Fatalf("runtime starts/stops = %d/%d, want 1/1", len(runner.started), len(runner.stopCalls))
	}
	stored, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after runtime timeout")
	}
	if len(stored.ReviewCycle.ArtifactFailures) != 0 {
		t.Fatalf("runtime timeout was misreported as artifact failure: %#v", stored.ReviewCycle.ArtifactFailures)
	}
	ownership, found := reviewCycleRecoveryOwnership(
		stored.ReviewCycle,
		reviewCycleRecoveryWork{Identity: identity},
	)
	if !found || ownership.Failure == nil ||
		!ownership.Failure.Retryable {
		t.Fatalf(
			"persisted runtime timeout retryability = %#v",
			ownership,
		)
	}
}

func TestTargetedChallengesRejectDuplicatesAndReverifyNewCandidates(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 1, false)
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	targets := buildReviewChallengeTargets(current.ReviewCycle)
	if len(targets) == 0 ||
		targets[0].Kind != ReviewChallengeCoverageGap {
		t.Fatalf("targeted coverage challenges = %#v", targets)
	}
	target := targets[0]
	assignment, err := harness.bot.queueTargetedReviewChallenge(
		harness.reviewer.ID,
		round.Round,
		target,
	)
	if err != nil {
		t.Fatalf("queueTargetedReviewChallenge() error = %v", err)
	}
	if _, err := harness.bot.queueTargetedReviewChallenge(
		harness.reviewer.ID,
		round.Round,
		target,
	); err == nil || !strings.Contains(err.Error(), "identical") {
		t.Fatalf(
			"repeated identical challenge error = %v",
			err,
		)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	prompt, err := reviewChallengePrompt(
		current,
		assignment,
		"FULL-DIFF-SENTINEL",
	)
	if err != nil {
		t.Fatalf("reviewChallengePrompt() error = %v", err)
	}
	for _, required := range []string{
		target.ID,
		target.Description,
		"FULL-DIFF-SENTINEL",
		"coverage_gaps",
		"verifications",
		reviewPullRequestDiffRange(
			current.ReviewCycle.Plan.BaseSHA,
			current.ReviewCycle.Plan.HeadSHA,
		),
	} {
		if !strings.Contains(prompt, required) {
			t.Fatalf(
				"targeted challenge prompt does not contain %q",
				required,
			)
		}
	}
	if err := harness.bot.markReviewChallengeRunning(
		harness.reviewer.ID,
		assignment.ID,
		nil,
	); err != nil {
		t.Fatalf("markReviewChallengeRunning(initial) error = %v", err)
	}
	ownership, store := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleChallenge,
		assignment.Round,
		assignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewChallengeRunning(
				harness.reviewer.ID,
				assignment.ID,
				&ownership,
			)
		},
	)
	beforeGapCount := len(current.ReviewCycle.UnresolvedCoverage)
	candidate := convergenceCandidate(100)
	payload := ReviewChallengePayload{
		Outcome:      ReviewChallengeUpheld,
		Summary:      "targeted challenge found a distinct behavior",
		AssignmentID: assignment.ID,
		TargetKind:   target.Kind,
		TargetID:     target.ID,
		Candidates:   []ReviewFindingCandidate{candidate},
		Coverage: []ReviewCoverageClaim{{
			RequirementID: target.RequirementID,
			Kind:          targetCoverageKind(t, current.ReviewCycle, target),
			Status:        ReviewCoverageCovered,
			Evidence: []ReviewEvidence{{
				Summary: "targeted path was exercised",
				Path:    "tracked.txt",
			}},
		}},
	}
	envelope, err := newReviewArtifactEnvelope(
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		ReviewArtifactPhaseChallenge,
		ReviewArtifactPayload{
			Kind:      ReviewArtifactPayloadChallenge,
			Challenge: &payload,
		},
	)
	if err != nil {
		t.Fatalf("newReviewArtifactEnvelope(challenge) error = %v", err)
	}
	path, err := store.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	)
	if err != nil {
		t.Fatalf("publish(challenge) error = %v", err)
	}
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		ownership.OwnerID,
		path,
		ReviewArtifactPhaseChallenge,
	); err != nil {
		t.Fatalf("intake(challenge) error = %v", err)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if len(current.ReviewCycle.UnresolvedCoverage) != beforeGapCount-1 {
		t.Fatalf(
			"evidenced targeted challenge gaps = %d, want %d",
			len(current.ReviewCycle.UnresolvedCoverage),
			beforeGapCount-1,
		)
	}
	if len(current.ReviewCycle.CanonicalFindings) != 2 {
		t.Fatalf(
			"challenge candidates = %#v",
			current.ReviewCycle.CanonicalFindings,
		)
	}
	pending := 0
	for _, decision := range current.ReviewCycle.Convergence.FindingVerifications {
		if decision.Status == ReviewFindingVerificationPending {
			pending++
		}
	}
	if pending != 2 || len(current.ReviewCycle.PublishableFindings()) != 0 {
		t.Fatalf(
			"challenge candidates bypassed verification: decisions=%#v",
			current.ReviewCycle.Convergence.FindingVerifications,
		)
	}
}

func TestChallengeWithNewEvidenceReopensVerificationAcrossRestart(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 1, false)
	originalAssignment := queueOneReviewVerification(t, harness)
	verifier, verifierStore := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleVerifier,
		originalAssignment.Round,
		originalAssignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewVerificationRunning(
				harness.reviewer.ID,
				originalAssignment.ID,
				&ownership,
			)
		},
	)
	originalFinding := currentReviewFinding(
		t,
		harness,
		originalAssignment.FindingID,
	)
	verification := verificationEnvelope(
		t,
		harness,
		verifier,
		originalFinding,
		ReviewVerificationRejected,
	)
	verificationPath, err := verifierStore.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		verifier,
		verification,
	)
	if err != nil {
		t.Fatalf("publish(original verification) error = %v", err)
	}
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		verifier.OwnerID,
		verificationPath,
		ReviewArtifactPhaseVerification,
	); err != nil {
		t.Fatalf("intake(original verification) error = %v", err)
	}

	current, _ := harness.agents.Get(harness.reviewer.ID)
	if current.ReviewCycle.Convergence.FindingVerifications[0].Status !=
		ReviewFindingVerificationRejected {
		t.Fatal("original candidate was not terminally rejected")
	}
	targets := buildReviewChallengeTargets(current.ReviewCycle)
	if len(targets) == 0 ||
		targets[0].Kind != ReviewChallengeCoverageGap {
		t.Fatalf("challenge targets = %#v, want a coverage gap", targets)
	}
	challenge, err := harness.bot.queueTargetedReviewChallenge(
		harness.reviewer.ID,
		originalAssignment.Round,
		targets[0],
	)
	if err != nil {
		t.Fatalf("queueTargetedReviewChallenge() error = %v", err)
	}
	equivalent := convergenceCandidate(1)
	equivalent.CandidateID = "challenge-equivalent-candidate"
	equivalent.Evidence = append(equivalent.Evidence, ReviewEvidence{
		Summary:   "challenge observed the causal branch independently",
		Path:      "tracked.txt",
		StartLine: 2,
		EndLine:   2,
	})
	challengeOwner := completeConvergenceChallenge(
		t,
		harness,
		challenge,
		ReviewChallengePayload{
			Outcome:      ReviewChallengeUpheld,
			Summary:      "challenge supplied new causal evidence",
			AssignmentID: challenge.ID,
			TargetKind:   challenge.Target.Kind,
			TargetID:     challenge.Target.ID,
			Candidates:   []ReviewFindingCandidate{equivalent},
			Coverage: []ReviewCoverageClaim{{
				RequirementID: challenge.Target.RequirementID,
				Kind: targetCoverageKind(
					t,
					current.ReviewCycle,
					challenge.Target,
				),
				Status: ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary: "challenge exercised the targeted path",
					Path:    "tracked.txt",
				}},
			}},
		},
	)
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if len(current.ReviewCycle.CanonicalFindings) != 1 ||
		len(current.ReviewCycle.CanonicalFindings[0].Provenance) != 2 {
		t.Fatalf(
			"challenged finding canonicalization = %#v",
			current.ReviewCycle.CanonicalFindings,
		)
	}
	storedOriginal, ok := reviewVerificationAssignmentByID(
		current.ReviewCycle,
		originalAssignment.ID,
	)
	if !ok || !storedOriginal.Superseded {
		t.Fatalf(
			"historical verification assignment = %#v, want superseded",
			storedOriginal,
		)
	}
	decision := current.ReviewCycle.Convergence.FindingVerifications[0]
	if decision.Status != ReviewFindingVerificationPending ||
		decision.CandidateRevision == originalAssignment.CandidateRevision {
		t.Fatalf(
			"new challenge evidence did not reopen verification: %#v",
			decision,
		)
	}
	if stringSliceContains(
		originalAssignment.ExcludedWorkerIDs,
		challengeOwner.OwnerID,
	) {
		t.Fatal("historical assignment unexpectedly changed its exclusions")
	}
	if err := validatePersistedReviewCycleSnapshot(
		current.ReviewCycle,
	); err != nil {
		t.Fatalf(
			"validatePersistedReviewCycleSnapshot(challenged) error = %v",
			err,
		)
	}

	restarted := &Orchestrator{
		cfg:    harness.bot.cfg,
		agents: NewAgentManager(),
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	restored, ok := restarted.agents.Get(harness.reviewer.ID)
	if !ok || restored.ReviewCycle == nil {
		t.Fatal("challenged review cycle did not survive restart")
	}
	restoredDecision :=
		restored.ReviewCycle.Convergence.FindingVerifications[0]
	if restoredDecision.Status != ReviewFindingVerificationPending ||
		restoredDecision.CandidateRevision != decision.CandidateRevision {
		t.Fatalf(
			"restored candidate decision = %#v, want pending revision %q",
			restoredDecision,
			decision.CandidateRevision,
		)
	}
	restoredOriginal, ok := reviewVerificationAssignmentByID(
		restored.ReviewCycle,
		originalAssignment.ID,
	)
	if !ok || !restoredOriginal.Superseded {
		t.Fatalf(
			"restored historical assignment = %#v, want superseded",
			restoredOriginal,
		)
	}
	reverify, err := restarted.queueReviewVerificationRound(
		harness.reviewer.ID,
		originalAssignment.Round,
	)
	if err != nil {
		t.Fatalf("queueReviewVerificationRound(reopened) error = %v", err)
	}
	if len(reverify) != 1 ||
		reverify[0].Ordinal != originalAssignment.Ordinal+1 ||
		reverify[0].CandidateRevision != decision.CandidateRevision ||
		!stringSliceContains(
			reverify[0].ExcludedWorkerIDs,
			challengeOwner.OwnerID,
		) {
		t.Fatalf(
			"reopened verification assignment = %#v",
			reverify,
		)
	}
}

func TestCompetingHypothesisRequiresCandidateAndReentersVerification(
	t *testing.T,
) {
	t.Run("empty conclusive artifact is rejected", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		challenge, _ := queueInconclusiveFindingChallenge(t, harness)
		if err := harness.bot.markReviewChallengeRunning(
			harness.reviewer.ID,
			challenge.ID,
			nil,
		); err != nil {
			t.Fatalf("markReviewChallengeRunning(initial) error = %v", err)
		}
		ownership, _ := bindConvergenceWorker(
			t,
			harness,
			AgentProfileRoleChallenge,
			challenge.Round,
			challenge.Lane,
			func(ownership ReviewWorkerOwnership) error {
				return harness.bot.markReviewChallengeRunning(
					harness.reviewer.ID,
					challenge.ID,
					&ownership,
				)
			},
		)
		payload := ReviewChallengePayload{
			Outcome:      ReviewChallengeOverturned,
			Summary:      "targeted hypothesis was conclusive",
			AssignmentID: challenge.ID,
			TargetKind:   challenge.Target.Kind,
			TargetID:     challenge.Target.ID,
			Candidates:   []ReviewFindingCandidate{},
			Coverage:     []ReviewCoverageClaim{},
		}
		envelope, err := newReviewArtifactEnvelope(
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			ReviewArtifactPhaseChallenge,
			ReviewArtifactPayload{
				Kind:      ReviewArtifactPayloadChallenge,
				Challenge: &payload,
			},
		)
		if err != nil {
			t.Fatalf("newReviewArtifactEnvelope(challenge) error = %v", err)
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		err = completeReviewChallengeFromReceipt(
			current.ReviewCycle,
			ownership,
			envelope,
			time.Now().UTC(),
		)
		artifactErr := asReviewArtifactError(err)
		if artifactErr == nil ||
			artifactErr.Code != ReviewArtifactFailureMalformed ||
			!strings.Contains(
				artifactErr.Detail,
				"does not reproduce its assigned competing hypothesis",
			) {
			t.Fatalf(
				"empty conclusive hypothesis error = %v, want detailed malformed",
				err,
			)
		}
	})

	t.Run("target evidence creates a revision for verification", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		challenge, originalDecision :=
			queueInconclusiveFindingChallenge(t, harness)
		candidate := convergenceCandidate(1)
		candidate.CandidateID = "competing-hypothesis-evidence"
		candidate.Evidence = append(
			candidate.Evidence,
			ReviewEvidence{
				Summary:   "challenge independently reproduced the target",
				Path:      "tracked.txt",
				StartLine: 1,
				EndLine:   1,
			},
		)
		challengeOwner := completeConvergenceChallenge(
			t,
			harness,
			challenge,
			ReviewChallengePayload{
				Outcome:      ReviewChallengeOverturned,
				Summary:      "new evidence resolves the competing hypothesis",
				AssignmentID: challenge.ID,
				TargetKind:   challenge.Target.Kind,
				TargetID:     challenge.Target.ID,
				Candidates:   []ReviewFindingCandidate{candidate},
				Coverage:     []ReviewCoverageClaim{},
			},
		)
		current, _ := harness.agents.Get(harness.reviewer.ID)
		decision := current.ReviewCycle.Convergence.FindingVerifications[0]
		if decision.FindingID != challenge.Target.ID ||
			decision.Status != ReviewFindingVerificationPending ||
			decision.CandidateRevision == originalDecision.CandidateRevision {
			t.Fatalf(
				"challenged hypothesis verification state = %#v, original=%#v",
				decision,
				originalDecision,
			)
		}
		reverify, err := harness.bot.queueReviewVerificationRound(
			harness.reviewer.ID,
			challenge.Round,
		)
		if err != nil {
			t.Fatalf("queueReviewVerificationRound(challenge) error = %v", err)
		}
		if len(reverify) != 1 ||
			reverify[0].CandidateRevision != decision.CandidateRevision ||
			!stringSliceContains(
				reverify[0].ExcludedWorkerIDs,
				challengeOwner.OwnerID,
			) {
			t.Fatalf(
				"competing-hypothesis verification assignment = %#v",
				reverify,
			)
		}
	})
}

func TestTargetedChallengeSkipsPartiallyEvidencedGapWithinRound(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 0, false)
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	targets := buildReviewChallengeTargets(current.ReviewCycle)
	if len(targets) < 2 ||
		targets[0].Kind != ReviewChallengeCoverageGap ||
		targets[1].Kind != ReviewChallengeCoverageGap {
		t.Fatalf("coverage challenge targets = %#v, want at least two", targets)
	}
	first, err := harness.bot.queueTargetedReviewChallenge(
		harness.reviewer.ID,
		round.Round,
		targets[0],
	)
	if err != nil {
		t.Fatalf("queueTargetedReviewChallenge(first) error = %v", err)
	}
	completeConvergenceChallenge(
		t,
		harness,
		first,
		ReviewChallengePayload{
			Outcome:      ReviewChallengeUpheld,
			Summary:      "targeted evidence reduced but did not close the gap",
			AssignmentID: first.ID,
			TargetKind:   first.Target.Kind,
			TargetID:     first.Target.ID,
			Candidates:   []ReviewFindingCandidate{},
			Coverage: []ReviewCoverageClaim{{
				RequirementID: first.Target.RequirementID,
				Kind: targetCoverageKind(
					t,
					current.ReviewCycle,
					first.Target,
				),
				Status: ReviewCoveragePartial,
				Evidence: []ReviewEvidence{{
					Summary: "one branch was exercised",
					Path:    "tracked.txt",
				}},
			}},
		},
	)
	current, _ = harness.agents.Get(harness.reviewer.ID)
	stillOpen := false
	for _, gap := range current.ReviewCycle.UnresolvedCoverage {
		if gap.ID == first.Target.ID &&
			gap.Status == ReviewCoverageGapPartial {
			stillOpen = true
			break
		}
	}
	if !stillOpen {
		t.Fatalf(
			"partially evidenced target did not remain open: %#v",
			current.ReviewCycle.UnresolvedCoverage,
		)
	}
	if _, err := harness.bot.queueTargetedReviewChallenge(
		harness.reviewer.ID,
		round.Round,
		first.Target,
	); err == nil || !strings.Contains(err.Error(), "already attempted") {
		t.Fatalf(
			"same-round changed-context target error = %v",
			err,
		)
	}
	var selected ReviewChallengeAssignment
	harness.bot.reviewChallengeRunner = func(
		_ context.Context,
		_ string,
		assignment ReviewChallengeAssignment,
	) error {
		selected = assignment
		return errors.New("selection sentinel")
	}
	next, err := harness.bot.runNextTargetedReviewChallenge(
		context.Background(),
		harness.reviewer.ID,
		round.Round,
	)
	if err != nil {
		t.Fatalf("contained targeted challenge failure escaped: %v", err)
	}
	if next.ID == "" ||
		selected.ID != next.ID ||
		next.Target.ID == first.Target.ID {
		t.Fatalf(
			"next targeted challenge = %#v, selected=%#v",
			next,
			selected,
		)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	failed, ok := reviewChallengeAssignmentByID(current.ReviewCycle, next.ID)
	if !ok || failed.Status != ReviewChallengeFailed {
		t.Fatalf("selected challenge failure state = %#v", failed)
	}
	ids := make(map[string]struct{})
	for _, assignment := range current.ReviewCycle.Convergence.ChallengeAssignments {
		if _, duplicate := ids[assignment.ID]; duplicate {
			t.Fatalf(
				"duplicate challenge assignment ID %q persisted",
				assignment.ID,
			)
		}
		ids[assignment.ID] = struct{}{}
	}
}

func TestTargetedReviewChallengesRunAsOneParallelBatch(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, false)
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	if targets := buildReviewChallengeTargets(current.ReviewCycle); len(targets) < 2 {
		t.Fatalf("challenge targets = %#v, want at least two", targets)
	}
	started := make(chan string, 32)
	release := make(chan struct{})
	harness.bot.reviewChallengeRunner = func(
		_ context.Context,
		_ string,
		assignment ReviewChallengeAssignment,
	) error {
		started <- assignment.ID
		<-release
		return errors.New("parallel challenge sentinel")
	}
	type result struct {
		assignments []ReviewChallengeAssignment
		err         error
	}
	done := make(chan result, 1)
	go func() {
		assignments, err := harness.bot.runTargetedReviewChallengeRound(
			context.Background(),
			harness.reviewer.ID,
			round.Round,
		)
		done <- result{assignments: assignments, err: err}
	}()
	first := ""
	second := ""
	for index := 0; index < 2; index++ {
		select {
		case id := <-started:
			if index == 0 {
				first = id
			} else {
				second = id
			}
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("challenge batch did not start two workers concurrently")
		}
	}
	if first == second {
		close(release)
		t.Fatalf("parallel challenge workers repeated assignment %q", first)
	}
	close(release)
	select {
	case got := <-done:
		if len(got.assignments) < 2 ||
			got.err != nil {
			t.Fatalf(
				"runTargetedReviewChallengeRound() = %#v, %v",
				got.assignments,
				got.err,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parallel challenge batch did not drain")
	}
}

func TestFailedChallengeDoesNotBlockSuccessfulCandidateVerification(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, false)
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	targets := buildReviewChallengeTargets(current.ReviewCycle)
	if len(targets) < 2 {
		t.Fatalf("challenge targets = %#v, want at least two", targets)
	}
	failedTarget := targets[0]
	materialTarget := targets[1]
	acceptedBefore := len(current.ReviewCycle.ArtifactReceipts)
	var publishMu sync.Mutex
	harness.bot.reviewChallengeRunner = func(
		_ context.Context,
		_ string,
		assignment ReviewChallengeAssignment,
	) error {
		if assignment.Target.ID == failedTarget.ID {
			return errors.New("challenge launch failed")
		}
		current, _ := harness.agents.Get(harness.reviewer.ID)
		candidates := []ReviewFindingCandidate{}
		if assignment.Target.ID == materialTarget.ID {
			candidates = append(candidates, convergenceCandidate(100))
		}
		payload := ReviewChallengePayload{
			Outcome:      ReviewChallengeUpheld,
			Summary:      "independent target completed",
			AssignmentID: assignment.ID,
			TargetKind:   assignment.Target.Kind,
			TargetID:     assignment.Target.ID,
			Candidates:   candidates,
			Coverage: []ReviewCoverageClaim{{
				RequirementID: assignment.Target.RequirementID,
				Kind: targetCoverageKind(
					t,
					current.ReviewCycle,
					assignment.Target,
				),
				Status: ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary: "targeted path was exercised",
					Path:    "tracked.txt",
				}},
			}},
		}
		publishMu.Lock()
		defer publishMu.Unlock()
		completeConvergenceChallenge(t, harness, assignment, payload)
		return nil
	}
	assignments, err := harness.bot.runTargetedReviewChallengeRound(
		context.Background(),
		harness.reviewer.ID,
		round.Round,
	)
	if err != nil {
		t.Fatalf("contained challenge failure escaped the batch: %v", err)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	var failed *ReviewChallengeAssignment
	for _, assignment := range assignments {
		if assignment.Target.ID == failedTarget.ID {
			failed, _ = reviewChallengeAssignmentByID(
				current.ReviewCycle,
				assignment.ID,
			)
			break
		}
	}
	if failed == nil || failed.Status != ReviewChallengeFailed {
		t.Fatalf("failed challenge state = %#v", failed)
	}
	if len(current.ReviewCycle.ArtifactReceipts) <= acceptedBefore {
		t.Fatal("successful challenge receipts were suppressed by its failed sibling")
	}
	failedTargetUnresolved := false
	for _, gap := range current.ReviewCycle.UnresolvedCoverage {
		if gap.RequirementID == failedTarget.RequirementID {
			failedTargetUnresolved = true
		}
		if gap.RequirementID == materialTarget.RequirementID {
			t.Fatalf("successful challenge target remained unresolved: %#v", gap)
		}
	}
	if !failedTargetUnresolved {
		t.Fatal("failed challenge target did not remain unresolved")
	}

	harness.bot.reviewVerificationRunner = func(
		_ context.Context,
		_ string,
		assignment ReviewVerificationAssignment,
	) error {
		publishMu.Lock()
		defer publishMu.Unlock()
		completeConvergenceVerification(
			t,
			harness,
			assignment,
			ReviewVerificationConfirmed,
		)
		return nil
	}
	verifications, err := harness.bot.runReviewVerificationRound(
		context.Background(),
		harness.reviewer.ID,
		round.Round,
	)
	if err != nil || len(verifications) == 0 {
		t.Fatalf("verification after mixed challenge batch = %#v, %v", verifications, err)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if got := len(current.ReviewCycle.PublishableFindings()); got != 1 {
		t.Fatalf("publishable findings after verification = %d, want 1", got)
	}
}

func TestTargetedReviewChallengeBatchHonorsRemainingCapacity(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 4, false)
	harness.agents.mu.Lock()
	cycle := harness.agents.agents[harness.reviewer.ID].ReviewCycle
	policy := cycle.Policy
	policy.Swarm.Retries = 0
	policy.Verification.Retries = 0
	policy.Convergence.MaxReviewAgentsPerSHA = 9
	var err error
	policy, err = snapshotEffectiveReviewPolicy(policy)
	if err != nil {
		harness.agents.mu.Unlock()
		t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
	}
	cycle.Policy = policy
	cycle.PolicyVersion = policy.Version
	cycle.PolicyFingerprint = policy.Fingerprint
	cycle.Plan.PolicyFingerprint = policy.Fingerprint
	harness.agents.mu.Unlock()
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	verifications, err := harness.bot.queueReviewVerificationRound(
		harness.reviewer.ID,
		round.Round,
	)
	if err != nil || len(verifications) != 4 {
		t.Fatalf("queueReviewVerificationRound() = %#v, %v", verifications, err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	targets := buildReviewChallengeTargets(current.ReviewCycle)
	if len(targets) < 2 {
		t.Fatalf("challenge targets = %#v, want at least two", targets)
	}
	harness.bot.reviewChallengeRunner = func(
		context.Context,
		string,
		ReviewChallengeAssignment,
	) error {
		return errors.New("bounded challenge sentinel")
	}
	assignments, err := harness.bot.runTargetedReviewChallengeRound(
		context.Background(),
		harness.reviewer.ID,
		round.Round,
	)
	if len(assignments) != 1 || err != nil {
		t.Fatalf(
			"bounded challenge batch = %#v, %v",
			assignments,
			err,
		)
	}
}

func TestInconclusiveChallengeCannotConvergeOrApprove(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, false)
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	targets := buildReviewChallengeTargets(current.ReviewCycle)
	if len(targets) == 0 {
		t.Fatal("review cycle has no coverage target")
	}
	challenge, err := harness.bot.queueTargetedReviewChallenge(
		harness.reviewer.ID,
		round.Round,
		targets[0],
	)
	if err != nil {
		t.Fatalf("queueTargetedReviewChallenge() error = %v", err)
	}
	coverage := make(
		[]ReviewCoverageClaim,
		0,
		len(current.ReviewCycle.Plan.CoverageRequirements),
	)
	for _, requirement := range current.ReviewCycle.Plan.CoverageRequirements {
		coverage = append(coverage, ReviewCoverageClaim{
			RequirementID: requirement.ID,
			Kind:          requirement.Kind,
			Status:        ReviewCoverageCovered,
			Evidence: []ReviewEvidence{{
				Summary: "challenge supplied coverage evidence",
				Path:    "tracked.txt",
			}},
		})
	}
	completeConvergenceChallenge(
		t,
		harness,
		challenge,
		ReviewChallengePayload{
			Outcome:      ReviewChallengeInconclusive,
			Summary:      "coverage evidence did not resolve the challenge",
			AssignmentID: challenge.ID,
			TargetKind:   challenge.Target.Kind,
			TargetID:     challenge.Target.ID,
			Candidates:   []ReviewFindingCandidate{},
			Coverage:     coverage,
		},
	)
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if len(current.ReviewCycle.UnresolvedCoverage) != 0 {
		t.Fatalf(
			"evidenced coverage gaps = %#v, want none",
			current.ReviewCycle.UnresolvedCoverage,
		)
	}
	completed, err := harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRound() error = %v", err)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if completed.Outcome != ReviewConvergenceRoundUnresolved ||
		completed.Action == "" ||
		current.ReviewCycle.Convergence.Status !=
			ReviewConvergenceUnresolved ||
		current.ReviewCycle.Convergence.QuietRounds != 0 {
		t.Fatalf(
			"inconclusive challenge convergence state = round:%#v convergence:%#v",
			completed,
			current.ReviewCycle.Convergence,
		)
	}
	approvalProbe := cloneReviewCycle(current.ReviewCycle)
	approvalProbe.Convergence.Status = ReviewConvergenceConverged
	if approvalProbe.ApprovalEligible() {
		t.Fatal("completed inconclusive challenge became approval-eligible")
	}
}

func TestReviewRoundPersistsMaterialObservedBeforeCompletion(t *testing.T) {
	t.Run("introduced then resolved coverage gap is material", func(t *testing.T) {
		harness := newReviewConvergenceTestHarness(t, 1, true)
		challenge, _ := queueInconclusiveFindingChallenge(t, harness)
		current, _ := harness.agents.Get(harness.reviewer.ID)
		requirement := current.ReviewCycle.Plan.CoverageRequirements[0]
		candidate := convergenceCandidate(1)
		candidate.CandidateID = "challenge-gap-introduction"
		candidate.Evidence = append(candidate.Evidence, ReviewEvidence{
			Summary:   "challenge reproduced the equivalent behavior",
			Path:      "tracked.txt",
			StartLine: 1,
			EndLine:   1,
		})
		completeConvergenceChallenge(
			t,
			harness,
			challenge,
			ReviewChallengePayload{
				Outcome:      ReviewChallengeUpheld,
				Summary:      "challenge exposed an uncovered plan path",
				AssignmentID: challenge.ID,
				TargetKind:   challenge.Target.Kind,
				TargetID:     challenge.Target.ID,
				Candidates:   []ReviewFindingCandidate{candidate},
				Coverage: []ReviewCoverageClaim{{
					RequirementID: requirement.ID,
					Kind:          requirement.Kind,
					Status:        ReviewCoverageNotCovered,
					Evidence: []ReviewEvidence{{
						Summary: "targeted branch was not exercised",
						Path:    "tracked.txt",
					}},
				}},
			},
		)
		current, _ = harness.agents.Get(harness.reviewer.ID)
		if len(current.ReviewCycle.UnresolvedCoverage) != 1 {
			t.Fatalf(
				"introduced coverage gaps = %#v",
				current.ReviewCycle.UnresolvedCoverage,
			)
		}
		gap := current.ReviewCycle.UnresolvedCoverage[0]
		active := current.ReviewCycle.Convergence.Rounds[len(current.ReviewCycle.Convergence.Rounds)-1]
		if !stringSliceContains(active.NewCoverageGapIDs, gap.ID) {
			t.Fatalf(
				"active round material ledger = %#v, want gap %q",
				active,
				gap.ID,
			)
		}

		restarted := &Orchestrator{
			cfg:    harness.bot.cfg,
			agents: NewAgentManager(),
		}
		if err := restarted.loadPersistedAgentState(); err != nil {
			t.Fatalf("loadPersistedAgentState() error = %v", err)
		}
		restored, ok := restarted.agents.Get(harness.reviewer.ID)
		if !ok || restored.ReviewCycle == nil {
			t.Fatal("active material ledger did not survive restart")
		}
		restoredRound := restored.ReviewCycle.Convergence.Rounds[len(restored.ReviewCycle.Convergence.Rounds)-1]
		if !stringSliceContains(
			restoredRound.NewCoverageGapIDs,
			gap.ID,
		) {
			t.Fatalf(
				"restored active material ledger = %#v",
				restoredRound,
			)
		}
		harness.bot = restarted
		harness.agents = restarted.agents

		current, _ = harness.agents.Get(harness.reviewer.ID)
		var gapTarget ReviewChallengeTarget
		for _, target := range buildReviewChallengeTargets(
			current.ReviewCycle,
		) {
			if target.Kind == ReviewChallengeCoverageGap &&
				target.ID == gap.ID {
				gapTarget = target
				break
			}
		}
		if gapTarget.ID == "" {
			t.Fatalf(
				"introduced gap challenge targets = %#v",
				buildReviewChallengeTargets(current.ReviewCycle),
			)
		}
		resolution, err := harness.bot.queueTargetedReviewChallenge(
			harness.reviewer.ID,
			challenge.Round,
			gapTarget,
		)
		if err != nil {
			t.Fatalf("queueTargetedReviewChallenge(resolution) error = %v", err)
		}
		completeConvergenceChallenge(
			t,
			harness,
			resolution,
			ReviewChallengePayload{
				Outcome:      ReviewChallengeUpheld,
				Summary:      "targeted evidence closed the introduced gap",
				AssignmentID: resolution.ID,
				TargetKind:   resolution.Target.Kind,
				TargetID:     resolution.Target.ID,
				Candidates:   []ReviewFindingCandidate{},
				Coverage: []ReviewCoverageClaim{{
					RequirementID: requirement.ID,
					Kind:          requirement.Kind,
					Status:        ReviewCoverageCovered,
					Evidence: []ReviewEvidence{{
						Summary: "targeted branch was exercised",
						Path:    "tracked.txt",
					}},
				}},
			},
		)
		current, _ = harness.agents.Get(harness.reviewer.ID)
		if len(current.ReviewCycle.UnresolvedCoverage) != 0 {
			t.Fatalf(
				"resolved coverage gaps = %#v",
				current.ReviewCycle.UnresolvedCoverage,
			)
		}
		reverify, err := harness.bot.queueReviewVerificationRound(
			harness.reviewer.ID,
			challenge.Round,
		)
		if err != nil {
			t.Fatalf("queueReviewVerificationRound(revised) error = %v", err)
		}
		if len(reverify) != 1 {
			t.Fatalf("revised verification assignments = %#v", reverify)
		}
		completeConvergenceVerification(
			t,
			harness,
			reverify[0],
			ReviewVerificationRejected,
		)
		completed, err := harness.bot.completeReviewConvergenceRound(
			harness.reviewer.ID,
		)
		if err != nil {
			t.Fatalf("completeReviewConvergenceRound() error = %v", err)
		}
		if completed.Outcome != ReviewConvergenceRoundMaterial ||
			completed.QuietRounds != 0 ||
			!stringSliceContains(completed.NewCoverageGapIDs, gap.ID) {
			t.Fatalf(
				"transient gap convergence round = %#v",
				completed,
			)
		}
	})

}

func TestVerifiedQuietRoundsConvergeResetPersistAndStopAtMaximum(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	harness.agents.mu.Lock()
	harness.agents.agents[harness.reviewer.ID].LastReviewCommentID = 99_999
	harness.agents.mu.Unlock()
	first, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound(first) error = %v", err)
	}
	first, err = harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRound(first) error = %v", err)
	}
	if first.Outcome != ReviewConvergenceRoundQuiet ||
		first.QuietRounds != 1 {
		t.Fatalf("first quiet round = %#v", first)
	}
	afterFirst, _ := harness.agents.Get(harness.reviewer.ID)
	if afterFirst.ReviewCycle.Convergence.Status ==
		ReviewConvergenceConverged {
		t.Fatal("raw review comment count satisfied convergence")
	}
	completeConvergenceRediscoveryPass(t, harness, 0)
	second, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound(second) error = %v", err)
	}
	second, err = harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRound(second) error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	if second.Outcome != ReviewConvergenceRoundQuiet ||
		current.ReviewCycle.Convergence.Status != ReviewConvergenceConverged {
		t.Fatalf(
			"verified quiet convergence = round:%#v state:%#v",
			second,
			current.ReviewCycle.Convergence,
		)
	}
	restarted := &Orchestrator{
		cfg:    harness.bot.cfg,
		agents: NewAgentManager(),
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	restored, ok := restarted.agents.Get(harness.reviewer.ID)
	if !ok || !reflect.DeepEqual(
		restored.ReviewCycle.Convergence,
		current.ReviewCycle.Convergence,
	) {
		t.Fatalf(
			"quiet-round restart changed state: current=%#v restored=%#v",
			current.ReviewCycle.Convergence,
			restored.ReviewCycle,
		)
	}

	late := cloneReviewCycle(current.ReviewCycle)
	late.Convergence.Status = ReviewConvergencePending
	late.Convergence.QuietRounds = 1
	late.Convergence.Rounds = late.Convergence.Rounds[:1]
	if _, err := beginReviewConvergenceRoundState(
		late,
		time.Unix(1_800_200_000, 0).UTC(),
	); err != nil {
		t.Fatalf("beginReviewConvergenceRoundState(late) error = %v", err)
	}
	finding := canonicalConvergenceFinding(late.HeadSHA, 999)
	candidateRevision, err := reviewFindingCandidateRevision(finding)
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision(late) error = %v", err)
	}
	late.CanonicalFindings = append(late.CanonicalFindings, finding)
	late.Convergence.VerificationAssignments = append(
		late.Convergence.VerificationAssignments,
		ReviewVerificationAssignment{
			ID:                "verification:" + finding.ID + ":001",
			FindingID:         finding.ID,
			ExactSHA:          late.HeadSHA,
			CandidateRevision: candidateRevision,
			CandidateSnapshot: cloneReviewCanonicalFinding(finding),
			Round:             2,
			Ordinal:           1,
			Lane:              reviewVerificationLane(finding.ID, 1),
			Status:            ReviewVerificationCompleted,
			ExcludedWorkerIDs: reviewFindingOriginWorkerIDs(finding),
			WorkerID:          "independent-late-verifier",
			Attempt:           1,
			QueuedAt:          time.Unix(1_800_200_001, 0).UTC(),
			StartedAt:         time.Unix(1_800_200_002, 0).UTC(),
			CompletedAt:       time.Unix(1_800_200_003, 0).UTC(),
			Outcome:           ReviewVerificationConfirmed,
			Summary:           "late challenge finding confirmed",
			Evidence: []ReviewEvidence{{
				Summary: "late behavior reproduced",
				Path:    "tracked.txt",
			}},
		},
	)
	if err := syncReviewFindingVerifications(late); err != nil {
		t.Fatalf("syncReviewFindingVerifications(late) error = %v", err)
	}
	lateRound, err := completeReviewConvergenceRoundState(
		late,
		time.Unix(1_800_200_004, 0).UTC(),
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRoundState(late) error = %v", err)
	}
	if lateRound.Outcome != ReviewConvergenceRoundChangesRequired ||
		lateRound.QuietRounds != 0 ||
		len(lateRound.NewConfirmedFindingIDs) != 1 {
		t.Fatalf("late finding did not terminate review: %#v", lateRound)
	}

	maxed := cloneReviewCycle(current.ReviewCycle)
	maxed.Policy.Convergence.MaxRounds = 2
	maxed.Policy.Convergence.QuietRoundsRequired = 2
	maxed.Convergence = nil
	maxed.DiscoveryPasses = cloneReviewDiscoveryPasses(
		maxed.DiscoveryPasses[:1],
	)
	maxed.UnresolvedCoverage = []ReviewCoverageGap{{
		ID:            "coverage-gap-persistent",
		RequirementID: maxed.Plan.CoverageRequirements[0].ID,
		Kind:          maxed.Plan.CoverageRequirements[0].Kind,
		Description:   "persistent material gap",
		Status:        ReviewCoverageGapMissing,
		Evidence:      []ReviewEvidence{},
	}}
	for index := 0; index < 2; index++ {
		if index > 0 {
			appendCompletedDiscoveryPassForState(
				maxed,
				index+1,
				time.Unix(1_800_299_999+int64(index*2), 0).UTC(),
			)
		}
		if _, err := beginReviewConvergenceRoundState(
			maxed,
			time.Unix(1_800_300_000+int64(index*2), 0).UTC(),
		); err != nil {
			t.Fatalf("beginReviewConvergenceRoundState(max %d) error = %v", index+1, err)
		}
		if _, err := completeReviewConvergenceRoundState(
			maxed,
			time.Unix(1_800_300_001+int64(index*2), 0).UTC(),
		); err != nil {
			t.Fatalf("completeReviewConvergenceRoundState(max %d) error = %v", index+1, err)
		}
	}
	last := maxed.Convergence.Rounds[len(maxed.Convergence.Rounds)-1]
	if maxed.Convergence.Status != ReviewConvergenceMaxRounds ||
		last.Outcome != ReviewConvergenceRoundMaxRounds ||
		last.Action == "" {
		t.Fatalf(
			"maximum round limit did not produce an explicit outcome: %#v",
			maxed.Convergence,
		)
	}
}

func TestConvergenceRoundRequiresFreshCompletedDiscoveryPass(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	first, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound(first) error = %v", err)
	}
	if first.DiscoveryPass != 1 {
		t.Fatalf("first discovery pass = %d, want 1", first.DiscoveryPass)
	}
	_, err = harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRound(first) error = %v", err)
	}

	if _, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	); err == nil || !strings.Contains(err.Error(), "fresh completed discovery pass") {
		t.Fatalf(
			"beginReviewConvergenceRound(without rediscovery) error = %v, want fresh-pass rejection",
			err,
		)
	}
}

func TestIncompleteRediscoveryPassCannotCountAsQuiet(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	first, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound(first) error = %v", err)
	}
	first, err = harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRound(first) error = %v", err)
	}

	current, _ := harness.agents.Get(harness.reviewer.ID)
	if first.Outcome != ReviewConvergenceRoundQuiet {
		t.Fatalf("first convergence round = %#v, want quiet", first)
	}
	tests := []struct {
		name   string
		mutate func(*ReviewDiscoveryPassState)
	}{
		{
			name: "failed lane",
			mutate: func(pass *ReviewDiscoveryPassState) {
				pass.Lanes[0].Status = ReviewDiscoveryLaneFailed
				pass.Lanes[0].FailureCode = ReviewDiscoveryFailureRuntime
			},
		},
		{
			name: "cancelled lane",
			mutate: func(pass *ReviewDiscoveryPassState) {
				pass.Lanes[0].Status = ReviewDiscoveryLaneFailed
				pass.Lanes[0].FailureCode = ReviewDiscoveryFailureCanceled
			},
		},
		{
			name: "zero assignments",
			mutate: func(pass *ReviewDiscoveryPassState) {
				pass.Lanes = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cycle := cloneReviewCycle(current.ReviewCycle)
			appendCompletedDiscoveryPassForState(
				cycle,
				2,
				time.Unix(1_800_250_000, 0).UTC(),
			)
			test.mutate(&cycle.DiscoveryPasses[1])
			if _, err := beginReviewConvergenceRoundState(
				cycle,
				time.Unix(1_800_250_001, 0).UTC(),
			); err != nil {
				t.Fatalf("beginReviewConvergenceRoundState(second) error = %v", err)
			}
			second, err := completeReviewConvergenceRoundState(
				cycle,
				time.Unix(1_800_250_002, 0).UTC(),
			)
			if err != nil {
				t.Fatalf("completeReviewConvergenceRoundState(second) error = %v", err)
			}
			if second.Outcome != ReviewConvergenceRoundUnresolved ||
				second.QuietRounds != 0 ||
				cycle.Convergence.Status != ReviewConvergenceUnresolved {
				t.Fatalf(
					"incomplete rediscovery counted toward convergence: round=%#v state=%#v",
					second,
					cycle.Convergence,
				)
			}
		})
	}

	tampered := cloneReviewCycle(current.ReviewCycle)
	tampered.DiscoveryPasses[0].Lanes[0].Status = ReviewDiscoveryLaneFailed
	tampered.DiscoveryPasses[0].Lanes[0].FailureCode =
		ReviewDiscoveryFailureRuntime
	if err := validatePersistedReviewConvergence(tampered); err == nil {
		t.Fatal("persisted quiet round accepted a failed discovery pass")
	}
}

func TestNewRediscoveryCandidateResetsQuietProgress(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	if _, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	); err != nil {
		t.Fatalf("beginReviewConvergenceRound(first) error = %v", err)
	}
	first, err := harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil || first.Outcome != ReviewConvergenceRoundQuiet ||
		first.QuietRounds != 1 {
		t.Fatalf("first convergence round = %#v, error = %v", first, err)
	}

	completeConvergenceRediscoveryPass(t, harness, 1)
	assignment := queueOneReviewVerification(t, harness)
	completeConvergenceVerification(
		t,
		harness,
		assignment,
		ReviewVerificationRejected,
	)
	second, err := harness.bot.completeReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("completeReviewConvergenceRound(second) error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	if second.Outcome != ReviewConvergenceRoundMaterial ||
		second.QuietRounds != 0 || len(second.NewCandidateIDs) != 1 ||
		current.ReviewCycle.Convergence.QuietRounds != 0 {
		t.Fatalf(
			"new rejected candidate did not reset quiet progress: round=%#v state=%#v",
			second,
			current.ReviewCycle.Convergence,
		)
	}
}

func TestReviewConvergentRoundCancellationDrainsTargetedWorker(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, false)
	started := make(chan struct{}, 1)
	var mu sync.Mutex
	active := 0
	harness.bot.reviewChallengeRunner = func(
		ctx context.Context,
		_ string,
		_ ReviewChallengeAssignment,
	) error {
		mu.Lock()
		active++
		mu.Unlock()
		started <- struct{}{}
		<-ctx.Done()
		mu.Lock()
		active--
		mu.Unlock()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := harness.bot.runReviewConvergentRound(
			ctx,
			harness.reviewer.ID,
		)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("targeted challenge did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"runReviewConvergentRound(canceled) error = %v",
				err,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled convergence round leaked a worker")
	}
	mu.Lock()
	defer mu.Unlock()
	if active != 0 {
		t.Fatalf("active targeted workers after cancellation = %d", active)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	latest := current.ReviewCycle.Convergence.Rounds[len(current.ReviewCycle.Convergence.Rounds)-1]
	if latest.CompletedAt.IsZero() ||
		latest.Outcome != ReviewConvergenceRoundUnresolved {
		t.Fatalf("canceled convergence round state = %#v", latest)
	}
}

func newReviewConvergenceTestHarness(
	t *testing.T,
	candidateCount int,
	coverAll bool,
) *reviewArtifactIntakeHarness {
	t.Helper()
	harness := newReviewArtifactIntakeHarness(t)
	harness.agents.mu.Lock()
	policy := harness.agents.agents[harness.reviewer.ID].ReviewCycle.Policy
	harness.agents.mu.Unlock()
	policy.Convergence.QuietRoundsRequired = 2
	policy.Convergence.MaxRounds = 4
	policy.Escalation.AfterNonConvergingRounds = 2
	policy, err := snapshotEffectiveReviewPolicy(policy)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
	}
	harness.agents.mu.Lock()
	cycle := harness.agents.agents[harness.reviewer.ID].ReviewCycle
	cycle.Policy = policy
	cycle.PolicyVersion = policy.Version
	cycle.PolicyFingerprint = policy.Fingerprint
	harness.reviewer = cloneAgent(harness.agents.agents[harness.reviewer.ID])
	harness.agents.mu.Unlock()
	attachDiscoveryPlanToArtifactHarness(t, harness)
	state, err := harness.bot.beginReviewDiscoveryPass(
		harness.reviewer.ID,
		1,
	)
	if err != nil {
		t.Fatalf("beginReviewDiscoveryPass() error = %v", err)
	}
	if state.Lanes[0].Lane != harness.ownership.Identity.Lane {
		t.Fatalf(
			"test ownership lane = %q, first scheduled lane = %q",
			harness.ownership.Identity.Lane,
			state.Lanes[0].Lane,
		)
	}
	if err := harness.bot.markReviewDiscoveryLaneRunning(
		harness.reviewer.ID,
		1,
		harness.ownership.Identity.Lane,
		nil,
	); err != nil {
		t.Fatalf("markReviewDiscoveryLaneRunning(initial) error = %v", err)
	}
	if err := harness.bot.markReviewDiscoveryLaneRunning(
		harness.reviewer.ID,
		1,
		harness.ownership.Identity.Lane,
		&harness.ownership,
	); err != nil {
		t.Fatalf("markReviewDiscoveryLaneRunning(owner) error = %v", err)
	}
	envelope := testDiscoveryArtifactEnvelope(
		t,
		harness.ownership,
		"convergence discovery",
	)
	envelope.Payload.Discovery.Candidates =
		make([]ReviewFindingCandidate, 0, candidateCount)
	for index := 0; index < candidateCount; index++ {
		envelope.Payload.Discovery.Candidates = append(
			envelope.Payload.Discovery.Candidates,
			convergenceCandidate(index+1),
		)
	}
	if coverAll {
		current, _ := harness.agents.Get(harness.reviewer.ID)
		for _, requirement := range current.ReviewCycle.Plan.CoverageRequirements {
			envelope.Payload.Discovery.Coverage = append(
				envelope.Payload.Discovery.Coverage,
				ReviewCoverageClaim{
					RequirementID: requirement.ID,
					Kind:          requirement.Kind,
					Status:        ReviewCoverageCovered,
					Evidence: []ReviewEvidence{{
						Summary: "exact requirement exercised",
						Path:    "tracked.txt",
					}},
				},
			)
		}
	}
	path := harness.publish(t, envelope)
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	); err != nil {
		t.Fatalf("intakeReviewWorkerArtifact(discovery) error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	latest, _ := latestReviewDiscoveryPassSnapshot(current.ReviewCycle)
	for _, lane := range latest.Lanes {
		if lane.Status != ReviewDiscoveryLaneQueued {
			continue
		}
		if lane.Required {
			ownership, store := bindConvergenceWorker(
				t,
				harness,
				AgentProfileRoleDiscovery,
				latest.Pass,
				lane.Lane,
				func(ownership ReviewWorkerOwnership) error {
					if err := harness.bot.markReviewDiscoveryLaneRunning(
						harness.reviewer.ID,
						latest.Pass,
						lane.Lane,
						nil,
					); err != nil {
						return err
					}
					return harness.bot.markReviewDiscoveryLaneRunning(
						harness.reviewer.ID,
						latest.Pass,
						lane.Lane,
						&ownership,
					)
				},
			)
			envelope := testDiscoveryArtifactEnvelope(
				t,
				ownership,
				"required convergence discovery",
			)
			if lane.Lane == reviewSynthesisLane {
				envelope.Payload.Discovery.Candidates =
					make([]ReviewFindingCandidate, 0, candidateCount)
				for index := 0; index < candidateCount; index++ {
					envelope.Payload.Discovery.Candidates = append(
						envelope.Payload.Discovery.Candidates,
						convergenceCandidate(index+1),
					)
				}
			}
			envelope.ExactSHA =
				harness.reviewer.ReviewCycle.HeadSHA
			envelope.Checkout.ExactSHA =
				harness.reviewer.ReviewCycle.HeadSHA
			path, err := store.publish(
				context.Background(),
				harness.reviewer.ReviewCycle.HeadSHA,
				ownership,
				envelope,
			)
			if err != nil {
				t.Fatalf(
					"publish(required discovery %s) error = %v",
					lane.Lane,
					err,
				)
			}
			if _, err := harness.bot.intakeReviewWorkerArtifact(
				context.Background(),
				harness.reviewer.ID,
				ownership.OwnerID,
				path,
				ReviewArtifactPhaseDiscovery,
			); err != nil {
				t.Fatalf(
					"intake(required discovery %s) error = %v",
					lane.Lane,
					err,
				)
			}
			continue
		}
		if err := harness.bot.failReviewDiscoveryLane(
			harness.reviewer.ID,
			latest.Pass,
			lane.Lane,
			ReviewDiscoveryFailureRuntime,
		); err != nil {
			t.Fatalf(
				"failReviewDiscoveryLane(%s) error = %v",
				lane.Lane,
				err,
			)
		}
	}
	return harness
}

func completeConvergenceRediscoveryPass(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
	candidateCount int,
) ReviewDiscoveryPassState {
	t.Helper()
	current, _ := harness.agents.Get(harness.reviewer.ID)
	passNumber := len(current.ReviewCycle.DiscoveryPasses) + 1
	pass, err := harness.bot.beginReviewDiscoveryPass(
		harness.reviewer.ID,
		passNumber,
	)
	if err != nil {
		t.Fatalf("beginReviewDiscoveryPass(%d) error = %v", passNumber, err)
	}
	for laneIndex, lane := range pass.Lanes {
		ownership, store := bindConvergenceWorker(
			t,
			harness,
			AgentProfileRoleDiscovery,
			pass.Pass,
			lane.Lane,
			func(ownership ReviewWorkerOwnership) error {
				if err := harness.bot.markReviewDiscoveryLaneRunning(
					harness.reviewer.ID,
					pass.Pass,
					lane.Lane,
					nil,
				); err != nil {
					return err
				}
				return harness.bot.markReviewDiscoveryLaneRunning(
					harness.reviewer.ID,
					pass.Pass,
					lane.Lane,
					&ownership,
				)
			},
		)
		envelope := testDiscoveryArtifactEnvelope(
			t,
			ownership,
			"independent convergence rediscovery",
		)
		envelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
		envelope.Checkout.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
		envelope.Payload.Discovery.Candidates = []ReviewFindingCandidate{}
		if laneIndex == 0 || lane.Lane == reviewSynthesisLane {
			for index := 0; index < candidateCount; index++ {
				envelope.Payload.Discovery.Candidates = append(
					envelope.Payload.Discovery.Candidates,
					convergenceCandidate(index+1),
				)
			}
		}
		envelope.Payload.Discovery.Coverage = []ReviewCoverageClaim{}
		for _, requirement := range current.ReviewCycle.Plan.CoverageRequirements {
			envelope.Payload.Discovery.Coverage = append(
				envelope.Payload.Discovery.Coverage,
				ReviewCoverageClaim{
					RequirementID: requirement.ID,
					Kind:          requirement.Kind,
					Status:        ReviewCoverageCovered,
					Evidence: []ReviewEvidence{{
						Summary: "exact requirement exercised again",
						Path:    "tracked.txt",
					}},
				},
			)
		}
		path, err := store.publish(
			context.Background(),
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf("publish(rediscovery %s) error = %v", lane.Lane, err)
		}
		if _, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			ownership.OwnerID,
			path,
			ReviewArtifactPhaseDiscovery,
		); err != nil {
			t.Fatalf("intake(rediscovery %s) error = %v", lane.Lane, err)
		}
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	latest, ok := latestReviewDiscoveryPassSnapshot(current.ReviewCycle)
	if !ok || latest.Pass != passNumber || latest.CompletedAt.IsZero() {
		t.Fatalf("completed rediscovery pass = %#v", latest)
	}
	return latest
}

func appendCompletedDiscoveryPassForState(
	cycle *ReviewCycleState,
	pass int,
	completedAt time.Time,
) {
	previous, ok := latestReviewDiscoveryPass(cycle)
	if !ok {
		return
	}
	next := ReviewDiscoveryPassState{
		Pass:        pass,
		HeadSHA:     cycle.HeadSHA,
		QueuedAt:    completedAt.Add(-2 * time.Second),
		CompletedAt: completedAt,
		Lanes:       append([]ReviewDiscoveryLaneState(nil), previous.Lanes...),
	}
	for index := range next.Lanes {
		next.Lanes[index].QueuedAt = next.QueuedAt
		next.Lanes[index].StartedAt = completedAt.Add(-time.Second)
		next.Lanes[index].CompletedAt = completedAt
	}
	cycle.DiscoveryPasses = append(cycle.DiscoveryPasses, next)
}

func convergenceCandidate(index int) ReviewFindingCandidate {
	return ReviewFindingCandidate{
		CandidateID: fmt.Sprintf("candidate-convergence-%03d", index),
		Summary: fmt.Sprintf(
			"Convergence behavior %d violates its invariant",
			index,
		),
		Location: ReviewFindingLocation{
			Path:      "tracked.txt",
			Symbol:    fmt.Sprintf("tracked behavior %d", index),
			StartLine: 1,
			EndLine:   1,
		},
		BehavioralPath: fmt.Sprintf(
			"tracked behavior %d changes during review",
			index,
		),
		ViolatedInvariant: fmt.Sprintf(
			"tracked behavior %d remains stable",
			index,
		),
		Severity:   ReviewFindingSeverityHigh,
		Confidence: ReviewFindingConfidenceMedium,
		Evidence: []ReviewEvidence{{
			Summary:   "tracked behavior is observable",
			Path:      "tracked.txt",
			StartLine: 1,
			EndLine:   1,
		}},
	}
}

func queueOneReviewVerification(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
) ReviewVerificationAssignment {
	t.Helper()
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	assignments, err := harness.bot.queueReviewVerificationRound(
		harness.reviewer.ID,
		round.Round,
	)
	if err != nil {
		t.Fatalf("queueReviewVerificationRound() error = %v", err)
	}
	if len(assignments) != 1 ||
		assignments[0].Status != ReviewVerificationQueued {
		t.Fatalf("queued verification assignments = %#v", assignments)
	}
	if err := harness.bot.markReviewVerificationRunning(
		harness.reviewer.ID,
		assignments[0].ID,
		nil,
	); err != nil {
		t.Fatalf("markReviewVerificationRunning(initial) error = %v", err)
	}
	return assignments[0]
}

func queueInconclusiveFindingChallenge(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
) (ReviewChallengeAssignment, ReviewFindingVerification) {
	t.Helper()
	round, err := harness.bot.beginReviewConvergenceRound(
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	maxVerifiers := current.ReviewCycle.Policy.Verification.MaxVerifiers
	findingID := current.ReviewCycle.CanonicalFindings[0].ID
	for ordinal := 1; ordinal <= maxVerifiers; ordinal++ {
		assignments, err := harness.bot.queueReviewVerificationRound(
			harness.reviewer.ID,
			round.Round,
		)
		if err != nil {
			t.Fatalf(
				"queueReviewVerificationRound(%d) error = %v",
				ordinal,
				err,
			)
		}
		if len(assignments) != 1 {
			t.Fatalf(
				"verification assignments at ordinal %d = %#v",
				ordinal,
				assignments,
			)
		}
		verification := assignments[0]
		if err := harness.bot.markReviewVerificationRunning(
			harness.reviewer.ID,
			verification.ID,
			nil,
		); err != nil {
			t.Fatalf(
				"markReviewVerificationRunning(%d) error = %v",
				ordinal,
				err,
			)
		}
		ownership, store := bindConvergenceWorker(
			t,
			harness,
			AgentProfileRoleVerifier,
			verification.Round,
			verification.Lane,
			func(ownership ReviewWorkerOwnership) error {
				return harness.bot.markReviewVerificationRunning(
					harness.reviewer.ID,
					verification.ID,
					&ownership,
				)
			},
		)
		finding := currentReviewFinding(
			t,
			harness,
			verification.FindingID,
		)
		envelope := verificationEnvelope(
			t,
			harness,
			ownership,
			finding,
			ReviewVerificationInconclusive,
		)
		path, err := store.publish(
			context.Background(),
			harness.reviewer.ReviewCycle.HeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf(
				"publish(inconclusive verification %d) error = %v",
				ordinal,
				err,
			)
		}
		if _, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			ownership.OwnerID,
			path,
			ReviewArtifactPhaseVerification,
		); err != nil {
			t.Fatalf(
				"intake(inconclusive verification %d) error = %v",
				ordinal,
				err,
			)
		}
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	var decision ReviewFindingVerification
	for _, value := range current.ReviewCycle.Convergence.FindingVerifications {
		if value.FindingID == findingID {
			decision = value
			break
		}
	}
	if decision.Status != ReviewFindingVerificationInconclusive {
		t.Fatalf("finding verification = %#v, want inconclusive", decision)
	}
	var target ReviewChallengeTarget
	for _, value := range buildReviewChallengeTargets(current.ReviewCycle) {
		if value.Kind == ReviewChallengeCompetingHypothesis &&
			value.ID == findingID {
			target = value
			break
		}
	}
	if target.ID == "" {
		t.Fatal("inconclusive finding did not create a hypothesis target")
	}
	challenge, err := harness.bot.queueTargetedReviewChallenge(
		harness.reviewer.ID,
		round.Round,
		target,
	)
	if err != nil {
		t.Fatalf("queueTargetedReviewChallenge(hypothesis) error = %v", err)
	}
	return challenge, decision
}

func bindConvergenceWorker(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
	role AgentProfileRole,
	pass int,
	lane string,
	bind func(ReviewWorkerOwnership) error,
) (ReviewWorkerOwnership, *reviewArtifactStore) {
	t.Helper()
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared")
	}
	identity, err := reviewWorkerIdentityForCycle(
		current.ReviewCycle,
		role,
		pass,
		lane,
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}
	ownership, _, err := harness.agents.allocateReviewWorkerOwnership(
		harness.reviewer.ID,
		identity,
		harness.bot.cfg.WorktreeDir,
		fmt.Sprintf("%032x", pass+len(current.ReviewCycle.WorkerOwnerships)+10),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership() error = %v", err)
	}
	runReviewArtifactGit(
		t,
		harness.bot.cfg.WorktreeDir,
		"clone",
		"--quiet",
		harness.bot.cfg.RepoPath,
		ownership.WorktreePath,
	)
	directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("Mkdir(worker artifact directory) error = %v", err)
	}
	store, err := newReviewArtifactStore(
		directory,
		current.ReviewCycle.Policy.Artifacts,
	)
	if err != nil {
		t.Fatalf("newReviewArtifactStore() error = %v", err)
	}
	if err := bind(ownership); err != nil {
		t.Fatalf("bind convergence worker error = %v", err)
	}
	return ownership, store
}

func currentReviewFinding(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
	findingID string,
) ReviewCanonicalFinding {
	t.Helper()
	current, _ := harness.agents.Get(harness.reviewer.ID)
	finding, ok := reviewCanonicalFindingByID(
		current.ReviewCycle,
		findingID,
	)
	if !ok {
		t.Fatalf("finding %q disappeared", findingID)
	}
	return *finding
}

func verificationEnvelope(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
	ownership ReviewWorkerOwnership,
	finding ReviewCanonicalFinding,
	outcome ReviewVerificationOutcome,
) ReviewArtifactEnvelope {
	t.Helper()
	location := finding.Location
	scopeDisposition := ReviewScopeInScope
	patchDisposition := ReviewPatchIntroduced
	if outcome == ReviewVerificationRejected {
		patchDisposition = ReviewPatchNotReproduced
	}
	if outcome == ReviewVerificationInconclusive {
		scopeDisposition = ReviewScopeUnclear
		patchDisposition = ReviewPatchUnclear
	}
	envelope, err := newReviewArtifactEnvelope(
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		ReviewArtifactPhaseVerification,
		ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadVerification,
			Verification: &ReviewVerificationPayload{
				FindingID:        finding.ID,
				Outcome:          outcome,
				Summary:          "independent behavior verification",
				ScopeDisposition: scopeDisposition,
				PatchDisposition: patchDisposition,
				Location:         &location,
				BehavioralPath:   finding.BehavioralPath,
				Evidence: []ReviewEvidence{{
					Summary:   "independent location inspection",
					Path:      "tracked.txt",
					StartLine: 1,
					EndLine:   1,
				}},
				CausalEvidence: []ReviewEvidence{{
					Summary:   "base-to-head diff changes the assigned behavior",
					Path:      "tracked.txt",
					StartLine: 1,
					EndLine:   1,
				}},
				TestEvidence: []ReviewEvidence{{
					Summary: "behavior reproduced in focused test",
					Path:    "tracked.txt",
				}},
			},
		},
	)
	if err != nil {
		t.Fatalf("newReviewArtifactEnvelope(verification) error = %v", err)
	}
	return envelope
}

func completeConvergenceVerification(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
	assignment ReviewVerificationAssignment,
	outcome ReviewVerificationOutcome,
) {
	t.Helper()
	current, _ := harness.agents.Get(harness.reviewer.ID)
	stored, ok := reviewVerificationAssignmentByID(
		current.ReviewCycle,
		assignment.ID,
	)
	if !ok {
		t.Fatalf("verification assignment %q disappeared", assignment.ID)
	}
	if stored.Status == ReviewVerificationQueued {
		if err := harness.bot.markReviewVerificationRunning(
			harness.reviewer.ID,
			assignment.ID,
			nil,
		); err != nil {
			t.Fatalf("markReviewVerificationRunning(initial) error = %v", err)
		}
	} else if stored.Status != ReviewVerificationRunning ||
		stored.WorkerID != "" {
		t.Fatalf(
			"verification assignment cannot be completed from %#v",
			*stored,
		)
	}
	ownership, store := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewVerificationRunning(
				harness.reviewer.ID,
				assignment.ID,
				&ownership,
			)
		},
	)
	finding := currentReviewFinding(t, harness, assignment.FindingID)
	envelope := verificationEnvelope(
		t,
		harness,
		ownership,
		finding,
		outcome,
	)
	path, err := store.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	)
	if err != nil {
		t.Fatalf("publish(verification) error = %v", err)
	}
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		ownership.OwnerID,
		path,
		ReviewArtifactPhaseVerification,
	); err != nil {
		t.Fatalf("intake(verification) error = %v", err)
	}
}

func completeConvergenceChallenge(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
	assignment ReviewChallengeAssignment,
	payload ReviewChallengePayload,
) ReviewWorkerOwnership {
	t.Helper()
	current, _ := harness.agents.Get(harness.reviewer.ID)
	stored, ok := reviewChallengeAssignmentByID(
		current.ReviewCycle,
		assignment.ID,
	)
	if !ok {
		t.Fatalf("challenge assignment %q disappeared", assignment.ID)
	}
	if stored.Status == ReviewChallengeQueued {
		if err := harness.bot.markReviewChallengeRunning(
			harness.reviewer.ID,
			assignment.ID,
			nil,
		); err != nil {
			t.Fatalf("markReviewChallengeRunning(initial) error = %v", err)
		}
	} else if stored.Status != ReviewChallengeRunning || stored.WorkerID != "" {
		t.Fatalf(
			"challenge assignment cannot be completed from %#v",
			*stored,
		)
	}
	ownership, store := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleChallenge,
		assignment.Round,
		assignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewChallengeRunning(
				harness.reviewer.ID,
				assignment.ID,
				&ownership,
			)
		},
	)
	envelope, err := newReviewArtifactEnvelope(
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		ReviewArtifactPhaseChallenge,
		ReviewArtifactPayload{
			Kind:      ReviewArtifactPayloadChallenge,
			Challenge: &payload,
		},
	)
	if err != nil {
		t.Fatalf("newReviewArtifactEnvelope(challenge) error = %v", err)
	}
	path, err := store.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	)
	if err != nil {
		t.Fatalf("publish(challenge) error = %v", err)
	}
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		ownership.OwnerID,
		path,
		ReviewArtifactPhaseChallenge,
	); err != nil {
		t.Fatalf("intake(challenge) error = %v", err)
	}
	return ownership
}

func targetCoverageKind(
	t *testing.T,
	cycle *ReviewCycleState,
	target ReviewChallengeTarget,
) ReviewCoverageKind {
	t.Helper()
	for _, requirement := range cycle.Plan.CoverageRequirements {
		if requirement.ID == target.RequirementID {
			return requirement.Kind
		}
	}
	t.Fatalf("target requirement %q disappeared", target.RequirementID)
	return ""
}

func canonicalConvergenceFinding(
	exactSHA string,
	index int,
) ReviewCanonicalFinding {
	candidate := convergenceCandidate(index)
	findings, _ := canonicalizeReviewFindingReports([]reviewCandidateReport{{
		ExactSHA:  exactSHA,
		WorkerID:  fmt.Sprintf("challenge-origin-%d", index),
		Lane:      "challenge-test",
		Pass:      2,
		Candidate: candidate,
	}})
	return findings[0]
}
