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
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReviewFindingCandidatesRequireMaterialEvidenceButSeparateConfidence(
	t *testing.T,
) {
	valid := testReviewFindingCandidate()
	valid.Severity = ReviewFindingSeverityCritical
	valid.Confidence = ReviewFindingConfidenceLow
	if err := validateReviewFindingCandidate(valid); err != nil {
		t.Fatalf(
			"high-impact low-confidence candidate was rejected: %v",
			err,
		)
	}

	tests := []struct {
		name   string
		mutate func(*ReviewFindingCandidate)
	}{
		{
			name: "missing location",
			mutate: func(candidate *ReviewFindingCandidate) {
				candidate.Location = ReviewFindingLocation{}
			},
		},
		{
			name: "missing behavioral path",
			mutate: func(candidate *ReviewFindingCandidate) {
				candidate.BehavioralPath = ""
			},
		},
		{
			name: "missing invariant",
			mutate: func(candidate *ReviewFindingCandidate) {
				candidate.ViolatedInvariant = ""
			},
		},
		{
			name: "missing severity",
			mutate: func(candidate *ReviewFindingCandidate) {
				candidate.Severity = ""
			},
		},
		{
			name: "missing confidence",
			mutate: func(candidate *ReviewFindingCandidate) {
				candidate.Confidence = ""
			},
		},
		{
			name: "missing evidence",
			mutate: func(candidate *ReviewFindingCandidate) {
				candidate.Evidence = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := testReviewFindingCandidate()
			test.mutate(&candidate)
			if err := validateReviewFindingCandidate(candidate); err == nil {
				t.Fatal("incomplete material candidate was accepted")
			}
		})
	}
}

func TestCanonicalReviewFindingsDeduplicateEquivalentReportsDeterministically(
	t *testing.T,
) {
	first := testReviewFindingCandidate()
	first.Severity = ReviewFindingSeverityHigh
	first.Confidence = ReviewFindingConfidenceLow
	second := testReviewFindingCandidate()
	second.CandidateID = "candidate-lifecycle"
	second.Summary = "Same failure from another lane"
	second.Location.Symbol = "  POLLreviewagent "
	second.BehavioralPath = " Poll exits while CLEANUP is running "
	second.ViolatedInvariant = "cleanup finishes BEFORE exit"
	second.Severity = ReviewFindingSeverityMedium
	second.Confidence = ReviewFindingConfidenceHigh
	second.Evidence = []ReviewEvidence{{
		Summary:   "cleanup remains active at return",
		Path:      "internal/review_workflow.go",
		StartLine: 41,
		EndLine:   43,
	}}
	reports := []reviewCandidateReport{
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-correctness",
			Lane:      "contract",
			Pass:      1,
			Candidate: first,
		},
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-lifecycle",
			Lane:      "lifecycle",
			Pass:      1,
			Candidate: second,
		},
	}
	findings, err := canonicalizeReviewFindingReports(reports)
	if err != nil {
		t.Fatalf("canonicalizeReviewFindingReports() error = %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("canonical findings = %d, want 1", len(findings))
	}
	finding := findings[0]
	if finding.ID != "finding-"+finding.Fingerprint ||
		finding.Severity != ReviewFindingSeverityHigh ||
		finding.Confidence != ReviewFindingConfidenceHigh ||
		len(finding.Provenance) != 2 ||
		len(finding.Evidence) != 2 {
		t.Fatalf("merged canonical finding = %#v", finding)
	}

	reversed := []reviewCandidateReport{reports[1], reports[0]}
	restarted, err := canonicalizeReviewFindingReports(reversed)
	if err != nil {
		t.Fatalf("canonicalizeReviewFindingReports(reversed) error = %v", err)
	}
	if !reflect.DeepEqual(restarted, findings) {
		t.Fatalf(
			"canonical findings changed across replay:\nfirst=%#v\nreplay=%#v",
			findings,
			restarted,
		)
	}

	distinct := testReviewFindingCandidate()
	distinct.CandidateID = "candidate-distinct"
	distinct.ViolatedInvariant = "worker cleanup errors remain observable"
	withNeighbor, err := canonicalizeReviewFindingReports(
		append(reports, reviewCandidateReport{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-concurrency",
			Lane:      "concurrency-ordering",
			Pass:      1,
			Candidate: distinct,
		}),
	)
	if err != nil {
		t.Fatalf("canonicalizeReviewFindingReports(neighbor) error = %v", err)
	}
	if len(withNeighbor) != 2 {
		t.Fatalf(
			"near-neighbor findings = %d, want 2 distinct defects",
			len(withNeighbor),
		)
	}
}

func TestCanonicalReviewFindingsKeepDistinctBehavioralPathsInOneSymbol(
	t *testing.T,
) {
	first := testReviewFindingCandidate()
	first.CandidateID = "planner-contract"
	first.Summary = "Planner create path returns an undocumented body"
	first.Location = ReviewFindingLocation{
		Path:      "cmd/minime/apis/planner.go",
		Symbol:    "handlePlannerMutation",
		StartLine: 37,
		EndLine:   67,
	}
	first.BehavioralPath = "create path returns a success response body"
	first.ViolatedInvariant = "the published http contract must match observable response body behavior"
	first.Evidence = []ReviewEvidence{{
		Summary:   "handler statuses differ from OpenAPI",
		Path:      first.Location.Path,
		StartLine: 37,
		EndLine:   67,
	}}
	second := first
	second.CandidateID = "planner-absence-contract"
	second.Summary = "Planner absence path omits a required body"
	second.BehavioralPath = "absence path returns no response body"
	findings, err := canonicalizeReviewFindingReports([]reviewCandidateReport{
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-correctness",
			Lane:      "contract",
			Pass:      1,
			Candidate: first,
		},
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-test-coverage",
			Lane:      "operations-tests",
			Pass:      1,
			Candidate: second,
		},
	})
	if err != nil {
		t.Fatalf("canonicalizeReviewFindingReports() error = %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("distinct behavioral-path findings = %#v, want two findings", findings)
	}
}

func TestCanonicalReviewFindingsMergeNestedOverlappingLocations(t *testing.T) {
	first := testReviewFindingCandidate()
	first.CandidateID = "ci-validation-job"
	first.Location = ReviewFindingLocation{
		Path:      ".github/workflows/ci.yml",
		Symbol:    "jobs.test",
		StartLine: 33,
		EndLine:   64,
	}
	first.BehavioralPath = "pull requests run Go checks and finish without gitleaks, YAML validation, or actionlint"
	first.ViolatedInvariant = "the added test and scan workflow must enforce gitleaks, YAML syntax, and actionlint validation"
	first.Evidence = []ReviewEvidence{{
		Summary:   "the job ends after govulncheck",
		Path:      first.Location.Path,
		StartLine: 49,
		EndLine:   64,
	}}
	second := first
	second.CandidateID = "ci-validation-steps"
	second.Summary = "The workflow omits required validation commands"
	second.Location.Symbol = "jobs.test.steps"
	second.Location.StartLine = 49
	second.BehavioralPath = "the test and scan job completes after Go verification without invoking actionlint"
	second.ViolatedInvariant = "test and scan automation must enforce required actionlint validation on pull requests"
	findings, err := canonicalizeReviewFindingReports([]reviewCandidateReport{
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-contract",
			Lane:      "contract",
			Pass:      1,
			Candidate: first,
		},
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-operations",
			Lane:      "operations-tests",
			Pass:      1,
			Candidate: second,
		},
	})
	if err != nil {
		t.Fatalf("canonicalizeReviewFindingReports() error = %v", err)
	}
	if len(findings) != 1 || len(findings[0].Provenance) != 2 {
		t.Fatalf("nested overlapping findings = %#v, want one merged finding", findings)
	}

	second.Location.StartLine = 70
	second.Location.EndLine = 80
	disjoint, err := canonicalizeReviewFindingReports([]reviewCandidateReport{
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-contract",
			Lane:      "contract",
			Pass:      1,
			Candidate: first,
		},
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-operations",
			Lane:      "operations-tests",
			Pass:      1,
			Candidate: second,
		},
	})
	if err != nil {
		t.Fatalf("canonicalizeReviewFindingReports(disjoint) error = %v", err)
	}
	if len(disjoint) != 2 {
		t.Fatalf("disjoint nested findings = %#v, want two findings", disjoint)
	}
}

func TestCanonicalReviewFindingsMergeSameSymbolParaphrases(t *testing.T) {
	first := testReviewFindingCandidate()
	first.CandidateID = "ci-validation-first"
	first.Location = ReviewFindingLocation{
		Path:      ".github/workflows/ci.yml",
		Symbol:    "jobs.test.steps",
		StartLine: 49,
		EndLine:   64,
	}
	first.BehavioralPath = "a pull request triggers CI; test and scan runs Go module checks, vet, tests, and govulncheck; the job reports success without gitleaks, YAML validation, or actionlint"
	first.ViolatedInvariant = "the added CI job must enforce the requested gitleaks, YAML, and actionlint validation"
	first.Evidence = []ReviewEvidence{{
		Summary:   "the job ends after govulncheck",
		Path:      first.Location.Path,
		StartLine: 49,
		EndLine:   64,
	}}
	second := first
	second.CandidateID = "ci-validation-second"
	second.Summary = "Required repository validation is absent from CI"
	second.BehavioralPath = "a pull request runs CI; test and scan executes Go module checks, vet, tests, and govulncheck; it then succeeds without invoking gitleaks, a YAML validator, or actionlint"
	second.ViolatedInvariant = "test and scan must execute requested gitleaks, YAML, and actionlint validation before reporting success"
	findings, err := canonicalizeReviewFindingReports([]reviewCandidateReport{
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-contract",
			Lane:      "contract",
			Pass:      1,
			Candidate: first,
		},
		{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "worker-operations",
			Lane:      "operations-tests",
			Pass:      1,
			Candidate: second,
		},
	})
	if err != nil {
		t.Fatalf("canonicalizeReviewFindingReports() error = %v", err)
	}
	if len(findings) != 1 || len(findings[0].Provenance) != 2 {
		t.Fatalf("same-symbol paraphrases = %#v, want one merged finding", findings)
	}
}

func TestSemanticFindingMergeSupersedesOrphanedVerification(t *testing.T) {
	original := testReviewFindingCandidate()
	original.CandidateID = "synthesis-finding"
	original.BehavioralPath = "request returns success without validation"
	replacement := original
	replacement.CandidateID = "challenge-finding"
	replacement.BehavioralPath =
		"request returns success without required validation"

	originalFindings, err := canonicalizeReviewFindingReports(
		[]reviewCandidateReport{{
			ExactSHA:  testReviewHeadSHA,
			WorkerID:  "synthesis-worker",
			Lane:      reviewSynthesisLane,
			Pass:      1,
			Candidate: original,
		}},
	)
	if err != nil || len(originalFindings) != 1 {
		t.Fatalf("canonicalize original finding = %#v, %v", originalFindings, err)
	}
	mergedFindings, err := canonicalizeReviewFindingReports(
		[]reviewCandidateReport{
			{
				ExactSHA:  testReviewHeadSHA,
				WorkerID:  "synthesis-worker",
				Lane:      reviewSynthesisLane,
				Pass:      1,
				Candidate: original,
			},
			{
				ExactSHA:  testReviewHeadSHA,
				WorkerID:  "challenge-worker",
				Lane:      "challenge-coverage_gap-test",
				Pass:      1,
				Candidate: replacement,
			},
		},
	)
	if err != nil || len(mergedFindings) != 1 {
		t.Fatalf("canonicalize merged finding = %#v, %v", mergedFindings, err)
	}
	if mergedFindings[0].ID == originalFindings[0].ID {
		t.Fatal("semantic merge did not change the canonical representative")
	}

	revision, err := reviewFindingCandidateRevision(originalFindings[0])
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision() error = %v", err)
	}
	assignment := ReviewVerificationAssignment{
		ID:                "verification:" + originalFindings[0].ID + ":001",
		FindingID:         originalFindings[0].ID,
		ExactSHA:          testReviewHeadSHA,
		CandidateRevision: revision,
		CandidateSnapshot: cloneReviewCanonicalFinding(originalFindings[0]),
		Round:             1,
		Ordinal:           1,
		Lane:              reviewVerificationLane(originalFindings[0].ID, 1),
		Status:            ReviewVerificationQueued,
		ExcludedWorkerIDs: reviewFindingOriginWorkerIDs(originalFindings[0]),
		QueuedAt:          time.Unix(1_800_000_000, 0).UTC(),
	}
	cycle := &ReviewCycleState{
		Attempt:           1,
		HeadSHA:           testReviewHeadSHA,
		Policy:            builtInReviewPolicy(),
		CanonicalFindings: mergedFindings,
		Convergence: &ReviewConvergenceState{
			Status:                  ReviewConvergencePending,
			VerificationAssignments: []ReviewVerificationAssignment{assignment},
			FindingVerifications:    []ReviewFindingVerification{},
			ChallengeAssignments:    []ReviewChallengeAssignment{},
			Rounds:                  []ReviewConvergenceRoundState{},
		},
	}
	if err := reconcileReviewFindingAssignments(cycle); err != nil {
		t.Fatalf("reconcileReviewFindingAssignments() error = %v", err)
	}
	if !cycle.Convergence.VerificationAssignments[0].Superseded {
		t.Fatal("orphaned verification assignment was not superseded")
	}
	if len(cycle.Convergence.FindingVerifications) != 1 ||
		cycle.Convergence.FindingVerifications[0].FindingID !=
			mergedFindings[0].ID ||
		cycle.Convergence.FindingVerifications[0].Status !=
			ReviewFindingVerificationPending {
		t.Fatalf(
			"current finding verification = %#v, want one pending decision",
			cycle.Convergence.FindingVerifications,
		)
	}
	if err := validatePersistedReviewConvergence(cycle); err != nil {
		t.Fatalf("validatePersistedReviewConvergence() error = %v", err)
	}
}

func TestSynthesisCandidatesReplaceLaneCandidates(t *testing.T) {
	laneCandidate := testReviewFindingCandidate()
	laneCandidate.CandidateID = "lane-candidate"
	synthesisCandidate := testReviewFindingCandidate()
	synthesisCandidate.CandidateID = "synthesis-candidate"
	synthesisCandidate.Summary = "Reconciled finding"
	cycle := &ReviewCycleState{
		Attempt: 1,
		HeadSHA: testReviewHeadSHA,
		Plan: &ReviewPlan{
			BaseSHA:              strings.Repeat("b", canonicalGitObjectIDLength),
			HeadSHA:              testReviewHeadSHA,
			CoverageRequirements: []ReviewCoverageRequirement{},
		},
		DiscoveryPasses: []ReviewDiscoveryPassState{{
			Pass:    1,
			HeadSHA: testReviewHeadSHA,
			Lanes: []ReviewDiscoveryLaneState{
				{
					Lane:     "contract",
					Status:   ReviewDiscoveryLaneCompleted,
					WorkerID: "lane-worker",
					Attempt:  1,
				},
				{
					Lane:     reviewSynthesisLane,
					Status:   ReviewDiscoveryLaneCompleted,
					WorkerID: "synthesis-worker",
					Attempt:  1,
				},
			},
		}},
	}
	receipt := func(
		workerID string,
		lane string,
		candidate ReviewFindingCandidate,
	) ReviewArtifactReceipt {
		return ReviewArtifactReceipt{
			WorkerID: workerID,
			Attempt:  1,
			Phase:    ReviewArtifactPhaseDiscovery,
			Lane:     lane,
			Pass:     1,
			Envelope: ReviewArtifactEnvelope{
				ExactSHA: testReviewHeadSHA,
				Payload: ReviewArtifactPayload{
					Kind: ReviewArtifactPayloadDiscovery,
					Discovery: &ReviewDiscoveryPayload{
						Candidates: []ReviewFindingCandidate{candidate},
						Coverage:   []ReviewCoverageClaim{},
					},
				},
			},
		}
	}
	cycle.ArtifactReceipts = []ReviewArtifactReceipt{
		receipt("lane-worker", "contract", laneCandidate),
		receipt("synthesis-worker", reviewSynthesisLane, synthesisCandidate),
	}
	if err := rebuildReviewDiscoveryDerivedState(cycle); err != nil {
		t.Fatalf("rebuildReviewDiscoveryDerivedState() error = %v", err)
	}
	if len(cycle.CanonicalFindings) != 1 ||
		len(cycle.CanonicalFindings[0].Provenance) != 1 ||
		cycle.CanonicalFindings[0].Provenance[0].Lane != reviewSynthesisLane ||
		cycle.CanonicalFindings[0].Provenance[0].CandidateID !=
			synthesisCandidate.CandidateID {
		t.Fatalf(
			"canonical findings after synthesis = %#v",
			cycle.CanonicalFindings,
		)
	}
}

func TestReviewCoverageRejectsUnsupportedClaimsAndAggregatesNamedGaps(
	t *testing.T,
) {
	requirements := []ReviewCoverageRequirement{
		{ID: "ac:1", Kind: ReviewCoverageAcceptanceCriterion, Description: "acceptance"},
		{ID: "call:1", Kind: ReviewCoverageCallPath, Description: "call path"},
		{ID: "risk:1", Kind: ReviewCoverageRiskDomain, Description: "risk"},
		{ID: "state:1", Kind: ReviewCoverageStateTransition, Description: "transition"},
	}
	sortReviewCoverageRequirementsForTest(requirements)
	evidence := []ReviewEvidence{{Summary: "inspected the relevant behavior"}}
	claims := []ReviewCoverageClaim{
		{
			RequirementID: "ac:1",
			Kind:          ReviewCoverageAcceptanceCriterion,
			Status:        ReviewCoverageCovered,
			Evidence:      evidence,
		},
		{
			RequirementID: "call:1",
			Kind:          ReviewCoverageCallPath,
			Status:        ReviewCoveragePartial,
			Evidence:      evidence,
		},
		{
			RequirementID: "risk:1",
			Kind:          ReviewCoverageRiskDomain,
			Status:        ReviewCoverageNotCovered,
			Evidence:      evidence,
		},
	}
	gaps, err := aggregateReviewCoverageGaps(requirements, claims)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps() error = %v", err)
	}
	if len(gaps) != 3 {
		t.Fatalf("coverage gaps = %#v, want 3", gaps)
	}
	statuses := map[string]ReviewCoverageGapStatus{}
	for _, gap := range gaps {
		if gap.ID == "" {
			t.Fatal("coverage gap has no stable name")
		}
		statuses[gap.RequirementID] = gap.Status
	}
	if statuses["call:1"] != ReviewCoverageGapPartial ||
		statuses["risk:1"] != ReviewCoverageGapUncovered ||
		statuses["state:1"] != ReviewCoverageGapMissing {
		t.Fatalf("coverage gap statuses = %#v", statuses)
	}

	reversed := append([]ReviewCoverageClaim(nil), claims...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	replayed, err := aggregateReviewCoverageGaps(requirements, reversed)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps(reversed) error = %v", err)
	}
	if !reflect.DeepEqual(replayed, gaps) {
		t.Fatalf(
			"coverage aggregation is order-dependent:\nfirst=%#v\nreplay=%#v",
			gaps,
			replayed,
		)
	}

	unsupported := append(claims, ReviewCoverageClaim{
		RequirementID: "not-in-plan",
		Kind:          ReviewCoverageRiskDomain,
		Status:        ReviewCoverageCovered,
		Evidence:      evidence,
	})
	if _, err := aggregateReviewCoverageGaps(
		requirements,
		unsupported,
	); err == nil {
		t.Fatal("unsupported coverage claim was accepted")
	}
}

func TestNotCoveredClaimMayHaveNoEvidence(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:   "risk:unreviewed",
		Kind: ReviewCoverageRiskDomain,
	}
	claims := []ReviewCoverageClaim{{
		RequirementID: requirement.ID,
		Kind:          requirement.Kind,
		Status:        ReviewCoverageNotCovered,
		Evidence:      []ReviewEvidence{},
	}}
	if err := validateReviewCoverageClaims(
		claims,
		[]ReviewCoverageRequirement{requirement},
	); err != nil {
		t.Fatalf("validateReviewCoverageClaims() error = %v", err)
	}
}

func TestReviewPlanBuildsEveryCoverageDimensionDeterministically(t *testing.T) {
	inputs := reviewPlanInputsForFiles(
		t,
		[]ReviewPlanChangedFile{{
			Path:      "internal/state_persistence.go",
			Additions: 2,
			Deletions: 1,
			ChangedRanges: []ReviewLineRange{{
				StartLine: 40,
				EndLine:   45,
				Symbol:    "func runReviewLifecycle(ctx context.Context)",
			}},
		}},
		[]string{"Cancellation drains lifecycle workers"},
	)
	plan, err := buildReviewPlan(
		inputs,
		snapshottedReviewPlanPolicy(t, nil),
	)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	kinds := make(map[ReviewCoverageKind]bool)
	for _, requirement := range plan.CoverageRequirements {
		kinds[requirement.Kind] = true
	}
	for _, kind := range []ReviewCoverageKind{
		ReviewCoverageAcceptanceCriterion,
		ReviewCoverageChangedSymbol,
		ReviewCoverageChangedBranch,
		ReviewCoverageCallPath,
		ReviewCoverageStateTransition,
		ReviewCoveragePersistenceBoundary,
		ReviewCoverageRiskDomain,
	} {
		if !kinds[kind] {
			t.Fatalf(
				"review plan omitted coverage dimension %q: %#v",
				kind,
				plan.CoverageRequirements,
			)
		}
	}
	for _, requirement := range plan.CoverageRequirements {
		if !requirement.Critical {
			continue
		}
		if requirement.AccountableLane == "" || len(requirement.ChangedTargets) == 0 ||
			requirement.ChangedTargets[0].Symbol == "" {
			t.Fatalf("critical requirement is not behavior-targeted: %#v", requirement)
		}
	}
	replayed, err := buildReviewPlan(inputs, planPolicySnapshot(t, plan))
	if err != nil {
		t.Fatalf("buildReviewPlan(replay) error = %v", err)
	}
	if !reflect.DeepEqual(replayed.CoverageRequirements, plan.CoverageRequirements) {
		t.Fatal("coverage requirements changed across deterministic rebuild")
	}
	cloned := cloneReviewPlan(plan)
	checkedAccountability := false
	for index := range cloned.CoverageRequirements {
		if len(cloned.CoverageRequirements[index].ChangedTargets) == 0 {
			continue
		}
		cloned.CoverageRequirements[index].ChangedTargets[0].Symbol = "mutated"
		if plan.CoverageRequirements[index].ChangedTargets[0].Symbol == "mutated" {
			t.Fatal("cloned coverage targets alias the immutable review plan")
		}
		cloned.CoverageRequirements[index].ChangedTargets[0].Symbol =
			plan.CoverageRequirements[index].ChangedTargets[0].Symbol
		cloned.CoverageRequirements[index].AccountableLane = "not-selected"
		if err := validateReviewPlan(cloned, planPolicySnapshot(t, plan)); err == nil {
			t.Fatal("critical coverage accepted an unselected accountable lane")
		}
		checkedAccountability = true
		break
	}
	if !checkedAccountability {
		t.Fatal("review plan has no critical coverage to validate")
	}
}

func TestReviewCoverageRequirementsAreBoundedByReviewDimensions(t *testing.T) {
	changedRanges := make([]ReviewLineRange, 0, 100)
	for index := 1; index <= 100; index++ {
		changedRanges = append(changedRanges, ReviewLineRange{
			StartLine: index,
			EndLine:   index,
			Symbol:    "import (",
		})
	}
	inputs := reviewPlanInputsForFiles(
		t,
		[]ReviewPlanChangedFile{
			{
				Path:          "internal/review.go",
				Additions:     100,
				ChangedRanges: changedRanges,
			},
			{
				Path:      "README.md",
				Additions: 1,
				ChangedRanges: []ReviewLineRange{{
					StartLine: 1,
					EndLine:   1,
					Symbol:    "# Configuration",
				}},
			},
		},
		nil,
	)
	requirements := buildReviewCoverageRequirements(
		inputs,
		nil,
		[]string{"contract", "callers", "operations-tests"},
	)
	if len(requirements) != 3 {
		t.Fatalf("coverage requirements = %d, want one per review dimension", len(requirements))
	}
	for _, requirement := range requirements {
		if strings.Contains(requirement.Description, "import (") ||
			strings.Contains(requirement.Description, "# Configuration") {
			t.Fatalf("coverage requirement exposes a raw hunk label: %#v", requirement)
		}
	}
}

func TestCriticalReviewCoverageRequiresCausalChangedLineEvidence(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:              "changed-symbol:worker",
		Kind:            ReviewCoverageChangedSymbol,
		Description:     "changed behavior in pollReviewAgent",
		Critical:        true,
		AccountableLane: "contract",
		ChangedTargets: []ReviewCoverageTarget{{
			Path:      "internal/review_workflow.go",
			Symbol:    "func pollReviewAgent()",
			StartLine: 40,
			EndLine:   45,
		}},
	}
	claim := ReviewCoverageClaim{
		RequirementID: requirement.ID,
		Kind:          requirement.Kind,
		Status:        ReviewCoverageCovered,
		Evidence: []ReviewEvidence{{
			Summary: "reviewed the implementation",
		}},
	}
	if err := validateReviewCoverageClaims(
		[]ReviewCoverageClaim{claim},
		[]ReviewCoverageRequirement{requirement},
	); err != nil {
		t.Fatalf("structurally valid claim rejected: %v", err)
	}
	gaps, err := aggregateReviewCoverageGaps(
		[]ReviewCoverageRequirement{requirement},
		[]ReviewCoverageClaim{claim},
	)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps(generic) error = %v", err)
	}
	if len(gaps) != 1 || gaps[0].Status != ReviewCoverageGapPartial {
		t.Fatalf("generic evidence gaps = %#v, want one partial gap", gaps)
	}
	claim.Evidence[0] = ReviewEvidence{
		Summary:   "return can run before cleanup joins",
		Path:      requirement.ChangedTargets[0].Path,
		StartLine: 42,
		EndLine:   43,
	}
	if err := validateReviewCoverageClaims(
		[]ReviewCoverageClaim{claim},
		[]ReviewCoverageRequirement{requirement},
	); err != nil {
		t.Fatalf("causal changed-line evidence rejected: %v", err)
	}
	gaps, err = aggregateReviewCoverageGaps(
		[]ReviewCoverageRequirement{requirement},
		[]ReviewCoverageClaim{claim},
	)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps(causal) error = %v", err)
	}
	if len(gaps) != 0 {
		t.Fatalf("causal evidence remained open: %#v", gaps)
	}
}

func TestAcceptanceCriteriaAllowRepositoryLevelVerificationEvidence(t *testing.T) {
	changedTargets := []ReviewCoverageTarget{{
		Path:      "internal/product/product_test.go",
		Symbol:    "func TestNXOSSSHCompletesObservedExternalUpgradeLifecycle(t *testing.T)",
		StartLine: 2385,
		EndLine:   2495,
	}}
	requirements := []ReviewCoverageRequirement{
		{
			ID:              "acceptance-criterion:000001",
			Kind:            ReviewCoverageAcceptanceCriterion,
			Description:     "make test",
			Critical:        true,
			AccountableLane: "operations-tests",
			ChangedTargets:  changedTargets,
		},
		{
			ID:              "acceptance-criterion:000002",
			Kind:            ReviewCoverageAcceptanceCriterion,
			Description:     "all Go library coverage gates remain above 80%",
			Critical:        true,
			AccountableLane: "operations-tests",
			ChangedTargets:  changedTargets,
		},
	}
	claims := []ReviewCoverageClaim{
		{
			RequirementID: requirements[0].ID,
			Kind:          requirements[0].Kind,
			Status:        ReviewCoverageCovered,
			Evidence: []ReviewEvidence{{
				Summary:   "make test completed successfully",
				Path:      "Makefile",
				StartLine: 147,
				EndLine:   160,
			}},
		},
		{
			RequirementID: requirements[1].ID,
			Kind:          requirements[1].Kind,
			Status:        ReviewCoverageCovered,
			Evidence: []ReviewEvidence{{
				Summary:   "the library coverage gate passed for every Go library",
				Path:      "scripts/check-library-coverage",
				StartLine: 24,
				EndLine:   57,
			}},
		},
	}

	gaps, err := aggregateReviewCoverageGaps(requirements, claims)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps() error = %v", err)
	}
	if len(gaps) != 0 {
		t.Fatalf("repository-level acceptance evidence left gaps = %#v", gaps)
	}
}

func TestConflictingReviewCoverageRemainsOpenUntilAdjudicated(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:              "changed-branch:worker",
		Kind:            ReviewCoverageChangedBranch,
		Description:     "changed cleanup branch",
		Critical:        true,
		AccountableLane: "operations-tests",
		ChangedTargets: []ReviewCoverageTarget{{
			Path:      "internal/review_workflow.go",
			Symbol:    "func pollReviewAgent()",
			StartLine: 40,
			EndLine:   45,
		}},
	}
	causalEvidence := []ReviewEvidence{{
		Summary:   "cleanup joins before the return",
		Path:      "internal/review_workflow.go",
		StartLine: 42,
		EndLine:   43,
	}}
	claims := []ReviewCoverageClaim{
		{
			RequirementID: requirement.ID,
			Kind:          requirement.Kind,
			Status:        ReviewCoverageCovered,
			Evidence:      causalEvidence,
		},
		{
			RequirementID: requirement.ID,
			Kind:          requirement.Kind,
			Status:        ReviewCoveragePartial,
			Evidence: []ReviewEvidence{{
				Summary: "cancellation path was not exercised",
			}},
		},
	}
	gaps, err := aggregateReviewCoverageGaps(
		[]ReviewCoverageRequirement{requirement},
		claims,
	)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps(conflict) error = %v", err)
	}
	if len(gaps) != 1 || gaps[0].Status != ReviewCoverageGapConflicting ||
		len(gaps[0].Evidence) != 2 {
		t.Fatalf("conflicting coverage = %#v", gaps)
	}
	resolved, err := aggregateReviewCoverageGaps(
		[]ReviewCoverageRequirement{requirement},
		claims[:1],
	)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps(adjudicated) error = %v", err)
	}
	if len(resolved) != 0 {
		t.Fatalf("adjudicated coverage remained open: %#v", resolved)
	}
}

func TestCriticalReviewCoverageIsOwnedByItsAccountableLane(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:              "callers:worker",
		Kind:            ReviewCoverageCallPath,
		Description:     "callers of pollReviewAgent",
		Critical:        true,
		AccountableLane: "callers",
		ChangedTargets: []ReviewCoverageTarget{{
			Path:      "internal/review_workflow.go",
			Symbol:    "func pollReviewAgent()",
			StartLine: 40,
			EndLine:   45,
		}},
	}
	claim := ReviewCoverageClaim{
		RequirementID: requirement.ID,
		Kind:          requirement.Kind,
		Status:        ReviewCoverageCovered,
		Evidence: []ReviewEvidence{{
			Summary:   "caller handles cleanup completion",
			Path:      "internal/review_workflow.go",
			StartLine: 40,
			EndLine:   45,
		}},
	}
	if got := reviewCoverageClaimsForLane(
		[]ReviewCoverageClaim{claim},
		[]ReviewCoverageRequirement{requirement},
		"contract",
	); len(got) != 0 {
		t.Fatalf("unaccountable lane closed critical coverage: %#v", got)
	}
	if got := reviewCoverageClaimsForLane(
		[]ReviewCoverageClaim{claim},
		[]ReviewCoverageRequirement{requirement},
		"callers",
	); !reflect.DeepEqual(got, []ReviewCoverageClaim{claim}) {
		t.Fatalf("accountable lane claim = %#v", got)
	}
}

func TestReviewCoverageRequirementsAreScopedToAccountableLane(t *testing.T) {
	shared := ReviewCoverageRequirement{
		ID:   "risk:shared",
		Kind: ReviewCoverageRiskDomain,
	}
	owned := ReviewCoverageRequirement{
		ID:              "callers:worker",
		Kind:            ReviewCoverageCallPath,
		Critical:        true,
		AccountableLane: "callers",
	}
	requirements := []ReviewCoverageRequirement{shared, owned}
	if got := reviewCoverageRequirementsForLane(
		requirements,
		"contract",
	); !reflect.DeepEqual(got, []ReviewCoverageRequirement{shared}) {
		t.Fatalf("contract requirements = %#v", got)
	}
	if got := reviewCoverageRequirementsForLane(
		requirements,
		"callers",
	); !reflect.DeepEqual(got, requirements) {
		t.Fatalf("callers requirements = %#v", got)
	}
	if got := reviewCoverageRequirementsForLane(
		requirements,
		reviewSynthesisLane,
	); !reflect.DeepEqual(got, requirements) {
		t.Fatalf("synthesis requirements = %#v", got)
	}
}

func TestReviewDiscoveryLaneStatesPersistAndRequiredFailureBlocksApproval(
	t *testing.T,
) {
	bot, reviewer := newReviewDiscoverySchedulerHarness(t, 2)
	state, err := bot.beginReviewDiscoveryPass(reviewer.ID, 1)
	if err != nil {
		t.Fatalf("beginReviewDiscoveryPass() error = %v", err)
	}
	if len(state.Lanes) == 0 ||
		state.Lanes[0].Status != ReviewDiscoveryLaneQueued {
		t.Fatalf("queued discovery state = %#v", state)
	}
	assertPersistedDiscoveryStatus(
		t,
		bot,
		reviewer.ID,
		state.Lanes[0].Lane,
		ReviewDiscoveryLaneQueued,
	)
	if err := bot.markReviewDiscoveryLaneRunning(
		reviewer.ID,
		1,
		state.Lanes[0].Lane,
		nil,
	); err != nil {
		t.Fatalf("markReviewDiscoveryLaneRunning() error = %v", err)
	}
	assertPersistedDiscoveryStatus(
		t,
		bot,
		reviewer.ID,
		state.Lanes[0].Lane,
		ReviewDiscoveryLaneRunning,
	)
	if err := bot.failReviewDiscoveryLane(
		reviewer.ID,
		1,
		state.Lanes[0].Lane,
		ReviewDiscoveryFailureRuntime,
	); err != nil {
		t.Fatalf("failReviewDiscoveryLane() error = %v", err)
	}
	assertPersistedDiscoveryStatus(
		t,
		bot,
		reviewer.ID,
		state.Lanes[0].Lane,
		ReviewDiscoveryLaneFailed,
	)
	stored, ok := bot.agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared")
	}
	latest, _ := latestReviewDiscoveryPassSnapshot(stored.ReviewCycle)
	if latest.ApprovalEligible(stored.ReviewCycle.UnresolvedCoverage) {
		t.Fatal("required-lane failure yielded approval eligibility")
	}
	if _, err := bot.beginReviewDiscoveryPass(reviewer.ID, 1); err == nil {
		t.Fatal("the same discovery pass was scheduled twice")
	}
	if _, err := bot.beginReviewDiscoveryPass(reviewer.ID, 2); err == nil {
		t.Fatal("a new pass was scheduled while the prior pass was active")
	}
}

func TestReviewDiscoveryAcceptedArtifactPersistsCompletedLane(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
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
			"first scheduled lane = %q, want ownership lane %q",
			state.Lanes[0].Lane,
			harness.ownership.Identity.Lane,
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
		"one finding",
	)
	envelope.Payload.Discovery.Candidates = []ReviewFindingCandidate{{
		CandidateID: "candidate-restart-stability",
		Summary:     "Tracked behavior violates its invariant",
		Location: ReviewFindingLocation{
			Path:      "tracked.txt",
			Symbol:    "tracked behavior",
			StartLine: 1,
			EndLine:   1,
		},
		BehavioralPath:    "tracked behavior changes",
		ViolatedInvariant: "tracked behavior remains stable",
		Severity:          ReviewFindingSeverityHigh,
		Confidence:        ReviewFindingConfidenceLow,
		Evidence: []ReviewEvidence{{
			Summary:   "tracked behavior is observable",
			Path:      "tracked.txt",
			StartLine: 1,
			EndLine:   1,
		}},
	}}
	path := harness.publish(t, envelope)
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	); err != nil {
		t.Fatalf("intakeReviewWorkerArtifact() error = %v", err)
	}
	assertPersistedDiscoveryStatus(
		t,
		harness.bot,
		harness.reviewer.ID,
		harness.ownership.Identity.Lane,
		ReviewDiscoveryLaneCompleted,
	)
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil ||
		len(current.ReviewCycle.CanonicalFindings) != 1 {
		t.Fatalf(
			"current canonical findings = %#v",
			current.ReviewCycle,
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
	if !ok || restored.ReviewCycle == nil ||
		len(restored.ReviewCycle.CanonicalFindings) != 1 ||
		restored.ReviewCycle.CanonicalFindings[0].ID !=
			current.ReviewCycle.CanonicalFindings[0].ID {
		t.Fatalf(
			"restart changed canonical finding identity: current=%#v restored=%#v",
			current.ReviewCycle.CanonicalFindings,
			restored.ReviewCycle,
		)
	}
}

func TestScheduledDiscoveryIntakeRejectsUnsupportedCoverage(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	attachDiscoveryPlanToArtifactHarness(t, harness)
	if _, err := harness.bot.beginReviewDiscoveryPass(
		harness.reviewer.ID,
		1,
	); err != nil {
		t.Fatalf("beginReviewDiscoveryPass() error = %v", err)
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
		"unsupported coverage",
	)
	envelope.Payload.Discovery.Coverage = []ReviewCoverageClaim{{
		RequirementID: "risk-domain:not-in-plan",
		Kind:          ReviewCoverageRiskDomain,
		Status:        ReviewCoverageCovered,
		Evidence: []ReviewEvidence{{
			Summary: "unsupported claim",
			Path:    "tracked.txt",
		}},
	}}
	path := harness.publish(t, envelope)
	_, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	)
	assertReviewArtifactError(
		t,
		err,
		ReviewArtifactFailureTerminal,
		ReviewArtifactFailureMalformed,
	)
	artifactErr := asReviewArtifactError(err)
	if artifactErr == nil || !strings.Contains(
		artifactErr.Detail,
		`review coverage claim "risk-domain:not-in-plan" is not required by the exact-SHA plan`,
	) {
		t.Fatalf("unsupported coverage diagnostic = %#v", artifactErr)
	}
	stored, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared")
	}
	lane, _ := scheduledReviewDiscoveryLane(
		stored.ReviewCycle,
		1,
		harness.ownership.Identity.Lane,
	)
	if lane.Status != ReviewDiscoveryLaneRunning ||
		len(stored.ReviewCycle.LaneCompletions) != 0 {
		t.Fatalf("unsupported coverage advanced discovery = %#v", lane)
	}
}

func TestReviewDiscoverySchedulerBoundsParallelism(t *testing.T) {
	const parallelism = 2
	bot, reviewer := newReviewDiscoverySchedulerHarness(t, parallelism)
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	var mu sync.Mutex
	active := 0
	maxActive := 0
	bot.reviewDiscoveryLaneRunner = func(
		context.Context,
		string,
		int,
		string,
	) error {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()
		started <- struct{}{}
		<-release
		mu.Lock()
		active--
		mu.Unlock()
		return newReviewDiscoveryLaneRunError(
			ReviewDiscoveryFailureRuntime,
			errors.New("synthetic lane failure"),
		)
	}
	done := make(chan error, 1)
	go func() {
		_, err := bot.runReviewDiscoveryPass(
			context.Background(),
			reviewer.ID,
			1,
		)
		done <- err
	}()
	for index := 0; index < parallelism; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("parallel discovery lanes did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("discovery parallelism exceeded policy before a slot released")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("contained discovery failures escaped the pass: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bounded discovery scheduler did not drain")
	}
	mu.Lock()
	defer mu.Unlock()
	if maxActive > parallelism || active != 0 {
		t.Fatalf(
			"discovery concurrency = max:%d active:%d, want max<=%d and drained",
			maxActive,
			active,
			parallelism,
		)
	}
}

func TestReviewDiscoveryCancellationDrainsAllWorkers(t *testing.T) {
	const parallelism = 2
	bot, reviewer := newReviewDiscoverySchedulerHarness(t, parallelism)
	started := make(chan struct{}, parallelism)
	var mu sync.Mutex
	active := 0
	bot.reviewDiscoveryLaneRunner = func(
		ctx context.Context,
		_ string,
		_ int,
		_ string,
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
		_, err := bot.runReviewDiscoveryPass(ctx, reviewer.ID, 1)
		done <- err
	}()
	for index := 0; index < parallelism; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("cancel test workers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"runReviewDiscoveryPass(canceled) error = %v, want cancellation",
				err,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled discovery scheduler leaked workers")
	}
	mu.Lock()
	if active != 0 {
		t.Fatalf("active discovery workers after cancellation = %d", active)
	}
	mu.Unlock()
	stored, ok := bot.agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after cancellation")
	}
	latest, _ := latestReviewDiscoveryPassSnapshot(stored.ReviewCycle)
	for _, lane := range latest.Lanes {
		if lane.Status != ReviewDiscoveryLaneFailed ||
			lane.FailureCode != ReviewDiscoveryFailureCanceled {
			t.Fatalf("canceled lane state = %#v", lane)
		}
	}
}

func TestReviewDiscoveryProductionRunnerAcceptsLiveRetryArtifact(
	t *testing.T,
) {
	bot, reviewer, runner := newProductionReviewDiscoverySchedulerHarness(
		t,
		1,
		2,
	)
	state, err := bot.runReviewDiscoveryPass(
		context.Background(),
		reviewer.ID,
		1,
	)
	if err != nil {
		t.Fatalf("runReviewDiscoveryPass() error = %v", err)
	}
	if state.CompletedAt.IsZero() {
		t.Fatal("production discovery pass did not complete")
	}

	runner.mu.Lock()
	startsByLane := make(map[string]int, len(runner.startsByLane))
	for lane, attempts := range runner.startsByLane {
		startsByLane[lane] = attempts
	}
	publishedAlive := make(map[string]bool, len(runner.publishedAlive))
	for lane, alive := range runner.publishedAlive {
		publishedAlive[lane] = alive
	}
	stopCalls := append([]RuntimeHandle(nil), runner.stopCalls...)
	runner.mu.Unlock()

	if len(startsByLane) != len(state.Lanes) {
		t.Fatalf(
			"production runner started lanes = %v, want %d lanes",
			startsByLane,
			len(state.Lanes),
		)
	}
	for _, lane := range state.Lanes {
		if lane.Status != ReviewDiscoveryLaneCompleted ||
			lane.Attempt != 2 ||
			lane.WorkerID == "" {
			t.Fatalf("retried production lane state = %#v", lane)
		}
		if startsByLane[lane.Lane] != 2 {
			t.Fatalf(
				"production starts for lane %q = %d, want 2",
				lane.Lane,
				startsByLane[lane.Lane],
			)
		}
		if !publishedAlive[lane.Lane] {
			t.Fatalf(
				"lane %q artifact was not accepted while its runtime was alive",
				lane.Lane,
			)
		}
	}
	if len(stopCalls) != len(state.Lanes) {
		t.Fatalf(
			"production runtime stops = %d, want %d accepted live workers",
			len(stopCalls),
			len(state.Lanes),
		)
	}

	stored, ok := bot.agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after production discovery")
	}
	ownershipsByLane := make(map[string][]ReviewWorkerOwnership)
	for _, ownership := range stored.ReviewCycle.WorkerOwnerships {
		ownershipsByLane[ownership.Identity.Lane] = append(
			ownershipsByLane[ownership.Identity.Lane],
			ownership,
		)
	}
	for _, lane := range state.Lanes {
		ownerships := ownershipsByLane[lane.Lane]
		if len(ownerships) != 2 ||
			ownerships[0].Attempt != 1 ||
			ownerships[1].Attempt != 2 ||
			ownerships[0].OwnerID == ownerships[1].OwnerID {
			t.Fatalf(
				"fresh retry ownerships for lane %q = %#v",
				lane.Lane,
				ownerships,
			)
		}
		if ownerships[0].Failure == nil ||
			!ownerships[0].Failure.Retryable {
			t.Fatalf(
				"first retryable discovery failure = %#v",
				ownerships[0].Failure,
			)
		}
	}
}

func TestNotApplicableTestFixtureCoverageClosesGap(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:              "changed-branches:test-fixture",
		Kind:            ReviewCoverageChangedBranch,
		Description:     "error guard around test fixture setup",
		Critical:        true,
		AccountableLane: "contract",
		ChangedTargets: []ReviewCoverageTarget{{
			Path:      "internal/example_test.go",
			StartLine: 42,
			EndLine:   44,
		}},
	}
	claim := ReviewCoverageClaim{
		RequirementID: requirement.ID,
		Kind:          requirement.Kind,
		Status:        ReviewCoverageNotApplicable,
		Evidence: []ReviewEvidence{{
			Summary:   "the changed branch only converts an os.Chmod fixture failure into t.Fatal and does not change product behavior",
			Path:      "internal/example_test.go",
			StartLine: 42,
			EndLine:   44,
		}},
	}

	gaps, err := aggregateReviewCoverageGaps(
		[]ReviewCoverageRequirement{requirement},
		[]ReviewCoverageClaim{claim},
	)
	if err != nil {
		t.Fatalf("aggregateReviewCoverageGaps() error = %v", err)
	}
	if len(gaps) != 0 {
		t.Fatalf("not-applicable test-fixture coverage left gaps = %#v", gaps)
	}
}

func TestAcceptanceCriterionCannotBeMarkedNotApplicable(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:          "acceptance-criterion:000001",
		Kind:        ReviewCoverageAcceptanceCriterion,
		Description: "the requested behavior works",
	}
	err := validateReviewCoverageClaims(
		[]ReviewCoverageClaim{{
			RequirementID: requirement.ID,
			Kind:          requirement.Kind,
			Status:        ReviewCoverageNotApplicable,
			Evidence: []ReviewEvidence{{
				Summary: "attempted invalid dismissal",
			}},
		}},
		[]ReviewCoverageRequirement{requirement},
	)
	if err == nil || !strings.Contains(err.Error(), "acceptance criterion") {
		t.Fatalf("not-applicable acceptance criterion error = %v", err)
	}
}

func TestReviewDiscoveryRuntimeDeductsWorktreeSetupFromCycleBudget(
	t *testing.T,
) {
	bot, reviewer, _ := newProductionReviewDiscoverySchedulerHarness(
		t,
		0,
		1,
	)
	runner := &testIsolatedReviewWorkerRunner{alive: true}
	bot.runner = runner
	const (
		remainingBeforeSetup = 2 * time.Second
		setupDuration        = 600 * time.Millisecond
	)
	prepare := bot.prepareReviewWorkerWorktreeFunc
	bot.prepareReviewWorkerWorktreeFunc = func(
		ctx context.Context,
		worker Agent,
	) error {
		time.Sleep(setupDuration)
		return prepare(ctx, worker)
	}
	state, err := bot.beginReviewDiscoveryPass(reviewer.ID, 1)
	if err != nil {
		t.Fatalf("beginReviewDiscoveryPass() error = %v", err)
	}
	current, ok := bot.agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared before discovery timeout")
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
	bot.agents.mu.Lock()
	bot.agents.agents[reviewer.ID].ReviewCycle.Metrics = metrics
	bot.agents.mu.Unlock()
	if err := bot.markReviewDiscoveryLaneRunning(
		reviewer.ID,
		state.Pass,
		state.Lanes[0].Lane,
		nil,
	); err != nil {
		t.Fatalf("markReviewDiscoveryLaneRunning() error = %v", err)
	}

	runErr := bot.runReviewDiscoveryLane(
		context.Background(),
		reviewer.ID,
		state.Pass,
		state.Lanes[0].Lane,
	)
	if reviewDiscoveryFailureCodeForError(runErr) !=
		ReviewDiscoveryFailureRuntime {
		t.Fatalf(
			"discovery timeout code = %q, error=%v",
			reviewDiscoveryFailureCodeForError(runErr),
			runErr,
		)
	}
	var runtimeTimeout *reviewWorkerRuntimeTimeoutError
	if !errors.As(runErr, &runtimeTimeout) {
		t.Fatalf("discovery timeout classification = %v", runErr)
	}
	if runtimeTimeout.timeout <= 0 ||
		runtimeTimeout.timeout >= remainingBeforeSetup-setupDuration+100*time.Millisecond {
		t.Fatalf(
			"discovery runtime allowance after %s setup = %s, want setup time deducted from %s remaining cycle budget",
			setupDuration,
			runtimeTimeout.timeout,
			remainingBeforeSetup,
		)
	}
	if len(runner.started) != 1 || len(runner.stopCalls) != 1 {
		t.Fatalf(
			"discovery runtime starts/stops = %d/%d, want 1/1",
			len(runner.started),
			len(runner.stopCalls),
		)
	}
}

func TestReviewDiscoveryCorrectsMalformedArtifactInSameRuntime(
	t *testing.T,
) {
	bot, reviewer, runner := newProductionReviewDiscoverySchedulerHarness(
		t,
		0,
		1,
	)
	runner.malformedArtifactsBeforeValid = 1
	state, err := bot.runReviewDiscoveryPass(
		context.Background(),
		reviewer.ID,
		1,
	)
	if err != nil {
		t.Fatalf("runReviewDiscoveryPass() error = %v", err)
	}
	if !reviewDiscoveryPassCompletedSuccessfully(&state) {
		t.Fatalf("corrected discovery pass did not complete: %#v", state)
	}

	runner.mu.Lock()
	startsByLane := make(map[string]int, len(runner.startsByLane))
	for lane, attempts := range runner.startsByLane {
		startsByLane[lane] = attempts
	}
	correctionPrompts := append([]string(nil), runner.correctionPrompts...)
	runner.mu.Unlock()
	if len(correctionPrompts) != len(state.Lanes) {
		t.Fatalf(
			"correction prompts = %d, want one for each of %d lanes",
			len(correctionPrompts),
			len(state.Lanes),
		)
	}
	for _, prompt := range correctionPrompts {
		if !strings.Contains(prompt, `json: unknown field \"malformed\"`) ||
			!strings.Contains(prompt, "correct only the JSON handoff") {
			t.Fatalf("correction prompt lacks exact guidance: %s", prompt)
		}
	}

	stored, ok := bot.agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after artifact correction")
	}
	if len(stored.ReviewCycle.ArtifactFailures) != 0 {
		t.Fatalf(
			"corrected artifacts recorded terminal failures: %#v",
			stored.ReviewCycle.ArtifactFailures,
		)
	}
	for _, lane := range state.Lanes {
		if startsByLane[lane.Lane] != 1 ||
			lane.Status != ReviewDiscoveryLaneCompleted ||
			lane.Attempt != 1 {
			t.Fatalf(
				"lane %q correction launched a replacement: starts=%d state=%#v",
				lane.Lane,
				startsByLane[lane.Lane],
				lane,
			)
		}
	}
}

func TestReviewDiscoveryCorrectsTwoMalformedArtifactsInSameRuntime(
	t *testing.T,
) {
	bot, reviewer, runner := newProductionReviewDiscoverySchedulerHarness(
		t,
		0,
		1,
	)
	runner.malformedArtifactsBeforeValid = 2
	state, err := bot.runReviewDiscoveryPass(
		context.Background(),
		reviewer.ID,
		1,
	)
	if err != nil {
		t.Fatalf("runReviewDiscoveryPass() error = %v", err)
	}
	if !reviewDiscoveryPassCompletedSuccessfully(&state) {
		t.Fatalf("twice-corrected discovery pass did not complete: %#v", state)
	}

	runner.mu.Lock()
	correctionPrompts := append([]string(nil), runner.correctionPrompts...)
	runner.mu.Unlock()
	if len(correctionPrompts) != 2*len(state.Lanes) {
		t.Fatalf(
			"correction prompts = %d, want two for each of %d lanes",
			len(correctionPrompts),
			len(state.Lanes),
		)
	}
	firstAttempts := 0
	secondAttempts := 0
	for _, prompt := range correctionPrompts {
		switch {
		case strings.Contains(prompt, "correction attempt 1 of 2"):
			firstAttempts++
		case strings.Contains(prompt, "correction attempt 2 of 2"):
			secondAttempts++
		default:
			t.Fatalf("correction prompt lacks bounded attempt: %s", prompt)
		}
	}
	if firstAttempts != len(state.Lanes) || secondAttempts != len(state.Lanes) {
		t.Fatalf(
			"correction attempts = (first=%d second=%d), want %d each",
			firstAttempts,
			secondAttempts,
			len(state.Lanes),
		)
	}
}

func TestReviewDiscoverySynthesisCoversLaneAfterCorrectionFails(
	t *testing.T,
) {
	bot, reviewer, runner := newProductionReviewDiscoverySchedulerHarness(
		t,
		0,
		1,
	)
	runner.uncorrectableLane = "operations-tests"
	state, err := bot.runReviewDiscoveryPass(
		context.Background(),
		reviewer.ID,
		1,
	)
	if err != nil {
		t.Fatalf("synthesis-recovered discovery returned an error: %v", err)
	}
	if !reviewDiscoveryPassCompletedSuccessfully(&state) {
		t.Fatalf("fallback synthesis did not complete the review: %#v", state)
	}
	failedLane, ok := reviewDiscoveryLaneStateByName(
		state,
		"operations-tests",
	)
	if !ok || failedLane.Status != ReviewDiscoveryLaneFailed ||
		!failedLane.RecoveredBySynthesis {
		t.Fatalf("failed specialized lane state = %#v", failedLane)
	}
	synthesis, ok := reviewDiscoveryLaneStateByName(state, reviewSynthesisLane)
	if !ok || synthesis.Status != ReviewDiscoveryLaneCompleted {
		t.Fatalf("fallback synthesis state = %#v", synthesis)
	}

	runner.mu.Lock()
	synthesisPrompt := runner.promptsByLane[reviewSynthesisLane]
	runner.mu.Unlock()
	for _, required := range []string{
		`"lane":"operations-tests"`,
		`"failure_code":"artifact_rejected"`,
		"scale, operational behavior, and regression tests",
		"perform that lane's supplied brief yourself",
	} {
		if !strings.Contains(synthesisPrompt, required) {
			t.Fatalf(
				"fallback synthesis prompt does not contain %q: %s",
				required,
				synthesisPrompt,
			)
		}
	}
}

func reviewDiscoveryLaneStateByName(
	state ReviewDiscoveryPassState,
	lane string,
) (ReviewDiscoveryLaneState, bool) {
	for _, candidate := range state.Lanes {
		if candidate.Lane == lane {
			return candidate, true
		}
	}
	return ReviewDiscoveryLaneState{}, false
}

func TestReviewDiscoveryArtifactRetryPolicy(
	t *testing.T,
) {
	t.Run("transient failures exhaust configured retries", func(t *testing.T) {
		const retries = 2
		bot, reviewer := newReviewDiscoverySchedulerHarnessWithPolicy(
			t,
			2,
			func(policy *ReviewPolicy) {
				policy.Swarm.Retries = retries
				policy.Artifacts.Retries = 0
			},
		)

		var mu sync.Mutex
		attempts := make(map[string]int)
		bot.reviewDiscoveryLaneRunner = func(
			_ context.Context,
			_ string,
			_ int,
			lane string,
		) error {
			mu.Lock()
			attempts[lane]++
			mu.Unlock()
			return newReviewDiscoveryLaneRunError(
				ReviewDiscoveryFailureArtifact,
				newReviewArtifactError(
					ReviewArtifactFailureTransient,
					ReviewArtifactFailureIO,
				),
			)
		}

		state, err := bot.runReviewDiscoveryPass(
			context.Background(),
			reviewer.ID,
			1,
		)
		if err != nil {
			t.Fatalf("contained discovery failures escaped the pass: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		for _, lane := range state.Lanes {
			if lane.Lane == reviewSynthesisLane {
				if attempts[lane.Lane] != retries+1 ||
					lane.Status != ReviewDiscoveryLaneFailed ||
					lane.FailureCode != ReviewDiscoveryFailureArtifact {
					t.Fatalf("fallback synthesis lane = %#v attempts=%d", lane, attempts[lane.Lane])
				}
				continue
			}
			if attempts[lane.Lane] != retries+1 {
				t.Fatalf(
					"attempts for lane %q = %d, want %d",
					lane.Lane,
					attempts[lane.Lane],
					retries+1,
				)
			}
			if lane.Status != ReviewDiscoveryLaneFailed ||
				lane.FailureCode != ReviewDiscoveryFailureArtifact {
				t.Fatalf("exhausted lane state = %#v", lane)
			}
		}
	})

	t.Run("terminal trust failure is final", func(t *testing.T) {
		bot, reviewer := newReviewDiscoverySchedulerHarnessWithPolicy(
			t,
			2,
			func(policy *ReviewPolicy) {
				policy.Swarm.Retries = 3
				policy.Artifacts.Retries = 0
			},
		)

		var mu sync.Mutex
		attempts := make(map[string]int)
		bot.reviewDiscoveryLaneRunner = func(
			_ context.Context,
			_ string,
			_ int,
			lane string,
		) error {
			mu.Lock()
			attempts[lane]++
			mu.Unlock()
			return newReviewDiscoveryLaneRunError(
				ReviewDiscoveryFailureArtifact,
				newReviewArtifactError(
					ReviewArtifactFailureTerminal,
					ReviewArtifactFailureSecretMaterial,
				),
			)
		}

		state, err := bot.runReviewDiscoveryPass(
			context.Background(),
			reviewer.ID,
			1,
		)
		if err != nil {
			t.Fatalf("contained discovery failures escaped the pass: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		for _, lane := range state.Lanes {
			if lane.Lane == reviewSynthesisLane {
				if attempts[lane.Lane] != 1 ||
					lane.Status != ReviewDiscoveryLaneFailed ||
					lane.FailureCode != ReviewDiscoveryFailureArtifact {
					t.Fatalf("fallback synthesis lane = %#v attempts=%d", lane, attempts[lane.Lane])
				}
				continue
			}
			if attempts[lane.Lane] != 1 {
				t.Fatalf(
					"terminal attempts for lane %q = %d, want 1",
					lane.Lane,
					attempts[lane.Lane],
				)
			}
		}
	})
}

func TestReviewDiscoveryPauseDrainsPassAndAllowsNextPassAfterUnpause(
	t *testing.T,
) {
	const parallelism = 2
	bot, reviewer := newReviewDiscoverySchedulerHarness(t, parallelism)
	bot.runner = &stubRunner{
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-discovery-unpaused",
		},
	}
	started := make(chan struct{}, parallelism)
	bot.reviewDiscoveryLaneRunner = func(
		ctx context.Context,
		_ string,
		_ int,
		_ string,
	) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	passDone := make(chan error, 1)
	go func() {
		_, err := bot.runReviewDiscoveryPass(
			context.Background(),
			reviewer.ID,
			1,
		)
		passDone <- err
	}()
	for index := 0; index < parallelism; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("discovery pass did not start before pause")
		}
	}

	if err := bot.PauseAgent(
		context.Background(),
		reviewer.ID,
	); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}
	select {
	case err := <-passDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"paused discovery pass error = %v, want cancellation",
				err,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pause did not drain the active discovery pass")
	}

	paused, ok := bot.agents.Get(reviewer.ID)
	if !ok || !paused.Paused || paused.ReviewCycle == nil {
		t.Fatalf("paused review coordinator = %#v", paused)
	}
	firstPass, _ := latestReviewDiscoveryPassSnapshot(paused.ReviewCycle)
	if firstPass.CompletedAt.IsZero() {
		t.Fatal("pause persisted before the canceled pass became terminal")
	}
	for _, lane := range firstPass.Lanes {
		if lane.Status != ReviewDiscoveryLaneFailed ||
			lane.FailureCode != ReviewDiscoveryFailureCanceled {
			t.Fatalf("pause-terminalized lane = %#v", lane)
		}
	}
	restarted := &Orchestrator{
		cfg:    bot.cfg,
		agents: NewAgentManager(),
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState(after pause) error = %v", err)
	}
	persisted, ok := restarted.agents.Get(reviewer.ID)
	if !ok || !persisted.Paused || persisted.ReviewCycle == nil {
		t.Fatalf("persisted paused coordinator = %#v", persisted)
	}
	persistedPass, _ := latestReviewDiscoveryPassSnapshot(
		persisted.ReviewCycle,
	)
	if persistedPass.CompletedAt.IsZero() {
		t.Fatal("persisted paused coordinator retained an active pass")
	}

	if err := bot.UnpauseAgent(
		context.Background(),
		reviewer.ID,
	); err != nil {
		t.Fatalf("UnpauseAgent() error = %v", err)
	}
	if _, err := bot.beginReviewDiscoveryPass(
		reviewer.ID,
		2,
	); err != nil {
		t.Fatalf(
			"beginReviewDiscoveryPass(after unpause) error = %v",
			err,
		)
	}
}

type productionReviewDiscoveryRunner struct {
	mu                            sync.Mutex
	bot                           *Orchestrator
	publishAttempt                int
	malformedArtifactsBeforeValid int
	malformedArtifactsLeftByOwner map[string]int
	uncorrectableLane             string
	startsByLane                  map[string]int
	promptsByLane                 map[string]string
	publishedAlive                map[string]bool
	aliveBySession                map[string]bool
	reviewerBySession             map[string]string
	ownerBySession                map[string]string
	correctionPrompts             []string
	stopCalls                     []RuntimeHandle
}

func (runner *productionReviewDiscoveryRunner) Start(
	agent Agent,
	_ string,
) (RuntimeHandle, error) {
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: tmuxSessionName(agent),
	}, nil
}

func (runner *productionReviewDiscoveryRunner) StartReviewWorker(
	agent Agent,
	_ string,
	_ []string,
) (RuntimeHandle, error) {
	return runner.StartReviewWorkerContext(
		context.Background(),
		agent,
		"",
		nil,
	)
}

func (runner *productionReviewDiscoveryRunner) StartReviewWorkerContext(
	ctx context.Context,
	agent Agent,
	prompt string,
	_ []string,
) (RuntimeHandle, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeHandle{}, err
	}
	reviewer, ok := runner.bot.agents.Get(agent.ParentAgentID)
	if !ok || reviewer.ReviewCycle == nil {
		return RuntimeHandle{}, errors.New("review coordinator disappeared")
	}
	var ownership ReviewWorkerOwnership
	for _, candidate := range reviewer.ReviewCycle.WorkerOwnerships {
		if candidate.OwnerID == agent.ID {
			ownership = candidate
			break
		}
	}
	if ownership.OwnerID == "" {
		return RuntimeHandle{}, errors.New("review worker ownership disappeared")
	}
	handle := RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: ownership.SessionName,
	}
	runner.mu.Lock()
	if runner.startsByLane == nil {
		runner.startsByLane = make(map[string]int)
		runner.publishedAlive = make(map[string]bool)
		runner.aliveBySession = make(map[string]bool)
		runner.reviewerBySession = make(map[string]string)
		runner.ownerBySession = make(map[string]string)
		runner.promptsByLane = make(map[string]string)
		runner.malformedArtifactsLeftByOwner = make(map[string]int)
	}
	runner.startsByLane[ownership.Identity.Lane]++
	runner.promptsByLane[ownership.Identity.Lane] = prompt
	attempt := runner.startsByLane[ownership.Identity.Lane]
	publish := attempt == runner.publishAttempt
	runner.aliveBySession[handle.Session] = publish
	runner.reviewerBySession[handle.Session] = agent.ParentAgentID
	runner.ownerBySession[handle.Session] = ownership.OwnerID
	if _, ok := runner.malformedArtifactsLeftByOwner[ownership.OwnerID]; !ok {
		runner.malformedArtifactsLeftByOwner[ownership.OwnerID] =
			runner.malformedArtifactsBeforeValid
	}
	publishMalformed :=
		runner.malformedArtifactsLeftByOwner[ownership.OwnerID] > 0
	if publishMalformed {
		runner.malformedArtifactsLeftByOwner[ownership.OwnerID]--
	}
	runner.mu.Unlock()
	if !publish {
		return handle, nil
	}

	if publishMalformed ||
		ownership.Identity.Lane == runner.uncorrectableLane {
		directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
		if err != nil {
			return RuntimeHandle{}, err
		}
		if err := os.WriteFile(
			filepath.Join(directory, reviewArtifactFilename(ownership)),
			[]byte(`{"malformed":true}`),
			0o600,
		); err != nil {
			return RuntimeHandle{}, err
		}
		return handle, nil
	}
	if err := runner.publishValidArtifact(
		agent.ParentAgentID,
		ownership,
		handle,
	); err != nil {
		return RuntimeHandle{}, err
	}
	return handle, nil
}

func (runner *productionReviewDiscoveryRunner) publishValidArtifact(
	reviewerID string,
	ownership ReviewWorkerOwnership,
	handle RuntimeHandle,
) error {
	reviewer, ok := runner.bot.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return errors.New("review coordinator disappeared")
	}
	directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
	if err != nil {
		return err
	}
	store, err := newReviewArtifactStore(
		directory,
		reviewer.ReviewCycle.Policy.Artifacts,
	)
	if err != nil {
		return err
	}
	envelope, err := newReviewArtifactEnvelope(
		reviewer.ReviewCycle.HeadSHA,
		ownership,
		ReviewArtifactPhaseDiscovery,
		ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadDiscovery,
			Discovery: &ReviewDiscoveryPayload{
				Summary:         "production runner finding discovery",
				Candidates:      []ReviewFindingCandidate{},
				Coverage:        []ReviewCoverageClaim{},
				UnreviewedAreas: []string{},
			},
		},
	)
	if err != nil {
		return err
	}
	if _, err := store.publish(
		context.Background(),
		reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	); err != nil {
		return err
	}
	runner.mu.Lock()
	runner.publishedAlive[ownership.Identity.Lane] =
		runner.aliveBySession[handle.Session]
	runner.mu.Unlock()
	return nil
}

func (runner *productionReviewDiscoveryRunner) ValidateReviewWorkerIsolation(
	_ []string,
) error {
	return nil
}

func (runner *productionReviewDiscoveryRunner) Send(
	handle RuntimeHandle,
	prompt string,
) error {
	runner.mu.Lock()
	runner.correctionPrompts = append(runner.correctionPrompts, prompt)
	reviewerID := runner.reviewerBySession[handle.Session]
	ownerID := runner.ownerBySession[handle.Session]
	publishMalformed := runner.malformedArtifactsLeftByOwner[ownerID] > 0
	if publishMalformed {
		runner.malformedArtifactsLeftByOwner[ownerID]--
	}
	runner.mu.Unlock()
	reviewer, ok := runner.bot.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return errors.New("review coordinator disappeared")
	}
	for _, ownership := range reviewer.ReviewCycle.WorkerOwnerships {
		if ownership.OwnerID == ownerID {
			if publishMalformed ||
				ownership.Identity.Lane == runner.uncorrectableLane {
				directory, err := reviewWorkerArtifactDirectory(
					ownership.WorktreePath,
				)
				if err != nil {
					return err
				}
				return os.WriteFile(
					filepath.Join(
						directory,
						reviewArtifactFilename(ownership),
					),
					[]byte(`{"malformed":true}`),
					0o600,
				)
			}
			return runner.publishValidArtifact(reviewerID, ownership, handle)
		}
	}
	return errors.New("review worker ownership disappeared")
}

func (runner *productionReviewDiscoveryRunner) Capture(
	RuntimeHandle,
	int,
) (string, error) {
	return "", nil
}

func (runner *productionReviewDiscoveryRunner) Stop(
	handle RuntimeHandle,
) error {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.aliveBySession[handle.Session] = false
	runner.stopCalls = append(runner.stopCalls, handle)
	return nil
}

func (runner *productionReviewDiscoveryRunner) IsAlive(
	handle RuntimeHandle,
) (bool, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.aliveBySession[handle.Session], nil
}

func newProductionReviewDiscoverySchedulerHarness(
	t *testing.T,
	retries int,
	publishAttempt int,
) (*Orchestrator, Agent, *productionReviewDiscoveryRunner) {
	t.Helper()
	seedRepo := filepath.Join(t.TempDir(), "seed")
	if err := os.Mkdir(seedRepo, 0o755); err != nil {
		t.Fatalf("Mkdir(seed repo) error = %v", err)
	}
	runReviewArtifactGit(t, seedRepo, "init", "--initial-branch=main")
	runReviewArtifactGit(
		t,
		seedRepo,
		"config",
		"user.email",
		"test@example.com",
	)
	runReviewArtifactGit(
		t,
		seedRepo,
		"config",
		"user.name",
		"Discovery Test",
	)
	if err := os.WriteFile(
		filepath.Join(seedRepo, "tracked.txt"),
		[]byte("clean\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(seed tracked file) error = %v", err)
	}
	runReviewArtifactGit(t, seedRepo, "add", "tracked.txt")
	runReviewArtifactGit(
		t,
		seedRepo,
		"commit",
		"-m",
		"test: seed discovery repo",
	)
	headSHA := strings.TrimSpace(
		runReviewArtifactGit(t, seedRepo, "rev-parse", "HEAD"),
	)

	policy := builtInReviewPolicy()
	policy.Swarm.Retries = retries
	policy.Artifacts.Retries = 0
	policy.Artifacts.TimeoutSeconds = 2
	policy.Swarm.MaxParallelReviewers = 2
	policy.Convergence.MaxReviewAgentsPerSHA = 20
	cycle, err := newReviewCycleState(headSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	fixture := loadReviewPlanFixtures(t)[1]
	inputs := reviewPlanInputsForFiles(
		t,
		fixture.Files,
		fixture.AcceptanceCriteria,
	)
	inputs.HeadSHA = headSHA
	plan, err := buildReviewPlan(inputs, cycle.Policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	if err := attachReviewPlanInputs(cycle, inputs); err != nil {
		t.Fatalf("attachReviewPlanInputs() error = %v", err)
	}
	if err := attachReviewPlan(cycle, plan); err != nil {
		t.Fatalf("attachReviewPlan() error = %v", err)
	}
	profile, err := cycle.Policy.effectiveProfileForRole(
		AgentProfileRoleChallenge,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := Agent{
		ID:                "review-discovery-production",
		Role:              RoleReviewer,
		IssueNumber:       37,
		IssueTitle:        "Run production discovery",
		PRNumber:          50,
		PRTitle:           "Run production discovery",
		PRURL:             "https://example.test/pull/50",
		ObservedPRHeadSHA: headSHA,
		RuntimeProfile:    profile,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-discovery-production",
		},
		State:            StateWorking,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(&reviewer); err != nil {
		t.Fatalf("AgentManager.Add() error = %v", err)
	}
	worktreeDir := filepath.Join(t.TempDir(), "workers")
	if err := os.Mkdir(worktreeDir, 0o755); err != nil {
		t.Fatalf("Mkdir(worker root) error = %v", err)
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     seedRepo,
			LogDir:       t.TempDir(),
			WorktreeDir:  worktreeDir,
			ReviewPolicy: cycle.Policy,
		},
		agents: agents,
	}
	bot.prepareReviewWorkerWorktreeFunc = func(
		ctx context.Context,
		worker Agent,
	) error {
		command := newCommandContext(
			ctx,
			"git",
			"clone",
			"--quiet",
			seedRepo,
			worker.WorktreePath,
		)
		output, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf(
				"failed to clone discovery worker checkout: %w: %s",
				err,
				strings.TrimSpace(string(output)),
			)
		}
		return nil
	}
	runner := &productionReviewDiscoveryRunner{
		bot:            bot,
		publishAttempt: publishAttempt,
	}
	bot.runner = runner
	return bot, reviewer, runner
}

func testReviewFindingCandidate() ReviewFindingCandidate {
	return ReviewFindingCandidate{
		CandidateID: "candidate-correctness",
		Summary:     "Cleanup can be overtaken by coordinator exit",
		Location: ReviewFindingLocation{
			Path:      "internal/review_workflow.go",
			Symbol:    "pollReviewAgent",
			StartLine: 40,
			EndLine:   45,
		},
		BehavioralPath:    "poll exits while cleanup is running",
		ViolatedInvariant: "cleanup finishes before exit",
		Severity:          ReviewFindingSeverityHigh,
		Confidence:        ReviewFindingConfidenceMedium,
		Evidence: []ReviewEvidence{{
			Summary:   "return happens before cleanup joins",
			Path:      "internal/review_workflow.go",
			StartLine: 40,
			EndLine:   45,
		}},
	}
}

func sortReviewCoverageRequirementsForTest(
	requirements []ReviewCoverageRequirement,
) {
	for left := 0; left < len(requirements); left++ {
		for right := left + 1; right < len(requirements); right++ {
			leftKey := string(requirements[left].Kind) + "\x00" +
				requirements[left].ID
			rightKey := string(requirements[right].Kind) + "\x00" +
				requirements[right].ID
			if rightKey < leftKey {
				requirements[left], requirements[right] =
					requirements[right], requirements[left]
			}
		}
	}
}

func planPolicySnapshot(t *testing.T, plan ReviewPlan) ReviewPolicy {
	t.Helper()
	policy := snapshottedReviewPlanPolicy(t, nil)
	if policy.Fingerprint != plan.PolicyFingerprint {
		t.Fatalf(
			"test policy fingerprint = %q, want plan %q",
			policy.Fingerprint,
			plan.PolicyFingerprint,
		)
	}
	return policy
}

func newReviewDiscoverySchedulerHarness(
	t *testing.T,
	parallelism int,
) (*Orchestrator, Agent) {
	t.Helper()
	return newReviewDiscoverySchedulerHarnessWithPolicy(
		t,
		parallelism,
		nil,
	)
}

func newReviewDiscoverySchedulerHarnessWithPolicy(
	t *testing.T,
	parallelism int,
	mutate func(*ReviewPolicy),
) (*Orchestrator, Agent) {
	t.Helper()
	policy := builtInReviewPolicy()
	policy.Swarm.MaxParallelReviewers = parallelism
	if mutate != nil {
		mutate(&policy)
	}
	minimumCapacity, err := minimumReviewLifecycleCapacity(policy)
	if err != nil {
		t.Fatalf("minimumReviewLifecycleCapacity() error = %v", err)
	}
	policy.Convergence.MaxReviewAgentsPerSHA = int(minimumCapacity)
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	fixture := loadReviewPlanFixtures(t)[1]
	inputs := reviewPlanInputsForFiles(
		t,
		fixture.Files,
		fixture.AcceptanceCriteria,
	)
	plan, err := buildReviewPlan(inputs, cycle.Policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	if err := attachReviewPlanInputs(cycle, inputs); err != nil {
		t.Fatalf("attachReviewPlanInputs() error = %v", err)
	}
	if err := attachReviewPlan(cycle, plan); err != nil {
		t.Fatalf("attachReviewPlan() error = %v", err)
	}
	profile, err := cycle.Policy.effectiveProfileForRole(
		AgentProfileRoleChallenge,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := Agent{
		ID:                "review-discovery-scheduler",
		Role:              RoleReviewer,
		IssueNumber:       37,
		IssueTitle:        "Run discovery",
		ObservedPRHeadSHA: cycle.HeadSHA,
		RuntimeProfile:    profile,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-discovery-scheduler",
		},
		State:            StateWorking,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(&reviewer); err != nil {
		t.Fatalf("AgentManager.Add() error = %v", err)
	}
	bot := &Orchestrator{
		cfg: Config{
			LogDir:       t.TempDir(),
			WorktreeDir:  t.TempDir(),
			ReviewPolicy: cycle.Policy,
		},
		agents: agents,
	}
	return bot, reviewer
}

func assertPersistedDiscoveryStatus(
	t *testing.T,
	bot *Orchestrator,
	reviewerID string,
	lane string,
	want ReviewDiscoveryLaneStatus,
) {
	t.Helper()
	restarted := &Orchestrator{
		cfg:    bot.cfg,
		agents: NewAgentManager(),
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	reviewer, ok := restarted.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		t.Fatalf("persisted reviewer %q was not restored", reviewerID)
	}
	state, ok := scheduledReviewDiscoveryLane(
		reviewer.ReviewCycle,
		1,
		lane,
	)
	if !ok || state.Status != want {
		t.Fatalf(
			"persisted discovery lane %q = %#v, want status %q",
			lane,
			state,
			want,
		)
	}
}

func attachDiscoveryPlanToArtifactHarness(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
) {
	t.Helper()
	harness.agents.mu.Lock()
	defer harness.agents.mu.Unlock()
	reviewer := harness.agents.agents[harness.reviewer.ID]
	inputs := ReviewPlanInputs{
		SchemaVersion:    reviewPlanInputsSchemaVersion,
		BaseSHA:          testOtherReviewHeadSHA,
		HeadSHA:          reviewer.ReviewCycle.HeadSHA,
		ChangedFileCount: 1,
		ChangedLineCount: 1,
		ChangedFiles: []ReviewPlanChangedFile{{
			Path:      "tracked.txt",
			Additions: 1,
		}},
		AcceptanceCriteria: []string{
			"Discovery records evidence",
			"Discovery reports unreviewed behavior",
		},
	}
	if err := validateReviewPlanInputs(inputs); err != nil {
		t.Fatalf("validateReviewPlanInputs() error = %v", err)
	}
	plan, err := buildReviewPlan(inputs, reviewer.ReviewCycle.Policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	if err := attachReviewPlanInputs(reviewer.ReviewCycle, inputs); err != nil {
		t.Fatalf("attachReviewPlanInputs() error = %v", err)
	}
	if err := attachReviewPlan(reviewer.ReviewCycle, plan); err != nil {
		t.Fatalf("attachReviewPlan() error = %v", err)
	}
	body, err := json.Marshal(reviewer.ReviewCycle)
	if err != nil || len(body) == 0 {
		t.Fatalf("failed to encode prepared discovery cycle: %v", err)
	}
	harness.reviewer = cloneAgent(reviewer)
}
