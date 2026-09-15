// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2026 Cisco Systems, Inc. and its affiliates

package orchestrator

import "testing"

func TestReviewLedgerCoverageConsolidationKeepsDisagreementOpen(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:          "callers:runtime",
		Kind:        ReviewCoverageCallPath,
		Description: "runtime callers",
	}
	claim := consolidateReviewLedgerCoverageClaims(
		requirement,
		[]ReviewCoverageClaim{
			{
				Status: ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary: "caller handles the change",
				}},
			},
			{
				Status: ReviewCoverageNotCovered,
				Evidence: []ReviewEvidence{{
					Summary: "recovery caller was not checked",
				}},
			},
		},
	)
	if claim.RequirementID != requirement.ID ||
		claim.Kind != requirement.Kind ||
		claim.Status != ReviewCoveragePartial ||
		len(claim.Evidence) != 2 {
		t.Fatalf("consolidated disagreement = %#v", claim)
	}
}

func TestReviewLedgerCoverageConsolidationPreservesNotApplicable(t *testing.T) {
	requirement := ReviewCoverageRequirement{
		ID:          "changed-branches:test-fixture",
		Kind:        ReviewCoverageChangedBranch,
		Description: "test fixture error guard",
	}
	claim := consolidateReviewLedgerCoverageClaims(
		requirement,
		[]ReviewCoverageClaim{{
			Status: ReviewCoverageNotApplicable,
			Evidence: []ReviewEvidence{{
				Summary: "fixture setup does not exercise product behavior",
			}},
		}},
	)
	if claim.Status != ReviewCoverageNotApplicable || len(claim.Evidence) != 1 {
		t.Fatalf("consolidated not-applicable coverage = %#v", claim)
	}
}

func testReviewLedgerRuntimeCycle(
	t *testing.T,
	prior ReviewLedger,
	delta ReviewPlanInputs,
	finding ReviewCanonicalFinding,
	outcome ReviewVerificationOutcome,
	patchDisposition ReviewPatchDisposition,
) *ReviewCycleState {
	t.Helper()
	finding.ExactSHA = delta.HeadSHA
	revision, err := reviewFindingCandidateRevision(finding)
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision() error = %v", err)
	}
	status := ReviewFindingVerificationConfirmed
	if outcome == ReviewVerificationRejected {
		status = ReviewFindingVerificationRejected
	}
	assignment := ReviewVerificationAssignment{
		ID:                "verification:" + finding.ID + ":runtime",
		FindingID:         finding.ID,
		ExactSHA:          delta.HeadSHA,
		CandidateRevision: revision,
		CandidateSnapshot: finding,
		Round:             1,
		Ordinal:           1,
		Lane:              reviewVerificationLane(finding.ID, 1),
		Status:            ReviewVerificationCompleted,
		WorkerID:          "runtime-verifier-" + delta.HeadSHA,
		Attempt:           1,
		Outcome:           outcome,
		PatchDisposition:  patchDisposition,
		Evidence: []ReviewEvidence{{
			Summary: "independent runtime verification",
			Path:    finding.Location.Path,
		}},
	}
	return &ReviewCycleState{
		Attempt:            1,
		HeadSHA:            delta.HeadSHA,
		PriorReviewLedger:  cloneReviewLedger(&prior),
		ReviewLedgerInputs: &delta,
		CanonicalFindings:  []ReviewCanonicalFinding{finding},
		Convergence: &ReviewConvergenceState{
			VerificationAssignments: []ReviewVerificationAssignment{assignment},
			FindingVerifications: []ReviewFindingVerification{{
				FindingID:         finding.ID,
				ExactSHA:          delta.HeadSHA,
				CandidateRevision: revision,
				Status:            status,
			}},
		},
	}
}

func TestReviewLedgerRuntimeCarriesFixesAndRegressionsAcrossHeads(t *testing.T) {
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/runtime.go",
		"Runtime",
		"runtime state remains valid",
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(testLedgerBaseSHA, testLedgerFirstSHA, finding.Location.Path),
		testReviewLedgerCoverageRequirements(finding.Location.Path),
		[]ReviewLedgerFindingObservation{testReviewLedgerFindingObservation(
			finding,
			ReviewLedgerFindingReported,
			"initial runtime verification",
			reviewLedgerBool(false),
		)},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	fixDelta := testReviewLedgerDelta(testLedgerFirstSHA, testLedgerSecondSHA, finding.Location.Path)
	fixedCycle := testReviewLedgerRuntimeCycle(
		t,
		first.Ledger,
		fixDelta,
		finding,
		ReviewVerificationRejected,
		ReviewPatchNotReproduced,
	)
	fixObservations, err := reviewLedgerFindingObservations(fixedCycle)
	if err != nil {
		t.Fatalf("reviewLedgerFindingObservations(fixed) error = %v", err)
	}
	fixed, err := transitionReviewLedger(
		&first.Ledger,
		fixDelta,
		testReviewLedgerCoverageRequirements(finding.Location.Path),
		fixObservations,
		nil,
	)
	if err != nil {
		t.Fatalf("fixed transitionReviewLedger() error = %v", err)
	}
	fixedFinding := findingByLedgerID(t, fixed.Ledger, finding.ID)
	if fixedFinding.Status != ReviewLedgerFindingFixed ||
		fixedFinding.FixedSHA != testLedgerSecondSHA || fixedFinding.NeedsRecheck {
		t.Fatalf("fixed runtime finding = %#v", fixedFinding)
	}

	regressionDelta := testReviewLedgerDelta(testLedgerSecondSHA, testLedgerThirdSHA, finding.Location.Path)
	regressedCycle := testReviewLedgerRuntimeCycle(
		t,
		fixed.Ledger,
		regressionDelta,
		finding,
		ReviewVerificationConfirmed,
		ReviewPatchIntroduced,
	)
	regressionObservations, err := reviewLedgerFindingObservations(regressedCycle)
	if err != nil {
		t.Fatalf("reviewLedgerFindingObservations(regressed) error = %v", err)
	}
	regressed, err := transitionReviewLedger(
		&fixed.Ledger,
		regressionDelta,
		testReviewLedgerCoverageRequirements(finding.Location.Path),
		regressionObservations,
		nil,
	)
	if err != nil {
		t.Fatalf("regressed transitionReviewLedger() error = %v", err)
	}
	regressedFinding := findingByLedgerID(t, regressed.Ledger, finding.ID)
	if regressedFinding.Status != ReviewLedgerFindingReported ||
		regressedFinding.Provenance != ReviewLedgerFindingFixIntroduced ||
		regressedFinding.FirstSeenSHA != testLedgerFirstSHA {
		t.Fatalf("regressed runtime finding = %#v", regressedFinding)
	}
}

func TestReviewLedgerRuntimeKeepsStillOpenAndClassifiesNewMiss(t *testing.T) {
	priorFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/prior.go",
		"Prior",
		"prior state remains valid",
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(testLedgerBaseSHA, testLedgerFirstSHA, priorFinding.Location.Path),
		testReviewLedgerCoverageRequirements(priorFinding.Location.Path),
		[]ReviewLedgerFindingObservation{testReviewLedgerFindingObservation(
			priorFinding,
			ReviewLedgerFindingReported,
			"initial verification",
			reviewLedgerBool(false),
		)},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	delta := testReviewLedgerDelta(testLedgerFirstSHA, testLedgerSecondSHA, priorFinding.Location.Path)
	stillOpenCycle := testReviewLedgerRuntimeCycle(
		t,
		first.Ledger,
		delta,
		priorFinding,
		ReviewVerificationConfirmed,
		ReviewPatchWorsened,
	)
	stillOpenObservations, err := reviewLedgerFindingObservations(stillOpenCycle)
	if err != nil {
		t.Fatalf("reviewLedgerFindingObservations(still open) error = %v", err)
	}
	stillOpen, err := transitionReviewLedger(
		&first.Ledger,
		delta,
		testReviewLedgerCoverageRequirements(priorFinding.Location.Path),
		stillOpenObservations,
		nil,
	)
	if err != nil {
		t.Fatalf("still-open transitionReviewLedger() error = %v", err)
	}
	if got := findingByLedgerID(t, stillOpen.Ledger, priorFinding.ID); got.Status != ReviewLedgerFindingReported ||
		got.Provenance != ReviewLedgerFindingPreviouslyReported {
		t.Fatalf("still-open runtime finding = %#v", got)
	}

	newFinding := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		"internal/missed.go",
		"Missed",
		"missed state remains valid",
	)
	newCycle := testReviewLedgerRuntimeCycle(
		t,
		first.Ledger,
		delta,
		newFinding,
		ReviewVerificationConfirmed,
		ReviewPatchPreExisting,
	)
	newObservations, err := reviewLedgerFindingObservations(newCycle)
	if err != nil {
		t.Fatalf("reviewLedgerFindingObservations(new miss) error = %v", err)
	}
	newResult, err := transitionReviewLedger(
		&first.Ledger,
		delta,
		testReviewLedgerCoverageRequirements(priorFinding.Location.Path),
		newObservations,
		nil,
	)
	if err != nil {
		t.Fatalf("new-miss transitionReviewLedger() error = %v", err)
	}
	if got := findingByLedgerID(t, newResult.Ledger, newFinding.ID); got.Provenance != ReviewLedgerFindingPreviouslyMissed ||
		got.FirstSeenSHA != testLedgerSecondSHA {
		t.Fatalf("new missed runtime finding = %#v", got)
	}
}

func TestReviewCorrectionRoundEscalationUsesPersistedThreshold(t *testing.T) {
	cycle := &ReviewCycleState{
		CorrectionRound: 2,
		Policy: ReviewPolicy{Escalation: EscalationPolicy{
			AfterCorrectionRounds: 2,
		}},
	}
	if !reviewCycleRequiresHumanCorrectionEscalation(cycle, ReviewVerdictNeedsChanges) {
		t.Fatal("configured correction threshold did not require human escalation")
	}
	cycle.CorrectionRound = 1
	if reviewCycleRequiresHumanCorrectionEscalation(cycle, ReviewVerdictNeedsChanges) {
		t.Fatal("correction round below configured threshold escalated")
	}
	cycle.CorrectionRound = 2
	if reviewCycleRequiresHumanCorrectionEscalation(cycle, ReviewVerdictThumbsUp) {
		t.Fatal("THUMBS_UP escalated as a correction round")
	}
}
