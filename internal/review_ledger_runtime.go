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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

func validateReviewLedgerCycleContext(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("review cycle is missing")
	}
	if cycle.CorrectionRound < 0 {
		return errors.New("correction round cannot be negative")
	}
	if cycle.PriorReviewLedger != nil {
		if err := validateReviewLedger(cycle.PriorReviewLedger); err != nil {
			return fmt.Errorf("prior review ledger is invalid: %w", err)
		}
	}
	if (cycle.ReviewLedgerInputs == nil) != (cycle.ReviewLedgerPlan == nil) {
		return errors.New("review ledger delta inputs and plan must be recorded together")
	}
	if cycle.ReviewLedgerInputs == nil {
		return nil
	}
	if err := validateReviewPlanInputs(*cycle.ReviewLedgerInputs); err != nil {
		return err
	}
	if cycle.ReviewLedgerInputs.HeadSHA != cycle.HeadSHA {
		return errors.New("review ledger delta does not target the review head")
	}
	if cycle.PriorReviewLedger != nil {
		if cycle.ReviewLedgerInputs.BaseSHA != cycle.PriorReviewLedger.HeadSHA {
			return errors.New("review ledger delta does not continue the prior head")
		}
	} else if cycle.Inputs != nil && cycle.ReviewLedgerInputs.BaseSHA != cycle.Inputs.BaseSHA {
		return errors.New("initial review ledger delta does not start at the PR base")
	}
	temporary := *cycle
	temporary.Inputs = cycle.ReviewLedgerInputs
	temporary.Plan = cycle.ReviewLedgerPlan
	if err := validateReviewPlanAgainstCycle(&temporary); err != nil {
		return err
	}
	return nil
}

func (m *AgentManager) SetReviewLedgerCycleContext(
	reviewerID string,
	inputs ReviewPlanInputs,
	plan ReviewPlan,
) error {
	if m == nil {
		return errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer.Role != RoleReviewer || reviewer.ReviewCycle == nil {
		return fmt.Errorf("review coordinator %q was not found", strings.TrimSpace(reviewerID))
	}
	if reviewer.ReviewCycle.ReviewLedgerInputs != nil {
		if reflect.DeepEqual(*reviewer.ReviewCycle.ReviewLedgerInputs, inputs) &&
			reviewer.ReviewCycle.ReviewLedgerPlan != nil &&
			reflect.DeepEqual(*reviewer.ReviewCycle.ReviewLedgerPlan, plan) {
			return nil
		}
		return errors.New("review ledger cycle context is already recorded")
	}
	candidate := cloneReviewCycle(reviewer.ReviewCycle)
	candidate.ReviewLedgerInputs = &inputs
	candidate.ReviewLedgerPlan = &plan
	if err := validatePersistedReviewCycleSnapshot(candidate); err != nil {
		return err
	}
	reviewer.ReviewCycle = candidate
	reviewer.LastActivityTime = time.Now().UTC()
	return nil
}

func (b *Orchestrator) persistReviewLedgerCycleContext(
	reviewerID string,
	inputs ReviewPlanInputs,
	plan ReviewPlan,
) error {
	if err := b.agents.SetReviewLedgerCycleContext(reviewerID, inputs, plan); err != nil {
		return err
	}
	if err := b.persistAgentState(); err != nil {
		return fmt.Errorf("failed to persist review ledger cycle context for %s: %w", reviewerID, err)
	}
	return nil
}

func reviewLedgerSeedFindings(cycle *ReviewCycleState) []ReviewCanonicalFinding {
	if cycle == nil || cycle.PriorReviewLedger == nil {
		return nil
	}
	seeded := make([]ReviewCanonicalFinding, 0, len(cycle.PriorReviewLedger.Findings))
	for _, item := range cycle.PriorReviewLedger.Findings {
		unresolved := item.Status == ReviewLedgerFindingReported || item.Status == ReviewLedgerFindingMissed
		recentlyResolved := (item.Status == ReviewLedgerFindingFixed || item.Status == ReviewLedgerFindingRejected) &&
			item.LastObservedSHA == cycle.PriorReviewLedger.HeadSHA
		if !unresolved && !recentlyResolved {
			continue
		}
		finding := cloneReviewCanonicalFinding(item.Finding)
		finding.ExactSHA = cycle.HeadSHA
		seeded = append(seeded, finding)
	}
	return seeded
}

func mergeReviewLedgerSeedFindings(
	current []ReviewCanonicalFinding,
	seeded []ReviewCanonicalFinding,
) []ReviewCanonicalFinding {
	byID := make(map[string]ReviewCanonicalFinding, len(current)+len(seeded))
	for _, finding := range seeded {
		byID[finding.ID] = finding
	}
	for _, finding := range current {
		byID[finding.ID] = finding
	}
	merged := make([]ReviewCanonicalFinding, 0, len(byID))
	for _, finding := range byID {
		merged = append(merged, finding)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	if merged == nil {
		return []ReviewCanonicalFinding{}
	}
	return merged
}

func reviewLedgerAssignmentForDecision(
	cycle *ReviewCycleState,
	decision ReviewFindingVerification,
) (ReviewVerificationAssignment, bool) {
	if cycle == nil || cycle.Convergence == nil {
		return ReviewVerificationAssignment{}, false
	}
	want := ReviewVerificationConfirmed
	if decision.Status == ReviewFindingVerificationRejected {
		want = ReviewVerificationRejected
	}
	for _, assignment := range cycle.Convergence.VerificationAssignments {
		if !assignment.Superseded &&
			assignment.FindingID == decision.FindingID &&
			assignment.CandidateRevision == decision.CandidateRevision &&
			assignment.Status == ReviewVerificationCompleted &&
			assignment.Outcome == want {
			return assignment, true
		}
	}
	return ReviewVerificationAssignment{}, false
}

func reviewLedgerFindingObservations(cycle *ReviewCycleState) ([]ReviewLedgerFindingObservation, error) {
	if cycle == nil || cycle.Convergence == nil || cycle.ReviewLedgerInputs == nil {
		return nil, nil
	}
	prior := make(map[string]ReviewLedgerFinding)
	if cycle.PriorReviewLedger != nil {
		for _, finding := range cycle.PriorReviewLedger.Findings {
			prior[finding.ID] = finding
		}
	}
	changed := reviewLedgerChangedPaths(*cycle.ReviewLedgerInputs)
	observations := make([]ReviewLedgerFindingObservation, 0)
	for _, decision := range cycle.Convergence.FindingVerifications {
		if decision.Status != ReviewFindingVerificationConfirmed &&
			decision.Status != ReviewFindingVerificationRejected {
			continue
		}
		finding, ok := reviewCanonicalFindingByID(cycle, decision.FindingID)
		if !ok {
			return nil, fmt.Errorf("verified review finding %s is missing", decision.FindingID)
		}
		assignment, ok := reviewLedgerAssignmentForDecision(cycle, decision)
		if !ok {
			return nil, fmt.Errorf("verified review finding %s has no terminal assignment", decision.FindingID)
		}
		status := ReviewLedgerFindingReported
		if decision.Status == ReviewFindingVerificationRejected {
			if old, exists := prior[decision.FindingID]; exists {
				if (old.Status == ReviewLedgerFindingReported || old.Status == ReviewLedgerFindingMissed) &&
					(old.NeedsRecheck || reviewLedgerFindingImpacted(old, changed)) {
					status = ReviewLedgerFindingFixed
				} else {
					continue
				}
			} else {
				status = ReviewLedgerFindingRejected
			}
		}
		evidence := append([]ReviewEvidence(nil), assignment.Evidence...)
		evidence = append(evidence, assignment.CausalEvidence...)
		evidence = append(evidence, assignment.TestEvidence...)
		evidence = uniqueSortedReviewEvidence(evidence)
		present := assignment.PatchDisposition != ReviewPatchIntroduced &&
			assignment.PatchDisposition != ReviewPatchWorsened
		observations = append(observations, ReviewLedgerFindingObservation{
			Finding:              cloneReviewCanonicalFinding(*finding),
			Status:               status,
			VerificationEvidence: evidence,
			VerificationReceipt: ReviewLedgerVerificationReceipt{
				FindingID:         assignment.FindingID,
				ExactSHA:          assignment.ExactSHA,
				AssignmentID:      assignment.ID,
				CandidateRevision: assignment.CandidateRevision,
				VerifierWorkerID:  assignment.WorkerID,
				Outcome:           assignment.Outcome,
			},
			PresentInDeltaBase: &present,
		})
	}
	return observations, nil
}

func reviewCycleRequiresHumanCorrectionEscalation(
	cycle *ReviewCycleState,
	verdict ReviewVerdict,
) bool {
	return verdict == ReviewVerdictNeedsChanges && cycle != nil &&
		cycle.CorrectionRound >= cycle.Policy.Escalation.AfterCorrectionRounds
}

func reviewLedgerCoverageObservations(cycle *ReviewCycleState) []ReviewLedgerCoverageObservation {
	if cycle == nil || cycle.ReviewLedgerPlan == nil {
		return nil
	}
	latest, ok := latestReviewDiscoveryPass(cycle)
	if !ok {
		return nil
	}
	claims := make(map[string][]ReviewCoverageClaim)
	type challengeCoverage struct {
		acceptedAt time.Time
		workerID   string
		claims     []ReviewCoverageClaim
	}
	challengeClaims := make([]challengeCoverage, 0)
	for _, receipt := range cycle.ArtifactReceipts {
		switch receipt.Phase {
		case ReviewArtifactPhaseDiscovery:
			if receipt.Pass != latest.Pass || receipt.Envelope.Payload.Discovery == nil {
				continue
			}
			for _, claim := range reviewCoverageClaimsForLane(
				receipt.Envelope.Payload.Discovery.Coverage,
				cycle.ReviewLedgerPlan.CoverageRequirements,
				receipt.Lane,
			) {
				claims[claim.RequirementID] = append(
					claims[claim.RequirementID],
					claim,
				)
			}
		case ReviewArtifactPhaseChallenge:
			if receipt.Envelope.Payload.Challenge != nil {
				challengeClaims = append(challengeClaims, challengeCoverage{
					acceptedAt: receipt.AcceptedAt,
					workerID:   receipt.WorkerID,
					claims:     receipt.Envelope.Payload.Challenge.Coverage,
				})
			}
		}
	}
	sort.Slice(challengeClaims, func(i, j int) bool {
		if challengeClaims[i].acceptedAt.Equal(challengeClaims[j].acceptedAt) {
			return challengeClaims[i].workerID < challengeClaims[j].workerID
		}
		return challengeClaims[i].acceptedAt.Before(challengeClaims[j].acceptedAt)
	})
	for _, report := range challengeClaims {
		for _, claim := range report.claims {
			claims[claim.RequirementID] = []ReviewCoverageClaim{claim}
		}
	}
	observations := make([]ReviewLedgerCoverageObservation, 0, len(claims))
	for _, requirement := range cycle.ReviewLedgerPlan.CoverageRequirements {
		requirementClaims := claims[requirement.ID]
		if len(requirementClaims) == 0 {
			continue
		}
		observations = append(observations, ReviewLedgerCoverageObservation{
			Requirement: requirement,
			Claim: consolidateReviewLedgerCoverageClaims(
				requirement,
				requirementClaims,
			),
		})
	}
	return observations
}

func consolidateReviewLedgerCoverageClaims(
	requirement ReviewCoverageRequirement,
	claims []ReviewCoverageClaim,
) ReviewCoverageClaim {
	status := ReviewCoverageNotCovered
	hasCovered := false
	hasPartial := false
	hasNotApplicable := false
	allCovered := true
	allNotApplicable := true
	evidence := make([]ReviewEvidence, 0)
	for _, claim := range claims {
		switch claim.Status {
		case ReviewCoverageCovered:
			hasCovered = true
		case ReviewCoveragePartial:
			hasPartial = true
		case ReviewCoverageNotApplicable:
			hasNotApplicable = true
		}
		if claim.Status != ReviewCoverageCovered {
			allCovered = false
		}
		if claim.Status != ReviewCoverageNotApplicable {
			allNotApplicable = false
		}
		evidence = append(evidence, claim.Evidence...)
	}
	if hasCovered && allCovered {
		status = ReviewCoverageCovered
	} else if hasNotApplicable && allNotApplicable {
		status = ReviewCoverageNotApplicable
	} else if hasCovered || hasPartial || hasNotApplicable {
		status = ReviewCoveragePartial
	}
	return ReviewCoverageClaim{
		RequirementID: requirement.ID,
		Kind:          requirement.Kind,
		Status:        status,
		Evidence:      uniqueSortedReviewEvidence(evidence),
	}
}

func (b *Orchestrator) persistCompletedReviewLedger(reviewer Agent) error {
	if reviewer.ParentAgentID == "" || reviewer.ReviewCycle == nil ||
		reviewer.ReviewCycle.ReviewLedgerInputs == nil || reviewer.ReviewCycle.ReviewLedgerPlan == nil {
		return nil
	}
	observations, err := reviewLedgerFindingObservations(reviewer.ReviewCycle)
	if err != nil {
		return err
	}
	result, err := transitionReviewLedger(
		reviewer.ReviewCycle.PriorReviewLedger,
		*reviewer.ReviewCycle.ReviewLedgerInputs,
		reviewer.ReviewCycle.ReviewLedgerPlan.CoverageRequirements,
		observations,
		reviewLedgerCoverageObservations(reviewer.ReviewCycle),
	)
	if err != nil {
		return fmt.Errorf("failed to transition review ledger: %w", err)
	}
	return b.persistReviewLedger(reviewer.ParentAgentID, result.Ledger, ReviewLedgerTransitionSnapshot{
		Inputs: *reviewer.ReviewCycle.ReviewLedgerInputs,
		Plan:   *reviewer.ReviewCycle.ReviewLedgerPlan,
	})
}
