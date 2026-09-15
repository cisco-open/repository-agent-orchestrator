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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
)

type ReviewVerificationAssignmentStatus string

const (
	ReviewVerificationQueued    ReviewVerificationAssignmentStatus = "queued"
	ReviewVerificationRunning   ReviewVerificationAssignmentStatus = "running"
	ReviewVerificationCompleted ReviewVerificationAssignmentStatus = "completed"
	ReviewVerificationFailed    ReviewVerificationAssignmentStatus = "failed"
)

type ReviewVerificationFailureCode string

const (
	ReviewVerificationFailureCanceled ReviewVerificationFailureCode = "canceled"
	ReviewVerificationFailureLaunch   ReviewVerificationFailureCode = "launch_failed"
	ReviewVerificationFailureRuntime  ReviewVerificationFailureCode = "runtime_failed"
	ReviewVerificationFailureArtifact ReviewVerificationFailureCode = "artifact_rejected"
	ReviewVerificationFailureState    ReviewVerificationFailureCode = "state_persistence_failed"
	ReviewVerificationFailureEvidence ReviewVerificationFailureCode = "evidence_incomplete"
)

type ReviewVerificationAssignment struct {
	ID                string                             `json:"id"`
	FindingID         string                             `json:"finding_id"`
	ExactSHA          string                             `json:"exact_sha"`
	CandidateRevision string                             `json:"candidate_revision"`
	CandidateSnapshot ReviewCanonicalFinding             `json:"candidate_snapshot"`
	Superseded        bool                               `json:"superseded,omitempty"`
	Round             int                                `json:"round"`
	Ordinal           int                                `json:"ordinal"`
	Lane              string                             `json:"lane"`
	Status            ReviewVerificationAssignmentStatus `json:"status"`
	ExcludedWorkerIDs []string                           `json:"excluded_worker_ids"`
	WorkerID          string                             `json:"worker_id,omitempty"`
	Attempt           int                                `json:"attempt,omitempty"`
	QueuedAt          time.Time                          `json:"queued_at"`
	StartedAt         time.Time                          `json:"started_at,omitempty"`
	CompletedAt       time.Time                          `json:"completed_at,omitempty"`
	Outcome           ReviewVerificationOutcome          `json:"outcome,omitempty"`
	Summary           string                             `json:"summary,omitempty"`
	ScopeDisposition  ReviewScopeDisposition             `json:"scope_disposition,omitempty"`
	PatchDisposition  ReviewPatchDisposition             `json:"patch_disposition,omitempty"`
	Location          *ReviewFindingLocation             `json:"location,omitempty"`
	BehavioralPath    string                             `json:"behavioral_path,omitempty"`
	Evidence          []ReviewEvidence                   `json:"evidence,omitempty"`
	CausalEvidence    []ReviewEvidence                   `json:"causal_evidence,omitempty"`
	TestEvidence      []ReviewEvidence                   `json:"test_evidence,omitempty"`
	TestNotPractical  string                             `json:"test_not_practical,omitempty"`
	FailureCode       ReviewVerificationFailureCode      `json:"failure_code,omitempty"`
	Action            ReviewAction                       `json:"action,omitempty"`
}

type ReviewFindingVerificationStatus string

const (
	ReviewFindingVerificationPending      ReviewFindingVerificationStatus = "pending"
	ReviewFindingVerificationConfirmed    ReviewFindingVerificationStatus = "confirmed"
	ReviewFindingVerificationRejected     ReviewFindingVerificationStatus = "rejected"
	ReviewFindingVerificationInconclusive ReviewFindingVerificationStatus = "inconclusive"
	ReviewFindingVerificationUnresolved   ReviewFindingVerificationStatus = "unresolved"
)

type ReviewFindingVerification struct {
	FindingID         string                          `json:"finding_id"`
	ExactSHA          string                          `json:"exact_sha"`
	CandidateRevision string                          `json:"candidate_revision"`
	Status            ReviewFindingVerificationStatus `json:"status"`
	Action            ReviewAction                    `json:"action,omitempty"`
	VerifierWorkerIDs []string                        `json:"verifier_worker_ids"`
	Summaries         []string                        `json:"summaries"`
	Evidence          []ReviewEvidence                `json:"evidence"`
}

type ReviewChallengeTargetKind string

const (
	ReviewChallengeCoverageGap         ReviewChallengeTargetKind = "coverage_gap"
	ReviewChallengeCompetingHypothesis ReviewChallengeTargetKind = "competing_hypothesis"
)

type ReviewChallengeTarget struct {
	Kind          ReviewChallengeTargetKind `json:"kind"`
	ID            string                    `json:"id"`
	RequirementID string                    `json:"requirement_id,omitempty"`
	Description   string                    `json:"description"`
}

type ReviewChallengeAssignmentStatus string

const (
	ReviewChallengeQueued    ReviewChallengeAssignmentStatus = "queued"
	ReviewChallengeRunning   ReviewChallengeAssignmentStatus = "running"
	ReviewChallengeCompleted ReviewChallengeAssignmentStatus = "completed"
	ReviewChallengeFailed    ReviewChallengeAssignmentStatus = "failed"
)

type ReviewChallengeAssignment struct {
	ID                string                          `json:"id"`
	ExactSHA          string                          `json:"exact_sha"`
	Round             int                             `json:"round"`
	Lane              string                          `json:"lane"`
	Target            ReviewChallengeTarget           `json:"target"`
	CandidateRevision string                          `json:"candidate_revision,omitempty"`
	CandidateSnapshot *ReviewCanonicalFinding         `json:"candidate_snapshot,omitempty"`
	Superseded        bool                            `json:"superseded,omitempty"`
	PromptFingerprint string                          `json:"prompt_fingerprint"`
	Status            ReviewChallengeAssignmentStatus `json:"status"`
	WorkerID          string                          `json:"worker_id,omitempty"`
	Attempt           int                             `json:"attempt,omitempty"`
	QueuedAt          time.Time                       `json:"queued_at"`
	StartedAt         time.Time                       `json:"started_at,omitempty"`
	CompletedAt       time.Time                       `json:"completed_at,omitempty"`
	Outcome           ReviewChallengeOutcome          `json:"outcome,omitempty"`
	Summary           string                          `json:"summary,omitempty"`
	Action            ReviewAction                    `json:"action,omitempty"`
}

type ReviewConvergenceRoundOutcome string

const (
	ReviewConvergenceRoundActive          ReviewConvergenceRoundOutcome = "active"
	ReviewConvergenceRoundQuiet           ReviewConvergenceRoundOutcome = "quiet"
	ReviewConvergenceRoundMaterial        ReviewConvergenceRoundOutcome = "material"
	ReviewConvergenceRoundChangesRequired ReviewConvergenceRoundOutcome = "changes_required"
	ReviewConvergenceRoundUnresolved      ReviewConvergenceRoundOutcome = "unresolved"
	ReviewConvergenceRoundMaxRounds       ReviewConvergenceRoundOutcome = "max_rounds"
)

type ReviewConvergenceFindingSnapshot struct {
	FindingID         string `json:"finding_id"`
	CandidateRevision string `json:"candidate_revision"`
}

type ReviewConvergenceRoundState struct {
	Round                       int                                `json:"round"`
	DiscoveryPass               int                                `json:"discovery_pass"`
	StartedAt                   time.Time                          `json:"started_at"`
	CompletedAt                 time.Time                          `json:"completed_at,omitempty"`
	Outcome                     ReviewConvergenceRoundOutcome      `json:"outcome"`
	StartingConfirmedFindingIDs []string                           `json:"starting_confirmed_finding_ids"`
	StartingConfirmedFindings   []ReviewConvergenceFindingSnapshot `json:"starting_confirmed_findings,omitempty"`
	StartingCandidateIDs        []string                           `json:"starting_candidate_ids"`
	ObservedCandidateIDs        []string                           `json:"observed_candidate_ids"`
	NewCandidateIDs             []string                           `json:"new_candidate_ids,omitempty"`
	StartingCoverageGapIDs      []string                           `json:"starting_coverage_gap_ids"`
	NewConfirmedFindingIDs      []string                           `json:"new_confirmed_finding_ids,omitempty"`
	NewCoverageGapIDs           []string                           `json:"new_coverage_gap_ids,omitempty"`
	QuietRounds                 int                                `json:"quiet_rounds"`
	Action                      ReviewAction                       `json:"action,omitempty"`
}

type ReviewConvergenceStatus string

const (
	ReviewConvergencePending         ReviewConvergenceStatus = "pending"
	ReviewConvergenceRunning         ReviewConvergenceStatus = "running"
	ReviewConvergenceConverged       ReviewConvergenceStatus = "converged"
	ReviewConvergenceChangesRequired ReviewConvergenceStatus = "changes_required"
	ReviewConvergenceMaxRounds       ReviewConvergenceStatus = "max_rounds"
	ReviewConvergenceUnresolved      ReviewConvergenceStatus = "unresolved"
)

type ReviewConvergenceState struct {
	Status                  ReviewConvergenceStatus        `json:"status"`
	QuietRounds             int                            `json:"quiet_rounds"`
	VerificationAssignments []ReviewVerificationAssignment `json:"verification_assignments"`
	FindingVerifications    []ReviewFindingVerification    `json:"finding_verifications"`
	ChallengeAssignments    []ReviewChallengeAssignment    `json:"challenge_assignments"`
	Rounds                  []ReviewConvergenceRoundState  `json:"rounds"`
}

func supportedReviewChallengeTargetKind(kind ReviewChallengeTargetKind) bool {
	return kind == ReviewChallengeCoverageGap ||
		kind == ReviewChallengeCompetingHypothesis
}

func ensureReviewConvergenceState(cycle *ReviewCycleState) *ReviewConvergenceState {
	if cycle.Convergence == nil {
		cycle.Convergence = &ReviewConvergenceState{
			Status:                  ReviewConvergencePending,
			VerificationAssignments: []ReviewVerificationAssignment{},
			FindingVerifications:    []ReviewFindingVerification{},
			ChallengeAssignments:    []ReviewChallengeAssignment{},
			Rounds:                  []ReviewConvergenceRoundState{},
		}
	}
	return cycle.Convergence
}

func cloneReviewConvergenceState(
	state *ReviewConvergenceState,
) *ReviewConvergenceState {
	if state == nil {
		return nil
	}
	body, err := json.Marshal(state)
	if err != nil {
		clone := *state
		return &clone
	}
	var clone ReviewConvergenceState
	if err := json.Unmarshal(body, &clone); err != nil {
		fallback := *state
		return &fallback
	}
	return &clone
}

func reviewCanonicalFindingByID(
	cycle *ReviewCycleState,
	findingID string,
) (*ReviewCanonicalFinding, bool) {
	if cycle == nil {
		return nil, false
	}
	for index := range cycle.CanonicalFindings {
		if cycle.CanonicalFindings[index].ID == findingID {
			return &cycle.CanonicalFindings[index], true
		}
	}
	return nil, false
}

func reviewFindingOriginWorkerIDs(
	finding ReviewCanonicalFinding,
) []string {
	workers := make([]string, 0, len(finding.Provenance))
	for _, provenance := range finding.Provenance {
		workers = append(workers, provenance.WorkerID)
	}
	return uniqueSortedStrings(workers)
}

func cloneReviewCanonicalFinding(
	finding ReviewCanonicalFinding,
) ReviewCanonicalFinding {
	finding.Evidence = append([]ReviewEvidence(nil), finding.Evidence...)
	finding.Provenance = append(
		[]ReviewFindingProvenance(nil),
		finding.Provenance...,
	)
	return finding
}

func reviewFindingCandidateRevision(
	finding ReviewCanonicalFinding,
) (string, error) {
	// Verification binds to the finding's causal claim, not to discovery
	// bookkeeping. Additional worker provenance must not make an already
	// verified finding look new; changed causal evidence must.
	claim := struct {
		ID                string                `json:"id"`
		Fingerprint       string                `json:"fingerprint"`
		ExactSHA          string                `json:"exact_sha"`
		Summary           string                `json:"summary"`
		Location          ReviewFindingLocation `json:"location"`
		BehavioralPath    string                `json:"behavioral_path"`
		ViolatedInvariant string                `json:"violated_invariant"`
		Evidence          []ReviewEvidence      `json:"evidence"`
	}{
		ID:                finding.ID,
		Fingerprint:       finding.Fingerprint,
		ExactSHA:          finding.ExactSHA,
		Summary:           finding.Summary,
		Location:          finding.Location,
		BehavioralPath:    finding.BehavioralPath,
		ViolatedInvariant: finding.ViolatedInvariant,
		Evidence:          finding.Evidence,
	}
	body, err := json.Marshal(claim)
	if err != nil {
		return "", fmt.Errorf(
			"failed to compute review candidate revision: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func reviewVerificationAssignmentTargetsCurrentCandidate(
	cycle *ReviewCycleState,
	assignment ReviewVerificationAssignment,
) bool {
	finding, ok := reviewCanonicalFindingByID(
		cycle,
		assignment.FindingID,
	)
	if !ok {
		return false
	}
	revision, err := reviewFindingCandidateRevision(*finding)
	return err == nil && assignment.CandidateRevision == revision
}

func reviewVerificationAssignmentMatchesCurrentCandidate(
	cycle *ReviewCycleState,
	assignment ReviewVerificationAssignment,
) bool {
	return !assignment.Superseded &&
		reviewVerificationAssignmentTargetsCurrentCandidate(cycle, assignment)
}

func reviewChallengeAssignmentTargetsCurrentCandidate(
	cycle *ReviewCycleState,
	assignment ReviewChallengeAssignment,
) bool {
	if assignment.Target.Kind != ReviewChallengeCompetingHypothesis ||
		assignment.CandidateSnapshot == nil {
		return false
	}
	finding, ok := reviewCanonicalFindingByID(cycle, assignment.Target.ID)
	if !ok {
		return false
	}
	revision, err := reviewFindingCandidateRevision(*finding)
	return err == nil && assignment.CandidateRevision == revision
}

func reconcileReviewFindingAssignments(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("review cycle is missing")
	}
	if cycle.Convergence == nil {
		return nil
	}
	state := cycle.Convergence
	for index := range state.VerificationAssignments {
		assignment := &state.VerificationAssignments[index]
		assignment.Superseded =
			!reviewVerificationAssignmentTargetsCurrentCandidate(
				cycle,
				*assignment,
			)
	}
	for index := range state.ChallengeAssignments {
		assignment := &state.ChallengeAssignments[index]
		if assignment.Target.Kind != ReviewChallengeCompetingHypothesis {
			assignment.Superseded = false
			continue
		}
		assignment.Superseded =
			!reviewChallengeAssignmentTargetsCurrentCandidate(
				cycle,
				*assignment,
			)
	}
	return syncReviewFindingVerifications(cycle)
}

func syncReviewFindingVerifications(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("review cycle is missing")
	}
	if cycle.Convergence == nil {
		return nil
	}
	state := cycle.Convergence
	decisions := make([]ReviewFindingVerification, 0, len(cycle.CanonicalFindings))
	for _, finding := range cycle.CanonicalFindings {
		candidateRevision, err := reviewFindingCandidateRevision(finding)
		if err != nil {
			return err
		}
		assignments := make([]ReviewVerificationAssignment, 0)
		for _, assignment := range state.VerificationAssignments {
			if !assignment.Superseded &&
				assignment.FindingID == finding.ID &&
				assignment.CandidateRevision == candidateRevision {
				assignments = append(assignments, assignment)
			}
		}
		sort.Slice(assignments, func(i, j int) bool {
			return assignments[i].Ordinal < assignments[j].Ordinal
		})
		decision := ReviewFindingVerification{
			FindingID:         finding.ID,
			ExactSHA:          cycle.HeadSHA,
			CandidateRevision: candidateRevision,
			Status:            ReviewFindingVerificationPending,
			VerifierWorkerIDs: []string{},
			Summaries:         []string{},
			Evidence:          []ReviewEvidence{},
		}
		confirmed := 0
		rejected := 0
		terminal := 0
		failed := false
		for _, assignment := range assignments {
			if assignment.WorkerID != "" {
				decision.VerifierWorkerIDs = append(
					decision.VerifierWorkerIDs,
					assignment.WorkerID,
				)
			}
			if assignment.Summary != "" {
				decision.Summaries = append(
					decision.Summaries,
					assignment.Summary,
				)
			}
			decision.Evidence = append(
				decision.Evidence,
				assignment.Evidence...,
			)
			decision.Evidence = append(
				decision.Evidence,
				assignment.TestEvidence...,
			)
			switch assignment.Status {
			case ReviewVerificationCompleted:
				terminal++
				switch assignment.Outcome {
				case ReviewVerificationConfirmed:
					confirmed++
				case ReviewVerificationRejected:
					rejected++
				}
			case ReviewVerificationFailed:
				terminal++
				failed = true
				if decision.Action == "" {
					decision.Action = assignment.Action
				}
			}
		}
		decision.VerifierWorkerIDs =
			uniqueSortedStrings(decision.VerifierWorkerIDs)
		sort.Strings(decision.Summaries)
		decision.Evidence = uniqueSortedReviewEvidence(decision.Evidence)
		switch {
		case failed:
			decision.Status = ReviewFindingVerificationUnresolved
			if decision.Action == "" {
				decision.Action =
					cycle.Policy.FailureActions.VerificationFailure
			}
		case confirmed >= cycle.Policy.Verification.MinVerifiers:
			decision.Status = ReviewFindingVerificationConfirmed
			decision.Action = ""
		case rejected >= cycle.Policy.Verification.MinVerifiers:
			decision.Status = ReviewFindingVerificationRejected
			decision.Action = ""
		case len(assignments) >= cycle.Policy.Verification.MaxVerifiers &&
			terminal == len(assignments):
			decision.Status = ReviewFindingVerificationInconclusive
			decision.Action =
				cycle.Policy.Verification.InconclusiveAction
		}
		decisions = append(decisions, decision)
	}
	sort.Slice(decisions, func(i, j int) bool {
		return decisions[i].FindingID < decisions[j].FindingID
	})
	state.FindingVerifications = decisions
	recordActiveReviewConvergenceMaterial(cycle)
	return nil
}

func recordActiveReviewConvergenceMaterial(cycle *ReviewCycleState) {
	if cycle == nil || cycle.Convergence == nil ||
		len(cycle.Convergence.Rounds) == 0 {
		return
	}
	state := cycle.Convergence
	round := &state.Rounds[len(state.Rounds)-1]
	if round.Outcome != ReviewConvergenceRoundActive {
		return
	}
	newConfirmed := append(
		[]string(nil),
		round.NewConfirmedFindingIDs...,
	)
	for _, decision := range state.FindingVerifications {
		if decision.Status != ReviewFindingVerificationConfirmed ||
			reviewConvergenceFindingMatchesStartingSnapshot(
				*round,
				decision,
			) {
			continue
		}
		newConfirmed = append(newConfirmed, decision.FindingID)
	}
	round.NewConfirmedFindingIDs = uniqueSortedStrings(newConfirmed)
	round.NewCandidateIDs = uniqueSortedStrings(append(
		append([]string(nil), round.NewCandidateIDs...),
		stringSetDifference(
			reviewCanonicalFindingIDs(cycle),
			round.StartingCandidateIDs,
		)...,
	))
	round.NewCoverageGapIDs = uniqueSortedStrings(append(
		append([]string(nil), round.NewCoverageGapIDs...),
		stringSetDifference(
			reviewCoverageGapIDs(cycle.UnresolvedCoverage),
			round.StartingCoverageGapIDs,
		)...,
	))
}

func reviewConvergenceFindingMatchesStartingSnapshot(
	round ReviewConvergenceRoundState,
	decision ReviewFindingVerification,
) bool {
	for _, snapshot := range round.StartingConfirmedFindings {
		if snapshot.FindingID == decision.FindingID &&
			snapshot.CandidateRevision == decision.CandidateRevision {
			return true
		}
	}
	return false
}

func reviewConvergenceRemainingAgentCapacity(cycle *ReviewCycleState) int {
	if cycle == nil {
		return 0
	}
	used := len(cycle.WorkerOwnerships)
	if cycle.Convergence != nil {
		for _, assignment := range cycle.Convergence.VerificationAssignments {
			if assignment.Superseded {
				continue
			}
			if assignment.Status == ReviewVerificationQueued ||
				(assignment.Status == ReviewVerificationRunning &&
					assignment.WorkerID == "" &&
					!reviewConvergenceAssignmentHasOwnership(
						cycle,
						AgentProfileRoleVerifier,
						assignment.Round,
						assignment.Lane,
					)) {
				used++
			}
		}
		for _, assignment := range cycle.Convergence.ChallengeAssignments {
			if assignment.Superseded {
				continue
			}
			if assignment.Status == ReviewChallengeQueued ||
				(assignment.Status == ReviewChallengeRunning &&
					assignment.WorkerID == "" &&
					!reviewConvergenceAssignmentHasOwnership(
						cycle,
						AgentProfileRoleChallenge,
						assignment.Round,
						assignment.Lane,
					)) {
				used++
			}
		}
	}
	remaining := effectiveReviewAgentCapacity(cycle) - used
	if remaining < 0 {
		return 0
	}
	return remaining
}

func reviewConvergenceAssignmentHasOwnership(
	cycle *ReviewCycleState,
	role AgentProfileRole,
	pass int,
	lane string,
) bool {
	for _, ownership := range cycle.WorkerOwnerships {
		if ownership.Identity.Role == role &&
			ownership.Identity.Pass == pass &&
			ownership.Identity.Lane == lane {
			return true
		}
	}
	return false
}

func queueReviewVerificationAssignments(
	cycle *ReviewCycleState,
	round int,
	queuedAt time.Time,
) ([]ReviewVerificationAssignment, error) {
	if err := existingReviewLimitReachedError(cycle); err != nil {
		return nil, err
	}
	if cycle == nil || cycle.Plan == nil {
		return nil, errors.New(
			"review verification requires an exact-SHA review plan",
		)
	}
	if round <= 0 || queuedAt.IsZero() {
		return nil, errors.New(
			"review verification round and queue time are required",
		)
	}
	state := ensureReviewConvergenceState(cycle)
	if err := syncReviewFindingVerifications(cycle); err != nil {
		return nil, err
	}
	decisions := make(map[string]ReviewFindingVerification)
	for _, decision := range state.FindingVerifications {
		decisions[decision.FindingID] = decision
	}
	findings := append(
		[]ReviewCanonicalFinding(nil),
		cycle.CanonicalFindings...,
	)
	sort.Slice(findings, func(i, j int) bool {
		return findings[i].ID < findings[j].ID
	})
	added := make([]ReviewVerificationAssignment, 0)
	remainingCapacity := reviewConvergenceRemainingAgentCapacity(cycle)
	if remainingCapacity == 0 {
		return added, nil
	}
	capacityReached := false
	for _, finding := range findings {
		decision := decisions[finding.ID]
		if decision.Status == ReviewFindingVerificationConfirmed ||
			decision.Status == ReviewFindingVerificationRejected ||
			decision.Status == ReviewFindingVerificationUnresolved ||
			decision.Status == ReviewFindingVerificationInconclusive {
			continue
		}
		candidateRevision, err := reviewFindingCandidateRevision(finding)
		if err != nil {
			return nil, err
		}
		existing := make([]ReviewVerificationAssignment, 0)
		excluded := reviewFindingOriginWorkerIDs(finding)
		nextOrdinal := 1
		for _, assignment := range state.VerificationAssignments {
			if assignment.FindingID != finding.ID {
				continue
			}
			if assignment.Ordinal >= nextOrdinal {
				nextOrdinal = assignment.Ordinal + 1
			}
			if !assignment.Superseded &&
				assignment.CandidateRevision == candidateRevision {
				existing = append(existing, assignment)
			}
			if assignment.WorkerID != "" {
				excluded = append(excluded, assignment.WorkerID)
			}
		}
		excluded = uniqueSortedStrings(excluded)
		needed := cycle.Policy.Verification.MinVerifiers - len(existing)
		if needed <= 0 {
			allTerminal := true
			for _, assignment := range existing {
				if assignment.Status == ReviewVerificationQueued ||
					assignment.Status == ReviewVerificationRunning {
					allTerminal = false
					break
				}
			}
			if allTerminal &&
				len(existing) < cycle.Policy.Verification.MaxVerifiers {
				needed = 1
			}
		}
		for index := 0; index < needed; index++ {
			if len(added) == remainingCapacity {
				capacityReached = true
				break
			}
			ordinal := nextOrdinal + index
			lane := reviewVerificationLane(finding.ID, ordinal)
			assignment := ReviewVerificationAssignment{
				ID: fmt.Sprintf(
					"verification:%s:%03d",
					finding.ID,
					ordinal,
				),
				FindingID:         finding.ID,
				ExactSHA:          cycle.HeadSHA,
				CandidateRevision: candidateRevision,
				CandidateSnapshot: cloneReviewCanonicalFinding(finding),
				Round:             round,
				Ordinal:           ordinal,
				Lane:              lane,
				Status:            ReviewVerificationQueued,
				ExcludedWorkerIDs: append([]string(nil), excluded...),
				QueuedAt:          queuedAt.UTC(),
			}
			state.VerificationAssignments = append(
				state.VerificationAssignments,
				assignment,
			)
			added = append(added, assignment)
		}
		if capacityReached {
			break
		}
	}
	if err := syncReviewFindingVerifications(cycle); err != nil {
		return nil, err
	}
	return added, nil
}

func reviewVerificationLane(findingID string, ordinal int) string {
	sum := sha256.Sum256([]byte(findingID))
	return fmt.Sprintf(
		"verification-%s-%03d",
		hex.EncodeToString(sum[:8]),
		ordinal,
	)
}

func scheduledReviewVerificationAssignment(
	cycle *ReviewCycleState,
	pass int,
	lane string,
) (*ReviewVerificationAssignment, bool) {
	if cycle == nil || cycle.Convergence == nil {
		return nil, false
	}
	for index := range cycle.Convergence.VerificationAssignments {
		assignment := &cycle.Convergence.VerificationAssignments[index]
		if assignment.Round == pass && assignment.Lane == lane {
			return assignment, true
		}
	}
	return nil, false
}

func reviewVerificationOwnershipMatches(
	cycle *ReviewCycleState,
	assignment ReviewVerificationAssignment,
	ownership ReviewWorkerOwnership,
) bool {
	if cycle == nil ||
		ownership.Identity.CycleID != cycle.ID ||
		ownership.Identity.Revision != cycle.Revision ||
		ownership.Identity.Role != AgentProfileRoleVerifier ||
		ownership.Identity.Pass != assignment.Round ||
		ownership.Identity.Lane != assignment.Lane ||
		stringSliceContains(
			assignment.ExcludedWorkerIDs,
			ownership.OwnerID,
		) {
		return false
	}
	for _, registered := range cycle.WorkerOwnerships {
		if registered.OwnerID == ownership.OwnerID &&
			registered.Attempt == ownership.Attempt {
			return true
		}
	}
	return false
}

func validateReviewVerificationEvidence(
	cycle *ReviewCycleState,
	assignment ReviewVerificationAssignment,
	payload ReviewVerificationPayload,
) error {
	finding := assignment.CandidateSnapshot
	revision, err := reviewFindingCandidateRevision(finding)
	if cycle == nil ||
		revision == "" ||
		err != nil ||
		revision != assignment.CandidateRevision ||
		finding.ID != assignment.FindingID ||
		finding.ExactSHA != assignment.ExactSHA ||
		payload.FindingID != finding.ID {
		return errors.New(
			"verification result does not match its assigned finding",
		)
	}
	if payload.Outcome == ReviewVerificationInconclusive {
		return nil
	}
	if !supportedReviewScopeDisposition(payload.ScopeDisposition) ||
		!supportedReviewPatchDisposition(payload.PatchDisposition) {
		return errors.New(
			"verification result has no task-scope or patch disposition",
		)
	}
	if payload.Outcome == ReviewVerificationConfirmed {
		if payload.ScopeDisposition != ReviewScopeInScope {
			return errors.New(
				"confirmed finding is outside the immutable task intent",
			)
		}
		if payload.PatchDisposition != ReviewPatchIntroduced &&
			payload.PatchDisposition != ReviewPatchWorsened {
			return errors.New(
				"confirmed finding was not introduced or worsened by the patch",
			)
		}
		if err := validateReviewPatchCausalEvidence(
			cycle,
			payload.CausalEvidence,
		); err != nil {
			return err
		}
	}
	if !cycle.Policy.Verification.RequireEvidence {
		return nil
	}
	if payload.Location == nil ||
		!reflect.DeepEqual(
			normalizedReviewFindingLocation(*payload.Location),
			normalizedReviewFindingLocation(finding.Location),
		) {
		return errors.New(
			"verification result does not validate the assigned location",
		)
	}
	if normalizedReviewFindingText(payload.BehavioralPath) !=
		normalizedReviewFindingText(finding.BehavioralPath) {
		return errors.New(
			"verification result does not validate the behavioral path",
		)
	}
	if len(payload.Evidence) == 0 {
		return errors.New("verification result has no independent evidence")
	}
	assignedPath := normalizedReviewFindingLocation(finding.Location).Path
	evidenceSupportsAssignedLocation := func(values []ReviewEvidence) bool {
		for _, evidence := range values {
			if normalizedReviewEvidence(evidence).Path == assignedPath {
				return true
			}
		}
		return false
	}
	if !evidenceSupportsAssignedLocation(payload.Evidence) &&
		!evidenceSupportsAssignedLocation(payload.TestEvidence) {
		return errors.New(
			"verification evidence does not support the assigned location",
		)
	}
	if len(payload.TestEvidence) == 0 &&
		strings.TrimSpace(payload.TestNotPracticalReason) == "" {
		return errors.New(
			"verification result has no test, reproduction, or practicality evidence",
		)
	}
	return nil
}

func validateReviewPatchCausalEvidence(
	cycle *ReviewCycleState,
	evidence []ReviewEvidence,
) error {
	if cycle == nil || cycle.Inputs == nil || len(evidence) == 0 {
		return errors.New(
			"confirmed finding has no base-to-head causal evidence",
		)
	}
	changed := make(
		map[string][]ReviewLineRange,
		len(cycle.Inputs.ChangedFiles)*2,
	)
	for _, file := range cycle.Inputs.ChangedFiles {
		changed[file.Path] = file.ChangedRanges
		if file.PreviousPath != "" {
			changed[file.PreviousPath] = file.ChangedRanges
		}
	}
	for _, item := range evidence {
		if err := validateReviewEvidence(item); err != nil {
			return err
		}
		changedRanges, ok := changed[item.Path]
		if !ok || item.Path == "" {
			continue
		}
		if changedRanges == nil {
			return nil
		}
		for _, changedRange := range changedRanges {
			if item.StartLine > 0 &&
				item.StartLine <= changedRange.EndLine &&
				changedRange.StartLine <= item.EndLine {
				return nil
			}
		}
	}
	return errors.New(
		"confirmed finding causal evidence does not reference an exact changed line",
	)
}

func completeReviewVerificationFromReceipt(
	cycle *ReviewCycleState,
	ownership ReviewWorkerOwnership,
	envelope ReviewArtifactEnvelope,
	completedAt time.Time,
) error {
	if envelope.Phase != ReviewArtifactPhaseVerification ||
		envelope.Payload.Verification == nil {
		return nil
	}
	assignment, scheduled := scheduledReviewVerificationAssignment(
		cycle,
		envelope.Pass,
		envelope.Lane,
	)
	if !scheduled ||
		assignment.Status != ReviewVerificationRunning ||
		!reviewVerificationAssignmentMatchesCurrentCandidate(
			cycle,
			*assignment,
		) ||
		!reviewVerificationOwnershipMatches(
			cycle,
			*assignment,
			ownership,
		) ||
		assignment.WorkerID != ownership.OwnerID ||
		assignment.Attempt != ownership.Attempt {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureConflict,
		)
	}
	payload := *envelope.Payload.Verification
	if err := validateReviewVerificationEvidence(
		cycle,
		*assignment,
		payload,
	); err != nil {
		return malformedReviewArtifactError(err.Error())
	}
	assignment.Status = ReviewVerificationCompleted
	assignment.CompletedAt = completedAt.UTC()
	assignment.Outcome = payload.Outcome
	assignment.Summary = strings.Join(
		strings.Fields(payload.Summary),
		" ",
	)
	assignment.ScopeDisposition = payload.ScopeDisposition
	assignment.PatchDisposition = payload.PatchDisposition
	assignment.Location = payload.Location
	assignment.BehavioralPath = payload.BehavioralPath
	assignment.Evidence = append(
		[]ReviewEvidence(nil),
		payload.Evidence...,
	)
	assignment.CausalEvidence = append(
		[]ReviewEvidence(nil),
		payload.CausalEvidence...,
	)
	assignment.TestEvidence = append(
		[]ReviewEvidence(nil),
		payload.TestEvidence...,
	)
	assignment.TestNotPractical =
		strings.Join(strings.Fields(payload.TestNotPracticalReason), " ")
	if payload.Outcome == ReviewVerificationInconclusive {
		assignment.Action =
			cycle.Policy.Verification.InconclusiveAction
	}
	return syncReviewFindingVerifications(cycle)
}

func buildReviewChallengeTargets(
	cycle *ReviewCycleState,
) []ReviewChallengeTarget {
	if cycle == nil {
		return nil
	}
	targets := make([]ReviewChallengeTarget, 0)
	for _, gap := range cycle.UnresolvedCoverage {
		targets = append(targets, ReviewChallengeTarget{
			Kind:          ReviewChallengeCoverageGap,
			ID:            gap.ID,
			RequirementID: gap.RequirementID,
			Description:   gap.Description,
		})
	}
	if cycle.Convergence != nil {
		for _, decision := range cycle.Convergence.FindingVerifications {
			if decision.Status != ReviewFindingVerificationInconclusive {
				continue
			}
			finding, ok := reviewCanonicalFindingByID(
				cycle,
				decision.FindingID,
			)
			if !ok {
				continue
			}
			targets = append(targets, ReviewChallengeTarget{
				Kind:        ReviewChallengeCompetingHypothesis,
				ID:          finding.ID,
				Description: finding.ViolatedInvariant,
			})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Kind == targets[j].Kind {
			return targets[i].ID < targets[j].ID
		}
		return targets[i].Kind < targets[j].Kind
	})
	return targets
}

func validateReviewChallengeTarget(
	cycle *ReviewCycleState,
	target ReviewChallengeTarget,
) error {
	if cycle == nil || cycle.Plan == nil ||
		!supportedReviewChallengeTargetKind(target.Kind) ||
		strings.TrimSpace(target.ID) != target.ID ||
		target.ID == "" ||
		strings.TrimSpace(target.Description) == "" {
		return errors.New(
			"review challenge must name a coverage gap or competing hypothesis",
		)
	}
	switch target.Kind {
	case ReviewChallengeCoverageGap:
		for _, gap := range cycle.UnresolvedCoverage {
			if gap.ID == target.ID &&
				gap.RequirementID == target.RequirementID {
				return nil
			}
		}
		return errors.New(
			"review challenge coverage target is not an unresolved plan gap",
		)
	case ReviewChallengeCompetingHypothesis:
		if _, ok := reviewCanonicalFindingByID(cycle, target.ID); !ok {
			return errors.New(
				"review challenge hypothesis target is not a known finding",
			)
		}
		return nil
	default:
		return errors.New("review challenge target kind is unsupported")
	}
}

func reviewChallengeContextFingerprint(
	cycle *ReviewCycleState,
	target ReviewChallengeTarget,
) (string, error) {
	body, err := json.Marshal(struct {
		HeadSHA       string                      `json:"head_sha"`
		Plan          *ReviewPlan                 `json:"plan"`
		Target        ReviewChallengeTarget       `json:"target"`
		Findings      []ReviewCanonicalFinding    `json:"findings"`
		Verifications []ReviewFindingVerification `json:"verifications"`
		Coverage      []ReviewCoverageGap         `json:"coverage"`
	}{
		HeadSHA:       cycle.HeadSHA,
		Plan:          cycle.Plan,
		Target:        target,
		Findings:      cycle.CanonicalFindings,
		Verifications: cycle.Convergence.FindingVerifications,
		Coverage:      cycle.UnresolvedCoverage,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func queueTargetedReviewChallenge(
	cycle *ReviewCycleState,
	round int,
	target ReviewChallengeTarget,
	queuedAt time.Time,
) (ReviewChallengeAssignment, error) {
	if err := existingReviewLimitReachedError(cycle); err != nil {
		return ReviewChallengeAssignment{}, err
	}
	if round <= 0 || queuedAt.IsZero() {
		return ReviewChallengeAssignment{}, errors.New(
			"review challenge round and queue time are required",
		)
	}
	if err := validateReviewChallengeTarget(cycle, target); err != nil {
		return ReviewChallengeAssignment{}, err
	}
	state := ensureReviewConvergenceState(cycle)
	fingerprint, err := reviewChallengeContextFingerprint(cycle, target)
	if err != nil {
		return ReviewChallengeAssignment{}, err
	}
	for _, existing := range state.ChallengeAssignments {
		if existing.Superseded {
			continue
		}
		if existing.PromptFingerprint == fingerprint {
			return ReviewChallengeAssignment{}, errors.New(
				"identical review challenge context was already scheduled",
			)
		}
	}
	for _, existing := range state.ChallengeAssignments {
		if existing.Superseded {
			continue
		}
		if existing.Round == round &&
			existing.Target.Kind == target.Kind &&
			existing.Target.ID == target.ID {
			return ReviewChallengeAssignment{}, errors.New(
				"review challenge target was already attempted in this round",
			)
		}
	}
	sum := sha256.Sum256([]byte(string(target.Kind) + "\x00" + target.ID))
	lane := fmt.Sprintf(
		"challenge-%s-%s",
		target.Kind,
		hex.EncodeToString(sum[:8]),
	)
	assignment := ReviewChallengeAssignment{
		ID: fmt.Sprintf(
			"challenge:%03d:%s",
			round,
			hex.EncodeToString(sum[:]),
		),
		ExactSHA:          cycle.HeadSHA,
		Round:             round,
		Lane:              lane,
		Target:            target,
		PromptFingerprint: fingerprint,
		Status:            ReviewChallengeQueued,
		QueuedAt:          queuedAt.UTC(),
	}
	if target.Kind == ReviewChallengeCompetingHypothesis {
		finding, ok := reviewCanonicalFindingByID(cycle, target.ID)
		if !ok {
			return ReviewChallengeAssignment{}, errors.New(
				"review challenge hypothesis target is not a known finding",
			)
		}
		revision, err := reviewFindingCandidateRevision(*finding)
		if err != nil {
			return ReviewChallengeAssignment{}, err
		}
		snapshot := cloneReviewCanonicalFinding(*finding)
		assignment.CandidateRevision = revision
		assignment.CandidateSnapshot = &snapshot
	}
	state.ChallengeAssignments = append(
		state.ChallengeAssignments,
		assignment,
	)
	return assignment, nil
}

func scheduledReviewChallengeAssignment(
	cycle *ReviewCycleState,
	pass int,
	lane string,
) (*ReviewChallengeAssignment, bool) {
	if cycle == nil || cycle.Convergence == nil {
		return nil, false
	}
	for index := range cycle.Convergence.ChallengeAssignments {
		assignment := &cycle.Convergence.ChallengeAssignments[index]
		if assignment.Round == pass && assignment.Lane == lane {
			return assignment, true
		}
	}
	return nil, false
}

func reviewChallengeOwnershipMatches(
	cycle *ReviewCycleState,
	assignment ReviewChallengeAssignment,
	ownership ReviewWorkerOwnership,
) bool {
	if cycle == nil ||
		ownership.Identity.CycleID != cycle.ID ||
		ownership.Identity.Revision != cycle.Revision ||
		ownership.Identity.Role != AgentProfileRoleChallenge ||
		ownership.Identity.Pass != assignment.Round ||
		ownership.Identity.Lane != assignment.Lane {
		return false
	}
	for _, registered := range cycle.WorkerOwnerships {
		if registered.OwnerID == ownership.OwnerID &&
			registered.Attempt == ownership.Attempt {
			return true
		}
	}
	return false
}

func completeReviewChallengeFromReceipt(
	cycle *ReviewCycleState,
	ownership ReviewWorkerOwnership,
	envelope ReviewArtifactEnvelope,
	completedAt time.Time,
) error {
	if envelope.Phase != ReviewArtifactPhaseChallenge ||
		envelope.Payload.Challenge == nil {
		return nil
	}
	assignment, scheduled := scheduledReviewChallengeAssignment(
		cycle,
		envelope.Pass,
		envelope.Lane,
	)
	if !scheduled ||
		assignment.Superseded ||
		assignment.Status != ReviewChallengeRunning ||
		!reviewChallengeOwnershipMatches(cycle, *assignment, ownership) ||
		assignment.WorkerID != ownership.OwnerID ||
		assignment.Attempt != ownership.Attempt {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureConflict,
		)
	}
	payload := envelope.Payload.Challenge
	if payload.AssignmentID != assignment.ID {
		return malformedReviewArtifactError(
			"challenge result does not match its assigned challenge",
		)
	}
	if payload.TargetKind != assignment.Target.Kind ||
		payload.TargetID != assignment.Target.ID {
		return malformedReviewArtifactError(
			"challenge result does not match its assigned target",
		)
	}
	if payload.Candidates == nil {
		return malformedReviewArtifactError("payload.candidates is required")
	}
	if payload.Coverage == nil {
		return malformedReviewArtifactError("payload.coverage is required")
	}
	if err := validateReviewCoverageClaims(
		payload.Coverage,
		cycle.Plan.CoverageRequirements,
	); err != nil {
		return malformedReviewArtifactError(err.Error())
	}
	if assignment.Target.Kind == ReviewChallengeCoverageGap {
		targetClaimed := false
		for _, claim := range payload.Coverage {
			if claim.RequirementID == assignment.Target.RequirementID {
				targetClaimed = true
				break
			}
		}
		if !targetClaimed {
			return malformedReviewArtifactError(fmt.Sprintf(
				"challenge result does not cover assigned requirement %q",
				assignment.Target.RequirementID,
			))
		}
	}
	if assignment.Target.Kind == ReviewChallengeCompetingHypothesis &&
		payload.Outcome != ReviewChallengeInconclusive &&
		!reviewChallengeCandidatesReproduceTarget(
			*assignment,
			payload.Candidates,
		) {
		return malformedReviewArtifactError(
			"challenge result does not reproduce its assigned competing hypothesis",
		)
	}
	assignment.Status = ReviewChallengeCompleted
	assignment.WorkerID = ownership.OwnerID
	assignment.Attempt = ownership.Attempt
	assignment.CompletedAt = completedAt.UTC()
	assignment.Outcome = payload.Outcome
	assignment.Summary = strings.Join(
		strings.Fields(payload.Summary),
		" ",
	)
	if payload.Outcome == ReviewChallengeInconclusive {
		assignment.Action =
			cycle.Policy.Verification.InconclusiveAction
	}
	return nil
}

func reviewChallengeCandidatesReproduceTarget(
	assignment ReviewChallengeAssignment,
	candidates []ReviewFindingCandidate,
) bool {
	if assignment.Target.Kind != ReviewChallengeCompetingHypothesis ||
		assignment.CandidateSnapshot == nil {
		return false
	}
	finding := assignment.CandidateSnapshot
	for _, candidate := range candidates {
		assigned := ReviewFindingCandidate{
			CandidateID:       "assigned-finding",
			Summary:           finding.Summary,
			Location:          finding.Location,
			BehavioralPath:    finding.BehavioralPath,
			ViolatedInvariant: finding.ViolatedInvariant,
			Severity:          finding.Severity,
			Confidence:        finding.Confidence,
			Evidence:          finding.Evidence,
		}
		if reviewFindingCandidatesSemanticallyEquivalent(candidate, assigned) {
			return true
		}
	}
	return false
}

func reviewChallengeTargetAttemptedInRound(
	cycle *ReviewCycleState,
	round int,
	target ReviewChallengeTarget,
) bool {
	if cycle == nil || cycle.Convergence == nil {
		return false
	}
	for _, assignment := range cycle.Convergence.ChallengeAssignments {
		if assignment.Superseded {
			continue
		}
		if assignment.Round == round &&
			assignment.Target.Kind == target.Kind &&
			assignment.Target.ID == target.ID {
			return true
		}
	}
	return false
}

func beginReviewConvergenceRoundState(
	cycle *ReviewCycleState,
	startedAt time.Time,
) (ReviewConvergenceRoundState, error) {
	if err := existingReviewLimitReachedError(cycle); err != nil {
		return ReviewConvergenceRoundState{}, err
	}
	if cycle == nil || cycle.Plan == nil || startedAt.IsZero() {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence requires an exact-SHA plan and start time",
		)
	}
	if cycle.Stale {
		return ReviewConvergenceRoundState{}, errReviewCycleStale
	}
	if cycle.VerdictPublication != nil {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence verdict publication is already prepared",
		)
	}
	latestDiscovery, ok := latestReviewDiscoveryPass(cycle)
	if !ok || latestDiscovery.CompletedAt.IsZero() {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence requires a fresh completed discovery pass",
		)
	}
	state := ensureReviewConvergenceState(cycle)
	if err := syncReviewFindingVerifications(cycle); err != nil {
		return ReviewConvergenceRoundState{}, err
	}
	if state.Status == ReviewConvergenceConverged ||
		state.Status == ReviewConvergenceChangesRequired ||
		state.Status == ReviewConvergenceMaxRounds {
		return ReviewConvergenceRoundState{}, fmt.Errorf(
			"review convergence is already terminal with %q",
			state.Status,
		)
	}
	if len(state.Rounds) > 0 &&
		state.Rounds[len(state.Rounds)-1].Outcome ==
			ReviewConvergenceRoundActive {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence already has an active round",
		)
	}
	roundNumber := len(state.Rounds) + 1
	if roundNumber > cycle.Policy.Convergence.MaxRounds {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence maximum round limit is exhausted",
		)
	}
	if latestDiscovery.Pass != roundNumber {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence requires a fresh completed discovery pass",
		)
	}
	startingCandidateIDs := []string{}
	if len(state.Rounds) > 0 {
		previous := state.Rounds[len(state.Rounds)-1]
		startingCandidateIDs = append(
			startingCandidateIDs,
			previous.ObservedCandidateIDs...,
		)
	}
	observedCandidateIDs := reviewCanonicalFindingIDs(cycle)
	round := ReviewConvergenceRoundState{
		Round:                       roundNumber,
		DiscoveryPass:               latestDiscovery.Pass,
		StartedAt:                   startedAt.UTC(),
		Outcome:                     ReviewConvergenceRoundActive,
		QuietRounds:                 state.QuietRounds,
		StartingConfirmedFindingIDs: reviewConfirmedFindingIDs(state.FindingVerifications),
		StartingConfirmedFindings:   reviewConfirmedFindingSnapshots(state.FindingVerifications),
		StartingCandidateIDs:        startingCandidateIDs,
		ObservedCandidateIDs:        observedCandidateIDs,
		NewCandidateIDs: stringSetDifference(
			observedCandidateIDs,
			startingCandidateIDs,
		),
		StartingCoverageGapIDs: reviewCoverageGapIDs(cycle.UnresolvedCoverage),
	}
	state.Rounds = append(state.Rounds, round)
	state.Status = ReviewConvergenceRunning
	return round, nil
}

func completeReviewConvergenceRoundState(
	cycle *ReviewCycleState,
	completedAt time.Time,
) (ReviewConvergenceRoundState, error) {
	if cycle == nil || cycle.Convergence == nil || completedAt.IsZero() {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence has no active round",
		)
	}
	if cycle.Stale {
		return ReviewConvergenceRoundState{}, errReviewCycleStale
	}
	state := cycle.Convergence
	if len(state.Rounds) == 0 ||
		state.Rounds[len(state.Rounds)-1].Outcome !=
			ReviewConvergenceRoundActive {
		return ReviewConvergenceRoundState{}, errors.New(
			"review convergence has no active round",
		)
	}
	if err := syncReviewFindingVerifications(cycle); err != nil {
		return ReviewConvergenceRoundState{}, err
	}
	round := &state.Rounds[len(state.Rounds)-1]
	for _, assignment := range state.VerificationAssignments {
		if !assignment.Superseded &&
			assignment.Round == round.Round &&
			(assignment.Status == ReviewVerificationQueued ||
				assignment.Status == ReviewVerificationRunning) {
			return ReviewConvergenceRoundState{}, errors.New(
				"review convergence round still has active verification",
			)
		}
	}
	for _, assignment := range state.ChallengeAssignments {
		if !assignment.Superseded &&
			assignment.Round == round.Round &&
			(assignment.Status == ReviewChallengeQueued ||
				assignment.Status == ReviewChallengeRunning) {
			return ReviewConvergenceRoundState{}, errors.New(
				"review convergence round still has an active challenge",
			)
		}
	}
	for _, decision := range state.FindingVerifications {
		if decision.Status == ReviewFindingVerificationPending {
			return ReviewConvergenceRoundState{}, errors.New(
				"review convergence round has an unverified material candidate",
			)
		}
	}
	gaps := reviewCoverageGapIDs(cycle.UnresolvedCoverage)
	recordActiveReviewConvergenceMaterial(cycle)
	round.ObservedCandidateIDs = uniqueSortedStrings(append(
		round.ObservedCandidateIDs,
		reviewCanonicalFindingIDs(cycle)...,
	))
	round.NewCandidateIDs = uniqueSortedStrings(round.NewCandidateIDs)
	round.NewConfirmedFindingIDs =
		uniqueSortedStrings(round.NewConfirmedFindingIDs)
	round.NewCoverageGapIDs =
		uniqueSortedStrings(round.NewCoverageGapIDs)
	round.CompletedAt = completedAt.UTC()
	unresolved := round.Action != ""
	discoveryPass, hasDiscovery := reviewDiscoveryPassByNumber(
		cycle,
		round.DiscoveryPass,
	)
	if !hasDiscovery ||
		!reviewDiscoveryPassCompletedSuccessfully(discoveryPass) {
		unresolved = true
		if round.Action == "" {
			round.Action =
				cycle.Policy.FailureActions.RequiredLaneFailure
		}
	}
	for _, decision := range state.FindingVerifications {
		if decision.Status == ReviewFindingVerificationInconclusive ||
			decision.Status == ReviewFindingVerificationUnresolved {
			unresolved = true
			if round.Action == "" {
				round.Action = decision.Action
			}
		}
	}
	for _, challenge := range state.ChallengeAssignments {
		if !challenge.Superseded &&
			challenge.Round == round.Round &&
			(challenge.Status == ReviewChallengeFailed ||
				(challenge.Status == ReviewChallengeCompleted &&
					challenge.Outcome ==
						ReviewChallengeInconclusive)) {
			unresolved = true
			if round.Action == "" {
				round.Action = challenge.Action
			}
		}
	}
	switch {
	case unresolved:
		state.QuietRounds = 0
		round.Outcome = ReviewConvergenceRoundUnresolved
		round.QuietRounds = 0
		state.Status = ReviewConvergenceUnresolved
	case reviewCycleReadyForChangesRequiredVerdict(cycle):
		state.QuietRounds = 0
		round.Outcome = ReviewConvergenceRoundChangesRequired
		round.QuietRounds = 0
		state.Status = ReviewConvergenceChangesRequired
	case len(round.NewConfirmedFindingIDs) != 0 ||
		len(round.NewCandidateIDs) != 0 ||
		len(round.NewCoverageGapIDs) != 0 ||
		len(gaps) != 0:
		state.QuietRounds = 0
		round.Outcome = ReviewConvergenceRoundMaterial
		round.QuietRounds = 0
		state.Status = ReviewConvergencePending
	default:
		state.QuietRounds++
		round.Outcome = ReviewConvergenceRoundQuiet
		round.QuietRounds = state.QuietRounds
		state.Status = ReviewConvergencePending
		if state.QuietRounds >=
			cycle.Policy.Convergence.QuietRoundsRequired {
			state.Status = ReviewConvergenceConverged
		}
	}
	if round.Round >= cycle.Policy.Convergence.MaxRounds &&
		state.Status != ReviewConvergenceConverged {
		round.Outcome = ReviewConvergenceRoundMaxRounds
		if round.Action == "" {
			round.Action =
				cycle.Policy.FailureActions.BudgetExhaustion
		}
		state.Status = ReviewConvergenceMaxRounds
	}
	return *round, nil
}

func reviewDiscoveryPassCompletedSuccessfully(
	pass *ReviewDiscoveryPassState,
) bool {
	if pass == nil || pass.CompletedAt.IsZero() || len(pass.Lanes) == 0 {
		return false
	}
	synthesisCompleted := false
	for _, lane := range pass.Lanes {
		if lane.Lane == reviewSynthesisLane {
			synthesisCompleted = lane.Status == ReviewDiscoveryLaneCompleted
			continue
		}
		if lane.Status != ReviewDiscoveryLaneCompleted &&
			!(lane.Status == ReviewDiscoveryLaneFailed &&
				lane.RecoveredBySynthesis) {
			return false
		}
	}
	return synthesisCompleted
}

func reviewConfirmedFindingIDs(
	decisions []ReviewFindingVerification,
) []string {
	values := make([]string, 0)
	for _, decision := range decisions {
		if decision.Status == ReviewFindingVerificationConfirmed {
			values = append(values, decision.FindingID)
		}
	}
	sort.Strings(values)
	return values
}

func reviewConfirmedFindingSnapshots(
	decisions []ReviewFindingVerification,
) []ReviewConvergenceFindingSnapshot {
	values := make([]ReviewConvergenceFindingSnapshot, 0)
	for _, decision := range decisions {
		if decision.Status == ReviewFindingVerificationConfirmed {
			values = append(values, ReviewConvergenceFindingSnapshot{
				FindingID:         decision.FindingID,
				CandidateRevision: decision.CandidateRevision,
			})
		}
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].FindingID < values[j].FindingID
	})
	return values
}

func reviewCoverageGapIDs(gaps []ReviewCoverageGap) []string {
	values := make([]string, 0, len(gaps))
	for _, gap := range gaps {
		values = append(values, gap.ID)
	}
	sort.Strings(values)
	return values
}

func reviewCanonicalFindingIDs(cycle *ReviewCycleState) []string {
	if cycle == nil {
		return []string{}
	}
	values := make([]string, 0, len(cycle.CanonicalFindings))
	for _, finding := range cycle.CanonicalFindings {
		if finding.ExactSHA == cycle.HeadSHA {
			values = append(values, finding.ID)
		}
	}
	return uniqueSortedStrings(values)
}

func stringSetDifference(values, baseline []string) []string {
	seen := make(map[string]struct{}, len(baseline))
	for _, value := range baseline {
		seen[value] = struct{}{}
	}
	difference := make([]string, 0)
	for _, value := range values {
		if _, exists := seen[value]; !exists {
			difference = append(difference, value)
		}
	}
	sort.Strings(difference)
	return difference
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (cycle *ReviewCycleState) PublishableFindings() []ReviewCanonicalFinding {
	if cycle == nil || cycle.Stale || cycle.Convergence == nil {
		return []ReviewCanonicalFinding{}
	}
	confirmed := make(map[string]struct{})
	for _, decision := range cycle.Convergence.FindingVerifications {
		if decision.Status == ReviewFindingVerificationConfirmed &&
			decision.ExactSHA == cycle.HeadSHA {
			confirmed[decision.FindingID] = struct{}{}
		}
	}
	findings := make([]ReviewCanonicalFinding, 0, len(confirmed))
	for _, finding := range cycle.CanonicalFindings {
		if _, ok := confirmed[finding.ID]; ok &&
			finding.ExactSHA == cycle.HeadSHA {
			findings = append(findings, finding)
		}
	}
	if findings == nil {
		return []ReviewCanonicalFinding{}
	}
	return findings
}

func reviewCycleReadyForChangesRequiredVerdict(
	cycle *ReviewCycleState,
) bool {
	if cycle == nil || cycle.Stale || cycle.Convergence == nil ||
		len(cycle.PublishableFindings()) == 0 {
		return false
	}
	decisions := make(map[string]ReviewFindingVerification)
	for _, decision := range cycle.Convergence.FindingVerifications {
		decisions[decision.FindingID] = decision
	}
	for _, finding := range cycle.CanonicalFindings {
		if finding.ExactSHA != cycle.HeadSHA {
			continue
		}
		decision, ok := decisions[finding.ID]
		if !ok || decision.ExactSHA != cycle.HeadSHA ||
			(decision.Status != ReviewFindingVerificationConfirmed &&
				decision.Status != ReviewFindingVerificationRejected) {
			return false
		}
	}
	return true
}

func (cycle *ReviewCycleState) MandatoryCoverageComplete() bool {
	if cycle == nil || cycle.LimitTransition != nil ||
		cycle.Convergence == nil ||
		(cycle.Convergence.Status != ReviewConvergenceConverged &&
			cycle.Convergence.Status != ReviewConvergenceChangesRequired) ||
		len(cycle.UnresolvedCoverage) != 0 {
		return false
	}
	latest, ok := latestReviewDiscoveryPass(cycle)
	if !ok || !latest.ApprovalEligible(cycle.UnresolvedCoverage) {
		return false
	}
	if cycle.Plan == nil {
		return false
	}
	laneStates := make(map[string]ReviewDiscoveryLaneState, len(latest.Lanes))
	for _, lane := range latest.Lanes {
		laneStates[lane.Lane] = lane
	}
	for _, requiredLane := range cycle.Plan.RequiredLanes {
		lane, exists := laneStates[requiredLane]
		if !exists || !lane.Required ||
			(lane.Status != ReviewDiscoveryLaneCompleted &&
				!(lane.Status == ReviewDiscoveryLaneFailed &&
					lane.RecoveredBySynthesis)) {
			return false
		}
	}
	decisions := make(
		map[string]ReviewFindingVerification,
		len(cycle.Convergence.FindingVerifications),
	)
	for _, decision := range cycle.Convergence.FindingVerifications {
		if decision.Status != ReviewFindingVerificationConfirmed &&
			decision.Status != ReviewFindingVerificationRejected {
			return false
		}
		decisions[decision.FindingID] = decision
	}
	for _, finding := range cycle.CanonicalFindings {
		if finding.ExactSHA != cycle.HeadSHA {
			continue
		}
		decision, exists := decisions[finding.ID]
		if !exists || decision.ExactSHA != cycle.HeadSHA ||
			(decision.Status != ReviewFindingVerificationConfirmed &&
				decision.Status != ReviewFindingVerificationRejected) {
			return false
		}
	}
	for _, assignment := range cycle.Convergence.VerificationAssignments {
		if !assignment.Superseded &&
			assignment.Status != ReviewVerificationCompleted {
			return false
		}
	}
	for _, assignment := range cycle.Convergence.ChallengeAssignments {
		if assignment.Superseded {
			continue
		}
		if assignment.Status != ReviewChallengeCompleted ||
			assignment.Outcome == ReviewChallengeInconclusive {
			return false
		}
	}
	return true
}

func (cycle *ReviewCycleState) ApprovalEligible() bool {
	return cycle != nil && !cycle.Stale &&
		cycle.MandatoryCoverageComplete() &&
		cycle.Convergence.Status == ReviewConvergenceConverged &&
		len(cycle.PublishableFindings()) == 0
}

func validatePersistedReviewConvergence(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("persisted review cycle is missing")
	}
	if cycle.Convergence == nil {
		return nil
	}
	state := cycle.Convergence
	if state.VerificationAssignments == nil ||
		state.FindingVerifications == nil ||
		state.ChallengeAssignments == nil ||
		state.Rounds == nil {
		return errors.New(
			"persisted review convergence collections are not initialized",
		)
	}
	assignmentIDs := make(map[string]struct{})
	assignmentOrdinals := make(map[string]map[int]struct{})
	assignmentCounts := make(map[string]int)
	for _, assignment := range state.VerificationAssignments {
		targetsCurrentCandidate :=
			reviewVerificationAssignmentTargetsCurrentCandidate(
				cycle,
				assignment,
			)
		snapshotRevision, revisionErr :=
			reviewFindingCandidateRevision(assignment.CandidateSnapshot)
		countKey := assignment.FindingID + "\x00" +
			assignment.CandidateRevision
		if assignment.Superseded == targetsCurrentCandidate ||
			assignment.ID == "" ||
			assignment.ExactSHA != cycle.HeadSHA ||
			revisionErr != nil ||
			validateReviewPolicyFingerprint(
				assignment.CandidateRevision,
			) != nil ||
			snapshotRevision != assignment.CandidateRevision ||
			assignment.CandidateSnapshot.ID != assignment.FindingID ||
			assignment.CandidateSnapshot.ExactSHA != assignment.ExactSHA ||
			assignment.Round <= 0 ||
			assignment.Ordinal <= 0 ||
			assignment.Round >
				cycle.Policy.Convergence.MaxRounds ||
			assignment.ID != fmt.Sprintf(
				"verification:%s:%03d",
				assignment.FindingID,
				assignment.Ordinal,
			) ||
			assignment.Lane != reviewVerificationLane(
				assignment.FindingID,
				assignment.Ordinal,
			) ||
			assignment.QueuedAt.IsZero() ||
			!reflect.DeepEqual(
				assignment.ExcludedWorkerIDs,
				uniqueSortedStrings(assignment.ExcludedWorkerIDs),
			) {
			return errors.New(
				"persisted review verification assignment is invalid",
			)
		}
		if assignmentOrdinals[assignment.FindingID] == nil {
			assignmentOrdinals[assignment.FindingID] =
				make(map[int]struct{})
		}
		if _, duplicate :=
			assignmentOrdinals[assignment.FindingID][assignment.Ordinal]; duplicate {
			return errors.New(
				"persisted review verification ordinal is duplicated",
			)
		}
		assignmentOrdinals[assignment.FindingID][assignment.Ordinal] =
			struct{}{}
		assignmentCounts[countKey]++
		if assignmentCounts[countKey] >
			cycle.Policy.Verification.MaxVerifiers {
			return errors.New(
				"persisted review verification exceeds the verifier maximum",
			)
		}
		for _, origin := range reviewFindingOriginWorkerIDs(
			assignment.CandidateSnapshot,
		) {
			if !stringSliceContains(
				assignment.ExcludedWorkerIDs,
				origin,
			) {
				return errors.New(
					"persisted review verification permits self-confirmation",
				)
			}
		}
		if _, duplicate := assignmentIDs[assignment.ID]; duplicate {
			return errors.New(
				"persisted review verification assignment is duplicated",
			)
		}
		assignmentIDs[assignment.ID] = struct{}{}
		switch assignment.Status {
		case ReviewVerificationQueued:
			if (!assignment.Superseded &&
				!reviewVerificationAssignmentMatchesCurrentCandidate(
					cycle,
					assignment,
				)) ||
				!assignment.StartedAt.IsZero() ||
				!assignment.CompletedAt.IsZero() ||
				assignment.WorkerID != "" ||
				assignment.Attempt != 0 {
				return errors.New(
					"persisted queued review verification is invalid",
				)
			}
		case ReviewVerificationRunning:
			if (!assignment.Superseded &&
				!reviewVerificationAssignmentMatchesCurrentCandidate(
					cycle,
					assignment,
				)) ||
				assignment.StartedAt.IsZero() ||
				!assignment.CompletedAt.IsZero() ||
				(assignment.WorkerID == "") !=
					(assignment.Attempt == 0) {
				return errors.New(
					"persisted running review verification is invalid",
				)
			}
			if assignment.WorkerID != "" {
				ownership, ok := reviewArtifactOwnershipForAttempt(
					cycle,
					assignment.WorkerID,
					assignment.Attempt,
				)
				if !ok || !reviewVerificationOwnershipMatches(
					cycle,
					assignment,
					ownership,
				) {
					return errors.New(
						"persisted running review verification has an invalid owner",
					)
				}
			}
		case ReviewVerificationCompleted:
			if assignment.StartedAt.IsZero() ||
				assignment.CompletedAt.IsZero() ||
				assignment.WorkerID == "" ||
				assignment.Attempt <= 0 {
				return errors.New(
					"persisted completed review verification is invalid",
				)
			}
			if !reviewVerificationAssignmentHasTrustedReceipt(
				cycle,
				assignment,
			) {
				return errors.New(
					"persisted completed review verification has no trusted evidence",
				)
			}
		case ReviewVerificationFailed:
			if assignment.CompletedAt.IsZero() ||
				assignment.FailureCode == "" ||
				assignment.Action == "" {
				return errors.New(
					"persisted failed review verification is invalid",
				)
			}
		default:
			return errors.New(
				"persisted review verification status is unsupported",
			)
		}
	}
	expected := cloneReviewCycle(cycle)
	expected.Convergence.FindingVerifications = nil
	if err := syncReviewFindingVerifications(expected); err != nil {
		return err
	}
	if !reflect.DeepEqual(
		expected.Convergence.FindingVerifications,
		state.FindingVerifications,
	) {
		return errors.New(
			"persisted finding verification does not match trusted assignments",
		)
	}
	challengeIDs := make(map[string]struct{})
	promptFingerprints := make(map[string]struct{})
	for _, assignment := range state.ChallengeAssignments {
		if assignment.ID == "" ||
			assignment.ExactSHA != cycle.HeadSHA ||
			assignment.Round <= 0 ||
			assignment.Round >
				cycle.Policy.Convergence.MaxRounds ||
			assignment.Lane == "" ||
			assignment.QueuedAt.IsZero() ||
			assignment.PromptFingerprint == "" ||
			!supportedReviewChallengeTargetKind(
				assignment.Target.Kind,
			) ||
			assignment.Target.ID == "" ||
			strings.TrimSpace(assignment.Target.Description) == "" {
			return errors.New(
				"persisted review challenge assignment is invalid",
			)
		}
		if _, duplicate := challengeIDs[assignment.ID]; duplicate {
			return errors.New(
				"persisted review challenge assignment is duplicated",
			)
		}
		challengeIDs[assignment.ID] = struct{}{}
		if _, duplicate :=
			promptFingerprints[assignment.PromptFingerprint]; duplicate {
			return errors.New(
				"persisted review challenge repeats an identical prompt",
			)
		}
		promptFingerprints[assignment.PromptFingerprint] = struct{}{}
		switch assignment.Target.Kind {
		case ReviewChallengeCoverageGap:
			if assignment.Superseded ||
				assignment.CandidateRevision != "" ||
				assignment.CandidateSnapshot != nil {
				return errors.New(
					"persisted review coverage challenge has finding state",
				)
			}
			matched := false
			for _, requirement := range cycle.Plan.CoverageRequirements {
				if requirement.ID == assignment.Target.RequirementID {
					matched = true
					break
				}
			}
			if !matched {
				return errors.New(
					"persisted review challenge targets unknown coverage",
				)
			}
		case ReviewChallengeCompetingHypothesis:
			if assignment.CandidateSnapshot == nil {
				return errors.New(
					"persisted review hypothesis challenge has no candidate snapshot",
				)
			}
			snapshotRevision, err := reviewFindingCandidateRevision(
				*assignment.CandidateSnapshot,
			)
			targetsCurrentCandidate :=
				reviewChallengeAssignmentTargetsCurrentCandidate(
					cycle,
					assignment,
				)
			if err != nil ||
				assignment.CandidateRevision != snapshotRevision ||
				assignment.CandidateSnapshot.ID != assignment.Target.ID ||
				assignment.CandidateSnapshot.ExactSHA != assignment.ExactSHA ||
				assignment.Superseded == targetsCurrentCandidate {
				return errors.New(
					"persisted review challenge hypothesis snapshot is invalid",
				)
			}
		}
		switch assignment.Status {
		case ReviewChallengeQueued:
			if !assignment.StartedAt.IsZero() ||
				!assignment.CompletedAt.IsZero() ||
				assignment.WorkerID != "" ||
				assignment.Attempt != 0 {
				return errors.New(
					"persisted queued review challenge is invalid",
				)
			}
		case ReviewChallengeRunning:
			if assignment.StartedAt.IsZero() ||
				!assignment.CompletedAt.IsZero() ||
				(assignment.WorkerID == "") !=
					(assignment.Attempt == 0) {
				return errors.New(
					"persisted running review challenge is invalid",
				)
			}
			if assignment.WorkerID != "" {
				ownership, ok := reviewArtifactOwnershipForAttempt(
					cycle,
					assignment.WorkerID,
					assignment.Attempt,
				)
				if !ok || !reviewChallengeOwnershipMatches(
					cycle,
					assignment,
					ownership,
				) {
					return errors.New(
						"persisted running review challenge has an invalid owner",
					)
				}
			}
		case ReviewChallengeCompleted:
			if assignment.StartedAt.IsZero() ||
				assignment.CompletedAt.IsZero() ||
				assignment.WorkerID == "" ||
				assignment.Attempt <= 0 ||
				!reviewChallengeAssignmentHasTrustedReceipt(
					cycle,
					assignment,
				) {
				return errors.New(
					"persisted completed review challenge has no trusted evidence",
				)
			}
		case ReviewChallengeFailed:
			if assignment.CompletedAt.IsZero() ||
				assignment.Action == "" {
				return errors.New(
					"persisted failed review challenge is invalid",
				)
			}
		default:
			return errors.New(
				"persisted review challenge status is unsupported",
			)
		}
	}
	for index, round := range state.Rounds {
		discoveryPass, hasDiscovery := reviewDiscoveryPassByNumber(
			cycle,
			round.DiscoveryPass,
		)
		if round.Round != index+1 ||
			round.DiscoveryPass != round.Round ||
			!hasDiscovery || discoveryPass.CompletedAt.IsZero() ||
			len(discoveryPass.Lanes) == 0 ||
			((round.Outcome == ReviewConvergenceRoundQuiet ||
				round.Outcome == ReviewConvergenceRoundMaterial ||
				round.Outcome == ReviewConvergenceRoundChangesRequired) &&
				!reviewDiscoveryPassCompletedSuccessfully(discoveryPass)) ||
			round.StartedAt.IsZero() ||
			(index < len(state.Rounds)-1 &&
				round.CompletedAt.IsZero()) ||
			!reviewConvergenceRoundLedgerIsValid(round) {
			return errors.New(
				"persisted review convergence round is invalid",
			)
		}
	}
	switch state.Status {
	case ReviewConvergencePending,
		ReviewConvergenceRunning,
		ReviewConvergenceConverged,
		ReviewConvergenceChangesRequired,
		ReviewConvergenceMaxRounds,
		ReviewConvergenceUnresolved:
	default:
		return errors.New(
			"persisted review convergence status is unsupported",
		)
	}
	if len(state.Rounds) == 0 {
		if state.Status != ReviewConvergencePending ||
			state.QuietRounds != 0 {
			return errors.New(
				"persisted review convergence has invalid initial state",
			)
		}
		return nil
	}
	latest := state.Rounds[len(state.Rounds)-1]
	if latest.QuietRounds != state.QuietRounds {
		return errors.New(
			"persisted review convergence quiet-round count is inconsistent",
		)
	}
	switch latest.Outcome {
	case ReviewConvergenceRoundActive:
		if state.Status == ReviewConvergenceRunning {
			break
		}
		return errors.New(
			"persisted active review round is not running",
		)
	case ReviewConvergenceRoundQuiet:
		if state.Status == ReviewConvergencePending ||
			(state.Status == ReviewConvergenceConverged &&
				state.QuietRounds >=
					cycle.Policy.Convergence.QuietRoundsRequired) {
			break
		}
		return errors.New(
			"persisted quiet review round has an invalid convergence status",
		)
	case ReviewConvergenceRoundMaterial:
		if state.Status == ReviewConvergencePending &&
			state.QuietRounds == 0 {
			break
		}
		return errors.New(
			"persisted material review round has an invalid convergence status",
		)
	case ReviewConvergenceRoundChangesRequired:
		if state.Status == ReviewConvergenceChangesRequired &&
			state.QuietRounds == 0 &&
			reviewCycleReadyForChangesRequiredVerdict(cycle) {
			break
		}
		return errors.New(
			"persisted changes-required review round has an invalid convergence status",
		)
	case ReviewConvergenceRoundUnresolved:
		if state.Status == ReviewConvergenceUnresolved &&
			state.QuietRounds == 0 {
			break
		}
		return errors.New(
			"persisted unresolved review round has an invalid convergence status",
		)
	case ReviewConvergenceRoundMaxRounds:
		if state.Status == ReviewConvergenceMaxRounds &&
			latest.Action != "" {
			break
		}
		return errors.New(
			"persisted review convergence maximum-round outcome is missing",
		)
	default:
		return errors.New(
			"persisted review convergence round outcome is unsupported",
		)
	}
	return nil
}

func reviewConvergenceRoundLedgerIsValid(
	round ReviewConvergenceRoundState,
) bool {
	for _, values := range [][]string{
		round.StartingConfirmedFindingIDs,
		round.StartingCandidateIDs,
		round.ObservedCandidateIDs,
		round.NewCandidateIDs,
		round.StartingCoverageGapIDs,
		round.NewConfirmedFindingIDs,
		round.NewCoverageGapIDs,
	} {
		for index, value := range values {
			if strings.TrimSpace(value) != value || value == "" ||
				(index > 0 && values[index-1] >= value) {
				return false
			}
		}
	}
	previousFindingID := ""
	for _, snapshot := range round.StartingConfirmedFindings {
		if snapshot.FindingID == "" ||
			snapshot.FindingID <= previousFindingID ||
			!stringSliceContains(
				round.StartingConfirmedFindingIDs,
				snapshot.FindingID,
			) ||
			validateReviewPolicyFingerprint(
				snapshot.CandidateRevision,
			) != nil {
			return false
		}
		previousFindingID = snapshot.FindingID
	}
	return true
}

func reviewVerificationAssignmentHasTrustedReceipt(
	cycle *ReviewCycleState,
	assignment ReviewVerificationAssignment,
) bool {
	ownership, ok := reviewArtifactOwnershipForAttempt(
		cycle,
		assignment.WorkerID,
		assignment.Attempt,
	)
	if !ok || !reviewVerificationOwnershipMatches(
		cycle,
		assignment,
		ownership,
	) {
		return false
	}
	for _, receipt := range cycle.ArtifactReceipts {
		if receipt.WorkerID != assignment.WorkerID ||
			receipt.Attempt != assignment.Attempt ||
			receipt.Phase != ReviewArtifactPhaseVerification ||
			receipt.Envelope.Payload.Verification == nil {
			continue
		}
		payload := receipt.Envelope.Payload.Verification
		if validateReviewVerificationEvidence(
			cycle,
			assignment,
			*payload,
		) != nil {
			return false
		}
		return payload.FindingID == assignment.FindingID &&
			payload.Outcome == assignment.Outcome &&
			strings.Join(strings.Fields(payload.Summary), " ") ==
				assignment.Summary &&
			payload.ScopeDisposition == assignment.ScopeDisposition &&
			payload.PatchDisposition == assignment.PatchDisposition &&
			reflect.DeepEqual(payload.Location, assignment.Location) &&
			payload.BehavioralPath == assignment.BehavioralPath &&
			slices.Equal(payload.Evidence, assignment.Evidence) &&
			slices.Equal(
				payload.CausalEvidence,
				assignment.CausalEvidence,
			) &&
			slices.Equal(
				payload.TestEvidence,
				assignment.TestEvidence,
			) &&
			strings.Join(
				strings.Fields(payload.TestNotPracticalReason),
				" ",
			) == assignment.TestNotPractical &&
			receipt.Envelope.ExactSHA == assignment.ExactSHA
	}
	return false
}

func reviewChallengeAssignmentHasTrustedReceipt(
	cycle *ReviewCycleState,
	assignment ReviewChallengeAssignment,
) bool {
	ownership, ok := reviewArtifactOwnershipForAttempt(
		cycle,
		assignment.WorkerID,
		assignment.Attempt,
	)
	if !ok || !reviewChallengeOwnershipMatches(
		cycle,
		assignment,
		ownership,
	) {
		return false
	}
	for _, receipt := range cycle.ArtifactReceipts {
		if receipt.WorkerID != assignment.WorkerID ||
			receipt.Attempt != assignment.Attempt ||
			receipt.Phase != ReviewArtifactPhaseChallenge ||
			receipt.Envelope.Payload.Challenge == nil {
			continue
		}
		payload := receipt.Envelope.Payload.Challenge
		if payload.Candidates == nil ||
			payload.Coverage == nil ||
			validateReviewFindingCandidates(payload.Candidates) != nil ||
			validateReviewCoverageClaims(
				payload.Coverage,
				cycle.Plan.CoverageRequirements,
			) != nil {
			return false
		}
		if assignment.Target.Kind == ReviewChallengeCoverageGap {
			targetClaimed := false
			for _, claim := range payload.Coverage {
				if claim.RequirementID ==
					assignment.Target.RequirementID {
					targetClaimed = true
					break
				}
			}
			if !targetClaimed {
				return false
			}
		}
		if assignment.Target.Kind == ReviewChallengeCompetingHypothesis &&
			payload.Outcome != ReviewChallengeInconclusive &&
			!reviewChallengeCandidatesReproduceTarget(
				assignment,
				payload.Candidates,
			) {
			return false
		}
		return payload.AssignmentID == assignment.ID &&
			payload.TargetKind == assignment.Target.Kind &&
			payload.TargetID == assignment.Target.ID &&
			payload.Outcome == assignment.Outcome &&
			strings.Join(strings.Fields(payload.Summary), " ") ==
				assignment.Summary &&
			receipt.Envelope.ExactSHA == assignment.ExactSHA
	}
	return false
}
