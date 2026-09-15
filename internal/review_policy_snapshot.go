// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2026 Cisco Systems, Inc. and its affiliates
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

package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type ReviewCycleState struct {
	ID                   string                        `json:"id,omitempty"`
	Revision             int                           `json:"revision,omitempty"`
	Attempt              int                           `json:"attempt"`
	HeadSHA              string                        `json:"head_sha"`
	ResultState          ReviewCycleResultState        `json:"result_state,omitempty"`
	Stale                bool                          `json:"stale,omitempty"`
	StaleAt              time.Time                     `json:"stale_at,omitempty"`
	SupersededByHeadSHA  string                        `json:"superseded_by_head_sha,omitempty"`
	PolicyVersion        int                           `json:"policy_version"`
	PolicyFingerprint    string                        `json:"policy_fingerprint"`
	Policy               ReviewPolicy                  `json:"policy"`
	Inputs               *ReviewPlanInputs             `json:"inputs,omitempty"`
	Plan                 *ReviewPlan                   `json:"plan,omitempty"`
	PriorReviewLedger    *ReviewLedger                 `json:"prior_review_ledger,omitempty"`
	ReviewLedgerInputs   *ReviewPlanInputs             `json:"review_ledger_inputs,omitempty"`
	ReviewLedgerPlan     *ReviewPlan                   `json:"review_ledger_plan,omitempty"`
	CorrectionRound      int                           `json:"correction_round,omitempty"`
	WorkerOwnerships     []ReviewWorkerOwnership       `json:"worker_ownerships,omitempty"`
	ArtifactReceipts     []ReviewArtifactReceipt       `json:"artifact_receipts,omitempty"`
	ArtifactFailures     []ReviewArtifactIntakeFailure `json:"artifact_failures,omitempty"`
	LaneCompletions      []ReviewLaneCompletion        `json:"lane_completions,omitempty"`
	DiscoveryPasses      []ReviewDiscoveryPassState    `json:"discovery_passes,omitempty"`
	CanonicalFindings    []ReviewCanonicalFinding      `json:"canonical_findings,omitempty"`
	UnresolvedCoverage   []ReviewCoverageGap           `json:"unresolved_coverage,omitempty"`
	Convergence          *ReviewConvergenceState       `json:"convergence,omitempty"`
	Metrics              *ReviewMetricsState           `json:"metrics,omitempty"`
	LimitTransition      *ReviewLimitTransition        `json:"limit_transition,omitempty"`
	EscalationTransition *ReviewEscalationTransition   `json:"escalation_transition,omitempty"`
	VerdictPublication   *ReviewVerdictPublication     `json:"verdict_publication,omitempty"`
}

const canonicalGitObjectIDLength = 40

func snapshotEffectiveReviewPolicy(policy ReviewPolicy) (ReviewPolicy, error) {
	snapshot, err := cloneReviewPolicy(policy)
	if err != nil {
		return ReviewPolicy{}, err
	}

	referencedProfiles := make(map[string]struct{})
	for _, profileName := range snapshot.RoleProfiles {
		referencedProfiles[strings.TrimSpace(profileName)] = struct{}{}
	}
	for _, lane := range snapshot.Swarm.Lanes {
		referencedProfiles[strings.TrimSpace(lane.Profile)] = struct{}{}
	}
	referencedProfiles[strings.TrimSpace(snapshot.Escalation.Profile)] = struct{}{}

	profileNames := make([]string, 0, len(referencedProfiles))
	for profileName := range referencedProfiles {
		if profileName != "" {
			profileNames = append(profileNames, profileName)
		}
	}
	sort.Strings(profileNames)
	for _, profileName := range profileNames {
		profile, ok := snapshot.AgentProfiles[profileName]
		if !ok {
			return ReviewPolicy{}, fmt.Errorf("effective review policy references unknown profile %q", profileName)
		}
		profile, err = applyAgentProfileOverride(profile, snapshot.runtimeProfileCLIOverride)
		if err != nil {
			return ReviewPolicy{}, fmt.Errorf("failed to resolve effective review profile %q: %w", profileName, err)
		}
		if err := validateAgentProfile(profile, snapshot.activeModelCatalog()); err != nil {
			return ReviewPolicy{}, fmt.Errorf("effective review profile %q is invalid: %w", profileName, err)
		}
		snapshot.AgentProfiles[profileName] = profile
	}
	snapshot.runtimeProfileCLIOverride = agentProfileOverride{}

	snapshot.Swarm.Lanes = append([]ReviewLane(nil), snapshot.Swarm.Lanes...)
	for model, capability := range snapshot.ModelCatalog {
		capability.ReasoningEfforts = append([]string(nil), capability.ReasoningEfforts...)
		sort.Strings(capability.ReasoningEfforts)
		snapshot.ModelCatalog[model] = capability
	}

	if err := validateReviewPolicyConfiguration(snapshot); err != nil {
		return ReviewPolicy{}, fmt.Errorf("effective review policy is invalid: %w", err)
	}
	snapshot.Fingerprint = ""
	body, err := json.Marshal(snapshot)
	if err != nil {
		return ReviewPolicy{}, fmt.Errorf("failed to canonicalize effective review policy: %w", err)
	}
	sum := sha256.Sum256(body)
	snapshot.Fingerprint = hex.EncodeToString(sum[:])
	return snapshot, nil
}

func cloneReviewPolicy(policy ReviewPolicy) (ReviewPolicy, error) {
	body, err := json.Marshal(policy)
	if err != nil {
		return ReviewPolicy{}, fmt.Errorf("failed to clone review policy: %w", err)
	}
	var clone ReviewPolicy
	if err := json.Unmarshal(body, &clone); err != nil {
		return ReviewPolicy{}, fmt.Errorf("failed to clone review policy: %w", err)
	}
	clone.repositoryProfileNames = make(map[string]struct{}, len(policy.repositoryProfileNames))
	for name := range policy.repositoryProfileNames {
		clone.repositoryProfileNames[name] = struct{}{}
	}
	clone.runtimeProfileCLIOverride = policy.runtimeProfileCLIOverride
	return clone, nil
}

func newReviewCycleState(headSHA string, policy ReviewPolicy) (*ReviewCycleState, error) {
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	if headSHA == "" {
		return nil, errors.New("review cycle exact head SHA is missing")
	}
	if err := validateCanonicalGitObjectID(headSHA); err != nil {
		return nil, fmt.Errorf("review cycle exact head SHA is invalid: %w", err)
	}
	if policy.Version == 0 {
		policy = builtInReviewPolicy()
	}
	snapshot, err := snapshotEffectiveReviewPolicy(policy)
	if err != nil {
		return nil, err
	}
	cycleID, err := newReviewCycleID(headSHA)
	if err != nil {
		return nil, err
	}
	cycle := &ReviewCycleState{
		ID:                cycleID,
		Revision:          1,
		Attempt:           1,
		HeadSHA:           headSHA,
		PolicyVersion:     snapshot.Version,
		PolicyFingerprint: snapshot.Fingerprint,
		Policy:            snapshot,
	}
	cycle.Metrics, err = newReviewMetricsState(cycle, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize review metrics: %w", err)
	}
	return cycle, nil
}

func cloneReviewCycle(cycle *ReviewCycleState) *ReviewCycleState {
	if cycle == nil {
		return nil
	}
	body, err := json.Marshal(cycle)
	if err != nil {
		clone := *cycle
		return &clone
	}
	var clone ReviewCycleState
	if err := json.Unmarshal(body, &clone); err != nil {
		fallback := *cycle
		return &fallback
	}
	return &clone
}

func snapshotReviewCycleResult(reviewer Agent) *ReviewCycleState {
	cycle := cloneReviewCycle(reviewer.ReviewCycle)
	if cycle == nil {
		return nil
	}
	reviewer.ReviewCycle = cycle
	cycle.ResultState = ""
	if state, final := reviewCycleResultForReviewer(reviewer); final {
		cycle.ResultState = state
	}
	return cycle
}

func validatePersistedReviewCycleSnapshot(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("persisted review cycle policy snapshot is missing")
	}
	if cycle.Attempt <= 0 {
		return errors.New("persisted review cycle attempt must be greater than zero")
	}
	if err := validateReviewCycleResult(cycle); err != nil {
		return err
	}
	if err := validateReviewCycleWorkerOwnerships(cycle); err != nil {
		return err
	}
	if strings.TrimSpace(cycle.HeadSHA) == "" {
		return errors.New("persisted review cycle exact head SHA is missing")
	}
	if err := validateCanonicalGitObjectID(cycle.HeadSHA); err != nil {
		return fmt.Errorf("persisted review cycle exact head SHA is invalid: %w", err)
	}
	switch {
	case cycle.Stale:
		if cycle.StaleAt.IsZero() {
			return errors.New(
				"persisted stale review cycle invalidation time is missing",
			)
		}
		if err := validateCanonicalGitObjectID(
			cycle.SupersededByHeadSHA,
		); err != nil {
			return fmt.Errorf(
				"persisted stale review cycle successor head SHA is invalid: %w",
				err,
			)
		}
		if cycle.SupersededByHeadSHA == cycle.HeadSHA {
			return errors.New(
				"persisted stale review cycle successor head matches the stale head",
			)
		}
	case !cycle.StaleAt.IsZero() ||
		strings.TrimSpace(cycle.SupersededByHeadSHA) != "":
		return errors.New(
			"persisted active review cycle contains stale-head audit state",
		)
	}
	if cycle.PolicyVersion != supportedReviewPolicyVersion {
		return fmt.Errorf(
			"persisted review cycle policy version %d is unsupported; expected %d",
			cycle.PolicyVersion,
			supportedReviewPolicyVersion,
		)
	}
	if cycle.Policy.Version != cycle.PolicyVersion {
		return fmt.Errorf(
			"persisted review cycle policy version mismatch: state=%d snapshot=%d",
			cycle.PolicyVersion,
			cycle.Policy.Version,
		)
	}
	if err := validateReviewPolicyFingerprint(cycle.PolicyFingerprint); err != nil {
		return fmt.Errorf("persisted review cycle policy fingerprint is invalid: %w", err)
	}
	if cycle.Policy.Fingerprint != cycle.PolicyFingerprint {
		return fmt.Errorf(
			"persisted review cycle policy fingerprint mismatch: state=%q snapshot=%q",
			cycle.PolicyFingerprint,
			cycle.Policy.Fingerprint,
		)
	}
	computed, err := snapshotEffectiveReviewPolicy(cycle.Policy)
	if err != nil {
		return fmt.Errorf("persisted review cycle policy snapshot is invalid: %w", err)
	}
	if computed.Fingerprint != cycle.PolicyFingerprint {
		return fmt.Errorf(
			"persisted review cycle policy fingerprint mismatch: state=%q snapshot=%q computed=%q",
			cycle.PolicyFingerprint,
			cycle.Policy.Fingerprint,
			computed.Fingerprint,
		)
	}
	if cycle.Inputs != nil {
		if err := validateReviewPlanInputs(*cycle.Inputs); err != nil {
			return fmt.Errorf("persisted review cycle plan inputs are invalid: %w", err)
		}
		if cycle.Inputs.HeadSHA != cycle.HeadSHA {
			return fmt.Errorf(
				"persisted review cycle plan input head mismatch: cycle=%s inputs=%s",
				abbreviateSHA(cycle.HeadSHA),
				abbreviateSHA(cycle.Inputs.HeadSHA),
			)
		}
	}
	if err := validateReviewLedgerCycleContext(cycle); err != nil {
		return fmt.Errorf("persisted review ledger cycle context is invalid: %w", err)
	}
	if err := validateReviewPlanAgainstCycle(cycle); err != nil {
		return fmt.Errorf("persisted review cycle plan is invalid: %w", err)
	}
	if err := validatePersistedReviewArtifacts(cycle); err != nil {
		return fmt.Errorf("persisted review cycle artifacts are invalid: %w", err)
	}
	if err := validatePersistedReviewDiscovery(cycle); err != nil {
		return fmt.Errorf("persisted review cycle discovery is invalid: %w", err)
	}
	if err := validatePersistedReviewConvergence(cycle); err != nil {
		return fmt.Errorf(
			"persisted review cycle convergence is invalid: %w",
			err,
		)
	}
	if err := validateReviewMetricsState(cycle, cycle.Metrics); err != nil {
		return fmt.Errorf(
			"persisted review cycle metrics are invalid: %w",
			err,
		)
	}
	if err := validateReviewLimitState(cycle); err != nil {
		return fmt.Errorf(
			"persisted review cycle limit state is invalid: %w",
			err,
		)
	}
	if err := validateReviewVerdictPublication(cycle); err != nil {
		return fmt.Errorf(
			"persisted review verdict publication is invalid: %w",
			err,
		)
	}
	return nil
}

func validateCanonicalGitObjectID(objectID string) error {
	if len(objectID) != canonicalGitObjectIDLength {
		return fmt.Errorf(
			"must be a canonical %d-character lowercase hexadecimal Git object ID",
			canonicalGitObjectIDLength,
		)
	}
	decoded, err := hex.DecodeString(objectID)
	if err != nil || len(decoded) != canonicalGitObjectIDLength/2 {
		return fmt.Errorf(
			"must be a canonical %d-character lowercase hexadecimal Git object ID",
			canonicalGitObjectIDLength,
		)
	}
	if objectID != strings.ToLower(objectID) {
		return fmt.Errorf(
			"must be a canonical %d-character lowercase hexadecimal Git object ID",
			canonicalGitObjectIDLength,
		)
	}
	return nil
}

func validateReviewPolicyFingerprint(fingerprint string) error {
	fingerprint = strings.TrimSpace(fingerprint)
	if len(fingerprint) != sha256.Size*2 {
		return fmt.Errorf("must be a %d-character SHA-256 value", sha256.Size*2)
	}
	decoded, err := hex.DecodeString(fingerprint)
	if err != nil || len(decoded) != sha256.Size {
		return errors.New("must be a lowercase hexadecimal SHA-256 value")
	}
	if fingerprint != strings.ToLower(fingerprint) {
		return errors.New("must be a lowercase hexadecimal SHA-256 value")
	}
	return nil
}
