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
	"sort"
	"strings"
	"time"
)

const reviewLimitTransitionSchemaVersion = 1

type ReviewLimitKind string

const (
	ReviewLimitAgentCount     ReviewLimitKind = "agent_count"
	ReviewLimitWallTime       ReviewLimitKind = "wall_time"
	ReviewLimitSupportedUsage ReviewLimitKind = "supported_usage"
)

type ReviewLimitOutcome string

const (
	ReviewLimitOutcomeInconclusive    ReviewLimitOutcome = "inconclusive"
	ReviewLimitOutcomeEscalation      ReviewLimitOutcome = "escalation"
	ReviewLimitOutcomeTerminalFailure ReviewLimitOutcome = "terminal_failure"
)

type ReviewEscalationTrigger string

const (
	ReviewEscalationNonConvergence ReviewEscalationTrigger = "non_convergence"
)

type ReviewLimitUnresolvedEvidence struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
}

// ReviewLimitTransition is the one immutable terminal budget decision for an
// exact-SHA cycle. It is persisted at the same serialization boundary as
// worker ownership, before the over-budget launch is rejected.
type ReviewLimitTransition struct {
	SchemaVersion      int                             `json:"schema_version"`
	Kind               ReviewLimitKind                 `json:"kind"`
	TransitionedAt     time.Time                       `json:"transitioned_at"`
	Actual             uint64                          `json:"actual"`
	Maximum            uint64                          `json:"maximum"`
	Unit               string                          `json:"unit"`
	Action             ReviewAction                    `json:"action"`
	Outcome            ReviewLimitOutcome              `json:"outcome"`
	Reason             string                          `json:"reason"`
	MetricEventCount   uint64                          `json:"metric_event_count"`
	MetricEventDigest  string                          `json:"metric_event_digest"`
	RequiredLanes      []string                        `json:"required_lanes"`
	UnresolvedEvidence []ReviewLimitUnresolvedEvidence `json:"unresolved_evidence"`
}

// ReviewEscalationTransition records why subsequent verification or challenge
// work must use the snapshotted escalation profile.
type ReviewEscalationTransition struct {
	SchemaVersion     int                     `json:"schema_version"`
	Trigger           ReviewEscalationTrigger `json:"trigger"`
	TransitionedAt    time.Time               `json:"transitioned_at"`
	Profile           string                  `json:"profile"`
	CompletedRounds   uint64                  `json:"completed_rounds,omitempty"`
	MetricEventCount  uint64                  `json:"metric_event_count"`
	MetricEventDigest string                  `json:"metric_event_digest"`
}

type reviewLaunchLimitEvaluation struct {
	Limit      *ReviewLimitTransition
	Escalation *ReviewEscalationTransition
}

type reviewLaunchLimitMutation struct {
	reviewerID               string
	previousLimit            *ReviewLimitTransition
	previousEscalation       *ReviewEscalationTransition
	previousLastActivityTime time.Time
	updatedAt                time.Time
	changed                  bool
}

var errReviewLimitReached = errors.New("review launch limit reached")

type reviewLimitReachedError struct {
	transition ReviewLimitTransition
}

func (failure *reviewLimitReachedError) Error() string {
	if failure == nil {
		return errReviewLimitReached.Error()
	}
	return failure.transition.Reason
}

func (failure *reviewLimitReachedError) Unwrap() error {
	return errReviewLimitReached
}

func existingReviewLimitReachedError(
	cycle *ReviewCycleState,
) error {
	if cycle == nil || cycle.LimitTransition == nil {
		return nil
	}
	return &reviewLimitReachedError{
		transition: *cloneReviewLimitTransition(
			cycle.LimitTransition,
		),
	}
}

func reviewLimitOutcomeForAction(action ReviewAction) ReviewLimitOutcome {
	switch action {
	case ReviewActionEscalate:
		return ReviewLimitOutcomeEscalation
	case ReviewActionFail:
		return ReviewLimitOutcomeTerminalFailure
	default:
		return ReviewLimitOutcomeInconclusive
	}
}

func cloneReviewLimitTransition(
	transition *ReviewLimitTransition,
) *ReviewLimitTransition {
	if transition == nil {
		return nil
	}
	clone := *transition
	clone.RequiredLanes = append([]string(nil), transition.RequiredLanes...)
	clone.UnresolvedEvidence = append(
		[]ReviewLimitUnresolvedEvidence(nil),
		transition.UnresolvedEvidence...,
	)
	return &clone
}

func cloneReviewEscalationTransition(
	transition *ReviewEscalationTransition,
) *ReviewEscalationTransition {
	if transition == nil {
		return nil
	}
	clone := *transition
	return &clone
}

func reviewLimitUnresolvedEvidence(
	cycle *ReviewCycleState,
) ([]string, []ReviewLimitUnresolvedEvidence) {
	if cycle == nil {
		return []string{}, []ReviewLimitUnresolvedEvidence{}
	}
	requiredLanes := []string{}
	if cycle.Plan != nil {
		requiredLanes = append(requiredLanes, cycle.Plan.SelectedLanes...)
		requiredLanes = append(requiredLanes, reviewSynthesisLane)
		requiredLanes = uniqueSortedStrings(requiredLanes)
	}
	evidence := make([]ReviewLimitUnresolvedEvidence, 0)
	latestDiscovery, hasDiscovery := latestReviewDiscoveryPass(cycle)
	laneStates := make(map[string]ReviewDiscoveryLaneState)
	if hasDiscovery {
		for _, lane := range latestDiscovery.Lanes {
			laneStates[lane.Lane] = lane
		}
	}
	for _, lane := range requiredLanes {
		state, ok := laneStates[lane]
		if ok && state.Status == ReviewDiscoveryLaneCompleted {
			continue
		}
		status := "missing"
		if ok {
			status = string(state.Status)
		}
		evidence = append(evidence, ReviewLimitUnresolvedEvidence{
			Kind:        "mandatory_lane",
			ID:          lane,
			Status:      status,
			Description: "planned discovery or synthesis lane did not complete",
		})
	}

	decisions := make(map[string]ReviewFindingVerification)
	if cycle.Convergence != nil {
		for _, decision := range cycle.Convergence.FindingVerifications {
			decisions[decision.FindingID] = decision
		}
	}
	for _, finding := range cycle.CanonicalFindings {
		if finding.ExactSHA != cycle.HeadSHA {
			continue
		}
		decision, ok := decisions[finding.ID]
		if ok && decision.ExactSHA == cycle.HeadSHA &&
			(decision.Status == ReviewFindingVerificationConfirmed ||
				decision.Status == ReviewFindingVerificationRejected) {
			continue
		}
		status := "unverified"
		if ok {
			status = string(decision.Status)
		}
		evidence = append(evidence, ReviewLimitUnresolvedEvidence{
			Kind:        "candidate",
			ID:          finding.ID,
			Status:      status,
			Description: strings.Join(strings.Fields(finding.Summary), " "),
		})
	}
	for _, gap := range cycle.UnresolvedCoverage {
		evidence = append(evidence, ReviewLimitUnresolvedEvidence{
			Kind:        "coverage_gap",
			ID:          gap.ID,
			Status:      string(gap.Status),
			Description: strings.Join(strings.Fields(gap.Description), " "),
		})
	}
	if cycle.Convergence != nil {
		for _, challenge := range cycle.Convergence.ChallengeAssignments {
			if challenge.Superseded {
				continue
			}
			if challenge.Status == ReviewChallengeCompleted &&
				challenge.Outcome != ReviewChallengeInconclusive {
				continue
			}
			status := string(challenge.Status)
			if challenge.Outcome != "" {
				status += ":" + string(challenge.Outcome)
			}
			evidence = append(evidence, ReviewLimitUnresolvedEvidence{
				Kind:        string(challenge.Target.Kind),
				ID:          challenge.Target.ID,
				Status:      status,
				Description: strings.Join(strings.Fields(challenge.Target.Description), " "),
			})
		}
	}
	sort.Slice(evidence, func(i, j int) bool {
		if evidence[i].Kind != evidence[j].Kind {
			return evidence[i].Kind < evidence[j].Kind
		}
		return evidence[i].ID < evidence[j].ID
	})
	return requiredLanes, evidence
}

func evaluateReviewLaunchLimits(
	cycle *ReviewCycleState,
	snapshot ReviewMetricsSnapshot,
	now time.Time,
) (reviewLaunchLimitEvaluation, error) {
	return evaluateReviewLaunchLimitsWithAgentCount(
		cycle,
		snapshot,
		now,
		true,
	)
}

func evaluateReviewLaunchLimitsWithAgentCount(
	cycle *ReviewCycleState,
	snapshot ReviewMetricsSnapshot,
	now time.Time,
	includeAgentCount bool,
) (reviewLaunchLimitEvaluation, error) {
	if cycle == nil || now.IsZero() {
		return reviewLaunchLimitEvaluation{},
			errors.New("review launch limit evaluation requires a cycle and time")
	}
	if snapshot.SchemaVersion != reviewMetricsSchemaVersion ||
		strings.TrimSpace(snapshot.EventDigest) == "" {
		return reviewLaunchLimitEvaluation{},
			errors.New("review launch limit evaluation requires a trusted metrics snapshot")
	}
	limits := cycle.Policy.Convergence
	effectiveAgentMaximum := effectiveReviewAgentCapacity(cycle)
	action := cycle.Policy.FailureActions.BudgetExhaustion
	requiredLanes, unresolved := reviewLimitUnresolvedEvidence(cycle)
	newLimit := func(
		kind ReviewLimitKind,
		actual uint64,
		maximum uint64,
		unit string,
		reason string,
	) *ReviewLimitTransition {
		return &ReviewLimitTransition{
			SchemaVersion:      reviewLimitTransitionSchemaVersion,
			Kind:               kind,
			TransitionedAt:     now.UTC(),
			Actual:             actual,
			Maximum:            maximum,
			Unit:               unit,
			Action:             action,
			Outcome:            reviewLimitOutcomeForAction(action),
			Reason:             reason,
			MetricEventCount:   snapshot.EventCount,
			MetricEventDigest:  snapshot.EventDigest,
			RequiredLanes:      requiredLanes,
			UnresolvedEvidence: unresolved,
		}
	}

	if includeAgentCount &&
		snapshot.Cost.AgentCount >=
			uint64(effectiveAgentMaximum) {
		maximum := uint64(effectiveAgentMaximum)
		return reviewLaunchLimitEvaluation{Limit: newLimit(
			ReviewLimitAgentCount,
			snapshot.Cost.AgentCount,
			maximum,
			"agents",
			fmt.Sprintf(
				"review agent count %d reached snapshotted maximum %d",
				snapshot.Cost.AgentCount,
				maximum,
			),
		)}, nil
	}
	maxWallTime := time.Duration(limits.MaxWallTimeMinutes) * time.Minute
	elapsed := time.Duration(0)
	if !snapshot.Cost.StartedAt.IsZero() &&
		now.After(snapshot.Cost.StartedAt) {
		elapsed = now.Sub(snapshot.Cost.StartedAt)
	}
	if elapsed >= maxWallTime {
		return reviewLaunchLimitEvaluation{Limit: newLimit(
			ReviewLimitWallTime,
			uint64(elapsed.Milliseconds()),
			uint64(maxWallTime.Milliseconds()),
			"milliseconds",
			fmt.Sprintf(
				"review wall time %dms reached snapshotted maximum %dms",
				elapsed.Milliseconds(),
				maxWallTime.Milliseconds(),
			),
		)}, nil
	}
	if snapshot.Cost.Usage != nil &&
		snapshot.Cost.Usage.TotalTokens != nil &&
		*snapshot.Cost.Usage.TotalTokens >=
			uint64(limits.MaxUsageTokens) {
		maximum := uint64(limits.MaxUsageTokens)
		return reviewLaunchLimitEvaluation{Limit: newLimit(
			ReviewLimitSupportedUsage,
			*snapshot.Cost.Usage.TotalTokens,
			maximum,
			"tokens",
			fmt.Sprintf(
				"supported review usage %d tokens reached snapshotted maximum %d",
				*snapshot.Cost.Usage.TotalTokens,
				maximum,
			),
		)}, nil
	}

	trigger := ReviewEscalationTrigger("")
	if snapshot.Quality.ConvergenceRoundsCompleted >=
		uint64(cycle.Policy.Escalation.AfterNonConvergingRounds) {
		trigger = ReviewEscalationNonConvergence
	}
	if trigger == "" {
		return reviewLaunchLimitEvaluation{}, nil
	}
	return reviewLaunchLimitEvaluation{
		Escalation: &ReviewEscalationTransition{
			SchemaVersion:     reviewLimitTransitionSchemaVersion,
			Trigger:           trigger,
			TransitionedAt:    now.UTC(),
			Profile:           cycle.Policy.Escalation.Profile,
			CompletedRounds:   snapshot.Quality.ConvergenceRoundsCompleted,
			MetricEventCount:  snapshot.EventCount,
			MetricEventDigest: snapshot.EventDigest,
		},
	}, nil
}

func validateReviewLimitState(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("persisted review limit state has no cycle")
	}
	if transition := cycle.LimitTransition; transition != nil {
		if transition.SchemaVersion !=
			reviewLimitTransitionSchemaVersion ||
			transition.TransitionedAt.IsZero() ||
			transition.Actual < transition.Maximum ||
			transition.Maximum == 0 ||
			strings.TrimSpace(transition.Unit) == "" ||
			strings.TrimSpace(transition.Reason) == "" ||
			transition.Action !=
				cycle.Policy.FailureActions.BudgetExhaustion ||
			transition.Outcome !=
				reviewLimitOutcomeForAction(transition.Action) ||
			transition.MetricEventCount == 0 ||
			validateReviewPolicyFingerprint(
				transition.MetricEventDigest,
			) != nil {
			return errors.New("persisted review limit transition is invalid")
		}
		switch transition.Kind {
		case ReviewLimitAgentCount:
			if transition.Maximum != uint64(
				effectiveReviewAgentCapacity(cycle),
			) || transition.Unit != "agents" {
				return errors.New(
					"persisted review agent-count transition does not match policy",
				)
			}
		case ReviewLimitWallTime:
			maximum := time.Duration(
				cycle.Policy.Convergence.MaxWallTimeMinutes,
			) * time.Minute
			if transition.Maximum !=
				uint64(maximum.Milliseconds()) ||
				transition.Unit != "milliseconds" {
				return errors.New(
					"persisted review wall-time transition does not match policy",
				)
			}
		case ReviewLimitSupportedUsage:
			if transition.Maximum != uint64(
				cycle.Policy.Convergence.MaxUsageTokens,
			) || transition.Unit != "tokens" {
				return errors.New(
					"persisted review usage transition does not match policy",
				)
			}
		default:
			return errors.New(
				"persisted review limit kind is unsupported",
			)
		}
		for _, evidence := range transition.UnresolvedEvidence {
			if strings.TrimSpace(evidence.Kind) == "" ||
				strings.TrimSpace(evidence.ID) == "" ||
				strings.TrimSpace(evidence.Status) == "" {
				return errors.New(
					"persisted review limit evidence is invalid",
				)
			}
		}
	}
	if escalation := cycle.EscalationTransition; escalation != nil {
		if escalation.SchemaVersion !=
			reviewLimitTransitionSchemaVersion ||
			escalation.TransitionedAt.IsZero() ||
			escalation.Profile != cycle.Policy.Escalation.Profile ||
			escalation.MetricEventCount == 0 ||
			validateReviewPolicyFingerprint(
				escalation.MetricEventDigest,
			) != nil {
			return errors.New(
				"persisted review escalation transition is invalid",
			)
		}
		switch escalation.Trigger {
		case ReviewEscalationNonConvergence:
			if escalation.CompletedRounds <
				uint64(cycle.Policy.Escalation.AfterNonConvergingRounds) {
				return errors.New(
					"persisted non-convergence escalation is below its threshold",
				)
			}
		default:
			return errors.New(
				"persisted review escalation trigger is unsupported",
			)
		}
	}
	return nil
}

func (m *AgentManager) evaluateReviewLaunchLimits(
	reviewerID string,
	now time.Time,
	includeAgentCount bool,
) (
	reviewLaunchLimitEvaluation,
	reviewLaunchLimitMutation,
	error,
) {
	if m == nil {
		return reviewLaunchLimitEvaluation{},
			reviewLaunchLimitMutation{},
			errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return reviewLaunchLimitEvaluation{},
			reviewLaunchLimitMutation{},
			fmt.Errorf(
				"review coordinator %q was not found or has no review cycle",
				strings.TrimSpace(reviewerID),
			)
	}
	if agentLifecycleTerminal(reviewer) || reviewer.Paused ||
		reviewer.ReviewCycle.Stale {
		return reviewLaunchLimitEvaluation{},
			reviewLaunchLimitMutation{},
			fmt.Errorf(
				"review coordinator %q is not active for limit evaluation",
				reviewer.ID,
			)
	}
	if reviewer.ReviewCycle.LimitTransition != nil {
		return reviewLaunchLimitEvaluation{
				Limit: cloneReviewLimitTransition(
					reviewer.ReviewCycle.LimitTransition,
				),
			},
			reviewLaunchLimitMutation{},
			nil
	}
	snapshot, err := reviewer.ReviewCycle.ReviewMetricsSnapshot()
	if err != nil {
		return reviewLaunchLimitEvaluation{},
			reviewLaunchLimitMutation{},
			fmt.Errorf(
				"failed to read trusted review metrics before launch: %w",
				err,
			)
	}
	evaluation, err := evaluateReviewLaunchLimitsWithAgentCount(
		reviewer.ReviewCycle,
		snapshot,
		now,
		includeAgentCount,
	)
	if err != nil {
		return reviewLaunchLimitEvaluation{},
			reviewLaunchLimitMutation{},
			err
	}
	mutation := reviewLaunchLimitMutation{
		reviewerID: reviewer.ID,
		previousLimit: cloneReviewLimitTransition(
			reviewer.ReviewCycle.LimitTransition,
		),
		previousEscalation: cloneReviewEscalationTransition(
			reviewer.ReviewCycle.EscalationTransition,
		),
		previousLastActivityTime: reviewer.LastActivityTime,
		updatedAt:                now.UTC(),
	}
	if evaluation.Limit != nil {
		reviewer.ReviewCycle.LimitTransition =
			cloneReviewLimitTransition(evaluation.Limit)
		mutation.changed = true
	}
	if reviewer.ReviewCycle.EscalationTransition == nil &&
		evaluation.Escalation != nil {
		reviewer.ReviewCycle.EscalationTransition =
			cloneReviewEscalationTransition(evaluation.Escalation)
		mutation.changed = true
	}
	if !mutation.changed {
		return evaluation, reviewLaunchLimitMutation{}, nil
	}
	if err := validateReviewLimitState(reviewer.ReviewCycle); err != nil {
		reviewer.ReviewCycle.LimitTransition =
			mutation.previousLimit
		reviewer.ReviewCycle.EscalationTransition =
			mutation.previousEscalation
		return reviewLaunchLimitEvaluation{},
			reviewLaunchLimitMutation{},
			err
	}
	reviewer.LastActivityTime = mutation.updatedAt
	return evaluation, mutation, nil
}

func (b *Orchestrator) enforceReviewWorkerDispatchLimits(
	reviewerID string,
) error {
	b.reviewArtifactMu.Lock()
	defer b.reviewArtifactMu.Unlock()
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	evaluation, mutation, err := b.agents.evaluateReviewLaunchLimits(
		reviewerID,
		time.Now().UTC(),
		false,
	)
	if err != nil {
		return err
	}
	if !mutation.changed {
		if evaluation.Limit != nil {
			return &reviewLimitReachedError{
				transition: *evaluation.Limit,
			}
		}
		return nil
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if !b.agents.rollbackReviewLaunchLimitMutation(mutation) {
			return fmt.Errorf(
				"failed to persist pre-dispatch review limit transition and failed to roll it back: %w",
				err,
			)
		}
		return fmt.Errorf(
			"failed to persist pre-dispatch review limit transition: %w",
			err,
		)
	}
	if evaluation.Limit != nil {
		return &reviewLimitReachedError{
			transition: *evaluation.Limit,
		}
	}
	return nil
}

func (m *AgentManager) rollbackReviewLaunchLimitMutation(
	mutation reviewLaunchLimitMutation,
) bool {
	if m == nil || !mutation.changed || mutation.reviewerID == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[mutation.reviewerID]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return false
	}
	reviewer.ReviewCycle.LimitTransition =
		cloneReviewLimitTransition(mutation.previousLimit)
	reviewer.ReviewCycle.EscalationTransition =
		cloneReviewEscalationTransition(mutation.previousEscalation)
	if reviewer.LastActivityTime.Equal(mutation.updatedAt) {
		reviewer.LastActivityTime =
			mutation.previousLastActivityTime
	}
	return true
}

func reviewWorkerUsesEscalationProfile(
	cycle *ReviewCycleState,
	identity ReviewWorkerIdentity,
) bool {
	if cycle == nil || cycle.EscalationTransition == nil {
		return false
	}
	return identity.Role == AgentProfileRoleVerifier ||
		identity.Role == AgentProfileRoleChallenge
}

func effectiveProfileForReviewLaunch(
	cycle *ReviewCycleState,
	identity ReviewWorkerIdentity,
) (AgentProfile, error) {
	if cycle == nil {
		return AgentProfile{}, errors.New(
			"review worker launch has no exact-SHA cycle",
		)
	}
	if reviewWorkerUsesEscalationProfile(cycle, identity) {
		return cycle.Policy.effectiveNamedProfileForReviewWorker(
			strings.TrimSpace(cycle.EscalationTransition.Profile),
			"review escalation",
		)
	}
	if identity.Role == AgentProfileRoleDiscovery &&
		identity.Lane == reviewSynthesisLane {
		return cycle.Policy.effectiveNamedProfileForReviewWorker(
			strings.TrimSpace(cycle.Policy.RoleProfiles[AgentProfileRoleDiscovery]),
			"review synthesis",
		)
	}
	return cycle.Policy.effectiveProfileForReviewWorker(identity)
}
