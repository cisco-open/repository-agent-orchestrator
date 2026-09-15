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
	"sort"
	"strings"
	"time"
	"unicode"
)

const reviewMetricsSchemaVersion = 1

type ReviewMetricEventKind string

const (
	ReviewMetricCycleStarted        ReviewMetricEventKind = "cycle_started"
	ReviewMetricAgentAllocated      ReviewMetricEventKind = "agent_allocated"
	ReviewMetricLaneActivity        ReviewMetricEventKind = "lane_activity"
	ReviewMetricFindingProvenance   ReviewMetricEventKind = "finding_provenance"
	ReviewMetricDeduplication       ReviewMetricEventKind = "deduplication"
	ReviewMetricConvergenceRound    ReviewMetricEventKind = "convergence_round"
	ReviewMetricVerifierOutcome     ReviewMetricEventKind = "verifier_outcome"
	ReviewMetricMaterialPublished   ReviewMetricEventKind = "material_finding_published"
	ReviewMetricLateOriginalFinding ReviewMetricEventKind = "late_original_material_finding"
	ReviewMetricUsageObserved       ReviewMetricEventKind = "usage_observed"
)

type ReviewMetricActivity string

const (
	ReviewMetricActivityQueued     ReviewMetricActivity = "queued"
	ReviewMetricActivityStarted    ReviewMetricActivity = "started"
	ReviewMetricActivityCompleted  ReviewMetricActivity = "completed"
	ReviewMetricActivityFailed     ReviewMetricActivity = "failed"
	ReviewMetricActivityUnresolved ReviewMetricActivity = "unresolved"
)

// ReviewMetricUsage contains only numeric accounting fields that a configured
// runtime may authoritatively expose. A nil field means unsupported; totals are
// never inferred from other fields.
type ReviewMetricUsage struct {
	InputTokens       *uint64 `json:"input_tokens,omitempty"`
	CachedInputTokens *uint64 `json:"cached_input_tokens,omitempty"`
	OutputTokens      *uint64 `json:"output_tokens,omitempty"`
	TotalTokens       *uint64 `json:"total_tokens,omitempty"`
}

// ReviewMetricEvent deliberately contains no free-form text. Every string is
// coordinator-owned identity or a closed enum, so prompts, credentials,
// authorization data, and worker-supplied summaries have no metrics path.
type ReviewMetricEvent struct {
	ID                  string                        `json:"id"`
	Kind                ReviewMetricEventKind         `json:"kind"`
	ObservedAt          time.Time                     `json:"observed_at,omitempty"`
	HeadSHA             string                        `json:"head_sha"`
	Role                AgentProfileRole              `json:"role,omitempty"`
	Lane                string                        `json:"lane,omitempty"`
	Activity            ReviewMetricActivity          `json:"activity,omitempty"`
	Pass                int                           `json:"pass,omitempty"`
	Round               int                           `json:"round,omitempty"`
	Attempt             int                           `json:"attempt,omitempty"`
	Ordinal             int                           `json:"ordinal,omitempty"`
	WorkerID            string                        `json:"worker_id,omitempty"`
	FindingID           string                        `json:"finding_id,omitempty"`
	AssignmentID        string                        `json:"assignment_id,omitempty"`
	VerificationOutcome ReviewVerificationOutcome     `json:"verification_outcome,omitempty"`
	RoundOutcome        ReviewConvergenceRoundOutcome `json:"round_outcome,omitempty"`
	FindingProvenance   ReviewLedgerFindingProvenance `json:"finding_provenance,omitempty"`
	Usage               *ReviewMetricUsage            `json:"usage,omitempty"`
}

// ReviewMetricsState is an append-only set ordered by event ID. Namespace
// binds otherwise identical event identities to one durable coordinator or
// cross-SHA ledger.
type ReviewMetricsState struct {
	SchemaVersion int                 `json:"schema_version"`
	Namespace     string              `json:"namespace"`
	Events        []ReviewMetricEvent `json:"events"`
}

type ReviewMetricRate struct {
	Numerator   uint64 `json:"numerator"`
	Denominator uint64 `json:"denominator"`
}

type ReviewMetricsQualitySnapshot struct {
	LanesQueued                     uint64           `json:"lanes_queued"`
	LanesStarted                    uint64           `json:"lanes_started"`
	LanesCompleted                  uint64           `json:"lanes_completed"`
	LanesFailed                     uint64           `json:"lanes_failed"`
	LanesUnresolved                 uint64           `json:"lanes_unresolved"`
	FindingReports                  uint64           `json:"finding_reports"`
	UniqueCandidateFindings         uint64           `json:"unique_candidate_findings"`
	DeduplicatedFindingGroups       uint64           `json:"deduplicated_finding_groups"`
	DeduplicatedCandidateReports    uint64           `json:"deduplicated_candidate_reports"`
	DeduplicationRate               ReviewMetricRate `json:"deduplication_rate"`
	VerifierOutcomes                uint64           `json:"verifier_outcomes"`
	VerifierRejections              uint64           `json:"verifier_rejections"`
	VerifierRejectionRate           ReviewMetricRate `json:"verifier_rejection_rate"`
	ConvergenceRoundsStarted        uint64           `json:"convergence_rounds_started"`
	ConvergenceRoundsCompleted      uint64           `json:"convergence_rounds_completed"`
	PublishedMaterialFindings       uint64           `json:"published_material_findings"`
	LateOriginalMaterialFindings    uint64           `json:"late_original_material_findings"`
	LateOriginalMaterialFindingRate ReviewMetricRate `json:"late_original_material_finding_rate"`
}

type ReviewMetricsCostSnapshot struct {
	StartedAt      time.Time          `json:"started_at,omitempty"`
	LastObservedAt time.Time          `json:"last_observed_at,omitempty"`
	LatencyMillis  int64              `json:"latency_millis"`
	AgentCount     uint64             `json:"agent_count"`
	Usage          *ReviewMetricUsage `json:"usage,omitempty"`
}

// ReviewMetricsSnapshot is a detached value projection. It contains no event
// slice or mutable map, and any usage pointers are freshly allocated.
type ReviewMetricsSnapshot struct {
	SchemaVersion int                          `json:"schema_version"`
	EventCount    uint64                       `json:"event_count"`
	EventDigest   string                       `json:"event_digest"`
	Quality       ReviewMetricsQualitySnapshot `json:"quality"`
	Cost          ReviewMetricsCostSnapshot    `json:"cost"`
}

func cloneReviewMetricUsage(usage *ReviewMetricUsage) *ReviewMetricUsage {
	if usage == nil {
		return nil
	}
	clone := &ReviewMetricUsage{}
	if usage.InputTokens != nil {
		value := *usage.InputTokens
		clone.InputTokens = &value
	}
	if usage.CachedInputTokens != nil {
		value := *usage.CachedInputTokens
		clone.CachedInputTokens = &value
	}
	if usage.OutputTokens != nil {
		value := *usage.OutputTokens
		clone.OutputTokens = &value
	}
	if usage.TotalTokens != nil {
		value := *usage.TotalTokens
		clone.TotalTokens = &value
	}
	return clone
}

func cloneReviewMetricsState(state *ReviewMetricsState) *ReviewMetricsState {
	if state == nil {
		return nil
	}
	clone := &ReviewMetricsState{
		SchemaVersion: state.SchemaVersion,
		Namespace:     state.Namespace,
		Events:        make([]ReviewMetricEvent, len(state.Events)),
	}
	copy(clone.Events, state.Events)
	for index := range clone.Events {
		clone.Events[index].Usage =
			cloneReviewMetricUsage(clone.Events[index].Usage)
	}
	return clone
}

func supportedReviewMetricEventKind(kind ReviewMetricEventKind) bool {
	switch kind {
	case ReviewMetricCycleStarted,
		ReviewMetricAgentAllocated,
		ReviewMetricLaneActivity,
		ReviewMetricFindingProvenance,
		ReviewMetricDeduplication,
		ReviewMetricConvergenceRound,
		ReviewMetricVerifierOutcome,
		ReviewMetricMaterialPublished,
		ReviewMetricLateOriginalFinding,
		ReviewMetricUsageObserved:
		return true
	default:
		return false
	}
}

func supportedReviewMetricActivity(activity ReviewMetricActivity) bool {
	switch activity {
	case ReviewMetricActivityQueued,
		ReviewMetricActivityStarted,
		ReviewMetricActivityCompleted,
		ReviewMetricActivityFailed,
		ReviewMetricActivityUnresolved:
		return true
	default:
		return false
	}
}

func supportedReviewMetricRoundOutcome(
	outcome ReviewConvergenceRoundOutcome,
) bool {
	switch outcome {
	case ReviewConvergenceRoundQuiet,
		ReviewConvergenceRoundMaterial,
		ReviewConvergenceRoundChangesRequired,
		ReviewConvergenceRoundUnresolved,
		ReviewConvergenceRoundMaxRounds:
		return true
	default:
		return false
	}
}

func reviewMetricSafeIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 512 {
		return false
	}
	for _, char := range value {
		if unicode.IsLetter(char) || unicode.IsDigit(char) ||
			strings.ContainsRune("-_.:", char) {
			continue
		}
		return false
	}
	return true
}

func validReviewMetricFindingID(findingID string) bool {
	const prefix = "finding-"
	if !strings.HasPrefix(findingID, prefix) {
		return false
	}
	digest := strings.TrimPrefix(findingID, prefix)
	if len(digest) != sha256.Size*2 || strings.ToLower(digest) != digest {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func reviewMetricUsageIsEmpty(usage *ReviewMetricUsage) bool {
	return usage == nil ||
		(usage.InputTokens == nil &&
			usage.CachedInputTokens == nil &&
			usage.OutputTokens == nil &&
			usage.TotalTokens == nil)
}

func validateReviewMetricEvent(event ReviewMetricEvent) error {
	if !supportedReviewMetricEventKind(event.Kind) {
		return fmt.Errorf("review metric event kind %q is unsupported", event.Kind)
	}
	if err := validateCanonicalGitObjectID(event.HeadSHA); err != nil {
		return fmt.Errorf("review metric event head SHA is invalid: %w", err)
	}
	requireObservedAt := func() error {
		if event.ObservedAt.IsZero() {
			return errors.New("review metric event observation time is missing")
		}
		return nil
	}
	requireLane := func() error {
		identity := ReviewWorkerIdentity{
			CycleID:  "review-metric-validation",
			Revision: 1,
			Role:     event.Role,
			Pass:     event.Pass,
			Lane:     event.Lane,
		}
		return validateReviewWorkerIdentity(identity)
	}
	requireFinding := func() error {
		if !validReviewMetricFindingID(event.FindingID) {
			return errors.New("review metric finding identity is invalid")
		}
		return nil
	}

	switch event.Kind {
	case ReviewMetricCycleStarted:
		return requireObservedAt()
	case ReviewMetricAgentAllocated:
		if event.WorkerID == "" || event.Attempt <= 0 ||
			!reviewMetricSafeIdentifier(event.WorkerID) {
			return errors.New("review metric agent allocation is invalid")
		}
		if err := requireLane(); err != nil {
			return err
		}
		return requireObservedAt()
	case ReviewMetricLaneActivity:
		if !supportedReviewMetricActivity(event.Activity) {
			return errors.New("review metric lane activity is invalid")
		}
		if err := requireLane(); err != nil {
			return err
		}
		if event.AssignmentID != "" &&
			!reviewMetricSafeIdentifier(event.AssignmentID) {
			return errors.New("review metric lane assignment identity is invalid")
		}
		return requireObservedAt()
	case ReviewMetricFindingProvenance:
		if err := requireFinding(); err != nil {
			return err
		}
		if event.WorkerID == "" || event.Ordinal <= 0 ||
			!reviewMetricSafeIdentifier(event.WorkerID) {
			return errors.New("review metric finding provenance is invalid")
		}
		if err := requireLane(); err != nil {
			return err
		}
		return requireObservedAt()
	case ReviewMetricDeduplication:
		if err := requireFinding(); err != nil {
			return err
		}
		return requireObservedAt()
	case ReviewMetricConvergenceRound:
		if event.Round <= 0 ||
			(event.Activity != ReviewMetricActivityStarted &&
				event.Activity != ReviewMetricActivityCompleted) {
			return errors.New("review metric convergence round is invalid")
		}
		if event.Activity == ReviewMetricActivityCompleted &&
			!supportedReviewMetricRoundOutcome(event.RoundOutcome) {
			return errors.New("review metric completed round outcome is invalid")
		}
		return requireObservedAt()
	case ReviewMetricVerifierOutcome:
		if event.Role != AgentProfileRoleVerifier ||
			event.Round <= 0 ||
			event.WorkerID == "" ||
			!reviewMetricSafeIdentifier(event.WorkerID) ||
			event.AssignmentID == "" ||
			!reviewMetricSafeIdentifier(event.AssignmentID) {
			return errors.New("review metric verifier identity is invalid")
		}
		if err := requireFinding(); err != nil {
			return err
		}
		switch event.VerificationOutcome {
		case ReviewVerificationConfirmed,
			ReviewVerificationRejected,
			ReviewVerificationInconclusive:
		default:
			return errors.New("review metric verifier outcome is invalid")
		}
		return requireObservedAt()
	case ReviewMetricMaterialPublished:
		if err := requireFinding(); err != nil {
			return err
		}
		if !supportedReviewLedgerFindingProvenance(
			event.FindingProvenance,
		) {
			return errors.New("review metric finding provenance class is invalid")
		}
		return nil
	case ReviewMetricLateOriginalFinding:
		if err := requireFinding(); err != nil {
			return err
		}
		if event.FindingProvenance !=
			ReviewLedgerFindingPreviouslyMissed {
			return errors.New("review metric late-original provenance is invalid")
		}
		return nil
	case ReviewMetricUsageObserved:
		if event.WorkerID == "" || event.Attempt <= 0 ||
			!reviewMetricSafeIdentifier(event.WorkerID) ||
			reviewMetricUsageIsEmpty(event.Usage) {
			return errors.New("review metric usage observation is invalid")
		}
		if err := requireLane(); err != nil {
			return err
		}
		return requireObservedAt()
	default:
		return errors.New("review metric event is unsupported")
	}
}

func reviewMetricEventID(
	namespace string,
	event ReviewMetricEvent,
) (string, error) {
	if !reviewMetricSafeIdentifier(namespace) {
		return "", errors.New("review metrics namespace is missing or unsafe")
	}
	if err := validateReviewMetricEvent(event); err != nil {
		return "", err
	}
	identity := struct {
		Namespace    string                `json:"namespace"`
		Kind         ReviewMetricEventKind `json:"kind"`
		HeadSHA      string                `json:"head_sha"`
		Role         AgentProfileRole      `json:"role,omitempty"`
		Lane         string                `json:"lane,omitempty"`
		Activity     ReviewMetricActivity  `json:"activity,omitempty"`
		Pass         int                   `json:"pass,omitempty"`
		Round        int                   `json:"round,omitempty"`
		Attempt      int                   `json:"attempt,omitempty"`
		Ordinal      int                   `json:"ordinal,omitempty"`
		WorkerID     string                `json:"worker_id,omitempty"`
		FindingID    string                `json:"finding_id,omitempty"`
		AssignmentID string                `json:"assignment_id,omitempty"`
	}{
		Namespace:    namespace,
		Kind:         event.Kind,
		HeadSHA:      event.HeadSHA,
		Role:         event.Role,
		Lane:         event.Lane,
		Activity:     event.Activity,
		Pass:         event.Pass,
		Round:        event.Round,
		Attempt:      event.Attempt,
		Ordinal:      event.Ordinal,
		WorkerID:     event.WorkerID,
		FindingID:    event.FindingID,
		AssignmentID: event.AssignmentID,
	}
	body, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("failed to encode review metric identity: %w", err)
	}
	sum := sha256.Sum256(body)
	return "review-metric-" + hex.EncodeToString(sum[:]), nil
}

// recordTrustedReviewMetricEvent is replay-safe: the first coordinator
// observation for one identity wins, and replay returns added=false.
func recordTrustedReviewMetricEvent(
	state *ReviewMetricsState,
	event ReviewMetricEvent,
) (bool, error) {
	if state == nil {
		return false, errors.New("review metrics state is missing")
	}
	if state.SchemaVersion != reviewMetricsSchemaVersion {
		return false, fmt.Errorf(
			"review metrics schema version %d is unsupported",
			state.SchemaVersion,
		)
	}
	event.ObservedAt = event.ObservedAt.UTC()
	event.Usage = cloneReviewMetricUsage(event.Usage)
	eventID, err := reviewMetricEventID(state.Namespace, event)
	if err != nil {
		return false, err
	}
	if event.ID != "" && event.ID != eventID {
		return false, errors.New(
			"review metric event identity is not coordinator-derived",
		)
	}
	event.ID = eventID
	index := sort.Search(len(state.Events), func(index int) bool {
		return state.Events[index].ID >= event.ID
	})
	if index < len(state.Events) && state.Events[index].ID == event.ID {
		return false, nil
	}
	state.Events = append(state.Events, ReviewMetricEvent{})
	copy(state.Events[index+1:], state.Events[index:])
	state.Events[index] = event
	return true, nil
}

func validateReviewMetricsState(
	cycle *ReviewCycleState,
	state *ReviewMetricsState,
) error {
	if state == nil {
		return nil
	}
	if cycle == nil {
		return errors.New("persisted review metrics have no review cycle")
	}
	if state.SchemaVersion != reviewMetricsSchemaVersion ||
		state.Namespace != cycle.ID {
		return errors.New("persisted review metrics identity is invalid")
	}
	for index, event := range state.Events {
		if event.HeadSHA != cycle.HeadSHA {
			return errors.New("persisted review metric event head SHA mismatch")
		}
		expectedID, err := reviewMetricEventID(state.Namespace, event)
		if err != nil {
			return fmt.Errorf("persisted review metric event is invalid: %w", err)
		}
		if event.ID != expectedID ||
			(index > 0 && state.Events[index-1].ID >= event.ID) {
			return errors.New(
				"persisted review metric event identity or order is invalid",
			)
		}
	}
	return nil
}

func reviewMetricsStateForCycle(
	cycle *ReviewCycleState,
) (*ReviewMetricsState, error) {
	if cycle == nil {
		return nil, errors.New("review cycle is missing")
	}
	state := cloneReviewMetricsState(cycle.Metrics)
	if state == nil {
		state = &ReviewMetricsState{
			SchemaVersion: reviewMetricsSchemaVersion,
			Namespace:     cycle.ID,
			Events:        []ReviewMetricEvent{},
		}
	}
	if state.SchemaVersion != reviewMetricsSchemaVersion ||
		state.Namespace != cycle.ID {
		return nil, errors.New("review metrics state does not match its cycle")
	}
	add := func(event ReviewMetricEvent) error {
		_, err := recordTrustedReviewMetricEvent(state, event)
		return err
	}
	startedAt := reviewCycleMetricsStartedAt(cycle)
	if !startedAt.IsZero() {
		if err := add(ReviewMetricEvent{
			Kind:       ReviewMetricCycleStarted,
			ObservedAt: startedAt,
			HeadSHA:    cycle.HeadSHA,
		}); err != nil {
			return nil, err
		}
	}
	for _, ownership := range cycle.WorkerOwnerships {
		if err := add(ReviewMetricEvent{
			Kind:       ReviewMetricAgentAllocated,
			ObservedAt: ownership.AllocatedAt,
			HeadSHA:    cycle.HeadSHA,
			Role:       ownership.Identity.Role,
			Lane:       ownership.Identity.Lane,
			Pass:       ownership.Identity.Pass,
			Attempt:    ownership.Attempt,
			WorkerID:   ownership.OwnerID,
		}); err != nil {
			return nil, err
		}
	}
	for _, pass := range cycle.DiscoveryPasses {
		for _, lane := range pass.Lanes {
			events := []ReviewMetricEvent{{
				Kind:       ReviewMetricLaneActivity,
				ObservedAt: lane.QueuedAt,
				HeadSHA:    cycle.HeadSHA,
				Role:       AgentProfileRoleDiscovery,
				Lane:       lane.Lane,
				Pass:       pass.Pass,
				Activity:   ReviewMetricActivityQueued,
			}}
			if !lane.StartedAt.IsZero() {
				events = append(events, ReviewMetricEvent{
					Kind:       ReviewMetricLaneActivity,
					ObservedAt: lane.StartedAt,
					HeadSHA:    cycle.HeadSHA,
					Role:       AgentProfileRoleDiscovery,
					Lane:       lane.Lane,
					Pass:       pass.Pass,
					Activity:   ReviewMetricActivityStarted,
				})
			}
			if !lane.CompletedAt.IsZero() {
				activity := ReviewMetricActivityCompleted
				if lane.Status == ReviewDiscoveryLaneFailed {
					activity = ReviewMetricActivityFailed
				}
				events = append(events, ReviewMetricEvent{
					Kind:       ReviewMetricLaneActivity,
					ObservedAt: lane.CompletedAt,
					HeadSHA:    cycle.HeadSHA,
					Role:       AgentProfileRoleDiscovery,
					Lane:       lane.Lane,
					Pass:       pass.Pass,
					Activity:   activity,
				})
			}
			for _, event := range events {
				if err := add(event); err != nil {
					return nil, err
				}
			}
		}
	}
	receiptTimes := make(map[string]time.Time, len(cycle.ArtifactReceipts))
	for _, receipt := range cycle.ArtifactReceipts {
		key := reviewMetricProvenanceSourceKey(
			receipt.WorkerID,
			receipt.Lane,
			receipt.Pass,
		)
		receiptTimes[key] = receipt.AcceptedAt
	}
	for _, finding := range cycle.CanonicalFindings {
		ordinals := make(map[string]int)
		latest := time.Time{}
		currentCycleReports := 0
		for _, provenance := range finding.Provenance {
			key := reviewMetricProvenanceSourceKey(
				provenance.WorkerID,
				provenance.Lane,
				provenance.Pass,
			)
			observedAt, observedThisCycle := receiptTimes[key]
			if !observedThisCycle {
				// Provenance carried forward by reviewLedgerSeedFindings
				// from a prior review cycle's ledger never matches this
				// cycle's ArtifactReceipts. Metrics stay cycle-local:
				// skip it here rather than inventing or reusing a stale
				// cross-cycle timestamp, which would corrupt this
				// cycle's own StartedAt/latency accounting. It resumes
				// contributing once a current-cycle worker re-observes
				// the finding.
				continue
			}
			ordinals[key]++
			currentCycleReports++
			if observedAt.After(latest) {
				latest = observedAt
			}
			if err := add(ReviewMetricEvent{
				Kind:       ReviewMetricFindingProvenance,
				ObservedAt: observedAt,
				HeadSHA:    cycle.HeadSHA,
				Role:       reviewMetricProvenanceRole(cycle, provenance),
				Lane:       provenance.Lane,
				Pass:       provenance.Pass,
				Ordinal:    ordinals[key],
				WorkerID:   provenance.WorkerID,
				FindingID:  finding.ID,
			}); err != nil {
				return nil, err
			}
		}
		if currentCycleReports > 1 {
			if err := add(ReviewMetricEvent{
				Kind:       ReviewMetricDeduplication,
				ObservedAt: latest,
				HeadSHA:    cycle.HeadSHA,
				FindingID:  finding.ID,
			}); err != nil {
				return nil, err
			}
		}
	}
	if cycle.Convergence != nil {
		if err := addReviewConvergenceMetricEvents(state, cycle); err != nil {
			return nil, err
		}
	}
	if err := validateReviewMetricsState(cycle, state); err != nil {
		return nil, err
	}
	return state, nil
}

func reviewMetricProvenanceSourceKey(
	workerID string,
	lane string,
	pass int,
) string {
	return fmt.Sprintf("%s\x00%s\x00%d", workerID, lane, pass)
}

func reviewMetricProvenanceRole(
	cycle *ReviewCycleState,
	provenance ReviewFindingProvenance,
) AgentProfileRole {
	for _, ownership := range cycle.WorkerOwnerships {
		if ownership.OwnerID == provenance.WorkerID {
			return ownership.Identity.Role
		}
	}
	if strings.HasPrefix(provenance.Lane, "challenge-") {
		return AgentProfileRoleChallenge
	}
	return AgentProfileRoleDiscovery
}

func addReviewConvergenceMetricEvents(
	state *ReviewMetricsState,
	cycle *ReviewCycleState,
) error {
	add := func(event ReviewMetricEvent) error {
		_, err := recordTrustedReviewMetricEvent(state, event)
		return err
	}
	for _, assignment := range cycle.Convergence.VerificationAssignments {
		events := reviewMetricAssignmentActivityEvents(
			cycle.HeadSHA,
			AgentProfileRoleVerifier,
			assignment.Lane,
			assignment.Round,
			assignment.ID,
			assignment.Status,
			assignment.QueuedAt,
			assignment.StartedAt,
			assignment.CompletedAt,
		)
		for _, event := range events {
			if err := add(event); err != nil {
				return err
			}
		}
		if assignment.Status == ReviewVerificationCompleted {
			if err := add(ReviewMetricEvent{
				Kind:                ReviewMetricVerifierOutcome,
				ObservedAt:          assignment.CompletedAt,
				HeadSHA:             cycle.HeadSHA,
				Role:                AgentProfileRoleVerifier,
				Round:               assignment.Round,
				WorkerID:            assignment.WorkerID,
				FindingID:           assignment.FindingID,
				AssignmentID:        assignment.ID,
				VerificationOutcome: assignment.Outcome,
			}); err != nil {
				return err
			}
		}
	}
	for _, assignment := range cycle.Convergence.ChallengeAssignments {
		events := reviewMetricAssignmentActivityEvents(
			cycle.HeadSHA,
			AgentProfileRoleChallenge,
			assignment.Lane,
			assignment.Round,
			assignment.ID,
			assignment.Status,
			assignment.QueuedAt,
			assignment.StartedAt,
			assignment.CompletedAt,
		)
		for _, event := range events {
			if err := add(event); err != nil {
				return err
			}
		}
	}
	for _, round := range cycle.Convergence.Rounds {
		if err := add(ReviewMetricEvent{
			Kind:       ReviewMetricConvergenceRound,
			ObservedAt: round.StartedAt,
			HeadSHA:    cycle.HeadSHA,
			Activity:   ReviewMetricActivityStarted,
			Round:      round.Round,
		}); err != nil {
			return err
		}
		if !round.CompletedAt.IsZero() {
			if err := add(ReviewMetricEvent{
				Kind:         ReviewMetricConvergenceRound,
				ObservedAt:   round.CompletedAt,
				HeadSHA:      cycle.HeadSHA,
				Activity:     ReviewMetricActivityCompleted,
				Round:        round.Round,
				RoundOutcome: round.Outcome,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func reviewMetricAssignmentActivityEvents(
	headSHA string,
	role AgentProfileRole,
	lane string,
	pass int,
	assignmentID string,
	status any,
	queuedAt time.Time,
	startedAt time.Time,
	completedAt time.Time,
) []ReviewMetricEvent {
	events := []ReviewMetricEvent{{
		Kind:         ReviewMetricLaneActivity,
		ObservedAt:   queuedAt,
		HeadSHA:      headSHA,
		Role:         role,
		Lane:         lane,
		Pass:         pass,
		Activity:     ReviewMetricActivityQueued,
		AssignmentID: assignmentID,
	}}
	if !startedAt.IsZero() {
		events = append(events, ReviewMetricEvent{
			Kind:         ReviewMetricLaneActivity,
			ObservedAt:   startedAt,
			HeadSHA:      headSHA,
			Role:         role,
			Lane:         lane,
			Pass:         pass,
			Activity:     ReviewMetricActivityStarted,
			AssignmentID: assignmentID,
		})
	}
	if !completedAt.IsZero() {
		activity := ReviewMetricActivityCompleted
		switch status {
		case ReviewVerificationFailed, ReviewChallengeFailed:
			activity = ReviewMetricActivityFailed
		}
		events = append(events, ReviewMetricEvent{
			Kind:         ReviewMetricLaneActivity,
			ObservedAt:   completedAt,
			HeadSHA:      headSHA,
			Role:         role,
			Lane:         lane,
			Pass:         pass,
			Activity:     activity,
			AssignmentID: assignmentID,
		})
	}
	return events
}

func reviewCycleMetricsStartedAt(cycle *ReviewCycleState) time.Time {
	if cycle == nil {
		return time.Time{}
	}
	if cycle.Metrics != nil {
		for _, event := range cycle.Metrics.Events {
			if event.Kind == ReviewMetricCycleStarted {
				return event.ObservedAt.UTC()
			}
		}
	}
	earliest := time.Time{}
	observe := func(value time.Time) {
		if value.IsZero() {
			return
		}
		value = value.UTC()
		if earliest.IsZero() || value.Before(earliest) {
			earliest = value
		}
	}
	for _, ownership := range cycle.WorkerOwnerships {
		observe(ownership.AllocatedAt)
	}
	for _, receipt := range cycle.ArtifactReceipts {
		observe(receipt.AcceptedAt)
	}
	for _, pass := range cycle.DiscoveryPasses {
		observe(pass.QueuedAt)
		for _, lane := range pass.Lanes {
			observe(lane.QueuedAt)
			observe(lane.StartedAt)
			observe(lane.CompletedAt)
		}
	}
	if cycle.Convergence != nil {
		for _, assignment := range cycle.Convergence.VerificationAssignments {
			observe(assignment.QueuedAt)
			observe(assignment.StartedAt)
			observe(assignment.CompletedAt)
		}
		for _, assignment := range cycle.Convergence.ChallengeAssignments {
			observe(assignment.QueuedAt)
			observe(assignment.StartedAt)
			observe(assignment.CompletedAt)
		}
		for _, round := range cycle.Convergence.Rounds {
			observe(round.StartedAt)
			observe(round.CompletedAt)
		}
	}
	return earliest
}

func newReviewMetricsState(
	cycle *ReviewCycleState,
	startedAt time.Time,
) (*ReviewMetricsState, error) {
	if cycle == nil || startedAt.IsZero() {
		return nil, errors.New("review metrics cycle and start time are required")
	}
	state := &ReviewMetricsState{
		SchemaVersion: reviewMetricsSchemaVersion,
		Namespace:     cycle.ID,
		Events:        []ReviewMetricEvent{},
	}
	if _, err := recordTrustedReviewMetricEvent(state, ReviewMetricEvent{
		Kind:       ReviewMetricCycleStarted,
		ObservedAt: startedAt.UTC(),
		HeadSHA:    cycle.HeadSHA,
	}); err != nil {
		return nil, err
	}
	return state, nil
}

// recordTrustedReviewUsage accepts accounting only for a coordinator-owned
// worker attempt. Current tmux RuntimeHandle values expose no usage, so no
// production caller records this event today.
func recordTrustedReviewUsage(
	cycle *ReviewCycleState,
	workerID string,
	attempt int,
	observedAt time.Time,
	usage ReviewMetricUsage,
) (bool, error) {
	if cycle == nil {
		return false, errors.New("review cycle is missing")
	}
	var ownership *ReviewWorkerOwnership
	for index := range cycle.WorkerOwnerships {
		candidate := &cycle.WorkerOwnerships[index]
		if candidate.OwnerID == workerID &&
			candidate.Attempt == attempt {
			ownership = candidate
			break
		}
	}
	if ownership == nil {
		return false, errors.New(
			"review usage does not belong to a coordinator-owned worker attempt",
		)
	}
	state, err := reviewMetricsStateForCycle(cycle)
	if err != nil {
		return false, err
	}
	added, err := recordTrustedReviewMetricEvent(
		state,
		ReviewMetricEvent{
			Kind:       ReviewMetricUsageObserved,
			ObservedAt: observedAt.UTC(),
			HeadSHA:    cycle.HeadSHA,
			Role:       ownership.Identity.Role,
			Lane:       ownership.Identity.Lane,
			Pass:       ownership.Identity.Pass,
			Attempt:    ownership.Attempt,
			WorkerID:   ownership.OwnerID,
			Usage:      &usage,
		},
	)
	if err != nil {
		return false, err
	}
	cycle.Metrics = state
	return added, nil
}

func reviewLedgerMetricEvents(
	ledger *ReviewLedger,
) ([]ReviewMetricEvent, error) {
	if ledger == nil {
		return nil, nil
	}
	if err := validateReviewLedger(ledger); err != nil {
		return nil, err
	}
	state := &ReviewMetricsState{
		SchemaVersion: reviewMetricsSchemaVersion,
		Namespace:     "review-ledger-" + ledger.BaseSHA,
		Events:        []ReviewMetricEvent{},
	}
	for _, finding := range ledger.Findings {
		for _, transition := range finding.History {
			if transition.ToStatus != ReviewLedgerFindingReported &&
				transition.ToStatus != ReviewLedgerFindingMissed {
				continue
			}
			event := ReviewMetricEvent{
				Kind:              ReviewMetricMaterialPublished,
				HeadSHA:           transition.SourceSHA,
				FindingID:         transition.ToFindingID,
				FindingProvenance: transition.ToProvenance,
			}
			if _, err := recordTrustedReviewMetricEvent(state, event); err != nil {
				return nil, err
			}
			if transition.ToProvenance ==
				ReviewLedgerFindingPreviouslyMissed {
				event.Kind = ReviewMetricLateOriginalFinding
				if _, err := recordTrustedReviewMetricEvent(
					state,
					event,
				); err != nil {
					return nil, err
				}
			}
			break
		}
	}
	return state.Events, nil
}

func addReviewMetricUsageValue(total **uint64, value *uint64) {
	if value == nil {
		return
	}
	if *total == nil {
		initial := uint64(0)
		*total = &initial
	}
	**total += *value
}

func aggregateReviewMetricEvents(
	events []ReviewMetricEvent,
) (ReviewMetricsSnapshot, error) {
	unique := make(map[string]ReviewMetricEvent, len(events))
	for _, event := range events {
		if event.ID == "" {
			return ReviewMetricsSnapshot{},
				errors.New("review metric event identity is missing")
		}
		if _, exists := unique[event.ID]; !exists {
			unique[event.ID] = event
		}
	}
	ordered := make([]ReviewMetricEvent, 0, len(unique))
	for _, event := range unique {
		event.Usage = cloneReviewMetricUsage(event.Usage)
		ordered = append(ordered, event)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID < ordered[j].ID
	})
	body, err := json.Marshal(ordered)
	if err != nil {
		return ReviewMetricsSnapshot{}, fmt.Errorf(
			"failed to encode review metric events: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	snapshot := ReviewMetricsSnapshot{
		SchemaVersion: reviewMetricsSchemaVersion,
		EventCount:    uint64(len(ordered)),
		EventDigest:   hex.EncodeToString(sum[:]),
	}
	findings := make(map[string]struct{})
	usage := &ReviewMetricUsage{}
	for _, event := range ordered {
		if !event.ObservedAt.IsZero() {
			if snapshot.Cost.StartedAt.IsZero() ||
				event.ObservedAt.Before(snapshot.Cost.StartedAt) {
				snapshot.Cost.StartedAt = event.ObservedAt
			}
			if event.ObservedAt.After(snapshot.Cost.LastObservedAt) {
				snapshot.Cost.LastObservedAt = event.ObservedAt
			}
		}
		switch event.Kind {
		case ReviewMetricAgentAllocated:
			snapshot.Cost.AgentCount++
		case ReviewMetricLaneActivity:
			switch event.Activity {
			case ReviewMetricActivityQueued:
				snapshot.Quality.LanesQueued++
			case ReviewMetricActivityStarted:
				snapshot.Quality.LanesStarted++
			case ReviewMetricActivityCompleted:
				snapshot.Quality.LanesCompleted++
			case ReviewMetricActivityFailed:
				snapshot.Quality.LanesFailed++
			case ReviewMetricActivityUnresolved:
				snapshot.Quality.LanesUnresolved++
			}
		case ReviewMetricFindingProvenance:
			snapshot.Quality.FindingReports++
			findings[event.FindingID] = struct{}{}
		case ReviewMetricDeduplication:
			snapshot.Quality.DeduplicatedFindingGroups++
		case ReviewMetricConvergenceRound:
			if event.Activity == ReviewMetricActivityStarted {
				snapshot.Quality.ConvergenceRoundsStarted++
			} else {
				snapshot.Quality.ConvergenceRoundsCompleted++
			}
		case ReviewMetricVerifierOutcome:
			snapshot.Quality.VerifierOutcomes++
			if event.VerificationOutcome == ReviewVerificationRejected {
				snapshot.Quality.VerifierRejections++
			}
		case ReviewMetricMaterialPublished:
			snapshot.Quality.PublishedMaterialFindings++
		case ReviewMetricLateOriginalFinding:
			snapshot.Quality.LateOriginalMaterialFindings++
		case ReviewMetricUsageObserved:
			addReviewMetricUsageValue(
				&usage.InputTokens,
				event.Usage.InputTokens,
			)
			addReviewMetricUsageValue(
				&usage.CachedInputTokens,
				event.Usage.CachedInputTokens,
			)
			addReviewMetricUsageValue(
				&usage.OutputTokens,
				event.Usage.OutputTokens,
			)
			addReviewMetricUsageValue(
				&usage.TotalTokens,
				event.Usage.TotalTokens,
			)
		}
	}
	snapshot.Quality.UniqueCandidateFindings = uint64(len(findings))
	if snapshot.Quality.FindingReports >
		snapshot.Quality.UniqueCandidateFindings {
		snapshot.Quality.DeduplicatedCandidateReports =
			snapshot.Quality.FindingReports -
				snapshot.Quality.UniqueCandidateFindings
	}
	snapshot.Quality.DeduplicationRate = ReviewMetricRate{
		Numerator:   snapshot.Quality.DeduplicatedCandidateReports,
		Denominator: snapshot.Quality.FindingReports,
	}
	snapshot.Quality.VerifierRejectionRate = ReviewMetricRate{
		Numerator:   snapshot.Quality.VerifierRejections,
		Denominator: snapshot.Quality.VerifierOutcomes,
	}
	snapshot.Quality.LateOriginalMaterialFindingRate = ReviewMetricRate{
		Numerator:   snapshot.Quality.LateOriginalMaterialFindings,
		Denominator: snapshot.Quality.PublishedMaterialFindings,
	}
	if !snapshot.Cost.StartedAt.IsZero() &&
		!snapshot.Cost.LastObservedAt.Before(snapshot.Cost.StartedAt) {
		snapshot.Cost.LatencyMillis =
			snapshot.Cost.LastObservedAt.
				Sub(snapshot.Cost.StartedAt).
				Milliseconds()
	}
	if !reviewMetricUsageIsEmpty(usage) {
		snapshot.Cost.Usage = usage
	}
	return snapshot, nil
}

// ReviewMetricsSnapshot returns the one detached exact-SHA snapshot consumed
// by budget decisions.
func (cycle *ReviewCycleState) ReviewMetricsSnapshot() (
	ReviewMetricsSnapshot,
	error,
) {
	return reviewMetricsSnapshotWithLedger(cycle, nil)
}

// reviewMetricsSnapshotWithLedger is the evaluation projection that adds
// cross-SHA publication provenance and late-original findings.
func reviewMetricsSnapshotWithLedger(
	cycle *ReviewCycleState,
	ledger *ReviewLedger,
) (ReviewMetricsSnapshot, error) {
	state, err := reviewMetricsStateForCycle(cycle)
	if err != nil {
		return ReviewMetricsSnapshot{}, err
	}
	events := append([]ReviewMetricEvent(nil), state.Events...)
	ledgerEvents, err := reviewLedgerMetricEvents(ledger)
	if err != nil {
		return ReviewMetricsSnapshot{}, err
	}
	events = append(events, ledgerEvents...)
	return aggregateReviewMetricEvents(events)
}
