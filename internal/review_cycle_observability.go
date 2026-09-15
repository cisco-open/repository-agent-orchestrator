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
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

type reviewWorkerCounts struct {
	Planned                     int `json:"planned"`
	Reserved                    int `json:"reserved"`
	Running                     int `json:"running"`
	Completed                   int `json:"completed"`
	Failed                      int `json:"failed"`
	Cancelled                   int `json:"cancelled"`
	MaxParallelObserved         int `json:"max_parallel_observed"`
	MaxReviewerParallelObserved int `json:"max_reviewer_parallel_observed"`
	MaxVerifierParallelObserved int `json:"max_verifier_parallel_observed"`
}

type reviewCycleBounds struct {
	MinReviewers              int `json:"min_reviewers"`
	MaxReviewers              int `json:"max_reviewers"`
	MaxParallelReviewers      int `json:"max_parallel_reviewers"`
	MinVerifiers              int `json:"min_verifiers"`
	MaxVerifiers              int `json:"max_verifiers"`
	MaxParallelVerifiers      int `json:"max_parallel_verifiers"`
	ConfiguredMaxAgentsPerSHA int `json:"configured_max_agents_per_sha"`
	RequiredMaxAgentsPerSHA   int `json:"required_max_agents_per_sha"`
	MaxAgentsPerSHA           int `json:"max_agents_per_sha"`
}

type reviewWorkerObservation struct {
	LogicalAssignment    string                     `json:"logical_assignment"`
	Role                 AgentProfileRole           `json:"role"`
	Pass                 int                        `json:"pass"`
	Lane                 string                     `json:"lane"`
	Profile              string                     `json:"profile"`
	Model                string                     `json:"model"`
	ReasoningEffort      string                     `json:"reasoning_effort"`
	Attempt              int                        `json:"attempt"`
	Lifecycle            ReviewWorkerLifecycleState `json:"lifecycle"`
	ReservedAt           time.Time                  `json:"reserved_at"`
	StartedAt            time.Time                  `json:"started_at,omitempty"`
	FinishedAt           time.Time                  `json:"finished_at,omitempty"`
	WorktreeSetupFailure string                     `json:"worktree_setup_failure,omitempty"`
}

type reviewCyclePublicationObservation struct {
	State     string `json:"state"`
	Identity  string `json:"identity,omitempty"`
	CommentID int64  `json:"comment_id,omitempty"`
}

type reviewCycleObservation struct {
	CycleID             string                            `json:"cycle_id"`
	Revision            int                               `json:"revision"`
	Attempt             int                               `json:"attempt"`
	ReviewerID          string                            `json:"reviewer_id"`
	PRNumber            int                               `json:"pr_number"`
	HeadSHA             string                            `json:"head_sha"`
	PolicyFingerprint   string                            `json:"policy_fingerprint"`
	Bounds              reviewCycleBounds                 `json:"bounds"`
	Counts              reviewWorkerCounts                `json:"counts"`
	WithinBounds        bool                              `json:"within_bounds"`
	ResultState         ReviewCycleResultState            `json:"result_state,omitempty"`
	Publication         reviewCyclePublicationObservation `json:"publication"`
	Workers             []reviewWorkerObservation         `json:"workers"`
	SupersededByHeadSHA string                            `json:"superseded_by_head_sha,omitempty"`
}

type reviewCycleLogRecord struct {
	Timestamp time.Time              `json:"timestamp"`
	Event     string                 `json:"event"`
	Cycle     reviewCycleObservation `json:"review_cycle"`
}

func reviewWorkerLogicalAssignment(identity ReviewWorkerIdentity) string {
	return fmt.Sprintf(
		"%s:%d:%s",
		identity.Role,
		identity.Pass,
		identity.Lane,
	)
}

func reviewCyclePlannedWorkerCount(cycle *ReviewCycleState) int {
	if cycle == nil {
		return 0
	}
	planned := 0
	if len(cycle.DiscoveryPasses) > 0 {
		for _, pass := range cycle.DiscoveryPasses {
			planned += len(pass.Lanes)
		}
	} else if cycle.Plan != nil {
		planned += len(cycle.Plan.SelectedLanes)
	}
	if cycle.Convergence != nil {
		planned += len(cycle.Convergence.VerificationAssignments)
		planned += len(cycle.Convergence.ChallengeAssignments)
	}
	return planned
}

func reviewCycleMaxParallelObserved(
	cycle *ReviewCycleState,
	include func(ReviewWorkerOwnership) bool,
) int {
	if cycle == nil {
		return 0
	}
	type boundary struct {
		at    time.Time
		delta int
	}
	boundaries := make([]boundary, 0, len(cycle.WorkerOwnerships)*2)
	for _, ownership := range cycle.WorkerOwnerships {
		if ownership.StartedAt.IsZero() ||
			(include != nil && !include(ownership)) {
			continue
		}
		boundaries = append(boundaries, boundary{
			at:    ownership.StartedAt,
			delta: 1,
		})
		if !ownership.FinishedAt.IsZero() {
			boundaries = append(boundaries, boundary{
				at:    ownership.FinishedAt,
				delta: -1,
			})
		}
	}
	sort.Slice(boundaries, func(i, j int) bool {
		if boundaries[i].at.Equal(boundaries[j].at) {
			return boundaries[i].delta < boundaries[j].delta
		}
		return boundaries[i].at.Before(boundaries[j].at)
	})
	active := 0
	maximum := 0
	for _, item := range boundaries {
		active += item.delta
		if active > maximum {
			maximum = active
		}
	}
	return maximum
}

func observableReviewCycle(reviewer Agent) (
	reviewCycleObservation,
	error,
) {
	cycle := reviewer.ReviewCycle
	if cycle == nil {
		return reviewCycleObservation{},
			fmt.Errorf(
				"review coordinator %q has no review cycle",
				reviewer.ID,
			)
	}
	resultState, _ := reviewCycleResultForReviewer(reviewer)
	configuredAgentMaximum := cycle.Policy.Convergence.MaxReviewAgentsPerSHA
	requiredAgentMaximum := 0
	effectiveAgentMaximum := configuredAgentMaximum
	if cycle.Plan != nil {
		capacity, err := buildReviewCapacityPlan(cycle)
		if err != nil {
			return reviewCycleObservation{}, err
		}
		requiredAgentMaximum = capacity.RequiredMaximum
		effectiveAgentMaximum = capacity.EffectiveMaximum
	}
	observation := reviewCycleObservation{
		CycleID:           cycle.ID,
		Revision:          cycle.Revision,
		Attempt:           cycle.Attempt,
		ReviewerID:        reviewer.ID,
		PRNumber:          reviewer.PRNumber,
		HeadSHA:           cycle.HeadSHA,
		PolicyFingerprint: cycle.PolicyFingerprint,
		Bounds: reviewCycleBounds{
			MinReviewers:              cycle.Policy.Swarm.MinReviewers,
			MaxReviewers:              cycle.Policy.Swarm.MaxReviewers,
			MaxParallelReviewers:      cycle.Policy.Swarm.MaxParallelReviewers,
			MinVerifiers:              cycle.Policy.Verification.MinVerifiers,
			MaxVerifiers:              cycle.Policy.Verification.MaxVerifiers,
			MaxParallelVerifiers:      cycle.Policy.Verification.MaxParallelVerifiers,
			ConfiguredMaxAgentsPerSHA: configuredAgentMaximum,
			RequiredMaxAgentsPerSHA:   requiredAgentMaximum,
			MaxAgentsPerSHA:           effectiveAgentMaximum,
		},
		ResultState:         resultState,
		SupersededByHeadSHA: cycle.SupersededByHeadSHA,
		Publication: reviewCyclePublicationObservation{
			State: "not_prepared",
		},
		Workers: []reviewWorkerObservation{},
	}
	if cycle.Stale {
		observation.Publication.State = "suppressed_stale_head"
	} else if cycle.VerdictPublication != nil {
		observation.Publication = reviewCyclePublicationObservation{
			State:     string(cycle.VerdictPublication.Status),
			Identity:  cycle.VerdictPublication.ID,
			CommentID: cycle.VerdictPublication.CommentID,
		}
	}
	observation.Counts.Planned =
		reviewCyclePlannedWorkerCount(cycle)
	observation.Counts.Reserved =
		len(cycle.WorkerOwnerships)
	observation.Counts.MaxParallelObserved =
		reviewCycleMaxParallelObserved(cycle, nil)
	observation.Counts.MaxReviewerParallelObserved =
		reviewCycleMaxParallelObserved(
			cycle,
			func(ownership ReviewWorkerOwnership) bool {
				return ownership.Identity.Role ==
					AgentProfileRoleDiscovery ||
					ownership.Identity.Role ==
						AgentProfileRoleChallenge
			},
		)
	observation.Counts.MaxVerifierParallelObserved =
		reviewCycleMaxParallelObserved(
			cycle,
			func(ownership ReviewWorkerOwnership) bool {
				return ownership.Identity.Role ==
					AgentProfileRoleVerifier
			},
		)
	for _, ownership := range cycle.WorkerOwnerships {
		if err := validateReviewWorkerOwnership(ownership); err != nil {
			return reviewCycleObservation{}, err
		}
		switch ownership.Lifecycle {
		case ReviewWorkerRunning:
			observation.Counts.Running++
		case ReviewWorkerCompleted:
			observation.Counts.Completed++
		case ReviewWorkerFailed:
			observation.Counts.Failed++
		case ReviewWorkerCancelled:
			observation.Counts.Cancelled++
		}
		setupFailure := ""
		if ownership.Failure != nil &&
			ownership.Failure.Kind == DurableLaunchFailureSetup {
			setupFailure = ownership.Failure.Detail
		}
		observation.Workers = append(
			observation.Workers,
			reviewWorkerObservation{
				LogicalAssignment: reviewWorkerLogicalAssignment(
					ownership.Identity,
				),
				Role:                 ownership.Identity.Role,
				Pass:                 ownership.Identity.Pass,
				Lane:                 ownership.Identity.Lane,
				Profile:              ownership.Profile,
				Model:                ownership.Model,
				ReasoningEffort:      ownership.ReasoningEffort,
				Attempt:              ownership.Attempt,
				Lifecycle:            ownership.Lifecycle,
				ReservedAt:           ownership.AllocatedAt,
				StartedAt:            ownership.StartedAt,
				FinishedAt:           ownership.FinishedAt,
				WorktreeSetupFailure: setupFailure,
			},
		)
	}
	sort.Slice(observation.Workers, func(i, j int) bool {
		if observation.Workers[i].LogicalAssignment !=
			observation.Workers[j].LogicalAssignment {
			return observation.Workers[i].LogicalAssignment <
				observation.Workers[j].LogicalAssignment
		}
		return observation.Workers[i].Attempt <
			observation.Workers[j].Attempt
	})
	observation.WithinBounds =
		observation.Counts.Reserved <=
			observation.Bounds.MaxAgentsPerSHA &&
			observation.Counts.MaxReviewerParallelObserved <=
				observation.Bounds.MaxParallelReviewers &&
			observation.Counts.MaxVerifierParallelObserved <=
				observation.Bounds.MaxParallelVerifiers
	return observation, nil
}

func formatReviewCyclesStatus(agents []Agent) string {
	reviewers := make([]Agent, 0)
	for _, agent := range agents {
		if agent.Role == RoleReviewer &&
			agent.ReviewCycle != nil {
			reviewers = append(reviewers, agent)
		}
	}
	sort.Slice(reviewers, func(i, j int) bool {
		if reviewers[i].PRNumber != reviewers[j].PRNumber {
			return reviewers[i].PRNumber < reviewers[j].PRNumber
		}
		return reviewers[i].ReviewCycle.ID <
			reviewers[j].ReviewCycle.ID
	})
	var status strings.Builder
	status.WriteString("Convergent Review Cycles\n")
	if len(reviewers) == 0 {
		status.WriteString("  none\n")
		return status.String()
	}
	for _, reviewer := range reviewers {
		observation, err := observableReviewCycle(reviewer)
		if err != nil {
			fmt.Fprintf(
				&status,
				"  - reviewer=%s state=unavailable\n",
				reviewer.ID,
			)
			continue
		}
		fmt.Fprintf(
			&status,
			"  - cycle=%s revision=%d attempt=%d reviewer=%s pr=%d\n",
			observation.CycleID,
			observation.Revision,
			observation.Attempt,
			observation.ReviewerID,
			observation.PRNumber,
		)
		fmt.Fprintf(
			&status,
			"    head=%s result=%s\n",
			observation.HeadSHA,
			fallback(string(observation.ResultState), "pending"),
		)
		if observation.SupersededByHeadSHA != "" {
			fmt.Fprintf(
				&status,
				"    superseded_by_head=%s\n",
				observation.SupersededByHeadSHA,
			)
		}
		fmt.Fprintf(
			&status,
			"    counts: planned=%d reserved=%d running=%d completed=%d failed=%d cancelled=%d max_parallel_observed=%d reviewer_parallel_observed=%d verifier_parallel_observed=%d within_bounds=%t\n",
			observation.Counts.Planned,
			observation.Counts.Reserved,
			observation.Counts.Running,
			observation.Counts.Completed,
			observation.Counts.Failed,
			observation.Counts.Cancelled,
			observation.Counts.MaxParallelObserved,
			observation.Counts.MaxReviewerParallelObserved,
			observation.Counts.MaxVerifierParallelObserved,
			observation.WithinBounds,
		)
		fmt.Fprintf(
			&status,
			"    bounds: reviewers=%d..%d reviewer_parallel=%d verifiers=%d..%d verifier_parallel=%d configured_max_agents_per_sha=%d required_max_agents_per_sha=%d max_agents_per_sha=%d\n",
			observation.Bounds.MinReviewers,
			observation.Bounds.MaxReviewers,
			observation.Bounds.MaxParallelReviewers,
			observation.Bounds.MinVerifiers,
			observation.Bounds.MaxVerifiers,
			observation.Bounds.MaxParallelVerifiers,
			observation.Bounds.ConfiguredMaxAgentsPerSHA,
			observation.Bounds.RequiredMaxAgentsPerSHA,
			observation.Bounds.MaxAgentsPerSHA,
		)
		fmt.Fprintf(
			&status,
			"    publication: state=%s identity=%q comment_id=%d\n",
			observation.Publication.State,
			observation.Publication.Identity,
			observation.Publication.CommentID,
		)
		status.WriteString("    workers:\n")
		if len(observation.Workers) == 0 {
			status.WriteString("      none\n")
			continue
		}
		for _, worker := range observation.Workers {
			fmt.Fprintf(
				&status,
				"      - assignment=%q role=%s lane=%q pass=%d profile=%q model=%q effort=%q attempt=%d lifecycle=%s\n",
				worker.LogicalAssignment,
				worker.Role,
				worker.Lane,
				worker.Pass,
				worker.Profile,
				worker.Model,
				worker.ReasoningEffort,
				worker.Attempt,
				worker.Lifecycle,
			)
			if worker.WorktreeSetupFailure != "" {
				fmt.Fprintf(
					&status,
					"        worktree_setup_failure=%q\n",
					worker.WorktreeSetupFailure,
				)
			}
		}
	}
	return status.String()
}

func formatReviewCycleLogRecord(
	reviewer Agent,
	event string,
	timestamp time.Time,
) string {
	observation, err := observableReviewCycle(reviewer)
	if err != nil {
		return fmt.Sprintf(
			`{"timestamp":%q,"event":"review_cycle_observation","trigger":%q,"error":"metadata_unavailable"}`,
			timestamp.UTC().Format(time.RFC3339Nano),
			strings.TrimSpace(event),
		)
	}
	body, err := json.Marshal(reviewCycleLogRecord{
		Timestamp: timestamp.UTC(),
		Event:     strings.TrimSpace(event),
		Cycle:     observation,
	})
	if err != nil {
		return fmt.Sprintf(
			`{"timestamp":%q,"event":"review_cycle_observation","trigger":%q,"error":"metadata_unavailable"}`,
			timestamp.UTC().Format(time.RFC3339Nano),
			strings.TrimSpace(event),
		)
	}
	return string(body)
}

func (b *Orchestrator) logReviewCycleObservation(
	reviewerID string,
	event string,
) {
	if b == nil || b.agents == nil {
		return
	}
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok || reviewer.ReviewCycle == nil {
		return
	}
	structuredLogger := log.New(log.Writer(), "", 0)
	structuredLogger.Print(
		formatReviewCycleLogRecord(
			reviewer,
			event,
			time.Now().UTC(),
		),
	)
}
