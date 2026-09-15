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
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func newReviewLimitTestCycle(
	t *testing.T,
	mutate func(*ReviewPolicy),
) *ReviewCycleState {
	t.Helper()
	policy := builtInReviewPolicy()
	if mutate != nil {
		mutate(&policy)
	}
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	return cycle
}

func attachReviewLimitTestPlan(
	t *testing.T,
	cycle *ReviewCycleState,
) {
	t.Helper()
	criteria := []string{"Review the changed call path and its tests"}
	path := "internal/review_limits.go"
	inputs := ReviewPlanInputs{
		SchemaVersion:    reviewPlanInputsSchemaVersion,
		BaseSHA:          testOtherReviewHeadSHA,
		HeadSHA:          cycle.HeadSHA,
		ChangedFileCount: 1,
		ChangedLineCount: 12,
		ChangedFiles: []ReviewPlanChangedFile{{
			Path:      path,
			Additions: 12,
		}},
		AcceptanceCriteria: criteria,
	}
	if err := validateReviewPlanInputs(inputs); err != nil {
		t.Fatalf("validateReviewPlanInputs() error = %v", err)
	}
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
}

func TestEvaluateReviewLaunchLimitsAtConfiguredBoundaries(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	snapshot, err := cycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot() error = %v", err)
	}
	baseNow := snapshot.Cost.StartedAt.Add(time.Minute)
	totalTokens := uint64(cycle.Policy.Convergence.MaxUsageTokens)
	inputTokens := totalTokens
	tests := []struct {
		name           string
		mutateSnapshot func(*ReviewMetricsSnapshot)
		now            time.Time
		wantLimit      ReviewLimitKind
		wantEscalation ReviewEscalationTrigger
	}{
		{
			name: "agent count",
			mutateSnapshot: func(snapshot *ReviewMetricsSnapshot) {
				snapshot.Cost.AgentCount = uint64(
					cycle.Policy.Convergence.MaxReviewAgentsPerSHA,
				)
			},
			now:       baseNow,
			wantLimit: ReviewLimitAgentCount,
		},
		{
			name: "wall time",
			now: snapshot.Cost.StartedAt.Add(
				time.Duration(
					cycle.Policy.Convergence.MaxWallTimeMinutes,
				) * time.Minute,
			),
			wantLimit: ReviewLimitWallTime,
		},
		{
			name: "supported usage",
			mutateSnapshot: func(snapshot *ReviewMetricsSnapshot) {
				snapshot.Cost.Usage = &ReviewMetricUsage{
					TotalTokens: &totalTokens,
				}
			},
			now:       baseNow,
			wantLimit: ReviewLimitSupportedUsage,
		},
		{
			name: "non convergence",
			mutateSnapshot: func(snapshot *ReviewMetricsSnapshot) {
				snapshot.Quality.ConvergenceRoundsCompleted =
					uint64(
						cycle.Policy.Escalation.
							AfterNonConvergingRounds,
					)
			},
			now:            baseNow,
			wantEscalation: ReviewEscalationNonConvergence,
		},
		{
			name: "unsupported total usage is not estimated",
			mutateSnapshot: func(snapshot *ReviewMetricsSnapshot) {
				snapshot.Cost.Usage = &ReviewMetricUsage{
					InputTokens: &inputTokens,
				}
			},
			now: baseNow,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := snapshot
			candidate.Cost.Usage =
				cloneReviewMetricUsage(snapshot.Cost.Usage)
			if test.mutateSnapshot != nil {
				test.mutateSnapshot(&candidate)
			}
			evaluation, err := evaluateReviewLaunchLimits(
				cycle,
				candidate,
				test.now,
			)
			if err != nil {
				t.Fatalf("evaluateReviewLaunchLimits() error = %v", err)
			}
			if test.wantLimit == "" {
				if evaluation.Limit != nil {
					t.Fatalf(
						"limit transition = %#v, want none",
						evaluation.Limit,
					)
				}
			} else if evaluation.Limit == nil ||
				evaluation.Limit.Kind != test.wantLimit ||
				evaluation.Limit.Actual <
					evaluation.Limit.Maximum {
				t.Fatalf(
					"limit transition = %#v, want kind %q at its boundary",
					evaluation.Limit,
					test.wantLimit,
				)
			}
			if test.wantEscalation == "" {
				if evaluation.Escalation != nil {
					t.Fatalf(
						"escalation transition = %#v, want none",
						evaluation.Escalation,
					)
				}
			} else if evaluation.Escalation == nil ||
				evaluation.Escalation.Trigger !=
					test.wantEscalation {
				t.Fatalf(
					"escalation transition = %#v, want trigger %q",
					evaluation.Escalation,
					test.wantEscalation,
				)
			}
		})
	}
}

func TestReviewBudgetExhaustionSelectsConfiguredExplicitOutcome(
	t *testing.T,
) {
	tests := []struct {
		action  ReviewAction
		outcome ReviewLimitOutcome
	}{
		{ReviewActionInconclusive, ReviewLimitOutcomeInconclusive},
		{ReviewActionEscalate, ReviewLimitOutcomeEscalation},
		{ReviewActionFail, ReviewLimitOutcomeTerminalFailure},
	}
	for _, test := range tests {
		t.Run(string(test.action), func(t *testing.T) {
			cycle := newReviewLimitTestCycle(
				t,
				func(policy *ReviewPolicy) {
					policy.FailureActions.BudgetExhaustion =
						test.action
				},
			)
			snapshot, err := cycle.ReviewMetricsSnapshot()
			if err != nil {
				t.Fatalf("ReviewMetricsSnapshot() error = %v", err)
			}
			snapshot.Cost.AgentCount = uint64(
				cycle.Policy.Convergence.MaxReviewAgentsPerSHA,
			)
			evaluation, err := evaluateReviewLaunchLimits(
				cycle,
				snapshot,
				snapshot.Cost.StartedAt.Add(time.Minute),
			)
			if err != nil {
				t.Fatalf("evaluateReviewLaunchLimits() error = %v", err)
			}
			if evaluation.Limit == nil ||
				evaluation.Limit.Action != test.action ||
				evaluation.Limit.Outcome != test.outcome {
				t.Fatalf(
					"configured outcome = %#v, want action=%q outcome=%q",
					evaluation.Limit,
					test.action,
					test.outcome,
				)
			}
		})
	}
}

func TestReviewAgentLimitUsesAndPersistsConfiguredCapacity(t *testing.T) {
	const candidateCount = 13
	cycle := newReviewLimitTestCycle(t, nil)
	cycle.Policy.Convergence.MaxReviewAgentsPerSHA = 12
	attachReviewLimitTestPlan(t, cycle)
	for index := 0; index < candidateCount; index++ {
		candidate := convergenceCandidate(index + 1)
		cycle.CanonicalFindings = append(
			cycle.CanonicalFindings,
			findingCandidateToCanonicalForVerdictTest(
				fmt.Sprintf("dynamic-finding-%03d", index+1),
				cycle.HeadSHA,
				candidate,
			),
		)
	}
	capacity, err := buildReviewCapacityPlan(cycle)
	if err != nil {
		t.Fatalf("buildReviewCapacityPlan() error = %v", err)
	}
	if capacity.RequiredMaximum <= capacity.ConfiguredMaximum ||
		capacity.EffectiveMaximum != capacity.ConfiguredMaximum {
		t.Fatalf("configured capacity is not a hard ceiling: %#v", capacity)
	}
	snapshot, err := cycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot() error = %v", err)
	}
	now := snapshot.Cost.StartedAt.Add(time.Minute)
	snapshot.Cost.AgentCount = uint64(capacity.ConfiguredMaximum)
	evaluation, err := evaluateReviewLaunchLimits(cycle, snapshot, now)
	if err != nil || evaluation.Limit == nil {
		t.Fatalf("configured boundary was not enforced: evaluation=%#v err=%v", evaluation, err)
	}
	cycle.LimitTransition = evaluation.Limit
	if effectiveReviewAgentCapacity(cycle) != capacity.EffectiveMaximum {
		t.Fatalf(
			"persisted configured maximum changed from %d to %d",
			capacity.EffectiveMaximum,
			effectiveReviewAgentCapacity(cycle),
		)
	}
	if err := validateReviewLimitState(cycle); err != nil {
		t.Fatalf("validateReviewLimitState() error = %v", err)
	}
}

func TestReviewLimitOutcomePreservesRequiredLanesAndEvidenceInternally(
	t *testing.T,
) {
	cycle := newReviewLimitTestCycle(t, nil)
	attachReviewLimitTestPlan(t, cycle)
	requirement := cycle.Plan.CoverageRequirements[0]
	cycle.UnresolvedCoverage = []ReviewCoverageGap{{
		ID:            "coverage-gap-budget",
		RequirementID: requirement.ID,
		Kind:          requirement.Kind,
		Description:   "the persisted call path remains unreviewed",
		Status:        ReviewCoverageGapMissing,
		Evidence:      []ReviewEvidence{},
	}}
	snapshot, err := cycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot() error = %v", err)
	}
	snapshot.Cost.AgentCount = uint64(
		cycle.Policy.Convergence.MaxReviewAgentsPerSHA,
	)
	evaluation, err := evaluateReviewLaunchLimits(
		cycle,
		snapshot,
		snapshot.Cost.StartedAt.Add(time.Minute),
	)
	if err != nil || evaluation.Limit == nil {
		t.Fatalf(
			"evaluateReviewLaunchLimits() = (%#v, %v)",
			evaluation,
			err,
		)
	}
	cycle.LimitTransition = evaluation.Limit
	if !reflect.DeepEqual(
		cycle.LimitTransition.RequiredLanes,
		uniqueSortedStrings(append(
			append([]string(nil), cycle.Plan.SelectedLanes...),
			reviewSynthesisLane,
		)),
	) {
		t.Fatalf(
			"required lanes = %v, want %v",
			cycle.LimitTransition.RequiredLanes,
			cycle.Plan.SelectedLanes,
		)
	}
	if len(cycle.LimitTransition.UnresolvedEvidence) == 0 ||
		len(cycle.UnresolvedCoverage) != 1 ||
		cycle.UnresolvedCoverage[0].ID != "coverage-gap-budget" {
		t.Fatalf(
			"budget transition dropped internal required work: transition=%#v coverage=%#v",
			cycle.LimitTransition,
			cycle.UnresolvedCoverage,
		)
	}
	if reviewCycleHasPublishableReport(cycle) {
		t.Fatal("budget exhaustion is publishable as a code verdict")
	}
	if _, err := buildReviewVerdictAggregation(cycle); !errors.Is(err, errReviewResultNotPublishable) {
		t.Fatalf("budget aggregation error = %v, want non-publishable", err)
	}
}

func TestConcurrentReviewLaunchesPersistOneLimitTransition(
	t *testing.T,
) {
	agents := NewAgentManager()
	reviewer, _ := newReviewWorkerTestCoordinator(t, agents)
	maximum := reviewer.ReviewCycle.Policy.Convergence.
		MaxReviewAgentsPerSHA
	worktreeDir := t.TempDir()
	for index := 0; index < maximum-1; index++ {
		identity, err := reviewWorkerIdentityForCycle(
			reviewer.ReviewCycle,
			AgentProfileRoleDiscovery,
			index+1,
			"contract",
		)
		if err != nil {
			t.Fatalf("reviewWorkerIdentityForCycle(%d) error = %v", index, err)
		}
		if _, _, err := agents.allocateReviewWorkerOwnership(
			reviewer.ID,
			identity,
			worktreeDir,
			fmt.Sprintf("%032x", index+1),
			time.Now().UTC(),
		); err != nil {
			t.Fatalf(
				"allocateReviewWorkerOwnership(%d) error = %v",
				index,
				err,
			)
		}
	}
	logDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: worktreeDir,
		},
		agents: agents,
	}
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for index := 0; index < 2; index++ {
		identity, err := reviewWorkerIdentityForCycle(
			reviewer.ReviewCycle,
			AgentProfileRoleDiscovery,
			maximum+index,
			"contract",
		)
		if err != nil {
			t.Fatalf(
				"reviewWorkerIdentityForCycle(concurrent %d) error = %v",
				index,
				err,
			)
		}
		workers.Add(1)
		go func(identity ReviewWorkerIdentity) {
			defer workers.Done()
			_, err := bot.
				reserveReviewWorkerOwnershipForActiveCoordinator(
					reviewer.ID,
					identity,
				)
			results <- err
		}(identity)
	}
	workers.Wait()
	close(results)
	successes := 0
	limited := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, errReviewLimitReached):
			limited++
		default:
			t.Fatalf("concurrent reservation error = %v", err)
		}
	}
	if successes != 1 || limited != 1 {
		t.Fatalf(
			"concurrent reservations = success:%d limited:%d, want 1/1",
			successes,
			limited,
		)
	}
	current, ok := agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil ||
		current.ReviewCycle.LimitTransition == nil {
		t.Fatal("durable limit transition was not retained")
	}
	snapshot, err := current.ReviewCycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot() error = %v", err)
	}
	if snapshot.Cost.AgentCount != uint64(maximum) ||
		current.ReviewCycle.LimitTransition.Kind !=
			ReviewLimitAgentCount {
		t.Fatalf(
			"post-race state = agents:%d transition:%#v",
			snapshot.Cost.AgentCount,
			current.ReviewCycle.LimitTransition,
		)
	}

	restartedAgents := NewAgentManager()
	restarted := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: worktreeDir,
		},
		agents: restartedAgents,
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	restored, ok := restartedAgents.Get(reviewer.ID)
	if !ok || restored.ReviewCycle == nil ||
		restored.ReviewCycle.LimitTransition == nil {
		t.Fatal("restart dropped the durable limit transition")
	}
	transitionBeforeRetry := cloneReviewLimitTransition(
		restored.ReviewCycle.LimitTransition,
	)
	retryIdentity, err := reviewWorkerIdentityForCycle(
		restored.ReviewCycle,
		AgentProfileRoleDiscovery,
		maximum+2,
		"contract",
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle(retry) error = %v", err)
	}
	if _, err := restarted.
		reserveReviewWorkerOwnershipForActiveCoordinator(
			reviewer.ID,
			retryIdentity,
		); !errors.Is(err, errReviewLimitReached) {
		t.Fatalf(
			"restart reservation error = %v, want durable limit",
			err,
		)
	}
	after, _ := restartedAgents.Get(reviewer.ID)
	afterSnapshot, err := after.ReviewCycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot(after retry) error = %v", err)
	}
	if afterSnapshot.Cost.AgentCount != uint64(maximum) {
		t.Fatalf(
			"restart retry allocated agent count %d, want %d",
			afterSnapshot.Cost.AgentCount,
			maximum,
		)
	}
	if !reflect.DeepEqual(
		after.ReviewCycle.LimitTransition,
		transitionBeforeRetry,
	) {
		t.Fatalf(
			"restart retry replaced the durable transition: before=%#v after=%#v",
			transitionBeforeRetry,
			after.ReviewCycle.LimitTransition,
		)
	}
	if _, err := beginReviewConvergenceRoundState(
		after.ReviewCycle,
		time.Now().UTC(),
	); !errors.Is(err, errReviewLimitReached) {
		t.Fatalf(
			"assignment scheduling after limit error = %v, want durable limit",
			err,
		)
	}
}

func TestNonConvergingLaunchesUseEscalationProfile(
	t *testing.T,
) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *ReviewCycleState)
		trigger ReviewEscalationTrigger
	}{
		{
			name: "non converging",
			prepare: func(t *testing.T, cycle *ReviewCycleState) {
				state, err := reviewMetricsStateForCycle(cycle)
				if err != nil {
					t.Fatalf(
						"reviewMetricsStateForCycle() error = %v",
						err,
					)
				}
				for round := 1; round <=
					cycle.Policy.Escalation.
						AfterNonConvergingRounds; round++ {
					_, err := recordTrustedReviewMetricEvent(
						state,
						ReviewMetricEvent{
							Kind: ReviewMetricConvergenceRound,
							ObservedAt: time.Now().UTC().
								Add(time.Duration(round) * time.Second),
							HeadSHA:      cycle.HeadSHA,
							Activity:     ReviewMetricActivityCompleted,
							Round:        round,
							RoundOutcome: ReviewConvergenceRoundMaterial,
						},
					)
					if err != nil {
						t.Fatalf(
							"recordTrustedReviewMetricEvent(%d) error = %v",
							round,
							err,
						)
					}
				}
				cycle.Metrics = state
			},
			trigger: ReviewEscalationNonConvergence,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agents := NewAgentManager()
			reviewer, _ := newReviewWorkerTestCoordinator(
				t,
				agents,
			)
			agents.mu.Lock()
			cycle := agents.agents[reviewer.ID].ReviewCycle
			agents.mu.Unlock()
			test.prepare(t, cycle)
			reviewer, _ = agents.Get(reviewer.ID)
			identity, err := reviewWorkerIdentityForCycle(
				reviewer.ReviewCycle,
				AgentProfileRoleChallenge,
				1,
				"escalation-check",
			)
			if err != nil {
				t.Fatalf(
					"reviewWorkerIdentityForCycle() error = %v",
					err,
				)
			}
			runner := &testIsolatedReviewWorkerRunner{}
			bot := &Orchestrator{
				cfg: Config{
					RepoOwner:   "acme",
					RepoName:    "widget",
					LogDir:      t.TempDir(),
					WorktreeDir: t.TempDir(),
				},
				agents: agents,
				runner: runner,
			}
			bot.prepareReviewWorkerWorktreeFunc = func(
				_ context.Context,
				worker Agent,
			) error {
				return os.MkdirAll(worker.WorktreePath, 0o755)
			}
			if _, _, err := bot.launchReviewWorker(
				context.Background(),
				reviewer.ID,
				reviewWorkerLaunchRequest{
					Identity: identity,
					Prompt:   "exercise escalation profile",
				},
			); err != nil {
				t.Fatalf("launchReviewWorker() error = %v", err)
			}
			current, ok := agents.Get(reviewer.ID)
			if !ok || current.ReviewCycle == nil ||
				current.ReviewCycle.EscalationTransition == nil ||
				current.ReviewCycle.EscalationTransition.Trigger !=
					test.trigger {
				t.Fatalf(
					"escalation transition = %#v, want trigger %q",
					current.ReviewCycle.EscalationTransition,
					test.trigger,
				)
			}
			want := current.ReviewCycle.Policy.AgentProfiles[current.ReviewCycle.Policy.Escalation.Profile]
			if len(runner.started) != 1 ||
				runner.started[0].RuntimeProfile != want {
				t.Fatalf(
					"escalated runtime = %#v, want profile %#v",
					runner.started,
					want,
				)
			}
		})
	}
}
