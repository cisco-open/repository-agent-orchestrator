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
	"strings"
	"testing"
	"time"
)

// reviewPolicyWithHistoricalIncidentBudget returns builtInReviewPolicy()
// with CONVERGENCE.MAX_REVIEW_AGENTS_PER_SHA and MAX_WALL_TIME_MINUTES
// pinned back to 12 and 20 -- the exact values that produced the real
// wall-time and agent-count exhaustion incident this advisor exists to
// catch automatically. The built-in defaults themselves no longer carry
// these values (see the "prevent healthy reviews from timing out silently"
// fixes which raised them above those values precisely because they were that
// incident's root cause), so the regression coverage for "does the advisor
// still catch a policy this badly under-provisioned" has to pin the
// historical numbers explicitly rather than assume the current defaults
// are still bad.
func reviewPolicyWithHistoricalIncidentBudget() ReviewPolicy {
	policy := builtInReviewPolicy()
	policy.Convergence.MaxReviewAgentsPerSHA = 12
	policy.Convergence.MaxWallTimeMinutes = 20
	return policy
}

func TestAnalyzeReviewPolicyBudgetFlagsTheHistoricalIncidentPolicy(t *testing.T) {
	policy := reviewPolicyWithHistoricalIncidentBudget()

	warnings := analyzeReviewPolicyBudget(policy)
	if len(warnings) != 2 {
		t.Fatalf("analyzeReviewPolicyBudget(historical incident policy) = %d warnings, want 2: %v", len(warnings), warnings)
	}
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "MAX_WALL_TIME_MINUTES=20") {
		t.Fatalf("warnings missing wall-time flag: %v", warnings)
	}
	if !strings.Contains(joined, "MAX_REVIEW_AGENTS_PER_SHA=12") {
		t.Fatalf("warnings missing agent-count flag: %v", warnings)
	}
	if !strings.Contains(joined, "consider raising it to at least") {
		t.Fatalf("warnings missing a concrete suggestion: %v", warnings)
	}
}

func TestAnalyzeReviewPolicyBudgetAcceptsCurrentBuiltInDefaults(t *testing.T) {
	// The current built-in defaults are sized specifically to clear this
	// advisor's own estimate (see "prevent healthy reviews from timing out
	// silently"); if they ever regress below the realistic estimate again,
	// this should start flagging them just like
	// TestAnalyzeReviewPolicyBudgetFlagsTheHistoricalIncidentPolicy proves
	// it would.
	if warnings := analyzeReviewPolicyBudget(builtInReviewPolicy()); len(warnings) != 0 {
		t.Fatalf("analyzeReviewPolicyBudget(defaults) = %v, want no warnings", warnings)
	}
}

func TestEstimateReviewPolicyBudgetScalesAgentsWithAssumedContestedFindings(t *testing.T) {
	// A policy that only ever assumes one contested finding per round
	// (MaxVerifiers per round rather than per finding) systematically
	// undercounts as soon as a review surfaces more than one -- which is
	// the common case, not an edge case. Raising SWARM.MIN_REVIEWERS (an
	// existing, already-meaningful concurrency knob) must raise the
	// assumed contested-finding count, and therefore the agent estimate,
	// proportionally; a formula that ignores finding count entirely would
	// leave this unchanged.
	low := builtInReviewPolicy()
	low.Swarm.MinReviewers = 1
	high := builtInReviewPolicy()
	high.Swarm.MinReviewers = 5

	lowEstimate := estimateReviewPolicyBudget(low)
	highEstimate := estimateReviewPolicyBudget(high)

	if lowEstimate.AssumedContestedFindings != 1 {
		t.Fatalf("AssumedContestedFindings(MinReviewers=1) = %d, want 1", lowEstimate.AssumedContestedFindings)
	}
	if highEstimate.AssumedContestedFindings != 5 {
		t.Fatalf("AssumedContestedFindings(MinReviewers=5) = %d, want 5", highEstimate.AssumedContestedFindings)
	}
	if highEstimate.EstimatedAgents <= lowEstimate.EstimatedAgents {
		t.Fatalf(
			"EstimatedAgents did not scale with assumed contested findings: low=%d high=%d",
			lowEstimate.EstimatedAgents, highEstimate.EstimatedAgents,
		)
	}
}

func TestAnalyzeReviewPolicyBudgetWarningNamesTheContestedFindingAssumption(t *testing.T) {
	policy := reviewPolicyWithHistoricalIncidentBudget()

	warnings := analyzeReviewPolicyBudget(policy)
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "concurrently-contested finding(s) per round") {
		t.Fatalf("agent-count warning does not name its contested-finding assumption: %v", warnings)
	}
	if !strings.Contains(joined, "SWARM.MIN_REVIEWERS=") {
		t.Fatalf("agent-count warning does not cite the policy knob behind the assumption: %v", warnings)
	}
	if !strings.Contains(joined, "a review surfacing more contested findings per round") {
		t.Fatalf("agent-count warning does not disclose the estimate's limit: %v", warnings)
	}
}

func TestFormatReviewBudgetReportAlwaysDisclosesSupportedContestedFindings(t *testing.T) {
	// This line must appear even when there is no warning at all, since a
	// policy with a generous-looking MAX_REVIEW_AGENTS_PER_SHA can still be
	// exhausted by a review that surfaces more contested findings per
	// round than the budget has room for -- a fact no warning-only report
	// would ever surface.
	generous := builtInReviewPolicy()
	generous.Convergence.MaxWallTimeMinutes = 600
	generous.Convergence.MaxReviewAgentsPerSHA = 60
	if warnings := analyzeReviewPolicyBudget(generous); len(warnings) != 0 {
		t.Fatalf("expected no warnings for the generous policy, got %v", warnings)
	}

	report := formatReviewBudgetReport(generous, nil)
	if !strings.Contains(report, "this budget has room for up to") {
		t.Fatalf("report missing always-on supported-findings disclosure: %q", report)
	}
	if !strings.Contains(report, "concurrently-contested finding(s) per round") {
		t.Fatalf("report missing contested-finding capacity wording: %q", report)
	}

	tight := reviewPolicyWithHistoricalIncidentBudget()
	if !strings.Contains(formatReviewBudgetReport(tight, nil), "room for up to 0 concurrently-contested finding(s)") {
		t.Fatalf("report should show zero remaining finding capacity for the historical incident policy")
	}
}

func TestAnalyzeReviewPolicyBudgetAcceptsGenerouslyConfiguredPolicy(t *testing.T) {
	policy := builtInReviewPolicy()
	policy.Convergence.MaxWallTimeMinutes = 600
	policy.Convergence.MaxReviewAgentsPerSHA = 60

	if warnings := analyzeReviewPolicyBudget(policy); len(warnings) != 0 {
		t.Fatalf("analyzeReviewPolicyBudget(generous) = %v, want no warnings", warnings)
	}
}

func TestFindReviewBudgetLimitHitsFindsPersistedTransition(t *testing.T) {
	cycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	transitionedAt := time.Date(2026, 8, 21, 8, 18, 21, 0, time.UTC)
	cycle.LimitTransition = &ReviewLimitTransition{
		Kind:           ReviewLimitWallTime,
		TransitionedAt: transitionedAt,
		Actual:         1201049,
		Maximum:        1200000,
		Unit:           "milliseconds",
	}
	agents := []Agent{
		{ID: "review-agent-980", Role: RoleReviewer, PRNumber: 990, ReviewCycle: cycle},
		{ID: "coding-agent-980", Role: RoleCoder, PRNumber: 990},
	}

	hits := findReviewBudgetLimitHits(agents)
	if len(hits) != 1 {
		t.Fatalf("findReviewBudgetLimitHits() = %d hits, want 1", len(hits))
	}
	if hits[0].ReviewerID != "review-agent-980" ||
		hits[0].Kind != ReviewLimitWallTime ||
		hits[0].Actual != 1201049 ||
		hits[0].Maximum != 1200000 {
		t.Fatalf("findReviewBudgetLimitHits() = %#v", hits[0])
	}
}

func TestFindReviewBudgetLimitHitsFindsUsageTokenExhaustion(t *testing.T) {
	// MAX_USAGE_TOKENS has no proactive estimate (no ReviewPolicy field
	// bounds per-agent token usage), so this forensic path is the *only*
	// detection available for it -- it must actually work.
	cycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	cycle.LimitTransition = &ReviewLimitTransition{
		Kind:    ReviewLimitSupportedUsage,
		Actual:  2_000_500,
		Maximum: 2_000_000,
		Unit:    "tokens",
	}

	hits := findReviewBudgetLimitHits([]Agent{
		{ID: "review-agent-990", Role: RoleReviewer, PRNumber: 990, ReviewCycle: cycle},
	})
	if len(hits) != 1 || hits[0].Kind != ReviewLimitSupportedUsage {
		t.Fatalf("findReviewBudgetLimitHits() = %#v, want one supported_usage hit", hits)
	}
}

func TestFindReviewBudgetLimitHitsFindsMaxRoundsExhaustion(t *testing.T) {
	// MAX_ROUNDS exhaustion has no ReviewLimitTransition of its own --
	// it's tracked as cycle.Convergence.Status == ReviewConvergenceMaxRounds
	// -- so it needs its own detection path, separate from LimitTransition.
	policy := builtInReviewPolicy()
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	cycle.Convergence = &ReviewConvergenceState{
		Status: ReviewConvergenceMaxRounds,
		Rounds: []ReviewConvergenceRoundState{
			{Round: 1, Outcome: ReviewConvergenceRoundChangesRequired},
			{Round: 2, Outcome: ReviewConvergenceRoundMaxRounds, CompletedAt: time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC)},
		},
	}

	hits := findReviewBudgetLimitHits([]Agent{
		{ID: "review-agent-990", Role: RoleReviewer, PRNumber: 990, ReviewCycle: cycle},
	})
	if len(hits) != 1 || hits[0].Kind != reviewLimitKindMaxRoundsExhausted {
		t.Fatalf("findReviewBudgetLimitHits() = %#v, want one max_rounds_exhausted hit", hits)
	}
	if hits[0].Actual != 2 || hits[0].Maximum != uint64(policy.Convergence.MaxRounds) {
		t.Fatalf("findReviewBudgetLimitHits() round counts = actual:%d maximum:%d", hits[0].Actual, hits[0].Maximum)
	}
}

func TestFormatReviewBudgetReportDisclosesScope(t *testing.T) {
	report := formatReviewBudgetReport(builtInReviewPolicy(), nil)
	if !strings.Contains(report, "MAX_USAGE_TOKENS is not") {
		t.Fatalf("report does not disclose the MAX_USAGE_TOKENS scope limitation: %q", report)
	}
	if !strings.Contains(report, "MAX_ROUNDS exhaustion is not flagged") {
		t.Fatalf("report does not disclose the MAX_ROUNDS scope limitation: %q", report)
	}
}

func TestFormatReviewBudgetReportCombinesEstimateAndForensics(t *testing.T) {
	policy := reviewPolicyWithHistoricalIncidentBudget()
	report := formatReviewBudgetReport(policy, nil)
	if !strings.Contains(report, "Budget warnings") {
		t.Fatalf("report missing budget warnings section: %q", report)
	}
	if !strings.Contains(report, "No currently loaded reviewer has recorded an actual budget exhaustion") {
		t.Fatalf("report missing empty-forensics line: %q", report)
	}

	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	cycle.LimitTransition = &ReviewLimitTransition{
		Kind:    ReviewLimitAgentCount,
		Actual:  12,
		Maximum: 12,
		Unit:    "agents",
	}
	reportWithHit := formatReviewBudgetReport(policy, []Agent{
		{ID: "review-agent-980", Role: RoleReviewer, PRNumber: 990, ReviewCycle: cycle},
	})
	if !strings.Contains(reportWithHit, "1 currently loaded reviewer(s) have actually exhausted a budget") {
		t.Fatalf("report missing forensic hit summary: %q", reportWithHit)
	}
	if !strings.Contains(reportWithHit, "review-agent-980") {
		t.Fatalf("report missing hit reviewer id: %q", reportWithHit)
	}
}
