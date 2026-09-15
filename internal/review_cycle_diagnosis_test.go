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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDiagnoseReviewCycleHealthyNonReviewer(t *testing.T) {
	agent := Agent{Role: RoleCoder}
	diagnosis := diagnoseReviewCycle(agent, time.Now())
	if !diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisHealthy {
		t.Fatalf("diagnoseReviewCycle(coder) = %#v, want healthy", diagnosis)
	}
}

func TestDiagnoseReviewCycleHealthyReviewerWithoutCycle(t *testing.T) {
	agent := Agent{Role: RoleReviewer, State: StateInitializing}
	diagnosis := diagnoseReviewCycle(agent, time.Now())
	if !diagnosis.Healthy {
		t.Fatalf("diagnoseReviewCycle(no cycle) = %#v, want healthy", diagnosis)
	}
}

// TestDiagnoseReviewCyclePausedNeverStalls is the regression test for
// independent review feedback: AgentManager.Active() does not exclude
// paused agents, so a reviewer paused for longer than its expected phase
// duration (and with a non-terminal ReviewCycle) must never be classified
// as unknown_stall -- which would also wrongly auto-trigger a
// tech-support capture for what is actually just an operator-requested
// pause, not a problem.
func TestDiagnoseReviewCyclePausedNeverStalls(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	now := time.Now()
	agent := Agent{
		Role:             RoleReviewer,
		State:            StateWorking,
		Paused:           true,
		ReviewCycle:      cycle,
		LastActivityTime: now.Add(-24 * time.Hour),
	}
	diagnosis := diagnoseReviewCycle(agent, now)
	if !diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisPaused {
		t.Fatalf("diagnoseReviewCycle(paused) = %#v, want healthy paused", diagnosis)
	}
	line := formatReviewCycleDiagnosisLine(agent, diagnosis)
	if strings.Contains(line, "STUCK") {
		t.Fatalf("paused reviewer line should never read STUCK: %q", line)
	}
}

func TestDiagnoseReviewCycleReviewGateWedge(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	now := time.Now()
	agent := Agent{
		Role:             RoleReviewer,
		State:            StateReviewGate,
		ReviewCycle:      cycle,
		LastActivityTime: now.Add(-(reviewLaunchStallTimeout + time.Minute)),
	}
	diagnosis := diagnoseReviewCycle(agent, now)
	if diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisReviewGateWedge {
		t.Fatalf("diagnoseReviewCycle(gate wedge) = %#v, want review_gate_wedge_after_restart", diagnosis)
	}
	if !strings.Contains(diagnosis.Detail, "docs/TROUBLESHOOTING.md") {
		t.Fatalf("diagnosis detail missing troubleshooting pointer: %q", diagnosis.Detail)
	}
}

func TestDiagnoseReviewCycleReviewGateNotYetStale(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	now := time.Now()
	agent := Agent{
		Role:             RoleReviewer,
		State:            StateReviewGate,
		ReviewCycle:      cycle,
		LastActivityTime: now.Add(-time.Minute),
	}
	diagnosis := diagnoseReviewCycle(agent, now)
	if !diagnosis.Healthy {
		t.Fatalf("diagnoseReviewCycle(fresh gate) = %#v, want healthy (too early to call it wedged)", diagnosis)
	}
}

// newOrphanedVerificationAssignmentCycle builds a cycle whose persisted
// convergence state fails validatePersistedReviewCycleSnapshot: a
// VerificationAssignment pointing at a finding ID that isn't in
// CanonicalFindings, the same class of corruption as issue #128.
func newOrphanedVerificationAssignmentCycle(t *testing.T) *ReviewCycleState {
	t.Helper()
	cycle := newReviewLimitTestCycle(t, nil)
	findingA := canonicalConvergenceFinding(cycle.HeadSHA, 1)
	cycle.CanonicalFindings = []ReviewCanonicalFinding{findingA}
	orphanedID := "finding-" + strings.Repeat("a", 64)
	orphanedSnapshot := cloneReviewCanonicalFinding(findingA)
	orphanedSnapshot.ID = orphanedID
	revision, err := reviewFindingCandidateRevision(orphanedSnapshot)
	if err != nil {
		t.Fatalf("reviewFindingCandidateRevision() error = %v", err)
	}
	assignment := ReviewVerificationAssignment{
		ID:                "verification:" + orphanedID + ":001",
		FindingID:         orphanedID,
		ExactSHA:          cycle.HeadSHA,
		CandidateRevision: revision,
		CandidateSnapshot: orphanedSnapshot,
		Round:             1,
		Ordinal:           1,
		Lane:              reviewVerificationLane(orphanedID, 1),
		Status:            ReviewVerificationQueued,
		ExcludedWorkerIDs: reviewFindingOriginWorkerIDs(findingA),
		QueuedAt:          time.Now().Add(-time.Minute),
	}
	cycle.Convergence = &ReviewConvergenceState{
		Status:                  ReviewConvergencePending,
		VerificationAssignments: []ReviewVerificationAssignment{assignment},
		FindingVerifications:    []ReviewFindingVerification{},
		ChallengeAssignments:    []ReviewChallengeAssignment{},
		Rounds:                  []ReviewConvergenceRoundState{},
	}
	return cycle
}

func TestDiagnoseReviewCyclePersistedStateInvalid(t *testing.T) {
	cycle := newOrphanedVerificationAssignmentCycle(t)
	agent := Agent{
		Role:             RoleReviewer,
		State:            StateWorking,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now(),
	}
	diagnosis := diagnoseReviewCycle(agent, time.Now())
	if diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisPersistedStateInvalid {
		t.Fatalf("diagnoseReviewCycle(invalid) = %#v, want persisted_convergence_invalid", diagnosis)
	}
}

// TestDiagnoseReviewCyclePausedDoesNotMaskPersistedInvalid is the
// regression test for independent review feedback: pausing must never
// hide a genuinely invalid persisted cycle behind the benign "paused"
// status -- the invalidity is a fact about the data, not something pause
// caused or unpause will fix, and persisted_convergence_invalid is
// exactly the kind of known failure signature this feature exists to
// surface.
func TestDiagnoseReviewCyclePausedDoesNotMaskPersistedInvalid(t *testing.T) {
	cycle := newOrphanedVerificationAssignmentCycle(t)
	agent := Agent{
		Role:             RoleReviewer,
		State:            StateWorking,
		Paused:           true,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now(),
	}
	diagnosis := diagnoseReviewCycle(agent, time.Now())
	if diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisPersistedStateInvalid {
		t.Fatalf("diagnoseReviewCycle(paused + invalid) = %#v, want STUCK persisted_convergence_invalid, not paused", diagnosis)
	}
}

func TestDiagnoseReviewCycleBudgetExhausted(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	maximum := uint64(time.Duration(cycle.Policy.Convergence.MaxWallTimeMinutes) * time.Minute / time.Millisecond)
	cycle.LimitTransition = &ReviewLimitTransition{
		SchemaVersion:     reviewLimitTransitionSchemaVersion,
		Kind:              ReviewLimitWallTime,
		TransitionedAt:    time.Now(),
		Actual:            maximum + 500,
		Maximum:           maximum,
		Unit:              "milliseconds",
		Action:            cycle.Policy.FailureActions.BudgetExhaustion,
		Outcome:           reviewLimitOutcomeForAction(cycle.Policy.FailureActions.BudgetExhaustion),
		Reason:            "review wall time reached snapshotted maximum",
		MetricEventCount:  1,
		MetricEventDigest: strings.Repeat("a", 64),
	}
	now := time.Now()
	if reviewCycleHasPublishableReport(cycle) {
		t.Fatal("test fixture is publishable; a LimitTransition should never be, per ApprovalEligible()")
	}

	// Freshly exhausted: this is an expected, momentary state while the
	// coordinator retires the reviewer, not a problem in itself. No
	// verdict will ever be published for this cycle (see ApprovalEligible
	// and classifyReviewCycleResult's partial/no-review state), so
	// the detail must say "cleanup," never "verdict publication."
	fresh := Agent{
		Role: RoleReviewer, State: StateWorking, ReviewCycle: cycle,
		LastActivityTime: now,
	}
	diagnosis := diagnoseReviewCycle(fresh, now)
	if !diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisBudgetExhausted {
		t.Fatalf("diagnoseReviewCycle(fresh budget exhaustion) = %#v, want healthy budget_exhausted", diagnosis)
	}
	if !strings.Contains(diagnosis.Detail, "wall_time") {
		t.Fatalf("diagnosis detail missing limit kind: %q", diagnosis.Detail)
	}
	if strings.Contains(diagnosis.Detail, "verdict publication") || strings.Contains(diagnosis.Detail, "awaiting verdict") {
		t.Fatalf("diagnosis detail claims a report is coming without any trusted result: %q", diagnosis.Detail)
	}

	// Exhausted a long time ago with no coordinator cleanup: a real
	// problem, escalating on the same short grace window as
	// terminal_non_publishable_stall (this is the same class of issue --
	// "reached a terminal state with no publishable work and wasn't retired" --
	// not the longer, GitHub-API-call-oriented verdict-publication window).
	stuck := Agent{
		Role: RoleReviewer, State: StateWorking, ReviewCycle: cycle,
		LastActivityTime: now.Add(-(reviewCycleCleanupGraceWindow + time.Minute)),
	}
	diagnosis = diagnoseReviewCycle(stuck, now)
	if diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisBudgetExhausted {
		t.Fatalf("diagnoseReviewCycle(stale budget exhaustion) = %#v, want STUCK budget_exhausted", diagnosis)
	}
}

// TestDiagnoseReviewCyclePublishedVerdictAwaitsCleanupNotPublication is the
// regression test for independent review feedback: once a verdict is
// actually published, reporting "awaiting verdict publication" directly
// contradicts the cycle's own persisted VerdictPublication.Status.
func TestDiagnoseReviewCyclePublishedVerdictAwaitsCleanupNotPublication(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	fixture := &reviewVerdictGitHubFixture{headSHA: harness.reviewer.ReviewCycle.HeadSHA}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()
	if _, err := harness.bot.publishReviewCycleVerdict(context.Background(), harness.reviewer.ID); err != nil {
		t.Fatalf("publishReviewCycleVerdict() error = %v", err)
	}
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok {
		t.Fatal("reviewer disappeared after publishing")
	}
	if current.ReviewCycle.VerdictPublication == nil ||
		current.ReviewCycle.VerdictPublication.Status != ReviewVerdictPublicationPublished {
		t.Fatalf("test setup did not actually publish: %#v", current.ReviewCycle.VerdictPublication)
	}
	if reviewCycleNeedsReportPublication(current.ReviewCycle) {
		t.Fatal("test setup still needs publication; fixture no longer exercises the published path")
	}
	now := time.Now()

	fresh := Agent{Role: RoleReviewer, State: StateWorking, ReviewCycle: current.ReviewCycle, LastActivityTime: now.Add(-time.Second)}
	diagnosis := diagnoseReviewCycle(fresh, now)
	if diagnosis.Pattern != reviewCycleDiagnosisVerdictPublishedAwaitingCleanup {
		t.Fatalf("diagnoseReviewCycle(published, fresh) = %#v, want verdict_published_awaiting_cleanup", diagnosis)
	}
	if !diagnosis.Healthy {
		t.Fatalf("diagnoseReviewCycle(published, fresh) should allow a brief cleanup grace period: %#v", diagnosis)
	}
	if strings.Contains(diagnosis.Detail, "awaiting verdict publication") {
		t.Fatalf("diagnosis detail contradicts the already-published state: %q", diagnosis.Detail)
	}

	stuck := Agent{Role: RoleReviewer, State: StateWorking, ReviewCycle: current.ReviewCycle, LastActivityTime: now.Add(-(reviewCycleCleanupGraceWindow + time.Minute))}
	diagnosis = diagnoseReviewCycle(stuck, now)
	if diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisVerdictPublishedAwaitingCleanup {
		t.Fatalf(
			"diagnoseReviewCycle(published, stuck) = %#v, want STUCK verdict_published_awaiting_cleanup (not awaiting_verdict_publication)",
			diagnosis,
		)
	}
}

// TestDiagnoseReviewCycleTerminalConvergenceAwaitsPublicationNotUnknownStall
// is the regression test for the independent review feedback: a cycle that
// reached a terminal convergence decision (converged/changes_required/
// max_rounds/unresolved) without a LimitTransition must not fall through to
// the generic unknown_stall fallback just because it's been quiet -- it has
// a known, named reason to be quiet.
func TestDiagnoseReviewCycleTerminalConvergenceAwaitsPublicationNotUnknownStall(t *testing.T) {
	// newTerminalReviewVerdictHarness drives a real convergence cycle
	// through to a genuinely, fully-validly terminal Converged status --
	// constructing that by hand would mean re-deriving a large slice of
	// review_convergence.go's own validation requirements (matching
	// rounds, discovery passes, quiet-round counts, etc.) with no benefit
	// over using the existing, already-correct harness.
	harness := newTerminalReviewVerdictHarness(t)
	cycle := harness.reviewer.ReviewCycle
	now := time.Now()

	fresh := Agent{
		Role: RoleReviewer, State: StateWorking, ReviewCycle: cycle,
		LastActivityTime: now.Add(-time.Minute),
	}
	diagnosis := diagnoseReviewCycle(fresh, now)
	if !diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisAwaitingVerdictPublication {
		t.Fatalf("diagnoseReviewCycle(converged, fresh) = %#v, want healthy awaiting_verdict_publication", diagnosis)
	}

	stuck := Agent{
		Role: RoleReviewer, State: StateWorking, ReviewCycle: cycle,
		LastActivityTime: now.Add(-(reviewCycleVerdictPublicationQuietWindow + time.Minute)),
	}
	diagnosis = diagnoseReviewCycle(stuck, now)
	if diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisAwaitingVerdictPublication {
		t.Fatalf(
			"diagnoseReviewCycle(converged, long quiet) = %#v, want STUCK awaiting_verdict_publication (not unknown_stall)",
			diagnosis,
		)
	}
}

// An unresolved terminal cycle with accepted artifacts now awaits a partial
// report instead of cleanup without publication.
func TestDiagnoseReviewCyclePartialResultAwaitsPublication(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, false)
	round, err := harness.bot.beginReviewConvergenceRound(harness.reviewer.ID)
	if err != nil {
		t.Fatalf("beginReviewConvergenceRound() error = %v", err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	targets := buildReviewChallengeTargets(current.ReviewCycle)
	if len(targets) == 0 {
		t.Fatal("review cycle has no coverage target")
	}
	challenge, err := harness.bot.queueTargetedReviewChallenge(harness.reviewer.ID, round.Round, targets[0])
	if err != nil {
		t.Fatalf("queueTargetedReviewChallenge() error = %v", err)
	}
	coverage := make([]ReviewCoverageClaim, 0, len(current.ReviewCycle.Plan.CoverageRequirements))
	for _, requirement := range current.ReviewCycle.Plan.CoverageRequirements {
		coverage = append(coverage, ReviewCoverageClaim{
			RequirementID: requirement.ID,
			Kind:          requirement.Kind,
			Status:        ReviewCoverageCovered,
			Evidence:      []ReviewEvidence{{Summary: "challenge supplied coverage evidence", Path: "tracked.txt"}},
		})
	}
	completeConvergenceChallenge(t, harness, challenge, ReviewChallengePayload{
		Outcome:      ReviewChallengeInconclusive,
		Summary:      "coverage evidence did not resolve the challenge",
		AssignmentID: challenge.ID,
		TargetKind:   challenge.Target.Kind,
		TargetID:     challenge.Target.ID,
		Candidates:   []ReviewFindingCandidate{},
		Coverage:     coverage,
	})
	if _, err := harness.bot.completeReviewConvergenceRound(harness.reviewer.ID); err != nil {
		t.Fatalf("completeReviewConvergenceRound() error = %v", err)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if current.ReviewCycle.Convergence.Status != ReviewConvergenceUnresolved {
		t.Fatalf("test setup did not reach ReviewConvergenceUnresolved: %#v", current.ReviewCycle.Convergence)
	}
	if !reviewCycleHasPublishableReport(current.ReviewCycle) {
		t.Fatal("accepted partial result is not publishable")
	}
	now := time.Now()

	fresh := Agent{Role: RoleReviewer, State: StateWorking, ReviewCycle: current.ReviewCycle, LastActivityTime: now.Add(-time.Second)}
	diagnosis := diagnoseReviewCycle(fresh, now)
	if !diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisAwaitingVerdictPublication {
		t.Fatalf("diagnoseReviewCycle(fresh partial result) = %#v, want awaiting_verdict_publication", diagnosis)
	}

	stuck := Agent{Role: RoleReviewer, State: StateWorking, ReviewCycle: current.ReviewCycle, LastActivityTime: now.Add(-(reviewCycleVerdictPublicationQuietWindow + time.Minute))}
	diagnosis = diagnoseReviewCycle(stuck, now)
	if diagnosis.Healthy || diagnosis.Pattern != reviewCycleDiagnosisAwaitingVerdictPublication {
		t.Fatalf(
			"diagnoseReviewCycle(stuck partial result) = %#v, want STUCK awaiting_verdict_publication",
			diagnosis,
		)
	}
}

// TestDiagnoseReviewCycleStaleIsSupersededNotHealthy is the regression test
// for the second piece of independent review feedback: a stale cycle must
// be reported distinctly, not as plain "healthy" (misleading -- nothing is
// progressing) and not routed through the other checks meant for cycles
// still expected to advance.
func TestDiagnoseReviewCycleStaleIsSupersededNotHealthy(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	cycle.Stale = true
	cycle.SupersededByHeadSHA = strings.Repeat("b", 40)
	agent := Agent{
		Role: RoleReviewer, State: StateWorking, ReviewCycle: cycle,
		LastActivityTime: time.Now().Add(-time.Hour),
	}
	diagnosis := diagnoseReviewCycle(agent, time.Now())
	if diagnosis.Pattern != reviewCycleDiagnosisSuperseded {
		t.Fatalf("diagnoseReviewCycle(stale) = %#v, want superseded_stale_head", diagnosis)
	}
	if !diagnosis.Healthy {
		t.Fatalf("diagnoseReviewCycle(stale) should not be reported as a problem: %#v", diagnosis)
	}
	line := formatReviewCycleDiagnosisLine(agent, diagnosis)
	if strings.Contains(line, ": healthy --") {
		t.Fatalf("stale reviewer line should not read as plain \"healthy\": %q", line)
	}
	if !strings.Contains(line, "superseded") {
		t.Fatalf("stale reviewer line should say superseded: %q", line)
	}
}

// The remaining two scenarios (a stalled discovery lane past its expected
// budget, and a healthy in-progress discovery pass) are tested directly
// against reviewCycleExpectedMaxQuietDuration/summarizeReviewCycleProgress
// rather than through the full diagnoseReviewCycle ->
// validatePersistedReviewCycleSnapshot path, since a DiscoveryPasses entry
// that's realistic enough to pass full plan/lane/ownership validation is
// unrelated machinery to what these two checks compute.

func TestReviewCycleExpectedMaxQuietDurationBeforeDiscoveryStarts(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	got := reviewCycleExpectedMaxQuietDuration(cycle)
	swarmAttempts := max(1, cycle.Policy.Swarm.Retries+1)
	want := time.Duration(cycle.Policy.Swarm.TimeoutMinutes) * time.Duration(swarmAttempts) * 2 * time.Minute
	if got != want {
		t.Fatalf("reviewCycleExpectedMaxQuietDuration(no passes) = %s, want %s", got, want)
	}
}

func TestReviewCycleExpectedMaxQuietDurationDiscoveryStillRunning(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	cycle.DiscoveryPasses = []ReviewDiscoveryPassState{
		{
			Pass: 1, HeadSHA: cycle.HeadSHA, QueuedAt: time.Now(),
			Lanes: []ReviewDiscoveryLaneState{
				{Lane: "contract", Required: true, Status: ReviewDiscoveryLaneQueued, QueuedAt: time.Now()},
			},
		},
	}
	swarmAttempts := max(1, cycle.Policy.Swarm.Retries+1)
	want := time.Duration(cycle.Policy.Swarm.TimeoutMinutes) * time.Duration(swarmAttempts) * 2 * time.Minute
	if got := reviewCycleExpectedMaxQuietDuration(cycle); got != want {
		t.Fatalf("reviewCycleExpectedMaxQuietDuration(discovery running) = %s, want %s", got, want)
	}
}

func TestReviewCycleExpectedMaxQuietDurationBetweenRounds(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	cycle.DiscoveryPasses = []ReviewDiscoveryPassState{
		{Pass: 1, HeadSHA: cycle.HeadSHA, QueuedAt: time.Now(), CompletedAt: time.Now()},
	}
	if got := reviewCycleExpectedMaxQuietDuration(cycle); got != reviewCycleDiagnosisFallbackQuietWindow {
		t.Fatalf("reviewCycleExpectedMaxQuietDuration(no convergence yet) = %s, want %s", got, reviewCycleDiagnosisFallbackQuietWindow)
	}
	cycle.Convergence = &ReviewConvergenceState{
		Rounds: []ReviewConvergenceRoundState{{Round: 1, Outcome: ReviewConvergenceRoundQuiet}},
	}
	if got := reviewCycleExpectedMaxQuietDuration(cycle); got != reviewCycleDiagnosisFallbackQuietWindow {
		t.Fatalf("reviewCycleExpectedMaxQuietDuration(settled round) = %s, want %s", got, reviewCycleDiagnosisFallbackQuietWindow)
	}
}

func TestReviewCycleExpectedMaxQuietDurationActiveRound(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	cycle.DiscoveryPasses = []ReviewDiscoveryPassState{
		{Pass: 1, HeadSHA: cycle.HeadSHA, QueuedAt: time.Now(), CompletedAt: time.Now()},
	}
	cycle.Convergence = &ReviewConvergenceState{
		Rounds: []ReviewConvergenceRoundState{{Round: 1, Outcome: ReviewConvergenceRoundActive}},
	}
	verificationAttempts := max(1, cycle.Policy.Verification.Retries+1)
	verificationBudget := time.Duration(cycle.Policy.Verification.TimeoutMinutes) * time.Duration(verificationAttempts) * time.Minute
	swarmAttempts := max(1, cycle.Policy.Swarm.Retries+1)
	discoveryBudget := time.Duration(cycle.Policy.Swarm.TimeoutMinutes) * time.Duration(swarmAttempts) * 2 * time.Minute
	want := verificationBudget
	if discoveryBudget > want {
		want = discoveryBudget
	}
	if got := reviewCycleExpectedMaxQuietDuration(cycle); got != want {
		t.Fatalf("reviewCycleExpectedMaxQuietDuration(active round) = %s, want %s", got, want)
	}
}

// TestSummarizeReviewCycleProgressExcludesSupersededAssignments guards
// against a real staleness bug found while rebasing onto PR #130 (issue
// #128's landed fix): a verification/challenge assignment can be
// Superseded while still nominally Queued/Running (its worker, if any,
// simply hasn't reported back yet -- its eventual completion will be
// rejected as a conflict). The progress summary must not count it as
// active work still in flight.
func TestSummarizeReviewCycleProgressExcludesSupersededAssignments(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	cycle.DiscoveryPasses = []ReviewDiscoveryPassState{
		{Pass: 1, HeadSHA: cycle.HeadSHA, QueuedAt: time.Now(), CompletedAt: time.Now()},
	}
	cycle.Convergence = &ReviewConvergenceState{
		Rounds: []ReviewConvergenceRoundState{{Round: 1, Outcome: ReviewConvergenceRoundActive}},
		VerificationAssignments: []ReviewVerificationAssignment{
			{Status: ReviewVerificationRunning, Superseded: true},
			{Status: ReviewVerificationQueued, Superseded: false},
		},
		ChallengeAssignments: []ReviewChallengeAssignment{
			{Status: ReviewChallengeRunning, Superseded: true},
		},
	}
	got := summarizeReviewCycleProgress(cycle)
	if !strings.Contains(got, "1 verification(s) + 0 challenge(s) active") {
		t.Fatalf("summarizeReviewCycleProgress() = %q, want superseded assignments excluded from the active counts", got)
	}
}

func TestSummarizeReviewCycleProgressDiscoveryRunning(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	cycle.DiscoveryPasses = []ReviewDiscoveryPassState{
		{
			Pass: 1, HeadSHA: cycle.HeadSHA, QueuedAt: time.Now(),
			Lanes: []ReviewDiscoveryLaneState{
				{Lane: "contract", Required: true, Status: ReviewDiscoveryLaneCompleted, CompletedAt: time.Now()},
				{Lane: "callers", Required: true, Status: ReviewDiscoveryLaneQueued},
			},
		},
	}
	got := summarizeReviewCycleProgress(cycle)
	if got != "discovery pass 1 running (1/2 lanes done)" {
		t.Fatalf("summarizeReviewCycleProgress() = %q", got)
	}
}

func TestSummarizeReviewCycleProgressNoDiscoveryYet(t *testing.T) {
	cycle := newReviewLimitTestCycle(t, nil)
	if got := summarizeReviewCycleProgress(cycle); got != "discovery not yet started" {
		t.Fatalf("summarizeReviewCycleProgress(none) = %q", got)
	}
}

func TestFormatReviewCycleHealthSummaryHandlesMultipleReviewers(t *testing.T) {
	now := time.Now()
	agents := []Agent{
		{ID: "review-agent-1", Role: RoleReviewer, PRNumber: 100, State: StateWorking},
		{
			ID: "review-agent-2", Role: RoleReviewer, PRNumber: 200,
			State: StateReviewGate, ReviewCycle: newReviewLimitTestCycle(t, nil),
			LastActivityTime: now.Add(-(reviewLaunchStallTimeout + time.Minute)),
		},
		{ID: "coding-agent-1", Role: RoleCoder, PRNumber: 100},
	}
	summary := formatReviewCycleHealthSummary(agents, now)
	if strings.Count(summary, "reviewer=") != 2 {
		t.Fatalf("expected exactly two reviewer lines (not one, not zero): %q", summary)
	}
	if !strings.Contains(summary, "pr=#100") || !strings.Contains(summary, "pr=#200") {
		t.Fatalf("summary should identify each reviewer by PR: %q", summary)
	}
	if !strings.Contains(summary, "STUCK") {
		t.Fatalf("summary should flag the wedged reviewer: %q", summary)
	}
}

func TestFormatReviewCycleHealthSummaryNoReviewers(t *testing.T) {
	summary := formatReviewCycleHealthSummary([]Agent{{Role: RoleCoder}}, time.Now())
	if strings.TrimSpace(summary) != "none" {
		t.Fatalf("formatReviewCycleHealthSummary(no reviewers) = %q, want \"none\"", summary)
	}
}

func TestCheckReviewCycleHealthNotifiesOnlyOnTransition(t *testing.T) {
	var notifications []string
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		notifications = append(notifications, string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	bot := &Orchestrator{
		cfg:      Config{WebexWebhookURL: webex.URL},
		notifier: NewWebexNotifier(webex.URL),
		agents:   NewAgentManager(),
	}

	now := time.Now()
	cycle := newReviewLimitTestCycle(t, nil)
	agent := Agent{
		ID: "review-agent-980", Role: RoleReviewer, PRNumber: 990,
		State: StateReviewGate, ReviewCycle: cycle,
		LastActivityTime: now.Add(-(reviewLaunchStallTimeout + time.Minute)),
	}

	bot.checkReviewCycleHealth(context.Background(), []Agent{agent}, now)
	bot.checkReviewCycleHealth(context.Background(), []Agent{agent}, now)
	bot.checkReviewCycleHealth(context.Background(), []Agent{agent}, now)

	if len(notifications) != 1 {
		t.Fatalf("notify count = %d, want exactly 1 (only on the first transition to STUCK)", len(notifications))
	}

	record, ok := bot.reviewCycleLastDiagnosisRecord(agent.ID)
	if !ok || record.Pattern != reviewCycleDiagnosisReviewGateWedge || record.Healthy {
		t.Fatalf("tracked record = %+v (ok=%v), want {review_gate_wedge_after_restart, healthy=false}", record, ok)
	}
}

// TestCheckReviewCycleHealthNotifiesOnHealthyToStuckTransitionSamePattern is
// the regression test for independent review feedback on PR #132: several
// patterns (budget_exhausted, awaiting_verdict_publication, and other
// reviewCycleAwaitingCleanupDiagnosis-shaped ones) report the *same*
// Pattern name whether they're still healthy (within their grace window)
// or have become STUCK (past it) -- only the Healthy field differs.
// Tracking only the pattern would treat that transition as "no change"
// from the previous check and silently skip the notification for a
// reviewer that just became genuinely stuck.
func TestCheckReviewCycleHealthNotifiesOnHealthyToStuckTransitionSamePattern(t *testing.T) {
	var notifications []string
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		notifications = append(notifications, string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	bot := &Orchestrator{
		cfg:      Config{WebexWebhookURL: webex.URL},
		notifier: NewWebexNotifier(webex.URL),
		agents:   NewAgentManager(),
	}

	cycle := newReviewLimitTestCycle(t, nil)
	maximum := uint64(time.Duration(cycle.Policy.Convergence.MaxWallTimeMinutes) * time.Minute / time.Millisecond)
	cycle.LimitTransition = &ReviewLimitTransition{
		SchemaVersion:     reviewLimitTransitionSchemaVersion,
		Kind:              ReviewLimitWallTime,
		TransitionedAt:    time.Now(),
		Actual:            maximum + 500,
		Maximum:           maximum,
		Unit:              "milliseconds",
		Action:            cycle.Policy.FailureActions.BudgetExhaustion,
		Outcome:           reviewLimitOutcomeForAction(cycle.Policy.FailureActions.BudgetExhaustion),
		Reason:            "review wall time reached snapshotted maximum",
		MetricEventCount:  1,
		MetricEventDigest: strings.Repeat("a", 64),
	}
	now := time.Now()
	agent := Agent{
		ID: "review-agent-980", Role: RoleReviewer, PRNumber: 990,
		State: StateWorking, ReviewCycle: cycle,
	}

	// First check: freshly exhausted, still within the grace window --
	// healthy, no notification, but the pattern (budget_exhausted) gets
	// recorded.
	agent.LastActivityTime = now
	bot.checkReviewCycleHealth(context.Background(), []Agent{agent}, now)
	if len(notifications) != 0 {
		t.Fatalf("notify count after healthy check = %d, want 0", len(notifications))
	}
	record, ok := bot.reviewCycleLastDiagnosisRecord(agent.ID)
	if !ok || record.Pattern != reviewCycleDiagnosisBudgetExhausted || !record.Healthy {
		t.Fatalf("tracked record after healthy check = %+v (ok=%v), want {budget_exhausted, healthy=true}", record, ok)
	}

	// Second check: same reviewer, same pattern, but now past the grace
	// window -- genuinely STUCK. This must notify even though the pattern
	// name hasn't changed, since the previous check was healthy.
	agent.LastActivityTime = now.Add(-(reviewCycleCleanupGraceWindow + time.Minute))
	bot.checkReviewCycleHealth(context.Background(), []Agent{agent}, now)
	if len(notifications) != 1 {
		t.Fatalf("notify count after healthy-to-stuck transition = %d, want 1", len(notifications))
	}
	record, ok = bot.reviewCycleLastDiagnosisRecord(agent.ID)
	if !ok || record.Pattern != reviewCycleDiagnosisBudgetExhausted || record.Healthy {
		t.Fatalf("tracked record after stuck check = %+v (ok=%v), want {budget_exhausted, healthy=false}", record, ok)
	}

	// Third check: still stuck, same pattern -- must not renotify.
	bot.checkReviewCycleHealth(context.Background(), []Agent{agent}, now)
	if len(notifications) != 1 {
		t.Fatalf("notify count after repeated stuck check = %d, want still 1 (no duplicate notification)", len(notifications))
	}
}

func TestCheckReviewCycleHealthPrunesRetiredReviewers(t *testing.T) {
	bot := &Orchestrator{agents: NewAgentManager()}
	now := time.Now()
	cycle := newReviewLimitTestCycle(t, nil)
	agent := Agent{
		ID: "review-agent-980", Role: RoleReviewer, PRNumber: 990,
		State: StateReviewGate, ReviewCycle: cycle,
		LastActivityTime: now.Add(-(reviewLaunchStallTimeout + time.Minute)),
	}
	bot.checkReviewCycleHealth(context.Background(), []Agent{agent}, now)
	if _, ok := bot.reviewCycleLastDiagnosisRecord(agent.ID); !ok {
		t.Fatal("expected diagnosis to be tracked after first check")
	}
	bot.checkReviewCycleHealth(context.Background(), nil, now)
	if _, ok := bot.reviewCycleLastDiagnosisRecord(agent.ID); ok {
		t.Fatal("expected tracking to be pruned once the reviewer is no longer active")
	}
}
