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
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestReviewCycleStatusShowsExactSafeWorkerRoutingAndAccounting(
	t *testing.T,
) {
	agents := NewAgentManager()
	reviewer, identity :=
		newReviewWorkerTestCoordinator(t, agents)
	ownership, _, err := agents.allocateReviewWorkerOwnership(
		reviewer.ID,
		identity,
		t.TempDir(),
		strings.Repeat("a", reviewWorkerIdentityTokenBytes*2),
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership() error = %v", err)
	}
	if _, _, err := agents.transitionReviewWorkerLifecycle(
		reviewer.ID,
		ownership.OwnerID,
		ReviewWorkerRunning,
		nil,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("transitionReviewWorkerLifecycle(running) error = %v", err)
	}
	if _, _, err := agents.transitionReviewWorkerLifecycle(
		reviewer.ID,
		ownership.OwnerID,
		ReviewWorkerCompleted,
		nil,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("transitionReviewWorkerLifecycle(completed) error = %v", err)
	}
	current, _ := agents.Get(reviewer.ID)
	observation, err := observableReviewCycle(current)
	if err != nil {
		t.Fatalf("observableReviewCycle() error = %v", err)
	}
	if observation.Counts.Reserved != 1 ||
		observation.Counts.Completed != 1 ||
		observation.Counts.MaxParallelObserved != 1 ||
		!observation.WithinBounds {
		t.Fatalf("worker counts = %#v", observation.Counts)
	}
	status := formatReviewCyclesStatus([]Agent{current})
	for _, expected := range []string{
		current.ReviewCycle.HeadSHA,
		`assignment="discovery:1:contract"`,
		`role=discovery`,
		`lane="contract"`,
		`profile="discovery"`,
		`model="gpt-5.6-terra"`,
		`effort="high"`,
		`attempt=1`,
		`lifecycle=completed`,
		`planned=0 reserved=1 running=0 completed=1 failed=0 cancelled=0`,
		`reviewers=3..6 reviewer_parallel=6`,
		`configured_max_agents_per_sha=50 required_max_agents_per_sha=0 max_agents_per_sha=50`,
	} {
		if !strings.Contains(status, expected) {
			t.Fatalf(
				"status is missing %q:\n%s",
				expected,
				status,
			)
		}
	}
}

func TestReviewCycleObservabilityOmitsPromptsDiffsAndSecrets(
	t *testing.T,
) {
	const secret = "TOP-SECRET-REVIEW-CYCLE-MARKER"
	agents := NewAgentManager()
	reviewer, identity :=
		newReviewWorkerTestCoordinator(t, agents)
	reviewer.IssueTitle = secret
	reviewer.IssueBody = "prompt diff source excerpt " + secret
	agents.agents[reviewer.ID].IssueTitle = reviewer.IssueTitle
	agents.agents[reviewer.ID].IssueBody = reviewer.IssueBody
	if _, _, err := agents.allocateReviewWorkerOwnership(
		reviewer.ID,
		identity,
		t.TempDir(),
		strings.Repeat("b", reviewWorkerIdentityTokenBytes*2),
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("allocateReviewWorkerOwnership() error = %v", err)
	}
	current, _ := agents.Get(reviewer.ID)
	outputs := []string{
		formatReviewCyclesStatus([]Agent{current}),
		formatReviewCycleLogRecord(
			current,
			"worker_reserved",
			time.Now().UTC(),
		),
	}
	for _, output := range outputs {
		for _, forbidden := range []string{
			secret,
			"prompt",
			"diff",
			"source excerpt",
			"authorization",
		} {
			if strings.Contains(output, forbidden) {
				t.Fatalf(
					"safe review-cycle output exposed %q: %s",
					forbidden,
					output,
				)
			}
		}
	}
}

func TestReviewCycleObservabilityChecksRoleSpecificParallelBounds(
	t *testing.T,
) {
	agents := NewAgentManager()
	reviewer, firstIdentity :=
		newReviewWorkerTestCoordinator(t, agents)
	secondIdentity := firstIdentity
	secondIdentity.Lane = "lifecycle"
	base := time.Now().UTC()
	first, _, err := agents.allocateReviewWorkerOwnership(
		reviewer.ID,
		firstIdentity,
		t.TempDir(),
		strings.Repeat("d", reviewWorkerIdentityTokenBytes*2),
		base,
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership(first) error = %v", err)
	}
	second, _, err := agents.allocateReviewWorkerOwnership(
		reviewer.ID,
		secondIdentity,
		t.TempDir(),
		strings.Repeat("e", reviewWorkerIdentityTokenBytes*2),
		base.Add(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership(second) error = %v", err)
	}
	for _, worker := range []ReviewWorkerOwnership{first, second} {
		if _, _, err := agents.transitionReviewWorkerLifecycle(
			reviewer.ID,
			worker.OwnerID,
			ReviewWorkerRunning,
			nil,
			base.Add(2*time.Millisecond),
		); err != nil {
			t.Fatalf(
				"transitionReviewWorkerLifecycle(%s, running) error = %v",
				worker.OwnerID,
				err,
			)
		}
	}
	for _, worker := range []ReviewWorkerOwnership{first, second} {
		if _, _, err := agents.transitionReviewWorkerLifecycle(
			reviewer.ID,
			worker.OwnerID,
			ReviewWorkerCompleted,
			nil,
			base.Add(3*time.Millisecond),
		); err != nil {
			t.Fatalf(
				"transitionReviewWorkerLifecycle(%s, completed) error = %v",
				worker.OwnerID,
				err,
			)
		}
	}
	current, _ := agents.Get(reviewer.ID)
	current.ReviewCycle.Policy.Swarm.MaxParallelReviewers = 1
	current.ReviewCycle.Policy.Verification.MaxParallelVerifiers = 3
	observation, err := observableReviewCycle(current)
	if err != nil {
		t.Fatalf("observableReviewCycle() error = %v", err)
	}
	if observation.Counts.MaxReviewerParallelObserved != 2 ||
		observation.Counts.MaxVerifierParallelObserved != 0 ||
		observation.WithinBounds {
		t.Fatalf(
			"role-specific parallel accounting = %#v within_bounds=%t",
			observation.Counts,
			observation.WithinBounds,
		)
	}
}

func TestReviewCycleObservabilityAccountsChallengeAsReviewerWork(
	t *testing.T,
) {
	agents := NewAgentManager()
	reviewer, _ := newReviewWorkerTestCoordinator(t, agents)
	base := time.Now().UTC()
	identities := []ReviewWorkerIdentity{}
	for _, role := range []AgentProfileRole{
		AgentProfileRoleChallenge,
		AgentProfileRoleVerifier,
	} {
		identity, err := reviewWorkerIdentityForCycle(
			reviewer.ReviewCycle,
			role,
			1,
			string(role)+"-parallel",
		)
		if err != nil {
			t.Fatalf("reviewWorkerIdentityForCycle(%s) error = %v", role, err)
		}
		identities = append(identities, identity)
	}
	workers := make([]ReviewWorkerOwnership, 0, len(identities))
	for index, identity := range identities {
		worker, _, err := agents.allocateReviewWorkerOwnership(
			reviewer.ID,
			identity,
			t.TempDir(),
			fmt.Sprintf("%032x", index+1),
			base,
		)
		if err != nil {
			t.Fatalf("allocateReviewWorkerOwnership(%s) error = %v", identity.Role, err)
		}
		workers = append(workers, worker)
	}
	for _, worker := range workers {
		if _, _, err := agents.transitionReviewWorkerLifecycle(
			reviewer.ID,
			worker.OwnerID,
			ReviewWorkerRunning,
			nil,
			base.Add(time.Millisecond),
		); err != nil {
			t.Fatalf("transitionReviewWorkerLifecycle(%s, running) error = %v", worker.Identity.Role, err)
		}
	}
	for _, worker := range workers {
		if _, _, err := agents.transitionReviewWorkerLifecycle(
			reviewer.ID,
			worker.OwnerID,
			ReviewWorkerCompleted,
			nil,
			base.Add(2*time.Millisecond),
		); err != nil {
			t.Fatalf("transitionReviewWorkerLifecycle(%s, completed) error = %v", worker.Identity.Role, err)
		}
	}
	current, _ := agents.Get(reviewer.ID)
	current.ReviewCycle.Policy.Swarm.MaxParallelReviewers = 1
	current.ReviewCycle.Policy.Verification.MaxParallelVerifiers = 1
	observation, err := observableReviewCycle(current)
	if err != nil {
		t.Fatalf("observableReviewCycle() error = %v", err)
	}
	if observation.Counts.MaxReviewerParallelObserved != 1 ||
		observation.Counts.MaxVerifierParallelObserved != 1 ||
		!observation.WithinBounds {
		t.Fatalf(
			"challenge/verifier parallel accounting = %#v within_bounds=%t",
			observation.Counts,
			observation.WithinBounds,
		)
	}
}

func TestReviewCycleResultRecordsNoReviewPossibleAfterEarlyStop(t *testing.T) {
	agents := NewAgentManager()
	reviewer, _ := newReviewWorkerTestCoordinator(t, agents)
	reviewer.State = StateStopped
	reviewer.Stopped = true
	agents.agents[reviewer.ID].State = StateStopped
	agents.agents[reviewer.ID].Stopped = true
	current, _ := agents.Get(reviewer.ID)
	observation, err := observableReviewCycle(current)
	if err != nil {
		t.Fatalf("observableReviewCycle() error = %v", err)
	}
	if observation.ResultState != ReviewCycleResultNoReviewPossible {
		t.Fatalf("observed result = %q, want no_review_possible", observation.ResultState)
	}
}

func TestReviewCycleResultSeparatesCoverageFromFindings(t *testing.T) {
	cleanHarness := newTerminalReviewVerdictHarness(t)
	completeClean := cloneReviewCycle(cleanHarness.reviewer.ReviewCycle)
	staleComplete := cloneReviewCycle(completeClean)
	staleComplete.Stale = true

	findingHarness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, findingHarness)
	completeConvergenceVerification(
		t,
		findingHarness,
		assignment,
		ReviewVerificationConfirmed,
	)
	if _, err := findingHarness.bot.completeReviewConvergenceRound(
		findingHarness.reviewer.ID,
	); err != nil {
		t.Fatalf("completeReviewConvergenceRound() error = %v", err)
	}
	withFinding, _ := findingHarness.agents.Get(findingHarness.reviewer.ID)
	completeWithFindings := cloneReviewCycle(withFinding.ReviewCycle)

	partialNoFindings := cloneReviewCycle(completeClean)
	if len(partialNoFindings.ArtifactReceipts) < 6 {
		t.Fatalf(
			"terminal fixture accepted %d workers, want at least six",
			len(partialNoFindings.ArtifactReceipts),
		)
	}
	partialNoFindings.ArtifactReceipts = append(
		[]ReviewArtifactReceipt(nil),
		partialNoFindings.ArtifactReceipts[:6]...,
	)
	partialNoFindings.Convergence.Status = ReviewConvergenceUnresolved
	partialNoFindings.Convergence.ChallengeAssignments =
		[]ReviewChallengeAssignment{{Status: ReviewChallengeFailed}}

	partialWithFindings := cloneReviewCycle(completeWithFindings)
	partialWithFindings.Convergence.Status = ReviewConvergenceUnresolved
	partialWithFindings.Convergence.ChallengeAssignments =
		[]ReviewChallengeAssignment{{Status: ReviewChallengeFailed}}

	noReviewPossible := cloneReviewCycle(partialNoFindings)
	noReviewPossible.ArtifactReceipts = nil

	for _, test := range []struct {
		name  string
		cycle *ReviewCycleState
		want  ReviewCycleResultState
	}{
		{"complete clean", completeClean, ReviewCycleResultCompleteClean},
		{"stale complete clean", staleComplete, ReviewCycleResultCompleteClean},
		{"complete with findings", completeWithFindings, ReviewCycleResultCompleteWithFindings},
		{"six receipts and failed challenge", partialNoFindings, ReviewCycleResultPartialNoFindings},
		{"finding and failed challenge", partialWithFindings, ReviewCycleResultPartialWithFindings},
		{"no trusted result", noReviewPossible, ReviewCycleResultNoReviewPossible},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyReviewCycleResult(test.cycle); got != test.want {
				t.Fatalf("classifyReviewCycleResult() = %q, want %q", got, test.want)
			}
		})
	}

	reviewer := cleanHarness.reviewer
	reviewer.ReviewCycle = partialNoFindings
	reviewer.State = StateStopped
	reviewer.Stopped = true
	persisted := toPersistedAgent(&reviewer)
	if persisted.ReviewCycle.ResultState != ReviewCycleResultPartialNoFindings {
		t.Fatalf("persisted result = %q", persisted.ReviewCycle.ResultState)
	}
	if err := validatePersistedReviewCycleSnapshots(
		[]persistedAgent{persisted},
	); err != nil {
		t.Fatalf("validatePersistedReviewCycleSnapshots() error = %v", err)
	}
	restored := persisted.toAgent()
	if got := classifyReviewCycleResult(restored.ReviewCycle); got != persisted.ReviewCycle.ResultState {
		t.Fatalf("restored result = %q, want %q", got, persisted.ReviewCycle.ResultState)
	}
	persisted.ReviewCycle.ResultState = ReviewCycleResultCompleteClean
	if err := validatePersistedReviewCycleSnapshots(
		[]persistedAgent{persisted},
	); err == nil {
		t.Fatal("persisted complete result accepted unresolved challenge")
	}
}
