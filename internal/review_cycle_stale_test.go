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
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeadChangeBeforeWorkerDispatchInvalidatesOnce(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	var successorLaunches atomic.Int32
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
		reviewHeadResolver: func(
			context.Context,
			int,
		) (string, error) {
			return testOtherReviewHeadSHA, nil
		},
		reviewSuccessorLauncher: func(
			context.Context,
			Agent,
			string,
		) error {
			successorLaunches.Add(1)
			return nil
		},
	}

	_, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   "do not dispatch stale work",
		},
	)
	if !errors.Is(err, errReviewCycleStale) {
		t.Fatalf("launchReviewWorker() error = %v, want stale cycle", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("stale worker dispatches = %d, want 0", got)
	}
	current, ok := agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatal("stale review cycle disappeared")
	}
	if !current.ReviewCycle.Stale ||
		current.ReviewCycle.SupersededByHeadSHA !=
			testOtherReviewHeadSHA ||
		!current.Stopped ||
		current.State != StateStopped {
		t.Fatalf("stale cycle state = %#v, agent=%#v", current.ReviewCycle, current)
	}
	if got := len(current.ReviewCycle.WorkerOwnerships); got != 0 {
		t.Fatalf("stale worker ownerships = %d, want 0", got)
	}

	if _, err := bot.ensureReviewCycleHeadCurrent(
		context.Background(),
		reviewer.ID,
	); !errors.Is(err, errReviewCycleStale) {
		t.Fatalf("repeated head observation error = %v, want stale", err)
	}
	if got := successorLaunches.Load(); got != 1 {
		t.Fatalf("successor launches = %d, want 1", got)
	}
	bot.waitForStaleReviewCleanups()
	audit, ok := agents.Get(reviewer.ID)
	if !ok || audit.ReviewCycle == nil || !audit.ReviewCycle.Stale ||
		audit.ReviewCycle.SupersededByHeadSHA != testOtherReviewHeadSHA {
		t.Fatalf("stale cycle audit was not preserved: %#v", audit)
	}
}

func TestLateStaleArtifactCannotMutateSuccessorOrLedger(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	oldHeadSHA := harness.reviewer.ReviewCycle.HeadSHA
	newHeadSHA := testOtherReviewHeadSHA
	delta := testReviewLedgerDelta(
		testThirdReviewHeadSHA,
		oldHeadSHA,
		"tracked.txt",
	)
	ledgerResult, err := transitionReviewLedger(
		nil,
		delta,
		testReviewLedgerCoverageRequirements("tracked.txt"),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("transitionReviewLedger() error = %v", err)
	}
	coder := &Agent{
		ID:                  "coder-stale-artifact",
		Role:                RoleCoder,
		IssueNumber:         47,
		PRNumber:            147,
		ObservedPRHeadSHA:   oldHeadSHA,
		ActiveReviewAgentID: harness.reviewer.ID,
		ReviewLedger:        cloneReviewLedger(&ledgerResult.Ledger),
		State:               StateWaiting,
		LastActivityTime:    time.Now().UTC(),
	}
	if err := harness.agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	harness.agents.mu.Lock()
	storedReviewer := harness.agents.agents[harness.reviewer.ID]
	storedReviewer.ParentAgentID = coder.ID
	storedReviewer.PRNumber = coder.PRNumber
	storedReviewer.PRURL = "https://example.test/pull/147"
	harness.agents.mu.Unlock()
	harness.reviewer, _ = harness.agents.Get(harness.reviewer.ID)

	envelope := testDiscoveryArtifactEnvelope(
		t,
		harness.ownership,
		"late stale evidence",
	)
	artifactPath := harness.publish(t, envelope)
	ledgerBefore := cloneReviewLedger(coder.ReviewLedger)
	var successorID atomic.Value
	var successorLaunches atomic.Int32
	harness.bot.runner = &testIsolatedReviewWorkerRunner{}
	harness.bot.reviewHeadResolver = func(
		context.Context,
		int,
	) (string, error) {
		return newHeadSHA, nil
	}
	harness.bot.reviewSuccessorLauncher = func(
		_ context.Context,
		staleReviewer Agent,
		headSHA string,
	) error {
		successorLaunches.Add(1)
		cycle, cycleErr := newReviewCycleState(
			headSHA,
			staleReviewer.ReviewCycle.Policy,
		)
		if cycleErr != nil {
			return cycleErr
		}
		successor := &Agent{
			ID:                "review-successor-stale-artifact",
			Role:              RoleReviewer,
			ParentAgentID:     coder.ID,
			IssueNumber:       coder.IssueNumber,
			PRNumber:          coder.PRNumber,
			ObservedPRHeadSHA: headSHA,
			ReviewCycle:       cycle,
			State:             StateWorking,
			LastActivityTime:  time.Now().UTC(),
		}
		if addErr := harness.agents.Add(successor); addErr != nil {
			return addErr
		}
		if !harness.agents.SetActiveReviewAgent(coder.ID, successor.ID) {
			return errors.New("failed to link successor review cycle")
		}
		successorID.Store(successor.ID)
		return nil
	}

	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		artifactPath,
		ReviewArtifactPhaseDiscovery,
	); err == nil {
		t.Fatal("late stale artifact was accepted")
	}
	oldCycle, _ := harness.agents.Get(harness.reviewer.ID)
	if !oldCycle.ReviewCycle.Stale ||
		len(oldCycle.ReviewCycle.ArtifactReceipts) != 0 ||
		len(oldCycle.ReviewCycle.ArtifactFailures) != 0 ||
		len(oldCycle.ReviewCycle.LaneCompletions) != 0 {
		t.Fatalf("late artifact mutated stale cycle: %#v", oldCycle.ReviewCycle)
	}
	value := successorID.Load()
	if value == nil {
		t.Fatal("successor review cycle was not created")
	}
	successor, ok := harness.agents.Get(value.(string))
	if !ok || successor.ReviewCycle == nil {
		t.Fatal("successor review cycle disappeared")
	}
	if successor.ReviewCycle.HeadSHA != newHeadSHA ||
		len(successor.ReviewCycle.ArtifactReceipts) != 0 ||
		len(successor.ReviewCycle.ArtifactFailures) != 0 ||
		len(successor.ReviewCycle.LaneCompletions) != 0 {
		t.Fatalf("late artifact mutated successor cycle: %#v", successor.ReviewCycle)
	}
	currentCoder, _ := harness.agents.Get(coder.ID)
	if !reflect.DeepEqual(currentCoder.ReviewLedger, ledgerBefore) {
		t.Fatal("late stale artifact mutated the cross-SHA ledger")
	}
	if got := successorLaunches.Load(); got != 1 {
		t.Fatalf("successor launches = %d, want 1", got)
	}
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		artifactPath,
		ReviewArtifactPhaseDiscovery,
	); err == nil {
		t.Fatal("artifact arriving after invalidation was accepted")
	} else {
		assertReviewArtifactError(
			t,
			err,
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureStaleSHA,
		)
	}
	oldCycle, _ = harness.agents.Get(harness.reviewer.ID)
	currentCoder, _ = harness.agents.Get(coder.ID)
	if len(oldCycle.ReviewCycle.ArtifactReceipts) != 0 ||
		len(oldCycle.ReviewCycle.ArtifactFailures) != 0 ||
		!reflect.DeepEqual(currentCoder.ReviewLedger, ledgerBefore) {
		t.Fatal("artifact arriving after invalidation mutated durable review state")
	}
	if _, err := harness.bot.ensureReviewCycleHeadCurrent(
		context.Background(),
		harness.reviewer.ID,
	); !errors.Is(err, errReviewCycleStale) {
		t.Fatalf("repeated stale observation error = %v", err)
	}
	if got := successorLaunches.Load(); got != 1 {
		t.Fatalf("successor launches after repeat = %d, want 1", got)
	}
	harness.bot.waitForStaleReviewCleanups()
}

func TestStaleCycleIsNeverApprovalEligibleOrSentToVerdict(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	current, _ := harness.agents.Get(harness.reviewer.ID)
	cycle := cloneReviewCycle(current.ReviewCycle)
	for round := 0; round < cycle.Policy.Convergence.QuietRoundsRequired; round++ {
		if round > 0 {
			appendCompletedDiscoveryPassForState(
				cycle,
				round+1,
				time.Now().UTC(),
			)
		}
		if _, err := beginReviewConvergenceRoundState(
			cycle,
			time.Now().UTC(),
		); err != nil {
			t.Fatalf("beginReviewConvergenceRoundState() error = %v", err)
		}
		if _, err := completeReviewConvergenceRoundState(
			cycle,
			time.Now().UTC(),
		); err != nil {
			t.Fatalf("completeReviewConvergenceRoundState() error = %v", err)
		}
	}
	if !cycle.ApprovalEligible() {
		t.Fatal("converged current cycle is not approval eligible")
	}
	cycle.Stale = true
	cycle.StaleAt = time.Now().UTC()
	cycle.SupersededByHeadSHA = testOtherReviewHeadSHA
	if cycle.ApprovalEligible() {
		t.Fatal("stale cycle remained approval eligible")
	}
	if findings := cycle.PublishableFindings(); len(findings) != 0 {
		t.Fatalf("stale publishable findings = %#v, want none", findings)
	}
	if _, err := beginReviewConvergenceRoundState(
		cycle,
		time.Now().UTC(),
	); !errors.Is(err, errReviewCycleStale) {
		t.Fatalf("stale convergence start error = %v, want stale", err)
	}

	harness.bot.runner = &testIsolatedReviewWorkerRunner{}
	harness.bot.reviewHeadResolver = func(
		context.Context,
		int,
	) (string, error) {
		return testOtherReviewHeadSHA, nil
	}
	var successorLaunches atomic.Int32
	harness.bot.reviewSuccessorLauncher = func(
		context.Context,
		Agent,
		string,
	) error {
		successorLaunches.Add(1)
		return nil
	}

	eligible, err := harness.bot.reviewCycleApprovalEligible(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("reviewCycleApprovalEligible() error = %v", err)
	}
	if eligible {
		t.Fatal("moved-head cycle reached approval eligibility")
	}
	staleReviewer, _ := harness.agents.Get(harness.reviewer.ID)
	cycle = staleReviewer.ReviewCycle
	if !cycle.Stale || cycle.SupersededByHeadSHA != testOtherReviewHeadSHA {
		t.Fatalf("approval boundary did not invalidate cycle: %#v", cycle)
	}
	if err := harness.bot.handleReviewVerdict(
		context.Background(),
		staleReviewer,
		ReviewVerdictThumbsUp,
		nil,
	); err != nil {
		t.Fatalf("handleReviewVerdict(stale) error = %v", err)
	}
	afterVerdict, _ := harness.agents.Get(harness.reviewer.ID)
	if !afterVerdict.ReviewCycle.Stale ||
		afterVerdict.State != StateStopped ||
		!afterVerdict.Stopped {
		t.Fatalf("stale verdict mutated reviewer: %#v", afterVerdict)
	}
	if got := successorLaunches.Load(); got != 1 {
		t.Fatalf("successor launches = %d, want 1", got)
	}
	harness.bot.waitForStaleReviewCleanups()
}

func TestStaleInvalidationCancelsConcurrentDiscoveryWorkers(t *testing.T) {
	bot, reviewer := newReviewDiscoverySchedulerHarness(t, 2)
	if !bot.agents.SetPR(
		reviewer.ID,
		247,
		"stale discovery",
		"https://example.test/pull/247",
		reviewer.ReviewCycle.HeadSHA,
	) {
		t.Fatal("SetPR(reviewer) = false")
	}
	var liveHead atomic.Value
	liveHead.Store(reviewer.ReviewCycle.HeadSHA)
	bot.reviewHeadResolver = func(
		context.Context,
		int,
	) (string, error) {
		return liveHead.Load().(string), nil
	}
	var successorLaunches atomic.Int32
	bot.reviewSuccessorLauncher = func(
		context.Context,
		Agent,
		string,
	) error {
		successorLaunches.Add(1)
		return nil
	}
	bot.runner = &testIsolatedReviewWorkerRunner{}
	started := make(chan struct{}, 4)
	bot.reviewDiscoveryLaneRunner = func(
		ctx context.Context,
		_ string,
		_ int,
		_ string,
	) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		_, err := bot.runReviewDiscoveryPass(
			context.Background(),
			reviewer.ID,
			1,
		)
		done <- err
	}()
	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent discovery workers did not start")
		}
	}
	liveHead.Store(testOtherReviewHeadSHA)
	if _, err := bot.ensureReviewCycleHeadCurrent(
		context.Background(),
		reviewer.ID,
	); !errors.Is(err, errReviewCycleStale) {
		t.Fatalf("stale discovery observation error = %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled stale discovery unexpectedly succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stale discovery workers did not drain")
	}
	if got := successorLaunches.Load(); got != 1 {
		t.Fatalf("successor launches = %d, want 1", got)
	}
	bot.waitForStaleReviewCleanups()
}

func TestStaleInvalidationCancelsConcurrentVerificationWorkers(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 2, true)
	if !harness.agents.SetPR(
		harness.reviewer.ID,
		347,
		"stale verification",
		"https://example.test/pull/347",
		harness.reviewer.ReviewCycle.HeadSHA,
	) {
		t.Fatal("SetPR(reviewer) = false")
	}
	var liveHead atomic.Value
	liveHead.Store(harness.reviewer.ReviewCycle.HeadSHA)
	harness.bot.reviewHeadResolver = func(
		context.Context,
		int,
	) (string, error) {
		return liveHead.Load().(string), nil
	}
	var successorLaunches atomic.Int32
	harness.bot.reviewSuccessorLauncher = func(
		context.Context,
		Agent,
		string,
	) error {
		successorLaunches.Add(1)
		return nil
	}
	harness.bot.runner = &testIsolatedReviewWorkerRunner{}
	started := make(chan struct{}, 4)
	harness.bot.reviewVerificationRunner = func(
		ctx context.Context,
		_ string,
		_ ReviewVerificationAssignment,
	) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		_, err := harness.bot.runReviewConvergentRound(
			context.Background(),
			harness.reviewer.ID,
		)
		done <- err
	}()
	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent verification workers did not start")
		}
	}
	liveHead.Store(testOtherReviewHeadSHA)
	if _, err := harness.bot.ensureReviewCycleHeadCurrent(
		context.Background(),
		harness.reviewer.ID,
	); !errors.Is(err, errReviewCycleStale) {
		t.Fatalf("stale verification observation error = %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled stale verification unexpectedly succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stale verification workers did not drain")
	}
	if got := successorLaunches.Load(); got != 1 {
		t.Fatalf("successor launches = %d, want 1", got)
	}
	harness.bot.waitForStaleReviewCleanups()
}
