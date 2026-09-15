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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type reviewLifecycleRecordingRunner struct {
	mu                   sync.Mutex
	stopCalls            map[string]int
	runtimeStateRemovals map[string]int
	started              []Agent
}

func newReviewLifecycleRecordingRunner() *reviewLifecycleRecordingRunner {
	return &reviewLifecycleRecordingRunner{
		stopCalls:            make(map[string]int),
		runtimeStateRemovals: make(map[string]int),
	}
}

func (r *reviewLifecycleRecordingRunner) Start(
	agent Agent,
	_ string,
) (RuntimeHandle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = append(r.started, agent)
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: tmuxSessionName(agent),
	}, nil
}

func (r *reviewLifecycleRecordingRunner) Send(
	RuntimeHandle,
	string,
) error {
	return nil
}

func (r *reviewLifecycleRecordingRunner) Capture(
	RuntimeHandle,
	int,
) (string, error) {
	return "", nil
}

func (r *reviewLifecycleRecordingRunner) Stop(
	handle RuntimeHandle,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopCalls[handle.Session]++
	return nil
}

func (r *reviewLifecycleRecordingRunner) IsAlive(
	RuntimeHandle,
) (bool, error) {
	return true, nil
}

func (r *reviewLifecycleRecordingRunner) RemoveRuntimeState(
	agent Agent,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runtimeStateRemovals[agent.ID]++
	return nil
}

func (r *reviewLifecycleRecordingRunner) stopCallSnapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := make(map[string]int, len(r.stopCalls))
	for session, count := range r.stopCalls {
		snapshot[session] = count
	}
	return snapshot
}

func (r *reviewLifecycleRecordingRunner) runtimeStateRemovalSnapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := make(map[string]int, len(r.runtimeStateRemovals))
	for agentID, count := range r.runtimeStateRemovals {
		snapshot[agentID] = count
	}
	return snapshot
}

func TestReviewCoordinatorCleanupRemovesCoordinatorAndWorkerCodexState(
	t *testing.T,
) {
	bot, reviewer, identity, runner := newReviewLifecycleHarness(t)
	ownership := prepareReviewLifecycleWorker(t, bot, reviewer, identity)

	if err := bot.CleanupAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("CleanupAgent() error = %v", err)
	}
	want := map[string]int{
		reviewer.ID:       1,
		ownership.OwnerID: 1,
	}
	if got := runner.runtimeStateRemovalSnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime state removals = %#v, want %#v", got, want)
	}
	current, ok := bot.agents.Get(reviewer.ID)
	if !ok || current.ReviewCoordinatorLifecycle == nil {
		t.Fatal("completed review coordinator lifecycle is missing")
	}
	state := current.ReviewCoordinatorLifecycle
	if state.CodexHomeReleasedAt.IsZero() ||
		len(state.WorkerCleanups) != 1 ||
		state.WorkerCleanups[0].CodexHomeReleasedAt.IsZero() {
		t.Fatalf("Codex cleanup audit state = %#v", state)
	}
}

// TestCleanupAgentReleasesLinkedCoderLaunchAttempt is the regression test
// for a real production incident: a reviewer
// cleaned up via CleanupAgent (not retireReviewer) without ever
// producing a publishable verdict -- e.g. its challenge lanes exhausted
// retries -- left its linked coder's own review_coordinator
// DurableLaunchAttempt permanently non-terminal (Running). Every future
// review launch for that exact head then reused the same dead owner_id
// via reserveReviewLaunchAttempt (which only mints a fresh attempt once
// the latest one for that head is terminal), and AgentManager.Add
// rejected it forever since the original reviewer's agent record is
// never purged on cleanup. retireReviewer already calls
// cancelLinkedReviewLaunchOnRetirement to avoid exactly this; CleanupAgent
// did not.
func TestCleanupAgentReleasesLinkedCoderLaunchAttempt(t *testing.T) {
	bot, reviewer, identity, _ := newReviewLifecycleHarness(t)
	_ = identity

	coder := &Agent{
		ID:                  "coding-agent-982",
		Role:                RoleCoder,
		IssueNumber:         982,
		PRNumber:            1054,
		State:               StateWaiting,
		ObservedPRHeadSHA:   testReviewHeadSHA,
		ActiveReviewAgentID: reviewer.ID,
		LastActivityTime:    time.Now(),
	}
	attempt, err := newDurableLaunchAttempt(
		reviewer.ReviewCycle.ID,
		DurableLaunchReviewCoordinator,
		testReviewHeadSHA,
		1,
		reviewer.ID,
		"",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("newDurableLaunchAttempt() error = %v", err)
	}
	if _, err := transitionDurableLaunchAttempt(
		&attempt,
		DurableLaunchRunning,
		nil,
		time.Time{},
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	coder.LaunchAttempts = []DurableLaunchAttempt{attempt}
	if err := bot.agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	bot.agents.mu.Lock()
	bot.agents.agents[reviewer.ID].ParentAgentID = coder.ID
	bot.agents.mu.Unlock()

	if err := bot.CleanupAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("CleanupAgent() error = %v", err)
	}

	updatedCoder, ok := bot.agents.Get(coder.ID)
	if !ok {
		t.Fatal("coder disappeared after reviewer cleanup")
	}
	if len(updatedCoder.LaunchAttempts) != 1 {
		t.Fatalf("coder launch attempts = %#v, want exactly one", updatedCoder.LaunchAttempts)
	}
	if lifecycle := updatedCoder.LaunchAttempts[0].Lifecycle; !durableLaunchTerminal(lifecycle) {
		t.Fatalf(
			"linked launch attempt lifecycle = %q, want terminal (would otherwise permanently block relaunch for this head)",
			lifecycle,
		)
	}
}

// TestCleanupAgentRetriesLinkedLaunchAttemptCancellationAfterAlreadyCompletedLifecycle
// is the regression test for review feedback: the
// original fix called cancelLinkedReviewLaunchOnRetirement only after
// the `!result.Changed` early return, so it never ran again once the
// reviewer's own lifecycle transition was already complete. That is
// exactly the shape left behind by a transient persistence failure: if
// cancellation's own persistCoderLaunchAttemptMutation call fails on the
// first CleanupAgent invocation (e.g. a transient disk error), it rolls
// the coder's attempt back to "running" while the reviewer's lifecycle
// transition -- a separate persistence step that already succeeded --
// stays completed. Every subsequent CleanupAgent retry then hit
// `!result.Changed` and returned before cancellation was ever attempted
// again, permanently stranding the coder's launch attempt. This
// reproduces that exact shape directly (a completed reviewer lifecycle
// paired with a still-running linked attempt) rather
// than the transient persistence failure itself, and confirms a retried
// CleanupAgent call now finishes the cancellation instead of no-op'ing.
func TestCleanupAgentRetriesLinkedLaunchAttemptCancellationAfterAlreadyCompletedLifecycle(t *testing.T) {
	bot, reviewer, identity, _ := newReviewLifecycleHarness(t)
	_ = identity

	coder := &Agent{
		ID:                  "coding-agent-982",
		Role:                RoleCoder,
		IssueNumber:         982,
		PRNumber:            1054,
		State:               StateWaiting,
		ObservedPRHeadSHA:   testReviewHeadSHA,
		ActiveReviewAgentID: reviewer.ID,
		LastActivityTime:    time.Now(),
	}
	attempt, err := newDurableLaunchAttempt(
		reviewer.ReviewCycle.ID,
		DurableLaunchReviewCoordinator,
		testReviewHeadSHA,
		1,
		reviewer.ID,
		"",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("newDurableLaunchAttempt() error = %v", err)
	}
	if _, err := transitionDurableLaunchAttempt(
		&attempt,
		DurableLaunchRunning,
		nil,
		time.Time{},
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	coder.LaunchAttempts = []DurableLaunchAttempt{attempt}
	if err := bot.agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	bot.agents.mu.Lock()
	bot.agents.agents[reviewer.ID].ParentAgentID = coder.ID
	bot.agents.mu.Unlock()

	// First cleanup: completes the reviewer's own lifecycle transition
	// and cancels the linked attempt normally.
	if err := bot.CleanupAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("CleanupAgent() first call error = %v", err)
	}

	// Simulate exactly the shape a transient persistence failure would
	// leave behind: the reviewer's lifecycle transition already
	// completed (untouched below), but the coder's linked attempt is
	// non-terminal again, as if cancelLinkedReviewLaunchOnRetirement's
	// own persist step had failed and rolled it back.
	bot.agents.mu.Lock()
	resurrected := bot.agents.agents[coder.ID].LaunchAttempts[0]
	resurrected.Lifecycle = DurableLaunchRunning
	resurrected.FinishedAt = time.Time{}
	resurrected.RetryAfter = time.Time{}
	bot.agents.agents[coder.ID].LaunchAttempts[0] = resurrected
	bot.agents.mu.Unlock()

	current, ok := bot.agents.Get(reviewer.ID)
	if !ok || current.ReviewCoordinatorLifecycle == nil ||
		current.ReviewCoordinatorLifecycle.CompletedAt.IsZero() {
		t.Fatal("reviewer lifecycle is not actually completed; test setup does not match the reported shape")
	}

	// Retry: the reviewer's own lifecycle transition is already
	// complete (transitionReviewCoordinatorLifecycle will report
	// !result.Changed), but cancellation must still run and succeed.
	if err := bot.CleanupAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("CleanupAgent() retry error = %v, want nil (cancellation must still complete)", err)
	}

	updatedCoder, ok := bot.agents.Get(coder.ID)
	if !ok {
		t.Fatal("coder disappeared after reviewer cleanup retry")
	}
	if lifecycle := updatedCoder.LaunchAttempts[0].Lifecycle; !durableLaunchTerminal(lifecycle) {
		t.Fatalf(
			"linked launch attempt lifecycle after retry = %q, want terminal (cancellation was never retried after the already-completed lifecycle's early return)",
			lifecycle,
		)
	}
}

func persistedReviewCoordinatorForLifecycleTest(
	statePath string,
	reviewerID string,
) (Agent, error) {
	body, err := os.ReadFile(statePath)
	if err != nil {
		return Agent{}, err
	}
	var payload persistedStateFile
	if err := json.Unmarshal(body, &payload); err != nil {
		return Agent{}, err
	}
	for _, persisted := range payload.Agents {
		if persisted.ID == reviewerID {
			return persisted.toAgent(), nil
		}
	}
	return Agent{}, errors.New("persisted review coordinator was not found")
}

func prepareReviewLifecycleWorker(
	t *testing.T,
	bot *Orchestrator,
	reviewer Agent,
	identity ReviewWorkerIdentity,
) ReviewWorkerOwnership {
	t.Helper()
	ownership, err := bot.reserveReviewWorkerOwnership(
		reviewer.ID,
		identity,
	)
	if err != nil {
		t.Fatalf("reserveReviewWorkerOwnership() error = %v", err)
	}
	if err := os.MkdirAll(ownership.WorktreePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(worker worktree) error = %v", err)
	}
	if err := os.MkdirAll(ownership.GitHubConfigPath, 0o700); err != nil {
		t.Fatalf("MkdirAll(worker GitHub config) error = %v", err)
	}
	if _, err := prepareReviewWorkerArtifactDirectory(ownership); err != nil {
		t.Fatalf("prepareReviewWorkerArtifactDirectory() error = %v", err)
	}
	return ownership
}

func newReviewLifecycleHarness(
	t *testing.T,
) (
	*Orchestrator,
	Agent,
	ReviewWorkerIdentity,
	*reviewLifecycleRecordingRunner,
) {
	t.Helper()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	worktreeDir := t.TempDir()
	reviewer.WorktreePath = filepath.Join(worktreeDir, "coordinator")
	reviewer.RuntimeCWD = reviewer.WorktreePath
	reviewer.RuntimeHandle = RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: "review-lifecycle-coordinator",
	}
	if err := os.MkdirAll(reviewer.WorktreePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(coordinator worktree) error = %v", err)
	}
	agents.mu.Lock()
	stored := agents.agents[reviewer.ID]
	stored.WorktreePath = reviewer.WorktreePath
	stored.RuntimeCWD = reviewer.RuntimeCWD
	stored.RuntimeHandle = reviewer.RuntimeHandle
	agents.mu.Unlock()

	runner := newReviewLifecycleRecordingRunner()
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      t.TempDir(),
			WorktreeDir: worktreeDir,
		},
		agents: agents,
		runner: runner,
	}
	if err := bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}
	return bot, reviewer, identity, runner
}

func TestReviewCoordinatorConcurrentTerminalTransitionsUseOneDurableLifecycle(
	t *testing.T,
) {
	bot, reviewer, identity, runner := newReviewLifecycleHarness(t)
	first := prepareReviewLifecycleWorker(t, bot, reviewer, identity)
	second := prepareReviewLifecycleWorker(t, bot, reviewer, identity)

	var (
		cleanupMu     sync.Mutex
		cleanupCalls  = make(map[string]int)
		checkpointErr error
	)
	bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		persisted, err := persistedReviewCoordinatorForLifecycleTest(
			bot.agentStateFilePath(),
			reviewer.ID,
		)
		if err == nil {
			lifecycle := persisted.ReviewCoordinatorLifecycle
			if lifecycle == nil ||
				(lifecycle.Intent != ReviewCoordinatorLifecycleStop &&
					lifecycle.Intent != ReviewCoordinatorLifecycleCleanup) ||
				lifecycle.RequestedAt.IsZero() ||
				lifecycle.WorkerStopsAcknowledgedAt.IsZero() {
				err = errors.New(
					"worktree cleanup ran before durable stop intent and worker acknowledgement",
				)
			}
		}
		cleanupMu.Lock()
		if err != nil && checkpointErr == nil {
			checkpointErr = err
		}
		cleanupCalls[worktreePath]++
		cleanupMu.Unlock()
		return os.RemoveAll(worktreePath)
	}

	const callers = 12
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func(cleanup bool) {
			defer wg.Done()
			if cleanup {
				errs <- bot.CleanupAgent(
					context.Background(),
					reviewer.ID,
				)
				return
			}
			errs <- bot.StopAgent(context.Background(), reviewer.ID)
		}(index%2 == 0)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent terminal transition error = %v", err)
		}
	}

	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	if checkpointErr != nil {
		t.Fatal(checkpointErr)
	}
	for _, path := range []string{
		first.WorktreePath,
		second.WorktreePath,
		reviewer.WorktreePath,
	} {
		if got := cleanupCalls[path]; got != 1 {
			t.Fatalf("cleanup calls for %q = %d, want 1", path, got)
		}
	}
	for _, session := range []string{
		reviewer.RuntimeHandle.Session,
		first.SessionName,
		second.SessionName,
	} {
		if got := runner.stopCallSnapshot()[session]; got != 1 {
			t.Fatalf("stop calls for %q = %d, want 1", session, got)
		}
	}
	current, ok := bot.agents.Get(reviewer.ID)
	if !ok || current.ReviewCoordinatorLifecycle == nil {
		t.Fatal("review coordinator lifecycle disappeared")
	}
	if current.State != StateDone ||
		!current.Stopped ||
		current.ReviewCoordinatorLifecycle.Intent !=
			ReviewCoordinatorLifecycleCleanup ||
		current.ReviewCoordinatorLifecycle.CompletedAt.IsZero() {
		t.Fatalf(
			"terminal review coordinator = state:%s stopped:%v lifecycle:%#v",
			current.State,
			current.Stopped,
			current.ReviewCoordinatorLifecycle,
		)
	}
	if err := validateReviewCoordinatorLifecycleState(current); err != nil {
		t.Fatalf("validateReviewCoordinatorLifecycleState() error = %v", err)
	}
}

func TestReviewCoordinatorCleanupFailureKeepsRetryableOwnership(
	t *testing.T,
) {
	bot, reviewer, identity, runner := newReviewLifecycleHarness(t)
	ownership := prepareReviewLifecycleWorker(t, bot, reviewer, identity)

	var (
		mu                 sync.Mutex
		workerCleanupCalls int
		failWorkerCleanup  = true
	)
	bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		mu.Lock()
		defer mu.Unlock()
		if worktreePath == ownership.WorktreePath {
			workerCleanupCalls++
			if failWorkerCleanup {
				return errors.New("injected worker worktree cleanup failure")
			}
		}
		return os.RemoveAll(worktreePath)
	}

	err := bot.CleanupAgent(context.Background(), reviewer.ID)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"injected worker worktree cleanup failure",
		) {
		t.Fatalf("CleanupAgent() error = %v, want cleanup failure", err)
	}
	persisted, err := persistedReviewCoordinatorForLifecycleTest(
		bot.agentStateFilePath(),
		reviewer.ID,
	)
	if err != nil {
		t.Fatalf("persistedReviewCoordinatorForLifecycleTest() error = %v", err)
	}
	if persisted.State != StateStopped ||
		!persisted.Stopped ||
		persisted.ReviewCoordinatorLifecycle == nil ||
		!persisted.ReviewCoordinatorLifecycle.CompletedAt.IsZero() ||
		len(persisted.ReviewCoordinatorLifecycle.WorkerCleanups) != 1 {
		t.Fatalf("retryable persisted coordinator = %#v", persisted)
	}
	cleanup := persisted.ReviewCoordinatorLifecycle.WorkerCleanups[0]
	if cleanup.StopAcknowledgedAt.IsZero() ||
		!cleanup.WorktreeReleasedAt.IsZero() {
		t.Fatalf("retryable worker cleanup state = %#v", cleanup)
	}
	if len(persisted.ReviewCycle.WorkerOwnerships) != 1 ||
		persisted.ReviewCycle.WorkerOwnerships[0].OwnerID !=
			ownership.OwnerID {
		t.Fatalf(
			"retryable ownerships = %#v",
			persisted.ReviewCycle.WorkerOwnerships,
		)
	}

	mu.Lock()
	failWorkerCleanup = false
	mu.Unlock()
	if err := bot.StopAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("StopAgent(retry of durable cleanup intent) error = %v", err)
	}
	retried, ok := bot.agents.Get(reviewer.ID)
	if !ok ||
		retried.State != StateDone ||
		retried.ReviewCoordinatorLifecycle == nil ||
		retried.ReviewCoordinatorLifecycle.Intent !=
			ReviewCoordinatorLifecycleCleanup ||
		!retried.ReviewCoordinatorLifecycle.ReleaseRuntimeArtifacts ||
		retried.ReviewCoordinatorLifecycle.CompletedAt.IsZero() {
		t.Fatalf("retried durable cleanup result = %#v", retried)
	}
	beforeRepeatStops := runner.stopCallSnapshot()
	mu.Lock()
	beforeRepeatCleanupCalls := workerCleanupCalls
	mu.Unlock()
	if err := bot.CleanupAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("CleanupAgent(repeated terminal request) error = %v", err)
	}
	if !reflect.DeepEqual(runner.stopCallSnapshot(), beforeRepeatStops) {
		t.Fatalf(
			"repeated terminal stop created runtime work: before=%#v after=%#v",
			beforeRepeatStops,
			runner.stopCallSnapshot(),
		)
	}
	mu.Lock()
	defer mu.Unlock()
	if workerCleanupCalls != beforeRepeatCleanupCalls {
		t.Fatalf(
			"repeated terminal cleanup calls = %d, want unchanged %d",
			workerCleanupCalls,
			beforeRepeatCleanupCalls,
		)
	}
}

func TestPausedReviewCoordinatorPreservesExactCycleForResume(
	t *testing.T,
) {
	bot, reviewer, identity, runner := newReviewLifecycleHarness(t)
	ownership := prepareReviewLifecycleWorker(t, bot, reviewer, identity)
	before, ok := bot.agents.Get(reviewer.ID)
	if !ok {
		t.Fatal("review coordinator disappeared before pause")
	}
	exactCycle := cloneReviewCycle(before.ReviewCycle)
	bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		return os.RemoveAll(worktreePath)
	}

	if err := bot.PauseAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}
	paused, ok := bot.agents.Get(reviewer.ID)
	if !ok || !paused.Paused || paused.Stopped {
		t.Fatalf("paused review coordinator = %#v", paused)
	}
	if !reflect.DeepEqual(paused.ReviewCycle, exactCycle) {
		t.Fatalf(
			"paused review cycle changed:\n got: %#v\nwant: %#v",
			paused.ReviewCycle,
			exactCycle,
		)
	}
	if paused.ReviewCoordinatorLifecycle == nil ||
		paused.ReviewCoordinatorLifecycle.Intent !=
			ReviewCoordinatorLifecyclePause ||
		paused.ReviewCoordinatorLifecycle.CompletedAt.IsZero() {
		t.Fatalf(
			"paused lifecycle = %#v",
			paused.ReviewCoordinatorLifecycle,
		)
	}
	if _, err := os.Stat(reviewer.WorktreePath); err != nil {
		t.Fatalf("paused coordinator worktree was not preserved: %v", err)
	}
	for _, path := range []string{
		ownership.WorktreePath,
		ownership.GitHubConfigPath,
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("paused worker resource %q survived: %v", path, err)
		}
	}
	stopsBeforeRepeat := runner.stopCallSnapshot()
	if err := bot.PauseAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("PauseAgent(repeated) error = %v", err)
	}
	if !reflect.DeepEqual(runner.stopCallSnapshot(), stopsBeforeRepeat) {
		t.Fatalf(
			"repeated pause created stop work: before=%#v after=%#v",
			stopsBeforeRepeat,
			runner.stopCallSnapshot(),
		)
	}

	restartedAgents := NewAgentManager()
	restarted := &Orchestrator{
		cfg:    bot.cfg,
		agents: restartedAgents,
		runner: newReviewLifecycleRecordingRunner(),
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	restored, ok := restartedAgents.Get(reviewer.ID)
	if !ok || !restored.Paused {
		t.Fatalf("restored paused coordinator = %#v", restored)
	}
	if !reflect.DeepEqual(restored.ReviewCycle, exactCycle) {
		t.Fatalf("restored paused review cycle changed")
	}
}
