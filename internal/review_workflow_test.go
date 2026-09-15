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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v90/github"
)

func completedCorrectionLaunchAttempts(
	t *testing.T,
	coder Agent,
	count int,
) []DurableLaunchAttempt {
	t.Helper()
	attempts := make([]DurableLaunchAttempt, 0, count)
	base := time.Now().UTC().Add(-time.Duration(count) * time.Minute)
	for number := 1; number <= count; number++ {
		observedAt := base.Add(time.Duration(number) * time.Minute)
		attempt, err := newDurableLaunchAttempt(
			fmt.Sprintf("correction-test-%d", number),
			DurableLaunchCorrectionRuntime,
			fmt.Sprintf("review-comment-%d", number),
			number,
			coder.ID,
			tmuxSessionName(coder),
			observedAt,
		)
		if err != nil {
			t.Fatalf("newDurableLaunchAttempt() error = %v", err)
		}
		if _, err := transitionDurableLaunchAttempt(
			&attempt,
			DurableLaunchCompleted,
			nil,
			time.Time{},
			observedAt,
		); err != nil {
			t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
		}
		attempts = append(attempts, attempt)
	}
	return attempts
}

func correctionLaunchAttemptsWithActive(
	t *testing.T,
	coder Agent,
	count int,
) []DurableLaunchAttempt {
	t.Helper()
	attempts := completedCorrectionLaunchAttempts(t, coder, count)
	active := &attempts[len(attempts)-1]
	active.Lifecycle = DurableLaunchRunning
	active.StartedAt = active.FinishedAt
	active.FinishedAt = time.Time{}
	return attempts
}

func incompleteReviewLaunchAttempt(
	t *testing.T,
	headSHA string,
	id string,
	attemptNumber int,
	retryAfter time.Time,
) DurableLaunchAttempt {
	t.Helper()
	allocatedAt := retryAfter.Add(-reviewSameHeadRetryDelay)
	attempt, err := newDurableLaunchAttempt(
		id,
		DurableLaunchReviewCoordinator,
		headSHA,
		attemptNumber,
		"reviewer-"+id,
		"",
		allocatedAt,
	)
	if err != nil {
		t.Fatalf("newDurableLaunchAttempt() error = %v", err)
	}
	if _, err := transitionDurableLaunchAttempt(
		&attempt,
		DurableLaunchFailed,
		&DurableLaunchFailure{Kind: DurableLaunchFailureIncomplete},
		retryAfter,
		allocatedAt,
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	return attempt
}

func runningReviewLaunchAttempt(
	t *testing.T,
	headSHA string,
	id string,
	attemptNumber int,
) DurableLaunchAttempt {
	t.Helper()
	now := time.Now().UTC()
	attempt, err := newDurableLaunchAttempt(
		id,
		DurableLaunchReviewCoordinator,
		headSHA,
		attemptNumber,
		"reviewer-"+id,
		"",
		now,
	)
	if err != nil {
		t.Fatalf("newDurableLaunchAttempt() error = %v", err)
	}
	if _, err := transitionDurableLaunchAttempt(
		&attempt,
		DurableLaunchRunning,
		nil,
		time.Time{},
		now,
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	return attempt
}

func newWorkflowTestCycle(
	t *testing.T,
	headSHA string,
) *ReviewCycleState {
	t.Helper()
	cycle, err := newReviewCycleState(headSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	return cycle
}

func TestParseReviewVerdictAndMarkers(t *testing.T) {
	t.Parallel()

	body := strings.Join([]string{
		"Summary text.",
		"CODEX_AGENT_ID: reviewer-42-1",
		"CODEX_AGENT_ROLE: reviewer",
		"CODEX_REVIEWED_SHA: ABCDEF1234",
		"CODEX_VERDICT: NEEDS_CHANGES",
	}, "\n")

	if got, ok := parseReviewAgentID(body); !ok || got != "reviewer-42-1" {
		t.Fatalf("parseReviewAgentID() = (%q, %v), want (%q, true)", got, ok, "reviewer-42-1")
	}
	if got, ok := parseReviewedSHA(body); !ok || got != "abcdef1234" {
		t.Fatalf("parseReviewedSHA() = (%q, %v), want (%q, true)", got, ok, "abcdef1234")
	}
	if got, ok := parseReviewVerdict(body); !ok || got != ReviewVerdictNeedsChanges {
		t.Fatalf("parseReviewVerdict() = (%q, %v), want (%q, true)", got, ok, ReviewVerdictNeedsChanges)
	}
	if got, ok := parseReviewVerdict("CODEX_VERDICT: THUMBS_UP"); !ok || got != ReviewVerdictThumbsUp {
		t.Fatalf("parseReviewVerdict(THUMBS_UP) = (%q, %v), want (%q, true)", got, ok, ReviewVerdictThumbsUp)
	}
	if _, ok := parseReviewVerdict("CODEX_VERDICT: WHATEVER"); ok {
		t.Fatal("parseReviewVerdict() should reject unsupported verdict")
	}

	if _, ok := isReviewerVerdictComment(body, "reviewer-42-2", "abcdef1234"); ok {
		t.Fatal("isReviewerVerdictComment() should reject mismatched reviewer id")
	}
	if _, ok := isReviewerVerdictComment(body, "reviewer-42-1", "bbbbbbbbbb"); ok {
		t.Fatal("isReviewerVerdictComment() should reject mismatched reviewed SHA")
	}
	if got, ok := isReviewerVerdictComment(body, "reviewer-42-1", "abcdef1234"); !ok || got != ReviewVerdictNeedsChanges {
		t.Fatalf("isReviewerVerdictComment() = (%q, %v), want (%q, true)", got, ok, ReviewVerdictNeedsChanges)
	}

}

func TestNewAgentIDUsesRoleSpecificTimestampPrecision(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 123456789)

	if got, want := newAgentID(RoleCoder, 54, now), "coding-agent-54-1700000000"; got != want {
		t.Fatalf("newAgentID(RoleCoder) = %q, want %q", got, want)
	}
	if got, want := newAgentID(RoleReviewer, 54, now), "review-agent-54-1700000000123456789"; got != want {
		t.Fatalf("newAgentID(RoleReviewer) = %q, want %q", got, want)
	}
}

func TestLaunchReviewAgentReturnsAfterReviewerRegistration(t *testing.T) {
	commentsRequested := make(chan struct{}, 1)
	releaseComments := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseComments) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":   54,
				"html_url": "https://github.com/acme/widget/pull/54",
				"head": map[string]any{
					"sha": testReviewHeadSHA,
				},
			})
		case "/repos/acme/widget/issues/54/comments":
			select {
			case commentsRequested <- struct{}{}:
			default:
			}
			<-releaseComments
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := &stubRunner{}
	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-54",
		Role:                 RoleCoder,
		IssueNumber:          54,
		IssueTitle:           "Fix edge case",
		BranchName:           "repository-agent-orchestrator/issue-54",
		WorktreePath:         t.TempDir(),
		PRNumber:             54,
		PRURL:                "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:    testReviewHeadSHA,
		LastReviewedHeadSHA:  testReviewHeadSHA,
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     "/tmp/repo",
			WorktreeDir:  t.TempDir(),
			LogDir:       t.TempDir(),
			BaseBranch:   "main",
			ReviewPolicy: builtInReviewPolicy(),
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	launchResult := make(chan error, 1)
	go func() {
		launchResult <- bot.LaunchReviewAgent(
			context.Background(),
			agent.PRNumber,
		)
	}()

	select {
	case <-commentsRequested:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for review baseline request")
	}

	select {
	case err := <-launchResult:
		t.Fatalf("LaunchReviewAgent() returned before reviewer registration: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releaseComments) })
	select {
	case err := <-launchResult:
		if err != nil {
			t.Fatalf("LaunchReviewAgent() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for review launch")
	}

	// LaunchReviewAgent now returns once the reviewer is registered and
	// ready to enter the gate, not once the whole launch (hard gate plus
	// coordinator startup) has finished -- that work continues in a
	// background goroutine (see runReviewGateAndCoordinatorStartup). Wait
	// for it to actually reach StateWorking before this test's deferred
	// cleanup and t.TempDir()'s own cleanup run: otherwise they can race
	// the background goroutine's own worktree operations (observed in CI
	// as a "directory not empty" TempDir cleanup failure).
	reviewer := waitForReviewAgentForPR(t, agents, agent.PRNumber)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()

	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0", got)
	}
	if reviewer.ParentAgentID != agent.ID {
		t.Fatalf("review coordinator parent coder = %q, want %q", reviewer.ParentAgentID, agent.ID)
	}
	wantProfile, err := bot.cfg.ReviewPolicy.effectiveProfileForRole(
		AgentProfileRoleChallenge,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	if got, want := reviewer.RuntimeProfile, wantProfile; got != want {
		t.Fatalf("review coordinator profile = %+v, want %+v", got, want)
	}
}

// TestLaunchReviewAgentDoesNotBlockOnHardGate is the regression test for
// issue #138: LaunchReviewAgent used to run the entire mandatory-test hard
// gate synchronously, blocking its caller (the REPL's command dispatcher)
// for the gate's whole duration. It must now return once the reviewer is
// registered and ready to enter the gate, while the gate itself (and the
// coordinator startup that follows it) continues in the background.
// TestLaunchReviewAgentPersistsReviewerStateThroughBackgroundStartup is the
// regression test for issue #139 (review feedback on #138's fix):
// splitting a review launch into synchronous (registerReviewAgent,
// prepareReviewGateLaunch) and asynchronous (runReviewGateAndCoordinatorStartup)
// phases narrowed persistence to registration -- state changes made by
// the async phase (StateReviewGate, StateWorking, terminal states) were
// never flushed to disk by anything in that call chain. A restart while
// (or after) that background work ran would recover the reviewer from the
// last on-disk checkpoint as stale StateInitializing, even though the
// in-memory reviewer had already progressed or finished.
//
// Simulates a restart directly rather than actually restarting the
// process: after the background launch reaches StateWorking, loads the
// persisted state file into a *fresh* AgentManager/Orchestrator (exactly
// what a real restart's startup path does) and asserts the reloaded
// reviewer reflects StateWorking, not StateInitializing.
func TestLaunchReviewAgentPersistsReviewerStateThroughBackgroundStartup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/92":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 92,
				"head": map[string]any{
					"sha": testReviewHeadSHA,
				},
			})
		case "/repos/acme/widget/issues/92/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                   "agent-92",
		Role:                 RoleCoder,
		IssueNumber:          92,
		IssueTitle:           "Fix edge case",
		BranchName:           "repository-agent-orchestrator/issue-92",
		WorktreePath:         t.TempDir(),
		PRNumber:             92,
		PRURL:                "https://github.com/acme/widget/pull/92",
		ObservedPRHeadSHA:    testReviewHeadSHA,
		LastReviewedHeadSHA:  testReviewHeadSHA,
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	logDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     "/tmp/repo",
			WorktreeDir:  t.TempDir(),
			LogDir:       logDir,
			BaseBranch:   "main",
			ReviewPolicy: builtInReviewPolicy(),
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	if err := bot.LaunchReviewAgent(context.Background(), coder.PRNumber); err != nil {
		t.Fatalf("LaunchReviewAgent() error = %v", err)
	}
	reviewer := waitForReviewAgentForPR(t, agents, coder.PRNumber)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()

	reloaded := &Orchestrator{
		cfg:    Config{LogDir: logDir},
		agents: NewAgentManager(),
	}
	if err := reloaded.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	persistedReviewer, ok := reloaded.agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s missing from persisted state after simulated restart", reviewer.ID)
	}
	if persistedReviewer.State != StateWorking {
		t.Fatalf(
			"persisted reviewer state after simulated restart = %s, want %s (the in-memory reviewer already reached this state, but nothing in the async launch path had persisted it)",
			persistedReviewer.State, StateWorking,
		)
	}
}

func TestLaunchReviewAgentDoesNotBlockOnHardGate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/91":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 91,
				"head": map[string]any{
					"sha": testReviewHeadSHA,
				},
			})
		case "/repos/acme/widget/issues/91/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	gateCommandStarted := make(chan struct{}, 1)
	releaseGateCommand := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseGateCommand) })

	agents := NewAgentManager()
	coder := &Agent{
		ID:                   "agent-91",
		Role:                 RoleCoder,
		IssueNumber:          91,
		IssueTitle:           "Fix edge case",
		BranchName:           "repository-agent-orchestrator/issue-91",
		WorktreePath:         t.TempDir(),
		PRNumber:             91,
		PRURL:                "https://github.com/acme/widget/pull/91",
		ObservedPRHeadSHA:    testReviewHeadSHA,
		LastReviewedHeadSHA:  testReviewHeadSHA,
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			LogDir:         t.TempDir(),
			BaseBranch:     "main",
			MandatoryTests: []string{"make test"},
			ReviewPolicy:   builtInReviewPolicy(),
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name != "make" || len(args) != 1 || args[0] != "test" {
				return nil
			}
			select {
			case gateCommandStarted <- struct{}{}:
			default:
			}
			<-releaseGateCommand
			return nil
		},
	}

	launchStart := time.Now()
	if err := bot.LaunchReviewAgent(context.Background(), coder.PRNumber); err != nil {
		t.Fatalf("LaunchReviewAgent() error = %v", err)
	}
	launchDuration := time.Since(launchStart)

	// The reviewer must already be registered by the time LaunchReviewAgent
	// returns (a synchronous contract callers rely on -- see
	// TestLaunchReviewAgentReturnsAfterReviewerRegistration) even though the
	// gate command itself is still blocked.
	reviewer, err := agents.ResolveActiveReviewAgent(coder.PRNumber)
	if err != nil {
		t.Fatalf("ResolveActiveReviewAgent() immediately after LaunchReviewAgent = %v", err)
	}
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()

	select {
	case <-gateCommandStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the hard gate command to start in the background")
	}

	if launchDuration > 500*time.Millisecond {
		t.Fatalf("LaunchReviewAgent() took %s to return while the hard gate command was still blocked -- it must not wait on the gate", launchDuration)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls before the gate finished = %d, want 0", got)
	}

	releaseOnce.Do(func() { close(releaseGateCommand) })

	deadline := time.Now().Add(2 * time.Second)
	for {
		current, ok := agents.Get(reviewer.ID)
		if ok && current.State == StateWorking {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for reviewer to pass the hard gate and reach StateWorking in the background (last state ok=%v state=%s)", ok, current.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLaunchReviewAgentReturnsInitializationFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/260":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 260,
				"head": map[string]any{
					"sha": testReviewHeadSHA,
					"ref": "test-only-change",
				},
			})
		case "/repos/acme/widget/issues/260/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     "/tmp/repo",
			WorktreeDir:  t.TempDir(),
			LogDir:       t.TempDir(),
			ReviewPolicy: builtInReviewPolicy(),
		},
		github: newGitHubClientForTest(t, srv),
		agents: NewAgentManager(),
		runner: &stubRunner{},
		cmdRunner: func(context.Context, string, string, ...string) error {
			return errors.New("fetch failed")
		},
	}

	err := bot.LaunchReviewAgent(context.Background(), 260)
	if err == nil || !strings.Contains(
		err.Error(),
		"failed to launch review agent for PR #260: failed to prepare review worktree",
	) {
		t.Fatalf("LaunchReviewAgent() error = %v, want initialization failure", err)
	}
	if active := bot.agents.Active(); len(active) != 0 {
		t.Fatalf("active agents after failed launch = %#v, want none", active)
	}
}

func TestCleanupAgentBlocksReviewLaunchWaitingOnBaselineLookup(t *testing.T) {
	commentsRequested := make(chan struct{}, 1)
	releaseComments := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseComments) })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/72/comments":
			select {
			case commentsRequested <- struct{}{}:
			default:
			}
			<-releaseComments
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/pulls/72":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 72,
				"state":  "closed",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	repoPath, _ := setupRepoWithFeatureWorktree(t, "test/review-cleanup-race")
	agents := NewAgentManager()
	coder := &Agent{
		ID:                "coding-agent-72",
		Role:              RoleCoder,
		IssueNumber:       72,
		IssueTitle:        "Close cleanup race",
		PRNumber:          72,
		PRURL:             "https://github.com/acme/widget/pull/72",
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "late-review-runtime"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    repoPath,
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
	}

	launchErr := make(chan error, 1)
	go func() {
		snapshot, _ := agents.Get(coder.ID)
		launchErr <- bot.startReviewAgent(context.Background(), snapshot, coder.ObservedPRHeadSHA)
	}()

	select {
	case <-commentsRequested:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for review baseline lookup")
	}

	if err := bot.CleanupAgent(context.Background(), coder.ID); err != nil {
		t.Fatalf("CleanupAgent() error = %v", err)
	}
	releaseOnce.Do(func() { close(releaseComments) })

	select {
	case err := <-launchErr:
		if err != nil {
			t.Fatalf("startReviewAgent() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queued review launch to stop")
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", coder.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("coder after cleanup = (state=%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	for _, agent := range agents.Active() {
		if agent.Role == RoleReviewer {
			t.Fatalf("active reviewer created after cleanup: %#v", agent)
		}
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("review runtimes started after cleanup = %d, want 0", got)
	}
}

func TestLaunchReviewAgentForStandalonePR(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/77":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":   77,
				"title":    "Standalone review",
				"body":     "Review this pull request without a tracked coder.",
				"html_url": "https://github.com/acme/widget/pull/77",
				"head": map[string]any{
					"sha": testOtherReviewHeadSHA,
					"ref": "feature/standalone-review",
				},
			})
		case "/repos/acme/widget/issues/77/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := &stubRunner{}
	mandatoryTestRuns := 0
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			LogDir:         t.TempDir(),
			BaseBranch:     "main",
			MandatoryTests: []string{"make test"},
		},
		github: newGitHubClientForTest(t, srv),
		agents: NewAgentManager(),
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "make" && len(args) == 1 && args[0] == "test" {
				mandatoryTestRuns++
				return errors.New("manual review should skip mandatory test gate")
			}
			return nil
		},
	}

	if err := bot.LaunchReviewAgent(context.Background(), 77); err != nil {
		t.Fatalf("LaunchReviewAgent() error = %v", err)
	}

	reviewer := waitForReviewAgentForPR(t, bot.agents, 77)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()

	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0", got)
	}
	if reviewer.PRNumber != 77 {
		t.Fatalf("review coordinator pr = %d, want 77", reviewer.PRNumber)
	}
	if reviewer.ParentAgentID != "" {
		t.Fatalf("review coordinator parent coder = %q, want empty", reviewer.ParentAgentID)
	}
	if reviewer.BranchName != "feature/standalone-review" {
		t.Fatalf("review coordinator branch = %q, want %q", reviewer.BranchName, "feature/standalone-review")
	}
	if mandatoryTestRuns != 0 {
		t.Fatalf("mandatory test commands ran = %d, want 0", mandatoryTestRuns)
	}
}

func TestStartReviewAgentMarksReviewGateAndNotifies(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		notifications []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/54/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer gh.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "agent-54",
		Role:              RoleCoder,
		IssueNumber:       54,
		IssueTitle:        "Fix edge case",
		BranchName:        "repository-agent-orchestrator/issue-54",
		PRNumber:          54,
		PRURL:             "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	gateStarted := make(chan struct{}, 1)
	releaseGate := make(chan struct{})
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: make(chan struct{}, 1),
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			RepoPath:        "/tmp/repo",
			WorktreeDir:     t.TempDir(),
			LogDir:          t.TempDir(),
			BaseBranch:      "main",
			WebexWebhookURL: webex.URL,
			MandatoryTests:  []string{"make test-all"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			if name == "git" {
				return nil
			}
			if command == "make test-all" {
				select {
				case gateStarted <- struct{}{}:
				default:
				}
				<-releaseGate
				return nil
			}
			return nil
		},
	}

	errCh := make(chan error, 1)
	go func() {
		snapshot, _ := agents.Get(coder.ID)
		errCh <- bot.startReviewAgent(context.Background(), snapshot, testReviewHeadSHA)
	}()

	select {
	case <-gateStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for review gate to start")
	}

	var reviewer Agent
	found := false
	for _, agent := range agents.List() {
		if agent.Role == RoleReviewer {
			reviewer = agent
			found = true
			break
		}
	}
	if !found {
		t.Fatal("reviewer was not created")
	}
	if reviewer.State != StateReviewGate {
		t.Fatalf("reviewer state = %s, want %s", reviewer.State, StateReviewGate)
	}
	if reviewer.RuntimeHandle.Session != "" {
		t.Fatalf("reviewer runtime session = %q, want empty during gate", reviewer.RuntimeHandle.Session)
	}

	mu.Lock()
	startNotified := false
	for _, msg := range notifications {
		if strings.Contains(msg, "review hard gate started") {
			if !strings.Contains(msg, "Gate log: `") || !strings.Contains(msg, "-gate.log`") {
				t.Fatalf("start notification missing gate log path: %q", msg)
			}
			startNotified = true
			break
		}
	}
	mu.Unlock()
	if !startNotified {
		t.Fatalf("notifications missing review hard gate start: %v", notifications)
	}

	close(releaseGate)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("startReviewAgent() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for review launch to complete")
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0", got)
	}
}

// TestPollActiveAgentResumesReviewGateReviewerAfterRestart is the
// regression test for a real production incident on issue #982/PR #992,
// reproduced the way independent review feedback on PR #159 asked for:
// round-trip a realistic persisted reviewer through the actual
// restoration path, rather than manually constructing a state a restart
// cannot really produce.
//
// registerReviewAgent populates ReviewCycle at registration time, well
// before the hard gate ever runs, and shouldRestorePersistedAgent
// requires exactly that (ReviewCycle != nil) to restore a runtime-less
// StateInitializing/StateReviewGate reviewer at all -- so a reviewer
// that survives a restart mid-gate always has a non-nil, non-terminal
// ReviewCycle. pollActiveAgent's queuePersistedReviewCycleRecovery
// branch used to match on exactly that (ReviewCycle != nil and no
// terminal verdict) with no further check, catching these pre-gate
// reviewers before reconcilePendingReviewLaunch -- the only code path
// that can actually resume or terminalize an interrupted pre-gate
// launch -- ever ran. queuePersistedReviewCycleRecovery is the wrong
// recovery mechanism for a launch that never reached convergent review
// (validateConvergentReviewBoundary requires StateWorking), so it just
// failed silently forever, permanently wedging the reviewer.
//
// This test starts a real reviewer launch, captures it mid-gate (gate
// started, RuntimeHandle.Session still empty, ReviewCycle populated --
// the exact persisted shape a restart-restored reviewer has), builds a
// second Orchestrator with fresh in-memory maps (pendingReviewLaunches,
// reviewGateByAgent, reviewRecoveryWG all zero-valued, matching a real
// process restart) seeded with that snapshot, and drives its poll loop
// directly. It must resume the interrupted launch and reach a genuinely
// running convergent review coordinator (StateWorking) -- not stay
// wedged in StateReviewGate.
func TestPollActiveAgentResumesReviewGateReviewerAfterRestart(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/992/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/pulls/992":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 992,
				"state":  "open",
				"title":  "Fix edge case",
				"head": map[string]any{
					"sha":  testReviewHeadSHA,
					"ref":  "repository-agent-orchestrator/issue-982",
					"repo": map[string]any{"full_name": "acme/widget"},
				},
				"base": map[string]any{
					"ref":  "main",
					"sha":  strings.Repeat("b", 40),
					"repo": map[string]any{"full_name": "acme/widget"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	worktreeDir := t.TempDir()
	repoPath := "/tmp/repo"
	cfg := Config{
		RepoOwner:      "acme",
		RepoName:       "widget",
		RepoPath:       repoPath,
		WorktreeDir:    worktreeDir,
		LogDir:         t.TempDir(),
		BaseBranch:     "main",
		MandatoryTests: []string{"make test-all"},
	}

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "agent-982",
		Role:              RoleCoder,
		IssueNumber:       982,
		IssueTitle:        "Fix edge case",
		BranchName:        "repository-agent-orchestrator/issue-982",
		PRNumber:          992,
		PRURL:             "https://github.com/acme/widget/pull/992",
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	gateStarted := make(chan struct{}, 1)
	bot := &Orchestrator{
		cfg:    cfg,
		github: newGitHubClientForTest(t, gh),
		agents: agents,
		runner: &stubRunner{},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "git" {
				return nil
			}
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			if command == "make test-all" {
				select {
				case gateStarted <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}

	launchCtx, cancelLaunch := context.WithCancel(context.Background())
	defer cancelLaunch()
	errCh := make(chan error, 1)
	go func() {
		snapshot, _ := agents.Get(coder.ID)
		errCh <- bot.startReviewAgent(launchCtx, snapshot, testReviewHeadSHA)
	}()

	select {
	case <-gateStarted:
	case err := <-errCh:
		t.Fatalf("startReviewAgent() returned before gate started: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for review gate to start")
	}

	var midGateReviewer Agent
	found := false
	for _, agent := range agents.List() {
		if agent.Role == RoleReviewer {
			midGateReviewer = agent
			found = true
			break
		}
	}
	if !found {
		t.Fatal("reviewer was not created")
	}
	if midGateReviewer.State != StateReviewGate {
		t.Fatalf("reviewer state = %s, want %s", midGateReviewer.State, StateReviewGate)
	}
	if midGateReviewer.RuntimeHandle.Session != "" {
		t.Fatalf("reviewer runtime session = %q, want empty during gate", midGateReviewer.RuntimeHandle.Session)
	}
	if midGateReviewer.ReviewCycle == nil {
		t.Fatal("reviewer ReviewCycle is nil mid-gate, want populated at registration")
	}

	// Simulate the orchestrator process dying mid-gate: cancel the
	// in-flight launch's context (its "make test-all" child process
	// would die with the parent in reality) and stop waiting on it.
	cancelLaunch()
	<-errCh

	// Simulate a fresh restart: a brand-new Orchestrator with entirely
	// fresh in-memory state (pendingReviewLaunches, reviewGateByAgent,
	// reviewRecoveryWG all zero-valued -- nothing carries over from the
	// dead process) seeded only with the persisted snapshot captured
	// above, exactly as restoreFromPersistence would produce.
	restartedAgents := NewAgentManager()
	if err := restartedAgents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	restoredReviewer := midGateReviewer
	if err := restartedAgents.Add(&restoredReviewer); err != nil {
		t.Fatalf("Add(restoredReviewer) error = %v", err)
	}

	restarted := &Orchestrator{
		cfg:    cfg,
		github: newGitHubClientForTest(t, gh),
		agents: restartedAgents,
		runner: &stubRunner{},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "git" {
				return nil
			}
			// The retried gate succeeds quickly this time.
			return nil
		},
	}

	restarted.pollActiveAgent(context.Background(), restoredReviewer.ID, time.Now())
	restarted.waitForCheckpointedReviewLaunchRecoveries()

	final, ok := restartedAgents.Get(restoredReviewer.ID)
	if !ok {
		t.Fatal("reviewer disappeared after restart recovery")
	}
	if final.State != StateWorking {
		t.Fatalf(
			"reviewer state after restart recovery = %s, want %s (wedged in %s: pollActiveAgent's "+
				"queuePersistedReviewCycleRecovery branch intercepted it before reconcilePendingReviewLaunch could resume it)",
			final.State, StateWorking, StateReviewGate,
		)
	}
}

func TestReconcilePendingReviewLaunchAlertsForLongRunningGate(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		notifications []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	bot := &Orchestrator{
		cfg:      Config{WebexWebhookURL: webex.URL, MandatoryTests: []string{"make test-all", "make test-coverage"}},
		notifier: NewWebexNotifier(webex.URL),
		agents:   NewAgentManager(),
	}
	reviewer := &Agent{
		ID:                "reviewer-54-1",
		Role:              RoleReviewer,
		ParentAgentID:     "agent-54",
		IssueNumber:       54,
		PRNumber:          54,
		PRURL:             "https://github.com/acme/widget/pull/54",
		BranchName:        "repository-agent-orchestrator/issue-54",
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateReviewGate,
		LastActivityTime:  time.Unix(1700000000, 0),
	}
	if err := bot.agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	gateStartedAt := reviewer.LastActivityTime
	bot.setReviewGateStatus(reviewer.ID, reviewGateStatus{started: true, startedAt: gateStartedAt})

	now := gateStartedAt.Add(reviewGateAlertThreshold)
	updated, shouldContinue, err := bot.reconcilePendingReviewLaunch(context.Background(), *reviewer, now)
	if err != nil {
		t.Fatalf("reconcilePendingReviewLaunch() error = %v", err)
	}
	if !shouldContinue {
		t.Fatal("shouldContinue = false, want true for long-running alert")
	}
	if updated.ID != reviewer.ID {
		t.Fatalf("updated reviewer = %q, want %q", updated.ID, reviewer.ID)
	}
	if _, _, err := bot.reconcilePendingReviewLaunch(context.Background(), *reviewer, now.Add(time.Minute)); err != nil {
		t.Fatalf("second reconcilePendingReviewLaunch() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(notifications))
	}
	if !strings.Contains(notifications[0], "review hard gate is still running") {
		t.Fatalf("unexpected notification body: %q", notifications[0])
	}
	if !strings.Contains(notifications[0], "Gate log: `") || !strings.Contains(notifications[0], "-gate.log`") {
		t.Fatalf("still-running notification missing gate log path: %q", notifications[0])
	}
}

// TestReconcilePendingReviewLaunchAlertsUsingGateStartNotLastActivity is
// the regression test for the bug where the review-gate alert and stall
// timeout used agent.LastActivityTime -- a general "this agent record was
// touched for any reason" field bumped by many unrelated setters (Touch,
// SetState, SetRuntimeHandle, comment tracking, and more) -- instead of
// when the gate itself actually started. If something unrelated touches
// LastActivityTime while the mandatory-test gate command is still running
// in the background, the alert/stall clock appeared to reset even though
// the gate had made no real progress, which could silently suppress the
// "still running" alert for a genuinely stuck gate indefinitely.
//
// This sets the gate's real start far enough in the past that it has
// exceeded the alert threshold, but LastActivityTime recently (an
// unrelated touch, simulating routine agent-record activity unrelated to
// gate progress) -- so elapsed-since-LastActivityTime has NOT crossed the
// threshold, while elapsed-since-the-gate's-own-start has. The alert must
// still fire.
func TestReconcilePendingReviewLaunchAlertsUsingGateStartNotLastActivity(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		notifications []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	bot := &Orchestrator{
		cfg:      Config{WebexWebhookURL: webex.URL, MandatoryTests: []string{"make test-all"}},
		notifier: NewWebexNotifier(webex.URL),
		agents:   NewAgentManager(),
	}
	gateStartedAt := time.Unix(1700000000, 0)
	reviewer := &Agent{
		ID:                "reviewer-55-1",
		Role:              RoleReviewer,
		ParentAgentID:     "agent-55",
		IssueNumber:       55,
		PRNumber:          55,
		PRURL:             "https://github.com/acme/widget/pull/55",
		BranchName:        "repository-agent-orchestrator/issue-55",
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateReviewGate,
		// An unrelated touch refreshed LastActivityTime well after the
		// gate actually started, but still well before the alert
		// threshold has elapsed if measured from here.
		LastActivityTime: gateStartedAt.Add(reviewGateAlertThreshold - time.Minute),
	}
	if err := bot.agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	bot.setReviewGateStatus(reviewer.ID, reviewGateStatus{started: true, startedAt: gateStartedAt})

	// Elapsed since the gate's real start now exceeds the alert threshold,
	// even though elapsed since the (unrelated) LastActivityTime touch
	// does not.
	now := gateStartedAt.Add(reviewGateAlertThreshold + time.Minute)
	if now.Sub(reviewer.LastActivityTime) >= reviewGateAlertThreshold {
		t.Fatalf("test setup invalid: elapsed since LastActivityTime already exceeds the threshold on its own")
	}

	if _, _, err := bot.reconcilePendingReviewLaunch(context.Background(), *reviewer, now); err != nil {
		t.Fatalf("reconcilePendingReviewLaunch() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notifications) != 1 {
		t.Fatalf("notifications = %d, want 1 (alert must fire based on the gate's own start, not the unrelated LastActivityTime touch)", len(notifications))
	}
	if !strings.Contains(notifications[0], "review hard gate is still running") {
		t.Fatalf("unexpected notification body: %q", notifications[0])
	}
}

func TestReconcilePendingReviewLaunchRetiresStalledInitializingReviewer(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                "reviewer-88-1",
		Role:              RoleReviewer,
		IssueNumber:       88,
		PRNumber:          88,
		PRURL:             "https://github.com/acme/widget/pull/88",
		ObservedPRHeadSHA: testThirdReviewHeadSHA,
		State:             StateInitializing,
		LastActivityTime:  time.Unix(1700000000, 0),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	bot := &Orchestrator{agents: agents}
	_, shouldContinue, err := bot.reconcilePendingReviewLaunch(context.Background(), *reviewer, reviewer.LastActivityTime.Add(reviewInitStallTimeout))
	if err != nil {
		t.Fatalf("reconcilePendingReviewLaunch() error = %v", err)
	}
	if shouldContinue {
		t.Fatal("shouldContinue = true, want false after retiring stalled reviewer")
	}

	updated, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updated.State != StateErrored || !updated.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateErrored)
	}
}

func TestReconcilePendingReviewLaunchSilentlyRetiresUnownedInitializingReviewer(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		notifications []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                  "agent-88",
		Role:                RoleCoder,
		IssueNumber:         88,
		PRNumber:            88,
		PRURL:               "https://github.com/acme/widget/pull/88",
		ObservedPRHeadSHA:   testThirdReviewHeadSHA,
		LastReviewedHeadSHA: testThirdReviewHeadSHA,
		State:               StateApproved,
		LastActivityTime:    time.Now(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                "reviewer-88-2",
		Role:              RoleReviewer,
		ParentAgentID:     coder.ID,
		IssueNumber:       88,
		PRNumber:          88,
		PRURL:             coder.PRURL,
		ObservedPRHeadSHA: testThirdReviewHeadSHA,
		State:             StateInitializing,
		LastActivityTime:  time.Unix(1700000000, 0),
		WorktreePath:      t.TempDir(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	bot := &Orchestrator{
		cfg:      Config{WebexWebhookURL: webex.URL},
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
	}

	_, shouldContinue, err := bot.reconcilePendingReviewLaunch(context.Background(), *reviewer, reviewer.LastActivityTime.Add(reviewInitStallTimeout))
	if err != nil {
		t.Fatalf("reconcilePendingReviewLaunch() error = %v", err)
	}
	if shouldContinue {
		t.Fatal("shouldContinue = true, want false after retiring stale reviewer")
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateStopped || !updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true) so failed cleanup remains retryable", updatedReviewer.State, updatedReviewer.Stopped, StateStopped)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updatedCoder.State != StateApproved || updatedCoder.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", updatedCoder.State, updatedCoder.Stopped, StateApproved)
	}
	if updatedCoder.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", updatedCoder.ActiveReviewAgentID)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notifications) != 0 {
		t.Fatalf("expected no notifications for stale reviewer retirement, got %v", notifications)
	}
}

func TestHandleReviewVerdictNeedsChangesReplacesCoderRuntimeEveryCycle(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "agent-54",
		Role:              RoleCoder,
		IssueNumber:       54,
		BranchName:        "repository-agent-orchestrator/issue-54",
		PRNumber:          54,
		PRURL:             "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-54",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-54-1",
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-54-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  54,
		PRNumber:                     54,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testReviewHeadSHA,
		ReviewBaselineIssueCommentID: 10,
		ReviewCycle:                  newWorkflowTestCycle(t, testReviewHeadSHA),
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer-54-1",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-correction-coder-54"},
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all", "make test-coverage"},
		},
		agents: agents,
		runner: runner,
	}

	comment := &github.IssueComment{
		ID:      github.Int64(99),
		Body:    github.String("CODEX_AGENT_ID: reviewer-54-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: aaaaaaaaaaaa\nCODEX_VERDICT: NEEDS_CHANGES\n- Add tests."),
		HTMLURL: github.String("https://github.com/acme/widget/pull/54#issuecomment-99"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictNeedsChanges, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	reviewerState, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if reviewerState.State != StateDone || !reviewerState.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", reviewerState.State, reviewerState.Stopped, StateDone)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateWorking || coderState.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderState.State, coderState.Stopped, StateWorking)
	}
	if coderState.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", coderState.ActiveReviewAgentID)
	}
	if coderState.LastReviewVerdict != ReviewVerdictNeedsChanges {
		t.Fatalf("LastReviewVerdict = %q, want %q", coderState.LastReviewVerdict, ReviewVerdictNeedsChanges)
	}
	if coderState.LastReviewedHeadSHA != testReviewHeadSHA {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q", coderState.LastReviewedHeadSHA, testReviewHeadSHA)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runtime messages sent after restart = %d, want 0", got)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("replacement coder runtimes = %d, want 1", got)
	}
	if prompt := runner.prompts[0]; !strings.Contains(prompt, "This review result is the next Repository Agent Orchestrator instruction") || !strings.Contains(prompt, "NEEDS_CHANGES") || !strings.Contains(prompt, "Add tests") {
		t.Fatalf("replacement coder prompt = %q, want complete review correction instruction", prompt)
	}
	if got := len(runner.stopped); got != 2 {
		t.Fatalf("stopped runtimes = %d, want 2 (reviewer and previous coder)", got)
	}
	if got, want := coderState.RuntimeHandle.Session, "review-correction-coder-54"; got != want {
		t.Fatalf("replacement coder runtime = %q, want %q", got, want)
	}
	if got := correctionLaunchAttemptCount(coderState); got != 1 {
		t.Fatalf("correction launch attempts = %d, want 1 after one coder restart", got)
	}

	if !agents.SetPRHeadSHA(coder.ID, testOtherReviewHeadSHA) {
		t.Fatalf("SetPRHeadSHA(%s) = false", coder.ID)
	}
	if !agents.SetState(coder.ID, StateWaiting, false) {
		t.Fatalf("SetState(%s) = false", coder.ID)
	}
	reviewer2 := &Agent{
		ID:                           "reviewer-54-2",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  54,
		PRNumber:                     54,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testOtherReviewHeadSHA,
		ReviewBaselineIssueCommentID: 99,
		ReviewCycle:                  newWorkflowTestCycle(t, testOtherReviewHeadSHA),
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer-54-2",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer2); err != nil {
		t.Fatalf("Add(reviewer2) error = %v", err)
	}
	if !agents.SetActiveReviewAgent(coder.ID, reviewer2.ID) {
		t.Fatalf("SetActiveReviewAgent(%s) = false", coder.ID)
	}
	comment2 := &github.IssueComment{
		ID:      github.Int64(100),
		Body:    github.String("CODEX_AGENT_ID: reviewer-54-2\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: bbbbbbbbbbbb\nCODEX_VERDICT: NEEDS_CHANGES\n- Fix the second-cycle edge case."),
		HTMLURL: github.String("https://github.com/acme/widget/pull/54#issuecomment-100"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer2, ReviewVerdictNeedsChanges, comment2); err != nil {
		t.Fatalf("handleReviewVerdict() second cycle error = %v", err)
	}

	coderState, ok = agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after second cycle", coder.ID)
	}
	if coderState.State != StateWorking || coderState.Stopped || coderState.ActiveReviewAgentID != "" {
		t.Fatalf("coder after second cycle = (state=%s, stopped=%v, reviewer=%q), want working active coder", coderState.State, coderState.Stopped, coderState.ActiveReviewAgentID)
	}
	if coderState.LastReviewedHeadSHA != testOtherReviewHeadSHA || coderState.LastReviewVerdict != ReviewVerdictNeedsChanges || coderState.LastReviewCommentID != 100 {
		t.Fatalf("second-cycle review state = (head=%q, verdict=%q, comment=%d)", coderState.LastReviewedHeadSHA, coderState.LastReviewVerdict, coderState.LastReviewCommentID)
	}
	if got := len(runner.started); got != 2 {
		t.Fatalf("replacement coder runtimes after two cycles = %d, want 2", got)
	}
	if got := len(runner.stopped); got != 4 {
		t.Fatalf("stopped runtimes after two cycles = %d, want 4", got)
	}
	if prompt := runner.prompts[1]; !strings.Contains(prompt, "NEEDS_CHANGES") || !strings.Contains(prompt, "second-cycle edge case") {
		t.Fatalf("second-cycle replacement prompt = %q, want latest review feedback", prompt)
	}
	if got := correctionLaunchAttemptCount(coderState); got != 2 {
		t.Fatalf("correction launch attempts = %d, want 2 after two coder restarts", got)
	}
}

func TestHandleReviewVerdictDoesNotConsumeVerdictWhenConcurrentStopWins(
	t *testing.T,
) {
	agents := NewAgentManager()
	coder := &Agent{
		ID:                  "coder-concurrent-review-stop",
		Role:                RoleCoder,
		IssueNumber:         54,
		BranchName:          "repository-agent-orchestrator/issue-54",
		PRNumber:            54,
		PRURL:               "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:   testReviewHeadSHA,
		ActiveReviewAgentID: "reviewer-concurrent-stop",
		State:               StateWaiting,
		LastActivityTime:    time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coder-waiting-for-review",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "reviewer-concurrent-stop",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             coder.IssueNumber,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       coder.ObservedPRHeadSHA,
		ReviewCycle:             newWorkflowTestCycle(t, testReviewHeadSHA),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-runtime"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	stopStarted := make(chan struct{}, 1)
	releaseStop := make(chan struct{})
	runner := &stubRunner{
		stopStarted: stopStarted,
		stopRelease: releaseStop,
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "unexpected-correction-runtime",
		},
	}
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(
			context.Context,
			string,
			string,
			string,
		) error {
			return nil
		},
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- bot.StopAgent(context.Background(), reviewer.ID)
	}()
	select {
	case <-stopStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("manual reviewer stop did not reach runtime shutdown")
	}

	comment := &github.IssueComment{
		ID: github.Int64(991),
		Body: github.String(
			"CODEX_AGENT_ID: " + reviewer.ID +
				"\nCODEX_AGENT_ROLE: reviewer" +
				"\nCODEX_REVIEWED_SHA: " + testReviewHeadSHA +
				"\nCODEX_VERDICT: NEEDS_CHANGES",
		),
	}
	verdictDone := make(chan error, 1)
	go func() {
		verdictDone <- bot.handleReviewVerdict(
			context.Background(),
			*reviewer,
			ReviewVerdictNeedsChanges,
			comment,
		)
	}()
	select {
	case err := <-verdictDone:
		if err != nil {
			t.Fatalf("handleReviewVerdict() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("verdict handler did not observe the concurrent manual stop")
	}
	close(releaseStop)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("StopAgent() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("manual reviewer stop did not complete")
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %q disappeared", coder.ID)
	}
	if coderState.LastReviewVerdict != "" ||
		coderState.LastReviewCommentID != 0 {
		t.Fatalf(
			"concurrent stop consumed verdict = (verdict=%q, comment=%d)",
			coderState.LastReviewVerdict,
			coderState.LastReviewCommentID,
		)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("correction runtimes started = %d, want 0", got)
	}
}

func TestHandleReviewVerdictDoesNotApplyBeforeIntentPersistence(
	t *testing.T,
) {
	agents := NewAgentManager()
	coder := &Agent{
		ID:                  "coder-verdict-persistence-failure",
		Role:                RoleCoder,
		IssueNumber:         54,
		BranchName:          "repository-agent-orchestrator/issue-54",
		PRNumber:            54,
		PRURL:               "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:   testReviewHeadSHA,
		ActiveReviewAgentID: "reviewer-verdict-persistence-failure",
		State:               StateWaiting,
		LastActivityTime:    time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coder-before-correction",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "reviewer-verdict-persistence-failure",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             coder.IssueNumber,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       coder.ObservedPRHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-runtime"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	logDir := t.TempDir()
	stateDir := orchestratorStateDir(logDir)
	if err := os.WriteFile(stateDir, []byte("block state directory"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", stateDir, err)
	}
	runner := &stubRunner{
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coder-after-correction",
		},
	}
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(
			context.Context,
			string,
			string,
			string,
		) error {
			return nil
		},
	}
	comment := &github.IssueComment{
		ID: github.Int64(992),
		Body: github.String(
			"CODEX_AGENT_ID: " + reviewer.ID +
				"\nCODEX_AGENT_ROLE: reviewer" +
				"\nCODEX_REVIEWED_SHA: " + testReviewHeadSHA +
				"\nCODEX_VERDICT: NEEDS_CHANGES" +
				"\n- Preserve recoverable verdict state.",
		),
	}

	err := bot.handleReviewVerdict(
		context.Background(),
		*reviewer,
		ReviewVerdictNeedsChanges,
		comment,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "failed to persist pending review verdict") {
		t.Fatalf(
			"handleReviewVerdict() error = %v, want intent persistence failure",
			err,
		)
	}
	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %q disappeared", coder.ID)
	}
	if coderState.LastReviewedHeadSHA != "" ||
		coderState.LastReviewVerdict != "" ||
		coderState.LastReviewCommentID != 0 {
		t.Fatalf(
			"persistence failure consumed verdict = (head=%q, verdict=%q, comment=%d)",
			coderState.LastReviewedHeadSHA,
			coderState.LastReviewVerdict,
			coderState.LastReviewCommentID,
		)
	}
	if coderState.RuntimeHandle.Session != "coder-before-correction" ||
		coderState.State != StateWaiting {
		t.Fatalf(
			"recoverable coder state = (state=%s, session=%q), want (%s, %q)",
			coderState.State,
			coderState.RuntimeHandle.Session,
			StateWaiting,
			"coder-before-correction",
		)
	}
	if coderState.ActiveReviewAgentID != reviewer.ID {
		t.Fatalf(
			"ActiveReviewAgentID = %q, want %q after failed intent persistence",
			coderState.ActiveReviewAgentID,
			reviewer.ID,
		)
	}
	reviewerState, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if reviewerState.State != StateWorking || reviewerState.Stopped {
		t.Fatalf(
			"review coordinator state = (%s, stopped=%v), want (%s, false)",
			reviewerState.State,
			reviewerState.Stopped,
			StateWorking,
		)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("correction runtimes started = %d, want 0", got)
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("runtimes stopped before intent persistence = %d, want 0", got)
	}
}

func TestPendingReviewVerdictResumesAfterCompletionPersistenceFailure(
	t *testing.T,
) {
	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	coder := &Agent{
		ID:                  "coder-pending-verdict-recovery",
		Role:                RoleCoder,
		IssueNumber:         55,
		BranchName:          "repository-agent-orchestrator/issue-55",
		PRNumber:            55,
		PRURL:               "https://github.com/acme/widget/pull/55",
		ObservedPRHeadSHA:   testReviewHeadSHA,
		ActiveReviewAgentID: "reviewer-pending-verdict-recovery",
		State:               StateWaiting,
		LastActivityTime:    time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coder-pending-verdict-session",
		},
		RuntimeProfile:          AgentProfile{InheritGlobal: true},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "reviewer-pending-verdict-recovery",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             coder.IssueNumber,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       coder.ObservedPRHeadSHA,
		ReviewCycle:             newWorkflowTestCycle(t, testReviewHeadSHA),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-pending-verdict-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	blockingTempPath := filepath.Join(
		orchestratorStateDir(logDir),
		orchestratorStateFileName+".tmp",
	)
	runner := &stubRunner{
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: coder.RuntimeHandle.Session,
		},
		startHook: func() {
			if err := os.Mkdir(blockingTempPath, 0o755); err != nil {
				t.Fatalf("Mkdir(blocking state temp path) error = %v", err)
			}
		},
	}
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: worktreeDir,
		},
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(
			context.Context,
			string,
			string,
			string,
		) error {
			return nil
		},
	}
	comment := &github.IssueComment{
		ID: github.Int64(993),
		Body: github.String(
			"CODEX_AGENT_ID: " + reviewer.ID +
				"\nCODEX_AGENT_ROLE: reviewer" +
				"\nCODEX_REVIEWED_SHA: " + testReviewHeadSHA +
				"\nCODEX_VERDICT: NEEDS_CHANGES" +
				"\n- Resume the durable verdict exactly once.",
		),
	}

	err := bot.handleReviewVerdict(
		context.Background(),
		*reviewer,
		ReviewVerdictNeedsChanges,
		comment,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "failed to persist completed review verdict") {
		t.Fatalf(
			"handleReviewVerdict() error = %v, want completion persistence failure",
			err,
		)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("correction runtimes started before simulated crash = %d, want 1", got)
	}
	coderState, ok := agents.Get(coder.ID)
	if !ok || coderState.PendingReviewVerdict == nil {
		t.Fatal("in-memory pending review verdict was not restored")
	}
	if correctionLaunchAttemptCount(coderState) != 1 ||
		strings.TrimSpace(coderState.PendingReviewVerdict.CorrectionAttemptID) == "" {
		t.Fatalf(
			"in-memory correction reservation = attempts:%d pending:%q, want 1/non-empty",
			correctionLaunchAttemptCount(coderState),
			coderState.PendingReviewVerdict.CorrectionAttemptID,
		)
	}
	if coderState.LastReviewVerdict != "" ||
		coderState.LastReviewCommentID != 0 {
		t.Fatalf(
			"failed completion consumed verdict = (%q, %d)",
			coderState.LastReviewVerdict,
			coderState.LastReviewCommentID,
		)
	}

	body, err := os.ReadFile(bot.agentStateFilePath())
	if err != nil {
		t.Fatalf("ReadFile(persisted pending intent) error = %v", err)
	}
	var state persistedStateFile
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("Unmarshal(persisted pending intent) error = %v", err)
	}
	persistedPending := false
	for _, persisted := range state.Agents {
		if persisted.ID == coder.ID &&
			persisted.PendingReviewVerdict != nil {
			persistedPending = true
			if durableLaunchAttemptCount(
				persisted.LaunchAttempts,
				DurableLaunchCorrectionRuntime,
			) != 1 ||
				strings.TrimSpace(
					persisted.PendingReviewVerdict.CorrectionAttemptID,
				) == "" {
				t.Fatalf(
					"durable correction reservation = attempts:%d pending:%q, want 1/non-empty",
					durableLaunchAttemptCount(
						persisted.LaunchAttempts,
						DurableLaunchCorrectionRuntime,
					),
					persisted.PendingReviewVerdict.CorrectionAttemptID,
				)
			}
		}
	}
	if !persistedPending {
		t.Fatal("durable pending verdict was lost before completion")
	}
	if err := os.Remove(blockingTempPath); err != nil {
		t.Fatalf("Remove(blocking state temp path) error = %v", err)
	}

	restartedAgents := NewAgentManager()
	restartedRunner := &stubRunner{
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: coder.RuntimeHandle.Session,
		},
	}
	restarted := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: worktreeDir,
		},
		agents: restartedAgents,
		runner: restartedRunner,
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	if err := restarted.resumePendingReviewVerdict(
		context.Background(),
		coder.ID,
	); err != nil {
		t.Fatalf("resumePendingReviewVerdict() error = %v", err)
	}
	recovered, ok := restartedAgents.Get(coder.ID)
	if !ok {
		t.Fatalf("recovered coder %q disappeared", coder.ID)
	}
	if recovered.PendingReviewVerdict != nil ||
		recovered.LastReviewVerdict != ReviewVerdictNeedsChanges ||
		recovered.LastReviewCommentID != comment.GetID() ||
		correctionLaunchAttemptCount(recovered) != 1 ||
		!strings.EqualFold(
			recovered.LastReviewedHeadSHA,
			testReviewHeadSHA,
		) {
		t.Fatalf(
			"recovered verdict state = pending=%#v head=%q verdict=%q comment=%d",
			recovered.PendingReviewVerdict,
			recovered.LastReviewedHeadSHA,
			recovered.LastReviewVerdict,
			recovered.LastReviewCommentID,
		)
	}
	if got := len(restartedRunner.stopped); got != 0 {
		t.Fatalf(
			"correction runtimes stopped during recovery = %d, want 0; the live reserved runtime must be adopted",
			got,
		)
	}
	if got := len(restartedRunner.started); got != 0 {
		t.Fatalf("correction runtimes started during recovery = %d, want 0", got)
	}
}

func TestDeadPersistedCorrectionRuntimeConsumesNewDurableAttempt(
	t *testing.T,
) {
	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "coder-dead-correction-runtime",
		Role:                    RoleCoder,
		IssueNumber:             55,
		BranchName:              "repository-agent-orchestrator/issue-55",
		PRNumber:                55,
		PRURL:                   "https://github.com/acme/widget/pull/55",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWaiting,
		LastActivityTime:        time.Now().UTC(),
		RuntimeProfile:          AgentProfile{InheritGlobal: true},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	coder.RuntimeHandle = correctionRuntimeReservationHandle(*coder)
	now := time.Now().UTC().Add(-time.Minute)
	first, err := newDurableLaunchAttempt(
		"correction-993-first",
		DurableLaunchCorrectionRuntime,
		"review-comment-993",
		1,
		coder.ID,
		coder.RuntimeHandle.Session,
		now,
	)
	if err != nil {
		t.Fatalf("newDurableLaunchAttempt() error = %v", err)
	}
	if _, err := transitionDurableLaunchAttempt(
		&first,
		DurableLaunchRunning,
		nil,
		time.Time{},
		now,
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	coder.LaunchAttempts = []DurableLaunchAttempt{first}
	coder.PendingReviewVerdict = &PendingReviewVerdictApplication{
		ReviewerID:          "reviewer-dead-correction-runtime",
		ReviewedHeadSHA:     testReviewHeadSHA,
		Verdict:             ReviewVerdictNeedsChanges,
		CommentID:           993,
		ReviewInstruction:   "Fix the persisted failure.",
		CorrectionAttemptID: first.ID,
	}
	reviewer := &Agent{
		ID:                coder.PendingReviewVerdict.ReviewerID,
		Role:              RoleReviewer,
		ParentAgentID:     coder.ID,
		IssueNumber:       coder.IssueNumber,
		PRNumber:          coder.PRNumber,
		PRURL:             coder.PRURL,
		ObservedPRHeadSHA: coder.ObservedPRHeadSHA,
		ReviewCycle:       newWorkflowTestCycle(t, testReviewHeadSHA),
		State:             StateWorking,
		LastActivityTime:  time.Now().UTC(),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}
	runner := &stubRunner{
		aliveSet: true,
		alive:    false,
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: coder.RuntimeHandle.Session,
		},
	}
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(
			context.Context,
			string,
			string,
			string,
		) error {
			return nil
		},
	}

	if err := bot.resumePendingReviewVerdict(
		context.Background(),
		coder.ID,
	); err != nil {
		t.Fatalf("resumePendingReviewVerdict() error = %v", err)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s disappeared", coder.ID)
	}
	if updated.PendingReviewVerdict != nil {
		t.Fatalf("pending verdict = %#v, want completed", updated.PendingReviewVerdict)
	}
	if got := correctionLaunchAttemptCount(updated); got != 2 {
		t.Fatalf("correction attempts = %d, want 2", got)
	}
	if len(updated.LaunchAttempts) != 2 ||
		updated.LaunchAttempts[0].Lifecycle != DurableLaunchFailed ||
		updated.LaunchAttempts[0].Failure == nil ||
		updated.LaunchAttempts[0].Failure.Kind !=
			DurableLaunchFailureRuntimeMissing ||
		updated.LaunchAttempts[1].Attempt != 2 ||
		updated.LaunchAttempts[1].Lifecycle != DurableLaunchRunning {
		t.Fatalf("correction launch ledger = %#v", updated.LaunchAttempts)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("fresh correction runtimes = %d, want exactly 1", got)
	}
}

func TestHandleReviewVerdictIgnoresStaleThumbsUpAfterHeadDrift(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-54-stale-verdict",
		Role:                    RoleCoder,
		IssueNumber:             54,
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:       testOtherReviewHeadSHA,
		ActiveReviewAgentID:     "reviewer-54-stale-verdict",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	staleWorktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(staleWorktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	reviewer := &Agent{
		ID:                           "reviewer-54-stale-verdict",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  54,
		PRNumber:                     54,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testReviewHeadSHA,
		ReviewBaselineIssueCommentID: 10,
		ReviewCycle:                  newWorkflowTestCycle(t, testReviewHeadSHA),
		WorktreePath:                 staleWorktree,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle:                RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-session"},
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	var cleanupPaths []string
	runner := &stubRunner{}
	bot := &Orchestrator{
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(ctx context.Context, repoPath, worktreePath, branchName string) error {
			cleanupPaths = append(cleanupPaths, worktreePath)
			return nil
		},
	}

	comment := &github.IssueComment{
		ID:      github.Int64(1099),
		Body:    github.String("CODEX_AGENT_ID: reviewer-54-stale-verdict\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: aaaaaaaaaaaa\nCODEX_VERDICT: THUMBS_UP"),
		HTMLURL: github.String("https://github.com/acme/widget/pull/54#issuecomment-1099"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictThumbsUp, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if reviewerSnapshot.State != StateErrored || !reviewerSnapshot.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", reviewerSnapshot.State, reviewerSnapshot.Stopped, StateErrored)
	}
	if got := len(cleanupPaths); got != 1 || cleanupPaths[0] != staleWorktree {
		t.Fatalf("cleanup paths = %#v, want [%q]", cleanupPaths, staleWorktree)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderSnapshot.State != StateWaiting || coderSnapshot.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderSnapshot.State, coderSnapshot.Stopped, StateWaiting)
	}
	if coderSnapshot.RuntimeHandle.Session != "coder-session" {
		t.Fatalf("coder runtime session = %q, want %q", coderSnapshot.RuntimeHandle.Session, "coder-session")
	}
	if coderSnapshot.LastReviewedHeadSHA != "" {
		t.Fatalf("LastReviewedHeadSHA = %q, want empty after stale verdict", coderSnapshot.LastReviewedHeadSHA)
	}
	if coderSnapshot.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty after stale verdict", coderSnapshot.LastReviewVerdict)
	}
	if coderSnapshot.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty after stale verdict cleanup", coderSnapshot.ActiveReviewAgentID)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("stopped runtimes = %d, want 1 (reviewer only)", got)
	}
}

func TestHandleReviewVerdictNeedsChangesRestartsCoderWhenRuntimeIsMissing(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-54",
		Role:                    RoleCoder,
		IssueNumber:             54,
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "dead-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-54-1",
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-54-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  54,
		PRNumber:                     54,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testReviewHeadSHA,
		ReviewBaselineIssueCommentID: 10,
		ReviewCycle:                  newWorkflowTestCycle(t, testReviewHeadSHA),
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle:                RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-reviewer-54-1"},
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{
		aliveSet:    true,
		alive:       false,
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-coder-54"},
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all", "make test-coverage"},
		},
		agents: agents,
		runner: runner,
	}

	comment := &github.IssueComment{
		ID:      github.Int64(99),
		Body:    github.String("CODEX_AGENT_ID: reviewer-54-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: aaaaaaaaaaaa\nCODEX_VERDICT: NEEDS_CHANGES\n- Add tests."),
		HTMLURL: github.String("https://github.com/acme/widget/pull/54#issuecomment-99"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictNeedsChanges, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
	if runner.started[0].ID != coder.ID {
		t.Fatalf("restarted agent id = %q, want %q", runner.started[0].ID, coder.ID)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runtime messages sent after restart = %d, want 0", got)
	}
	if prompt := runner.prompts[0]; !strings.Contains(prompt, "NEEDS_CHANGES") || !strings.Contains(prompt, "Add tests") {
		t.Fatalf("replacement coder prompt = %q, want review feedback", prompt)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.RuntimeHandle.Session != "resumed-coder-54" {
		t.Fatalf("coder runtime session = %q, want %q", coderState.RuntimeHandle.Session, "resumed-coder-54")
	}
	if coderState.State != StateWorking || coderState.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderState.State, coderState.Stopped, StateWorking)
	}
}

func TestHandleReviewVerdictNeedsChangesKeepsPausedCoderPaused(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	coder := &Agent{
		ID:                      "agent-59",
		Role:                    RoleCoder,
		IssueNumber:             59,
		BranchName:              "repository-agent-orchestrator/issue-59",
		PRNumber:                59,
		PRURL:                   "https://github.com/acme/widget/pull/59",
		WorktreePath:            worktree,
		ObservedPRHeadSHA:       "abababababab",
		State:                   StateWaiting,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-59-1",
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-59-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  59,
		PRNumber:                     59,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            "abababababab",
		ReviewBaselineIssueCommentID: 10,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle:                RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-reviewer-59-1"},
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unexpected-resume"}}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all", "make test-coverage"},
		},
		agents:    agents,
		runner:    runner,
		messenger: messenger,
	}

	comment := &github.IssueComment{
		ID:      github.Int64(199),
		Body:    github.String("CODEX_AGENT_ID: reviewer-59-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: abababababab\nCODEX_VERDICT: NEEDS_CHANGES\n- Add paused coverage."),
		HTMLURL: github.String("https://github.com/acme/widget/pull/59#issuecomment-199"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictNeedsChanges, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 for paused coder", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0 for paused coder", got)
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("inbox messages = %d, want 1", got)
	}
	if !strings.Contains(messenger.messages[0], "NEEDS_CHANGES") {
		t.Fatalf("inbox message missing NEEDS_CHANGES marker: %q", messenger.messages[0])
	}

	reviewerState, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if reviewerState.State != StateDone || !reviewerState.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", reviewerState.State, reviewerState.Stopped, StateDone)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateWorking || coderState.Stopped || !coderState.Paused {
		t.Fatalf("coder state = (%s, stopped=%v, paused=%v), want (%s, false, true)", coderState.State, coderState.Stopped, coderState.Paused, StateWorking)
	}
	if coderState.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", coderState.ActiveReviewAgentID)
	}
	if coderState.LastReviewVerdict != ReviewVerdictNeedsChanges {
		t.Fatalf("LastReviewVerdict = %q, want %q", coderState.LastReviewVerdict, ReviewVerdictNeedsChanges)
	}
	if coderState.LastReviewedHeadSHA != "abababababab" {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q", coderState.LastReviewedHeadSHA, "abababababab")
	}
	if got := correctionLaunchAttemptCount(coderState); got != 0 {
		t.Fatalf("correction launch attempts = %d, want 0 without a coder restart", got)
	}
}

func TestHandleReviewVerdictCorrectionLimitKeepsPRDraftAndDoesNotRestartCoder(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/repos/acme/widget/pulls/60" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number": 60,
			"draft":  false,
			"head": map[string]any{
				"sha": testReviewHeadSHA,
			},
		})
	}))
	defer gh.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                  "coder-correction-limit",
		Role:                RoleCoder,
		IssueNumber:         60,
		BranchName:          "repository-agent-orchestrator/issue-60",
		PRNumber:            60,
		PRURL:               "https://github.com/acme/widget/pull/60",
		ObservedPRHeadSHA:   testReviewHeadSHA,
		ActiveReviewAgentID: "reviewer-correction-limit",
		State:               StateWaiting,
		LastActivityTime:    time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coder-correction-limit-runtime",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	coder.LaunchAttempts = correctionLaunchAttemptsWithActive(t, *coder, 2)
	cycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	cycle.CorrectionRound = correctionLaunchAttemptCount(*coder)
	reviewer := &Agent{
		ID:                      "reviewer-correction-limit",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             coder.IssueNumber,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       coder.ObservedPRHeadSHA,
		ReviewCycle:             cycle,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-correction-limit-runtime"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unexpected-correction-runtime"}}
	var ghCalls []string
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			WorktreeDir: t.TempDir(),
		},
		github: newGitHubClientForTest(t, gh),
		agents: agents,
		runner: runner,
		cmdRunner: func(_ context.Context, _ string, name string, args ...string) error {
			ghCalls = append(ghCalls, name+" "+strings.Join(args, " "))
			return nil
		},
		cleanupWorktreeFunc: func(context.Context, string, string, string) error {
			return nil
		},
	}
	comment := &github.IssueComment{
		ID: github.Int64(600),
		Body: github.String(
			"CODEX_AGENT_ID: " + reviewer.ID +
				"\nCODEX_AGENT_ROLE: reviewer" +
				"\nCODEX_REVIEWED_SHA: " + testReviewHeadSHA +
				"\nCODEX_VERDICT: NEEDS_CHANGES" +
				"\n- Human correction remains required.",
		),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictNeedsChanges, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updated.State != StateWaiting || updated.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want waiting and retained", updated.State, updated.Stopped)
	}
	if got := correctionLaunchAttemptCount(updated); got != 2 {
		t.Fatalf("correction launch attempts = %d, want unchanged at 2", got)
	}
	if active, found := activeCorrectionLaunchAttempt(updated); found {
		t.Fatalf(
			"terminal correction escalation retained active launch: %#v",
			active,
		)
	}
	if len(runner.started) != 0 {
		t.Fatalf("coder runtimes started = %d, want 0 at correction limit", len(runner.started))
	}
	if len(ghCalls) != 1 || ghCalls[0] != "gh pr ready --undo 60 -R acme/widget" {
		t.Fatalf("draft commands = %#v, want one same-PR draft command", ghCalls)
	}
}

func TestHandleReviewVerdictThumbsUpMovesCoderToApprovedAndNotifiesReady(t *testing.T) {
	t.Parallel()

	var (
		notifications []string
		ghCalls       []string
	)
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/77":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 77,
				"draft":  true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	var (
		notificationMu sync.Mutex
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		notificationMu.Lock()
		notifications = append(notifications, payload["markdown"])
		notificationMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "agent-77",
		Role:              RoleCoder,
		IssueNumber:       77,
		BranchName:        "repository-agent-orchestrator/issue-77",
		PRNumber:          77,
		PRURL:             "https://github.com/acme/widget/pull/77",
		ObservedPRHeadSHA: testOtherReviewHeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-77",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-77-1",
	}
	coder.LaunchAttempts = correctionLaunchAttemptsWithActive(t, *coder, 1)
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-77-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  77,
		PRNumber:                     77,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testOtherReviewHeadSHA,
		ReviewBaselineIssueCommentID: 100,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer-77-1",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			MandatoryTests:  []string{"make test-all", "make test-coverage"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				ghCalls = append(ghCalls, name+" "+strings.Join(args, " "))
				return nil
			}
			return nil
		},
	}

	comment := &github.IssueComment{
		ID:      github.Int64(120),
		Body:    github.String("CODEX_AGENT_ID: reviewer-77-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: bbbbbbbbbbbb\nCODEX_VERDICT: THUMBS_UP"),
		HTMLURL: github.String("https://github.com/acme/widget/pull/77#issuecomment-120"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictThumbsUp, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateApproved || coderState.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderState.State, coderState.Stopped, StateApproved)
	}
	if coderState.LastReviewVerdict != ReviewVerdictThumbsUp {
		t.Fatalf("LastReviewVerdict = %q, want %q", coderState.LastReviewVerdict, ReviewVerdictThumbsUp)
	}
	if coderState.PendingReviewVerdict != nil {
		t.Fatalf(
			"PendingReviewVerdict = %#v, want nil after ready-for-review succeeds",
			coderState.PendingReviewVerdict,
		)
	}
	if coderState.LaunchAttempts[0].Lifecycle != DurableLaunchCompleted {
		t.Fatalf(
			"approved correction launch lifecycle = %q, want %q",
			coderState.LaunchAttempts[0].Lifecycle,
			DurableLaunchCompleted,
		)
	}
	if got := len(runner.stopped); got != 2 {
		t.Fatalf("stopped runtimes = %d, want 2 (reviewer + coder)", got)
	}
	if len(ghCalls) != 1 {
		t.Fatalf("gh ready calls = %d, want 1", len(ghCalls))
	}
	if got, want := ghCalls[0], "gh pr ready 77 -R acme/widget"; got != want {
		t.Fatalf("gh ready command = %q, want %q", got, want)
	}

	notificationMu.Lock()
	defer notificationMu.Unlock()
	if countMessagesContaining(notifications, "PR ready for human review") != 1 {
		t.Fatalf("expected one readiness notification, got %v", notifications)
	}
	if countMessagesContaining(notifications, "waiting for GitHub approval before merge") != 1 {
		t.Fatalf("expected one waiting-for-approval notification, got %v", notifications)
	}
}

func TestHandleReviewVerdictThumbsUpMarksDetachedManualReviewReady(t *testing.T) {
	t.Parallel()

	var (
		notifications []string
		ghCalls       []string
	)
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/277":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 277,
				"draft":  true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	var notificationMu sync.Mutex
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		notificationMu.Lock()
		notifications = append(notifications, payload["markdown"])
		notificationMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                "reviewer-277-1",
		Role:              RoleReviewer,
		IssueNumber:       0,
		PRNumber:          277,
		PRURL:             "https://github.com/acme/widget/pull/277",
		BranchName:        "feature/manual-review",
		ObservedPRHeadSHA: testFifthReviewHeadSHA,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "manual-reviewer-277",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			MandatoryTests:  []string{"make test-all"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				ghCalls = append(ghCalls, name+" "+strings.Join(args, " "))
				return nil
			}
			return nil
		},
	}

	comment := &github.IssueComment{
		ID:      github.Int64(2770),
		Body:    github.String("CODEX_AGENT_ID: reviewer-277-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: eeeeeeeeeeee\nCODEX_VERDICT: THUMBS_UP"),
		HTMLURL: github.String("https://github.com/acme/widget/pull/277#issuecomment-2770"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictThumbsUp, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	reviewerState, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if reviewerState.State != StateDone || !reviewerState.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", reviewerState.State, reviewerState.Stopped, StateDone)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("stopped runtimes = %d, want 1 (reviewer only)", got)
	}
	if len(ghCalls) != 1 {
		t.Fatalf("gh ready calls = %d, want 1", len(ghCalls))
	}
	if got, want := ghCalls[0], "gh pr ready 277 -R acme/widget"; got != want {
		t.Fatalf("gh ready command = %q, want %q", got, want)
	}

	notificationMu.Lock()
	defer notificationMu.Unlock()
	if countMessagesContaining(notifications, "PR ready for human review from manual review agent") != 1 {
		t.Fatalf("expected one manual review readiness notification, got %v", notifications)
	}
}

func TestEnsureReviewAgentForCoderSkipsWhenPRLaunchAlreadyPending(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "coding-agent-91",
		Role:                    RoleCoder,
		IssueNumber:             91,
		IssueTitle:              "Avoid duplicate review launches",
		BranchName:              "repository-agent-orchestrator/issue-91",
		PRNumber:                91,
		PRURL:                   "https://github.com/acme/widget/pull/91",
		ObservedPRHeadSHA:       "pendinghead91",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewLaunchStarted := make(chan struct{}, 1)
	runner := &stubRunner{startStarted: reviewLaunchStarted}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			LogDir:         t.TempDir(),
			MandatoryTests: []string{"make test"},
		},
		agents: agents,
		runner: runner,
	}

	launchKey := reviewLaunchKeyForPR(coder.PRNumber)
	if !bot.reserveReviewLaunch(launchKey, coder.ObservedPRHeadSHA) {
		t.Fatal("reserveReviewLaunch() = false, want true")
	}
	defer bot.releaseReviewLaunch(launchKey, coder.ObservedPRHeadSHA)

	snapshot, _ := agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected reviewer launch while PR launch key is already pending")
	case <-time.After(200 * time.Millisecond):
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0", got)
	}
}

func TestHandleReviewVerdictThumbsUpKeepsPausedCoderPaused(t *testing.T) {
	t.Parallel()

	var notifications []string
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/177":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 177,
				"draft":  true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	var notificationMu sync.Mutex
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		notificationMu.Lock()
		notifications = append(notifications, payload["markdown"])
		notificationMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-177",
		Role:                    RoleCoder,
		IssueNumber:             177,
		BranchName:              "repository-agent-orchestrator/issue-177",
		PRNumber:                177,
		PRURL:                   "https://github.com/acme/widget/pull/177",
		ObservedPRHeadSHA:       "abababababab",
		State:                   StateWaiting,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-177-1",
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-177-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  177,
		PRNumber:                     177,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            "abababababab",
		ReviewBaselineIssueCommentID: 100,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer-177-1",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	ghCalls := make([]string, 0, 1)
	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			MandatoryTests:  []string{"make test-all", "make test-coverage"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				ghCalls = append(ghCalls, name+" "+strings.Join(args, " "))
				return nil
			}
			return nil
		},
	}

	comment := &github.IssueComment{
		ID:      github.Int64(177),
		Body:    github.String("CODEX_AGENT_ID: reviewer-177-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: abababababab\nCODEX_VERDICT: THUMBS_UP"),
		HTMLURL: github.String("https://github.com/acme/widget/pull/177#issuecomment-177"),
	}
	if err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictThumbsUp, comment); err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateApproved || coderState.Stopped || !coderState.Paused {
		t.Fatalf("coder state = (%s, stopped=%v, paused=%v), want (%s, false, true)", coderState.State, coderState.Stopped, coderState.Paused, StateApproved)
	}
	if coderState.LastReviewVerdict != ReviewVerdictThumbsUp {
		t.Fatalf("LastReviewVerdict = %q, want %q", coderState.LastReviewVerdict, ReviewVerdictThumbsUp)
	}
	if coderState.PendingReviewVerdict != nil {
		t.Fatalf(
			"PendingReviewVerdict = %#v, want nil after ready-for-review succeeds",
			coderState.PendingReviewVerdict,
		)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("stopped runtimes = %d, want 1 (reviewer only)", got)
	}
	if len(ghCalls) != 1 {
		t.Fatalf("gh ready calls = %d, want 1", len(ghCalls))
	}
	if got, want := ghCalls[0], "gh pr ready 177 -R acme/widget"; got != want {
		t.Fatalf("gh ready command = %q, want %q", got, want)
	}

	notificationMu.Lock()
	defer notificationMu.Unlock()
	if countMessagesContaining(notifications, "PR ready for human review") != 1 {
		t.Fatalf("expected one readiness notification, got %v", notifications)
	}
}

func TestHandleReviewVerdictThumbsUpLeavesCoderWaitingWhenReadyForReviewFails(t *testing.T) {
	t.Parallel()

	var notifications []string
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/78":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 78,
				"draft":  true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/repos/acme/widget/pulls/78/ready_for_review":
			http.Error(w, "draft promotion failed", http.StatusUnprocessableEntity)
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	var notificationMu sync.Mutex
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		notificationMu.Lock()
		notifications = append(notifications, payload["markdown"])
		notificationMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "agent-78",
		Role:              RoleCoder,
		IssueNumber:       78,
		BranchName:        "repository-agent-orchestrator/issue-78",
		PRNumber:          78,
		PRURL:             "https://github.com/acme/widget/pull/78",
		ObservedPRHeadSHA: testThirdReviewHeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-78",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-78-1",
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-78-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  78,
		PRNumber:                     78,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testThirdReviewHeadSHA,
		ReviewBaselineIssueCommentID: 100,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer-78-1",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			MandatoryTests:  []string{"make test-all"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				return fmt.Errorf("gh failed")
			}
			return nil
		},
	}

	comment := &github.IssueComment{
		ID:      github.Int64(121),
		Body:    github.String("CODEX_AGENT_ID: reviewer-78-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: cccccccccccc\nCODEX_VERDICT: THUMBS_UP"),
		HTMLURL: github.String("https://github.com/acme/widget/pull/78#issuecomment-121"),
	}
	err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictThumbsUp, comment)
	if err == nil || !strings.Contains(err.Error(), "failed to mark PR #78 ready for review after THUMBS_UP") {
		t.Fatalf("handleReviewVerdict() error = %v, want ready_for_review failure", err)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateWaiting || coderState.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderState.State, coderState.Stopped, StateWaiting)
	}
	if coderState.LastReviewVerdict != "" {
		t.Fatalf(
			"LastReviewVerdict = %q, want empty until ready-for-review succeeds",
			coderState.LastReviewVerdict,
		)
	}
	if coderState.PendingReviewVerdict == nil ||
		coderState.PendingReviewVerdict.Verdict != ReviewVerdictThumbsUp {
		t.Fatalf(
			"PendingReviewVerdict = %#v, want retryable THUMBS_UP",
			coderState.PendingReviewVerdict,
		)
	}
	if got := len(runner.stopped); got != 2 {
		t.Fatalf("stopped runtimes = %d, want 2 (reviewer + coder)", got)
	}

	notificationMu.Lock()
	defer notificationMu.Unlock()
	if countMessagesContaining(notifications, "failed to mark PR ready for review") != 1 {
		t.Fatalf("expected one ready_for_review failure notification, got %v", notifications)
	}
	if countMessagesContaining(notifications, "gh pr ready failed for PR #78") != 1 {
		t.Fatalf("expected gh failure details in notifications, got %v", notifications)
	}
}

func TestHandleReviewVerdictThumbsUpReadyForReviewFailureKeepsPausedCoderPaused(t *testing.T) {
	t.Parallel()

	var notifications []string
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/178":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 178,
				"draft":  true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/repos/acme/widget/pulls/178/ready_for_review":
			http.Error(w, "draft promotion failed", http.StatusUnprocessableEntity)
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	var notificationMu sync.Mutex
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		notificationMu.Lock()
		notifications = append(notifications, payload["markdown"])
		notificationMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-178",
		Role:                    RoleCoder,
		IssueNumber:             178,
		BranchName:              "repository-agent-orchestrator/issue-178",
		PRNumber:                178,
		PRURL:                   "https://github.com/acme/widget/pull/178",
		ObservedPRHeadSHA:       testThirdReviewHeadSHA,
		State:                   StateWaiting,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-178-1",
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-178-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  178,
		PRNumber:                     178,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testThirdReviewHeadSHA,
		ReviewBaselineIssueCommentID: 100,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer-178-1",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			MandatoryTests:  []string{"make test-all"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				return fmt.Errorf("gh failed")
			}
			return nil
		},
	}

	comment := &github.IssueComment{
		ID:      github.Int64(221),
		Body:    github.String("CODEX_AGENT_ID: reviewer-178-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: cccccccccccc\nCODEX_VERDICT: THUMBS_UP"),
		HTMLURL: github.String("https://github.com/acme/widget/pull/178#issuecomment-221"),
	}
	err := bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictThumbsUp, comment)
	if err == nil || !strings.Contains(err.Error(), "failed to mark PR #178 ready for review after THUMBS_UP") {
		t.Fatalf("handleReviewVerdict() error = %v, want ready_for_review failure", err)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateWaiting || coderState.Stopped || !coderState.Paused {
		t.Fatalf("coder state = (%s, stopped=%v, paused=%v), want (%s, false, true)", coderState.State, coderState.Stopped, coderState.Paused, StateWaiting)
	}
	if coderState.LastReviewVerdict != "" {
		t.Fatalf(
			"LastReviewVerdict = %q, want empty until ready-for-review succeeds",
			coderState.LastReviewVerdict,
		)
	}
	if coderState.PendingReviewVerdict == nil ||
		coderState.PendingReviewVerdict.Verdict != ReviewVerdictThumbsUp {
		t.Fatalf(
			"PendingReviewVerdict = %#v, want retryable THUMBS_UP",
			coderState.PendingReviewVerdict,
		)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("stopped runtimes = %d, want 1 (reviewer only)", got)
	}

	notificationMu.Lock()
	defer notificationMu.Unlock()
	if countMessagesContaining(notifications, "failed to mark PR ready for review") != 1 {
		t.Fatalf("expected one ready_for_review failure notification, got %v", notifications)
	}
	if countMessagesContaining(notifications, "gh pr ready failed for PR #178") != 1 {
		t.Fatalf("expected gh failure details in notifications, got %v", notifications)
	}
}

func TestHandleReviewVerdictThumbsUpDoesNotDoubleMarkReadyForReview(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/79":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 79,
				"draft":  true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "agent-79",
		Role:              RoleCoder,
		IssueNumber:       79,
		BranchName:        "repository-agent-orchestrator/issue-79",
		PRNumber:          79,
		PRURL:             "https://github.com/acme/widget/pull/79",
		ObservedPRHeadSHA: testFourthReviewHeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-79",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		ActiveReviewAgentID:     "reviewer-79-1",
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                           "reviewer-79-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  79,
		PRNumber:                     79,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testFourthReviewHeadSHA,
		ReviewBaselineIssueCommentID: 100,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer-79-1",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	ghStarted := make(chan struct{}, 1)
	releaseGH := make(chan struct{})
	var (
		mu      sync.Mutex
		ghCalls []string
	)
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test"},
		},
		github: newGitHubClientForTest(t, gh),
		agents: agents,
		runner: &stubRunner{},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name != "gh" {
				return nil
			}
			mu.Lock()
			ghCalls = append(ghCalls, name+" "+strings.Join(args, " "))
			mu.Unlock()
			select {
			case ghStarted <- struct{}{}:
			default:
			}
			<-releaseGH
			return nil
		},
	}

	comment := &github.IssueComment{
		ID:      github.Int64(122),
		Body:    github.String("CODEX_AGENT_ID: reviewer-79-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: dddddddddddd\nCODEX_VERDICT: THUMBS_UP"),
		HTMLURL: github.String("https://github.com/acme/widget/pull/79#issuecomment-122"),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- bot.handleReviewVerdict(context.Background(), *reviewer, ReviewVerdictThumbsUp, comment)
	}()

	select {
	case <-ghStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for gh pr ready to start")
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.maybePromoteReadyForHumanReview(context.Background(), snapshot); err != nil {
		t.Fatalf("maybePromoteReadyForHumanReview() error = %v", err)
	}

	mu.Lock()
	if got := len(ghCalls); got != 1 {
		mu.Unlock()
		t.Fatalf("gh ready calls during in-flight verdict = %d, want 1", got)
	}
	mu.Unlock()

	close(releaseGH)
	if err := <-errCh; err != nil {
		t.Fatalf("handleReviewVerdict() error = %v", err)
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updated.State != StateApproved {
		t.Fatalf("coder state = %s, want %s", updated.State, StateApproved)
	}
	if updated.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", updated.ActiveReviewAgentID)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := len(ghCalls); got != 1 {
		t.Fatalf("gh ready calls after verdict = %d, want 1", got)
	}
}

// TestMaybePromoteReadyForHumanReviewSkipsCoderWithNonTerminalLinkedReviewer
// is the regression test for issue #148 (symptom 2): ActiveReviewAgentID,
// ObservedPRHeadSHA, LastReviewVerdict, and LastReviewedHeadSHA are not
// updated atomically with each other, or with a newly-launched reviewer's
// own registration. In the live incident, a fresh reviewer for a newly
// pushed, unreviewed commit had already been created and was actively
// running, but the coder's ActiveReviewAgentID still read empty and
// LastReviewVerdict/LastReviewedHeadSHA still reflected the *previous*,
// now-superseded head's THUMBS_UP approval -- so coderCanStayApproved
// spuriously believed the coder was still validly approved for its
// (stale) observed head, and maybePromoteReadyForHumanReview posted a
// second, misleading "PR ready for human review" notice immediately after
// the new review had already started, before it had any chance to
// conclude.
func TestMaybePromoteReadyForHumanReviewSkipsCoderWithNonTerminalLinkedReviewer(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-981-race",
		Role:                    RoleCoder,
		IssueNumber:             981,
		BranchName:              "repository-agent-orchestrator/issue-981",
		PRNumber:                991,
		PRURL:                   "https://github.com/infrasec-cto/zeekfoundry/pull/991",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		LastReviewVerdict:       ReviewVerdictThumbsUp,
		LastReviewedHeadSHA:     testReviewHeadSHA,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	// A brand new reviewer already exists and is actively running for the
	// coder -- e.g. just launched for a newly-pushed commit -- but the
	// coder's own ActiveReviewAgentID has not (yet) been updated to name
	// it, reproducing the exact race window from the live incident.
	newReviewer := &Agent{
		ID:                      "review-agent-981-race",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             981,
		PRNumber:                991,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(newReviewer); err != nil {
		t.Fatalf("Add(newReviewer) error = %v", err)
	}

	bot := &Orchestrator{agents: agents}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.maybePromoteReadyForHumanReview(context.Background(), snapshot); err != nil {
		t.Fatalf("maybePromoteReadyForHumanReview() error = %v", err)
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after poll", coder.ID)
	}
	if updated.State != StateWaiting {
		t.Fatalf(
			"coder state after poll with a non-terminal linked reviewer already running = %s, want %s (must not spuriously re-declare it approved and ready for human review)",
			updated.State, StateWaiting,
		)
	}
}

func TestStartReviewAgentPreReviewHardGateFailureKeepsCoderWorking(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		notifications []string
		commands      []string
	)
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/88/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer gh.Close()

	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "agent-88",
		Role:              RoleCoder,
		IssueNumber:       88,
		BranchName:        "repository-agent-orchestrator/issue-88",
		PRNumber:          88,
		PRURL:             "https://github.com/acme/widget/pull/88",
		ObservedPRHeadSHA: testThirdReviewHeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-88",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			MandatoryTests:  []string{"make test-all", "make test-coverage"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			commands = append(commands, dir+"::"+command)
			if command == "make test-all" {
				return errors.New("command failed: make test-all")
			}
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.startReviewAgent(context.Background(), snapshot, testThirdReviewHeadSHA); err != nil {
		t.Fatalf("startReviewAgent() error = %v", err)
	}

	var reviewerID string
	for _, agent := range agents.List() {
		if agent.Role == RoleReviewer {
			reviewerID = agent.ID
			break
		}
	}
	if strings.TrimSpace(reviewerID) == "" {
		t.Fatal("reviewer id not found")
	}

	reviewerState, ok := agents.Get(reviewerID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewerID)
	}
	if reviewerState.State != StateDone || !reviewerState.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", reviewerState.State, reviewerState.Stopped, StateDone)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateWorking || coderState.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderState.State, coderState.Stopped, StateWorking)
	}
	if coderState.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty after hard-gate failure without a completed review", coderState.LastReviewVerdict)
	}
	if coderState.LastReviewedHeadSHA != "" {
		t.Fatalf("LastReviewedHeadSHA = %q, want empty after hard-gate failure", coderState.LastReviewedHeadSHA)
	}
	if coderState.LastPreReviewGateFailureHeadSHA != testThirdReviewHeadSHA {
		t.Fatalf("LastPreReviewGateFailureHeadSHA = %q, want %q", coderState.LastPreReviewGateFailureHeadSHA, testThirdReviewHeadSHA)
	}
	if coderState.LastPreReviewGateFailureAt.IsZero() {
		t.Fatal("LastPreReviewGateFailureAt should be recorded")
	}
	if coderState.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", coderState.ActiveReviewAgentID)
	}
	launch, found := reviewLaunchAttemptForHead(
		coderState,
		testThirdReviewHeadSHA,
	)
	if !found || launch.Lifecycle != DurableLaunchCancelled {
		t.Fatalf("failed hard-gate launch lifecycle = %#v, want cancelled", launch)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("review runtime starts = %d, want 0 when pre-review hard gate fails", got)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runtime messages sent to coder = %d, want 1", got)
	}
	if !strings.Contains(runner.sent[0], "pre-review hard gate failed") {
		t.Fatalf("runtime message missing pre-review hard gate failure marker: %q", runner.sent[0])
	}
	if !strings.Contains(runner.sent[0], "Cause: command failed: make test-all") {
		t.Fatalf("runtime message missing hard gate failure cause: %q", runner.sent[0])
	}
	if got := len(commands); got != 4 {
		t.Fatalf("pre-review command count = %d, want 4", got)
	}
	if !strings.Contains(commands[0], "git fetch --prune origin repository-agent-orchestrator/issue-88") {
		t.Fatalf("first setup command = %q, want branch fetch", commands[0])
	}
	if !strings.Contains(commands[1], "git worktree add --detach") || !strings.Contains(commands[1], testThirdReviewHeadSHA) {
		t.Fatalf("second setup command = %q, want detached worktree add for reviewed SHA", commands[1])
	}
	if !strings.Contains(commands[2], "::make test-all") {
		t.Fatalf("gate command = %q, want make test-all", commands[2])
	}
	if !strings.Contains(commands[3], "git worktree remove ") {
		t.Fatalf("cleanup command = %q, want git worktree remove", commands[3])
	}

	mu.Lock()
	defer mu.Unlock()
	if countMessagesContaining(notifications, "mandatory test gate failed before review") != 1 {
		t.Fatalf("expected one pre-review hard gate failure notification, got %v", notifications)
	}
	var failureNotification string
	for _, msg := range notifications {
		if strings.Contains(msg, "mandatory test gate failed before review") {
			failureNotification = msg
			break
		}
	}
	if failureNotification == "" {
		t.Fatalf("missing pre-review hard gate failure notification in %v", notifications)
	}
	if !strings.Contains(failureNotification, "Head SHA: `"+testThirdReviewHeadSHA+"`") {
		t.Fatalf("notification missing reviewed head sha: %q", failureNotification)
	}
	if !strings.Contains(failureNotification, "Cause: command failed: make test-all") {
		t.Fatalf("notification missing hard gate cause: %q", failureNotification)
	}
}

func TestStartReviewAgentPreReviewHardGateFailureRestartsCoderWhenRuntimeIsMissing(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/88/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer gh.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-88",
		Role:                    RoleCoder,
		IssueNumber:             88,
		BranchName:              "repository-agent-orchestrator/issue-88",
		PRNumber:                88,
		PRURL:                   "https://github.com/acme/widget/pull/88",
		ObservedPRHeadSHA:       testThirdReviewHeadSHA,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "dead-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	var commands []string
	runner := &stubRunner{
		aliveSet:    true,
		alive:       false,
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-coder-88"},
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all", "make test-coverage"},
		},
		github: newGitHubClientForTest(t, gh),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			commands = append(commands, dir+"::"+command)
			if command == "make test-all" {
				return errors.New("command failed: make test-all")
			}
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.startReviewAgent(context.Background(), snapshot, testThirdReviewHeadSHA); err != nil {
		t.Fatalf("startReviewAgent() error = %v", err)
	}

	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
	if runner.started[0].ID != coder.ID {
		t.Fatalf("restarted agent id = %q, want %q", runner.started[0].ID, coder.ID)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runtime messages sent to coder = %d, want 1", got)
	}
	if got := len(commands); got != 4 {
		t.Fatalf("pre-review command count = %d, want 4", got)
	}
	if !strings.Contains(commands[3], "git worktree remove ") {
		t.Fatalf("cleanup command = %q, want git worktree remove", commands[3])
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.RuntimeHandle.Session != "resumed-coder-88" {
		t.Fatalf("coder runtime session = %q, want %q", coderState.RuntimeHandle.Session, "resumed-coder-88")
	}
	if coderState.State != StateWorking || coderState.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderState.State, coderState.Stopped, StateWorking)
	}
}

func TestStartReviewAgentPreReviewHardGateFailureKeepsPausedCoderPaused(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/89/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer gh.Close()

	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	coder := &Agent{
		ID:                      "agent-89",
		Role:                    RoleCoder,
		IssueNumber:             89,
		BranchName:              "repository-agent-orchestrator/issue-89",
		PRNumber:                89,
		PRURL:                   "https://github.com/acme/widget/pull/89",
		WorktreePath:            worktree,
		ObservedPRHeadSHA:       testFourthReviewHeadSHA,
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	var commands []string
	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unexpected-resume"}}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all", "make test-coverage"},
		},
		github:    newGitHubClientForTest(t, gh),
		agents:    agents,
		runner:    runner,
		messenger: messenger,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			commands = append(commands, dir+"::"+command)
			if command == "make test-all" {
				return errors.New("command failed: make test-all")
			}
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.startReviewAgent(context.Background(), snapshot, testFourthReviewHeadSHA); err != nil {
		t.Fatalf("startReviewAgent() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 for paused coder", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0 for paused coder", got)
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("inbox messages = %d, want 1", got)
	}
	if !strings.Contains(messenger.messages[0], "pre-review hard gate failed") {
		t.Fatalf("inbox message missing hard gate failure marker: %q", messenger.messages[0])
	}
	if got := len(commands); got != 4 {
		t.Fatalf("pre-review command count = %d, want 4", got)
	}
	if !strings.Contains(commands[3], "git worktree remove ") {
		t.Fatalf("cleanup command = %q, want git worktree remove", commands[3])
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateWorking || coderState.Stopped || !coderState.Paused {
		t.Fatalf("coder state = (%s, stopped=%v, paused=%v), want (%s, false, true)", coderState.State, coderState.Stopped, coderState.Paused, StateWorking)
	}
	if coderState.LastPreReviewGateFailureHeadSHA != testFourthReviewHeadSHA {
		t.Fatalf("LastPreReviewGateFailureHeadSHA = %q, want %q", coderState.LastPreReviewGateFailureHeadSHA, testFourthReviewHeadSHA)
	}
	if coderState.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", coderState.ActiveReviewAgentID)
	}
}

func TestStartReviewAgentPreReviewHardGateFailureIncludesCapturedOutput(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		notifications []string
		commands      []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var payload struct {
			Markdown string `json:"markdown"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode webhook payload: %v", err)
		}
		mu.Lock()
		notifications = append(notifications, payload.Markdown)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/88/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer gh.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-88",
		Role:                    RoleCoder,
		IssueNumber:             88,
		BranchName:              "repository-agent-orchestrator/issue-88",
		PRNumber:                88,
		PRURL:                   "https://github.com/acme/widget/pull/88",
		ObservedPRHeadSHA:       testThirdReviewHeadSHA,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			MandatoryTests:  []string{"make test-all"},
		},
		github:   newGitHubClientForTest(t, gh),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			commands = append(commands, dir+"::"+command)
			return nil
		},
		mandatoryTestRunner: func(ctx context.Context, dir string, logPath string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			commands = append(commands, dir+"::"+command)
			return &commandExecutionError{
				command: command,
				cause:   errors.New("exit status 3"),
				detail:  "stdout:\npackage tests failed\nstderr:\ndocker daemon is not running",
			}
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.startReviewAgent(context.Background(), snapshot, testThirdReviewHeadSHA); err != nil {
		t.Fatalf("startReviewAgent() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("review runtime starts = %d, want 0 when pre-review hard gate fails", got)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runtime messages sent to coder = %d, want 1", got)
	}
	for _, want := range []string{
		"pre-review hard gate failed",
		"Cause: exit status 3",
		"stdout:",
		"package tests failed",
		"stderr:",
		"docker daemon is not running",
	} {
		if !strings.Contains(runner.sent[0], want) {
			t.Fatalf("runtime message missing %q: %q", want, runner.sent[0])
		}
	}
	if got := len(commands); got != 4 {
		t.Fatalf("pre-review command count = %d, want 4", got)
	}
	if !strings.Contains(commands[2], "::make test-all") {
		t.Fatalf("gate command = %q, want make test-all", commands[2])
	}
	if !strings.Contains(commands[3], "git worktree remove ") {
		t.Fatalf("cleanup command = %q, want git worktree remove", commands[3])
	}

	mu.Lock()
	defer mu.Unlock()
	var failureNotification string
	for _, msg := range notifications {
		if strings.Contains(msg, "mandatory test gate failed before review") {
			failureNotification = msg
			break
		}
	}
	if failureNotification == "" {
		t.Fatalf("missing pre-review hard gate failure notification in %v", notifications)
	}
	for _, want := range []string{
		"Cause: exit status 3",
		"stdout:",
		"package tests failed",
		"stderr:",
		"docker daemon is not running",
	} {
		if !strings.Contains(failureNotification, want) {
			t.Fatalf("notification missing %q: %q", want, failureNotification)
		}
	}
}

func TestStartReviewAgentCanceledHardGateDoesNotRecordFailure(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/90/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer gh.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-90",
		Role:                    RoleCoder,
		IssueNumber:             90,
		BranchName:              "repository-agent-orchestrator/issue-90",
		PRNumber:                90,
		PRURL:                   "https://github.com/acme/widget/pull/90",
		ObservedPRHeadSHA:       testFourthReviewHeadSHA,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-agent-90"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{}
	var commands []string
	var cleanedRepoPath string
	var cleanedWorktreePath string
	var cleanedBranchName string
	tempDir := t.TempDir()
	repoPath := filepath.Join(tempDir, "repo")
	worktreeDir := filepath.Join(tempDir, "worktrees")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("os.MkdirAll(repoPath) error = %v", err)
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       repoPath,
			WorktreeDir:    worktreeDir,
			LogDir:         filepath.Join(tempDir, "logs"),
			MandatoryTests: []string{"make test-all"},
		},
		github: newGitHubClientForTest(t, gh),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			commands = append(commands, dir+"::"+command)
			if name == "git" && len(args) >= 4 && args[0] == "worktree" && args[1] == "add" {
				if err := os.MkdirAll(args[len(args)-2], 0o755); err != nil {
					return err
				}
			}
			return nil
		},
		cleanupWorktreeFunc: func(ctx context.Context, repoPath, worktreePath, branchName string) error {
			cleanedRepoPath = repoPath
			cleanedWorktreePath = worktreePath
			cleanedBranchName = branchName
			return os.RemoveAll(worktreePath)
		},
		mandatoryTestRunner: func(ctx context.Context, dir string, logPath string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			commands = append(commands, dir+"::"+command)
			return context.Canceled
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.startReviewAgent(context.Background(), snapshot, testFourthReviewHeadSHA); err != nil {
		t.Fatalf("startReviewAgent() error = %v", err)
	}

	var reviewerID string
	for _, agent := range agents.List() {
		if agent.Role == RoleReviewer {
			reviewerID = agent.ID
			break
		}
	}
	if strings.TrimSpace(reviewerID) == "" {
		t.Fatal("reviewer id not found")
	}

	reviewerState, ok := agents.Get(reviewerID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewerID)
	}
	if reviewerState.State != StateDone || !reviewerState.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", reviewerState.State, reviewerState.Stopped, StateDone)
	}

	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if coderState.State != StateWaiting || coderState.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, false)", coderState.State, coderState.Stopped, StateWaiting)
	}
	if coderState.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty for interrupted gate", coderState.LastReviewVerdict)
	}
	if coderState.LastReviewedHeadSHA != "" {
		t.Fatalf("LastReviewedHeadSHA = %q, want empty for interrupted gate", coderState.LastReviewedHeadSHA)
	}
	if coderState.LastPreReviewGateFailureHeadSHA != "" {
		t.Fatalf("LastPreReviewGateFailureHeadSHA = %q, want empty for interrupted gate", coderState.LastPreReviewGateFailureHeadSHA)
	}
	if !coderState.LastPreReviewGateFailureAt.IsZero() {
		t.Fatalf("LastPreReviewGateFailureAt = %s, want zero for interrupted gate", coderState.LastPreReviewGateFailureAt)
	}
	if coderState.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", coderState.ActiveReviewAgentID)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("review runtime starts = %d, want 0 when hard gate is interrupted", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runtime messages sent to coder = %d, want 0 when hard gate is interrupted", got)
	}
	if got := len(commands); got != 3 {
		t.Fatalf("pre-review command count = %d, want 3", got)
	}
	if !strings.Contains(commands[2], "::make test-all") {
		t.Fatalf("gate command = %q, want make test-all", commands[2])
	}
	if cleanedRepoPath != repoPath {
		t.Fatalf("cleanup repo path = %q, want %q", cleanedRepoPath, repoPath)
	}
	if cleanedWorktreePath != reviewerState.WorktreePath {
		t.Fatalf("cleanup worktree path = %q, want %q", cleanedWorktreePath, reviewerState.WorktreePath)
	}
	if cleanedBranchName != "" {
		t.Fatalf("cleanup branch name = %q, want empty for detached review worktree", cleanedBranchName)
	}
	if _, err := os.Stat(reviewerState.WorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review worktree should be removed, got err=%v", err)
	}
}

func TestEnsureReviewAgentForCoderSkipsSameHeadAfterPreReviewGateFailure(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                              "agent-54",
		Role:                            RoleCoder,
		IssueNumber:                     54,
		BranchName:                      "repository-agent-orchestrator/issue-54",
		PRNumber:                        54,
		PRURL:                           "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:               testReviewHeadSHA,
		LastPreReviewGateFailureHeadSHA: testReviewHeadSHA,
		LastPreReviewGateFailureAt:      time.Now().Add(-time.Hour),
		LastReviewVerdict:               ReviewVerdictNeedsChanges,
		State:                           StateWorking,
		LastActivityTime:                time.Now(),
		seenReviewCommentIDs:            make(map[int64]struct{}),
		seenIssueCommentIDs:             make(map[int64]struct{}),
		pendingReviewCommentIDs:         make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewLaunchStarted := make(chan struct{}, 1)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all"},
		},
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected review launch after same-head hard-gate failure")
	case <-time.After(200 * time.Millisecond):
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("reviewer launches = %d, want 0 after same-head hard-gate failure", got)
	}
}

func TestIsManualReviewHoldForHead(t *testing.T) {
	t.Parallel()

	held := Agent{LastReviewedHeadSHA: testReviewHeadSHA, ManualReviewHold: true}
	if !isManualReviewHoldForHead(held, testReviewHeadSHA) {
		t.Fatal("isManualReviewHoldForHead() = false, want true for a head reviewed with no verdict or comment")
	}

	completed := Agent{
		LastReviewedHeadSHA: testReviewHeadSHA,
		ManualReviewHold:    true,
		LastReviewVerdict:   ReviewVerdictThumbsUp,
	}
	if isManualReviewHoldForHead(completed, testReviewHeadSHA) {
		t.Fatal("isManualReviewHoldForHead() = true, want false once a verdict is recorded")
	}

	commented := Agent{
		LastReviewedHeadSHA: testReviewHeadSHA,
		ManualReviewHold:    true,
		LastReviewCommentID: 7,
	}
	if isManualReviewHoldForHead(commented, testReviewHeadSHA) {
		t.Fatal("isManualReviewHoldForHead() = true, want false once a review comment is recorded")
	}

	if isManualReviewHoldForHead(held, testOtherReviewHeadSHA) {
		t.Fatal("isManualReviewHoldForHead() = true, want false for a different head")
	}

	// TestIsManualReviewHoldForHead/adopted_pr_baseline is the regression
	// case for review feedback on PR #180 (Craig): ContinueAgentForIssuePR
	// sets ObservedPRHeadSHA and LastReviewedHeadSHA to the current head
	// for every adopted PR, leaving verdict/comment empty -- the exact
	// same shape RecordManualReviewHold produces, but ManualReviewHold is
	// never set because no hold was ever recorded. Without checking that
	// explicit marker, this shape was indistinguishable from a real hold.
	adoptedBaseline := Agent{LastReviewedHeadSHA: testReviewHeadSHA}
	if isManualReviewHoldForHead(adoptedBaseline, testReviewHeadSHA) {
		t.Fatal("isManualReviewHoldForHead() = true, want false for an adopted PR's normal baseline (no hold was ever recorded)")
	}
}

func TestIsNonRetryableReviewBlockForHead(t *testing.T) {
	t.Parallel()

	nonRetryable := Agent{
		LaunchAttempts: []DurableLaunchAttempt{
			{
				ID:        "review-cycle-1",
				Kind:      DurableLaunchReviewCoordinator,
				Scope:     testReviewHeadSHA,
				Attempt:   1,
				Lifecycle: DurableLaunchFailed,
				Failure:   &DurableLaunchFailure{Kind: DurableLaunchFailureIncomplete},
			},
		},
	}
	if !isNonRetryableReviewBlockForHead(nonRetryable, testReviewHeadSHA) {
		t.Fatal("isNonRetryableReviewBlockForHead() = false, want true for a terminal attempt with no RetryAfter")
	}
	if isNonRetryableReviewBlockForHead(nonRetryable, testOtherReviewHeadSHA) {
		t.Fatal("isNonRetryableReviewBlockForHead() = true, want false for a different head")
	}

	retryable := Agent{
		LaunchAttempts: []DurableLaunchAttempt{
			{
				ID:         "review-cycle-2",
				Kind:       DurableLaunchReviewCoordinator,
				Scope:      testReviewHeadSHA,
				Attempt:    1,
				Lifecycle:  DurableLaunchFailed,
				Failure:    &DurableLaunchFailure{Kind: DurableLaunchFailureIncomplete, Retryable: true},
				RetryAfter: time.Now().UTC().Add(time.Hour),
			},
		},
	}
	if isNonRetryableReviewBlockForHead(retryable, testReviewHeadSHA) {
		t.Fatal("isNonRetryableReviewBlockForHead() = true, want false once a RetryAfter is scheduled")
	}

	running := Agent{
		LaunchAttempts: []DurableLaunchAttempt{
			{
				ID:        "review-cycle-3",
				Kind:      DurableLaunchReviewCoordinator,
				Scope:     testReviewHeadSHA,
				Attempt:   1,
				Lifecycle: DurableLaunchRunning,
			},
		},
	}
	if isNonRetryableReviewBlockForHead(running, testReviewHeadSHA) {
		t.Fatal("isNonRetryableReviewBlockForHead() = true, want false for a non-terminal attempt")
	}

	publishedPartial := Agent{
		LastReviewedHeadSHA: testReviewHeadSHA,
		LastReviewCommentID: 42,
		LaunchAttempts: []DurableLaunchAttempt{
			{
				ID:        "review-cycle-4",
				Kind:      DurableLaunchReviewCoordinator,
				Scope:     testReviewHeadSHA,
				Attempt:   1,
				Lifecycle: DurableLaunchFailed,
				Failure:   &DurableLaunchFailure{Kind: DurableLaunchFailureIncomplete},
			},
		},
	}
	if isNonRetryableReviewBlockForHead(publishedPartial, testReviewHeadSHA) {
		t.Fatal("isNonRetryableReviewBlockForHead() = true, want false once a verdict comment was already published for this head (e.g. finalizePartialNoFindingsReview's bookkeeping after a published partial verdict)")
	}
}

// TestEnsureReviewAgentForCoderSkipsManualReviewHold is the regression
// test for the second-order effect of the review_gate_wedge_after_restart
// interim mitigation (docs/TROUBLESHOOTING.md): stopping a reviewer that
// never reached a verdict makes holdReviewOnInactiveReviewer stamp
// LastReviewedHeadSHA on the coder with no verdict/comment, pausing
// automatic re-launch until a human runs `agent review`. This must keep
// skipping automatic launch (that pause is intentional -- it isn't
// undone here), but see TestFormatReviewerHeadlineSurfacesManualHold for
// the accompanying status-visibility fix.
func TestEnsureReviewAgentForCoderSkipsManualReviewHold(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-1051",
		Role:                    RoleCoder,
		IssueNumber:             1044,
		BranchName:              "repository-agent-orchestrator/issue-1044",
		PRNumber:                1051,
		PRURL:                   "https://github.com/acme/widget/pull/1051",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		LastReviewedHeadSHA:     testReviewHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewLaunchStarted := make(chan struct{}, 1)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all"},
		},
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected automatic review launch while the head is on manual hold")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestEnsureReviewAgentForCoderRelaunchesAfterPreReviewGateFailureOnNewHead(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/54/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                              "agent-54",
		Role:                            RoleCoder,
		IssueNumber:                     54,
		BranchName:                      "repository-agent-orchestrator/issue-54",
		PRNumber:                        54,
		PRURL:                           "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:               testReviewHeadSHA,
		LastPreReviewGateFailureHeadSHA: testReviewHeadSHA,
		LastPreReviewGateFailureAt:      time.Now().Add(-time.Hour),
		LastReviewVerdict:               ReviewVerdictNeedsChanges,
		State:                           StateWorking,
		LastActivityTime:                time.Now().Add(-time.Hour),
		seenReviewCommentIDs:            make(map[int64]struct{}),
		seenIssueCommentIDs:             make(map[int64]struct{}),
		pendingReviewCommentIDs:         make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if !agents.SetPRHeadSHA(coder.ID, testOtherReviewHeadSHA) {
		t.Fatal("SetPRHeadSHA(new head) = false, want true")
	}

	reviewLaunchStarted := make(chan struct{}, 1)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all"},
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	reviewer := waitForReviewAgentForPR(t, agents, 54)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level reviewer launches = %d, want 0 after new head", got)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after relaunch", coder.ID)
	}
	if updated.ActiveReviewAgentID == "" {
		t.Fatal("ActiveReviewAgentID should be set after relaunch")
	}
	if updated.LastPreReviewGateFailureHeadSHA != "" {
		t.Fatalf("LastPreReviewGateFailureHeadSHA = %q, want cleared after successful gate pass", updated.LastPreReviewGateFailureHeadSHA)
	}
}

func TestEnsureReviewAgentForCoderDefersSameHeadRetryUntilRetryTime(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-980",
		Role:                    RoleCoder,
		IssueNumber:             980,
		BranchName:              "repository-agent-orchestrator/issue-980",
		PRNumber:                990,
		PRURL:                   "https://github.com/acme/widget/pull/990",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	coder.LaunchAttempts = []DurableLaunchAttempt{
		incompleteReviewLaunchAttempt(
			t,
			testReviewHeadSHA,
			"review-cycle-1",
			1,
			time.Now().UTC().Add(time.Hour),
		),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewLaunchStarted := make(chan struct{}, 1)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all"},
		},
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected review launch before the same-head retry time")
	case <-time.After(200 * time.Millisecond):
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("reviewer launches = %d, want 0 before the same-head retry time", got)
	}
}

func TestReviewWorktreeSetupFailureConsumesExactDurableAttempt(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		if r.URL.Path != "/repos/acme/widget/issues/990/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-review-setup-attempt",
		Role:                    RoleCoder,
		IssueNumber:             980,
		BranchName:              "repository-agent-orchestrator/issue-980",
		PRNumber:                990,
		PRURL:                   "https://github.com/acme/widget/pull/990",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now().UTC(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     "/tmp/repo",
			WorktreeDir:  t.TempDir(),
			LogDir:       t.TempDir(),
			ReviewPolicy: builtInReviewPolicy(),
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
		cmdRunner: func(
			context.Context,
			string,
			string,
			...string,
		) error {
			return errors.New("fetch failed")
		},
	}

	waitForFailedAttempt := func(want int) DurableLaunchAttempt {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			current, ok := agents.Get(coder.ID)
			if ok {
				attempt, found := reviewLaunchAttemptForHead(
					current,
					testReviewHeadSHA,
				)
				bot.reviewLaunchMu.Lock()
				_, launchPending := bot.pendingReviewLaunches[reviewLaunchKey(
					reviewLaunchKeyForPR(coder.PRNumber),
					testReviewHeadSHA,
				)]
				bot.reviewLaunchMu.Unlock()
				if found && attempt.Attempt == want &&
					attempt.Lifecycle == DurableLaunchFailed &&
					current.ActiveReviewAgentID == "" && !launchPending {
					return attempt
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for failed review attempt %d", want)
		return DurableLaunchAttempt{}
	}

	current, _ := agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(
		context.Background(),
		current,
	); err != nil {
		t.Fatalf("ensureReviewAgentForCoder(first) error = %v", err)
	}
	first := waitForFailedAttempt(1)
	if first.Failure == nil ||
		first.Failure.Kind != DurableLaunchFailureSetup {
		t.Fatalf("first setup attempt = %#v", first)
	}

	agents.mu.Lock()
	for index := range agents.agents[coder.ID].LaunchAttempts {
		attempt := &agents.agents[coder.ID].LaunchAttempts[index]
		if attempt.ID == first.ID {
			attempt.RetryAfter = attempt.FinishedAt
		}
	}
	agents.mu.Unlock()
	current, _ = agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(
		context.Background(),
		current,
	); err != nil {
		t.Fatalf("ensureReviewAgentForCoder(second) error = %v", err)
	}
	second := waitForFailedAttempt(2)
	if second.ID == first.ID || second.Failure == nil ||
		second.Failure.Kind != DurableLaunchFailureSetup {
		t.Fatalf("second setup attempt = %#v, first = %#v", second, first)
	}

	current, _ = agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(
		context.Background(),
		current,
	); err != nil {
		t.Fatalf("ensureReviewAgentForCoder(cooldown) error = %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	current, _ = agents.Get(coder.ID)
	if got := reviewLaunchAttemptCountForHead(
		current,
		testReviewHeadSHA,
	); got != 2 {
		t.Fatalf("same-head launch attempts = %d, want exactly 2", got)
	}
}

func TestRecordReviewIncompleteIsIdempotentAndBoundedByCycle(t *testing.T) {
	agents := NewAgentManager()
	coder := &Agent{
		ID:               "coder-review-retry-state",
		Role:             RoleCoder,
		State:            StateWaiting,
		LastActivityTime: time.Now(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	agents.mu.Lock()
	agents.agents[coder.ID].LaunchAttempts = []DurableLaunchAttempt{
		runningReviewLaunchAttempt(
			t,
			testReviewHeadSHA,
			"cycle-1",
			1,
		),
	}
	agents.mu.Unlock()

	first, ok := agents.RecordReviewIncomplete(coder.ID, "cycle-1", testReviewHeadSHA)
	if !ok || first.Attempt != 1 || first.Failure == nil ||
		first.Failure.Kind != DurableLaunchFailureIncomplete {
		t.Fatalf("first retry state = %#v, recorded=%v", first, ok)
	}
	duplicate, ok := agents.RecordReviewIncomplete(coder.ID, "cycle-1", testReviewHeadSHA)
	if !ok || duplicate != first {
		t.Fatalf("duplicate retry state = %#v, want unchanged %#v", duplicate, first)
	}
	if _, recorded := agents.RecordReviewIncomplete(coder.ID, "", testReviewHeadSHA); recorded {
		t.Fatal("RecordReviewIncomplete() accepted an empty cycle ID")
	}
}

func TestEnsureReviewAgentForCoderAutomaticallyRetriesSameHead(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/990/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-980",
		Role:                    RoleCoder,
		IssueNumber:             980,
		BranchName:              "repository-agent-orchestrator/issue-980",
		PRNumber:                990,
		PRURL:                   "https://github.com/acme/widget/pull/990",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	coder.LaunchAttempts = completedCorrectionLaunchAttempts(t, *coder, 1)
	coder.LaunchAttempts = append(
		coder.LaunchAttempts,
		incompleteReviewLaunchAttempt(
			t,
			testReviewHeadSHA,
			"review-cycle-1",
			1,
			time.Now().UTC().Add(-time.Minute),
		),
	)
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all"},
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	reviewer := waitForReviewAgentForPR(t, agents, 990)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()
	if reviewer.ReviewCycle == nil || reviewer.ReviewCycle.HeadSHA != testReviewHeadSHA {
		t.Fatalf("review cycle = %#v, want same head %s", reviewer.ReviewCycle, testReviewHeadSHA)
	}
	if reviewer.ReviewCycle.Attempt != 2 {
		t.Fatalf("review attempt = %d, want 2", reviewer.ReviewCycle.Attempt)
	}
	if reviewer.ReviewCycle.CorrectionRound != 1 {
		t.Fatalf("correction round = %d, want the one actual coder restart", reviewer.ReviewCycle.CorrectionRound)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok || updated.ActiveReviewAgentID == "" {
		t.Fatal("same-head retry did not reserve an active reviewer")
	}
}

func TestEnsureReviewAgentForCoderDoesNotReplayCompletedNonConvergentHead(
	t *testing.T,
) {
	agents := NewAgentManager()
	coder := &Agent{
		ID:                "coder-non-convergent-head",
		Role:              RoleCoder,
		PRNumber:          312,
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now().UTC(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	attempt := runningReviewLaunchAttempt(
		t,
		testReviewHeadSHA,
		"review-cycle-non-convergent",
		1,
	)
	if _, err := transitionDurableLaunchAttempt(
		&attempt,
		DurableLaunchFailed,
		&DurableLaunchFailure{Kind: DurableLaunchFailureIncomplete},
		time.Time{},
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	agents.mu.Lock()
	agents.agents[coder.ID].LaunchAttempts = []DurableLaunchAttempt{attempt}
	agents.mu.Unlock()

	runner := &stubRunner{}
	bot := &Orchestrator{agents: agents, runner: runner}
	snapshot, _ := agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(
		context.Background(),
		snapshot,
	); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	if len(runner.started) != 0 {
		t.Fatalf("non-convergent same-head review relaunched = %#v", runner.started)
	}
	if _, _, reserved := agents.reserveReviewLaunchAttempt(
		coder.ID,
		testReviewHeadSHA,
		"review-cycle-racing-poll",
		"reviewer-racing-poll",
		true,
		time.Now().UTC(),
	); reserved {
		t.Fatal("authoritative reservation accepted a stale automatic launch")
	}
}

func TestEnsureReviewAgentForCoderRelaunchesAfterReviewIncompleteOnNewHead(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/990/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-980",
		Role:                    RoleCoder,
		IssueNumber:             980,
		BranchName:              "repository-agent-orchestrator/issue-980",
		PRNumber:                990,
		PRURL:                   "https://github.com/acme/widget/pull/990",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now().Add(-time.Hour),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	coder.LaunchAttempts = []DurableLaunchAttempt{
		incompleteReviewLaunchAttempt(
			t,
			testReviewHeadSHA,
			"review-cycle-1",
			1,
			time.Now().UTC().Add(-time.Hour),
		),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if !agents.SetPRHeadSHA(coder.ID, testOtherReviewHeadSHA) {
		t.Fatal("SetPRHeadSHA(new head) = false, want true")
	}

	reviewLaunchStarted := make(chan struct{}, 1)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all"},
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	reviewer := waitForReviewAgentForPR(t, agents, 990)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level reviewer launches = %d, want 0 after new head", got)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after relaunch", coder.ID)
	}
	if updated.ActiveReviewAgentID == "" {
		t.Fatal("ActiveReviewAgentID should be set after relaunch")
	}
	if retry, found := reviewLaunchAttemptForHead(
		updated,
		testReviewHeadSHA,
	); !found || retry.Scope != testReviewHeadSHA {
		t.Fatalf(
			"stale-head launch attempt = %#v, want scope %q preserved",
			retry,
			testReviewHeadSHA,
		)
	}
}

func TestEnsureReviewAgentForCoderSkipsPreviouslyReviewedHeadAfterForcePushBackFromGateFailure(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-55",
		Role:                    RoleCoder,
		IssueNumber:             55,
		BranchName:              "repository-agent-orchestrator/issue-55",
		PRNumber:                55,
		PRURL:                   "https://github.com/acme/widget/pull/55",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if !agents.RecordReviewVerdict(coder.ID, testReviewHeadSHA, ReviewVerdictNeedsChanges, 999) {
		t.Fatal("RecordReviewVerdict() = false, want true")
	}
	if !agents.SetPRHeadSHA(coder.ID, testOtherReviewHeadSHA) {
		t.Fatal("SetPRHeadSHA(H2) = false, want true")
	}
	if !agents.RecordPreReviewGateFailure(coder.ID, testOtherReviewHeadSHA) {
		t.Fatal("RecordPreReviewGateFailure() = false, want true")
	}
	if !agents.SetPRHeadSHA(coder.ID, testReviewHeadSHA) {
		t.Fatal("SetPRHeadSHA(H1) = false, want true")
	}

	reviewLaunchStarted := make(chan struct{}, 1)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			MandatoryTests: []string{"make test-all"},
		},
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if snapshot.LastReviewedHeadSHA != testReviewHeadSHA {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q", snapshot.LastReviewedHeadSHA, testReviewHeadSHA)
	}
	if snapshot.LastReviewVerdict != ReviewVerdictNeedsChanges {
		t.Fatalf("LastReviewVerdict = %q, want %q", snapshot.LastReviewVerdict, ReviewVerdictNeedsChanges)
	}
	if snapshot.LastReviewCommentID != 999 {
		t.Fatalf("LastReviewCommentID = %d, want 999", snapshot.LastReviewCommentID)
	}
	if snapshot.LastPreReviewGateFailureHeadSHA != "" {
		t.Fatalf("LastPreReviewGateFailureHeadSHA = %q, want cleared after force-pushing back to reviewed head", snapshot.LastPreReviewGateFailureHeadSHA)
	}
	if isPreReviewGateFailureForHead(snapshot, testReviewHeadSHA) {
		t.Fatal("reviewed head should not be reclassified as a pre-review gate failure after force-push back")
	}

	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected review launch for already reviewed head after force-push back")
	case <-time.After(200 * time.Millisecond):
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("reviewer launches = %d, want 0 when current head matches a previously reviewed head", got)
	}
}

func TestEnsureReviewAgentForCoderLaunchesOnlyForNewHead(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 501, "body": "existing comment"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	var (
		mu            sync.Mutex
		notifications []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-54",
		Role:                    RoleCoder,
		IssueNumber:             54,
		IssueTitle:              "Fix edge case",
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewLaunchStarted := make(chan struct{}, 4)
	runner := &stubRunner{
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-reviewer",
		},
		startStarted: reviewLaunchStarted,
	}
	commands := make([]string, 0)
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			RepoPath:        "/tmp/repo",
			WorktreeDir:     "/tmp/repo/.worktrees",
			BaseBranch:      "main",
			WebexWebhookURL: webex.URL,
			MandatoryTests:  []string{"make test-all", "make test-coverage"},
		},
		github:   ghClient,
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			commands = append(commands, strings.TrimSpace(dir+"::"+name+" "+strings.Join(args, " ")))
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() first launch error = %v", err)
	}
	firstReviewer := waitForReviewAgentForPR(t, agents, 54)
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level reviewer launches = %d, want 0", got)
	}
	if firstReviewer.RuntimeCWD != firstReviewer.WorktreePath {
		t.Fatalf("first coordinator cwd = %q, want %q", firstReviewer.RuntimeCWD, firstReviewer.WorktreePath)
	}
	if got := firstReviewer.ID; !strings.HasPrefix(got, "review-agent-54-") {
		t.Fatalf("first reviewer id = %q, want review-agent prefix", got)
	}

	afterFirst, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if afterFirst.ActiveReviewAgentID == "" {
		t.Fatal("ActiveReviewAgentID should be set after first launch")
	}

	_ = agents.RecordReviewVerdict(coder.ID, testReviewHeadSHA, ReviewVerdictNeedsChanges, 999)
	_ = agents.SetActiveReviewAgent(coder.ID, "")

	snapshot, _ = agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() same head error = %v", err)
	}
	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected reviewer launch for already reviewed head")
	case <-time.After(200 * time.Millisecond):
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level reviewer launches on same head = %d, want 0", got)
	}

	_ = agents.SetPRHeadSHA(coder.ID, testOtherReviewHeadSHA)
	snapshot, _ = agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() new head error = %v", err)
	}
	secondReviewer := waitForReplacementReviewAgent(
		t,
		agents,
		coder.ID,
		firstReviewer.ID,
	)
	defer func() { _ = bot.StopAgent(context.Background(), secondReviewer.ID) }()
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level reviewer launches after new head = %d, want 0", got)
	}
	if secondReviewer.RuntimeCWD != secondReviewer.WorktreePath {
		t.Fatalf("second coordinator cwd = %q, want %q", secondReviewer.RuntimeCWD, secondReviewer.WorktreePath)
	}
	if got := secondReviewer.ID; !strings.HasPrefix(got, "review-agent-54-") {
		t.Fatalf("second reviewer id = %q, want review-agent prefix", got)
	}
	if got := len(commands); got != 9 {
		t.Fatalf("review setup + lifecycle cleanup + gate command count = %d, want 9: %v", got, commands)
	}
	cleanupCommands := 0
	for _, command := range commands {
		if strings.Contains(command, "git worktree remove ") {
			cleanupCommands++
		}
	}
	if cleanupCommands != 1 {
		t.Fatalf("review coordinator cleanup commands = %d, want 1: %v", cleanupCommands, commands)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		ready := countMessagesContaining(notifications, "convergent review coordinator") == 2
		mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			mu.Lock()
			gotNotifications := append([]string(nil), notifications...)
			mu.Unlock()
			t.Fatalf("expected two review coordinator launch notifications, got %v", gotNotifications)
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for i := range runner.started {
		reviewerID := runner.started[i].ID
		found := false
		for _, msg := range notifications {
			if strings.Contains(msg, "runtime launched for review agent") && strings.Contains(msg, reviewerID) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("runtime launch notifications %v missing reviewer id %q", notifications, reviewerID)
		}
	}
}

func TestStartReviewAgentIncludesCoderGuidancePersistedBeforePRCreation(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues/54/comments" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer gh.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "coder-pre-pr-guidance",
		Role:                    RoleCoder,
		IssueNumber:             54,
		IssueTitle:              "Fix edge case",
		BranchName:              "repository-agent-orchestrator/issue-54",
		WorktreePath:            t.TempDir(),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-session"},
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			BaseBranch:     "main",
			MandatoryTests: []string{"make test"},
		},
		github: newGitHubClientForTest(t, gh),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	guidance := "Backward compatibility is explicitly out of scope for this PR."
	if err := bot.SteerAgent(context.Background(), coder.ID, guidance); err != nil {
		t.Fatalf("SteerAgent() error = %v", err)
	}
	if !agents.SetPR(coder.ID, 54, "Fix edge case", "https://github.com/acme/widget/pull/54", testReviewHeadSHA) {
		t.Fatalf("SetPR(%s) = false", coder.ID)
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after SetPR", coder.ID)
	}
	if err := bot.startReviewAgent(context.Background(), snapshot, testReviewHeadSHA); err != nil {
		t.Fatalf("startReviewAgent() error = %v", err)
	}
	reviewer := waitForReviewAgentForPR(t, agents, 54)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()
	if strings.Join(reviewer.HumanReviewGuidance, "\n") != guidance {
		t.Fatalf("review coordinator guidance = %q, want %q", reviewer.HumanReviewGuidance, guidance)
	}
	if got := len(runner.prompts); got != 0 {
		t.Fatalf("top-level review prompts = %d, want 0", got)
	}
}

func TestEnsureReviewAgentForCoderReplacesReviewGateReviewerAfterHeadDrift(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-54-head-drift",
		Role:                    RoleCoder,
		IssueNumber:             54,
		IssueTitle:              "Replace stale reviewer",
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:       testOtherReviewHeadSHA,
		ActiveReviewAgentID:     "review-agent-54-stale",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	staleWorktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(staleWorktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	staleCycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	staleReviewer := &Agent{
		ID:                           "review-agent-54-stale",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  54,
		PRNumber:                     54,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testReviewHeadSHA,
		ReviewBaselineIssueCommentID: 10,
		WorktreePath:                 staleWorktree,
		ReviewCycle:                  staleCycle,
		State:                        StateReviewGate,
		LastActivityTime:             time.Now(),
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	staleAttempt, err := newDurableLaunchAttempt(
		staleCycle.ID,
		DurableLaunchReviewCoordinator,
		testReviewHeadSHA,
		1,
		staleReviewer.ID,
		"",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("newDurableLaunchAttempt() error = %v", err)
	}
	if _, err := transitionDurableLaunchAttempt(
		&staleAttempt,
		DurableLaunchRunning,
		nil,
		time.Time{},
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	coder.LaunchAttempts = []DurableLaunchAttempt{staleAttempt}
	for _, agent := range []*Agent{coder, staleReviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	reviewLaunchStarted := make(chan struct{}, 2)
	var cleanupPaths []string
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "replacement-review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
		cleanupWorktreeFunc: func(ctx context.Context, repoPath, worktreePath, branchName string) error {
			cleanupPaths = append(cleanupPaths, worktreePath)
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	replacement := waitForReplacementReviewAgent(t, agents, coder.ID, staleReviewer.ID)
	defer func() { _ = bot.StopAgent(context.Background(), replacement.ID) }()

	staleSnapshot, ok := agents.Get(staleReviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", staleReviewer.ID)
	}
	if staleSnapshot.State != StateErrored || !staleSnapshot.Stopped {
		t.Fatalf("stale reviewer state = (%s, stopped=%v), want (%s, true)", staleSnapshot.State, staleSnapshot.Stopped, StateErrored)
	}
	if got := len(cleanupPaths); got != 1 || cleanupPaths[0] != staleWorktree {
		t.Fatalf("cleanup paths = %#v, want [%q]", cleanupPaths, staleWorktree)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after replacement launch", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID == "" || coderSnapshot.ActiveReviewAgentID == staleReviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want replacement reviewer id", coderSnapshot.ActiveReviewAgentID)
	}
	retiredAttempt, found := durableLaunchAttemptByID(
		coderSnapshot.LaunchAttempts,
		staleCycle.ID,
	)
	if !found || retiredAttempt.Lifecycle != DurableLaunchCancelled {
		t.Fatalf(
			"retired review attempt = (%v, %s), want present and cancelled",
			found,
			retiredAttempt.Lifecycle,
		)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0", got)
	}
	if replacement.ID == staleReviewer.ID {
		t.Fatalf("replacement coordinator reused stale id %q", replacement.ID)
	}
}

// TestEnsureReviewAgentForCoderDoesNotReplaceReviewerForUnchangedHead is the
// regression test for issue #148 (symptom 1): reviewLaunchStillOwned can
// return false for reasons that have nothing to do with a genuinely new
// head landing -- in the live incident, the active reviewer's own
// verdict-completion processing was concurrently in flight and had
// already touched one of the fields linkedReviewerOwnsCurrentHead checks,
// before transitioning the reviewer's own State to terminal. Retiring and
// replacing the reviewer in that window (labeling it "stale reviewer
// superseded by new head" when there was no new head at all) wastes a
// full duplicate discovery+verification cycle on a head that already has
// an active reviewer.
//
// This uses Paused (not the live incident's exact field) as a
// representative, easily-constructed reason for reviewLaunchStillOwned to
// return false while both heads genuinely match, since the fix's guard
// checks the heads directly and does not care why ownership lapsed.
func TestEnsureReviewAgentForCoderDoesNotReplaceReviewerForUnchangedHead(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-54-unchanged-head",
		Role:                    RoleCoder,
		IssueNumber:             54,
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		ActiveReviewAgentID:     "review-agent-54-unowned",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	cycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	reviewer := &Agent{
		ID:                           "review-agent-54-unowned",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  54,
		PRNumber:                     54,
		PRURL:                        coder.PRURL,
		BranchName:                   coder.BranchName,
		ObservedPRHeadSHA:            testReviewHeadSHA,
		ReviewBaselineIssueCommentID: 10,
		WorktreePath:                 t.TempDir(),
		ReviewCycle:                  cycle,
		State:                        StateWorking,
		Paused:                       true,
		LastActivityTime:             time.Now(),
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	var cleanupPaths []string
	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
		cleanupWorktreeFunc: func(ctx context.Context, repoPath, worktreePath, branchName string) error {
			cleanupPaths = append(cleanupPaths, worktreePath)
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if reviewerSnapshot.State != StateWorking || reviewerSnapshot.Stopped {
		t.Fatalf(
			"reviewer state after unowned-but-unchanged-head poll = (%s, stopped=%v), want (%s, false) -- it must not be retired just because ownership lapsed for an unrelated reason",
			reviewerSnapshot.State, reviewerSnapshot.Stopped, StateWorking,
		)
	}
	if got := len(cleanupPaths); got != 0 {
		t.Fatalf("cleanup paths = %#v, want none (reviewer must not be retired)", cleanupPaths)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0 (no duplicate reviewer should launch)", got)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after poll", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID != reviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want it to remain %q", coderSnapshot.ActiveReviewAgentID, reviewer.ID)
	}
}

func TestStopReviewerPausesAutomaticReviewForCurrentHead(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-54",
		Role:                    RoleCoder,
		IssueNumber:             54,
		IssueTitle:              "Fix edge case",
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		ActiveReviewAgentID:     "review-agent-54-1",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewCycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	reviewer := &Agent{
		ID:                      "review-agent-54-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             54,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       testReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-54"),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		ReviewCycle:             reviewCycle,
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	reviewLaunchStarted := make(chan struct{}, 2)
	var cleanupPaths []string
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "new-review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
		cleanupWorktreeFunc: func(ctx context.Context, repoPath, worktreePath, branchName string) error {
			cleanupPaths = append(cleanupPaths, worktreePath)
			return nil
		},
	}

	if err := bot.StopAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("StopAgent() error = %v", err)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after StopAgent", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty after stopping reviewer", coderSnapshot.ActiveReviewAgentID)
	}
	if coderSnapshot.LastReviewedHeadSHA != testReviewHeadSHA {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q", coderSnapshot.LastReviewedHeadSHA, testReviewHeadSHA)
	}
	if coderSnapshot.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty manual hold", coderSnapshot.LastReviewVerdict)
	}
	if coderSnapshot.State != StateWorking {
		t.Fatalf("coder state = %s, want %s after stopping reviewer", coderSnapshot.State, StateWorking)
	}

	if err := bot.ensureReviewAgentForCoder(context.Background(), coderSnapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() same head error = %v", err)
	}
	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected reviewer launch after manual stop on same head")
	case <-time.After(200 * time.Millisecond):
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("reviewer launches on held head = %d, want 0", got)
	}

	_ = agents.SetPRHeadSHA(coder.ID, testOtherReviewHeadSHA)
	coderSnapshot, _ = agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(context.Background(), coderSnapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() new head error = %v", err)
	}
	replacement := waitForReplacementReviewAgent(t, agents, coder.ID, reviewer.ID)
	defer func() { _ = bot.StopAgent(context.Background(), replacement.ID) }()
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level reviewer launches after new head = %d, want 0", got)
	}
	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after replacement launch", reviewer.ID)
	}
	if reviewerSnapshot.State != StateStopped || !reviewerSnapshot.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true) after replacement launch", reviewerSnapshot.State, reviewerSnapshot.Stopped, StateStopped)
	}
	if got := len(cleanupPaths); got != 1 || cleanupPaths[0] != reviewer.WorktreePath {
		t.Fatalf("cleanup paths = %#v, want [%q]", cleanupPaths, reviewer.WorktreePath)
	}
	coderSnapshot, ok = agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after replacement launch", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID == "" || coderSnapshot.ActiveReviewAgentID == reviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want replacement reviewer id after new head launch", coderSnapshot.ActiveReviewAgentID)
	}
}

func TestPauseReviewerPausesAutomaticReviewForCurrentHead(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/55/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-55",
		Role:                    RoleCoder,
		IssueNumber:             55,
		IssueTitle:              "Handle parallel gates",
		BranchName:              "repository-agent-orchestrator/issue-55",
		PRNumber:                55,
		PRURL:                   "https://github.com/acme/widget/pull/55",
		ObservedPRHeadSHA:       testThirdReviewHeadSHA,
		ActiveReviewAgentID:     "review-agent-55-1",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewCycle, err := newReviewCycleState(testThirdReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	reviewer := &Agent{
		ID:                      "review-agent-55-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             55,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       testThirdReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-55"),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		ReviewCycle:             reviewCycle,
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	reviewLaunchStarted := make(chan struct{}, 2)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "new-review-session"},
		startStarted: reviewLaunchStarted,
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
		cleanupWorktreeFunc: func(ctx context.Context, repoPath, worktreePath, branchName string) error {
			return nil
		},
	}

	if err := bot.PauseAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after PauseAgent", reviewer.ID)
	}
	if reviewerSnapshot.State != StateWorking || reviewerSnapshot.Stopped || !reviewerSnapshot.Paused {
		t.Fatalf("reviewer state = (%s, stopped=%v, paused=%v), want (%s, false, true)", reviewerSnapshot.State, reviewerSnapshot.Stopped, reviewerSnapshot.Paused, StateWorking)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after PauseAgent", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty after pausing reviewer", coderSnapshot.ActiveReviewAgentID)
	}
	if coderSnapshot.LastReviewedHeadSHA != testThirdReviewHeadSHA {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q", coderSnapshot.LastReviewedHeadSHA, testThirdReviewHeadSHA)
	}
	if coderSnapshot.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty manual hold", coderSnapshot.LastReviewVerdict)
	}
	if coderSnapshot.State != StateWorking {
		t.Fatalf("coder state = %s, want %s after pausing reviewer", coderSnapshot.State, StateWorking)
	}

	if err := bot.ensureReviewAgentForCoder(context.Background(), coderSnapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() same head error = %v", err)
	}
	select {
	case <-reviewLaunchStarted:
		t.Fatal("unexpected reviewer launch after manual pause on same head")
	case <-time.After(200 * time.Millisecond):
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("reviewer launches on held head = %d, want 0", got)
	}

	_ = agents.SetPRHeadSHA(coder.ID, testFourthReviewHeadSHA)
	coderSnapshot, _ = agents.Get(coder.ID)
	if err := bot.ensureReviewAgentForCoder(context.Background(), coderSnapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() new head error = %v", err)
	}
	replacement := waitForReplacementReviewAgent(t, agents, coder.ID, reviewer.ID)
	defer func() { _ = bot.StopAgent(context.Background(), replacement.ID) }()
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level reviewer launches after new head = %d, want 0", got)
	}
}

func TestPauseReviewerKeepsPausedCoderPaused(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-55-paused",
		Role:                    RoleCoder,
		IssueNumber:             155,
		IssueTitle:              "Keep coder paused during review hold",
		BranchName:              "repository-agent-orchestrator/issue-155",
		PRNumber:                155,
		PRURL:                   "https://github.com/acme/widget/pull/155",
		ObservedPRHeadSHA:       testThirdReviewHeadSHA,
		ActiveReviewAgentID:     "review-agent-155-1",
		State:                   StateWaiting,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "review-agent-155-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             155,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       testThirdReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-155"),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		agents: agents,
		runner: runner,
	}

	if err := bot.PauseAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after PauseAgent", reviewer.ID)
	}
	if reviewerSnapshot.State != StateWorking || reviewerSnapshot.Stopped || !reviewerSnapshot.Paused {
		t.Fatalf("reviewer state = (%s, stopped=%v, paused=%v), want (%s, false, true)", reviewerSnapshot.State, reviewerSnapshot.Stopped, reviewerSnapshot.Paused, StateWorking)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after PauseAgent", coder.ID)
	}
	if coderSnapshot.State != StateWorking || coderSnapshot.Stopped || !coderSnapshot.Paused {
		t.Fatalf("coder state = (%s, stopped=%v, paused=%v), want (%s, false, true)", coderSnapshot.State, coderSnapshot.Stopped, coderSnapshot.Paused, StateWorking)
	}
	if coderSnapshot.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty after pausing reviewer", coderSnapshot.ActiveReviewAgentID)
	}
	if coderSnapshot.LastReviewedHeadSHA != testThirdReviewHeadSHA {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q", coderSnapshot.LastReviewedHeadSHA, testThirdReviewHeadSHA)
	}
	if coderSnapshot.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty manual hold", coderSnapshot.LastReviewVerdict)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
}

func TestPauseReviewerDoesNotClobberDifferentActiveReviewer(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-55b",
		Role:                    RoleCoder,
		IssueNumber:             155,
		IssueTitle:              "Keep active reviewer ownership intact",
		BranchName:              "repository-agent-orchestrator/issue-155",
		PRNumber:                155,
		PRURL:                   "https://github.com/acme/widget/pull/155",
		ObservedPRHeadSHA:       testOtherReviewHeadSHA,
		ActiveReviewAgentID:     "review-agent-155-active",
		LastReviewedHeadSHA:     testOtherReviewHeadSHA,
		LastReviewVerdict:       ReviewVerdictNeedsChanges,
		LastReviewCommentID:     321,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	activeReviewer := &Agent{
		ID:                      "review-agent-155-active",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             155,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       testOtherReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-155-active"),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "active-review-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	staleReviewer := &Agent{
		ID:                      "review-agent-155-stale",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             155,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       testReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-155-stale"),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "stale-review-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, activeReviewer, staleReviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		agents: agents,
		runner: runner,
	}

	if err := bot.PauseAgent(context.Background(), staleReviewer.ID); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after PauseAgent", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID != activeReviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want %q", coderSnapshot.ActiveReviewAgentID, activeReviewer.ID)
	}
	if coderSnapshot.LastReviewedHeadSHA != testOtherReviewHeadSHA {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q", coderSnapshot.LastReviewedHeadSHA, testOtherReviewHeadSHA)
	}
	if coderSnapshot.LastReviewVerdict != ReviewVerdictNeedsChanges {
		t.Fatalf("LastReviewVerdict = %q, want %q", coderSnapshot.LastReviewVerdict, ReviewVerdictNeedsChanges)
	}
	if coderSnapshot.LastReviewCommentID != 321 {
		t.Fatalf("LastReviewCommentID = %d, want 321", coderSnapshot.LastReviewCommentID)
	}

	staleSnapshot, ok := agents.Get(staleReviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after PauseAgent", staleReviewer.ID)
	}
	if staleSnapshot.State != StateWorking || staleSnapshot.Stopped || !staleSnapshot.Paused {
		t.Fatalf("reviewer state = (%s, stopped=%v, paused=%v), want (%s, false, true)", staleSnapshot.State, staleSnapshot.Stopped, staleSnapshot.Paused, StateWorking)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
}

func TestUnpauseReviewerRestoresOwnership(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/56":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 56,
				"state":  "open",
				"merged": false,
				"head": map[string]any{
					"sha": testFifthReviewHeadSHA,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-56",
		Role:                    RoleCoder,
		IssueNumber:             56,
		IssueTitle:              "Resume review",
		BranchName:              "repository-agent-orchestrator/issue-56",
		PRNumber:                56,
		PRURL:                   "https://github.com/acme/widget/pull/56",
		ObservedPRHeadSHA:       testFifthReviewHeadSHA,
		LastReviewedHeadSHA:     testFifthReviewHeadSHA,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewCycle, err := newReviewCycleState(testFifthReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	reviewer := &Agent{
		ID:                      "review-agent-56-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             56,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       testFifthReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-56"),
		State:                   StateWorking,
		Paused:                  true,
		ReviewCycle:             reviewCycle,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-review-session"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    newGitHubClientForTest(t, srv),
		agents:    agents,
		runner:    runner,
		messenger: &stubMessenger{},
	}

	if err := bot.UnpauseAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("UnpauseAgent() error = %v", err)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after UnpauseAgent", reviewer.ID)
	}
	if reviewerSnapshot.State != StateWorking || reviewerSnapshot.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, false)", reviewerSnapshot.State, reviewerSnapshot.Stopped, StateWorking)
	}
	if reviewerSnapshot.RuntimeHandle.Session != "" {
		t.Fatalf("review coordinator has top-level runtime session %q", reviewerSnapshot.RuntimeHandle.Session)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after UnpauseAgent", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID != reviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want %q", coderSnapshot.ActiveReviewAgentID, reviewer.ID)
	}
	if coderSnapshot.State != StateWaiting {
		t.Fatalf("coder state = %s, want %s after reviewer unpause", coderSnapshot.State, StateWaiting)
	}
	if coderSnapshot.LastReviewedHeadSHA != "" {
		t.Fatalf("LastReviewedHeadSHA = %q, want empty after reviewer unpause restores ownership", coderSnapshot.LastReviewedHeadSHA)
	}
	if coderSnapshot.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty after reviewer unpause restores ownership", coderSnapshot.LastReviewVerdict)
	}
	if coderSnapshot.LastReviewCommentID != 0 {
		t.Fatalf("LastReviewCommentID = %d, want 0 after reviewer unpause restores ownership", coderSnapshot.LastReviewCommentID)
	}

	if err := bot.ensureReviewAgentForCoder(context.Background(), coderSnapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0 after restoring coordinator ownership", got)
	}
}

func TestUnpauseReviewerRestoresOwnershipWithoutUnpausingPausedCoder(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/156":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 156,
				"state":  "open",
				"merged": false,
				"head": map[string]any{
					"sha": testFifthReviewHeadSHA,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-56-paused",
		Role:                    RoleCoder,
		IssueNumber:             156,
		IssueTitle:              "Resume review without unpausing coder",
		BranchName:              "repository-agent-orchestrator/issue-156",
		PRNumber:                156,
		PRURL:                   "https://github.com/acme/widget/pull/156",
		ObservedPRHeadSHA:       testFifthReviewHeadSHA,
		LastReviewedHeadSHA:     testFifthReviewHeadSHA,
		State:                   StateWaiting,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewCycle, err := newReviewCycleState(testFifthReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	reviewer := &Agent{
		ID:                      "review-agent-156-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             156,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       testFifthReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-156"),
		State:                   StateWorking,
		Paused:                  true,
		ReviewCycle:             reviewCycle,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-review-session"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    newGitHubClientForTest(t, srv),
		agents:    agents,
		runner:    runner,
		messenger: &stubMessenger{},
	}

	if err := bot.UnpauseAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("UnpauseAgent() error = %v", err)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after UnpauseAgent", reviewer.ID)
	}
	if reviewerSnapshot.State != StateWorking || reviewerSnapshot.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, false)", reviewerSnapshot.State, reviewerSnapshot.Stopped, StateWorking)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after UnpauseAgent", coder.ID)
	}
	if coderSnapshot.State != StateWaiting || coderSnapshot.Stopped || !coderSnapshot.Paused {
		t.Fatalf("coder state = (%s, stopped=%v, paused=%v), want (%s, false, true)", coderSnapshot.State, coderSnapshot.Stopped, coderSnapshot.Paused, StateWaiting)
	}
	if coderSnapshot.ActiveReviewAgentID != reviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want %q", coderSnapshot.ActiveReviewAgentID, reviewer.ID)
	}
	if coderSnapshot.LastReviewedHeadSHA != "" {
		t.Fatalf("LastReviewedHeadSHA = %q, want empty after reviewer unpause restores ownership", coderSnapshot.LastReviewedHeadSHA)
	}
	if coderSnapshot.LastReviewVerdict != "" {
		t.Fatalf("LastReviewVerdict = %q, want empty after reviewer unpause restores ownership", coderSnapshot.LastReviewVerdict)
	}
	if coderSnapshot.LastReviewCommentID != 0 {
		t.Fatalf("LastReviewCommentID = %d, want 0 after reviewer unpause restores ownership", coderSnapshot.LastReviewCommentID)
	}

	if err := bot.ensureReviewAgentForCoder(context.Background(), coderSnapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0 after restoring coordinator ownership", got)
	}
}

func TestUnpauseReviewerFailsAfterSameHeadReviewRecorded(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/57":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 57,
				"state":  "open",
				"merged": false,
				"head": map[string]any{
					"sha": "ffffffffffff",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-57",
		Role:                    RoleCoder,
		IssueNumber:             57,
		IssueTitle:              "Obsolete paused review",
		BranchName:              "repository-agent-orchestrator/issue-57",
		PRNumber:                57,
		PRURL:                   "https://github.com/acme/widget/pull/57",
		ObservedPRHeadSHA:       "ffffffffffff",
		LastReviewedHeadSHA:     "ffffffffffff",
		LastReviewVerdict:       ReviewVerdictNeedsChanges,
		LastReviewCommentID:     321,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "review-agent-57-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             57,
		BranchName:              coder.BranchName,
		PRNumber:                coder.PRNumber,
		PRURL:                   coder.PRURL,
		ObservedPRHeadSHA:       "ffffffffffff",
		WorktreePath:            filepath.Join(t.TempDir(), "review-57"),
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unexpected-resume"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    newGitHubClientForTest(t, srv),
		agents:    agents,
		runner:    runner,
		messenger: &stubMessenger{},
	}

	err := bot.UnpauseAgent(context.Background(), reviewer.ID)
	if err == nil {
		t.Fatal("UnpauseAgent() error = nil, want rejection after same-head review recorded")
	}
	if !strings.Contains(err.Error(), "already has a completed review recorded") {
		t.Fatalf("UnpauseAgent() error = %v, want same-head review rejection", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 after rejected unpause", got)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after UnpauseAgent", reviewer.ID)
	}
	if reviewerSnapshot.State != StateWorking || reviewerSnapshot.Stopped || !reviewerSnapshot.Paused {
		t.Fatalf("reviewer state = (%s, stopped=%v, paused=%v), want (%s, false, true)", reviewerSnapshot.State, reviewerSnapshot.Stopped, reviewerSnapshot.Paused, StateWorking)
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after UnpauseAgent", coder.ID)
	}
	if coderSnapshot.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty after rejected reviewer unpause", coderSnapshot.ActiveReviewAgentID)
	}
	if coderSnapshot.LastReviewedHeadSHA != "ffffffffffff" {
		t.Fatalf("LastReviewedHeadSHA = %q, want %q after rejected reviewer unpause", coderSnapshot.LastReviewedHeadSHA, "ffffffffffff")
	}
	if coderSnapshot.LastReviewVerdict != ReviewVerdictNeedsChanges {
		t.Fatalf("LastReviewVerdict = %q, want %q after rejected reviewer unpause", coderSnapshot.LastReviewVerdict, ReviewVerdictNeedsChanges)
	}
	if coderSnapshot.LastReviewCommentID != 321 {
		t.Fatalf("LastReviewCommentID = %d, want 321 after rejected reviewer unpause", coderSnapshot.LastReviewCommentID)
	}
}

func TestUnpauseManualReviewerFailsAfterPRHeadChanges(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/58":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 58,
				"state":  "open",
				"merged": false,
				"head": map[string]any{
					"sha": testOtherReviewHeadSHA,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                      "review-agent-58-manual",
		Role:                    RoleReviewer,
		IssueNumber:             58,
		BranchName:              "feature/manual-review",
		PRNumber:                58,
		PRURL:                   "https://github.com/acme/widget/pull/58",
		ObservedPRHeadSHA:       testReviewHeadSHA,
		WorktreePath:            filepath.Join(t.TempDir(), "review-58"),
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(%s) error = %v", reviewer.ID, err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unexpected-resume"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    newGitHubClientForTest(t, srv),
		agents:    agents,
		runner:    runner,
		messenger: &stubMessenger{},
	}

	err := bot.UnpauseAgent(context.Background(), reviewer.ID)
	if err == nil {
		t.Fatal("UnpauseAgent() error = nil, want head-mismatch rejection")
	}
	if !strings.Contains(err.Error(), "is now on head") {
		t.Fatalf("UnpauseAgent() error = %v, want head-mismatch rejection", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 after rejected unpause", got)
	}

	reviewerSnapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after UnpauseAgent", reviewer.ID)
	}
	if reviewerSnapshot.State != StateWorking || reviewerSnapshot.Stopped || !reviewerSnapshot.Paused {
		t.Fatalf("reviewer state = (%s, stopped=%v, paused=%v), want (%s, false, true)", reviewerSnapshot.State, reviewerSnapshot.Stopped, reviewerSnapshot.Paused, StateWorking)
	}
}
