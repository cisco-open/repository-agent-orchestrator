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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v90/github"
)

type stubRunner struct {
	startErr        error
	startStarted    chan struct{}
	startHook       func()
	stopErr         error
	stopStarted     chan struct{}
	stopRelease     <-chan struct{}
	startHandle     RuntimeHandle
	startNeedsStop  bool
	alive           bool
	aliveSet        bool
	aliveErr        error
	captureOut      string
	captureSeq      []string
	captureIdx      int
	captureErr      error
	sendErr         error
	sendCalls       int
	sent            []string
	started         []Agent
	prompts         []string
	stopped         []RuntimeHandle
	reconcileErr    error
	reconciled      []Agent
	runtimeStateErr error
	runtimeRemoved  []Agent
}

func (r *stubRunner) Start(agent Agent, initialPrompt string) (RuntimeHandle, error) {
	if r.startErr != nil {
		return RuntimeHandle{}, r.startErr
	}
	if r.startNeedsStop && len(r.stopped) == 0 {
		return RuntimeHandle{}, errors.New("runtime restarted before ambiguous session was stopped")
	}
	r.started = append(r.started, agent)
	r.prompts = append(r.prompts, initialPrompt)
	if r.startHook != nil {
		r.startHook()
	}
	if r.startStarted != nil {
		select {
		case r.startStarted <- struct{}{}:
		default:
		}
	}
	if r.startHandle.Kind == "" {
		return RuntimeHandle{Kind: RuntimeKindTmux, Session: "stub-session"}, nil
	}
	return r.startHandle, nil
}

func (r *stubRunner) ValidateReviewWorkerIsolation([]string) error {
	return nil
}

func (r *stubRunner) StartReviewWorkerContext(
	_ context.Context,
	agent Agent,
	_ string,
	_ []string,
) (RuntimeHandle, error) {
	if r.startErr != nil {
		return RuntimeHandle{}, r.startErr
	}
	if r.startHook != nil {
		r.startHook()
	}
	if r.startStarted != nil {
		select {
		case r.startStarted <- struct{}{}:
		default:
		}
	}
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: tmuxSessionName(agent),
	}, nil
}

func (r *stubRunner) Send(handle RuntimeHandle, text string) error {
	r.sendCalls++
	if r.sendErr != nil {
		return r.sendErr
	}
	r.sent = append(r.sent, text)
	return nil
}

func (r *stubRunner) Capture(handle RuntimeHandle, lines int) (string, error) {
	if r.captureErr != nil {
		return "", r.captureErr
	}
	if len(r.captureSeq) > 0 {
		if r.captureIdx >= len(r.captureSeq) {
			return r.captureSeq[len(r.captureSeq)-1], nil
		}
		out := r.captureSeq[r.captureIdx]
		r.captureIdx++
		return out, nil
	}
	return r.captureOut, nil
}

func (r *stubRunner) Stop(handle RuntimeHandle) error {
	if r.stopErr != nil {
		return r.stopErr
	}
	if r.stopStarted != nil {
		select {
		case r.stopStarted <- struct{}{}:
		default:
		}
	}
	if r.stopRelease != nil {
		<-r.stopRelease
	}
	r.stopped = append(r.stopped, handle)
	return nil
}

func (r *stubRunner) IsAlive(handle RuntimeHandle) (bool, error) {
	if r.aliveErr != nil {
		return false, r.aliveErr
	}
	if r.aliveSet {
		return r.alive, nil
	}
	return true, nil
}

func (r *stubRunner) ReconcileRuntimeLaunch(agent Agent) error {
	if r.reconcileErr != nil {
		return r.reconcileErr
	}
	r.reconciled = append(r.reconciled, agent)
	return nil
}

func (r *stubRunner) RemoveRuntimeState(agent Agent) error {
	if r.runtimeStateErr != nil {
		return r.runtimeStateErr
	}
	r.runtimeRemoved = append(r.runtimeRemoved, agent)
	return nil
}

func waitForReviewAgentForPR(
	t *testing.T,
	agents *AgentManager,
	prNumber int,
) Agent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, agent := range agents.List() {
			if agent.Role == RoleReviewer && agent.PRNumber == prNumber &&
				agent.ReviewCycle != nil && agent.State == StateWorking {
				return agent
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for convergent review coordinator for PR #%d", prNumber)
	return Agent{}
}

func waitForReplacementReviewAgent(
	t *testing.T,
	agents *AgentManager,
	coderID string,
	previousReviewerID string,
) Agent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		coder, ok := agents.Get(coderID)
		if ok && coder.ActiveReviewAgentID != "" &&
			coder.ActiveReviewAgentID != previousReviewerID {
			reviewer, found := agents.Get(coder.ActiveReviewAgentID)
			if found && reviewer.ReviewCycle != nil &&
				reviewer.State == StateWorking {
				return reviewer
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for replacement convergent review coordinator for coder %s", coderID)
	return Agent{}
}

func TestLooksLikeInputWait(t *testing.T) {
	pane := `Plan updated.
I can continue once you answer:
Please provide your choice.`

	if !looksLikeInputWait(pane) {
		t.Fatal("looksLikeInputWait() = false, want true")
	}
	if !looksLikeUserInputWait(pane) {
		t.Fatal("looksLikeUserInputWait() = false, want true")
	}

	if looksLikeInputWait("running tests...\nall checks passed") {
		t.Fatal("looksLikeInputWait() = true, want false")
	}
	if looksLikeUserInputWait("100% context left · ? for shortcuts") {
		t.Fatal("looksLikeUserInputWait() = true, want false")
	}

	approvalPrompt := "\x1b[1mWould you like to run the following command?\x1b[0m\n" +
		"1. Yes, proceed (y)\n" +
		"2. No, and tell Codex what to do differently (esc)\n" +
		"Press enter to confirm or esc to cancel"
	if !looksLikeInputWait(approvalPrompt) {
		t.Fatal("looksLikeInputWait() = false for approval prompt, want true")
	}
	if !looksLikeUserInputWait(approvalPrompt) {
		t.Fatal("looksLikeUserInputWait() = false for approval prompt, want true")
	}
}

func TestRuntimeLoopFingerprintIgnoresWorkingTimer(t *testing.T) {
	t.Parallel()

	first := "• Ran gh pr diff 136 --patch\n• Working (1h 17m 05s • esc to interrupt)"
	second := "• Ran gh pr diff 136 --patch\n• Working (1h 19m 42s • esc to interrupt)"

	if !looksLikeWorkingLoop(first) {
		t.Fatal("looksLikeWorkingLoop(first) = false, want true")
	}
	if got, want := runtimeLoopFingerprint(first), runtimeLoopFingerprint(second); got != want {
		t.Fatalf("runtimeLoopFingerprint() mismatch for timer-only change: %q vs %q", got, want)
	}
}

func TestDetectInputWaitSendsSingleAlertAfterThreshold(t *testing.T) {
	var mu sync.Mutex
	notifications := make([]string, 0)

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
		cfg:      Config{PollInterval: 20 * time.Second, WebexWebhookURL: webex.URL},
		notifier: NewWebexNotifier(webex.URL),
		runner: &stubRunner{
			captureOut: "Please provide input to continue.",
		},
	}

	agent := Agent{
		ID:          "agent-11",
		IssueNumber: 11,
		BranchName:  "repository-agent-orchestrator/issue-11",
		State:       StateWorking,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-11",
		},
	}

	start := time.Unix(1700000000, 0)
	if err := bot.detectInputWait(context.Background(), agent, start); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}
	if err := bot.detectInputWait(context.Background(), agent, start.Add(59*time.Second)); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}
	mu.Lock()
	if len(notifications) != 0 {
		t.Fatalf("notifications before threshold = %d, want 0", len(notifications))
	}
	mu.Unlock()

	if err := bot.detectInputWait(context.Background(), agent, start.Add(time.Minute)); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}
	if err := bot.detectInputWait(context.Background(), agent, start.Add(2*time.Minute)); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notifications) != 1 {
		t.Fatalf("notifications = %d, want 1", len(notifications))
	}
	if !strings.Contains(notifications[0], "appears stuck waiting for user input") {
		t.Fatalf("unexpected notification body: %q", notifications[0])
	}
	if !strings.Contains(notifications[0], "> Please provide input to continue.") {
		t.Fatalf("notification missing runtime excerpt: %q", notifications[0])
	}
	runner := bot.runner.(*stubRunner)
	if got := len(runner.sent); got != 0 {
		t.Fatalf("auto-continue nudges = %d, want 0", got)
	}
}

func TestDetectRuntimeWorkingLoopRestartsCoderAfterGracePeriod(t *testing.T) {
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

	runner := &stubRunner{
		captureSeq: []string{
			"• Ran go test ./...\n• Working (3m 00s • esc to interrupt)",
			"• Ran go test ./...\n• Working (10m 02s • esc to interrupt)",
			"• Ran go test ./...\n• Working (19m 15s • esc to interrupt)",
		},
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coding-agent-21-recovered",
		},
	}
	agents := NewAgentManager()
	bot := &Orchestrator{
		cfg: Config{
			PollInterval:    20 * time.Second,
			WebexWebhookURL: webex.URL,
		},
		notifier:           NewWebexNotifier(webex.URL),
		agents:             agents,
		runner:             runner,
		runtimeLoopByAgent: make(map[string]runtimeLoopStatus),
		inputWaitByAgent:   make(map[string]inputWaitStatus),
	}
	agent := Agent{
		ID:                  "coding-agent-21",
		Role:                RoleCoder,
		IssueNumber:         21,
		BranchName:          "repository-agent-orchestrator/issue-21",
		PRNumber:            21,
		PRURL:               "https://github.com/acme/widget/pull/21",
		LastReviewedHeadSHA: "deadbeef21",
		LastReviewVerdict:   ReviewVerdictNeedsChanges,
		LastReviewCommentID: 2101,
		State:               StateWorking,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coding-agent-21",
		},
	}
	if err := agents.Add(&agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	start := time.Unix(1700001000, 0)
	if _, shouldContinue, err := bot.detectRuntimeWorkingLoop(context.Background(), agent, start); err != nil {
		t.Fatalf("detectRuntimeWorkingLoop() first call error = %v", err)
	} else if !shouldContinue {
		t.Fatal("detectRuntimeWorkingLoop() unexpectedly stopped coder")
	}
	if _, shouldContinue, err := bot.detectRuntimeWorkingLoop(context.Background(), agent, start.Add(10*time.Minute)); err != nil {
		t.Fatalf("detectRuntimeWorkingLoop() second call error = %v", err)
	} else if !shouldContinue {
		t.Fatal("detectRuntimeWorkingLoop() unexpectedly stopped coder after alert")
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("stopped runtimes during grace period = %d, want 0", got)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("started runtimes during grace period = %d, want 0", got)
	}

	if _, shouldContinue, err := bot.detectRuntimeWorkingLoop(context.Background(), agent, start.Add(20*time.Minute)); err != nil {
		t.Fatalf("detectRuntimeWorkingLoop() third call error = %v", err)
	} else if shouldContinue {
		t.Fatal("detectRuntimeWorkingLoop() should stop polling the replaced coder runtime")
	}

	if got := len(runner.stopped); got != 1 {
		t.Fatalf("stopped runtimes after recovery = %d, want 1", got)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("started runtimes after recovery = %d, want 1", got)
	}
	if prompt := runner.prompts[0]; !strings.Contains(prompt, "This recovery prompt is the next Repository Agent Orchestrator instruction") || !strings.Contains(prompt, "Preserve all existing work") || !strings.Contains(prompt, "Verdict: NEEDS_CHANGES") || !strings.Contains(prompt, "Review comment ID: 2101") {
		t.Fatalf("recovery prompt = %q, want autonomous work-preserving guidance", prompt)
	}
	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", agent.ID)
	}
	if updated.State != StateWorking || updated.Stopped {
		t.Fatalf("recovered coder state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateWorking)
	}
	if got, want := updated.RuntimeHandle.Session, "coding-agent-21-recovered"; got != want {
		t.Fatalf("recovered runtime session = %q, want %q", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	if countMessagesContaining(notifications, "appears stuck in a working loop") != 1 {
		t.Fatalf("expected one working-loop notification, got %v", notifications)
	}
	if !strings.Contains(notifications[0], "will restart the coding runtime automatically") {
		t.Fatalf("notification missing automatic recovery guidance: %q", notifications[0])
	}
	if !strings.Contains(notifications[0], "> • Ran go test ./...") {
		t.Fatalf("notification missing runtime excerpt: %q", notifications[0])
	}
	if countMessagesContaining(notifications, "replaced runtime session") != 1 {
		t.Fatalf("expected one runtime replacement notification, got %v", notifications)
	}
}

func TestHandleStalledCoderAgentMarksRestartFailureErrored(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:               "coding-agent-22",
		Role:             RoleCoder,
		IssueNumber:      22,
		BranchName:       "repository-agent-orchestrator/issue-22",
		WorktreePath:     t.TempDir(),
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coding-agent-22-stalled",
		},
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	runner := &stubRunner{startErr: errors.New("restart failed")}
	bot := &Orchestrator{
		agents: agents,
		runner: runner,
	}
	err := bot.handleStalledCoderAgent(context.Background(), *coder, "• Working (20m • esc to interrupt)")
	if err == nil || !strings.Contains(err.Error(), "failed to restart stalled coding runtime") {
		t.Fatalf("handleStalledCoderAgent() error = %v, want restart failure", err)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("stopped runtimes = %d, want 1", got)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", coder.ID)
	}
	if updated.State != StateErrored || updated.Stopped {
		t.Fatalf("coder state after failed recovery = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateErrored)
	}
	if updated.RuntimeHandle.Session != "" {
		t.Fatalf("runtime session after failed recovery = %q, want empty", updated.RuntimeHandle.Session)
	}
}

func TestDetectInputWaitAutoContinueNudgesAreUnlimited(t *testing.T) {
	var mu sync.Mutex
	notifications := make([]string, 0)
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
		cfg:      Config{PollInterval: 20 * time.Second, WebexWebhookURL: webex.URL},
		notifier: NewWebexNotifier(webex.URL),
		runner: &stubRunner{
			captureOut: "100% context left · ? for shortcuts",
		},
	}
	agent := Agent{
		ID:          "agent-12",
		IssueNumber: 12,
		BranchName:  "repository-agent-orchestrator/issue-12",
		State:       StateWorking,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-12",
		},
	}

	start := time.Unix(1700000100, 0)
	if err := bot.detectInputWait(context.Background(), agent, start); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}
	const nudgeRounds = 10
	for i := 1; i <= nudgeRounds; i++ {
		if err := bot.detectInputWait(context.Background(), agent, start.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("detectInputWait() error = %v", err)
		}
	}

	runner := bot.runner.(*stubRunner)
	if got, want := len(runner.sent), nudgeRounds; got != want {
		t.Fatalf("auto-continue nudges = %d, want %d", got, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := len(notifications); got != 0 {
		t.Fatalf("notifications = %d, want 0", got)
	}
}

func TestDetectInputWaitSkipsAutoContinueWhenActiveReviewInProgress(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                  "agent-14",
		Role:                RoleCoder,
		IssueNumber:         14,
		BranchName:          "repository-agent-orchestrator/issue-14",
		PRNumber:            14,
		ActiveReviewAgentID: "review-agent-14-1",
		State:               StateWorking,
		LastActivityTime:    time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-14",
		},
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	reviewer := &Agent{
		ID:                "review-agent-14-1",
		Role:              RoleReviewer,
		ParentAgentID:     coder.ID,
		IssueNumber:       14,
		BranchName:        coder.BranchName,
		PRNumber:          14,
		ObservedPRHeadSHA: testReviewHeadSHA,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-agent-14-session",
		},
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{
		captureOut: "100% context left · ? for shortcuts",
	}
	bot := &Orchestrator{
		cfg:    Config{PollInterval: 20 * time.Second},
		agents: agents,
		runner: runner,
	}

	start := time.Unix(1700000300, 0)
	if err := bot.detectInputWait(context.Background(), *coder, start); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}
	if err := bot.detectInputWait(context.Background(), *coder, start.Add(2*time.Minute)); err != nil {
		t.Fatalf("detectInputWait() second call error = %v", err)
	}

	if got := len(runner.sent); got != 0 {
		t.Fatalf("auto-continue nudges = %d, want 0", got)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updated.State != StateWaiting {
		t.Fatalf("coder state = %s, want %s", updated.State, StateWaiting)
	}
}

func TestDetectInputWaitHandlesANSIRedrawsForApprovalPrompt(t *testing.T) {
	var mu sync.Mutex
	notifications := make([]string, 0)
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
		cfg:      Config{PollInterval: 20 * time.Second, WebexWebhookURL: webex.URL},
		notifier: NewWebexNotifier(webex.URL),
		runner: &stubRunner{
			captureSeq: []string{
				"\x1b[1mWould you like to run the following command?\x1b[0m\n1. Yes, proceed (y)\n2. No, and tell Codex what to do differently (esc)\nPress enter to confirm or esc to cancel",
				"\x1b[2J\x1b[3;3HWould you like to run the following command?\n1. Yes, proceed (y)\n2. No, and tell Codex what to do differently (esc)\nPress enter to confirm or esc to cancel",
				"\x1b[1mWould you like to run the following command?\x1b[0m\nPress enter to confirm or esc to cancel",
			},
		},
	}
	agent := Agent{
		ID:          "agent-13",
		IssueNumber: 13,
		BranchName:  "repository-agent-orchestrator/issue-13",
		State:       StateWorking,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-13",
		},
	}

	start := time.Unix(1700000200, 0)
	if err := bot.detectInputWait(context.Background(), agent, start); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}
	if err := bot.detectInputWait(context.Background(), agent, start.Add(time.Minute)); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}
	if err := bot.detectInputWait(context.Background(), agent, start.Add(2*time.Minute)); err != nil {
		t.Fatalf("detectInputWait() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := len(notifications), 1; got != want {
		t.Fatalf("notifications = %d, want %d", got, want)
	}
	if !strings.Contains(notifications[0], "appears stuck waiting for user input") {
		t.Fatalf("unexpected notification body: %q", notifications[0])
	}
	if !strings.Contains(notifications[0], "> Would you like to run the following command?") {
		t.Fatalf("notification missing approval prompt excerpt: %q", notifications[0])
	}
	runner := bot.runner.(*stubRunner)
	if got := len(runner.sent); got != 0 {
		t.Fatalf("auto-continue nudges = %d, want 0", got)
	}
}

func TestBuildTaskMarkdownIncludesRequiredConstraints(t *testing.T) {
	issue := &github.Issue{
		Number: github.Int(9),
		Title:  github.String("Fix bug"),
		Body:   github.String("Body"),
	}
	bot := &Orchestrator{cfg: Config{BaseBranch: "main", MandatoryTests: []string{"make test-all", "make test-coverage"}}}
	md := bot.buildTaskMarkdown(Agent{ID: "coding-agent-9-1700000000"}, issue)

	if !strings.Contains(md, "Do not create or modify `AGENTS.md` unless the issue explicitly requires it.") {
		t.Fatalf("buildTaskMarkdown() missing AGENTS.md instruction: %q", md)
	}
	if !strings.Contains(md, "Run the mandatory test commands before making changes:") {
		t.Fatalf("buildTaskMarkdown() missing baseline command instruction: %q", md)
	}
	if !strings.Contains(md, "- `make test-all`") || !strings.Contains(md, "- `make test-coverage`") {
		t.Fatalf("buildTaskMarkdown() missing mandatory test list: %q", md)
	}
	if !strings.Contains(md, "Run the same mandatory test commands again after making changes:") {
		t.Fatalf("buildTaskMarkdown() missing post-change command instruction: %q", md)
	}
	if !strings.Contains(md, "Never stage or commit Repository Agent Orchestrator runtime artifacts under `.repository-agent-orchestrator/` or `.worktrees/`; `.repository-agent-orchestrator/HANDOFF.yaml` is for Repository Agent Orchestrator state capture only.") {
		t.Fatalf("buildTaskMarkdown() missing runtime artifact commit rule: %q", md)
	}
	if !strings.Contains(md, "Open a draft pull request against `main`.") {
		t.Fatalf("buildTaskMarkdown() missing draft PR instruction: %q", md)
	}
	if !strings.Contains(md, "`CODEX_AGENT_ID: coding-agent-9-1700000000`") || !strings.Contains(md, "`CODEX_AGENT_ROLE: coder`") {
		t.Fatalf("buildTaskMarkdown() missing agent marker instruction: %q", md)
	}
	if !strings.Contains(md, "When writing PR comments/replies, use real line breaks and Markdown; never include literal `\\n` sequences in posted text.") {
		t.Fatalf("buildTaskMarkdown() missing PR comment newline instruction: %q", md)
	}
}
