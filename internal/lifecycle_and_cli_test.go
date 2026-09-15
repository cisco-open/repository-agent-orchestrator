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
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func captureStdoutStderr(t *testing.T, fn func() int) (string, string, int) {
	t.Helper()
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(stdout) error = %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(stderr) error = %v", err)
	}

	oldStdout := os.Stdout
	oldStderr := os.Stderr
	os.Stdout = stdoutW
	os.Stderr = stderrW
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	code := fn()

	if err := stdoutW.Close(); err != nil {
		t.Fatalf("stdout close error = %v", err)
	}
	if err := stderrW.Close(); err != nil {
		t.Fatalf("stderr close error = %v", err)
	}

	stdoutBytes, err := io.ReadAll(stdoutR)
	if err != nil {
		t.Fatalf("ReadAll(stdout) error = %v", err)
	}
	stderrBytes, err := io.ReadAll(stderrR)
	if err != nil {
		t.Fatalf("ReadAll(stderr) error = %v", err)
	}
	_ = stdoutR.Close()
	_ = stderrR.Close()

	return string(stdoutBytes), string(stderrBytes), code
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	oldStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("stdout close error = %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll(stdout) error = %v", err)
	}
	_ = r.Close()
	return string(out)
}

func TestFileAgentMessengerWritesAgentArtifacts(t *testing.T) {
	worktree := t.TempDir()
	agent := Agent{WorktreePath: worktree}
	messenger := &FileAgentMessenger{}

	if err := messenger.SendMessage(agent, "hello inbox"); err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
	inboxFiles, err := filepath.Glob(filepath.Join(worktree, ".repository-agent-orchestrator", "INBOX", "*.md"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(inboxFiles) != 1 {
		t.Fatalf("inbox file count = %d, want 1", len(inboxFiles))
	}
	content, err := os.ReadFile(inboxFiles[0])
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", inboxFiles[0], err)
	}
	if string(content) != "hello inbox" {
		t.Fatalf("inbox content = %q, want %q", string(content), "hello inbox")
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := messenger.SendMessageOnce(
			agent,
			"review-verdict-101-abcdef123456",
			"durable review verdict",
		); err != nil {
			t.Fatalf("SendMessageOnce() error = %v", err)
		}
	}
	inboxFiles, err = filepath.Glob(
		filepath.Join(
			worktree,
			".repository-agent-orchestrator",
			"INBOX",
			"*.md",
		),
	)
	if err != nil {
		t.Fatalf("Glob() after SendMessageOnce error = %v", err)
	}
	if len(inboxFiles) != 2 {
		t.Fatalf(
			"inbox file count after idempotent delivery = %d, want 2",
			len(inboxFiles),
		)
	}
	idempotentContent, err := os.ReadFile(
		filepath.Join(
			worktree,
			".repository-agent-orchestrator",
			"INBOX",
			"review-verdict-101-abcdef123456.md",
		),
	)
	if err != nil {
		t.Fatalf("ReadFile(idempotent inbox message) error = %v", err)
	}
	if string(idempotentContent) != "durable review verdict" {
		t.Fatalf(
			"idempotent inbox content = %q, want %q",
			string(idempotentContent),
			"durable review verdict",
		)
	}

	if err := messenger.SendTask(agent, "# Task\n"); err != nil {
		t.Fatalf("SendTask() error = %v", err)
	}
	taskPath := filepath.Join(worktree, ".repository-agent-orchestrator", "TASK.md")
	taskContent, err := os.ReadFile(taskPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", taskPath, err)
	}
	if string(taskContent) != "# Task\n" {
		t.Fatalf("task content = %q, want %q", string(taskContent), "# Task\\n")
	}

	if err := messenger.SendContext(agent, "# Context\n"); err != nil {
		t.Fatalf("SendContext() error = %v", err)
	}
	contextPath := filepath.Join(worktree, ".repository-agent-orchestrator", "CONTEXT.md")
	contextContent, err := os.ReadFile(contextPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", contextPath, err)
	}
	if string(contextContent) != "# Context\n" {
		t.Fatalf("context content = %q, want %q", string(contextContent), "# Context\\n")
	}

	if err := messenger.SendHandoff(agent, "schema_version: 1\n"); err != nil {
		t.Fatalf("SendHandoff() error = %v", err)
	}
	handoffPath := filepath.Join(worktree, ".repository-agent-orchestrator", "HANDOFF.yaml")
	handoffContent, err := os.ReadFile(handoffPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", handoffPath, err)
	}
	if string(handoffContent) != "schema_version: 1\n" {
		t.Fatalf("handoff content = %q, want %q", string(handoffContent), "schema_version: 1\\n")
	}
}

func TestSteerAgent(t *testing.T) {
	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}

	agent := &Agent{
		ID:                      "agent-1",
		IssueNumber:             1,
		WorktreePath:            worktree,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "session-1"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		agents:    agents,
		runner:    runner,
		messenger: messenger,
		inputWaitByAgent: map[string]inputWaitStatus{
			agent.ID: {alerted: true, lastChange: time.Unix(0, 0)},
		},
	}

	if err := bot.SteerAgent(context.Background(), agent.ID, "  fix tests now  "); err != nil {
		t.Fatalf("SteerAgent() error = %v", err)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}
	if runner.sent[0] != "fix tests now" {
		t.Fatalf("runner message = %q, want %q", runner.sent[0], "fix tests now")
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("messenger messages = %d, want 1", got)
	}
	if !strings.Contains(messenger.messages[0], "# Steering Message") {
		t.Fatalf("steering message missing header: %q", messenger.messages[0])
	}
	status := bot.getInputWaitStatus(agent.ID)
	if status.alerted {
		t.Fatal("input wait alert should be cleared after steering")
	}

	if err := bot.SteerAgent(context.Background(), "missing", "msg"); err == nil {
		t.Fatal("SteerAgent(missing) error = nil, want error")
	}

	stopped := *agent
	stopped.ID = "agent-stopped"
	stopped.Stopped = true
	if err := agents.Add(&stopped); err != nil {
		t.Fatalf("Add(stopped) error = %v", err)
	}
	if err := bot.SteerAgent(context.Background(), stopped.ID, "msg"); err == nil {
		t.Fatal("SteerAgent(stopped) error = nil, want error")
	}
}

func TestSteerAgentPersistsHumanReviewGuidanceForCoderBeforePRCreation(t *testing.T) {
	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}

	coder := &Agent{
		ID:                      "coder-pre-pr",
		Role:                    RoleCoder,
		IssueNumber:             12,
		BranchName:              "repository-agent-orchestrator/issue-12",
		WorktreePath:            worktree,
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

	runner := &stubRunner{}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		agents:    agents,
		runner:    runner,
		messenger: messenger,
	}

	guidance := "Do not add backwards-compatibility code for this issue."
	if err := bot.SteerAgent(context.Background(), coder.ID, guidance); err != nil {
		t.Fatalf("SteerAgent() error = %v", err)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after steering", coder.ID)
	}
	if got := strings.Join(updatedCoder.HumanReviewGuidance, " | "); got != guidance {
		t.Fatalf("HumanReviewGuidance = %q, want %q", got, guidance)
	}
}

func TestSteerAgentResumesCoderFromWaitingReviewState(t *testing.T) {
	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}

	coder := &Agent{
		ID:                      "coder-escalated",
		Role:                    RoleCoder,
		IssueNumber:             981,
		PRNumber:                991,
		PRURL:                   "https://github.com/infrasec-cto/zeekfoundry/pull/991",
		BranchName:              "repository-agent-orchestrator/issue-981",
		WorktreePath:            worktree,
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
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		agents:    agents,
		runner:    runner,
		messenger: messenger,
	}

	if err := bot.SteerAgent(context.Background(), coder.ID, "address the review feedback"); err != nil {
		t.Fatalf("SteerAgent() error = %v", err)
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after steering", coder.ID)
	}
	if updated.State != StateWorking {
		t.Fatalf("coder state after steering = %s, want %s", updated.State, StateWorking)
	}

	summary := issueContextSummary{Coder: &updated, PRNumber: updated.PRNumber}
	if got := formatWorkflowState(summary); got != "coding" {
		t.Fatalf("formatWorkflowState() = %q, want %q", got, "coding")
	}
}

// TestFormatReviewerHeadlineSurfacesManualHold covers the status-visibility
// gap found live: once a reviewer that never reached a verdict is stopped
// mid review-gate-wedge, holdReviewOnInactiveReviewer marks the current
// head reviewed with no verdict so automatic re-launch pauses -- and with
// the reviewer gone, `status` had nothing distinguishing that from a
// plain idle issue. formatReviewerHeadline must call it out instead of
// reporting "none".
func TestFormatReviewerHeadlineSurfacesManualHold(t *testing.T) {
	coder := Agent{
		ID:                  "coder-1051",
		Role:                RoleCoder,
		PRNumber:            1051,
		ObservedPRHeadSHA:   "5ced33017e7cd0a3ad7d2934ef235875d541518a",
		LastReviewedHeadSHA: "5ced33017e7cd0a3ad7d2934ef235875d541518a",
		ManualReviewHold:    true,
	}
	summary := issueContextSummary{Coder: &coder, PRNumber: coder.PRNumber}

	got := formatReviewerHeadline(summary)
	if !strings.Contains(got, "manual hold") || !strings.Contains(got, "agent review 1051") {
		t.Fatalf("formatReviewerHeadline() = %q, want it to name the manual hold and the agent review command", got)
	}
}

// TestFormatReviewerHeadlineDoesNotFlagAdoptedPRBaselineAsAHold is the
// regression test for review feedback on PR #180 (Craig):
// ContinueAgentForIssuePR sets ObservedPRHeadSHA and LastReviewedHeadSHA
// to the current head for every adopted PR, leaving verdict/comment
// empty -- the exact same shape a real manual hold has, but
// ManualReviewHold is never set because no hold was ever recorded.
// status must not falsely report this PR as on manual hold and tell the
// operator to run `agent review`.
func TestFormatReviewerHeadlineDoesNotFlagAdoptedPRBaselineAsAHold(t *testing.T) {
	coder := Agent{
		ID:                  "coder-1051",
		Role:                RoleCoder,
		PRNumber:            1051,
		AdoptedPR:           true,
		ObservedPRHeadSHA:   "5ced33017e7cd0a3ad7d2934ef235875d541518a",
		LastReviewedHeadSHA: "5ced33017e7cd0a3ad7d2934ef235875d541518a",
	}
	summary := issueContextSummary{Coder: &coder, PRNumber: coder.PRNumber}

	if got := formatReviewerHeadline(summary); strings.Contains(got, "manual hold") {
		t.Fatalf("formatReviewerHeadline() = %q, want no manual-hold report for a freshly adopted PR's normal baseline", got)
	}
}

// TestFormatReviewerHeadlineSurfacesNonRetryableReviewBlock covers a second
// status-visibility gap found live (issue #184): a review that ends via
// recordReviewIncompleteForCoder(..., retryable=false) -- e.g.
// finalizePartialNoFindingsReview -- leaves the coder's review-coordinator
// DurableLaunchAttempt terminally failed with RetryAfter unset, which
// permanently and silently blocks ensureReviewAgentForCoder's automatic
// relaunch for that exact head. With the reviewer gone, `status` had
// nothing distinguishing that from a plain idle issue.
// formatReviewerHeadline must call it out instead of reporting "none".
func TestFormatReviewerHeadlineSurfacesNonRetryableReviewBlock(t *testing.T) {
	headSHA := "5ced33017e7cd0a3ad7d2934ef235875d541518a"
	coder := Agent{
		ID:                "coder-1051",
		Role:              RoleCoder,
		PRNumber:          1051,
		ObservedPRHeadSHA: headSHA,
		LaunchAttempts: []DurableLaunchAttempt{
			{
				ID:        "review-cycle-1",
				Kind:      DurableLaunchReviewCoordinator,
				Scope:     headSHA,
				Attempt:   1,
				OwnerID:   "review-agent-1051",
				Lifecycle: DurableLaunchFailed,
				Failure:   &DurableLaunchFailure{Kind: DurableLaunchFailureIncomplete},
			},
		},
	}
	summary := issueContextSummary{Coder: &coder, PRNumber: coder.PRNumber}

	got := formatReviewerHeadline(summary)
	if !strings.Contains(got, "automatic retry disabled") || !strings.Contains(got, "agent review 1051") {
		t.Fatalf("formatReviewerHeadline() = %q, want it to name the disabled retry and the agent review command", got)
	}
}

func TestFormatReviewerHeadlineReportsNoneForGenuinelyReviewedHead(t *testing.T) {
	coder := Agent{
		ID:                  "coder-1051",
		Role:                RoleCoder,
		PRNumber:            1051,
		ObservedPRHeadSHA:   "5ced33017e7cd0a3ad7d2934ef235875d541518a",
		LastReviewedHeadSHA: "5ced33017e7cd0a3ad7d2934ef235875d541518a",
		LastReviewVerdict:   ReviewVerdictThumbsUp,
		LastReviewCommentID: 42,
	}
	summary := issueContextSummary{Coder: &coder, PRNumber: coder.PRNumber}

	if got := formatReviewerHeadline(summary); got != "none" {
		t.Fatalf("formatReviewerHeadline() = %q, want %q for a genuinely completed review", got, "none")
	}
}

func TestSteerAgentDoesNotResumeCoderWhileReviewerActivelyBlocking(t *testing.T) {
	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}

	coder := &Agent{
		ID:                      "coder-under-review",
		Role:                    RoleCoder,
		IssueNumber:             981,
		PRNumber:                991,
		PRURL:                   "https://github.com/infrasec-cto/zeekfoundry/pull/991",
		BranchName:              "repository-agent-orchestrator/issue-981",
		WorktreePath:            worktree,
		State:                   StateWaiting,
		ActiveReviewAgentID:     "reviewer-991",
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	reviewer := &Agent{
		ID:                      "reviewer-991",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             981,
		PRNumber:                991,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		agents:    agents,
		runner:    runner,
		messenger: messenger,
	}

	if err := bot.SteerAgent(context.Background(), coder.ID, "fyi for after review finishes"); err != nil {
		t.Fatalf("SteerAgent() error = %v", err)
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after steering", coder.ID)
	}
	if updated.State != StateWaiting {
		t.Fatalf("coder state after steering with an active blocking reviewer = %s, want %s (steering a coder must not resume it out from under an in-flight review)", updated.State, StateWaiting)
	}
}

func TestSteerAgentRestartsApprovedRuntimeWhenSessionIsDead(t *testing.T) {
	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}

	agent := &Agent{
		ID:                      "agent-approved",
		Role:                    RoleCoder,
		IssueNumber:             90,
		BranchName:              "repository-agent-orchestrator/issue-90",
		PRNumber:                103,
		PRURL:                   "https://github.com/acme/widget/pull/103",
		WorktreePath:            worktree,
		State:                   StateApproved,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "dead-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{
		aliveSet:    true,
		alive:       false,
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-session"},
	}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		agents:    agents,
		runner:    runner,
		messenger: messenger,
	}

	if err := bot.SteerAgent(context.Background(), agent.ID, "rebase and fix conflicts"); err != nil {
		t.Fatalf("SteerAgent() error = %v", err)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}
	if got := runner.sent[0]; got != "rebase and fix conflicts" {
		t.Fatalf("runner message = %q, want %q", got, "rebase and fix conflicts")
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("messenger messages = %d, want 1", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found after steering", agent.ID)
	}
	if updated.State != StateWorking {
		t.Fatalf("agent state = %s, want %s", updated.State, StateWorking)
	}
	if updated.RuntimeHandle.Session != "resumed-session" {
		t.Fatalf("agent runtime session = %q, want %q", updated.RuntimeHandle.Session, "resumed-session")
	}
}

func TestStopAgentAndShutdown(t *testing.T) {
	agents := NewAgentManager()
	active := &Agent{
		ID:                   "agent-active",
		IssueNumber:          10,
		BranchName:           "repository-agent-orchestrator/issue-10",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		RuntimeHandle:        RuntimeHandle{Kind: RuntimeKindTmux, Session: "active-session"},
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	done := &Agent{
		ID:                   "agent-done",
		IssueNumber:          11,
		BranchName:           "repository-agent-orchestrator/issue-11",
		State:                StateDone,
		Stopped:              true,
		LastActivityTime:     time.Now(),
		RuntimeHandle:        RuntimeHandle{Kind: RuntimeKindTmux, Session: "done-session"},
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(active); err != nil {
		t.Fatalf("Add(active) error = %v", err)
	}
	if err := agents.Add(done); err != nil {
		t.Fatalf("Add(done) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{agents: agents, runner: runner}

	if err := bot.StopAgent(context.Background(), active.ID); err != nil {
		t.Fatalf("StopAgent() error = %v", err)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls after StopAgent = %d, want 1", got)
	}

	if err := bot.StopAgent(context.Background(), "missing"); err == nil {
		t.Fatal("StopAgent(missing) error = nil, want error")
	}

	// Re-add one active agent and exercise shutdown path.
	another := &Agent{
		ID:                   "agent-another",
		IssueNumber:          12,
		BranchName:           "repository-agent-orchestrator/issue-12",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		RuntimeHandle:        RuntimeHandle{Kind: RuntimeKindTmux, Session: "another-session"},
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(another); err != nil {
		t.Fatalf("Add(another) error = %v", err)
	}
	bot.Shutdown(context.Background())

	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop total calls after Shutdown = %d, want 1", got)
	}
	updated, ok := agents.Get(another.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", another.ID)
	}
	if updated.State != StateWorking || updated.Stopped {
		t.Fatalf("shutdown state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateWorking)
	}
	if updated.RuntimeHandle.Session != "another-session" {
		t.Fatalf("shutdown runtime session = %q, want preserved session", updated.RuntimeHandle.Session)
	}
}

func TestPauseAgentAndResumeWithSteer(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-paused",
		IssueNumber:          13,
		BranchName:           "repository-agent-orchestrator/issue-13",
		WorktreePath:         t.TempDir(),
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		RuntimeHandle:        RuntimeHandle{Kind: RuntimeKindTmux, Session: "pause-session"},
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-session"}}
	bot := &Orchestrator{agents: agents, runner: runner}

	if err := bot.PauseAgent(context.Background(), agent.ID); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls after PauseAgent = %d, want 1", got)
	}

	paused, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", agent.ID)
	}
	if paused.State != StateWorking || paused.Stopped || !paused.Paused {
		t.Fatalf("paused state = (%s, stopped=%v, paused=%v), want (%s, false, true)", paused.State, paused.Stopped, paused.Paused, StateWorking)
	}
	if paused.RuntimeHandle.Session != "" {
		t.Fatalf("paused runtime session = %q, want empty", paused.RuntimeHandle.Session)
	}

	if err := bot.SteerAgent(context.Background(), agent.ID, "resume work now"); err != nil {
		t.Fatalf("SteerAgent() after pause error = %v", err)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls after resume = %d, want 1", got)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls after resume = %d, want 1", got)
	}

	resumed, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("Get(%s) after resume = not found", agent.ID)
	}
	if resumed.State != StateWorking || resumed.Stopped || resumed.Paused {
		t.Fatalf("resumed state = (%s, stopped=%v, paused=%v), want (%s, false, false)", resumed.State, resumed.Stopped, resumed.Paused, StateWorking)
	}
	if resumed.RuntimeHandle.Session != "resumed-session" {
		t.Fatalf("resumed runtime session = %q, want %q", resumed.RuntimeHandle.Session, "resumed-session")
	}
}

func TestPauseAgentStopFailureLeavesReviewerActive(t *testing.T) {
	agents := NewAgentManager()
	coder := &Agent{
		ID:                   "coder-1",
		Role:                 RoleCoder,
		IssueNumber:          21,
		PRNumber:             21,
		PRURL:                "https://github.com/acme/widget/pull/21",
		State:                StateWaiting,
		ActiveReviewAgentID:  "reviewer-21",
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                   "reviewer-21",
		Role:                 RoleReviewer,
		ParentAgentID:        coder.ID,
		IssueNumber:          21,
		PRNumber:             21,
		PRURL:                coder.PRURL,
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		ObservedPRHeadSHA:    "deadbeef",
		RuntimeHandle:        RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-session"},
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{stopErr: errors.New("stop failed")}
	bot := &Orchestrator{agents: agents, runner: runner}

	err := bot.PauseAgent(context.Background(), reviewer.ID)
	if err == nil {
		t.Fatal("PauseAgent() error = nil, want stop failure")
	}
	if !strings.Contains(err.Error(), "failed to stop runtime") {
		t.Fatalf("PauseAgent() error = %v, want stop failure", err)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after PauseAgent", reviewer.ID)
	}
	if updatedReviewer.State != StateWorking || updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, false)", updatedReviewer.State, updatedReviewer.Stopped, StateWorking)
	}
	if updatedReviewer.RuntimeHandle.Session != "reviewer-session" {
		t.Fatalf("reviewer runtime session = %q, want %q", updatedReviewer.RuntimeHandle.Session, "reviewer-session")
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after PauseAgent", coder.ID)
	}
	if updatedCoder.ActiveReviewAgentID != reviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want %q", updatedCoder.ActiveReviewAgentID, reviewer.ID)
	}
	if updatedCoder.LastReviewedHeadSHA != "" {
		t.Fatalf("LastReviewedHeadSHA = %q, want empty after failed pause", updatedCoder.LastReviewedHeadSHA)
	}
}

func TestUnpauseAgent(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-paused-unpause",
		IssueNumber:          14,
		BranchName:           "repository-agent-orchestrator/issue-14",
		WorktreePath:         t.TempDir(),
		State:                StateWorking,
		Paused:               true,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unpaused-session"}}
	bot := &Orchestrator{agents: agents, runner: runner}

	if err := bot.UnpauseAgent(context.Background(), agent.ID); err != nil {
		t.Fatalf("UnpauseAgent() error = %v", err)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls after UnpauseAgent = %d, want 1", got)
	}

	resumed, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("Get(%s) after unpause = not found", agent.ID)
	}
	if resumed.State != StateWorking || resumed.Stopped || resumed.Paused {
		t.Fatalf("resumed state = (%s, stopped=%v, paused=%v), want (%s, false, false)", resumed.State, resumed.Stopped, resumed.Paused, StateWorking)
	}
	if resumed.RuntimeHandle.Session != "unpaused-session" {
		t.Fatalf("resumed runtime session = %q, want %q", resumed.RuntimeHandle.Session, "unpaused-session")
	}
}

func TestUnpauseAgentRestoresApprovedStateForPausedThumbsUpCoder(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-paused-approved",
		Role:                 RoleCoder,
		IssueNumber:          141,
		BranchName:           "repository-agent-orchestrator/issue-141",
		PRNumber:             141,
		PRURL:                "https://github.com/acme/widget/pull/141",
		ObservedPRHeadSHA:    "aaaaaaaaaaaa",
		LastReviewedHeadSHA:  "aaaaaaaaaaaa",
		LastReviewVerdict:    ReviewVerdictThumbsUp,
		State:                StateApproved,
		Paused:               true,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "should-not-start"}}
	bot := &Orchestrator{agents: agents, runner: runner}

	if err := bot.UnpauseAgent(context.Background(), agent.ID); err != nil {
		t.Fatalf("UnpauseAgent() error = %v", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls after UnpauseAgent = %d, want 0", got)
	}

	resumed, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("Get(%s) after unpause = not found", agent.ID)
	}
	if resumed.State != StateApproved || resumed.Stopped || resumed.Paused {
		t.Fatalf("resumed state = (%s, stopped=%v, paused=%v), want (%s, false, false)", resumed.State, resumed.Stopped, resumed.Paused, StateApproved)
	}
	if resumed.RuntimeHandle.Session != "" {
		t.Fatalf("resumed runtime session = %q, want empty", resumed.RuntimeHandle.Session)
	}
}

func TestPauseAgentRejectsPrelaunchReviewer(t *testing.T) {
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:               "reviewer-prelaunch",
		Role:             RoleReviewer,
		IssueNumber:      15,
		PRNumber:         15,
		PRURL:            "https://github.com/acme/widget/pull/15",
		State:            StateReviewGate,
		LastActivityTime: time.Now(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	bot := &Orchestrator{agents: agents, runner: &stubRunner{}}

	err := bot.PauseAgent(context.Background(), reviewer.ID)
	if err == nil {
		t.Fatal("PauseAgent() error = nil, want rejection for prelaunch reviewer")
	}
	if !strings.Contains(err.Error(), "cannot be paused before runtime launch") {
		t.Fatalf("PauseAgent() error = %v, want prelaunch rejection", err)
	}
}

func TestPauseAgentAllowsApprovedCoder(t *testing.T) {
	agents := NewAgentManager()
	coder := &Agent{
		ID:               "coder-approved",
		Role:             RoleCoder,
		IssueNumber:      16,
		PRNumber:         16,
		PRURL:            "https://github.com/acme/widget/pull/16",
		State:            StateApproved,
		LastActivityTime: time.Now(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	bot := &Orchestrator{agents: agents, runner: &stubRunner{}}

	if err := bot.PauseAgent(context.Background(), coder.ID); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found after PauseAgent", coder.ID)
	}
	if updated.State != StateApproved || updated.Stopped || !updated.Paused {
		t.Fatalf("coder state = (%s, stopped=%v, paused=%v), want (%s, false, true)", updated.State, updated.Stopped, updated.Paused, StateApproved)
	}
}

func TestPauseAgentRejectsRepoIndexer(t *testing.T) {
	agents := NewAgentManager()
	indexer := &Agent{
		ID:               "indexer-active",
		Role:             RoleIndexer,
		IssueNumber:      17,
		BranchName:       "repository-agent-orchestrator/repo-index",
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle:    RuntimeHandle{Kind: RuntimeKindTmux, Session: "indexer-session"},
	}
	if err := agents.Add(indexer); err != nil {
		t.Fatalf("Add(indexer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{agents: agents, runner: runner}

	err := bot.PauseAgent(context.Background(), indexer.ID)
	if err == nil {
		t.Fatal("PauseAgent() error = nil, want rejection for repo indexer")
	}
	if !strings.Contains(err.Error(), "repo indexing agent indexer-active cannot be paused") {
		t.Fatalf("PauseAgent() error = %v, want repo-indexer rejection", err)
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("runner.Stop calls = %d, want 0", got)
	}

	updated, ok := agents.Get(indexer.ID)
	if !ok {
		t.Fatalf("indexer %s not found after PauseAgent", indexer.ID)
	}
	if updated.State != StateWorking || updated.Stopped {
		t.Fatalf("indexer state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateWorking)
	}
	if updated.RuntimeHandle.Session != "indexer-session" {
		t.Fatalf("indexer runtime session = %q, want %q", updated.RuntimeHandle.Session, "indexer-session")
	}
}

func TestStopAgentStopFailureLeavesAgentRunning(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-stop-failure",
		Role:                 RoleCoder,
		IssueNumber:          22,
		BranchName:           "repository-agent-orchestrator/issue-22",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		RuntimeHandle:        RuntimeHandle{Kind: RuntimeKindTmux, Session: "stop-session"},
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{stopErr: errors.New("stop failed")}
	bot := &Orchestrator{agents: agents, runner: runner}

	err := bot.StopAgent(context.Background(), agent.ID)
	if err == nil {
		t.Fatal("StopAgent() error = nil, want stop failure")
	}
	if !strings.Contains(err.Error(), "failed to stop runtime") {
		t.Fatalf("StopAgent() error = %v, want stop failure", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found after StopAgent", agent.ID)
	}
	if updated.State != StateWorking || updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateWorking)
	}
	if updated.RuntimeHandle.Session != "stop-session" {
		t.Fatalf("runtime session = %q, want %q", updated.RuntimeHandle.Session, "stop-session")
	}
}

func TestPollOnceAndPollLoop(t *testing.T) {
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:               "review-agent-1",
		Role:             RoleReviewer,
		IssueNumber:      99,
		PRNumber:         0,
		State:            StateWorking,
		LastActivityTime: time.Now(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	bot := &Orchestrator{
		cfg:    Config{PollInterval: 5 * time.Millisecond},
		agents: agents,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}
	if bot.agents.LastPoll().IsZero() {
		t.Fatal("PollOnce() did not update last poll timestamp")
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go bot.PollLoop(ctx, &wg)

	deadline := time.Now().Add(300 * time.Millisecond)
	for bot.agents.LastPoll().IsZero() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()
}

func TestPollLoopRunsImmediateStartupReconcile(t *testing.T) {
	agents := NewAgentManager()
	bot := &Orchestrator{
		cfg:    Config{PollInterval: time.Hour},
		agents: agents,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go bot.PollLoop(ctx, &wg)

	deadline := time.Now().Add(300 * time.Millisecond)
	for bot.agents.LastPoll().IsZero() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if bot.agents.LastPoll().IsZero() {
		t.Fatal("PollLoop() did not perform immediate startup reconcile")
	}
}

func TestPrintStatus(t *testing.T) {
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:           "acme",
			RepoName:            "widget",
			RepoPath:            "/tmp/repo",
			WorktreeDir:         "/tmp/repo/.worktrees",
			LogDir:              "/tmp/repository-agent-orchestrator/widget",
			BaseBranch:          "main",
			PollIntervalSeconds: 20,
		},
		agents: NewAgentManager(),
		runner: &stubRunner{},
	}

	outNever := captureStdout(t, func() { printStatus(bot) })
	if !strings.Contains(outNever, "last poll: never") {
		t.Fatalf("printStatus() output missing never marker: %q", outNever)
	}

	now := time.Date(2026, 3, 3, 10, 0, 0, 0, time.UTC)
	bot.agents.SetLastPoll(now)
	outWithTime := captureStdout(t, func() { printStatus(bot) })
	if !strings.Contains(outWithTime, now.Format(time.RFC3339)) {
		t.Fatalf("printStatus() output missing timestamp %s: %q", now.Format(time.RFC3339), outWithTime)
	}
	if !strings.Contains(outWithTime, "manual reviews: 0") {
		t.Fatalf("printStatus() output missing empty manual review count: %q", outWithTime)
	}
	if !strings.Contains(outWithTime, "review gates: 0") {
		t.Fatalf("printStatus() output missing empty review gate count: %q", outWithTime)
	}

	reviewer := &Agent{
		ID:                "review-agent-12",
		Role:              RoleReviewer,
		IssueNumber:       12,
		LogDir:            "/tmp/repository-agent-orchestrator/widget",
		PRNumber:          44,
		BranchName:        "repository-agent-orchestrator/issue-12",
		State:             StateReviewGate,
		ObservedPRHeadSHA: "deadbeefcafebabe",
		LastActivityTime:  time.Now().Add(-2 * time.Minute),
	}
	if err := bot.agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	outWithGate := captureStdout(t, func() { printStatus(bot) })
	if !strings.Contains(outWithGate, "review gates: 1") {
		t.Fatalf("printStatus() output missing review gate count: %q", outWithGate)
	}
	if !strings.Contains(outWithGate, "Review Gates") || !strings.Contains(outWithGate, "reviewer: review-agent-12") {
		t.Fatalf("printStatus() output missing review gate details: %q", outWithGate)
	}
	if !strings.Contains(outWithGate, "gate log: /tmp/repository-agent-orchestrator/widget/review-agent-12-gate.log") {
		t.Fatalf("printStatus() output missing review gate log path: %q", outWithGate)
	}

	manualReviewer := &Agent{
		ID:               "review-agent-77",
		Role:             RoleReviewer,
		LogDir:           "/tmp/repository-agent-orchestrator/widget",
		PRNumber:         77,
		PRTitle:          "Standalone review",
		PRURL:            "https://github.com/acme/widget/pull/77",
		BranchName:       "feature/standalone-review",
		State:            StateWorking,
		LastActivityTime: time.Now().Add(-time.Minute),
	}
	if err := bot.agents.Add(manualReviewer); err != nil {
		t.Fatalf("Add(manualReviewer) error = %v", err)
	}
	outWithManualReview := captureStdout(t, func() { printStatus(bot) })
	if !strings.Contains(outWithManualReview, "manual reviews: 1") {
		t.Fatalf("printStatus() output missing manual review count: %q", outWithManualReview)
	}
	if !strings.Contains(outWithManualReview, "Manual Reviews") || !strings.Contains(outWithManualReview, "[pr #77: Standalone review]") {
		t.Fatalf("printStatus() output missing manual review details: %q", outWithManualReview)
	}
}

func TestPrintAgentDetailsDisplaysLastActivityInLocalTime(t *testing.T) {
	stored := time.Date(
		2026,
		time.August,
		13,
		16,
		9,
		9,
		0,
		time.FixedZone("stored-offset", 11*60*60+37*60),
	)
	want := stored.Local().Format(time.RFC3339)
	if want == stored.Format(time.RFC3339) {
		t.Skip("test process local timezone matches the synthetic stored offset")
	}

	out := captureStdout(t, func() {
		printAgentDetails(nil, "coder", Agent{LastActivityTime: stored})
	})
	if !strings.Contains(out, "coder last activity: "+want) {
		t.Fatalf("printAgentDetails() output = %q, want local timestamp %s", out, want)
	}
	if strings.Contains(out, stored.Format(time.RFC3339)) {
		t.Fatalf("printAgentDetails() output retained stored offset: %q", out)
	}
}

func TestCommandErrorStrings(t *testing.T) {
	unknown := unknownCommandError{token: "doctor"}
	if got := unknown.Error(); got != "unknown command \"doctor\"" {
		t.Fatalf("unknownCommandError.Error() = %q", got)
	}
	ambiguous := ambiguousCommandError{token: "st", matches: []string{"start", "stop"}}
	if got := ambiguous.Error(); got != "ambiguous command \"st\"" {
		t.Fatalf("ambiguousCommandError.Error() = %q", got)
	}
}

func TestSetupProcessLoggerAndPrintCLIUsage(t *testing.T) {
	if _, err := setupProcessLogger("   "); err == nil {
		t.Fatal("setupProcessLogger(empty) error = nil, want error")
	}

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	defer log.SetOutput(oldWriter)
	defer log.SetFlags(oldFlags)

	logDir := filepath.Join(t.TempDir(), "logs")
	logFile, err := setupProcessLogger(logDir)
	if err != nil {
		t.Fatalf("setupProcessLogger(%s) error = %v", logDir, err)
	}
	log.Print("process logger test line")
	if err := logFile.Close(); err != nil {
		t.Fatalf("logFile.Close() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(logDir, orchestratorDaemonLogName))
	if err != nil {
		t.Fatalf("ReadFile(log) error = %v", err)
	}
	if !strings.Contains(string(data), "process logger test line") {
		t.Fatalf("log file missing expected message: %q", string(data))
	}

	var usage bytes.Buffer
	printCLIUsage(&usage)
	if got := usage.String(); !strings.Contains(got, "Usage: repository-agent-orchestrator --config <path>") {
		t.Fatalf("printCLIUsage() output = %q", got)
	}
}

func TestRunHelpAndArgumentErrors(t *testing.T) {
	stdout, stderr, code := captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", "--help"})
	})
	if code != 0 {
		t.Fatalf("Run(--help) code = %d, want 0", code)
	}
	if !strings.Contains(stdout, "Usage: repository-agent-orchestrator --config <path>") {
		t.Fatalf("Run(--help) stdout = %q, want usage", stdout)
	}
	if stderr != "" {
		t.Fatalf("Run(--help) stderr = %q, want empty", stderr)
	}

	stdout, stderr, code = captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", "--version"})
	})
	if code != 0 {
		t.Fatalf("Run(--version) code = %d, want 0", code)
	}
	if !strings.Contains(stdout, "repository-agent-orchestrator") {
		t.Fatalf("Run(--version) stdout = %q, want a version string", stdout)
	}
	if stderr != "" {
		t.Fatalf("Run(--version) stderr = %q, want empty", stderr)
	}

	_, stderr, code = captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator"})
	})
	if code != 1 {
		t.Fatalf("Run(no args) code = %d, want 1", code)
	}
	if !strings.Contains(stderr, "argument error") {
		t.Fatalf("Run(no args) stderr = %q, want argument error", stderr)
	}

	missingConfig := filepath.Join(t.TempDir(), "missing.yaml")
	_, stderr, code = captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", "--config", missingConfig})
	})
	if code != 1 {
		t.Fatalf("Run(missing config) code = %d, want 1", code)
	}
	if !strings.Contains(stderr, "configuration error") {
		t.Fatalf("Run(missing config) stderr = %q, want configuration error", stderr)
	}

	repoName := "widget-runtime-fail"
	repoPath := initGitRepoWithOrigin(t, "acme", repoName)
	configPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: "+repoName+"\nREPO_PATH: "+repoPath+"\nMANDATORY_TESTS:\n  - git status\n")
	t.Setenv("WEBEX_WEBHOOK_URL", "https://example.test/webhook")
	t.Setenv("PATH", t.TempDir()) // intentionally missing gh/git/make/tmux/codex
	_, stderr, code = captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", "--config", configPath})
	})
	if code != 1 {
		t.Fatalf("Run(runtime validation failure) code = %d, want 1", code)
	}
	if !strings.Contains(stderr, "runtime validation failed") {
		t.Fatalf("Run(runtime validation failure) stderr = %q, want runtime validation message", stderr)
	}
}
