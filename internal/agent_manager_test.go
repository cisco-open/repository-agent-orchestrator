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
	"testing"
	"time"
)

func testAgent(id string, issue int) *Agent {
	return &Agent{
		ID:                   id,
		IssueNumber:          issue,
		BranchName:           "repository-agent-orchestrator/issue-1",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
}

func TestAgentManagerAddAndGet(t *testing.T) {
	m := NewAgentManager()
	a := testAgent("agent-1", 42)

	if err := m.Add(a); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := m.Add(a); err == nil {
		t.Fatal("expected duplicate Add() to fail")
	}

	got, ok := m.Get("agent-1")
	if !ok {
		t.Fatal("expected agent to exist")
	}
	if got.IssueNumber != 42 {
		t.Fatalf("IssueNumber = %d, want 42", got.IssueNumber)
	}
}

func TestAgentManagerHasActiveIssueAndStop(t *testing.T) {
	m := NewAgentManager()
	a := testAgent("agent-1", 7)
	a.PRNumber = 57
	if err := m.Add(a); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if !m.HasActiveIssue(7) {
		t.Fatal("expected issue to be active")
	}
	if !m.HasActivePR(57) {
		t.Fatal("expected PR to be active")
	}

	stopped, ok := m.Stop("agent-1")
	if !ok {
		t.Fatal("expected Stop() to succeed")
	}
	if stopped.State != StateStopped {
		t.Fatalf("State = %s, want %s", stopped.State, StateStopped)
	}
	if m.HasActiveIssue(7) {
		t.Fatal("expected issue to be inactive after stop")
	}
	if m.HasActivePR(57) {
		t.Fatal("expected PR to be inactive after stop")
	}
	if _, err := m.ResolveActiveIssueAgent(7, RoleCoder); err == nil {
		t.Fatal("ResolveActiveIssueAgent() error = nil for stopped agent")
	}
	cleanupTarget, err := m.ResolveIssueAgentForCleanup(7, RoleCoder)
	if err != nil {
		t.Fatalf("ResolveIssueAgentForCleanup() error = %v", err)
	}
	if cleanupTarget.ID != a.ID {
		t.Fatalf("cleanup target ID = %q, want %q", cleanupTarget.ID, a.ID)
	}
}

func TestAgentManagerResolvesTrackedReviewByPR(t *testing.T) {
	m := NewAgentManager()
	reviewer := testAgent("review-agent-105", 104)
	reviewer.Role = RoleReviewer
	reviewer.PRNumber = 105
	if err := m.Add(reviewer); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	active, err := m.ResolveActiveReviewAgent(105)
	if err != nil {
		t.Fatalf("ResolveActiveReviewAgent() error = %v", err)
	}
	if active.ID != reviewer.ID {
		t.Fatalf("active manual reviewer ID = %q, want %q", active.ID, reviewer.ID)
	}

	if _, ok := m.Stop(reviewer.ID); !ok {
		t.Fatalf("Stop(%s) = false", reviewer.ID)
	}
	if _, err := m.ResolveActiveReviewAgent(105); err == nil {
		t.Fatal("ResolveActiveReviewAgent() error = nil for stopped reviewer")
	}
	cleanupTarget, err := m.ResolveReviewAgentForCleanup(105)
	if err != nil {
		t.Fatalf("ResolveReviewAgentForCleanup() error = %v", err)
	}
	if cleanupTarget.ID != reviewer.ID {
		t.Fatalf("cleanup target ID = %q, want %q", cleanupTarget.ID, reviewer.ID)
	}
}

// TestAgentManagerCleanupResolutionSkipsFullyRetiredSupersededReviewer is
// the regression test for a real production incident: a reviewer that had
// already been fully retired via StateStopped
// (superseded by a newer head -- routine, not an error) kept counting as
// an ambiguous "needs cleanup" candidate for its issue/PR forever, even
// though its own ReviewCoordinatorLifecycle showed every release step
// already acknowledged (CompletedAt set). Coder cleanup always lands in
// StateDone (already excluded), but reviewers routinely retire into
// StateStopped, and nothing ever purges these fully-cleaned records --
// so every reviewer a PR has ever superseded permanently blocked `agent
// cleanup` for that issue/PR from ever resolving unambiguously again,
// once a second (still-active) reviewer existed for the same issue/PR.
func TestAgentManagerCleanupResolutionSkipsFullyRetiredSupersededReviewer(t *testing.T) {
	m := NewAgentManager()

	superseded := testAgent("review-agent-982-1", 982)
	superseded.Role = RoleReviewer
	superseded.PRNumber = 992
	superseded.State = StateStopped
	superseded.Stopped = true
	superseded.ReviewCoordinatorLifecycle = &ReviewCoordinatorLifecycleState{
		Intent:                     ReviewCoordinatorLifecycleCleanup,
		FinalState:                 StateStopped,
		ReleaseCoordinatorWorktree: true,
		ReleaseRuntimeArtifacts:    true,
		CompletedAt: time.Date(
			2026, 8, 27, 20, 38, 45, 0, time.UTC,
		),
	}
	if err := m.Add(superseded); err != nil {
		t.Fatalf("Add(superseded) error = %v", err)
	}

	current := testAgent("review-agent-982-2", 982)
	current.Role = RoleReviewer
	current.PRNumber = 992
	current.State = StateWorking
	if err := m.Add(current); err != nil {
		t.Fatalf("Add(current) error = %v", err)
	}

	byIssue, err := m.ResolveIssueAgentForCleanup(982, RoleReviewer)
	if err != nil {
		t.Fatalf("ResolveIssueAgentForCleanup() error = %v, want the fully-retired superseded reviewer to be skipped", err)
	}
	if byIssue.ID != current.ID {
		t.Fatalf("cleanup target by issue = %q, want %q", byIssue.ID, current.ID)
	}

	byPR, err := m.ResolveReviewAgentForCleanup(992)
	if err != nil {
		t.Fatalf("ResolveReviewAgentForCleanup() error = %v, want the fully-retired superseded reviewer to be skipped", err)
	}
	if byPR.ID != current.ID {
		t.Fatalf("cleanup target by PR = %q, want %q", byPR.ID, current.ID)
	}
}

// TestAgentManagerCleanupResolutionStillFindsMerelyStoppedReviewer guards
// the boundary the fix above must not overreach: a reviewer whose
// ReviewCoordinatorLifecycle completed a "stop" intent (routine agent
// stop, e.g. via `agent stop`) rather than a "cleanup" intent has only
// had its runtime/worktree released as part of stopping -- worker
// artifacts, runtime state, logs, and persisted handoffs are still
// outstanding. It must remain a valid `agent cleanup` target, not be
// silently skipped the way a genuinely fully-cleaned reviewer is.
func TestAgentManagerCleanupResolutionStillFindsMerelyStoppedReviewer(t *testing.T) {
	m := NewAgentManager()

	reviewer := testAgent("review-agent-982-1", 982)
	reviewer.Role = RoleReviewer
	reviewer.PRNumber = 992
	reviewer.State = StateStopped
	reviewer.Stopped = true
	reviewer.ReviewCoordinatorLifecycle = &ReviewCoordinatorLifecycleState{
		Intent:     ReviewCoordinatorLifecycleStop,
		FinalState: StateStopped,
		CompletedAt: time.Date(
			2026, 8, 27, 20, 38, 45, 0, time.UTC,
		),
	}
	if err := m.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	byIssue, err := m.ResolveIssueAgentForCleanup(982, RoleReviewer)
	if err != nil {
		t.Fatalf("ResolveIssueAgentForCleanup() error = %v, want the merely-stopped reviewer to still be found", err)
	}
	if byIssue.ID != reviewer.ID {
		t.Fatalf("cleanup target by issue = %q, want %q", byIssue.ID, reviewer.ID)
	}

	byPR, err := m.ResolveReviewAgentForCleanup(992)
	if err != nil {
		t.Fatalf("ResolveReviewAgentForCleanup() error = %v, want the merely-stopped reviewer to still be found", err)
	}
	if byPR.ID != reviewer.ID {
		t.Fatalf("cleanup target by PR = %q, want %q", byPR.ID, reviewer.ID)
	}
}

// TestAgentManagerCleanupResolutionStillFindsReviewerWithPreservedArtifacts
// is the regression test for review feedback:
// several normal lifecycle paths complete a cleanup with
// ReleaseRuntimeArtifacts left false (or a persisted handoff
// intentionally preserved), leaving real artifacts behind for a later
// explicit `agent cleanup`. The original fix's skip predicate checked
// only Intent == ReviewCoordinatorLifecycleCleanup and CompletedAt,
// which missed this distinction: a reviewer whose completed cleanup
// intentionally left artifacts in place was wrongly treated as fully
// cleaned and skipped by both issue- and PR-based cleanup resolution,
// making those preserved artifacts unreachable. Reproduces exactly the
// case reported: Intent: cleanup, CompletedAt set,
// ReleaseRuntimeArtifacts: false.
func TestAgentManagerCleanupResolutionStillFindsReviewerWithPreservedArtifacts(t *testing.T) {
	m := NewAgentManager()

	reviewer := testAgent("review-agent-982-1", 982)
	reviewer.Role = RoleReviewer
	reviewer.PRNumber = 992
	reviewer.State = StateStopped
	reviewer.Stopped = true
	reviewer.ReviewCoordinatorLifecycle = &ReviewCoordinatorLifecycleState{
		Intent:                     ReviewCoordinatorLifecycleCleanup,
		FinalState:                 StateStopped,
		ReleaseCoordinatorWorktree: true,
		ReleaseRuntimeArtifacts:    false,
		CompletedAt: time.Date(
			2026, 8, 27, 20, 38, 45, 0, time.UTC,
		),
	}
	if err := m.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	byIssue, err := m.ResolveIssueAgentForCleanup(982, RoleReviewer)
	if err != nil {
		t.Fatalf("ResolveIssueAgentForCleanup() error = %v, want the reviewer with preserved artifacts to still be found", err)
	}
	if byIssue.ID != reviewer.ID {
		t.Fatalf("cleanup target by issue = %q, want %q", byIssue.ID, reviewer.ID)
	}

	byPR, err := m.ResolveReviewAgentForCleanup(992)
	if err != nil {
		t.Fatalf("ResolveReviewAgentForCleanup() error = %v, want the reviewer with preserved artifacts to still be found", err)
	}
	if byPR.ID != reviewer.ID {
		t.Fatalf("cleanup target by PR = %q, want %q", byPR.ID, reviewer.ID)
	}
}

func TestAgentManagerTerminalLifecycleCannotBeReactivated(t *testing.T) {
	m := NewAgentManager()
	a := testAgent("agent-terminal", 8)
	if err := m.Add(a); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if !m.SetState(a.ID, StateStopped, true) {
		t.Fatalf("SetState(%s, stopped) = false", a.ID)
	}
	if m.SetState(a.ID, StateReviewGate, false) {
		t.Fatal("SetState(review_gate) = true for terminal agent")
	}
	if m.SetRuntimeHandle(a.ID, RuntimeHandle{Kind: RuntimeKindTmux, Session: "late-runtime"}) {
		t.Fatal("SetRuntimeHandle() = true for terminal agent")
	}
	if m.SetActiveReviewAgent(a.ID, "late-reviewer") {
		t.Fatal("SetActiveReviewAgent() = true for terminal agent")
	}
	if _, ok := m.Pause(a.ID); ok {
		t.Fatal("Pause() = true for terminal agent")
	}
	if _, ok := m.Unpause(a.ID); ok {
		t.Fatal("Unpause() = true for terminal agent")
	}
	if !m.SetRuntimeHandle(a.ID, RuntimeHandle{}) {
		t.Fatal("SetRuntimeHandle(empty) = false for terminal agent")
	}
	if !m.SetState(a.ID, StateDone, true) {
		t.Fatalf("SetState(%s, done) = false", a.ID)
	}

	got, ok := m.Get(a.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", a.ID)
	}
	if got.State != StateDone || !got.Stopped {
		t.Fatalf("terminal agent = (state=%s, stopped=%v), want (%s, true)", got.State, got.Stopped, StateDone)
	}
	if got.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", got.ActiveReviewAgentID)
	}
	if got.RuntimeHandle.Session != "" {
		t.Fatalf("RuntimeHandle.Session = %q, want empty", got.RuntimeHandle.Session)
	}
}

func TestAgentManagerCommentSeenDedup(t *testing.T) {
	m := NewAgentManager()
	a := testAgent("agent-1", 1)
	if err := m.Add(a); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if !m.MarkReviewCommentSeen("agent-1", 101) {
		t.Fatal("first review comment should be new")
	}
	if m.MarkReviewCommentSeen("agent-1", 101) {
		t.Fatal("duplicate review comment should be ignored")
	}

	if !m.MarkIssueCommentSeen("agent-1", 202) {
		t.Fatal("first issue comment should be new")
	}
	if m.MarkIssueCommentSeen("agent-1", 202) {
		t.Fatal("duplicate issue comment should be ignored")
	}
}
