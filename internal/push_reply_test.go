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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPromptCommentRepliesAfterPushSendsReminderWhenPendingReviewComments(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 54,
				"head": map[string]any{
					"sha": "bbbbbbbbbbbbbbbb",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-54-1772371484",
		IssueNumber:          54,
		BranchName:           "repository-agent-orchestrator/issue-54",
		PRNumber:             54,
		PRURL:                "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:    "aaaaaaaaaaaaaaaa",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
		pendingReviewCommentIDs: map[int64]struct{}{
			101: {},
		},
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-54",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.promptCommentRepliesAfterPush(context.Background(), snapshot); err != nil {
		t.Fatalf("promptCommentRepliesAfterPush() error = %v", err)
	}

	if got, want := len(runner.sent), 1; got != want {
		t.Fatalf("runtime reminders = %d, want %d", got, want)
	}
	reminder := runner.sent[0]
	if !strings.Contains(reminder, "Now reply on GitHub to each addressed PR comment:") {
		t.Fatalf("reminder missing GitHub reply instruction: %q", reminder)
	}
	if !strings.Contains(reminder, "Pending review comments: 1") {
		t.Fatalf("reminder missing pending count: %q", reminder)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if got, want := updated.ObservedPRHeadSHA, "bbbbbbbbbbbbbbbb"; got != want {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", got, want)
	}
}

func TestPromptCommentRepliesAfterPushRestartsRuntimeWhenMissing(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 54,
				"head": map[string]any{
					"sha": "bbbbbbbbbbbbbbbb",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-54-1772371484",
		IssueNumber:          54,
		BranchName:           "repository-agent-orchestrator/issue-54",
		PRNumber:             54,
		PRURL:                "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:    "aaaaaaaaaaaaaaaa",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
		pendingReviewCommentIDs: map[int64]struct{}{
			101: {},
		},
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "dead-session",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{
		aliveSet:    true,
		alive:       false,
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-agent-54"},
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.promptCommentRepliesAfterPush(context.Background(), snapshot); err != nil {
		t.Fatalf("promptCommentRepliesAfterPush() error = %v", err)
	}

	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
	if runner.started[0].ID != agent.ID {
		t.Fatalf("restarted agent id = %q, want %q", runner.started[0].ID, agent.ID)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runtime reminders = %d, want 1", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.RuntimeHandle.Session != "resumed-agent-54" {
		t.Fatalf("runtime session = %q, want %q", updated.RuntimeHandle.Session, "resumed-agent-54")
	}
	if got, want := updated.ObservedPRHeadSHA, "bbbbbbbbbbbbbbbb"; got != want {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", got, want)
	}
}

func TestPromptCommentRepliesAfterPushKeepsPausedCoderPaused(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/58":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 58,
				"head": map[string]any{
					"sha": "9999999999999999",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	agent := &Agent{
		ID:                   "agent-58-paused",
		Role:                 RoleCoder,
		IssueNumber:          58,
		BranchName:           "repository-agent-orchestrator/issue-58",
		PRNumber:             58,
		PRURL:                "https://github.com/acme/widget/pull/58",
		WorktreePath:         worktree,
		ObservedPRHeadSHA:    "1111111111111111",
		State:                StateWorking,
		Paused:               true,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
		pendingReviewCommentIDs: map[int64]struct{}{
			101: {},
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unexpected-resume"}}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		agents:    agents,
		runner:    runner,
		messenger: messenger,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.promptCommentRepliesAfterPush(context.Background(), snapshot); err != nil {
		t.Fatalf("promptCommentRepliesAfterPush() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 for paused coder", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runtime reminders = %d, want 0 for paused coder", got)
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("inbox reminders = %d, want 1", got)
	}
	if !strings.Contains(messenger.messages[0], "Pending review comments: 1") {
		t.Fatalf("inbox reminder missing pending count: %q", messenger.messages[0])
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking || !updated.Paused {
		t.Fatalf("agent state = (%s, paused=%v), want (%s, true)", updated.State, updated.Paused, StateWorking)
	}
	if got, want := updated.ObservedPRHeadSHA, "9999999999999999"; got != want {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", got, want)
	}
}

func TestPromptCommentRepliesAfterPushSkipsReminderWhileReviewAgentIsActive(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/57":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 57,
				"head": map[string]any{
					"sha": "ffffffffffffffff",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	coder := &Agent{
		ID:                   "agent-57-1772371484",
		Role:                 RoleCoder,
		IssueNumber:          57,
		BranchName:           "repository-agent-orchestrator/issue-57",
		PRNumber:             57,
		PRURL:                "https://github.com/acme/widget/pull/57",
		ObservedPRHeadSHA:    "aaaaaaaaaaaaaaaa",
		ActiveReviewAgentID:  "review-agent-57-1",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
		pendingReviewCommentIDs: map[int64]struct{}{
			777: {},
		},
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-57",
		},
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	reviewer := &Agent{
		ID:               "review-agent-57-1",
		Role:             RoleReviewer,
		ParentAgentID:    coder.ID,
		IssueNumber:      57,
		BranchName:       coder.BranchName,
		PRNumber:         57,
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-agent-57-session",
		},
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("agent %s not found", coder.ID)
	}
	if err := bot.promptCommentRepliesAfterPush(context.Background(), snapshot); err != nil {
		t.Fatalf("promptCommentRepliesAfterPush() error = %v", err)
	}

	if got := len(runner.sent); got != 0 {
		t.Fatalf("runtime reminders = %d, want 0", got)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("agent %s not found", coder.ID)
	}
	if got, want := updated.ObservedPRHeadSHA, "ffffffffffffffff"; got != want {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", got, want)
	}
}

func TestPromptCommentRepliesAfterPushSkipsReminderWithoutPendingComments(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/55":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 55,
				"head": map[string]any{
					"sha": "cccccccccccccccc",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-55-1772371484",
		IssueNumber:             55,
		BranchName:              "repository-agent-orchestrator/issue-55",
		PRNumber:                55,
		PRURL:                   "https://github.com/acme/widget/pull/55",
		ObservedPRHeadSHA:       "dddddddddddddddd",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-55",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.promptCommentRepliesAfterPush(context.Background(), snapshot); err != nil {
		t.Fatalf("promptCommentRepliesAfterPush() error = %v", err)
	}

	if got := len(runner.sent); got != 0 {
		t.Fatalf("runtime reminders = %d, want 0", got)
	}
	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if got, want := updated.ObservedPRHeadSHA, "cccccccccccccccc"; got != want {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", got, want)
	}
}

func TestPromptCommentRepliesAfterPushSkipsReminderOnFirstObservedHead(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/56":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 56,
				"head": map[string]any{
					"sha": "eeeeeeeeeeeeeeee",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-56-1772371484",
		IssueNumber:          56,
		BranchName:           "repository-agent-orchestrator/issue-56",
		PRNumber:             56,
		PRURL:                "https://github.com/acme/widget/pull/56",
		ObservedPRHeadSHA:    "",
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
		pendingReviewCommentIDs: map[int64]struct{}{
			303: {},
		},
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-56",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.promptCommentRepliesAfterPush(context.Background(), snapshot); err != nil {
		t.Fatalf("promptCommentRepliesAfterPush() error = %v", err)
	}

	if got := len(runner.sent); got != 0 {
		t.Fatalf("runtime reminders = %d, want 0", got)
	}
	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if got, want := updated.ObservedPRHeadSHA, "eeeeeeeeeeeeeeee"; got != want {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", got, want)
	}
}
