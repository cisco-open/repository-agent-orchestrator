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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubMessenger struct {
	mu       sync.Mutex
	messages []string
	sendErr  error
	onceKeys map[string]struct{}
}

func (m *stubMessenger) SendMessage(agent Agent, text string) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, text)
	return nil
}

func (m *stubMessenger) SendMessageOnce(
	agent Agent,
	key string,
	text string,
) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.onceKeys == nil {
		m.onceKeys = make(map[string]struct{})
	}
	if _, exists := m.onceKeys[key]; exists {
		return nil
	}
	m.onceKeys[key] = struct{}{}
	m.messages = append(m.messages, text)
	return nil
}

func (m *stubMessenger) SendTask(agent Agent, markdown string) error {
	return nil
}

func (m *stubMessenger) SendContext(agent Agent, markdown string) error {
	return nil
}

func (m *stubMessenger) SendHandoff(agent Agent, content string) error {
	return nil
}

func TestForwardNewCommentsForwardsHumanIssueComment(t *testing.T) {
	t.Parallel()

	comment := map[string]any{
		"id":         501,
		"html_url":   "https://github.com/acme/widget/pull/54#issuecomment-501",
		"created_at": "2026-03-01T00:02:00Z",
		"body":       "Please add one more test before merge.",
		"user": map[string]any{
			"login": "member-user",
		},
	}
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{comment})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	webexMessages := make([]string, 0)
	var webexMu sync.Mutex
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		webexMu.Lock()
		webexMessages = append(webexMessages, payload["markdown"])
		webexMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-43-1772371484",
		IssueNumber:             43,
		BranchName:              "repository-agent-orchestrator/issue-43",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-43",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			WebexWebhookURL: webex.URL,
		},
		github:    ghClient,
		notifier:  NewWebexNotifier(webex.URL),
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0", got)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}
	if got := len(messenger.messages); got != 0 {
		t.Fatalf("forwarded inbox messages = %d, want 0", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking {
		t.Fatalf("agent state = %s, want %s", updated.State, StateWorking)
	}
	if !agents.HasSeenIssueComment(agent.ID, 501) {
		t.Fatal("human issue comment was not marked seen")
	}
	if got := agents.PendingReviewCommentCount(agent.ID); got != 1 {
		t.Fatalf("pending feedback count = %d, want 1", got)
	}

	webexMu.Lock()
	defer webexMu.Unlock()
	if len(webexMessages) != 1 {
		t.Fatalf("expected 1 notification, got %v", webexMessages)
	}
}

func TestForwardNewCommentsDoesNotReplayDeliveredCommentAfterQuarantine(
	t *testing.T,
) {
	comment := map[string]any{
		"id":         502,
		"html_url":   "https://github.com/acme/widget/pull/54#issuecomment-502",
		"created_at": "2026-03-01T00:03:00Z",
		"body":       "Please preserve the known delivery outcome.",
		"user": map[string]any{
			"login": "member-user",
		},
	}
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{comment})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-delivered-before-quarantine",
		Role:                    RoleCoder,
		IssueNumber:             43,
		PRNumber:                54,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "delivered-before-quarantine",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	runner := &stubRunner{
		sendErr: &runtimeDeliveryError{
			err:         errors.New("injected post-delivery isolation failure"),
			delivered:   true,
			quarantined: true,
		},
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
	err := bot.forwardNewComments(context.Background(), snapshot)
	if err == nil || !strings.Contains(err.Error(), "reached runtime") {
		t.Fatalf("first forwardNewComments() error = %v", err)
	}
	if !agents.HasSeenIssueComment(agent.ID, 502) {
		t.Fatal("delivered issue comment was not marked seen")
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("second forwardNewComments() error = %v", err)
	}
	if runner.sendCalls != 1 {
		t.Fatalf("runtime send calls = %d, want 1", runner.sendCalls)
	}
}

func TestForwardNewCommentsKeepsPausedCoderPausedForReviewComment(t *testing.T) {
	t.Parallel()

	reviewComment := map[string]any{
		"id":         5501,
		"html_url":   "https://github.com/acme/widget/pull/54#discussion_r5501",
		"created_at": "2026-03-01T00:04:00Z",
		"body":       "Please cover the paused lifecycle path too.",
		"path":       "internal/app.go",
		"line":       321,
		"user": map[string]any{
			"login": "member-user",
		},
	}
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{reviewComment})
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
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
		ID:                      "paused-coder-review-comment",
		Role:                    RoleCoder,
		IssueNumber:             54,
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		WorktreePath:            worktree,
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
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
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 for paused coder", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0 for paused coder", got)
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("forwarded inbox messages = %d, want 1", got)
	}
	if !strings.Contains(messenger.messages[0], "# PR Review Comment") {
		t.Fatalf("inbox message = %q, want PR review comment markdown", messenger.messages[0])
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking || !updated.Paused {
		t.Fatalf("agent state = (%s, paused=%v), want (%s, true)", updated.State, updated.Paused, StateWorking)
	}
	if updated.RuntimeHandle.Session != "" {
		t.Fatalf("runtime session = %q, want empty for paused coder", updated.RuntimeHandle.Session)
	}
	if !agents.HasSeenReviewComment(agent.ID, 5501) {
		t.Fatal("review comment was not marked seen")
	}
	if got := agents.PendingReviewCommentCount(agent.ID); got != 1 {
		t.Fatalf("pending feedback count = %d, want 1", got)
	}
}

func TestForwardNewCommentsKeepsPausedCoderPausedForIssueComment(t *testing.T) {
	t.Parallel()

	issueComment := map[string]any{
		"id":         5502,
		"html_url":   "https://github.com/acme/widget/pull/54#issuecomment-5502",
		"created_at": "2026-03-01T00:05:00Z",
		"body":       "Please follow up on the paused-agent workflow.",
		"user": map[string]any{
			"login": "member-user",
		},
	}
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{issueComment})
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
		ID:                      "paused-coder-issue-comment",
		Role:                    RoleCoder,
		IssueNumber:             54,
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		WorktreePath:            worktree,
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
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
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 for paused coder", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0 for paused coder", got)
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("forwarded inbox messages = %d, want 1", got)
	}
	if !strings.Contains(messenger.messages[0], "# PR Issue Comment") {
		t.Fatalf("inbox message = %q, want PR issue comment markdown", messenger.messages[0])
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking || !updated.Paused {
		t.Fatalf("agent state = (%s, paused=%v), want (%s, true)", updated.State, updated.Paused, StateWorking)
	}
	if updated.RuntimeHandle.Session != "" {
		t.Fatalf("runtime session = %q, want empty for paused coder", updated.RuntimeHandle.Session)
	}
	if !agents.HasSeenIssueComment(agent.ID, 5502) {
		t.Fatal("issue comment was not marked seen")
	}
	if got := agents.PendingReviewCommentCount(agent.ID); got != 1 {
		t.Fatalf("pending feedback count = %d, want 1", got)
	}
}

func TestForwardNewCommentsIgnoresMarkedCodingAgentIssueComment(t *testing.T) {
	t.Parallel()

	comment := map[string]any{
		"id":         502,
		"html_url":   "https://github.com/acme/widget/pull/54#issuecomment-502",
		"created_at": "2026-03-01T00:03:00Z",
		"body":       "Implemented the requested changes.\n\nCODEX_AGENT_ID: coding-agent-54\nCODEX_AGENT_ROLE: coder",
		"user": map[string]any{
			"login": "member-user",
		},
	}
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{comment})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-54",
		Role:                    RoleCoder,
		IssueNumber:             54,
		BranchName:              "repository-agent-orchestrator/issue-54",
		PRNumber:                54,
		PRURL:                   "https://github.com/acme/widget/pull/54",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-54",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0", got)
	}
	if got := len(messenger.messages); got != 0 {
		t.Fatalf("forwarded inbox messages = %d, want 0", got)
	}
	if !agents.HasSeenIssueComment(agent.ID, 502) {
		t.Fatal("marked coding-agent issue comment was not marked seen")
	}
	if got := agents.PendingReviewCommentCount(agent.ID); got != 0 {
		t.Fatalf("pending feedback count = %d, want 0", got)
	}
}

func TestForwardNewCommentsSkipsCoderWhenReviewAgentIsActive(t *testing.T) {
	t.Parallel()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                  "agent-54",
		Role:                RoleCoder,
		IssueNumber:         54,
		BranchName:          "repository-agent-orchestrator/issue-54",
		PRNumber:            54,
		PRURL:               "https://github.com/acme/widget/pull/54",
		ActiveReviewAgentID: "review-agent-54-1",
		State:               StateWorking,
		LastActivityTime:    time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "repository-agent-orchestrator-agent-54",
		},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	reviewer := &Agent{
		ID:               "review-agent-54-1",
		Role:             RoleReviewer,
		ParentAgentID:    coder.ID,
		IssueNumber:      54,
		BranchName:       coder.BranchName,
		PRNumber:         54,
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-agent-54-session",
		},
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

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.sent); got != 0 {
		t.Fatalf("forwarded runtime messages = %d, want 0", got)
	}
	if got := len(messenger.messages); got != 0 {
		t.Fatalf("forwarded inbox messages = %d, want 0", got)
	}
	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updated.State != StateWaiting {
		t.Fatalf("coder state = %s, want %s", updated.State, StateWaiting)
	}
}

func TestForwardNewCommentsRestartsApprovedRuntime(t *testing.T) {
	t.Parallel()

	reviewComment := map[string]any{
		"id":         7001,
		"html_url":   "https://github.com/acme/widget/pull/90#discussion_r7001",
		"created_at": "2026-03-05T16:20:00Z",
		"body":       "Please resolve this merge conflict and update tests.",
		"path":       "internal/app.go",
		"line":       123,
		"user": map[string]any{
			"login": "reviewer-a",
		},
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/90/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{reviewComment})
		case "/repos/acme/widget/issues/90/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-90",
		Role:                    RoleCoder,
		IssueNumber:             90,
		BranchName:              "repository-agent-orchestrator/issue-90",
		PRNumber:                90,
		PRURL:                   "https://github.com/acme/widget/pull/90",
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
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		messenger: &stubMessenger{},
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking {
		t.Fatalf("agent state = %s, want %s", updated.State, StateWorking)
	}
	if updated.RuntimeHandle.Session != "resumed-session" {
		t.Fatalf("runtime session = %q, want %q", updated.RuntimeHandle.Session, "resumed-session")
	}
}

func TestForwardNewCommentsRestartsApprovedRuntimeForHumanIssueComment(t *testing.T) {
	t.Parallel()

	issueComment := map[string]any{
		"id":         7002,
		"html_url":   "https://github.com/acme/widget/pull/90#issuecomment-7002",
		"created_at": "2026-03-05T16:21:00Z",
		"body":       "I found a bug during human review. Please fix the empty input case.",
		"user": map[string]any{
			"login": "reviewer-b",
		},
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/90/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/90/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{issueComment})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-90",
		Role:                    RoleCoder,
		IssueNumber:             90,
		BranchName:              "repository-agent-orchestrator/issue-90",
		PRNumber:                90,
		PRURL:                   "https://github.com/acme/widget/pull/90",
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
		startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-issue-session"},
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		messenger: &stubMessenger{},
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking {
		t.Fatalf("agent state = %s, want %s", updated.State, StateWorking)
	}
	if updated.RuntimeHandle.Session != "resumed-issue-session" {
		t.Fatalf("runtime session = %q, want %q", updated.RuntimeHandle.Session, "resumed-issue-session")
	}
	if got := agents.PendingReviewCommentCount(agent.ID); got != 1 {
		t.Fatalf("pending feedback count = %d, want 1", got)
	}
}

func TestForwardNewCommentsIgnoresMarkedCodingAgentReviewComment(t *testing.T) {
	t.Parallel()

	reviewComment := map[string]any{
		"id":         7003,
		"html_url":   "https://github.com/acme/widget/pull/90#discussion_r7003",
		"created_at": "2026-03-05T16:22:00Z",
		"body":       "Addressed this review thread.\n\nCODEX_AGENT_ID: coding-agent-90\nCODEX_AGENT_ROLE: coder",
		"path":       "internal/app.go",
		"line":       123,
		"user": map[string]any{
			"login": "member-user",
		},
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/90/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{reviewComment})
		case "/repos/acme/widget/issues/90/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-90",
		Role:                    RoleCoder,
		IssueNumber:             90,
		BranchName:              "repository-agent-orchestrator/issue-90",
		PRNumber:                90,
		PRURL:                   "https://github.com/acme/widget/pull/90",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-90-session"},
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
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0", got)
	}
	if got := len(messenger.messages); got != 0 {
		t.Fatalf("forwarded inbox messages = %d, want 0", got)
	}
	if !agents.HasSeenReviewComment(agent.ID, 7003) {
		t.Fatal("marked coding-agent review comment was not marked seen")
	}
	if got := agents.PendingReviewCommentCount(agent.ID); got != 0 {
		t.Fatalf("pending feedback count = %d, want 0", got)
	}
}

func TestForwardNewCommentsSkipsInternalReviewVerdictIssueComment(t *testing.T) {
	t.Parallel()

	verdictComment := map[string]any{
		"id":         8101,
		"html_url":   "https://github.com/acme/widget/pull/91#issuecomment-8101",
		"created_at": "2026-03-05T16:20:00Z",
		"body":       "CODEX_AGENT_ID: review-agent-91-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: abcdef123456\nCODEX_VERDICT: THUMBS_UP",
		"user": map[string]any{
			"login": "member-user",
		},
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/91/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/91/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{verdictComment})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-91",
		Role:                    RoleCoder,
		IssueNumber:             91,
		BranchName:              "repository-agent-orchestrator/issue-91",
		PRNumber:                91,
		PRURL:                   "https://github.com/acme/widget/pull/91",
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
		aliveSet: true,
		alive:    false,
	}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0", got)
	}
	if got := len(messenger.messages); got != 0 {
		t.Fatalf("forwarded inbox messages = %d, want 0", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateApproved {
		t.Fatalf("agent state = %s, want %s", updated.State, StateApproved)
	}
	if updated.RuntimeHandle.Session != "dead-session" {
		t.Fatalf("runtime session = %q, want %q", updated.RuntimeHandle.Session, "dead-session")
	}
	if !agents.HasSeenIssueComment(agent.ID, 8101) {
		t.Fatal("verdict issue comment was not marked seen")
	}
}

func TestForwardNewCommentsSkipsNeedsChangesVerdictIssueComment(t *testing.T) {
	t.Parallel()

	verdictComment := map[string]any{
		"id":         8201,
		"html_url":   "https://github.com/acme/widget/pull/93#issuecomment-8201",
		"created_at": "2026-03-05T16:20:00Z",
		"body":       "CODEX_AGENT_ID: review-agent-93-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: abcdef123456\nCODEX_VERDICT: NEEDS_CHANGES\n- Add one more test.",
		"user": map[string]any{
			"login": "member-user",
		},
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/93/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/93/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{verdictComment})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-93",
		Role:                    RoleCoder,
		IssueNumber:             93,
		BranchName:              "repository-agent-orchestrator/issue-93",
		PRNumber:                93,
		PRURL:                   "https://github.com/acme/widget/pull/93",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-93-session"},
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
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0", got)
	}
	if got := len(messenger.messages); got != 0 {
		t.Fatalf("forwarded inbox messages = %d, want 0 when no worktree exists", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking {
		t.Fatalf("agent state = %s, want %s", updated.State, StateWorking)
	}
	if !agents.HasSeenIssueComment(agent.ID, 8201) {
		t.Fatal("needs-changes verdict issue comment was not marked seen")
	}
	if got := agents.PendingReviewCommentCount(agent.ID); got != 0 {
		t.Fatalf("pending feedback count = %d, want 0", got)
	}
}

func TestForwardNewCommentsRetriesReviewCommentAfterSendFailure(t *testing.T) {
	t.Parallel()

	reviewComment := map[string]any{
		"id":         9101,
		"html_url":   "https://github.com/acme/widget/pull/92#discussion_r9101",
		"created_at": "2026-03-05T16:20:00Z",
		"body":       "Please add one more assertion.",
		"path":       "internal/app.go",
		"line":       123,
		"user": map[string]any{
			"login": "reviewer-a",
		},
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/92/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{reviewComment})
		case "/repos/acme/widget/issues/92/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
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
		ID:                      "coding-agent-92",
		Role:                    RoleCoder,
		IssueNumber:             92,
		BranchName:              "repository-agent-orchestrator/issue-92",
		PRNumber:                92,
		PRURL:                   "https://github.com/acme/widget/pull/92",
		WorktreePath:            worktree,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-92-session"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{sendErr: context.DeadlineExceeded}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
		},
		github:    ghClient,
		messenger: messenger,
		agents:    agents,
		runner:    runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	err := bot.forwardNewComments(context.Background(), snapshot)
	if err == nil {
		t.Fatal("forwardNewComments() error = nil, want send failure")
	}
	if agents.HasSeenReviewComment(agent.ID, 9101) {
		t.Fatal("review comment was marked seen after failed delivery")
	}
	if got := len(messenger.messages); got != 0 {
		t.Fatalf("forwarded inbox messages after failure = %d, want 0", got)
	}

	runner.sendErr = nil
	snapshot, _ = agents.Get(agent.ID)
	if err := bot.forwardNewComments(context.Background(), snapshot); err != nil {
		t.Fatalf("forwardNewComments() retry error = %v", err)
	}
	if !agents.HasSeenReviewComment(agent.ID, 9101) {
		t.Fatal("review comment was not marked seen after successful retry")
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("forwarded inbox messages = %d, want 1", got)
	}
}

func countMessagesContaining(messages []string, needle string) int {
	count := 0
	for _, msg := range messages {
		if strings.Contains(msg, needle) {
			count++
		}
	}
	return count
}
