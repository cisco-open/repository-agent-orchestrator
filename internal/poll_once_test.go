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

func TestPollOnceProcessesMixedAgents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/pulls/2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 2,
				"merged": false,
				"head": map[string]any{
					"sha": "bbbbbbbbbbbbbbbb",
				},
			})
		case "/repos/acme/widget/pulls/2/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/2/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/3/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coderNoPR := &Agent{
		ID:                      "coding-agent-1",
		Role:                    RoleCoder,
		IssueNumber:             1,
		BranchName:              "repository-agent-orchestrator/issue-1",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-1"},
	}
	coderWithPR := &Agent{
		ID:                      "coding-agent-2",
		Role:                    RoleCoder,
		IssueNumber:             2,
		BranchName:              "repository-agent-orchestrator/issue-2",
		PRNumber:                2,
		PRURL:                   "https://github.com/acme/widget/pull/2",
		ObservedPRHeadSHA:       "bbbbbbbbbbbbbbbb",
		LastReviewedHeadSHA:     "bbbbbbbbbbbbbbbb",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-2"},
	}
	reviewer := &Agent{
		ID:                           "review-agent-3",
		Role:                         RoleReviewer,
		ParentAgentID:                coderWithPR.ID,
		IssueNumber:                  3,
		BranchName:                   "repository-agent-orchestrator/issue-3",
		PRNumber:                     3,
		ObservedPRHeadSHA:            "cccccccccccc",
		ReviewBaselineIssueCommentID: 10,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle:                RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-3"},
	}
	for _, a := range []*Agent{coderNoPR, coderWithPR, reviewer} {
		if err := agents.Add(a); err != nil {
			t.Fatalf("Add(%s) error = %v", a.ID, err)
		}
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    "/tmp/repo/.worktrees",
			PollInterval:   20 * time.Second,
			MandatoryTests: []string{"make test"},
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	if got := bot.agents.LastPoll(); got.IsZero() {
		t.Fatal("LastPoll() is zero, want updated timestamp")
	}

	updatedNoPR, _ := agents.Get(coderNoPR.ID)
	if updatedNoPR.PRNumber != 0 {
		t.Fatalf("coder without PR unexpectedly changed PRNumber to %d", updatedNoPR.PRNumber)
	}

	updatedWithPR, _ := agents.Get(coderWithPR.ID)
	if updatedWithPR.State != StateWorking {
		t.Fatalf("coder with PR state = %s, want %s", updatedWithPR.State, StateWorking)
	}

	updatedReviewer, _ := agents.Get(reviewer.ID)
	if updatedReviewer.State != StateWorking {
		t.Fatalf("reviewer state = %s, want %s", updatedReviewer.State, StateWorking)
	}
}

func TestPollOnceProcessesAgentsConcurrently(t *testing.T) {
	const perRequestDelay = 350 * time.Millisecond

	var (
		mu          sync.Mutex
		inflight    int
		maxInflight int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls":
			mu.Lock()
			inflight++
			if inflight > maxInflight {
				maxInflight = inflight
			}
			mu.Unlock()

			time.Sleep(perRequestDelay)

			mu.Lock()
			inflight--
			mu.Unlock()
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	for _, a := range []*Agent{
		{
			ID:                      "coding-agent-a",
			Role:                    RoleCoder,
			IssueNumber:             101,
			BranchName:              "repository-agent-orchestrator/issue-101",
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		},
		{
			ID:                      "coding-agent-b",
			Role:                    RoleCoder,
			IssueNumber:             102,
			BranchName:              "repository-agent-orchestrator/issue-102",
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		},
	} {
		if err := agents.Add(a); err != nil {
			t.Fatalf("Add(%s) error = %v", a.ID, err)
		}
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
	}

	start := time.Now()
	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}
	elapsed := time.Since(start)

	mu.Lock()
	gotMaxInflight := maxInflight
	mu.Unlock()
	if gotMaxInflight < 2 {
		t.Fatalf("max concurrent pull list requests = %d, want >= 2", gotMaxInflight)
	}
	if elapsed > 650*time.Millisecond {
		t.Fatalf("PollOnce() elapsed = %s, expected concurrent runtime under 650ms", elapsed)
	}
}

func TestPollOnceProcessesPausedCoderLifecycle(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"number":    9,
					"html_url":  "https://github.com/acme/widget/pull/9",
					"draft":     true,
					"state":     "open",
					"mergeable": true,
					"head": map[string]any{
						"sha": "999999999999",
					},
					"base": map[string]any{
						"ref": "main",
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "coding-agent-paused",
		Role:                    RoleCoder,
		IssueNumber:             9,
		BranchName:              "repository-agent-orchestrator/issue-9",
		LastReviewedHeadSHA:     "999999999999",
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

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			BaseBranch:   "main",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updated.PRNumber != 9 {
		t.Fatalf("paused coder PRNumber = %d, want 9", updated.PRNumber)
	}
	if updated.State != StateWaiting || !updated.Paused {
		t.Fatalf("paused coder state = (%s, paused=%v), want (%s, true) after PR detection", updated.State, updated.Paused, StateWaiting)
	}
}

func TestPollOnceKeepsPausedCoderPausedWhileReviewerActive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/79":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          79,
				"merged":          false,
				"state":           "open",
				"mergeable_state": "clean",
				"head": map[string]any{
					"sha": "ffffffffffffffff",
				},
			})
		case "/repos/acme/widget/issues/79/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	coder := &Agent{
		ID:                      "coding-agent-79-paused",
		Role:                    RoleCoder,
		IssueNumber:             79,
		BranchName:              "repository-agent-orchestrator/issue-79",
		PRNumber:                79,
		PRURL:                   "https://github.com/acme/widget/pull/79",
		ObservedPRHeadSHA:       "ffffffffffffffff",
		ActiveReviewAgentID:     "review-agent-79-1",
		WorktreePath:            worktree,
		State:                   StateWaiting,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                           "review-agent-79-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  79,
		BranchName:                   coder.BranchName,
		PRNumber:                     79,
		PRURL:                        coder.PRURL,
		ObservedPRHeadSHA:            "ffffffffffffffff",
		ReviewBaselineIssueCommentID: 10,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle:                RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-79"},
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	for _, agent := range []*Agent{coder, reviewer} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updatedCoder.State != StateWaiting || !updatedCoder.Paused {
		t.Fatalf("coder state = (%s, paused=%v), want (%s, true)", updatedCoder.State, updatedCoder.Paused, StateWaiting)
	}
	if updatedCoder.ActiveReviewAgentID != reviewer.ID {
		t.Fatalf("ActiveReviewAgentID = %q, want %q", updatedCoder.ActiveReviewAgentID, reviewer.ID)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0", got)
	}
}

func TestPollOnceMarksUnexpectedDeadRuntimeAsErrored(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-4",
		Role:                    RoleCoder,
		IssueNumber:             4,
		BranchName:              "repository-agent-orchestrator/issue-4",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "dead-coder"},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			PollInterval: 20 * time.Second,
		},
		agents: agents,
		runner: &stubRunner{
			aliveSet: true,
			alive:    false,
		},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateErrored {
		t.Fatalf("agent state = %s, want %s", updated.State, StateErrored)
	}
	if updated.Stopped {
		t.Fatalf("agent stopped = true, want false")
	}
}

func TestPollOnceRepoIndexerStopsRuntimeAndWaitsForHumanReviewWhenPROpens(t *testing.T) {
	worktree := t.TempDir()
	sourcePath := filepath.Join(worktree, "agent_index.yaml")
	if err := os.WriteFile(sourcePath, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(agent_index.yaml) error = %v", err)
	}
	indexPath := filepath.Join(worktree, "INDEX.md")
	if err := os.WriteFile(indexPath, []byte("# Index\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(INDEX.md) error = %v", err)
	}

	draft := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"number":   44,
					"html_url": "https://github.com/acme/widget/pull/44",
					"base": map[string]any{
						"ref": "main",
					},
					"head": map[string]any{
						"sha": "deadbeef",
					},
				},
			})
		case "/repos/acme/widget/pulls/44":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 44,
				"merged": false,
				"draft":  draft,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var notifications []string
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		notifications = append(notifications, payload["markdown"])
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "index-agent-4",
		Role:                    RoleIndexer,
		WorktreePath:            worktree,
		BranchName:              "repository-agent-orchestrator/repo-index-4",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "indexer-4"},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	var ghCalls []string
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			PollInterval:    20 * time.Second,
			WebexWebhookURL: webex.URL,
		},
		github:   newGitHubClientForTest(t, srv),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			ghCalls = append(ghCalls, name+" "+strings.Join(args, " "))
			draft = false
			return nil
		},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWaiting || updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateWaiting)
	}
	if updated.PRNumber != 44 || updated.PRURL != "https://github.com/acme/widget/pull/44" {
		t.Fatalf("PR = (%d, %q), want (44, %q)", updated.PRNumber, updated.PRURL, "https://github.com/acme/widget/pull/44")
	}
	if updated.RuntimeHandle.Session != "" {
		t.Fatalf("runtime session = %q, want cleared handle", updated.RuntimeHandle.Session)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
	if got, want := ghCalls, "gh pr ready 44 -R acme/widget"; len(got) != 1 || got[0] != want {
		t.Fatalf("gh ready calls = %v, want [%s]", got, want)
	}
	if draft {
		t.Fatal("PR remained a draft after ready-for-review notification")
	}
	if got := countMessagesContaining(notifications, "repo indexing agent `index-agent-4` is ready for human review"); got != 1 {
		t.Fatalf("ready-for-review notifications = %d, want 1; messages=%v", got, notifications)
	}
}

func TestPollOnceRepoIndexerDoesNotClaimReadyWhenDraftPromotionFails(t *testing.T) {
	worktree := t.TempDir()
	for name, content := range map[string]string{
		"agent_index.yaml": "version: 1\n",
		"INDEX.md":         "# Index\n",
	} {
		if err := os.WriteFile(filepath.Join(worktree, name), []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"number":   44,
				"html_url": "https://github.com/acme/widget/pull/44",
				"base":     map[string]any{"ref": "main"},
				"head":     map[string]any{"sha": "deadbeef"},
			}})
		case "/repos/acme/widget/pulls/44":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 44,
				"merged": false,
				"draft":  true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var notifications []string
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		notifications = append(notifications, payload["markdown"])
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "index-agent-4",
		Role:                    RoleIndexer,
		WorktreePath:            worktree,
		BranchName:              "repository-agent-orchestrator/repo-index-4",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "indexer-4"},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			PollInterval:    20 * time.Second,
			WebexWebhookURL: webex.URL,
		},
		github:   newGitHubClientForTest(t, srv),
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return errors.New("draft promotion failed")
		},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateWorking || updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateWorking)
	}
	if updated.RuntimeHandle.Session != "indexer-4" {
		t.Fatalf("runtime session = %q, want %q", updated.RuntimeHandle.Session, "indexer-4")
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("runner.Stop calls = %d, want 0", got)
	}
	if got := countMessagesContaining(notifications, "ready for human review"); got != 0 {
		t.Fatalf("ready-for-review notifications = %d, want 0; messages=%v", got, notifications)
	}
}

func TestPollOnceRepoIndexerCleansUpAfterHumanMerge(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/repo-index-44"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/44":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 44,
				"merged": true,
				"merged_by": map[string]any{
					"login": "maintainer",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "index-agent-merged",
		Role:                    RoleIndexer,
		WorktreePath:            worktreePath,
		BranchName:              branch,
		PRNumber:                44,
		PRURL:                   "https://github.com/acme/widget/pull/44",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     repoPath,
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				return nil
			}
			return runCommandWithOutput(ctx, dir, name, args...)
		},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree path should be removed, got err=%v", err)
	}
	localBranch := strings.TrimSpace(runGit(t, repoPath, "branch", "--list", branch))
	if localBranch != "" {
		t.Fatalf("local branch %q should be deleted, output=%q", branch, localBranch)
	}
	remoteBranch := strings.TrimSpace(runGit(t, repoPath, "ls-remote", "--heads", "origin", branch))
	if remoteBranch != "" {
		t.Fatalf("remote branch %q should be deleted, output=%q", branch, remoteBranch)
	}
}

func TestPollOnceRepoIndexerCleansUpAfterPRClose(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/repo-index-45"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/45":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 45,
				"merged": false,
				"state":  "closed",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "index-agent-closed",
		Role:                    RoleIndexer,
		WorktreePath:            worktreePath,
		BranchName:              branch,
		PRNumber:                45,
		PRURL:                   "https://github.com/acme/widget/pull/45",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     repoPath,
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree path should be removed, got err=%v", err)
	}
	localBranch := strings.TrimSpace(runGit(t, repoPath, "branch", "--list", branch))
	if localBranch != "" {
		t.Fatalf("local branch %q should be deleted, output=%q", branch, localBranch)
	}
	remoteBranch := strings.TrimSpace(runGit(t, repoPath, "ls-remote", "--heads", "origin", branch))
	if remoteBranch != "" {
		t.Fatalf("remote branch %q should be deleted, output=%q", branch, remoteBranch)
	}
}

func TestPollOncePausedRepoIndexerCleansUpAfterPRClose(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/repo-index-46"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/46":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 46,
				"merged": false,
				"state":  "closed",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "index-agent-paused-closed",
		Role:                    RoleIndexer,
		WorktreePath:            worktreePath,
		BranchName:              branch,
		PRNumber:                46,
		PRURL:                   "https://github.com/acme/widget/pull/46",
		State:                   StateWaiting,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     repoPath,
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree path should be removed, got err=%v", err)
	}
	localBranch := strings.TrimSpace(runGit(t, repoPath, "branch", "--list", branch))
	if localBranch != "" {
		t.Fatalf("local branch %q should be deleted, output=%q", branch, localBranch)
	}
	remoteBranch := strings.TrimSpace(runGit(t, repoPath, "ls-remote", "--heads", "origin", branch))
	if remoteBranch != "" {
		t.Fatalf("remote branch %q should be deleted, output=%q", branch, remoteBranch)
	}
}

func TestPollOnceCleansUpPausedManualReviewerAfterPRClose(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/manual-review-147"
	repoPath, reviewWorktreePath := setupRepoWithFeatureWorktree(t, branch)

	logDir := t.TempDir()
	reviewerLogPath := filepath.Join(logDir, "reviewer.log")
	if err := os.WriteFile(reviewerLogPath, []byte("reviewer log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(reviewer log) error = %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/147":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 147,
				"merged": false,
				"state":  "closed",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                      "review-agent-147-manual",
		Role:                    RoleReviewer,
		IssueNumber:             147,
		BranchName:              branch,
		PRNumber:                147,
		PRURL:                   "https://github.com/acme/widget/pull/147",
		WorktreePath:            reviewWorktreePath,
		LogDir:                  logDir,
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, LogPath: reviewerLogPath},
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
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     repoPath,
			LogDir:       logDir,
			BaseBranch:   "main",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateDone || !updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updatedReviewer.State, updatedReviewer.Stopped, StateDone)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0", got)
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("runner.Stop calls = %d, want 0 for an already-paused manual reviewer", got)
	}

	if _, err := os.Stat(reviewWorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewerLogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review log should be removed, got err=%v", err)
	}
}

func TestPollOnceCleansUpPausedManualReviewerAfterPRHeadChanges(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/manual-review-148"
	repoPath, reviewWorktreePath := setupRepoWithFeatureWorktree(t, branch)

	logDir := t.TempDir()
	reviewerLogPath := filepath.Join(logDir, "reviewer.log")
	if err := os.WriteFile(reviewerLogPath, []byte("reviewer log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(reviewer log) error = %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/148":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 148,
				"merged": false,
				"state":  "open",
				"head": map[string]any{
					"sha": "bbbbbbbbbbbb",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                      "review-agent-148-manual",
		Role:                    RoleReviewer,
		IssueNumber:             148,
		BranchName:              branch,
		PRNumber:                148,
		PRURL:                   "https://github.com/acme/widget/pull/148",
		ObservedPRHeadSHA:       "aaaaaaaaaaaa",
		WorktreePath:            reviewWorktreePath,
		LogDir:                  logDir,
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, LogPath: reviewerLogPath},
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
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     repoPath,
			LogDir:       logDir,
			BaseBranch:   "main",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateDone || !updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updatedReviewer.State, updatedReviewer.Stopped, StateDone)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0", got)
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("runner.Stop calls = %d, want 0 for an already-paused manual reviewer", got)
	}

	if _, err := os.Stat(reviewWorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewerLogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review log should be removed, got err=%v", err)
	}
}

func TestPollOnceClearsDeadRuntimeHandleForApprovedAgent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/44":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 44,
				"merged": false,
			})
		case "/repos/acme/widget/pulls/44/reviews":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/pulls/44/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/44/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-44",
		Role:                    RoleCoder,
		IssueNumber:             44,
		BranchName:              "repository-agent-orchestrator/issue-44",
		PRNumber:                44,
		PRURL:                   "https://github.com/acme/widget/pull/44",
		State:                   StateApproved,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "old-approved-session"},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{
			aliveSet: true,
			alive:    false,
		},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateApproved {
		t.Fatalf("agent state = %s, want %s", updated.State, StateApproved)
	}
	if strings.TrimSpace(updated.RuntimeHandle.Session) != "" {
		t.Fatalf("agent runtime session = %q, want empty", updated.RuntimeHandle.Session)
	}
}

func TestPollOnceAutoMergePathIsLiveForApprovedAgents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/45":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          45,
				"merged":          false,
				"mergeable_state": "clean",
			})
		case "/repos/acme/widget/pulls/45/reviews":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"state": "APPROVED",
					"user": map[string]any{
						"login": "reviewer-a",
					},
				},
			})
		case "/repos/acme/widget/pulls/45/merge":
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "merge failed"})
		case "/repos/acme/widget/pulls/45/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/45/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-45",
		Role:                    RoleCoder,
		IssueNumber:             45,
		BranchName:              "repository-agent-orchestrator/issue-45",
		PRNumber:                45,
		PRURL:                   "https://github.com/acme/widget/pull/45",
		ObservedPRHeadSHA:       "aaaaaaaaaaaa",
		LastReviewedHeadSHA:     "aaaaaaaaaaaa",
		LastReviewVerdict:       ReviewVerdictThumbsUp,
		State:                   StateApproved,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateErrored {
		t.Fatalf("agent state = %s, want %s", updated.State, StateErrored)
	}
}

func TestPollOnceAutoMergesPausedThumbsUpCoderAfterGitHubApproval(t *testing.T) {
	branch := "repository-agent-orchestrator/issue-145"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)

	var mergeCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/145":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          145,
				"merged":          false,
				"mergeable_state": "clean",
				"draft":           false,
				"head": map[string]any{
					"sha": "aaaaaaaaaaaa",
				},
			})
		case "/repos/acme/widget/pulls/145/reviews":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"state": "APPROVED",
					"user": map[string]any{
						"login": "reviewer-a",
					},
				},
			})
		case "/repos/acme/widget/pulls/145/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/145/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/pulls/145/merge":
			mergeCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{"merged": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-145",
		Role:                    RoleCoder,
		IssueNumber:             145,
		BranchName:              branch,
		PRNumber:                145,
		PRURL:                   "https://github.com/acme/widget/pull/145",
		WorktreePath:            worktreePath,
		ObservedPRHeadSHA:       "aaaaaaaaaaaa",
		LastReviewedHeadSHA:     "aaaaaaaaaaaa",
		LastReviewVerdict:       ReviewVerdictThumbsUp,
		State:                   StateApproved,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     repoPath,
			BaseBranch:   "main",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}
	if mergeCalls != 1 {
		t.Fatalf("merge calls = %d, want 1", mergeCalls)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 for paused coder", got)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree path should be removed after merge cleanup, got err=%v", err)
	}
}

func TestPollOnceProcessesHumanFeedbackBeforeAutoMerge(t *testing.T) {
	var mergeCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/46":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          46,
				"merged":          false,
				"mergeable_state": "clean",
			})
		case "/repos/acme/widget/pulls/46/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/46/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"id":         4601,
					"html_url":   "https://github.com/acme/widget/pull/46#issuecomment-4601",
					"created_at": "2026-03-05T16:20:00Z",
					"body":       "Human review found a bug. Please handle nil input.",
					"user": map[string]any{
						"login": "reviewer-a",
					},
				},
			})
		case "/repos/acme/widget/pulls/46/merge":
			mergeCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{"merged": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-46",
		Role:                    RoleCoder,
		IssueNumber:             46,
		BranchName:              "repository-agent-orchestrator/issue-46",
		PRNumber:                46,
		PRURL:                   "https://github.com/acme/widget/pull/46",
		State:                   StateApproved,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "old-approved-session"},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{
			aliveSet:    true,
			alive:       false,
			startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "resumed-session"},
		},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
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
	if got := agents.PendingReviewCommentCount(agent.ID); got != 1 {
		t.Fatalf("pending feedback count = %d, want 1", got)
	}
	if mergeCalls != 0 {
		t.Fatalf("merge calls = %d, want 0", mergeCalls)
	}
}

func TestPollOncePromotesThumbsUpWaitingCoderAfterReadyForReviewSucceeds(t *testing.T) {
	var ghCalls []string
	var draft bool = true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/47":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          47,
				"merged":          false,
				"mergeable_state": "clean",
				"draft":           draft,
				"head": map[string]any{
					"sha": "aaaaaaaaaaaa",
				},
			})
		case "/repos/acme/widget/pulls/47/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/47/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/pulls/47/reviews":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-47",
		Role:                    RoleCoder,
		IssueNumber:             47,
		BranchName:              "repository-agent-orchestrator/issue-47",
		PRNumber:                47,
		PRURL:                   "https://github.com/acme/widget/pull/47",
		ObservedPRHeadSHA:       "aaaaaaaaaaaa",
		LastReviewedHeadSHA:     "aaaaaaaaaaaa",
		LastReviewVerdict:       ReviewVerdictThumbsUp,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: &stubRunner{},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				ghCalls = append(ghCalls, name+" "+strings.Join(args, " "))
				draft = false
				return nil
			}
			return nil
		},
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateApproved {
		t.Fatalf("agent state = %s, want %s", updated.State, StateApproved)
	}
	if len(ghCalls) != 1 {
		t.Fatalf("gh ready calls = %d, want 1", len(ghCalls))
	}
	if got, want := ghCalls[0], "gh pr ready 47 -R acme/widget"; got != want {
		t.Fatalf("gh ready command = %q, want %q", got, want)
	}
}

func TestPollOnceDetectsMergeConflictsAndSteersRebaseOncePerHead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/77":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          77,
				"merged":          false,
				"mergeable_state": "dirty",
				"head": map[string]any{
					"sha": "dddddddddddddddd",
				},
			})
		case "/repos/acme/widget/pulls/77/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/77/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-77",
		Role:                    RoleCoder,
		IssueNumber:             77,
		BranchName:              "repository-agent-orchestrator/issue-77",
		PRNumber:                77,
		PRURL:                   "https://github.com/acme/widget/pull/77",
		ObservedPRHeadSHA:       "dddddddddddddddd",
		LastReviewedHeadSHA:     "dddddddddddddddd",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-77-session"},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() first error = %v", err)
	}
	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() second error = %v", err)
	}

	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}
	if got := runner.sent[0]; !strings.Contains(got, "merge conflicts") || !strings.Contains(got, "Rebase") {
		t.Fatalf("conflict steer message = %q, want rebase guidance", got)
	}
	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.LastConflictHeadSHA != "dddddddddddddddd" {
		t.Fatalf("LastConflictHeadSHA = %q, want %q", updated.LastConflictHeadSHA, "dddddddddddddddd")
	}
	if updated.State != StateWorking {
		t.Fatalf("agent state = %s, want %s", updated.State, StateWorking)
	}
}

func TestPollOnceDetectsMergeConflictsWithoutResumingPausedCoder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/78":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          78,
				"merged":          false,
				"mergeable_state": "dirty",
				"head": map[string]any{
					"sha": "eeeeeeeeeeeeeeee",
				},
			})
		case "/repos/acme/widget/pulls/78/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		case "/repos/acme/widget/issues/78/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	worktree := t.TempDir()
	if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	agent := &Agent{
		ID:                      "coding-agent-78-paused",
		Role:                    RoleCoder,
		IssueNumber:             78,
		BranchName:              "repository-agent-orchestrator/issue-78",
		PRNumber:                78,
		PRURL:                   "https://github.com/acme/widget/pull/78",
		WorktreePath:            worktree,
		ObservedPRHeadSHA:       "eeeeeeeeeeeeeeee",
		LastReviewedHeadSHA:     "eeeeeeeeeeeeeeee",
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "unexpected-resume"}}
	messenger := &stubMessenger{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			PollInterval: 20 * time.Second,
		},
		github:    newGitHubClientForTest(t, srv),
		agents:    agents,
		runner:    runner,
		messenger: messenger,
	}

	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() first error = %v", err)
	}
	if err := bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() second error = %v", err)
	}

	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls = %d, want 0 for paused coder", got)
	}
	if got := len(runner.sent); got != 0 {
		t.Fatalf("runner.Send calls = %d, want 0 for paused coder", got)
	}
	if got := len(messenger.messages); got != 1 {
		t.Fatalf("conflict inbox messages = %d, want 1", got)
	}
	if !strings.Contains(messenger.messages[0], "Merge Conflict Detected") {
		t.Fatalf("conflict inbox message = %q, want merge conflict guidance", messenger.messages[0])
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.LastConflictHeadSHA != "eeeeeeeeeeeeeeee" {
		t.Fatalf("LastConflictHeadSHA = %q, want %q", updated.LastConflictHeadSHA, "eeeeeeeeeeeeeeee")
	}
	if updated.State != StateWorking || !updated.Paused {
		t.Fatalf("agent state = (%s, paused=%v), want (%s, true)", updated.State, updated.Paused, StateWorking)
	}
}
