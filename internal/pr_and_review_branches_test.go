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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-github/v90/github"
)

func newGitHubClientForTest(t *testing.T, srv *httptest.Server) *github.Client {
	t.Helper()
	client := mustNewGitHubClientForTest(t, srv.Client(), srv.URL+"/")
	return client
}

func TestDetectPRBranches(t *testing.T) {
	t.Run("skips reviewer agents", func(t *testing.T) {
		bot := &Orchestrator{cfg: Config{RepoOwner: "acme", RepoName: "widget"}, agents: NewAgentManager()}
		err := bot.detectPR(context.Background(), Agent{ID: "review-1", Role: RoleReviewer})
		if err != nil {
			t.Fatalf("detectPR() error = %v", err)
		}
	})

	t.Run("returns GitHub list error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/acme/widget/pulls" {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"message": "boom"})
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: NewAgentManager(),
		}
		err := bot.detectPR(context.Background(), Agent{ID: "coder-1", Role: RoleCoder, BranchName: "repository-agent-orchestrator/issue-1"})
		if err == nil {
			t.Fatal("detectPR() error = nil, want GitHub error")
		}
	})

	t.Run("returns nil when no PR exists", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/acme/widget/pulls" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode([]map[string]any{})
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: NewAgentManager(),
		}
		err := bot.detectPR(context.Background(), Agent{ID: "coder-2", Role: RoleCoder, BranchName: "repository-agent-orchestrator/issue-2"})
		if err != nil {
			t.Fatalf("detectPR() error = %v", err)
		}
	})

	t.Run("ignores PR when agent is not tracked", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/acme/widget/pulls" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode([]map[string]any{{
					"number":   9,
					"html_url": "https://github.com/acme/widget/pull/9",
					"head":     map[string]any{"sha": "abc"},
					"base":     map[string]any{"ref": "main"},
				}})
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		runner := &stubRunner{}
		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: NewAgentManager(),
			runner: runner,
		}
		err := bot.detectPR(context.Background(), Agent{ID: "missing", Role: RoleCoder, BranchName: "repository-agent-orchestrator/issue-9"})
		if err != nil {
			t.Fatalf("detectPR() error = %v", err)
		}
		if got := len(runner.started); got != 0 {
			t.Fatalf("runner.Start calls = %d, want 0", got)
		}
	})

	t.Run("sets PR and launches review agent", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/repos/acme/widget/pulls":
				_ = json.NewEncoder(w).Encode([]map[string]any{{
					"number":   42,
					"html_url": "https://github.com/acme/widget/pull/42",
					"head":     map[string]any{"sha": testReviewHeadSHA},
					"base":     map[string]any{"ref": "main"},
				}})
			case "/repos/acme/widget/issues/42/comments":
				_ = json.NewEncoder(w).Encode([]map[string]any{})
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		agents := NewAgentManager()
		coder := &Agent{
			ID:                      "coding-agent-42-1",
			Role:                    RoleCoder,
			IssueNumber:             42,
			IssueTitle:              "Improve coverage",
			BranchName:              "repository-agent-orchestrator/issue-42",
			WorktreePath:            t.TempDir(),
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		}
		if err := agents.Add(coder); err != nil {
			t.Fatalf("Add(coder) error = %v", err)
		}

		runner := &stubRunner{}
		commands := make([]string, 0)
		bot := &Orchestrator{
			cfg: Config{
				RepoOwner:      "acme",
				RepoName:       "widget",
				RepoPath:       "/tmp/repo",
				WorktreeDir:    t.TempDir(),
				BaseBranch:     "main",
				MandatoryTests: []string{"make test"},
			},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: runner,
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				commands = append(commands, strings.TrimSpace(dir+"::"+name+" "+strings.Join(args, " ")))
				return nil
			},
		}

		snapshot, ok := agents.Get(coder.ID)
		if !ok {
			t.Fatalf("Get(%s) = not found", coder.ID)
		}
		if err := bot.detectPR(context.Background(), snapshot); err != nil {
			t.Fatalf("detectPR() error = %v", err)
		}
		reviewer := waitForReviewAgentForPR(t, agents, 42)
		defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()

		updated, ok := agents.Get(coder.ID)
		if !ok {
			t.Fatalf("Get(%s) = not found", coder.ID)
		}
		if updated.PRNumber != 42 {
			t.Fatalf("PRNumber = %d, want 42", updated.PRNumber)
		}
		if updated.ObservedPRHeadSHA != testReviewHeadSHA {
			t.Fatalf("ObservedPRHeadSHA = %q, want %q", updated.ObservedPRHeadSHA, testReviewHeadSHA)
		}
		if updated.State != StateWaiting {
			t.Fatalf("state = %s, want %s", updated.State, StateWaiting)
		}
		if strings.TrimSpace(updated.ActiveReviewAgentID) == "" {
			t.Fatal("ActiveReviewAgentID is empty, want launched reviewer")
		}
		if got := len(runner.started); got != 0 {
			t.Fatalf("top-level runner.Start calls = %d, want 0", got)
		}
		if reviewer.RuntimeCWD != reviewer.WorktreePath {
			t.Fatalf("coordinator runtime cwd = %q, want %q", reviewer.RuntimeCWD, reviewer.WorktreePath)
		}
		if got := len(commands); got != 3 {
			t.Fatalf("review setup + gate command count = %d, want 3", got)
		}
		if !strings.Contains(commands[0], "git fetch --prune origin repository-agent-orchestrator/issue-42") {
			t.Fatalf("first review setup command = %q, want branch fetch", commands[0])
		}
		if !strings.Contains(commands[1], "git worktree add --detach") {
			t.Fatalf("second review setup command = %q, want detached worktree add", commands[1])
		}
		if !strings.Contains(commands[2], "::make test") {
			t.Fatalf("third gate command = %q, want make test", commands[2])
		}
	})

	t.Run("ignores open PRs for a different base branch", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/repos/acme/widget/pulls" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"number":   42,
				"html_url": "https://github.com/acme/widget/pull/42",
				"head":     map[string]any{"sha": testReviewHeadSHA},
				"base":     map[string]any{"ref": "release"},
			}})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		coder := &Agent{
			ID:                      "coding-agent-42-2",
			Role:                    RoleCoder,
			IssueNumber:             42,
			IssueTitle:              "Improve coverage",
			BranchName:              "repository-agent-orchestrator/issue-42",
			WorktreePath:            t.TempDir(),
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
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
				RepoOwner:      "acme",
				RepoName:       "widget",
				BaseBranch:     "main",
				MandatoryTests: []string{"make test"},
			},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: runner,
		}

		snapshot, ok := agents.Get(coder.ID)
		if !ok {
			t.Fatalf("Get(%s) = not found", coder.ID)
		}
		if err := bot.detectPR(context.Background(), snapshot); err != nil {
			t.Fatalf("detectPR() error = %v", err)
		}

		updated, ok := agents.Get(coder.ID)
		if !ok {
			t.Fatalf("Get(%s) = not found", coder.ID)
		}
		if updated.PRNumber != 0 {
			t.Fatalf("PRNumber = %d, want 0", updated.PRNumber)
		}
		if got := len(runner.started); got != 0 {
			t.Fatalf("runner.Start calls = %d, want 0", got)
		}
	})
}

func TestDetectPRKeepsPausedCoderPausedWhenReviewLaunchStarts(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"number":   142,
				"html_url": "https://github.com/acme/widget/pull/142",
				"head":     map[string]any{"sha": testThirdReviewHeadSHA},
				"base":     map[string]any{"ref": "main"},
			}})
		case "/repos/acme/widget/issues/142/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "coding-agent-142",
		Role:                    RoleCoder,
		IssueNumber:             142,
		IssueTitle:              "Preserve paused coder state",
		BranchName:              "repository-agent-orchestrator/issue-142",
		WorktreePath:            t.TempDir(),
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

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			BaseBranch:     "main",
			MandatoryTests: []string{"make test"},
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
		t.Fatalf("Get(%s) = not found", coder.ID)
	}
	if err := bot.detectPR(context.Background(), snapshot); err != nil {
		t.Fatalf("detectPR() error = %v", err)
	}

	reviewer := waitForReviewAgentForPR(t, agents, 142)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", coder.ID)
	}
	if updated.PRNumber != 142 {
		t.Fatalf("PRNumber = %d, want 142", updated.PRNumber)
	}
	if updated.ObservedPRHeadSHA != testThirdReviewHeadSHA {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", updated.ObservedPRHeadSHA, testThirdReviewHeadSHA)
	}
	if updated.State != StateWaiting || !updated.Paused {
		t.Fatalf("state = (%s, paused=%v), want (%s, true)", updated.State, updated.Paused, StateWaiting)
	}
	if strings.TrimSpace(updated.ActiveReviewAgentID) == "" {
		t.Fatal("ActiveReviewAgentID is empty, want launched reviewer")
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0", got)
	}
}

func TestDetectPRReturnsBeforePreReviewGateCompletes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"number":   55,
				"html_url": "https://github.com/acme/widget/pull/55",
				"head":     map[string]any{"sha": testOtherReviewHeadSHA},
			}})
		case "/repos/acme/widget/issues/55/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "coding-agent-55-1",
		Role:                    RoleCoder,
		IssueNumber:             55,
		IssueTitle:              "Gate async",
		BranchName:              "repository-agent-orchestrator/issue-55",
		WorktreePath:            t.TempDir(),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	gateStarted := make(chan struct{}, 2)
	gateRelease := make(chan struct{})
	reviewLaunchStarted := make(chan struct{}, 2)
	runner := &stubRunner{
		startHandle:  RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session"},
		startStarted: reviewLaunchStarted,
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			BaseBranch:     "main",
			MandatoryTests: []string{"make test-all"},
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			command := strings.TrimSpace(name + " " + strings.Join(args, " "))
			if name == "git" && len(args) >= 4 && args[0] == "worktree" && args[1] == "add" {
				worktreePath := args[3]
				if err := os.MkdirAll(worktreePath, 0o755); err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(worktreePath, ".git"), []byte("gitdir: /tmp/example\n"), 0o644)
			}
			if command == "make test-all" {
				select {
				case gateStarted <- struct{}{}:
				default:
				}
				<-gateRelease
			}
			return nil
		},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", coder.ID)
	}
	start := time.Now()
	if err := bot.detectPR(context.Background(), snapshot); err != nil {
		t.Fatalf("detectPR() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("detectPR() blocked for %s while pre-review gate was running", elapsed)
	}

	select {
	case <-gateStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for asynchronous pre-review gate start")
	}

	updated, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), updated); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() while gate in flight error = %v", err)
	}
	select {
	case <-gateStarted:
		t.Fatal("duplicate pre-review gate started while first gate was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(gateRelease)
	reviewer := waitForReviewAgentForPR(t, agents, 55)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()
	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0 after gate release", got)
	}
}

func TestListAllReviewsAndIsPRApproved(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/7/reviews":
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			if page <= 1 {
				w.Header().Set("Link", "<"+srv.URL+"/repos/acme/widget/pulls/7/reviews?page=2>; rel=\"next\"")
				_ = json.NewEncoder(w).Encode([]map[string]any{{
					"id":    1,
					"state": "COMMENTED",
					"user":  map[string]any{"login": "alice"},
				}})
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"id":    2,
				"state": "APPROVED",
				"user":  map[string]any{"login": "alice"},
			}})
		case "/repos/acme/widget/pulls/8/reviews":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 11, "state": "APPROVED", "user": map[string]any{"login": "bob"}},
				{"id": 12, "state": "CHANGES_REQUESTED", "user": map[string]any{"login": "bob"}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
		github: newGitHubClientForTest(t, srv),
	}

	reviews, err := bot.listAllReviews(context.Background(), 7)
	if err != nil {
		t.Fatalf("listAllReviews() error = %v", err)
	}
	if len(reviews) != 2 {
		t.Fatalf("len(listAllReviews()) = %d, want 2", len(reviews))
	}

	approved, err := bot.isPRApproved(context.Background(), 7)
	if err != nil {
		t.Fatalf("isPRApproved(7) error = %v", err)
	}
	if !approved {
		t.Fatal("isPRApproved(7) = false, want true")
	}

	approved, err = bot.isPRApproved(context.Background(), 8)
	if err != nil {
		t.Fatalf("isPRApproved(8) error = %v", err)
	}
	if approved {
		t.Fatal("isPRApproved(8) = true, want false")
	}

	_, err = bot.isPRApproved(context.Background(), 999)
	if err == nil {
		t.Fatal("isPRApproved(999) error = nil, want error")
	}
}

func TestHandleApprovedPRErrorPaths(t *testing.T) {
	newAgent := func(id string, pr int) *Agent {
		return &Agent{
			ID:                      id,
			Role:                    RoleCoder,
			IssueNumber:             pr,
			BranchName:              "repository-agent-orchestrator/issue-" + strconv.Itoa(pr),
			PRNumber:                pr,
			PRURL:                   "https://github.com/acme/widget/pull/" + strconv.Itoa(pr),
			WorktreePath:            filepath.Join(t.TempDir(), "worktree"),
			State:                   StateApproved,
			LastActivityTime:        time.Now(),
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		}
	}

	t.Run("returns nil when agent missing", func(t *testing.T) {
		bot := &Orchestrator{agents: NewAgentManager()}
		err := bot.handleApprovedPR(context.Background(), Agent{ID: "missing"})
		if err != nil {
			t.Fatalf("handleApprovedPR() error = %v", err)
		}
	})

	t.Run("returns nil when state already merging", func(t *testing.T) {
		agents := NewAgentManager()
		a := newAgent("agent-merge", 21)
		a.State = StateMerging
		if err := agents.Add(a); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		bot := &Orchestrator{agents: agents}
		snapshot, _ := agents.Get(a.ID)
		if err := bot.handleApprovedPR(context.Background(), snapshot); err != nil {
			t.Fatalf("handleApprovedPR() error = %v", err)
		}
	})

	t.Run("marks errored when merge API fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/acme/widget/pulls/21/merge" {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"message": "merge failed"})
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		agents := NewAgentManager()
		a := newAgent("agent-merge-fail", 21)
		if err := agents.Add(a); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
		}

		snapshot, _ := agents.Get(a.ID)
		err := bot.handleApprovedPR(context.Background(), snapshot)
		if err == nil {
			t.Fatal("handleApprovedPR() error = nil, want merge failure")
		}
		updated, _ := agents.Get(a.ID)
		if updated.State != StateErrored {
			t.Fatalf("state after merge failure = %s, want %s", updated.State, StateErrored)
		}
	})

	t.Run("marks errored when API reports not merged", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/acme/widget/pulls/22/merge" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"merged": false})
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		agents := NewAgentManager()
		a := newAgent("agent-not-merged", 22)
		if err := agents.Add(a); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
		}
		snapshot, _ := agents.Get(a.ID)
		err := bot.handleApprovedPR(context.Background(), snapshot)
		if err == nil || !strings.Contains(err.Error(), "pull request not merged") {
			t.Fatalf("handleApprovedPR() error = %v, want not merged error", err)
		}
		updated, _ := agents.Get(a.ID)
		if updated.State != StateErrored {
			t.Fatalf("state after not merged = %s, want %s", updated.State, StateErrored)
		}
	})

	t.Run("marks errored when cleanup fails after merge", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/acme/widget/pulls/23/merge" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"merged": true})
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		agents := NewAgentManager()
		a := newAgent("agent-cleanup-fail", 23)
		a.RuntimeHandle = RuntimeHandle{} // avoid runner dependency in stopRuntime
		if err := agents.Add(a); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		bot := &Orchestrator{
			cfg: Config{
				RepoOwner: "acme",
				RepoName:  "widget",
				RepoPath:  t.TempDir(),
			},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: &stubRunner{},
		}
		snapshot, _ := agents.Get(a.ID)
		err := bot.handleApprovedPR(context.Background(), snapshot)
		if err == nil {
			t.Fatal("handleApprovedPR() error = nil, want cleanup failure")
		}
		updated, _ := agents.Get(a.ID)
		if updated.State != StateErrored {
			t.Fatalf("state after cleanup failure = %s, want %s", updated.State, StateErrored)
		}
	})
}

func TestPollReviewAgentBranches(t *testing.T) {
	t.Run("skips non-reviewer and zero PR", func(t *testing.T) {
		bot := &Orchestrator{agents: NewAgentManager()}
		err := bot.pollReviewAgent(context.Background(), Agent{ID: "coder", Role: RoleCoder, PRNumber: 1})
		if err != nil {
			t.Fatalf("pollReviewAgent(coder) error = %v", err)
		}
		err = bot.pollReviewAgent(context.Background(), Agent{ID: "review", Role: RoleReviewer, PRNumber: 0})
		if err != nil {
			t.Fatalf("pollReviewAgent(pr=0) error = %v", err)
		}
	})

	t.Run("handles reviewer verdict comment", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/repos/acme/widget/pulls/66" {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"number": 66,
					"state":  "open",
					"head": map[string]any{
						"sha": testReviewHeadSHA,
					},
				})
				return
			}
			if r.URL.Path != "/repos/acme/widget/issues/66/comments" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 100, "body": "old baseline"},
				{
					"id": 101,
					"body": strings.Join([]string{
						"CODEX_AGENT_ID: review-agent-66-1",
						"CODEX_AGENT_ROLE: reviewer",
						"CODEX_REVIEWED_SHA: " + testReviewHeadSHA,
						"CODEX_VERDICT: NEEDS_CHANGES",
					}, "\n"),
					"html_url": "https://github.com/acme/widget/pull/66#issuecomment-101",
				},
			})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		coder := &Agent{
			ID:                      "coding-agent-66",
			Role:                    RoleCoder,
			IssueNumber:             66,
			BranchName:              "repository-agent-orchestrator/issue-66",
			PRNumber:                66,
			PRURL:                   "https://github.com/acme/widget/pull/66",
			State:                   StateWaiting,
			LastActivityTime:        time.Now(),
			RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-session"},
			ActiveReviewAgentID:     "review-agent-66-1",
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		}
		reviewer := &Agent{
			ID:                           "review-agent-66-1",
			Role:                         RoleReviewer,
			ParentAgentID:                coder.ID,
			IssueNumber:                  66,
			BranchName:                   coder.BranchName,
			PRNumber:                     66,
			PRURL:                        coder.PRURL,
			ObservedPRHeadSHA:            testReviewHeadSHA,
			ReviewBaselineIssueCommentID: 100,
			ReviewCycle:                  newWorkflowTestCycle(t, testReviewHeadSHA),
			State:                        StateWorking,
			LastActivityTime:             time.Now(),
			RuntimeHandle:                RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-session"},
			seenReviewCommentIDs:         make(map[int64]struct{}),
			seenIssueCommentIDs:          make(map[int64]struct{}),
			pendingReviewCommentIDs:      make(map[int64]struct{}),
		}
		if err := agents.Add(coder); err != nil {
			t.Fatalf("Add(coder) error = %v", err)
		}
		if err := agents.Add(reviewer); err != nil {
			t.Fatalf("Add(reviewer) error = %v", err)
		}

		runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "replacement-coder-session"}}
		bot := &Orchestrator{
			cfg: Config{
				RepoOwner:      "acme",
				RepoName:       "widget",
				RepoPath:       "/tmp/repo",
				WorktreeDir:    "/tmp/repo/.worktrees",
				MandatoryTests: []string{"make test"},
			},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: runner,
		}

		snapshot, ok := agents.Get(reviewer.ID)
		if !ok {
			t.Fatalf("Get(%s) = not found", reviewer.ID)
		}
		if err := bot.pollReviewAgent(context.Background(), snapshot); err != nil {
			t.Fatalf("pollReviewAgent() error = %v", err)
		}

		reviewerUpdated, _ := agents.Get(reviewer.ID)
		if reviewerUpdated.State != StateDone || !reviewerUpdated.Stopped {
			t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", reviewerUpdated.State, reviewerUpdated.Stopped, StateDone)
		}
		coderUpdated, _ := agents.Get(coder.ID)
		if coderUpdated.State != StateWorking {
			t.Fatalf("coder state = %s, want %s", coderUpdated.State, StateWorking)
		}
		if coderUpdated.LastReviewVerdict != ReviewVerdictNeedsChanges {
			t.Fatalf("LastReviewVerdict = %q, want %q", coderUpdated.LastReviewVerdict, ReviewVerdictNeedsChanges)
		}
		if got := len(runner.sent); got != 0 {
			t.Fatalf("runtime messages sent after restart = %d, want 0", got)
		}
		if got := len(runner.started); got != 1 {
			t.Fatalf("replacement coder runtimes = %d, want 1", got)
		}
		if prompt := runner.prompts[0]; !strings.Contains(prompt, "NEEDS_CHANGES") {
			t.Fatalf("replacement coder prompt = %q, want NEEDS_CHANGES", prompt)
		}
	})
}

func TestPollReviewAgentThumbsUpAsyncDoesNotBlockPollPath(t *testing.T) {
	stopStarted := make(chan struct{}, 1)
	releaseStop := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/issues/77/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 700, "body": "old baseline"},
				{
					"id": 701,
					"body": strings.Join([]string{
						"CODEX_AGENT_ID: review-agent-77-1",
						"CODEX_AGENT_ROLE: reviewer",
						"CODEX_REVIEWED_SHA: deadbeef77",
						"CODEX_VERDICT: THUMBS_UP",
					}, "\n"),
					"html_url": "https://github.com/acme/widget/pull/77#issuecomment-701",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/77":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 77,
				"draft":  true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "coding-agent-77",
		Role:                    RoleCoder,
		IssueNumber:             77,
		BranchName:              "repository-agent-orchestrator/issue-77",
		PRNumber:                77,
		PRURL:                   "https://github.com/acme/widget/pull/77",
		ObservedPRHeadSHA:       "deadbeef77",
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-77-session"},
		ActiveReviewAgentID:     "review-agent-77-1",
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                           "review-agent-77-1",
		Role:                         RoleReviewer,
		ParentAgentID:                coder.ID,
		IssueNumber:                  77,
		BranchName:                   coder.BranchName,
		PRNumber:                     77,
		PRURL:                        coder.PRURL,
		ObservedPRHeadSHA:            "deadbeef77",
		ReviewBaselineIssueCommentID: 700,
		State:                        StateWorking,
		LastActivityTime:             time.Now(),
		RuntimeHandle:                RuntimeHandle{Kind: RuntimeKindTmux, Session: "reviewer-77-session"},
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	runner.stopStarted = stopStarted
	runner.stopRelease = releaseStop
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			MandatoryTests: []string{},
		},
		github:                     newGitHubClientForTest(t, srv),
		agents:                     agents,
		runner:                     runner,
		asyncReviewVerdictHandling: true,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			if name == "gh" {
				return nil
			}
			return nil
		},
	}

	snapshot, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", reviewer.ID)
	}

	start := time.Now()
	if err := bot.pollReviewAgent(context.Background(), snapshot); err != nil {
		t.Fatalf("pollReviewAgent() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("pollReviewAgent() blocked for %s with async handling enabled", elapsed)
	}

	select {
	case <-stopStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async review verdict handling to start")
	}

	coderSnapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", coder.ID)
	}
	if err := bot.ensureReviewAgentForCoder(context.Background(), coderSnapshot); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("reviewer launches during in-flight verdict = %d, want 0", got)
	}

	duringGate, _ := agents.Get(coder.ID)
	if duringGate.State == StateApproved {
		t.Fatalf("coder state during async verdict handling = %s, want not approved yet", duringGate.State)
	}

	close(releaseStop)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		updated, _ := agents.Get(coder.ID)
		if updated.State == StateApproved {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	updated, _ := agents.Get(coder.ID)
	t.Fatalf("coder state after async verdict handling = %s, want %s", updated.State, StateApproved)
}
