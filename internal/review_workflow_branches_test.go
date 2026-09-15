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
	"testing"
	"time"
)

func TestEnsureReviewAgentForCoderEarlyReturns(t *testing.T) {
	t.Run("reviewer role is ignored", func(t *testing.T) {
		bot := &Orchestrator{agents: NewAgentManager()}
		err := bot.ensureReviewAgentForCoder(context.Background(), Agent{Role: RoleReviewer})
		if err != nil {
			t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
		}
	})

	t.Run("missing coder in manager", func(t *testing.T) {
		bot := &Orchestrator{agents: NewAgentManager()}
		err := bot.ensureReviewAgentForCoder(context.Background(), Agent{ID: "missing", Role: RoleCoder})
		if err != nil {
			t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
		}
	})

	t.Run("coder without PR is ignored", func(t *testing.T) {
		agents := NewAgentManager()
		coder := &Agent{ID: "coder-no-pr", Role: RoleCoder, IssueNumber: 1, State: StateWorking, LastActivityTime: time.Now()}
		if err := agents.Add(coder); err != nil {
			t.Fatalf("Add(coder) error = %v", err)
		}
		bot := &Orchestrator{agents: agents}
		snapshot, _ := agents.Get(coder.ID)
		if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
			t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
		}
	})

	t.Run("coder with active reviewer is ignored", func(t *testing.T) {
		agents := NewAgentManager()
		coder := &Agent{
			ID:                  "coder-active-review",
			Role:                RoleCoder,
			IssueNumber:         2,
			PRNumber:            2,
			ObservedPRHeadSHA:   "aaaaaaaaaaaa",
			State:               StateWorking,
			ActiveReviewAgentID: "review-2",
			LastActivityTime:    time.Now(),
		}
		reviewer := &Agent{
			ID:                "review-2",
			Role:              RoleReviewer,
			ParentAgentID:     coder.ID,
			PRNumber:          2,
			ObservedPRHeadSHA: "aaaaaaaaaaaa",
			State:             StateWorking,
			LastActivityTime:  time.Now(),
			RuntimeHandle: RuntimeHandle{
				Kind:    RuntimeKindTmux,
				Session: "review-2-session",
			},
		}
		if err := agents.Add(coder); err != nil {
			t.Fatalf("Add(coder) error = %v", err)
		}
		if err := agents.Add(reviewer); err != nil {
			t.Fatalf("Add(reviewer) error = %v", err)
		}
		runner := &stubRunner{}
		bot := &Orchestrator{agents: agents, runner: runner}
		snapshot, _ := agents.Get(coder.ID)
		if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
			t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
		}
		if got := len(runner.started); got != 0 {
			t.Fatalf("runner.Start calls = %d, want 0", got)
		}
	})

	t.Run("reviewer without runtime does not block when worktree is gone", func(t *testing.T) {
		if reviewerCanBlockLaunch(Agent{Role: RoleReviewer, State: StateInitializing}) {
			t.Fatal("reviewerCanBlockLaunch() = true, want false for missing runtime and worktree")
		}
		if !reviewerCanBlockLaunch(Agent{
			Role:        RoleReviewer,
			State:       StateInitializing,
			ReviewCycle: &ReviewCycleState{},
		}) {
			t.Fatal("reviewerCanBlockLaunch() = false, want checkpointed launch to retain ownership")
		}

		worktree := t.TempDir()
		gitMarker := worktree + "/.git"
		if err := os.WriteFile(gitMarker, []byte("gitdir: /tmp/example\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", gitMarker, err)
		}
		if !reviewerCanBlockLaunch(Agent{Role: RoleReviewer, State: StateInitializing, WorktreePath: worktree}) {
			t.Fatal("reviewerCanBlockLaunch() = false, want true when initializing worktree still exists")
		}
	})

	t.Run("empty fetched head SHA does not launch reviewer", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/repos/acme/widget/pulls/5" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 5, "head": map[string]any{}})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		coder := &Agent{
			ID:               "coder-empty-head",
			Role:             RoleCoder,
			IssueNumber:      5,
			IssueTitle:       "Coverage",
			BranchName:       "repository-agent-orchestrator/issue-5",
			PRNumber:         5,
			State:            StateWaiting,
			LastActivityTime: time.Now(),
		}
		if err := agents.Add(coder); err != nil {
			t.Fatalf("Add(coder) error = %v", err)
		}

		runner := &stubRunner{}
		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget", WorktreeDir: t.TempDir()},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: runner,
		}
		snapshot, _ := agents.Get(coder.ID)
		if err := bot.ensureReviewAgentForCoder(context.Background(), snapshot); err != nil {
			t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
		}
		if got := len(runner.started); got != 0 {
			t.Fatalf("runner.Start calls = %d, want 0", got)
		}
	})
}

func TestStartReviewAgentErrorBranches(t *testing.T) {
	t.Run("fails when runner is nil", func(t *testing.T) {
		bot := &Orchestrator{}
		err := bot.startReviewAgent(context.Background(), Agent{ID: "coder-1"}, "abc")
		if err == nil || err.Error() != "runner is not configured" {
			t.Fatalf("startReviewAgent() error = %v, want runner not configured", err)
		}
	})

	t.Run("returns error when baseline comment lookup fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "boom"})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		coder := &Agent{
			ID:                "coder-baseline-fail",
			Role:              RoleCoder,
			IssueNumber:       7,
			IssueTitle:        "Coverage",
			BranchName:        "repository-agent-orchestrator/issue-7",
			PRNumber:          7,
			PRURL:             "https://github.com/acme/widget/pull/7",
			ObservedPRHeadSHA: testReviewHeadSHA,
			State:             StateWorking,
			LastActivityTime:  time.Now(),
		}
		if err := agents.Add(coder); err != nil {
			t.Fatalf("Add(coder) error = %v", err)
		}

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget", WorktreeDir: t.TempDir(), RepoPath: "/tmp/repo"},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: &stubRunner{},
		}
		snapshot, _ := agents.Get(coder.ID)
		err := bot.startReviewAgent(context.Background(), snapshot, testReviewHeadSHA)
		if err == nil {
			t.Fatalf("startReviewAgent() error = %v, want lookup failure", err)
		}
	})

}
