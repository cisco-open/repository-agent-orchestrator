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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()

	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git -C %s %s failed: %v\n%s", dir, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output)
}

func remoteBranchHead(t *testing.T, repoPath, branch string) string {
	t.Helper()

	output := strings.TrimSpace(runGit(t, repoPath, "ls-remote", "--heads", "origin", branch))
	fields := strings.Fields(output)
	if len(fields) < 1 {
		t.Fatalf("ls-remote output for %s is empty", branch)
	}
	return fields[0]
}

func setupRepoWithFeatureWorktree(t *testing.T, branch string) (repoPath, worktreePath string) {
	t.Helper()

	root := t.TempDir()
	repoPath = filepath.Join(root, "repo")
	originPath := filepath.Join(root, "origin.git")
	worktreePath = filepath.Join(root, "worktree")

	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(repo) error = %v", err)
	}

	runGit(t, repoPath, "init")
	runGit(t, repoPath, "config", "user.email", "tester@example.com")
	runGit(t, repoPath, "config", "user.name", "Tester")
	runGit(t, repoPath, "checkout", "-b", "main")

	readmePath := filepath.Join(repoPath, "README.md")
	if err := os.WriteFile(readmePath, []byte("base\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", readmePath, err)
	}
	runGit(t, repoPath, "add", "README.md")
	runGit(t, repoPath, "commit", "-m", "initial")

	runGit(t, root, "init", "--bare", originPath)
	runGit(t, repoPath, "remote", "add", "origin", originPath)
	runGit(t, repoPath, "push", "-u", "origin", "main")

	runGit(t, repoPath, "worktree", "add", "-b", branch, worktreePath, "main")
	if err := os.WriteFile(filepath.Join(worktreePath, "README.md"), []byte("base\nfeature\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(worktree README.md) error = %v", err)
	}
	runGit(t, worktreePath, "add", "README.md")
	runGit(t, worktreePath, "commit", "-m", "feature commit")
	runGit(t, worktreePath, "push", "-u", "origin", branch)

	return repoPath, worktreePath
}

func TestHandleTerminalPRCleansUpMergedAgent(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/issue-88"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)

	originPath := strings.TrimSpace(runGit(t, repoPath, "remote", "get-url", "origin"))
	cloneRoot := t.TempDir()
	upstreamClone := filepath.Join(cloneRoot, "upstream")
	runGit(t, cloneRoot, "clone", originPath, upstreamClone)
	runGit(t, upstreamClone, "config", "user.email", "tester@example.com")
	runGit(t, upstreamClone, "config", "user.name", "Tester")
	runGit(t, upstreamClone, "checkout", "main")
	if err := os.WriteFile(filepath.Join(upstreamClone, "UPSTREAM.md"), []byte("upstream update\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(upstream UPSTREAM.md) error = %v", err)
	}
	runGit(t, upstreamClone, "add", "UPSTREAM.md")
	runGit(t, upstreamClone, "commit", "-m", "upstream main update")
	runGit(t, upstreamClone, "push", "origin", "main")

	localMainBefore := strings.TrimSpace(runGit(t, repoPath, "rev-parse", "main"))
	remoteMainBefore := remoteBranchHead(t, repoPath, "main")
	if localMainBefore == remoteMainBefore {
		t.Fatalf("expected local main to be behind origin/main before cleanup; both are %s", localMainBefore)
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/pulls/88" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number": 88,
			"merged": true,
			"merged_by": map[string]any{
				"login": "alice",
			},
		})
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-88",
		Role:                    RoleCoder,
		IssueNumber:             88,
		BranchName:              branch,
		PRNumber:                88,
		PRURL:                   "https://github.com/acme/widget/pull/88",
		WorktreePath:            worktreePath,
		State:                   StateApproved,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:  "acme",
			RepoName:   "widget",
			RepoPath:   repoPath,
			BaseBranch: "main",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.handleTerminalPR(context.Background(), snapshot); err != nil {
		t.Fatalf("handleTerminalPR() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if len(runner.runtimeRemoved) != 1 ||
		runner.runtimeRemoved[0].ID != agent.ID {
		t.Fatalf(
			"terminal runtime cleanup = %#v, want agent %s",
			runner.runtimeRemoved,
			agent.ID,
		)
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

	localMainAfter := strings.TrimSpace(runGit(t, repoPath, "rev-parse", "main"))
	remoteMainAfter := remoteBranchHead(t, repoPath, "main")
	if localMainAfter == localMainBefore {
		t.Fatalf("local main should advance during cleanup, before=%s after=%s", localMainBefore, localMainAfter)
	}
	if localMainAfter != remoteMainAfter {
		t.Fatalf("expected local main to match origin/main after cleanup, local=%s remote=%s", localMainAfter, remoteMainAfter)
	}
	if _, err := os.Stat(filepath.Join(repoPath, "UPSTREAM.md")); err != nil {
		t.Fatalf("expected upstream file to be pulled into repo path, got err=%v", err)
	}
}

func TestCleanupTerminalPRAgentRemovesAdoptedAndIndexerRuntimeState(
	t *testing.T,
) {
	tests := []struct {
		name              string
		role              AgentRole
		adopted           bool
		wantRemoteDeleted bool
	}{
		{
			name:    "adopted PR",
			role:    RoleCoder,
			adopted: true,
		},
		{
			name:              "repo indexer",
			role:              RoleIndexer,
			wantRemoteDeleted: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			branch := "repository-agent-orchestrator/" +
				strings.ReplaceAll(test.name, " ", "-")
			repoPath, worktreePath := setupRepoWithFeatureWorktree(
				t,
				branch,
			)
			agent := &Agent{
				ID:           "terminal-" + strings.ReplaceAll(test.name, " ", "-"),
				Role:         test.role,
				BranchName:   branch,
				WorktreePath: worktreePath,
				AdoptedPR:    test.adopted,
				State:        StateApproved,
			}
			agents := NewAgentManager()
			if err := agents.Add(agent); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			runner := &stubRunner{}
			bot := &Orchestrator{
				cfg: Config{
					RepoPath:    repoPath,
					BaseBranch:  "main",
					WorktreeDir: filepath.Dir(worktreePath),
				},
				agents: agents,
				runner: runner,
			}

			if err := bot.cleanupTerminalPRAgent(
				context.Background(),
				*agent,
			); err != nil {
				t.Fatalf("cleanupTerminalPRAgent() error = %v", err)
			}
			if len(runner.runtimeRemoved) != 1 ||
				runner.runtimeRemoved[0].ID != agent.ID {
				t.Fatalf(
					"terminal runtime cleanup = %#v, want agent %s",
					runner.runtimeRemoved,
					agent.ID,
				)
			}
			remoteBranch := strings.TrimSpace(runGit(
				t,
				repoPath,
				"ls-remote",
				"--heads",
				"origin",
				branch,
			))
			if test.wantRemoteDeleted && remoteBranch != "" {
				t.Fatalf("remote branch %q was not deleted", branch)
			}
			if !test.wantRemoteDeleted && remoteBranch == "" {
				t.Fatalf("adopted remote branch %q was deleted", branch)
			}
		})
	}
}

func TestSyncBaseBranchUpdatesLocalRefWithoutSwitchingBranches(t *testing.T) {
	t.Parallel()

	repoPath, _ := setupRepoWithFeatureWorktree(t, "repository-agent-orchestrator/issue-120")
	runGit(t, repoPath, "checkout", "-b", "scratch")

	originPath := strings.TrimSpace(runGit(t, repoPath, "remote", "get-url", "origin"))
	cloneRoot := t.TempDir()
	upstreamClone := filepath.Join(cloneRoot, "upstream")
	runGit(t, cloneRoot, "clone", originPath, upstreamClone)
	runGit(t, upstreamClone, "config", "user.email", "tester@example.com")
	runGit(t, upstreamClone, "config", "user.name", "Tester")
	runGit(t, upstreamClone, "checkout", "main")
	if err := os.WriteFile(filepath.Join(upstreamClone, "SYNCED.md"), []byte("synced\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(upstream SYNCED.md) error = %v", err)
	}
	runGit(t, upstreamClone, "add", "SYNCED.md")
	runGit(t, upstreamClone, "commit", "-m", "sync main")
	runGit(t, upstreamClone, "push", "origin", "main")

	bot := &Orchestrator{
		cfg: Config{
			RepoPath:   repoPath,
			BaseBranch: "main",
		},
	}

	if err := bot.syncBaseBranch(context.Background()); err != nil {
		t.Fatalf("syncBaseBranch() error = %v", err)
	}

	currentBranch := strings.TrimSpace(runGit(t, repoPath, "branch", "--show-current"))
	if currentBranch != "scratch" {
		t.Fatalf("current branch = %q, want scratch", currentBranch)
	}

	localMain := strings.TrimSpace(runGit(t, repoPath, "rev-parse", "main"))
	remoteMain := remoteBranchHead(t, repoPath, "main")
	if localMain != remoteMain {
		t.Fatalf("local main = %s, want %s", localMain, remoteMain)
	}

	if _, err := os.Stat(filepath.Join(repoPath, "SYNCED.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected repo worktree to remain on scratch branch contents, got err=%v", err)
	}
}

func TestHandleTerminalPRSkipsWhenPROpen(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/pulls/99" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number": 99,
			"merged": false,
			"state":  "open",
		})
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	agent := &Agent{
		ID:               "agent-99",
		Role:             RoleCoder,
		IssueNumber:      99,
		BranchName:       "repository-agent-orchestrator/issue-99",
		PRNumber:         99,
		PRURL:            "https://github.com/acme/widget/pull/99",
		State:            StateApproved,
		LastActivityTime: time.Now(),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
			RepoPath:  t.TempDir(),
		},
		github: ghClient,
		agents: agents,
	}

	snapshot, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if err := bot.handleTerminalPR(context.Background(), snapshot); err != nil {
		t.Fatalf("handleTerminalPR() error = %v", err)
	}

	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	if updated.State != StateApproved || updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateApproved)
	}
}

func TestHandleTerminalPRCleansUpClosedCoderAndReviewer(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/issue-144"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)
	reviewWorktreePath := filepath.Join(filepath.Dir(worktreePath), "review-worktree")
	runGit(t, repoPath, "worktree", "add", "--detach", reviewWorktreePath, "HEAD")

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/pulls/144" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number": 144,
			"merged": false,
			"state":  "closed",
		})
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-144",
		Role:                    RoleCoder,
		IssueNumber:             144,
		BranchName:              branch,
		PRNumber:                144,
		PRURL:                   "https://github.com/acme/widget/pull/144",
		WorktreePath:            worktreePath,
		State:                   StateWaiting,
		ActiveReviewAgentID:     "review-agent-144-1",
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "review-agent-144-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             144,
		BranchName:              branch,
		PRNumber:                144,
		PRURL:                   coder.PRURL,
		WorktreePath:            reviewWorktreePath,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:  "acme",
			RepoName:   "widget",
			RepoPath:   repoPath,
			BaseBranch: "main",
		},
		github: ghClient,
		agents: agents,
		runner: &stubRunner{},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("agent %s not found", coder.ID)
	}
	if err := bot.handleTerminalPR(context.Background(), snapshot); err != nil {
		t.Fatalf("handleTerminalPR() error = %v", err)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updatedCoder.State != StateDone || !updatedCoder.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, true)", updatedCoder.State, updatedCoder.Stopped, StateDone)
	}
	if updatedCoder.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", updatedCoder.ActiveReviewAgentID)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateDone || !updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updatedReviewer.State, updatedReviewer.Stopped, StateDone)
	}

	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("coder worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewWorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review worktree path should be removed, got err=%v", err)
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

func TestHandleTerminalPRPreservesCompletedReviewerHandoff(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/issue-146"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)

	logDir := t.TempDir()
	handoffPath := filepath.Join(orchestratorStateDir(logDir), "handoffs.json")
	if err := os.MkdirAll(filepath.Dir(handoffPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(handoff dir) error = %v", err)
	}
	handoffBody, err := json.Marshal([]persistedAgentHandoff{
		{AgentID: "review-agent-146-1", IssueNumber: 146, PRNumber: 146, Summary: "Reviewer summary"},
		{AgentID: "agent-keep", IssueNumber: 999, Summary: "Keep me"},
	})
	if err != nil {
		t.Fatalf("json.Marshal(handoffs) error = %v", err)
	}
	if err := os.WriteFile(handoffPath, handoffBody, 0o644); err != nil {
		t.Fatalf("WriteFile(handoffs.json) error = %v", err)
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/pulls/146" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number": 146,
			"merged": false,
			"state":  "closed",
		})
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-146",
		Role:                    RoleCoder,
		IssueNumber:             146,
		BranchName:              branch,
		PRNumber:                146,
		PRURL:                   "https://github.com/acme/widget/pull/146",
		WorktreePath:            worktreePath,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                   "review-agent-146-1",
		Role:                 RoleReviewer,
		ParentAgentID:        coder.ID,
		IssueNumber:          146,
		BranchName:           branch,
		PRNumber:             146,
		PRURL:                coder.PRURL,
		State:                StateDone,
		Stopped:              true,
		HandoffCaptured:      true,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:  "acme",
			RepoName:   "widget",
			RepoPath:   repoPath,
			LogDir:     logDir,
			BaseBranch: "main",
		},
		github: ghClient,
		agents: agents,
		runner: &stubRunner{},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("agent %s not found", coder.ID)
	}
	if err := bot.handleTerminalPR(context.Background(), snapshot); err != nil {
		t.Fatalf("handleTerminalPR() error = %v", err)
	}

	updatedHandoffBody, err := os.ReadFile(handoffPath)
	if err != nil {
		t.Fatalf("ReadFile(handoffs.json) error = %v", err)
	}
	var updatedHandoffs []persistedAgentHandoff
	if err := json.Unmarshal(updatedHandoffBody, &updatedHandoffs); err != nil {
		t.Fatalf("json.Unmarshal(handoffs.json) error = %v", err)
	}
	if len(updatedHandoffs) != 2 {
		t.Fatalf("len(updatedHandoffs) = %d, want 2", len(updatedHandoffs))
	}
	foundReviewer := false
	for _, handoff := range updatedHandoffs {
		if handoff.AgentID == reviewer.ID {
			foundReviewer = true
			if handoff.Summary != "Reviewer summary" {
				t.Fatalf("reviewer handoff summary = %q, want preserved summary", handoff.Summary)
			}
		}
	}
	if !foundReviewer {
		t.Fatalf("reviewer handoff missing after terminal cleanup: %#v", updatedHandoffs)
	}
}

func TestHandleTerminalPRCleansUpClosedCoderAndPausedReviewer(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/issue-145"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)
	reviewWorktreePath := filepath.Join(filepath.Dir(worktreePath), "review-worktree")
	runGit(t, repoPath, "worktree", "add", "--detach", reviewWorktreePath, "HEAD")

	logDir := t.TempDir()
	reviewerLogPath := filepath.Join(logDir, "reviewer.log")
	if err := os.WriteFile(reviewerLogPath, []byte("reviewer log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(reviewer log) error = %v", err)
	}

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/pulls/145" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number": 145,
			"merged": false,
			"state":  "closed",
		})
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-145",
		Role:                    RoleCoder,
		IssueNumber:             145,
		BranchName:              branch,
		PRNumber:                145,
		PRURL:                   "https://github.com/acme/widget/pull/145",
		WorktreePath:            worktreePath,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "review-agent-145-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             145,
		BranchName:              branch,
		PRNumber:                145,
		PRURL:                   coder.PRURL,
		WorktreePath:            reviewWorktreePath,
		ObservedPRHeadSHA:       "aaaaaaaaaaaa",
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, LogPath: reviewerLogPath},
		LogDir:                  logDir,
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
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:  "acme",
			RepoName:   "widget",
			RepoPath:   repoPath,
			BaseBranch: "main",
		},
		github: ghClient,
		agents: agents,
		runner: &stubRunner{},
	}

	snapshot, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("agent %s not found", coder.ID)
	}
	if err := bot.handleTerminalPR(context.Background(), snapshot); err != nil {
		t.Fatalf("handleTerminalPR() error = %v", err)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updatedCoder.State != StateDone || !updatedCoder.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, true)", updatedCoder.State, updatedCoder.Stopped, StateDone)
	}
	if updatedCoder.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", updatedCoder.ActiveReviewAgentID)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateDone || !updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updatedReviewer.State, updatedReviewer.Stopped, StateDone)
	}

	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("coder worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewWorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewerLogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer log should be removed, got err=%v", err)
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

func TestCleanupAgentClosesOpenPRAndRemovesArtifacts(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/issue-244"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)
	reviewWorktreePath := filepath.Join(filepath.Dir(worktreePath), "review-worktree")
	runGit(t, repoPath, "worktree", "add", "--detach", reviewWorktreePath, "HEAD")

	logDir := t.TempDir()
	coderLogPath := filepath.Join(logDir, "coder.log")
	reviewerLogPath := filepath.Join(logDir, "reviewer.log")
	if err := os.WriteFile(coderLogPath, []byte("coder log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(coder log) error = %v", err)
	}
	if err := os.WriteFile(reviewerLogPath, []byte("reviewer log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(reviewer log) error = %v", err)
	}

	handoffPath := filepath.Join(orchestratorStateDir(logDir), "handoffs.json")
	if err := os.MkdirAll(filepath.Dir(handoffPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(handoff dir) error = %v", err)
	}
	handoffBody, err := json.Marshal([]persistedAgentHandoff{
		{AgentID: "agent-244", IssueNumber: 244},
		{AgentID: "review-agent-244-1", IssueNumber: 244},
		{AgentID: "agent-keep", IssueNumber: 999},
	})
	if err != nil {
		t.Fatalf("json.Marshal(handoffs) error = %v", err)
	}
	if err := os.WriteFile(handoffPath, handoffBody, 0o644); err != nil {
		t.Fatalf("WriteFile(handoffs.json) error = %v", err)
	}

	closeCalls := 0
	var closedStates []string
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/pulls/244" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 244,
				"merged": false,
				"state":  "open",
			})
		case http.MethodPatch:
			payload := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("Decode(close PR payload) error = %v", err)
			}
			state, _ := payload["state"].(string)
			closedStates = append(closedStates, state)
			closeCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 244,
				"merged": false,
				"state":  "closed",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-244",
		Role:                    RoleCoder,
		IssueNumber:             244,
		BranchName:              branch,
		PRNumber:                244,
		PRURL:                   "https://github.com/acme/widget/pull/244",
		WorktreePath:            worktreePath,
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-session", LogPath: coderLogPath},
		State:                   StateWaiting,
		ActiveReviewAgentID:     "review-agent-244-1",
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "review-agent-244-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             244,
		BranchName:              branch,
		PRNumber:                244,
		PRURL:                   coder.PRURL,
		WorktreePath:            reviewWorktreePath,
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-session", LogPath: reviewerLogPath},
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:  "acme",
			RepoName:   "widget",
			RepoPath:   repoPath,
			LogDir:     logDir,
			BaseBranch: "main",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	if err := bot.CleanupAgent(context.Background(), coder.ID); err != nil {
		t.Fatalf("CleanupAgent() error = %v", err)
	}

	if closeCalls != 1 {
		t.Fatalf("close PR calls = %d, want 1", closeCalls)
	}
	if len(closedStates) != 1 || closedStates[0] != "closed" {
		t.Fatalf("closed PR states = %#v, want [closed]", closedStates)
	}
	if got := len(runner.stopped); got != 2 {
		t.Fatalf("runner.Stop calls = %d, want 2", got)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updatedCoder.State != StateDone || !updatedCoder.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, true)", updatedCoder.State, updatedCoder.Stopped, StateDone)
	}
	if updatedCoder.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", updatedCoder.ActiveReviewAgentID)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateDone || !updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updatedReviewer.State, updatedReviewer.Stopped, StateDone)
	}

	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("coder worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewWorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(coderLogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("coder log should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewerLogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer log should be removed, got err=%v", err)
	}

	localBranch := strings.TrimSpace(runGit(t, repoPath, "branch", "--list", branch))
	if localBranch != "" {
		t.Fatalf("local branch %q should be deleted, output=%q", branch, localBranch)
	}
	remoteBranch := strings.TrimSpace(runGit(t, repoPath, "ls-remote", "--heads", "origin", branch))
	if remoteBranch != "" {
		t.Fatalf("remote branch %q should be deleted, output=%q", branch, remoteBranch)
	}

	updatedHandoffBody, err := os.ReadFile(handoffPath)
	if err != nil {
		t.Fatalf("ReadFile(handoffs.json) error = %v", err)
	}
	var updatedHandoffs []persistedAgentHandoff
	if err := json.Unmarshal(updatedHandoffBody, &updatedHandoffs); err != nil {
		t.Fatalf("json.Unmarshal(handoffs.json) error = %v", err)
	}
	if len(updatedHandoffs) != 1 || updatedHandoffs[0].AgentID != "agent-keep" {
		t.Fatalf("handoffs after cleanup = %#v, want only agent-keep", updatedHandoffs)
	}
}

func TestCleanupAgentCleansPausedReviewerWithoutActiveLink(t *testing.T) {
	t.Parallel()

	branch := "repository-agent-orchestrator/issue-245"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)
	reviewWorktreePath := filepath.Join(filepath.Dir(worktreePath), "review-worktree")
	runGit(t, repoPath, "worktree", "add", "--detach", reviewWorktreePath, "HEAD")

	logDir := t.TempDir()
	coderLogPath := filepath.Join(logDir, "coder.log")
	reviewerLogPath := filepath.Join(logDir, "reviewer.log")
	if err := os.WriteFile(coderLogPath, []byte("coder log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(coder log) error = %v", err)
	}
	if err := os.WriteFile(reviewerLogPath, []byte("reviewer log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(reviewer log) error = %v", err)
	}

	closeCalls := 0
	var closedStates []string
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/pulls/245" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 245,
				"merged": false,
				"state":  "open",
			})
		case http.MethodPatch:
			payload := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("Decode(close PR payload) error = %v", err)
			}
			state, _ := payload["state"].(string)
			closedStates = append(closedStates, state)
			closeCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 245,
				"merged": false,
				"state":  "closed",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	coder := &Agent{
		ID:                      "agent-245",
		Role:                    RoleCoder,
		IssueNumber:             245,
		BranchName:              branch,
		PRNumber:                245,
		PRURL:                   "https://github.com/acme/widget/pull/245",
		WorktreePath:            worktreePath,
		LogDir:                  logDir,
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-session", LogPath: coderLogPath},
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	reviewer := &Agent{
		ID:                      "review-agent-245-1",
		Role:                    RoleReviewer,
		ParentAgentID:           coder.ID,
		IssueNumber:             245,
		BranchName:              branch,
		PRNumber:                245,
		PRURL:                   coder.PRURL,
		WorktreePath:            reviewWorktreePath,
		ObservedPRHeadSHA:       "aaaaaaaaaaaa",
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, LogPath: reviewerLogPath},
		LogDir:                  logDir,
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
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:  "acme",
			RepoName:   "widget",
			RepoPath:   repoPath,
			LogDir:     logDir,
			BaseBranch: "main",
		},
		github: ghClient,
		agents: agents,
		runner: runner,
	}

	if err := bot.CleanupAgent(context.Background(), coder.ID); err != nil {
		t.Fatalf("CleanupAgent() error = %v", err)
	}

	if closeCalls != 1 {
		t.Fatalf("close PR calls = %d, want 1", closeCalls)
	}
	if len(closedStates) != 1 || closedStates[0] != "closed" {
		t.Fatalf("closed PR states = %#v, want [closed]", closedStates)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if updatedCoder.State != StateDone || !updatedCoder.Stopped {
		t.Fatalf("coder state = (%s, stopped=%v), want (%s, true)", updatedCoder.State, updatedCoder.Stopped, StateDone)
	}
	if updatedCoder.ActiveReviewAgentID != "" {
		t.Fatalf("ActiveReviewAgentID = %q, want empty", updatedCoder.ActiveReviewAgentID)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateDone || !updatedReviewer.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updatedReviewer.State, updatedReviewer.Stopped, StateDone)
	}

	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("coder worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewWorktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review worktree path should be removed, got err=%v", err)
	}
	if _, err := os.Stat(coderLogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("coder log should be removed, got err=%v", err)
	}
	if _, err := os.Stat(reviewerLogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer log should be removed, got err=%v", err)
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
