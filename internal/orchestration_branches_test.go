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
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type taskFailMessenger struct {
	sendTaskErr error
}

func (m *taskFailMessenger) SendMessage(agent Agent, text string) error {
	return nil
}

func (m *taskFailMessenger) SendTask(agent Agent, markdown string) error {
	return m.sendTaskErr
}

func (m *taskFailMessenger) SendContext(agent Agent, markdown string) error {
	return nil
}

func (m *taskFailMessenger) SendHandoff(agent Agent, content string) error {
	return nil
}

func singleAgentState(t *testing.T, agents *AgentManager) Agent {
	t.Helper()
	items := agents.List()
	if len(items) != 1 {
		t.Fatalf("len(agents.List()) = %d, want 1", len(items))
	}
	return items[0]
}

func TestInitAgentForIssueErrorBranches(t *testing.T) {
	t.Run("returns error when active issue already exists", func(t *testing.T) {
		agents := NewAgentManager()
		a := testAgent("coding-agent-50", 50)
		if err := agents.Add(a); err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		bot := &Orchestrator{agents: agents}
		err := bot.InitAgentForIssue(context.Background(), 50)
		if err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("InitAgentForIssue() error = %v, want active issue error", err)
		}
	})

	t.Run("returns fetch error from GitHub", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "boom"})
		}))
		defer srv.Close()

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: NewAgentManager(),
		}

		err := bot.InitAgentForIssue(context.Background(), 51)
		if err == nil || !strings.Contains(err.Error(), "failed to fetch issue #51") {
			t.Fatalf("InitAgentForIssue() error = %v, want fetch error", err)
		}
	})

	t.Run("rejects pull request issues", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/repos/acme/widget/issues/52" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 52,
				"title":  "Already a pull request",
				"pull_request": map[string]any{
					"url": "https://api.github.com/repos/acme/widget/pulls/52",
				},
			})
		}))
		defer srv.Close()

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: NewAgentManager(),
		}

		err := bot.InitAgentForIssue(context.Background(), 52)
		if err == nil || !strings.Contains(err.Error(), "is a pull request") {
			t.Fatalf("InitAgentForIssue() error = %v, want pull request error", err)
		}
	})

	t.Run("marks agent errored when worktree preparation fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/repos/acme/widget/issues/53" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 53,
				"title":  "Prep failure",
				"body":   "Trigger worktree prep failure.",
			})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		bot := &Orchestrator{
			cfg: Config{
				RepoOwner:   "acme",
				RepoName:    "widget",
				WorktreeDir: t.TempDir(),
			},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: &stubRunner{},
		}

		err := bot.InitAgentForIssue(context.Background(), 53)
		if err == nil || !strings.Contains(err.Error(), "failed to prepare worktree") {
			t.Fatalf("InitAgentForIssue() error = %v, want worktree prep failure", err)
		}

		updated := singleAgentState(t, agents)
		if updated.State != StateErrored || updated.Stopped {
			t.Fatalf("agent state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateErrored)
		}
	})

	t.Run("marks agent errored when task write fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/repos/acme/widget/issues/54" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 54,
				"title":  "Task write failure",
				"body":   "Trigger task write failure.",
			})
		}))
		defer srv.Close()

		agents := NewAgentManager()
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
			github:    newGitHubClientForTest(t, srv),
			agents:    agents,
			runner:    runner,
			messenger: &taskFailMessenger{sendTaskErr: errors.New("task write failed")},
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error { return nil },
		}

		err := bot.InitAgentForIssue(context.Background(), 54)
		if err == nil || !strings.Contains(err.Error(), "failed to write task file") {
			t.Fatalf("InitAgentForIssue() error = %v, want task write failure", err)
		}

		updated := singleAgentState(t, agents)
		if updated.State != StateErrored || updated.Stopped {
			t.Fatalf("agent state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateErrored)
		}
		if got := len(runner.started); got != 0 {
			t.Fatalf("runner.Start calls = %d, want 0", got)
		}
	})

	t.Run("marks agent errored when runtime launch fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/repos/acme/widget/issues/55" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 55,
				"title":  "Runtime failure",
				"body":   "Trigger runtime failure.",
			})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		bot := &Orchestrator{
			cfg: Config{
				RepoOwner:   "acme",
				RepoName:    "widget",
				RepoPath:    "/tmp/repo",
				WorktreeDir: t.TempDir(),
				BaseBranch:  "main",
			},
			github:    newGitHubClientForTest(t, srv),
			agents:    agents,
			runner:    &stubRunner{startErr: errors.New("runtime launch failed")},
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error { return nil },
		}

		err := bot.InitAgentForIssue(context.Background(), 55)
		if err == nil || !strings.Contains(err.Error(), "failed to launch codex runtime") {
			t.Fatalf("InitAgentForIssue() error = %v, want runtime launch failure", err)
		}

		updated := singleAgentState(t, agents)
		if updated.State != StateErrored || updated.Stopped {
			t.Fatalf("agent state = (%s, stopped=%v), want (%s, false)", updated.State, updated.Stopped, StateErrored)
		}
	})
}

func TestLaunchRepoIndexAgentErrorBranches(t *testing.T) {
	t.Run("requires agent manager", func(t *testing.T) {
		bot := &Orchestrator{}
		if _, err := bot.LaunchRepoIndexAgent(context.Background()); err == nil || err.Error() != "agent manager is not configured" {
			t.Fatalf("LaunchRepoIndexAgent() error = %v, want agent manager error", err)
		}
	})

	t.Run("rejects duplicate active repo indexing agent", func(t *testing.T) {
		agents := NewAgentManager()
		indexer := &Agent{
			ID:                   "index-agent-1",
			Role:                 RoleIndexer,
			BranchName:           "repository-agent-orchestrator/repo-index-1",
			State:                StateWorking,
			LastActivityTime:     time.Now(),
			seenReviewCommentIDs: make(map[int64]struct{}),
			seenIssueCommentIDs:  make(map[int64]struct{}),
		}
		if err := agents.Add(indexer); err != nil {
			t.Fatalf("Add(indexer) error = %v", err)
		}

		bot := &Orchestrator{agents: agents}
		if _, err := bot.LaunchRepoIndexAgent(context.Background()); err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("LaunchRepoIndexAgent() error = %v, want duplicate indexer error", err)
		}
	})
}

func TestCleanupWorktreeFallsBackToRemovingManagedPath(t *testing.T) {
	t.Parallel()

	repoPath := t.TempDir()
	worktreeDir := filepath.Join(t.TempDir(), "worktrees")
	worktreePath := filepath.Join(worktreeDir, "issue-71")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", worktreePath, err)
	}
	if err := os.WriteFile(filepath.Join(worktreePath, "stale.txt"), []byte("stale\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(stale.txt) error = %v", err)
	}

	var commands []string
	bot := &Orchestrator{
		cfg: Config{
			RepoPath:    repoPath,
			WorktreeDir: worktreeDir,
		},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			commands = append(commands, dir+"::"+name+" "+strings.Join(args, " "))
			if name == "git" && len(args) >= 3 && args[0] == "worktree" && args[1] == "remove" {
				return errors.New("worktree busy")
			}
			return nil
		},
	}

	if err := bot.cleanupWorktree(context.Background(), worktreePath, "repository-agent-orchestrator/issue-71"); err != nil {
		t.Fatalf("cleanupWorktree() error = %v", err)
	}

	if _, err := os.Stat(worktreePath); !os.IsNotExist(err) {
		t.Fatalf("worktree path still exists after fallback cleanup: err=%v", err)
	}
	if got := len(commands); got != 3 {
		t.Fatalf("len(commands) = %d, want 3", got)
	}
	if !strings.Contains(commands[0], "git worktree remove "+worktreePath+" --force") {
		t.Fatalf("first command = %q, want git worktree remove", commands[0])
	}
	if !strings.Contains(commands[1], "git worktree prune --expire now") {
		t.Fatalf("second command = %q, want git worktree prune", commands[1])
	}
	if !strings.Contains(commands[2], "git branch -D repository-agent-orchestrator/issue-71") {
		t.Fatalf("third command = %q, want git branch delete", commands[2])
	}
}

// TestCleanupWorktreeRepairsFalsePositiveGitRemoveSuccess is the
// regression test for the disk-leak bug where a review coordinator
// retires cleanly, with no error logged anywhere, while a review worker's
// worktree remains on disk indefinitely. `git worktree remove --force`
// (and its own fallback os.RemoveAll) can report success while the
// directory is, in fact, still present -- e.g. a mandatory test left a
// mount point, loop device, or another process's open file handle
// somewhere underneath it -- and prior to this fix, cleanupWorktree
// trusted that reported success without ever checking. This simulates
// exactly that: `git worktree remove` reports success via the stubbed
// cmdRunner, but never actually touches the filesystem, leaving the
// directory present; cleanupWorktree must detect that and repair it
// rather than silently returning success.
func TestCleanupWorktreeRepairsFalsePositiveGitRemoveSuccess(t *testing.T) {
	t.Parallel()

	repoPath := t.TempDir()
	worktreeDir := filepath.Join(t.TempDir(), "worktrees")
	worktreePath := filepath.Join(worktreeDir, "issue-72")
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", worktreePath, err)
	}
	if err := os.WriteFile(filepath.Join(worktreePath, "leftover.txt"), []byte("leftover\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(leftover.txt) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoPath:    repoPath,
			WorktreeDir: worktreeDir,
		},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			// Reports success for every git command without touching the
			// filesystem at all -- the worktree directory is untouched by
			// design, standing in for a partial/false-positive removal.
			return nil
		},
	}

	if err := bot.cleanupWorktree(context.Background(), worktreePath, ""); err != nil {
		t.Fatalf("cleanupWorktree() error = %v, want it to detect and repair the leftover directory", err)
	}
	if _, err := os.Stat(worktreePath); !os.IsNotExist(err) {
		t.Fatalf("worktree path still exists after cleanupWorktree() reported success: err=%v", err)
	}
}

// TestCleanupWorktreeReturnsErrorWhenLeftoverDirectoryCannotBeRemoved
// covers the other half: if the leftover directory genuinely cannot be
// removed even by the repair attempt (e.g. a busy mount point), that must
// surface as a real, loud error rather than being silently swallowed the
// way the false-positive git exit code was before this fix.
func TestCleanupWorktreeReturnsErrorWhenLeftoverDirectoryCannotBeRemoved(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses the permission-based block this test relies on")
	}
	t.Parallel()

	repoPath := t.TempDir()
	worktreeDir := filepath.Join(t.TempDir(), "worktrees")
	worktreePath := filepath.Join(worktreeDir, "issue-73")
	blockedDir := filepath.Join(worktreePath, "blocked")
	if err := os.MkdirAll(blockedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", blockedDir, err)
	}
	if err := os.WriteFile(filepath.Join(blockedDir, "stuck.txt"), []byte("stuck\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(stuck.txt) error = %v", err)
	}
	// Stripping all permissions from the parent prevents os.RemoveAll from
	// being able to list or remove its contents, standing in for a
	// directory kept busy by something outside the orchestrator's control
	// (a mount point, an open file handle from a still-running process).
	if err := os.Chmod(worktreePath, 0o000); err != nil {
		t.Fatalf("Chmod(%s) error = %v", worktreePath, err)
	}
	defer os.Chmod(worktreePath, 0o755)

	bot := &Orchestrator{
		cfg: Config{
			RepoPath:    repoPath,
			WorktreeDir: worktreeDir,
		},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	err := bot.cleanupWorktree(context.Background(), worktreePath, "")
	if err == nil {
		t.Fatal("cleanupWorktree() error = nil, want a loud failure for the undeletable leftover directory")
	}
	if !strings.Contains(err.Error(), worktreePath) {
		t.Fatalf("cleanupWorktree() error = %v, want it to name the leftover path", err)
	}
}

// TestExecCommandSurfacesStderrOnFailure confirms execCommand's default
// path (runCommandWithOutput) surfaces a failed command's stdout/stderr
// through to the caller -- not just "command failed: git fetch ..." with
// no hint whether that was a credentials problem, a network problem, or
// something else entirely. This is exactly the information a human needs
// to fix a `git` subprocess that failed fast on a disabled interactive
// credential/passphrase prompt (GIT_TERMINAL_PROMPT=0 / BatchMode=yes).
func TestExecCommandSurfacesStderrOnFailure(t *testing.T) {
	bot := &Orchestrator{}
	err := bot.execCommand(context.Background(), "", "sh", "-c", "echo permission denied to stderr>&2; exit 1")
	if err == nil {
		t.Fatal("execCommand() error = nil, want a failure")
	}
	if !strings.Contains(err.Error(), "permission denied to stderr") {
		t.Fatalf("execCommand() error = %q, want it to include the captured stderr", err.Error())
	}
}

func TestExecCommandBoundsGitWithoutADeadline(t *testing.T) {
	var gotDeadline time.Time
	var gotOK bool
	bot := &Orchestrator{
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			gotDeadline, gotOK = ctx.Deadline()
			return nil
		},
	}

	// A caller-supplied context with no deadline of its own -- exactly
	// what a synchronous REPL command handler passes in -- must not let a
	// wedged `git` subprocess (e.g. blocked on an unanswerable SSH
	// passphrase or credential prompt) hang forever.
	if err := bot.execCommand(context.Background(), "/tmp/repo", "git", "fetch"); err != nil {
		t.Fatalf("execCommand() error = %v", err)
	}
	if !gotOK {
		t.Fatal("execCommand() did not impose a deadline on a git subprocess")
	}
	remaining := time.Until(gotDeadline)
	if remaining <= 0 || remaining > gitCommandTimeout {
		t.Fatalf("execCommand() git deadline = %s from now, want (0, %s]", remaining, gitCommandTimeout)
	}
}

func TestExecCommandDoesNotBoundNonGitCommands(t *testing.T) {
	var gotOK bool
	bot := &Orchestrator{
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			_, gotOK = ctx.Deadline()
			return nil
		},
	}

	if err := bot.execCommand(context.Background(), "/tmp/repo", "make", "test"); err != nil {
		t.Fatalf("execCommand() error = %v", err)
	}
	if gotOK {
		t.Fatal("execCommand() imposed a deadline on a non-git command")
	}
}

func TestPrepareCoderWorktreeBranches(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		bot := &Orchestrator{}
		if err := bot.prepareCoderWorktree(context.Background(), Agent{WorktreePath: "/tmp/worktree", BranchName: "repository-agent-orchestrator/issue-1"}); err == nil || err.Error() != "repo path is empty" {
			t.Fatalf("prepareCoderWorktree() error = %v, want repo path error", err)
		}

		bot.cfg.RepoPath = "/tmp/repo"
		if err := bot.prepareCoderWorktree(context.Background(), Agent{BranchName: "repository-agent-orchestrator/issue-1"}); err == nil || err.Error() != "agent worktree path is empty" {
			t.Fatalf("prepareCoderWorktree() error = %v, want worktree path error", err)
		}

		if err := bot.prepareCoderWorktree(context.Background(), Agent{WorktreePath: "/tmp/worktree"}); err == nil || err.Error() != "agent branch name is empty" {
			t.Fatalf("prepareCoderWorktree() error = %v, want branch error", err)
		}
	})

	t.Run("returns fetch failure", func(t *testing.T) {
		bot := &Orchestrator{
			cfg: Config{RepoPath: "/tmp/repo", BaseBranch: "main"},
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				return errors.New("fetch failed")
			},
		}
		err := bot.prepareCoderWorktree(context.Background(), Agent{WorktreePath: "/tmp/worktree", BranchName: "repository-agent-orchestrator/issue-1"})
		if err == nil || !strings.Contains(err.Error(), "git fetch failed for base branch main") {
			t.Fatalf("prepareCoderWorktree() error = %v, want fetch failure", err)
		}
	})

	t.Run("returns worktree add failure", func(t *testing.T) {
		call := 0
		bot := &Orchestrator{
			cfg: Config{RepoPath: "/tmp/repo", BaseBranch: "main"},
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				call++
				if call == 2 {
					return errors.New("worktree add failed")
				}
				return nil
			},
		}
		err := bot.prepareCoderWorktree(context.Background(), Agent{WorktreePath: "/tmp/worktree", BranchName: "repository-agent-orchestrator/issue-1"})
		if err == nil || !strings.Contains(err.Error(), "git worktree add failed for /tmp/worktree") {
			t.Fatalf("prepareCoderWorktree() error = %v, want worktree add failure", err)
		}
	})
}

func TestPrepareReviewerWorktreeBranches(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		bot := &Orchestrator{}
		if err := bot.prepareReviewerWorktree(context.Background(), Agent{WorktreePath: "/tmp/worktree", ObservedPRHeadSHA: "abc"}); err == nil || err.Error() != "repo path is empty" {
			t.Fatalf("prepareReviewerWorktree() error = %v, want repo path error", err)
		}

		bot.cfg.RepoPath = "/tmp/repo"
		if err := bot.prepareReviewerWorktree(context.Background(), Agent{ObservedPRHeadSHA: "abc"}); err == nil || err.Error() != "reviewer worktree path is empty" {
			t.Fatalf("prepareReviewerWorktree() error = %v, want worktree path error", err)
		}

		if err := bot.prepareReviewerWorktree(context.Background(), Agent{WorktreePath: "/tmp/worktree"}); err == nil || err.Error() != "reviewed head sha is empty" {
			t.Fatalf("prepareReviewerWorktree() error = %v, want head sha error", err)
		}
	})

	t.Run("returns branch fetch failure", func(t *testing.T) {
		bot := &Orchestrator{
			cfg: Config{RepoPath: "/tmp/repo"},
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				return errors.New("fetch failed")
			},
		}
		err := bot.prepareReviewerWorktree(context.Background(), Agent{
			WorktreePath:      "/tmp/worktree",
			ObservedPRHeadSHA: "abc123",
			BranchName:        "repository-agent-orchestrator/issue-7",
		})
		if err == nil || !strings.Contains(err.Error(), "git fetch failed for review branch repository-agent-orchestrator/issue-7") {
			t.Fatalf("prepareReviewerWorktree() error = %v, want fetch failure", err)
		}
	})

	t.Run("skips branch fetch when branch is empty", func(t *testing.T) {
		commands := make([]string, 0)
		bot := &Orchestrator{
			cfg: Config{RepoPath: "/tmp/repo"},
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				commands = append(commands, name+" "+strings.Join(args, " "))
				return nil
			},
		}
		err := bot.prepareReviewerWorktree(context.Background(), Agent{
			WorktreePath:      "/tmp/worktree",
			ObservedPRHeadSHA: "abc123",
		})
		if err != nil {
			t.Fatalf("prepareReviewerWorktree() error = %v", err)
		}
		if got := len(commands); got != 1 {
			t.Fatalf("command count = %d, want 1", got)
		}
		if !strings.Contains(commands[0], "git worktree add --detach /tmp/worktree abc123") {
			t.Fatalf("command = %q, want detached worktree add", commands[0])
		}
	})
}

func TestGitWorktreeMutationFailurePreservesSanitizedOutputAndExitCause(t *testing.T) {
	const secret = "git-worktree-secret"
	fakeBin := t.TempDir()
	gitPath := filepath.Join(fakeBin, "git")
	script := "#!/bin/sh\n" +
		"printf 'worktree stdout " + secret + "\\n'\n" +
		"printf 'worktree stderr " + secret + "\\n' >&2\n" +
		"exit 23\n"
	if err := os.WriteFile(gitPath, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile(fake git) error = %v", err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	bot := &Orchestrator{
		cfg: Config{
			RepoPath:    t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		token: secret,
	}
	err := bot.prepareReviewerWorktree(
		context.Background(),
		Agent{
			WorktreePath:      filepath.Join(bot.cfg.WorktreeDir, "owned"),
			ObservedPRHeadSHA: strings.Repeat("a", 40),
		},
	)
	if err == nil {
		t.Fatal("prepareReviewerWorktree() error = nil, want injected Git failure")
	}
	for _, want := range []string{
		"Cause: exit status 23",
		"stdout:",
		"worktree stdout [REDACTED]",
		"stderr:",
		"worktree stderr [REDACTED]",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("prepareReviewerWorktree() error missing %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("prepareReviewerWorktree() leaked configured secret: %v", err)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("prepareReviewerWorktree() exit cause = %#v, want exit code 23", exitErr)
	}
}

func TestGitWorktreeMetadataMutationsAreSerialized(t *testing.T) {
	worktreeDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			RepoPath:    t.TempDir(),
			WorktreeDir: worktreeDir,
		},
	}
	var mu sync.Mutex
	active := 0
	maximum := 0
	adds := 0
	removes := 0
	prunes := 0
	bot.cmdRunner = func(
		_ context.Context,
		_ string,
		name string,
		args ...string,
	) error {
		if name != "git" || len(args) < 2 || args[0] != "worktree" {
			return nil
		}
		mu.Lock()
		active++
		if active > maximum {
			maximum = active
		}
		switch args[1] {
		case "add":
			adds++
		case "remove":
			removes++
		case "prune":
			prunes++
		}
		mu.Unlock()
		time.Sleep(15 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		if args[1] == "remove" {
			return errors.New("unregistered owned path")
		}
		return nil
	}

	const parallel = 8
	start := make(chan struct{})
	errCh := make(chan error, parallel)
	var wait sync.WaitGroup
	for index := 0; index < parallel; index++ {
		index := index
		path := filepath.Join(worktreeDir, fmt.Sprintf("owned-%d", index))
		if index%2 != 0 {
			if err := os.Mkdir(path, 0o755); err != nil {
				t.Fatalf("Mkdir(%s) error = %v", path, err)
			}
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if index%2 == 0 {
				errCh <- bot.prepareReviewerWorktree(
					context.Background(),
					Agent{
						WorktreePath:      path,
						ObservedPRHeadSHA: strings.Repeat("b", 40),
					},
				)
				return
			}
			errCh <- bot.cleanupWorktree(context.Background(), path, "")
		}()
	}
	close(start)
	wait.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("serialized mutation error = %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if maximum != 1 {
		t.Fatalf("maximum concurrent Git worktree mutations = %d, want 1", maximum)
	}
	if adds != parallel/2 || removes != parallel/2 || prunes != parallel/2 {
		t.Fatalf("mutation counts add/remove/prune = %d/%d/%d", adds, removes, prunes)
	}
}

func TestGitWorktreeMutationWaitHonorsContextCancellation(t *testing.T) {
	bot := &Orchestrator{}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- bot.runGitWorktreeMutation(context.Background(), func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runCalled := false
	err := bot.runGitWorktreeMutation(ctx, func() error {
		runCalled = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runGitWorktreeMutation() error = %v, want context cancellation", err)
	}
	if runCalled {
		t.Fatal("queued worktree mutation ran after its context was cancelled")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first runGitWorktreeMutation() error = %v", err)
	}
}

func TestLaunchReviewAgentValidationBranches(t *testing.T) {
	t.Run("requires agent manager", func(t *testing.T) {
		var bot *Orchestrator
		err := bot.LaunchReviewAgent(context.Background(), 54)
		if err == nil || err.Error() != "agent manager is not configured" {
			t.Fatalf("LaunchReviewAgent() error = %v, want agent manager error", err)
		}
	})

	t.Run("returns validation errors before launch", func(t *testing.T) {
		agents := NewAgentManager()
		activeReviewerCoder := &Agent{
			ID:                  "coder-1",
			Role:                RoleCoder,
			PRNumber:            4,
			State:               StateWorking,
			ActiveReviewAgentID: "active-reviewer",
			LastActivityTime:    time.Now(),
		}
		reviewCycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
		if err != nil {
			t.Fatalf("newReviewCycleState() error = %v", err)
		}
		activeReviewer := &Agent{
			ID:                "active-reviewer",
			Role:              RoleReviewer,
			PRNumber:          4,
			ObservedPRHeadSHA: testReviewHeadSHA,
			ReviewCycle:       reviewCycle,
			State:             StateInitializing,
			LastActivityTime:  time.Now(),
		}
		for _, agent := range []*Agent{activeReviewerCoder, activeReviewer} {
			agent.BranchName = "repository-agent-orchestrator/issue-1"
			agent.seenReviewCommentIDs = make(map[int64]struct{})
			agent.seenIssueCommentIDs = make(map[int64]struct{})
			agent.pendingReviewCommentIDs = make(map[int64]struct{})
			if err := agents.Add(agent); err != nil {
				t.Fatalf("Add(%s) error = %v", agent.ID, err)
			}
		}

		bot := &Orchestrator{agents: agents}
		cases := []struct {
			prNumber int
			want     string
		}{
			{prNumber: 0, want: "invalid PR number"},
			{prNumber: 4, want: "review agent already active"},
		}
		for _, tc := range cases {
			err := bot.LaunchReviewAgent(context.Background(), tc.prNumber)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LaunchReviewAgent(%d) error = %v, want %q", tc.prNumber, err, tc.want)
			}
		}
	})

	t.Run("returns head lookup errors", func(t *testing.T) {
		newCoder := func(id string, pr int) *Agent {
			return &Agent{
				ID:                      id,
				Role:                    RoleCoder,
				IssueNumber:             pr,
				BranchName:              "repository-agent-orchestrator/issue-1",
				PRNumber:                pr,
				State:                   StateWorking,
				LastActivityTime:        time.Now(),
				seenReviewCommentIDs:    make(map[int64]struct{}),
				seenIssueCommentIDs:     make(map[int64]struct{}),
				pendingReviewCommentIDs: make(map[int64]struct{}),
			}
		}

		t.Run("getPRHeadSHA failure", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"message": "boom"})
			}))
			defer srv.Close()

			agents := NewAgentManager()
			coder := newCoder("coder-head-error", 10)
			if err := agents.Add(coder); err != nil {
				t.Fatalf("Add() error = %v", err)
			}

			bot := &Orchestrator{
				cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
				github: newGitHubClientForTest(t, srv),
				agents: agents,
			}
			err := bot.LaunchReviewAgent(context.Background(), coder.PRNumber)
			if err == nil || !strings.Contains(err.Error(), "failed to determine PR head") {
				t.Fatalf("LaunchReviewAgent() error = %v, want head lookup error", err)
			}
		})

		t.Run("empty PR head sha", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"number": 11, "head": map[string]any{}})
			}))
			defer srv.Close()

			agents := NewAgentManager()
			coder := newCoder("coder-empty-head", 11)
			if err := agents.Add(coder); err != nil {
				t.Fatalf("Add() error = %v", err)
			}

			bot := &Orchestrator{
				cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
				github: newGitHubClientForTest(t, srv),
				agents: agents,
			}
			err := bot.LaunchReviewAgent(context.Background(), coder.PRNumber)
			if err == nil || !strings.Contains(err.Error(), "pull request head sha is empty") {
				t.Fatalf("LaunchReviewAgent() error = %v, want empty head error", err)
			}
		})

		t.Run("duplicate pending review launch", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"number": 12,
					"head": map[string]any{
						"sha": "deadbeef12",
					},
				})
			}))
			defer srv.Close()

			agents := NewAgentManager()
			coder := newCoder("coder-pending", 12)
			if err := agents.Add(coder); err != nil {
				t.Fatalf("Add() error = %v", err)
			}

			bot := &Orchestrator{
				cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
				github: newGitHubClientForTest(t, srv),
				agents: agents,
			}
			launchKey := reviewLaunchKeyForPR(coder.PRNumber)
			if !bot.reserveReviewLaunch(launchKey, "deadbeef12") {
				t.Fatal("reserveReviewLaunch() = false, want true")
			}
			defer bot.releaseReviewLaunch(launchKey, "deadbeef12")

			err := bot.LaunchReviewAgent(context.Background(), coder.PRNumber)
			if err == nil || !strings.Contains(err.Error(), "review launch already pending") {
				t.Fatalf("LaunchReviewAgent() error = %v, want pending launch error", err)
			}
		})
	})
}

func TestHandlePRMergeConflictsBranches(t *testing.T) {
	t.Run("clears remembered conflict when PR is clean", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          61,
				"mergeable_state": "clean",
				"head": map[string]any{
					"sha": "cleanhead61",
				},
			})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		agent := &Agent{
			ID:                      "agent-clean-61",
			Role:                    RoleCoder,
			IssueNumber:             61,
			BranchName:              "repository-agent-orchestrator/issue-61",
			PRNumber:                61,
			LastConflictHeadSHA:     "old-conflict",
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		}
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
		}

		snapshot, _ := agents.Get(agent.ID)
		if err := bot.handlePRMergeConflicts(context.Background(), snapshot); err != nil {
			t.Fatalf("handlePRMergeConflicts() error = %v", err)
		}

		updated, _ := agents.Get(agent.ID)
		if updated.LastConflictHeadSHA != "" {
			t.Fatalf("LastConflictHeadSHA = %q, want empty", updated.LastConflictHeadSHA)
		}
		if updated.ObservedPRHeadSHA != "cleanhead61" {
			t.Fatalf("ObservedPRHeadSHA = %q, want %q", updated.ObservedPRHeadSHA, "cleanhead61")
		}
	})

	t.Run("suppresses duplicate guidance for same dirty head", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          62,
				"mergeable_state": "dirty",
				"head": map[string]any{
					"sha": "dirtyhead62",
				},
			})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		agent := &Agent{
			ID:                      "agent-dirty-62",
			Role:                    RoleCoder,
			IssueNumber:             62,
			BranchName:              "repository-agent-orchestrator/issue-62",
			PRNumber:                62,
			LastConflictHeadSHA:     "dirtyhead62",
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
			RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-62"},
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		}
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		runner := &stubRunner{}
		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: runner,
		}

		snapshot, _ := agents.Get(agent.ID)
		if err := bot.handlePRMergeConflicts(context.Background(), snapshot); err != nil {
			t.Fatalf("handlePRMergeConflicts() error = %v", err)
		}
		if got := len(runner.sent); got != 0 {
			t.Fatalf("runner.Send calls = %d, want 0", got)
		}
	})

	t.Run("returns resume error when runtime cannot be restarted", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          63,
				"mergeable_state": "dirty",
				"head": map[string]any{
					"sha": "dirtyhead63",
				},
			})
		}))
		defer srv.Close()

		agents := NewAgentManager()
		agent := &Agent{
			ID:                      "agent-dirty-63",
			Role:                    RoleCoder,
			IssueNumber:             63,
			BranchName:              "repository-agent-orchestrator/issue-63",
			PRNumber:                63,
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
			RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "dead-session"},
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		}
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		runner := &stubRunner{aliveSet: true, alive: false, startErr: errors.New("restart failed")}
		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			agents: agents,
			runner: runner,
		}

		snapshot, _ := agents.Get(agent.ID)
		err := bot.handlePRMergeConflicts(context.Background(), snapshot)
		if err == nil || !strings.Contains(err.Error(), "failed to resume runtime for conflict handling") {
			t.Fatalf("handlePRMergeConflicts() error = %v, want resume failure", err)
		}
	})

	t.Run("returns inbox forwarding error before runtime send", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":          64,
				"mergeable_state": "dirty",
				"head": map[string]any{
					"sha": "dirtyhead64",
				},
			})
		}))
		defer srv.Close()

		worktree := t.TempDir()
		if err := os.Mkdir(filepath.Join(worktree, ".git"), 0o755); err != nil {
			t.Fatalf("Mkdir(.git) error = %v", err)
		}

		agents := NewAgentManager()
		agent := &Agent{
			ID:                      "agent-dirty-64",
			Role:                    RoleCoder,
			IssueNumber:             64,
			BranchName:              "repository-agent-orchestrator/issue-64",
			PRNumber:                64,
			PRURL:                   "https://github.com/acme/widget/pull/64",
			WorktreePath:            worktree,
			State:                   StateWorking,
			LastActivityTime:        time.Now(),
			RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "coder-64"},
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		}
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add() error = %v", err)
		}

		runner := &stubRunner{}
		messenger := &stubMessenger{sendErr: errors.New("inbox failed")}
		bot := &Orchestrator{
			cfg:       Config{RepoOwner: "acme", RepoName: "widget"},
			github:    newGitHubClientForTest(t, srv),
			agents:    agents,
			runner:    runner,
			messenger: messenger,
		}

		snapshot, _ := agents.Get(agent.ID)
		err := bot.handlePRMergeConflicts(context.Background(), snapshot)
		if err == nil || !strings.Contains(err.Error(), "failed to forward conflict guidance to inbox") {
			t.Fatalf("handlePRMergeConflicts() error = %v, want inbox forwarding error", err)
		}
		if got := len(runner.sent); got != 0 {
			t.Fatalf("runner.Send calls = %d, want 0", got)
		}
	})
}
