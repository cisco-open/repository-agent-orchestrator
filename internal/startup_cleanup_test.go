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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupRepoForStartupCleanup(t *testing.T) (repoPath, worktreeDir, worktreePath, branch string) {
	t.Helper()

	repoPath = filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", repoPath, err)
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

	worktreeDir = filepath.Join(repoPath, ".worktrees")
	worktreePath = filepath.Join(worktreeDir, "issue-9-123")
	branch = "repository-agent-orchestrator/issue-9"
	runGit(t, repoPath, "worktree", "add", "-b", branch, worktreePath, "main")
	return repoPath, worktreeDir, worktreePath, branch
}

func withTestStdin(t *testing.T, input string) func() {
	t.Helper()

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(stdin) error = %v", err)
	}
	if _, err := io.WriteString(inW, input); err != nil {
		t.Fatalf("stdin write error = %v", err)
	}
	if err := inW.Close(); err != nil {
		t.Fatalf("stdin close error = %v", err)
	}

	oldStdin := os.Stdin
	os.Stdin = inR
	return func() {
		os.Stdin = oldStdin
		_ = inR.Close()
	}
}

func TestCleanStartupStateRemovesRepoSessionsWorktreesAndLogs(t *testing.T) {
	toolDir := t.TempDir()
	tmuxLog := filepath.Join(t.TempDir(), "tmux.log")
	writeExecutable(t, toolDir, "tmux", "#!/bin/sh\nif [ -n \"$TMUX_CALLS_LOG\" ]; then\n  printf '%s\\n' \"$*\" >> \"$TMUX_CALLS_LOG\"\nfi\nif [ \"$1\" = \"-L\" ]; then\n  shift 2\nfi\ncase \"$1\" in\n  list-panes)\n    printf '%s' \"$TMUX_LIST_PANES_OUTPUT\"\n    ;;\n  kill-session)\n    exit 0\n    ;;\n  *)\n    exit 0\n    ;;\nesac\n")
	prependPATH(t, toolDir)

	repoPath, worktreeDir, worktreePath, branch := setupRepoForStartupCleanup(t)
	logDir := filepath.Join(t.TempDir(), "logs")
	stateDir := filepath.Join(logDir, orchestratorStateDirName)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", stateDir, err)
	}
	statePath := filepath.Join(stateDir, orchestratorStateFileName)
	if err := os.WriteFile(statePath, []byte(`{"version":1,"agents":[]}`), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", statePath, err)
	}
	staleLogPath := filepath.Join(logDir, "stale-runtime.log")
	if err := os.WriteFile(staleLogPath, []byte("stale\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", staleLogPath, err)
	}

	outsidePath := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(outsidePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", outsidePath, err)
	}
	t.Setenv("TMUX_CALLS_LOG", tmuxLog)
	t.Setenv("TMUX_LIST_PANES_OUTPUT", strings.Join([]string{
		"repository-agent-orchestrator-coding-agent-9\t" + worktreePath,
		"repository-agent-orchestrator-unrelated\t" + outsidePath,
		"other-session\t" + worktreePath,
		"",
	}, "\n"))

	cfg := Config{
		RepoOwner:   "acme",
		RepoName:    "widget",
		RepoPath:    repoPath,
		WorktreeDir: worktreeDir,
		LogDir:      logDir,
	}
	if err := cleanStartupState(context.Background(), cfg); err != nil {
		t.Fatalf("cleanStartupState() error = %v", err)
	}

	if _, err := os.Stat(worktreePath); !os.IsNotExist(err) {
		t.Fatalf("worktree path still exists after cleanup: err=%v", err)
	}
	if _, err := os.Stat(logDir); !os.IsNotExist(err) {
		t.Fatalf("log dir still exists after cleanup: err=%v", err)
	}
	if got := strings.TrimSpace(runGit(t, repoPath, "branch", "--list", branch)); got != "" {
		t.Fatalf("branch list = %q, want branch removed", got)
	}
	if got := runGit(t, repoPath, "worktree", "list", "--porcelain"); strings.Contains(got, worktreePath) {
		t.Fatalf("worktree list still contains removed path %q: %q", worktreePath, got)
	}

	tmuxCalls, err := os.ReadFile(tmuxLog)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", tmuxLog, err)
	}
	callText := string(tmuxCalls)
	if !strings.Contains(callText, "kill-session -t repository-agent-orchestrator-coding-agent-9") {
		t.Fatalf("tmux calls = %q, want repo session kill", callText)
	}
	server := runtimeTmuxServerName(cfg.RepoOwner, cfg.RepoName, cfg.RepoPath)
	if !strings.Contains(callText, "-L "+server+" list-panes") ||
		!strings.Contains(
			callText,
			"-L "+server+
				" kill-session -t repository-agent-orchestrator-coding-agent-9",
		) {
		t.Fatalf(
			"tmux calls = %q, want repository-specific server %q",
			callText,
			server,
		)
	}
	if strings.Contains(callText, "kill-session -t repository-agent-orchestrator-unrelated") {
		t.Fatalf("tmux calls = %q, want unrelated session preserved", callText)
	}
}

func TestRunCleanKillsExistingLockHolder(t *testing.T) {
	toolDir := installRuntimeToolShims(t, true)
	prependPATH(t, toolDir)
	t.Setenv("CODEX_CMD", testAgentRuntimeCommand(toolDir))
	t.Setenv("FAKE_GH_TOKEN", "token-run")
	t.Setenv("WEBEX_WEBHOOK_URL", "https://example.test/webhook")

	repoName := fmt.Sprintf("widget-clean-%d", time.Now().UnixNano())
	repoPath := initGitRepoWithOrigin(t, "acme", repoName)
	configPath := writeRepoConfig(t, fmt.Sprintf("REPO_OWNER: acme\nREPO_NAME: %s\nREPO_PATH: %s\nMANDATORY_TESTS:\n  - git status\n", repoName, repoPath))

	logDir, err := repoScopedLogDir(repoName)
	if err != nil {
		t.Fatalf("repoScopedLogDir() error = %v", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", logDir, err)
	}
	lockPath := filepath.Join(logDir, orchestratorLockFileName)
	readyPath := filepath.Join(t.TempDir(), "ready")

	cmd := exec.Command(os.Args[0], "-test.run=TestProcessLockHelper", "--", lockPath, readyPath)
	cmd.Env = append(os.Environ(), processLockHelperEnv+"=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start helper process: %v", err)
	}
	defer func() {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper did not signal readiness within timeout")
		}
		time.Sleep(25 * time.Millisecond)
	}

	restoreStdin := withTestStdin(t, "exit\n")
	defer restoreStdin()

	stdout, stderr, code := captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", "--clean", "--config", configPath})
	})
	if code != 0 {
		t.Fatalf("Run(--clean) code = %d, want 0\nstdout=%q\nstderr=%q", code, stdout, stderr)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("Run(--clean) stderr = %q, want empty", stderr)
	}
	if strings.Contains(stdout, "startup lock failed") {
		t.Fatalf("Run(--clean) stdout unexpectedly contains startup lock failure: %q", stdout)
	}

	err = cmd.Wait()
	if err == nil {
		t.Fatal("helper process exited cleanly, want termination by cleanup")
	}
	if !strings.Contains(err.Error(), "signal: terminated") && !strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("helper wait error = %v, want terminated/killed signal", err)
	}
}
