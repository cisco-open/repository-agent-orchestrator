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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	startupCleanupTimeout      = 5 * time.Second
	startupCleanupPollInterval = 100 * time.Millisecond
)

func cleanStartupState(ctx context.Context, cfg Config) error {
	lockPath := filepath.Join(strings.TrimSpace(cfg.LogDir), orchestratorLockFileName)
	if err := terminateExistingOrchestratorProcess(lockPath); err != nil {
		return fmt.Errorf("failed to terminate existing Repository Agent Orchestrator process: %w", err)
	}
	if err := killOrchestratorTmuxSessions(cfg); err != nil {
		return fmt.Errorf("failed to stop Repository Agent Orchestrator tmux sessions: %w", err)
	}
	if err := cleanupOrchestratorWorktrees(ctx, cfg); err != nil {
		return fmt.Errorf("failed to cleanup Repository Agent Orchestrator worktrees: %w", err)
	}
	if err := deleteOrchestratorBranches(ctx, cfg.RepoPath); err != nil {
		return fmt.Errorf("failed to delete Repository Agent Orchestrator branches: %w", err)
	}
	worktreeDir := strings.TrimSpace(cfg.WorktreeDir)
	if worktreeDir != "" {
		if err := os.RemoveAll(worktreeDir); err != nil {
			return fmt.Errorf("failed to remove worktree dir %q: %w", worktreeDir, err)
		}
	}
	logDir := strings.TrimSpace(cfg.LogDir)
	if logDir != "" {
		if err := os.RemoveAll(logDir); err != nil {
			return fmt.Errorf("failed to remove log dir %q: %w", logDir, err)
		}
	}
	return nil
}

func terminateExistingOrchestratorProcess(lockPath string) error {
	lockPath = strings.TrimSpace(lockPath)
	if lockPath == "" {
		return nil
	}

	lock, err := acquireProcessLock(lockPath)
	if err == nil {
		return lock.Release()
	}
	if !strings.Contains(err.Error(), "already holds lock file") {
		return err
	}

	pid, pidErr := resolveLockHolderPID(lockPath)
	if pidErr != nil {
		return pidErr
	}
	if pid <= 0 || !processExists(pid) {
		return nil
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process %d for lock %q: %w", pid, lockPath, err)
	}
	if err := process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("failed to send SIGTERM to process %d: %w", pid, err)
	}
	if waitForUnlockedLock(lockPath, startupCleanupTimeout) {
		return nil
	}
	if err := process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("failed to send SIGKILL to process %d: %w", pid, err)
	}
	if waitForUnlockedLock(lockPath, startupCleanupTimeout) {
		return nil
	}
	return fmt.Errorf("process %d still holds lock %q after cleanup", pid, lockPath)
}

func resolveLockHolderPID(lockPath string) (int, error) {
	pid, err := readLockPID(lockPath)
	if err == nil && pid > 0 {
		return pid, nil
	}

	lsofPID, lsofErr := readLockPIDFromLsof(lockPath)
	if lsofErr == nil && lsofPID > 0 {
		return lsofPID, nil
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("failed to read lock holder pid from %q: %w", lockPath, err)
	}
	if lsofErr != nil && !errors.Is(lsofErr, exec.ErrNotFound) {
		return 0, fmt.Errorf("failed to discover lock holder pid for %q: %w", lockPath, lsofErr)
	}
	return 0, fmt.Errorf("could not determine lock holder pid for %q", lockPath)
}

func readLockPID(lockPath string) (int, error) {
	body, err := os.ReadFile(lockPath)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(body))
	if len(fields) == 0 {
		return 0, nil
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, fmt.Errorf("invalid pid %q", fields[0])
	}
	return pid, nil
}

func readLockPIDFromLsof(lockPath string) (int, error) {
	if _, err := exec.LookPath("lsof"); err != nil {
		return 0, err
	}
	cmd := newCommand("lsof", "-t", lockPath)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		pid, err := strconv.Atoi(line)
		if err == nil && pid > 0 {
			return pid, nil
		}
	}
	return 0, nil
}

func waitForUnlockedLock(lockPath string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lock, err := acquireProcessLock(lockPath)
		if err == nil {
			_ = lock.Release()
			return true
		}
		time.Sleep(startupCleanupPollInterval)
	}
	lock, err := acquireProcessLock(lockPath)
	if err == nil {
		_ = lock.Release()
		return true
	}
	return false
}

func processExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func killOrchestratorTmuxSessions(cfg Config) error {
	servers := []string{
		runtimeTmuxServerName(cfg.RepoOwner, cfg.RepoName, cfg.RepoPath),
		"", // Remove unsafe sessions created by releases before repository isolation.
	}
	for _, server := range servers {
		if err := killOrchestratorTmuxSessionsOnServer(cfg, server); err != nil {
			return err
		}
	}
	return nil
}

func listOrchestratorTmuxSessions(cfg Config) ([]string, error) {
	return listOrchestratorTmuxSessionsOnServer(
		cfg,
		runtimeTmuxServerName(cfg.RepoOwner, cfg.RepoName, cfg.RepoPath),
	)
}

func killLegacyOrchestratorTmuxSessions(cfg Config) error {
	return killOrchestratorTmuxSessionsOnServer(cfg, "")
}

func killOrchestratorTmuxSessionsOnServer(cfg Config, server string) error {
	sessions, err := listOrchestratorTmuxSessionsOnServer(cfg, server)
	if err != nil {
		return err
	}
	exec := osCommandExecutor{}
	for _, session := range sessions {
		args := tmuxServerArgs(server, "kill-session", "-t", session)
		if err := exec.Run("tmux", args...); err != nil && !isTmuxSessionMissing(err) {
			return fmt.Errorf(
				"failed to kill tmux session %q on server %q: %w",
				session,
				server,
				err,
			)
		}
	}
	return nil
}

func listOrchestratorTmuxSessionsOnServer(
	cfg Config,
	server string,
) ([]string, error) {
	exec := osCommandExecutor{}
	args := tmuxServerArgs(
		server,
		"list-panes",
		"-a",
		"-F",
		"#{session_name}\t#{pane_current_path}",
	)
	output, err := exec.Output("tmux", args...)
	if err != nil {
		if isTmuxSessionMissing(err) {
			return nil, nil
		}
		return nil, err
	}

	worktreeDir := strings.TrimSpace(cfg.WorktreeDir)
	repoPath := strings.TrimSpace(cfg.RepoPath)
	seen := make(map[string]struct{})
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		session := strings.TrimSpace(parts[0])
		panePath := ""
		if len(parts) == 2 {
			panePath = strings.TrimSpace(parts[1])
		}
		if !strings.HasPrefix(session, "repository-agent-orchestrator-") {
			continue
		}
		if !pathWithin(worktreeDir, panePath) && !pathWithin(repoPath, panePath) {
			continue
		}
		seen[session] = struct{}{}
	}

	sessions := make([]string, 0, len(seen))
	for session := range seen {
		sessions = append(sessions, session)
	}
	sort.Strings(sessions)
	return sessions, nil
}

func pathWithin(basePath, targetPath string) bool {
	basePath = strings.TrimSpace(basePath)
	targetPath = strings.TrimSpace(targetPath)
	if basePath == "" || targetPath == "" {
		return false
	}

	baseResolved, err := resolvedPath(basePath)
	if err != nil {
		return false
	}
	targetResolved, err := resolvedPath(targetPath)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(baseResolved, targetResolved)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func resolvedPath(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absPath)
	if err == nil {
		return resolved, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return filepath.Clean(absPath), nil
	}
	return filepath.Clean(absPath), nil
}

func cleanupOrchestratorWorktrees(ctx context.Context, cfg Config) error {
	repoPath := strings.TrimSpace(cfg.RepoPath)
	worktreeDir := strings.TrimSpace(cfg.WorktreeDir)
	if repoPath == "" || worktreeDir == "" {
		return nil
	}

	worktrees, err := listOrchestratorWorktrees(ctx, repoPath, worktreeDir)
	if err != nil {
		return err
	}
	for _, worktreePath := range worktrees {
		if err := runCommandWithOutput(ctx, repoPath, "git", "worktree", "remove", worktreePath, "--force"); err != nil {
			if removeErr := os.RemoveAll(worktreePath); removeErr != nil {
				return fmt.Errorf("failed to remove worktree %q after git worktree remove error: %w", worktreePath, removeErr)
			}
		}
	}
	if err := runCommandWithOutput(ctx, repoPath, "git", "worktree", "prune", "--expire", "now"); err != nil {
		return fmt.Errorf("failed to prune git worktrees: %w", err)
	}
	return nil
}

func listOrchestratorWorktrees(ctx context.Context, repoPath, worktreeDir string) ([]string, error) {
	output, err := outputCommand(ctx, repoPath, "git", "worktree", "list", "--porcelain")
	if err != nil {
		return nil, fmt.Errorf("failed to list git worktrees: %w", err)
	}

	worktrees := make([]string, 0)
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "worktree ") {
			continue
		}
		worktreePath := strings.TrimSpace(strings.TrimPrefix(line, "worktree "))
		if pathWithin(worktreeDir, worktreePath) {
			worktrees = append(worktrees, worktreePath)
		}
	}
	sort.Strings(worktrees)
	return worktrees, nil
}

func deleteOrchestratorBranches(ctx context.Context, repoPath string) error {
	repoPath = strings.TrimSpace(repoPath)
	if repoPath == "" {
		return nil
	}

	output, err := outputCommand(ctx, repoPath, "git", "for-each-ref", "--format=%(refname:short)", "refs/heads/repository-agent-orchestrator/")
	if err != nil {
		return fmt.Errorf("failed to list Repository Agent Orchestrator branches: %w", err)
	}
	for _, line := range strings.Split(string(output), "\n") {
		branch := strings.TrimSpace(line)
		if branch == "" {
			continue
		}
		if err := runCommandWithOutput(ctx, repoPath, "git", "branch", "-D", branch); err != nil {
			if fallbackErr := runCommandWithOutput(ctx, repoPath, "git", "update-ref", "-d", "refs/heads/"+branch); fallbackErr != nil {
				return fmt.Errorf("failed to delete Repository Agent Orchestrator branch %q: %w", branch, err)
			}
		}
	}
	return nil
}

func outputCommand(ctx context.Context, dir string, name string, args ...string) ([]byte, error) {
	cmd := newCommandContext(ctx, name, args...)
	cmd.Dir = dir
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("command failed: %s %s", name, strings.Join(args, " "))
	}
	return output, nil
}
