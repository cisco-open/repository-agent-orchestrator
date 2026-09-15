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
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunEndToEndWithQuit(t *testing.T) {
	toolDir := installRuntimeToolShims(t, true)
	prependPATH(t, toolDir)
	t.Setenv("CODEX_CMD", testAgentRuntimeCommand(toolDir))
	t.Setenv("FAKE_GH_TOKEN", "token-run")
	t.Setenv("WEBEX_WEBHOOK_URL", "https://example.test/webhook")
	t.Setenv("POLL_INTERVAL_SECONDS", "1")
	t.Setenv("BASE_BRANCH", "main")
	t.Setenv("WORKTREE_DIR", "")

	repoName := fmt.Sprintf("widget-run-%d", time.Now().UnixNano())
	repoPath := initGitRepoWithOrigin(t, "acme", repoName)
	configPath := writeRepoConfig(t, fmt.Sprintf("REPO_OWNER: acme\nREPO_NAME: %s\nREPO_PATH: %s\nMANDATORY_TESTS:\n  - git status\n", repoName, repoPath))

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(stdin) error = %v", err)
	}
	input := strings.Join([]string{"status", "agent list", "exit", ""}, "\n")
	if _, err := io.WriteString(inW, input); err != nil {
		t.Fatalf("stdin write error = %v", err)
	}
	if err := inW.Close(); err != nil {
		t.Fatalf("stdin close error = %v", err)
	}
	oldStdin := os.Stdin
	os.Stdin = inR
	defer func() {
		os.Stdin = oldStdin
		_ = inR.Close()
	}()

	oldLogWriter := log.Writer()
	oldLogFlags := log.Flags()
	defer log.SetOutput(oldLogWriter)
	defer log.SetFlags(oldLogFlags)

	stdout, stderr, code := captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", "--config", configPath})
	})

	if code != 0 {
		t.Fatalf("Run() code = %d, want 0\nstdout=%q\nstderr=%q", code, stdout, stderr)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("Run() stderr = %q, want empty", stderr)
	}
	for _, needle := range []string{"Available commands:", "repo: acme/" + repoName, "No agents."} {
		if !strings.Contains(stdout, needle) {
			t.Fatalf("Run() stdout missing %q: %q", needle, stdout)
		}
	}
}

func TestRunFailsWhenStartupLockHeld(t *testing.T) {
	toolDir := installRuntimeToolShims(t, true)
	prependPATH(t, toolDir)
	t.Setenv("CODEX_CMD", testAgentRuntimeCommand(toolDir))
	t.Setenv("FAKE_GH_TOKEN", "token-run")
	t.Setenv("WEBEX_WEBHOOK_URL", "https://example.test/webhook")

	repoName := fmt.Sprintf("widget-lock-%d", time.Now().UnixNano())
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
	lock, err := acquireProcessLock(lockPath)
	if err != nil {
		t.Fatalf("acquireProcessLock(%s) error = %v", lockPath, err)
	}
	defer func() { _ = lock.Release() }()

	_, stderr, code := captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", "--config", configPath})
	})
	if code != 1 {
		t.Fatalf("Run() code = %d, want 1", code)
	}
	if !strings.Contains(stderr, "startup lock failed") {
		t.Fatalf("Run() stderr = %q, want startup lock failure", stderr)
	}
}

func TestRunExitsWhenParentContextIsCanceled(t *testing.T) {
	toolDir := installRuntimeToolShims(t, true)
	prependPATH(t, toolDir)
	t.Setenv("CODEX_CMD", testAgentRuntimeCommand(toolDir))
	t.Setenv("FAKE_GH_TOKEN", "token-run")
	t.Setenv("WEBEX_WEBHOOK_URL", "https://example.test/webhook")
	t.Setenv("POLL_INTERVAL_SECONDS", "1")
	t.Setenv("BASE_BRANCH", "main")
	t.Setenv("WORKTREE_DIR", "")

	repoName := fmt.Sprintf("widget-cancel-%d", time.Now().UnixNano())
	repoPath := initGitRepoWithOrigin(t, "acme", repoName)
	configPath := writeRepoConfig(t, fmt.Sprintf("REPO_OWNER: acme\nREPO_NAME: %s\nREPO_PATH: %s\nMANDATORY_TESTS:\n  - git status\n", repoName, repoPath))

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(stdin) error = %v", err)
	}
	defer inW.Close()

	oldStdin := os.Stdin
	os.Stdin = inR
	defer func() {
		os.Stdin = oldStdin
		_ = inR.Close()
	}()

	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(time.Second)
		cancel()
	}()

	start := time.Now()
	stdout, stderr, code := captureStdoutStderr(t, func() int {
		return Run(parentCtx, []string{"repository-agent-orchestrator", "--config", configPath})
	})
	if code != 0 {
		t.Fatalf("Run() code = %d, want 0\nstdout=%q\nstderr=%q", code, stdout, stderr)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("Run() stderr = %q, want empty", stderr)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Run() took %s to exit after parent context cancellation", elapsed)
	}
}
