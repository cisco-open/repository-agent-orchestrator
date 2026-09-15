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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func writeExecutable(t *testing.T, dir, name, script string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("os.WriteFile(%s) error = %v", path, err)
	}
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("os.Chmod(%s) error = %v", path, err)
	}
	return path
}

func initGitRepoWithOrigin(t *testing.T, owner, repo string) string {
	t.Helper()
	repoPath := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repoPath}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git -C %s %s failed: %v: %s", repoPath, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
	}
	run("init")
	run("remote", "add", "origin", fmt.Sprintf("https://github.com/%s/%s.git", owner, repo))
	return repoPath
}

const testAgentRuntimeExecutable = "test-agent-runtime"

func installRuntimeToolShims(t *testing.T, includeAgentRuntime bool) string {
	t.Helper()
	toolDir := t.TempDir()
	writeExecutable(t, toolDir, "gh", "#!/bin/sh\nif [ \"$1\" = \"auth\" ] && [ \"$2\" = \"token\" ]; then\n  if [ \"${FAKE_GH_FAIL:-}\" = \"1\" ]; then\n    exit 1\n  fi\n  printf '%s\\n' \"${FAKE_GH_TOKEN}\"\n  exit 0\nfi\nexit 2\n")
	writeExecutable(t, toolDir, "make", "#!/bin/sh\nexit 0\n")
	writeExecutable(t, toolDir, "tmux", "#!/bin/sh\nexit 0\n")
	if includeAgentRuntime {
		writeExecutable(
			t,
			toolDir,
			testAgentRuntimeExecutable,
			"#!/bin/sh\nexit 0\n",
		)
	}
	return toolDir
}

func testAgentRuntimeCommand(toolDir string) string {
	return filepath.Join(toolDir, testAgentRuntimeExecutable)
}

func prependPATH(t *testing.T, dir string) {
	t.Helper()
	orig := os.Getenv("PATH")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+orig)
}

func TestRuntimeCommandName(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty", raw: "", want: "codex"},
		{name: "spaces", raw: "   ", want: "codex"},
		{name: "command with args", raw: "codex --profile ci", want: "codex"},
		{name: "command only", raw: "codex", want: "codex"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := runtimeCommandName(tt.raw); got != tt.want {
				t.Fatalf("runtimeCommandName(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestEnsureDirExistsAndWritable(t *testing.T) {
	dir := t.TempDir()
	if err := ensureDirExists(dir); err != nil {
		t.Fatalf("ensureDirExists(dir) error = %v", err)
	}

	missing := filepath.Join(dir, "missing")
	if err := ensureDirExists(missing); err == nil {
		t.Fatal("ensureDirExists(missing) error = nil, want error")
	}

	filePath := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%s) error = %v", filePath, err)
	}
	if err := ensureDirExists(filePath); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("ensureDirExists(file) error = %v, want path is not a directory", err)
	}

	writable := filepath.Join(dir, "writable")
	if err := ensureDirWritable(writable); err != nil {
		t.Fatalf("ensureDirWritable(%s) error = %v", writable, err)
	}

	notDirParent := filepath.Join(dir, "not-dir")
	if err := os.WriteFile(notDirParent, []byte("x"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%s) error = %v", notDirParent, err)
	}
	if err := ensureDirWritable(filepath.Join(notDirParent, "child")); err == nil {
		t.Fatal("ensureDirWritable(file/child) error = nil, want error")
	}
}

func TestIsGitRepo(t *testing.T) {
	ctx := context.Background()
	repoPath := t.TempDir()
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "init")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %v: %s", err, strings.TrimSpace(string(out)))
	}

	ok, err := isGitRepo(ctx, repoPath)
	if err != nil {
		t.Fatalf("isGitRepo(repo) error = %v", err)
	}
	if !ok {
		t.Fatal("isGitRepo(repo) = false, want true")
	}

	nonRepo := t.TempDir()
	ok, err = isGitRepo(ctx, nonRepo)
	if err != nil {
		t.Fatalf("isGitRepo(nonRepo) error = %v", err)
	}
	if ok {
		t.Fatal("isGitRepo(nonRepo) = true, want false")
	}
}

func TestGetGitHubTokenFromGH(t *testing.T) {
	toolDir := installRuntimeToolShims(t, true)
	prependPATH(t, toolDir)

	t.Run("success", func(t *testing.T) {
		t.Setenv("FAKE_GH_TOKEN", "token-123")
		t.Setenv("FAKE_GH_FAIL", "")
		token, err := getGitHubTokenFromGH(context.Background())
		if err != nil {
			t.Fatalf("getGitHubTokenFromGH() error = %v", err)
		}
		if token != "token-123" {
			t.Fatalf("token = %q, want %q", token, "token-123")
		}
	})

	t.Run("empty token", func(t *testing.T) {
		t.Setenv("FAKE_GH_TOKEN", "")
		t.Setenv("FAKE_GH_FAIL", "")
		_, err := getGitHubTokenFromGH(context.Background())
		if err == nil || !strings.Contains(err.Error(), "gh auth token is empty") {
			t.Fatalf("getGitHubTokenFromGH() error = %v, want empty token error", err)
		}
	})

	t.Run("gh command fails", func(t *testing.T) {
		t.Setenv("FAKE_GH_FAIL", "1")
		_, err := getGitHubTokenFromGH(context.Background())
		if err == nil || !strings.Contains(err.Error(), "failed to read GitHub auth token") {
			t.Fatalf("getGitHubTokenFromGH() error = %v, want auth token failure", err)
		}
	})
}

func TestValidateRuntime(t *testing.T) {
	t.Run("fails when gh missing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		cfg := Config{CodexCmd: testAgentRuntimeExecutable}
		err := cfg.ValidateRuntime(context.Background())
		if err == nil || !strings.Contains(err.Error(), "gh not found in PATH") {
			t.Fatalf("ValidateRuntime() error = %v, want gh missing error", err)
		}
	})

	t.Run("fails when codex command missing", func(t *testing.T) {
		toolDir := installRuntimeToolShims(t, false)
		prependPATH(t, toolDir)
		cfg := Config{CodexCmd: "missing-codex --profile ci"}
		err := cfg.ValidateRuntime(context.Background())
		if err == nil || !strings.Contains(err.Error(), "missing-codex not found in PATH") {
			t.Fatalf("ValidateRuntime() error = %v, want missing codex error", err)
		}
	})

	t.Run("succeeds for valid runtime", func(t *testing.T) {
		toolDir := installRuntimeToolShims(t, true)
		prependPATH(t, toolDir)
		t.Setenv("FAKE_GH_TOKEN", "token-validate")
		repoPath := initGitRepoWithOrigin(t, "acme", "widget")

		cfg := Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       repoPath,
			WorktreeDir:    filepath.Join(t.TempDir(), "worktrees"),
			LogDir:         filepath.Join(t.TempDir(), "logs"),
			MandatoryTests: []string{"git status"},
			CodexCmd:       testAgentRuntimeCommand(toolDir) + " --profile ci",
		}
		if err := cfg.ValidateRuntime(context.Background()); err != nil {
			t.Fatalf("ValidateRuntime() error = %v", err)
		}
	})

	t.Run("fails when repository remote mismatches config", func(t *testing.T) {
		toolDir := installRuntimeToolShims(t, true)
		prependPATH(t, toolDir)
		t.Setenv("FAKE_GH_TOKEN", "token-validate")
		repoPath := initGitRepoWithOrigin(t, "acme", "widget")

		cfg := Config{
			RepoOwner:      "acme",
			RepoName:       "other",
			RepoPath:       repoPath,
			WorktreeDir:    filepath.Join(t.TempDir(), "worktrees"),
			LogDir:         filepath.Join(t.TempDir(), "logs"),
			MandatoryTests: []string{"git status"},
			CodexCmd:       testAgentRuntimeCommand(toolDir) + " --profile ci",
		}
		err := cfg.ValidateRuntime(context.Background())
		if err == nil || !strings.Contains(err.Error(), "repo path points to") {
			t.Fatalf("ValidateRuntime() error = %v, want repo mismatch error", err)
		}
	})

	t.Run("fails when mandatory test command is invalid", func(t *testing.T) {
		toolDir := installRuntimeToolShims(t, true)
		prependPATH(t, toolDir)
		t.Setenv("FAKE_GH_TOKEN", "token-validate")
		repoPath := initGitRepoWithOrigin(t, "acme", "widget")

		cfg := Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       repoPath,
			WorktreeDir:    filepath.Join(t.TempDir(), "worktrees"),
			LogDir:         filepath.Join(t.TempDir(), "logs"),
			MandatoryTests: []string{"   "},
			CodexCmd:       testAgentRuntimeCommand(toolDir) + " --profile ci",
		}
		err := cfg.ValidateRuntime(context.Background())
		if err == nil || !strings.Contains(err.Error(), "invalid mandatory test command") {
			t.Fatalf("ValidateRuntime() error = %v, want invalid mandatory test error", err)
		}
	})

	t.Run("fails when gh token command fails", func(t *testing.T) {
		toolDir := installRuntimeToolShims(t, true)
		prependPATH(t, toolDir)
		t.Setenv("FAKE_GH_FAIL", "1")
		repoPath := initGitRepoWithOrigin(t, "acme", "widget")

		cfg := Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       repoPath,
			WorktreeDir:    filepath.Join(t.TempDir(), "worktrees"),
			LogDir:         filepath.Join(t.TempDir(), "logs"),
			MandatoryTests: []string{"git status"},
			CodexCmd:       testAgentRuntimeCommand(toolDir) + " --profile ci",
		}
		err := cfg.ValidateRuntime(context.Background())
		if err == nil || !strings.Contains(err.Error(), "failed to read GitHub auth token") {
			t.Fatalf("ValidateRuntime() error = %v, want token read failure", err)
		}
	})
}

func TestNewOrchestrator(t *testing.T) {
	toolDir := installRuntimeToolShims(t, true)
	prependPATH(t, toolDir)
	t.Setenv("FAKE_GH_TOKEN", "token-new-orchestrator")

	cfg := Config{
		RepoOwner:       "acme",
		RepoName:        "widget",
		RepoPath:        t.TempDir(),
		LogDir:          t.TempDir(),
		CodexCmd:        testAgentRuntimeCommand(toolDir) + " --profile ci",
		WebexWebhookURL: "https://example.test/webhook",
	}
	bot, err := NewOrchestrator(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewOrchestrator() error = %v", err)
	}
	if bot == nil {
		t.Fatal("NewOrchestrator() = nil, want bot")
	}
	if bot.token != "token-new-orchestrator" {
		t.Fatalf("bot.token = %q, want %q", bot.token, "token-new-orchestrator")
	}
	if bot.github == nil {
		t.Fatal("bot.github = nil, want initialized GitHub client")
	}
	if bot.agents == nil {
		t.Fatal("bot.agents = nil, want initialized manager")
	}
	if bot.runner == nil {
		t.Fatal("bot.runner = nil, want initialized runner")
	}
	runner, ok := bot.runner.(*TmuxRunner)
	if !ok || runner.isolation == nil {
		t.Fatalf("bot.runner = %T, want repository-isolated tmux runner", bot.runner)
	}
	wantServer := runtimeTmuxServerName(cfg.RepoOwner, cfg.RepoName, cfg.RepoPath)
	if runner.isolation.tmuxServer != wantServer {
		t.Fatalf(
			"bot runtime server = %q, want %q",
			runner.isolation.tmuxServer,
			wantServer,
		)
	}
}
