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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestValidateMandatoryTestExecutables(t *testing.T) {
	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "fake-test")
	if err := os.WriteFile(binPath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	t.Setenv("PATH", binDir)

	err := validateMandatoryTestExecutables([]string{
		"fake-test ./...",
		"fake-test -run TestFoo",
	})
	if err != nil {
		t.Fatalf("validateMandatoryTestExecutables() error = %v", err)
	}
}

func TestValidateMandatoryTestExecutablesMissingExecutable(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	err := validateMandatoryTestExecutables([]string{"missing-tool ./..."})
	if err == nil {
		t.Fatal("validateMandatoryTestExecutables() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "mandatory test executable not found in PATH: missing-tool") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateMandatoryTestExecutablesInvalidCommand(t *testing.T) {
	err := validateMandatoryTestExecutables([]string{"   "})
	if err == nil {
		t.Fatal("validateMandatoryTestExecutables() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "invalid mandatory test command") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunMandatoryTestGateIncludesCapturedCommandOutput(t *testing.T) {
	worktree := t.TempDir()
	logDir := t.TempDir()
	script := filepath.Join(t.TempDir(), "fail-gate.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho stdout-line\necho stderr-line 1>&2\nexit 3\n"), 0o755); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			MandatoryTests: []string{script},
		},
	}

	agent := Agent{
		ID:           "review-agent-7",
		IssueNumber:  7,
		WorktreePath: worktree,
		LogDir:       logDir,
	}

	err := bot.runMandatoryTestGate(context.Background(), agent)
	if err == nil {
		t.Fatal("runMandatoryTestGate() error = nil, want non-nil")
	}
	for _, want := range []string{
		"hard gate failed: " + script,
		"Cause: exit status 3",
		"stdout:",
		"stdout-line",
		"stderr:",
		"stderr-line",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("runMandatoryTestGate() missing %q in error: %v", want, err)
		}
	}

	logPath, pathErr := mandatoryTestLogPath(agent)
	if pathErr != nil {
		t.Fatalf("mandatoryTestLogPath() error = %v", pathErr)
	}
	body, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", logPath, readErr)
	}
	logText := string(body)
	for _, want := range []string{
		"hard gate started",
		"command started: " + script,
		"stdout-line",
		"stderr-line",
		"command failed: " + script,
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("mandatory test log missing %q: %q", want, logText)
		}
	}
}

func TestRunMandatoryTestGateRejectsTrackedOrchestratorRuntimeArtifacts(t *testing.T) {
	repoPath := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", repoPath, err)
	}

	runGit(t, repoPath, "init")
	runGit(t, repoPath, "config", "user.email", "tester@example.com")
	runGit(t, repoPath, "config", "user.name", "Tester")
	runGit(t, repoPath, "checkout", "-b", "main")
	if err := os.WriteFile(filepath.Join(repoPath, ".gitignore"), []byte("/.repository-agent-orchestrator/\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(.gitignore) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "README.md"), []byte("base\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(README.md) error = %v", err)
	}
	runGit(t, repoPath, "add", ".gitignore", "README.md")
	runGit(t, repoPath, "commit", "-m", "initial")

	handoffPath := filepath.Join(repoPath, ".repository-agent-orchestrator", "HANDOFF.yaml")
	if err := os.MkdirAll(filepath.Dir(handoffPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(handoffPath), err)
	}
	if err := os.WriteFile(handoffPath, []byte("schema_version: 1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", handoffPath, err)
	}
	runGit(t, repoPath, "add", "-f", ".repository-agent-orchestrator/HANDOFF.yaml")

	logDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			MandatoryTests: []string{"git status --short"},
		},
	}
	agent := Agent{
		ID:           "review-agent-11",
		IssueNumber:  11,
		WorktreePath: repoPath,
		LogDir:       logDir,
	}

	err := bot.runMandatoryTestGate(context.Background(), agent)
	if err == nil {
		t.Fatal("runMandatoryTestGate() error = nil, want tracked runtime artifact failure")
	}
	for _, want := range []string{
		"hard gate failed: tracked Repository Agent Orchestrator runtime artifact check",
		".repository-agent-orchestrator/HANDOFF.yaml",
		"must remain ignored and must not be staged or committed",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("runMandatoryTestGate() missing %q in error: %v", want, err)
		}
	}

	logPath, pathErr := mandatoryTestLogPath(agent)
	if pathErr != nil {
		t.Fatalf("mandatoryTestLogPath() error = %v", pathErr)
	}
	body, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", logPath, readErr)
	}
	logText := string(body)
	for _, want := range []string{
		"tracked Repository Agent Orchestrator runtime artifact check started",
		"tracked Repository Agent Orchestrator runtime artifact check failed",
		".repository-agent-orchestrator/HANDOFF.yaml",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("mandatory test log missing %q: %q", want, logText)
		}
	}
	if strings.Contains(logText, "git status --short") {
		t.Fatalf("mandatory test log should fail before configured commands run: %q", logText)
	}
}

func TestRunMandatoryTestGateCancellationKillsProcessTree(t *testing.T) {
	worktree := t.TempDir()
	logDir := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	script := filepath.Join(t.TempDir(), "cancel-gate.sh")
	scriptBody := fmt.Sprintf("#!/bin/sh\nset -eu\nsleep 30 &\nchild=$!\nprintf '%%s\\n' \"$child\" > %q\nwait \"$child\"\n", pidFile)
	if err := os.WriteFile(script, []byte(scriptBody), 0o755); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			MandatoryTests: []string{script},
		},
	}

	agent := Agent{
		ID:           "review-agent-9",
		IssueNumber:  9,
		WorktreePath: worktree,
		LogDir:       logDir,
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- bot.runMandatoryTestGate(ctx, agent)
	}()

	var childPID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		body, err := os.ReadFile(pidFile)
		if err != nil {
			if os.IsNotExist(err) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			t.Fatalf("os.ReadFile(%s) error = %v", pidFile, err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
		if err != nil {
			t.Fatalf("invalid child pid %q: %v", string(body), err)
		}
		if processExists(pid) {
			childPID = pid
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if childPID == 0 {
		t.Fatal("child process did not start before timeout")
	}
	defer func() {
		if processExists(childPID) {
			_ = syscall.Kill(childPID, syscall.SIGKILL)
		}
	}()

	cancel()

	err := <-errCh
	if err == nil {
		t.Fatal("runMandatoryTestGate() error = nil, want non-nil after cancellation")
	}
	if !strings.Contains(err.Error(), "Cause: context canceled") {
		t.Fatalf("runMandatoryTestGate() error = %v, want context canceled cause", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !processExists(childPID) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("child process %d still running after gate cancellation", childPID)
}

func TestRunMandatoryTestGateInterruptsFailureAfterWallClockPause(t *testing.T) {
	oldInterval := hardGatePauseDetectionInterval
	oldThreshold := hardGatePauseThreshold
	hardGatePauseDetectionInterval = time.Hour
	hardGatePauseThreshold = time.Millisecond
	t.Cleanup(func() {
		hardGatePauseDetectionInterval = oldInterval
		hardGatePauseThreshold = oldThreshold
	})

	worktree := t.TempDir()
	logDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			MandatoryTests: []string{"make test-system"},
		},
		mandatoryTestRunner: func(ctx context.Context, dir string, logPath string, name string, args ...string) error {
			time.Sleep(5 * time.Millisecond)
			return errors.New("exit status 1")
		},
	}
	agent := Agent{
		ID:           "review-agent-10",
		IssueNumber:  10,
		WorktreePath: worktree,
		LogDir:       logDir,
	}

	err := bot.runMandatoryTestGate(context.Background(), agent)
	if err == nil {
		t.Fatal("runMandatoryTestGate() error = nil, want interruption")
	}
	if !isMandatoryTestGateInterrupted(err) {
		t.Fatalf("isMandatoryTestGateInterrupted(%v) = false, want true", err)
	}
	if !strings.Contains(err.Error(), "host sleep or long process suspension detected") {
		t.Fatalf("runMandatoryTestGate() error = %v, want host sleep reason", err)
	}

	var gateErr *mandatoryTestGateError
	if !errors.As(err, &gateErr) {
		t.Fatalf("runMandatoryTestGate() error = %T, want wrapped mandatoryTestGateError", err)
	}
	if gateErr.Command != "make test-system" {
		t.Fatalf("wrapped gate command = %q, want %q", gateErr.Command, "make test-system")
	}

	logPath, pathErr := mandatoryTestLogPath(agent)
	if pathErr != nil {
		t.Fatalf("mandatoryTestLogPath() error = %v", pathErr)
	}
	body, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", logPath, readErr)
	}
	if !strings.Contains(string(body), "hard gate interrupted: host sleep or long process suspension detected") {
		t.Fatalf("mandatory test log missing interruption note: %q", string(body))
	}
}

func TestRunMandatoryTestGateSerialModeRunsOneAtATime(t *testing.T) {
	worktreeOne := t.TempDir()
	worktreeTwo := t.TempDir()
	logDir := t.TempDir()
	release := make(chan struct{})
	started := make(chan string, 2)
	errCh := make(chan error, 2)
	var mu sync.Mutex
	callCount := 0

	bot := &Orchestrator{
		cfg: Config{
			MandatoryTests: []string{"make test"},
			HardGateMode:   HardGateModeSerial,
		},
		mandatoryTestRunner: func(ctx context.Context, dir string, logPath string, name string, args ...string) error {
			mu.Lock()
			callCount++
			mu.Unlock()
			started <- dir
			<-release
			return nil
		},
	}

	agentOne := Agent{ID: "review-agent-1", IssueNumber: 1, WorktreePath: worktreeOne, LogDir: logDir}
	agentTwo := Agent{ID: "review-agent-2", IssueNumber: 2, WorktreePath: worktreeTwo, LogDir: logDir}

	go func() { errCh <- bot.runMandatoryTestGate(context.Background(), agentOne) }()
	first := <-started
	if first != worktreeOne {
		t.Fatalf("first gate started in %q, want %q", first, worktreeOne)
	}

	go func() { errCh <- bot.runMandatoryTestGate(context.Background(), agentTwo) }()
	select {
	case second := <-started:
		t.Fatalf("second gate started before the first completed: %s", second)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	second := <-started
	if second != worktreeTwo {
		t.Fatalf("second gate started in %q, want %q", second, worktreeTwo)
	}

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("runMandatoryTestGate() error = %v", err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if callCount != 2 {
		t.Fatalf("mandatory test runner call count = %d, want 2", callCount)
	}
}
