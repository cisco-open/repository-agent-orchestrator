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
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

type fakeCommandCall struct {
	name string
	args []string
}

type fakeCommandExecutor struct {
	calls                        []fakeCommandCall
	errs                         map[int]error
	outs                         map[int][]byte
	blockOutputsUntilContextDone bool
	// loadedBuffers captures the content written to disk for each
	// "load-buffer -b <name> <path>" call observed, keyed by buffer name,
	// since prompt text now travels through a temp file rather than as a
	// literal command-line argument.
	loadedBuffers map[string]string
}

func (f *fakeCommandExecutor) Run(name string, args ...string) error {
	callArgs := append([]string(nil), args...)
	f.calls = append(f.calls, fakeCommandCall{name: name, args: callArgs})
	f.recordLoadedBuffer(callArgs)
	if err, ok := f.errs[len(f.calls)-1]; ok {
		return err
	}
	return nil
}

func (f *fakeCommandExecutor) recordLoadedBuffer(args []string) {
	loadIdx := -1
	for i, arg := range args {
		if arg == "load-buffer" {
			loadIdx = i
			break
		}
	}
	if loadIdx == -1 ||
		loadIdx+2 >= len(args) ||
		args[loadIdx+1] != "-b" {
		return
	}
	name := args[loadIdx+2]
	path := args[len(args)-1]
	content, err := os.ReadFile(path)
	if err != nil {
		return
	}
	if f.loadedBuffers == nil {
		f.loadedBuffers = make(map[string]string)
	}
	f.loadedBuffers[name] = string(content)
}

func (f *fakeCommandExecutor) Output(name string, args ...string) ([]byte, error) {
	callArgs := append([]string(nil), args...)
	f.calls = append(f.calls, fakeCommandCall{name: name, args: callArgs})
	if err, ok := f.errs[len(f.calls)-1]; ok {
		return nil, err
	}
	if out, ok := f.outs[len(f.calls)-1]; ok {
		return out, nil
	}
	return nil, nil
}

func (f *fakeCommandExecutor) RunContext(
	ctx context.Context,
	name string,
	args ...string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.Run(name, args...)
}

func (f *fakeCommandExecutor) OutputContext(
	ctx context.Context,
	name string,
	args ...string,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !f.blockOutputsUntilContextDone {
		return f.Output(name, args...)
	}
	callArgs := append([]string(nil), args...)
	f.calls = append(f.calls, fakeCommandCall{name: name, args: callArgs})
	if err, ok := f.errs[len(f.calls)-1]; ok {
		return nil, err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type fakeExitError struct {
	code int
}

type managedPromptRetryExecutor struct {
	pasted     bool
	enterCount int
}

func (e *managedPromptRetryExecutor) Run(name string, args ...string) error {
	return e.RunContext(context.Background(), name, args...)
}

func (e *managedPromptRetryExecutor) Output(
	name string,
	args ...string,
) ([]byte, error) {
	return e.OutputContext(context.Background(), name, args...)
}

func (e *managedPromptRetryExecutor) RunContext(
	ctx context.Context,
	_ string,
	args ...string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(args) > 0 && args[0] == "paste-buffer" {
		e.pasted = true
	}
	if len(args) > 0 && args[len(args)-1] == "Enter" {
		e.enterCount++
	}
	return nil
}

func (e *managedPromptRetryExecutor) OutputContext(
	ctx context.Context,
	_ string,
	_ ...string,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch {
	case e.enterCount >= 2:
		return []byte("prompt submitted"), nil
	case e.pasted:
		return []byte("prompt pasted"), nil
	default:
		return []byte("idle"), nil
	}
}

type busyIndicatorNoiseExecutor struct {
	calls int
}

func (e *busyIndicatorNoiseExecutor) Run(string, ...string) error {
	return nil
}

func (e *busyIndicatorNoiseExecutor) Output(
	string,
	...string,
) ([]byte, error) {
	e.calls++
	return []byte(fmt.Sprintf(
		"OpenAI Codex\nBooting MCP server: codex_apps (%ds • esc to interrupt)",
		e.calls,
	)), nil
}

func (e *busyIndicatorNoiseExecutor) RunContext(
	ctx context.Context,
	name string,
	args ...string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.Run(name, args...)
}

func (e *busyIndicatorNoiseExecutor) OutputContext(
	ctx context.Context,
	name string,
	args ...string,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.Output(name, args...)
}

func containsArgument(args []string, target string) bool {
	for _, arg := range args {
		if arg == target {
			return true
		}
	}
	return false
}

func (e fakeExitError) Error() string {
	return "exit error"
}

func (e fakeExitError) ExitCode() int {
	return e.code
}

func TestTmuxRunnerStartCreatesSessionAndSendsPrompt(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			4: []byte("OpenAI Codex"),
			5: []byte("OpenAI Codex"),
		},
	}
	runner := NewTmuxRunner("", fakeExec)
	worktree := t.TempDir()
	agent := Agent{
		ID:           "agent-7-1700000000",
		IssueNumber:  7,
		WorktreePath: worktree,
		BranchName:   "repository-agent-orchestrator/issue-7",
	}

	handle, err := runner.Start(agent, "line one\nline two")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	wantSession := "repository-agent-orchestrator-agent-7-1700000000"
	if got := handle.Kind; got != RuntimeKindTmux {
		t.Fatalf("handle.Kind = %q, want %q", got, RuntimeKindTmux)
	}
	if got := handle.Session; got != wantSession {
		t.Fatalf("handle.Session = %q, want %q", got, wantSession)
	}
	wantLogPath, err := runtimeLogPath(agent)
	if err != nil {
		t.Fatalf("runtimeLogPath() error = %v", err)
	}
	wantPipeCommand, err := runtimeLogPipeCommand(wantLogPath)
	if err != nil {
		t.Fatalf("runtimeLogPipeCommand() error = %v", err)
	}
	wantRuntimeCommand := runtimeLaunchCommand("codex", runtime.GOOS)
	if got := handle.LogPath; got != wantLogPath {
		t.Fatalf("handle.LogPath = %q, want %q", got, wantLogPath)
	}

	if len(fakeExec.calls) != 9 {
		t.Fatalf("commands = %#v, want 9 calls", fakeExec.calls)
	}
	loadBufferCall := fakeExec.calls[6]
	if loadBufferCall.name != "tmux" || len(loadBufferCall.args) != 4 ||
		loadBufferCall.args[0] != "load-buffer" || loadBufferCall.args[1] != "-b" {
		t.Fatalf(
			"load-buffer call = %#v, want load-buffer -b <name> <path>",
			loadBufferCall,
		)
	}
	bufferName := loadBufferCall.args[2]
	bufferPath := loadBufferCall.args[3]
	if bufferPath == "" {
		t.Fatal("load-buffer call missing buffer file path")
	}
	if want := tmuxPasteBufferName(wantSession); bufferName != want {
		t.Fatalf("buffer name = %q, want %q", bufferName, want)
	}

	wantCalls := []fakeCommandCall{
		{name: "tmux", args: []string{"new-session", "-d", "-s", wantSession, "-c", worktree}},
		{name: "tmux", args: []string{"pipe-pane", "-o", "-t", wantSession, wantPipeCommand}},
		{name: "tmux", args: []string{"send-keys", "-t", wantSession, "-l", "--", wantRuntimeCommand}},
		{name: "tmux", args: []string{"send-keys", "-t", wantSession, "Enter"}},
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", wantSession, "-S", codexReadyProbeLines}},
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", wantSession, "-S", codexReadyProbeLines}},
		{name: "tmux", args: []string{"load-buffer", "-b", bufferName, bufferPath}},
		{name: "tmux", args: []string{"paste-buffer", "-d", "-p", "-t", wantSession, "-b", bufferName}},
		{name: "tmux", args: []string{"send-keys", "-t", wantSession, "Enter"}},
	}
	if !reflect.DeepEqual(fakeExec.calls, wantCalls) {
		t.Fatalf("commands = %#v, want %#v", fakeExec.calls, wantCalls)
	}
	if got, want := fakeExec.loadedBuffers[bufferName], "line one\nline two"; got != want {
		t.Fatalf("loaded buffer content = %q, want %q", got, want)
	}
}

func TestManagedPromptRetriesEnterUntilSubmissionIsAcknowledged(t *testing.T) {
	executor := &managedPromptRetryExecutor{}
	runner := NewTmuxRunner("codex", executor)
	runner.managedPromptTimeout = 10 * time.Millisecond
	runner.managedPromptPoll = time.Millisecond

	if err := runner.sendManagedPromptContext(
		context.Background(),
		"review-worker",
		"review this change",
	); err != nil {
		t.Fatalf("sendManagedPromptContext() error = %v", err)
	}
	if executor.enterCount != managedPromptAttempts {
		t.Fatalf("Enter attempts = %d, want %d", executor.enterCount, managedPromptAttempts)
	}
}

func TestManagedPromptPaneChangeIgnoresBusyIndicatorNoise(t *testing.T) {
	executor := &busyIndicatorNoiseExecutor{}
	runner := NewTmuxRunner("codex", executor)
	runner.managedPromptTimeout = 20 * time.Millisecond
	runner.managedPromptPoll = time.Millisecond

	previous := "OpenAI Codex\nBooting MCP server: codex_apps (0s • esc to interrupt)"
	if _, err := runner.waitForManagedPromptPaneChange(
		context.Background(),
		"session",
		previous,
	); err == nil {
		t.Fatal("waitForManagedPromptPaneChange() error = nil, want timeout since only the busy-indicator timer changed")
	}
}

func TestTmuxRunnerStartUsesResolvedAgentProfile(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			4: []byte("OpenAI Codex"),
			5: []byte("OpenAI Codex"),
		},
	}
	runner := NewTmuxRunner("codex --model global-model", fakeExec)
	agent := Agent{
		ID:           "profiled-agent",
		WorktreePath: t.TempDir(),
		RuntimeProfile: AgentProfile{
			Name: "verifier", Model: "gpt-5.6-terra", ReasoningEffort: "high",
		},
	}

	if _, err := runner.Start(agent, "review"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if len(fakeExec.calls) < 3 {
		t.Fatalf("command calls = %d, want runtime command", len(fakeExec.calls))
	}
	got := fakeExec.calls[2]
	wantCommand := runtimeLaunchCommand(
		"codex --model 'gpt-5.6-terra' -c 'model_reasoning_effort=\"high\"'",
		runtime.GOOS,
	)
	want := fakeCommandCall{
		name: "tmux",
		args: []string{"send-keys", "-t", tmuxSessionName(agent), "-l", "--", wantCommand},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime launch command = %#v, want %#v", got, want)
	}
}

func TestTmuxRunnerStartsReviewWorkerFromEmptyAllowlistedEnvironment(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			4: []byte("OpenAI Codex"),
			5: []byte("OpenAI Codex"),
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)
	agent := Agent{
		ID:           "review-worker-isolated",
		WorktreePath: t.TempDir(),
		RuntimeProfile: AgentProfile{
			Name: "discovery", Model: "gpt-5.6-terra", ReasoningEffort: "high",
		},
	}
	gitHubConfigPath, err := reviewWorkerGitHubConfigPath(agent.WorktreePath)
	if err != nil {
		t.Fatalf("reviewWorkerGitHubConfigPath() error = %v", err)
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(agent.WorktreePath)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	environment := []string{
		"GH_CONFIG_DIR=" + gitHubConfigPath,
		"GIT_ASKPASS=",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"HOME=/safe/home",
		"PATH=/usr/bin:/bin",
		"RAO_REVIEW_ATTEMPT=1",
		"RAO_REVIEW_ARTIFACT_DIR=" + artifactDirectory,
		"RAO_REVIEW_CYCLE_ID=review-cycle-abcdef123456-0123456789abcdef0123456789abcdef",
		"RAO_REVIEW_HEAD_SHA=" + testReviewHeadSHA,
		"RAO_REVIEW_LANE=contract",
		"RAO_REVIEW_OWNER_ID=review-worker-isolated",
		"RAO_REVIEW_PASS=1",
		"RAO_REVIEW_PR_NUMBER=50",
		"RAO_REVIEW_REPO=acme/widget",
		"RAO_REVIEW_REPO_NAME=widget",
		"RAO_REVIEW_REPO_OWNER=acme",
		"RAO_REVIEW_REVISION=1",
		"RAO_REVIEW_ROLE=discovery",
		"RAO_REVIEW_WORKTREE=" + agent.WorktreePath,
		"SSH_ASKPASS=",
	}

	handle, err := runner.StartReviewWorker(agent, "review", environment)
	if err != nil {
		t.Fatalf("StartReviewWorker() error = %v", err)
	}
	if handle.Session != reviewWorkerTmuxSessionName(agent) {
		t.Fatalf(
			"session = %q, want %q",
			handle.Session,
			reviewWorkerTmuxSessionName(agent),
		)
	}
	if len(fakeExec.calls) == 0 {
		t.Fatal("StartReviewWorker() did not create a tmux session")
	}
	newSession := fakeExec.calls[0]
	if len(newSession.args) != 7 {
		t.Fatalf("new-session args = %#v, want isolated shell command", newSession.args)
	}
	isolatedShell := newSession.args[len(newSession.args)-1]
	if !strings.HasPrefix(isolatedShell, "/usr/bin/env -i ") {
		t.Fatalf("isolated shell = %q, want env -i boundary", isolatedShell)
	}
	for _, entry := range environment {
		if !strings.Contains(isolatedShell, shellSingleQuote(entry)) {
			t.Fatalf("isolated shell omitted allowlisted entry %q", entry)
		}
	}
	for _, blocked := range []string{
		"GH_TOKEN",
		"GITHUB_TOKEN",
		"GITHUB_ENTERPRISE_TOKEN",
		"WEBEX_WEBHOOK_URL",
		"CODEX_CMD",
	} {
		if strings.Contains(isolatedShell, blocked+"=") {
			t.Fatalf("isolated shell forwarded blocked variable %s", blocked)
		}
	}
	runtimeExecSeen := false
	for _, call := range fakeExec.calls {
		for _, arg := range call.args {
			if strings.HasPrefix(arg, "exec ") &&
				strings.Contains(arg, "codex --model") {
				runtimeExecSeen = true
			}
		}
	}
	if !runtimeExecSeen {
		t.Fatalf(
			"review worker launch did not replace the isolated shell: %#v",
			fakeExec.calls,
		)
	}
}

func TestTmuxRunnerRejectsUnallowlistedReviewWorkerEnvironmentBeforeSession(t *testing.T) {
	fakeExec := &fakeCommandExecutor{}
	runner := NewTmuxRunner("codex", fakeExec)
	_, err := runner.StartReviewWorker(
		Agent{ID: "review-worker-rejected", WorktreePath: t.TempDir()},
		"review",
		[]string{"GITHUB_TOKEN=must-not-launch"},
	)
	if err == nil || !strings.Contains(err.Error(), "not allowlisted") {
		t.Fatalf("StartReviewWorker() error = %v, want allowlist failure", err)
	}
	if len(fakeExec.calls) != 0 {
		t.Fatalf("tmux calls = %#v, want none", fakeExec.calls)
	}
}

func TestReviewWorkerTmuxSessionNameRetainsDigestForLongDistinctOwners(t *testing.T) {
	prefix := strings.Repeat("same-long-review-worker-owner-prefix-", 4)
	first := reviewWorkerTmuxSessionName(Agent{ID: prefix + "first"})
	second := reviewWorkerTmuxSessionName(Agent{ID: prefix + "second"})
	if first == second {
		t.Fatalf("long worker sessions collided: %q", first)
	}
	if len(first) > 64 || len(second) > 64 {
		t.Fatalf("long worker session lengths = %d, %d, want <= 64", len(first), len(second))
	}
}

func TestTmuxSessionNamePreservesLegacyTruncationForLongReviewer(t *testing.T) {
	reviewer := Agent{ID: "review-agent-54-1700000000123456789"}
	unbounded := "repository-agent-orchestrator-" + reviewer.ID
	if len(unbounded) <= 64 {
		t.Fatalf("test reviewer session length = %d, want > 64", len(unbounded))
	}
	if got, want := tmuxSessionName(reviewer), unbounded[:64]; got != want {
		t.Fatalf("tmuxSessionName() = %q, want legacy session %q", got, want)
	}
	if got := reviewWorkerTmuxSessionName(reviewer); got == unbounded[:64] {
		t.Fatalf("worker session = %q, want digest-distinct name", got)
	}
}

func TestCodexCommandForAgentProfileExecutesWithOneModelArgument(t *testing.T) {
	fakeCodex := filepath.Join(t.TempDir(), "fake-codex")
	script := `#!/bin/sh
count=0
model=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --model|-m)
      [ "$#" -ge 2 ] || exit 3
      count=$((count + 1))
      model=$2
      shift 2
      ;;
    --model=*)
      count=$((count + 1))
      model=${1#--model=}
      shift
      ;;
    -m=*)
      count=$((count + 1))
      model=${1#-m=}
      shift
      ;;
    -m*)
      count=$((count + 1))
      model=${1#-m}
      shift
      ;;
    *)
      shift
      ;;
  esac
done
[ "$count" -eq 1 ] && [ "$model" = "resolved-model" ]
`
	if err := os.WriteFile(fakeCodex, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", fakeCodex, err)
	}

	for _, test := range []struct {
		name string
		base string
	}{
		{name: "long option", base: shellSingleQuote(fakeCodex) + " --model old-model"},
		{name: "long inline option", base: shellSingleQuote(fakeCodex) + " --model=old-model"},
		{name: "short option", base: shellSingleQuote(fakeCodex) + " -m old-model"},
		{name: "short equals option", base: shellSingleQuote(fakeCodex) + " -m=old-model"},
		{name: "short attached option", base: shellSingleQuote(fakeCodex) + " -mold-model"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command, err := codexCommandForAgentProfile(test.base, AgentProfile{
				Name: "coder", Model: "resolved-model", ReasoningEffort: "high",
			})
			if err != nil {
				t.Fatalf("codexCommandForAgentProfile() error = %v", err)
			}
			output, err := exec.Command("sh", "-c", command).CombinedOutput()
			if err != nil {
				t.Fatalf("executable command failed: %v: %s\ncommand=%s", err, output, command)
			}
		})
	}
}

func TestTmuxRunnerStartRejectsInvalidProfileBeforeCreatingSession(t *testing.T) {
	fakeExec := &fakeCommandExecutor{}
	runner := NewTmuxRunner("codex", fakeExec)
	_, err := runner.Start(Agent{
		ID:           "invalid-profile",
		WorktreePath: t.TempDir(),
		RuntimeProfile: AgentProfile{
			Name: "coder", Model: "gpt-5.6-sol", ReasoningEffort: "impossible",
		},
	}, "work")
	if err == nil || !strings.Contains(err.Error(), `unsupported reasoning effort "impossible"`) {
		t.Fatalf("Start() error = %v, want unsupported effort", err)
	}
	if len(fakeExec.calls) != 0 {
		t.Fatalf("tmux calls = %#v, want no launch side effects", fakeExec.calls)
	}
}

func TestTmuxRunnerSendMultiline(t *testing.T) {
	fakeExec := &fakeCommandExecutor{}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-agent-1"}

	if err := runner.Send(handle, "first\n\nthird"); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if len(fakeExec.calls) != 3 {
		t.Fatalf("commands = %#v, want load-buffer, paste-buffer, Enter", fakeExec.calls)
	}
	bufferName := fakeExec.calls[0].args[2]
	bufferPath := fakeExec.calls[0].args[3]
	wantCalls := []fakeCommandCall{
		{name: "tmux", args: []string{"load-buffer", "-b", bufferName, bufferPath}},
		{name: "tmux", args: []string{"paste-buffer", "-d", "-p", "-t", handle.Session, "-b", bufferName}},
		{name: "tmux", args: []string{"send-keys", "-t", handle.Session, "Enter"}},
	}
	if !reflect.DeepEqual(fakeExec.calls, wantCalls) {
		t.Fatalf("commands = %#v, want %#v", fakeExec.calls, wantCalls)
	}
	if got, want := fakeExec.loadedBuffers[bufferName], "first\n\nthird"; got != want {
		t.Fatalf("loaded buffer content = %q, want %q", got, want)
	}
}

func TestTmuxRunnerSendDeliversLargePromptViaPasteBuffer(t *testing.T) {
	fakeExec := &fakeCommandExecutor{}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-agent-1"}
	// A large, multi-hundred-KB prompt: with keystroke-simulated chunking
	// this used to require dozens of separate "send-keys -l" invocations;
	// with a paste buffer it's a single load-buffer + paste-buffer pair
	// regardless of size.
	prompt := strings.Repeat("a", 250_000) + "界" + "\r\n" + strings.Repeat("b", 17)

	if err := runner.Send(handle, prompt); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if len(fakeExec.calls) != 3 {
		t.Fatalf(
			"commands = %#v, want exactly load-buffer, paste-buffer, Enter regardless of prompt size",
			fakeExec.calls,
		)
	}
	bufferName := fakeExec.calls[0].args[2]
	normalized := strings.ReplaceAll(prompt, "\r\n", "\n")
	if got := fakeExec.loadedBuffers[bufferName]; got != normalized {
		t.Fatalf(
			"loaded buffer content length = %d, want %d",
			len(got),
			len(normalized),
		)
	}
}

func TestTmuxRunnerSendReportsLoadBufferFailure(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		errs: map[int]error{0: errors.New("load-buffer failed")},
	}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-agent-1"}

	err := runner.Send(handle, "some prompt text")
	if err == nil || !strings.Contains(err.Error(), "failed to load prompt text") {
		t.Fatalf("Send() error = %v, want load-buffer failure", err)
	}
	if len(fakeExec.calls) != 1 {
		t.Fatalf("commands = %d, want send to stop after failed load-buffer", len(fakeExec.calls))
	}
}

func TestTmuxRunnerSendReportsPasteBufferFailure(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		errs: map[int]error{1: errors.New("paste-buffer failed")},
	}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-agent-1"}

	err := runner.Send(handle, "some prompt text")
	if err == nil || !strings.Contains(err.Error(), "failed to paste prompt text") {
		t.Fatalf("Send() error = %v, want paste-buffer failure", err)
	}
	if len(fakeExec.calls) != 3 {
		t.Fatalf("commands = %d, want load, failed paste, and buffer cleanup", len(fakeExec.calls))
	}
	bufferName := fakeExec.calls[0].args[2]
	wantCleanup := fakeCommandCall{
		name: "tmux",
		args: []string{"delete-buffer", "-b", bufferName},
	}
	if !reflect.DeepEqual(fakeExec.calls[2], wantCleanup) {
		t.Fatalf(
			"cleanup call = %#v, want %#v",
			fakeExec.calls[2],
			wantCleanup,
		)
	}
}

func TestTmuxRunnerStartUsesRuntimeCWDWhenSet(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			4: []byte("OpenAI Codex"),
			5: []byte("OpenAI Codex"),
		},
	}
	runner := NewTmuxRunner("", fakeExec)
	worktree := t.TempDir()
	agent := Agent{
		ID:           "agent-8-1700000001",
		IssueNumber:  8,
		WorktreePath: worktree,
		RuntimeCWD:   "/tmp/repo-root",
		BranchName:   "repository-agent-orchestrator/issue-8",
	}

	_, err := runner.Start(agent, "prompt")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if len(fakeExec.calls) == 0 {
		t.Fatal("expected tmux calls")
	}
	first := fakeExec.calls[0]
	want := fakeCommandCall{name: "tmux", args: []string{"new-session", "-d", "-s", "repository-agent-orchestrator-agent-8-1700000001", "-c", "/tmp/repo-root"}}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("first command = %#v, want %#v", first, want)
	}
}

func TestTmuxRunnerWaitForCodexReadySendsEnterWhenPrompted(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			0: []byte("Press enter to continue"),
			2: []byte("OpenAI Codex"),
			3: []byte("OpenAI Codex"),
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)

	if err := runner.waitForCodexReady("repository-agent-orchestrator-agent-1"); err != nil {
		t.Fatalf("waitForCodexReady() error = %v", err)
	}

	wantCalls := []fakeCommandCall{
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", "repository-agent-orchestrator-agent-1", "-S", codexReadyProbeLines}},
		{name: "tmux", args: []string{"send-keys", "-t", "repository-agent-orchestrator-agent-1", "Enter"}},
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", "repository-agent-orchestrator-agent-1", "-S", codexReadyProbeLines}},
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", "repository-agent-orchestrator-agent-1", "-S", codexReadyProbeLines}},
	}
	if !reflect.DeepEqual(fakeExec.calls, wantCalls) {
		t.Fatalf("commands = %#v, want %#v", fakeExec.calls, wantCalls)
	}
}

func TestTmuxRunnerWaitForCodexReadyWaitsOutMCPServerBoot(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			0: []byte("OpenAI Codex\nBooting MCP server: codex_apps (0s • esc to interrupt)"),
			1: []byte("OpenAI Codex\n›"),
			2: []byte("OpenAI Codex\n›"),
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)

	if err := runner.waitForCodexReady("repository-agent-orchestrator-agent-1"); err != nil {
		t.Fatalf("waitForCodexReady() error = %v", err)
	}

	wantCalls := []fakeCommandCall{
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", "repository-agent-orchestrator-agent-1", "-S", codexReadyProbeLines}},
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", "repository-agent-orchestrator-agent-1", "-S", codexReadyProbeLines}},
		{name: "tmux", args: []string{"capture-pane", "-p", "-t", "repository-agent-orchestrator-agent-1", "-S", codexReadyProbeLines}},
	}
	if !reflect.DeepEqual(fakeExec.calls, wantCalls) {
		t.Fatalf("commands = %#v, want %#v", fakeExec.calls, wantCalls)
	}
}

func TestTmuxRunnerWaitForCodexReadyRequiresTwoConsecutivePolls(t *testing.T) {
	// A busy indicator that flickers away for a single poll and then
	// returns must not be treated as ready: codexReadyConsecutivePolls
	// requires the idle state to hold across more than one poll.
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			0: []byte("OpenAI Codex\nBooting MCP server: codex_apps (0s \u2022 esc to interrupt)"),
			1: []byte("OpenAI Codex\n\u203a"),
			2: []byte("OpenAI Codex\nBooting MCP server: codex_apps (1s \u2022 esc to interrupt)"),
			3: []byte("OpenAI Codex\n\u203a"),
			4: []byte("OpenAI Codex\n\u203a"),
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)

	if err := runner.waitForCodexReady("repository-agent-orchestrator-agent-1"); err != nil {
		t.Fatalf("waitForCodexReady() error = %v", err)
	}
	if len(fakeExec.calls) != 5 {
		t.Fatalf(
			"capture-pane calls = %d, want 5 (flicker at poll 3 must reset the consecutive count)",
			len(fakeExec.calls),
		)
	}
}

func TestTmuxRunnerWaitForCodexReadyWaitsOutUnresolvedBanner(t *testing.T) {
	// Reproduces a captured production stall: the "OpenAI Codex" banner
	// text is present from the very first frame, well before Codex has
	// finished resolving its own model/directory fields. Sending input
	// during that window has been observed to leave Codex permanently
	// stuck later on (an MCP server boot that never completes), not just
	// queued for late processing. codexReady() must wait for the banner
	// itself to settle, not just appear.
	loadingBanner := "" +
		"\u256d\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u256e\n" +
		"\u2502 >_ OpenAI Codex (v0.148.0)                       \u2502\n" +
		"\u2502                                                  \u2502\n" +
		"\u2502 model:       loading   /model to change          \u2502\n" +
		"\u2502 directory:   loading                             \u2502\n" +
		"\u2570\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u256f"
	settledBanner := "" +
		"\u256d\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u256e\n" +
		"\u2502 >_ OpenAI Codex (v0.148.0)                       \u2502\n" +
		"\u2502                                                  \u2502\n" +
		"\u2502 model:       gpt-5.6-sol high   /model to change \u2502\n" +
		"\u2502 directory:   ~/zeekfoundry/.worktrees/issue-980  \u2502\n" +
		"\u2502 permissions: YOLO mode                           \u2502\n" +
		"\u2570\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u256f"
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			0: []byte(loadingBanner),
			1: []byte(settledBanner),
			2: []byte(settledBanner),
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)

	if err := runner.waitForCodexReady("repository-agent-orchestrator-agent-1"); err != nil {
		t.Fatalf("waitForCodexReady() error = %v", err)
	}
	if len(fakeExec.calls) != 3 {
		t.Fatalf(
			"capture-pane calls = %d, want 3 (must not treat an unresolved banner as ready)",
			len(fakeExec.calls),
		)
	}
}

func TestTmuxRunnerStopMissingSessionReturnsNil(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		errs: map[int]error{
			0: fakeExitError{code: 1},
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "does-not-exist"}

	if err := runner.Stop(handle); err != nil {
		t.Fatalf("Stop() error = %v, want nil", err)
	}
}

func TestTmuxRunnerIsAliveMissingSessionReturnsFalse(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		errs: map[int]error{
			0: fakeExitError{code: 1},
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "does-not-exist"}

	alive, err := runner.IsAlive(handle)
	if err != nil {
		t.Fatalf("IsAlive() error = %v", err)
	}
	if alive {
		t.Fatal("IsAlive() = true, want false")
	}
}

func TestTmuxRunnerCaptureReadsPaneContent(t *testing.T) {
	fakeExec := &fakeCommandExecutor{
		outs: map[int][]byte{
			0: []byte("waiting for input"),
		},
	}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-agent-1"}

	got, err := runner.Capture(handle, 50)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if got != "waiting for input" {
		t.Fatalf("Capture() = %q, want %q", got, "waiting for input")
	}

	wantCalls := []fakeCommandCall{
		{name: "tmux", args: []string{"capture-pane", "-p", "-J", "-t", "repository-agent-orchestrator-agent-1", "-S", "-50"}},
	}
	if !reflect.DeepEqual(fakeExec.calls, wantCalls) {
		t.Fatalf("commands = %#v, want %#v", fakeExec.calls, wantCalls)
	}
}

func TestRuntimeLogPathUsesAgentLogDir(t *testing.T) {
	agent := Agent{
		ID:     "coding-agent-42-1700000000",
		LogDir: filepath.Join(t.TempDir(), "repo-logs"),
	}

	got, err := runtimeLogPath(agent)
	if err != nil {
		t.Fatalf("runtimeLogPath() error = %v", err)
	}
	want := filepath.Join(agent.LogDir, "coding-agent-42-1700000000.log")
	if got != want {
		t.Fatalf("runtimeLogPath() = %q, want %q", got, want)
	}
}

func TestTmuxRunnerStartErrorPaths(t *testing.T) {
	t.Run("empty worktree path", func(t *testing.T) {
		runner := NewTmuxRunner("codex", &fakeCommandExecutor{})
		_, err := runner.Start(Agent{ID: "agent-1"}, "prompt")
		if err == nil || !strings.Contains(err.Error(), "worktree path is empty") {
			t.Fatalf("Start() error = %v, want empty worktree path error", err)
		}
	})

	t.Run("new session failure", func(t *testing.T) {
		fakeExec := &fakeCommandExecutor{errs: map[int]error{0: errors.New("tmux down")}}
		runner := NewTmuxRunner("codex", fakeExec)
		agent := Agent{ID: "agent-1", WorktreePath: t.TempDir()}
		_, err := runner.Start(agent, "prompt")
		if err == nil || !strings.Contains(err.Error(), "failed to create tmux session") {
			t.Fatalf("Start() error = %v, want new-session failure", err)
		}
	})

	t.Run("pipe-pane failure stops session", func(t *testing.T) {
		fakeExec := &fakeCommandExecutor{errs: map[int]error{1: errors.New("pipe failed")}}
		runner := NewTmuxRunner("codex", fakeExec)
		agent := Agent{ID: "agent-2", WorktreePath: t.TempDir()}
		_, err := runner.Start(agent, "prompt")
		if err == nil || !strings.Contains(err.Error(), "failed to configure tmux log piping") {
			t.Fatalf("Start() error = %v, want pipe-pane failure", err)
		}
		if len(fakeExec.calls) < 3 || fakeExec.calls[2].args[0] != "kill-session" {
			t.Fatalf("expected kill-session cleanup after pipe-pane failure, calls=%#v", fakeExec.calls)
		}
	})

	t.Run("runtime command failure stops session", func(t *testing.T) {
		fakeExec := &fakeCommandExecutor{errs: map[int]error{2: errors.New("send failed")}}
		runner := NewTmuxRunner("codex", fakeExec)
		agent := Agent{ID: "agent-3", WorktreePath: t.TempDir()}
		_, err := runner.Start(agent, "prompt")
		if err == nil || !strings.Contains(err.Error(), "failed to launch codex in tmux session") {
			t.Fatalf("Start() error = %v, want runtime command failure", err)
		}
		if len(fakeExec.calls) < 4 || fakeExec.calls[3].args[0] != "kill-session" {
			t.Fatalf("expected kill-session cleanup after runtime command failure, calls=%#v", fakeExec.calls)
		}
	})

	t.Run("readiness probe failure stops session", func(t *testing.T) {
		fakeExec := &fakeCommandExecutor{errs: map[int]error{4: errors.New("capture failed")}}
		runner := NewTmuxRunner("codex", fakeExec)
		agent := Agent{ID: "agent-4", WorktreePath: t.TempDir()}
		_, err := runner.Start(agent, "prompt")
		if err == nil || !strings.Contains(err.Error(), "failed to detect codex readiness") {
			t.Fatalf("Start() error = %v, want readiness failure", err)
		}
		if len(fakeExec.calls) < 6 || fakeExec.calls[5].args[0] != "kill-session" {
			t.Fatalf("expected kill-session cleanup after readiness failure, calls=%#v", fakeExec.calls)
		}
	})

	t.Run("initial prompt failure stops session", func(t *testing.T) {
		fakeExec := &fakeCommandExecutor{
			errs: map[int]error{6: errors.New("prompt send failed")},
			outs: map[int][]byte{
				4: []byte("OpenAI Codex"),
				5: []byte("OpenAI Codex"),
			},
		}
		runner := NewTmuxRunner("codex", fakeExec)
		agent := Agent{ID: "agent-5", WorktreePath: t.TempDir()}
		_, err := runner.Start(agent, "prompt")
		if err == nil || !strings.Contains(err.Error(), "failed to send initial prompt to codex") {
			t.Fatalf("Start() error = %v, want prompt send failure", err)
		}
		if len(fakeExec.calls) < 8 || fakeExec.calls[7].args[0] != "kill-session" {
			t.Fatalf("expected kill-session cleanup after prompt failure, calls=%#v", fakeExec.calls)
		}
	})
}

func TestTmuxRunnerSendStopCaptureAndIsAliveErrorBranches(t *testing.T) {
	runner := NewTmuxRunner("codex", &fakeCommandExecutor{})

	if err := runner.Send(RuntimeHandle{Kind: RuntimeKind("other"), Session: "x"}, "hello"); err == nil {
		t.Fatal("Send() unsupported runtime kind should fail")
	}
	if err := runner.Send(RuntimeHandle{Kind: RuntimeKindTmux}, "hello"); err == nil {
		t.Fatal("Send() empty session should fail")
	}

	if err := runner.Stop(RuntimeHandle{Kind: RuntimeKind("other"), Session: "x"}); err == nil {
		t.Fatal("Stop() unsupported runtime kind should fail")
	}

	if _, err := runner.IsAlive(RuntimeHandle{Kind: RuntimeKind("other"), Session: "x"}); err == nil {
		t.Fatal("IsAlive() unsupported runtime kind should fail")
	}

	if _, err := runner.Capture(RuntimeHandle{Kind: RuntimeKind("other"), Session: "x"}, 20); err == nil {
		t.Fatal("Capture() unsupported runtime kind should fail")
	}
}

func TestTmuxRunnerCaptureErrorBranches(t *testing.T) {
	fakeExec := &fakeCommandExecutor{errs: map[int]error{0: errors.New("capture-pane failed")}}
	runner := NewTmuxRunner("codex", fakeExec)
	handle := RuntimeHandle{Kind: RuntimeKindTmux, Session: "repository-agent-orchestrator-agent-1"}

	if _, err := runner.Capture(handle, 10); err == nil || !strings.Contains(err.Error(), "failed to capture tmux output") {
		t.Fatalf("Capture() error = %v, want wrapped capture failure", err)
	}

	fakeMissing := &fakeCommandExecutor{errs: map[int]error{0: fakeExitError{code: 1}}}
	runnerMissing := NewTmuxRunner("codex", fakeMissing)
	got, err := runnerMissing.Capture(handle, 10)
	if err != nil {
		t.Fatalf("Capture() missing session error = %v", err)
	}
	if got != "" {
		t.Fatalf("Capture() missing session = %q, want empty", got)
	}
}

func TestTmuxHelperUtilities(t *testing.T) {
	if got := shellSingleQuote(""); got != "''" {
		t.Fatalf("shellSingleQuote(\"\") = %q, want %q", got, "''")
	}
	if got := shellSingleQuote("a'b"); got != "'a'\\''b'" {
		t.Fatalf("shellSingleQuote(\"a'b\") = %q, want %q", got, "'a'\\''b'")
	}

	if got := runtimeLaunchCommand("codex --profile ci", "darwin"); got != "env -u MallocStackLogging -u MallocStackLoggingNoCompact codex --profile ci" {
		t.Fatalf("runtimeLaunchCommand(darwin) = %q", got)
	}
	if got := runtimeLaunchCommand("codex --profile ci", "linux"); got != "codex --profile ci" {
		t.Fatalf("runtimeLaunchCommand(non-darwin) = %q", got)
	}

	if _, err := runtimeLogPath(Agent{ID: "!!!"}); err == nil {
		t.Fatal("runtimeLogPath() for unsafe-only id should fail")
	}

	parentFile := filepath.Join(t.TempDir(), "not-dir")
	if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%s) error = %v", parentFile, err)
	}
	if err := ensureRuntimeLogFile(filepath.Join(parentFile, "child.log")); err == nil {
		t.Fatal("ensureRuntimeLogFile(file/child.log) error = nil, want error")
	}
}

func TestPruneRuntimeLogsForNewSessionKeepsLastTenAgents(t *testing.T) {
	logDir := t.TempDir()
	currentLogPath := filepath.Join(logDir, "review-worker-current.log")
	baseTime := time.Now().Add(-24 * time.Hour)

	// Add twelve historical review-worker runtime logs, plus the current
	// one we should preserve. Review workers are the only role that mints
	// a genuinely new log file per session (issue #150), so this cap only
	// applies to filenames carrying the review-worker prefix.
	for i := 0; i < 12; i++ {
		path := filepath.Join(logDir, fmt.Sprintf("review-worker-%02d.log", i))
		if err := os.WriteFile(path, []byte("log\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path, err)
		}
		ts := baseTime.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatalf("Chtimes(%s) error = %v", path, err)
		}
	}
	if err := os.WriteFile(currentLogPath, []byte("current\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", currentLogPath, err)
	}
	oldTS := baseTime.Add(-time.Hour)
	if err := os.Chtimes(currentLogPath, oldTS, oldTS); err != nil {
		t.Fatalf("Chtimes(%s) error = %v", currentLogPath, err)
	}

	daemonLogPath := filepath.Join(logDir, orchestratorDaemonLogName)
	if err := os.WriteFile(daemonLogPath, []byte("daemon\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", daemonLogPath, err)
	}

	if err := pruneRuntimeLogsForNewSession(currentLogPath, maxRuntimeAgentLogs); err != nil {
		t.Fatalf("pruneRuntimeLogsForNewSession() error = %v", err)
	}

	runtimeLogs, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	if err != nil {
		t.Fatalf("Glob(%s/*.log) error = %v", logDir, err)
	}
	agentLogs := make([]string, 0, len(runtimeLogs))
	for _, path := range runtimeLogs {
		if filepath.Base(path) == orchestratorDaemonLogName {
			continue
		}
		agentLogs = append(agentLogs, filepath.Base(path))
	}
	if got, want := len(agentLogs), maxRuntimeAgentLogs; got != want {
		t.Fatalf("agent log count = %d, want %d; logs=%v", got, want, agentLogs)
	}
	if _, err := os.Stat(currentLogPath); err != nil {
		t.Fatalf("current log missing after prune: %v", err)
	}
	if _, err := os.Stat(daemonLogPath); err != nil {
		t.Fatalf("daemon log missing after prune: %v", err)
	}
	if _, err := os.Stat(filepath.Join(logDir, "review-worker-00.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest runtime log should be pruned, stat error = %v", err)
	}
}

// TestPruneRuntimeLogsForNewSessionNeverPrunesCoderOrReviewerLogs is the
// regression test for issue #150: a coder or reviewer mints its agent ID
// once at initial launch and every later restart reuses that same log
// path (opened in append mode), so it never accumulates the way a burst
// of short-lived review workers does. Applying the shared-directory cap
// on every session start regardless of role meant that starting enough
// review worker sessions -- routine during a single busy review round --
// could push an unrelated, currently-active coder's own log past the cap
// and delete it, even though nothing about that coder was actually stale.
// This reproduces exactly that shape: many review-worker logs plus one
// coder log, all older than a brand new review-worker session about to
// start, and asserts the coder log survives regardless of the worker
// count.
func TestPruneRuntimeLogsForNewSessionNeverPrunesCoderOrReviewerLogs(t *testing.T) {
	logDir := t.TempDir()
	coderLogPath := filepath.Join(logDir, "coding-agent-982-1787714062.log")
	reviewerLogPath := filepath.Join(logDir, "review-agent-982-1787769866768205258.log")
	baseTime := time.Now().Add(-24 * time.Hour)

	if err := os.WriteFile(coderLogPath, []byte("coder log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", coderLogPath, err)
	}
	if err := os.Chtimes(coderLogPath, baseTime, baseTime); err != nil {
		t.Fatalf("Chtimes(%s) error = %v", coderLogPath, err)
	}
	if err := os.WriteFile(reviewerLogPath, []byte("reviewer log\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", reviewerLogPath, err)
	}
	if err := os.Chtimes(reviewerLogPath, baseTime, baseTime); err != nil {
		t.Fatalf("Chtimes(%s) error = %v", reviewerLogPath, err)
	}

	// Far more than maxRuntimeAgentLogs worker logs, all newer than the
	// coder/reviewer logs above -- exactly what one busy review round
	// produces (discovery passes x lanes, plus verifiers and challenge
	// workers).
	for i := 0; i < maxRuntimeAgentLogs+5; i++ {
		path := filepath.Join(logDir, fmt.Sprintf("review-worker-existing-%02d.log", i))
		if err := os.WriteFile(path, []byte("worker log\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path, err)
		}
		ts := baseTime.Add(time.Duration(i+1) * time.Minute)
		if err := os.Chtimes(path, ts, ts); err != nil {
			t.Fatalf("Chtimes(%s) error = %v", path, err)
		}
	}

	// A brand new review worker session starts, the same way one would
	// during that review round.
	newWorkerLogPath := filepath.Join(logDir, "review-worker-new.log")
	if err := pruneRuntimeLogsForNewSession(newWorkerLogPath, maxRuntimeAgentLogs); err != nil {
		t.Fatalf("pruneRuntimeLogsForNewSession(worker) error = %v", err)
	}
	if _, err := os.Stat(coderLogPath); err != nil {
		t.Fatalf("coder log must survive a review-worker session start: %v", err)
	}
	if _, err := os.Stat(reviewerLogPath); err != nil {
		t.Fatalf("reviewer log must survive a review-worker session start: %v", err)
	}

	// The worker session start above legitimately pruned the oldest worker
	// logs down to the cap (maxRuntimeAgentLogs-1, since the new worker's
	// own not-yet-created log counts toward the cap too) -- that part of
	// the behavior is unchanged and still exercised by
	// TestPruneRuntimeLogsForNewSessionKeepsLastTenAgents above. What
	// matters here is that the coder and reviewer logs survived it.
	afterWorkerPrune, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", logDir, err)
	}
	countAfterWorkerPrune := len(afterWorkerPrune)

	// A plain coder or reviewer session restart (reusing its own stable
	// log path) must not prune anything in the shared directory at all,
	// regardless of how many files are present in total.
	if err := pruneRuntimeLogsForNewSession(coderLogPath, maxRuntimeAgentLogs); err != nil {
		t.Fatalf("pruneRuntimeLogsForNewSession(coder) error = %v", err)
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", logDir, err)
	}
	if got, want := len(entries), countAfterWorkerPrune; got != want {
		t.Fatalf("log file count after coder session start = %d, want %d (nothing should be pruned)", got, want)
	}
	if _, err := os.Stat(coderLogPath); err != nil {
		t.Fatalf("coder log must survive its own session start: %v", err)
	}
	if _, err := os.Stat(reviewerLogPath); err != nil {
		t.Fatalf("reviewer log must survive an unrelated coder session start: %v", err)
	}
}
