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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/term"
)

type failingWriter struct{}

func (f failingWriter) Write(p []byte) (int, error) {
	return 0, errors.New("write failed")
}

type printRunner struct {
	stubRunner
	alive    bool
	aliveErr error
}

func (r *printRunner) IsAlive(handle RuntimeHandle) (bool, error) {
	return r.alive, r.aliveErr
}

func TestRunREPLCommandBranches(t *testing.T) {
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:           "acme",
			RepoName:            "widget",
			RepoPath:            "/tmp/repo",
			WorktreeDir:         "/tmp/repo/.worktrees",
			LogDir:              "/tmp/repository-agent-orchestrator/widget",
			BaseBranch:          "main",
			PollIntervalSeconds: 20,
		},
		agents: NewAgentManager(),
		runner: &stubRunner{},
	}

	input := strings.Join([]string{
		"list",
		"agent list",
		"agent",
		"agent cleanup missing-agent",
		"agent index",
		"agent index repo extra",
		"agent st",
		"agent list extra",
		"agent start nope",
		"agent review nope",
		"agent pause missing-agent",
		"agent unpause missing-agent",
		"agent stop missing-agent",
		"agent steer missing-agent hello",
		"status",
		"quit",
		"",
	}, "\n")

	output, err := runREPLForTest(t, input, bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}

	checks := []string{
		"unknown command; type 'help'",
		"No agents.",
		"usage: agent <help|cleanup|continue|index|list|pause|review|start|steer|stop|tail|unpause> ...",
		"usage: agent cleanup [coder|reviewer] <issueOrPRNumber>",
		"usage: agent index repo",
		"ambiguous agent command \"st\"; matches: start, steer, stop",
		"usage: agent start <issueNumber>",
		"usage: agent review <prNumber>",
		"usage: agent pause [coder|reviewer] <issueNumber>",
		"usage: agent unpause [coder|reviewer] <issueNumber>",
		"usage: agent stop [coder|reviewer] <issueOrPRNumber>",
		"usage: agent steer [coder|reviewer] <issueNumber> <text...>",
		"repo: acme/widget",
	}
	for _, needle := range checks {
		if !strings.Contains(output, needle) {
			t.Fatalf("runREPL output missing %q: %q", needle, output)
		}
	}
}

func TestPrintAgentsRuntimeStatusFormatting(t *testing.T) {
	agents := NewAgentManager()
	withRuntime := &Agent{
		ID:               "coding-agent-2",
		IssueNumber:      2,
		BranchName:       "repository-agent-orchestrator/issue-2",
		PRNumber:         2,
		PRTitle:          "Fix issue two",
		PRURL:            "https://github.com/acme/widget/pull/2",
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "session-2",
			LogPath: "/tmp/repository-agent-orchestrator/agent-2.log",
		},
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	withoutRuntime := &Agent{
		ID:                   "coding-agent-1",
		IssueNumber:          1,
		BranchName:           "repository-agent-orchestrator/issue-1",
		State:                StateWorking,
		LastActivityTime:     time.Now().Add(-time.Minute),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	reviewGate := &Agent{
		ID:                "review-agent-3",
		Role:              RoleReviewer,
		IssueNumber:       3,
		PRNumber:          44,
		BranchName:        "repository-agent-orchestrator/issue-3",
		State:             StateReviewGate,
		ObservedPRHeadSHA: "deadbeefcafebabe",
		LastActivityTime:  time.Now().Add(-2 * time.Minute),
	}
	if err := agents.Add(withRuntime); err != nil {
		t.Fatalf("Add(withRuntime) error = %v", err)
	}
	if err := agents.Add(withoutRuntime); err != nil {
		t.Fatalf("Add(withoutRuntime) error = %v", err)
	}
	if err := agents.Add(reviewGate); err != nil {
		t.Fatalf("Add(reviewGate) error = %v", err)
	}
	manualReview := &Agent{
		ID:               "review-agent-88",
		Role:             RoleReviewer,
		IssueNumber:      0,
		PRNumber:         88,
		PRTitle:          "Detached review",
		PRURL:            "https://github.com/acme/widget/pull/88",
		BranchName:       "feature/detached-review",
		State:            StateWorking,
		LastActivityTime: time.Now().Add(-time.Minute),
	}
	if err := agents.Add(manualReview); err != nil {
		t.Fatalf("Add(manualReview) error = %v", err)
	}

	bot := &Orchestrator{agents: agents, runner: &printRunner{alive: true}}
	out := captureStdout(t, func() { printAgents(bot) })
	for _, needle := range []string{"Active Agents", "[issue #2", "pr url: https://github.com/acme/widget/pull/2", "coder runtime: tmux session=session-2 alive=true", "coder log: /tmp/repository-agent-orchestrator/agent-2.log", "reviewer runtime: none", "reviewer phase: mandatory-tests", "reviewer head: deadbeefcafe", "reviewer gate log: /tmp/repository-agent-orchestrator/review-agent-3-gate.log", "Manual Reviews", "[pr #88: Detached review]", "mode: detached manual review"} {
		if !strings.Contains(out, needle) {
			t.Fatalf("printAgents() output missing %q: %q", needle, out)
		}
	}

	botErr := &Orchestrator{agents: agents, runner: &printRunner{aliveErr: errors.New("tmux down")}}
	outErr := captureStdout(t, func() { printAgents(botErr) })
	if !strings.Contains(outErr, "alive=unknown") {
		t.Fatalf("printAgents() error output missing alive=unknown: %q", outErr)
	}
}

func TestScannerLineReaderBranches(t *testing.T) {
	reader := &scannerLineReader{
		scanner: bufio.NewScanner(strings.NewReader("hello\n")),
		writer:  failingWriter{},
	}
	if _, err := reader.ReadLine("prompt> "); err == nil {
		t.Fatal("ReadLine() error = nil, want write error")
	}

	eofReader := &scannerLineReader{
		scanner: bufio.NewScanner(strings.NewReader("")),
		writer:  &bytes.Buffer{},
	}
	if _, err := eofReader.ReadLine("prompt> "); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadLine() error = %v, want io.EOF", err)
	}

	longInput := strings.Repeat("x", 70*1024)
	errReader := &scannerLineReader{
		scanner: bufio.NewScanner(strings.NewReader(longInput)),
		writer:  &bytes.Buffer{},
	}
	if _, err := errReader.ReadLine("prompt> "); err == nil {
		t.Fatal("ReadLine(long input) error = nil, want scanner error")
	}
}

func TestTerminalLineReaderAdditionalBranches(t *testing.T) {
	readerBackspace := &terminalLineReader{reader: bufio.NewReader(strings.NewReader("abc\b\r")), writer: &bytes.Buffer{}}
	line, err := readerBackspace.ReadLine("prompt> ")
	if err != nil {
		t.Fatalf("ReadLine(backspace) error = %v", err)
	}
	if line != "ab" {
		t.Fatalf("ReadLine(backspace) = %q, want %q", line, "ab")
	}

	ctrlOut := &bytes.Buffer{}
	readerCtrlC := &terminalLineReader{reader: bufio.NewReader(strings.NewReader("\x03")), writer: ctrlOut}
	line, err = readerCtrlC.ReadLine("prompt> ")
	if !errors.Is(err, errREPLInterrupted) {
		t.Fatalf("ReadLine(ctrl-c) error = %v, want errREPLInterrupted", err)
	}
	if line != "" {
		t.Fatalf("ReadLine(ctrl-c) = %q, want empty", line)
	}
	if !strings.Contains(ctrlOut.String(), "^C") {
		t.Fatalf("ctrl-c output = %q, want caret marker", ctrlOut.String())
	}

	readerEOFPartial := &terminalLineReader{reader: bufio.NewReader(strings.NewReader("partial")), writer: &bytes.Buffer{}}
	line, err = readerEOFPartial.ReadLine("prompt> ")
	if err != nil {
		t.Fatalf("ReadLine(eof-partial) error = %v", err)
	}
	if line != "partial" {
		t.Fatalf("ReadLine(eof-partial) = %q, want %q", line, "partial")
	}

	readerExtendedSeq := &terminalLineReader{reader: bufio.NewReader(strings.NewReader("ab\x1b[3~c\r")), writer: &bytes.Buffer{}}
	line, err = readerExtendedSeq.ReadLine("prompt> ")
	if err != nil {
		t.Fatalf("ReadLine(extended seq) error = %v", err)
	}
	if line != "abc" {
		t.Fatalf("ReadLine(extended seq) = %q, want %q", line, "abc")
	}

	closer := &terminalLineReader{}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	closerWithState := &terminalLineReader{fd: -1, state: &term.State{}}
	if err := closerWithState.Close(); err == nil {
		t.Fatal("Close() with invalid terminal state should fail")
	}
}

type interruptStubLineReader struct {
	err error
}

func (r *interruptStubLineReader) ReadLine(prompt string) (string, error) {
	return "", r.err
}

func (r *interruptStubLineReader) Close() error {
	return nil
}

func TestRunREPLWithLineReaderInterruptCancelsAndExits(t *testing.T) {
	reader := &interruptStubLineReader{err: errREPLInterrupted}
	bot := &Orchestrator{agents: NewAgentManager()}

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	cancelCalled := false
	cancel := func() {
		cancelCalled = true
		ctxCancel()
	}

	if err := runREPLWithLineReader(ctx, bot, cancel, reader); err != nil {
		t.Fatalf("runREPLWithLineReader() error = %v, want nil", err)
	}
	if !cancelCalled {
		t.Fatal("cancel func was not called on REPL interrupt")
	}
	if ctx.Err() == nil {
		t.Fatal("context should be canceled on REPL interrupt")
	}
}

func TestTerminalLineReaderMakeRawFailure(t *testing.T) {
	reader := &terminalLineReader{
		reader: bufio.NewReader(strings.NewReader("")),
		writer: &bytes.Buffer{},
		fd:     -1,
		state:  &term.State{},
	}
	if _, err := reader.ReadLine("prompt> "); err == nil || !strings.Contains(err.Error(), "failed to configure terminal input") {
		t.Fatalf("ReadLine() error = %v, want make-raw failure", err)
	}
}

func TestRunREPLAgentStartFlows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"number": 5, "title": "Issue five", "body": "Body five"},
				{"number": 99, "title": "PR item", "pull_request": map[string]any{"url": "https://api.github.com/repos/acme/widget/pulls/99"}},
			})
		case "/repos/acme/widget/issues/5":
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 5, "title": "Issue five", "body": "Body five"})
		case "/repos/acme/widget/issues/6":
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 6, "title": "Issue six", "body": "Body six"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "stub-session"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			WorktreeDir:    t.TempDir(),
			LogDir:         t.TempDir(),
			BaseBranch:     "main",
			MandatoryTests: []string{"make test"},
		},
		github: newGitHubClientForTest(t, srv),
		agents: NewAgentManager(),
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	output, err := runREPLForTest(t, "agent start next\nagent start 6\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	for _, needle := range []string{"selected issue #5: Issue five", "agent started for issue #5", "agent started for issue #6"} {
		if !strings.Contains(output, needle) {
			t.Fatalf("runREPL output missing %q: %q", needle, output)
		}
	}
	if got := len(runner.started); got != 2 {
		t.Fatalf("runner.Start calls = %d, want 2", got)
	}
}

func TestRunREPLAgentContinueLaunchesExistingPR(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/42":
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 42, "title": "Continue issue", "body": "Body"})
		case "/repos/acme/widget/pulls/57":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":   57,
				"title":    "Existing PR",
				"state":    "open",
				"html_url": "https://github.com/acme/widget/pull/57",
				"base":     map[string]any{"ref": "main"},
				"head": map[string]any{
					"ref":  "feature/existing",
					"sha":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"repo": map[string]any{"full_name": "acme/widget"},
				},
			})
		case "/repos/acme/widget/pulls/57/comments", "/repos/acme/widget/issues/57/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "continuation-session"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		github: newGitHubClientForTest(t, srv),
		agents: NewAgentManager(),
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	output, err := runREPLForTest(t, "agent continue nope 57\nagent continue 42 57\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "usage: agent continue <issueNumber> <prNumber>") {
		t.Fatalf("REPL output missing continuation usage: %q", output)
	}
	if !strings.Contains(output, "agent continued for issue #42 on PR #57") {
		t.Fatalf("REPL output missing continuation success: %q", output)
	}
	if got := len(runner.started); got != 1 || !runner.started[0].AdoptedPR {
		t.Fatalf("continued runtime starts = %#v", runner.started)
	}
}

func TestRunREPLAgentReviewLaunchesReviewerForCurrentHead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/pulls/54":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":   54,
				"html_url": "https://github.com/acme/widget/pull/54",
				"head": map[string]any{
					"sha": testReviewHeadSHA,
				},
			})
		case "/repos/acme/widget/issues/54/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	runner := &stubRunner{}
	agents := NewAgentManager()
	agent := &Agent{
		ID:                   "agent-54",
		Role:                 RoleCoder,
		IssueNumber:          54,
		IssueTitle:           "Fix edge case",
		BranchName:           "repository-agent-orchestrator/issue-54",
		WorktreePath:         t.TempDir(),
		PRNumber:             54,
		PRURL:                "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA:    testReviewHeadSHA,
		LastReviewedHeadSHA:  testReviewHeadSHA,
		State:                StateWorking,
		LastActivityTime:     time.Now(),
		seenReviewCommentIDs: make(map[int64]struct{}),
		seenIssueCommentIDs:  make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
	}

	output, err := runREPLForTest(t, "agent review 54\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "review agent launch started for PR #54") {
		t.Fatalf("runREPL output missing review confirmation: %q", output)
	}

	reviewer := waitForReviewAgentForPR(t, agents, 54)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()

	if got := len(runner.started); got != 0 {
		t.Fatalf("top-level runner.Start calls = %d, want 0", got)
	}
	if reviewer.ParentAgentID != agent.ID {
		t.Fatalf("review coordinator parent = %q, want %q", reviewer.ParentAgentID, agent.ID)
	}
}

func TestRunREPLAgentSteerAndStopSuccess(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-1",
		Role:                    RoleCoder,
		IssueNumber:             1,
		BranchName:              "repository-agent-orchestrator/issue-1",
		WorktreePath:            t.TempDir(),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "session-1",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		agents:    agents,
		runner:    runner,
		messenger: &stubMessenger{},
	}
	output, err := runREPLForTest(t, "agent steer 1 hello world\nagent stop 1\nno\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "message sent to coder for issue #1") {
		t.Fatalf("runREPL output missing steering confirmation: %q", output)
	}
	if !strings.Contains(output, "coder stopped for issue #1") {
		t.Fatalf("runREPL output missing stop confirmation: %q", output)
	}
	if !strings.Contains(output, "Clean up coder for issue #1 now") {
		t.Fatalf("runREPL output missing cleanup prompt: %q", output)
	}
	if !strings.Contains(output, "cleanup skipped; run `agent cleanup coder 1` later") {
		t.Fatalf("runREPL output missing deferred-cleanup guidance: %q", output)
	}
	if got := len(runner.sent); got != 1 {
		t.Fatalf("runner.Send calls = %d, want 1", got)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
}

func TestRunREPLAgentCleanupWorksAfterStop(t *testing.T) {
	branch := "repository-agent-orchestrator/issue-246"
	repoPath, worktreePath := setupRepoWithFeatureWorktree(t, branch)
	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-246",
		Role:                    RoleCoder,
		IssueNumber:             246,
		BranchName:              branch,
		WorktreePath:            worktreePath,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "session-246",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoPath:   repoPath,
			LogDir:     t.TempDir(),
			BaseBranch: "main",
		},
		agents: agents,
		runner: runner,
	}
	output, err := runREPLForTest(t, "agent stop 246\nno\nagent cleanup 246\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "coder stopped for issue #246") {
		t.Fatalf("runREPL output missing stop confirmation: %q", output)
	}
	if !strings.Contains(output, "coder cleaned up for issue #246") {
		t.Fatalf("runREPL output missing cleanup confirmation: %q", output)
	}
	if strings.Contains(output, "no active coder found") {
		t.Fatalf("runREPL output retained stopped-agent resolver failure: %q", output)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
	updated, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found after cleanup", agent.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("agent state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if _, err := os.Stat(worktreePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree path should be removed, got err=%v", err)
	}
	if localBranch := strings.TrimSpace(runGit(t, repoPath, "branch", "--list", branch)); localBranch != "" {
		t.Fatalf("local branch %q should be deleted, output=%q", branch, localBranch)
	}
	if remoteBranch := strings.TrimSpace(runGit(t, repoPath, "ls-remote", "--heads", "origin", branch)); remoteBranch != "" {
		t.Fatalf("remote branch %q should be deleted, output=%q", branch, remoteBranch)
	}
}

func TestRunREPLAgentStopAcceptsImmediateCleanup(t *testing.T) {
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                      "review-agent-247",
		Role:                    RoleReviewer,
		IssueNumber:             247,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-session-247",
		},
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{agents: agents, runner: runner}
	output, err := runREPLForTest(t, "agent stop reviewer 247\nyes\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "Clean up reviewer for issue #247 now") {
		t.Fatalf("runREPL output missing cleanup prompt: %q", output)
	}
	if !strings.Contains(output, "reviewer cleaned up for issue #247") {
		t.Fatalf("runREPL output missing cleanup confirmation: %q", output)
	}
	updated, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after cleanup", reviewer.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
}

func TestRunREPLAgentStopDetachedManualReviewByPR(t *testing.T) {
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                      "review-agent-105",
		Role:                    RoleReviewer,
		IssueNumber:             0,
		PRNumber:                105,
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-session-105",
		},
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{agents: agents, runner: runner}
	output, err := runREPLForTest(t, "agent stop 105\nno\nagent cleanup 105\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	for _, needle := range []string{
		"reviewer stopped for PR #105",
		"Clean up reviewer for PR #105 now",
		"cleanup skipped; run `agent cleanup reviewer 105` later",
		"reviewer cleaned up for PR #105",
	} {
		if !strings.Contains(output, needle) {
			t.Fatalf("runREPL output missing %q: %q", needle, output)
		}
	}
	updated, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after cleanup", reviewer.ID)
	}
	if updated.State != StateDone || !updated.Stopped {
		t.Fatalf("reviewer state = (%s, stopped=%v), want (%s, true)", updated.State, updated.Stopped, StateDone)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
}

func TestRunREPLAgentStopTrackedReviewerByPR(t *testing.T) {
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:               "review-agent-198-199",
		Role:             RoleReviewer,
		IssueNumber:      198,
		PRNumber:         199,
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-session-199",
		},
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{agents: agents, runner: runner}
	output, err := runREPLForTest(
		t,
		"agent stop reviewer 199\nno\nagent cleanup reviewer 199\nquit\n",
		bot,
	)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	for _, needle := range []string{
		"reviewer stopped for issue #198 (PR #199)",
		"Clean up reviewer for issue #198 (PR #199) now",
		"cleanup skipped; run `agent cleanup reviewer 199` later",
		"reviewer cleaned up for issue #198 (PR #199)",
	} {
		if !strings.Contains(output, needle) {
			t.Fatalf("runREPL output missing %q: %q", needle, output)
		}
	}
	updated, ok := agents.Get(reviewer.ID)
	if !ok || updated.State != StateDone || !updated.Stopped {
		t.Fatalf("tracked reviewer after cleanup = %#v", updated)
	}
}

func TestResolveAgentForStopPrefersIssueCoderOverDetachedPR(t *testing.T) {
	agents := NewAgentManager()
	coder := testAgent("coding-agent-105", 105)
	reviewer := testAgent("review-agent-105", 0)
	reviewer.Role = RoleReviewer
	reviewer.PRNumber = 105
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	target, consumed, err := resolveAgentForStop(&Orchestrator{agents: agents}, []string{"105"}, RoleCoder)
	if err != nil {
		t.Fatalf("resolveAgentForStop() error = %v", err)
	}
	if consumed != 1 {
		t.Fatalf("resolveAgentForStop() consumed = %d, want 1", consumed)
	}
	if target.ID != coder.ID {
		t.Fatalf("resolveAgentForStop() target = %q, want issue coder %q", target.ID, coder.ID)
	}
}

func TestRunREPLAgentPauseSuccess(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-2",
		Role:                    RoleCoder,
		IssueNumber:             2,
		BranchName:              "repository-agent-orchestrator/issue-2",
		WorktreePath:            t.TempDir(),
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "session-2",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		agents: agents,
		runner: runner,
	}
	output, err := runREPLForTest(t, "agent pause 2\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "coder paused for issue #2") {
		t.Fatalf("runREPL output missing pause confirmation: %q", output)
	}
	if got := len(runner.stopped); got != 1 {
		t.Fatalf("runner.Stop calls = %d, want 1", got)
	}
}

func TestRunREPLAgentUnpauseSuccess(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "agent-3",
		Role:                    RoleCoder,
		IssueNumber:             3,
		BranchName:              "repository-agent-orchestrator/issue-3",
		WorktreePath:            t.TempDir(),
		State:                   StateWorking,
		Paused:                  true,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "session-3"}}
	bot := &Orchestrator{
		agents: agents,
		runner: runner,
	}
	output, err := runREPLForTest(t, "agent unpause 3\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "coder unpaused for issue #3") {
		t.Fatalf("runREPL output missing unpause confirmation: %q", output)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
}

func TestRunREPLAgentPauseUnknownIssueRejected(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:               "index-agent-4",
		Role:             RoleIndexer,
		IssueNumber:      4,
		BranchName:       "repository-agent-orchestrator/repo-index",
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle:    RuntimeHandle{Kind: RuntimeKindTmux, Session: "index-session"},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	runner := &stubRunner{}
	bot := &Orchestrator{
		agents: agents,
		runner: runner,
	}
	output, err := runREPLForTest(t, "agent pause 4\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "error: no active coder found for issue #4") {
		t.Fatalf("runREPL output missing issue-scoped rejection: %q", output)
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("runner.Stop calls = %d, want 0", got)
	}
}

func TestRunREPLAgentIndexRepoLaunchesIndexer(t *testing.T) {
	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "index-session"}}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			WorktreeDir: t.TempDir(),
			LogDir:      t.TempDir(),
			BaseBranch:  "main",
		},
		agents: NewAgentManager(),
		runner: runner,
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			return nil
		},
		messenger: &stubMessenger{},
	}

	output, err := runREPLForTest(t, "agent index repo\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "repo indexing agent started: index-agent-") {
		t.Fatalf("runREPL output missing repo indexing confirmation: %q", output)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("runner.Start calls = %d, want 1", got)
	}
	if runner.started[0].Role != RoleIndexer {
		t.Fatalf("index launch role = %s, want %s", runner.started[0].Role, RoleIndexer)
	}
}

func TestRunREPLAgentTailByIssue(t *testing.T) {
	agents := NewAgentManager()
	agent := &Agent{
		ID:               "agent-17",
		Role:             RoleCoder,
		IssueNumber:      17,
		BranchName:       "repository-agent-orchestrator/issue-17",
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "session-17",
			LogPath: "/tmp/repository-agent-orchestrator/agent-17.log",
		},
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add(agent) error = %v", err)
	}

	called := ""
	bot := &Orchestrator{
		agents: agents,
		tailRunner: func(logPath string) error {
			called = logPath
			return nil
		},
	}

	output, err := runREPLForTest(t, "agent tail 17\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if called != "/tmp/repository-agent-orchestrator/agent-17.log" {
		t.Fatalf("tailRunner log path = %q, want %q", called, "/tmp/repository-agent-orchestrator/agent-17.log")
	}
	if strings.Contains(output, "error:") {
		t.Fatalf("runREPL output unexpectedly contained error: %q", output)
	}
}

func TestRunREPLAgentTailRequiresInteractiveTerminalWithoutID(t *testing.T) {
	bot := &Orchestrator{agents: NewAgentManager()}

	output, err := runREPLForTest(t, "agent tail\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "No agents with runtime logs.") {
		t.Fatalf("runREPL output missing empty tail message: %q", output)
	}
}

func TestRunREPLAgentTailInteractiveSelection(t *testing.T) {
	agents := NewAgentManager()
	first := &Agent{
		ID:               "agent-1",
		Role:             RoleCoder,
		IssueNumber:      1,
		State:            StateWorking,
		LastActivityTime: time.Now().Add(-time.Minute),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "session-1",
			LogPath: "/tmp/repository-agent-orchestrator/agent-1.log",
		},
	}
	second := &Agent{
		ID:               "agent-2",
		Role:             RoleReviewer,
		IssueNumber:      2,
		PRNumber:         44,
		State:            StateWorking,
		LastActivityTime: time.Now(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "session-2",
			LogPath: "/tmp/repository-agent-orchestrator/agent-2.log",
		},
	}
	if err := agents.Add(first); err != nil {
		t.Fatalf("Add(first) error = %v", err)
	}
	if err := agents.Add(second); err != nil {
		t.Fatalf("Add(second) error = %v", err)
	}

	var output bytes.Buffer
	selectedLog := ""
	reader := &terminalLineReader{
		reader: bufio.NewReader(strings.NewReader("agent tail\r\x1b[B\rquit\r")),
		writer: &output,
	}
	bot := &Orchestrator{
		agents: agents,
		tailRunner: func(logPath string) error {
			selectedLog = logPath
			return nil
		},
	}

	oldStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	os.Stdout = writePipe
	defer func() {
		os.Stdout = oldStdout
	}()

	ctx, cancel := context.WithCancel(context.Background())
	runErr := runREPLWithLineReader(ctx, bot, cancel, reader)
	if runErr != nil {
		t.Fatalf("runREPLWithLineReader() error = %v", runErr)
	}

	if err := writePipe.Close(); err != nil {
		t.Fatalf("stdout close failed: %v", err)
	}
	replOutput, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("stdout read failed: %v", err)
	}
	if err := readPipe.Close(); err != nil {
		t.Fatalf("stdout read pipe close failed: %v", err)
	}

	combined := string(replOutput) + output.String()
	if selectedLog != "/tmp/repository-agent-orchestrator/agent-1.log" {
		t.Fatalf("tailRunner log path = %q, want %q", selectedLog, "/tmp/repository-agent-orchestrator/agent-1.log")
	}
	if !strings.Contains(combined, "Select an agent log to tail:") {
		t.Fatalf("interactive tail output missing selector prompt: %q", combined)
	}
	if !strings.Contains(combined, "agent-2 role=reviewer state=working issue=2 pr=44") {
		t.Fatalf("interactive tail output missing selector option: %q", combined)
	}
}
