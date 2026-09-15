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
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRunREPLAgentListCommand(t *testing.T) {
	bot := &Orchestrator{
		agents: NewAgentManager(),
	}

	output, err := runREPLForTest(t, "agent list\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "agent list") {
		t.Fatalf("help output missing `agent list`: %q", output)
	}
	if !strings.Contains(output, "agent review <prNumber>") {
		t.Fatalf("help output missing `agent review <prNumber>`: %q", output)
	}
	if !strings.Contains(output, "agent help") {
		t.Fatalf("help output missing `agent help`: %q", output)
	}
	if !strings.Contains(output, "agent index repo") {
		t.Fatalf("help output missing `agent index repo`: %q", output)
	}
	if strings.Contains(output, "doctor") {
		t.Fatalf("help output should not include removed `doctor` command: %q", output)
	}
	if !strings.Contains(output, "No agents.") {
		t.Fatalf("agent list output missing empty agent message: %q", output)
	}
	if strings.Contains(output, "usage: agent list") {
		t.Fatalf("agent list command should not print usage for valid input: %q", output)
	}
}

func TestPrintHelpIncludesAliasesAndPrefixGuidance(t *testing.T) {
	output := captureStdout(t, printHelp)

	for _, needle := range []string{
		"Available commands:",
		"agent list",
		"agent review <prNumber>",
		"agent help",
		"agent index repo",
		"review standalone PRs while still rejoining tracked Repository Agent Orchestrator review flow",
		"unique command prefixes are accepted when unambiguous",
	} {
		if !strings.Contains(output, needle) {
			t.Fatalf("printHelp() output missing %q: %q", needle, output)
		}
	}
	if strings.Contains(output, "agent init") {
		t.Fatalf("printHelp() output should not include removed agent init alias: %q", output)
	}
	if strings.Contains(output, "list agents") {
		t.Fatalf("printHelp() output should not include removed list agents alias: %q", output)
	}
}

func TestPrintAgentHelpIncludesControlSyntax(t *testing.T) {
	output := captureStdout(t, printAgentHelp)

	for _, needle := range []string{
		"agent cleanup [coder|reviewer] <issueOrPRNumber>",
		"agent pause [coder|reviewer] <issueNumber>",
		"agent unpause [coder|reviewer] <issueNumber>",
		"agent stop [coder|reviewer] <issueOrPRNumber>",
		"agent steer [coder|reviewer] <issueNumber> <text...>",
		"agent tail [coder|reviewer] <issueNumber>",
		"omitted role defaults to `coder`",
		"stop and cleanup accept a tracked or manual reviewer PR",
		"tracked review semantics for Repository Agent Orchestrator PRs and detached one-shot review for standalone PRs",
	} {
		if !strings.Contains(output, needle) {
			t.Fatalf("printAgentHelp() output missing %q: %q", needle, output)
		}
	}
}

func TestNormalizeREPLNewlinesConvertsLFToCRLF(t *testing.T) {
	t.Parallel()

	input := "line one\nline two\n"
	if got, want := normalizeREPLNewlines(input), "line one\r\nline two\r\n"; got != want {
		t.Fatalf("normalizeREPLNewlines() = %q, want %q", got, want)
	}
}

func TestRunREPLAgentListOmitsCompletedAgents(t *testing.T) {
	agents := NewAgentManager()

	done := &Agent{
		ID:               "coding-agent-1",
		IssueNumber:      1,
		BranchName:       "repository-agent-orchestrator/issue-1",
		State:            StateDone,
		Stopped:          true,
		LastActivityTime: time.Now().Add(-time.Minute),
	}
	if err := agents.Add(done); err != nil {
		t.Fatalf("Add(done) error = %v", err)
	}

	active := &Agent{
		ID:               "coding-agent-2",
		IssueNumber:      2,
		BranchName:       "repository-agent-orchestrator/issue-2",
		State:            StateWorking,
		LastActivityTime: time.Now(),
	}
	if err := agents.Add(active); err != nil {
		t.Fatalf("Add(active) error = %v", err)
	}

	bot := &Orchestrator{agents: agents}
	output, err := runREPLForTest(t, "agent list\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "issue #2") {
		t.Fatalf("agent list output missing issue summary: %q", output)
	}
	if !strings.Contains(output, "coder: working ("+active.ID+")") {
		t.Fatalf("agent list output missing active agent id %q: %q", active.ID, output)
	}
	if strings.Contains(output, done.ID) {
		t.Fatalf("agent list output should omit done agent id %q: %q", done.ID, output)
	}
	if strings.Contains(output, "No agents.") {
		t.Fatalf("agent list output should not print empty message when active agents exist: %q", output)
	}
}

func TestRunREPLAgentListShowsFailedReviewAgent(t *testing.T) {
	agents := NewAgentManager()
	failed := &Agent{
		ID:                "review-agent-260-1",
		Role:              RoleReviewer,
		PRNumber:          260,
		ObservedPRHeadSHA: "da19590275a3ee6a6c8f22da56d549bf971af74c",
		State:             StateErrored,
		Stopped:           true,
		LastActivityTime:  time.Now(),
		FailureMessage:    "review worker routing rejected launch",
	}
	if err := agents.Add(failed); err != nil {
		t.Fatalf("Add(failed) error = %v", err)
	}

	bot := &Orchestrator{agents: agents}
	output, err := runREPLForTest(t, "agent list\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	for _, want := range []string{
		"Failed Agents",
		"reviewer: errored (" + failed.ID + ")",
		"pr: #260",
		"head: da19590275a3",
		"failure: review worker routing rejected launch",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("agent list output missing %q: %q", want, output)
		}
	}
	if strings.Contains(output, "No agents.") {
		t.Fatalf("failed review agent was reported as no agents: %q", output)
	}
}

func TestRunREPLPartialAgentListCommand(t *testing.T) {
	bot := &Orchestrator{
		agents: NewAgentManager(),
	}

	output, err := runREPLForTest(t, "ag li\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "No agents.") {
		t.Fatalf("partial command should resolve to agent list and show empty state: %q", output)
	}
	if strings.Contains(output, "unknown command") {
		t.Fatalf("partial command should not be treated as unknown: %q", output)
	}
}

func TestRunREPLRemovedListAgentsAliasUnknown(t *testing.T) {
	bot := &Orchestrator{
		agents: NewAgentManager(),
	}

	output, err := runREPLForTest(t, "list agents\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "unknown command; type 'help'") {
		t.Fatalf("removed list agents alias should be unknown: %q", output)
	}
}

func TestRunREPLDoctorCommandUnknown(t *testing.T) {
	bot := &Orchestrator{
		agents: NewAgentManager(),
	}

	output, err := runREPLForTest(t, "doctor\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "unknown command; type 'help'") {
		t.Fatalf("doctor command should be unknown: %q", output)
	}
}

func TestResolveCommandTokenUniquePrefix(t *testing.T) {
	got, err := resolveCommandToken("ag", replRootCommandOptions)
	if err != nil {
		t.Fatalf("resolveCommandToken() error = %v", err)
	}
	if got != "agent" {
		t.Fatalf("resolveCommandToken() = %q, want %q", got, "agent")
	}
}

func TestResolveCommandTokenRemovedAliasUnknown(t *testing.T) {
	_, err := resolveCommandToken("init", replAgentCommandOptions)
	if err == nil {
		t.Fatal("resolveCommandToken() error = nil, want unknown command error")
	}
	if _, ok := err.(unknownCommandError); !ok {
		t.Fatalf("resolveCommandToken() error type = %T, want unknownCommandError", err)
	}
}

func TestResolveCommandTokenAmbiguousPrefix(t *testing.T) {
	_, err := resolveCommandToken("st", replAgentCommandOptions)
	if err == nil {
		t.Fatal("resolveCommandToken() error = nil, want ambiguity error")
	}
	ambiguous, ok := err.(ambiguousCommandError)
	if !ok {
		t.Fatalf("resolveCommandToken() error type = %T, want ambiguousCommandError", err)
	}
	wantMatches := []string{"start", "steer", "stop"}
	if strings.Join(ambiguous.matches, ",") != strings.Join(wantMatches, ",") {
		t.Fatalf("ambiguous matches = %#v, want %#v", ambiguous.matches, wantMatches)
	}
}

func TestTerminalLineReaderHistoryNavigation(t *testing.T) {
	var output bytes.Buffer
	input := "agent list\r\x1b[A\r"
	reader := &terminalLineReader{
		reader: bufio.NewReader(strings.NewReader(input)),
		writer: &output,
	}

	first, err := reader.ReadLine("repository-agent-orchestrator> ")
	if err != nil {
		t.Fatalf("ReadLine(first) error = %v", err)
	}
	if first != "agent list" {
		t.Fatalf("ReadLine(first) = %q, want %q", first, "agent list")
	}

	second, err := reader.ReadLine("repository-agent-orchestrator> ")
	if err != nil {
		t.Fatalf("ReadLine(second) error = %v", err)
	}
	if second != "agent list" {
		t.Fatalf("ReadLine(second) = %q, want %q", second, "agent list")
	}
}

func TestTerminalLineReaderDownArrowRestoresDraft(t *testing.T) {
	var output bytes.Buffer
	input := "agent list\rag\x1b[A\x1b[B\r"
	reader := &terminalLineReader{
		reader: bufio.NewReader(strings.NewReader(input)),
		writer: &output,
	}

	first, err := reader.ReadLine("repository-agent-orchestrator> ")
	if err != nil {
		t.Fatalf("ReadLine(first) error = %v", err)
	}
	if first != "agent list" {
		t.Fatalf("ReadLine(first) = %q, want %q", first, "agent list")
	}

	second, err := reader.ReadLine("repository-agent-orchestrator> ")
	if err != nil {
		t.Fatalf("ReadLine(second) error = %v", err)
	}
	if second != "ag" {
		t.Fatalf("ReadLine(second) = %q, want %q", second, "ag")
	}
}

func runREPLForTest(t *testing.T, input string, bot *Orchestrator) (string, error) {
	t.Helper()

	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() stdin error = %v", err)
	}
	outReader, outWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() stdout error = %v", err)
	}

	oldStdin := os.Stdin
	oldStdout := os.Stdout
	os.Stdin = inReader
	os.Stdout = outWriter
	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	if _, err := io.WriteString(inWriter, input); err != nil {
		t.Fatalf("stdin write failed: %v", err)
	}
	if err := inWriter.Close(); err != nil {
		t.Fatalf("stdin close failed: %v", err)
	}
	defer inReader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := runREPL(ctx, bot, cancel)

	if err := outWriter.Close(); err != nil {
		t.Fatalf("stdout close failed: %v", err)
	}
	defer outReader.Close()

	output, err := io.ReadAll(outReader)
	if err != nil {
		t.Fatalf("stdout read failed: %v", err)
	}
	return string(output), runErr
}
