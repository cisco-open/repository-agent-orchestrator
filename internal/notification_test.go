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
	"strings"
	"testing"
)

func TestFormatNotificationMessageAddsRepoPrefix(t *testing.T) {
	bot := &Orchestrator{cfg: Config{RepoName: "repository-agent-orchestrator"}}
	got := bot.formatNotificationMessage("Repository Agent Orchestrator: New PR issue comment forwarded for agent `coding-agent-49-1772534331`")
	want := "Repository Agent Orchestrator (repository-agent-orchestrator): New PR issue comment forwarded for agent `coding-agent-49-1772534331`"
	if got != want {
		t.Fatalf("formatNotificationMessage() = %q, want %q", got, want)
	}
}

func TestFormatNotificationMessageNormalizesLegacyOrchestratorPrefix(t *testing.T) {
	bot := &Orchestrator{cfg: Config{RepoName: "example-repository"}}
	got := bot.formatNotificationMessage("Repository Agent Orchestrator started for `acme/widget`")
	want := "Repository Agent Orchestrator (example-repository): started for `acme/widget`"
	if got != want {
		t.Fatalf("formatNotificationMessage() = %q, want %q", got, want)
	}
}

func TestFormatNotificationMessageLeavesExplicitRepoPrefixUntouched(t *testing.T) {
	bot := &Orchestrator{cfg: Config{RepoName: "repository-agent-orchestrator"}}
	input := "Repository Agent Orchestrator (example-repository): New PR detected"
	if got := bot.formatNotificationMessage(input); got != input {
		t.Fatalf("formatNotificationMessage() = %q, want %q", got, input)
	}
}

func TestFormatNotificationMessageFallsBackToOrchestratorWithoutRepoName(t *testing.T) {
	bot := &Orchestrator{cfg: Config{}}
	got := bot.formatNotificationMessage("Repository Agent Orchestrator: runtime launched")
	want := "Repository Agent Orchestrator: runtime launched"
	if got != want {
		t.Fatalf("formatNotificationMessage() = %q, want %q", got, want)
	}
}

func TestNotifyUsesIndependentTimeoutContextAndRedactsSecrets(t *testing.T) {
	messages := make([]string, 0, 1)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		messages = append(messages, payload["markdown"])
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	bot := &Orchestrator{
		cfg:      Config{RepoName: "example-repository", WebexWebhookURL: webex.URL},
		token:    "gh-secret-token",
		notifier: NewWebexNotifier(webex.URL),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	bot.notify(ctx, "Repository Agent Orchestrator: token gh-secret-token webhook "+webex.URL)

	if len(messages) != 1 {
		t.Fatalf("notification count = %d, want 1", len(messages))
	}
	if strings.Contains(messages[0], "gh-secret-token") {
		t.Fatalf("notification leaked token: %q", messages[0])
	}
	if strings.Contains(messages[0], webex.URL) {
		t.Fatalf("notification leaked webhook URL: %q", messages[0])
	}
	if !strings.Contains(messages[0], "[REDACTED]") {
		t.Fatalf("notification missing redaction marker: %q", messages[0])
	}
	if !strings.Contains(messages[0], "Repository Agent Orchestrator (example-repository):") {
		t.Fatalf("notification missing repo prefix: %q", messages[0])
	}
}

// TestNotifyAgentFailureIncludesUnderlyingErrorDetail is the regression
// test for a plain "failed to prepare worktree" Webex notification giving
// an operator, who is not watching the REPL when this fires, no way to
// tell a credential/passphrase problem (git/ssh refusing to hang on an
// unanswerable prompt) apart from a network blip or any other failure
// without going to find and re-run the command by hand.
func TestNotifyAgentFailureIncludesUnderlyingErrorDetail(t *testing.T) {
	messages := make([]string, 0, 1)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		messages = append(messages, payload["markdown"])
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	bot := &Orchestrator{
		cfg:      Config{RepoName: "example-repository"},
		notifier: NewWebexNotifier(webex.URL),
	}

	cmdErr := &commandExecutionError{
		command: "git fetch --prune origin main",
		cause:   errors.New("exit status 255"),
		detail:  "[stderr]\ngit@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.",
	}
	bot.notifyAgentFailure(context.Background(), "Repository Agent Orchestrator: failed to prepare worktree for coder `coder-1`", cmdErr)

	if len(messages) != 1 {
		t.Fatalf("notification count = %d, want 1", len(messages))
	}
	if !strings.Contains(messages[0], "Permission denied (publickey)") {
		t.Fatalf("notification missing underlying error detail: %q", messages[0])
	}
	if !strings.Contains(messages[0], "failed to prepare worktree for coder `coder-1`") {
		t.Fatalf("notification missing original message: %q", messages[0])
	}
}

func TestNotifyAgentFailureOmitsReasonSectionForNilError(t *testing.T) {
	messages := make([]string, 0, 1)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		messages = append(messages, payload["markdown"])
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	bot := &Orchestrator{
		cfg:      Config{RepoName: "example-repository"},
		notifier: NewWebexNotifier(webex.URL),
	}

	bot.notifyAgentFailure(context.Background(), "Repository Agent Orchestrator: failed to prepare worktree for coder `coder-1`", nil)

	if len(messages) != 1 {
		t.Fatalf("notification count = %d, want 1", len(messages))
	}
	if strings.Contains(messages[0], "Reason:") {
		t.Fatalf("notification added an empty Reason section for a nil error: %q", messages[0])
	}
}
