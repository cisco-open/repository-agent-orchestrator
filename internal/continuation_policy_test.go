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
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestContinuationPushCommandShellQuotesHeadBranch(t *testing.T) {
	t.Parallel()

	got := continuationPushCommand(Agent{PRHeadBranch: "feature/$(touch-pwned)"})
	want := "git push origin 'HEAD:feature/$(touch-pwned)'"
	if got != want {
		t.Fatalf("continuationPushCommand() = %q, want %q", got, want)
	}
}

func TestAdoptedPRSkipsRemoteOwnershipActions(t *testing.T) {
	t.Parallel()

	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.Error(w, "unexpected GitHub request", http.StatusInternalServerError)
	}))
	defer srv.Close()

	agent := Agent{
		ID:           "continued-agent",
		Role:         RoleCoder,
		IssueNumber:  42,
		PRNumber:     57,
		BranchName:   "repository-agent-orchestrator/continue-issue-42-pr-57",
		PRHeadBranch: "feature/in-progress",
		AdoptedPR:    true,
		State:        StateApproved,
	}
	agents := NewAgentManager()
	if err := agents.Add(&agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget", RepoPath: "/does/not/exist"},
		github: newGitHubClientForTest(t, srv),
		agents: agents,
	}

	if err := bot.closeAgentPR(context.Background(), agent); err != nil {
		t.Fatalf("closeAgentPR() error = %v", err)
	}
	if err := bot.deleteAgentRemoteBranch(context.Background(), agent); err != nil {
		t.Fatalf("deleteAgentRemoteBranch() error = %v", err)
	}
	if err := bot.maybeAutoMergeApprovedPR(context.Background(), agent); err != nil {
		t.Fatalf("maybeAutoMergeApprovedPR() error = %v", err)
	}
	if requests != 0 {
		t.Fatalf("GitHub requests for adopted PR ownership actions = %d, want 0", requests)
	}
}

func TestReviewLaunchForAdoptedPRFetchesRemoteHeadBranch(t *testing.T) {
	t.Parallel()

	target := reviewLaunchTargetFromCoder(Agent{
		ID:           "continued-agent",
		Role:         RoleCoder,
		IssueNumber:  42,
		PRNumber:     57,
		BranchName:   "repository-agent-orchestrator/continue-issue-42-pr-57",
		PRHeadBranch: "feature/in-progress",
		AdoptedPR:    true,
	})
	if target.BranchName != "feature/in-progress" {
		t.Fatalf("review target branch = %q, want remote PR head", target.BranchName)
	}
}

func TestAdoptedPRInitialHeadDoesNotLaunchReview(t *testing.T) {
	t.Parallel()

	agent := &Agent{
		ID:                  "continued-agent",
		Role:                RoleCoder,
		IssueNumber:         42,
		PRNumber:            57,
		ObservedPRHeadSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		LastReviewedHeadSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AdoptedPR:           true,
		State:               StateWorking,
	}
	agents := NewAgentManager()
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	bot := &Orchestrator{agents: agents}
	if err := bot.ensureReviewAgentForCoder(context.Background(), *agent); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	current, ok := agents.Get(agent.ID)
	if !ok || current.ActiveReviewAgentID != "" {
		t.Fatalf("review unexpectedly launched for initial head: %#v", current)
	}
}
