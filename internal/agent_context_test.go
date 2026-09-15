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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildAgentContextMarkdownUsesIndexAndRecentHandoffs(t *testing.T) {
	repoPath := t.TempDir()
	logDir := t.TempDir()

	index := `repository:
  name: repository-agent-orchestrator
  summary: Deterministic local orchestrator for coding and review agents.
  preferred_starting_points:
    - README.md
  stable_components:
    - agent_lifecycle
    - github_control_plane
  primary_concerns:
    - comment_forwarding
    - review_automation
groups:
  repo:
    summary: Top-level orientation docs.
  review:
    summary: Review and comment-handling behavior.
documents:
  - path: README.md
    title: Overview
    group: repo
    kind: overview
    summary: Operator entry point.
    components: [agent_lifecycle]
    concerns: [orientation]
    artifacts: [repository-agent-orchestrator_cli]
    read_when:
      - You need the broad system map.
    usually_skip_when:
      - You already know the repo.
    priority: foundational
  - path: docs/COMMENTS.md
    title: Comment Forwarding
    group: review
    kind: reference
    summary: Explains review comment forwarding, dedupe, and follow-up expectations.
    components: [github_control_plane]
    concerns: [comment_forwarding, review_automation]
    artifacts: [pull_request_comments]
    read_when:
      - You are changing PR comment forwarding.
    usually_skip_when:
      - The task is unrelated to PR feedback.
    priority: important
`
	if err := os.WriteFile(filepath.Join(repoPath, "agent_index.yaml"), []byte(index), 0o644); err != nil {
		t.Fatalf("WriteFile(agent_index.yaml) error = %v", err)
	}

	handoffPath := filepath.Join(logDir, orchestratorStateDirName, "handoffs.json")
	if err := os.MkdirAll(filepath.Dir(handoffPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(handoff dir) error = %v", err)
	}
	entry := persistedAgentHandoff{
		SchemaVersion: agentHandoffSchemaVersion,
		Timestamp:     time.Now().Add(-2 * time.Hour).UTC(),
		AgentID:       "coding-agent-12",
		Role:          RoleCoder,
		IssueNumber:   12,
		IssueTitle:    "Tighten review comment dedupe",
		Summary:       "Improved comment forwarding dedupe and follow-up handling.",
		Components:    []string{"github_control_plane"},
		Concerns:      []string{"comment_forwarding", "review_automation"},
		FollowUps:     []string{"Verify routing guidance for review agents."},
	}
	body, err := json.Marshal([]persistedAgentHandoff{entry})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := os.WriteFile(handoffPath, append(body, '\n'), 0o644); err != nil {
		t.Fatalf("WriteFile(handoffs.json) error = %v", err)
	}

	bot := &Orchestrator{cfg: Config{RepoPath: repoPath, LogDir: logDir}}
	agent := Agent{
		ID:                  "coding-agent-44",
		Role:                RoleCoder,
		IssueNumber:         44,
		IssueTitle:          "Fix comment forwarding resume flow",
		PRNumber:            101,
		HumanReviewGuidance: []string{"Backward compatibility is explicitly out of scope for this PR."},
	}

	markdown, err := bot.buildAgentContextMarkdown(agent, agentContextRequest{
		Role:        RoleCoder,
		IssueNumber: 44,
		IssueTitle:  "Fix comment forwarding resume flow",
		IssueBody:   "Ensure review comment forwarding is deduped and resumed safely.",
		PRNumber:    101,
	})
	if err != nil {
		t.Fatalf("buildAgentContextMarkdown() error = %v", err)
	}

	for _, want := range []string{
		"# Repository Agent Orchestrator Agent Context",
		"## Human Review Guidance",
		"Backward compatibility is explicitly out of scope for this PR.",
		"## Repository Routing",
		"Deterministic local orchestrator for coding and review agents.",
		"Preferred starting points: `README.md`",
		"## Read These Docs First",
		"1. `README.md`",
		"`docs/COMMENTS.md`",
		"group: `review` (Review and comment-handling behavior.)",
		"matched:",
		"`comment`",
		"## Recent Handoffs",
		"Improved comment forwarding dedupe and follow-up handling.",
		"Verify routing guidance for review agents.",
		"## Completion State",
		"update `.repository-agent-orchestrator/HANDOFF.yaml`",
	} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("buildAgentContextMarkdown() missing expected content: %q\nmarkdown:\n%s", want, markdown)
		}
	}
}

// TestLoadAgentIndexAcceptsBareStringGroupSummary is the regression test
// for a real production incident: agent_index.yaml's groups entries are
// documented (formatRepoIndexPrompt) as "define top-level groups" without
// pinning down that each entry must be a mapping, and a bare string is
// the more natural reading of that instruction. Before agentIndexGroup
// grew a tolerant UnmarshalYAML (mirroring flexibleStringList's identical
// tolerance for the same class of AI-authored shape drift elsewhere in
// this file), a bare-string group entry failed the whole agent launch:
// "yaml: unmarshal errors: ... cannot unmarshal !!str `...` into
// orchestrator.agentIndexGroup".
func TestLoadAgentIndexAcceptsBareStringGroupSummary(t *testing.T) {
	repoPath := t.TempDir()
	index := `repository:
  name: example-repository
  summary: Local upgrade validation agent.
groups:
  repo: Top-level orientation docs.
  review:
    summary: Review and comment-handling behavior.
documents:
  - path: README.md
    title: Overview
    group: repo
    kind: overview
    summary: Operator entry point.
    priority: foundational
`
	if err := os.WriteFile(filepath.Join(repoPath, "agent_index.yaml"), []byte(index), 0o644); err != nil {
		t.Fatalf("WriteFile(agent_index.yaml) error = %v", err)
	}

	bot := &Orchestrator{cfg: Config{RepoPath: repoPath}}
	idx, err := bot.loadAgentIndex()
	if err != nil {
		t.Fatalf("loadAgentIndex() error = %v, want the bare-string group entry tolerated", err)
	}
	if got, want := idx.Groups["repo"].Summary, "Top-level orientation docs."; got != want {
		t.Fatalf("groups[repo].Summary = %q, want %q", got, want)
	}
	if got, want := idx.Groups["review"].Summary, "Review and comment-handling behavior."; got != want {
		t.Fatalf("groups[review].Summary = %q, want %q", got, want)
	}
}

func TestBuildAgentContextMarkdownAcceptsStructuredRepositoryLists(t *testing.T) {
	repoPath := t.TempDir()
	index := `repository:
  name: example-repository
  summary: Local upgrade validation agent.
  preferred_starting_points:
    - path: README.md
      reason: Start with product scope and commands.
    - path: AGENTS.md
      reason: Repository work contract.
  stable_components:
    - id: product_api_ui
      summary: Embedded server and UI workspace.
  primary_concerns:
    - id: memory_mode
      summary: The server starts empty by default.
groups:
  repo:
    summary: Top-level orientation docs.
documents:
  - path: README.md
    title: Overview
    group: repo
    kind: overview
    summary: Product entry point.
    components: [product_api_ui]
    concerns: [memory_mode]
    read_when:
      - Start here.
    priority: foundational
`
	if err := os.WriteFile(filepath.Join(repoPath, "agent_index.yaml"), []byte(index), 0o644); err != nil {
		t.Fatalf("WriteFile(agent_index.yaml) error = %v", err)
	}

	bot := &Orchestrator{cfg: Config{RepoPath: repoPath}}
	markdown, err := bot.buildAgentContextMarkdown(Agent{ID: "review-agent-21", Role: RoleReviewer, PRNumber: 21}, agentContextRequest{
		Role:     RoleReviewer,
		PRNumber: 21,
	})
	if err != nil {
		t.Fatalf("buildAgentContextMarkdown() error = %v", err)
	}
	for _, want := range []string{
		"Preferred starting points: `README.md`, `AGENTS.md`",
		"Stable components: `product_api_ui`",
		"Primary concerns: `memory_mode`",
		"1. `README.md`",
	} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("buildAgentContextMarkdown() missing expected content: %q\nmarkdown:\n%s", want, markdown)
		}
	}
}

func TestCaptureAgentHandoffNonFatalPersistsStructuredEntry(t *testing.T) {
	logDir := t.TempDir()
	worktree := t.TempDir()
	if err := os.MkdirAll(filepath.Join(worktree, ".repository-agent-orchestrator"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.repository-agent-orchestrator) error = %v", err)
	}

	handoff := `schema_version: 1
status: completed
summary: Tightened review-agent context routing.
components:
  - agent_lifecycle
concerns:
  - review_automation
artifacts:
  - context_md
docs_read:
  - README.md
files_touched:
  - internal/agent_context.go
tests_run:
  - make test
decisions:
  - Keep recent handoffs in a bounded JSON file.
follow_ups:
  - Consider repo-local persistence in a future change.
risks:
  - Routing quality depends on index quality.
`
	if err := os.WriteFile(filepath.Join(worktree, ".repository-agent-orchestrator", "HANDOFF.yaml"), []byte(handoff), 0o644); err != nil {
		t.Fatalf("WriteFile(HANDOFF.yaml) error = %v", err)
	}

	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-55",
		Role:                    RoleCoder,
		IssueNumber:             55,
		IssueTitle:              "Route agent docs from agent_index",
		WorktreePath:            worktree,
		LogDir:                  logDir,
		BranchName:              "repository-agent-orchestrator/issue-55",
		State:                   StateWorking,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	bot := &Orchestrator{
		cfg:    Config{LogDir: logDir},
		agents: agents,
	}

	bot.captureAgentHandoffNonFatal(*agent)

	current, ok := agents.Get(agent.ID)
	if !ok {
		t.Fatalf("Get(%s) returned !ok", agent.ID)
	}
	if !current.HandoffCaptured {
		t.Fatal("HandoffCaptured = false, want true")
	}

	body, err := os.ReadFile(filepath.Join(logDir, orchestratorStateDirName, "handoffs.json"))
	if err != nil {
		t.Fatalf("ReadFile(handoffs.json) error = %v", err)
	}
	var entries []persistedAgentHandoff
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	entry := entries[0]

	if entry.AgentID != agent.ID {
		t.Fatalf("AgentID = %q, want %q", entry.AgentID, agent.ID)
	}
	if entry.Summary != "Tightened review-agent context routing." {
		t.Fatalf("Summary = %q, want persisted summary", entry.Summary)
	}
	if got, want := strings.Join(entry.Components, ","), "agent_lifecycle"; got != want {
		t.Fatalf("Components = %q, want %q", got, want)
	}
	if got, want := strings.Join(entry.FollowUps, ","), "Consider repo-local persistence in a future change."; got != want {
		t.Fatalf("FollowUps = %q, want %q", got, want)
	}
}

func TestCompactPersistedHandoffsPreservesFollowUpsPreferentially(t *testing.T) {
	items := []persistedAgentHandoff{
		{AgentID: "a-1", FollowUps: nil},
		{AgentID: "a-2", FollowUps: []string{"keep unresolved"}},
		{AgentID: "a-3", FollowUps: nil},
		{AgentID: "a-4", FollowUps: []string{"also keep unresolved"}},
	}

	compacted := compactPersistedHandoffs(items, 2)
	if len(compacted) != 2 {
		t.Fatalf("len(compacted) = %d, want 2", len(compacted))
	}
	if got, want := compacted[0].AgentID, "a-2"; got != want {
		t.Fatalf("compacted[0].AgentID = %q, want %q", got, want)
	}
	if got, want := compacted[1].AgentID, "a-4"; got != want {
		t.Fatalf("compacted[1].AgentID = %q, want %q", got, want)
	}
}
