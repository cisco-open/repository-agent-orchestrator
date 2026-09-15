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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-github/v90/github"
)

func TestFormatInitialPromptIncludesRequiredInstructions(t *testing.T) {
	issue := &github.Issue{
		Number: github.Int(42),
		Title:  github.String("Fix flaky worker"),
		Body:   github.String("Investigate and stabilize retry timing."),
	}
	agent := Agent{
		ID:           "coding-agent-42-1700000000",
		WorktreePath: "/tmp/repository-agent-orchestrator/issue-42",
		BranchName:   "repository-agent-orchestrator/issue-42",
	}

	msg := formatInitialPrompt(issue, agent, "/tmp/repos/widget", "main", []string{"make test-all", "make test-coverage"})

	for _, want := range []string{
		"Issue: #42",
		"Title: Fix flaky worker",
		"Investigate and stabilize retry timing.",
		"Startup sequence (run these steps in order before coding):",
		"Repository Agent Orchestrator setup is complete:",
		"Local repository path: /tmp/repos/widget",
		"Base branch: main",
		"Worktree path: /tmp/repository-agent-orchestrator/issue-42",
		"Branch: repository-agent-orchestrator/issue-42",
		"Task file: /tmp/repository-agent-orchestrator/issue-42/.repository-agent-orchestrator/TASK.md",
		"Context file: /tmp/repository-agent-orchestrator/issue-42/.repository-agent-orchestrator/CONTEXT.md",
		"Handoff file: /tmp/repository-agent-orchestrator/issue-42/.repository-agent-orchestrator/HANDOFF.yaml",
		"Change to `/tmp/repository-agent-orchestrator/issue-42`.",
		"Read `.repository-agent-orchestrator/TASK.md`.",
		"Read `.repository-agent-orchestrator/CONTEXT.md` and load the recommended repo documents before coding.",
		"Run the mandatory test commands to verify baseline behavior:",
		"- `make test-all`",
		"- `make test-coverage`",
		"Work only in this worktree path: /tmp/repository-agent-orchestrator/issue-42",
		"Work only on this branch: repository-agent-orchestrator/issue-42",
		"Do not run coding commands in the primary repo path `/tmp/repos/widget`; use only `/tmp/repository-agent-orchestrator/issue-42`.",
		"Do not create or modify `AGENTS.md` unless the issue explicitly requires it.",
		"Never stage or commit Repository Agent Orchestrator runtime artifacts under `.repository-agent-orchestrator/` or `.worktrees/`; `.repository-agent-orchestrator/HANDOFF.yaml` is for Repository Agent Orchestrator state capture only.",
		"Run the same mandatory test commands again after making changes:",
		"Treat this as an autonomous run: continue working until the issue is complete or you hit a true blocker.",
		"Do not stop after each command or status update; continue directly to the next required step.",
		"For startup/test phases, prefer one continuous shell block (`set -euo pipefail` + ordered commands) so the sequence does not stall after single commands.",
		"Continue executing without waiting for extra input after status updates unless a true blocker requires clarification.",
		"Push branch `repository-agent-orchestrator/issue-42` and open a draft pull request against `main`.",
		"Wait for PR comments and respond with follow-up changes when needed.",
		"After each push that addresses PR feedback, reply on GitHub to each addressed PR comment with what changed; if no code change was made, explain why.",
		"Every GitHub PR comment or reply you post must end with these exact metadata lines so Repository Agent Orchestrator can recognize its own agent output and avoid forwarding it back to you:",
		"CODEX_AGENT_ID: coding-agent-42-1700000000",
		"CODEX_AGENT_ROLE: coder",
		"Before you finish, update `.repository-agent-orchestrator/HANDOFF.yaml` with the final machine-readable summary of the work, tests, follow-ups, and risks.",
		"When writing GitHub comments/replies, use real line breaks and Markdown formatting; never include literal `\\n` sequences in the posted text.",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatInitialPrompt() missing expected content: %q", want)
		}
	}
}

func TestFormatResumePromptForCoderIncludesCommentMarkers(t *testing.T) {
	agent := Agent{
		ID:           "coding-agent-77-1700000000",
		Role:         RoleCoder,
		IssueNumber:  77,
		BranchName:   "repository-agent-orchestrator/issue-77",
		WorktreePath: "/tmp/repository-agent-orchestrator/issue-77",
		PRNumber:     77,
		PRURL:        "https://github.com/acme/widget/pull/77",
	}

	msg := formatResumePrompt(agent, "new PR comment")

	for _, want := range []string{
		"Resume reason: new PR comment",
		"Any GitHub PR comment or reply you post must end with:",
		"- CODEX_AGENT_ID: coding-agent-77-1700000000",
		"- CODEX_AGENT_ROLE: coder",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatResumePrompt() missing expected content: %q", want)
		}
	}
}

func TestFormatForwardedReviewCommentIncludesContext(t *testing.T) {
	createdAt := time.Date(2026, time.January, 15, 8, 30, 0, 0, time.UTC)
	comment := &github.PullRequestComment{
		Body:      github.String("Please rename this variable for clarity."),
		Path:      github.String("internal/app.go"),
		Line:      github.Int(187),
		CreatedAt: &github.Timestamp{Time: createdAt},
		User:      &github.User{Login: github.String("reviewer1")},
	}
	agent := Agent{PRURL: "https://github.com/acme/widget/pull/99"}

	msg := formatForwardedReviewComment(agent, comment)

	for _, want := range []string{
		"PR URL: https://github.com/acme/widget/pull/99",
		"Author: @reviewer1",
		"Created: 2026-01-15T08:30:00Z",
		"File: internal/app.go",
		"Line: 187",
		"Please rename this variable for clarity.",
		"After you push fixes for this feedback, reply on GitHub to this comment with what changed.",
		"Use real line breaks in the reply body; do not post literal `\\n` sequences.",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatForwardedReviewComment() missing expected content: %q", want)
		}
	}
}

func TestFormatForwardedIssueCommentIncludesNewlineGuidance(t *testing.T) {
	createdAt := time.Date(2026, time.January, 15, 9, 0, 0, 0, time.UTC)
	comment := &github.IssueComment{
		Body:      github.String("Please summarize the retry behavior changes."),
		CreatedAt: &github.Timestamp{Time: createdAt},
		User:      &github.User{Login: github.String("reviewer2")},
	}
	agent := Agent{PRURL: "https://github.com/acme/widget/pull/99"}

	msg := formatForwardedIssueComment(agent, comment)

	for _, want := range []string{
		"PR URL: https://github.com/acme/widget/pull/99",
		"Author: @reviewer2",
		"Created: 2026-01-15T09:00:00Z",
		"Please summarize the retry behavior changes.",
		"After you push fixes based on this feedback, post a PR comment describing what changed.",
		"Use real line breaks in the PR comment body; do not post literal `\\n` sequences.",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatForwardedIssueComment() missing expected content: %q", want)
		}
	}
}

func TestFormatIssueCommentMarkdownIncludesContext(t *testing.T) {
	comment := &github.IssueComment{
		Body:    github.String("Please summarize the retry behavior changes."),
		HTMLURL: github.String("https://github.com/acme/widget/pull/99#issuecomment-100"),
		User:    &github.User{Login: github.String("reviewer2")},
	}

	msg := formatIssueCommentMarkdown(comment)

	for _, want := range []string{
		"# PR Issue Comment",
		"Author: @reviewer2",
		"URL: https://github.com/acme/widget/pull/99#issuecomment-100",
		"Please summarize the retry behavior changes.",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatIssueCommentMarkdown() missing expected content: %q", want)
		}
	}
}

func TestFormatPostPushCommentReplyReminderIncludesRequiredGuidance(t *testing.T) {
	agent := Agent{PRURL: "https://github.com/acme/widget/pull/99"}
	msg := formatPostPushCommentReplyReminder(
		agent,
		2,
		"0123456789abcdef",
		"fedcba9876543210",
	)

	for _, want := range []string{
		"Repository Agent Orchestrator detected a new push for this PR while review feedback is still pending.",
		"PR URL: https://github.com/acme/widget/pull/99",
		"Pending review comments: 2",
		"Previous head SHA: 0123456789abcdef",
		"Current head SHA: fedcba9876543210",
		"Now reply on GitHub to each addressed PR comment:",
		"- Say what changed.",
		"- If nothing changed, explain why.",
		"- Use real line breaks; do not include literal `\\n` in the posted comment text.",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatPostPushCommentReplyReminder() missing expected content: %q", want)
		}
	}
}

func TestFormatRepoIndexPromptIncludesIndexingInstructions(t *testing.T) {
	agent := Agent{
		Role:         RoleIndexer,
		WorktreePath: "/tmp/repository-agent-orchestrator/repo-index",
		BranchName:   "repository-agent-orchestrator/repo-index-1700000000",
	}

	msg := formatRepoIndexPrompt(agent, "/tmp/repos/example-repository", "main")

	for _, want := range []string{
		"You are Codex running as a Repository Agent Orchestrator repository indexing agent.",
		"Repository path: /tmp/repos/example-repository",
		"Base branch: main",
		"Worktree path: /tmp/repository-agent-orchestrator/repo-index",
		"Branch: repository-agent-orchestrator/repo-index-1700000000",
		"Task file: /tmp/repository-agent-orchestrator/repo-index/.repository-agent-orchestrator/TASK.md",
		"Canonical source file: /tmp/repository-agent-orchestrator/repo-index/agent_index.yaml",
		"Generated output file: /tmp/repository-agent-orchestrator/repo-index/INDEX.md",
		"excluding the generated top-level `INDEX.md`",
		"create or update the top-level `agent_index.yaml`",
		"Generate `INDEX.md` from `agent_index.yaml`",
		"Define top-level repository routing hints, including a repository summary, preferred starting points, stable components, and primary concerns.",
		"Define top-level groups as a mapping from group name to a mapping with a `summary` field",
		"never a bare string",
		"make every document's `group` reference one of those defined groups.",
		"Normalize `kind` to a small enum",
		"Normalize `components`, `concerns`, and `artifacts` to lowercase `snake_case` identifiers",
		"Use repo structure only as a hint for candidate names",
		"Use `components` only for durable repo subsystems",
		"Keep `read_when` and `usually_skip_when` as arrays",
		"Include per-document `priority` using only `foundational`, `important`, `targeted`, or `situational`.",
		"Use real newlines in the commit body rather than literal `\\n` sequences.",
		"Never stage or commit Repository Agent Orchestrator runtime artifacts under `.repository-agent-orchestrator/` or `.worktrees/`; `.repository-agent-orchestrator/HANDOFF.yaml` is for Repository Agent Orchestrator state capture only.",
		"Commit the documentation changes using the repository's commit style.",
		"Push branch `repository-agent-orchestrator/repo-index-1700000000` and open a pull request against `main`.",
		"Repository Agent Orchestrator will not launch a review agent for this PR; once it is open, stop and leave it ready for human review.",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatRepoIndexPrompt() missing expected content: %q", want)
		}
	}
}

func TestFormatReviewVerdictNeedsChangesIncludesFollowUpGuidance(t *testing.T) {
	comment := &github.IssueComment{
		Body:    github.String("CODEX_VERDICT: NEEDS_CHANGES\n- Add test coverage for edge cases."),
		HTMLURL: github.String("https://github.com/acme/widget/pull/42#issuecomment-900"),
	}
	coder := Agent{
		PRURL:      "https://github.com/acme/widget/pull/42",
		BranchName: "repository-agent-orchestrator/issue-42",
	}
	reviewer := Agent{
		ID:                "reviewer-42-1700000000",
		ObservedPRHeadSHA: "abcdef1234567890",
	}

	msg := formatReviewVerdictNeedsChanges(coder, reviewer, comment)
	for _, want := range []string{
		"Repository Agent Orchestrator review cycle result: NEEDS_CHANGES.",
		"PR URL: https://github.com/acme/widget/pull/42",
		"Review agent: reviewer-42-1700000000",
		"Reviewed head SHA: abcdef1234567890",
		"Review comment URL: https://github.com/acme/widget/pull/42#issuecomment-900",
		"CODEX_VERDICT: NEEDS_CHANGES",
		"push commits to `repository-agent-orchestrator/issue-42`",
		"launch a fresh review agent",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatReviewVerdictNeedsChanges() missing expected content: %q", want)
		}
	}
}

func TestFormatPreReviewHardGateFailedIncludesRequiredCommands(t *testing.T) {
	coder := Agent{
		ObservedPRHeadSHA: "abcdef1234567890",
		PRURL:             "https://github.com/acme/widget/pull/42",
		BranchName:        "repository-agent-orchestrator/issue-42",
	}

	msg := formatPreReviewHardGateFailed(coder, []string{"make test-all", "make test-coverage"}, errors.New("hard gate failed: make test-all"))
	for _, want := range []string{
		"pre-review hard gate failed",
		"PR URL: https://github.com/acme/widget/pull/42",
		"Current head SHA: abcdef1234567890",
		"- make test-all",
		"- make test-coverage",
		"Failure: hard gate failed: make test-all",
		"Treat this as NEEDS_CHANGES.",
		"push commits to `repository-agent-orchestrator/issue-42`",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatPreReviewHardGateFailed() missing expected content: %q", want)
		}
	}
}

func TestFormatPreReviewHardGateNotificationIncludesFailureDetails(t *testing.T) {
	coder := Agent{
		ID:                "coding-agent-42",
		ObservedPRHeadSHA: "abcdef1234567890",
		PRURL:             "https://github.com/acme/widget/pull/42",
	}

	msg := formatPreReviewHardGateNotification(coder, "deadbeef", errors.New("hard gate failed: make test-coverage\nCause: exit status 2"))
	for _, want := range []string{
		"mandatory test gate failed before review",
		"coder `coding-agent-42`",
		"Head SHA: `deadbeef`",
		"> hard gate failed: make test-coverage",
		"> Cause: exit status 2",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("formatPreReviewHardGateNotification() missing expected content: %q", want)
		}
	}
}
