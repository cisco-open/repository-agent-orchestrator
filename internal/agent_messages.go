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
	"fmt"
	"strings"
	"time"

	"github.com/google/go-github/v90/github"
)

func continuationPushCommand(agent Agent) string {
	return fmt.Sprintf("git push origin %s", shellSingleQuote("HEAD:"+strings.TrimSpace(agent.PRHeadBranch)))
}

func formatInitialPrompt(issue *github.Issue, agent Agent, repoPath, baseBranch string, mandatoryTests []string) string {
	title := strings.TrimSpace(issue.GetTitle())
	if title == "" {
		title = "(untitled issue)"
	}

	body := strings.TrimSpace(issue.GetBody())
	if body == "" {
		body = "(no issue body provided)"
	}
	mandatoryTestsMarkdown := formatMandatoryTestsMarkdownList(mandatoryTests, "   ")

	return fmt.Sprintf(
		"You are Codex running as a Repository Agent Orchestrator agent.\n\n"+
			"Read and solve this issue:\n"+
			"- Issue: #%d\n"+
			"- Title: %s\n\n"+
			"Issue body:\n%s\n\n"+
			"Repository Agent Orchestrator setup is complete:\n"+
			"- Local repository path: %s\n"+
			"- Base branch: %s\n"+
			"- Worktree path: %s\n"+
			"- Branch: %s\n"+
			"- Task file: %s/.repository-agent-orchestrator/TASK.md\n\n"+
			"- Context file: %s/.repository-agent-orchestrator/CONTEXT.md\n"+
			"- Handoff file: %s/.repository-agent-orchestrator/HANDOFF.yaml\n\n"+
			"Startup sequence (run these steps in order before coding):\n"+
			"1. Change to `%s`.\n"+
			"2. Read `.repository-agent-orchestrator/TASK.md`.\n"+
			"3. Read `.repository-agent-orchestrator/CONTEXT.md` and load the recommended repo documents before coding.\n"+
			"4. Run the mandatory test commands to verify baseline behavior:\n%s\n"+
			"5. Start implementation after the baseline passes.\n\n"+
			"Execution constraints:\n"+
			"- Work only in this worktree path: %s\n"+
			"- Work only on this branch: %s\n"+
			"- Do not run coding commands in the primary repo path `%s`; use only `%s`.\n"+
			"- Do not create or modify `AGENTS.md` unless the issue explicitly requires it.\n"+
			"- %s\n"+
			"- Run the same mandatory test commands again after making changes:\n%s\n"+
			"- Treat this as an autonomous run: continue working until the issue is complete or you hit a true blocker.\n"+
			"- Do not stop after each command or status update; continue directly to the next required step.\n"+
			"- For startup/test phases, prefer one continuous shell block (`set -euo pipefail` + ordered commands) so the sequence does not stall after single commands.\n"+
			"- Continue executing without waiting for extra input after status updates unless a true blocker requires clarification.\n"+
			"- Commit using this repository's style.\n"+
			"- Push branch `%s` and open a draft pull request against `%s`.\n"+
			"- Include \"Closes #%d\" or equivalent issue link in the PR description.\n"+
			"- Wait for PR comments and respond with follow-up changes when needed.\n"+
			"- After each push that addresses PR feedback, reply on GitHub to each addressed PR comment with what changed; if no code change was made, explain why.\n"+
			"- Every GitHub PR comment or reply you post must end with these exact metadata lines so Repository Agent Orchestrator can recognize its own agent output and avoid forwarding it back to you:\n"+
			"  CODEX_AGENT_ID: %s\n"+
			"  CODEX_AGENT_ROLE: coder\n"+
			"- Before you finish, update `.repository-agent-orchestrator/HANDOFF.yaml` with the final machine-readable summary of the work, tests, follow-ups, and risks.\n"+
			"- When writing GitHub comments/replies, use real line breaks and Markdown formatting; never include literal `\\n` sequences in the posted text.\n",
		issue.GetNumber(),
		title,
		body,
		repoPath,
		baseBranch,
		agent.WorktreePath,
		agent.BranchName,
		agent.WorktreePath,
		agent.WorktreePath,
		agent.WorktreePath,
		agent.WorktreePath,
		mandatoryTestsMarkdown,
		agent.WorktreePath,
		agent.BranchName,
		repoPath,
		agent.WorktreePath,
		runtimeArtifactsCommitRule,
		mandatoryTestsMarkdown,
		agent.BranchName,
		baseBranch,
		issue.GetNumber(),
		agent.ID,
	)
}

func formatContinuationPrompt(issue *github.Issue, pr *github.PullRequest, agent Agent, repoPath, baseBranch string, mandatoryTests []string) string {
	title := strings.TrimSpace(issue.GetTitle())
	if title == "" {
		title = "(untitled issue)"
	}
	body := strings.TrimSpace(issue.GetBody())
	if body == "" {
		body = "(no issue body provided)"
	}
	mandatoryTestsMarkdown := formatMandatoryTestsMarkdownList(mandatoryTests, "   ")
	return fmt.Sprintf(
		"You are Codex running as a Repository Agent Orchestrator continuation agent.\n\n"+
			"Continue the in-progress change for this issue and existing pull request:\n"+
			"- Issue: #%d\n"+
			"- Issue title: %s\n"+
			"- Existing PR: #%d (%s)\n"+
			"- PR title: %s\n\n"+
			"Issue body:\n%s\n\n"+
			"Repository Agent Orchestrator setup is complete:\n"+
			"- Local repository path: %s\n"+
			"- Base branch: %s\n"+
			"- Worktree path: %s\n"+
			"- Managed local branch: %s\n"+
			"- Existing PR head branch: %s\n"+
			"- Task file: %s/.repository-agent-orchestrator/TASK.md\n"+
			"- Context file: %s/.repository-agent-orchestrator/CONTEXT.md\n"+
			"- Handoff file: %s/.repository-agent-orchestrator/HANDOFF.yaml\n\n"+
			"Startup sequence (run these steps in order before coding):\n"+
			"1. Change to `%s`.\n"+
			"2. Read `.repository-agent-orchestrator/TASK.md`, `.repository-agent-orchestrator/CONTEXT.md`, and `.repository-agent-orchestrator/HANDOFF.yaml`.\n"+
			"3. Inspect PR #%d using GitHub: read its description, commits, diff, checks, reviews, review threads, and comments. Determine which feedback remains applicable.\n"+
			"4. Run the mandatory test commands to verify the existing PR baseline:\n%s\n"+
			"5. Continue implementation and address applicable feedback after the baseline passes.\n\n"+
			"Execution constraints:\n"+
			"- Work only in this worktree path: %s\n"+
			"- Work only on managed local branch: %s\n"+
			"- Do not run coding commands in the primary repo path `%s`; use only `%s`.\n"+
			"- Do not create or modify `AGENTS.md` unless the issue explicitly requires it.\n"+
			"- %s\n"+
			"- Run the same mandatory test commands again after making changes:\n%s\n"+
			"- Commit using this repository's style.\n"+
			"- Update the existing PR with `%s`; do not push the managed local branch by name and do not create another PR.\n"+
			"- Wait for new PR comments and respond with follow-up changes when needed.\n"+
			"- After each push that addresses PR feedback, reply to each addressed PR comment with what changed; if no code change was made, explain why.\n"+
			"- Every GitHub PR comment or reply you post must end with these exact metadata lines:\n"+
			"  CODEX_AGENT_ID: %s\n"+
			"  CODEX_AGENT_ROLE: coder\n"+
			"- Before finishing, update `.repository-agent-orchestrator/HANDOFF.yaml` with the work, tests, follow-ups, and risks.\n"+
			"- Use real line breaks and Markdown in GitHub comments; never include literal `\\n` sequences.\n"+
			"- Continue autonomously until the issue is complete or a true blocker requires user input.\n",
		issue.GetNumber(),
		title,
		agent.PRNumber,
		fallback(strings.TrimSpace(agent.PRURL), "unknown URL"),
		strings.TrimSpace(pr.GetTitle()),
		body,
		repoPath,
		baseBranch,
		agent.WorktreePath,
		agent.BranchName,
		agent.PRHeadBranch,
		agent.WorktreePath,
		agent.WorktreePath,
		agent.WorktreePath,
		agent.WorktreePath,
		agent.PRNumber,
		mandatoryTestsMarkdown,
		agent.WorktreePath,
		agent.BranchName,
		repoPath,
		agent.WorktreePath,
		runtimeArtifactsCommitRule,
		mandatoryTestsMarkdown,
		continuationPushCommand(agent),
		agent.ID,
	)
}

func formatResumePrompt(agent Agent, reason string) string {
	role := "coding"
	if agent.Role == RoleReviewer {
		role = "review"
	} else if agent.Role == RoleIndexer {
		role = "repo indexing"
	}
	cause := strings.TrimSpace(reason)
	if cause == "" {
		cause = "runtime restart requested"
	}
	prLine := "PR: not detected yet."
	if agent.PRNumber > 0 {
		prLine = fmt.Sprintf("PR: #%d (%s)", agent.PRNumber, fallback(strings.TrimSpace(agent.PRURL), "unknown URL"))
	}
	commentMarkerLines := ""
	if agent.Role == RoleCoder {
		commentMarkerLines = fmt.Sprintf(
			"Any GitHub PR comment or reply you post must end with:\n- CODEX_AGENT_ID: %s\n- CODEX_AGENT_ROLE: coder\n\n",
			fallback(strings.TrimSpace(agent.ID), "(unknown-agent-id)"),
		)
	}
	if agent.AdoptedPR {
		commentMarkerLines += fmt.Sprintf(
			"This agent continues an existing PR. Push updates only with `%s`; do not create another PR.\n\n",
			continuationPushCommand(agent),
		)
	}
	return fmt.Sprintf(
		"You are resuming a Repository Agent Orchestrator %s agent run.\n\n"+
			"Resume scope:\n"+
			"- Issue: #%d\n"+
			"- Branch: %s\n"+
			"- Worktree path: %s\n"+
			"- %s\n"+
			"- Resume reason: %s\n\n"+
			"%s"+
			"Wait for the next Repository Agent Orchestrator instruction and then continue autonomously.\n",
		role,
		agent.IssueNumber,
		fallback(strings.TrimSpace(agent.BranchName), "(unknown branch)"),
		fallback(strings.TrimSpace(agent.WorktreePath), "(unknown worktree)"),
		prLine,
		cause,
		commentMarkerLines,
	) + formatResumeFiles(agent)
}

func formatAutonomousRecoveryPrompt(agent Agent, reason string) string {
	reviewState := ""
	if agent.Role == RoleCoder && agent.LastReviewVerdict != "" {
		reviewState = fmt.Sprintf(
			"\nRecorded review state:\n- Verdict: %s\n- Reviewed head SHA: %s\n- Review comment ID: %d\n"+
				"Inspect the current PR conversation and finish any outstanding review feedback before pushing.\n",
			agent.LastReviewVerdict,
			fallback(strings.TrimSpace(agent.LastReviewedHeadSHA), "unknown"),
			agent.LastReviewCommentID,
		)
	}
	return formatResumePrompt(agent, reason) +
		"\nThis recovery prompt is the next Repository Agent Orchestrator instruction. Continue now without waiting for another message. " +
		"Preserve all existing work, inspect the current worktree and recent inbox items, finish any interrupted verification, and continue the assigned issue autonomously.\n" +
		reviewState
}

func formatReviewCorrectionPrompt(agent Agent, reviewInstruction string) string {
	return formatResumePrompt(agent, "review requested changes") +
		"\nThis review result is the next Repository Agent Orchestrator instruction. Continue now without waiting for another message. " +
		"Preserve the current worktree, apply the requested changes, run the required verification, and push the next revision.\n\n" +
		strings.TrimSpace(reviewInstruction) + "\n"
}

func formatResumeFiles(agent Agent) string {
	if agent.Role == RoleIndexer {
		return "Read `.repository-agent-orchestrator/TASK.md` and recent `.repository-agent-orchestrator/INBOX/*.md` items if present, then continue autonomously.\n" +
			runtimeArtifactsCommitRule + "\n"
	}
	return "Read `.repository-agent-orchestrator/TASK.md`, `.repository-agent-orchestrator/CONTEXT.md`, `.repository-agent-orchestrator/HANDOFF.yaml`, and recent `.repository-agent-orchestrator/INBOX/*.md` items if present, then continue autonomously.\n" +
		runtimeArtifactsCommitRule + "\n"
}

func formatRepoIndexPrompt(agent Agent, repoPath, baseBranch string) string {
	return fmt.Sprintf(
		"You are Codex running as a Repository Agent Orchestrator repository indexing agent.\n\n"+
			"Your task is to build or update a top-level `agent_index.yaml` for this repository, then generate a matching top-level `INDEX.md` from that YAML so future Repository Agent Orchestrator runs can decide which Markdown docs to read for a given task without loading the full documentation set.\n\n"+
			"Repository Agent Orchestrator setup is complete:\n"+
			"- Repository path: %s\n"+
			"- Base branch: %s\n"+
			"- Worktree path: %s\n"+
			"- Branch: %s\n"+
			"- Task file: %s/.repository-agent-orchestrator/TASK.md\n"+
			"- Canonical source file: %s/agent_index.yaml\n"+
			"- Generated output file: %s/INDEX.md\n\n"+
			"Execution rules:\n"+
			"1. Change to `%s` before running commands.\n"+
			"2. Read `.repository-agent-orchestrator/TASK.md`.\n"+
			"3. Enumerate tracked Markdown docs across the entire repo, excluding the generated top-level `INDEX.md`.\n"+
			"4. Read the relevant Markdown docs and create or update the top-level `agent_index.yaml`.\n"+
			"5. Generate `INDEX.md` from `agent_index.yaml`; do not treat `INDEX.md` as an independently authored document.\n"+
			"6. Define top-level repository routing hints, including a repository summary, preferred starting points, stable components, and primary concerns.\n"+
			"7. Define top-level groups as a mapping from group name to a mapping with a `summary` field (e.g. `some_group:\\n  summary: \"...\"`, never a bare string), and make every document's `group` reference one of those defined groups.\n"+
			"8. Normalize `kind` to a small enum: `overview`, `agent_contract`, `architecture`, `spec`, `api_spec`, `security_model`, `adr`, `runbook`, `operations`, `cli_guide`, `testing_guide`, `test_architecture`, `test_summary`, `reference`, or `example_note`.\n"+
			"9. Normalize `components`, `concerns`, and `artifacts` to lowercase `snake_case` identifiers with stable naming.\n"+
			"10. Use repo structure only as a hint for candidate names; do not automatically promote every package or directory to a top-level component.\n"+
			"11. Use `components` only for durable repo subsystems that would matter for doc routing. Prefer names backed by architecture docs, ADRs, top-level commands, or major packages rather than helper layers like integrations, notifiers, or generic runtime plumbing.\n"+
			"12. Use `concerns` for problem or topic tags and `artifacts` for named interfaces, CLIs, CRDs, fixtures, scripts, endpoints, or other concrete mechanisms.\n"+
			"13. Keep `read_when` and `usually_skip_when` as arrays even when there is only one item.\n"+
			"14. Include per-document `priority` using only `foundational`, `important`, `targeted`, or `situational`.\n"+
			"15. Keep both files concise and factual; do not copy large passages from the source docs.\n"+
			"16. Do not modify non-documentation source files.\n"+
			"17. %s\n"+
			"18. Commit the documentation changes using the repository's commit style.\n"+
			"19. Use real newlines in the commit body rather than literal `\\n` sequences.\n"+
			"20. Push branch `%s` and open a pull request against `%s`.\n"+
			"21. Repository Agent Orchestrator will not launch a review agent for this PR; once it is open, stop and leave it ready for human review.\n",
		repoPath,
		baseBranch,
		agent.WorktreePath,
		agent.BranchName,
		agent.WorktreePath,
		agent.WorktreePath,
		agent.WorktreePath,
		agent.WorktreePath,
		runtimeArtifactsCommitRule,
		agent.BranchName,
		baseBranch,
	)
}

func formatReviewCommentMarkdown(c *github.PullRequestComment) string {
	body := strings.TrimSpace(c.GetBody())
	if body == "" {
		body = "(empty comment)"
	}
	return fmt.Sprintf(`# PR Review Comment

Author: @%s
URL: %s

%s
`, c.GetUser().GetLogin(), c.GetHTMLURL(), body)
}

func formatIssueCommentMarkdown(c *github.IssueComment) string {
	body := strings.TrimSpace(c.GetBody())
	if body == "" {
		body = "(empty comment)"
	}
	return fmt.Sprintf(`# PR Issue Comment

Author: @%s
URL: %s

%s
`, c.GetUser().GetLogin(), c.GetHTMLURL(), body)
}

func formatForwardedReviewComment(agent Agent, c *github.PullRequestComment) string {
	body := strings.TrimSpace(c.GetBody())
	if body == "" {
		body = "(empty comment)"
	}

	author := strings.TrimSpace(c.GetUser().GetLogin())
	if author == "" {
		author = "unknown"
	}

	location := ""
	path := strings.TrimSpace(c.GetPath())
	line := c.GetLine()
	if line <= 0 {
		line = c.GetOriginalLine()
	}
	locationLines := make([]string, 0, 2)
	if path != "" {
		locationLines = append(locationLines, fmt.Sprintf("File: %s", path))
	}
	if line > 0 {
		locationLines = append(locationLines, fmt.Sprintf("Line: %d", line))
	}
	if len(locationLines) > 0 {
		location = strings.Join(locationLines, "\n") + "\n"
	}

	return fmt.Sprintf(
		"Repository Agent Orchestrator forwarded a PR review comment.\n\n"+
			"PR URL: %s\n"+
			"Author: @%s\n"+
			"Created: %s\n"+
			"%s"+
			"Comment:\n%s\n"+
			"\nAfter you push fixes for this feedback, reply on GitHub to this comment with what changed. If no code change was needed, explain why.\n"+
			"Use real line breaks in the reply body; do not post literal `\\n` sequences.\n",
		fallback(strings.TrimSpace(agent.PRURL), "(unknown)"),
		author,
		formatCommentTime(c.GetCreatedAt().Time),
		location,
		body,
	)
}

func formatForwardedIssueComment(agent Agent, c *github.IssueComment) string {
	body := strings.TrimSpace(c.GetBody())
	if body == "" {
		body = "(empty comment)"
	}

	author := strings.TrimSpace(c.GetUser().GetLogin())
	if author == "" {
		author = "unknown"
	}

	return fmt.Sprintf(
		"Repository Agent Orchestrator forwarded a PR comment.\n\n"+
			"PR URL: %s\n"+
			"Author: @%s\n"+
			"Created: %s\n"+
			"Comment:\n%s\n"+
			"\nAfter you push fixes based on this feedback, post a PR comment describing what changed. If no code change was needed, explain why.\n"+
			"Use real line breaks in the PR comment body; do not post literal `\\n` sequences.\n",
		fallback(strings.TrimSpace(agent.PRURL), "(unknown)"),
		author,
		formatCommentTime(c.GetCreatedAt().Time),
		body,
	)
}

func formatPostPushCommentReplyReminder(agent Agent, pendingComments int, previousHeadSHA, latestHeadSHA string) string {
	return fmt.Sprintf(
		"Repository Agent Orchestrator detected a new push for this PR while review feedback is still pending.\n\n"+
			"PR URL: %s\n"+
			"Pending review comments: %d\n"+
			"Previous head SHA: %s\n"+
			"Current head SHA: %s\n\n"+
			"Now reply on GitHub to each addressed PR comment:\n"+
			"- Say what changed.\n"+
			"- If nothing changed, explain why.\n"+
			"- Use real line breaks; do not include literal `\\n` in the posted comment text.\n",
		fallback(strings.TrimSpace(agent.PRURL), "(unknown)"),
		pendingComments,
		fallback(strings.TrimSpace(previousHeadSHA), "unknown"),
		fallback(strings.TrimSpace(latestHeadSHA), "unknown"),
	)
}

func formatReviewVerdictNeedsChanges(coder Agent, reviewer Agent, verdictComment *github.IssueComment) string {
	body := strings.TrimSpace(verdictComment.GetBody())
	if body == "" {
		body = "(empty comment)"
	}
	return fmt.Sprintf(
		"Repository Agent Orchestrator review cycle result: NEEDS_CHANGES.\n\n"+
			"PR URL: %s\n"+
			"Review agent: %s\n"+
			"Reviewed head SHA: %s\n"+
			"Review comment URL: %s\n\n"+
			"Review comment:\n%s\n\n"+
			"Apply requested updates, push commits to `%s`, and continue. "+
			"Repository Agent Orchestrator will launch a fresh review agent after the new push is detected.\n",
		fallback(strings.TrimSpace(coder.PRURL), "(unknown)"),
		reviewer.ID,
		fallback(strings.TrimSpace(reviewer.ObservedPRHeadSHA), "unknown"),
		fallback(strings.TrimSpace(verdictComment.GetHTMLURL()), "(unknown)"),
		body,
		fallback(strings.TrimSpace(coder.BranchName), "(unknown branch)"),
	)
}

func formatPreReviewHardGateFailed(coder Agent, mandatoryTests []string, gateErr error) string {
	failure := "unknown"
	if gateErr != nil {
		failure = strings.TrimSpace(gateErr.Error())
		if failure == "" {
			failure = "unknown"
		}
	}
	mandatoryTestsPlain := formatMandatoryTestsPlainList(mandatoryTests, "")

	return fmt.Sprintf(
		"Repository Agent Orchestrator review cycle result: pre-review hard gate failed.\n\n"+
			"PR URL: %s\n"+
			"Current head SHA: %s\n"+
			"Required gate commands:\n"+
			"%s\n"+
			"Failure: %s\n\n"+
			"Treat this as NEEDS_CHANGES. Fix the failing mandatory command(s), push commits to `%s`, and continue. "+
			"Repository Agent Orchestrator will launch a fresh review agent after the new push is detected.\n",
		fallback(strings.TrimSpace(coder.PRURL), "(unknown)"),
		fallback(strings.TrimSpace(coder.ObservedPRHeadSHA), "unknown"),
		mandatoryTestsPlain,
		failure,
		fallback(strings.TrimSpace(coder.BranchName), "(unknown branch)"),
	)
}

func formatPreReviewHardGateNotification(coder Agent, headSHA string, gateErr error) string {
	failure := "unknown"
	if gateErr != nil {
		failure = strings.TrimSpace(gateErr.Error())
		if failure == "" {
			failure = "unknown"
		}
	}

	return fmt.Sprintf(
		"Repository Agent Orchestrator: mandatory test gate failed before review for coder `%s` on PR %s; treating as NEEDS_CHANGES.\n\n"+
			"Head SHA: `%s`\n"+
			"Failure details:\n%s",
		fallback(strings.TrimSpace(coder.ID), "(unknown agent)"),
		fallback(strings.TrimSpace(coder.PRURL), "(unknown)"),
		fallback(strings.TrimSpace(headSHA), fallback(strings.TrimSpace(coder.ObservedPRHeadSHA), "unknown")),
		prefixLines(failure, "> "),
	)
}

func prefixLines(text, prefix string) string {
	lines := strings.Split(strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n"), "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

func formatCommentTime(t time.Time) string {
	if t.IsZero() {
		return "unknown"
	}
	return t.UTC().Format(time.RFC3339)
}

func fallback(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}
