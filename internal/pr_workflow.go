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
	"log"
	"strings"

	"github.com/google/go-github/v90/github"
)

func (b *Orchestrator) detectPR(ctx context.Context, agent Agent) error {
	if agent.Role != RoleCoder {
		return nil
	}

	head := fmt.Sprintf("%s:%s", b.cfg.RepoOwner, agent.BranchName)
	opt := &github.PullRequestListOptions{State: "open", Head: head, ListOptions: github.ListOptions{PerPage: 10, Page: 1}}
	prs, _, err := b.github.PullRequests.List(ctx, b.cfg.RepoOwner, b.cfg.RepoName, opt)
	if err != nil {
		return err
	}

	pr := selectTrackedPR(prs, b.defaultBaseBranch())
	if pr == nil {
		return nil
	}

	headSHA := ""
	if pr.GetHead() != nil {
		headSHA = pr.GetHead().GetSHA()
	}
	if !b.agents.SetPR(agent.ID, pr.GetNumber(), pr.GetTitle(), pr.GetHTMLURL(), headSHA) {
		return nil
	}
	_ = b.setCoderLifecycleState(agent.ID, StateWaiting)
	b.clearInputWaitTracking(agent.ID)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR detected for agent `%s`: %s", agent.ID, pr.GetHTMLURL()))
	log.Printf("pr detected agent=%s pr=%d", agent.ID, pr.GetNumber())
	if err := b.ensureReviewAgentForCoder(ctx, agent); err != nil {
		return fmt.Errorf("failed to launch review agent for pr %d: %w", pr.GetNumber(), err)
	}
	return nil
}

func (b *Orchestrator) detectRepoIndexPR(ctx context.Context, agent Agent) error {
	if agent.Role != RoleIndexer || agent.PRNumber != 0 {
		return nil
	}

	head := fmt.Sprintf("%s:%s", b.cfg.RepoOwner, agent.BranchName)
	opt := &github.PullRequestListOptions{State: "open", Head: head, ListOptions: github.ListOptions{PerPage: 10, Page: 1}}
	prs, _, err := b.github.PullRequests.List(ctx, b.cfg.RepoOwner, b.cfg.RepoName, opt)
	if err != nil {
		return err
	}

	pr := selectTrackedPR(prs, b.defaultBaseBranch())
	if pr == nil {
		return nil
	}

	headSHA := ""
	if pr.GetHead() != nil {
		headSHA = pr.GetHead().GetSHA()
	}
	if !b.agents.SetPR(agent.ID, pr.GetNumber(), pr.GetTitle(), pr.GetHTMLURL(), headSHA) {
		return nil
	}
	author := b.getAuthenticatedUserLogin(ctx)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR detected for repo indexing agent `%s`: %s %s", agent.ID, pr.GetHTMLURL(), author))
	log.Printf("repo index pr detected agent=%s pr=%d", agent.ID, pr.GetNumber())
	return nil
}

func selectTrackedPR(prs []*github.PullRequest, baseBranch string) *github.PullRequest {
	baseBranch = strings.TrimSpace(baseBranch)

	var fallback *github.PullRequest
	fallbackCount := 0
	for _, pr := range prs {
		if pr == nil {
			continue
		}
		ref := strings.TrimSpace(pr.GetBase().GetRef())
		if ref == "" {
			fallback = pr
			fallbackCount++
			continue
		}
		if strings.EqualFold(ref, baseBranch) {
			return pr
		}
	}

	if len(prs) == 1 && fallbackCount == 1 {
		return fallback
	}
	return nil
}

func isOrchestratorInternalComment(body string) bool {
	if _, ok := parseOrchestratorAgentID(body); !ok {
		return false
	}
	_, ok := parseOrchestratorAgentRole(body)
	return ok
}

func isInternalReviewVerdictBody(body string) bool {
	body = strings.TrimSpace(body)
	if body == "" {
		return false
	}
	if _, ok := parseReviewAgentID(body); !ok {
		return false
	}
	if _, ok := parseReviewedSHA(body); !ok {
		return false
	}
	_, ok := parseReviewVerdict(body)
	return ok
}

func isHumanFeedbackComment(body string) bool {
	body = strings.TrimSpace(body)
	if body == "" {
		return false
	}
	if isInternalReviewVerdictBody(body) {
		return false
	}
	return !isOrchestratorInternalComment(body)
}

func (b *Orchestrator) forwardNewComments(ctx context.Context, agent Agent) error {
	if agent.PRNumber == 0 {
		return nil
	}
	if b.coderHasActiveReviewInProgress(agent) {
		_ = b.setCoderLifecycleState(agent.ID, StateWaiting)
		return nil
	}

	reviewComments, err := b.listAllReviewComments(ctx, agent.PRNumber)
	if err != nil {
		return err
	}
	for _, c := range reviewComments {
		if b.agents.HasSeenReviewComment(agent.ID, c.GetID()) {
			continue
		}
		if !isHumanFeedbackComment(c.GetBody()) {
			_ = b.agents.MarkReviewCommentSeen(agent.ID, c.GetID())
			continue
		}
		if err := b.forwardReviewComment(ctx, agent, c); err != nil {
			return err
		}
	}

	issueComments, err := b.listAllIssueComments(ctx, agent.PRNumber)
	if err != nil {
		return err
	}
	for _, c := range issueComments {
		if b.agents.HasSeenIssueComment(agent.ID, c.GetID()) {
			continue
		}
		if !isHumanFeedbackComment(c.GetBody()) {
			_ = b.agents.MarkIssueCommentSeen(agent.ID, c.GetID())
			continue
		}
		if err := b.forwardIssueComment(ctx, agent, c); err != nil {
			return err
		}
	}

	return nil
}

func (b *Orchestrator) forwardReviewComment(ctx context.Context, agent Agent, c *github.PullRequestComment) error {
	agentSnapshot, ok := b.agents.Get(agent.ID)
	if !ok || agentSnapshot.Stopped {
		return nil
	}

	runtimeMsg := formatForwardedReviewComment(agentSnapshot, c)
	agentSnapshot, deliveredToRuntime, err := b.sendAutomatedRuntimeMessage(ctx, agentSnapshot, "new PR review comment", runtimeMsg, "")
	if err != nil {
		if deliveredToRuntime {
			if b.agents.MarkReviewCommentSeen(agent.ID, c.GetID()) {
				_ = b.agents.AddPendingReviewComment(agent.ID, c.GetID())
				b.demoteAgentForHumanFeedback(agent.ID)
				b.clearInputWaitAlert(agent.ID)
				b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: New PR review comment forwarded for agent `%s`", agent.ID))
			}
			return fmt.Errorf(
				"review comment reached runtime before delivery channel was quarantined: %w",
				err,
			)
		}
		return fmt.Errorf("failed to prepare runtime for forwarded review comment: %w", err)
	}

	if b.messenger != nil && isGitWorktreePath(agentSnapshot.WorktreePath) {
		msg := formatReviewCommentMarkdown(c)
		if err := b.messenger.SendMessage(agentSnapshot, msg); err != nil {
			return fmt.Errorf("failed to forward review comment: %w", err)
		}
	}

	if !b.agents.MarkReviewCommentSeen(agent.ID, c.GetID()) {
		return nil
	}
	_ = b.agents.AddPendingReviewComment(agent.ID, c.GetID())
	b.demoteAgentForHumanFeedback(agent.ID)
	if deliveredToRuntime {
		b.clearInputWaitAlert(agent.ID)
	}
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: New PR review comment forwarded for agent `%s`", agent.ID))
	return nil
}

func (b *Orchestrator) forwardIssueComment(ctx context.Context, agent Agent, c *github.IssueComment) error {
	agentSnapshot, ok := b.agents.Get(agent.ID)
	if !ok || agentSnapshot.Stopped {
		return nil
	}

	runtimeMsg := formatForwardedIssueComment(agentSnapshot, c)
	agentSnapshot, deliveredToRuntime, err := b.sendAutomatedRuntimeMessage(ctx, agentSnapshot, "new PR issue comment", runtimeMsg, "")
	if err != nil {
		if deliveredToRuntime {
			if b.agents.MarkIssueCommentSeen(agent.ID, c.GetID()) {
				_ = b.agents.AddPendingReviewComment(agent.ID, c.GetID())
				b.demoteAgentForHumanFeedback(agent.ID)
				b.clearInputWaitAlert(agent.ID)
				b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: New PR issue comment forwarded for agent `%s`", agent.ID))
			}
			return fmt.Errorf(
				"issue comment reached runtime before delivery channel was quarantined: %w",
				err,
			)
		}
		return fmt.Errorf("failed to prepare runtime for forwarded issue comment: %w", err)
	}

	if b.messenger != nil && isGitWorktreePath(agentSnapshot.WorktreePath) {
		msg := formatIssueCommentMarkdown(c)
		if err := b.messenger.SendMessage(agentSnapshot, msg); err != nil {
			return fmt.Errorf("failed to forward issue comment: %w", err)
		}
	}

	if !b.agents.MarkIssueCommentSeen(agent.ID, c.GetID()) {
		return nil
	}
	_ = b.agents.AddPendingReviewComment(agent.ID, c.GetID())
	b.demoteAgentForHumanFeedback(agent.ID)
	if deliveredToRuntime {
		b.clearInputWaitAlert(agent.ID)
	}
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: New PR issue comment forwarded for agent `%s`", agent.ID))
	return nil
}

func isInternalReviewVerdictComment(c *github.IssueComment) bool {
	if c == nil {
		return false
	}
	return isInternalReviewVerdictBody(c.GetBody())
}

func (b *Orchestrator) demoteAgentForHumanFeedback(agentID string) {
	if b == nil || b.agents == nil {
		return
	}
	current, ok := b.agents.Get(agentID)
	if !ok || current.Stopped {
		return
	}
	if current.State == StateWorking {
		return
	}
	b.agents.SetState(agentID, StateWorking, false)
}

func (b *Orchestrator) listAllReviewComments(ctx context.Context, prNumber int) ([]*github.PullRequestComment, error) {
	all := make([]*github.PullRequestComment, 0)
	opt := &github.PullRequestListCommentsOptions{Sort: "created", Direction: "asc", ListOptions: github.ListOptions{PerPage: 100, Page: 1}}
	for {
		comments, resp, err := b.github.PullRequests.ListComments(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber, opt)
		if err != nil {
			return nil, err
		}
		all = append(all, comments...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	return all, nil
}

func (b *Orchestrator) listAllIssueComments(ctx context.Context, prNumber int) ([]*github.IssueComment, error) {
	all := make([]*github.IssueComment, 0)
	opt := &github.IssueListCommentsOptions{
		Sort:        github.String("created"),
		Direction:   github.String("asc"),
		ListOptions: github.ListOptions{PerPage: 100, Page: 1},
	}
	for {
		comments, resp, err := b.github.Issues.ListComments(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber, opt)
		if err != nil {
			return nil, err
		}
		all = append(all, comments...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	return all, nil
}

func (b *Orchestrator) promptCommentRepliesAfterPush(ctx context.Context, agent Agent) error {
	if agent.PRNumber == 0 {
		return nil
	}

	latestHeadSHA, err := b.getPRHeadSHA(ctx, agent.PRNumber)
	if err != nil {
		return err
	}
	latestHeadSHA = strings.TrimSpace(latestHeadSHA)
	if latestHeadSHA == "" {
		return nil
	}

	previousHeadSHA := strings.TrimSpace(agent.ObservedPRHeadSHA)
	if previousHeadSHA == latestHeadSHA {
		return nil
	}

	pending := b.agents.PendingReviewCommentCount(agent.ID)
	shouldRemind := previousHeadSHA != "" && pending > 0 && b.runner != nil && !b.coderHasActiveReviewInProgress(agent)
	if shouldRemind {
		agentSnapshot, ok := b.agents.Get(agent.ID)
		if ok && !agentSnapshot.Stopped {
			reminder := formatPostPushCommentReplyReminder(agentSnapshot, pending, previousHeadSHA, latestHeadSHA)
			agentSnapshot, deliveredToRuntime, err := b.sendAutomatedRuntimeMessage(ctx, agentSnapshot, "post-push review reply reminder", reminder, reminder)
			if err != nil {
				return fmt.Errorf("failed to send post-push comment reply reminder to runtime: %w", err)
			}
			if deliveredToRuntime {
				b.clearInputWaitAlert(agent.ID)
				b.agents.Touch(agent.ID)
				log.Printf(
					"post-push reply reminder sent agent=%s pr=%d pending=%d prev_head=%s new_head=%s",
					agent.ID,
					agent.PRNumber,
					pending,
					previousHeadSHA,
					latestHeadSHA,
				)
			} else {
				log.Printf(
					"post-push reply reminder queued without runtime resume agent=%s pr=%d pending=%d prev_head=%s new_head=%s state=%s",
					agent.ID,
					agent.PRNumber,
					pending,
					previousHeadSHA,
					latestHeadSHA,
					agentSnapshot.State,
				)
			}
		}
	}
	if !b.agents.SetPRHeadSHA(agent.ID, latestHeadSHA) {
		return nil
	}
	return nil
}

func (b *Orchestrator) getAuthenticatedUserLogin(_ context.Context) string {
	email := strings.TrimSpace(b.cfg.WebexUID)
	if email == "" {
		return ""
	}
	return fmt.Sprintf("<@personEmail:%s>", email)
}

func (b *Orchestrator) getPRHeadSHA(ctx context.Context, prNumber int) (string, error) {
	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber)
	if err != nil {
		return "", err
	}
	head := pr.GetHead()
	if head == nil {
		return "", nil
	}
	return strings.TrimSpace(head.GetSHA()), nil
}

func (b *Orchestrator) ensurePRReadyForReview(ctx context.Context, prNumber int) error {
	if prNumber <= 0 {
		return fmt.Errorf("invalid pull request number %d", prNumber)
	}

	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber)
	if err != nil {
		return err
	}
	if !pr.GetDraft() {
		return nil
	}

	repoSlug := fmt.Sprintf("%s/%s", b.cfg.RepoOwner, b.cfg.RepoName)
	if err := b.execCommand(ctx, "", "gh", "pr", "ready", fmt.Sprintf("%d", prNumber), "-R", repoSlug); err != nil {
		return fmt.Errorf("gh pr ready failed for PR #%d: %w", prNumber, err)
	}
	return nil
}

func (b *Orchestrator) ensurePRDraft(ctx context.Context, prNumber int) error {
	if prNumber <= 0 {
		return fmt.Errorf("invalid pull request number %d", prNumber)
	}

	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber)
	if err != nil {
		return err
	}
	if pr.GetDraft() {
		return nil
	}

	repoSlug := fmt.Sprintf("%s/%s", b.cfg.RepoOwner, b.cfg.RepoName)
	if err := b.execCommand(ctx, "", "gh", "pr", "ready", "--undo", fmt.Sprintf("%d", prNumber), "-R", repoSlug); err != nil {
		return fmt.Errorf("gh pr ready --undo failed for PR #%d: %w", prNumber, err)
	}
	return nil
}

type prMergeStatus struct {
	HeadSHA        string
	MergeableState string
}

func (b *Orchestrator) getPRMergeStatus(ctx context.Context, prNumber int) (prMergeStatus, error) {
	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber)
	if err != nil {
		return prMergeStatus{}, err
	}
	status := prMergeStatus{
		MergeableState: strings.ToLower(strings.TrimSpace(pr.GetMergeableState())),
	}
	if head := pr.GetHead(); head != nil {
		status.HeadSHA = strings.TrimSpace(head.GetSHA())
	}
	return status, nil
}

func (b *Orchestrator) handlePRMergeConflicts(ctx context.Context, agent Agent) error {
	if agent.Role == RoleReviewer || agent.PRNumber == 0 {
		return nil
	}
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.Stopped {
		return nil
	}
	if current.State == StateDone || current.State == StateErrored || current.State == StateStopped || current.State == StateMerging {
		return nil
	}

	status, err := b.getPRMergeStatus(ctx, current.PRNumber)
	if err != nil {
		return err
	}
	if status.HeadSHA != "" {
		_ = b.agents.SetPRHeadSHA(current.ID, status.HeadSHA)
	}
	if status.MergeableState != "dirty" {
		if strings.TrimSpace(current.LastConflictHeadSHA) != "" {
			_ = b.agents.SetConflictHeadSHA(current.ID, "")
		}
		return nil
	}
	if status.HeadSHA != "" && strings.EqualFold(strings.TrimSpace(current.LastConflictHeadSHA), status.HeadSHA) {
		return nil
	}
	conflictGuidance := autoRebaseSteeringText
	if current.AdoptedPR {
		conflictGuidance += fmt.Sprintf(" Update the existing PR with `%s`; do not create another remote branch or PR.", continuationPushCommand(current))
	}

	if b.messenger != nil && isGitWorktreePath(current.WorktreePath) {
		msg := fmt.Sprintf("# Merge Conflict Detected\n\n%s\n", conflictGuidance)
		if err := b.messenger.SendMessage(current, msg); err != nil {
			return fmt.Errorf("failed to forward conflict guidance to inbox: %w", err)
		}
	}
	current, deliveredToRuntime, err := b.sendAutomatedRuntimeMessage(ctx, current, "PR merge conflicts detected", conflictGuidance, "")
	if err != nil {
		return fmt.Errorf("failed to resume runtime for conflict handling: %w", err)
	}
	_ = b.agents.SetConflictHeadSHA(current.ID, status.HeadSHA)
	if deliveredToRuntime {
		_ = b.agents.SetState(current.ID, StateWorking, false)
		b.clearInputWaitAlert(current.ID)
	}
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: merge conflicts detected on PR %s for agent `%s`; sent rebase guidance", fallback(strings.TrimSpace(current.PRURL), "(unknown)"), current.ID))
	log.Printf("merge conflicts detected agent=%s pr=%d head=%s state=%s", current.ID, current.PRNumber, status.HeadSHA, status.MergeableState)
	return nil
}

func (b *Orchestrator) maybeAutoMergeApprovedPR(ctx context.Context, agent Agent) error {
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.Stopped || current.PRNumber == 0 {
		return nil
	}
	if current.State != StateApproved {
		return nil
	}
	if current.AdoptedPR {
		return nil
	}
	if !b.coderCanStayApproved(current) {
		return nil
	}
	if err := b.ensurePRReadyForReview(ctx, current.PRNumber); err != nil {
		return fmt.Errorf("failed to ensure PR #%d is ready for review: %w", current.PRNumber, err)
	}

	approved, err := b.isPRApproved(ctx, current.PRNumber)
	if err != nil {
		return err
	}
	if !approved {
		return nil
	}
	return b.handleApprovedPR(ctx, current)
}

func (b *Orchestrator) maybePromoteReadyForHumanReview(ctx context.Context, agent Agent) error {
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.Stopped || current.Role != RoleCoder || current.PRNumber == 0 {
		return nil
	}
	if current.State != StateWaiting {
		return nil
	}
	if !b.coderCanStayApproved(current) {
		return nil
	}
	if err := b.ensurePRReadyForReview(ctx, current.PRNumber); err != nil {
		return fmt.Errorf("failed to ensure PR #%d is ready for human review: %w", current.PRNumber, err)
	}
	b.agents.SetState(current.ID, StateApproved, false)
	author := b.getAuthenticatedUserLogin(ctx)
	if current.AdoptedPR {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: adopted PR ready for human review from agent `%s`: %s (waiting for a human to merge) %s", current.ID, current.PRURL, author))
	} else {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR ready for human review from agent `%s`: %s (waiting for GitHub approval before merge) %s", current.ID, current.PRURL, author))
	}
	return nil
}

func (b *Orchestrator) handleTerminalManualReviewerPR(ctx context.Context, agent Agent) error {
	if agent.Role != RoleReviewer || strings.TrimSpace(agent.ParentAgentID) != "" || agent.PRNumber == 0 {
		return nil
	}
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.Stopped {
		return nil
	}
	if current.State == StateDone || current.State == StateErrored || current.State == StateStopped {
		return nil
	}

	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, current.PRNumber)
	if err != nil {
		return err
	}
	return b.handleTerminalManualReviewerPRSnapshot(ctx, current, pr)
}

func (b *Orchestrator) handleTerminalManualReviewerPRSnapshot(
	ctx context.Context,
	current Agent,
	pr *github.PullRequest,
) error {
	unlockSend := b.agents.lockRuntimeSend(current.ID)
	defer unlockSend()
	authoritative, ok := b.agents.Get(current.ID)
	if !ok || agentLifecycleTerminal(&authoritative) {
		return nil
	}
	current = authoritative
	merged := pr.GetMerged()
	closed := strings.EqualFold(strings.TrimSpace(pr.GetState()), "closed")
	liveHeadSHA := ""
	if head := pr.GetHead(); head != nil {
		liveHeadSHA = strings.TrimSpace(head.GetSHA())
	}
	if !merged && !closed {
		pinnedHeadSHA := strings.TrimSpace(current.ObservedPRHeadSHA)
		if current.Paused && pinnedHeadSHA != "" && liveHeadSHA != "" && !strings.EqualFold(pinnedHeadSHA, liveHeadSHA) {
			b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: manual review agent `%s` is stale because PR head moved from `%s` to `%s`; cleaning up", current.ID, abbreviateSHA(pinnedHeadSHA), abbreviateSHA(liveHeadSHA)))
			if err := b.retireReviewerPreservingPersistedHandoff(current, StateDone, true, "manual reviewer superseded by new head"); err != nil {
				return err
			}
			log.Printf("manual reviewer stale head cleaned id=%s pr=%d old_head=%s new_head=%s", current.ID, current.PRNumber, abbreviateSHA(pinnedHeadSHA), abbreviateSHA(liveHeadSHA))
		}
		return nil
	}

	action := "closed"
	actor := "unknown"
	if merged {
		action = "merged"
		if pr.GetMergedBy() != nil {
			login := strings.TrimSpace(pr.GetMergedBy().GetLogin())
			if login != "" {
				actor = login
			}
		}
	}

	if merged {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR merged by `%s` for manual review agent `%s`; cleaning up", actor, current.ID))
	} else {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR closed for manual review agent `%s`; cleaning up", current.ID))
	}

	if err := b.retireReviewerPreservingPersistedHandoff(current, StateDone, true, "manual reviewer terminal pr cleanup"); err != nil {
		return err
	}

	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: %s PR cleaned up for manual review agent `%s`", action, current.ID))
	if merged {
		log.Printf("manual reviewer terminal pr cleaned id=%s pr=%d action=%s actor=%s", current.ID, current.PRNumber, action, actor)
	} else {
		log.Printf("manual reviewer terminal pr cleaned id=%s pr=%d action=%s", current.ID, current.PRNumber, action)
	}
	return nil
}

func (b *Orchestrator) handleTerminalPR(ctx context.Context, agent Agent) error {
	if agent.Role == RoleReviewer || agent.PRNumber == 0 {
		return nil
	}
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.Stopped {
		return nil
	}
	if current.State == StateDone || current.State == StateErrored || current.State == StateStopped {
		return nil
	}

	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, current.PRNumber)
	if err != nil {
		return err
	}
	return b.handleTerminalPRSnapshot(ctx, current, pr)
}

func (b *Orchestrator) handleTerminalPRSnapshot(
	ctx context.Context,
	current Agent,
	pr *github.PullRequest,
) error {
	unlockSend := b.agents.lockRuntimeSend(current.ID)
	defer unlockSend()
	authoritative, ok := b.agents.Get(current.ID)
	if !ok || agentLifecycleTerminal(&authoritative) {
		return nil
	}
	current = authoritative
	merged := pr.GetMerged()
	closed := strings.EqualFold(strings.TrimSpace(pr.GetState()), "closed")
	if !merged && !closed {
		return nil
	}

	action := "closed"
	actor := "unknown"
	if merged {
		action = "merged"
		if pr.GetMergedBy() != nil {
			login := strings.TrimSpace(pr.GetMergedBy().GetLogin())
			if login != "" {
				actor = login
			}
		}
	}

	b.agents.SetState(current.ID, StateMerging, false)
	if merged {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR merged by `%s` for agent `%s`; cleaning up", actor, current.ID))
	} else {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR closed for agent `%s`; cleaning up", current.ID))
	}

	if err := b.cleanupTerminalPRAgent(ctx, current); err != nil {
		b.agents.SetState(current.ID, StateErrored, false)
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: cleanup failed after %s PR for agent `%s`", action, current.ID))
		return err
	}

	b.agents.SetState(current.ID, StateDone, true)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: %s PR cleaned up for agent `%s`", action, current.ID))
	if merged {
		log.Printf("agent terminal pr cleaned id=%s pr=%d action=%s actor=%s", current.ID, current.PRNumber, action, actor)
	} else {
		log.Printf("agent terminal pr cleaned id=%s pr=%d action=%s", current.ID, current.PRNumber, action)
	}
	return nil
}

func (b *Orchestrator) isPRApproved(ctx context.Context, prNumber int) (bool, error) {
	reviews, err := b.listAllReviews(ctx, prNumber)
	if err != nil {
		return false, err
	}
	return isApprovedFromReviews(reviews), nil
}

func isApprovedFromReviews(reviews []*github.PullRequestReview) bool {
	latestByUser := make(map[string]string)
	for _, r := range reviews {
		user := r.GetUser().GetLogin()
		if user == "" {
			continue
		}
		latestByUser[user] = strings.ToUpper(strings.TrimSpace(r.GetState()))
	}

	approvals := 0
	changesRequested := false
	for _, state := range latestByUser {
		switch state {
		case "APPROVED":
			approvals++
		case "CHANGES_REQUESTED":
			changesRequested = true
		}
	}

	return approvals >= 1 && !changesRequested
}

func (b *Orchestrator) listAllReviews(ctx context.Context, prNumber int) ([]*github.PullRequestReview, error) {
	all := make([]*github.PullRequestReview, 0)
	opt := &github.ListOptions{PerPage: 100, Page: 1}
	for {
		reviews, resp, err := b.github.PullRequests.ListReviews(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber, opt)
		if err != nil {
			return nil, err
		}
		all = append(all, reviews...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	return all, nil
}

func (b *Orchestrator) handleApprovedPR(ctx context.Context, agent Agent) error {
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.Stopped {
		return nil
	}
	if current.State != StateApproved || current.State == StateMerging || current.State == StateDone {
		return nil
	}

	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR approved for agent `%s`; merging", agent.ID))
	b.agents.SetState(agent.ID, StateMerging, false)

	mergeMsg := fmt.Sprintf("Repository Agent Orchestrator merge issue #%d", current.IssueNumber)
	result, _, err := b.github.PullRequests.Merge(ctx, b.cfg.RepoOwner, b.cfg.RepoName, current.PRNumber, mergeMsg, &github.PullRequestOptions{MergeMethod: b.cfg.MergeMethod})
	if err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: merge failed for agent `%s` PR %s", agent.ID, current.PRURL))
		return err
	}
	if !result.GetMerged() {
		b.agents.SetState(agent.ID, StateErrored, false)
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: merge API did not merge PR for agent `%s`", agent.ID))
		return errors.New("pull request not merged")
	}

	if err := b.cleanupTerminalPRAgent(ctx, current); err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: merged PR but cleanup failed for agent `%s`", agent.ID))
		return err
	}

	b.agents.SetState(agent.ID, StateDone, true)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: merged and cleaned up agent `%s`", agent.ID))
	log.Printf("agent merged and cleaned id=%s pr=%d", agent.ID, current.PRNumber)
	return nil
}

func (b *Orchestrator) cleanupTerminalPRAgent(ctx context.Context, agent Agent) error {
	if err := b.cleanupActiveReviewerForTerminalPR(ctx, agent); err != nil {
		return err
	}
	b.captureAgentHandoffNonFatal(agent)
	if err := b.stopRuntime(agent); err != nil {
		return fmt.Errorf("failed to stop runtime for merged agent: %w", err)
	}
	b.clearInputWaitTracking(agent.ID)
	if err := b.removeAgentRuntimeState(agent); err != nil {
		return fmt.Errorf(
			"failed to remove runtime state for terminal PR agent: %w",
			err,
		)
	}

	if !agent.AdoptedPR {
		if err := b.execCommand(ctx, b.cfg.RepoPath, "git", "push", "origin", "--delete", agent.BranchName); err != nil {
			log.Printf("non-fatal: remote branch deletion failed for %s: %s", agent.BranchName, b.safeError(err))
		}
	}

	if err := b.cleanupWorktree(ctx, agent.WorktreePath, agent.BranchName); err != nil {
		return err
	}

	if err := b.syncBaseBranch(ctx); err != nil {
		return err
	}

	return nil
}

func (b *Orchestrator) cleanupActiveReviewerForTerminalPR(ctx context.Context, agent Agent) error {
	if agent.Role != RoleCoder {
		return nil
	}
	errs := make([]string, 0)
	for _, reviewer := range b.relatedReviewersForCoder(agent) {
		errs = appendCleanupError(errs, b.retireReviewerPreservingPersistedHandoff(reviewer, StateDone, true, "terminal pr cleanup"))
	}
	if activeReviewerID := strings.TrimSpace(agent.ActiveReviewAgentID); activeReviewerID != "" {
		_ = b.agents.ClearActiveReviewAgentIfMatches(agent.ID, activeReviewerID)
	}
	if len(errs) > 0 {
		return fmt.Errorf("terminal review cleanup completed with errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
