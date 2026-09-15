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
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/go-github/v90/github"
)

type AgentRole string

const (
	RoleCoder    AgentRole = "coder"
	RoleReviewer AgentRole = "reviewer"
	RoleIndexer  AgentRole = "repo_indexer"
)

func agentRoleLabel(role AgentRole) string {
	switch role {
	case RoleReviewer:
		return "review agent"
	case RoleIndexer:
		return "repo indexing agent"
	default:
		return "coding agent"
	}
}

func newAgentID(role AgentRole, issueNumber int, now time.Time) string {
	switch role {
	case RoleReviewer:
		// Reviewers are disposable and may be relaunched multiple times in quick succession.
		return fmt.Sprintf("review-agent-%d-%d", issueNumber, now.UnixNano())
	case RoleIndexer:
		return fmt.Sprintf("index-agent-%d", now.Unix())
	default:
		// Coding agents are unique per active issue, so second precision is sufficient.
		return fmt.Sprintf("coding-agent-%d-%d", issueNumber, now.Unix())
	}
}

type ReviewVerdict string

const (
	ReviewVerdictThumbsUp     ReviewVerdict = "THUMBS_UP"
	ReviewVerdictNeedsChanges ReviewVerdict = "NEEDS_CHANGES"
)

const mandatoryTestGateTimeout = 45 * time.Minute

const (
	reviewSameHeadAttemptLimit = 3
	reviewSameHeadRetryDelay   = 30 * time.Second
)

var (
	hardGatePauseDetectionInterval = 10 * time.Second
	hardGatePauseThreshold         = time.Minute
)

var (
	reviewedSHAPattern           = regexp.MustCompile(`(?mi)^CODEX_REVIEWED_SHA:\s*([0-9a-fA-F]{7,64})\s*$`)
	reviewObjectIDPattern        = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)
	reviewVerdictPattern         = regexp.MustCompile(`(?mi)^CODEX_VERDICT:\s*(THUMBS_UP|NEEDS_CHANGES)\s*$`)
	orchestratorAgentIDPattern   = regexp.MustCompile(`(?mi)^CODEX_AGENT_ID:\s*([^\s]+)\s*$`)
	orchestratorAgentRolePattern = regexp.MustCompile(`(?mi)^CODEX_AGENT_ROLE:\s*([^\s]+)\s*$`)
)

type reviewLaunchTarget struct {
	LaunchKey           string
	CoderID             string
	Automatic           bool
	SkipHardGate        bool
	IssueNumber         int
	IssueTitle          string
	IssueBody           string
	BranchName          string
	PRNumber            int
	PRTitle             string
	PRURL               string
	HumanReviewGuidance []string
}

func parseReviewVerdict(text string) (ReviewVerdict, bool) {
	match := reviewVerdictPattern.FindStringSubmatch(strings.TrimSpace(text))
	if len(match) != 2 {
		return "", false
	}
	switch strings.ToUpper(strings.TrimSpace(match[1])) {
	case string(ReviewVerdictThumbsUp):
		return ReviewVerdictThumbsUp, true
	case string(ReviewVerdictNeedsChanges):
		return ReviewVerdictNeedsChanges, true
	default:
		return "", false
	}
}

func parseReviewAgentID(text string) (string, bool) {
	if agentID, ok := parseOrchestratorAgentID(text); ok {
		if role, roleOK := parseOrchestratorAgentRole(text); roleOK && role == RoleReviewer {
			return agentID, true
		}
	}
	return "", false
}

func parseReviewedSHA(text string) (string, bool) {
	match := reviewedSHAPattern.FindStringSubmatch(strings.TrimSpace(text))
	if len(match) != 2 {
		return "", false
	}
	sha := strings.ToLower(strings.TrimSpace(match[1]))
	return sha, sha != ""
}

func parseOrchestratorAgentID(text string) (string, bool) {
	match := orchestratorAgentIDPattern.FindStringSubmatch(strings.TrimSpace(text))
	if len(match) != 2 {
		return "", false
	}
	id := strings.TrimSpace(match[1])
	return id, id != ""
}

func parseOrchestratorAgentRole(text string) (AgentRole, bool) {
	match := orchestratorAgentRolePattern.FindStringSubmatch(strings.TrimSpace(text))
	if len(match) != 2 {
		return "", false
	}
	role := AgentRole(strings.TrimSpace(match[1]))
	return role, strings.TrimSpace(string(role)) != ""
}

func isReviewerVerdictComment(commentBody, reviewerID, expectedSHA string) (ReviewVerdict, bool) {
	reviewAgentID, ok := parseReviewAgentID(commentBody)
	if !ok || !strings.EqualFold(strings.TrimSpace(reviewerID), reviewAgentID) {
		return "", false
	}
	reviewedSHA, ok := parseReviewedSHA(commentBody)
	if !ok || !strings.EqualFold(strings.TrimSpace(expectedSHA), reviewedSHA) {
		return "", false
	}
	verdict, ok := parseReviewVerdict(commentBody)
	if !ok {
		return "", false
	}
	return verdict, true
}

func maxIssueCommentID(comments []*github.IssueComment) int64 {
	var maxID int64
	for _, c := range comments {
		if c.GetID() > maxID {
			maxID = c.GetID()
		}
	}
	return maxID
}

func reviewerCanBlockLaunch(reviewer Agent) bool {
	if reviewer.Role != RoleReviewer || reviewer.Stopped ||
		(reviewer.ReviewCycle != nil && reviewer.ReviewCycle.Stale) {
		return false
	}
	switch reviewer.State {
	case StateDone, StateErrored, StateStopped:
		return false
	}
	if reviewer.Paused {
		return false
	}
	if strings.TrimSpace(reviewer.RuntimeHandle.Session) != "" {
		return true
	}
	if (reviewer.State == StateInitializing || reviewer.State == StateReviewGate) && reviewer.ReviewCycle != nil {
		return true
	}
	return isGitWorktreePath(reviewer.WorktreePath)
}

func (t reviewLaunchTarget) promptAgent() Agent {
	return Agent{
		ID:                  strings.TrimSpace(t.CoderID),
		IssueNumber:         t.IssueNumber,
		IssueTitle:          t.IssueTitle,
		IssueBody:           t.IssueBody,
		BranchName:          t.BranchName,
		PRNumber:            t.PRNumber,
		PRTitle:             t.PRTitle,
		PRURL:               t.PRURL,
		HumanReviewGuidance: append([]string(nil), t.HumanReviewGuidance...),
	}
}

func reviewLaunchKeyForPR(prNumber int) string {
	return fmt.Sprintf("pr-%d", prNumber)
}

func reviewLaunchTargetFromCoder(coder Agent) reviewLaunchTarget {
	return reviewLaunchTarget{
		LaunchKey:           reviewLaunchKeyForPR(coder.PRNumber),
		CoderID:             strings.TrimSpace(coder.ID),
		IssueNumber:         coder.IssueNumber,
		IssueTitle:          coder.IssueTitle,
		IssueBody:           coder.IssueBody,
		BranchName:          prHeadBranch(coder),
		PRNumber:            coder.PRNumber,
		PRTitle:             coder.PRTitle,
		PRURL:               coder.PRURL,
		HumanReviewGuidance: append([]string(nil), coder.HumanReviewGuidance...),
	}
}

func linkedReviewerOwnsCurrentHead(coder, reviewer Agent) bool {
	if coder.Role != RoleCoder || reviewer.Role != RoleReviewer {
		return false
	}
	if reviewer.ReviewCycle != nil && reviewer.ReviewCycle.Stale {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(reviewer.ParentAgentID), strings.TrimSpace(coder.ID)) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(coder.ActiveReviewAgentID), strings.TrimSpace(reviewer.ID)) {
		return false
	}
	coderHead := strings.TrimSpace(coder.ObservedPRHeadSHA)
	reviewerHead := strings.TrimSpace(reviewer.ObservedPRHeadSHA)
	if coderHead != "" && reviewerHead != "" && !strings.EqualFold(coderHead, reviewerHead) {
		return false
	}
	return true
}

func (b *Orchestrator) reviewLaunchStillOwned(reviewerID, coderID string) bool {
	if b == nil || b.agents == nil {
		return false
	}
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok || reviewer.Stopped {
		return false
	}
	switch reviewer.State {
	case StateDone, StateErrored, StateStopped:
		return false
	}
	if reviewer.Paused {
		return false
	}
	coderID = strings.TrimSpace(coderID)
	if coderID == "" {
		return true
	}
	coder, ok := b.agents.Get(coderID)
	if !ok || coder.Stopped {
		return false
	}
	return linkedReviewerOwnsCurrentHead(coder, reviewer)
}

func (b *Orchestrator) findBlockingReviewerForPR(prNumber int) (Agent, bool) {
	if b == nil || b.agents == nil || prNumber <= 0 {
		return Agent{}, false
	}
	agents := b.agents.List()
	var matched Agent
	found := false
	for _, agent := range agents {
		if agent.Role != RoleReviewer || agent.PRNumber != prNumber {
			continue
		}
		if !reviewerCanBlockLaunch(agent) {
			continue
		}
		if !found || agent.LastActivityTime.After(matched.LastActivityTime) {
			matched = agent
			found = true
		}
	}
	return matched, found
}

func (b *Orchestrator) retireStaleReviewersForPR(prNumber int) {
	if b == nil || b.agents == nil || prNumber <= 0 {
		return
	}
	for _, reviewer := range b.agents.List() {
		if reviewer.Role != RoleReviewer || reviewer.PRNumber != prNumber {
			continue
		}
		switch reviewer.State {
		case StateDone, StateErrored, StateStopped:
			continue
		}
		if reviewerCanBlockLaunch(reviewer) {
			parentID := strings.TrimSpace(reviewer.ParentAgentID)
			if parentID == "" {
				continue
			}
			coder, ok := b.agents.Get(parentID)
			if ok && !coder.Stopped && linkedReviewerOwnsCurrentHead(coder, reviewer) {
				continue
			}
		}
		if err := b.retireReviewer(reviewer, StateErrored, false, "stale reviewer retirement"); err != nil {
			log.Printf("non-fatal: stale review cleanup failed reviewer=%s pr=%d: %s", reviewer.ID, reviewer.PRNumber, b.safeError(err))
		}
	}
}

func (b *Orchestrator) loadReviewLaunchTarget(ctx context.Context, prNumber int) (reviewLaunchTarget, string, error) {
	if prNumber <= 0 {
		return reviewLaunchTarget{}, "", fmt.Errorf("invalid PR number: %d", prNumber)
	}
	if coder, ok := b.findTrackedCoderForPR(prNumber); ok {
		headSHA, err := b.getPRHeadSHA(ctx, coder.PRNumber)
		if err != nil {
			return reviewLaunchTarget{}, "", fmt.Errorf("failed to determine PR head for PR #%d: %w", prNumber, err)
		}
		headSHA = strings.TrimSpace(headSHA)
		if headSHA == "" {
			return reviewLaunchTarget{}, "", fmt.Errorf("pull request head sha is empty for PR #%d", prNumber)
		}
		if !b.agents.SetPRHeadSHA(coder.ID, headSHA) {
			return reviewLaunchTarget{}, "", fmt.Errorf("failed to record PR head sha for coder %s", coder.ID)
		}
		current, ok := b.agents.Get(coder.ID)
		if !ok {
			return reviewLaunchTarget{}, "", fmt.Errorf("coder not found after updating head: %s", coder.ID)
		}
		target := reviewLaunchTargetFromCoder(current)
		target.SkipHardGate = true
		return target, headSHA, nil
	}
	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber)
	if err != nil {
		return reviewLaunchTarget{}, "", fmt.Errorf("failed to determine PR head for PR #%d: %w", prNumber, err)
	}
	headSHA := strings.TrimSpace(pr.GetHead().GetSHA())
	if headSHA == "" {
		return reviewLaunchTarget{}, "", fmt.Errorf("pull request head sha is empty for PR #%d", prNumber)
	}
	branchName := strings.TrimSpace(pr.GetHead().GetRef())
	if branchName == "" {
		branchName = fmt.Sprintf("pr-%d", prNumber)
	}
	return reviewLaunchTarget{
		LaunchKey:    reviewLaunchKeyForPR(prNumber),
		SkipHardGate: true,
		IssueNumber:  0,
		IssueTitle:   pr.GetTitle(),
		IssueBody:    pr.GetBody(),
		BranchName:   branchName,
		PRNumber:     prNumber,
		PRTitle:      pr.GetTitle(),
		PRURL:        pr.GetHTMLURL(),
	}, headSHA, nil
}

func (b *Orchestrator) findTrackedCoderForPR(prNumber int) (Agent, bool) {
	if b == nil || b.agents == nil || prNumber <= 0 {
		return Agent{}, false
	}
	agents := b.agents.List()
	var matched Agent
	found := false
	for _, agent := range agents {
		if agent.Role != RoleCoder || agent.PRNumber != prNumber || agent.Stopped {
			continue
		}
		switch agent.State {
		case StateDone, StateErrored, StateStopped:
			continue
		}
		if !found || agent.LastActivityTime.After(matched.LastActivityTime) {
			matched = agent
			found = true
		}
	}
	return matched, found
}

func (m *AgentManager) SetActiveReviewAgent(agentID, reviewerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	reviewerID = strings.TrimSpace(reviewerID)
	if reviewerID != "" && agentLifecycleTerminal(a) {
		return false
	}
	a.ActiveReviewAgentID = reviewerID
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) ClearActiveReviewAgentIfMatches(agentID, reviewerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(a.ActiveReviewAgentID), strings.TrimSpace(reviewerID)) {
		return false
	}
	a.ActiveReviewAgentID = ""
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) RecordReviewVerdict(agentID, reviewedHeadSHA string, verdict ReviewVerdict, commentID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.LastReviewedHeadSHA = strings.TrimSpace(reviewedHeadSHA)
	a.ManualReviewHold = false
	a.LastReviewVerdict = verdict
	a.LastReviewCommentID = commentID
	a.LastPreReviewGateFailureHeadSHA = ""
	a.LastPreReviewGateFailureAt = time.Time{}
	a.LastActivityTime = time.Now().UTC()
	return true
}

type PendingReviewVerdictApplication struct {
	ReviewerID          string        `json:"reviewer_id"`
	ReviewedHeadSHA     string        `json:"reviewed_head_sha"`
	Verdict             ReviewVerdict `json:"verdict"`
	CommentID           int64         `json:"comment_id"`
	ReviewInstruction   string        `json:"review_instruction,omitempty"`
	CorrectionAttemptID string        `json:"correction_attempt_id,omitempty"`
}

func clonePendingReviewVerdictApplication(
	pending *PendingReviewVerdictApplication,
) *PendingReviewVerdictApplication {
	if pending == nil {
		return nil
	}
	clone := *pending
	return &clone
}

func validatePendingReviewVerdictApplication(
	pending *PendingReviewVerdictApplication,
) error {
	if pending == nil {
		return errors.New("pending review verdict is missing")
	}
	if strings.TrimSpace(pending.ReviewerID) == "" {
		return errors.New("pending review verdict reviewer is missing")
	}
	if !reviewObjectIDPattern.MatchString(
		strings.TrimSpace(pending.ReviewedHeadSHA),
	) {
		return errors.New("pending review verdict head is invalid")
	}
	switch pending.Verdict {
	case ReviewVerdictNeedsChanges:
		if strings.TrimSpace(pending.ReviewInstruction) == "" {
			return errors.New(
				"pending NEEDS_CHANGES verdict instruction is missing",
			)
		}
	case ReviewVerdictThumbsUp:
	default:
		return fmt.Errorf(
			"pending review verdict %q is unsupported",
			pending.Verdict,
		)
	}
	if pending.CommentID <= 0 {
		return errors.New("pending review verdict comment ID is invalid")
	}
	if strings.TrimSpace(pending.CorrectionAttemptID) != "" &&
		pending.Verdict != ReviewVerdictNeedsChanges {
		return errors.New(
			"pending correction attempt requires NEEDS_CHANGES",
		)
	}
	return nil
}

type linkedReviewVerdictIntentRollback struct {
	coderID                  string
	reviewerID               string
	activeReviewAgentID      string
	pendingReviewVerdict     *PendingReviewVerdictApplication
	coderLastActivityTime    time.Time
	reviewerState            AgentState
	reviewerStopped          bool
	reviewerPaused           bool
	reviewerLastActivityTime time.Time
}

func (m *AgentManager) beginLinkedReviewVerdictApplication(
	coderID string,
	reviewerID string,
	pending PendingReviewVerdictApplication,
) (linkedReviewVerdictIntentRollback, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validatePendingReviewVerdictApplication(&pending); err != nil {
		return linkedReviewVerdictIntentRollback{}, false
	}
	coder, coderOK := m.agents[strings.TrimSpace(coderID)]
	reviewer, reviewerOK := m.agents[strings.TrimSpace(reviewerID)]
	if !coderOK || coder == nil || coder.Role != RoleCoder || coder.Stopped ||
		coder.PendingReviewVerdict != nil ||
		!reviewerOK || reviewer == nil || reviewer.Role != RoleReviewer ||
		agentLifecycleTerminal(reviewer) ||
		(reviewer.ReviewCycle != nil && reviewer.ReviewCycle.Stale) ||
		!strings.EqualFold(
			strings.TrimSpace(reviewer.ParentAgentID),
			strings.TrimSpace(coder.ID),
		) ||
		!strings.EqualFold(
			strings.TrimSpace(coder.ActiveReviewAgentID),
			strings.TrimSpace(reviewer.ID),
		) ||
		!strings.EqualFold(
			strings.TrimSpace(pending.ReviewerID),
			strings.TrimSpace(reviewer.ID),
		) {
		return linkedReviewVerdictIntentRollback{}, false
	}
	coderHeadSHA := strings.TrimSpace(coder.ObservedPRHeadSHA)
	reviewerHeadSHA := strings.TrimSpace(reviewer.ObservedPRHeadSHA)
	if (reviewerHeadSHA != "" &&
		!strings.EqualFold(reviewerHeadSHA, pending.ReviewedHeadSHA)) ||
		(coderHeadSHA != "" &&
			!strings.EqualFold(coderHeadSHA, pending.ReviewedHeadSHA)) {
		return linkedReviewVerdictIntentRollback{}, false
	}

	rollback := linkedReviewVerdictIntentRollback{
		coderID:                  coder.ID,
		reviewerID:               reviewer.ID,
		activeReviewAgentID:      coder.ActiveReviewAgentID,
		pendingReviewVerdict:     clonePendingReviewVerdictApplication(coder.PendingReviewVerdict),
		coderLastActivityTime:    coder.LastActivityTime,
		reviewerState:            reviewer.State,
		reviewerStopped:          reviewer.Stopped,
		reviewerPaused:           reviewer.Paused,
		reviewerLastActivityTime: reviewer.LastActivityTime,
	}
	now := time.Now().UTC()
	coder.PendingReviewVerdict = clonePendingReviewVerdictApplication(&pending)
	coder.ActiveReviewAgentID = ""
	coder.LastActivityTime = now
	reviewer.State = StateStopped
	reviewer.Stopped = true
	reviewer.Paused = false
	reviewer.LastActivityTime = now
	return rollback, true
}

func (m *AgentManager) rollbackLinkedReviewVerdictIntent(
	rollback linkedReviewVerdictIntentRollback,
) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	coder, coderOK := m.agents[rollback.coderID]
	reviewer, reviewerOK := m.agents[rollback.reviewerID]
	if !coderOK || coder == nil ||
		coder.PendingReviewVerdict == nil ||
		!strings.EqualFold(
			coder.PendingReviewVerdict.ReviewerID,
			rollback.reviewerID,
		) {
		return false
	}
	coder.PendingReviewVerdict =
		clonePendingReviewVerdictApplication(rollback.pendingReviewVerdict)
	coder.ActiveReviewAgentID = rollback.activeReviewAgentID
	coder.LastActivityTime = rollback.coderLastActivityTime
	if reviewerOK && reviewer != nil &&
		reviewer.State == StateStopped && reviewer.Stopped {
		reviewer.State = rollback.reviewerState
		reviewer.Stopped = rollback.reviewerStopped
		reviewer.Paused = rollback.reviewerPaused
		reviewer.LastActivityTime = rollback.reviewerLastActivityTime
	}
	return true
}

type linkedReviewVerdictCompletionRollback struct {
	coderID                         string
	reviewerID                      string
	pendingReviewVerdict            *PendingReviewVerdictApplication
	lastReviewedHeadSHA             string
	lastManualReviewHold            bool
	lastReviewVerdict               ReviewVerdict
	lastReviewCommentID             int64
	lastPreReviewGateFailureHeadSHA string
	lastPreReviewGateFailureAt      time.Time
	launchAttempts                  []DurableLaunchAttempt
	coderLastActivityTime           time.Time
	reviewerState                   AgentState
	reviewerStopped                 bool
	reviewerPaused                  bool
	reviewerLastActivityTime        time.Time
}

func (m *AgentManager) completePendingReviewVerdict(
	coderID string,
) (linkedReviewVerdictCompletionRollback, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	coder, ok := m.agents[strings.TrimSpace(coderID)]
	if !ok || coder == nil || coder.Role != RoleCoder || coder.Stopped ||
		coder.PendingReviewVerdict == nil {
		return linkedReviewVerdictCompletionRollback{}, false
	}
	pending := clonePendingReviewVerdictApplication(
		coder.PendingReviewVerdict,
	)
	if err := validatePendingReviewVerdictApplication(pending); err != nil {
		return linkedReviewVerdictCompletionRollback{}, false
	}
	reviewer := m.agents[pending.ReviewerID]
	rollback := linkedReviewVerdictCompletionRollback{
		coderID:                         coder.ID,
		reviewerID:                      pending.ReviewerID,
		pendingReviewVerdict:            pending,
		lastReviewedHeadSHA:             coder.LastReviewedHeadSHA,
		lastManualReviewHold:            coder.ManualReviewHold,
		lastReviewVerdict:               coder.LastReviewVerdict,
		lastReviewCommentID:             coder.LastReviewCommentID,
		lastPreReviewGateFailureHeadSHA: coder.LastPreReviewGateFailureHeadSHA,
		lastPreReviewGateFailureAt:      coder.LastPreReviewGateFailureAt,
		launchAttempts:                  cloneDurableLaunchAttempts(coder.LaunchAttempts),
		coderLastActivityTime:           coder.LastActivityTime,
	}
	if reviewer != nil {
		rollback.reviewerState = reviewer.State
		rollback.reviewerStopped = reviewer.Stopped
		rollback.reviewerPaused = reviewer.Paused
		rollback.reviewerLastActivityTime = reviewer.LastActivityTime
	}
	now := time.Now().UTC()
	if reviewer != nil && reviewer.ReviewCycle != nil {
		for index := range coder.LaunchAttempts {
			attempt := &coder.LaunchAttempts[index]
			if attempt.ID != reviewer.ReviewCycle.ID {
				continue
			}
			if _, err := transitionDurableLaunchAttempt(
				attempt,
				DurableLaunchCompleted,
				nil,
				time.Time{},
				now,
			); err != nil {
				return linkedReviewVerdictCompletionRollback{}, false
			}
			break
		}
	}
	coder.LastReviewedHeadSHA = pending.ReviewedHeadSHA
	coder.ManualReviewHold = false
	coder.LastReviewVerdict = pending.Verdict
	coder.LastReviewCommentID = pending.CommentID
	coder.LastPreReviewGateFailureHeadSHA = ""
	coder.LastPreReviewGateFailureAt = time.Time{}
	coder.PendingReviewVerdict = nil
	coder.LastActivityTime = now
	if reviewer != nil {
		reviewer.State = StateDone
		reviewer.Stopped = true
		reviewer.Paused = false
		reviewer.LastActivityTime = now
	}
	return rollback, true
}

func (m *AgentManager) rollbackPendingReviewVerdictCompletion(
	rollback linkedReviewVerdictCompletionRollback,
) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	coder, ok := m.agents[rollback.coderID]
	if !ok || coder == nil ||
		coder.PendingReviewVerdict != nil ||
		!strings.EqualFold(
			strings.TrimSpace(coder.LastReviewedHeadSHA),
			strings.TrimSpace(rollback.pendingReviewVerdict.ReviewedHeadSHA),
		) ||
		coder.LastReviewVerdict != rollback.pendingReviewVerdict.Verdict ||
		coder.LastReviewCommentID != rollback.pendingReviewVerdict.CommentID {
		return false
	}
	coder.PendingReviewVerdict =
		clonePendingReviewVerdictApplication(rollback.pendingReviewVerdict)
	coder.LastReviewedHeadSHA = rollback.lastReviewedHeadSHA
	coder.ManualReviewHold = rollback.lastManualReviewHold
	coder.LastReviewVerdict = rollback.lastReviewVerdict
	coder.LastReviewCommentID = rollback.lastReviewCommentID
	coder.LastPreReviewGateFailureHeadSHA =
		rollback.lastPreReviewGateFailureHeadSHA
	coder.LastPreReviewGateFailureAt = rollback.lastPreReviewGateFailureAt
	coder.LaunchAttempts = cloneDurableLaunchAttempts(rollback.launchAttempts)
	coder.LastActivityTime = rollback.coderLastActivityTime
	if reviewer := m.agents[rollback.reviewerID]; reviewer != nil &&
		reviewer.State == StateDone && reviewer.Stopped {
		reviewer.State = rollback.reviewerState
		reviewer.Stopped = rollback.reviewerStopped
		reviewer.Paused = rollback.reviewerPaused
		reviewer.LastActivityTime = rollback.reviewerLastActivityTime
	}
	return true
}

func (m *AgentManager) RecordReviewedHead(agentID, reviewedHeadSHA string, commentID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.LastReviewedHeadSHA = strings.TrimSpace(reviewedHeadSHA)
	a.ManualReviewHold = false
	a.LastReviewCommentID = commentID
	a.LastPreReviewGateFailureHeadSHA = ""
	a.LastPreReviewGateFailureAt = time.Time{}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) RecordPreReviewGateFailure(agentID, headSHA string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.LastPreReviewGateFailureHeadSHA = strings.TrimSpace(headSHA)
	a.LastPreReviewGateFailureAt = time.Now()
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) ClearPreReviewGateFailure(agentID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.LastPreReviewGateFailureHeadSHA = ""
	a.LastPreReviewGateFailureAt = time.Time{}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func isPreReviewGateFailureForHead(agent Agent, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(agent.LastPreReviewGateFailureHeadSHA), headSHA) {
		return true
	}
	return agent.LastReviewVerdict == ReviewVerdictNeedsChanges &&
		agent.LastReviewCommentID == 0 &&
		strings.EqualFold(strings.TrimSpace(agent.LastReviewedHeadSHA), headSHA)
}

// RecordReviewIncomplete terminalizes the exact durable launch attempt for a
// coder-linked review. Cycle identity is the attempt identity, so recovery
// cannot charge or schedule the same physical launch twice.
func (m *AgentManager) RecordReviewIncomplete(
	agentID string,
	cycleID string,
	headSHA string,
) (DurableLaunchAttempt, bool) {
	if m == nil {
		return DurableLaunchAttempt{}, false
	}
	cycleID = strings.TrimSpace(cycleID)
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	if cycleID == "" || validateCanonicalGitObjectID(headSHA) != nil {
		return DurableLaunchAttempt{}, false
	}
	now := time.Now().UTC()
	_, attempt, err := m.transitionCoderLaunchAttempt(
		agentID,
		cycleID,
		DurableLaunchFailed,
		&DurableLaunchFailure{
			Kind:      DurableLaunchFailureIncomplete,
			Retryable: true,
		},
		now.Add(reviewSameHeadRetryDelay),
		now,
	)
	if err != nil || !strings.EqualFold(attempt.Scope, headSHA) {
		return DurableLaunchAttempt{}, false
	}
	return attempt, true
}

func reviewRetryForHead(
	agent Agent,
	headSHA string,
) (DurableLaunchAttempt, bool) {
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return DurableLaunchAttempt{}, false
	}
	attempt, found := reviewLaunchAttemptForHead(agent, headSHA)
	return attempt, found && durableLaunchTerminal(attempt.Lifecycle)
}

func reviewAttemptNumberForHead(agent Agent, headSHA string) int {
	return reviewLaunchAttemptCountForHead(agent, headSHA) + 1
}

// isManualReviewHoldForHead reports whether headSHA only looks "already
// reviewed" because holdReviewOnInactiveReviewer recorded a manual hold --
// its owning reviewer went inactive (stopped, paused, or failed to
// restart) before ever reaching a verdict -- not because a review
// actually completed. RecordManualReviewHold deliberately stamps
// LastReviewedHeadSHA with no verdict/comment to pause automatic
// re-launch until a human pushes a new commit or runs `agent review`;
// this lets callers tell that apart from a genuinely completed review so
// they don't report it the same way.
//
// This must check the explicit ManualReviewHold marker, not just the
// LastReviewedHeadSHA-with-no-verdict shape: ContinueAgentForIssuePR sets
// that exact same shape as its normal baseline for every adopted PR
// (ObservedPRHeadSHA and LastReviewedHeadSHA both set to the current head,
// verdict/comment left empty), which is not a hold at all. Without this
// marker, status falsely reported that the reviewer had gone inactive and
// told the operator to run `agent review` for a PR that was never
// reviewed in the first place.
func isManualReviewHoldForHead(agent Agent, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return false
	}
	return agent.ManualReviewHold &&
		strings.EqualFold(strings.TrimSpace(agent.LastReviewedHeadSHA), headSHA) &&
		agent.LastReviewVerdict == "" &&
		agent.LastReviewCommentID == 0
}

// isNonRetryableReviewBlockForHead reports whether headSHA's most recent
// review-coordinator launch attempt ended terminally with automatic
// same-head retry disabled (its RetryAfter was never set, because
// recordReviewIncompleteForCoder was called with retryable=false). In that
// state ensureReviewAgentForCoder's reserveReviewLaunchAttempt check will
// never relaunch a fresh reviewer for this head on its own -- an operator
// has to run `agent review` explicitly. Callers use this to tell that apart
// from "never reviewed" so they don't report it the same way.
//
// A head that already carries a genuine verdict or comment (e.g. a
// published "partial, no findings" result -- finalizePartialNoFindingsReview
// terminalizes the same launch attempt purely as retry-blocking bookkeeping
// *after* a verdict was already posted to the PR) is excluded: nothing is
// pending there, so flagging it would wrongly suggest an operator still
// needs to run `agent review`.
func isNonRetryableReviewBlockForHead(agent Agent, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if headSHA == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(agent.LastReviewedHeadSHA), headSHA) &&
		(agent.LastReviewVerdict != "" || agent.LastReviewCommentID != 0) {
		return false
	}
	retry, ok := reviewRetryForHead(agent, headSHA)
	return ok && retry.RetryAfter.IsZero()
}

func (b *Orchestrator) ensureReviewAgentForCoder(ctx context.Context, coder Agent) error {
	if coder.Role == RoleReviewer {
		return nil
	}
	current, ok := b.agents.Get(coder.ID)
	if !ok || current.Stopped {
		return nil
	}
	if current.Role == RoleReviewer || current.PRNumber == 0 {
		return nil
	}
	if current.PendingReviewVerdict != nil {
		log.Printf(
			"review launch skipped coder=%s pr=%d: review verdict application is pending",
			current.ID,
			current.PRNumber,
		)
		return nil
	}
	if current.State == StateApproved || current.State == StateMerging || current.State == StateDone || current.State == StateErrored || current.State == StateStopped {
		return nil
	}

	activeReviewerID := strings.TrimSpace(current.ActiveReviewAgentID)
	if activeReviewerID != "" {
		reviewer, ok := b.agents.Get(activeReviewerID)
		if ok && b.reviewLaunchStillOwned(reviewer.ID, current.ID) {
			log.Printf("review launch skipped coder=%s pr=%d: active reviewer %s still owns head", current.ID, current.PRNumber, reviewer.ID)
			return nil
		}
		if ok && reviewer.Role == RoleReviewer && !reviewer.Stopped && reviewer.State != StateDone && reviewer.State != StateErrored && reviewer.State != StateStopped {
			// reviewLaunchStillOwned can return false for reasons other
			// than a genuinely new head -- e.g. this exact reviewer's own
			// verdict-completion processing is concurrently in flight and
			// has already cleared one of the fields
			// linkedReviewerOwnsCurrentHead checks (ActiveReviewAgentID,
			// ObservedPRHeadSHA) without yet having transitioned the
			// reviewer's own State to terminal. Retiring and
			// replacing it in that window races with its own legitimate
			// completion and wastes a full duplicate discovery+
			// verification cycle on a head that already has (or just had)
			// an active reviewer. Only treat this as a genuine
			// supersession if the reviewer's own recorded head actually
			// differs from what the coder currently observes; otherwise
			// leave it in place; a poll from the very next tick.
			reviewerHead := strings.TrimSpace(reviewer.ObservedPRHeadSHA)
			currentHead := strings.TrimSpace(current.ObservedPRHeadSHA)
			sameHead := reviewerHead != "" && currentHead != "" &&
				strings.EqualFold(reviewerHead, currentHead)
			if sameHead {
				log.Printf(
					"review launch skipped coder=%s pr=%d: active reviewer %s appears unowned but head %s is unchanged; leaving it in place",
					current.ID,
					current.PRNumber,
					reviewer.ID,
					abbreviateSHA(currentHead),
				)
				return nil
			}
			if err := b.retireReviewer(reviewer, StateErrored, false, "stale reviewer superseded by new head"); err != nil {
				return err
			}
		}
		_ = b.agents.ClearActiveReviewAgentIfMatches(current.ID, activeReviewerID)
		current, _ = b.agents.Get(current.ID)
	}

	headSHA := strings.TrimSpace(current.ObservedPRHeadSHA)
	if headSHA == "" {
		if b.github == nil {
			return nil
		}
		latest, err := b.getPRHeadSHA(ctx, current.PRNumber)
		if err != nil {
			return err
		}
		headSHA = strings.TrimSpace(latest)
		if headSHA == "" {
			return nil
		}
		if !b.agents.SetPRHeadSHA(current.ID, headSHA) {
			return nil
		}
		current, _ = b.agents.Get(current.ID)
	}
	if headSHA == "" {
		return nil
	}

	if strings.EqualFold(strings.TrimSpace(current.LastReviewedHeadSHA), headSHA) && !isPreReviewGateFailureForHead(current, headSHA) {
		if isManualReviewHoldForHead(current, headSHA) {
			log.Printf(
				"review launch skipped coder=%s pr=%d head=%s: on manual hold (its reviewer went inactive before a verdict); run `agent review %d` to resume",
				current.ID,
				current.PRNumber,
				abbreviateSHA(headSHA),
				current.PRNumber,
			)
			return nil
		}
		log.Printf("review launch skipped coder=%s pr=%d head=%s: already reviewed", current.ID, current.PRNumber, abbreviateSHA(headSHA))
		return nil
	}
	if isPreReviewGateFailureForHead(current, headSHA) {
		log.Printf("review launch skipped coder=%s pr=%d head=%s: pre-review gate failed for this head", current.ID, current.PRNumber, abbreviateSHA(headSHA))
		return nil
	}
	if retry, ok := reviewRetryForHead(current, headSHA); ok {
		if retry.RetryAfter.IsZero() {
			log.Printf("review launch skipped coder=%s pr=%d head=%s: completed review result is not automatically retryable", current.ID, current.PRNumber, abbreviateSHA(headSHA))
			return nil
		}
		if retry.Attempt >= reviewSameHeadAttemptLimit {
			log.Printf("review launch skipped coder=%s pr=%d head=%s: automatic same-head attempts exhausted attempts=%d", current.ID, current.PRNumber, abbreviateSHA(headSHA), retry.Attempt)
			return nil
		}
		if time.Now().UTC().Before(retry.RetryAfter) {
			log.Printf("review launch deferred coder=%s pr=%d head=%s: same-head retry attempt=%d retry_after=%s", current.ID, current.PRNumber, abbreviateSHA(headSHA), retry.Attempt+1, retry.RetryAfter.Format(time.RFC3339))
			return nil
		}
		log.Printf("review same-head retry launching coder=%s pr=%d head=%s attempt=%d", current.ID, current.PRNumber, abbreviateSHA(headSHA), retry.Attempt+1)
	}

	b.retireStaleReviewersForPR(current.PRNumber)

	launchKey := reviewLaunchKeyForPR(current.PRNumber)
	if !b.reserveReviewLaunch(launchKey, headSHA) {
		log.Printf("review launch skipped coder=%s pr=%d head=%s: another launch already pending", current.ID, current.PRNumber, abbreviateSHA(headSHA))
		return nil
	}

	go func(coderSnapshot Agent, reviewedHeadSHA string) {
		defer b.releaseReviewLaunch(reviewLaunchKeyForPR(coderSnapshot.PRNumber), reviewedHeadSHA)
		if err := b.startReviewAgent(ctx, coderSnapshot, reviewedHeadSHA); err != nil {
			log.Printf(
				"review agent launch error coder=%s pr=%d head=%s: %s",
				coderSnapshot.ID,
				coderSnapshot.PRNumber,
				abbreviateSHA(reviewedHeadSHA),
				b.safeError(err),
			)
		}
	}(current, headSHA)

	return nil
}

// LaunchReviewAgent manually launches a reviewer for prNumber. Registration
// and worktree prep happen synchronously, so a caller gets an immediate,
// synchronous error for a genuine setup failure (e.g. no such PR, worktree
// prep failed) -- but the hard gate and coordinator startup that follow,
// which can take minutes, run in a background goroutine so this returns
// promptly once the reviewer is registered and ready to enter the gate,
// rather than blocking its caller (the REPL's command dispatcher) for the
// entire gate duration.
func (b *Orchestrator) LaunchReviewAgent(ctx context.Context, prNumber int) error {
	defer b.persistAgentStateNonFatal("launch-review-agent")

	if b == nil || b.agents == nil {
		return errors.New("agent manager is not configured")
	}
	if prNumber <= 0 {
		return fmt.Errorf("invalid PR number: %d", prNumber)
	}
	if reviewer, ok := b.findBlockingReviewerForPR(prNumber); ok {
		return fmt.Errorf("review agent already active for PR #%d: %s", prNumber, reviewer.ID)
	}
	b.retireStaleReviewersForPR(prNumber)

	target, headSHA, err := b.loadReviewLaunchTarget(ctx, prNumber)
	if err != nil {
		return err
	}

	if !b.reserveReviewLaunch(target.LaunchKey, headSHA) {
		return fmt.Errorf("review launch already pending for PR #%d", prNumber)
	}

	reviewCtx := context.Background()
	if ctx != nil {
		reviewCtx = context.WithoutCancel(ctx)
	}

	logLaunchError := func(err error) error {
		log.Printf(
			"manual review agent launch error pr=%d source=%s head=%s: %s",
			target.PRNumber,
			fallback(strings.TrimSpace(target.CoderID), reviewLaunchKeyForPR(target.PRNumber)),
			abbreviateSHA(headSHA),
			b.safeError(err),
		)
		return fmt.Errorf("failed to launch review agent for PR #%d: %w", prNumber, err)
	}

	reviewer, err := b.registerReviewAgent(reviewCtx, target, headSHA)
	if err != nil {
		b.releaseReviewLaunch(target.LaunchKey, headSHA)
		return logLaunchError(err)
	}
	if reviewer == nil {
		// Nothing to launch (e.g. the coder disappeared mid-registration) --
		// not an error, matches registerReviewAgent's own (nil, nil) contract.
		b.releaseReviewLaunch(target.LaunchKey, headSHA)
		return nil
	}
	// registerReviewAgent may have re-derived the target internally (e.g.
	// resolving SkipHardGate); reconstruct it from the now-persisted
	// reviewer record rather than reusing this function's original,
	// potentially-stale copy, matching startReviewAgentForTarget.
	target = reviewLaunchTargetFromReviewer(*reviewer)
	if err := b.prepareReviewGateLaunch(reviewCtx, target, reviewer); err != nil {
		b.releaseReviewLaunch(target.LaunchKey, headSHA)
		if errors.Is(err, errReviewLaunchAbandoned) {
			return nil
		}
		return logLaunchError(err)
	}

	log.Printf(
		"manual review agent registered pr=%d source=%s head=%s reviewer=%s",
		target.PRNumber,
		fallback(strings.TrimSpace(target.CoderID), target.LaunchKey),
		abbreviateSHA(headSHA),
		reviewer.ID,
	)

	// Hand the goroutine its own snapshot, not the *Agent registerReviewAgent
	// returned: that pointer is the same one stored in the AgentManager's
	// map (Add does not clone), so once LaunchReviewAgent returns here the
	// caller is free to interact with this reviewer through the manager's
	// normal synchronized methods (e.g. agent stop) while this goroutine is
	// still reading from it -- a real, race-detector-confirmed data race.
	// runReviewGateAndCoordinatorStartup's own state changes already go
	// through AgentManager's synchronized methods (SetState, Touch, ...),
	// never by mutating this snapshot directly, so working from a frozen
	// copy of registration-time data is safe.
	reviewerSnapshot := cloneAgent(reviewer)
	go func(launchTarget reviewLaunchTarget, launchedReviewer *Agent, launchHeadSHA string) {
		defer b.releaseReviewLaunch(launchTarget.LaunchKey, launchHeadSHA)
		// reviewCtx, not context.Background(): it's already cancellation-free
		// (see above) but still carries whatever values the caller's ctx
		// had attached, matching what the synchronous registration/prepare
		// phases above were already given.
		if err := b.runReviewGateAndCoordinatorStartup(reviewCtx, launchTarget, launchedReviewer); err != nil {
			log.Printf(
				"manual review agent launch error pr=%d source=%s head=%s: %s",
				launchTarget.PRNumber,
				fallback(strings.TrimSpace(launchTarget.CoderID), reviewLaunchKeyForPR(launchTarget.PRNumber)),
				abbreviateSHA(launchHeadSHA),
				b.safeError(err),
			)
		}
	}(target, &reviewerSnapshot, headSHA)

	return nil
}

func reviewLaunchKey(coderID, headSHA string) string {
	return strings.TrimSpace(coderID) + "|" + strings.ToLower(strings.TrimSpace(headSHA))
}

func (b *Orchestrator) reserveReviewLaunch(coderID, headSHA string) bool {
	if b == nil {
		return false
	}
	if strings.TrimSpace(coderID) == "" || strings.TrimSpace(headSHA) == "" {
		return false
	}
	key := reviewLaunchKey(coderID, headSHA)
	b.reviewLaunchMu.Lock()
	defer b.reviewLaunchMu.Unlock()
	if b.pendingReviewLaunches == nil {
		b.pendingReviewLaunches = make(map[string]struct{})
	}
	if _, exists := b.pendingReviewLaunches[key]; exists {
		return false
	}
	b.pendingReviewLaunches[key] = struct{}{}
	return true
}

func (b *Orchestrator) releaseReviewLaunch(coderID, headSHA string) {
	if b == nil {
		return
	}
	if strings.TrimSpace(coderID) == "" || strings.TrimSpace(headSHA) == "" {
		return
	}
	key := reviewLaunchKey(coderID, headSHA)
	b.reviewLaunchMu.Lock()
	defer b.reviewLaunchMu.Unlock()
	if b.pendingReviewLaunches == nil {
		return
	}
	delete(b.pendingReviewLaunches, key)
}

func (b *Orchestrator) startReviewAgent(ctx context.Context, coder Agent, headSHA string) error {
	return b.startReviewAgentForTarget(ctx, reviewLaunchTarget{
		LaunchKey: strings.TrimSpace(coder.ID),
		CoderID:   strings.TrimSpace(coder.ID),
		Automatic: true,
	}, headSHA)
}

func (b *Orchestrator) startReviewAgentForTarget(ctx context.Context, target reviewLaunchTarget, headSHA string) error {
	reviewer, err := b.registerReviewAgent(ctx, target, headSHA)
	if err != nil || reviewer == nil {
		return err
	}
	// registerReviewAgent may have re-derived target from the coder's
	// current state (e.g. resolving SkipHardGate, or refreshing
	// IssueTitle/PRTitle/etc. if the coder's own record changed between
	// this call starting and the registration completing) -- reconstruct
	// it from the now-persisted reviewer record rather than reusing the
	// caller's original, potentially-stale copy.
	return b.launchCheckpointedReviewAgent(ctx, reviewLaunchTargetFromReviewer(*reviewer), reviewer)
}

// registerReviewAgent creates and persists the reviewer agent record (and,
// for a coder-linked review, attaches its prior review ledger) but does not
// itself run the worktree prep, hard gate, or coordinator startup that
// follow -- those are the genuinely slow part of a launch and are handled
// separately (launchCheckpointedReviewAgent) so a caller that needs a
// synchronous answer to "did registration succeed" isn't forced to also
// wait for everything after it. Returns (nil, nil) for the same "nothing to
// launch" conditions the pre-refactor combined function used to signal via
// a bare `return nil`.
func (b *Orchestrator) registerReviewAgent(ctx context.Context, target reviewLaunchTarget, headSHA string) (*Agent, error) {
	defer b.persistAgentStateNonFatal("register-review-agent")

	if b.runner == nil {
		return nil, fmt.Errorf("runner is not configured")
	}
	reviewCycle, err := newReviewCycleState(headSHA, b.cfg.ReviewPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to snapshot effective review policy: %w", err)
	}
	headSHA = reviewCycle.HeadSHA
	runtimeProfile, err := reviewCycle.Policy.effectiveProfileForRole(AgentProfileRoleChallenge)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve snapshotted review runtime profile: %w", err)
	}

	var launchAttempt DurableLaunchAttempt
	if strings.TrimSpace(target.CoderID) != "" {
		coderCurrent, ok := b.agents.Get(target.CoderID)
		if !ok || coderCurrent.Stopped {
			return nil, nil
		}
		automatic := target.Automatic
		target = reviewLaunchTargetFromCoder(coderCurrent)
		target.Automatic = automatic
		reviewCycle.PriorReviewLedger = cloneReviewLedger(coderCurrent.ReviewLedger)
		reviewCycle.CorrectionRound = correctionLaunchAttemptCount(coderCurrent)

		reservedAt := time.Now().UTC()
		reviewerNameScope := target.IssueNumber
		if reviewerNameScope == 0 {
			reviewerNameScope = target.PRNumber
		}
		reviewerID := newAgentID(RoleReviewer, reviewerNameScope, reservedAt)
		b.statePersistenceMu.Lock()
		mutation, reserved, reservedOK := b.agents.reserveReviewLaunchAttempt(
			coderCurrent.ID,
			headSHA,
			reviewCycle.ID,
			reviewerID,
			target.Automatic,
			reservedAt,
		)
		if !reservedOK {
			b.statePersistenceMu.Unlock()
			return nil, nil
		}
		if err := b.persistCoderLaunchAttemptMutation(
			mutation,
			"review coordinator launch reservation",
		); err != nil {
			b.statePersistenceMu.Unlock()
			return nil, err
		}
		b.statePersistenceMu.Unlock()
		launchAttempt = reserved
		reviewCycle.ID = launchAttempt.ID
		reviewCycle.Attempt = launchAttempt.Attempt
		reviewCycle.Metrics, err = newReviewMetricsState(
			reviewCycle,
			reservedAt,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to bind review metrics to launch attempt: %w",
				err,
			)
		}
		if err := validateReviewLedgerCycleContext(reviewCycle); err != nil {
			return nil, fmt.Errorf("failed to attach prior review ledger: %w", err)
		}
	}

	comments, err := b.listAllIssueComments(ctx, target.PRNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to determine review comment baseline: %w", err)
	}
	baselineCommentID := maxIssueCommentID(comments)
	// The baseline lookup is a network call. Cleanup may have stopped the coder
	// while it was in flight, so do not register a reviewer from the stale
	// snapshot. SetActiveReviewAgent repeats this check under the manager lock.
	if strings.TrimSpace(target.CoderID) != "" {
		coderCurrent, ok := b.agents.Get(target.CoderID)
		if !ok || agentLifecycleTerminal(&coderCurrent) {
			return nil, nil
		}
		automatic := target.Automatic
		target = reviewLaunchTargetFromCoder(coderCurrent)
		target.Automatic = automatic
	}

	now := time.Now().UTC()
	reviewerNameScope := target.IssueNumber
	if reviewerNameScope == 0 {
		reviewerNameScope = target.PRNumber
	}
	reviewerID := newAgentID(RoleReviewer, reviewerNameScope, now)
	if launchAttempt.ID != "" {
		reviewerID = launchAttempt.OwnerID
	}
	reviewWorktreePath := filepath.Join(b.cfg.WorktreeDir, reviewerID)
	reviewer := &Agent{
		ID:                           reviewerID,
		Role:                         RoleReviewer,
		ParentAgentID:                target.CoderID,
		IssueNumber:                  target.IssueNumber,
		IssueTitle:                   target.IssueTitle,
		IssueBody:                    target.IssueBody,
		WorktreePath:                 reviewWorktreePath,
		LogDir:                       b.cfg.LogDir,
		RuntimeCWD:                   reviewWorktreePath,
		BranchName:                   target.BranchName,
		PRNumber:                     target.PRNumber,
		PRTitle:                      target.PRTitle,
		PRURL:                        target.PRURL,
		HumanReviewGuidance:          append([]string(nil), target.HumanReviewGuidance...),
		ObservedPRHeadSHA:            headSHA,
		ReviewBaselineIssueCommentID: baselineCommentID,
		RuntimeProfile:               runtimeProfile,
		ReviewCycle:                  reviewCycle,
		ReviewSkipHardGate:           target.SkipHardGate,
		State:                        StateInitializing,
		LastActivityTime:             now,
		seenReviewCommentIDs:         make(map[int64]struct{}),
		seenIssueCommentIDs:          make(map[int64]struct{}),
		pendingReviewCommentIDs:      make(map[int64]struct{}),
	}
	if err := b.agents.Add(reviewer); err != nil {
		return nil, err
	}
	if strings.TrimSpace(target.CoderID) != "" && !b.agents.SetActiveReviewAgent(target.CoderID, reviewerID) {
		b.agents.SetState(reviewerID, StateErrored, true)
		return nil, fmt.Errorf("failed to set active review agent for %s", target.CoderID)
	}
	var persistErr error
	if launchAttempt.ID != "" {
		b.statePersistenceMu.Lock()
		mutation, _, transitionErr := b.agents.transitionCoderLaunchAttempt(
			target.CoderID,
			launchAttempt.ID,
			DurableLaunchRunning,
			nil,
			time.Time{},
			time.Now().UTC(),
		)
		if transitionErr != nil {
			persistErr = transitionErr
		} else {
			persistErr = b.persistCoderLaunchAttemptMutation(
				mutation,
				"review coordinator registration",
			)
		}
		b.statePersistenceMu.Unlock()
	} else {
		persistErr = b.persistAgentState()
	}
	if persistErr != nil {
		if strings.TrimSpace(target.CoderID) != "" {
			_ = b.agents.ClearActiveReviewAgentIfMatches(target.CoderID, reviewerID)
		}
		b.agents.SetState(reviewerID, StateErrored, true)
		return nil, fmt.Errorf("failed to persist review policy snapshot before launch: %w", persistErr)
	}
	return reviewer, nil
}

// launchCheckpointedReviewAgent runs the worktree-prep, hard-gate, and
// coordinator-startup phases of a review launch against an already
// registered reviewer. This is the genuinely slow part of a launch; see
// prepareReviewGateLaunch and runReviewGateAndCoordinatorStartup, which
// this just chains together, for why they're split into two functions
// rather than kept as one.
func (b *Orchestrator) launchCheckpointedReviewAgent(ctx context.Context, target reviewLaunchTarget, reviewer *Agent) error {
	if err := b.prepareReviewGateLaunch(ctx, target, reviewer); err != nil {
		if errors.Is(err, errReviewLaunchAbandoned) {
			return nil
		}
		return err
	}
	return b.runReviewGateAndCoordinatorStartup(ctx, target, reviewer)
}

// errReviewLaunchAbandoned signals that a review launch was correctly and
// silently abandoned (e.g. ownership of the launch slot was lost to a
// concurrent event) rather than that it failed -- callers must not surface
// it as an error, only stop.
var errReviewLaunchAbandoned = errors.New("review launch abandoned")

// prepareReviewGateLaunch validates the reviewer's checkpointed review
// cycle and prepares its worktree. It deliberately stops short of the hard
// gate and coordinator startup (runReviewGateAndCoordinatorStartup) so a
// caller that needs a synchronous answer to "is this reviewer ready to
// proceed" (e.g. LaunchReviewAgent, so it can return a synchronous error
// from the REPL without blocking on the hard gate) doesn't have to wait
// for the slow part too.
func (b *Orchestrator) prepareReviewGateLaunch(ctx context.Context, target reviewLaunchTarget, reviewer *Agent) error {
	// Review feedback found that splitting the
	// launch into synchronous/asynchronous phases narrowed persistence to
	// registerReviewAgent's own defer, which only covers state as of
	// registration -- this function's own state changes (e.g. StateErrored
	// on a worktree-prep failure below) were never flushed to disk by
	// anything in this call chain, so a restart could recover a reviewer
	// as stale StateInitializing even though it had already failed (or,
	// via runReviewGateAndCoordinatorStartup below, progressed through or
	// finished the gate). Every durable transition in both functions now
	// gets its own persistence defer.
	defer b.persistAgentStateNonFatal("prepare-review-gate-launch")

	if reviewer == nil || reviewer.ReviewCycle == nil {
		return errors.New("review policy checkpoint is missing")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validatePersistedReviewCycleSnapshot(reviewer.ReviewCycle); err != nil {
		return fmt.Errorf("review policy checkpoint is invalid: %w", err)
	}
	if reviewer.ReviewCycle.HeadSHA != reviewer.ObservedPRHeadSHA {
		return fmt.Errorf(
			"review policy checkpoint head mismatch: reviewer=%s snapshot=%s",
			abbreviateSHA(reviewer.ObservedPRHeadSHA),
			abbreviateSHA(reviewer.ReviewCycle.HeadSHA),
		)
	}
	reviewerID := reviewer.ID
	if err := b.prepareReviewerWorktree(ctx, *reviewer); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			b.clearReviewGateTracking(reviewerID)
			return ctxErr
		}
		b.clearReviewGateTracking(reviewerID)
		var launchStateErr error
		if strings.TrimSpace(target.CoderID) != "" {
			_, launchStateErr =
				b.transitionAndPersistCoderLaunchAttempt(
					target.CoderID,
					reviewer.ReviewCycle.ID,
					DurableLaunchFailed,
					&DurableLaunchFailure{
						Kind: DurableLaunchFailureSetup,
					},
					time.Now().UTC().Add(
						reviewSameHeadRetryDelay,
					),
				)
		}
		if strings.TrimSpace(target.CoderID) != "" {
			_ = b.agents.ClearActiveReviewAgentIfMatches(target.CoderID, reviewerID)
		}
		b.agents.SetState(reviewerID, StateErrored, true)
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to prepare worktree for %s `%s`", agentRoleLabel(RoleReviewer), reviewerID), err)
		return errors.Join(
			fmt.Errorf(
				"failed to prepare review worktree for %s: %w",
				reviewerID,
				err,
			),
			launchStateErr,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !b.reviewLaunchStillOwned(reviewerID, target.CoderID) {
		b.abandonReviewLaunch(reviewer, target, "ownership lost after worktree prep")
		return errReviewLaunchAbandoned
	}
	return nil
}

// runReviewGateAndCoordinatorStartup runs the mandatory-test hard gate (the
// slow part of a review launch -- this is what previously blocked the REPL
// for the entire gate duration when invoked from LaunchReviewAgent) and,
// once it passes, writes the reviewer's context artifacts and starts its
// convergent review coordinator. Callers that need to stay responsive
// while this runs (LaunchReviewAgent) should invoke it from a goroutine
// after prepareReviewGateLaunch has already returned successfully;
// callers that are already running in their own goroutine
// (ensureReviewAgentForCoder, via launchCheckpointedReviewAgent) can just
// call it inline.
func (b *Orchestrator) runReviewGateAndCoordinatorStartup(ctx context.Context, target reviewLaunchTarget, reviewer *Agent) error {
	// See the comment on prepareReviewGateLaunch's own defer:
	// this function transitions the reviewer through StateReviewGate,
	// StateWorking, and terminal states, none of which were persisted by
	// anything else in this call chain. Deferred so it still runs on every
	// exit path, including an early return or a panic unwinding through
	// here -- not just the success path.
	defer b.persistAgentStateNonFatal("run-review-gate-and-coordinator-startup")

	if ctx == nil {
		ctx = context.Background()
	}
	reviewerID := reviewer.ID
	headSHA := reviewer.ReviewCycle.HeadSHA
	if !target.SkipHardGate {
		if !b.agents.SetState(reviewerID, StateReviewGate, false) {
			return nil
		}
		b.setReviewGateStatus(reviewerID, reviewGateStatus{})
		gateLogPath, gateLogPathErr := mandatoryTestLogPath(*reviewer)
		gateLogLine := ""
		if gateLogPathErr == nil {
			gateLogLine = fmt.Sprintf("\nGate log: `%s`", gateLogPath)
		}
		err := b.runMandatoryTestGateWithStart(ctx, *reviewer, func() {
			b.setReviewGateStatus(reviewerID, reviewGateStatus{started: true, startedAt: time.Now()})
			_ = b.agents.Touch(reviewerID)
			b.notify(
				ctx,
				fmt.Sprintf(
					"Repository Agent Orchestrator: review hard gate started for review agent `%s` on PR %s.\n\nHead SHA: `%s`\nMandatory tests:\n%s%s",
					reviewerID,
					fallback(strings.TrimSpace(target.PRURL), fmt.Sprintf("#%d", target.PRNumber)),
					fallback(strings.TrimSpace(headSHA), "unknown"),
					formatMandatoryTestList(b.cfg.MandatoryTests),
					gateLogLine,
				),
			)
			log.Printf(
				"review hard gate started reviewer=%s coder=%s pr=%d head=%s",
				reviewerID,
				fallback(strings.TrimSpace(target.CoderID), target.LaunchKey),
				target.PRNumber,
				abbreviateSHA(headSHA),
			)
		})
		if ctxErr := ctx.Err(); ctxErr != nil {
			b.clearReviewGateTracking(reviewerID)
			return ctxErr
		}
		if !b.reviewLaunchStillOwned(reviewerID, target.CoderID) {
			b.abandonReviewLaunch(reviewer, target, "ownership lost after hard gate")
			return nil
		}
		if err != nil {
			if isMandatoryTestGateInterrupted(err) {
				b.clearReviewGateTracking(reviewerID)
				if strings.TrimSpace(target.CoderID) != "" {
					_ = b.agents.ClearActiveReviewAgentIfMatches(target.CoderID, reviewerID)
				}
				b.agents.SetState(reviewerID, StateDone, true)
				b.cleanupDetachedReviewWorktree(reviewerID, reviewer.WorktreePath, "interrupted hard gate")
				log.Printf(
					"review hard gate interrupted reviewer=%s coder=%s pr=%d head=%s: %s",
					reviewerID,
					fallback(strings.TrimSpace(target.CoderID), target.LaunchKey),
					target.PRNumber,
					abbreviateSHA(headSHA),
					b.safeError(err),
				)
				return nil
			}
			b.clearReviewGateTracking(reviewerID)
			if strings.TrimSpace(target.CoderID) != "" {
				_ = b.agents.ClearActiveReviewAgentIfMatches(target.CoderID, reviewerID)
				_ = b.agents.RecordPreReviewGateFailure(target.CoderID, headSHA)
				if _, ok := b.agents.Get(target.CoderID); ok {
					b.agents.SetState(target.CoderID, StateWorking, false)
				}
			}
			b.agents.SetState(reviewerID, StateDone, true)
			b.cleanupDetachedReviewWorktree(reviewerID, reviewer.WorktreePath, "pre-review hard gate failure")
			if strings.TrimSpace(target.CoderID) != "" {
				coderCurrent, ok := b.agents.Get(target.CoderID)
				if ok && b.runner != nil {
					msg := formatPreReviewHardGateFailed(coderCurrent, b.cfg.MandatoryTests, err)
					if _, deliveredToRuntime, sendErr := b.sendAutomatedRuntimeMessage(ctx, coderCurrent, "pre-review hard gate failed", msg, msg); sendErr != nil {
						log.Printf("non-fatal: failed to send pre-review hard gate failure to coder=%s: %s", coderCurrent.ID, b.safeError(sendErr))
					} else if deliveredToRuntime {
						b.clearInputWaitAlert(coderCurrent.ID)
						b.agents.Touch(coderCurrent.ID)
					}
				}
				if ok {
					b.notify(ctx, formatPreReviewHardGateNotification(coderCurrent, headSHA, err))
					log.Printf("pre-review hard gate failed coder=%s pr=%d head=%s: %s", coderCurrent.ID, coderCurrent.PRNumber, abbreviateSHA(headSHA), b.safeError(err))
				} else {
					log.Printf("pre-review hard gate failed pr=%d head=%s: %s", target.PRNumber, abbreviateSHA(headSHA), b.safeError(err))
				}
			} else {
				b.notify(
					ctx,
					fmt.Sprintf(
						"Repository Agent Orchestrator: mandatory test gate failed before manual review for PR %s; review agent `%s` stopped.\n\nHead SHA: `%s`\nFailure details:\n%s",
						fallback(strings.TrimSpace(target.PRURL), fmt.Sprintf("#%d", target.PRNumber)),
						reviewerID,
						fallback(strings.TrimSpace(headSHA), "unknown"),
						prefixLines(strings.TrimSpace(err.Error()), "> "),
					),
				)
				log.Printf("pre-review hard gate failed pr=%d head=%s: %s", target.PRNumber, abbreviateSHA(headSHA), b.safeError(err))
			}
			return nil
		}
		b.clearReviewGateTracking(reviewerID)
		if strings.TrimSpace(target.CoderID) != "" {
			_ = b.agents.ClearPreReviewGateFailure(target.CoderID)
		}
		log.Printf("review hard gate passed reviewer=%s coder=%s pr=%d head=%s", reviewerID, fallback(strings.TrimSpace(target.CoderID), target.LaunchKey), target.PRNumber, abbreviateSHA(headSHA))
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if strings.TrimSpace(target.CoderID) != "" {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: %s `%s` initialized for coder `%s` on PR %s", agentRoleLabel(RoleReviewer), reviewerID, target.CoderID, target.PRURL))
	} else {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: %s `%s` initialized for manual PR review on %s", agentRoleLabel(RoleReviewer), reviewerID, fallback(strings.TrimSpace(target.PRURL), fmt.Sprintf("#%d", target.PRNumber))))
	}

	if err := b.writeAgentContextArtifacts(*reviewer, agentContextRequest{
		Role:        RoleReviewer,
		IssueNumber: target.IssueNumber,
		IssueTitle:  target.IssueTitle,
		IssueBody:   target.IssueBody,
		PRNumber:    target.PRNumber,
	}); err != nil {
		b.clearReviewGateTracking(reviewerID)
		if strings.TrimSpace(target.CoderID) != "" {
			_ = b.agents.ClearActiveReviewAgentIfMatches(target.CoderID, reviewerID)
		}
		b.agents.SetState(reviewerID, StateErrored, true)
		b.cleanupDetachedReviewWorktree(reviewerID, reviewer.WorktreePath, "context write error")
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to write context artifacts for %s `%s`", agentRoleLabel(RoleReviewer), reviewerID), err)
		return fmt.Errorf("failed to write context artifacts for review agent %s: %w", reviewerID, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !b.reviewLaunchStillOwned(reviewerID, target.CoderID) {
		b.abandonReviewLaunch(reviewer, target, "ownership lost after context write")
		return nil
	}

	b.clearReviewGateTracking(reviewerID)
	if !b.agents.SetState(reviewerID, StateWorking, false) {
		return nil
	}
	if strings.TrimSpace(target.CoderID) != "" {
		_ = b.setCoderLifecycleState(target.CoderID, StateWaiting)
	}
	b.notify(
		ctx,
		fmt.Sprintf(
			"Repository Agent Orchestrator: convergent review coordinator `%s` started for exact head `%s` (PR %s)",
			reviewerID,
			headSHA,
			fallback(
				strings.TrimSpace(target.PRURL),
				fmt.Sprintf("#%d", target.PRNumber),
			),
		),
	)
	log.Printf(
		"convergent review coordinator started reviewer=%s source=%s pr=%d head=%s policy=%s",
		reviewerID,
		fallback(strings.TrimSpace(target.CoderID), target.LaunchKey),
		target.PRNumber,
		abbreviateSHA(headSHA),
		reviewer.ReviewCycle.PolicyFingerprint,
	)
	b.queuePersistedReviewCycleRecovery(ctx, reviewerID)
	return nil
}

func reviewLaunchTargetFromReviewer(reviewer Agent) reviewLaunchTarget {
	return reviewLaunchTarget{
		LaunchKey:           reviewLaunchKeyForPR(reviewer.PRNumber),
		SkipHardGate:        reviewer.ReviewSkipHardGate,
		CoderID:             strings.TrimSpace(reviewer.ParentAgentID),
		IssueNumber:         reviewer.IssueNumber,
		IssueTitle:          reviewer.IssueTitle,
		IssueBody:           reviewer.IssueBody,
		BranchName:          reviewer.BranchName,
		PRNumber:            reviewer.PRNumber,
		PRTitle:             reviewer.PRTitle,
		PRURL:               reviewer.PRURL,
		HumanReviewGuidance: append([]string(nil), reviewer.HumanReviewGuidance...),
	}
}

func (b *Orchestrator) recoverCheckpointedReviewLaunch(ctx context.Context, reviewer Agent) error {
	defer b.persistAgentStateNonFatal("recover-checkpointed-review-launch")

	if b.runner == nil {
		return errors.New("runner is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	current, ok := b.agents.Get(reviewer.ID)
	if !ok {
		return nil
	}
	if !b.reviewLaunchStillOwned(current.ID, current.ParentAgentID) {
		return b.retireReviewer(current, StateErrored, false, "checkpointed review launch lost ownership")
	}
	if current.ReviewCycle == nil {
		return fmt.Errorf("review agent %s is missing its review policy checkpoint", current.ID)
	}
	target := reviewLaunchTargetFromReviewer(current)
	if err := ctx.Err(); err != nil {
		return err
	}

	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := b.cleanupWorktree(cleanupCtx, current.WorktreePath, ""); err != nil {
		cancel()
		return fmt.Errorf("failed to reset checkpointed review worktree for %s: %w", current.ID, err)
	}
	cancel()
	if !b.agents.SetState(current.ID, StateInitializing, false) {
		return nil
	}
	current, ok = b.agents.Get(current.ID)
	if !ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	log.Printf(
		"retrying checkpointed review launch reviewer=%s coder=%s pr=%d head=%s policy=%s",
		current.ID,
		fallback(target.CoderID, target.LaunchKey),
		current.PRNumber,
		abbreviateSHA(current.ObservedPRHeadSHA),
		current.ReviewCycle.PolicyFingerprint,
	)
	return b.launchCheckpointedReviewAgent(ctx, target, &current)
}

func (b *Orchestrator) beginCheckpointedReviewLaunchRecovery() bool {
	if b == nil {
		return false
	}
	b.reviewRecoveryMu.Lock()
	defer b.reviewRecoveryMu.Unlock()
	if b.reviewRecoveryClosing {
		return false
	}
	b.reviewRecoveryWG.Add(1)
	return true
}

func (b *Orchestrator) finishCheckpointedReviewLaunchRecovery() {
	if b == nil {
		return
	}
	b.reviewRecoveryWG.Done()
}

func (b *Orchestrator) waitForCheckpointedReviewLaunchRecoveries() {
	if b == nil {
		return
	}
	b.reviewRecoveryMu.Lock()
	b.reviewRecoveryClosing = true
	b.reviewRecoveryMu.Unlock()
	b.reviewRecoveryWG.Wait()
}

func (b *Orchestrator) queueCheckpointedReviewLaunchRecovery(ctx context.Context, reviewer Agent) bool {
	if reviewer.ReviewCycle == nil {
		return false
	}
	target := reviewLaunchTargetFromReviewer(reviewer)
	headSHA := reviewer.ReviewCycle.HeadSHA
	if !b.reserveReviewLaunch(target.LaunchKey, headSHA) {
		return true
	}
	if !b.beginCheckpointedReviewLaunchRecovery() {
		b.releaseReviewLaunch(target.LaunchKey, headSHA)
		return true
	}
	recoveryCtx := ctx
	if recoveryCtx == nil {
		recoveryCtx = context.Background()
	}
	go func() {
		defer b.finishCheckpointedReviewLaunchRecovery()
		defer b.releaseReviewLaunch(target.LaunchKey, headSHA)
		if err := b.recoverCheckpointedReviewLaunch(recoveryCtx, reviewer); err != nil {
			if recoveryCtx.Err() != nil {
				return
			}
			log.Printf(
				"checkpointed review launch recovery error reviewer=%s pr=%d head=%s: %s",
				reviewer.ID,
				reviewer.PRNumber,
				abbreviateSHA(headSHA),
				b.safeError(err),
			)
		}
	}()
	return true
}

func (b *Orchestrator) cleanupDetachedReviewWorktree(reviewerID, _ string, reason string) {
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok {
		return
	}
	finalState := reviewer.State
	if !terminalAgentState(finalState) {
		finalState = StateStopped
	}
	cleanupErr := b.retireReviewer(reviewer, finalState, false, reason)
	if cleanupErr != nil {
		log.Printf(
			"non-fatal: failed to cleanup review worktree after %s reviewer=%s: %s",
			reason,
			reviewerID,
			b.safeError(cleanupErr),
		)
	}
}

func (b *Orchestrator) abandonReviewLaunch(reviewer *Agent, target reviewLaunchTarget, reason string) {
	if reviewer == nil {
		return
	}
	log.Printf(
		"review launch abandoned reviewer=%s coder=%s pr=%d head=%s reason=%s",
		reviewer.ID,
		fallback(strings.TrimSpace(target.CoderID), target.LaunchKey),
		target.PRNumber,
		abbreviateSHA(reviewer.ObservedPRHeadSHA),
		reason,
	)
	b.clearReviewGateTracking(reviewer.ID)
	if strings.TrimSpace(target.CoderID) != "" {
		_ = b.agents.ClearActiveReviewAgentIfMatches(target.CoderID, reviewer.ID)
	}
	b.agents.SetState(reviewer.ID, StateDone, true)
	if strings.TrimSpace(reviewer.WorktreePath) != "" {
		b.cleanupDetachedReviewWorktree(reviewer.ID, reviewer.WorktreePath, reason)
	}
}

func (b *Orchestrator) pollReviewAgent(ctx context.Context, reviewer Agent) error {
	if reviewer.Role != RoleReviewer || reviewer.PRNumber == 0 {
		return nil
	}

	comments, err := b.listAllIssueComments(ctx, reviewer.PRNumber)
	if err != nil {
		return err
	}
	for _, c := range comments {
		if c.GetID() <= reviewer.ReviewBaselineIssueCommentID {
			continue
		}
		verdict, ok := isReviewerVerdictComment(c.GetBody(), reviewer.ID, reviewer.ObservedPRHeadSHA)
		if !ok {
			continue
		}
		if b.asyncReviewVerdictHandling {
			verdictComment := c
			go func() {
				if err := b.handleReviewVerdict(ctx, reviewer, verdict, verdictComment); err != nil {
					log.Printf(
						"review verdict handling error reviewer=%s coder=%s pr=%d: %s",
						reviewer.ID,
						reviewer.ParentAgentID,
						reviewer.PRNumber,
						b.safeError(err),
					)
				}
			}()
			return nil
		}
		return b.handleReviewVerdict(ctx, reviewer, verdict, c)
	}
	return nil
}

func (b *Orchestrator) handleReviewVerdict(ctx context.Context, reviewer Agent, verdict ReviewVerdict, verdictComment *github.IssueComment) error {
	defer b.persistAgentStateNonFatal("handle-review-verdict")

	switch verdict {
	case ReviewVerdictNeedsChanges, ReviewVerdictThumbsUp:
	default:
		return fmt.Errorf("unsupported review verdict %q", verdict)
	}
	if reviewer.ReviewCycle != nil {
		if _, err := b.ensureReviewCycleHeadCurrent(
			ctx,
			reviewer.ID,
		); err != nil {
			if errors.Is(err, errReviewCycleStale) &&
				b.reviewCycleMarkedStale(reviewer.ID) {
				log.Printf(
					"stale review verdict ignored reviewer=%s pr=%d head=%s verdict=%s",
					reviewer.ID,
					reviewer.PRNumber,
					abbreviateSHA(reviewer.ObservedPRHeadSHA),
					verdict,
				)
				return nil
			}
			return fmt.Errorf(
				"failed to recheck live PR head before review verdict: %w",
				err,
			)
		}
	}

	if current, found := b.agents.Get(reviewer.ID); !found ||
		agentLifecycleTerminal(&current) {
		return nil
	}
	unlockLifecycle := b.agents.lockReviewCoordinatorTerminalization(
		reviewer.ID,
	)
	currentReviewer, ok := b.agents.Get(reviewer.ID)
	if !ok || agentLifecycleTerminal(&currentReviewer) {
		unlockLifecycle()
		return nil
	}

	coderID := strings.TrimSpace(currentReviewer.ParentAgentID)
	reviewedHeadSHA := strings.TrimSpace(currentReviewer.ObservedPRHeadSHA)
	var coder Agent
	var hasActiveCoder bool
	if coderID != "" {
		coder, ok = b.agents.Get(coderID)
		hasActiveCoder = ok && !coder.Stopped
		if hasActiveCoder &&
			!linkedReviewerOwnsCurrentHead(coder, currentReviewer) {
			unlockLifecycle()
			if err := b.retireReviewer(
				currentReviewer,
				StateErrored,
				false,
				"stale review verdict ignored",
			); err != nil {
				return err
			}
			log.Printf(
				"stale review verdict ignored reviewer=%s coder=%s reviewer_head=%s coder_head=%s verdict=%s",
				currentReviewer.ID,
				coderID,
				abbreviateSHA(reviewedHeadSHA),
				abbreviateSHA(coder.ObservedPRHeadSHA),
				verdict,
			)
			return nil
		}
	}
	lifecycleLocked := true
	defer func() {
		if lifecycleLocked {
			unlockLifecycle()
		}
	}()

	persistUnappliedVerdict := func(cause error) error {
		if coderID != "" {
			_ = b.agents.ClearActiveReviewAgentIfMatches(
				coderID,
				currentReviewer.ID,
			)
		}
		errs := make([]string, 0, 2)
		errs = appendCleanupError(errs, cause)
		errs = appendCleanupError(
			errs,
			b.persistReviewCoordinatorTerminalState(
				currentReviewer.ID,
				StateStopped,
			),
		)
		return fmt.Errorf(
			"failed to apply review verdict for coordinator %s: %s",
			currentReviewer.ID,
			strings.Join(errs, "; "),
		)
	}

	if !hasActiveCoder {
		lifecycleLocked = false
		unlockLifecycle()
		result, lifecycleErr := b.transitionReviewCoordinatorLifecycle(
			ctx,
			currentReviewer.ID,
			reviewCoordinatorLifecycleRequest{
				Intent:                     ReviewCoordinatorLifecycleCleanup,
				FinalState:                 StateStopped,
				ReleaseCoordinatorWorktree: true,
				CaptureHandoff:             true,
			},
		)
		if strings.TrimSpace(result.Agent.ID) != "" {
			currentReviewer = result.Agent
		}
		if lifecycleErr != nil {
			return persistUnappliedVerdict(
				fmt.Errorf(
					"failed to terminalize review coordinator %s: %w",
					currentReviewer.ID,
					lifecycleErr,
				),
			)
		}
	}

	if coderID == "" {
		switch verdict {
		case ReviewVerdictThumbsUp:
			if err := b.ensurePRReadyForReview(ctx, currentReviewer.PRNumber); err != nil {
				b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: manual review agent `%s` reported THUMBS_UP, but failed to mark PR ready for review on %s: %s", currentReviewer.ID, currentReviewer.PRURL, err))
				return persistUnappliedVerdict(
					fmt.Errorf(
						"failed to mark PR #%d ready for review after THUMBS_UP: %w",
						currentReviewer.PRNumber,
						err,
					),
				)
			}
		}
		if err := b.persistReviewCoordinatorTerminalState(
			currentReviewer.ID,
			StateDone,
		); err != nil {
			return err
		}
		log.Printf(
			"review verdict received reviewer=%s coder=%s pr=%d verdict=%s comment_id=%d",
			currentReviewer.ID,
			currentReviewer.ParentAgentID,
			currentReviewer.PRNumber,
			verdict,
			verdictComment.GetID(),
		)
		switch verdict {
		case ReviewVerdictThumbsUp:
			author := b.getAuthenticatedUserLogin(ctx)
			b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR ready for human review from manual review agent `%s`: %s %s", currentReviewer.ID, currentReviewer.PRURL, author))
			log.Printf("review verdict thumbs-up reviewer=%s pr=%d", currentReviewer.ID, currentReviewer.PRNumber)
		case ReviewVerdictNeedsChanges:
			b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: manual review agent `%s` reported NEEDS_CHANGES on PR %s", currentReviewer.ID, currentReviewer.PRURL))
			log.Printf("review verdict needs-changes reviewer=%s pr=%d", currentReviewer.ID, currentReviewer.PRNumber)
		}
		return nil
	}

	if !hasActiveCoder {
		if err := b.persistReviewCoordinatorTerminalState(
			currentReviewer.ID,
			StateDone,
		); err != nil {
			return err
		}
		return nil
	}

	pending := PendingReviewVerdictApplication{
		ReviewerID:      currentReviewer.ID,
		ReviewedHeadSHA: reviewedHeadSHA,
		Verdict:         verdict,
		CommentID:       verdictComment.GetID(),
	}
	if verdict == ReviewVerdictNeedsChanges {
		pending.ReviewInstruction = formatReviewVerdictNeedsChanges(
			coder,
			currentReviewer,
			verdictComment,
		)
	}
	intentRollback, begun := b.agents.beginLinkedReviewVerdictApplication(
		coderID,
		currentReviewer.ID,
		pending,
	)
	if !begun {
		return persistUnappliedVerdict(
			fmt.Errorf(
				"coder %s no longer links reviewer %s to head %s",
				coderID,
				currentReviewer.ID,
				abbreviateSHA(reviewedHeadSHA),
			),
		)
	}
	if err := b.persistAgentState(); err != nil {
		if !b.agents.rollbackLinkedReviewVerdictIntent(intentRollback) {
			return fmt.Errorf(
				"failed to persist pending review verdict for coordinator %s: %w; failed to roll back the in-memory intent",
				currentReviewer.ID,
				err,
			)
		}
		return fmt.Errorf(
			"failed to persist pending review verdict before applying it: %w",
			err,
		)
	}
	lifecycleLocked = false
	unlockLifecycle()
	return b.resumePendingReviewVerdict(ctx, coderID)
}

func (b *Orchestrator) resumePendingReviewVerdict(
	ctx context.Context,
	coderID string,
) error {
	if b == nil || b.agents == nil {
		return errors.New("agent manager is not configured")
	}
	b.reviewVerdictApplicationMu.Lock()
	defer b.reviewVerdictApplicationMu.Unlock()

	coder, ok := b.agents.Get(strings.TrimSpace(coderID))
	if !ok || coder.Role != RoleCoder {
		return fmt.Errorf(
			"coder %q for pending review verdict was not found",
			strings.TrimSpace(coderID),
		)
	}
	if coder.Stopped {
		return nil
	}
	pending := clonePendingReviewVerdictApplication(
		coder.PendingReviewVerdict,
	)
	if pending == nil {
		return nil
	}
	if err := validatePendingReviewVerdictApplication(pending); err != nil {
		return err
	}
	if head := strings.TrimSpace(coder.ObservedPRHeadSHA); head != "" &&
		!strings.EqualFold(head, pending.ReviewedHeadSHA) {
		return fmt.Errorf(
			"pending review verdict head %s no longer matches coder %s head %s",
			abbreviateSHA(pending.ReviewedHeadSHA),
			coder.ID,
			abbreviateSHA(head),
		)
	}

	persistRetryableFailure := func(cause error) error {
		if err := b.persistAgentState(); err != nil {
			return fmt.Errorf(
				"%w; failed to persist retryable pending verdict state: %v",
				cause,
				err,
			)
		}
		return cause
	}

	reviewer, ok := b.agents.Get(pending.ReviewerID)
	if !ok || reviewer.Role != RoleReviewer {
		return persistRetryableFailure(
			fmt.Errorf(
				"review coordinator %s for pending verdict was not found",
				pending.ReviewerID,
			),
		)
	}
	humanCorrectionEscalation := reviewCycleRequiresHumanCorrectionEscalation(
		reviewer.ReviewCycle,
		pending.Verdict,
	)
	correctionLimit := 0
	if reviewer.ReviewCycle != nil {
		correctionLimit = reviewer.ReviewCycle.Policy.
			Escalation.AfterCorrectionRounds
	}
	if pending.Verdict == ReviewVerdictNeedsChanges &&
		strings.TrimSpace(pending.CorrectionAttemptID) == "" &&
		correctionLimit > 0 &&
		correctionLaunchAttemptCount(coder) >= correctionLimit {
		humanCorrectionEscalation = true
	}

	result, lifecycleErr := b.transitionReviewCoordinatorLifecycle(
		ctx,
		reviewer.ID,
		reviewCoordinatorLifecycleRequest{
			Intent:                     ReviewCoordinatorLifecycleCleanup,
			FinalState:                 StateStopped,
			ReleaseCoordinatorWorktree: true,
			CaptureHandoff:             true,
		},
	)
	if strings.TrimSpace(result.Agent.ID) != "" {
		reviewer = result.Agent
	}
	if lifecycleErr != nil {
		return persistRetryableFailure(
			fmt.Errorf(
				"failed to terminalize review coordinator %s: %w",
				reviewer.ID,
				lifecycleErr,
			),
		)
	}

	switch pending.Verdict {
	case ReviewVerdictNeedsChanges:
		if humanCorrectionEscalation {
			if err := b.stopCoderRuntimeForPendingVerdict(coder); err != nil {
				_ = b.setCoderLifecycleState(coder.ID, StateWaiting)
				return persistRetryableFailure(fmt.Errorf(
					"failed to stop coder runtime %s for correction-round escalation: %w",
					coder.ID,
					err,
				))
			}
			if err := b.ensurePRDraft(ctx, coder.PRNumber); err != nil {
				_ = b.setCoderLifecycleState(coder.ID, StateWaiting)
				return persistRetryableFailure(fmt.Errorf(
					"failed to keep PR #%d draft for human correction: %w",
					coder.PRNumber,
					err,
				))
			}
			_ = b.setCoderLifecycleState(coder.ID, StateWaiting)
			break
		}
		if !b.agents.SetState(coder.ID, StateWorking, false) {
			return persistRetryableFailure(
				fmt.Errorf(
					"coder %s became terminal before NEEDS_CHANGES could be applied",
					coder.ID,
				),
			)
		}
		coder, _ = b.agents.Get(coder.ID)
		if coder.Paused {
			messenger, ok := b.messenger.(idempotentAgentMessenger)
			if !ok {
				return persistRetryableFailure(
					fmt.Errorf(
						"cannot deliver NEEDS_CHANGES notice to paused coder %s idempotently",
						coder.ID,
					),
				)
			}
			if !isGitWorktreePath(coder.WorktreePath) {
				return persistRetryableFailure(
					fmt.Errorf(
						"cannot deliver NEEDS_CHANGES notice to paused coder %s: worktree is unavailable",
						coder.ID,
					),
				)
			}
			key := fmt.Sprintf(
				"review-verdict-%d-%s",
				pending.CommentID,
				abbreviateSHA(pending.ReviewedHeadSHA),
			)
			if err := messenger.SendMessageOnce(
				coder,
				key,
				pending.ReviewInstruction,
			); err != nil {
				return persistRetryableFailure(
					fmt.Errorf(
						"failed to persist NEEDS_CHANGES notice for paused coder %s: %w",
						coder.ID,
						err,
					),
				)
			}
		} else {
			if b.runner == nil {
				return persistRetryableFailure(
					fmt.Errorf(
						"cannot restart coder %s for NEEDS_CHANGES: runtime is not configured",
						coder.ID,
					),
				)
			}
			var correctionAttempt DurableLaunchAttempt
			if attemptID := strings.TrimSpace(
				pending.CorrectionAttemptID,
			); attemptID != "" {
				var found bool
				correctionAttempt, found = durableLaunchAttemptByID(
					coder.LaunchAttempts,
					attemptID,
				)
				if !found || correctionAttempt.Kind !=
					DurableLaunchCorrectionRuntime {
					return persistRetryableFailure(fmt.Errorf(
						"pending verdict references missing correction attempt %s",
						attemptID,
					))
				}
				if correctionAttempt.Lifecycle == DurableLaunchRunning {
					handle := correctionRuntimeReservationHandle(coder)
					alive, err := b.runner.IsAlive(handle)
					if err != nil {
						return persistRetryableFailure(fmt.Errorf(
							"failed to inspect correction runtime for coder %s: %w",
							coder.ID,
							err,
						))
					}
					if alive {
						if !b.agents.SetRuntimeHandle(coder.ID, handle) {
							return persistRetryableFailure(fmt.Errorf(
								"failed to recover correction runtime for coder %s",
								coder.ID,
							))
						}
						coder, _ = b.agents.Get(coder.ID)
						b.clearInputWaitAlert(coder.ID)
						b.agents.Touch(coder.ID)
						log.Printf(
							"coder correction runtime recovered coder=%s pr=%d review_comment_id=%d round=%d session=%s",
							coder.ID,
							coder.PRNumber,
							pending.CommentID,
							correctionAttempt.Attempt,
							coder.RuntimeHandle.Session,
						)
						break
					}
					if _, err := b.transitionAndPersistCoderLaunchAttempt(
						coder.ID,
						correctionAttempt.ID,
						DurableLaunchFailed,
						&DurableLaunchFailure{
							Kind: DurableLaunchFailureRuntimeMissing,
						},
						time.Time{},
					); err != nil {
						return err
					}
					coder, _ = b.agents.Get(coder.ID)
					pending = clonePendingReviewVerdictApplication(
						coder.PendingReviewVerdict,
					)
					correctionAttempt = DurableLaunchAttempt{}
				}
			}

			if correctionAttempt.ID == "" {
				if correctionLimit <= 0 ||
					correctionLaunchAttemptCount(coder) >= correctionLimit {
					return persistRetryableFailure(fmt.Errorf(
						"automated correction launch limit reached for coder %s",
						coder.ID,
					))
				}
				if err := b.stopCoderRuntimeForPendingVerdict(coder); err != nil {
					_ = b.setCoderLifecycleState(coder.ID, StateWaiting)
					return persistRetryableFailure(fmt.Errorf(
						"failed to stop coder runtime %s before review correction restart: %w",
						coder.ID,
						err,
					))
				}
				token, err := randomReviewWorkerToken()
				if err != nil {
					return persistRetryableFailure(fmt.Errorf(
						"failed to allocate correction attempt identity: %w",
						err,
					))
				}
				attemptID := fmt.Sprintf(
					"correction-%d-%s",
					pending.CommentID,
					token,
				)
				b.statePersistenceMu.Lock()
				mutation, reserved, reservedOK :=
					b.agents.reserveCorrectionLaunchAttempt(
						coder.ID,
						attemptID,
						fmt.Sprintf("review-comment-%d", pending.CommentID),
						tmuxSessionName(coder),
						correctionLimit,
						time.Now().UTC(),
					)
				if !reservedOK {
					b.statePersistenceMu.Unlock()
					return persistRetryableFailure(fmt.Errorf(
						"failed to reserve a correction launch attempt for coder %s",
						coder.ID,
					))
				}
				if err := b.persistCoderLaunchAttemptMutation(
					mutation,
					"correction launch reservation",
				); err != nil {
					b.statePersistenceMu.Unlock()
					return err
				}
				b.statePersistenceMu.Unlock()
				correctionAttempt = reserved
				coder, _ = b.agents.Get(coder.ID)
				pending = clonePendingReviewVerdictApplication(
					coder.PendingReviewVerdict,
				)
			}

			if correctionAttempt.Lifecycle == DurableLaunchReserved {
				started, err := b.transitionAndPersistCoderLaunchAttempt(
					coder.ID,
					correctionAttempt.ID,
					DurableLaunchRunning,
					nil,
					time.Time{},
				)
				if err != nil {
					return err
				}
				correctionAttempt = started
				coder, _ = b.agents.Get(coder.ID)
			}
			updatedCoder, err := b.restartRuntimeWithPrompt(
				ctx,
				coder,
				formatReviewCorrectionPrompt(
					coder,
					pending.ReviewInstruction,
				),
			)
			if err != nil {
				_ = b.setCoderLifecycleState(coder.ID, StateWaiting)
				b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to restart coder `%s` after NEEDS_CHANGES on PR %s; the pending verdict remains recoverable and the worktree was preserved", coder.ID, fallback(strings.TrimSpace(coder.PRURL), "(unknown)")))
				launchErr := fmt.Errorf(
					"failed to restart coder %s for review corrections: %w",
					coder.ID,
					err,
				)
				_, lifecycleErr :=
					b.transitionAndPersistCoderLaunchAttempt(
						coder.ID,
						correctionAttempt.ID,
						DurableLaunchFailed,
						&DurableLaunchFailure{
							Kind: DurableLaunchFailureStart,
						},
						time.Time{},
					)
				return errors.Join(launchErr, lifecycleErr)
			}
			coder = updatedCoder
			b.clearInputWaitAlert(coder.ID)
			b.agents.Touch(coder.ID)
			log.Printf("coder runtime replaced for review corrections coder=%s pr=%d review_comment_id=%d round=%d session=%s", coder.ID, coder.PRNumber, pending.CommentID, correctionAttempt.Attempt, coder.RuntimeHandle.Session)
		}
	case ReviewVerdictThumbsUp:
		if b.github == nil {
			return persistRetryableFailure(
				fmt.Errorf(
					"cannot mark PR #%d ready for review: github client is not configured",
					coder.PRNumber,
				),
			)
		}
		if err := b.stopCoderRuntimeForPendingVerdict(coder); err != nil {
			_ = b.setCoderLifecycleState(coder.ID, StateWaiting)
			b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to stop coder agent `%s` after THUMBS_UP verdict; the pending verdict remains recoverable", coder.ID))
			return persistRetryableFailure(
				fmt.Errorf(
					"failed to stop coder runtime for %s: %w",
					coder.ID,
					err,
				),
			)
		}
		if err := b.ensurePRReadyForReview(ctx, coder.PRNumber); err != nil {
			_ = b.setCoderLifecycleState(coder.ID, StateWaiting)
			b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: review agent `%s` reported THUMBS_UP for coder `%s`, but failed to mark PR ready for review on %s: %s", pending.ReviewerID, coder.ID, coder.PRURL, err))
			return persistRetryableFailure(
				fmt.Errorf(
					"failed to mark PR #%d ready for review after THUMBS_UP: %w",
					coder.PRNumber,
					err,
				),
			)
		}
		_ = b.setCoderLifecycleState(coder.ID, StateApproved)
	}

	completionRollback, completed :=
		b.agents.completePendingReviewVerdict(
			coder.ID,
		)
	if !completed {
		return persistRetryableFailure(
			fmt.Errorf(
				"pending review verdict for coder %s could not be completed",
				coder.ID,
			),
		)
	}
	if err := b.persistAgentState(); err != nil {
		if !b.agents.rollbackPendingReviewVerdictCompletion(
			completionRollback,
		) {
			return fmt.Errorf(
				"failed to persist completed review verdict for coder %s: %w; failed to restore its durable pending intent",
				coder.ID,
				err,
			)
		}
		return fmt.Errorf(
			"failed to persist completed review verdict for coder %s: %w",
			coder.ID,
			err,
		)
	}

	log.Printf(
		"review verdict received reviewer=%s coder=%s pr=%d verdict=%s comment_id=%d",
		pending.ReviewerID,
		coder.ID,
		coder.PRNumber,
		pending.Verdict,
		pending.CommentID,
	)
	switch pending.Verdict {
	case ReviewVerdictNeedsChanges:
		if humanCorrectionEscalation {
			b.notify(ctx, fmt.Sprintf(
				"Repository Agent Orchestrator: PR %s reached the configured limit of %d automated correction rounds; remaining NEEDS_CHANGES require human review, the PR remains draft, and coder `%s` was not restarted",
				coder.PRURL,
				reviewer.ReviewCycle.Policy.Escalation.AfterCorrectionRounds,
				coder.ID,
			))
		} else {
			b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: review agent `%s` reported NEEDS_CHANGES for coder `%s` on PR %s", pending.ReviewerID, coder.ID, coder.PRURL))
		}
		log.Printf("review verdict needs-changes coder=%s pr=%d", coder.ID, coder.PRNumber)
	case ReviewVerdictThumbsUp:
		author := b.getAuthenticatedUserLogin(ctx)
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: PR ready for human review from agent `%s`: %s (waiting for GitHub approval before merge) %s", coder.ID, coder.PRURL, author))
		log.Printf("review verdict thumbs-up coder=%s pr=%d", coder.ID, coder.PRNumber)
	}
	return nil
}

func (b *Orchestrator) stopCoderRuntimeForPendingVerdict(
	coder Agent,
) error {
	attempt, hasActiveCorrection := activeCorrectionLaunchAttempt(coder)
	if strings.TrimSpace(coder.RuntimeHandle.Session) != "" {
		if err := b.stopRuntime(coder); err != nil {
			return err
		}
	} else if b != nil && b.runner != nil &&
		(!coder.Paused || hasActiveCorrection) {
		handle := RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: tmuxSessionName(coder),
		}
		if err := b.runner.Stop(handle); err != nil {
			return fmt.Errorf(
				"failed to stop an ambiguous correction runtime for coder %s: %w",
				coder.ID,
				err,
			)
		}
	}
	if !hasActiveCorrection {
		return nil
	}
	_, err := b.transitionAndPersistCoderLaunchAttempt(
		coder.ID,
		attempt.ID,
		DurableLaunchCompleted,
		nil,
		time.Time{},
	)
	return err
}

func correctionRuntimeReservationHandle(coder Agent) RuntimeHandle {
	handle := coder.RuntimeHandle
	handle.Kind = RuntimeKindTmux
	handle.Session = tmuxSessionName(coder)
	return handle
}

func (b *Orchestrator) runMandatoryTestGate(ctx context.Context, agent Agent) error {
	return b.runMandatoryTestGateWithStart(ctx, agent, nil)
}

func (b *Orchestrator) runMandatoryTestGateWithStart(ctx context.Context, agent Agent, onStart func()) error {
	path := strings.TrimSpace(agent.WorktreePath)
	if path == "" {
		return fmt.Errorf("hard gate cannot run: worktree path is empty")
	}
	logPath, err := mandatoryTestLogPath(agent)
	if err != nil {
		return fmt.Errorf("hard gate log path error: %w", err)
	}
	gateCtx, gateCancel := context.WithCancel(ctx)
	defer gateCancel()
	b.registerReviewGateCancel(agent.ID, gateCancel)
	defer b.clearReviewGateCancel(agent.ID)
	if err := b.acquireHardGateSlot(gateCtx); err != nil {
		return err
	}
	defer b.releaseHardGateSlot()
	if err := resetCommandLog(logPath); err != nil {
		return fmt.Errorf("failed to initialize hard gate log %q: %w", logPath, err)
	}
	if err := appendCommandLogNote(logPath, "hard gate started"); err != nil {
		return fmt.Errorf("failed to write hard gate log %q: %w", logPath, err)
	}
	if onStart != nil {
		onStart()
	}
	execCtx, cancel := context.WithTimeout(gateCtx, mandatoryTestGateTimeout)
	defer cancel()
	pauseDetector := startWallClockPauseDetector(execCtx, hardGatePauseDetectionInterval, hardGatePauseThreshold)
	defer pauseDetector.Stop()
	if err := verifyTrackedRuntimeArtifactsInWorktree(execCtx, path, logPath); err != nil {
		if pauseDetector.SawPause() {
			return interruptedMandatoryTestGateError(logPath, err)
		}
		return err
	}
	for _, raw := range b.cfg.MandatoryTests {
		command := strings.TrimSpace(raw)
		name, args, err := parseMandatoryTestCommand(command)
		if err != nil {
			return fmt.Errorf("hard gate failed: invalid mandatory test command %q: %w", raw, err)
		}
		if err := b.runMandatoryTestCommand(execCtx, path, logPath, name, args...); err != nil {
			gateErr := &mandatoryTestGateError{Command: command, Cause: err}
			if pauseDetector.SawPause() {
				return interruptedMandatoryTestGateError(logPath, gateErr)
			}
			return gateErr
		}
	}
	if err := appendCommandLogNote(logPath, "hard gate passed"); err != nil {
		return fmt.Errorf("failed to finalize hard gate log %q: %w", logPath, err)
	}

	return nil
}

func verifyTrackedRuntimeArtifactsInWorktree(ctx context.Context, worktreePath, logPath string) error {
	ok, err := isGitRepo(ctx, worktreePath)
	if err != nil || !ok {
		return nil
	}
	const gateCheck = "tracked Repository Agent Orchestrator runtime artifact check"
	if err := appendCommandLogNote(logPath, gateCheck+" started"); err != nil {
		return fmt.Errorf("failed to write hard gate log %q: %w", logPath, err)
	}
	out, err := outputCommand(ctx, worktreePath, "git", "ls-files", "--cached", "--full-name", "--", ".repository-agent-orchestrator", ".worktrees")
	if err != nil {
		_ = appendCommandLogNote(logPath, gateCheck+" failed: "+strings.TrimSpace(err.Error()))
		return &mandatoryTestGateError{Command: gateCheck, Cause: err}
	}
	lines := strings.Split(strings.ReplaceAll(string(out), "\r\n", "\n"), "\n")
	paths := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		paths = append(paths, line)
	}
	paths = uniqueSortedStrings(paths)
	if len(paths) == 0 {
		if err := appendCommandLogNote(logPath, gateCheck+" passed"); err != nil {
			return fmt.Errorf("failed to finalize hard gate log %q: %w", logPath, err)
		}
		return nil
	}
	detail := "tracked Repository Agent Orchestrator runtime artifacts are committed in this change:\n- " +
		strings.Join(paths, "\n- ") +
		"\nRemove these paths from the PR; Repository Agent Orchestrator runtime artifacts must remain ignored and must not be staged or committed."
	_ = appendCommandLogNote(logPath, gateCheck+" failed:\n"+detail)
	return &mandatoryTestGateError{Command: gateCheck, Cause: errors.New(detail)}
}

func (b *Orchestrator) runMandatoryTestCommand(ctx context.Context, dir string, logPath string, name string, args ...string) error {
	if b != nil && b.mandatoryTestRunner != nil {
		return b.mandatoryTestRunner(ctx, dir, logPath, name, args...)
	}
	if b != nil && b.cmdRunner != nil {
		return b.cmdRunner(ctx, dir, name, args...)
	}
	return runCommandWithOutputAndLiveLog(ctx, dir, logPath, name, args...)
}

type wallClockPauseDetector struct {
	threshold    time.Duration
	lastUnixNano atomic.Int64
	sawPause     atomic.Bool
	cancel       context.CancelFunc
}

func startWallClockPauseDetector(ctx context.Context, interval, threshold time.Duration) *wallClockPauseDetector {
	d := &wallClockPauseDetector{threshold: threshold}
	d.lastUnixNano.Store(wallClockNow().UnixNano())
	if interval <= 0 || threshold <= 0 {
		return d
	}
	detectCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-detectCtx.Done():
				return
			case <-ticker.C:
				d.SawPause()
			}
		}
	}()
	return d
}

func (d *wallClockPauseDetector) Stop() {
	if d == nil || d.cancel == nil {
		return
	}
	d.cancel()
}

func (d *wallClockPauseDetector) SawPause() bool {
	if d == nil || d.threshold <= 0 {
		return false
	}
	now := wallClockNow().UnixNano()
	last := d.lastUnixNano.Swap(now)
	if last > 0 && time.Duration(now-last) > d.threshold {
		d.sawPause.Store(true)
	}
	return d.sawPause.Load()
}

func wallClockNow() time.Time {
	return time.Unix(0, time.Now().UnixNano())
}

func interruptedMandatoryTestGateError(logPath string, err error) error {
	const reason = "host sleep or long process suspension detected"
	_ = appendCommandLogNote(logPath, "hard gate interrupted: "+reason)
	return &mandatoryTestGateInterruptedError{Reason: reason, Cause: err}
}

func isMandatoryTestGateInterrupted(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	var interrupted *mandatoryTestGateInterruptedError
	return errors.As(err, &interrupted)
}

type mandatoryTestGateInterruptedError struct {
	Reason string
	Cause  error
}

func (e *mandatoryTestGateInterruptedError) Error() string {
	reason := strings.TrimSpace(e.Reason)
	if reason == "" {
		reason = "interrupted"
	}
	if e.Cause == nil {
		return "hard gate interrupted: " + reason
	}
	return fmt.Sprintf("hard gate interrupted: %s\n%s", reason, strings.TrimSpace(e.Cause.Error()))
}

func (e *mandatoryTestGateInterruptedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type mandatoryTestGateError struct {
	Command string
	Cause   error
}

func (e *mandatoryTestGateError) Error() string {
	command := strings.TrimSpace(e.Command)
	if command == "" {
		command = "(unknown command)"
	}
	message := fmt.Sprintf("hard gate failed: %s", command)
	if detail := strings.TrimSpace(formatMandatoryTestGateCause(e.Cause)); detail != "" {
		return message + "\n" + detail
	}
	return message
}

func (e *mandatoryTestGateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func formatMandatoryTestGateCause(err error) string {
	if err == nil {
		return ""
	}
	var commandErr *commandExecutionError
	if errors.As(err, &commandErr) {
		if detail := strings.TrimSpace(commandErr.Detail()); detail != "" {
			return detail
		}
	}
	cause := strings.TrimSpace(err.Error())
	if cause == "" {
		return ""
	}
	return "Cause: " + cause
}

func abbreviateSHA(sha string) string {
	trimmed := strings.TrimSpace(sha)
	if len(trimmed) <= 12 {
		return trimmed
	}
	return trimmed[:12]
}
