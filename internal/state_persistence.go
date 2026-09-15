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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const persistedStateVersion = 6

type persistedStateFile struct {
	Version     int              `json:"version"`
	LastSavedAt time.Time        `json:"last_saved_at"`
	LastPoll    time.Time        `json:"last_poll"`
	Agents      []persistedAgent `json:"agents"`
}

type persistedAgent struct {
	ID                              string                           `json:"id"`
	Role                            AgentRole                        `json:"role"`
	ParentAgentID                   string                           `json:"parent_agent_id"`
	IssueNumber                     int                              `json:"issue_number"`
	IssueTitle                      string                           `json:"issue_title"`
	IssueBody                       string                           `json:"issue_body"`
	WorktreePath                    string                           `json:"worktree_path"`
	LogDir                          string                           `json:"log_dir"`
	RuntimeCWD                      string                           `json:"runtime_cwd"`
	BranchName                      string                           `json:"branch_name"`
	PRHeadBranch                    string                           `json:"pr_head_branch"`
	AdoptedPR                       bool                             `json:"adopted_pr"`
	RepoIndexHadOutput              bool                             `json:"repo_index_had_output"`
	RepoIndexBaselineModTime        time.Time                        `json:"repo_index_baseline_mod_time"`
	RepoIndexHadSource              bool                             `json:"repo_index_had_source"`
	RepoIndexSourceBaselineTime     time.Time                        `json:"repo_index_source_baseline_time"`
	HandoffCaptured                 bool                             `json:"handoff_captured"`
	RuntimeHandle                   RuntimeHandle                    `json:"runtime_handle"`
	RuntimeProfile                  AgentProfile                     `json:"runtime_profile"`
	PRNumber                        int                              `json:"pr_number"`
	PRTitle                         string                           `json:"pr_title"`
	PRURL                           string                           `json:"pr_url"`
	ObservedPRHeadSHA               string                           `json:"observed_pr_head_sha"`
	ActiveReviewAgentID             string                           `json:"active_review_agent_id"`
	HumanReviewGuidance             []string                         `json:"human_review_guidance"`
	LastPreReviewGateFailureHeadSHA string                           `json:"last_pre_review_gate_failure_head_sha"`
	LastPreReviewGateFailureAt      time.Time                        `json:"last_pre_review_gate_failure_at"`
	LaunchAttempts                  []DurableLaunchAttempt           `json:"launch_attempts,omitempty"`
	LastReviewedHeadSHA             string                           `json:"last_reviewed_head_sha"`
	ManualReviewHold                bool                             `json:"manual_review_hold,omitempty"`
	LastReviewVerdict               ReviewVerdict                    `json:"last_review_verdict"`
	LastReviewCommentID             int64                            `json:"last_review_comment_id"`
	PendingReviewVerdict            *PendingReviewVerdictApplication `json:"pending_review_verdict,omitempty"`
	ReviewCycle                     *ReviewCycleState                `json:"review_cycle,omitempty"`
	ReviewCoordinatorLifecycle      *ReviewCoordinatorLifecycleState `json:"review_coordinator_lifecycle,omitempty"`
	ReviewLedger                    *ReviewLedger                    `json:"review_ledger,omitempty"`
	ReviewSkipHardGate              bool                             `json:"review_skip_hard_gate,omitempty"`
	LastConflictHeadSHA             string                           `json:"last_conflict_head_sha"`
	ReviewBaselineIssueCommentID    int64                            `json:"review_baseline_issue_comment_id"`
	State                           AgentState                       `json:"state"`
	Paused                          bool                             `json:"paused"`
	LastActivityTime                time.Time                        `json:"last_activity_time"`
	Stopped                         bool                             `json:"stopped"`
	SeenReviewCommentIDs            []int64                          `json:"seen_review_comment_ids"`
	SeenIssueCommentIDs             []int64                          `json:"seen_issue_comment_ids"`
	PendingReviewCommentIDs         []int64                          `json:"pending_review_comment_ids"`
}

func (b *Orchestrator) agentStateFilePath() string {
	if b == nil {
		return ""
	}
	logDir := strings.TrimSpace(b.cfg.LogDir)
	if logDir == "" {
		return ""
	}
	return filepath.Join(orchestratorStateDir(logDir), orchestratorStateFileName)
}

func (b *Orchestrator) persistAgentState() error {
	if b == nil || b.agents == nil {
		return nil
	}
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	return b.persistAgentStateLocked()
}

func (b *Orchestrator) persistAgentStateLocked() error {
	statePath := b.agentStateFilePath()
	if strings.TrimSpace(statePath) == "" {
		return nil
	}
	lastPoll, agents := b.agents.snapshotForPersistence()
	for index := range agents {
		cycle := agents[index].ReviewCycle
		if cycle == nil {
			continue
		}
		metrics, err := reviewMetricsStateForCycle(cycle)
		if err != nil {
			return fmt.Errorf(
				"failed to aggregate review metrics for agent %s: %w",
				agents[index].ID,
				err,
			)
		}
		cycle.Metrics = metrics
	}
	payload := persistedStateFile{
		Version:     persistedStateVersion,
		LastSavedAt: time.Now().UTC(),
		LastPoll:    lastPoll,
		Agents:      agents,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode agent state: %w", err)
	}
	stateDir := filepath.Dir(statePath)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return fmt.Errorf("failed to create agent state dir %q: %w", stateDir, err)
	}
	tmpPath := statePath + ".tmp"
	if err := os.WriteFile(tmpPath, body, 0o644); err != nil {
		return fmt.Errorf("failed to write temporary agent state file %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, statePath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to replace agent state file %q: %w", statePath, err)
	}
	b.agents.mergePersistedReviewMetrics(agents)
	return nil
}

func (b *Orchestrator) loadPersistedAgentState() error {
	if b == nil || b.agents == nil {
		return nil
	}
	statePath := b.agentStateFilePath()
	if strings.TrimSpace(statePath) == "" {
		return nil
	}
	body, err := os.ReadFile(statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to read agent state file %q: %w", statePath, err)
	}
	var payload persistedStateFile
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("failed to decode agent state file %q: %w", statePath, err)
	}
	if payload.Version != persistedStateVersion {
		return fmt.Errorf(
			"persisted agent state %q has unsupported version %d (expected %d); rerun with --clean to discard old state",
			statePath,
			payload.Version,
			persistedStateVersion,
		)
	}
	if err := validatePersistedReviewLedgers(payload.Agents); err != nil {
		return err
	}
	if err := validatePersistedReviewCycleSnapshots(payload.Agents); err != nil {
		return err
	}
	if err := validatePersistedReviewCoordinatorLifecycles(
		payload.Agents,
	); err != nil {
		return err
	}
	if err := validatePersistedPendingReviewVerdicts(payload.Agents); err != nil {
		return err
	}
	if err := validatePersistedLaunchAttempts(payload.Agents); err != nil {
		return err
	}
	cleanupTargets := skippedPersistedAgentsForCleanup(payload.Agents)
	loadedAgents := b.agents.restoreFromPersistence(payload.LastPoll, payload.Agents)
	if err := b.restoreMissingRuntimeProfiles(); err != nil {
		return err
	}
	if loadedAgents > 0 {
		log.Printf("loaded persisted agent state path=%s agents=%d", statePath, loadedAgents)
	}
	if len(cleanupTargets) > 0 {
		if err := b.cleanupSkippedPersistedAgents(cleanupTargets); err != nil {
			return err
		}
	}
	return nil
}

func validatePersistedLaunchAttempts(persistedAgents []persistedAgent) error {
	for _, persisted := range persistedAgents {
		if len(persisted.LaunchAttempts) > 0 && persisted.Role != RoleCoder {
			return fmt.Errorf(
				"persisted non-coder agent %s has coder launch attempts",
				persisted.ID,
			)
		}
		seen := make(map[string]struct{}, len(persisted.LaunchAttempts))
		sequences := make(map[string]int, len(persisted.LaunchAttempts))
		active := make(map[string]struct{}, len(persisted.LaunchAttempts))
		for _, attempt := range persisted.LaunchAttempts {
			if err := validateDurableLaunchAttempt(attempt); err != nil {
				return fmt.Errorf(
					"persisted coder %s has invalid launch attempt: %w",
					persisted.ID,
					err,
				)
			}
			if attempt.Kind == DurableLaunchReviewWorker {
				return fmt.Errorf(
					"persisted coder %s owns a review-worker attempt",
					persisted.ID,
				)
			}
			sequenceKey := string(attempt.Kind) + "\x00" + attempt.Scope
			if attempt.Kind == DurableLaunchCorrectionRuntime {
				sequenceKey = string(attempt.Kind)
			}
			sequences[sequenceKey]++
			if attempt.Attempt != sequences[sequenceKey] {
				return fmt.Errorf(
					"persisted coder %s launch attempt %s has non-contiguous ordinal %d",
					persisted.ID,
					attempt.ID,
					attempt.Attempt,
				)
			}
			if !durableLaunchTerminal(attempt.Lifecycle) {
				if _, duplicate := active[sequenceKey]; duplicate {
					return fmt.Errorf(
						"persisted coder %s has multiple active launches in scope %s",
						persisted.ID,
						attempt.Scope,
					)
				}
				active[sequenceKey] = struct{}{}
			}
			switch attempt.Kind {
			case DurableLaunchReviewCoordinator:
				if err := validateCanonicalGitObjectID(attempt.Scope); err != nil {
					return fmt.Errorf(
						"persisted coder %s review launch scope is invalid: %w",
						persisted.ID,
						err,
					)
				}
				if attempt.SessionName != "" {
					return fmt.Errorf(
						"persisted coder %s review coordinator launch owns a runtime session",
						persisted.ID,
					)
				}
			case DurableLaunchCorrectionRuntime:
				expectedSession := tmuxSessionName(Agent{
					ID:          persisted.ID,
					IssueNumber: persisted.IssueNumber,
				})
				if attempt.OwnerID != persisted.ID ||
					attempt.SessionName != expectedSession {
					return fmt.Errorf(
						"persisted coder %s correction launch ownership is invalid",
						persisted.ID,
					)
				}
			}
			if _, duplicate := seen[attempt.ID]; duplicate {
				return fmt.Errorf(
					"persisted coder %s duplicates launch attempt %s",
					persisted.ID,
					attempt.ID,
				)
			}
			seen[attempt.ID] = struct{}{}
		}
	}
	return nil
}

func validatePersistedPendingReviewVerdicts(
	persistedAgents []persistedAgent,
) error {
	for _, persisted := range persistedAgents {
		if persisted.PendingReviewVerdict == nil {
			continue
		}
		if persisted.Role != RoleCoder {
			return fmt.Errorf(
				"persisted non-coder agent %s has a pending review verdict",
				persisted.ID,
			)
		}
		if err := validatePendingReviewVerdictApplication(
			persisted.PendingReviewVerdict,
		); err != nil {
			return fmt.Errorf(
				"persisted coder %s has invalid pending review verdict: %w",
				persisted.ID,
				err,
			)
		}
		if attemptID := strings.TrimSpace(
			persisted.PendingReviewVerdict.CorrectionAttemptID,
		); attemptID != "" {
			attempt, found := durableLaunchAttemptByID(
				persisted.LaunchAttempts,
				attemptID,
			)
			if !found ||
				attempt.Kind != DurableLaunchCorrectionRuntime ||
				durableLaunchTerminal(attempt.Lifecycle) {
				return fmt.Errorf(
					"persisted coder %s pending verdict references invalid correction attempt %s",
					persisted.ID,
					attemptID,
				)
			}
			expectedSession := tmuxSessionName(Agent{
				ID:          persisted.ID,
				IssueNumber: persisted.IssueNumber,
			})
			if attempt.SessionName != expectedSession ||
				persisted.RuntimeHandle.Kind != RuntimeKindTmux ||
				persisted.RuntimeHandle.Session != expectedSession {
				return fmt.Errorf(
					"persisted coder %s correction reservation does not own deterministic runtime %s",
					persisted.ID,
					expectedSession,
				)
			}
		}
	}
	return nil
}

func validatePersistedReviewLedgers(
	persistedAgents []persistedAgent,
) error {
	for _, persisted := range persistedAgents {
		if persisted.ReviewLedger == nil {
			continue
		}
		if persisted.Role != RoleCoder {
			return fmt.Errorf(
				"persisted non-coder agent %s has a review ledger",
				persisted.ID,
			)
		}
		if err := validateReviewLedger(persisted.ReviewLedger); err != nil {
			return fmt.Errorf(
				"persisted coder %s has an invalid review ledger: %w",
				persisted.ID,
				err,
			)
		}
	}
	return nil
}

func validatePersistedReviewCycleSnapshots(persistedAgents []persistedAgent) error {
	for _, persisted := range persistedAgents {
		if persisted.Role != RoleReviewer {
			continue
		}
		agent := persisted.toAgent()
		if agent.ReviewCycle != nil {
			expected, final := reviewCycleResultForReviewer(agent)
			switch {
			case final && agent.ReviewCycle.ResultState != expected:
				return fmt.Errorf(
					"persisted review agent %s result state is %q; expected %q",
					agent.ID,
					agent.ReviewCycle.ResultState,
					expected,
				)
			case !final && agent.ReviewCycle.ResultState != "":
				return fmt.Errorf(
					"persisted active review agent %s has a final result state",
					agent.ID,
				)
			}
		}
		if persisted.Stopped || terminalAgentState(persisted.State) {
			continue
		}
		if strings.TrimSpace(agent.ObservedPRHeadSHA) == "" {
			return fmt.Errorf(
				"persisted review agent %s is missing its exact head SHA; rerun with --clean to discard incomplete state",
				agent.ID,
			)
		}
		if err := validateCanonicalGitObjectID(agent.ObservedPRHeadSHA); err != nil {
			return fmt.Errorf(
				"persisted review agent %s has invalid exact head SHA: %w",
				agent.ID,
				err,
			)
		}
		if agent.ReviewCycle == nil {
			return fmt.Errorf(
				"persisted review agent %s for head %s is missing its review policy snapshot; rerun with --clean to discard incomplete state",
				agent.ID,
				abbreviateSHA(agent.ObservedPRHeadSHA),
			)
		}
		if agent.ReviewCycle.HeadSHA != agent.ObservedPRHeadSHA &&
			validateCanonicalGitObjectID(agent.ReviewCycle.HeadSHA) == nil {
			return fmt.Errorf(
				"persisted review agent %s policy snapshot head mismatch: reviewer=%s snapshot=%s",
				agent.ID,
				abbreviateSHA(agent.ObservedPRHeadSHA),
				abbreviateSHA(agent.ReviewCycle.HeadSHA),
			)
		}
		if err := validatePersistedReviewCycleSnapshot(agent.ReviewCycle); err != nil {
			return fmt.Errorf("persisted review agent %s has invalid review policy state: %w", agent.ID, err)
		}
		if agent.ReviewCycle.HeadSHA != agent.ObservedPRHeadSHA {
			return fmt.Errorf(
				"persisted review agent %s policy snapshot head mismatch: reviewer=%s snapshot=%s",
				agent.ID,
				abbreviateSHA(agent.ObservedPRHeadSHA),
				abbreviateSHA(agent.ReviewCycle.HeadSHA),
			)
		}
	}
	return nil
}

func validatePersistedReviewCoordinatorLifecycles(
	persistedAgents []persistedAgent,
) error {
	for _, persisted := range persistedAgents {
		if persisted.ReviewCoordinatorLifecycle == nil {
			continue
		}
		reviewer := persisted.toAgent()
		if err := validateReviewCoordinatorLifecycleState(reviewer); err != nil {
			return fmt.Errorf(
				"persisted review coordinator lifecycle for agent %s is invalid: %w",
				persisted.ID,
				err,
			)
		}
	}
	return nil
}

func (b *Orchestrator) restoreMissingRuntimeProfiles() error {
	if b == nil || b.agents == nil {
		return nil
	}
	for _, agent := range b.agents.Active() {
		if agent.Role == RoleReviewer {
			if err := b.restoreReviewRuntimeProfile(agent); err != nil {
				return err
			}
			continue
		}
		if agentProfileIsConfigured(agent.RuntimeProfile) {
			if err := validateAgentProfile(agent.RuntimeProfile, b.cfg.ReviewPolicy.activeModelCatalog()); err != nil {
				return fmt.Errorf(
					"persisted runtime profile for agent %s is invalid under the active REVIEW_POLICY.MODEL_CATALOG: %w",
					agent.ID,
					err,
				)
			}
			continue
		}
		var role AgentProfileRole
		switch agent.Role {
		case RoleCoder, "":
			role = AgentProfileRoleCoder
		case RoleIndexer:
			role = AgentProfileRoleIndexer
		case RoleReviewer:
			role = AgentProfileRoleChallenge
		default:
			continue
		}
		profile, err := b.cfg.runtimeProfileForRole(role)
		if err != nil {
			return fmt.Errorf("failed to restore runtime profile for agent %s: %w", agent.ID, err)
		}
		if !b.agents.SetRuntimeProfile(agent.ID, profile) {
			return fmt.Errorf("failed to restore runtime profile for agent %s", agent.ID)
		}
	}
	return nil
}

func (b *Orchestrator) restoreReviewRuntimeProfile(agent Agent) error {
	if agent.ReviewCycle == nil {
		return fmt.Errorf(
			"persisted review agent %s is missing its review policy snapshot; rerun with --clean to discard incomplete state",
			agent.ID,
		)
	}
	snapshotProfile, err := agent.ReviewCycle.Policy.effectiveProfileForRole(AgentProfileRoleChallenge)
	if err != nil {
		return fmt.Errorf(
			"persisted review agent %s has invalid snapshotted challenge profile: %w",
			agent.ID,
			err,
		)
	}
	if agentProfileIsConfigured(agent.RuntimeProfile) {
		if err := validateAgentProfile(agent.RuntimeProfile, agent.ReviewCycle.Policy.activeModelCatalog()); err != nil {
			return fmt.Errorf(
				"persisted runtime profile for review agent %s is invalid under its snapshotted REVIEW_POLICY.MODEL_CATALOG: %w",
				agent.ID,
				err,
			)
		}
		if agent.RuntimeProfile != snapshotProfile {
			return fmt.Errorf(
				"persisted runtime profile for review agent %s does not match its snapshotted challenge profile",
				agent.ID,
			)
		}
		return nil
	}
	if !b.agents.SetRuntimeProfile(agent.ID, snapshotProfile) {
		return fmt.Errorf("failed to restore snapshotted runtime profile for review agent %s", agent.ID)
	}
	return nil
}

func agentProfileIsConfigured(profile AgentProfile) bool {
	return profile.InheritGlobal ||
		strings.TrimSpace(profile.Name) != "" ||
		strings.TrimSpace(profile.Model) != "" ||
		strings.TrimSpace(profile.ReasoningEffort) != ""
}

func skippedPersistedAgentsForCleanup(persistedAgents []persistedAgent) []persistedAgent {
	targets := make([]persistedAgent, 0)
	for _, persisted := range persistedAgents {
		if shouldCleanupSkippedPersistedAgent(persisted) {
			targets = append(targets, persisted)
		}
	}
	return targets
}

func shouldCleanupSkippedPersistedAgent(agent persistedAgent) bool {
	if shouldRestorePersistedAgent(agent) {
		return false
	}
	if strings.TrimSpace(agent.WorktreePath) == "" {
		return false
	}
	switch agent.State {
	case StateInitializing, StateReviewGate:
		return strings.TrimSpace(agent.RuntimeHandle.Session) == ""
	default:
		return false
	}
}

func (b *Orchestrator) cleanupSkippedPersistedAgents(
	persistedAgents []persistedAgent,
) error {
	if b == nil {
		return nil
	}
	for _, persisted := range persistedAgents {
		agent := persisted.toAgent()
		if b.runner != nil {
			reconciler, ok := b.runner.(RuntimeLaunchReconciler)
			if !ok {
				return fmt.Errorf(
					"cannot reconcile abandoned runtime launch for agent %s: runner does not support launch reconciliation",
					agent.ID,
				)
			}
			if err := reconciler.ReconcileRuntimeLaunch(agent); err != nil {
				return fmt.Errorf(
					"failed to reconcile abandoned runtime launch for agent %s: %w",
					agent.ID,
					err,
				)
			}
			if _, ok := b.runner.(RuntimeStateCleaner); !ok {
				return fmt.Errorf(
					"cannot remove abandoned runtime state for agent %s: runner does not support runtime state cleanup",
					agent.ID,
				)
			}
		}
		if err := b.removeAgentRuntimeState(agent); err != nil {
			return fmt.Errorf(
				"failed to remove abandoned runtime state for agent %s: %w",
				agent.ID,
				err,
			)
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := b.cleanupWorktree(cleanupCtx, agent.WorktreePath, cleanupBranchName(agent))
		cancel()
		if err != nil {
			log.Printf(
				"non-fatal: failed to cleanup abandoned persisted agent worktree agent=%s role=%s state=%s path=%s: %s",
				agent.ID,
				agent.Role,
				agent.State,
				agent.WorktreePath,
				b.safeError(err),
			)
		}
		if err := b.removeAgentRuntimeLog(agent); err != nil {
			log.Printf("non-fatal: failed to remove abandoned persisted agent runtime log agent=%s: %s", agent.ID, b.safeError(err))
		}
		if err := b.removeAgentMandatoryTestLog(agent); err != nil {
			log.Printf("non-fatal: failed to remove abandoned persisted agent gate log agent=%s: %s", agent.ID, b.safeError(err))
		}
		if err := b.removePersistedHandoffs(agent.ID); err != nil {
			log.Printf("non-fatal: failed to remove abandoned persisted agent handoffs agent=%s: %s", agent.ID, b.safeError(err))
		}
	}
	return nil
}

func (m *AgentManager) snapshotForPersistence() (time.Time, []persistedAgent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make([]string, 0, len(m.agents))
	for id, agent := range m.agents {
		if !shouldPersistAgentState(agent.State, agent.Stopped) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	agents := make([]persistedAgent, 0, len(ids))
	for _, id := range ids {
		agents = append(agents, toPersistedAgent(m.agents[id]))
	}
	return m.lastPoll, agents
}

// mergePersistedReviewMetrics makes the successfully written append-only
// projection visible in memory without rolling a failed checkpoint into live
// accounting. Concurrently added trusted events are retained.
func (m *AgentManager) mergePersistedReviewMetrics(
	persistedAgents []persistedAgent,
) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, persisted := range persistedAgents {
		if persisted.ReviewCycle == nil ||
			persisted.ReviewCycle.Metrics == nil {
			continue
		}
		current, ok := m.agents[persisted.ID]
		if !ok || current == nil || current.ReviewCycle == nil ||
			current.ReviewCycle.ID != persisted.ReviewCycle.ID ||
			current.ReviewCycle.HeadSHA !=
				persisted.ReviewCycle.HeadSHA {
			continue
		}
		if current.ReviewCycle.Metrics == nil {
			current.ReviewCycle.Metrics = &ReviewMetricsState{
				SchemaVersion: reviewMetricsSchemaVersion,
				Namespace:     current.ReviewCycle.ID,
				Events:        []ReviewMetricEvent{},
			}
		}
		for _, event := range persisted.ReviewCycle.Metrics.Events {
			_, _ = recordTrustedReviewMetricEvent(
				current.ReviewCycle.Metrics,
				event,
			)
		}
	}
}

func (m *AgentManager) restoreFromPersistence(lastPoll time.Time, persistedAgents []persistedAgent) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastPoll = lastPoll
	m.agents = make(map[string]*Agent, len(persistedAgents))
	m.reviewCoordinatorLifecycle = make(map[string]*sync.RWMutex)
	restored := 0
	for _, persisted := range persistedAgents {
		if !shouldRestorePersistedAgent(persisted) {
			continue
		}
		agent := persisted.toAgent()
		a := agent
		m.agents[a.ID] = &a
		if a.Role == RoleReviewer {
			m.reviewCoordinatorLifecycleLocked(a.ID)
		}
		restored++
	}
	m.clearDanglingReviewerReferencesLocked()
	return restored
}

func shouldRestorePersistedAgent(agent persistedAgent) bool {
	if strings.TrimSpace(agent.ID) == "" || !shouldPersistAgentState(agent.State, agent.Stopped) {
		return false
	}
	// Most launch workflows are not resumable across Repository Agent
	// Orchestrator restarts. Reviewers are the exception: their validated cycle
	// checkpoint is sufficient to retry launch without consulting current policy.
	if (agent.State == StateInitializing || agent.State == StateReviewGate) && strings.TrimSpace(agent.RuntimeHandle.Session) == "" {
		return agent.Role == RoleReviewer && agent.ReviewCycle != nil
	}
	return true
}

func (m *AgentManager) clearDanglingReviewerReferencesLocked() {
	for _, agent := range m.agents {
		if agent == nil || agent.Role != RoleCoder {
			continue
		}
		reviewerID := strings.TrimSpace(agent.ActiveReviewAgentID)
		if reviewerID == "" {
			continue
		}
		reviewer, ok := m.agents[reviewerID]
		if !ok || reviewer == nil || reviewer.Role != RoleReviewer || reviewer.Stopped {
			agent.ActiveReviewAgentID = ""
			continue
		}
		switch reviewer.State {
		case StateDone, StateErrored, StateStopped:
			agent.ActiveReviewAgentID = ""
		}
	}
}

func toPersistedAgent(agent *Agent) persistedAgent {
	if agent == nil {
		return persistedAgent{}
	}
	return persistedAgent{
		ID:                              agent.ID,
		Role:                            agent.Role,
		ParentAgentID:                   agent.ParentAgentID,
		IssueNumber:                     agent.IssueNumber,
		IssueTitle:                      agent.IssueTitle,
		IssueBody:                       agent.IssueBody,
		WorktreePath:                    agent.WorktreePath,
		LogDir:                          agent.LogDir,
		RuntimeCWD:                      agent.RuntimeCWD,
		BranchName:                      agent.BranchName,
		PRHeadBranch:                    agent.PRHeadBranch,
		AdoptedPR:                       agent.AdoptedPR,
		RepoIndexHadOutput:              agent.RepoIndexHadOutput,
		RepoIndexBaselineModTime:        agent.RepoIndexBaselineModTime,
		RepoIndexHadSource:              agent.RepoIndexHadSource,
		RepoIndexSourceBaselineTime:     agent.RepoIndexSourceBaselineTime,
		HandoffCaptured:                 agent.HandoffCaptured,
		RuntimeHandle:                   agent.RuntimeHandle,
		RuntimeProfile:                  agent.RuntimeProfile,
		PRNumber:                        agent.PRNumber,
		PRTitle:                         agent.PRTitle,
		PRURL:                           agent.PRURL,
		ObservedPRHeadSHA:               agent.ObservedPRHeadSHA,
		ActiveReviewAgentID:             agent.ActiveReviewAgentID,
		HumanReviewGuidance:             append([]string(nil), agent.HumanReviewGuidance...),
		LastPreReviewGateFailureHeadSHA: agent.LastPreReviewGateFailureHeadSHA,
		LastPreReviewGateFailureAt:      agent.LastPreReviewGateFailureAt,
		LaunchAttempts:                  cloneDurableLaunchAttempts(agent.LaunchAttempts),
		LastReviewedHeadSHA:             agent.LastReviewedHeadSHA,
		ManualReviewHold:                agent.ManualReviewHold,
		LastReviewVerdict:               agent.LastReviewVerdict,
		LastReviewCommentID:             agent.LastReviewCommentID,
		PendingReviewVerdict:            clonePendingReviewVerdictApplication(agent.PendingReviewVerdict),
		ReviewCycle:                     snapshotReviewCycleResult(*agent),
		ReviewCoordinatorLifecycle:      cloneReviewCoordinatorLifecycle(agent.ReviewCoordinatorLifecycle),
		ReviewLedger:                    cloneReviewLedger(agent.ReviewLedger),
		ReviewSkipHardGate:              agent.ReviewSkipHardGate,
		LastConflictHeadSHA:             agent.LastConflictHeadSHA,
		ReviewBaselineIssueCommentID:    agent.ReviewBaselineIssueCommentID,
		State:                           agent.State,
		Paused:                          agent.Paused,
		LastActivityTime:                agent.LastActivityTime.UTC(),
		Stopped:                         agent.Stopped,
		SeenReviewCommentIDs:            sortedCommentIDs(agent.seenReviewCommentIDs),
		SeenIssueCommentIDs:             sortedCommentIDs(agent.seenIssueCommentIDs),
		PendingReviewCommentIDs:         sortedCommentIDs(agent.pendingReviewCommentIDs),
	}
}

func (p persistedAgent) toAgent() Agent {
	agent := Agent{
		ID:                              strings.TrimSpace(p.ID),
		Role:                            p.Role,
		ParentAgentID:                   p.ParentAgentID,
		IssueNumber:                     p.IssueNumber,
		IssueTitle:                      p.IssueTitle,
		IssueBody:                       p.IssueBody,
		WorktreePath:                    p.WorktreePath,
		LogDir:                          p.LogDir,
		RuntimeCWD:                      p.RuntimeCWD,
		BranchName:                      p.BranchName,
		PRHeadBranch:                    p.PRHeadBranch,
		AdoptedPR:                       p.AdoptedPR,
		RepoIndexHadOutput:              p.RepoIndexHadOutput,
		RepoIndexBaselineModTime:        p.RepoIndexBaselineModTime,
		RepoIndexHadSource:              p.RepoIndexHadSource,
		RepoIndexSourceBaselineTime:     p.RepoIndexSourceBaselineTime,
		HandoffCaptured:                 p.HandoffCaptured,
		RuntimeHandle:                   p.RuntimeHandle,
		RuntimeProfile:                  p.RuntimeProfile,
		PRNumber:                        p.PRNumber,
		PRTitle:                         p.PRTitle,
		PRURL:                           p.PRURL,
		ObservedPRHeadSHA:               p.ObservedPRHeadSHA,
		ActiveReviewAgentID:             p.ActiveReviewAgentID,
		HumanReviewGuidance:             append([]string(nil), p.HumanReviewGuidance...),
		LastPreReviewGateFailureHeadSHA: p.LastPreReviewGateFailureHeadSHA,
		LastPreReviewGateFailureAt:      p.LastPreReviewGateFailureAt,
		LaunchAttempts:                  cloneDurableLaunchAttempts(p.LaunchAttempts),
		LastReviewedHeadSHA:             p.LastReviewedHeadSHA,
		ManualReviewHold:                p.ManualReviewHold,
		LastReviewVerdict:               p.LastReviewVerdict,
		LastReviewCommentID:             p.LastReviewCommentID,
		PendingReviewVerdict:            clonePendingReviewVerdictApplication(p.PendingReviewVerdict),
		ReviewCycle:                     cloneReviewCycle(p.ReviewCycle),
		ReviewCoordinatorLifecycle:      cloneReviewCoordinatorLifecycle(p.ReviewCoordinatorLifecycle),
		ReviewLedger:                    cloneReviewLedger(p.ReviewLedger),
		ReviewSkipHardGate:              p.ReviewSkipHardGate,
		LastConflictHeadSHA:             p.LastConflictHeadSHA,
		ReviewBaselineIssueCommentID:    p.ReviewBaselineIssueCommentID,
		State:                           p.State,
		Paused:                          p.Paused,
		LastActivityTime:                p.LastActivityTime.UTC(),
		Stopped:                         p.Stopped,
		seenReviewCommentIDs:            commentIDSet(p.SeenReviewCommentIDs),
		seenIssueCommentIDs:             commentIDSet(p.SeenIssueCommentIDs),
		pendingReviewCommentIDs:         commentIDSet(p.PendingReviewCommentIDs),
	}
	return agent
}

func sortedCommentIDs(items map[int64]struct{}) []int64 {
	if len(items) == 0 {
		return []int64{}
	}
	ids := make([]int64, 0, len(items))
	for id := range items {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func commentIDSet(items []int64) map[int64]struct{} {
	if len(items) == 0 {
		return make(map[int64]struct{})
	}
	out := make(map[int64]struct{}, len(items))
	for _, id := range items {
		out[id] = struct{}{}
	}
	return out
}

func shouldPersistAgentState(state AgentState, stopped bool) bool {
	switch state {
	case StateStopped:
		return true
	case StateDone, StateErrored:
		return false
	default:
		return !stopped
	}
}
