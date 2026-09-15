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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPersistAndLoadAgentStateRoundTrip(t *testing.T) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	agent := &Agent{
		ID:                      "coding-agent-12",
		Role:                    RoleCoder,
		IssueNumber:             12,
		IssueTitle:              "Handle retries",
		BranchName:              "repository-agent-orchestrator/issue-12",
		PRHeadBranch:            "feature/handle-retries",
		AdoptedPR:               true,
		WorktreePath:            "/tmp/repo/.worktrees/issue-12",
		PRNumber:                34,
		PRURL:                   "https://github.com/acme/widget/pull/34",
		ObservedPRHeadSHA:       "abc123",
		HumanReviewGuidance:     []string{"Backward compatibility is explicitly out of scope for this PR."},
		LastConflictHeadSHA:     "abc123",
		State:                   StateWorking,
		LastActivityTime:        time.Unix(1700000000, 0).UTC(),
		RuntimeHandle:           RuntimeHandle{Kind: RuntimeKindTmux, Session: "session-12", LogPath: "/tmp/repository-agent-orchestrator/widget/agent-12.log"},
		RuntimeProfile:          AgentProfile{Name: "coder", Model: "gpt-5.6-sol", ReasoningEffort: "high"},
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	agent.LaunchAttempts = completedCorrectionLaunchAttempts(t, *agent, 2)
	agent.LaunchAttempts = append(
		agent.LaunchAttempts,
		incompleteReviewLaunchAttempt(
			t,
			strings.Repeat("a", canonicalGitObjectIDLength),
			"review-cycle-retry-1",
			1,
			time.Unix(1700000300, 0).UTC(),
		),
	)
	if err := agents.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	_ = agents.MarkReviewCommentSeen(agent.ID, 101)
	_ = agents.MarkIssueCommentSeen(agent.ID, 202)
	_ = agents.AddPendingReviewComment(agent.ID, 303)
	lastPoll := time.Unix(1700001000, 0).UTC()
	agents.SetLastPoll(lastPoll)

	bot := &Orchestrator{cfg: Config{LogDir: logDir}, agents: agents}
	if err := bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}

	stateFile := filepath.Join(logDir, orchestratorStateDirName, orchestratorStateFileName)
	if _, err := os.Stat(stateFile); err != nil {
		t.Fatalf("persisted state file missing at %s: %v", stateFile, err)
	}

	restoredAgents := NewAgentManager()
	restored := &Orchestrator{cfg: Config{LogDir: logDir}, agents: restoredAgents}
	if err := restored.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}

	loaded, ok := restoredAgents.Get(agent.ID)
	if !ok {
		t.Fatalf("restored agent %s not found", agent.ID)
	}
	if loaded.PRNumber != agent.PRNumber {
		t.Fatalf("loaded.PRNumber = %d, want %d", loaded.PRNumber, agent.PRNumber)
	}
	if loaded.RuntimeHandle.Session != agent.RuntimeHandle.Session {
		t.Fatalf("loaded runtime session = %q, want %q", loaded.RuntimeHandle.Session, agent.RuntimeHandle.Session)
	}
	if loaded.RuntimeProfile != agent.RuntimeProfile {
		t.Fatalf("loaded runtime profile = %+v, want %+v", loaded.RuntimeProfile, agent.RuntimeProfile)
	}
	if !loaded.AdoptedPR || loaded.PRHeadBranch != "feature/handle-retries" {
		t.Fatalf("loaded adopted PR metadata = (adopted=%v, head=%q)", loaded.AdoptedPR, loaded.PRHeadBranch)
	}
	if loaded.LastConflictHeadSHA != "abc123" {
		t.Fatalf("loaded.LastConflictHeadSHA = %q, want %q", loaded.LastConflictHeadSHA, "abc123")
	}
	if !reflect.DeepEqual(loaded.LaunchAttempts, agent.LaunchAttempts) {
		t.Fatalf("loaded.LaunchAttempts = %#v, want %#v", loaded.LaunchAttempts, agent.LaunchAttempts)
	}
	if got := strings.Join(loaded.HumanReviewGuidance, " | "); got != "Backward compatibility is explicitly out of scope for this PR." {
		t.Fatalf("loaded.HumanReviewGuidance = %q, want persisted guidance", got)
	}
	if !restoredAgents.LastPoll().Equal(lastPoll) {
		t.Fatalf("restored LastPoll() = %s, want %s", restoredAgents.LastPoll().Format(time.RFC3339), lastPoll.Format(time.RFC3339))
	}
	if got := restoredAgents.PendingReviewCommentCount(agent.ID); got != 1 {
		t.Fatalf("PendingReviewCommentCount() = %d, want 1", got)
	}
	if got := restoredAgents.MarkReviewCommentSeen(agent.ID, 101); got {
		t.Fatal("MarkReviewCommentSeen(existing) = true, want false")
	}
	if got := restoredAgents.MarkIssueCommentSeen(agent.ID, 202); got {
		t.Fatal("MarkIssueCommentSeen(existing) = true, want false")
	}
}

func TestToPersistedAgentNormalizesLastActivityTimeToUTC(t *testing.T) {
	offset := time.FixedZone("test-offset", -7*60*60)
	lastActivity := time.Date(2026, time.August, 13, 9, 0, 0, 0, offset)

	persisted := toPersistedAgent(&Agent{LastActivityTime: lastActivity})
	if persisted.LastActivityTime.Location() != time.UTC {
		t.Fatalf("LastActivityTime location = %s, want UTC", persisted.LastActivityTime.Location())
	}
	if !persisted.LastActivityTime.Equal(lastActivity) {
		t.Fatalf("LastActivityTime = %s, want instant %s", persisted.LastActivityTime, lastActivity)
	}
}

func TestLoadPersistedAgentStateRestoresMissingEnabledRuntimeProfile(t *testing.T) {
	logDir := t.TempDir()
	payload := persistedStateFile{
		Version: persistedStateVersion,
		Agents: []persistedAgent{
			{
				ID:          "legacy-coder",
				Role:        RoleCoder,
				IssueNumber: 12,
				State:       StateWorking,
				RuntimeHandle: RuntimeHandle{
					Kind: RuntimeKindTmux, Session: "legacy-session",
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	stateDir := filepath.Join(logDir, orchestratorStateDirName)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", stateDir, err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, orchestratorStateFileName), body, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	agents := NewAgentManager()
	bot := &Orchestrator{
		cfg:    Config{LogDir: logDir, ReviewPolicy: builtInReviewPolicy()},
		agents: agents,
	}
	if err := bot.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	loaded, ok := agents.Get("legacy-coder")
	if !ok {
		t.Fatal("legacy-coder was not restored")
	}
	want := builtInAgentProfile(AgentProfileRoleCoder)
	if loaded.RuntimeProfile != want {
		t.Fatalf("restored runtime profile = %+v, want %+v", loaded.RuntimeProfile, want)
	}
}

func TestLoadPersistedAgentStateRestoresMissingRuntimeProfilesFromCLIOverride(t *testing.T) {
	setRequiredEnv(t)
	override := agentProfileOverride{
		Model:              "gpt-5.6-sol",
		ReasoningEffort:    "xhigh",
		HasModel:           true,
		HasReasoningEffort: true,
	}
	configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(""))
	cfg, err := loadConfigWithRuntimeProfileOverride(configPath, override)
	if err != nil {
		t.Fatalf("loadConfigWithRuntimeProfileOverride() error = %v", err)
	}
	policy := cfg.ReviewPolicy
	if policy.runtimeProfileCLIOverride != (agentProfileOverride{}) {
		t.Fatalf("runtimeProfileCLIOverride = %+v, want empty after materialization", policy.runtimeProfileCLIOverride)
	}

	tests := []struct {
		name        string
		role        AgentRole
		profileRole AgentProfileRole
	}{
		{name: "coder", role: RoleCoder, profileRole: AgentProfileRoleCoder},
		{name: "indexer", role: RoleIndexer, profileRole: AgentProfileRoleIndexer},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logDir := t.TempDir()
			agentID := "legacy-" + test.name
			payload := persistedStateFile{
				Version: persistedStateVersion,
				Agents: []persistedAgent{
					{
						ID:    agentID,
						Role:  test.role,
						State: StateWorking,
						RuntimeHandle: RuntimeHandle{
							Kind: RuntimeKindTmux, Session: "legacy-session",
						},
					},
				},
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			stateDir := filepath.Join(logDir, orchestratorStateDirName)
			if err := os.MkdirAll(stateDir, 0o755); err != nil {
				t.Fatalf("MkdirAll(%s) error = %v", stateDir, err)
			}
			if err := os.WriteFile(filepath.Join(stateDir, orchestratorStateFileName), body, 0o644); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			agents := NewAgentManager()
			restartConfig := cfg
			restartConfig.LogDir = logDir
			bot := &Orchestrator{
				cfg:    restartConfig,
				agents: agents,
			}
			if err := bot.loadPersistedAgentState(); err != nil {
				t.Fatalf("loadPersistedAgentState() error = %v", err)
			}
			loaded, ok := agents.Get(agentID)
			if !ok {
				t.Fatalf("%s was not restored", agentID)
			}
			want, err := policy.effectiveProfileForRole(test.profileRole)
			if err != nil {
				t.Fatalf("effectiveProfileForRole(%q) error = %v", test.profileRole, err)
			}
			if loaded.RuntimeProfile != want {
				t.Fatalf("restored runtime profile = %+v, want materialized CLI profile %+v", loaded.RuntimeProfile, want)
			}
		})
	}
}

func TestLoadPersistedAgentStateRejectsProfileOutsideActiveModelCatalog(t *testing.T) {
	tests := []struct {
		name      string
		profile   AgentProfile
		configure func(*ReviewPolicy)
		wantError string
	}{
		{
			name:      "unknown model",
			profile:   AgentProfile{Name: "coder", Model: "removed-model", ReasoningEffort: "high"},
			configure: func(*ReviewPolicy) {},
			wantError: "is not present in REVIEW_POLICY.MODEL_CATALOG",
		},
		{
			name:    "unavailable model",
			profile: AgentProfile{Name: "coder", Model: "offline-model", ReasoningEffort: "high"},
			configure: func(policy *ReviewPolicy) {
				policy.ModelCatalog["offline-model"] = ModelCapability{
					ReasoningEfforts: []string{"high"},
					Available:        false,
				}
			},
			wantError: "is configured as unavailable",
		},
		{
			name:    "unsupported effort",
			profile: AgentProfile{Name: "coder", Model: "limited-model", ReasoningEffort: "xhigh"},
			configure: func(policy *ReviewPolicy) {
				policy.ModelCatalog["limited-model"] = ModelCapability{
					ReasoningEfforts: []string{"low", "high"},
					Available:        true,
				}
			},
			wantError: `does not support reasoning effort "xhigh"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logDir := t.TempDir()
			payload := persistedStateFile{
				Version: persistedStateVersion,
				Agents: []persistedAgent{
					{
						ID:             "persisted-coder",
						Role:           RoleCoder,
						IssueNumber:    12,
						State:          StateWorking,
						RuntimeProfile: test.profile,
						RuntimeHandle: RuntimeHandle{
							Kind: RuntimeKindTmux, Session: "persisted-session",
						},
					},
				},
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			stateDir := filepath.Join(logDir, orchestratorStateDirName)
			if err := os.MkdirAll(stateDir, 0o755); err != nil {
				t.Fatalf("MkdirAll(%s) error = %v", stateDir, err)
			}
			if err := os.WriteFile(filepath.Join(stateDir, orchestratorStateFileName), body, 0o644); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}

			policy := builtInReviewPolicy()
			test.configure(&policy)
			bot := &Orchestrator{
				cfg:    Config{LogDir: logDir, ReviewPolicy: policy},
				agents: NewAgentManager(),
			}
			err = bot.loadPersistedAgentState()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("loadPersistedAgentState() error = %v, want containing %q", err, test.wantError)
			}
			if !strings.Contains(err.Error(), "persisted runtime profile for agent persisted-coder") {
				t.Fatalf("loadPersistedAgentState() error = %v, want persisted-agent context", err)
			}
		})
	}
}

func TestLoadPersistedAgentStateMissingFileIsNoop(t *testing.T) {
	bot := &Orchestrator{cfg: Config{LogDir: t.TempDir()}, agents: NewAgentManager()}
	if err := bot.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	if got := len(bot.agents.List()); got != 0 {
		t.Fatalf("len(agents.List()) = %d, want 0", got)
	}
}

func TestLoadPersistedAgentStateUnsupportedVersionRequiresClean(t *testing.T) {
	logDir := t.TempDir()
	payload := persistedStateFile{
		Version: persistedStateVersion + 1,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	stateDir := filepath.Join(logDir, orchestratorStateDirName)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", stateDir, err)
	}
	stateFile := filepath.Join(stateDir, orchestratorStateFileName)
	if err := os.WriteFile(stateFile, body, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", stateFile, err)
	}

	bot := &Orchestrator{cfg: Config{LogDir: logDir}, agents: NewAgentManager()}
	err = bot.loadPersistedAgentState()
	if err == nil || !strings.Contains(err.Error(), "rerun with --clean") {
		t.Fatalf("loadPersistedAgentState() error = %v, want unsupported-version --clean guidance", err)
	}
}

func TestPersistAgentStateKeepsStoppedAgentsForCleanup(t *testing.T) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	now := time.Unix(1700000000, 0).UTC()

	for _, agent := range []*Agent{
		{
			ID:                      "agent-approved",
			IssueNumber:             1,
			BranchName:              "repository-agent-orchestrator/issue-1",
			State:                   StateApproved,
			LastActivityTime:        now,
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		},
		{
			ID:                      "agent-done",
			IssueNumber:             2,
			BranchName:              "repository-agent-orchestrator/issue-2",
			State:                   StateDone,
			Stopped:                 true,
			LastActivityTime:        now,
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		},
		{
			ID:                      "agent-errored",
			IssueNumber:             3,
			BranchName:              "repository-agent-orchestrator/issue-3",
			State:                   StateErrored,
			LastActivityTime:        now,
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		},
		{
			ID:                      "agent-stopped",
			IssueNumber:             4,
			BranchName:              "repository-agent-orchestrator/issue-4",
			State:                   StateStopped,
			Stopped:                 true,
			LastActivityTime:        now,
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		},
		{
			ID:                      "agent-working",
			IssueNumber:             5,
			BranchName:              "repository-agent-orchestrator/issue-5",
			State:                   StateWorking,
			LastActivityTime:        now,
			seenReviewCommentIDs:    make(map[int64]struct{}),
			seenIssueCommentIDs:     make(map[int64]struct{}),
			pendingReviewCommentIDs: make(map[int64]struct{}),
		},
	} {
		if err := agents.Add(agent); err != nil {
			t.Fatalf("Add(%s) error = %v", agent.ID, err)
		}
	}

	bot := &Orchestrator{cfg: Config{LogDir: logDir}, agents: agents}
	if err := bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}

	stateFile := filepath.Join(logDir, orchestratorStateDirName, orchestratorStateFileName)
	body, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", stateFile, err)
	}

	var payload persistedStateFile
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(payload.Agents) != 3 {
		t.Fatalf("len(payload.Agents) = %d, want 3", len(payload.Agents))
	}
	gotIDs := []string{payload.Agents[0].ID, payload.Agents[1].ID, payload.Agents[2].ID}
	wantIDs := []string{"agent-approved", "agent-stopped", "agent-working"}
	if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
		t.Fatalf("persisted agent ids = %v, want %v", gotIDs, wantIDs)
	}
}

func TestLoadPersistedAgentStateRestoresStoppedAgentsForCleanup(t *testing.T) {
	logDir := t.TempDir()
	lastPoll := time.Unix(1700001000, 0).UTC()
	payload := persistedStateFile{
		Version:     persistedStateVersion,
		LastSavedAt: time.Unix(1700002000, 0).UTC(),
		LastPoll:    lastPoll,
		Agents: []persistedAgent{
			{ID: "agent-working", State: StateWorking, IssueNumber: 1, BranchName: "repository-agent-orchestrator/issue-1"},
			{ID: "agent-approved", State: StateApproved, IssueNumber: 2, BranchName: "repository-agent-orchestrator/issue-2"},
			{ID: "agent-done", State: StateDone, Stopped: true, IssueNumber: 3, BranchName: "repository-agent-orchestrator/issue-3"},
			{ID: "agent-errored", State: StateErrored, IssueNumber: 4, BranchName: "repository-agent-orchestrator/issue-4"},
			{ID: "agent-stopped", State: StateStopped, Stopped: true, IssueNumber: 5, BranchName: "repository-agent-orchestrator/issue-5"},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	stateDir := filepath.Join(logDir, orchestratorStateDirName)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", stateDir, err)
	}
	stateFile := filepath.Join(stateDir, orchestratorStateFileName)
	if err := os.WriteFile(stateFile, body, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", stateFile, err)
	}

	restoredAgents := NewAgentManager()
	restored := &Orchestrator{cfg: Config{LogDir: logDir}, agents: restoredAgents}
	if err := restored.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}

	if !restoredAgents.LastPoll().Equal(lastPoll) {
		t.Fatalf("restored LastPoll() = %s, want %s", restoredAgents.LastPoll().Format(time.RFC3339), lastPoll.Format(time.RFC3339))
	}
	if got := len(restoredAgents.List()); got != 3 {
		t.Fatalf("len(restoredAgents.List()) = %d, want 3", got)
	}
	for _, agentID := range []string{"agent-working", "agent-approved", "agent-stopped"} {
		if _, ok := restoredAgents.Get(agentID); !ok {
			t.Fatalf("Get(%s) = not found", agentID)
		}
	}
	for _, agentID := range []string{"agent-done", "agent-errored"} {
		if _, ok := restoredAgents.Get(agentID); ok {
			t.Fatalf("Get(%s) = found, want filtered legacy exited agent", agentID)
		}
	}
	cleanupTarget, err := restoredAgents.ResolveIssueAgentForCleanup(5, RoleCoder)
	if err != nil {
		t.Fatalf("ResolveIssueAgentForCleanup() error = %v", err)
	}
	if cleanupTarget.ID != "agent-stopped" {
		t.Fatalf("cleanup target ID = %q, want %q", cleanupTarget.ID, "agent-stopped")
	}
}

func TestLoadPersistedAgentStateFailsClosedWhenLaunchReconciliationFails(
	t *testing.T,
) {
	logDir := t.TempDir()
	worktree := t.TempDir()
	payload := persistedStateFile{
		Version: persistedStateVersion,
		Agents: []persistedAgent{
			{
				ID:           "coder-unreconciled",
				Role:         RoleCoder,
				State:        StateInitializing,
				WorktreePath: worktree,
				LogDir:       logDir,
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	stateDir := filepath.Join(logDir, orchestratorStateDirName)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(state) error = %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(stateDir, orchestratorStateFileName),
		body,
		0o644,
	); err != nil {
		t.Fatalf("WriteFile(state) error = %v", err)
	}

	runner := &stubRunner{
		reconcileErr: errors.New("injected reconciliation failure"),
	}
	cleanupCalled := false
	bot := &Orchestrator{
		cfg: Config{
			LogDir:   logDir,
			RepoPath: t.TempDir(),
		},
		agents: NewAgentManager(),
		runner: runner,
		cleanupWorktreeFunc: func(
			context.Context,
			string,
			string,
			string,
		) error {
			cleanupCalled = true
			return nil
		},
	}
	err = bot.loadPersistedAgentState()
	if err == nil || !strings.Contains(err.Error(), "failed to reconcile") {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	if cleanupCalled {
		t.Fatal("worktree cleanup ran before runtime reconciliation succeeded")
	}
	if len(runner.runtimeRemoved) != 0 {
		t.Fatalf(
			"runtime state cleanup ran after failed reconciliation: %#v",
			runner.runtimeRemoved,
		)
	}
	if _, err := os.Stat(worktree); err != nil {
		t.Fatalf("owned worktree was removed after failed reconciliation: %v", err)
	}
}
