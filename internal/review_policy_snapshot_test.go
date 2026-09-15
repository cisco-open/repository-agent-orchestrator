// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2026 Cisco Systems, Inc. and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testReviewHeadSHA       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testOtherReviewHeadSHA  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testThirdReviewHeadSHA  = "cccccccccccccccccccccccccccccccccccccccc"
	testFourthReviewHeadSHA = "dddddddddddddddddddddddddddddddddddddddd"
	testFifthReviewHeadSHA  = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

func TestEquivalentEffectiveReviewPoliciesHaveStableFingerprints(t *testing.T) {
	first := builtInReviewPolicy()
	second := builtInReviewPolicy()

	second.AgentProfiles = map[string]AgentProfile{}
	for i := len(requiredAgentProfileRoles) - 1; i >= 0; i-- {
		profile := builtInAgentProfile(requiredAgentProfileRoles[i])
		second.AgentProfiles[profile.Name] = profile
	}
	second.RoleProfiles = map[AgentProfileRole]string{}
	for i := len(requiredAgentProfileRoles) - 1; i >= 0; i-- {
		role := requiredAgentProfileRoles[i]
		second.RoleProfiles[role] = string(role)
	}
	for model, capability := range second.ModelCatalog {
		for left, right := 0, len(capability.ReasoningEfforts)-1; left < right; left, right = left+1, right-1 {
			capability.ReasoningEfforts[left], capability.ReasoningEfforts[right] =
				capability.ReasoningEfforts[right], capability.ReasoningEfforts[left]
		}
		second.ModelCatalog[model] = capability
	}
	firstSnapshot, err := snapshotEffectiveReviewPolicy(first)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(first) error = %v", err)
	}
	secondSnapshot, err := snapshotEffectiveReviewPolicy(second)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(second) error = %v", err)
	}
	if firstSnapshot.Fingerprint == "" {
		t.Fatal("effective review policy fingerprint is empty")
	}
	if got, want := secondSnapshot.Fingerprint, firstSnapshot.Fingerprint; got != want {
		t.Fatalf("equivalent policy fingerprint = %q, want %q", got, want)
	}
}

func TestLoadConfigFingerprintsEffectiveReviewPolicy(t *testing.T) {
	// Runner.Start can crash after creating tmux, while waiting for Codex, or
	// after prompt delivery but before the handle is persisted. All three
	// windows intentionally restore the same handle-less checkpoint, so each
	// live-session case must stop the ambiguous session before relaunch.
	tests := []struct {
		name   string
		policy string
	}{
		{name: "omitted policy"},
		{name: "configured policy", policy: validReviewPolicyConfig()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(test.policy))
			cfg, err := loadConfig(configPath)
			if err != nil {
				t.Fatalf("loadConfig() error = %v", err)
			}
			if err := validateReviewPolicyFingerprint(cfg.ReviewPolicy.Fingerprint); err != nil {
				t.Fatalf("loaded policy fingerprint is invalid: %v", err)
			}
			recomputed, err := snapshotEffectiveReviewPolicy(cfg.ReviewPolicy)
			if err != nil {
				t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
			}
			if got, want := recomputed.Fingerprint, cfg.ReviewPolicy.Fingerprint; got != want {
				t.Fatalf("recomputed fingerprint = %q, want loaded fingerprint %q", got, want)
			}
		})
	}
}

func TestEffectiveReviewPolicyFingerprintIncludesResolvedCLIOverrides(t *testing.T) {
	base := builtInReviewPolicy()
	withoutOverride, err := snapshotEffectiveReviewPolicy(base)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(base) error = %v", err)
	}

	base.runtimeProfileCLIOverride = agentProfileOverride{
		Model:              "gpt-5.6-terra",
		ReasoningEffort:    "low",
		HasModel:           true,
		HasReasoningEffort: true,
	}
	withOverride, err := snapshotEffectiveReviewPolicy(base)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(override) error = %v", err)
	}
	if withOverride.Fingerprint == withoutOverride.Fingerprint {
		t.Fatal("effective CLI override did not change the policy fingerprint")
	}
	for _, role := range requiredAgentProfileRoles {
		profile, err := withOverride.effectiveProfileForRole(role)
		if err != nil {
			t.Fatalf("effectiveProfileForRole(%q) error = %v", role, err)
		}
		if profile.Model != "gpt-5.6-terra" || profile.ReasoningEffort != "low" {
			t.Fatalf("effective profile for %q = %+v, want materialized CLI override", role, profile)
		}
	}
}

func TestReviewCyclePolicySnapshotIsImmutableFromConfigChanges(t *testing.T) {
	policy := builtInReviewPolicy()
	cycle, err := newReviewCycleState(strings.ToUpper(testReviewHeadSHA), policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	if got := cycle.HeadSHA; got != testReviewHeadSHA {
		t.Fatalf("canonical review head = %q, want %q", got, testReviewHeadSHA)
	}
	originalFingerprint := cycle.PolicyFingerprint
	originalMaxRounds := cycle.Policy.Convergence.MaxRounds
	originalLane := cycle.Policy.Swarm.Lanes[0]
	originalProfile := cycle.Policy.AgentProfiles[string(AgentProfileRoleDiscovery)]

	policy.Convergence.MaxRounds++
	policy.Swarm.Lanes[0].Required = !policy.Swarm.Lanes[0].Required
	profile := policy.AgentProfiles[string(AgentProfileRoleDiscovery)]
	profile.Model = "changed-model"
	policy.AgentProfiles[string(AgentProfileRoleDiscovery)] = profile

	if cycle.PolicyFingerprint != originalFingerprint {
		t.Fatal("config mutation changed the in-flight cycle fingerprint")
	}
	if cycle.Policy.Convergence.MaxRounds != originalMaxRounds {
		t.Fatal("config mutation changed the in-flight convergence snapshot")
	}
	if cycle.Policy.Swarm.Lanes[0] != originalLane {
		t.Fatal("config mutation changed the in-flight lane snapshot")
	}
	if cycle.Policy.AgentProfiles[string(AgentProfileRoleDiscovery)] != originalProfile {
		t.Fatal("config mutation changed the in-flight profile snapshot")
	}
	if err := validatePersistedReviewCycleSnapshot(cycle); err != nil {
		t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
	}

	clone := cloneReviewCycle(cycle)
	clone.Policy.Swarm.Lanes[0].Profile = "mutated-clone"
	if cycle.Policy.Swarm.Lanes[0].Profile == "mutated-clone" {
		t.Fatal("cloneReviewCycle() shared mutable lane storage")
	}
}

func TestNewReviewCycleStateRejectsMissingAndMalformedHeads(t *testing.T) {
	tests := []struct {
		name      string
		headSHA   string
		wantError string
	}{
		{name: "missing", wantError: "exact head SHA is missing"},
		{name: "abbreviated", headSHA: "aaaaaaaaaaaa", wantError: "canonical 40-character"},
		{name: "non-hexadecimal", headSHA: strings.Repeat("g", canonicalGitObjectIDLength), wantError: "canonical 40-character"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newReviewCycleState(test.headSHA, builtInReviewPolicy()); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("newReviewCycleState(%q) error = %v, want containing %q", test.headSHA, err, test.wantError)
			}
		})
	}
}

func TestPersistedReviewCycleRecoversSnapshotAcrossConfigChange(t *testing.T) {
	logDir := t.TempDir()
	launchPolicyConfig := builtInReviewPolicy()
	snapshotProfile := launchPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)]
	snapshotProfile.Model = "snapshot-only-model"
	snapshotProfile.ReasoningEffort = "ultra"
	launchPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)] = snapshotProfile
	launchPolicyConfig.ModelCatalog[snapshotProfile.Model] = ModelCapability{
		ReasoningEfforts: []string{snapshotProfile.ReasoningEffort},
		Available:        true,
	}
	launchPolicy, err := snapshotEffectiveReviewPolicy(launchPolicyConfig)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(launch) error = %v", err)
	}
	cycle, err := newReviewCycleState(testReviewHeadSHA, launchPolicy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                "review-agent-policy-recovery",
		Role:              RoleReviewer,
		ObservedPRHeadSHA: testReviewHeadSHA,
		ReviewCycle:       cycle,
		RuntimeHandle:     RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-policy-session"},
		RuntimeProfile:    snapshotProfile,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	launched := &Orchestrator{cfg: Config{LogDir: logDir, ReviewPolicy: launchPolicy}, agents: agents}
	if err := launched.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}

	restartedPolicy := builtInReviewPolicy()
	restartedPolicy.Convergence.MaxWallTimeMinutes = launchPolicy.Convergence.MaxWallTimeMinutes + 30
	restartedPolicy, err = snapshotEffectiveReviewPolicy(restartedPolicy)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(restarted) error = %v", err)
	}
	if restartedPolicy.Fingerprint == launchPolicy.Fingerprint {
		t.Fatal("test setup did not change the active config fingerprint")
	}

	restored := &Orchestrator{
		cfg:    Config{LogDir: logDir, ReviewPolicy: restartedPolicy},
		agents: NewAgentManager(),
	}
	if err := restored.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	loaded, ok := restored.agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("restored reviewer %s not found", reviewer.ID)
	}
	if got, want := loaded.ReviewCycle.PolicyFingerprint, launchPolicy.Fingerprint; got != want {
		t.Fatalf("restored policy fingerprint = %q, want launch fingerprint %q", got, want)
	}
	if got, want := loaded.ReviewCycle.Policy.Convergence.MaxWallTimeMinutes, launchPolicy.Convergence.MaxWallTimeMinutes; got != want {
		t.Fatalf("restored wall-time policy = %d, want launch value %d", got, want)
	}
	if got := loaded.RuntimeProfile; got != snapshotProfile {
		t.Fatalf("restored runtime profile = %+v, want snapshotted profile %+v", got, snapshotProfile)
	}
}

func TestLoadPersistedReviewCycleRejectsMissingAndMismatchedPolicyState(t *testing.T) {
	policy, err := snapshotEffectiveReviewPolicy(builtInReviewPolicy())
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
	}
	validCycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}

	tests := []struct {
		name         string
		observedHead string
		cycle        func() *ReviewCycleState
		wantError    string
	}{
		{
			name:      "missing reviewer head and snapshot",
			cycle:     func() *ReviewCycleState { return nil },
			wantError: "missing its exact head SHA",
		},
		{
			name:         "abbreviated reviewer head",
			observedHead: "aaaaaaaaaaaa",
			cycle:        func() *ReviewCycleState { return cloneReviewCycle(validCycle) },
			wantError:    "invalid exact head SHA",
		},
		{
			name:         "malformed reviewer head",
			observedHead: strings.Repeat("g", canonicalGitObjectIDLength),
			cycle:        func() *ReviewCycleState { return cloneReviewCycle(validCycle) },
			wantError:    "invalid exact head SHA",
		},
		{
			name:         "non-canonical reviewer head",
			observedHead: strings.ToUpper(testReviewHeadSHA),
			cycle:        func() *ReviewCycleState { return cloneReviewCycle(validCycle) },
			wantError:    "invalid exact head SHA",
		},
		{
			name:         "missing snapshot",
			observedHead: testReviewHeadSHA,
			cycle:        func() *ReviewCycleState { return nil },
			wantError:    "missing its review policy snapshot",
		},
		{
			name:         "abbreviated snapshot head",
			observedHead: testReviewHeadSHA,
			cycle: func() *ReviewCycleState {
				cycle := cloneReviewCycle(validCycle)
				cycle.HeadSHA = "aaaaaaaaaaaa"
				return cycle
			},
			wantError: "exact head SHA is invalid",
		},
		{
			name:         "malformed snapshot head",
			observedHead: testReviewHeadSHA,
			cycle: func() *ReviewCycleState {
				cycle := cloneReviewCycle(validCycle)
				cycle.HeadSHA = strings.Repeat("g", canonicalGitObjectIDLength)
				return cycle
			},
			wantError: "exact head SHA is invalid",
		},
		{
			name:         "version mismatch",
			observedHead: testReviewHeadSHA,
			cycle: func() *ReviewCycleState {
				cycle := cloneReviewCycle(validCycle)
				cycle.PolicyVersion++
				return cycle
			},
			wantError: "policy version",
		},
		{
			name:         "fingerprint mismatch",
			observedHead: testReviewHeadSHA,
			cycle: func() *ReviewCycleState {
				cycle := cloneReviewCycle(validCycle)
				cycle.Policy.Convergence.MaxRounds++
				return cycle
			},
			wantError: "fingerprint mismatch",
		},
		{
			name:         "head mismatch",
			observedHead: testReviewHeadSHA,
			cycle: func() *ReviewCycleState {
				cycle := cloneReviewCycle(validCycle)
				cycle.HeadSHA = testOtherReviewHeadSHA
				return cycle
			},
			wantError: "snapshot head mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logDir := t.TempDir()
			writeReviewPolicyStateFixture(t, logDir, persistedAgent{
				ID:                "persisted-reviewer",
				Role:              RoleReviewer,
				ObservedPRHeadSHA: test.observedHead,
				ReviewCycle:       test.cycle(),
				RuntimeHandle:     RuntimeHandle{Kind: RuntimeKindTmux, Session: "persisted-review-session"},
				RuntimeProfile:    builtInAgentProfile(AgentProfileRoleChallenge),
				State:             StateWorking,
			})
			bot := &Orchestrator{
				cfg:    Config{LogDir: logDir, ReviewPolicy: policy},
				agents: NewAgentManager(),
			}
			err := bot.loadPersistedAgentState()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("loadPersistedAgentState() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestPersistedReviewCycleRuntimeProfileUsesSnapshotAcrossConfigChanges(t *testing.T) {
	launchPolicyConfig := builtInReviewPolicy()
	snapshotProfile := launchPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)]
	snapshotProfile.Model = "snapshot-only-model"
	snapshotProfile.ReasoningEffort = "ultra"
	launchPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)] = snapshotProfile
	launchPolicyConfig.ModelCatalog[snapshotProfile.Model] = ModelCapability{
		ReasoningEfforts: []string{snapshotProfile.ReasoningEffort},
		Available:        true,
	}
	launchPolicy, err := snapshotEffectiveReviewPolicy(launchPolicyConfig)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(launch) error = %v", err)
	}
	cycle, err := newReviewCycleState(testReviewHeadSHA, launchPolicy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}

	restartedPolicyConfig := builtInReviewPolicy()
	currentOnlyProfile := restartedPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)]
	currentOnlyProfile.Model = "current-only-model"
	currentOnlyProfile.ReasoningEffort = "max"
	restartedPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)] = currentOnlyProfile
	restartedPolicyConfig.ModelCatalog[currentOnlyProfile.Model] = ModelCapability{
		ReasoningEfforts: []string{currentOnlyProfile.ReasoningEffort},
		Available:        true,
	}
	restartedPolicy, err := snapshotEffectiveReviewPolicy(restartedPolicyConfig)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(restarted) error = %v", err)
	}
	currentProfile, err := restartedPolicy.effectiveProfileForRole(AgentProfileRoleChallenge)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(restarted challenge) error = %v", err)
	}
	if currentProfile == snapshotProfile {
		t.Fatal("test setup did not change the challenge profile")
	}
	if currentProfile != currentOnlyProfile {
		t.Fatalf("current challenge profile = %+v, want %+v", currentProfile, currentOnlyProfile)
	}
	if _, ok := restartedPolicy.ModelCatalog[snapshotProfile.Model]; ok {
		t.Fatal("test setup unexpectedly retained the snapshotted model in the restarted catalog")
	}
	differentSnapshotValidProfile := builtInAgentProfile(AgentProfileRoleChallenge)

	tests := []struct {
		name        string
		profile     AgentProfile
		wantProfile AgentProfile
		wantError   string
	}{
		{
			name:        "missing profile restored from snapshot",
			wantProfile: snapshotProfile,
		},
		{
			name:      "valid current profile rejected when snapshot differs",
			profile:   differentSnapshotValidProfile,
			wantError: "does not match its snapshotted challenge profile",
		},
		{
			name:      "current-only profile rejected by snapshot catalog",
			profile:   currentProfile,
			wantError: "snapshotted REVIEW_POLICY.MODEL_CATALOG",
		},
		{
			name:        "matching snapshot profile ignores current catalog",
			profile:     snapshotProfile,
			wantProfile: snapshotProfile,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logDir := t.TempDir()
			writeReviewPolicyStateFixture(t, logDir, persistedAgent{
				ID:                "persisted-reviewer",
				Role:              RoleReviewer,
				ObservedPRHeadSHA: testReviewHeadSHA,
				ReviewCycle:       cloneReviewCycle(cycle),
				RuntimeHandle:     RuntimeHandle{Kind: RuntimeKindTmux, Session: "persisted-review-session"},
				RuntimeProfile:    test.profile,
				State:             StateWorking,
			})
			bot := &Orchestrator{
				cfg:    Config{LogDir: logDir, ReviewPolicy: restartedPolicy},
				agents: NewAgentManager(),
			}
			err := bot.loadPersistedAgentState()
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("loadPersistedAgentState() error = %v, want containing %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadPersistedAgentState() error = %v", err)
			}
			loaded, ok := bot.agents.Get("persisted-reviewer")
			if !ok {
				t.Fatal("persisted reviewer was not restored")
			}
			if got := loaded.RuntimeProfile; got != test.wantProfile {
				t.Fatalf("restored runtime profile = %+v, want snapshotted profile %+v", got, test.wantProfile)
			}
		})
	}
}

func TestPersistedReviewLaunchRecoversSnapshotAcrossConfigChanges(t *testing.T) {
	launchPolicyConfig := builtInReviewPolicy()
	snapshotProfile := launchPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)]
	snapshotProfile.Model = "launch-only-model"
	snapshotProfile.ReasoningEffort = "ultra"
	launchPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)] = snapshotProfile
	launchPolicyConfig.ModelCatalog[snapshotProfile.Model] = ModelCapability{
		ReasoningEfforts: []string{snapshotProfile.ReasoningEffort},
		Available:        true,
	}
	launchPolicy, err := snapshotEffectiveReviewPolicy(launchPolicyConfig)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(launch) error = %v", err)
	}
	cycle, err := newReviewCycleState(testReviewHeadSHA, launchPolicy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}

	restartedPolicyConfig := builtInReviewPolicy()
	restartedProfile := restartedPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)]
	restartedProfile.Model = "restart-only-model"
	restartedProfile.ReasoningEffort = "max"
	restartedPolicyConfig.AgentProfiles[string(AgentProfileRoleChallenge)] = restartedProfile
	restartedPolicyConfig.ModelCatalog[restartedProfile.Model] = ModelCapability{
		ReasoningEfforts: []string{restartedProfile.ReasoningEffort},
		Available:        true,
	}
	restartedPolicy, err := snapshotEffectiveReviewPolicy(restartedPolicyConfig)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy(restarted) error = %v", err)
	}
	if launchPolicy.Fingerprint == restartedPolicy.Fingerprint {
		t.Fatal("test setup did not change the effective policy")
	}

	tests := []struct {
		name         string
		state        AgentState
		wantGateRuns int
	}{
		{
			name:         "initializing",
			state:        StateInitializing,
			wantGateRuns: 1,
		},
		{
			name:         "review_gate",
			state:        StateReviewGate,
			wantGateRuns: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logDir := t.TempDir()
			worktreeDir := t.TempDir()
			repoPath := t.TempDir()
			reviewerID := "review-agent-54-1700000000123456789"
			coderID := "coding-agent-restart-window"
			reviewWorktree := filepath.Join(worktreeDir, "review-restart-window")
			writeReviewPolicyStateFixture(
				t,
				logDir,
				persistedAgent{
					ID:                  coderID,
					Role:                RoleCoder,
					IssueNumber:         54,
					BranchName:          "repository-agent-orchestrator/issue-54",
					PRNumber:            54,
					PRURL:               "https://github.com/acme/widget/pull/54",
					ObservedPRHeadSHA:   testReviewHeadSHA,
					ActiveReviewAgentID: reviewerID,
					State:               StateWaiting,
				},
				persistedAgent{
					ID:                 reviewerID,
					Role:               RoleReviewer,
					ParentAgentID:      coderID,
					IssueNumber:        54,
					IssueTitle:         "Recover immutable launch",
					WorktreePath:       reviewWorktree,
					LogDir:             logDir,
					RuntimeCWD:         reviewWorktree,
					BranchName:         "repository-agent-orchestrator/issue-54",
					PRNumber:           54,
					PRURL:              "https://github.com/acme/widget/pull/54",
					ObservedPRHeadSHA:  testReviewHeadSHA,
					ReviewCycle:        cloneReviewCycle(cycle),
					ReviewSkipHardGate: false,
					State:              test.state,
				},
			)

			runner := &stubRunner{}
			gateRuns := 0
			cleanupRuns := 0
			bot := &Orchestrator{
				cfg: Config{
					RepoOwner:      "acme",
					RepoName:       "widget",
					RepoPath:       repoPath,
					WorktreeDir:    worktreeDir,
					LogDir:         logDir,
					BaseBranch:     "main",
					MandatoryTests: []string{"make test"},
					ReviewPolicy:   restartedPolicy,
				},
				agents: NewAgentManager(),
				runner: runner,
				cmdRunner: func(_ context.Context, _ string, name string, args ...string) error {
					if name == "make" && strings.Join(args, " ") == "test" {
						gateRuns++
					}
					return nil
				},
				cleanupWorktreeFunc: func(context.Context, string, string, string) error {
					cleanupRuns++
					return nil
				},
			}
			if err := bot.loadPersistedAgentState(); err != nil {
				t.Fatalf("loadPersistedAgentState() error = %v", err)
			}
			loaded, ok := bot.agents.Get(reviewerID)
			if !ok {
				t.Fatal("checkpointed launch reviewer was filtered during restart")
			}
			if got := loaded.RuntimeProfile; got != snapshotProfile {
				t.Fatalf("restored runtime profile = %+v, want launch snapshot %+v", got, snapshotProfile)
			}
			if got := loaded.ReviewCycle.PolicyFingerprint; got != launchPolicy.Fingerprint {
				t.Fatalf("restored fingerprint = %q, want launch fingerprint %q", got, launchPolicy.Fingerprint)
			}
			if err := bot.recoverCheckpointedReviewLaunch(context.Background(), loaded); err != nil {
				t.Fatalf("recoverCheckpointedReviewLaunch() error = %v", err)
			}
			bot.waitForPersistedReviewCycleRecoveries()
			if got := len(runner.started); got != 0 {
				t.Fatalf("top-level runner.Start calls = %d, want 0", got)
			}
			if gateRuns != test.wantGateRuns {
				t.Fatalf("mandatory gate runs = %d, want %d", gateRuns, test.wantGateRuns)
			}
			if cleanupRuns != 1 {
				t.Fatalf("worktree cleanup calls = %d, want 1 before launch retry", cleanupRuns)
			}
			recovered, ok := bot.agents.Get(reviewerID)
			if !ok {
				t.Fatal("reviewer missing after launch recovery")
			}
			if recovered.State != StateWorking {
				t.Fatalf("recovered reviewer state = %s, want %s", recovered.State, StateWorking)
			}
			if recovered.ReviewCycle.PolicyFingerprint != launchPolicy.Fingerprint {
				t.Fatalf(
					"recovered fingerprint = %q, want launch fingerprint %q",
					recovered.ReviewCycle.PolicyFingerprint,
					launchPolicy.Fingerprint,
				)
			}
			if recovered.RuntimeProfile != snapshotProfile {
				t.Fatalf("recovered runtime profile = %+v, want launch snapshot %+v", recovered.RuntimeProfile, snapshotProfile)
			}
			if recovered.RuntimeHandle.Session != "" {
				t.Fatalf(
					"coordinator retained top-level runtime session %q",
					recovered.RuntimeHandle.Session,
				)
			}
			if got := recovered.ReviewCycle.HeadSHA; got != testReviewHeadSHA {
				t.Fatalf(
					"recovered coordinator head = %q, want exact head %q",
					got,
					testReviewHeadSHA,
				)
			}

			coder, ok := bot.agents.Get(coderID)
			if !ok {
				t.Fatal("coder missing after launch recovery")
			}
			if coder.ActiveReviewAgentID != reviewerID {
				t.Fatalf("coder active reviewer = %q, want %q", coder.ActiveReviewAgentID, reviewerID)
			}
		})
	}
}

func TestShutdownCancelsAndJoinsCheckpointedReviewRecovery(t *testing.T) {
	policy, err := snapshotEffectiveReviewPolicy(builtInReviewPolicy())
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
	}
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}

	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	reviewer := &Agent{
		ID:                "review-agent-shutdown-recovery",
		Role:              RoleReviewer,
		IssueNumber:       54,
		WorktreePath:      filepath.Join(worktreeDir, "review-shutdown-recovery"),
		LogDir:            logDir,
		RuntimeCWD:        filepath.Join(worktreeDir, "review-shutdown-recovery"),
		PRNumber:          54,
		PRURL:             "https://github.com/acme/widget/pull/54",
		ObservedPRHeadSHA: testReviewHeadSHA,
		RuntimeProfile:    policy.AgentProfiles[string(AgentProfileRoleChallenge)],
		ReviewCycle:       cycle,
		State:             StateInitializing,
		LastActivityTime:  time.Now(),
	}
	agents := NewAgentManager()
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}

	prepareStarted := make(chan struct{})
	prepareCanceled := make(chan struct{})
	prepareRelease := make(chan struct{}, 1)
	runner := &stubRunner{alive: false, aliveSet: true}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     t.TempDir(),
			WorktreeDir:  worktreeDir,
			LogDir:       logDir,
			BaseBranch:   "main",
			ReviewPolicy: policy,
		},
		agents: agents,
		runner: runner,
		cmdRunner: func(ctx context.Context, _ string, name string, args ...string) error {
			if name != "git" || strings.Join(args, " ") != "worktree add --detach "+reviewer.WorktreePath+" "+testReviewHeadSHA {
				return nil
			}
			close(prepareStarted)
			select {
			case <-ctx.Done():
				close(prepareCanceled)
			case <-time.After(time.Second):
				return errors.New("review preparation did not inherit daemon cancellation")
			}
			<-prepareRelease
			return ctx.Err()
		},
		cleanupWorktreeFunc: func(context.Context, string, string, string) error {
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !bot.queueCheckpointedReviewLaunchRecovery(ctx, *reviewer) {
		t.Fatal("queueCheckpointedReviewLaunchRecovery() = false, want true")
	}
	select {
	case <-prepareStarted:
	case <-time.After(time.Second):
		t.Fatal("checkpointed recovery did not begin worktree preparation")
	}

	cancel()
	select {
	case <-prepareCanceled:
	case <-time.After(time.Second):
		prepareRelease <- struct{}{}
		t.Fatal("checkpointed recovery did not observe daemon cancellation")
	}

	shutdownDone := make(chan struct{})
	go func() {
		bot.Shutdown(ctx)
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		prepareRelease <- struct{}{}
		t.Fatal("Shutdown() returned before checkpointed recovery completed")
	case <-time.After(25 * time.Millisecond):
	}

	prepareRelease <- struct{}{}
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown() did not join canceled checkpointed recovery")
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("runner.Start calls after recovery cancellation = %d, want 0", got)
	}
	if got := len(runner.stopped); got != 0 {
		t.Fatalf("runner.Stop calls after recovery cancellation = %d, want 0 without a launched runtime", got)
	}
	recovered, ok := bot.agents.Get(reviewer.ID)
	if !ok {
		t.Fatal("checkpointed reviewer missing after canceled recovery")
	}
	if recovered.State != StateInitializing {
		t.Fatalf("reviewer state after canceled recovery = %s, want %s", recovered.State, StateInitializing)
	}
	if recovered.RuntimeHandle.Session != "" {
		t.Fatalf("reviewer runtime after canceled recovery = %+v, want empty handle", recovered.RuntimeHandle)
	}
}

func TestLoadPersistedReviewCycleValidatesLaunchCheckpointBeforeFiltering(t *testing.T) {
	policy, err := snapshotEffectiveReviewPolicy(builtInReviewPolicy())
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
	}
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	cycle.Policy.Convergence.MaxRounds++

	for _, state := range []AgentState{StateInitializing, StateReviewGate} {
		t.Run(string(state), func(t *testing.T) {
			logDir := t.TempDir()
			writeReviewPolicyStateFixture(t, logDir, persistedAgent{
				ID:                "checkpointed-launch-reviewer",
				Role:              RoleReviewer,
				ObservedPRHeadSHA: testReviewHeadSHA,
				ReviewCycle:       cloneReviewCycle(cycle),
				State:             state,
			})
			bot := &Orchestrator{
				cfg:    Config{LogDir: logDir, ReviewPolicy: policy},
				agents: NewAgentManager(),
			}
			err := bot.loadPersistedAgentState()
			if err == nil || !strings.Contains(err.Error(), "fingerprint mismatch") {
				t.Fatalf("loadPersistedAgentState() error = %v, want pre-filter fingerprint rejection", err)
			}
		})
	}
}

type policyCheckpointRunner struct {
	statePath string
	starts    int
	checked   bool
}

func (r *policyCheckpointRunner) Start(agent Agent, _ string) (RuntimeHandle, error) {
	r.starts++
	body, err := os.ReadFile(r.statePath)
	if err != nil {
		return RuntimeHandle{}, fmtError("review runtime started before policy state was durable", err)
	}
	var state persistedStateFile
	if err := json.Unmarshal(body, &state); err != nil {
		return RuntimeHandle{}, fmtError("review runtime observed invalid persisted state", err)
	}
	for _, persisted := range state.Agents {
		if persisted.ID != agent.ID {
			continue
		}
		if err := validatePersistedReviewCycleSnapshot(persisted.ReviewCycle); err != nil {
			return RuntimeHandle{}, fmtError("review runtime observed invalid policy snapshot", err)
		}
		r.checked = true
		return RuntimeHandle{Kind: RuntimeKindTmux, Session: "policy-checkpoint-session"}, nil
	}
	return RuntimeHandle{}, errors.New("review runtime started before its agent state was durable")
}

func (r *policyCheckpointRunner) Send(RuntimeHandle, string) error           { return nil }
func (r *policyCheckpointRunner) Capture(RuntimeHandle, int) (string, error) { return "", nil }
func (r *policyCheckpointRunner) Stop(RuntimeHandle) error                   { return nil }
func (r *policyCheckpointRunner) IsAlive(RuntimeHandle) (bool, error)        { return true, nil }

func TestReviewPolicySnapshotIsPersistedBeforeCoordinatorStart(t *testing.T) {
	server := emptyReviewCommentsServer(t)
	defer server.Close()

	logDir := t.TempDir()
	policy, err := snapshotEffectiveReviewPolicy(builtInReviewPolicy())
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
	}
	runner := &policyCheckpointRunner{
		statePath: filepath.Join(logDir, orchestratorStateDirName, orchestratorStateFileName),
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     "/tmp/repo",
			LogDir:       logDir,
			WorktreeDir:  t.TempDir(),
			BaseBranch:   "main",
			ReviewPolicy: policy,
		},
		github: newGitHubClientForTest(t, server),
		agents: NewAgentManager(),
		runner: runner,
		cmdRunner: func(context.Context, string, string, ...string) error {
			return nil
		},
	}
	target := reviewLaunchTarget{
		LaunchKey:    reviewLaunchKeyForPR(54),
		SkipHardGate: true,
		PRNumber:     54,
		PRURL:        "https://github.com/acme/widget/pull/54",
	}
	if err := bot.startReviewAgentForTarget(context.Background(), target, testReviewHeadSHA); err != nil {
		t.Fatalf("startReviewAgentForTarget() error = %v", err)
	}
	reviewer := waitForReviewAgentForPR(t, bot.agents, 54)
	defer func() { _ = bot.StopAgent(context.Background(), reviewer.ID) }()
	body, err := os.ReadFile(runner.statePath)
	if err != nil {
		t.Fatalf("read persisted coordinator state: %v", err)
	}
	var state persistedStateFile
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("decode persisted coordinator state: %v", err)
	}
	checked := false
	for _, persisted := range state.Agents {
		if persisted.ID != reviewer.ID {
			continue
		}
		if err := validatePersistedReviewCycleSnapshot(persisted.ReviewCycle); err != nil {
			t.Fatalf("persisted coordinator policy snapshot: %v", err)
		}
		checked = true
	}
	if runner.starts != 0 || !checked {
		t.Fatalf("top-level runtime starts = %d, durable policy checked = %v; want 0 and true", runner.starts, checked)
	}
}

func TestReviewCoordinatorDoesNotStartWhenPolicySnapshotCannotPersist(t *testing.T) {
	server := emptyReviewCommentsServer(t)
	defer server.Close()

	logPath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(logPath, []byte("occupied"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(%s) error = %v", logPath, err)
	}
	runner := &policyCheckpointRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			RepoPath:    "/tmp/repo",
			LogDir:      logPath,
			WorktreeDir: t.TempDir(),
		},
		github: newGitHubClientForTest(t, server),
		agents: NewAgentManager(),
		runner: runner,
	}
	target := reviewLaunchTarget{
		LaunchKey:    reviewLaunchKeyForPR(54),
		SkipHardGate: true,
		PRNumber:     54,
	}
	err := bot.startReviewAgentForTarget(context.Background(), target, testReviewHeadSHA)
	if err == nil || !strings.Contains(err.Error(), "persist review policy snapshot before launch") {
		t.Fatalf("startReviewAgentForTarget() error = %v, want persistence failure", err)
	}
	if runner.starts != 0 {
		t.Fatalf("runtime starts = %d, want 0 after policy persistence failure", runner.starts)
	}
}

func TestPersistAgentStateSerializesConcurrentCheckpoints(t *testing.T) {
	agents := NewAgentManager()
	if err := agents.Add(&Agent{
		ID:                "serialized-reviewer",
		Role:              RoleReviewer,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
		RuntimeHandle:     RuntimeHandle{Kind: RuntimeKindTmux, Session: "serialized-session"},
		ObservedPRHeadSHA: "",
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	bot := &Orchestrator{
		cfg:    Config{LogDir: t.TempDir()},
		agents: agents,
	}

	bot.statePersistenceMu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- bot.persistAgentState()
	}()
	<-started

	select {
	case err := <-done:
		bot.statePersistenceMu.Unlock()
		t.Fatalf("persistAgentState() bypassed serialization lock with error %v", err)
	case <-time.After(25 * time.Millisecond):
		bot.statePersistenceMu.Unlock()
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("persistAgentState() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("persistAgentState() did not continue after serialization lock was released")
	}
}

func writeReviewPolicyStateFixture(t *testing.T, logDir string, agents ...persistedAgent) {
	t.Helper()
	payload := persistedStateFile{
		Version: persistedStateVersion,
		Agents:  agents,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	stateDir := filepath.Join(logDir, orchestratorStateDirName)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("os.MkdirAll(%s) error = %v", stateDir, err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, orchestratorStateFileName), body, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
}

func emptyReviewCommentsServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/issues/54/comments") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
}

func fmtError(message string, err error) error {
	return errors.New(message + ": " + err.Error())
}
