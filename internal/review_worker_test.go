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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/go-github/v90/github"
)

func TestReviewWorkerRuntimeBoundsAreSeparateFromArtifactIO(t *testing.T) {
	policy := builtInReviewPolicy()
	policy.Swarm.TimeoutMinutes = 47
	policy.Artifacts.TimeoutSeconds = 1
	policy.Verification.TimeoutMinutes = 53
	policy.Convergence.MaxWallTimeMinutes = 60
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}

	for _, role := range []AgentProfileRole{
		AgentProfileRoleDiscovery,
		AgentProfileRoleChallenge,
	} {
		if got, want := reviewWorkerRuntimeTimeout(cycle, role),
			47*time.Minute; got != want {
			t.Fatalf("reviewWorkerRuntimeTimeout(%s) = %s, want %s", role, got, want)
		}
	}
	if got, want := reviewWorkerRuntimeTimeout(
		cycle,
		AgentProfileRoleVerifier,
	), 53*time.Minute; got != want {
		t.Fatalf("verifier runtime timeout = %s, want %s", got, want)
	}
	if got := reviewWorkerRuntimeTimeout(cycle, AgentProfileRoleCoder); got != 0 {
		t.Fatalf("unsupported runtime timeout = %s, want zero", got)
	}
	if got := reviewWorkerRuntimeTimeout(nil, AgentProfileRoleDiscovery); got != 0 {
		t.Fatalf("nil-cycle runtime timeout = %s, want zero", got)
	}
}

func TestReviewWorkerRuntimeIsCappedByRemainingCycleTime(t *testing.T) {
	policy := builtInReviewPolicy()
	policy.Convergence.MaxWallTimeMinutes = 20
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	startedAt := reviewCycleMetricsStartedAt(cycle)
	if got, want := reviewWorkerRuntimeTimeoutAt(
		cycle,
		AgentProfileRoleDiscovery,
		startedAt.Add(19*time.Minute),
	), time.Minute; got != want {
		t.Fatalf("remaining runtime timeout = %s, want %s", got, want)
	}
}

func TestReviewWorkerRuntimeDeadlineStartsAtPersistedRuntimeStart(t *testing.T) {
	policy := builtInReviewPolicy()
	policy.Swarm.TimeoutMinutes = 30
	policy.Convergence.MaxWallTimeMinutes = 60
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	cycleStartedAt := reviewCycleMetricsStartedAt(cycle)
	runtimeStartedAt := cycleStartedAt.Add(7 * time.Minute)
	if got, want := reviewWorkerRuntimeDeadline(
		cycle,
		AgentProfileRoleDiscovery,
		runtimeStartedAt,
	), runtimeStartedAt.Add(30*time.Minute); !got.Equal(want) {
		t.Fatalf("runtime deadline = %v, want %v", got, want)
	}

	cycle.Policy.Convergence.MaxWallTimeMinutes = 20
	if got, want := reviewWorkerRuntimeDeadline(
		cycle,
		AgentProfileRoleDiscovery,
		runtimeStartedAt,
	), cycleStartedAt.Add(20*time.Minute); !got.Equal(want) {
		t.Fatalf("cycle-capped runtime deadline = %v, want %v", got, want)
	}
}

func TestReviewWorkerArtifactContractIsExplicitForEveryWorkerRole(t *testing.T) {
	tests := []struct {
		role    AgentProfileRole
		phase   string
		payload string
	}{
		{AgentProfileRoleDiscovery, "discovery", `"candidates"`},
		{AgentProfileRoleVerifier, "verification", `"finding_id"`},
		{AgentProfileRoleChallenge, "challenge", `"assignment_id"`},
		{AgentProfileRoleEscalation, "challenge", `"target_kind"`},
	}
	for _, test := range tests {
		t.Run(string(test.role), func(t *testing.T) {
			contract, err := reviewWorkerArtifactContract(test.role)
			if err != nil {
				t.Fatalf("reviewWorkerArtifactContract() error = %v", err)
			}
			for _, want := range []string{
				`$RAO_REVIEW_ARTIFACT_DIR/$RAO_REVIEW_OWNER_ID.json`,
				`.review-artifact-partial-`,
				`Before the rename`,
				"`jq -e .`",
				`{"exact_sha":$RAO_REVIEW_HEAD_SHA`,
				`"worker_id":$RAO_REVIEW_OWNER_ID`,
				`"phase":"` + test.phase + `"`,
				test.payload,
				"copy requirement_id and kind exactly from the supplied plan",
				"use an empty array only for not_covered",
				"Do not cite temporary files that you delete",
			} {
				if !strings.Contains(contract, want) {
					t.Fatalf("artifact contract for %s missing %q: %s", test.role, want, contract)
				}
			}
			if test.role == AgentProfileRoleVerifier &&
				(!strings.Contains(contract, "copy the assigned finding's location") ||
					!strings.Contains(contract, "evidence or test_evidence item must cite the assigned location path") ||
					!strings.Contains(contract, `"scope_disposition"`) ||
					!strings.Contains(contract, `"patch_disposition"`) ||
					!strings.Contains(contract, `"causal_evidence"`)) {
				t.Fatalf("verifier contract does not preserve assigned identity: %s", contract)
			}
			if test.role != AgentProfileRoleVerifier &&
				(!strings.Contains(contract, `changed_branch`) ||
					!strings.Contains(contract, `persistence_boundary`)) {
				t.Fatalf("coverage artifact contract omits plan kinds: %s", contract)
			}
			if strings.Contains(contract, "schema_version") {
				t.Fatalf("artifact contract contains obsolete schema version: %s", contract)
			}
		})
	}
	if _, err := reviewWorkerArtifactContract(AgentProfileRoleCoder); err == nil {
		t.Fatal("coder unexpectedly has a review artifact contract")
	}
}

type testIsolatedReviewWorkerRunner struct {
	validateErr error
	startErr    error
	stopErr     error
	startHook   func()
	started     []Agent
	prompts     []string
	environment [][]string
	stopCalls   []RuntimeHandle
	alive       bool
}

func (r *testIsolatedReviewWorkerRunner) Start(
	agent Agent,
	_ string,
) (RuntimeHandle, error) {
	r.started = append(r.started, agent)
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: tmuxSessionName(agent),
	}, nil
}

func (r *testIsolatedReviewWorkerRunner) StartReviewWorker(
	agent Agent,
	_ string,
	environment []string,
) (RuntimeHandle, error) {
	return r.StartReviewWorkerContext(
		context.Background(),
		agent,
		"",
		environment,
	)
}

func (r *testIsolatedReviewWorkerRunner) StartReviewWorkerContext(
	ctx context.Context,
	agent Agent,
	prompt string,
	environment []string,
) (RuntimeHandle, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeHandle{}, err
	}
	r.started = append(r.started, agent)
	r.prompts = append(r.prompts, prompt)
	r.environment = append(r.environment, append([]string(nil), environment...))
	if r.startErr != nil {
		return RuntimeHandle{}, r.startErr
	}
	if r.startHook != nil {
		r.startHook()
	}
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: reviewWorkerTmuxSessionName(agent),
	}, nil
}

func (r *testIsolatedReviewWorkerRunner) ValidateReviewWorkerIsolation(
	_ []string,
) error {
	return r.validateErr
}

func (r *testIsolatedReviewWorkerRunner) Send(RuntimeHandle, string) error {
	return nil
}

func (r *testIsolatedReviewWorkerRunner) Capture(RuntimeHandle, int) (string, error) {
	return "", nil
}

func (r *testIsolatedReviewWorkerRunner) Stop(handle RuntimeHandle) error {
	r.stopCalls = append(r.stopCalls, handle)
	return r.stopErr
}

func (r *testIsolatedReviewWorkerRunner) IsAlive(RuntimeHandle) (bool, error) {
	return r.alive, nil
}

func newReviewWorkerTestCoordinator(
	t *testing.T,
	agents *AgentManager,
) (Agent, ReviewWorkerIdentity) {
	t.Helper()
	return newReviewWorkerTestCoordinatorWithPolicy(t, agents, builtInReviewPolicy())
}

func newReviewWorkerTestCoordinatorWithPolicy(
	t *testing.T,
	agents *AgentManager,
	policy ReviewPolicy,
) (Agent, ReviewWorkerIdentity) {
	t.Helper()
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	profile, err := cycle.Policy.effectiveProfileForRole(AgentProfileRoleChallenge)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	reviewer := Agent{
		ID:                "review-coordinator-worker-test",
		Role:              RoleReviewer,
		IssueNumber:       32,
		IssueTitle:        "Allocate and isolate review workers",
		IssueBody:         "Keep coordinator credentials out of workers.",
		LogDir:            t.TempDir(),
		BranchName:        "repository-agent-orchestrator/issue-32",
		PRNumber:          72,
		PRTitle:           "Allocate and isolate review workers",
		PRURL:             "https://example.test/pull/72",
		ObservedPRHeadSHA: testReviewHeadSHA,
		RuntimeProfile:    profile,
		ReviewCycle:       cycle,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
	}
	if err := agents.Add(&reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	identity, err := reviewWorkerIdentityForCycle(
		cycle,
		AgentProfileRoleDiscovery,
		1,
		"contract",
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}
	return reviewer, identity
}

func TestReviewCycleIDsAreCollisionResistantForTheSameHead(t *testing.T) {
	policy := builtInReviewPolicy()
	first, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState(first) error = %v", err)
	}
	second, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState(second) error = %v", err)
	}
	if first.ID == second.ID {
		t.Fatalf("cycle IDs collided: %q", first.ID)
	}
	for _, cycle := range []*ReviewCycleState{first, second} {
		if cycle.Revision != 1 {
			t.Fatalf("cycle revision = %d, want 1", cycle.Revision)
		}
		if !strings.Contains(cycle.ID, abbreviateSHA(testReviewHeadSHA)) {
			t.Fatalf("cycle ID %q is not traceable to head %s", cycle.ID, testReviewHeadSHA)
		}
		if err := validatePersistedReviewCycleSnapshot(cycle); err != nil {
			t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
		}
	}
}

func TestConcurrentReviewWorkerAllocationsCannotCollide(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	worktreeDir := t.TempDir()
	const workers = 64

	ownerships := make(chan ReviewWorkerOwnership, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			token := fmt.Sprintf("%032x", index+1)
			ownership, _, err := agents.allocateReviewWorkerOwnership(
				reviewer.ID,
				identity,
				worktreeDir,
				token,
				time.Unix(1700000000, 0).UTC(),
			)
			if err != nil {
				errs <- err
				return
			}
			ownerships <- ownership
		}(index)
	}
	wait.Wait()
	close(errs)
	close(ownerships)
	for err := range errs {
		t.Errorf("allocateReviewWorkerOwnership() error = %v", err)
	}
	if t.Failed() {
		return
	}

	owners := make(map[string]struct{}, workers)
	sessions := make(map[string]struct{}, workers)
	worktrees := make(map[string]struct{}, workers)
	gitHubConfigs := make(map[string]struct{}, workers)
	localPaths := make(map[string]struct{}, workers*2)
	attempts := make([]int, 0, workers)
	for ownership := range ownerships {
		owners[ownership.OwnerID] = struct{}{}
		sessions[ownership.SessionName] = struct{}{}
		worktrees[ownership.WorktreePath] = struct{}{}
		gitHubConfigs[ownership.GitHubConfigPath] = struct{}{}
		localPaths[ownership.WorktreePath] = struct{}{}
		localPaths[ownership.GitHubConfigPath] = struct{}{}
		attempts = append(attempts, ownership.Attempt)
	}
	if len(owners) != workers ||
		len(sessions) != workers ||
		len(worktrees) != workers ||
		len(gitHubConfigs) != workers ||
		len(localPaths) != workers*2 {
		t.Fatalf(
			"unique owners/sessions/worktrees/GitHub configs/paths = %d/%d/%d/%d/%d, want %d each and %d paths",
			len(owners),
			len(sessions),
			len(worktrees),
			len(gitHubConfigs),
			len(localPaths),
			workers,
			workers*2,
		)
	}
	sort.Ints(attempts)
	for index, attempt := range attempts {
		if attempt != index+1 {
			t.Fatalf("attempts = %v, want contiguous 1..%d", attempts, workers)
		}
	}
}

func TestReviewWorkerRetriesRetainLogicalIdentityWithDistinctAttempts(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	worktreeDir := t.TempDir()
	allocatedAt := time.Unix(1700000000, 0).UTC()

	first, _, err := agents.allocateReviewWorkerOwnership(
		reviewer.ID,
		identity,
		worktreeDir,
		strings.Repeat("1", reviewWorkerIdentityTokenBytes*2),
		allocatedAt,
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership(first) error = %v", err)
	}
	retry, _, err := agents.allocateReviewWorkerOwnership(
		reviewer.ID,
		identity,
		worktreeDir,
		strings.Repeat("2", reviewWorkerIdentityTokenBytes*2),
		allocatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership(retry) error = %v", err)
	}
	if !reflect.DeepEqual(first.Identity, retry.Identity) {
		t.Fatalf("retry identity = %#v, want %#v", retry.Identity, first.Identity)
	}
	if first.Attempt != 1 || retry.Attempt != 2 {
		t.Fatalf("attempts = %d, %d, want 1, 2", first.Attempt, retry.Attempt)
	}
	if first.OwnerID == retry.OwnerID ||
		first.SessionName == retry.SessionName ||
		first.WorktreePath == retry.WorktreePath ||
		first.GitHubConfigPath == retry.GitHubConfigPath {
		t.Fatalf("retry reused resources: first=%#v retry=%#v", first, retry)
	}
}

func TestReviewWorkerAllocationAndLaunchRejectTerminalCoordinator(t *testing.T) {
	tests := []struct {
		name    string
		state   AgentState
		stopped bool
	}{
		{name: "done", state: StateDone},
		{name: "errored", state: StateErrored},
		{name: "stopped flag", state: StateWorking, stopped: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agents := NewAgentManager()
			reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
			if !agents.SetState(reviewer.ID, test.state, test.stopped) {
				t.Fatalf(
					"SetState(%s, %s, %v) = false",
					reviewer.ID,
					test.state,
					test.stopped,
				)
			}

			_, _, err := agents.allocateReviewWorkerOwnership(
				reviewer.ID,
				identity,
				t.TempDir(),
				strings.Repeat("a", reviewWorkerIdentityTokenBytes*2),
				time.Unix(1700000000, 0).UTC(),
			)
			if err == nil || !strings.Contains(err.Error(), "stopped or terminal") {
				t.Fatalf(
					"allocateReviewWorkerOwnership() error = %v, want terminal coordinator failure",
					err,
				)
			}

			runner := &testIsolatedReviewWorkerRunner{}
			bot := &Orchestrator{
				cfg: Config{
					RepoOwner:   "acme",
					RepoName:    "widget",
					LogDir:      t.TempDir(),
					WorktreeDir: t.TempDir(),
				},
				agents: agents,
				runner: runner,
			}
			prepared := 0
			bot.prepareReviewWorkerWorktreeFunc = func(context.Context, Agent) error {
				prepared++
				return nil
			}

			ownership, handle, err := bot.launchReviewWorker(
				context.Background(),
				reviewer.ID,
				reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
			)
			if err == nil || !strings.Contains(err.Error(), "stopped or terminal") {
				t.Fatalf(
					"launchReviewWorker() error = %v, want terminal coordinator failure",
					err,
				)
			}
			if ownership.OwnerID != "" || handle.Session != "" {
				t.Fatalf(
					"terminal launch returned ownership/handle = %#v/%#v",
					ownership,
					handle,
				)
			}
			if prepared != 0 || len(runner.started) != 0 {
				t.Fatalf(
					"prepare/start calls = %d/%d, want 0/0",
					prepared,
					len(runner.started),
				)
			}
		})
	}
}

func TestReviewWorkerOwnershipPersistsForCleanupLookup(t *testing.T) {
	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	reviewer.LogDir = logDir
	if stored, ok := agents.agents[reviewer.ID]; ok {
		stored.LogDir = logDir
	}
	bot := &Orchestrator{
		cfg: Config{
			LogDir:       logDir,
			WorktreeDir:  worktreeDir,
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: agents,
	}
	ownership, err := bot.reserveReviewWorkerOwnership(reviewer.ID, identity)
	if err != nil {
		t.Fatalf("reserveReviewWorkerOwnership() error = %v", err)
	}

	restartedAgents := NewAgentManager()
	restarted := &Orchestrator{
		cfg: Config{
			LogDir:       logDir,
			WorktreeDir:  worktreeDir,
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: restartedAgents,
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	owner, cleanupTarget, ok := restartedAgents.findReviewWorkerOwnership(ownership.OwnerID)
	if !ok {
		t.Fatalf("findReviewWorkerOwnership(%q) = not found", ownership.OwnerID)
	}
	if owner.ID != reviewer.ID {
		t.Fatalf("cleanup owner coordinator = %q, want %q", owner.ID, reviewer.ID)
	}
	if !reflect.DeepEqual(cleanupTarget, ownership) {
		t.Fatalf("cleanup target = %#v, want %#v", cleanupTarget, ownership)
	}
}

func TestReviewWorkerEnvironmentIsDefaultDenyWithSafeContext(t *testing.T) {
	identity := ReviewWorkerIdentity{
		CycleID:  "review-cycle-abcdef123456-0123456789abcdef0123456789abcdef",
		Revision: 7,
		Role:     AgentProfileRoleDiscovery,
		Pass:     2,
		Lane:     "concurrency-ordering",
	}
	ownership, err := allocateReviewWorkerOwnership(
		identity,
		3,
		t.TempDir(),
		"0123456789abcdef0123456789abcdef",
		time.Unix(1700000000, 0).UTC(),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership() error = %v", err)
	}
	ambient := []string{
		"PATH=/usr/bin:/bin",
		"HOME=/safe/home",
		"LANG=en_US.UTF-8",
		"GH_TOKEN=blocked-gh",
		"GITHUB_TOKEN=blocked-github",
		"GITHUB_ENTERPRISE_TOKEN=blocked-enterprise",
		"WEBEX_WEBHOOK_URL=blocked-webex",
		"CODEX_CMD=blocked-command",
		"RAO_PROFILE_DISCOVERY_MODEL=blocked-profile",
		"COORDINATOR_ONLY=blocked-private",
	}
	environment, err := reviewWorkerEnvironment(
		ambient,
		"acme",
		"widget",
		72,
		testReviewHeadSHA,
		ownership,
	)
	if err != nil {
		t.Fatalf("reviewWorkerEnvironment() error = %v", err)
	}
	values := environmentMap(t, environment)
	for _, blocked := range []string{
		"GH_TOKEN",
		"GITHUB_TOKEN",
		"GITHUB_ENTERPRISE_TOKEN",
		"WEBEX_WEBHOOK_URL",
		"CODEX_CMD",
		"RAO_PROFILE_DISCOVERY_MODEL",
		"COORDINATOR_ONLY",
	} {
		if _, ok := values[blocked]; ok {
			t.Fatalf("blocked environment variable %s was forwarded", blocked)
		}
	}
	want := map[string]string{
		reviewWorkerEnvRepo:     "acme/widget",
		reviewWorkerEnvPRNumber: "72",
		reviewWorkerEnvHeadSHA:  testReviewHeadSHA,
		reviewWorkerEnvCycleID:  identity.CycleID,
		reviewWorkerEnvRevision: "7",
		reviewWorkerEnvRole:     string(identity.Role),
		reviewWorkerEnvPass:     "2",
		reviewWorkerEnvLane:     "concurrency-ordering",
		reviewWorkerEnvAttempt:  "3",
		reviewWorkerEnvOwnerID:  ownership.OwnerID,
	}
	for key, value := range want {
		if values[key] != value {
			t.Fatalf("%s = %q, want %q", key, values[key], value)
		}
	}
	if values["GH_CONFIG_DIR"] != ownership.GitHubConfigPath ||
		reviewWorkerPathWithin(ownership.WorktreePath, values["GH_CONFIG_DIR"]) {
		t.Fatalf(
			"GH_CONFIG_DIR = %q, want owned checkout-independent path %q",
			values["GH_CONFIG_DIR"],
			ownership.GitHubConfigPath,
		)
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	if values[reviewWorkerEnvArtifactDir] != artifactDirectory ||
		reviewWorkerPathWithin(
			ownership.WorktreePath,
			values[reviewWorkerEnvArtifactDir],
		) {
		t.Fatalf(
			"%s = %q, want owned checkout-independent path %q",
			reviewWorkerEnvArtifactDir,
			values[reviewWorkerEnvArtifactDir],
			artifactDirectory,
		)
	}
}

func TestReviewWorkerLaunchPersistsOwnerBeforeResourcesAndUsesIsolation(t *testing.T) {
	t.Setenv("GH_TOKEN", "blocked-gh")
	t.Setenv("GITHUB_TOKEN", "blocked-github")
	t.Setenv("GITHUB_ENTERPRISE_TOKEN", "blocked-enterprise")
	t.Setenv("WEBEX_WEBHOOK_URL", "blocked-webex")
	t.Setenv("COORDINATOR_ONLY", "blocked-private")

	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     "/unused/repo",
			LogDir:       logDir,
			WorktreeDir:  worktreeDir,
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: agents,
		runner: runner,
	}
	prepared := 0
	bot.prepareReviewWorkerWorktreeFunc = func(_ context.Context, worker Agent) error {
		prepared++
		body, err := os.ReadFile(bot.agentStateFilePath())
		if err != nil {
			return fmt.Errorf("owner state was not persisted before worktree prep: %w", err)
		}
		var state persistedStateFile
		if err := json.Unmarshal(body, &state); err != nil {
			return err
		}
		found := false
		for _, persisted := range state.Agents {
			if persisted.ID != reviewer.ID || persisted.ReviewCycle == nil {
				continue
			}
			for _, ownership := range persisted.ReviewCycle.WorkerOwnerships {
				if ownership.OwnerID == worker.ID &&
					ownership.WorktreePath == worker.WorktreePath {
					found = true
				}
			}
		}
		if !found {
			return errors.New("persisted state has no resource owner")
		}
		return os.MkdirAll(worker.WorktreePath, 0o755)
	}
	runner.startHook = func() {
		current, ok := agents.Get(reviewer.ID)
		if !ok || current.ReviewCycle == nil {
			t.Fatal("review coordinator disappeared at launcher invocation")
		}
		observation, err := observableReviewCycle(current)
		if err != nil {
			t.Fatalf(
				"observableReviewCycle() at launcher invocation error = %v",
				err,
			)
		}
		if observation.Counts.Running != 1 ||
			observation.Counts.MaxParallelObserved != 1 ||
			observation.Counts.MaxReviewerParallelObserved != 1 {
			t.Fatalf(
				"launcher invocation counts = %#v, want one running reviewer",
				observation.Counts,
			)
		}
		if len(observation.Workers) != 1 ||
			observation.Workers[0].Lifecycle != ReviewWorkerRunning ||
			observation.Workers[0].StartedAt.IsZero() {
			t.Fatalf(
				"launcher invocation workers = %#v, want durable running worker",
				observation.Workers,
			)
		}
		body, err := os.ReadFile(bot.agentStateFilePath())
		if err != nil {
			t.Fatalf(
				"ReadFile(persisted launcher checkpoint) error = %v",
				err,
			)
		}
		var state persistedStateFile
		if err := json.Unmarshal(body, &state); err != nil {
			t.Fatalf(
				"Unmarshal(persisted launcher checkpoint) error = %v",
				err,
			)
		}
		for _, persisted := range state.Agents {
			if persisted.ID != reviewer.ID ||
				persisted.ReviewCycle == nil ||
				len(persisted.ReviewCycle.WorkerOwnerships) != 1 {
				continue
			}
			worker := persisted.ReviewCycle.WorkerOwnerships[0]
			if worker.Lifecycle == ReviewWorkerRunning &&
				!worker.StartedAt.IsZero() {
				return
			}
		}
		t.Fatal("running worker was not persisted before launcher invocation")
	}

	ownership, handle, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   "Review the assigned lane.",
		},
	)
	if err != nil {
		t.Fatalf("launchReviewWorker() error = %v", err)
	}
	if prepared != 1 || len(runner.started) != 1 {
		t.Fatalf("prepare/start calls = %d/%d, want 1/1", prepared, len(runner.started))
	}
	if handle.Session != ownership.SessionName {
		t.Fatalf("runtime session = %q, want persisted %q", handle.Session, ownership.SessionName)
	}
	if !ownership.ArtifactDirectoryClaimed {
		t.Fatal("launched worker did not return its artifact directory claim")
	}
	stored, ok := agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after worker launch")
	}
	claimPersisted := false
	for _, persisted := range stored.ReviewCycle.WorkerOwnerships {
		if persisted.OwnerID == ownership.OwnerID {
			claimPersisted = persisted.ArtifactDirectoryClaimed
		}
	}
	if !claimPersisted {
		t.Fatal("launched worker artifact directory claim was not persisted")
	}
	values := environmentMap(t, runner.environment[0])
	for _, blocked := range []string{
		"GH_TOKEN",
		"GITHUB_TOKEN",
		"GITHUB_ENTERPRISE_TOKEN",
		"WEBEX_WEBHOOK_URL",
		"COORDINATOR_ONLY",
	} {
		if _, ok := values[blocked]; ok {
			t.Fatalf("blocked environment variable %s reached worker launch", blocked)
		}
	}
	if values[reviewWorkerEnvRepo] != "acme/widget" ||
		values[reviewWorkerEnvHeadSHA] != testReviewHeadSHA ||
		values[reviewWorkerEnvLane] != identity.Lane {
		t.Fatalf("safe worker context = %#v", values)
	}
	if values["GH_CONFIG_DIR"] != ownership.GitHubConfigPath {
		t.Fatalf(
			"GH_CONFIG_DIR = %q, want persisted path %q",
			values["GH_CONFIG_DIR"],
			ownership.GitHubConfigPath,
		)
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	if values[reviewWorkerEnvArtifactDir] != artifactDirectory {
		t.Fatalf(
			"%s = %q, want %q",
			reviewWorkerEnvArtifactDir,
			values[reviewWorkerEnvArtifactDir],
			artifactDirectory,
		)
	}
	if len(runner.prompts) != 1 ||
		!strings.Contains(runner.prompts[0], "Review the assigned lane.") ||
		!strings.Contains(
			runner.prompts[0],
			`$RAO_REVIEW_ARTIFACT_DIR/$RAO_REVIEW_OWNER_ID.json`,
		) ||
		!strings.Contains(runner.prompts[0], `{"exact_sha":$RAO_REVIEW_HEAD_SHA`) ||
		strings.Contains(runner.prompts[0], "schema_version") {
		t.Fatalf("review worker prompt is missing the artifact contract: %#v", runner.prompts)
	}
	entries, err := os.ReadDir(ownership.GitHubConfigPath)
	if err != nil {
		t.Fatalf("ReadDir(GitHub config) error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("GitHub config entries = %#v, want empty directory", entries)
	}
	entries, err = os.ReadDir(artifactDirectory)
	if err != nil {
		t.Fatalf("ReadDir(artifact directory) error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("artifact directory entries = %#v, want empty directory", entries)
	}
}

func TestReviewWorkerWorktreeSetupRetryUsesOneOwnershipAndOneRuntime(t *testing.T) {
	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			LogDir:       logDir,
			WorktreeDir:  worktreeDir,
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: agents,
		runner: runner,
	}
	prepareCalls := 0
	bot.prepareReviewWorkerWorktreeFunc = func(
		_ context.Context,
		worker Agent,
	) error {
		prepareCalls++
		if prepareCalls == 1 {
			if err := os.Mkdir(worker.WorktreePath, 0o755); err != nil {
				return err
			}
			return errors.New("transient worktree metadata lock")
		}
		if _, err := os.Stat(worker.WorktreePath); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("owned allocation was not cleaned before retry: %v", err)
		}
		return os.Mkdir(worker.WorktreePath, 0o755)
	}
	var cleanupPaths []string
	bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		branchName string,
	) error {
		if branchName != "" {
			t.Fatalf("retry cleanup branch = %q, want no branch mutation", branchName)
		}
		cleanupPaths = append(cleanupPaths, worktreePath)
		return os.RemoveAll(worktreePath)
	}

	ownership, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if err != nil {
		t.Fatalf("launchReviewWorker() error = %v", err)
	}
	if prepareCalls != 2 || len(cleanupPaths) != 1 ||
		cleanupPaths[0] != ownership.WorktreePath {
		t.Fatalf(
			"prepare/cleanup = %d/%#v, want two attempts and exact owned path %q",
			prepareCalls,
			cleanupPaths,
			ownership.WorktreePath,
		)
	}
	if len(runner.started) != 1 {
		t.Fatalf("model runtime starts = %d, want exactly one", len(runner.started))
	}
	current, ok := agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil ||
		len(current.ReviewCycle.WorkerOwnerships) != 1 {
		t.Fatalf("logical review ownerships = %#v, want exactly one", current.ReviewCycle)
	}
	stored := current.ReviewCycle.WorkerOwnerships[0]
	if stored.OwnerID != ownership.OwnerID || stored.Attempt != 1 ||
		stored.Lifecycle != ReviewWorkerRunning ||
		stored.Failure != nil {
		t.Fatalf("retried setup ownership = %#v", stored)
	}
}

func TestReviewWorkerWorktreeSetupExhaustionPersistsBoundedSanitizedDiagnostic(
	t *testing.T,
) {
	const secret = "git-diagnostic-secret"
	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			LogDir:       logDir,
			WorktreeDir:  worktreeDir,
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		token:  secret,
		agents: agents,
		runner: runner,
	}
	exitCause := errors.New("exit status 75")
	prepareCalls := 0
	bot.prepareReviewWorkerWorktreeFunc = func(
		_ context.Context,
		worker Agent,
	) error {
		prepareCalls++
		if err := os.Mkdir(worker.WorktreePath, 0o755); err != nil {
			return err
		}
		return &commandExecutionError{
			command: "git worktree add --detach " + worker.WorktreePath,
			cause:   exitCause,
			detail: "stdout:\n" + strings.Repeat("x", reviewWorkerWorktreeSetupDiagnosticBytes) +
				"\nstderr:\nfatal: worktree metadata locked " + secret + "\xff",
		}
	}
	var cleanupPaths []string
	bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		cleanupPaths = append(cleanupPaths, worktreePath)
		return os.RemoveAll(worktreePath)
	}

	ownership, handle, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if err == nil || !errors.Is(err, exitCause) {
		t.Fatalf("launchReviewWorker() error = %v, want retained exit cause", err)
	}
	if prepareCalls != reviewWorkerWorktreeSetupAttempts ||
		len(cleanupPaths) != reviewWorkerWorktreeSetupAttempts-1 {
		t.Fatalf(
			"prepare/cleanup calls = %d/%d, want %d/%d",
			prepareCalls,
			len(cleanupPaths),
			reviewWorkerWorktreeSetupAttempts,
			reviewWorkerWorktreeSetupAttempts-1,
		)
	}
	for _, path := range cleanupPaths {
		if path != ownership.WorktreePath {
			t.Fatalf("retry cleanup path = %q, want exact owner path %q", path, ownership.WorktreePath)
		}
	}
	if handle.Session != "" || len(runner.started) != 0 {
		t.Fatalf("runtime handle/starts = %#v/%d, want token-free setup failure", handle, len(runner.started))
	}
	current, ok := agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil ||
		len(current.ReviewCycle.WorkerOwnerships) != 1 {
		t.Fatalf("persisted review cycle = %#v", current.ReviewCycle)
	}
	stored := current.ReviewCycle.WorkerOwnerships[0]
	diagnostic := ""
	if stored.Failure != nil {
		diagnostic = stored.Failure.Detail
	}
	if stored.Lifecycle != ReviewWorkerFailed || !stored.StartedAt.IsZero() ||
		stored.Failure == nil ||
		stored.Failure.Kind != DurableLaunchFailureSetup ||
		len(diagnostic) == 0 ||
		len(diagnostic) > reviewWorkerWorktreeSetupDiagnosticBytes ||
		!utf8.ValidString(diagnostic) ||
		strings.Contains(diagnostic, secret) ||
		!strings.Contains(diagnostic, "[REDACTED]") ||
		!strings.Contains(diagnostic, string(utf8.RuneError)) ||
		!strings.Contains(diagnostic, "stderr:") ||
		!strings.Contains(diagnostic, "exit status 75") {
		t.Fatalf("persisted setup failure = lifecycle:%s started:%s diagnostic:%q", stored.Lifecycle, stored.StartedAt, diagnostic)
	}
	observation, observationErr := observableReviewCycle(current)
	if observationErr != nil {
		t.Fatalf("observableReviewCycle() error = %v", observationErr)
	}
	if len(observation.Workers) != 1 ||
		observation.Workers[0].WorktreeSetupFailure != diagnostic {
		t.Fatalf("observable setup diagnostic = %#v", observation.Workers)
	}
	for _, event := range current.ReviewCycle.Metrics.Events {
		if event.Usage != nil {
			t.Fatalf("setup failure recorded model usage: %#v", event.Usage)
		}
	}
}

func TestReviewWorkerDeterministicWorktreeSetupFailureDoesNotRetry(t *testing.T) {
	err := &commandExecutionError{
		command: "git worktree add --detach /owned deadbeef",
		cause:   errors.New("exit status 128"),
		detail:  "stderr:\nfatal: invalid reference: deadbeef",
	}
	if reviewWorkerWorktreeSetupFailureIsRetryable(err) {
		t.Fatal("deterministic invalid-reference failure was classified retryable")
	}
}

func TestReviewWorkerLaunchCanceledAtEntryPreventsAllocation(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}
	prepared := 0
	bot.prepareReviewWorkerWorktreeFunc = func(context.Context, Agent) error {
		prepared++
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ownership, handle, err := bot.launchReviewWorker(
		ctx,
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("launchReviewWorker() error = %v, want context cancellation", err)
	}
	if ownership.OwnerID != "" || handle.Session != "" {
		t.Fatalf("canceled launch returned ownership/handle = %#v/%#v", ownership, handle)
	}
	if prepared != 0 || len(runner.started) != 0 || len(runner.stopCalls) != 0 {
		t.Fatalf(
			"prepare/start/stop calls = %d/%d/%d, want 0/0/0",
			prepared,
			len(runner.started),
			len(runner.stopCalls),
		)
	}
	stored, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if got := len(stored.ReviewCycle.WorkerOwnerships); got != 0 {
		t.Fatalf("persisted worker ownerships = %d, want 0", got)
	}
}

func TestReviewWorkerLaunchCancellationAfterPreparationPreventsRuntimeStart(
	t *testing.T,
) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}
	ctx, cancel := context.WithCancel(context.Background())
	bot.prepareReviewWorkerWorktreeFunc = func(_ context.Context, worker Agent) error {
		if err := os.MkdirAll(worker.WorktreePath, 0o755); err != nil {
			return err
		}
		cancel()
		return nil
	}

	ownership, handle, err := bot.launchReviewWorker(
		ctx,
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("launchReviewWorker() error = %v, want context cancellation", err)
	}
	if ownership.OwnerID == "" || handle.Session != "" {
		t.Fatalf("canceled launch returned ownership/handle = %#v/%#v", ownership, handle)
	}
	if len(runner.started) != 0 || len(runner.stopCalls) != 0 {
		t.Fatalf(
			"start/stop calls = %d/%d, want 0/0",
			len(runner.started),
			len(runner.stopCalls),
		)
	}
	if _, statErr := os.Stat(ownership.GitHubConfigPath); !errors.Is(
		statErr,
		os.ErrNotExist,
	) {
		t.Fatalf(
			"GitHub config path %q exists after pre-start cancellation: %v",
			ownership.GitHubConfigPath,
			statErr,
		)
	}
	if _, persisted, ok := agents.findReviewWorkerOwnership(
		ownership.OwnerID,
	); !ok || persisted.WorktreePath != ownership.WorktreePath {
		t.Fatal("canceled launch owner was not retained for cleanup lookup")
	}
}

func TestReviewWorkerLaunchCancellationDuringSetupPersistsCancelledWithoutDiagnostic(
	t *testing.T,
) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}
	ctx, cancel := context.WithCancel(context.Background())
	bot.prepareReviewWorkerWorktreeFunc = func(
		setupCtx context.Context,
		_ Agent,
	) error {
		cancel()
		<-setupCtx.Done()
		return setupCtx.Err()
	}

	ownership, handle, err := bot.launchReviewWorker(
		ctx,
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("launchReviewWorker() error = %v, want context cancellation", err)
	}
	if ownership.OwnerID == "" || handle.Session != "" ||
		len(runner.started) != 0 {
		t.Fatalf(
			"canceled setup ownership/handle/starts = %#v/%#v/%d",
			ownership,
			handle,
			len(runner.started),
		)
	}
	current, ok := agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after canceled setup")
	}
	_, stored, ok := agents.findReviewWorkerOwnership(ownership.OwnerID)
	if !ok || stored.Lifecycle != ReviewWorkerCancelled ||
		stored.Failure != nil || stored.FinishedAt.IsZero() {
		t.Fatalf("canceled setup lifecycle = %#v", stored)
	}
}

func TestReviewWorkerLaunchCancellationAfterStartupStopsRuntime(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	ctx, cancel := context.WithCancel(context.Background())
	runner := &testIsolatedReviewWorkerRunner{startHook: cancel}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}
	bot.prepareReviewWorkerWorktreeFunc = func(_ context.Context, worker Agent) error {
		return os.MkdirAll(worker.WorktreePath, 0o755)
	}

	ownership, handle, err := bot.launchReviewWorker(
		ctx,
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("launchReviewWorker() error = %v, want context cancellation", err)
	}
	if ownership.OwnerID == "" || handle.Session != "" {
		t.Fatalf("canceled launch returned ownership/handle = %#v/%#v", ownership, handle)
	}
	if len(runner.started) != 1 || len(runner.stopCalls) != 1 {
		t.Fatalf(
			"start/stop calls = %d/%d, want 1/1",
			len(runner.started),
			len(runner.stopCalls),
		)
	}
	if got := runner.stopCalls[0].Session; got != ownership.SessionName {
		t.Fatalf("stopped session = %q, want %q", got, ownership.SessionName)
	}
}

func TestReviewWorkerReservationSerializesConcurrentTerminalizationWithPersistence(
	t *testing.T,
) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
	}

	_, unlockLifecycle, err :=
		agents.lockActiveReviewCoordinatorForWorkerLaunch(reviewer.ID)
	if err != nil {
		t.Fatalf(
			"lockActiveReviewCoordinatorForWorkerLaunch() error = %v",
			err,
		)
	}
	lifecycleLocked := true
	defer func() {
		if lifecycleLocked {
			unlockLifecycle()
		}
	}()

	type reservationResult struct {
		ownership ReviewWorkerOwnership
		err       error
	}
	bot.statePersistenceMu.Lock()
	persistenceLocked := true
	defer func() {
		if persistenceLocked {
			bot.statePersistenceMu.Unlock()
		}
	}()
	reservationDone := make(chan reservationResult, 1)
	go func() {
		ownership, err :=
			bot.reserveReviewWorkerOwnershipForActiveCoordinator(
				reviewer.ID,
				identity,
			)
		reservationDone <- reservationResult{
			ownership: ownership,
			err:       err,
		}
	}()

	select {
	case result := <-reservationDone:
		t.Fatalf(
			"reservation completed before persistence gate: %#v",
			result,
		)
	case <-time.After(25 * time.Millisecond):
	}
	stored, ok := agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if len(stored.ReviewCycle.WorkerOwnerships) != 0 {
		t.Fatalf(
			"uncheckpointed ownership became visible: %#v",
			stored.ReviewCycle.WorkerOwnerships,
		)
	}

	terminalStarted := make(chan struct{})
	terminalDone := make(chan bool, 1)
	go func() {
		close(terminalStarted)
		terminalDone <- agents.SetState(reviewer.ID, StateDone, true)
	}()
	<-terminalStarted
	select {
	case result := <-terminalDone:
		t.Fatalf(
			"terminalization completed while reservation lifecycle was locked: %v",
			result,
		)
	case <-time.After(25 * time.Millisecond):
	}

	bot.statePersistenceMu.Unlock()
	persistenceLocked = false
	var result reservationResult
	select {
	case result = <-reservationDone:
	case <-time.After(2 * time.Second):
		t.Fatal("review worker reservation did not continue after persistence resumed")
	}
	if result.err != nil {
		t.Fatalf(
			"reserveReviewWorkerOwnershipForActiveCoordinator() error = %v",
			result.err,
		)
	}
	if result.ownership.OwnerID == "" {
		t.Fatal("persisted reservation returned an empty owner")
	}

	unlockLifecycle()
	lifecycleLocked = false
	select {
	case terminalized := <-terminalDone:
		if !terminalized {
			t.Fatal("SetState(done) = false after reservation released lifecycle")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminalization did not continue after reservation completed")
	}

	body, err := os.ReadFile(bot.agentStateFilePath())
	if err != nil {
		t.Fatalf("ReadFile(persisted state) error = %v", err)
	}
	var state persistedStateFile
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("Unmarshal(persisted state) error = %v", err)
	}
	found := false
	for _, persisted := range state.Agents {
		if persisted.ID != reviewer.ID || persisted.ReviewCycle == nil {
			continue
		}
		for _, ownership := range persisted.ReviewCycle.WorkerOwnerships {
			if ownership.OwnerID == result.ownership.OwnerID {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("persisted state omitted owner reserved before terminalization")
	}
}

func TestReviewVerdictCleansWorkerLaunchedBeforeTerminalizationPersistence(
	t *testing.T,
) {
	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      logDir,
			WorktreeDir: worktreeDir,
		},
		agents: agents,
		runner: runner,
	}

	preparationStarted := make(chan struct{})
	releasePreparation := make(chan struct{})
	bot.prepareReviewWorkerWorktreeFunc = func(
		_ context.Context,
		worker Agent,
	) error {
		if err := os.MkdirAll(worker.WorktreePath, 0o755); err != nil {
			return err
		}
		close(preparationStarted)
		<-releasePreparation
		return nil
	}
	var cleanupPaths []string
	bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		cleanupPaths = append(cleanupPaths, worktreePath)
		return os.RemoveAll(worktreePath)
	}

	type launchResult struct {
		ownership ReviewWorkerOwnership
		handle    RuntimeHandle
		err       error
	}
	launchDone := make(chan launchResult, 1)
	go func() {
		ownership, handle, err := bot.launchReviewWorker(
			context.Background(),
			reviewer.ID,
			reviewWorkerLaunchRequest{
				Identity: identity,
				Prompt:   "review",
			},
		)
		launchDone <- launchResult{
			ownership: ownership,
			handle:    handle,
			err:       err,
		}
	}()
	select {
	case <-preparationStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("review worker launch did not reach worktree preparation")
	}

	comment := &github.IssueComment{
		ID: github.Int64(99),
		Body: github.String(
			"CODEX_AGENT_ID: " + reviewer.ID +
				"\nCODEX_AGENT_ROLE: reviewer" +
				"\nCODEX_REVIEWED_SHA: " + testReviewHeadSHA +
				"\nCODEX_VERDICT: NEEDS_CHANGES",
		),
	}
	verdictDone := make(chan error, 1)
	go func() {
		verdictDone <- bot.handleReviewVerdict(
			context.Background(),
			reviewer,
			ReviewVerdictNeedsChanges,
			comment,
		)
	}()
	select {
	case err := <-verdictDone:
		t.Fatalf(
			"handleReviewVerdict() completed during worker launch: %v",
			err,
		)
	case <-time.After(25 * time.Millisecond):
	}

	close(releasePreparation)
	var launched launchResult
	select {
	case launched = <-launchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("review worker launch did not complete")
	}
	if launched.err != nil {
		t.Fatalf("launchReviewWorker() error = %v", launched.err)
	}
	select {
	case err := <-verdictDone:
		if err != nil {
			t.Fatalf("handleReviewVerdict() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("review verdict terminalization did not complete")
	}

	if got := len(runner.stopCalls); got != 1 {
		t.Fatalf("stopped worker runtimes = %d, want 1", got)
	}
	if runner.stopCalls[0].Session != launched.ownership.SessionName {
		t.Fatalf(
			"stopped worker session = %q, want %q",
			runner.stopCalls[0].Session,
			launched.ownership.SessionName,
		)
	}
	if got := cleanupPaths; len(got) != 1 ||
		got[0] != launched.ownership.WorktreePath {
		t.Fatalf(
			"cleaned worktrees = %#v, want [%q]",
			got,
			launched.ownership.WorktreePath,
		)
	}
	for _, resourcePath := range []string{
		launched.ownership.WorktreePath,
		launched.ownership.GitHubConfigPath,
	} {
		if _, err := os.Lstat(resourcePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned resource %q still exists: %v", resourcePath, err)
		}
	}

	stored, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if stored.State != StateDone || !stored.Stopped {
		t.Fatalf(
			"review coordinator state = (%s, stopped=%v), want (%s, true)",
			stored.State,
			stored.Stopped,
			StateDone,
		)
	}
	body, err := os.ReadFile(bot.agentStateFilePath())
	if err != nil {
		t.Fatalf("ReadFile(persisted state) error = %v", err)
	}
	var state persistedStateFile
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("Unmarshal(persisted state) error = %v", err)
	}
	for _, persisted := range state.Agents {
		if persisted.ID == reviewer.ID {
			t.Fatalf(
				"terminal coordinator remained persisted with ownerships: %#v",
				persisted.ReviewCycle,
			)
		}
	}
	if _, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   "late review",
		},
	); err == nil || !strings.Contains(err.Error(), "stopped or terminal") {
		t.Fatalf(
			"late launchReviewWorker() error = %v, want terminal coordinator failure",
			err,
		)
	}
}

func TestReviewVerdictRetainsWorkerOwnershipWhenCleanupFails(t *testing.T) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	coder := &Agent{
		ID:                      "coder-review-cleanup-failure",
		Role:                    RoleCoder,
		IssueNumber:             reviewer.IssueNumber,
		PRNumber:                reviewer.PRNumber,
		PRURL:                   reviewer.PRURL,
		ObservedPRHeadSHA:       reviewer.ObservedPRHeadSHA,
		ActiveReviewAgentID:     reviewer.ID,
		State:                   StateWaiting,
		LastActivityTime:        time.Now(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	agents.mu.Lock()
	agents.agents[reviewer.ID].ParentAgentID = coder.ID
	agents.mu.Unlock()
	runner := &testIsolatedReviewWorkerRunner{
		stopErr: errors.New("worker stop failed"),
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      logDir,
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(
			context.Context,
			string,
			string,
			string,
		) error {
			return nil
		},
	}
	ownership, err := bot.reserveReviewWorkerOwnership(
		reviewer.ID,
		identity,
	)
	if err != nil {
		t.Fatalf("reserveReviewWorkerOwnership() error = %v", err)
	}

	comment := &github.IssueComment{ID: github.Int64(100)}
	err = bot.handleReviewVerdict(
		context.Background(),
		reviewer,
		ReviewVerdictNeedsChanges,
		comment,
	)
	if err == nil || !strings.Contains(err.Error(), "worker stop failed") {
		t.Fatalf(
			"handleReviewVerdict() error = %v, want worker cleanup failure",
			err,
		)
	}
	stored, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if stored.State != StateStopped || !stored.Stopped {
		t.Fatalf(
			"review coordinator state = (%s, stopped=%v), want (%s, true)",
			stored.State,
			stored.Stopped,
			StateStopped,
		)
	}

	body, err := os.ReadFile(bot.agentStateFilePath())
	if err != nil {
		t.Fatalf("ReadFile(persisted state) error = %v", err)
	}
	var state persistedStateFile
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("Unmarshal(persisted state) error = %v", err)
	}
	found := false
	for _, persisted := range state.Agents {
		if persisted.ID != reviewer.ID ||
			persisted.ReviewCycle == nil {
			continue
		}
		for _, persistedOwnership := range persisted.ReviewCycle.WorkerOwnerships {
			if persistedOwnership.OwnerID == ownership.OwnerID {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("failed worker cleanup dropped durable ownership")
	}
	coderState, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %q disappeared", coder.ID)
	}
	if coderState.LastReviewedHeadSHA != "" ||
		coderState.LastReviewVerdict != "" ||
		coderState.LastReviewCommentID != 0 {
		t.Fatalf(
			"failed cleanup consumed verdict = (head=%q, verdict=%q, comment=%d)",
			coderState.LastReviewedHeadSHA,
			coderState.LastReviewVerdict,
			coderState.LastReviewCommentID,
		)
	}
	if coderState.State != StateWaiting {
		t.Fatalf(
			"coder state = %s, want %s without correction restart",
			coderState.State,
			StateWaiting,
		)
	}
	if coderState.ActiveReviewAgentID != "" {
		t.Fatalf(
			"ActiveReviewAgentID = %q, want empty after failed coordinator cleanup",
			coderState.ActiveReviewAgentID,
		)
	}
}

func TestReviewCoordinatorRuntimeExitCleansAndDropsOwnedWorkers(t *testing.T) {
	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: worktreeDir,
		},
		agents: agents,
		runner: runner,
	}
	ownership, err := bot.reserveReviewWorkerOwnership(
		reviewer.ID,
		identity,
	)
	if err != nil {
		t.Fatalf("reserveReviewWorkerOwnership() error = %v", err)
	}
	for _, resourcePath := range []string{
		ownership.WorktreePath,
		ownership.GitHubConfigPath,
	} {
		if err := os.MkdirAll(resourcePath, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", resourcePath, err)
		}
	}
	if !agents.SetRuntimeHandle(
		reviewer.ID,
		RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "dead-review-coordinator",
		},
	) {
		t.Fatalf("SetRuntimeHandle(%s) = false", reviewer.ID)
	}
	var cleanupPaths []string
	bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		cleanupPaths = append(cleanupPaths, worktreePath)
		return os.RemoveAll(worktreePath)
	}

	current, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	updated, shouldContinue, err := bot.reconcileRuntimeHealth(
		context.Background(),
		current,
	)
	if err != nil {
		t.Fatalf("reconcileRuntimeHealth() error = %v", err)
	}
	if shouldContinue {
		t.Fatal("shouldContinue = true after coordinator runtime exit")
	}
	if updated.State != StateErrored || !updated.Stopped {
		t.Fatalf(
			"review coordinator state = (%s, stopped=%v), want (%s, true)",
			updated.State,
			updated.Stopped,
			StateErrored,
		)
	}
	if got := len(runner.stopCalls); got != 1 ||
		runner.stopCalls[0].Session != ownership.SessionName {
		t.Fatalf(
			"stopped worker runtimes = %#v, want session %q",
			runner.stopCalls,
			ownership.SessionName,
		)
	}
	if got := cleanupPaths; len(got) != 1 ||
		got[0] != ownership.WorktreePath {
		t.Fatalf(
			"cleaned worker worktrees = %#v, want [%q]",
			got,
			ownership.WorktreePath,
		)
	}
	for _, resourcePath := range []string{
		ownership.WorktreePath,
		ownership.GitHubConfigPath,
	} {
		if _, err := os.Lstat(resourcePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned resource %q still exists: %v", resourcePath, err)
		}
	}

	body, err := os.ReadFile(bot.agentStateFilePath())
	if err != nil {
		t.Fatalf("ReadFile(persisted state) error = %v", err)
	}
	var state persistedStateFile
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("Unmarshal(persisted state) error = %v", err)
	}
	for _, persisted := range state.Agents {
		if persisted.ID == reviewer.ID {
			t.Fatalf(
				"errored coordinator remained persisted with ownerships: %#v",
				persisted.ReviewCycle,
			)
		}
	}
}

func TestReviewWorkerLaunchRejectsPrelaunchCoordinatorState(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	if !agents.SetState(reviewer.ID, StateReviewGate, false) {
		t.Fatalf("SetState(%s, review gate) = false", reviewer.ID)
	}
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}

	_, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   "review",
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "not active for worker launch") {
		t.Fatalf(
			"launchReviewWorker() error = %v, want inactive coordinator failure",
			err,
		)
	}
	if got := len(runner.started); got != 0 {
		t.Fatalf("started workers = %d, want 0", got)
	}
	current, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if got := len(current.ReviewCycle.WorkerOwnerships); got != 0 {
		t.Fatalf("reserved worker ownerships = %d, want 0", got)
	}
}

func TestPauseReviewCoordinatorStopsAndCleansOwnedWorkers(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	worktreeDir := t.TempDir()
	if !agents.SetRuntimeHandle(
		reviewer.ID,
		RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-coordinator-session",
		},
	) {
		t.Fatalf("SetRuntimeHandle(%s) = false", reviewer.ID)
	}
	runner := &testIsolatedReviewWorkerRunner{}
	cleaned := make([]string, 0, 1)
	bot := &Orchestrator{
		cfg: Config{
			LogDir:      reviewer.LogDir,
			WorktreeDir: worktreeDir,
		},
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(
			_ context.Context,
			_ string,
			path string,
			_ string,
		) error {
			cleaned = append(cleaned, path)
			return os.RemoveAll(path)
		},
	}
	ownership, err := bot.reserveReviewWorkerOwnership(
		reviewer.ID,
		identity,
	)
	if err != nil {
		t.Fatalf("reserveReviewWorkerOwnership() error = %v", err)
	}
	if err := os.MkdirAll(ownership.WorktreePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(worker worktree) error = %v", err)
	}
	if err := os.MkdirAll(ownership.GitHubConfigPath, 0o700); err != nil {
		t.Fatalf("MkdirAll(worker GitHub config) error = %v", err)
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	if err := os.MkdirAll(artifactDirectory, 0o700); err != nil {
		t.Fatalf("MkdirAll(worker artifact directory) error = %v", err)
	}
	if !agents.markReviewWorkerArtifactDirectoryClaimed(
		reviewer.ID,
		ownership.OwnerID,
	) {
		t.Fatal("failed to mark test artifact directory claimed")
	}

	if err := bot.PauseAgent(context.Background(), reviewer.ID); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}

	if got := len(runner.stopCalls); got != 2 {
		t.Fatalf("runner.Stop calls = %d, want coordinator and worker", got)
	}
	if runner.stopCalls[0].Session != "review-coordinator-session" {
		t.Fatalf(
			"first stopped session = %q, want coordinator",
			runner.stopCalls[0].Session,
		)
	}
	if runner.stopCalls[1].Session != ownership.SessionName {
		t.Fatalf(
			"second stopped session = %q, want worker %q",
			runner.stopCalls[1].Session,
			ownership.SessionName,
		)
	}
	if !reflect.DeepEqual(cleaned, []string{ownership.WorktreePath}) {
		t.Fatalf(
			"cleaned worktrees = %#v, want worker %q",
			cleaned,
			ownership.WorktreePath,
		)
	}
	for _, path := range []string{
		ownership.WorktreePath,
		ownership.GitHubConfigPath,
		artifactDirectory,
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned worker path %q still exists after pause", path)
		}
	}
	paused, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if !paused.Paused || paused.Stopped || paused.State != StateWorking {
		t.Fatalf(
			"paused coordinator state = (%s, paused=%v, stopped=%v), want (%s, true, false)",
			paused.State,
			paused.Paused,
			paused.Stopped,
			StateWorking,
		)
	}
	if _, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   "must not launch while paused",
		},
	); err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf(
			"launchReviewWorker() error = %v, want paused coordinator rejection",
			err,
		)
	}
}

func TestReviewWorkerLaunchUsesSnapshottedDiscoveryLaneProfile(t *testing.T) {
	policy := builtInReviewPolicy()
	laneProfileName := policy.RoleProfiles[AgentProfileRoleChallenge]
	discoveryProfileName := policy.RoleProfiles[AgentProfileRoleDiscovery]
	if laneProfileName == discoveryProfileName {
		t.Fatal("test requires distinct lane and discovery role profiles")
	}
	for index := range policy.Swarm.Lanes {
		if policy.Swarm.Lanes[index].Name == "contract" {
			policy.Swarm.Lanes[index].Profile = laneProfileName
		}
	}

	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinatorWithPolicy(t, agents, policy)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}
	bot.prepareReviewWorkerWorktreeFunc = func(_ context.Context, worker Agent) error {
		return os.MkdirAll(worker.WorktreePath, 0o755)
	}

	if _, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review correctness"},
	); err != nil {
		t.Fatalf("launchReviewWorker() error = %v", err)
	}
	if len(runner.started) != 1 {
		t.Fatalf("started workers = %d, want 1", len(runner.started))
	}
	want := reviewer.ReviewCycle.Policy.AgentProfiles[laneProfileName]
	if got := runner.started[0].RuntimeProfile; got != want {
		t.Fatalf("worker runtime profile = %+v, want lane profile %+v", got, want)
	}
	discovery, err := reviewer.ReviewCycle.Policy.effectiveProfileForRole(
		AgentProfileRoleDiscovery,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(discovery) error = %v", err)
	}
	if runner.started[0].RuntimeProfile == discovery {
		t.Fatalf(
			"worker runtime profile = %+v, must differ from discovery role profile",
			runner.started[0].RuntimeProfile,
		)
	}
}

func TestReviewWorkerLaunchUsesSnapshottedEscalationPolicyProfile(t *testing.T) {
	policy := builtInReviewPolicy()
	escalationRoleProfileName :=
		policy.RoleProfiles[AgentProfileRoleEscalation]
	policy.Escalation.Profile = policy.RoleProfiles[AgentProfileRoleChallenge]
	if policy.Escalation.Profile == escalationRoleProfileName {
		t.Fatal("test requires distinct escalation policy and role profiles")
	}

	agents := NewAgentManager()
	reviewer, _ := newReviewWorkerTestCoordinatorWithPolicy(t, agents, policy)
	identity, err := reviewWorkerIdentityForCycle(
		reviewer.ReviewCycle,
		AgentProfileRoleEscalation,
		1,
		"escalation",
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}
	bot.prepareReviewWorkerWorktreeFunc = func(
		_ context.Context,
		worker Agent,
	) error {
		return os.MkdirAll(worker.WorktreePath, 0o755)
	}

	if _, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "escalate review"},
	); err != nil {
		t.Fatalf("launchReviewWorker() error = %v", err)
	}
	if len(runner.started) != 1 {
		t.Fatalf("started workers = %d, want 1", len(runner.started))
	}
	want := reviewer.ReviewCycle.Policy.AgentProfiles[reviewer.ReviewCycle.Policy.Escalation.Profile]
	if got := runner.started[0].RuntimeProfile; got != want {
		t.Fatalf(
			"worker runtime profile = %+v, want escalation policy profile %+v",
			got,
			want,
		)
	}
	roleProfile, err := reviewer.ReviewCycle.Policy.effectiveProfileForRole(
		AgentProfileRoleEscalation,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(escalation) error = %v", err)
	}
	if runner.started[0].RuntimeProfile == roleProfile {
		t.Fatalf(
			"worker runtime profile = %+v, must differ from escalation role profile",
			runner.started[0].RuntimeProfile,
		)
	}
}

func TestReviewWorkerRoutingPreservesPreEscalationOwnership(t *testing.T) {
	policy := builtInReviewPolicy()
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	identity, err := reviewWorkerIdentityForCycle(
		cycle,
		AgentProfileRoleChallenge,
		1,
		"pre-escalation-challenge",
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}

	transitionedAt := time.Now().UTC()
	preEscalation := ReviewWorkerOwnership{
		DurableLaunchAttempt: DurableLaunchAttempt{
			AllocatedAt: transitionedAt.Add(-time.Minute),
		},
		Identity: identity,
	}
	if err := populateReviewWorkerRouting(cycle, &preEscalation); err != nil {
		t.Fatalf("populateReviewWorkerRouting(pre-escalation) error = %v", err)
	}
	challengeProfile, err := policy.effectiveProfileForRole(
		AgentProfileRoleChallenge,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	if got, want := preEscalation.Profile, challengeProfile.Name; got != want {
		t.Fatalf("pre-escalation profile = %q, want %q", got, want)
	}

	cycle.EscalationTransition = &ReviewEscalationTransition{
		TransitionedAt: transitionedAt,
		Profile:        policy.Escalation.Profile,
	}
	if err := validateReviewWorkerRouting(cycle, preEscalation); err != nil {
		t.Fatalf("validateReviewWorkerRouting(pre-escalation) error = %v", err)
	}

	postEscalation := ReviewWorkerOwnership{
		DurableLaunchAttempt: DurableLaunchAttempt{
			AllocatedAt: transitionedAt.Add(time.Minute),
		},
		Identity: identity,
	}
	if err := populateReviewWorkerRouting(cycle, &postEscalation); err != nil {
		t.Fatalf("populateReviewWorkerRouting(post-escalation) error = %v", err)
	}
	escalationProfile := policy.AgentProfiles[policy.Escalation.Profile]
	if got, want := postEscalation.Profile, escalationProfile.Name; got != want {
		t.Fatalf("post-escalation profile = %q, want %q", got, want)
	}
	if err := validateReviewWorkerRouting(cycle, postEscalation); err != nil {
		t.Fatalf("validateReviewWorkerRouting(post-escalation) error = %v", err)
	}
}

func TestReviewWorkerLaunchRejectsUnconfiguredDiscoveryLane(t *testing.T) {
	agents := NewAgentManager()
	reviewer, _ := newReviewWorkerTestCoordinator(t, agents)
	identity, err := reviewWorkerIdentityForCycle(
		reviewer.ReviewCycle,
		AgentProfileRoleDiscovery,
		1,
		"unconfigured",
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:   "acme",
			RepoName:    "widget",
			LogDir:      t.TempDir(),
			WorktreeDir: t.TempDir(),
		},
		agents: agents,
		runner: runner,
	}
	prepared := 0
	bot.prepareReviewWorkerWorktreeFunc = func(context.Context, Agent) error {
		prepared++
		return nil
	}

	ownership, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		`discovery lane "unconfigured" is not configured`,
	) {
		t.Fatalf("launchReviewWorker() error = %v, want unconfigured lane failure", err)
	}
	if ownership.OwnerID != "" {
		t.Fatalf("unconfigured lane allocated ownership %#v", ownership)
	}
	if prepared != 0 || len(runner.started) != 0 {
		t.Fatalf("prepare/start calls = %d/%d, want 0/0", prepared, len(runner.started))
	}
	stored, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	if got := len(stored.ReviewCycle.WorkerOwnerships); got != 0 {
		t.Fatalf("persisted worker ownerships = %d, want 0", got)
	}
}

func TestReviewWorkerIsolationFailurePreventsResourceCreationAndLaunch(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{
		validateErr: errors.New("empty-environment boundary unavailable"),
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			LogDir:       t.TempDir(),
			WorktreeDir:  t.TempDir(),
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: agents,
		runner: runner,
	}
	prepared := 0
	bot.prepareReviewWorkerWorktreeFunc = func(context.Context, Agent) error {
		prepared++
		return nil
	}

	ownership, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if err == nil || !strings.Contains(err.Error(), "isolation could not be established") {
		t.Fatalf("launchReviewWorker() error = %v, want isolation failure", err)
	}
	if prepared != 0 || len(runner.started) != 0 {
		t.Fatalf("prepare/start calls = %d/%d, want 0/0", prepared, len(runner.started))
	}
	if ownership.OwnerID == "" {
		t.Fatal("failed isolation should retain the persisted cleanup owner")
	}
	if _, persisted, ok := agents.findReviewWorkerOwnership(ownership.OwnerID); !ok ||
		persisted.WorktreePath != ownership.WorktreePath {
		t.Fatal("failed isolation owner was not retained for cleanup lookup")
	}
	if _, statErr := os.Stat(ownership.WorktreePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("worktree %q exists after isolation failure: %v", ownership.WorktreePath, statErr)
	}
}

func TestReviewWorkerLaunchRejectsPreexistingGitHubConfigPath(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{
			name: "populated directory",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatalf("Mkdir(%s) error = %v", path, err)
				}
				if err := os.WriteFile(
					filepath.Join(path, "hosts.yml"),
					[]byte("attacker-controlled"),
					0o600,
				); err != nil {
					t.Fatalf("WriteFile(attacker config) error = %v", err)
				}
			},
		},
		{
			name: "directory symlink",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Symlink(t.TempDir(), path); err != nil {
					t.Fatalf("Symlink(%s) error = %v", path, err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agents := NewAgentManager()
			reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
			runner := &testIsolatedReviewWorkerRunner{}
			bot := &Orchestrator{
				cfg: Config{
					RepoOwner:    "acme",
					RepoName:     "widget",
					LogDir:       t.TempDir(),
					WorktreeDir:  t.TempDir(),
					ReviewPolicy: reviewer.ReviewCycle.Policy,
				},
				agents: agents,
				runner: runner,
			}
			prepared := 0
			bot.prepareReviewWorkerWorktreeFunc = func(
				_ context.Context,
				worker Agent,
			) error {
				prepared++
				if err := os.Mkdir(worker.WorktreePath, 0o755); err != nil {
					return err
				}
				configPath, err := reviewWorkerGitHubConfigPath(worker.WorktreePath)
				if err != nil {
					return err
				}
				test.setup(t, configPath)
				return nil
			}

			ownership, _, err := bot.launchReviewWorker(
				context.Background(),
				reviewer.ID,
				reviewWorkerLaunchRequest{
					Identity: identity,
					Prompt:   "review",
				},
			)
			if err == nil ||
				!strings.Contains(
					err.Error(),
					"fresh credentialless review worker GitHub config",
				) {
				t.Fatalf(
					"launchReviewWorker() error = %v, want fresh config failure",
					err,
				)
			}
			if prepared != 1 || len(runner.started) != 0 {
				t.Fatalf(
					"prepare/start calls = %d/%d, want 1/0",
					prepared,
					len(runner.started),
				)
			}
			if ownership.GitHubConfigPath == "" {
				t.Fatal("failed launch did not retain its GitHub config cleanup path")
			}
			if _, persisted, ok := agents.findReviewWorkerOwnership(
				ownership.OwnerID,
			); !ok ||
				persisted.GitHubConfigPath != ownership.GitHubConfigPath {
				t.Fatal("failed launch owner did not retain its GitHub config cleanup path")
			}
		})
	}
}

func TestReviewWorkerLaunchRejectsPreexistingArtifactDirectory(t *testing.T) {
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	var injectedArtifactDirectory string
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			LogDir:       t.TempDir(),
			WorktreeDir:  t.TempDir(),
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: agents,
		runner: runner,
		cleanupWorktreeFunc: func(
			_ context.Context,
			_ string,
			worktreePath string,
			_ string,
		) error {
			return os.RemoveAll(worktreePath)
		},
	}
	bot.prepareReviewWorkerWorktreeFunc = func(
		_ context.Context,
		worker Agent,
	) error {
		if err := os.Mkdir(worker.WorktreePath, 0o755); err != nil {
			return err
		}
		artifactDirectory, err := reviewWorkerArtifactDirectory(
			worker.WorktreePath,
		)
		if err != nil {
			return err
		}
		injectedArtifactDirectory = artifactDirectory
		if err := os.Mkdir(artifactDirectory, 0o700); err != nil {
			return err
		}
		return os.WriteFile(
			filepath.Join(artifactDirectory, "injected.json"),
			[]byte(`{"untrusted":true}`),
			0o600,
		)
	}

	ownership, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   "review",
		},
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"fresh review worker artifact directory",
		) {
		t.Fatalf(
			"launchReviewWorker() error = %v, want fresh artifact directory failure",
			err,
		)
	}
	if len(runner.started) != 0 {
		t.Fatalf("started workers = %d, want 0", len(runner.started))
	}
	if ownership.OwnerID == "" {
		t.Fatal("failed launch did not retain persisted cleanup ownership")
	}
	stored, ok := agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after failed launch")
	}
	if _, err := bot.transitionReviewCoordinatorLifecycle(
		context.Background(),
		stored.ID,
		reviewCoordinatorLifecycleRequest{
			Intent:                     ReviewCoordinatorLifecycleCleanup,
			FinalState:                 StateStopped,
			ReleaseCoordinatorWorktree: true,
		},
	); err != nil {
		t.Fatalf("transitionReviewCoordinatorLifecycle() error = %v", err)
	}
	injectedPath := filepath.Join(
		injectedArtifactDirectory,
		"injected.json",
	)
	if body, err := os.ReadFile(injectedPath); err != nil ||
		string(body) != `{"untrusted":true}` {
		t.Fatalf(
			"pre-existing artifact content after cleanup = %q, error = %v",
			body,
			err,
		)
	}
}

func TestReviewWorkerRestartCleansCreateBeforeClaimCheckpointWindow(
	t *testing.T,
) {
	logDir := t.TempDir()
	worktreeDir := t.TempDir()
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			LogDir:       logDir,
			WorktreeDir:  worktreeDir,
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: agents,
		runner: runner,
	}
	checkpointBlocker := bot.agentStateFilePath() + ".tmp"
	bot.prepareReviewWorkerWorktreeFunc = func(
		_ context.Context,
		worker Agent,
	) error {
		if err := os.MkdirAll(worker.WorktreePath, 0o755); err != nil {
			return err
		}
		return os.Mkdir(checkpointBlocker, 0o700)
	}

	ownership, handle, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   "review",
		},
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"persist review worker artifact directory claim",
		) {
		t.Fatalf(
			"launchReviewWorker() error = %v, want claim checkpoint failure",
			err,
		)
	}
	if ownership.OwnerID == "" ||
		ownership.ArtifactDirectoryClaimed ||
		handle.Session != "" {
		t.Fatalf(
			"failed launch ownership/handle = %#v/%#v",
			ownership,
			handle,
		)
	}
	if len(runner.started) != 0 {
		t.Fatalf("started workers = %d, want 0", len(runner.started))
	}
	stored, ok := agents.Get(reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after failed claim checkpoint")
	}
	if len(stored.ReviewCycle.WorkerOwnerships) != 1 ||
		stored.ReviewCycle.WorkerOwnerships[0].ArtifactDirectoryClaimed {
		t.Fatalf(
			"in-memory claim was not rolled back: %#v",
			stored.ReviewCycle.WorkerOwnerships,
		)
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	markerPath, err := reviewWorkerArtifactOwnerMarkerPath(
		artifactDirectory,
	)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactOwnerMarkerPath() error = %v", err)
	}
	if info, err := os.Lstat(artifactDirectory); err != nil ||
		!info.IsDir() {
		t.Fatalf(
			"artifact directory before restart = %#v, error = %v",
			info,
			err,
		)
	}
	markerExists, markerMatches, err :=
		verifyReviewWorkerArtifactOwnerMarker(ownership)
	if err != nil || !markerExists || !markerMatches {
		t.Fatalf(
			"artifact owner marker = exists:%v matches:%v error:%v",
			markerExists,
			markerMatches,
			err,
		)
	}

	if err := os.RemoveAll(checkpointBlocker); err != nil {
		t.Fatalf("RemoveAll(checkpoint blocker) error = %v", err)
	}
	restartedAgents := NewAgentManager()
	restarted := &Orchestrator{
		cfg: Config{
			LogDir:      logDir,
			WorktreeDir: worktreeDir,
		},
		agents: restartedAgents,
		runner: &testIsolatedReviewWorkerRunner{},
		cleanupWorktreeFunc: func(
			_ context.Context,
			_ string,
			worktreePath string,
			_ string,
		) error {
			return os.RemoveAll(worktreePath)
		},
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	restored, ok := restartedAgents.Get(reviewer.ID)
	if !ok || restored.ReviewCycle == nil ||
		len(restored.ReviewCycle.WorkerOwnerships) != 1 {
		t.Fatalf("restored review coordinator = %#v", restored)
	}
	restoredOwnership := restored.ReviewCycle.WorkerOwnerships[0]
	if restoredOwnership.ArtifactDirectoryClaimed {
		t.Fatal("failed claim checkpoint became durable across restart")
	}
	if _, err := restarted.transitionReviewCoordinatorLifecycle(
		context.Background(),
		restored.ID,
		reviewCoordinatorLifecycleRequest{
			Intent:                     ReviewCoordinatorLifecycleCleanup,
			FinalState:                 StateStopped,
			ReleaseCoordinatorWorktree: true,
		},
	); err != nil {
		t.Fatalf("transitionReviewCoordinatorLifecycle() error = %v", err)
	}
	for _, path := range []string{artifactDirectory, markerPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("restart cleanup left owned path %q: %v", path, err)
		}
	}

	if _, err := prepareReviewWorkerArtifactDirectory(
		restoredOwnership,
	); err != nil {
		t.Fatalf(
			"same ownership could not recreate artifact directory: %v",
			err,
		)
	}
	if err := cleanupReviewWorkerArtifactDirectory(
		worktreeDir,
		restoredOwnership,
	); err != nil {
		t.Fatalf("cleanupReviewWorkerArtifactDirectory() error = %v", err)
	}
	for _, path := range []string{artifactDirectory, markerPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("recreated owned path %q survived cleanup: %v", path, err)
		}
	}
}

func TestReviewWorkerPersistenceFailurePreventsResourceCreationAndLaunch(t *testing.T) {
	logParent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(logParent, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", logParent, err)
	}
	agents := NewAgentManager()
	reviewer, identity := newReviewWorkerTestCoordinator(t, agents)
	runner := &testIsolatedReviewWorkerRunner{}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			LogDir:       filepath.Join(logParent, "logs"),
			WorktreeDir:  t.TempDir(),
			ReviewPolicy: reviewer.ReviewCycle.Policy,
		},
		agents: agents,
		runner: runner,
	}
	prepared := 0
	bot.prepareReviewWorkerWorktreeFunc = func(context.Context, Agent) error {
		prepared++
		return nil
	}

	_, _, err := bot.launchReviewWorker(
		context.Background(),
		reviewer.ID,
		reviewWorkerLaunchRequest{Identity: identity, Prompt: "review"},
	)
	if err == nil || !strings.Contains(err.Error(), "before resource creation") {
		t.Fatalf("launchReviewWorker() error = %v, want persistence failure", err)
	}
	if prepared != 0 || len(runner.started) != 0 {
		t.Fatalf("prepare/start calls = %d/%d, want 0/0", prepared, len(runner.started))
	}
}

func environmentMap(t *testing.T, environment []string) map[string]string {
	t.Helper()
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, found := strings.Cut(entry, "=")
		if !found {
			t.Fatalf("malformed environment entry %q", entry)
		}
		values[key] = value
	}
	return values
}
