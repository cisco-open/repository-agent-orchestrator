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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

type reviewPlanFixture struct {
	Name               string                  `json:"name"`
	Files              []ReviewPlanChangedFile `json:"files"`
	AcceptanceCriteria []string                `json:"acceptance_criteria"`
	RiskTags           []string                `json:"risk_tags"`
	RiskReasons        []ReviewPlanRiskReason  `json:"risk_reasons"`
}

func loadReviewPlanFixtures(t *testing.T) []reviewPlanFixture {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "review_plan_fixtures.json"))
	if err != nil {
		t.Fatalf("ReadFile(review plan fixtures) error = %v", err)
	}
	var fixtures []reviewPlanFixture
	if err := json.Unmarshal(body, &fixtures); err != nil {
		t.Fatalf("Unmarshal(review plan fixtures) error = %v", err)
	}
	if len(fixtures) == 0 {
		t.Fatal("review plan fixtures are empty")
	}
	return fixtures
}

func reviewPlanInputsForFiles(
	t *testing.T,
	files []ReviewPlanChangedFile,
	criteria []string,
) ReviewPlanInputs {
	t.Helper()
	files = append([]ReviewPlanChangedFile(nil), files...)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Path == files[j].Path {
			return files[i].PreviousPath < files[j].PreviousPath
		}
		return files[i].Path < files[j].Path
	})
	criteria = append([]string(nil), criteria...)
	if files == nil {
		files = []ReviewPlanChangedFile{}
	}
	if criteria == nil {
		criteria = []string{}
	}
	sort.Strings(criteria)
	var changedLines int64
	for _, file := range files {
		changedLines += file.Additions + file.Deletions
	}
	inputs := ReviewPlanInputs{
		SchemaVersion:      reviewPlanInputsSchemaVersion,
		BaseSHA:            testOtherReviewHeadSHA,
		HeadSHA:            testReviewHeadSHA,
		ChangedFileCount:   len(files),
		ChangedLineCount:   changedLines,
		ChangedFiles:       files,
		AcceptanceCriteria: criteria,
	}
	if err := validateReviewPlanInputs(inputs); err != nil {
		t.Fatalf("fixture review-plan inputs are invalid: %v", err)
	}
	return inputs
}

func snapshottedReviewPlanPolicy(
	t *testing.T,
	mutate func(*ReviewPolicy),
) ReviewPolicy {
	t.Helper()
	policy := builtInReviewPolicy()
	if mutate != nil {
		mutate(&policy)
	}
	snapshot, err := snapshotEffectiveReviewPolicy(policy)
	if err != nil {
		t.Fatalf("snapshotEffectiveReviewPolicy() error = %v", err)
	}
	return snapshot
}

func TestReviewPlanRiskClassificationIsStableAndExplainable(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, nil)
	for _, fixture := range loadReviewPlanFixtures(t) {
		t.Run(fixture.Name, func(t *testing.T) {
			inputs := reviewPlanInputsForFiles(
				t,
				fixture.Files,
				fixture.AcceptanceCriteria,
			)
			first, err := buildReviewPlan(inputs, policy)
			if err != nil {
				t.Fatalf("buildReviewPlan(first) error = %v", err)
			}
			second, err := buildReviewPlan(cloneReviewPlanInputs(inputs), policy)
			if err != nil {
				t.Fatalf("buildReviewPlan(second) error = %v", err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("fixture plan is nondeterministic:\nfirst=%#v\nsecond=%#v", first, second)
			}
			if !reflect.DeepEqual(first.RiskTags, fixture.RiskTags) {
				t.Fatalf("RiskTags = %#v, want %#v", first.RiskTags, fixture.RiskTags)
			}
			if len(first.RiskReasons) != len(first.RiskTags) {
				t.Fatalf("RiskReasons = %#v", first.RiskReasons)
			}
			if fixture.RiskReasons != nil &&
				!reflect.DeepEqual(first.RiskReasons, fixture.RiskReasons) {
				t.Fatalf(
					"RiskReasons = %#v, want %#v",
					first.RiskReasons,
					fixture.RiskReasons,
				)
			}
			for _, item := range first.RiskReasons {
				if len(item.Reasons) == 0 {
					t.Fatalf("risk %q has no reason", item.Tag)
				}
			}
		})
	}
}

func TestReviewPlanClassificationNeverUsesModelPreferenceAsReason(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, nil)
	fixture := loadReviewPlanFixtures(t)[1]
	plan, err := buildReviewPlan(
		reviewPlanInputsForFiles(t, fixture.Files, fixture.AcceptanceCriteria),
		policy,
	)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	reasons := append([]string(nil), plan.SelectionReasons...)
	for _, item := range plan.RiskReasons {
		reasons = append(reasons, item.Reasons...)
	}
	for _, reason := range reasons {
		lower := strings.ToLower(reason)
		if strings.Contains(lower, "model") ||
			strings.Contains(lower, "reasoning effort") ||
			strings.Contains(lower, "profile") {
			t.Fatalf("unsafe model-preference planning reason = %q", reason)
		}
	}
}

func TestReviewPlanSelectsDifferentBoundedPlansForDifferentReviewDomains(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, nil)
	fixtures := loadReviewPlanFixtures(t)
	small, err := buildReviewPlan(
		reviewPlanInputsForFiles(t, fixtures[0].Files, fixtures[0].AcceptanceCriteria),
		policy,
	)
	if err != nil {
		t.Fatalf("buildReviewPlan(small) error = %v", err)
	}
	high, err := buildReviewPlan(
		reviewPlanInputsForFiles(t, fixtures[1].Files, fixtures[1].AcceptanceCriteria),
		policy,
	)
	if err != nil {
		t.Fatalf("buildReviewPlan(high) error = %v", err)
	}
	if small.InitialReviewerCount != policy.Swarm.MinReviewers {
		t.Fatalf(
			"small InitialReviewerCount = %d, want minimum %d",
			small.InitialReviewerCount,
			policy.Swarm.MinReviewers,
		)
	}
	if high.InitialReviewerCount != policy.Swarm.MaxReviewers {
		t.Fatalf(
			"high InitialReviewerCount = %d, want maximum %d",
			high.InitialReviewerCount,
			policy.Swarm.MaxReviewers,
		)
	}
	if reflect.DeepEqual(small.SelectedLanes, high.SelectedLanes) {
		t.Fatalf(
			"small and multi-domain lane plans match: %#v",
			small.SelectedLanes,
		)
	}
	for _, plan := range []ReviewPlan{small, high} {
		if plan.InitialReviewerCount < policy.Swarm.MinReviewers ||
			plan.InitialReviewerCount > policy.Swarm.MaxReviewers {
			t.Fatalf("reviewer count violates bounds: %#v", plan)
		}
	}
}

func TestReviewPlanDoesNotExpandOneDomainToUnrelatedLanes(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, func(policy *ReviewPolicy) {
		discovery := policy.RoleProfiles[AgentProfileRoleDiscovery]
		policy.Swarm = ReviewSwarmPolicy{
			MinReviewers:         1,
			MaxReviewers:         6,
			MaxParallelReviewers: 3,
			TimeoutMinutes:       defaultReviewWorkerTimeout,
			Retries:              defaultReviewWorkerRetries,
			Lanes: []ReviewLane{
				{Name: "contract", Required: true, Profile: discovery},
				{Name: "lifecycle", Required: false, Profile: discovery},
				{Name: "operations-tests", Required: false, Profile: discovery},
			},
		}
	})
	inputs := reviewPlanInputsForFiles(t, []ReviewPlanChangedFile{
		{
			Path:      "internal/security/auth.go",
			Additions: 1,
		},
		{
			Path:      "internal/security/auth_test.go",
			Additions: 1,
		},
	}, nil)
	plan, err := buildReviewPlan(inputs, policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	if plan.InitialReviewerCount != policy.Swarm.MinReviewers {
		t.Fatalf(
			"InitialReviewerCount = %d, want configured minimum %d",
			plan.InitialReviewerCount,
			policy.Swarm.MinReviewers,
		)
	}
	reasons := strings.Join(plan.SelectionReasons, "\n")
	if strings.Contains(reasons, "maximum target") {
		t.Fatalf("one review domain expanded the swarm: %#v", plan.SelectionReasons)
	}
}

func TestReviewPlanSizeAndTestVolumeDoNotChangeLaneSelection(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, nil)
	files := []ReviewPlanChangedFile{
		{Path: "internal/config.go", Additions: 1},
		{Path: "internal/config_test.go", Additions: 1},
	}
	baseline, err := buildReviewPlan(
		reviewPlanInputsForFiles(t, files, nil),
		policy,
	)
	if err != nil {
		t.Fatalf("buildReviewPlan(baseline) error = %v", err)
	}
	for index := 0; index < 40; index++ {
		files = append(files, ReviewPlanChangedFile{
			Path:      fmt.Sprintf("internal/fixture_%02d_test.go", index),
			Additions: 100,
		})
	}
	large, err := buildReviewPlan(
		reviewPlanInputsForFiles(t, files, nil),
		policy,
	)
	if err != nil {
		t.Fatalf("buildReviewPlan(large) error = %v", err)
	}
	if !reflect.DeepEqual(large.RiskTags, baseline.RiskTags) ||
		!reflect.DeepEqual(large.SelectedLanes, baseline.SelectedLanes) {
		t.Fatalf(
			"size or test volume changed planning: baseline=%#v large=%#v",
			baseline,
			large,
		)
	}
}

func TestTestOnlyAcceptanceCriteriaBelongToOperationsTests(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, nil)
	inputs := reviewPlanInputsForFiles(
		t,
		[]ReviewPlanChangedFile{{
			Path:      "internal/product/product_test.go",
			Additions: 118,
			ChangedRanges: []ReviewLineRange{{
				StartLine: 2385,
				EndLine:   2495,
				Symbol:    "func TestNXOSSSHCompletesObservedExternalUpgradeLifecycle(t *testing.T)",
			}},
		}},
		[]string{"make test", "all Go library coverage gates remain above 80%"},
	)
	plan, err := buildReviewPlan(inputs, policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}

	acceptanceRequirements := 0
	for _, requirement := range plan.CoverageRequirements {
		if requirement.Kind != ReviewCoverageAcceptanceCriterion {
			continue
		}
		acceptanceRequirements++
		if got, want := requirement.AccountableLane, "operations-tests"; got != want {
			t.Fatalf(
				"acceptance criterion %q accountable lane = %q, want %q",
				requirement.Description,
				got,
				want,
			)
		}
	}
	if acceptanceRequirements != 2 {
		t.Fatalf("acceptance requirements = %d, want 2", acceptanceRequirements)
	}
	if !reflect.DeepEqual(plan.RiskTags, []string{reviewRiskTestCoverage}) {
		t.Fatalf("test-only risk tags = %#v, want test-coverage only", plan.RiskTags)
	}
}

func TestReviewPlanPreservesEveryExplicitRequiredLaneAtCapacity(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, func(policy *ReviewPolicy) {
		discovery := policy.RoleProfiles[AgentProfileRoleDiscovery]
		policy.Swarm = ReviewSwarmPolicy{
			MinReviewers:         1,
			MaxReviewers:         3,
			MaxParallelReviewers: 2,
			TimeoutMinutes:       defaultReviewWorkerTimeout,
			Retries:              defaultReviewWorkerRetries,
			Lanes: []ReviewLane{
				{Name: "contract", Required: false, Profile: discovery},
				{Name: "lifecycle", Required: true, Profile: discovery},
				{Name: "concurrency-ordering", Required: true, Profile: discovery},
				{Name: "operations-tests", Required: true, Profile: discovery},
			},
		}
	})
	inputs := reviewPlanInputsForFiles(t, []ReviewPlanChangedFile{{
		Path:      "internal/security/auth.go",
		Additions: 1,
	}}, nil)
	plan, err := buildReviewPlan(inputs, policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	wantRequired := []string{"lifecycle", "concurrency-ordering", "operations-tests"}
	if !reflect.DeepEqual(plan.RequiredLanes, wantRequired) {
		t.Fatalf("RequiredLanes = %#v, want %#v", plan.RequiredLanes, wantRequired)
	}
	if !reflect.DeepEqual(plan.SelectedLanes, wantRequired) {
		t.Fatalf("SelectedLanes = %#v, want %#v", plan.SelectedLanes, wantRequired)
	}
	if plan.InitialReviewerCount != policy.Swarm.MaxReviewers {
		t.Fatalf(
			"InitialReviewerCount = %d, want %d",
			plan.InitialReviewerCount,
			policy.Swarm.MaxReviewers,
		)
	}
}

func TestReviewPlanPersistsWithExactInputsAndPolicy(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, nil)
	fixture := loadReviewPlanFixtures(t)[1]
	inputs := reviewPlanInputsForFiles(t, fixture.Files, fixture.AcceptanceCriteria)
	cycle, err := newReviewCycleState(inputs.HeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	if err := attachReviewPlanInputs(cycle, inputs); err != nil {
		t.Fatalf("attachReviewPlanInputs() error = %v", err)
	}
	plan, err := buildReviewPlan(inputs, cycle.Policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	runtimeProfile, err := cycle.Policy.effectiveProfileForRole(AgentProfileRoleChallenge)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                "review-agent-plan",
		Role:              RoleReviewer,
		ObservedPRHeadSHA: inputs.HeadSHA,
		RuntimeProfile:    runtimeProfile,
		RuntimeHandle:     RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-plan"},
		ReviewCycle:       cycle,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	logDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			LogDir:       logDir,
			ReviewPolicy: cycle.Policy,
		},
		agents: agents,
	}
	if err := bot.persistReviewPlan(reviewer.ID, plan); err != nil {
		t.Fatalf("persistReviewPlan() error = %v", err)
	}
	plan.SelectedLanes[0] = "mutated-after-set"

	restarted := &Orchestrator{
		cfg: Config{
			LogDir:       logDir,
			ReviewPolicy: cycle.Policy,
		},
		agents: NewAgentManager(),
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	loaded, ok := restarted.agents.Get(reviewer.ID)
	if !ok || loaded.ReviewCycle == nil || loaded.ReviewCycle.Plan == nil {
		t.Fatal("persisted exact-SHA review plan was not restored")
	}
	persisted := loaded.ReviewCycle.Plan
	if persisted.HeadSHA != inputs.HeadSHA ||
		persisted.BaseSHA != inputs.BaseSHA ||
		persisted.PolicyFingerprint != cycle.PolicyFingerprint {
		t.Fatalf("persisted plan bindings = %#v", persisted)
	}
	if persisted.SelectedLanes[0] == "mutated-after-set" {
		t.Fatal("persisted plan shared mutable lane storage")
	}
	if !reflect.DeepEqual(persisted.RiskTags, fixture.RiskTags) {
		t.Fatalf("persisted RiskTags = %#v, want %#v", persisted.RiskTags, fixture.RiskTags)
	}
}

func TestReviewPlanCannotBeReplacedOrRebound(t *testing.T) {
	policy := snapshottedReviewPlanPolicy(t, nil)
	inputs := reviewPlanInputsForFiles(t, []ReviewPlanChangedFile{{
		Path:      "internal/state.go",
		Additions: 1,
	}}, nil)
	cycle, err := newReviewCycleState(inputs.HeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	if err := attachReviewPlanInputs(cycle, inputs); err != nil {
		t.Fatalf("attachReviewPlanInputs() error = %v", err)
	}
	plan, err := buildReviewPlan(inputs, cycle.Policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                "review-agent-immutable-plan",
		Role:              RoleReviewer,
		ObservedPRHeadSHA: inputs.HeadSHA,
		ReviewCycle:       cycle,
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	if err := agents.SetReviewPlan(reviewer.ID, plan); err != nil {
		t.Fatalf("SetReviewPlan(first) error = %v", err)
	}
	if err := agents.SetReviewPlan(reviewer.ID, cloneReviewPlan(plan)); err != nil {
		t.Fatalf("SetReviewPlan(idempotent) error = %v", err)
	}
	replacement := cloneReviewPlan(plan)
	replacement.HeadSHA = testThirdReviewHeadSHA
	err = agents.SetReviewPlan(reviewer.ID, replacement)
	if err == nil || !strings.Contains(err.Error(), "already recorded") {
		t.Fatalf("SetReviewPlan(replacement) error = %v, want immutable plan error", err)
	}
	loaded, ok := agents.Get(reviewer.ID)
	if !ok || loaded.ReviewCycle == nil || loaded.ReviewCycle.Plan == nil {
		t.Fatal("review plan missing after replacement attempt")
	}
	if loaded.ReviewCycle.Plan.HeadSHA != testReviewHeadSHA {
		t.Fatalf(
			"persisted plan head = %s, want %s",
			loaded.ReviewCycle.Plan.HeadSHA,
			testReviewHeadSHA,
		)
	}
}
