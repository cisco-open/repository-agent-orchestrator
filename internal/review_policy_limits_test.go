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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSampleSwarmConfigIncludesValidExplicitReviewLimits(t *testing.T) {
	setRequiredEnv(t)
	samplePath := filepath.Join("..", "config", "sample-swarm.yaml")
	body, err := os.ReadFile(samplePath)
	if err != nil {
		t.Fatalf("os.ReadFile(config/sample-swarm.yaml) error = %v", err)
	}
	var raw repoConfigFile
	if err := yaml.Unmarshal(body, &raw); err != nil {
		t.Fatalf("yaml.Unmarshal(config/sample-swarm.yaml) error = %v", err)
	}
	if raw.ReviewPolicy == nil {
		t.Fatal("sample REVIEW_POLICY is missing")
	}
	if raw.ReviewPolicy.Convergence == nil ||
		raw.ReviewPolicy.Convergence.MaxReviewAgentsPerSHA == nil ||
		raw.ReviewPolicy.Convergence.MaxWallTimeMinutes == nil ||
		raw.ReviewPolicy.Convergence.MaxUsageTokens == nil {
		t.Fatal("sample REVIEW_POLICY.CONVERGENCE must expose every hard budget")
	}
	if raw.ReviewPolicy.ReviewSwarm == nil ||
		raw.ReviewPolicy.ReviewSwarm.TimeoutMinutes == nil ||
		raw.ReviewPolicy.ReviewSwarm.Retries == nil {
		t.Fatal("sample REVIEW_POLICY.REVIEW_SWARM must expose worker runtime bounds")
	}
	if raw.ReviewPolicy.Escalation == nil ||
		raw.ReviewPolicy.FailureActions == nil {
		t.Fatal("sample REVIEW_POLICY must expose escalation and failure actions")
	}
	if raw.ReviewPolicy.Verification == nil ||
		raw.ReviewPolicy.Verification.InconclusiveAction == nil {
		t.Fatal("sample REVIEW_POLICY.VERIFICATION must expose INCONCLUSIVE_ACTION")
	}
	if raw.ReviewPolicy.Artifacts == nil ||
		raw.ReviewPolicy.Artifacts.MaxBytes == nil ||
		raw.ReviewPolicy.Artifacts.TimeoutSeconds == nil ||
		raw.ReviewPolicy.Artifacts.Retries == nil {
		t.Fatal("sample REVIEW_POLICY.ARTIFACTS must expose every publication bound")
	}

	_, err = loadConfig(samplePath)
	if err != nil {
		t.Fatalf("loadConfig(config/sample-swarm.yaml) error = %v", err)
	}
}

func TestLoadConfigReviewLimitDefaults(t *testing.T) {
	tests := []struct {
		name              string
		policy            string
		escalationProfile string
	}{
		{
			name:              "review policy omitted",
			escalationProfile: string(AgentProfileRoleEscalation),
		},
		{
			name:              "limit settings omitted",
			policy:            validReviewPolicyConfig(),
			escalationProfile: "escalation",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(test.policy))

			cfg, err := loadConfig(cfgPath)
			if err != nil {
				t.Fatalf("loadConfig() error = %v", err)
			}
			if got, want := cfg.ReviewPolicy.Escalation, defaultEscalationPolicy(test.escalationProfile); !reflect.DeepEqual(got, want) {
				t.Fatalf("ReviewPolicy.Escalation = %#v, want %#v", got, want)
			}
			if got, want := cfg.ReviewPolicy.FailureActions, defaultReviewFailureActions(); !reflect.DeepEqual(got, want) {
				t.Fatalf("ReviewPolicy.FailureActions = %#v, want %#v", got, want)
			}
			if got, want := cfg.ReviewPolicy.Convergence, defaultConvergencePolicy(); !reflect.DeepEqual(got, want) {
				t.Fatalf("ReviewPolicy.Convergence = %#v, want %#v", got, want)
			}
			if got, want := cfg.ReviewPolicy.Verification.InconclusiveAction, ReviewActionEscalate; got != want {
				t.Fatalf("ReviewPolicy.Verification.InconclusiveAction = %q, want %q", got, want)
			}
		})
	}
}

func TestLoadConfigReviewLimitsAndActionsRoundTrip(t *testing.T) {
	setRequiredEnv(t)
	policy := validReviewPolicyConfig() + `  VERIFICATION:
    INCONCLUSIVE_ACTION: " FAIL "
  CONVERGENCE:
    MAX_ROUNDS: 4
    MAX_REVIEW_AGENTS_PER_SHA: 18
    MAX_WALL_TIME_MINUTES: 120
    MAX_USAGE_TOKENS: 3000000
  ESCALATION:
    AFTER_NON_CONVERGING_ROUNDS: 3
    AFTER_CORRECTION_ROUNDS: 4
    PROFILE: challenge
  FAILURE_ACTIONS:
    REQUIRED_LANE_FAILURE: fail
    VERIFICATION_FAILURE: inconclusive
    BUDGET_EXHAUSTION: escalate
`
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	wantEscalation := EscalationPolicy{
		AfterNonConvergingRounds: 3,
		AfterCorrectionRounds:    4,
		Profile:                  "challenge",
	}
	wantFailureActions := ReviewFailureActions{
		RequiredLaneFailure: ReviewActionFail,
		VerificationFailure: ReviewActionInconclusive,
		BudgetExhaustion:    ReviewActionEscalate,
	}
	if got := cfg.ReviewPolicy.Escalation; !reflect.DeepEqual(got, wantEscalation) {
		t.Fatalf("ReviewPolicy.Escalation = %#v, want %#v", got, wantEscalation)
	}
	if got := cfg.ReviewPolicy.FailureActions; !reflect.DeepEqual(got, wantFailureActions) {
		t.Fatalf("ReviewPolicy.FailureActions = %#v, want %#v", got, wantFailureActions)
	}
	if got, want := cfg.ReviewPolicy.Verification.InconclusiveAction, ReviewActionFail; got != want {
		t.Fatalf("ReviewPolicy.Verification.InconclusiveAction = %q, want %q", got, want)
	}

	encoded, err := yaml.Marshal(struct {
		Escalation     EscalationPolicy     `yaml:"ESCALATION"`
		FailureActions ReviewFailureActions `yaml:"FAILURE_ACTIONS"`
	}{
		Escalation:     cfg.ReviewPolicy.Escalation,
		FailureActions: cfg.ReviewPolicy.FailureActions,
	})
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	var roundTripped struct {
		Escalation     EscalationPolicy     `yaml:"ESCALATION"`
		FailureActions ReviewFailureActions `yaml:"FAILURE_ACTIONS"`
	}
	if err := yaml.Unmarshal(encoded, &roundTripped); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(roundTripped.Escalation, wantEscalation) {
		t.Fatalf("round-tripped escalation = %#v, want %#v", roundTripped.Escalation, wantEscalation)
	}
	if !reflect.DeepEqual(roundTripped.FailureActions, wantFailureActions) {
		t.Fatalf("round-tripped failure actions = %#v, want %#v", roundTripped.FailureActions, wantFailureActions)
	}
}

func TestLoadConfigRejectsInvalidReviewLimits(t *testing.T) {
	tests := []struct {
		name      string
		settings  string
		wantError string
	}{
		{
			name:      "zero review agent budget",
			settings:  "  CONVERGENCE:\n    MAX_REVIEW_AGENTS_PER_SHA: 0\n",
			wantError: "MAX_REVIEW_AGENTS_PER_SHA must be greater than zero",
		},
		{
			name:      "zero wall time budget",
			settings:  "  CONVERGENCE:\n    MAX_WALL_TIME_MINUTES: 0\n",
			wantError: "MAX_WALL_TIME_MINUTES must be greater than zero",
		},
		{
			name:      "zero usage budget",
			settings:  "  CONVERGENCE:\n    MAX_USAGE_TOKENS: 0\n",
			wantError: "MAX_USAGE_TOKENS must be greater than zero",
		},
		{
			name:      "zero escalation round",
			settings:  "  ESCALATION:\n    AFTER_NON_CONVERGING_ROUNDS: 0\n",
			wantError: "AFTER_NON_CONVERGING_ROUNDS must be greater than zero",
		},
		{
			name:      "zero correction round",
			settings:  "  ESCALATION:\n    AFTER_CORRECTION_ROUNDS: 0\n",
			wantError: "AFTER_CORRECTION_ROUNDS must be greater than zero",
		},
		{
			name:      "agent budget cannot run minimum pipeline",
			settings:  "  CONVERGENCE:\n    MAX_REVIEW_AGENTS_PER_SHA: 4\n",
			wantError: "must be at least 10 for discovery, synthesis, retries, verifier fan-out, and rediscovery",
		},
		{
			name: "agent budget cannot run every required lane",
			settings: `  REVIEW_SWARM:
    MIN_REVIEWERS: 1
    MAX_REVIEWERS: 4
    MAX_PARALLEL_REVIEWERS: 4
    LANES:
      - NAME: contract
        REQUIRED: true
        PROFILE: discovery
      - NAME: lifecycle
        REQUIRED: true
        PROFILE: discovery
      - NAME: concurrency-ordering
        REQUIRED: true
        PROFILE: discovery
      - NAME: operations-tests
        REQUIRED: true
        PROFILE: discovery
  VERIFICATION:
    MIN_VERIFIERS: 1
  CONVERGENCE:
    MAX_REVIEW_AGENTS_PER_SHA: 3
`,
			wantError: "must be at least 12 for discovery, synthesis, retries, verifier fan-out, and rediscovery",
		},
		{
			name: "escalation exceeds convergence rounds",
			settings: `  CONVERGENCE:
    MAX_ROUNDS: 3
  ESCALATION:
    AFTER_NON_CONVERGING_ROUNDS: 4
`,
			wantError: "AFTER_NON_CONVERGING_ROUNDS must be <= REVIEW_POLICY.CONVERGENCE.MAX_ROUNDS",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			policy := validReviewPolicyConfig() + test.settings
			cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))

			_, err := loadConfig(cfgPath)
			if err == nil {
				t.Fatal("expected configuration error")
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("loadConfig() error = %q, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestValidateReviewLimitsRejectsOverflowingMinimumWorkerTotal(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	policy := ReviewPolicy{
		Swarm: ReviewSwarmPolicy{
			MinReviewers: 1,
			Lanes: []ReviewLane{
				{Required: true},
				{Required: true},
				{Required: true},
				{Required: true},
			},
		},
		Verification: VerificationPolicy{
			MinVerifiers: maxInt,
		},
		Convergence: ConvergencePolicy{
			MaxReviewAgentsPerSHA: maxInt,
		},
	}

	err := validateReviewLimits(policy)
	if err == nil {
		t.Fatal("expected configuration error")
	}
	if !strings.Contains(err.Error(), "MAX_REVIEW_AGENTS_PER_SHA must be at least") {
		t.Fatalf("validateReviewLimits() error = %q, want agent budget error", err)
	}
}

func TestLoadConfigRejectsInvalidReviewActions(t *testing.T) {
	tests := []struct {
		name     string
		settings string
		field    string
	}{
		{
			name:     "verification inconclusive action",
			settings: "  VERIFICATION:\n    INCONCLUSIVE_ACTION: approve\n",
			field:    "REVIEW_POLICY.VERIFICATION.INCONCLUSIVE_ACTION",
		},
		{
			name:     "required lane failure action",
			settings: "  FAILURE_ACTIONS:\n    REQUIRED_LANE_FAILURE: approve\n",
			field:    "REVIEW_POLICY.FAILURE_ACTIONS.REQUIRED_LANE_FAILURE",
		},
		{
			name:     "verification failure action",
			settings: "  FAILURE_ACTIONS:\n    VERIFICATION_FAILURE: approve\n",
			field:    "REVIEW_POLICY.FAILURE_ACTIONS.VERIFICATION_FAILURE",
		},
		{
			name:     "budget exhaustion action",
			settings: "  FAILURE_ACTIONS:\n    BUDGET_EXHAUSTION: approve\n",
			field:    "REVIEW_POLICY.FAILURE_ACTIONS.BUDGET_EXHAUSTION",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			policy := validReviewPolicyConfig() + test.settings
			cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))

			_, err := loadConfig(cfgPath)
			if err == nil {
				t.Fatal("expected configuration error")
			}
			if !strings.Contains(err.Error(), test.field) ||
				!strings.Contains(err.Error(), "unsupported action") {
				t.Fatalf("loadConfig() error = %q, want clear error for %s", err, test.field)
			}
		})
	}
}

func TestLoadConfigRejectsInvalidEscalationProfileReferences(t *testing.T) {
	tests := []struct {
		name      string
		profile   string
		wantError string
	}{
		{
			name:      "empty reference",
			profile:   `""`,
			wantError: "ESCALATION.PROFILE must reference a named profile",
		},
		{
			name:      "unknown reference",
			profile:   "missing",
			wantError: `ESCALATION.PROFILE references unknown profile "missing"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			policy := validReviewPolicyConfig() +
				"  ESCALATION:\n    PROFILE: " + test.profile + "\n"
			cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))

			_, err := loadConfig(cfgPath)
			if err == nil {
				t.Fatal("expected configuration error")
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("loadConfig() error = %q, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestLoadConfigStrictlyParsesEveryReviewLimitField(t *testing.T) {
	tests := []struct {
		name     string
		settings string
	}{
		{name: "escalation rounds", settings: "  ESCALATION:\n    AFTER_NON_CONVERGING_ROUNDS: invalid\n"},
		{name: "correction rounds", settings: "  ESCALATION:\n    AFTER_CORRECTION_ROUNDS: invalid\n"},
		{name: "escalation profile", settings: "  ESCALATION:\n    PROFILE: []\n"},
		{name: "required lane failure", settings: "  FAILURE_ACTIONS:\n    REQUIRED_LANE_FAILURE: []\n"},
		{name: "verification failure", settings: "  FAILURE_ACTIONS:\n    VERIFICATION_FAILURE: []\n"},
		{name: "budget exhaustion", settings: "  FAILURE_ACTIONS:\n    BUDGET_EXHAUSTION: []\n"},
		{name: "unknown escalation field", settings: "  ESCALATION:\n    RETRIES: 2\n"},
		{name: "unknown failure action", settings: "  FAILURE_ACTIONS:\n    CHALLENGE_FAILURE: fail\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			policy := validReviewPolicyConfig() + test.settings
			cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))

			_, err := loadConfig(cfgPath)
			if err == nil {
				t.Fatal("expected strict configuration parsing error")
			}
			if !strings.Contains(err.Error(), "failed to decode config YAML") {
				t.Fatalf("loadConfig() error = %q, want strict YAML decoding error", err)
			}
		})
	}
}
