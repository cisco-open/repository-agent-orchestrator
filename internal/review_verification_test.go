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
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadConfigVerificationAndConvergenceDefaults(t *testing.T) {
	tests := []struct {
		name   string
		policy string
	}{
		{name: "review policy omitted"},
		{name: "settings omitted", policy: validReviewPolicyConfig()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(test.policy))

			cfg, err := loadConfig(cfgPath)
			if err != nil {
				t.Fatalf("loadConfig() error = %v", err)
			}
			if got, want := cfg.ReviewPolicy.Verification, defaultVerificationPolicy(); !reflect.DeepEqual(got, want) {
				t.Fatalf("ReviewPolicy.Verification = %#v, want %#v", got, want)
			}
			if got, want := cfg.ReviewPolicy.Convergence, defaultConvergencePolicy(); !reflect.DeepEqual(got, want) {
				t.Fatalf("ReviewPolicy.Convergence = %#v, want %#v", got, want)
			}
		})
	}
}

func TestLoadConfigVerificationAndConvergenceRoundTrip(t *testing.T) {
	setRequiredEnv(t)
	policy := validReviewPolicyConfig() + `  VERIFICATION:
    MIN_VERIFIERS: 2
    MAX_VERIFIERS: 5
    MAX_PARALLEL_VERIFIERS: 4
    TIMEOUT_MINUTES: 45
    RETRIES: 0
    REQUIRE_EVIDENCE: false
    INCONCLUSIVE_ACTION: fail
  CONVERGENCE:
    QUIET_ROUNDS_REQUIRED: 3
    MAX_ROUNDS: 7
    MAX_REVIEW_AGENTS_PER_SHA: 26
    MAX_WALL_TIME_MINUTES: 120
    MAX_USAGE_TOKENS: 3000000
`
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	wantVerification := VerificationPolicy{
		MinVerifiers:         2,
		MaxVerifiers:         5,
		MaxParallelVerifiers: 4,
		TimeoutMinutes:       45,
		Retries:              0,
		RequireEvidence:      false,
		InconclusiveAction:   ReviewActionFail,
	}
	if got := cfg.ReviewPolicy.Verification; !reflect.DeepEqual(got, wantVerification) {
		t.Fatalf("ReviewPolicy.Verification = %#v, want %#v", got, wantVerification)
	}
	wantConvergence := ConvergencePolicy{
		QuietRoundsRequired:   3,
		MaxRounds:             7,
		MaxReviewAgentsPerSHA: 26,
		MaxWallTimeMinutes:    120,
		MaxUsageTokens:        3_000_000,
	}
	if got := cfg.ReviewPolicy.Convergence; !reflect.DeepEqual(got, wantConvergence) {
		t.Fatalf("ReviewPolicy.Convergence = %#v, want %#v", got, wantConvergence)
	}

	encoded, err := yaml.Marshal(struct {
		Verification VerificationPolicy `yaml:"VERIFICATION"`
		Convergence  ConvergencePolicy  `yaml:"CONVERGENCE"`
	}{
		Verification: cfg.ReviewPolicy.Verification,
		Convergence:  cfg.ReviewPolicy.Convergence,
	})
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	var roundTripped struct {
		Verification VerificationPolicy `yaml:"VERIFICATION"`
		Convergence  ConvergencePolicy  `yaml:"CONVERGENCE"`
	}
	if err := yaml.Unmarshal(encoded, &roundTripped); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(roundTripped.Verification, wantVerification) {
		t.Fatalf("round-tripped verification = %#v, want %#v", roundTripped.Verification, wantVerification)
	}
	if !reflect.DeepEqual(roundTripped.Convergence, wantConvergence) {
		t.Fatalf("round-tripped convergence = %#v, want %#v", roundTripped.Convergence, wantConvergence)
	}
}

func TestLoadConfigRejectsInvalidVerificationAndConvergenceBounds(t *testing.T) {
	tests := []struct {
		name      string
		settings  string
		wantError string
	}{
		{
			name:      "zero minimum verifiers",
			settings:  "  VERIFICATION:\n    MIN_VERIFIERS: 0\n",
			wantError: "MIN_VERIFIERS must be greater than zero",
		},
		{
			name:      "zero maximum verifiers",
			settings:  "  VERIFICATION:\n    MAX_VERIFIERS: 0\n",
			wantError: "MAX_VERIFIERS must be greater than zero",
		},
		{
			name:      "zero parallel verifiers",
			settings:  "  VERIFICATION:\n    MAX_PARALLEL_VERIFIERS: 0\n",
			wantError: "MAX_PARALLEL_VERIFIERS must be greater than zero",
		},
		{
			name:      "zero timeout",
			settings:  "  VERIFICATION:\n    TIMEOUT_MINUTES: 0\n",
			wantError: "TIMEOUT_MINUTES must be greater than zero",
		},
		{
			name:      "negative retries",
			settings:  "  VERIFICATION:\n    RETRIES: -1\n",
			wantError: "RETRIES must not be negative",
		},
		{
			name: "minimum exceeds maximum",
			settings: `  VERIFICATION:
    MIN_VERIFIERS: 4
    MAX_VERIFIERS: 3
`,
			wantError: "MIN_VERIFIERS must be <= MAX_VERIFIERS",
		},
		{
			name: "parallelism exceeds maximum",
			settings: `  VERIFICATION:
    MAX_VERIFIERS: 3
    MAX_PARALLEL_VERIFIERS: 4
`,
			wantError: "MAX_PARALLEL_VERIFIERS must be <= MAX_VERIFIERS",
		},
		{
			name:      "zero quiet rounds",
			settings:  "  CONVERGENCE:\n    QUIET_ROUNDS_REQUIRED: 0\n",
			wantError: "QUIET_ROUNDS_REQUIRED must be greater than zero",
		},
		{
			name:      "zero maximum rounds",
			settings:  "  CONVERGENCE:\n    MAX_ROUNDS: 0\n",
			wantError: "MAX_ROUNDS must be greater than zero",
		},
		{
			name: "quiet rounds exceed maximum",
			settings: `  CONVERGENCE:
    QUIET_ROUNDS_REQUIRED: 5
    MAX_ROUNDS: 4
`,
			wantError: "QUIET_ROUNDS_REQUIRED must be <= MAX_ROUNDS",
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

func TestLoadConfigStrictlyParsesEveryVerificationAndConvergenceField(t *testing.T) {
	tests := []struct {
		name     string
		settings string
	}{
		{name: "minimum verifiers", settings: "  VERIFICATION:\n    MIN_VERIFIERS: invalid\n"},
		{name: "maximum verifiers", settings: "  VERIFICATION:\n    MAX_VERIFIERS: invalid\n"},
		{name: "parallel verifiers", settings: "  VERIFICATION:\n    MAX_PARALLEL_VERIFIERS: invalid\n"},
		{name: "timeout", settings: "  VERIFICATION:\n    TIMEOUT_MINUTES: invalid\n"},
		{name: "retries", settings: "  VERIFICATION:\n    RETRIES: invalid\n"},
		{name: "evidence", settings: "  VERIFICATION:\n    REQUIRE_EVIDENCE: invalid\n"},
		{name: "inconclusive action", settings: "  VERIFICATION:\n    INCONCLUSIVE_ACTION: []\n"},
		{name: "quiet rounds", settings: "  CONVERGENCE:\n    QUIET_ROUNDS_REQUIRED: invalid\n"},
		{name: "maximum rounds", settings: "  CONVERGENCE:\n    MAX_ROUNDS: invalid\n"},
		{name: "maximum review agents", settings: "  CONVERGENCE:\n    MAX_REVIEW_AGENTS_PER_SHA: invalid\n"},
		{name: "maximum wall time", settings: "  CONVERGENCE:\n    MAX_WALL_TIME_MINUTES: invalid\n"},
		{name: "maximum usage", settings: "  CONVERGENCE:\n    MAX_USAGE_TOKENS: invalid\n"},
		{name: "unknown verification field", settings: "  VERIFICATION:\n    ASSIGNMENT_MODE: random\n"},
		{name: "unknown convergence field", settings: "  CONVERGENCE:\n    STOP_EARLY: true\n"},
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
