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
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadConfigReviewSwarmDefaults(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(validReviewPolicyConfig()))

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	swarm := cfg.ReviewPolicy.Swarm
	if got, want := swarm.MinReviewers, defaultReviewMinReviewers; got != want {
		t.Fatalf("MinReviewers = %d, want %d", got, want)
	}
	if got, want := swarm.MaxReviewers, defaultReviewMaxReviewers; got != want {
		t.Fatalf("MaxReviewers = %d, want %d", got, want)
	}
	if got, want := swarm.MaxParallelReviewers, defaultReviewParallelReviewers; got != want {
		t.Fatalf("MaxParallelReviewers = %d, want %d", got, want)
	}
	if got, want := swarm.TimeoutMinutes, defaultReviewWorkerTimeout; got != want {
		t.Fatalf("TimeoutMinutes = %d, want %d", got, want)
	}
	if got, want := swarm.Retries, defaultReviewWorkerRetries; got != want {
		t.Fatalf("Retries = %d, want %d", got, want)
	}
	if got, want := len(swarm.Lanes), len(supportedReviewLaneNames); got != want {
		t.Fatalf("len(Lanes) = %d, want %d", got, want)
	}
	for _, lane := range swarm.Lanes {
		if got, want := lane.Profile, "discovery"; got != want {
			t.Fatalf("lane %q profile = %q, want %q", lane.Name, got, want)
		}
	}
}

func TestExampleConfigsUseConvergentReview(t *testing.T) {
	setRequiredEnv(t)

	tests := []struct{ path string }{
		{path: "sample.yaml"},
		{path: "sample-swarm-minimal.yaml"},
		{path: "sample-swarm.yaml"},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			path := filepath.Join("..", "config", test.path)
			cfg, err := loadConfig(path)
			if err != nil {
				t.Fatalf("loadConfig(%s) error = %v", path, err)
			}
			if got, want := len(cfg.ReviewPolicy.Swarm.Lanes), len(supportedReviewLaneNames); got != want {
				t.Fatalf("len(lanes) = %d, want %d", got, want)
			}
		})
	}
}

func TestLoadConfigReviewSwarmRoundTripPreservesRequiredAndOptionalLanes(t *testing.T) {
	setRequiredEnv(t)
	policy := validReviewPolicyConfig() + `  REVIEW_SWARM:
    MIN_REVIEWERS: 2
    MAX_REVIEWERS: 3
    MAX_PARALLEL_REVIEWERS: 2
    TIMEOUT_MINUTES: 45
    RETRIES: 2
    LANES:
      - NAME: contract
        REQUIRED: true
        PROFILE: discovery
      - NAME: concurrency-ordering
        REQUIRED: false
        PROFILE: verification
      - NAME: operations-tests
        REQUIRED: true
        PROFILE: challenge
  CONVERGENCE:
    MAX_REVIEW_AGENTS_PER_SHA: 20
`
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	want := ReviewSwarmPolicy{
		MinReviewers:         2,
		MaxReviewers:         3,
		MaxParallelReviewers: 2,
		TimeoutMinutes:       45,
		Retries:              2,
		Lanes: []ReviewLane{
			{Name: "contract", Required: true, Profile: "discovery"},
			{Name: "concurrency-ordering", Required: false, Profile: "verification"},
			{Name: "operations-tests", Required: true, Profile: "challenge"},
		},
	}
	if got := cfg.ReviewPolicy.Swarm; !reflect.DeepEqual(got, want) {
		t.Fatalf("ReviewPolicy.Swarm = %#v, want %#v", got, want)
	}

	encoded, err := yaml.Marshal(cfg.ReviewPolicy.Swarm)
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	if !strings.Contains(string(encoded), "REQUIRED: false") {
		t.Fatalf("yaml.Marshal() output = %q, want explicit REQUIRED: false", encoded)
	}
	var roundTripped ReviewSwarmPolicy
	if err := yaml.Unmarshal(encoded, &roundTripped); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(roundTripped, want) {
		t.Fatalf("round-tripped swarm = %#v, want %#v", roundTripped, want)
	}
}

func TestLoadConfigRejectsInvalidReviewSwarm(t *testing.T) {
	tests := []struct {
		name      string
		swarm     string
		wantError string
	}{
		{
			name:      "zero minimum reviewers",
			swarm:     "    MIN_REVIEWERS: 0\n",
			wantError: "MIN_REVIEWERS must be greater than zero",
		},
		{
			name:      "zero maximum reviewers",
			swarm:     "    MAX_REVIEWERS: 0\n",
			wantError: "MAX_REVIEWERS must be greater than zero",
		},
		{
			name:      "zero parallel reviewers",
			swarm:     "    MAX_PARALLEL_REVIEWERS: 0\n",
			wantError: "MAX_PARALLEL_REVIEWERS must be greater than zero",
		},
		{
			name:      "zero worker timeout",
			swarm:     "    TIMEOUT_MINUTES: 0\n",
			wantError: "TIMEOUT_MINUTES must be greater than zero",
		},
		{
			name:      "negative worker retries",
			swarm:     "    RETRIES: -1\n",
			wantError: "RETRIES must not be negative",
		},
		{
			name: "minimum exceeds maximum",
			swarm: `    MIN_REVIEWERS: 4
    MAX_REVIEWERS: 3
`,
			wantError: "MIN_REVIEWERS must be <= MAX_REVIEWERS",
		},
		{
			name: "parallelism exceeds maximum",
			swarm: `    MAX_REVIEWERS: 3
    MAX_PARALLEL_REVIEWERS: 4
`,
			wantError: "MAX_PARALLEL_REVIEWERS must be <= MAX_REVIEWERS",
		},
		{
			name: "too few configured lanes",
			swarm: `    MIN_REVIEWERS: 2
    MAX_REVIEWERS: 2
    MAX_PARALLEL_REVIEWERS: 1
    LANES:
      - NAME: contract
        REQUIRED: true
        PROFILE: discovery
`,
			wantError: "configures 1 lanes but MIN_REVIEWERS is 2",
		},
		{
			name: "required lanes exceed maximum",
			swarm: `    MIN_REVIEWERS: 1
    MAX_REVIEWERS: 1
    MAX_PARALLEL_REVIEWERS: 1
    LANES:
      - NAME: contract
        REQUIRED: true
        PROFILE: discovery
      - NAME: lifecycle
        REQUIRED: true
        PROFILE: discovery
`,
			wantError: "has 2 required lanes but MAX_REVIEWERS is 1",
		},
		{
			name: "duplicate normalized lane",
			swarm: `    MIN_REVIEWERS: 1
    LANES:
      - NAME: contract
        REQUIRED: true
        PROFILE: discovery
      - NAME: " Contract "
        REQUIRED: false
        PROFILE: discovery
`,
			wantError: `contains duplicate lane "contract"`,
		},
		{
			name: "unknown lane",
			swarm: `    MIN_REVIEWERS: 1
    LANES:
      - NAME: vibes
        REQUIRED: true
        PROFILE: discovery
`,
			wantError: `contains unknown lane "vibes"`,
		},
		{
			name: "missing required flag",
			swarm: `    MIN_REVIEWERS: 1
    LANES:
      - NAME: contract
        PROFILE: discovery
`,
			wantError: `lane "contract" must specify REQUIRED`,
		},
		{
			name: "null required flag",
			swarm: `    MIN_REVIEWERS: 1
    LANES:
      - NAME: contract
        REQUIRED: null
        PROFILE: discovery
`,
			wantError: `lane "contract" must specify REQUIRED`,
		},
		{
			name: "missing profile reference",
			swarm: `    MIN_REVIEWERS: 1
    LANES:
      - NAME: contract
        REQUIRED: true
`,
			wantError: `lane "contract" must reference a named profile`,
		},
		{
			name: "unknown profile reference",
			swarm: `    MIN_REVIEWERS: 1
    LANES:
      - NAME: contract
        REQUIRED: true
        PROFILE: missing
`,
			wantError: `lane "contract" references unknown profile "missing"`,
		},
		{
			name: "unknown lane field",
			swarm: `    MIN_REVIEWERS: 1
    LANES:
      - NAME: contract
        ENABLED: true
        REQUIRED: true
        PROFILE: discovery
`,
			wantError: "field ENABLED not found",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			policy := validReviewPolicyConfig() + "  REVIEW_SWARM:\n" + test.swarm
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
