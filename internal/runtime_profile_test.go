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
	"fmt"
	"strings"
	"testing"
)

func TestEffectiveAgentProfilePrecedence(t *testing.T) {
	tests := []struct {
		name          string
		policy        func() ReviewPolicy
		environment   map[string]string
		cliOverride   agentProfileOverride
		wantModel     string
		wantEffort    string
		wantInherited bool
	}{
		{
			name: "built-in default",
			policy: func() ReviewPolicy {
				return ReviewPolicy{}
			},
			wantModel:  "gpt-5.6-sol",
			wantEffort: "high",
		},
		{
			name: "repository profile overrides built-in",
			policy: func() ReviewPolicy {
				return testReviewPolicyWithCoderProfile(AgentProfile{
					Name:  "repository-case",
					Model: "repository-model", ReasoningEffort: "medium",
				})
			},
			wantModel:  "repository-model",
			wantEffort: "medium",
		},
		{
			name: "deployment model and effort override repository",
			policy: func() ReviewPolicy {
				return testReviewPolicyWithCoderProfile(AgentProfile{
					Name:  "deployment-case",
					Model: "repository-model", ReasoningEffort: "medium",
				})
			},
			environment: map[string]string{
				"RAO_PROFILE_DEPLOYMENT_CASE_MODEL":            "deployment-model",
				"RAO_PROFILE_DEPLOYMENT_CASE_REASONING_EFFORT": "xhigh",
			},
			wantModel:  "deployment-model",
			wantEffort: "xhigh",
		},
		{
			name: "deployment model combines with repository effort",
			policy: func() ReviewPolicy {
				return testReviewPolicyWithCoderProfile(AgentProfile{
					Name:  "partial-deployment-case",
					Model: "repository-model", ReasoningEffort: "medium",
				})
			},
			environment: map[string]string{
				"RAO_PROFILE_PARTIAL_DEPLOYMENT_CASE_MODEL": "deployment-model",
			},
			wantModel:  "deployment-model",
			wantEffort: "medium",
		},
		{
			name: "CLI model and effort override deployment and repository",
			policy: func() ReviewPolicy {
				return testReviewPolicyWithCoderProfile(AgentProfile{
					Name:  "cli-case",
					Model: "repository-model", ReasoningEffort: "medium",
				})
			},
			environment: map[string]string{
				"RAO_PROFILE_CLI_CASE_MODEL":            "deployment-model",
				"RAO_PROFILE_CLI_CASE_REASONING_EFFORT": "low",
			},
			cliOverride: agentProfileOverride{
				Model: "cli-model", ReasoningEffort: "xhigh", HasModel: true, HasReasoningEffort: true,
			},
			wantModel:  "cli-model",
			wantEffort: "xhigh",
		},
		{
			name: "CLI effort combines with deployment model",
			policy: func() ReviewPolicy {
				return testReviewPolicyWithCoderProfile(AgentProfile{
					Name:  "partial-cli-case",
					Model: "repository-model", ReasoningEffort: "medium",
				})
			},
			environment: map[string]string{
				"RAO_PROFILE_PARTIAL_CLI_CASE_MODEL": "deployment-model",
			},
			cliOverride: agentProfileOverride{
				ReasoningEffort: "high", HasReasoningEffort: true,
			},
			wantModel:  "deployment-model",
			wantEffort: "high",
		},
		{
			name: "repository explicitly inherits global",
			policy: func() ReviewPolicy {
				return testReviewPolicyWithCoderProfile(AgentProfile{
					Name: "repository-inherit-case", InheritGlobal: true,
				})
			},
			wantInherited: true,
		},
		{
			name: "deployment explicitly inherits global",
			policy: func() ReviewPolicy {
				return testReviewPolicyWithCoderProfile(AgentProfile{
					Name:  "deployment-inherit-case",
					Model: "repository-model", ReasoningEffort: "medium",
				})
			},
			environment: map[string]string{
				"RAO_PROFILE_DEPLOYMENT_INHERIT_CASE_INHERIT_GLOBAL": "true",
			},
			wantInherited: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy := tc.policy()
			for name, value := range tc.environment {
				t.Setenv(name, value)
			}
			if err := applyAgentProfileEnvironmentOverrides(&policy); err != nil {
				t.Fatalf("applyAgentProfileEnvironmentOverrides() error = %v", err)
			}
			policy.runtimeProfileCLIOverride = tc.cliOverride
			if err := validateAgentProfiles(policy); err != nil {
				t.Fatalf("validateAgentProfiles() error = %v", err)
			}

			got, err := policy.effectiveProfileForRole(AgentProfileRoleCoder)
			if err != nil {
				t.Fatalf("effectiveProfileForRole() error = %v", err)
			}
			if got.Model != tc.wantModel || got.ReasoningEffort != tc.wantEffort || got.InheritGlobal != tc.wantInherited {
				t.Fatalf(
					"effectiveProfileForRole() = %+v, want model=%q effort=%q inherit=%v",
					got,
					tc.wantModel,
					tc.wantEffort,
					tc.wantInherited,
				)
			}
		})
	}
}

func TestCLIProfileOverrideRequiresBothValuesForInheritedProfile(t *testing.T) {
	policy := testReviewPolicyWithCoderProfile(AgentProfile{
		Name: "inherited", InheritGlobal: true,
	})
	policy.runtimeProfileCLIOverride = agentProfileOverride{
		Model: "cli-model", HasModel: true,
	}
	_, err := policy.effectiveProfileForRole(AgentProfileRoleCoder)
	if err == nil || !strings.Contains(err.Error(), "set both CLI --model and --reasoning-effort") {
		t.Fatalf("effectiveProfileForRole() error = %v, want inherited-profile conflict guidance", err)
	}
}

func TestAgentProfileValidationRejectsUnsupportedCombinationsBeforeLaunch(t *testing.T) {
	tests := []struct {
		name      string
		profile   AgentProfile
		catalog   map[string]ModelCapability
		wantError string
	}{
		{
			name:      "unknown model",
			profile:   AgentProfile{Name: "coder", Model: "missing-model", ReasoningEffort: "high"},
			catalog:   defaultModelCatalog(),
			wantError: "add the model and its supported REASONING_EFFORTS",
		},
		{
			name:    "unavailable model",
			profile: AgentProfile{Name: "coder", Model: "offline-model", ReasoningEffort: "high"},
			catalog: map[string]ModelCapability{
				"offline-model": {ReasoningEfforts: []string{"high"}, Available: false},
			},
			wantError: "configured as unavailable",
		},
		{
			name:    "unsupported effort for model",
			profile: AgentProfile{Name: "coder", Model: "limited-model", ReasoningEffort: "xhigh"},
			catalog: map[string]ModelCapability{
				"limited-model": {ReasoningEfforts: []string{"low", "high"}, Available: true},
			},
			wantError: "does not support reasoning effort \"xhigh\"; supported efforts: low, high",
		},
		{
			name:      "unsafe model alias",
			profile:   AgentProfile{Name: "coder", Model: "bad;model", ReasoningEffort: "high"},
			catalog:   map[string]ModelCapability{},
			wantError: "not a safe Codex model or alias",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAgentProfile(tc.profile, tc.catalog)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("validateAgentProfile() error = %v, want containing %q", err, tc.wantError)
			}
		})
	}
}

func TestLoadConfigRejectsUnsupportedBuiltInCombinationBeforeLaunch(t *testing.T) {
	setRequiredEnv(t)
	configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
  MODEL_CATALOG:
    gpt-5.6-sol:
      REASONING_EFFORTS: [high]
`))

	_, err := loadConfig(configPath)
	if err == nil || !strings.Contains(err.Error(), `model "gpt-5.6-sol" does not support reasoning effort "xhigh"`) {
		t.Fatalf("loadConfig() error = %v, want actionable unsupported combination", err)
	}
}

func TestLoadConfigAppliesDeploymentOverridesToBuiltInProfile(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RAO_PROFILE_CODER_MODEL", "gpt-5.6-terra")
	t.Setenv("RAO_PROFILE_CODER_REASONING_EFFORT", "low")
	configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
`))

	cfg, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	got, err := cfg.runtimeProfileForRole(AgentProfileRoleCoder)
	if err != nil {
		t.Fatalf("runtimeProfileForRole() error = %v", err)
	}
	want := AgentProfile{Name: "coder", Model: "gpt-5.6-terra", ReasoningEffort: "low"}
	if got != want {
		t.Fatalf("runtimeProfileForRole() = %+v, want %+v", got, want)
	}
}

func TestLoadConfigAllowsFullyCustomRolesWhenBuiltInModelsAreUnavailable(t *testing.T) {
	setRequiredEnv(t)
	configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
  AGENT_PROFILES:
    custom:
      MODEL: custom-model
      REASONING_EFFORT: high
  MODEL_CATALOG:
    custom-model:
      REASONING_EFFORTS: [high]
      AVAILABLE: true
    gpt-5.6-sol:
      REASONING_EFFORTS: [none]
      AVAILABLE: false
    gpt-5.6-terra:
      REASONING_EFFORTS: [none]
      AVAILABLE: false
    gpt-5.6-luna:
      REASONING_EFFORTS: [none]
      AVAILABLE: false
  ROLE_PROFILES:
    CODER: custom
    INDEXER: custom
    DISCOVERY: custom
    VERIFIER: custom
    CHALLENGE: custom
    ESCALATION: custom
`))

	cfg, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	for _, role := range requiredAgentProfileRoles {
		got, err := cfg.runtimeProfileForRole(role)
		if err != nil {
			t.Fatalf("runtimeProfileForRole(%q) error = %v", role, err)
		}
		if got.Model != "custom-model" || got.ReasoningEffort != "high" {
			t.Fatalf("runtimeProfileForRole(%q) = %+v, want custom profile", role, got)
		}
	}
}

func TestLoadConfigAppliesCLIOverridesAfterDeploymentOverrides(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("RAO_PROFILE_CODER_MODEL", "gpt-5.6-terra")
	t.Setenv("RAO_PROFILE_CODER_REASONING_EFFORT", "low")
	configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
`))
	override := agentProfileOverride{
		Model:              "gpt-5.6-sol",
		ReasoningEffort:    "xhigh",
		HasModel:           true,
		HasReasoningEffort: true,
	}

	cfg, err := loadConfigWithRuntimeProfileOverride(configPath, override)
	if err != nil {
		t.Fatalf("loadConfigWithRuntimeProfileOverride() error = %v", err)
	}
	got, err := cfg.runtimeProfileForRole(AgentProfileRoleCoder)
	if err != nil {
		t.Fatalf("runtimeProfileForRole() error = %v", err)
	}
	want := AgentProfile{Name: "coder", Model: "gpt-5.6-sol", ReasoningEffort: "xhigh"}
	if got != want {
		t.Fatalf("runtimeProfileForRole() = %+v, want %+v", got, want)
	}
}

func TestLoadConfigAppliesPartialCLIOverrideOnlyToSelectedProfiles(t *testing.T) {
	setRequiredEnv(t)
	configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
  AGENT_PROFILES:
    selected:
      MODEL: gpt-5.6-terra
      REASONING_EFFORT: high
    unused-inherited:
      INHERIT_GLOBAL: true
  ROLE_PROFILES:
    CODER: selected
    INDEXER: selected
    DISCOVERY: selected
    VERIFIER: selected
    CHALLENGE: selected
    ESCALATION: selected
`))
	override := agentProfileOverride{
		Model:    "gpt-5.6-sol",
		HasModel: true,
	}

	cfg, err := loadConfigWithRuntimeProfileOverride(configPath, override)
	if err != nil {
		t.Fatalf("loadConfigWithRuntimeProfileOverride() error = %v", err)
	}
	if got := cfg.ReviewPolicy.AgentProfiles["unused-inherited"]; !got.InheritGlobal {
		t.Fatalf("unused repository profile = %+v, want unchanged global inheritance", got)
	}
	for _, role := range requiredAgentProfileRoles {
		got, err := cfg.runtimeProfileForRole(role)
		if err != nil {
			t.Fatalf("runtimeProfileForRole(%q) error = %v", role, err)
		}
		if got.Model != "gpt-5.6-sol" || got.ReasoningEffort != "high" || got.InheritGlobal {
			t.Fatalf(
				"runtimeProfileForRole(%q) = %+v, want selected profile with partial CLI override",
				role,
				got,
			)
		}
	}
}

func TestLoadConfigCLIOverridesInvalidSelectedLowerPrecedenceValues(t *testing.T) {
	tests := []struct {
		name             string
		repositoryModel  string
		repositoryEffort string
		deploymentModel  string
		deploymentEffort string
		cliModel         string
		cliEffort        string
		wantModel        string
		wantEffort       string
	}{
		{
			name:             "full override replaces invalid repository values",
			repositoryModel:  "retired-model",
			repositoryEffort: "ultra",
			cliModel:         "replacement-model",
			cliEffort:        "high",
			wantModel:        "replacement-model",
			wantEffort:       "high",
		},
		{
			name:             "partial model override replaces unavailable repository model",
			repositoryModel:  "retired-model",
			repositoryEffort: "high",
			cliModel:         "replacement-model",
			wantModel:        "replacement-model",
			wantEffort:       "high",
		},
		{
			name:             "partial effort override replaces unsupported repository effort",
			repositoryModel:  "limited-model",
			repositoryEffort: "high",
			cliEffort:        "low",
			wantModel:        "limited-model",
			wantEffort:       "low",
		},
		{
			name:             "full override replaces invalid deployment values",
			repositoryModel:  "replacement-model",
			repositoryEffort: "high",
			deploymentModel:  "retired-model",
			deploymentEffort: "ultra",
			cliModel:         "replacement-model",
			cliEffort:        "low",
			wantModel:        "replacement-model",
			wantEffort:       "low",
		},
		{
			name:             "partial model override replaces unavailable deployment model",
			repositoryModel:  "replacement-model",
			repositoryEffort: "high",
			deploymentModel:  "retired-model",
			cliModel:         "replacement-model",
			wantModel:        "replacement-model",
			wantEffort:       "high",
		},
		{
			name:             "partial effort override replaces unsupported deployment effort",
			repositoryModel:  "limited-model",
			repositoryEffort: "low",
			deploymentEffort: "high",
			cliEffort:        "low",
			wantModel:        "limited-model",
			wantEffort:       "low",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setRequiredEnv(t)
			if tc.deploymentModel != "" {
				t.Setenv("RAO_PROFILE_SELECTED_MODEL", tc.deploymentModel)
			}
			if tc.deploymentEffort != "" {
				t.Setenv("RAO_PROFILE_SELECTED_REASONING_EFFORT", tc.deploymentEffort)
			}
			configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(fmt.Sprintf(`REVIEW_POLICY:
  VERSION: 1
  AGENT_PROFILES:
    selected:
      MODEL: %s
      REASONING_EFFORT: %s
  MODEL_CATALOG:
    replacement-model:
      REASONING_EFFORTS: [low, high]
    retired-model:
      REASONING_EFFORTS: [high]
      AVAILABLE: false
    limited-model:
      REASONING_EFFORTS: [low]
  ROLE_PROFILES:
    CODER: selected
    INDEXER: selected
    DISCOVERY: selected
    VERIFIER: selected
    CHALLENGE: selected
    ESCALATION: selected
`, tc.repositoryModel, tc.repositoryEffort)))
			override := agentProfileOverride{
				Model:              tc.cliModel,
				ReasoningEffort:    tc.cliEffort,
				HasModel:           tc.cliModel != "",
				HasReasoningEffort: tc.cliEffort != "",
			}

			cfg, err := loadConfigWithRuntimeProfileOverride(configPath, override)
			if err != nil {
				t.Fatalf("loadConfigWithRuntimeProfileOverride() error = %v", err)
			}
			for _, role := range requiredAgentProfileRoles {
				got, err := cfg.runtimeProfileForRole(role)
				if err != nil {
					t.Fatalf("runtimeProfileForRole(%q) error = %v", role, err)
				}
				if got.Model != tc.wantModel || got.ReasoningEffort != tc.wantEffort {
					t.Fatalf(
						"runtimeProfileForRole(%q) = %+v, want model=%q effort=%q",
						role,
						got,
						tc.wantModel,
						tc.wantEffort,
					)
				}
			}
		})
	}
}

func TestLoadConfigCLIOverrideDoesNotMaskInvalidUnusedRepositoryProfile(t *testing.T) {
	setRequiredEnv(t)
	configPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
  AGENT_PROFILES:
    selected:
      MODEL: replacement-model
      REASONING_EFFORT: high
    unused:
      MODEL: retired-model
      REASONING_EFFORT: high
  MODEL_CATALOG:
    replacement-model:
      REASONING_EFFORTS: [low, high]
    retired-model:
      REASONING_EFFORTS: [high]
      AVAILABLE: false
  ROLE_PROFILES:
    CODER: selected
    INDEXER: selected
    DISCOVERY: selected
    VERIFIER: selected
    CHALLENGE: selected
    ESCALATION: selected
`))
	override := agentProfileOverride{
		Model:              "replacement-model",
		ReasoningEffort:    "low",
		HasModel:           true,
		HasReasoningEffort: true,
	}

	_, err := loadConfigWithRuntimeProfileOverride(configPath, override)
	if err == nil || !strings.Contains(
		err.Error(),
		`REVIEW_POLICY.AGENT_PROFILES.unused: model "retired-model" is configured as unavailable`,
	) {
		t.Fatalf("loadConfigWithRuntimeProfileOverride() error = %v, want invalid unused profile", err)
	}
}

func TestCodexCommandForAgentProfileAppendsRoleSpecificArguments(t *testing.T) {
	policy := testReviewPolicyWithCoderProfile(AgentProfile{
		Name:  "coder-runtime",
		Model: "repository-model", ReasoningEffort: "xhigh",
	})
	profile, err := policy.effectiveProfileForRole(AgentProfileRoleCoder)
	if err != nil {
		t.Fatalf("effectiveProfileForRole() error = %v", err)
	}

	base := "codex --model global-model -c 'model_reasoning_effort=\"low\"'"
	got, err := codexCommandForAgentProfile(base, profile)
	if err != nil {
		t.Fatalf("codexCommandForAgentProfile() error = %v", err)
	}
	want := "codex -c 'model_reasoning_effort=\"low\"' --model 'repository-model' -c 'model_reasoning_effort=\"xhigh\"'"
	if got != want {
		t.Fatalf("runtime command = %q, want %q", got, want)
	}

	inherited, err := codexCommandForAgentProfile(base, AgentProfile{InheritGlobal: true})
	if err != nil || inherited != base {
		t.Fatalf("inherited runtime command = (%q, %v), want unchanged base", inherited, err)
	}
}

func testReviewPolicyWithCoderProfile(profile AgentProfile) ReviewPolicy {
	policy := builtInReviewPolicy()
	policy.AgentProfiles[profile.Name] = profile
	policy.RoleProfiles[AgentProfileRoleCoder] = profile.Name
	for _, model := range []string{"repository-model", "deployment-model", "cli-model"} {
		policy.ModelCatalog[model] = ModelCapability{
			ReasoningEfforts: []string{"low", "medium", "high", "xhigh"},
			Available:        true,
		}
	}
	return policy
}
