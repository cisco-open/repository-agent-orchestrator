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
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeRepoConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "repo.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	return path
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("WEBEX_WEBHOOK_URL", "https://example.test/webhook")
}

func repoConfigWithReviewPolicy(policy string) string {
	return "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\n" + policy
}

func validReviewPolicyConfig() string {
	return `REVIEW_POLICY:
  VERSION: 1
  AGENT_PROFILES:
    coding:
      MODEL: coder-model
      REASONING_EFFORT: high
    indexing:
      MODEL: indexer-model
      REASONING_EFFORT: medium
    discovery:
      MODEL: discovery-model
      REASONING_EFFORT: high
    verification:
      MODEL: verifier-model
      REASONING_EFFORT: xhigh
    challenge:
      MODEL: challenge-model
      REASONING_EFFORT: max
    escalation:
      MODEL: escalation-model
      REASONING_EFFORT: max
  MODEL_CATALOG:
    coder-model:
      REASONING_EFFORTS: [high]
    indexer-model:
      REASONING_EFFORTS: [medium]
    discovery-model:
      REASONING_EFFORTS: [high]
    verifier-model:
      REASONING_EFFORTS: [xhigh]
    challenge-model:
      REASONING_EFFORTS: [max]
    escalation-model:
      REASONING_EFFORTS: [max]
  ROLE_PROFILES:
    CODER: coding
    INDEXER: indexing
    DISCOVERY: discovery
    VERIFIER: verification
    CHALLENGE: challenge
    ESCALATION: escalation
`
}

func TestLoadConfigDefaults(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WORKTREE_DIR", "")
	t.Setenv("POLL_INTERVAL_SECONDS", "")
	t.Setenv("BASE_BRANCH", "")
	t.Setenv("CODEX_CMD", "")

	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test-all\n  - make test-coverage\n")
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if got, want := cfg.WorktreeDir, filepath.Join("/tmp/repo", ".worktrees"); got != want {
		t.Fatalf("WorktreeDir = %q, want %q", got, want)
	}
	if got, want := cfg.LogDir, filepath.Join(orchestratorLogDir, "widget"); got != want {
		t.Fatalf("LogDir = %q, want %q", got, want)
	}
	if got, want := cfg.PollIntervalSeconds, 20; got != want {
		t.Fatalf("PollIntervalSeconds = %d, want %d", got, want)
	}
	if got, want := cfg.BaseBranch, "main"; got != want {
		t.Fatalf("BaseBranch = %q, want %q", got, want)
	}
	if got, want := cfg.MergeMethod, "squash"; got != want {
		t.Fatalf("MergeMethod = %q, want %q", got, want)
	}
	if got, want := cfg.CodexCmd, defaultCodexCmd; got != want {
		t.Fatalf("CodexCmd = %q, want %q", got, want)
	}
	if got, want := cfg.MaxStoredHandoffs, defaultMaxStoredHandoffs; got != want {
		t.Fatalf("MaxStoredHandoffs = %d, want %d", got, want)
	}
	if got, want := cfg.MaxHandoffsInContext, defaultMaxHandoffsInContext; got != want {
		t.Fatalf("MaxHandoffsInContext = %d, want %d", got, want)
	}
	if got, want := cfg.ConfigPath, cfgPath; got != want {
		t.Fatalf("ConfigPath = %q, want %q", got, want)
	}
	if got, want := strings.Join(cfg.MandatoryTests, ","), "make test-all,make test-coverage"; got != want {
		t.Fatalf("MandatoryTests = %q, want %q", got, want)
	}
	if got, want := cfg.HardGateMode, HardGateModeParallel; got != want {
		t.Fatalf("HardGateMode = %q, want %q", got, want)
	}
	if got, want := cfg.ReviewGateAlertThreshold, 15*time.Minute; got != want {
		t.Fatalf("ReviewGateAlertThreshold = %s, want %s", got, want)
	}
	if got, want := cfg.ReviewPolicy.Version, supportedReviewPolicyVersion; got != want {
		t.Fatalf("ReviewPolicy.Version = %d, want %d", got, want)
	}
	for _, role := range requiredAgentProfileRoles {
		profile, err := cfg.ReviewPolicy.ProfileForRole(role)
		if err != nil {
			t.Fatalf("ReviewPolicy.ProfileForRole(%q) error = %v", role, err)
		}
		if profile.InheritGlobal || profile.Model == "" ||
			profile.ReasoningEffort == "" {
			t.Fatalf("built-in convergent profile for %q is incomplete: %+v", role, profile)
		}
	}
}

func TestLoadConfigLogPathOverrideUsesRepoDotCraigbot(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nLOG_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if got, want := cfg.LogDir, filepath.Join("/tmp/repo", orchestratorStateDirName); got != want {
		t.Fatalf("LogDir = %q, want %q", got, want)
	}
}

func TestLoadConfigExpandsHomeDirectoryPaths(t *testing.T) {
	setRequiredEnv(t)

	currentUser, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current() error = %v", err)
	}

	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: ~/workdir/scratch\nLOG_PATH: ~"+currentUser.Username+"/workdir/scratch\nMANDATORY_TESTS:\n  - make test\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	wantBasePath := filepath.Join(currentUser.HomeDir, "workdir", "scratch")
	if got := cfg.RepoPath; got != wantBasePath {
		t.Fatalf("RepoPath = %q, want %q", got, wantBasePath)
	}
	if got, want := cfg.WorktreeDir, filepath.Join(wantBasePath, ".worktrees"); got != want {
		t.Fatalf("WorktreeDir = %q, want %q", got, want)
	}
	if got, want := cfg.LogDir, filepath.Join(wantBasePath, orchestratorStateDirName); got != want {
		t.Fatalf("LogDir = %q, want %q", got, want)
	}
}

func TestLoadConfigExpandsBareHomeDirectoryPaths(t *testing.T) {
	setRequiredEnv(t)

	currentUser, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current() error = %v", err)
	}

	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: ~\nLOG_PATH: ~\nMANDATORY_TESTS:\n  - make test\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if got, want := cfg.RepoPath, currentUser.HomeDir; got != want {
		t.Fatalf("RepoPath = %q, want %q", got, want)
	}
	if got, want := cfg.WorktreeDir, filepath.Join(currentUser.HomeDir, ".worktrees"); got != want {
		t.Fatalf("WorktreeDir = %q, want %q", got, want)
	}
	if got, want := cfg.LogDir, filepath.Join(currentUser.HomeDir, orchestratorStateDirName); got != want {
		t.Fatalf("LogDir = %q, want %q", got, want)
	}
}

func TestLoadConfigRejectsNullRepoPath(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: null\nMANDATORY_TESTS:\n  - make test\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected missing configuration error")
	}
	if !strings.Contains(err.Error(), "REPO_PATH (config)") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigPreservesAliasBackedPaths(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: &repo ~/workdir/scratch\nLOG_PATH: *repo\nMANDATORY_TESTS:\n  - make test\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	currentUser, err := user.Current()
	if err != nil {
		t.Fatalf("user.Current() error = %v", err)
	}

	wantBasePath := filepath.Join(currentUser.HomeDir, "workdir", "scratch")
	if got := cfg.RepoPath; got != wantBasePath {
		t.Fatalf("RepoPath = %q, want %q", got, wantBasePath)
	}
	if got, want := cfg.LogDir, filepath.Join(wantBasePath, orchestratorStateDirName); got != want {
		t.Fatalf("LogDir = %q, want %q", got, want)
	}
}

func TestDefaultCodexCmd(t *testing.T) {
	if strings.Contains(defaultCodexCmd, "--full-auto") {
		t.Fatalf("defaultCodexCmd must not include --full-auto: %q", defaultCodexCmd)
	}
	if got, want := defaultCodexCmd, "codex --ask-for-approval never --sandbox danger-full-access -c tui.animations=false -c check_for_update_on_startup=false"; got != want {
		t.Fatalf("defaultCodexCmd = %q, want %q", got, want)
	}
}

func TestLoadConfigInvalidPollInterval(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("POLL_INTERVAL_SECONDS", "nope")
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for invalid poll interval")
	}
	if !strings.Contains(err.Error(), "invalid POLL_INTERVAL_SECONDS") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigCodexCmdOverride(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("CODEX_CMD", "codex --profile ci")
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if got, want := cfg.CodexCmd, "codex --profile ci"; got != want {
		t.Fatalf("CodexCmd = %q, want %q", got, want)
	}
}

func TestLoadConfigHandoffOverrides(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nMAX_STORED_HANDOFFS: 42\nMAX_HANDOFFS_IN_CONTEXT: 7\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if got, want := cfg.MaxStoredHandoffs, 42; got != want {
		t.Fatalf("MaxStoredHandoffs = %d, want %d", got, want)
	}
	if got, want := cfg.MaxHandoffsInContext, 7; got != want {
		t.Fatalf("MaxHandoffsInContext = %d, want %d", got, want)
	}
}

func TestLoadConfigReviewGateAlertOverride(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nREVIEW_GATE_ALERT_MINUTES: 30\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if got, want := cfg.ReviewGateAlertThreshold, 30*time.Minute; got != want {
		t.Fatalf("ReviewGateAlertThreshold = %s, want %s", got, want)
	}
}

func TestLoadConfigReviewPolicy(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(validReviewPolicyConfig()))
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if got, want := cfg.ReviewPolicy.Version, supportedReviewPolicyVersion; got != want {
		t.Fatalf("ReviewPolicy.Version = %d, want %d", got, want)
	}
}

func TestLoadConfigRejectsObsoleteReviewPolicyEnabledField(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
  ENABLED: false
`))
	if _, err := loadConfig(cfgPath); err == nil ||
		!strings.Contains(err.Error(), "field ENABLED not found") {
		t.Fatalf("loadConfig() error = %v, want obsolete ENABLED rejection", err)
	}
}

func TestLoadConfigResolvesNamedProfileForEveryRequiredRole(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(validReviewPolicyConfig()))

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	expected := map[AgentProfileRole]AgentProfile{
		AgentProfileRoleCoder:      {Name: "coding", Model: "coder-model", ReasoningEffort: "high"},
		AgentProfileRoleIndexer:    {Name: "indexing", Model: "indexer-model", ReasoningEffort: "medium"},
		AgentProfileRoleDiscovery:  {Name: "discovery", Model: "discovery-model", ReasoningEffort: "high"},
		AgentProfileRoleVerifier:   {Name: "verification", Model: "verifier-model", ReasoningEffort: "xhigh"},
		AgentProfileRoleChallenge:  {Name: "challenge", Model: "challenge-model", ReasoningEffort: "max"},
		AgentProfileRoleEscalation: {Name: "escalation", Model: "escalation-model", ReasoningEffort: "max"},
	}
	for _, role := range requiredAgentProfileRoles {
		got, err := cfg.ReviewPolicy.ProfileForRole(role)
		if err != nil {
			t.Fatalf("ReviewPolicy.ProfileForRole(%q) error = %v", role, err)
		}
		if want := expected[role]; got != want {
			t.Fatalf("ReviewPolicy.ProfileForRole(%q) = %+v, want %+v", role, got, want)
		}
	}
}

func TestLoadConfigRejectsGlobalInheritanceForConvergentWorker(
	t *testing.T,
) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(`REVIEW_POLICY:
  VERSION: 1
  AGENT_PROFILES:
    explicit:
      MODEL: gpt-5.6-terra
      REASONING_EFFORT: high
    inherited:
      INHERIT_GLOBAL: true
  ROLE_PROFILES:
    CODER: explicit
    INDEXER: explicit
    DISCOVERY: inherited
    VERIFIER: explicit
    CHALLENGE: explicit
    ESCALATION: explicit
`))

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected configuration error")
	}
	for _, want := range []string{
		"REVIEW_POLICY.ROLE_PROFILES.DISCOVERY",
		`profile "inherited" with INHERIT_GLOBAL`,
		"explicit effective MODEL and REASONING_EFFORT",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf(
				"loadConfig() error = %q, want substring %q",
				err,
				want,
			)
		}
	}

	override := agentProfileOverride{
		Model:              "gpt-5.6-sol",
		ReasoningEffort:    "xhigh",
		HasModel:           true,
		HasReasoningEffort: true,
	}
	cfg, err := loadConfigWithRuntimeProfileOverride(
		cfgPath,
		override,
	)
	if err != nil {
		t.Fatalf(
			"loadConfigWithRuntimeProfileOverride() error = %v",
			err,
		)
	}
	profile, err := cfg.ReviewPolicy.effectiveProfileForReviewWorker(
		ReviewWorkerIdentity{
			Role: AgentProfileRoleDiscovery,
			Lane: "contract",
		},
	)
	if err != nil {
		t.Fatalf(
			"effectiveProfileForReviewWorker() error = %v",
			err,
		)
	}
	if profile.InheritGlobal ||
		profile.Model != override.Model ||
		profile.ReasoningEffort != override.ReasoningEffort {
		t.Fatalf(
			"materialized convergent worker profile = %+v, want model=%q effort=%q",
			profile,
			override.Model,
			override.ReasoningEffort,
		)
	}
}

func TestLoadConfigRejectsInvalidAgentProfileConfiguration(t *testing.T) {
	validInheritedProfile := `  AGENT_PROFILES:
    inherited:
      INHERIT_GLOBAL: true
`
	validRoleProfiles := `  ROLE_PROFILES:
    CODER: inherited
    INDEXER: inherited
    DISCOVERY: inherited
    VERIFIER: inherited
    CHALLENGE: inherited
    ESCALATION: inherited
`
	policyHeader := "REVIEW_POLICY:\n  VERSION: 1\n"

	tests := []struct {
		name      string
		policy    string
		wantError string
	}{
		{
			name:      "missing profile definitions",
			policy:    policyHeader + validRoleProfiles,
			wantError: "REVIEW_POLICY.AGENT_PROFILES must be configured",
		},
		{
			name:      "missing role references",
			policy:    policyHeader + validInheritedProfile,
			wantError: "REVIEW_POLICY.ROLE_PROFILES must be configured",
		},
		{
			name: "missing required role reference",
			policy: policyHeader + validInheritedProfile + `  ROLE_PROFILES:
    CODER: inherited
    INDEXER: inherited
    DISCOVERY: inherited
    VERIFIER: inherited
    CHALLENGE: inherited
`,
			wantError: "REVIEW_POLICY.ROLE_PROFILES.ESCALATION must reference a named profile",
		},
		{
			name: "unknown profile reference",
			policy: policyHeader + validInheritedProfile + strings.Replace(
				validRoleProfiles,
				"CODER: inherited",
				"CODER: missing",
				1,
			),
			wantError: `REVIEW_POLICY.ROLE_PROFILES.CODER references unknown profile "missing"`,
		},
		{
			name: "profile collection is a sequence",
			policy: policyHeader + `  AGENT_PROFILES:
    - inherited
` + validRoleProfiles,
			wantError: "cannot unmarshal !!seq",
		},
		{
			name: "named profile is a scalar",
			policy: policyHeader + `  AGENT_PROFILES:
    inherited: global
` + validRoleProfiles,
			wantError: "cannot unmarshal !!str",
		},
		{
			name: "profile contains unknown field",
			policy: policyHeader + `  AGENT_PROFILES:
    inherited:
      INHERIT_GLOBAL: true
      TEMPERATURE: 0
` + validRoleProfiles,
			wantError: "field TEMPERATURE not found",
		},
		{
			name: "role references are a sequence",
			policy: policyHeader + validInheritedProfile + `  ROLE_PROFILES:
    - inherited
`,
			wantError: "cannot unmarshal !!seq",
		},
		{
			name: "role reference is a mapping",
			policy: policyHeader + validInheritedProfile + `  ROLE_PROFILES:
    CODER:
      PROFILE: inherited
`,
			wantError: "cannot unmarshal !!map",
		},
		{
			name: "role references contain unknown role",
			policy: policyHeader + validInheritedProfile + validRoleProfiles + `    SYNTHESIS: inherited
`,
			wantError: "field SYNTHESIS not found",
		},
		{
			name: "profile omits explicit mode",
			policy: policyHeader + `  AGENT_PROFILES:
    inherited: {}
` + validRoleProfiles,
			wantError: "must set MODEL and REASONING_EFFORT or explicitly set INHERIT_GLOBAL to true",
		},
		{
			name: "profile omits reasoning effort",
			policy: policyHeader + `  AGENT_PROFILES:
    inherited:
      MODEL: model
` + validRoleProfiles,
			wantError: "must set MODEL and REASONING_EFFORT or explicitly set INHERIT_GLOBAL to true",
		},
		{
			name: "profile mixes inheritance with explicit settings",
			policy: policyHeader + `  AGENT_PROFILES:
    inherited:
      MODEL: model
      REASONING_EFFORT: high
      INHERIT_GLOBAL: true
` + validRoleProfiles,
			wantError: "cannot combine INHERIT_GLOBAL with MODEL or REASONING_EFFORT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(test.policy))

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

func TestLoadConfigRejectsUnknownReviewPolicyField(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nREVIEW_POLICY:\n  VERSION: 1\n  REVIEWERS: 3\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for unknown REVIEW_POLICY field")
	}
	if !strings.Contains(err.Error(), "field REVIEWERS not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigRejectsUnsupportedReviewPolicyVersion(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nREVIEW_POLICY:\n  VERSION: 2\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for unsupported REVIEW_POLICY version")
	}
	if !strings.Contains(err.Error(), "unsupported REVIEW_POLICY VERSION 2") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigHardGateModeOverride(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nHARD_GATE_MODE: serial\n")

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if got, want := cfg.HardGateMode, HardGateModeSerial; got != want {
		t.Fatalf("HardGateMode = %q, want %q", got, want)
	}
}

func TestLoadConfigRejectsInvalidHardGateMode(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nHARD_GATE_MODE: queued\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for invalid hard gate mode")
	}
	if !strings.Contains(err.Error(), "HARD_GATE_MODE must be SERIAL or PARALLEL") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigMissingRequired(t *testing.T) {
	t.Setenv("WEBEX_WEBHOOK_URL", "")
	cfgPath := writeRepoConfig(t, "REPO_OWNER: \"\"\nREPO_NAME: \"\"\nREPO_PATH: \"\"\nMANDATORY_TESTS:\n  - make test\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected missing configuration error")
	}

	errMsg := err.Error()
	for _, key := range []string{"WEBEX_WEBHOOK_URL", "REPO_OWNER (config)", "REPO_NAME (config)", "REPO_PATH (config)"} {
		if !strings.Contains(errMsg, key) {
			t.Fatalf("error %q does not include missing key %s", errMsg, key)
		}
	}
}

func TestLoadConfigRequiresMandatoryTests(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS: []\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for empty mandatory tests")
	}
	if !strings.Contains(err.Error(), "MANDATORY_TESTS") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigRejectsNegativeHandoffLimits(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nMAX_STORED_HANDOFFS: -1\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for negative handoff limit")
	}
	if !strings.Contains(err.Error(), "MAX_STORED_HANDOFFS") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadConfigRejectsNegativeReviewGateAlertMinutes(t *testing.T) {
	setRequiredEnv(t)
	cfgPath := writeRepoConfig(t, "REPO_OWNER: acme\nREPO_NAME: widget\nREPO_PATH: /tmp/repo\nMANDATORY_TESTS:\n  - make test\nREVIEW_GATE_ALERT_MINUTES: -1\n")

	_, err := loadConfig(cfgPath)
	if err == nil {
		t.Fatal("expected error for negative review gate alert minutes")
	}
	if !strings.Contains(err.Error(), "REVIEW_GATE_ALERT_MINUTES") {
		t.Fatalf("unexpected error: %v", err)
	}
}
