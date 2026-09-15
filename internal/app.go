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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/go-github/v90/github"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

type Config struct {
	WebexWebhookURL          string
	WebexUID                 string
	RepoOwner                string
	RepoName                 string
	RepoPath                 string
	LogDir                   string
	WorktreeDir              string
	PollInterval             time.Duration
	BaseBranch               string
	MergeMethod              string
	PollIntervalSeconds      int
	CodexCmd                 string
	MandatoryTests           []string
	HardGateMode             HardGateMode
	ReviewGateAlertThreshold time.Duration
	ReviewPolicy             ReviewPolicy
	MaxStoredHandoffs        int
	MaxHandoffsInContext     int
	ConfigPath               string
}

const defaultCodexCmd = "codex --ask-for-approval never --sandbox danger-full-access -c tui.animations=false -c check_for_update_on_startup=false"
const defaultGitHubHTTPTimeout = 30 * time.Second
const defaultMaxStoredHandoffs = 100
const defaultMaxHandoffsInContext = 5
const supportedReviewPolicyVersion = 1
const tailInputPollInterval = 25 * time.Millisecond

type HardGateMode string

const (
	HardGateModeParallel HardGateMode = "PARALLEL"
	HardGateModeSerial   HardGateMode = "SERIAL"
)

type ReviewPolicy struct {
	Version                   int                         `json:"version"`
	Fingerprint               string                      `json:"fingerprint"`
	AgentProfiles             map[string]AgentProfile     `json:"agent_profiles"`
	RoleProfiles              map[AgentProfileRole]string `json:"role_profiles"`
	ModelCatalog              map[string]ModelCapability  `json:"model_catalog"`
	Swarm                     ReviewSwarmPolicy           `json:"review_swarm"`
	Artifacts                 ReviewArtifactPolicy        `json:"artifacts"`
	Verification              VerificationPolicy          `json:"verification"`
	Convergence               ConvergencePolicy           `json:"convergence"`
	Escalation                EscalationPolicy            `json:"escalation"`
	FailureActions            ReviewFailureActions        `json:"failure_actions"`
	repositoryProfileNames    map[string]struct{}
	runtimeProfileCLIOverride agentProfileOverride
}

type AgentProfile struct {
	Name            string `json:"name"`
	Model           string `json:"model,omitempty"`
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
	InheritGlobal   bool   `json:"inherit_global,omitempty"`
}

type AgentProfileRole string

const (
	AgentProfileRoleCoder      AgentProfileRole = "coder"
	AgentProfileRoleIndexer    AgentProfileRole = "indexer"
	AgentProfileRoleDiscovery  AgentProfileRole = "discovery"
	AgentProfileRoleVerifier   AgentProfileRole = "verifier"
	AgentProfileRoleChallenge  AgentProfileRole = "challenge"
	AgentProfileRoleEscalation AgentProfileRole = "escalation"
)

var requiredAgentProfileRoles = []AgentProfileRole{
	AgentProfileRoleCoder,
	AgentProfileRoleIndexer,
	AgentProfileRoleDiscovery,
	AgentProfileRoleVerifier,
	AgentProfileRoleChallenge,
	AgentProfileRoleEscalation,
}

type reviewPolicyConfigFile struct {
	Version        int                             `yaml:"VERSION"`
	AgentProfiles  map[string]agentProfileFile     `yaml:"AGENT_PROFILES"`
	RoleProfiles   *roleProfileReferencesFile      `yaml:"ROLE_PROFILES"`
	ModelCatalog   map[string]modelCapabilityFile  `yaml:"MODEL_CATALOG"`
	ReviewSwarm    *reviewSwarmConfigFile          `yaml:"REVIEW_SWARM"`
	Artifacts      *reviewArtifactConfigFile       `yaml:"ARTIFACTS"`
	Verification   *verificationConfigFile         `yaml:"VERIFICATION"`
	Convergence    *convergenceConfigFile          `yaml:"CONVERGENCE"`
	Escalation     *escalationConfigFile           `yaml:"ESCALATION"`
	FailureActions *reviewFailureActionsConfigFile `yaml:"FAILURE_ACTIONS"`
}

type agentProfileFile struct {
	Model           string `yaml:"MODEL"`
	ReasoningEffort string `yaml:"REASONING_EFFORT"`
	InheritGlobal   *bool  `yaml:"INHERIT_GLOBAL"`
}

type modelCapabilityFile struct {
	ReasoningEfforts []string `yaml:"REASONING_EFFORTS"`
	Available        *bool    `yaml:"AVAILABLE"`
}

type roleProfileReferencesFile struct {
	Coder      string `yaml:"CODER"`
	Indexer    string `yaml:"INDEXER"`
	Discovery  string `yaml:"DISCOVERY"`
	Verifier   string `yaml:"VERIFIER"`
	Challenge  string `yaml:"CHALLENGE"`
	Escalation string `yaml:"ESCALATION"`
}

type repoConfigFile struct {
	RepoOwner              string                  `yaml:"REPO_OWNER"`
	RepoName               string                  `yaml:"REPO_NAME"`
	RepoPath               string                  `yaml:"REPO_PATH"`
	LogPath                string                  `yaml:"LOG_PATH"`
	MandatoryTests         []string                `yaml:"MANDATORY_TESTS"`
	HardGateMode           string                  `yaml:"HARD_GATE_MODE"`
	ReviewGateAlertMinutes int                     `yaml:"REVIEW_GATE_ALERT_MINUTES"`
	ReviewPolicy           *reviewPolicyConfigFile `yaml:"REVIEW_POLICY"`
	MaxStoredHandoffs      int                     `yaml:"MAX_STORED_HANDOFFS"`
	MaxHandoffsInContext   int                     `yaml:"MAX_HANDOFFS_IN_CONTEXT"`
}

func applyRawConfigPathValues(raw []byte, repoCfg *repoConfigFile) error {
	pathValues, err := configScalarValues(raw, "REPO_PATH", "LOG_PATH")
	if err != nil {
		return err
	}
	if value, ok := pathValues["REPO_PATH"]; ok {
		repoCfg.RepoPath = value
	}
	if value, ok := pathValues["LOG_PATH"]; ok {
		repoCfg.LogPath = value
	}
	return nil
}

func configScalarValues(raw []byte, fields ...string) (map[string]string, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	if len(root.Content) == 0 {
		return map[string]string{}, nil
	}

	doc := root.Content[0]
	if doc.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("config root must be a mapping")
	}

	wanted := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		wanted[field] = struct{}{}
	}

	values := make(map[string]string, len(fields))
	for i := 0; i+1 < len(doc.Content); i += 2 {
		keyNode := doc.Content[i]
		valueNode := doc.Content[i+1]
		if _, ok := wanted[keyNode.Value]; !ok {
			continue
		}
		scalarNode, err := resolveScalarNode(valueNode)
		if err != nil {
			return nil, fmt.Errorf("%s %w", keyNode.Value, err)
		}
		if scalarNode.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf("%s must be a scalar", keyNode.Value)
		}
		if scalarNode.Tag == "!!null" && scalarNode.Value != "~" {
			values[keyNode.Value] = ""
			continue
		}
		values[keyNode.Value] = scalarNode.Value
	}

	return values, nil
}

func resolveScalarNode(node *yaml.Node) (*yaml.Node, error) {
	for node != nil && node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node == nil {
		return nil, fmt.Errorf("must be a scalar")
	}
	return node, nil
}

func loadConfig(configPath string) (Config, error) {
	return loadConfigWithRuntimeProfileOverride(configPath, agentProfileOverride{})
}

func loadConfigWithRuntimeProfileOverride(configPath string, profileOverride agentProfileOverride) (Config, error) {
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return Config{}, errors.New("missing required --config <path> argument")
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var repoCfg repoConfigFile
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&repoCfg); err != nil {
		return Config{}, fmt.Errorf("failed to decode config YAML: %w", err)
	}
	if err := applyRawConfigPathValues(raw, &repoCfg); err != nil {
		return Config{}, fmt.Errorf("failed to decode config YAML: %w", err)
	}

	required := []string{"WEBEX_WEBHOOK_URL"}
	missing := make([]string, 0)
	for _, name := range required {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			missing = append(missing, name)
		}
	}
	if strings.TrimSpace(repoCfg.RepoOwner) == "" {
		missing = append(missing, "REPO_OWNER (config)")
	}
	if strings.TrimSpace(repoCfg.RepoName) == "" {
		missing = append(missing, "REPO_NAME (config)")
	}
	if strings.TrimSpace(repoCfg.RepoPath) == "" {
		missing = append(missing, "REPO_PATH (config)")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required configuration values: %s", strings.Join(missing, ", "))
	}

	repoPath, err := expandConfigPath(repoCfg.RepoPath)
	if err != nil {
		return Config{}, fmt.Errorf("invalid REPO_PATH: %w", err)
	}
	logPath, err := expandConfigPath(repoCfg.LogPath)
	if err != nil {
		return Config{}, fmt.Errorf("invalid LOG_PATH: %w", err)
	}

	mandatoryTests, err := normalizeMandatoryTests(repoCfg.MandatoryTests)
	if err != nil {
		return Config{}, err
	}
	hardGateMode, err := normalizeHardGateMode(repoCfg.HardGateMode)
	if err != nil {
		return Config{}, err
	}
	reviewGateAlertMinutes, err := normalizePositiveConfigInt(repoCfg.ReviewGateAlertMinutes, int(reviewGateAlertThreshold/time.Minute), "REVIEW_GATE_ALERT_MINUTES")
	if err != nil {
		return Config{}, err
	}
	reviewPolicy, err := normalizeReviewPolicy(repoCfg.ReviewPolicy, profileOverride)
	if err != nil {
		return Config{}, err
	}
	maxStoredHandoffs, err := normalizePositiveConfigInt(repoCfg.MaxStoredHandoffs, defaultMaxStoredHandoffs, "MAX_STORED_HANDOFFS")
	if err != nil {
		return Config{}, err
	}
	maxHandoffsInContext, err := normalizePositiveConfigInt(repoCfg.MaxHandoffsInContext, defaultMaxHandoffsInContext, "MAX_HANDOFFS_IN_CONTEXT")
	if err != nil {
		return Config{}, err
	}

	logDir, err := resolveLogDir(repoCfg.RepoName, logPath)
	if err != nil {
		return Config{}, err
	}

	worktreeDir := strings.TrimSpace(os.Getenv("WORKTREE_DIR"))
	if worktreeDir == "" {
		worktreeDir = filepath.Join(repoPath, ".worktrees")
	}

	pollSeconds := 20
	pollRaw := strings.TrimSpace(os.Getenv("POLL_INTERVAL_SECONDS"))
	if pollRaw != "" {
		v, err := strconv.Atoi(pollRaw)
		if err != nil || v <= 0 {
			return Config{}, fmt.Errorf("invalid POLL_INTERVAL_SECONDS value")
		}
		pollSeconds = v
	}

	baseBranch := strings.TrimSpace(os.Getenv("BASE_BRANCH"))
	if baseBranch == "" {
		baseBranch = "main"
	}

	codexCmd := strings.TrimSpace(os.Getenv("CODEX_CMD"))
	if codexCmd == "" {
		codexCmd = defaultCodexCmd
	}

	return Config{
		WebexWebhookURL:          strings.TrimSpace(os.Getenv("WEBEX_WEBHOOK_URL")),
		WebexUID:                 strings.TrimSpace(os.Getenv("WEBEX_UID")),
		RepoOwner:                strings.TrimSpace(repoCfg.RepoOwner),
		RepoName:                 strings.TrimSpace(repoCfg.RepoName),
		RepoPath:                 repoPath,
		LogDir:                   logDir,
		WorktreeDir:              worktreeDir,
		PollInterval:             time.Duration(pollSeconds) * time.Second,
		BaseBranch:               baseBranch,
		MergeMethod:              "squash",
		PollIntervalSeconds:      pollSeconds,
		CodexCmd:                 codexCmd,
		MandatoryTests:           mandatoryTests,
		HardGateMode:             hardGateMode,
		ReviewGateAlertThreshold: time.Duration(reviewGateAlertMinutes) * time.Minute,
		ReviewPolicy:             reviewPolicy,
		MaxStoredHandoffs:        maxStoredHandoffs,
		MaxHandoffsInContext:     maxHandoffsInContext,
		ConfigPath:               configPath,
	}, nil
}

func normalizeReviewPolicy(raw *reviewPolicyConfigFile, profileOverride agentProfileOverride) (ReviewPolicy, error) {
	if raw == nil {
		policy := builtInReviewPolicy()
		policy.runtimeProfileCLIOverride = profileOverride
		if err := validateReviewPolicyConfiguration(policy); err != nil {
			return ReviewPolicy{}, err
		}
		return snapshotEffectiveReviewPolicy(policy)
	}
	if raw.Version != supportedReviewPolicyVersion {
		return ReviewPolicy{}, fmt.Errorf(
			"unsupported REVIEW_POLICY VERSION %d; supported version is %d",
			raw.Version,
			supportedReviewPolicyVersion,
		)
	}
	if (raw.AgentProfiles == nil) != (raw.RoleProfiles == nil) {
		if raw.AgentProfiles == nil {
			return ReviewPolicy{}, errors.New("REVIEW_POLICY.AGENT_PROFILES must be configured")
		}
		return ReviewPolicy{}, errors.New("REVIEW_POLICY.ROLE_PROFILES must be configured")
	}

	policy := builtInReviewPolicy()
	if raw.AgentProfiles != nil {
		profiles, err := normalizeAgentProfiles(raw.AgentProfiles)
		if err != nil {
			return ReviewPolicy{}, err
		}
		policy.AgentProfiles = profiles
		policy.repositoryProfileNames = make(map[string]struct{}, len(profiles))
		for name := range profiles {
			policy.repositoryProfileNames[name] = struct{}{}
		}
		roleProfiles, err := normalizeRoleProfileReferences(*raw.RoleProfiles, policy.AgentProfiles)
		if err != nil {
			return ReviewPolicy{}, err
		}
		policy.RoleProfiles = roleProfiles
	}
	policy.Swarm = defaultReviewSwarmPolicy(policy.RoleProfiles[AgentProfileRoleDiscovery])
	swarm, err := normalizeReviewSwarm(raw.ReviewSwarm, policy.Swarm)
	if err != nil {
		return ReviewPolicy{}, err
	}
	policy.Swarm = swarm
	policy.Artifacts = normalizeReviewArtifactPolicy(
		raw.Artifacts,
		policy.Artifacts,
	)
	policy.Verification = normalizeVerificationPolicy(raw.Verification, policy.Verification)
	policy.Convergence = normalizeConvergencePolicy(raw.Convergence, policy.Convergence)
	policy.Escalation = normalizeEscalationPolicy(
		raw.Escalation,
		defaultEscalationPolicy(policy.RoleProfiles[AgentProfileRoleEscalation]),
	)
	policy.FailureActions = normalizeReviewFailureActions(raw.FailureActions, policy.FailureActions)

	modelCatalog, err := normalizeModelCatalog(raw.ModelCatalog, policy.ModelCatalog)
	if err != nil {
		return ReviewPolicy{}, err
	}
	policy.ModelCatalog = modelCatalog
	if err := applyAgentProfileEnvironmentOverrides(&policy); err != nil {
		return ReviewPolicy{}, err
	}
	policy.runtimeProfileCLIOverride = profileOverride
	if err := validateReviewPolicyConfiguration(policy); err != nil {
		return ReviewPolicy{}, err
	}
	return snapshotEffectiveReviewPolicy(policy)
}

func builtInReviewPolicy() ReviewPolicy {
	profiles := make(map[string]AgentProfile, len(requiredAgentProfileRoles))
	roleProfiles := make(map[AgentProfileRole]string, len(requiredAgentProfileRoles))
	for _, role := range requiredAgentProfileRoles {
		profile := builtInAgentProfile(role)
		profiles[profile.Name] = profile
		roleProfiles[role] = profile.Name
	}
	return ReviewPolicy{
		Version:        supportedReviewPolicyVersion,
		AgentProfiles:  profiles,
		RoleProfiles:   roleProfiles,
		ModelCatalog:   defaultModelCatalog(),
		Swarm:          defaultReviewSwarmPolicy(roleProfiles[AgentProfileRoleDiscovery]),
		Artifacts:      defaultReviewArtifactPolicy(),
		Verification:   defaultVerificationPolicy(),
		Convergence:    defaultConvergencePolicy(),
		Escalation:     defaultEscalationPolicy(roleProfiles[AgentProfileRoleEscalation]),
		FailureActions: defaultReviewFailureActions(),
	}
}

func normalizeAgentProfiles(raw map[string]agentProfileFile) (map[string]AgentProfile, error) {
	if len(raw) == 0 {
		return nil, errors.New("REVIEW_POLICY.AGENT_PROFILES must define at least one named profile")
	}

	rawNames := make([]string, 0, len(raw))
	for rawName := range raw {
		rawNames = append(rawNames, rawName)
	}
	sort.Strings(rawNames)

	profiles := make(map[string]AgentProfile, len(raw))
	for _, rawName := range rawNames {
		rawProfile := raw[rawName]
		name := strings.TrimSpace(rawName)
		if name == "" {
			return nil, errors.New("REVIEW_POLICY.AGENT_PROFILES contains an empty profile name")
		}
		if _, exists := profiles[name]; exists {
			return nil, fmt.Errorf("REVIEW_POLICY.AGENT_PROFILES contains duplicate profile name %q after trimming whitespace", name)
		}

		model := strings.TrimSpace(rawProfile.Model)
		effort := normalizeReasoningEffort(rawProfile.ReasoningEffort)
		inheritGlobal := rawProfile.InheritGlobal != nil && *rawProfile.InheritGlobal
		if inheritGlobal {
			if model != "" || effort != "" {
				return nil, fmt.Errorf(
					"REVIEW_POLICY.AGENT_PROFILES.%s cannot combine INHERIT_GLOBAL with MODEL or REASONING_EFFORT",
					name,
				)
			}
		} else if model == "" || effort == "" {
			return nil, fmt.Errorf(
				"REVIEW_POLICY.AGENT_PROFILES.%s must set MODEL and REASONING_EFFORT or explicitly set INHERIT_GLOBAL to true",
				name,
			)
		}

		profiles[name] = AgentProfile{
			Name:            name,
			Model:           model,
			ReasoningEffort: effort,
			InheritGlobal:   inheritGlobal,
		}
	}
	return profiles, nil
}

func normalizeRoleProfileReferences(raw roleProfileReferencesFile, profiles map[string]AgentProfile) (map[AgentProfileRole]string, error) {
	roleProfiles := map[AgentProfileRole]string{
		AgentProfileRoleCoder:      strings.TrimSpace(raw.Coder),
		AgentProfileRoleIndexer:    strings.TrimSpace(raw.Indexer),
		AgentProfileRoleDiscovery:  strings.TrimSpace(raw.Discovery),
		AgentProfileRoleVerifier:   strings.TrimSpace(raw.Verifier),
		AgentProfileRoleChallenge:  strings.TrimSpace(raw.Challenge),
		AgentProfileRoleEscalation: strings.TrimSpace(raw.Escalation),
	}
	for _, role := range requiredAgentProfileRoles {
		profileName := roleProfiles[role]
		fieldName := strings.ToUpper(string(role))
		if profileName == "" {
			return nil, fmt.Errorf("REVIEW_POLICY.ROLE_PROFILES.%s must reference a named profile", fieldName)
		}
		if _, ok := profiles[profileName]; !ok {
			return nil, fmt.Errorf(
				"REVIEW_POLICY.ROLE_PROFILES.%s references unknown profile %q",
				fieldName,
				profileName,
			)
		}
	}
	return roleProfiles, nil
}

func (p ReviewPolicy) ProfileForRole(role AgentProfileRole) (AgentProfile, error) {
	profileName, ok := p.RoleProfiles[role]
	if !ok {
		return AgentProfile{}, fmt.Errorf("unknown agent profile role %q", role)
	}
	profile, ok := p.AgentProfiles[profileName]
	if !ok {
		return AgentProfile{}, fmt.Errorf("agent profile role %q references unknown profile %q", role, profileName)
	}
	return profile, nil
}

func normalizePositiveConfigInt(raw int, defaultValue int, fieldName string) (int, error) {
	if raw == 0 {
		return defaultValue, nil
	}
	if raw < 0 {
		return 0, fmt.Errorf("%s must be greater than zero", fieldName)
	}
	return raw, nil
}

func expandConfigPath(raw string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" || !strings.HasPrefix(path, "~") {
		return path, nil
	}

	rest := strings.TrimPrefix(path, "~")
	username := ""
	suffix := ""
	if idx := strings.IndexRune(rest, '/'); idx >= 0 {
		username = rest[:idx]
		suffix = rest[idx+1:]
	} else {
		username = rest
	}

	var u *user.User
	var err error
	if username == "" {
		u, err = user.Current()
	} else {
		u, err = user.Lookup(username)
	}
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(u.HomeDir) == "" {
		return "", fmt.Errorf("user %q has no home directory", u.Username)
	}
	if suffix == "" {
		return u.HomeDir, nil
	}
	return filepath.Join(u.HomeDir, filepath.FromSlash(suffix)), nil
}

func repoScopedLogDir(repoName string) (string, error) {
	safeRepoName := sanitizeSessionPart(repoName)
	if safeRepoName == "" {
		return "", fmt.Errorf("invalid REPO_NAME %q: cannot derive log directory", strings.TrimSpace(repoName))
	}
	return filepath.Join(orchestratorLogDir, safeRepoName), nil
}

func resolveLogDir(repoName, logPath string) (string, error) {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" {
		return repoScopedLogDir(repoName)
	}
	return orchestratorArtifactsDir(logPath), nil
}

func orchestratorArtifactsDir(basePath string) string {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		return ""
	}
	cleaned := filepath.Clean(basePath)
	if filepath.Base(cleaned) == orchestratorStateDirName {
		return cleaned
	}
	return filepath.Join(cleaned, orchestratorStateDirName)
}

func (c Config) ValidateRuntime(ctx context.Context) error {
	if _, err := exec.LookPath("gh"); err != nil {
		return errors.New("gh not found in PATH")
	}
	if _, err := exec.LookPath("git"); err != nil {
		return errors.New("git not found in PATH")
	}
	if _, err := exec.LookPath("make"); err != nil {
		return errors.New("make not found in PATH")
	}
	if _, err := exec.LookPath("tmux"); err != nil {
		return errors.New("tmux not found in PATH")
	}
	codexExec := runtimeCommandName(c.CodexCmd)
	if _, err := exec.LookPath(codexExec); err != nil {
		return fmt.Errorf("%s not found in PATH", codexExec)
	}

	if err := ensureDirExists(c.RepoPath); err != nil {
		return fmt.Errorf("repo path validation failed: %w", err)
	}

	if ok, err := isGitRepo(ctx, c.RepoPath); err != nil {
		return fmt.Errorf("git repository check failed: %w", err)
	} else if !ok {
		return errors.New("repo path is not a git repository")
	}
	if ok, actual, err := repoPathMatchesTargetRepo(ctx, c.RepoPath, c.RepoOwner, c.RepoName); err != nil {
		return fmt.Errorf("repo remote validation failed: %w", err)
	} else if !ok {
		return fmt.Errorf("repo path points to %s but expected %s/%s", actual, c.RepoOwner, c.RepoName)
	}

	if err := ensureDirWritable(c.WorktreeDir); err != nil {
		return fmt.Errorf("worktree dir is not writable: %w", err)
	}
	if err := ensureDirWritable(c.LogDir); err != nil {
		return fmt.Errorf("log dir is not writable: %w", err)
	}

	if _, err := getGitHubTokenFromGH(ctx); err != nil {
		return err
	}
	if err := validateMandatoryTestExecutables(c.MandatoryTests); err != nil {
		return err
	}

	return nil
}

type AgentState string

const (
	StateInitializing AgentState = "initializing"
	StateReviewGate   AgentState = "review_gate"
	StateWorking      AgentState = "working"
	StateWaiting      AgentState = "waiting_review"
	StateApproved     AgentState = "approved"
	StateMerging      AgentState = "merging"
	StateDone         AgentState = "done"
	StateErrored      AgentState = "errored"
	StateStopped      AgentState = "stopped"
)

const (
	runtimeCaptureLines        = 200
	inputWaitMinimumDuration   = time.Minute
	reviewLoopMinimumDuration  = 10 * time.Minute
	coderLoopMinimumDuration   = 10 * time.Minute
	reviewGateAlertThreshold   = 15 * time.Minute
	reviewLaunchStallTimeout   = mandatoryTestGateTimeout + 5*time.Minute
	reviewInitStallTimeout     = 5 * time.Minute
	autoContinueSteeringText   = "Continue the assigned issue in autonomous mode now. Do not stop after a single command or status update. Execute the next several concrete steps in sequence (batch safe commands in one shell block when possible), and only pause if you hit a real blocker that requires user input."
	autoRebaseSteeringText     = "GitHub reports this PR has merge conflicts against the base branch. Rebase your branch onto the current base branch, resolve conflicts, run the mandatory tests, and push the updated branch."
	runtimeArtifactsCommitRule = "Never stage or commit Repository Agent Orchestrator runtime artifacts under `.repository-agent-orchestrator/` or `.worktrees/`; `.repository-agent-orchestrator/HANDOFF.yaml` is for Repository Agent Orchestrator state capture only."
	webexNotificationTimeout   = 15 * time.Second
	commandFailureOutputBytes  = 16 * 1024
	commandFailureOutputLines  = 40
	orchestratorLogDir         = "/tmp/repository-agent-orchestrator"
	orchestratorDaemonLogName  = "repository-agent-orchestrator.log"
	orchestratorLockFileName   = "instance.lock"
	orchestratorStateDirName   = ".repository-agent-orchestrator"
	orchestratorStateFileName  = "agents_state.json"
	gitCommandTimeout          = 5 * time.Minute
)

var (
	errREPLInterrupted            = errors.New("terminal input interrupted")
	ansiOSCSequencePattern        = regexp.MustCompile(`\x1b\][^\x1b\x07]*(?:\x07|\x1b\\)`)
	ansiControlSequencePattern    = regexp.MustCompile(`\x1b(?:\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])`)
	inputWaitNormalizationPattern = regexp.MustCompile(`[^a-z0-9]+`)
)

type Agent struct {
	ID                              string
	Role                            AgentRole
	ParentAgentID                   string
	IssueNumber                     int
	IssueTitle                      string
	IssueBody                       string
	WorktreePath                    string
	LogDir                          string
	RuntimeCWD                      string
	BranchName                      string
	PRHeadBranch                    string
	AdoptedPR                       bool
	RepoIndexHadOutput              bool
	RepoIndexBaselineModTime        time.Time
	RepoIndexHadSource              bool
	RepoIndexSourceBaselineTime     time.Time
	HandoffCaptured                 bool
	RuntimeHandle                   RuntimeHandle
	RuntimeProfile                  AgentProfile
	PRNumber                        int
	PRTitle                         string
	PRURL                           string
	ObservedPRHeadSHA               string
	ActiveReviewAgentID             string
	HumanReviewGuidance             []string
	LastPreReviewGateFailureHeadSHA string
	LastPreReviewGateFailureAt      time.Time
	LaunchAttempts                  []DurableLaunchAttempt
	LastReviewedHeadSHA             string
	// ManualReviewHold is true only while RecordManualReviewHold's hold on
	// LastReviewedHeadSHA is still in effect (see isManualReviewHoldForHead).
	// LastReviewedHeadSHA/LastReviewVerdict/LastReviewCommentID alone do not
	// distinguish "held pending a human agent review" from "just adopted
	// via ContinueAgentForIssuePR," which sets the exact same
	// LastReviewedHeadSHA-with-no-verdict shape as its normal baseline for
	// every adopted PR.
	ManualReviewHold             bool
	LastReviewVerdict            ReviewVerdict
	LastReviewCommentID          int64
	PendingReviewVerdict         *PendingReviewVerdictApplication
	ReviewCycle                  *ReviewCycleState
	ReviewCoordinatorLifecycle   *ReviewCoordinatorLifecycleState
	ReviewLedger                 *ReviewLedger
	ReviewSkipHardGate           bool
	LastConflictHeadSHA          string
	ReviewBaselineIssueCommentID int64
	State                        AgentState
	Paused                       bool
	LastActivityTime             time.Time
	FailureMessage               string

	Stopped bool

	seenReviewCommentIDs    map[int64]struct{}
	seenIssueCommentIDs     map[int64]struct{}
	pendingReviewCommentIDs map[int64]struct{}
}

func (c Config) effectiveReviewGateAlertThreshold() time.Duration {
	if c.ReviewGateAlertThreshold > 0 {
		return c.ReviewGateAlertThreshold
	}
	return reviewGateAlertThreshold
}

func (c Config) effectiveHardGateMode() HardGateMode {
	if c.HardGateMode == HardGateModeSerial {
		return HardGateModeSerial
	}
	return HardGateModeParallel
}

func (a Agent) String() string {
	pr := "-"
	if a.PRNumber > 0 {
		pr = fmt.Sprintf("#%d", a.PRNumber)
	}
	role := a.Role
	if role == "" {
		role = RoleCoder
	}
	if a.Paused {
		return fmt.Sprintf("%s role=%s issue=%d state=%s paused=true pr=%s branch=%s", a.ID, role, a.IssueNumber, a.State, pr, a.BranchName)
	}
	return fmt.Sprintf("%s role=%s issue=%d state=%s pr=%s branch=%s", a.ID, role, a.IssueNumber, a.State, pr, a.BranchName)
}

type AgentManager struct {
	mu                         sync.Mutex
	agents                     map[string]*Agent
	reviewCoordinatorLifecycle map[string]*sync.RWMutex
	runtimeDelivery            map[string]*runtimeDeliveryGuard
	lastPoll                   time.Time
}

// runtimeDeliveryGuard separates slow, non-mutating scope binding from the
// short read lock held across the actual tmux delivery. Scope writers can
// invalidate a binding while it is being prepared, but cannot race a verified
// delivery. send serializes chunked writes to a single agent pane.
type runtimeDeliveryGuard struct {
	send  sync.Mutex
	scope sync.RWMutex
}

func NewAgentManager() *AgentManager {
	return &AgentManager{
		agents:                     make(map[string]*Agent),
		reviewCoordinatorLifecycle: make(map[string]*sync.RWMutex),
		runtimeDelivery:            make(map[string]*runtimeDeliveryGuard),
	}
}

func cloneAgent(a *Agent) Agent {
	copy := *a
	copy.HumanReviewGuidance = append([]string(nil), a.HumanReviewGuidance...)
	copy.LaunchAttempts = cloneDurableLaunchAttempts(a.LaunchAttempts)
	copy.ReviewCoordinatorLifecycle =
		cloneReviewCoordinatorLifecycle(a.ReviewCoordinatorLifecycle)
	copy.ReviewCycle = snapshotReviewCycleResult(copy)
	copy.ReviewLedger = cloneReviewLedger(a.ReviewLedger)
	if a.PendingReviewVerdict != nil {
		pending := *a.PendingReviewVerdict
		copy.PendingReviewVerdict = &pending
	}
	copy.seenIssueCommentIDs = nil
	copy.seenReviewCommentIDs = nil
	copy.pendingReviewCommentIDs = nil
	return copy
}

func (m *AgentManager) Add(agent *Agent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.agents[agent.ID]; ok {
		return fmt.Errorf("agent id already exists: %s", agent.ID)
	}
	agent.LastActivityTime = agent.LastActivityTime.UTC()
	m.agents[agent.ID] = agent
	m.runtimeDeliveryGuardLocked(agent.ID)
	if agent.Role == RoleReviewer {
		m.reviewCoordinatorLifecycleLocked(agent.ID)
	}
	return nil
}

func (m *AgentManager) runtimeDeliveryGuardLocked(
	agentID string,
) *runtimeDeliveryGuard {
	if m.runtimeDelivery == nil {
		m.runtimeDelivery = make(map[string]*runtimeDeliveryGuard)
	}
	agentID = strings.TrimSpace(agentID)
	guard := m.runtimeDelivery[agentID]
	if guard == nil {
		guard = &runtimeDeliveryGuard{}
		m.runtimeDelivery[agentID] = guard
	}
	return guard
}

func (m *AgentManager) runtimeDeliveryGuardFor(
	agentID string,
) *runtimeDeliveryGuard {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runtimeDeliveryGuardLocked(agentID)
}

func (m *AgentManager) lockRuntimeSend(agentID string) func() {
	guard := m.runtimeDeliveryGuardFor(agentID)
	guard.send.Lock()
	return guard.send.Unlock
}

func (m *AgentManager) lockRuntimeScopeRead(agentID string) func() {
	guard := m.runtimeDeliveryGuardFor(agentID)
	guard.scope.RLock()
	return guard.scope.RUnlock
}

func (m *AgentManager) lockRuntimeScopeMutation(agentID string) func() {
	guard := m.runtimeDeliveryGuardFor(agentID)
	guard.scope.Lock()
	return guard.scope.Unlock
}

func (m *AgentManager) lockRuntimeScopeMutations(agentIDs ...string) func() {
	unique := make(map[string]struct{}, len(agentIDs))
	ids := make([]string, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, exists := unique[agentID]; exists {
			continue
		}
		unique[agentID] = struct{}{}
		ids = append(ids, agentID)
	}
	sort.Strings(ids)
	guards := make([]*runtimeDeliveryGuard, 0, len(ids))
	for _, agentID := range ids {
		guards = append(guards, m.runtimeDeliveryGuardFor(agentID))
	}
	for _, guard := range guards {
		guard.scope.Lock()
	}
	return func() {
		for index := len(guards) - 1; index >= 0; index-- {
			guards[index].scope.Unlock()
		}
	}
}

func (m *AgentManager) reviewCoordinatorLifecycleLocked(
	agentID string,
) (*sync.RWMutex, bool) {
	agent, ok := m.agents[strings.TrimSpace(agentID)]
	if !ok || agent == nil || agent.Role != RoleReviewer {
		return nil, false
	}
	if m.reviewCoordinatorLifecycle == nil {
		m.reviewCoordinatorLifecycle = make(map[string]*sync.RWMutex)
	}
	lifecycle, ok := m.reviewCoordinatorLifecycle[agent.ID]
	if !ok {
		lifecycle = &sync.RWMutex{}
		m.reviewCoordinatorLifecycle[agent.ID] = lifecycle
	}
	return lifecycle, true
}

func (m *AgentManager) lockReviewCoordinatorTerminalization(
	agentID string,
) func() {
	m.mu.Lock()
	lifecycle, ok := m.reviewCoordinatorLifecycleLocked(agentID)
	m.mu.Unlock()
	if !ok {
		return func() {}
	}
	lifecycle.Lock()
	return lifecycle.Unlock
}

func (m *AgentManager) lockAllReviewCoordinatorTerminalizations() func() {
	type coordinatorLifecycle struct {
		agentID   string
		lifecycle *sync.RWMutex
	}

	m.mu.Lock()
	lifecycles := make([]coordinatorLifecycle, 0)
	for agentID, agent := range m.agents {
		if agent == nil || agent.Role != RoleReviewer {
			continue
		}
		lifecycle, _ := m.reviewCoordinatorLifecycleLocked(agentID)
		lifecycles = append(lifecycles, coordinatorLifecycle{
			agentID:   agentID,
			lifecycle: lifecycle,
		})
	}
	m.mu.Unlock()
	sort.Slice(lifecycles, func(i, j int) bool {
		return lifecycles[i].agentID < lifecycles[j].agentID
	})
	for _, item := range lifecycles {
		item.lifecycle.Lock()
	}
	return func() {
		for index := len(lifecycles) - 1; index >= 0; index-- {
			lifecycles[index].lifecycle.Unlock()
		}
	}
}

func (m *AgentManager) HasActiveIssue(issueNumber int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.agents {
		if !a.Stopped && a.State != StateDone && a.State != StateErrored && a.IssueNumber == issueNumber {
			return true
		}
	}
	return false
}

func (m *AgentManager) HasActivePR(prNumber int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.agents {
		if !a.Stopped && a.State != StateDone && a.State != StateErrored && a.State != StateStopped && a.PRNumber == prNumber {
			return true
		}
	}
	return false
}

func (m *AgentManager) HasActiveRole(role AgentRole) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.agents {
		if !a.Stopped && a.State != StateDone && a.State != StateErrored && a.State != StateStopped && a.Role == role {
			return true
		}
	}
	return false
}

func (m *AgentManager) Get(agentID string) (Agent, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return Agent{}, false
	}
	return cloneAgent(a), true
}

func (m *AgentManager) List() []Agent {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]Agent, 0, len(m.agents))
	for _, a := range m.agents {
		items = append(items, cloneAgent(a))
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].LastActivityTime.After(items[j].LastActivityTime)
	})
	return items
}

func (m *AgentManager) Active() []Agent {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]Agent, 0)
	for _, a := range m.agents {
		if !a.Stopped && a.State != StateDone && a.State != StateErrored && a.State != StateStopped {
			items = append(items, cloneAgent(a))
		}
	}
	return items
}

func (m *AgentManager) ResolveActiveIssueAgent(issueNumber int, role AgentRole) (Agent, error) {
	return m.resolveIssueAgent(issueNumber, role, false)
}

func (m *AgentManager) ResolveIssueAgentForCleanup(issueNumber int, role AgentRole) (Agent, error) {
	return m.resolveIssueAgent(issueNumber, role, true)
}

func (m *AgentManager) ResolveActiveReviewAgent(prNumber int) (Agent, error) {
	return m.resolveReviewAgent(prNumber, false)
}

func (m *AgentManager) ResolveReviewAgentForCleanup(prNumber int) (Agent, error) {
	return m.resolveReviewAgent(prNumber, true)
}

type agentSelectorNotFoundError struct {
	message string
}

func (e *agentSelectorNotFoundError) Error() string {
	return e.message
}

// reviewerCleanupRequest is the same full-cleanup request CleanupAgent
// issues for a reviewer (see its RoleReviewer branch). Reused here so
// reviewerFullyRetiredAndCleaned shares one definition of "fully
// cleaned" with the code that actually performs cleanup, instead of
// duplicating it.
var reviewerCleanupRequest = reviewCoordinatorLifecycleRequest{
	Intent:                     ReviewCoordinatorLifecycleCleanup,
	FinalState:                 StateDone,
	ReleaseCoordinatorWorktree: true,
	ReleaseRuntimeArtifacts:    true,
}

// reviewerFullyRetiredAndCleaned reports whether a reviewer's own
// retirement cleanup already ran to full completion -- specifically,
// whether a fresh CleanupAgent call on it would now be a no-op.
// Reviewers routinely retire into StateStopped -- not just
// StateDone/StateErrored -- when superseded by a newer head, and unlike
// coder cleanup (which always lands in StateDone, already excluded
// below), nothing ever purges these fully-cleaned StateStopped reviewer
// records from the agent map. Without this check, every reviewer a PR
// has ever superseded keeps counting as an ambiguous "needs cleanup"
// candidate for that issue/PR forever, even though there is nothing left
// for cleanup to do -- `agent cleanup` only ever gets more ambiguous
// over time as more superseded reviewers accumulate, never less.
//
// Several normal lifecycle paths complete a *stop* -- or even a
// *cleanup* -- with ReleaseRuntimeArtifacts left false (or a persisted
// handoff intentionally preserved), leaving real artifacts behind for a
// later explicit `agent cleanup`. Checking only Intent ==
// ReviewCoordinatorLifecycleCleanup and CompletedAt (as an earlier
// version of this function did) missed that distinction and could skip
// those reviewers too, making their preserved artifacts unreachable.
// reviewCoordinatorLifecycleAlreadyCompleted -- the same helper used to
// short-circuit redundant lifecycle transition requests elsewhere --
// already encodes the full definition of "this exact request has
// already been satisfied," so delegate to it with CleanupAgent's own
// request shape rather than re-deriving a partial version of the same
// check.
func reviewerFullyRetiredAndCleaned(agent *Agent) bool {
	if agent == nil || normalizedAgentRole(agent.Role) != RoleReviewer {
		return false
	}
	return reviewCoordinatorLifecycleAlreadyCompleted(*agent, reviewerCleanupRequest)
}

func (m *AgentManager) resolveIssueAgent(issueNumber int, role AgentRole, includeStopped bool) (Agent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if issueNumber <= 0 {
		return Agent{}, fmt.Errorf("invalid issue number: %d", issueNumber)
	}

	matches := make([]Agent, 0, 1)
	for _, agent := range m.agents {
		if agent == nil || agent.State == StateDone || agent.State == StateErrored {
			continue
		}
		if reviewerFullyRetiredAndCleaned(agent) {
			continue
		}
		if !includeStopped && (agent.Stopped || agent.State == StateStopped) {
			continue
		}
		if agent.IssueNumber != issueNumber || normalizedAgentRole(agent.Role) != role {
			continue
		}
		matches = append(matches, cloneAgent(agent))
	}

	switch len(matches) {
	case 0:
		if includeStopped {
			return Agent{}, &agentSelectorNotFoundError{message: fmt.Sprintf("no %s available for cleanup for issue #%d", selectorRoleLabel(role), issueNumber)}
		}
		return Agent{}, &agentSelectorNotFoundError{message: fmt.Sprintf("no active %s found for issue #%d", selectorRoleLabel(role), issueNumber)}
	case 1:
		return matches[0], nil
	default:
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].LastActivityTime.After(matches[j].LastActivityTime)
		})
		ids := make([]string, 0, len(matches))
		for _, match := range matches {
			ids = append(ids, match.ID)
		}
		qualifier := "active "
		if includeStopped {
			qualifier = ""
		}
		return Agent{}, fmt.Errorf("multiple %s%ss found for issue #%d: %s", qualifier, selectorRoleLabel(role), issueNumber, strings.Join(ids, ", "))
	}
}

func (m *AgentManager) resolveReviewAgent(prNumber int, includeStopped bool) (Agent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if prNumber <= 0 {
		return Agent{}, fmt.Errorf("invalid PR number: %d", prNumber)
	}

	matches := make([]Agent, 0, 1)
	for _, agent := range m.agents {
		if agent == nil || agent.State == StateDone || agent.State == StateErrored {
			continue
		}
		if reviewerFullyRetiredAndCleaned(agent) {
			continue
		}
		if !includeStopped && (agent.Stopped || agent.State == StateStopped) {
			continue
		}
		if normalizedAgentRole(agent.Role) != RoleReviewer || agent.PRNumber != prNumber {
			continue
		}
		matches = append(matches, cloneAgent(agent))
	}

	switch len(matches) {
	case 0:
		if includeStopped {
			return Agent{}, &agentSelectorNotFoundError{message: fmt.Sprintf("no reviewer available for cleanup for PR #%d", prNumber)}
		}
		return Agent{}, &agentSelectorNotFoundError{message: fmt.Sprintf("no active reviewer found for PR #%d", prNumber)}
	case 1:
		return matches[0], nil
	default:
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].LastActivityTime.After(matches[j].LastActivityTime)
		})
		ids := make([]string, 0, len(matches))
		for _, match := range matches {
			ids = append(ids, match.ID)
		}
		qualifier := "active "
		if includeStopped {
			qualifier = ""
		}
		return Agent{}, fmt.Errorf("multiple %sreviewers found for PR #%d: %s", qualifier, prNumber, strings.Join(ids, ", "))
	}
}

type issueContextSummary struct {
	IssueNumber int
	IssueTitle  string
	Coder       *Agent
	Reviewer    *Agent
	PRNumber    int
	PRTitle     string
	PRURL       string
}

func activeIssueSummaries(bot *Orchestrator) ([]issueContextSummary, []Agent, []Agent) {
	if bot == nil || bot.agents == nil {
		return nil, nil, nil
	}

	byIssue := make(map[int]*issueContextSummary)
	manualReviewers := make([]Agent, 0)
	indexers := make([]Agent, 0)
	for _, agent := range bot.agents.Active() {
		role := normalizedAgentRole(agent.Role)
		if role == RoleIndexer {
			indexers = append(indexers, agent)
			continue
		}
		if role == RoleReviewer && agent.IssueNumber <= 0 {
			manualReviewers = append(manualReviewers, agent)
			continue
		}
		if agent.IssueNumber <= 0 {
			continue
		}

		summary, ok := byIssue[agent.IssueNumber]
		if !ok {
			summary = &issueContextSummary{IssueNumber: agent.IssueNumber}
			byIssue[agent.IssueNumber] = summary
		}
		if summary.IssueTitle == "" && strings.TrimSpace(agent.IssueTitle) != "" {
			summary.IssueTitle = strings.TrimSpace(agent.IssueTitle)
		}
		if agent.PRNumber > 0 && (summary.PRNumber == 0 || role == RoleCoder) {
			summary.PRNumber = agent.PRNumber
			summary.PRTitle = strings.TrimSpace(agent.PRTitle)
			summary.PRURL = strings.TrimSpace(agent.PRURL)
		}

		switch role {
		case RoleCoder:
			if summary.Coder == nil || agent.LastActivityTime.After(summary.Coder.LastActivityTime) {
				current := agent
				summary.Coder = &current
			}
		case RoleReviewer:
			if summary.Reviewer == nil || agent.LastActivityTime.After(summary.Reviewer.LastActivityTime) {
				current := agent
				summary.Reviewer = &current
			}
		}
	}

	summaries := make([]issueContextSummary, 0, len(byIssue))
	for _, summary := range byIssue {
		if summary.IssueTitle == "" && summary.PRTitle != "" {
			summary.IssueTitle = summary.PRTitle
		}
		summaries = append(summaries, *summary)
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].IssueNumber < summaries[j].IssueNumber
	})
	sort.Slice(manualReviewers, func(i, j int) bool {
		if manualReviewers[i].PRNumber == manualReviewers[j].PRNumber {
			return manualReviewers[i].LastActivityTime.After(manualReviewers[j].LastActivityTime)
		}
		return manualReviewers[i].PRNumber < manualReviewers[j].PRNumber
	})
	sort.Slice(indexers, func(i, j int) bool {
		return indexers[i].LastActivityTime.After(indexers[j].LastActivityTime)
	})
	return summaries, manualReviewers, indexers
}

func selectorRoleLabel(role AgentRole) string {
	switch role {
	case RoleReviewer:
		return "reviewer"
	case RoleIndexer:
		return "repo indexer"
	default:
		return "coder"
	}
}

func normalizedAgentRole(role AgentRole) AgentRole {
	if strings.TrimSpace(string(role)) == "" {
		return RoleCoder
	}
	return role
}

func formatIssueHeadline(summary issueContextSummary) string {
	title := strings.TrimSpace(summary.IssueTitle)
	if title == "" {
		return fmt.Sprintf("issue #%d", summary.IssueNumber)
	}
	return fmt.Sprintf("issue #%d: %s", summary.IssueNumber, title)
}

func formatAgentStateSummary(agent *Agent) string {
	if agent == nil {
		return "none"
	}
	if agent.Paused {
		return fmt.Sprintf("%s paused", agent.State)
	}
	return string(agent.State)
}

func formatPRSummary(summary issueContextSummary) string {
	if summary.PRNumber <= 0 {
		return "none"
	}
	if strings.TrimSpace(summary.PRTitle) != "" {
		return fmt.Sprintf("#%d %s", summary.PRNumber, strings.TrimSpace(summary.PRTitle))
	}
	return fmt.Sprintf("#%d", summary.PRNumber)
}

func (m *AgentManager) SetState(agentID string, state AgentState, stopped bool) bool {
	unlockLifecycle := func() {}
	if stopped || terminalAgentState(state) {
		unlockLifecycle = m.lockReviewCoordinatorTerminalization(agentID)
	}
	defer unlockLifecycle()

	return m.setStateWithoutLifecycleGuard(agentID, state, stopped)
}

func (m *AgentManager) SetFailureMessage(agentID, message string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.FailureMessage = strings.TrimSpace(message)
	a.LastActivityTime = time.Now().UTC()
	return true
}

// setStateWithoutLifecycleGuard is used by terminal review-coordinator paths
// that already hold the coordinator lifecycle write lock across resource
// cleanup. Other terminal callers must use SetState so worker launches remain
// serialized with the state transition.
func (m *AgentManager) setStateWithoutLifecycleGuard(
	agentID string,
	state AgentState,
	stopped bool,
) bool {
	unlockScope := m.lockRuntimeScopeMutation(agentID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	if agentLifecycleTerminal(a) && !stopped && !terminalAgentState(state) {
		return false
	}
	a.State = state
	// Once cleanup or another terminal lifecycle path stops an agent, an
	// in-flight goroutine must not make it active again with a late update.
	a.Stopped = a.Stopped || stopped
	if stopped || state == StateDone || state == StateErrored || state == StateStopped || state == StateMerging {
		a.Paused = false
	}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func terminalAgentState(state AgentState) bool {
	switch state {
	case StateDone, StateErrored, StateStopped:
		return true
	default:
		return false
	}
}

func agentLifecycleTerminal(agent *Agent) bool {
	return agent != nil && (agent.Stopped || terminalAgentState(agent.State))
}

func (m *AgentManager) SetPR(agentID string, prNumber int, prTitle, prURL, prHeadSHA string) bool {
	unlockScope := m.lockRuntimeScopeMutation(agentID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.PRNumber = prNumber
	a.PRTitle = strings.TrimSpace(prTitle)
	a.PRURL = prURL
	a.ObservedPRHeadSHA = strings.TrimSpace(prHeadSHA)
	if !strings.EqualFold(a.LastPreReviewGateFailureHeadSHA, a.ObservedPRHeadSHA) {
		a.LastPreReviewGateFailureHeadSHA = ""
		a.LastPreReviewGateFailureAt = time.Time{}
	}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) SetPRHeadSHA(agentID, prHeadSHA string) bool {
	unlockScope := m.lockRuntimeScopeMutation(agentID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.ObservedPRHeadSHA = strings.TrimSpace(prHeadSHA)
	if !strings.EqualFold(a.LastPreReviewGateFailureHeadSHA, a.ObservedPRHeadSHA) {
		a.LastPreReviewGateFailureHeadSHA = ""
		a.LastPreReviewGateFailureAt = time.Time{}
	}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) SetHandoffCaptured(agentID string, captured bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.HandoffCaptured = captured
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) SetConflictHeadSHA(agentID, prHeadSHA string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.LastConflictHeadSHA = strings.TrimSpace(prHeadSHA)
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) Touch(agentID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) SetRuntimeHandle(agentID string, handle RuntimeHandle) bool {
	unlockScope := m.lockRuntimeScopeMutation(agentID)
	defer unlockScope()
	return m.setRuntimeHandleWithoutScopeGuard(agentID, handle)
}

func (m *AgentManager) setRuntimeHandleWithoutScopeGuard(
	agentID string,
	handle RuntimeHandle,
) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	if agentLifecycleTerminal(a) && strings.TrimSpace(handle.Session) != "" {
		return false
	}
	a.RuntimeHandle = handle
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) SetRuntimeProfile(agentID string, profile AgentProfile) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.RuntimeProfile = profile
	return true
}

func (m *AgentManager) MarkReviewCommentSeen(agentID string, commentID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	if a.seenReviewCommentIDs == nil {
		a.seenReviewCommentIDs = make(map[int64]struct{})
	}
	if _, exists := a.seenReviewCommentIDs[commentID]; exists {
		return false
	}
	a.seenReviewCommentIDs[commentID] = struct{}{}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) HasSeenReviewComment(agentID string, commentID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok || a.seenReviewCommentIDs == nil {
		return false
	}
	_, exists := a.seenReviewCommentIDs[commentID]
	return exists
}

func (m *AgentManager) MarkIssueCommentSeen(agentID string, commentID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	if a.seenIssueCommentIDs == nil {
		a.seenIssueCommentIDs = make(map[int64]struct{})
	}
	if _, exists := a.seenIssueCommentIDs[commentID]; exists {
		return false
	}
	a.seenIssueCommentIDs[commentID] = struct{}{}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) HasSeenIssueComment(agentID string, commentID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok || a.seenIssueCommentIDs == nil {
		return false
	}
	_, exists := a.seenIssueCommentIDs[commentID]
	return exists
}

func (m *AgentManager) AddPendingReviewComment(agentID string, commentID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	if a.pendingReviewCommentIDs == nil {
		a.pendingReviewCommentIDs = make(map[int64]struct{})
	}
	if _, exists := a.pendingReviewCommentIDs[commentID]; exists {
		return false
	}
	a.pendingReviewCommentIDs[commentID] = struct{}{}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) ClearPendingReviewComments(agentID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return 0
	}
	cleared := len(a.pendingReviewCommentIDs)
	if cleared == 0 {
		return 0
	}
	a.pendingReviewCommentIDs = make(map[int64]struct{})
	a.LastActivityTime = time.Now().UTC()
	return cleared
}

func (m *AgentManager) PendingReviewCommentCount(agentID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return 0
	}
	return len(a.pendingReviewCommentIDs)
}

func appendUniqueReviewGuidance(items []string, note string, limit int) []string {
	note = strings.TrimSpace(note)
	if note == "" {
		return append([]string(nil), items...)
	}
	out := make([]string, 0, len(items)+1)
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" || trimmed == note {
			continue
		}
		out = append(out, trimmed)
	}
	out = append(out, note)
	if limit > 0 && len(out) > limit {
		out = append([]string(nil), out[len(out)-limit:]...)
	}
	return out
}

func (m *AgentManager) AppendHumanReviewGuidance(agentID, note string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	const maxHumanReviewGuidance = 5
	updated := appendUniqueReviewGuidance(a.HumanReviewGuidance, note, maxHumanReviewGuidance)
	if strings.Join(updated, "\n") == strings.Join(a.HumanReviewGuidance, "\n") {
		return false
	}
	a.HumanReviewGuidance = updated
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) RecordManualReviewHold(agentID, reviewedHeadSHA string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	a.LastReviewedHeadSHA = strings.TrimSpace(reviewedHeadSHA)
	a.ManualReviewHold = true
	a.LastReviewVerdict = ""
	a.LastReviewCommentID = 0
	a.LastPreReviewGateFailureHeadSHA = ""
	a.LastPreReviewGateFailureAt = time.Time{}
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) ClearManualReviewHold(agentID, reviewedHeadSHA string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return false
	}
	headSHA := strings.TrimSpace(reviewedHeadSHA)
	if headSHA == "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(a.LastReviewedHeadSHA), headSHA) {
		return false
	}
	if a.LastReviewVerdict != "" || a.LastReviewCommentID != 0 {
		return false
	}
	a.LastReviewedHeadSHA = ""
	a.ManualReviewHold = false
	a.LastActivityTime = time.Now().UTC()
	return true
}

func (m *AgentManager) SetLastPoll(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastPoll = t
}

func (m *AgentManager) LastPoll() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastPoll
}

func (m *AgentManager) StopAllActive() []Agent {
	unlockLifecycles := m.lockAllReviewCoordinatorTerminalizations()
	defer unlockLifecycles()

	m.mu.Lock()
	agentIDs := make([]string, 0, len(m.agents))
	for agentID := range m.agents {
		agentIDs = append(agentIDs, agentID)
	}
	m.mu.Unlock()
	unlockScopes := m.lockRuntimeScopeMutations(agentIDs...)
	defer unlockScopes()

	m.mu.Lock()
	defer m.mu.Unlock()
	stopped := make([]Agent, 0)
	for _, a := range m.agents {
		if !a.Stopped && a.State != StateDone && a.State != StateStopped {
			a.State = StateStopped
			a.Stopped = true
			a.LastActivityTime = time.Now().UTC()
			stopped = append(stopped, cloneAgent(a))
		}
	}
	return stopped
}

func (m *AgentManager) Stop(agentID string) (Agent, bool) {
	unlockLifecycle := m.lockReviewCoordinatorTerminalization(agentID)
	defer unlockLifecycle()

	unlockScope := m.lockRuntimeScopeMutation(agentID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok {
		return Agent{}, false
	}
	a.State = StateStopped
	a.Paused = false
	a.Stopped = true
	a.LastActivityTime = time.Now().UTC()
	return cloneAgent(a), true
}

func (m *AgentManager) Pause(agentID string) (Agent, bool) {
	unlockLifecycle := m.lockReviewCoordinatorTerminalization(agentID)
	defer unlockLifecycle()

	return m.pauseWithoutLifecycleGuard(agentID)
}

func (m *AgentManager) pauseWithoutLifecycleGuard(
	agentID string,
) (Agent, bool) {
	unlockScope := m.lockRuntimeScopeMutation(agentID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok || agentLifecycleTerminal(a) {
		return Agent{}, false
	}
	a.Paused = true
	a.LastActivityTime = time.Now().UTC()
	return cloneAgent(a), true
}

func (m *AgentManager) Unpause(agentID string) (Agent, bool) {
	unlockScope := m.lockRuntimeScopeMutation(agentID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.agents[agentID]
	if !ok || agentLifecycleTerminal(a) {
		return Agent{}, false
	}
	a.Paused = false
	a.LastActivityTime = time.Now().UTC()
	return cloneAgent(a), true
}

type AgentMessenger interface {
	SendMessage(agent Agent, text string) error
	SendTask(agent Agent, markdown string) error
	SendContext(agent Agent, markdown string) error
	SendHandoff(agent Agent, content string) error
}

type idempotentAgentMessenger interface {
	SendMessageOnce(agent Agent, key string, text string) error
}

type FileAgentMessenger struct{}

func (m *FileAgentMessenger) SendMessage(agent Agent, text string) error {
	inboxDir := filepath.Join(agent.WorktreePath, ".repository-agent-orchestrator", "INBOX")
	if err := os.MkdirAll(inboxDir, 0o755); err != nil {
		return err
	}
	file := filepath.Join(inboxDir, fmt.Sprintf("%d.md", time.Now().UnixNano()))
	return os.WriteFile(file, []byte(text), 0o644)
}

func (m *FileAgentMessenger) SendMessageOnce(
	agent Agent,
	key string,
	text string,
) error {
	inboxDir := filepath.Join(
		agent.WorktreePath,
		".repository-agent-orchestrator",
		"INBOX",
	)
	if err := os.MkdirAll(inboxDir, 0o755); err != nil {
		return err
	}
	key = sanitizeSessionPart(strings.TrimSpace(key))
	if key == "" {
		return errors.New("idempotent inbox message key is empty")
	}
	path := filepath.Join(inboxDir, key+".md")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temp, err := os.CreateTemp(inboxDir, "."+key+"-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		_ = temp.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o644); err != nil {
		return err
	}
	if _, err := temp.WriteString(text); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	removeTemp = false
	return nil
}

func (m *FileAgentMessenger) SendTask(agent Agent, markdown string) error {
	dir := filepath.Join(agent.WorktreePath, ".repository-agent-orchestrator")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file := filepath.Join(dir, "TASK.md")
	return os.WriteFile(file, []byte(markdown), 0o644)
}

func (m *FileAgentMessenger) SendContext(agent Agent, markdown string) error {
	dir := filepath.Join(agent.WorktreePath, ".repository-agent-orchestrator")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file := filepath.Join(dir, "CONTEXT.md")
	return os.WriteFile(file, []byte(markdown), 0o644)
}

func (m *FileAgentMessenger) SendHandoff(agent Agent, content string) error {
	dir := filepath.Join(agent.WorktreePath, ".repository-agent-orchestrator")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	file := filepath.Join(dir, "HANDOFF.yaml")
	return os.WriteFile(file, []byte(content), 0o644)
}

type WebexNotifier struct {
	webhookURL string
	client     *http.Client
}

func NewWebexNotifier(webhookURL string) *WebexNotifier {
	return &WebexNotifier{
		webhookURL: webhookURL,
		client:     &http.Client{Timeout: 15 * time.Second},
	}
}

func (w *WebexNotifier) Send(ctx context.Context, markdown string) error {
	payload := map[string]string{"markdown": markdown}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode webex payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create webex request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webex request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("webex webhook returned status code %d", resp.StatusCode)
	}
	return nil
}

type Orchestrator struct {
	cfg       Config
	token     string
	github    *github.Client
	notifier  *WebexNotifier
	messenger AgentMessenger
	agents    *AgentManager
	runner    Runner
	cmdRunner commandRunnerFunc
	// Mandatory tests should preserve failure output tails for diagnosis.
	mandatoryTestRunner             mandatoryTestRunnerFunc
	tailRunner                      agentTailRunnerFunc
	cleanupWorktreeFunc             cleanupWorktreeFunc
	prepareReviewWorkerWorktreeFunc prepareReviewWorkerWorktreeFunc
	reviewDiscoveryLaneRunner       reviewDiscoveryLaneRunnerFunc
	reviewVerificationRunner        reviewVerificationAssignmentRunnerFunc
	reviewChallengeRunner           reviewChallengeAssignmentRunnerFunc
	reviewCoordinatorGit            reviewCoordinatorGit
	reviewCoordinatorPullRequests   reviewCoordinatorPullRequestGetter
	reviewHeadResolver              reviewHeadResolverFunc
	reviewSuccessorLauncher         reviewSuccessorLauncherFunc
	// When true, review verdict handling runs outside the poll loop.
	asyncReviewVerdictHandling bool
	reviewLaunchMu             sync.Mutex
	pendingReviewLaunches      map[string]struct{}
	reviewRecoveryMu           sync.Mutex
	reviewRecoveryWG           sync.WaitGroup
	reviewRecoveryClosing      bool
	reviewCycleRecoveryMu      sync.Mutex
	reviewCycleRecoveryWG      sync.WaitGroup
	reviewCycleRecoveryClosing bool
	reviewCycleRecoveryPending map[string]struct{}
	reviewCycleRecoveryInitMu  sync.Mutex
	reviewCycleRecoveryReady   bool
	reviewArtifactMu           sync.Mutex
	reviewDiscoveryPassMu      sync.Mutex
	reviewDiscoveryPassByAgent map[string]*reviewDiscoveryPassRuntime
	reviewDiscoveryPassBlocked map[string]struct{}
	reviewCoordinatorRunMu     sync.Mutex
	reviewCoordinatorRuns      map[string]struct{}
	reviewCleanupMu            sync.Mutex
	reviewCleanupWG            sync.WaitGroup
	reviewCleanupClosing       bool
	statePersistenceMu         sync.Mutex
	reviewVerdictApplicationMu sync.Mutex
	reviewVerdictPublicationMu sync.Mutex
	gitWorktreeMutationInitMu  sync.Mutex
	gitWorktreeMutationSlot    chan struct{}

	runtimeLoopMu           sync.Mutex
	runtimeLoopByAgent      map[string]runtimeLoopStatus
	inputWaitMu             sync.Mutex
	inputWaitByAgent        map[string]inputWaitStatus
	reviewGateMu            sync.Mutex
	reviewGateByAgent       map[string]reviewGateStatus
	hardGateMu              sync.Mutex
	hardGateSlot            chan struct{}
	reviewGateCancelMu      sync.Mutex
	reviewGateCancelByAgent map[string]context.CancelFunc

	reviewCycleDiagnosisMu   sync.Mutex
	reviewCycleLastDiagnosis map[string]reviewCycleDiagnosisRecord

	// techSupportBundleMu serializes the entire generateTechSupportBundle
	// call (not just the REPL-output capture within it): it can be
	// triggered both interactively (REPL `tech-support`) and
	// automatically (checkReviewCycleHealth on unknown_stall), and two
	// overlapping calls previously could race on the same
	// second-precision bundle filename via os.Create, which truncates.
	techSupportBundleMu sync.Mutex
}

type commandRunnerFunc func(ctx context.Context, dir string, name string, args ...string) error
type mandatoryTestRunnerFunc func(ctx context.Context, dir string, logPath string, name string, args ...string) error
type agentTailRunnerFunc func(logPath string) error
type cleanupWorktreeFunc func(ctx context.Context, repoPath, worktreePath, branchName string) error
type prepareReviewWorkerWorktreeFunc func(ctx context.Context, worker Agent) error
type reviewHeadResolverFunc func(ctx context.Context, prNumber int) (string, error)
type reviewSuccessorLauncherFunc func(
	ctx context.Context,
	staleReviewer Agent,
	headSHA string,
) error

type inputWaitStatus struct {
	lastPaneHash string
	lastChange   time.Time
	alerted      bool
	autoNudges   int
}

type runtimeLoopStatus struct {
	lastPaneHash string
	lastChange   time.Time
	alerted      bool
}

type reviewGateStatus struct {
	alerted   bool
	started   bool
	startedAt time.Time
}

func NewOrchestrator(ctx context.Context, cfg Config) (*Orchestrator, error) {
	token, err := getGitHubTokenFromGH(ctx)
	if err != nil {
		return nil, err
	}
	runtimeRunner, err := NewRepositoryTmuxRunner(cfg.CodexCmd, cfg, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to configure runtime isolation: %w", err)
	}

	ghClient, err := newGitHubClient(token)
	if err != nil {
		return nil, fmt.Errorf("failed to configure GitHub client: %w", err)
	}
	bot := &Orchestrator{
		cfg:                           cfg,
		token:                         token,
		github:                        ghClient,
		notifier:                      NewWebexNotifier(cfg.WebexWebhookURL),
		messenger:                     &FileAgentMessenger{},
		agents:                        NewAgentManager(),
		runner:                        runtimeRunner,
		cmdRunner:                     runCommandWithOutput,
		mandatoryTestRunner:           runCommandWithOutputAndLiveLog,
		tailRunner:                    runAgentTailCommand,
		asyncReviewVerdictHandling:    true,
		pendingReviewLaunches:         make(map[string]struct{}),
		reviewCycleRecoveryPending:    make(map[string]struct{}),
		reviewCoordinatorGit:          osReviewCoordinatorGit{},
		reviewCoordinatorPullRequests: ghClient.PullRequests,
		runtimeLoopByAgent:            make(map[string]runtimeLoopStatus),
		inputWaitByAgent:              make(map[string]inputWaitStatus),
	}
	if err := bot.loadPersistedAgentState(); err != nil {
		return nil, fmt.Errorf("failed to load persisted state: %w", err)
	}
	return bot, nil
}

func newGitHubClient(token string) (*github.Client, error) {
	return github.NewClient(
		github.WithAuthToken(token),
		github.WithTimeout(defaultGitHubHTTPTimeout),
	)
}

func (b *Orchestrator) notify(ctx context.Context, markdown string) {
	if b.notifier == nil {
		return
	}
	message := b.sanitizeSensitiveText(b.formatNotificationMessage(markdown))
	notifyCtx, cancel := context.WithTimeout(context.Background(), webexNotificationTimeout)
	defer cancel()
	if err := b.notifier.Send(notifyCtx, message); err != nil {
		log.Printf("webex notification failed: %s", b.safeError(err))
	}
}

func (b *Orchestrator) hardGateSemaphore() chan struct{} {
	if b == nil {
		return nil
	}
	b.hardGateMu.Lock()
	defer b.hardGateMu.Unlock()
	if b.hardGateSlot == nil {
		b.hardGateSlot = make(chan struct{}, 1)
		b.hardGateSlot <- struct{}{}
	}
	return b.hardGateSlot
}

func (b *Orchestrator) acquireHardGateSlot(ctx context.Context) error {
	if b == nil || b.cfg.effectiveHardGateMode() != HardGateModeSerial {
		return nil
	}
	sem := b.hardGateSemaphore()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sem:
		return nil
	}
}

func (b *Orchestrator) releaseHardGateSlot() {
	if b == nil || b.cfg.effectiveHardGateMode() != HardGateModeSerial {
		return
	}
	sem := b.hardGateSemaphore()
	select {
	case sem <- struct{}{}:
	default:
	}
}

func (b *Orchestrator) registerReviewGateCancel(agentID string, cancel context.CancelFunc) {
	if b == nil || cancel == nil {
		return
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return
	}
	b.reviewGateCancelMu.Lock()
	defer b.reviewGateCancelMu.Unlock()
	if b.reviewGateCancelByAgent == nil {
		b.reviewGateCancelByAgent = make(map[string]context.CancelFunc)
	}
	b.reviewGateCancelByAgent[agentID] = cancel
}

func (b *Orchestrator) clearReviewGateCancel(agentID string) {
	if b == nil {
		return
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return
	}
	b.reviewGateCancelMu.Lock()
	defer b.reviewGateCancelMu.Unlock()
	if b.reviewGateCancelByAgent == nil {
		return
	}
	delete(b.reviewGateCancelByAgent, agentID)
}

func (b *Orchestrator) cancelReviewGate(agentID string) {
	if b == nil {
		return
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return
	}
	b.reviewGateCancelMu.Lock()
	cancel := b.reviewGateCancelByAgent[agentID]
	b.reviewGateCancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (b *Orchestrator) formatNotificationMessage(markdown string) string {
	message := strings.TrimSpace(markdown)
	if message == "" {
		return b.notificationPrefix()
	}
	if strings.HasPrefix(message, "Repository Agent Orchestrator (") {
		return message
	}
	if strings.HasPrefix(message, "Repository Agent Orchestrator:") {
		message = strings.TrimSpace(strings.TrimPrefix(message, "Repository Agent Orchestrator:"))
	} else if strings.HasPrefix(message, "Repository Agent Orchestrator ") {
		message = strings.TrimSpace(strings.TrimPrefix(message, "Repository Agent Orchestrator "))
	}
	return fmt.Sprintf("%s: %s", b.notificationPrefix(), message)
}

func (b *Orchestrator) notificationPrefix() string {
	repo := strings.TrimSpace(b.cfg.RepoName)
	if repo == "" {
		return "Repository Agent Orchestrator"
	}
	return fmt.Sprintf("Repository Agent Orchestrator (%s)", repo)
}

// notifyAgentFailure sends the given failure message plus a truncated
// excerpt of the underlying error. Operators are often not watching the
// REPL when one of these fires -- an unattended poll-tick reconciliation,
// or a REPL command whose result they've stopped waiting on -- so a bare
// "failed to prepare worktree" notification leaves them unable to act
// without first finding and re-running the failing command by hand.
// Surfacing the real cause (e.g. a `git`/`ssh` credential or passphrase
// prompt refusing to hang: "Permission denied (publickey)") directly in
// the notification lets them fix it from wherever they saw the alert.
func (b *Orchestrator) notifyAgentFailure(ctx context.Context, message string, err error) {
	if detail := abbreviateLogText(b.safeError(err), 600); detail != "" {
		message = message + "\n\nReason:\n```\n" + detail + "\n```"
	}
	b.notify(ctx, message)
}

func (b *Orchestrator) safeError(err error) string {
	if err == nil {
		return ""
	}
	return b.sanitizeSensitiveText(err.Error())
}

func (b *Orchestrator) sanitizeSensitiveText(text string) string {
	msg := text
	if b.token != "" {
		msg = strings.ReplaceAll(msg, b.token, "[REDACTED]")
	}
	if b.cfg.WebexWebhookURL != "" {
		msg = strings.ReplaceAll(msg, b.cfg.WebexWebhookURL, "[REDACTED]")
	}
	return msg
}

func (b *Orchestrator) persistAgentStateNonFatal(contextLabel string) {
	if b == nil {
		return
	}
	if err := b.persistAgentState(); err != nil {
		log.Printf("agent state persistence failed context=%s: %s", contextLabel, b.safeError(err))
	}
}

func (b *Orchestrator) ContinueAgentForIssuePR(ctx context.Context, issueNumber, prNumber int) error {
	defer b.persistAgentStateNonFatal("continue-agent")

	if issueNumber <= 0 {
		return fmt.Errorf("invalid issue number: %d", issueNumber)
	}
	if prNumber <= 0 {
		return fmt.Errorf("invalid PR number: %d", prNumber)
	}
	if b.agents.HasActiveIssue(issueNumber) {
		return fmt.Errorf("an active agent for issue #%d already exists", issueNumber)
	}
	if b.agents.HasActivePR(prNumber) {
		return fmt.Errorf("an active agent for PR #%d already exists", prNumber)
	}

	issue, _, err := b.github.Issues.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, issueNumber)
	if err != nil {
		return fmt.Errorf("failed to fetch issue #%d: %w", issueNumber, err)
	}
	if issue.GetPullRequestLinks() != nil {
		return fmt.Errorf("issue #%d is a pull request, cannot continue agent", issueNumber)
	}

	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, prNumber)
	if err != nil {
		return fmt.Errorf("failed to fetch PR #%d: %w", prNumber, err)
	}
	if pr.GetMerged() {
		return fmt.Errorf("cannot continue merged PR #%d", prNumber)
	}
	if !strings.EqualFold(strings.TrimSpace(pr.GetState()), "open") {
		return fmt.Errorf("cannot continue PR #%d because it is %s", prNumber, fallback(strings.TrimSpace(pr.GetState()), "not open"))
	}
	baseBranch := strings.TrimSpace(pr.GetBase().GetRef())
	if !strings.EqualFold(baseBranch, b.defaultBaseBranch()) {
		return fmt.Errorf("cannot continue PR #%d because base branch %q does not match configured base branch %q", prNumber, baseBranch, b.defaultBaseBranch())
	}
	headBranch := strings.TrimSpace(pr.GetHead().GetRef())
	if headBranch == "" {
		return fmt.Errorf("cannot continue PR #%d because its head branch is empty", prNumber)
	}
	headSHA := strings.TrimSpace(pr.GetHead().GetSHA())
	if headSHA == "" {
		return fmt.Errorf("cannot continue PR #%d because its head SHA is empty", prNumber)
	}
	headRepo := pr.GetHead().GetRepo()
	headRepoName := strings.TrimSpace(headRepo.GetFullName())
	if headRepoName == "" && headRepo != nil {
		headRepoName = strings.Trim(strings.TrimSpace(headRepo.GetOwner().GetLogin())+"/"+strings.TrimSpace(headRepo.GetName()), "/")
	}
	wantRepoName := strings.Trim(strings.TrimSpace(b.cfg.RepoOwner)+"/"+strings.TrimSpace(b.cfg.RepoName), "/")
	if !strings.EqualFold(headRepoName, wantRepoName) {
		return fmt.Errorf("cannot continue PR #%d because head repository %q is not configured repository %q; fork PR continuation is unsupported", prNumber, fallback(headRepoName, "unknown"), wantRepoName)
	}

	reviewComments, err := b.listAllReviewComments(ctx, prNumber)
	if err != nil {
		return fmt.Errorf("failed to baseline review comments for PR #%d: %w", prNumber, err)
	}
	issueComments, err := b.listAllIssueComments(ctx, prNumber)
	if err != nil {
		return fmt.Errorf("failed to baseline issue comments for PR #%d: %w", prNumber, err)
	}
	seenReviewCommentIDs := make(map[int64]struct{}, len(reviewComments))
	for _, comment := range reviewComments {
		seenReviewCommentIDs[comment.GetID()] = struct{}{}
	}
	seenIssueCommentIDs := make(map[int64]struct{}, len(issueComments))
	for _, comment := range issueComments {
		seenIssueCommentIDs[comment.GetID()] = struct{}{}
	}

	runtimeProfile, err := b.cfg.runtimeProfileForRole(AgentProfileRoleCoder)
	if err != nil {
		return fmt.Errorf("failed to resolve coder runtime profile: %w", err)
	}
	now := time.Now().UTC()
	agent := &Agent{
		ID:                      newAgentID(RoleCoder, issueNumber, now),
		Role:                    RoleCoder,
		IssueNumber:             issueNumber,
		IssueTitle:              issue.GetTitle(),
		IssueBody:               issue.GetBody(),
		WorktreePath:            filepath.Join(b.cfg.WorktreeDir, fmt.Sprintf("continue-issue-%d-pr-%d-%d", issueNumber, prNumber, now.Unix())),
		LogDir:                  b.cfg.LogDir,
		RuntimeCWD:              "",
		BranchName:              fmt.Sprintf("repository-agent-orchestrator/continue-issue-%d-pr-%d", issueNumber, prNumber),
		PRHeadBranch:            headBranch,
		AdoptedPR:               true,
		PRNumber:                prNumber,
		PRTitle:                 pr.GetTitle(),
		PRURL:                   pr.GetHTMLURL(),
		ObservedPRHeadSHA:       headSHA,
		LastReviewedHeadSHA:     headSHA,
		RuntimeProfile:          runtimeProfile,
		State:                   StateInitializing,
		LastActivityTime:        now,
		seenReviewCommentIDs:    seenReviewCommentIDs,
		seenIssueCommentIDs:     seenIssueCommentIDs,
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	agent.RuntimeCWD = agent.WorktreePath
	if err := b.agents.Add(agent); err != nil {
		return err
	}

	log.Printf("continuing agent=%s issue=%d pr=%d worktree=%s branch=%s pr_head_branch=%s", agent.ID, issueNumber, prNumber, agent.WorktreePath, agent.BranchName, agent.PRHeadBranch)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: %s `%s` initialized to continue issue #%d on PR #%d", agentRoleLabel(agent.Role), agent.ID, issueNumber, prNumber))

	if err := b.prepareContinuationWorktree(ctx, *agent); err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to prepare continuation worktree for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
		return fmt.Errorf("failed to prepare continuation worktree for agent %s: %w", agent.ID, err)
	}
	cleanupOnError := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if cleanupErr := b.cleanupWorktree(cleanupCtx, agent.WorktreePath, agent.BranchName); cleanupErr != nil {
			log.Printf("non-fatal: failed to cleanup continuation worktree agent=%s: %s", agent.ID, b.safeError(cleanupErr))
		}
	}

	if b.messenger != nil {
		if err := b.messenger.SendTask(*agent, b.buildContinuationTaskMarkdown(*agent, issue)); err != nil {
			b.agents.SetState(agent.ID, StateErrored, false)
			cleanupOnError()
			b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to write continuation task file for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
			return fmt.Errorf("failed to write continuation task file for agent %s: %w", agent.ID, err)
		}
		if err := b.writeAgentContextArtifacts(*agent, agentContextRequest{
			Role:        RoleCoder,
			IssueNumber: issueNumber,
			IssueTitle:  issue.GetTitle(),
			IssueBody:   issue.GetBody(),
			PRNumber:    prNumber,
		}); err != nil {
			b.agents.SetState(agent.ID, StateErrored, false)
			cleanupOnError()
			b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to write continuation context artifacts for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
			return fmt.Errorf("failed to write continuation context artifacts for agent %s: %w", agent.ID, err)
		}
	}

	handle, err := b.launchRuntime(
		*agent,
		formatContinuationPrompt(
			issue,
			pr,
			*agent,
			b.cfg.RepoPath,
			b.cfg.BaseBranch,
			b.cfg.MandatoryTests,
		),
		true,
	)
	if err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		cleanupOnError()
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to launch continuation runtime for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
		return fmt.Errorf("failed to launch continuation codex runtime: %w", err)
	}

	b.agents.SetState(agent.ID, StateWorking, false)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: runtime launched for %s `%s` to continue issue #%d on PR #%d", agentRoleLabel(agent.Role), agent.ID, issueNumber, prNumber))
	log.Printf("continuation agent ready id=%s issue=%d pr=%d runtime=%s session=%s", agent.ID, issueNumber, prNumber, handle.Kind, handle.Session)
	return nil
}

func (b *Orchestrator) InitAgentForIssue(ctx context.Context, issueNumber int) error {
	defer b.persistAgentStateNonFatal("init-agent")

	if b.agents.HasActiveIssue(issueNumber) {
		return fmt.Errorf("an active agent for issue #%d already exists", issueNumber)
	}

	issue, _, err := b.github.Issues.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, issueNumber)
	if err != nil {
		return fmt.Errorf("failed to fetch issue #%d: %w", issueNumber, err)
	}
	if issue.GetPullRequestLinks() != nil {
		return fmt.Errorf("issue #%d is a pull request, cannot initialize agent", issueNumber)
	}

	runtimeProfile, err := b.cfg.runtimeProfileForRole(AgentProfileRoleCoder)
	if err != nil {
		return fmt.Errorf("failed to resolve coder runtime profile: %w", err)
	}
	now := time.Now().UTC()
	agentID := newAgentID(RoleCoder, issueNumber, now)
	branch := fmt.Sprintf("repository-agent-orchestrator/issue-%d", issueNumber)
	worktreePath := filepath.Join(b.cfg.WorktreeDir, fmt.Sprintf("issue-%d-%d", issueNumber, now.Unix()))

	agent := &Agent{
		ID:                      agentID,
		Role:                    RoleCoder,
		IssueNumber:             issueNumber,
		IssueTitle:              issue.GetTitle(),
		IssueBody:               issue.GetBody(),
		WorktreePath:            worktreePath,
		LogDir:                  b.cfg.LogDir,
		RuntimeCWD:              worktreePath,
		BranchName:              branch,
		RuntimeProfile:          runtimeProfile,
		State:                   StateInitializing,
		LastActivityTime:        time.Now().UTC(),
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := b.agents.Add(agent); err != nil {
		return err
	}

	log.Printf("initializing agent=%s issue=%d worktree=%s branch=%s", agent.ID, agent.IssueNumber, agent.WorktreePath, agent.BranchName)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: %s `%s` initialized for issue #%d", agentRoleLabel(agent.Role), agent.ID, issueNumber))

	if err := b.prepareCoderWorktree(ctx, *agent); err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to prepare worktree for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
		return fmt.Errorf("failed to prepare worktree for agent %s: %w", agent.ID, err)
	}

	if b.messenger != nil {
		taskMarkdown := b.buildTaskMarkdown(*agent, issue)
		if err := b.messenger.SendTask(*agent, taskMarkdown); err != nil {
			b.agents.SetState(agent.ID, StateErrored, false)
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if cleanupErr := b.cleanupWorktree(cleanupCtx, agent.WorktreePath, agent.BranchName); cleanupErr != nil {
				log.Printf("non-fatal: failed to cleanup worktree after task write error agent=%s: %s", agent.ID, b.safeError(cleanupErr))
			}
			b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to write task file for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
			return fmt.Errorf("failed to write task file for agent %s: %w", agent.ID, err)
		}
		if err := b.writeAgentContextArtifacts(*agent, agentContextRequest{
			Role:        RoleCoder,
			IssueNumber: issueNumber,
			IssueTitle:  issue.GetTitle(),
			IssueBody:   issue.GetBody(),
		}); err != nil {
			b.agents.SetState(agent.ID, StateErrored, false)
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if cleanupErr := b.cleanupWorktree(cleanupCtx, agent.WorktreePath, agent.BranchName); cleanupErr != nil {
				log.Printf("non-fatal: failed to cleanup worktree after context write error agent=%s: %s", agent.ID, b.safeError(cleanupErr))
			}
			b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to write context artifacts for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
			return fmt.Errorf("failed to write context artifacts for agent %s: %w", agent.ID, err)
		}
	}

	initialPrompt := formatInitialPrompt(issue, *agent, b.cfg.RepoPath, b.cfg.BaseBranch, b.cfg.MandatoryTests)
	handle, err := b.launchRuntime(*agent, initialPrompt, true)
	if err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if cleanupErr := b.cleanupWorktree(cleanupCtx, agent.WorktreePath, agent.BranchName); cleanupErr != nil {
			log.Printf("non-fatal: failed to cleanup worktree after runtime launch failure agent=%s: %s", agent.ID, b.safeError(cleanupErr))
		}
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to launch runtime for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
		return fmt.Errorf("failed to launch codex runtime: %w", err)
	}
	agent.RuntimeHandle = handle

	b.agents.SetState(agent.ID, StateWorking, false)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: runtime launched for %s `%s` (issue #%d)", agentRoleLabel(agent.Role), agent.ID, issueNumber))
	if strings.TrimSpace(handle.LogPath) != "" {
		log.Printf("agent ready id=%s issue=%d runtime=%s session=%s log=%s", agent.ID, issueNumber, handle.Kind, handle.Session, handle.LogPath)
	} else {
		log.Printf("agent ready id=%s issue=%d runtime=%s session=%s", agent.ID, issueNumber, handle.Kind, handle.Session)
	}
	return nil
}

func (b *Orchestrator) LaunchRepoIndexAgent(ctx context.Context) (string, error) {
	defer b.persistAgentStateNonFatal("launch-repo-index-agent")

	if b == nil || b.agents == nil {
		return "", errors.New("agent manager is not configured")
	}
	if b.agents.HasActiveRole(RoleIndexer) {
		return "", errors.New("an active repo indexing agent already exists")
	}

	runtimeProfile, err := b.cfg.runtimeProfileForRole(AgentProfileRoleIndexer)
	if err != nil {
		return "", fmt.Errorf("failed to resolve indexer runtime profile: %w", err)
	}
	now := time.Now().UTC()
	agentID := newAgentID(RoleIndexer, 0, now)
	branch := fmt.Sprintf("repository-agent-orchestrator/repo-index-%d", now.Unix())
	worktreePath := filepath.Join(b.cfg.WorktreeDir, fmt.Sprintf("repo-index-%d", now.Unix()))

	agent := &Agent{
		ID:                      agentID,
		Role:                    RoleIndexer,
		IssueTitle:              "Build repository markdown index",
		WorktreePath:            worktreePath,
		LogDir:                  b.cfg.LogDir,
		RuntimeCWD:              worktreePath,
		BranchName:              branch,
		RuntimeProfile:          runtimeProfile,
		State:                   StateInitializing,
		LastActivityTime:        now,
		seenReviewCommentIDs:    make(map[int64]struct{}),
		seenIssueCommentIDs:     make(map[int64]struct{}),
		pendingReviewCommentIDs: make(map[int64]struct{}),
	}
	if err := b.agents.Add(agent); err != nil {
		return "", err
	}

	log.Printf("initializing agent=%s role=%s worktree=%s branch=%s", agent.ID, agent.Role, agent.WorktreePath, agent.BranchName)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: %s `%s` initialized for repository `%s/%s`", agentRoleLabel(agent.Role), agent.ID, b.cfg.RepoOwner, b.cfg.RepoName))

	if err := b.prepareCoderWorktree(ctx, *agent); err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to prepare worktree for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
		return "", fmt.Errorf("failed to prepare worktree for agent %s: %w", agent.ID, err)
	}
	agent.RepoIndexHadSource, agent.RepoIndexSourceBaselineTime = repoIndexSourceBaseline(*agent)
	agent.RepoIndexHadOutput, agent.RepoIndexBaselineModTime = repoIndexOutputBaseline(*agent)

	if b.messenger != nil {
		taskMarkdown := b.buildRepoIndexTaskMarkdown(*agent)
		if err := b.messenger.SendTask(*agent, taskMarkdown); err != nil {
			b.agents.SetState(agent.ID, StateErrored, false)
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if cleanupErr := b.cleanupWorktree(cleanupCtx, agent.WorktreePath, agent.BranchName); cleanupErr != nil {
				log.Printf("non-fatal: failed to cleanup worktree after task write error agent=%s: %s", agent.ID, b.safeError(cleanupErr))
			}
			b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to write task file for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
			return "", fmt.Errorf("failed to write task file for agent %s: %w", agent.ID, err)
		}
	}

	initialPrompt := formatRepoIndexPrompt(*agent, b.cfg.RepoPath, b.cfg.BaseBranch)
	handle, err := b.launchRuntime(*agent, initialPrompt, true)
	if err != nil {
		b.agents.SetState(agent.ID, StateErrored, false)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if cleanupErr := b.cleanupWorktree(cleanupCtx, agent.WorktreePath, agent.BranchName); cleanupErr != nil {
			log.Printf("non-fatal: failed to cleanup worktree after runtime launch failure agent=%s: %s", agent.ID, b.safeError(cleanupErr))
		}
		b.notifyAgentFailure(ctx, fmt.Sprintf("Repository Agent Orchestrator: failed to launch runtime for %s `%s`", agentRoleLabel(agent.Role), agent.ID), err)
		return "", fmt.Errorf("failed to launch codex runtime: %w", err)
	}
	agent.RuntimeHandle = handle

	b.agents.SetState(agent.ID, StateWorking, false)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: runtime launched for %s `%s`", agentRoleLabel(agent.Role), agent.ID))
	if strings.TrimSpace(handle.LogPath) != "" {
		log.Printf("agent ready id=%s role=%s runtime=%s session=%s log=%s", agent.ID, agent.Role, handle.Kind, handle.Session, handle.LogPath)
	} else {
		log.Printf("agent ready id=%s role=%s runtime=%s session=%s", agent.ID, agent.Role, handle.Kind, handle.Session)
	}
	return agent.ID, nil
}

func (b *Orchestrator) buildTaskMarkdown(agent Agent, issue *github.Issue) string {
	body := strings.TrimSpace(issue.GetBody())
	if body == "" {
		body = "(no body provided)"
	}
	mandatoryTests := formatMandatoryTestsMarkdownList(b.cfg.MandatoryTests, "   ")

	return fmt.Sprintf(
		"# Repository Agent Orchestrator Task\n\n"+
			"Issue: #%d\n"+
			"Title: %s\n\n"+
			"## Issue Body\n\n"+
			"%s\n\n"+
			"## Instructions\n\n"+
			"1. Run the mandatory test commands before making changes:\n%s\n"+
			"2. Fix the issue in this worktree.\n"+
			"3. Do not create or modify `AGENTS.md` unless the issue explicitly requires it.\n"+
			"4. %s\n"+
			"5. Run the same mandatory test commands again after making changes:\n%s\n"+
			"6. Commit your changes.\n"+
			"7. Push branch `%s`.\n"+
			"8. Open a draft pull request against `%s`.\n"+
			"9. Every GitHub PR comment or reply you post must end with:\n"+
			"   - `CODEX_AGENT_ID: %s`\n"+
			"   - `CODEX_AGENT_ROLE: coder`\n"+
			"10. When writing PR comments/replies, use real line breaks and Markdown; never include literal `\\n` sequences in posted text.\n",
		issue.GetNumber(),
		strings.TrimSpace(issue.GetTitle()),
		body,
		mandatoryTests,
		runtimeArtifactsCommitRule,
		mandatoryTests,
		fmt.Sprintf("repository-agent-orchestrator/issue-%d", issue.GetNumber()),
		b.cfg.BaseBranch,
		fallback(strings.TrimSpace(agent.ID), "(unknown-agent-id)"),
	)
}

func (b *Orchestrator) buildContinuationTaskMarkdown(agent Agent, issue *github.Issue) string {
	body := strings.TrimSpace(issue.GetBody())
	if body == "" {
		body = "(no body provided)"
	}
	mandatoryTests := formatMandatoryTestsMarkdownList(b.cfg.MandatoryTests, "   ")
	return fmt.Sprintf(
		"# Repository Agent Orchestrator Continuation Task\n\n"+
			"Issue: #%d\nTitle: %s\nExisting PR: #%d (%s)\nPR head branch: `%s`\n\n"+
			"## Issue Body\n\n%s\n\n"+
			"## Instructions\n\n"+
			"1. Inspect the existing PR description, commits, diff, checks, reviews, review threads, and comments before editing.\n"+
			"2. Run the mandatory test commands to establish the current baseline:\n%s\n"+
			"3. Continue the in-progress implementation in this worktree and address applicable existing feedback.\n"+
			"4. Do not create or modify `AGENTS.md` unless the issue explicitly requires it.\n"+
			"5. %s\n"+
			"6. Run the same mandatory test commands after making changes:\n%s\n"+
			"7. Commit your changes and update the existing PR with `%s`. Do not create another PR.\n"+
			"8. Every GitHub PR comment or reply you post must end with:\n"+
			"   - `CODEX_AGENT_ID: %s`\n"+
			"   - `CODEX_AGENT_ROLE: coder`\n"+
			"9. Use real line breaks and Markdown in GitHub comments; never include literal `\\n` sequences.\n",
		issue.GetNumber(),
		strings.TrimSpace(issue.GetTitle()),
		agent.PRNumber,
		agent.PRURL,
		agent.PRHeadBranch,
		body,
		mandatoryTests,
		runtimeArtifactsCommitRule,
		mandatoryTests,
		continuationPushCommand(agent),
		fallback(strings.TrimSpace(agent.ID), "(unknown-agent-id)"),
	)
}

func (b *Orchestrator) buildRepoIndexTaskMarkdown(agent Agent) string {
	return fmt.Sprintf(
		"# Repository Agent Orchestrator Repository Index Task\n\n"+
			"Repository: %s/%s\n"+
			"Repo path: %s\n"+
			"Base branch: %s\n"+
			"Canonical source: agent_index.yaml\n"+
			"Generated output: INDEX.md\n\n"+
			"## Goal\n\n"+
			"Create or update a top-level `agent_index.yaml` that describes the repository's Markdown docs, then generate a matching top-level `INDEX.md` from that YAML so the two files do not drift.\n\n"+
			"## Required workflow\n\n"+
			"1. Work only in this worktree.\n"+
			"2. Enumerate tracked Markdown files across the entire repo, excluding the generated top-level `INDEX.md`.\n"+
			"3. Read the relevant Markdown files across the repo, including top-level files such as `README.md` and `AGENTS.md` when present.\n"+
			"4. Create or update the canonical top-level `agent_index.yaml`.\n"+
			"5. Generate a matching top-level `INDEX.md` from `agent_index.yaml`.\n"+
			"6. Keep naming regular for deterministic reuse.\n"+
			"7. Use repo structure only as a hint for candidate components and artifacts; do not automatically promote every package or directory into the schema.\n"+
			"8. Keep both files concise, factual, and optimized for retrieval.\n"+
			"9. Do not modify non-documentation source files.\n"+
			"10. %s\n"+
			"11. Commit the documentation changes using the repository's commit style.\n"+
			"12. Use real newlines in the commit body rather than literal `\\n` sequences.\n"+
			"13. Push branch `%s` and open a pull request against `%s`.\n"+
			"14. There is no Repository Agent Orchestrator review-agent cycle for this PR; once the PR is open, stop and leave it ready for human review.\n\n"+
			"## agent_index.yaml schema\n\n"+
			"- Store repository-level metadata plus per-document entries.\n"+
			"- Repository metadata should include at least:\n"+
			"  - repository summary\n"+
			"  - preferred starting points\n"+
			"  - stable components\n"+
			"  - primary concerns\n"+
			"- Define top-level groups as a mapping from group name to a mapping with a `summary` field (never a bare string), and make every document `group` value reference one of those defined groups.\n"+
			"- Each entry should include at least:\n"+
			"  - path\n"+
			"  - group\n"+
			"  - kind\n"+
			"  - summary\n"+
			"  - components\n"+
			"  - concerns\n"+
			"  - artifacts\n"+
			"  - read_when\n"+
			"  - usually_skip_when\n"+
			"  - priority\n"+
			"- `kind` must use only: `overview`, `agent_contract`, `architecture`, `spec`, `api_spec`, `security_model`, `adr`, `runbook`, `operations`, `cli_guide`, `testing_guide`, `test_architecture`, `test_summary`, `reference`, `example_note`.\n"+
			"- `priority` must use only: `foundational`, `important`, `targeted`, `situational`.\n"+
			"- `components`, `concerns`, and `artifacts` must use lowercase `snake_case` identifiers.\n"+
			"- `read_when` and `usually_skip_when` must stay as arrays, even for a single item.\n"+
			"- `components` are durable repo subsystems only.\n"+
			"- `concerns` are problem or topic tags.\n"+
			"- `artifacts` are named interfaces, CLIs, CRDs, fixtures, scripts, endpoints, or other concrete mechanisms.\n"+
			"- Use repo structure as a source of candidate names, but prefer names reinforced by canonical docs, commands, ADRs, or major package boundaries.\n"+
			"- Do not promote generic implementation packages or architecture layers like helpers, integrations, notifiers, common code, or generic runtime plumbing into top-level `components` unless the docs clearly treat them as first-class subsystems.\n"+
			"- Keep group IDs and other identifiers compact and regular.\n"+
			"- `INDEX.md` must be a human-readable rendering of the YAML content, not an independently authored artifact.\n",
		b.cfg.RepoOwner,
		b.cfg.RepoName,
		b.cfg.RepoPath,
		b.defaultBaseBranch(),
		runtimeArtifactsCommitRule,
		agent.BranchName,
		b.defaultBaseBranch(),
	)
}

func repoIndexOutputPath(agent Agent) string {
	worktreePath := strings.TrimSpace(agent.WorktreePath)
	if worktreePath == "" {
		return ""
	}
	return filepath.Join(worktreePath, "INDEX.md")
}

func repoIndexSourcePath(agent Agent) string {
	worktreePath := strings.TrimSpace(agent.WorktreePath)
	if worktreePath == "" {
		return ""
	}
	return filepath.Join(worktreePath, "agent_index.yaml")
}

func repoIndexSourceBaseline(agent Agent) (bool, time.Time) {
	path := repoIndexSourcePath(agent)
	if path == "" {
		return false, time.Time{}
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, time.Time{}
	}
	return true, info.ModTime().UTC()
}

func repoIndexOutputBaseline(agent Agent) (bool, time.Time) {
	path := repoIndexOutputPath(agent)
	if path == "" {
		return false, time.Time{}
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, time.Time{}
	}
	return true, info.ModTime().UTC()
}

func repoIndexOutputReady(agent Agent) (bool, error) {
	path := repoIndexOutputPath(agent)
	if path == "" {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	modTime := info.ModTime().UTC()
	if !agent.RepoIndexHadOutput {
		return true, nil
	}
	return modTime.After(agent.RepoIndexBaselineModTime), nil
}

func repoIndexSourceReady(agent Agent) (bool, error) {
	path := repoIndexSourcePath(agent)
	if path == "" {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	modTime := info.ModTime().UTC()
	if !agent.RepoIndexHadSource {
		return true, nil
	}
	return modTime.After(agent.RepoIndexSourceBaselineTime), nil
}

func (b *Orchestrator) maybePauseRepoIndexAgentForHumanReview(ctx context.Context, agent Agent) error {
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.Stopped || current.Role != RoleIndexer {
		return nil
	}
	sourceReady, err := repoIndexSourceReady(current)
	if err != nil {
		return fmt.Errorf("failed to inspect repo index source for agent %s: %w", current.ID, err)
	}
	outputReady, err := repoIndexOutputReady(current)
	if err != nil {
		return fmt.Errorf("failed to inspect repo index output for agent %s: %w", current.ID, err)
	}
	if !sourceReady || !outputReady {
		return nil
	}
	if current.PRNumber == 0 || strings.TrimSpace(current.PRURL) == "" {
		return nil
	}
	if err := b.ensurePRReadyForReview(ctx, current.PRNumber); err != nil {
		return fmt.Errorf("failed to ensure repo index PR #%d is ready for human review: %w", current.PRNumber, err)
	}
	if current.RuntimeHandle.Session == "" {
		if current.State != StateWaiting {
			b.agents.SetState(current.ID, StateWaiting, false)
		}
		return nil
	}
	if err := b.stopRuntime(current); err != nil {
		return fmt.Errorf("failed to stop repo indexing runtime for human review on agent %s: %w", current.ID, err)
	}
	b.agents.SetState(current.ID, StateWaiting, false)
	sourcePath := repoIndexSourcePath(current)
	outputPath := repoIndexOutputPath(current)
	author := b.getAuthenticatedUserLogin(ctx)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: repo indexing agent `%s` is ready for human review: %s (source `%s`, output `%s`) %s", current.ID, current.PRURL, sourcePath, outputPath, author))
	log.Printf("repo indexing agent ready for human review id=%s pr=%d source=%s output=%s", current.ID, current.PRNumber, sourcePath, outputPath)
	return nil
}

func (b *Orchestrator) FindNextOpenIssue(ctx context.Context) (int, string, error) {
	opt := &github.IssueListByRepoOptions{
		State:       "open",
		Sort:        "created",
		Direction:   "asc",
		ListOptions: github.ListOptions{PerPage: 100, Page: 1},
	}
	lowestNum := int(^uint(0) >> 1)
	lowestTitle := ""
	found := false

	for {
		issues, resp, err := b.github.Issues.ListByRepo(ctx, b.cfg.RepoOwner, b.cfg.RepoName, opt)
		if err != nil {
			return 0, "", err
		}
		for _, issue := range issues {
			if issue.GetPullRequestLinks() != nil {
				continue
			}
			num := issue.GetNumber()
			if b.agents.HasActiveIssue(num) {
				continue
			}
			if num < lowestNum {
				lowestNum = num
				lowestTitle = issue.GetTitle()
				found = true
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opt.ListOptions.Page = resp.NextPage
	}

	if !found {
		return 0, "", errors.New("no open issues found")
	}
	return lowestNum, lowestTitle, nil
}

func (b *Orchestrator) SteerAgent(ctx context.Context, agentID, text string) error {
	defer b.persistAgentStateNonFatal("steer-agent")

	agent, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	if agent.Stopped {
		return fmt.Errorf("agent is stopped: %s", agentID)
	}
	switch agent.State {
	case StateDone, StateMerging, StateStopped:
		return fmt.Errorf("agent %s is not steerable in state %s", agentID, agent.State)
	}
	if isReviewCoordinator(agent) {
		return fmt.Errorf(
			"review coordinator %s cannot be steered directly; steer its coding agent instead",
			agent.ID,
		)
	}

	steeringText := strings.TrimSpace(text)
	if steeringText == "" {
		return errors.New("steering message is empty")
	}

	msg := fmt.Sprintf("# Steering Message\n\n%s\n", steeringText)
	if isGitWorktreePath(agent.WorktreePath) {
		if err := b.messenger.SendMessage(agent, msg); err != nil {
			return err
		}
	}
	agent, err := b.sendRuntimeMessage(
		ctx,
		agent,
		"steering requested",
		steeringText,
	)
	if err != nil {
		return fmt.Errorf("failed to send steering to runtime: %w", err)
	}
	if agent.Role == RoleCoder && agent.State == StateWaiting && !b.coderHasActiveReviewInProgress(agent) {
		if b.agents.SetState(agent.ID, StateWorking, false) {
			agent, _ = b.agents.Get(agent.ID)
		}
	}
	b.recordHumanReviewGuidance(agent, steeringText)
	b.clearInputWaitAlert(agentID)
	b.agents.Touch(agentID)
	log.Printf("steering sent to agent=%s", agentID)
	return nil
}

func (b *Orchestrator) recordHumanReviewGuidance(agent Agent, note string) {
	if b == nil || b.agents == nil {
		return
	}
	targetAgentID := ""
	switch agent.Role {
	case RoleReviewer:
		targetAgentID = strings.TrimSpace(agent.ParentAgentID)
	case RoleCoder:
		targetAgentID = strings.TrimSpace(agent.ID)
	}
	if targetAgentID == "" {
		return
	}
	target, ok := b.agents.Get(targetAgentID)
	if !ok || target.Stopped {
		return
	}
	if b.agents.AppendHumanReviewGuidance(targetAgentID, note) {
		log.Printf("recorded human review guidance agent=%s pr=%d", targetAgentID, target.PRNumber)
	}
}

func (b *Orchestrator) setCoderLifecycleState(coderID string, state AgentState) bool {
	if b == nil || b.agents == nil {
		return false
	}
	current, ok := b.agents.Get(coderID)
	if !ok || current.Stopped || current.Role != RoleCoder {
		return false
	}
	return b.agents.SetState(coderID, state, false)
}

func coderHasCurrentHeadVerdict(coder Agent, verdict ReviewVerdict) bool {
	if coder.Role != RoleCoder || coder.Stopped || coder.PRNumber == 0 {
		return false
	}
	if strings.TrimSpace(coder.ActiveReviewAgentID) != "" {
		return false
	}
	if coder.LastReviewVerdict != verdict {
		return false
	}
	reviewedHeadSHA := strings.TrimSpace(coder.LastReviewedHeadSHA)
	currentHeadSHA := strings.TrimSpace(coder.ObservedPRHeadSHA)
	if reviewedHeadSHA == "" || currentHeadSHA == "" {
		return false
	}
	return strings.EqualFold(reviewedHeadSHA, currentHeadSHA)
}

func coderHasCurrentHeadThumbsUp(coder Agent) bool {
	return coderHasCurrentHeadVerdict(coder, ReviewVerdictThumbsUp)
}

func (b *Orchestrator) coderCanStayApproved(coder Agent) bool {
	current := coder
	if b != nil && b.agents != nil {
		if snapshot, ok := b.agents.Get(coder.ID); ok {
			current = snapshot
		}
	}
	if !coderHasCurrentHeadThumbsUp(current) {
		return false
	}
	if b == nil || b.agents == nil {
		return false
	}
	if b.coderHasAnyNonTerminalLinkedReviewer(current) {
		return false
	}
	return b.agents.PendingReviewCommentCount(current.ID) == 0
}

// coderHasAnyNonTerminalLinkedReviewer scans for any reviewer whose
// ParentAgentID names this coder and that isn't stopped or in a terminal
// State, independent of whether the coder's own ActiveReviewAgentID
// currently names it (issue #148, symptom 2). ActiveReviewAgentID and a
// newly-registered reviewer's ParentAgentID linkage are not updated
// atomically with each other or with ObservedPRHeadSHA, so there is a
// real window -- observed live -- where a fresh reviewer for a newly
// pushed, unreviewed commit already exists and is actively running, but
// ActiveReviewAgentID still reads empty and LastReviewVerdict/
// LastReviewedHeadSHA still reflect the *previous*, now-superseded head's
// approval. coderCanStayApproved must not declare the coder "still
// approved" in that window purely because ActiveReviewAgentID happens to
// be unset; a reviewer already exists and is entitled to reach its own
// verdict for the new head first.
func (b *Orchestrator) coderHasAnyNonTerminalLinkedReviewer(coder Agent) bool {
	if b == nil || b.agents == nil || coder.Role != RoleCoder {
		return false
	}
	coderID := strings.TrimSpace(coder.ID)
	if coderID == "" {
		return false
	}
	for _, reviewer := range b.agents.List() {
		if reviewer.Role != RoleReviewer || reviewer.Stopped {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(reviewer.ParentAgentID), coderID) {
			continue
		}
		switch reviewer.State {
		case StateDone, StateErrored, StateStopped:
			continue
		}
		return true
	}
	return false
}

func isReviewCoordinator(agent Agent) bool {
	return agent.Role == RoleReviewer
}

func (b *Orchestrator) holdReviewOnInactiveReviewer(reviewer Agent) {
	if b == nil || b.agents == nil || reviewer.Role != RoleReviewer {
		return
	}
	parentID := strings.TrimSpace(reviewer.ParentAgentID)
	if parentID == "" {
		return
	}
	parent, ok := b.agents.Get(parentID)
	if !ok || parent.Stopped {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(parent.ActiveReviewAgentID), reviewer.ID) {
		return
	}
	headSHA := strings.TrimSpace(reviewer.ObservedPRHeadSHA)
	_ = b.agents.ClearActiveReviewAgentIfMatches(parentID, reviewer.ID)
	recordedHold := false
	if headSHA != "" {
		sameHeadCompletedReview := strings.EqualFold(strings.TrimSpace(parent.LastReviewedHeadSHA), headSHA) &&
			(parent.LastReviewVerdict != "" || parent.LastReviewCommentID != 0)
		if !sameHeadCompletedReview {
			recordedHold = b.agents.RecordManualReviewHold(parentID, headSHA)
		}
	}
	parent, ok = b.agents.Get(parentID)
	if !ok || parent.Stopped {
		return
	}
	switch parent.State {
	case StateDone, StateErrored, StateStopped, StateApproved, StateMerging:
		return
	default:
		_ = b.agents.SetState(parentID, StateWorking, false)
	}
	if recordedHold {
		log.Printf("manual review hold recorded coder=%s pr=%d head=%s reviewer=%s", parentID, parent.PRNumber, abbreviateSHA(headSHA), reviewer.ID)
	}
}

func (b *Orchestrator) validateReviewerResume(ctx context.Context, reviewer Agent) (string, error) {
	if b == nil || b.agents == nil || reviewer.Role != RoleReviewer || !reviewer.Paused {
		return "", nil
	}
	reviewerHead := strings.TrimSpace(reviewer.ObservedPRHeadSHA)
	if reviewerHead == "" {
		return "", fmt.Errorf("cannot resume reviewer %s without a pinned review head", reviewer.ID)
	}
	liveHead := reviewerHead
	if reviewer.PRNumber > 0 && b.github != nil {
		pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, reviewer.PRNumber)
		if err != nil {
			return "", fmt.Errorf("failed to determine current PR head for reviewer %s: %w", reviewer.ID, err)
		}
		if pr.GetMerged() {
			return "", fmt.Errorf("cannot resume reviewer %s because PR #%d is merged", reviewer.ID, reviewer.PRNumber)
		}
		if strings.EqualFold(strings.TrimSpace(pr.GetState()), "closed") {
			return "", fmt.Errorf("cannot resume reviewer %s because PR #%d is closed", reviewer.ID, reviewer.PRNumber)
		}
		if head := pr.GetHead(); head != nil {
			liveHead = strings.TrimSpace(head.GetSHA())
		} else {
			liveHead = ""
		}
		if liveHead == "" {
			return "", fmt.Errorf("cannot resume reviewer %s because PR #%d head sha is empty", reviewer.ID, reviewer.PRNumber)
		}
		if !strings.EqualFold(reviewerHead, liveHead) {
			return "", fmt.Errorf(
				"cannot resume reviewer %s for head %s because PR #%d is now on head %s",
				reviewer.ID,
				abbreviateSHA(reviewerHead),
				reviewer.PRNumber,
				abbreviateSHA(liveHead),
			)
		}
	}
	if reviewer.PRNumber > 0 && b.github == nil {
		return "", fmt.Errorf("cannot resume reviewer %s without a github client", reviewer.ID)
	}
	return liveHead, nil
}

func (b *Orchestrator) restorePausedReviewerOwnership(ctx context.Context, reviewer Agent) error {
	if b == nil || b.agents == nil || reviewer.Role != RoleReviewer || !reviewer.Paused {
		return nil
	}
	liveHead, err := b.validateReviewerResume(ctx, reviewer)
	if err != nil {
		return err
	}
	reviewerHead := strings.TrimSpace(reviewer.ObservedPRHeadSHA)
	parentID := strings.TrimSpace(reviewer.ParentAgentID)
	if parentID == "" {
		return nil
	}
	parent, ok := b.agents.Get(parentID)
	if !ok || parent.Stopped {
		return fmt.Errorf("parent coder not found for reviewer %s", reviewer.ID)
	}
	if parent.State == StateDone || parent.State == StateErrored || parent.State == StateStopped || parent.State == StateApproved || parent.State == StateMerging {
		return fmt.Errorf("cannot resume reviewer %s while parent coder %s is in state %s", reviewer.ID, parentID, parent.State)
	}

	if liveHead != "" {
		_ = b.agents.SetPRHeadSHA(parentID, liveHead)
		parent, ok = b.agents.Get(parentID)
		if !ok || parent.Stopped {
			return fmt.Errorf("parent coder not found for reviewer %s", reviewer.ID)
		}
	}
	parentHead := strings.TrimSpace(parent.ObservedPRHeadSHA)
	if parentHead != "" && reviewerHead != "" && !strings.EqualFold(parentHead, reviewerHead) {
		return fmt.Errorf(
			"cannot resume reviewer %s for head %s while coder %s is on head %s",
			reviewer.ID,
			abbreviateSHA(reviewerHead),
			parentID,
			abbreviateSHA(parentHead),
		)
	}
	if reviewerHead != "" &&
		strings.EqualFold(strings.TrimSpace(parent.LastReviewedHeadSHA), reviewerHead) &&
		(parent.LastReviewVerdict != "" || parent.LastReviewCommentID != 0) {
		return fmt.Errorf(
			"cannot resume reviewer %s for head %s because coder %s already has a completed review recorded",
			reviewer.ID,
			abbreviateSHA(reviewerHead),
			parentID,
		)
	}

	activeReviewerID := strings.TrimSpace(parent.ActiveReviewAgentID)
	if activeReviewerID != "" && !strings.EqualFold(activeReviewerID, reviewer.ID) {
		activeReviewer, ok := b.agents.Get(activeReviewerID)
		if ok && reviewerCanBlockLaunch(activeReviewer) {
			return fmt.Errorf("coder %s already has active review agent %s", parentID, activeReviewerID)
		}
	}

	if !b.agents.SetActiveReviewAgent(parentID, reviewer.ID) {
		return fmt.Errorf("failed to restore active review agent %s for coder %s", reviewer.ID, parentID)
	}
	_ = b.setCoderLifecycleState(parentID, StateWaiting)
	if reviewerHead != "" {
		_ = b.agents.ClearManualReviewHold(parentID, reviewerHead)
	}
	return nil
}

func (b *Orchestrator) ensureSteerableRuntimeLocked(
	ctx context.Context,
	agent Agent,
	reason string,
) (Agent, error) {
	if b.agents == nil {
		return agent, errors.New("agent manager is not configured")
	}
	current, ok := b.agents.Get(agent.ID)
	if !ok {
		return agent, fmt.Errorf("agent not found: %s", agent.ID)
	}
	agent = current
	if isReviewCoordinator(agent) {
		return agent, fmt.Errorf(
			"review coordinator %s has no steerable top-level runtime",
			agent.ID,
		)
	}
	if b.runner == nil {
		return agent, errors.New("runtime runner is not configured")
	}
	if strings.TrimSpace(agent.RuntimeHandle.Session) != "" {
		alive, err := b.runner.IsAlive(agent.RuntimeHandle)
		if err != nil {
			return agent, fmt.Errorf("failed to check runtime status for agent %s: %w", agent.ID, err)
		}
		if alive {
			return agent, nil
		}
	}
	restarted, err := b.restartRuntimeWithPromptLocked(
		ctx,
		agent,
		formatResumePrompt(agent, reason),
	)
	if err != nil {
		return agent, err
	}
	if agent.State == StateApproved {
		resumeReason := strings.TrimSpace(reason)
		if resumeReason == "" {
			resumeReason = "follow-up activity"
		}
		b.notify(
			ctx,
			fmt.Sprintf("Repository Agent Orchestrator: %s `%s` resumed from approved state (%s) on PR %s", agentRoleLabel(agent.Role), agent.ID, resumeReason, fallback(strings.TrimSpace(agent.PRURL), "(unknown)")),
		)
	}
	if agent.Paused {
		resumeReason := strings.TrimSpace(reason)
		if resumeReason == "" {
			resumeReason = "manual resume"
		}
		b.notify(
			ctx,
			fmt.Sprintf("Repository Agent Orchestrator: %s `%s` resumed from paused state (%s) on PR %s", agentRoleLabel(agent.Role), agent.ID, resumeReason, fallback(strings.TrimSpace(agent.PRURL), "(unknown)")),
		)
	}
	return restarted, nil
}

func (b *Orchestrator) sendRuntimeMessage(ctx context.Context, agent Agent, reason, text string) (Agent, error) {
	if b == nil || b.agents == nil {
		return agent, errors.New("agent manager is not configured")
	}
	unlockSend := b.agents.lockRuntimeSend(agent.ID)
	defer unlockSend()
	prepared, err := b.ensureSteerableRuntimeLocked(ctx, agent, reason)
	if err != nil {
		return agent, err
	}
	return b.sendScopedRuntimeMessageLocked(prepared, text)
}

func (b *Orchestrator) sendScopedRuntimeMessage(agent Agent, text string) (Agent, error) {
	if b == nil || b.runner == nil {
		return agent, errors.New("runtime runner is not configured")
	}
	if _, ok := b.runner.(ScopedRuntimeSender); !ok {
		return agent, b.runner.Send(agent.RuntimeHandle, text)
	}
	if b.agents == nil {
		return agent, errors.New("runtime message rejected: agent manager is not configured")
	}
	unlockSend := b.agents.lockRuntimeSend(agent.ID)
	defer unlockSend()
	return b.sendScopedRuntimeMessageLocked(agent, text)
}

func (b *Orchestrator) sendScopedRuntimeMessageLocked(
	agent Agent,
	text string,
) (Agent, error) {
	if b == nil || b.runner == nil {
		return agent, errors.New("runtime runner is not configured")
	}
	scoped, ok := b.runner.(ScopedRuntimeSender)
	if !ok {
		return agent, b.runner.Send(agent.RuntimeHandle, text)
	}
	authoritative, ok := b.agents.Get(agent.ID)
	if !ok {
		return agent, fmt.Errorf("runtime message rejected: agent not found: %s", agent.ID)
	}
	bound, err := scoped.BindRuntimeHandle(authoritative, authoritative.RuntimeHandle)
	if err != nil {
		return authoritative, fmt.Errorf("runtime message rejected: %w", err)
	}
	unlockScope := b.agents.lockRuntimeScopeRead(authoritative.ID)
	defer unlockScope()

	current, ok := b.agents.Get(authoritative.ID)
	if !ok {
		return authoritative, fmt.Errorf(
			"runtime message rejected: agent not found after binding: %s",
			authoritative.ID,
		)
	}
	if !sameRuntimeDeliveryTarget(authoritative, current) {
		return current, fmt.Errorf(
			"runtime message rejected: agent scope changed while binding: %s",
			authoritative.ID,
		)
	}
	if !b.agents.setRuntimeHandleWithoutScopeGuard(authoritative.ID, bound) {
		return authoritative, fmt.Errorf(
			"runtime message rejected: failed to persist scope for agent %s",
			authoritative.ID,
		)
	}
	current.RuntimeHandle = bound
	if err := scoped.SendScoped(current, bound, text); err != nil {
		return current, err
	}
	return current, nil
}

func sameRuntimeDeliveryTarget(left, right Agent) bool {
	return left.ID == right.ID &&
		left.PRNumber == right.PRNumber &&
		strings.EqualFold(
			strings.TrimSpace(left.ObservedPRHeadSHA),
			strings.TrimSpace(right.ObservedPRHeadSHA),
		) &&
		cleanAbsolutePath(left.WorktreePath) ==
			cleanAbsolutePath(right.WorktreePath) &&
		left.RuntimeHandle == right.RuntimeHandle &&
		left.State == right.State &&
		left.Stopped == right.Stopped &&
		left.Paused == right.Paused
}

func (b *Orchestrator) sendAutomatedRuntimeMessage(ctx context.Context, agent Agent, reason, text, pausedInboxText string) (Agent, bool, error) {
	current := agent
	if b != nil && b.agents != nil {
		if snapshot, ok := b.agents.Get(agent.ID); ok {
			current = snapshot
		}
	}
	if current.Stopped {
		return current, false, nil
	}
	if current.Paused {
		if pausedInboxText != "" && b.messenger != nil && isGitWorktreePath(current.WorktreePath) {
			if err := b.messenger.SendMessage(current, pausedInboxText); err != nil {
				return current, false, fmt.Errorf("failed to write paused-agent inbox message: %w", err)
			}
		}
		return current, false, nil
	}
	if b.runner == nil {
		return current, false, errors.New("runtime runner is not configured")
	}
	prepared, err := b.sendRuntimeMessage(ctx, current, reason, text)
	if err != nil {
		return prepared, runtimePayloadDelivered(err), err
	}
	return prepared, true, nil
}

func (b *Orchestrator) restartRuntime(ctx context.Context, agent Agent, reason string) (Agent, error) {
	return b.restartRuntimeWithPrompt(ctx, agent, formatResumePrompt(agent, reason))
}

func (b *Orchestrator) restartRuntimeWithPrompt(ctx context.Context, agent Agent, prompt string) (Agent, error) {
	if b == nil || b.agents == nil {
		return agent, errors.New("agent manager is not configured")
	}
	unlockSend := b.agents.lockRuntimeSend(agent.ID)
	defer unlockSend()
	return b.restartRuntimeWithPromptLocked(ctx, agent, prompt)
}

func (b *Orchestrator) restartRuntimeWithPromptLocked(
	ctx context.Context,
	agent Agent,
	prompt string,
) (Agent, error) {
	if b.runner == nil {
		return agent, errors.New("runtime runner is not configured")
	}
	resumePausedReviewer := agent.Role == RoleReviewer && agent.Paused
	if resumePausedReviewer {
		if err := b.restorePausedReviewerOwnership(ctx, agent); err != nil {
			return agent, err
		}
	}
	handle, err := b.runner.Start(agent, prompt)
	if err != nil {
		if resumePausedReviewer {
			b.holdReviewOnInactiveReviewer(agent)
		}
		return agent, fmt.Errorf("failed to restart runtime for agent %s: %w", agent.ID, err)
	}
	if !b.agents.SetRuntimeHandle(agent.ID, handle) {
		_ = b.runner.Stop(handle)
		if resumePausedReviewer {
			b.holdReviewOnInactiveReviewer(agent)
		}
		return agent, fmt.Errorf("failed to persist restarted runtime handle for agent %s", agent.ID)
	}
	if agent.State == StateApproved {
		b.agents.SetState(agent.ID, StateWorking, false)
	}
	if agent.Paused {
		_, _ = b.agents.Unpause(agent.ID)
	}
	b.clearInputWaitTracking(agent.ID)

	updated, ok := b.agents.Get(agent.ID)
	if !ok {
		return agent, fmt.Errorf("failed to load agent after runtime restart: %s", agent.ID)
	}

	if strings.TrimSpace(handle.LogPath) != "" {
		log.Printf("runtime restarted for agent=%s issue=%d runtime=%s session=%s log=%s", agent.ID, agent.IssueNumber, handle.Kind, handle.Session, handle.LogPath)
	} else {
		log.Printf("runtime restarted for agent=%s issue=%d runtime=%s session=%s", agent.ID, agent.IssueNumber, handle.Kind, handle.Session)
	}
	return updated, nil
}

func (b *Orchestrator) launchRuntime(
	agent Agent,
	prompt string,
	checkpointOwnership bool,
) (RuntimeHandle, error) {
	if b == nil || b.agents == nil {
		return RuntimeHandle{}, errors.New("agent manager is not configured")
	}
	if b.runner == nil {
		return RuntimeHandle{}, errors.New("runtime runner is not configured")
	}
	unlockSend := b.agents.lockRuntimeSend(agent.ID)
	defer unlockSend()
	if checkpointOwnership {
		if strings.TrimSpace(b.agentStateFilePath()) == "" {
			return RuntimeHandle{}, errors.New(
				"runtime launch requires a durable agent state path",
			)
		}
		if err := b.persistAgentState(); err != nil {
			return RuntimeHandle{}, fmt.Errorf(
				"failed to checkpoint runtime ownership for agent %s: %w",
				agent.ID,
				err,
			)
		}
	}
	handle, err := b.runner.Start(agent, prompt)
	if err != nil {
		return RuntimeHandle{}, err
	}
	if !b.agents.SetRuntimeHandle(agent.ID, handle) {
		_ = b.runner.Stop(handle)
		return RuntimeHandle{}, fmt.Errorf(
			"failed to persist runtime handle for agent %s",
			agent.ID,
		)
	}
	return handle, nil
}

func (b *Orchestrator) StopAgent(ctx context.Context, agentID string) error {
	defer b.persistAgentStateNonFatal("stop-agent")

	agent, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	if agent.Role == RoleReviewer {
		b.cancelReviewGate(agent.ID)
		b.clearReviewGateTracking(agent.ID)
	}
	if agent.Role == RoleReviewer {
		result, err := b.transitionReviewCoordinatorLifecycle(
			ctx,
			agent.ID,
			reviewCoordinatorLifecycleRequest{
				Intent:                     ReviewCoordinatorLifecycleStop,
				FinalState:                 StateStopped,
				ReleaseCoordinatorWorktree: true,
			},
		)
		if err != nil {
			return err
		}
		agent = result.Agent
		if !result.Changed {
			return nil
		}
	} else {
		if err := b.stopRuntime(agent); err != nil {
			return err
		}
		agent, ok = b.agents.Stop(agentID)
		if !ok {
			return fmt.Errorf("agent not found: %s", agentID)
		}
	}
	if agent.Role == RoleReviewer {
		b.holdReviewOnInactiveReviewer(agent)
	}
	b.clearInputWaitTracking(agent.ID)
	if agent.Role == RoleReviewer && strings.TrimSpace(agent.ParentAgentID) != "" {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: agent `%s` stopped; automatic review is paused for the current PR head until a new push or manual `agent review`", agent.ID))
	} else {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: agent `%s` stopped", agent.ID))
	}
	log.Printf("agent stopped id=%s", agent.ID)
	return nil
}

func (b *Orchestrator) PauseAgent(ctx context.Context, agentID string) error {
	defer b.persistAgentStateNonFatal("pause-agent")

	if b == nil || b.agents == nil {
		return errors.New("agent manager is not configured")
	}
	current, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	if current.Role == RoleIndexer {
		return fmt.Errorf("repo indexing agent %s cannot be paused", agentID)
	}
	if current.Role == RoleReviewer && (current.State == StateInitializing || current.State == StateReviewGate) {
		return fmt.Errorf("review agent %s cannot be paused before runtime launch", agentID)
	}
	if current.Paused {
		if current.Role != RoleReviewer {
			return fmt.Errorf("agent %s is already paused", agentID)
		}
		if current.ReviewCoordinatorLifecycle == nil ||
			!current.ReviewCoordinatorLifecycle.CompletedAt.IsZero() {
			return nil
		}
	}
	switch current.State {
	case StateDone, StateErrored, StateStopped, StateMerging:
		return fmt.Errorf("agent %s is not pausable in state %s", agentID, current.State)
	}

	var agent Agent
	if current.Role == RoleReviewer {
		result, err := b.transitionReviewCoordinatorLifecycle(
			ctx,
			current.ID,
			reviewCoordinatorLifecycleRequest{
				Intent:                     ReviewCoordinatorLifecyclePause,
				FinalState:                 current.State,
				ReleaseCoordinatorWorktree: false,
			},
		)
		if err != nil {
			return fmt.Errorf(
				"failed to pause review agent %s safely: %w",
				current.ID,
				err,
			)
		}
		agent = result.Agent
		ok = true
		if !result.Changed {
			return nil
		}
	} else {
		if err := b.stopRuntime(current); err != nil {
			return err
		}
		agent, ok = b.agents.Pause(agentID)
	}
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	if agent.Role == RoleReviewer {
		b.holdReviewOnInactiveReviewer(agent)
	}
	b.clearInputWaitTracking(agent.ID)
	if agent.Role == RoleReviewer && strings.TrimSpace(agent.ParentAgentID) != "" {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: agent `%s` paused; automatic review is paused for the current PR head until a new push, manual `agent review`, `agent unpause`, or a steering resume", agent.ID))
	} else {
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: agent `%s` paused", agent.ID))
	}
	log.Printf("agent paused id=%s", agent.ID)
	return nil
}

func (b *Orchestrator) UnpauseAgent(ctx context.Context, agentID string) error {
	defer b.persistAgentStateNonFatal("unpause-agent")

	if b == nil || b.agents == nil {
		return errors.New("agent manager is not configured")
	}
	agent, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	if agent.Stopped {
		return fmt.Errorf("agent is stopped: %s", agentID)
	}
	if !agent.Paused {
		return fmt.Errorf("agent %s is not paused", agentID)
	}
	if agent.Role == RoleReviewer &&
		agent.ReviewCoordinatorLifecycle != nil &&
		(agent.ReviewCoordinatorLifecycle.Intent !=
			ReviewCoordinatorLifecyclePause ||
			agent.ReviewCoordinatorLifecycle.CompletedAt.IsZero()) {
		return fmt.Errorf(
			"review agent %s pause cleanup is incomplete",
			agentID,
		)
	}

	if agent.Role == RoleCoder && agent.State == StateApproved && b.coderCanStayApproved(agent) {
		resumed, ok := b.agents.Unpause(agentID)
		if !ok {
			return fmt.Errorf("failed to reload agent after unpause: %s", agentID)
		}
		b.clearInputWaitTracking(agent.ID)
		b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: agent `%s` unpaused", resumed.ID))
		log.Printf("agent unpaused id=%s restored_state=%s", resumed.ID, resumed.State)
		return nil
	}
	if agent.Role == RoleCoder && agent.State == StateApproved {
		if !b.agents.SetState(agentID, StateWorking, false) {
			return fmt.Errorf("failed to restore working state for agent %s", agentID)
		}
		agent, ok = b.agents.Get(agentID)
		if !ok {
			return fmt.Errorf("failed to reload agent after state restore: %s", agentID)
		}
	}

	if isReviewCoordinator(agent) {
		if err := b.restorePausedReviewerOwnership(
			ctx,
			agent,
		); err != nil {
			return err
		}
		resumed, ok := b.agents.Unpause(agentID)
		if !ok {
			b.holdReviewOnInactiveReviewer(agent)
			return fmt.Errorf(
				"failed to reload review coordinator after unpause: %s",
				agentID,
			)
		}
		b.clearInputWaitTracking(agent.ID)
		recoveryCtx := context.Background()
		if ctx != nil {
			recoveryCtx = context.WithoutCancel(ctx)
		}
		b.queuePersistedReviewCycleRecovery(
			recoveryCtx,
			resumed.ID,
		)
		b.notify(
			ctx,
			fmt.Sprintf(
				"Repository Agent Orchestrator: review coordinator `%s` unpaused",
				resumed.ID,
			),
		)
		log.Printf(
			"review coordinator unpaused id=%s",
			resumed.ID,
		)
		return nil
	}

	resumed, err := b.restartRuntime(ctx, agent, "manual unpause")
	if err != nil {
		return err
	}
	b.clearInputWaitTracking(agent.ID)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: agent `%s` unpaused", resumed.ID))
	log.Printf("agent unpaused id=%s", resumed.ID)
	return nil
}

func (b *Orchestrator) CleanupAgent(ctx context.Context, agentID string) error {
	defer b.persistAgentStateNonFatal("cleanup-agent")

	if b == nil || b.agents == nil {
		return errors.New("agent manager is not configured")
	}
	agent, ok := b.agents.Get(agentID)
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	if agent.Role == RoleReviewer {
		b.cancelReviewGate(agent.ID)
		result, err := b.transitionReviewCoordinatorLifecycle(
			ctx,
			agent.ID,
			reviewCoordinatorLifecycleRequest{
				Intent:                     ReviewCoordinatorLifecycleCleanup,
				FinalState:                 StateDone,
				ReleaseCoordinatorWorktree: true,
				ReleaseRuntimeArtifacts:    true,
			},
		)
		if err != nil {
			return err
		}
		agent = result.Agent
		// Mirrors retireReviewer's identical call: a reviewer cleaned up
		// here without ever producing a publishable verdict (e.g. its
		// linked coder's own review_coordinator DurableLaunchAttempt was
		// still Running/Reserved) would otherwise leave that attempt
		// permanently non-terminal. reserveReviewLaunchAttempt reuses a
		// non-terminal attempt's owner_id on every future launch for
		// that exact head, and AgentManager.Add rejects it forever since
		// this reviewer's own agent record is never purged -- see issue
		// #982/PR #1054, where this exact gap needed a manual state-file
		// edit to unblock after CleanupAgent (not retireReviewer) retired
		// a reviewer whose challenge lanes exhausted retries.
		//
		// cancelLinkedReviewLaunchOnRetirement is idempotent (it no-ops
		// once the linked attempt is already terminal), so it must run
		// unconditionally here, before the no-change early return below
		// -- not only when result.Changed. Otherwise: if a prior
		// CleanupAgent call already completed the reviewer's own
		// lifecycle transition but this cancellation failed to persist
		// (e.g. a transient write failure rolls
		// persistCoderLaunchAttemptMutation back to "running"), every
		// subsequent retry hits !result.Changed and returns before ever
		// attempting cancellation again -- permanently stranding the
		// coder's launch attempt exactly as before, just reached via a
		// narrower, transient-failure-triggered path. See Craig's review
		// on PR #183.
		if cancelErr := b.cancelLinkedReviewLaunchOnRetirement(agent); cancelErr != nil {
			return fmt.Errorf("failed to release linked launch attempt: %w", cancelErr)
		}
		if !result.Changed {
			return nil
		}

		b.clearReviewGateTracking(agent.ID)
		if parentID := strings.TrimSpace(agent.ParentAgentID); parentID != "" {
			_ = b.agents.ClearActiveReviewAgentIfMatches(
				parentID,
				agent.ID,
			)
		}
		b.clearInputWaitTracking(agent.ID)
		b.notify(
			ctx,
			fmt.Sprintf(
				"Repository Agent Orchestrator: agent `%s` cleaned up",
				agent.ID,
			),
		)
		log.Printf("agent cleaned up id=%s", agent.ID)
		return nil
	}
	// Block queued review launches and other asynchronous lifecycle work before
	// cleanup starts making network calls or removing resources.
	if !b.agents.SetState(agent.ID, StateStopped, true) {
		return fmt.Errorf("failed to mark agent for cleanup: %s", agent.ID)
	}
	agent, ok = b.agents.Get(agent.ID)
	if !ok {
		return fmt.Errorf("failed to reload agent for cleanup: %s", agent.ID)
	}

	errs := make([]string, 0)
	if agent.Role == RoleCoder {
		errs = appendCleanupError(errs, b.cleanupLinkedReviewer(ctx, agent))
	}
	if err := b.stopRuntime(agent); err != nil {
		return err
	}

	if agent.Role != RoleReviewer && !agent.AdoptedPR {
		errs = appendCleanupError(errs, b.closeAgentPR(ctx, agent))
		errs = appendCleanupError(errs, b.deleteAgentRemoteBranch(ctx, agent))
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errs = appendCleanupError(errs, b.cleanupWorktree(cleanupCtx, agent.WorktreePath, cleanupBranchName(agent)))

	if agent.Role != RoleReviewer {
		errs = appendCleanupError(errs, b.syncBaseBranch(ctx))
	}

	errs = appendCleanupError(errs, b.removeAgentRuntimeState(agent))
	errs = appendCleanupError(errs, b.removeAgentRuntimeLog(agent))
	errs = appendCleanupError(errs, b.removeAgentMandatoryTestLog(agent))
	errs = appendCleanupError(errs, b.removePersistedHandoffs(agent.ID))
	b.agents.SetState(agent.ID, StateDone, true)
	b.clearInputWaitTracking(agent.ID)
	b.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator: agent `%s` cleaned up", agent.ID))
	log.Printf("agent cleaned up id=%s", agent.ID)

	if len(errs) > 0 {
		return fmt.Errorf("cleanup completed with errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (b *Orchestrator) Shutdown(ctx context.Context) {
	_ = ctx
	b.waitForCheckpointedReviewLaunchRecoveries()
	b.waitForPersistedReviewCycleRecoveries()
	b.waitForReviewCleanups()
	b.persistAgentStateNonFatal("shutdown")
}

func (b *Orchestrator) PollLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	if err := b.PollOnce(ctx); err != nil {
		log.Printf("poll error: %s", b.safeError(err))
	}

	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.PollOnce(ctx); err != nil {
				log.Printf("poll error: %s", b.safeError(err))
			}
		}
	}
}

func (b *Orchestrator) PollOnce(ctx context.Context) error {
	defer b.persistAgentStateNonFatal("poll-once")

	if err := b.reconcilePersistedReviewCycles(ctx); err != nil {
		return fmt.Errorf(
			"failed to reconcile persisted review cycles: %w",
			err,
		)
	}
	now := time.Now()
	b.agents.SetLastPoll(now)
	active := b.agents.Active()

	var wg sync.WaitGroup
	for _, agent := range active {
		agentID := agent.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.pollActiveAgent(ctx, agentID, now)
		}()
	}
	wg.Wait()

	b.checkReviewCycleHealth(ctx, b.agents.Active(), time.Now())

	return nil
}

func (b *Orchestrator) pollActiveAgent(ctx context.Context, agentID string, now time.Time) {
	agent, ok := b.agents.Get(agentID)
	if !ok || agent.Stopped || agent.State == StateDone || agent.State == StateErrored || agent.State == StateStopped {
		return
	}
	if agent.Role == RoleReviewer &&
		agent.ReviewCycle != nil &&
		(b.reviewCoordinatorPullRequests != nil ||
			b.github != nil) {
		reconciled, err :=
			b.reconcileConvergentReviewPullRequest(
				ctx,
				agent.ID,
			)
		if err != nil {
			log.Printf(
				"review PR reconciliation failed agent=%s pr=%d: %s",
				agent.ID,
				agent.PRNumber,
				b.safeError(err),
			)
			return
		}
		agent = reconciled
		if agent.Paused || agentLifecycleTerminal(&agent) ||
			agent.ReviewCycle == nil ||
			agent.ReviewCycle.Stale {
			return
		}
	}
	if agent.Role == RoleReviewer &&
		reviewCycleNeedsReportPublication(agent.ReviewCycle) {
		// The poll interval is the outer backoff after the publication
		// boundary exhausts its short in-call retries.
		if _, err := b.publishReviewCycleVerdict(
			ctx,
			agent.ID,
		); err != nil {
			log.Printf(
				"pending review verdict publication failed agent=%s pr=%d: %s",
				agent.ID,
				agent.PRNumber,
				b.safeError(err),
			)
		}
		return
	}
	if agent.Role == RoleReviewer &&
		agent.State == StateWorking &&
		agent.ReviewCycle != nil &&
		!reviewCycleHasTerminalVerdict(agent.ReviewCycle) {
		// Coordinators have no top-level Codex runtime to monitor.
		// Retry the durable production entry point on normal poll ticks; its
		// in-process guard and persisted worker ownership prevent duplicate
		// active work.
		//
		// Gated on StateWorking specifically (issue #158): ReviewCycle is
		// populated at registration, well before the convergent review
		// coordinator itself ever starts, so a reviewer still on its first,
		// pre-gate launch attempt (StateInitializing/StateReviewGate) also
		// has a non-nil, non-terminal ReviewCycle. Without this guard, this
		// branch caught those reviewers too and returned before
		// reconcilePendingReviewLaunch below -- the only code path that
		// resumes or terminalizes an interrupted pre-gate launch -- ever
		// ran. queuePersistedReviewCycleRecovery is the wrong recovery
		// mechanism for a launch that never reached convergent review in
		// the first place (validateConvergentReviewBoundary requires
		// StateWorking to consider a reviewer "active" for it at all), so
		// it just failed silently forever ("convergent review coordinator
		// is not active"), permanently wedging the reviewer with no alert
		// and no way back to the recovery path that could actually resume
		// or retire it. See reviewCycleDiagnosisReviewGateWedge and
		// docs/TROUBLESHOOTING.md.
		b.queuePersistedReviewCycleRecovery(ctx, agent.ID)
		return
	}
	if agent.Role == RoleReviewer && strings.TrimSpace(agent.ParentAgentID) == "" {
		if err := b.handleTerminalManualReviewerPR(ctx, agent); err != nil {
			log.Printf("manual review terminal-pr cleanup check failed agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
			return
		}
		refreshed, ok := b.agents.Get(agent.ID)
		if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
			return
		}
		agent = refreshed
	}
	if agent.Paused && agent.Role == RoleReviewer {
		return
	}
	if agent.Role == RoleCoder && agent.PendingReviewVerdict != nil {
		if agent.PRNumber > 0 && b.github != nil {
			if err := b.handleTerminalPR(ctx, agent); err != nil {
				log.Printf("pending-verdict terminal-pr check failed agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
				return
			}
			refreshed, found := b.agents.Get(agent.ID)
			if !found || refreshed.Stopped ||
				refreshed.State == StateDone ||
				refreshed.State == StateErrored ||
				refreshed.State == StateStopped {
				return
			}
			agent = refreshed
		}
		if err := b.resumePendingReviewVerdict(ctx, agent.ID); err != nil {
			log.Printf("pending review verdict recovery failed agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
		}
		return
	}

	nextAgent, shouldContinue, err := b.reconcilePendingReviewLaunch(ctx, agent, now)
	if err != nil {
		log.Printf("pending review launch check error agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
		return
	}
	agent = nextAgent
	if !shouldContinue {
		return
	}

	nextAgent, shouldContinue, err = b.reconcileRuntimeHealth(ctx, agent)
	if err != nil {
		log.Printf("runtime health check error agent=%s: %s", agent.ID, b.safeError(err))
		return
	}
	agent = nextAgent
	if !shouldContinue {
		return
	}

	if err := b.detectInputWait(ctx, agent, now); err != nil {
		log.Printf("input wait detection error agent=%s: %s", agent.ID, b.safeError(err))
	}

	nextAgent, shouldContinue, err = b.detectRuntimeWorkingLoop(ctx, agent, now)
	if err != nil {
		log.Printf("working loop detection error agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
	}
	agent = nextAgent
	if !shouldContinue {
		return
	}

	if agent.Role == RoleReviewer {
		if err := b.pollReviewAgent(ctx, agent); err != nil {
			log.Printf("review verdict polling error agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
		}
		return
	}
	if agent.Role == RoleIndexer {
		if agent.PRNumber == 0 {
			if err := b.detectRepoIndexPR(ctx, agent); err != nil {
				log.Printf("repo index pr detection error agent=%s: %s", agent.ID, b.safeError(err))
			}
			refreshed, ok := b.agents.Get(agent.ID)
			if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
				return
			}
			agent = refreshed
		}
		if !agent.Paused {
			if err := b.maybePauseRepoIndexAgentForHumanReview(ctx, agent); err != nil {
				log.Printf("repo index ready-for-review check failed agent=%s: %s", agent.ID, b.safeError(err))
			}
		}
		refreshed, ok := b.agents.Get(agent.ID)
		if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
			return
		}
		if err := b.handleTerminalPR(ctx, refreshed); err != nil {
			log.Printf("repo index terminal-pr cleanup check failed agent=%s pr=%d: %s", refreshed.ID, refreshed.PRNumber, b.safeError(err))
		}
		return
	}
	if agent.Role != RoleCoder {
		return
	}

	if agent.PRNumber == 0 {
		if err := b.detectPR(ctx, agent); err != nil {
			log.Printf("pr detection error agent=%s: %s", agent.ID, b.safeError(err))
		}
		return
	}
	if err := b.handleTerminalPR(ctx, agent); err != nil {
		log.Printf("terminal-pr cleanup check failed agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
	}
	refreshed, ok := b.agents.Get(agent.ID)
	if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
		return
	}
	agent = refreshed
	if err := b.handlePRMergeConflicts(ctx, agent); err != nil {
		log.Printf("merge-conflict polling error agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
	}
	refreshed, ok = b.agents.Get(agent.ID)
	if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
		return
	}
	agent = refreshed
	if err := b.maybePromoteReadyForHumanReview(ctx, agent); err != nil {
		log.Printf("ready-for-human-review polling error agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
	}
	refreshed, ok = b.agents.Get(agent.ID)
	if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
		return
	}
	agent = refreshed

	if err := b.forwardNewComments(ctx, agent); err != nil {
		log.Printf("comment polling error agent=%s: %s", agent.ID, b.safeError(err))
	}
	refreshed, ok = b.agents.Get(agent.ID)
	if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
		return
	}
	agent = refreshed
	if agent.State == StateApproved {
		if err := b.maybeAutoMergeApprovedPR(ctx, agent); err != nil {
			log.Printf("approval polling error agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
		}
		refreshed, ok = b.agents.Get(agent.ID)
		if !ok || refreshed.Stopped || refreshed.State == StateDone || refreshed.State == StateErrored || refreshed.State == StateStopped {
			return
		}
		agent = refreshed
	}
	if agent.State == StateMerging {
		return
	}
	if err := b.promptCommentRepliesAfterPush(ctx, agent); err != nil {
		log.Printf("post-push reply reminder polling error agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
	}
	if err := b.ensureReviewAgentForCoder(ctx, agent); err != nil {
		log.Printf("review agent launch check failed agent=%s pr=%d: %s", agent.ID, agent.PRNumber, b.safeError(err))
	}
}

func (b *Orchestrator) reconcileRuntimeHealth(ctx context.Context, agent Agent) (Agent, bool, error) {
	if b.runner == nil || strings.TrimSpace(agent.RuntimeHandle.Session) == "" {
		return agent, true, nil
	}
	if _, scoped := b.runner.(ScopedRuntimeSender); scoped &&
		runtimeHandleNeedsIsolationMigration(agent.RuntimeHandle) {
		switch {
		case agent.State == StateWorking && !agent.Paused:
			reconciler, ok := b.runner.(RuntimeLaunchReconciler)
			if !ok {
				return agent, false, fmt.Errorf(
					"failed to migrate runtime isolation for agent %s: "+
						"runtime launch reconciliation is unavailable",
					agent.ID,
				)
			}
			if err := reconciler.ReconcileRuntimeLaunch(agent); err != nil {
				return agent, false, fmt.Errorf(
					"failed to reconcile interrupted runtime isolation "+
						"migration for agent %s: %w",
					agent.ID,
					err,
				)
			}
			restarted, err := b.restartRuntime(
				ctx,
				agent,
				"mandatory runtime isolation migration",
			)
			if err != nil {
				return agent, false, fmt.Errorf(
					"failed to migrate runtime isolation for agent %s: %w",
					agent.ID,
					err,
				)
			}
			log.Printf(
				"legacy runtime migrated agent=%s session=%s tmux_server=%s",
				restarted.ID,
				restarted.RuntimeHandle.Session,
				restarted.RuntimeHandle.TmuxServer,
			)
			return restarted, true, nil
		case agent.State == StateWaiting ||
			agent.State == StateApproved ||
			agent.State == StateMerging ||
			agent.Paused:
			if b.agents.SetRuntimeHandle(agent.ID, RuntimeHandle{}) {
				if refreshed, ok := b.agents.Get(agent.ID); ok {
					agent = refreshed
				}
			}
			log.Printf(
				"legacy runtime handle cleared agent=%s state=%s",
				agent.ID,
				agent.State,
			)
			return agent, true, nil
		}
	}
	alive, err := b.runner.IsAlive(agent.RuntimeHandle)
	if err != nil {
		return agent, true, err
	}
	if alive {
		return agent, true, nil
	}

	b.clearRuntimeLoopTracking(agent.ID)
	b.clearInputWaitTracking(agent.ID)

	switch agent.State {
	case StateApproved, StateMerging:
		if b.agents.SetRuntimeHandle(agent.ID, RuntimeHandle{}) {
			if refreshed, ok := b.agents.Get(agent.ID); ok {
				agent = refreshed
			}
		}
		log.Printf("runtime no longer alive for agent=%s state=%s; cleared runtime handle", agent.ID, agent.State)
		return agent, true, nil
	default:
		var terminalizationErr error
		if agent.Role == RoleReviewer {
			agent, terminalizationErr =
				b.terminalizeReviewCoordinatorAfterWorkerCleanup(
					agent.ID,
					StateErrored,
				)
		} else {
			b.agents.SetState(agent.ID, StateErrored, false)
			if refreshed, ok := b.agents.Get(agent.ID); ok {
				agent = refreshed
			}
		}
		b.notify(
			ctx,
			fmt.Sprintf(
				"Repository Agent Orchestrator: runtime exited unexpectedly for %s `%s` (issue #%d, branch `%s`); terminal state set to `%s`",
				agentRoleLabel(agent.Role),
				agent.ID,
				agent.IssueNumber,
				agent.BranchName,
				agent.State,
			),
		)
		log.Printf(
			"runtime exited unexpectedly agent=%s issue=%d branch=%s state=%s",
			agent.ID,
			agent.IssueNumber,
			agent.BranchName,
			agent.State,
		)
		return agent, false, terminalizationErr
	}
}

func runtimeHandleNeedsIsolationMigration(handle RuntimeHandle) bool {
	if strings.TrimSpace(handle.Session) == "" {
		return false
	}
	return strings.TrimSpace(handle.TmuxServer) == "" ||
		strings.TrimSpace(handle.CodexHome) == "" ||
		handle.Scope.Version != runtimeScopeVersion
}

func (b *Orchestrator) reconcilePendingReviewLaunch(ctx context.Context, agent Agent, now time.Time) (Agent, bool, error) {
	if agent.Role != RoleReviewer || strings.TrimSpace(agent.RuntimeHandle.Session) != "" {
		b.clearReviewGateTracking(agent.ID)
		return agent, true, nil
	}

	switch agent.State {
	case StateInitializing:
		b.clearReviewGateTracking(agent.ID)
		if b.queueCheckpointedReviewLaunchRecovery(ctx, agent) {
			return agent, false, nil
		}
		if now.Sub(agent.LastActivityTime) < reviewInitStallTimeout {
			return agent, true, nil
		}
		return b.handleStalledRuntimeLessReviewer(ctx, agent, now, "initialization")
	case StateReviewGate:
		if b.queueCheckpointedReviewLaunchRecovery(ctx, agent) {
			return agent, false, nil
		}
		status := b.getReviewGateStatus(agent.ID)
		if !status.started {
			return agent, true, nil
		}
		// Use the gate's own startedAt, not agent.LastActivityTime: that
		// field is a general "this agent record was touched for any
		// reason" timestamp, bumped by many unrelated setters (Touch,
		// SetState, SetRuntimeHandle, comment tracking, and more) that can
		// fire while the mandatory-test gate command is still running in
		// the background. Using it here meant a genuinely stuck gate could
		// silently never reach the alert threshold or the stall timeout,
		// as long as something unrelated kept refreshing LastActivityTime
		// in the meantime.
		elapsed := now.Sub(status.startedAt)
		alertThreshold := b.cfg.effectiveReviewGateAlertThreshold()
		if elapsed >= alertThreshold {
			if !status.alerted {
				status.alerted = true
				b.setReviewGateStatus(agent.ID, status)
				gateLogLine := ""
				if gateLogPath, err := mandatoryTestLogPath(agent); err == nil {
					gateLogLine = fmt.Sprintf("\nGate log: `%s`", gateLogPath)
				}
				b.notify(
					ctx,
					fmt.Sprintf(
						"Repository Agent Orchestrator: review hard gate is still running for review agent `%s` on PR %s after `%s`.\n\nHead SHA: `%s`\nMandatory tests:\n%s%s",
						agent.ID,
						fallback(strings.TrimSpace(agent.PRURL), fmt.Sprintf("#%d", agent.PRNumber)),
						formatElapsedDuration(elapsed),
						fallback(strings.TrimSpace(agent.ObservedPRHeadSHA), "unknown"),
						formatMandatoryTestList(b.cfg.MandatoryTests),
						gateLogLine,
					),
				)
				log.Printf(
					"review hard gate still running reviewer=%s coder=%s pr=%d head=%s elapsed=%s",
					agent.ID,
					strings.TrimSpace(agent.ParentAgentID),
					agent.PRNumber,
					abbreviateSHA(agent.ObservedPRHeadSHA),
					formatElapsedDuration(elapsed),
				)
			}
		}
		if elapsed < reviewLaunchStallTimeout {
			return agent, true, nil
		}
		return b.handleStalledRuntimeLessReviewer(ctx, agent, now, "review gate timeout")
	default:
		b.clearReviewGateTracking(agent.ID)
		return agent, true, nil
	}
}

func (b *Orchestrator) detectInputWait(ctx context.Context, agent Agent, now time.Time) error {
	if b.runner == nil {
		return nil
	}
	if b.coderHasActiveReviewInProgress(agent) {
		_ = b.setCoderLifecycleState(agent.ID, StateWaiting)
		b.clearInputWaitTracking(agent.ID)
		return nil
	}
	if agent.RuntimeHandle.Session == "" || (agent.State != StateInitializing && agent.State != StateWorking) {
		b.clearInputWaitTracking(agent.ID)
		return nil
	}

	pane, err := b.runner.Capture(agent.RuntimeHandle, runtimeCaptureLines)
	if err != nil {
		return err
	}
	paneHash := hashPane(normalizeInputWaitSearchText(pane))
	waitingForInput := looksLikeInputWait(pane)
	waitingForUserInput := looksLikeUserInputWait(pane)

	status := b.getInputWaitStatus(agent.ID)
	if status.lastPaneHash == "" || paneHash != status.lastPaneHash {
		status.lastPaneHash = paneHash
		status.lastChange = now
		status.alerted = false
	}
	if status.lastChange.IsZero() {
		status.lastChange = now
	}

	if !waitingForInput {
		status.alerted = false
		b.setInputWaitStatus(agent.ID, status)
		return nil
	}

	if now.Sub(status.lastChange) < b.inputWaitAlertThreshold() {
		b.setInputWaitStatus(agent.ID, status)
		return nil
	}

	if waitingForUserInput {
		if status.alerted {
			b.setInputWaitStatus(agent.ID, status)
			return nil
		}
		status.alerted = true
		b.setInputWaitStatus(agent.ID, status)
		b.notify(
			ctx,
			fmt.Sprintf(
				"Repository Agent Orchestrator: agent `%s` appears stuck waiting for user input (issue #%d, branch `%s`, session `%s`).\n\nRecent runtime output:\n%s",
				agent.ID,
				agent.IssueNumber,
				agent.BranchName,
				agent.RuntimeHandle.Session,
				formatRuntimePaneExcerpt(pane, 10),
			),
		)
		return nil
	}

	status.alerted = false
	_, err = b.sendScopedRuntimeMessage(agent, autoContinueSteeringText)
	if err != nil {
		log.Printf("auto-continue nudge failed agent=%s session=%s: %s", agent.ID, agent.RuntimeHandle.Session, b.safeError(err))
	} else {
		status.autoNudges++
		status.lastChange = now
		if b.agents != nil {
			_ = b.agents.Touch(agent.ID)
		}
		log.Printf(
			"auto-continue nudge sent agent=%s issue=%d branch=%s session=%s count=%d",
			agent.ID,
			agent.IssueNumber,
			agent.BranchName,
			agent.RuntimeHandle.Session,
			status.autoNudges,
		)
	}
	b.setInputWaitStatus(agent.ID, status)
	return nil
}

func (b *Orchestrator) detectRuntimeWorkingLoop(ctx context.Context, agent Agent, now time.Time) (Agent, bool, error) {
	if b.runner == nil || agent.RuntimeHandle.Session == "" || (agent.State != StateInitializing && agent.State != StateWorking) {
		b.clearRuntimeLoopTracking(agent.ID)
		return agent, true, nil
	}

	pane, err := b.runner.Capture(agent.RuntimeHandle, runtimeCaptureLines)
	if err != nil {
		return agent, true, err
	}
	if looksLikeInputWait(pane) || !looksLikeWorkingLoop(pane) {
		b.clearRuntimeLoopTracking(agent.ID)
		return agent, true, nil
	}

	status := b.getRuntimeLoopStatus(agent.ID)
	paneHash := runtimeLoopFingerprint(pane)
	if status.lastPaneHash == "" || paneHash != status.lastPaneHash {
		status.lastPaneHash = paneHash
		status.lastChange = now
		status.alerted = false
		b.setRuntimeLoopStatus(agent.ID, status)
		return agent, true, nil
	}
	if status.lastChange.IsZero() {
		status.lastChange = now
		status.alerted = false
		b.setRuntimeLoopStatus(agent.ID, status)
		return agent, true, nil
	}
	if now.Sub(status.lastChange) < b.runtimeLoopThreshold(agent) {
		b.setRuntimeLoopStatus(agent.ID, status)
		return agent, true, nil
	}

	if agent.Role == RoleReviewer {
		if err := b.handleStalledReviewAgent(ctx, agent, pane); err != nil {
			return agent, false, err
		}
		refreshed, ok := b.agents.Get(agent.ID)
		if !ok {
			return agent, false, nil
		}
		return refreshed, false, nil
	}
	if agent.Role == RoleCoder && status.alerted {
		if err := b.handleStalledCoderAgent(ctx, agent, pane); err != nil {
			return agent, false, err
		}
		refreshed, ok := b.agents.Get(agent.ID)
		if !ok {
			return agent, false, nil
		}
		return refreshed, false, nil
	}

	if status.alerted {
		b.setRuntimeLoopStatus(agent.ID, status)
		return agent, true, nil
	}
	status.alerted = true
	if agent.Role == RoleCoder {
		// Give a potentially long-running command one more full threshold before
		// replacing its runtime. A changed fingerprint clears this grace period.
		status.lastChange = now
	}
	b.setRuntimeLoopStatus(agent.ID, status)
	guidance := "Human steering is likely required."
	if agent.Role == RoleCoder {
		guidance = fmt.Sprintf(
			"Repository Agent Orchestrator will restart the coding runtime automatically if output remains unchanged for another %s; the worktree will be preserved.",
			formatElapsedDuration(b.runtimeLoopThreshold(agent)),
		)
	}
	b.notify(
		ctx,
		fmt.Sprintf(
			"Repository Agent Orchestrator: %s `%s` appears stuck in a working loop (issue #%d, branch `%s`, session `%s`). %s\n\nRecent runtime output:\n%s",
			agentRoleLabel(agent.Role),
			agent.ID,
			agent.IssueNumber,
			agent.BranchName,
			agent.RuntimeHandle.Session,
			guidance,
			formatRuntimePaneExcerpt(pane, 10),
		),
	)
	log.Printf(
		"working loop detected agent=%s role=%s issue=%d branch=%s pane=%q",
		agent.ID,
		agent.Role,
		agent.IssueNumber,
		agent.BranchName,
		abbreviateLogText(normalizeInputWaitSearchText(pane), 160),
	)
	return agent, true, nil
}

func (b *Orchestrator) coderHasActiveReviewInProgress(agent Agent) bool {
	if b == nil || b.agents == nil || agent.Role != RoleCoder {
		return false
	}
	activeReviewerID := strings.TrimSpace(agent.ActiveReviewAgentID)
	if activeReviewerID == "" {
		return false
	}
	reviewer, ok := b.agents.Get(activeReviewerID)
	if !ok || !reviewerCanBlockLaunch(reviewer) {
		return false
	}
	return true
}

func (b *Orchestrator) getReviewGateStatus(agentID string) reviewGateStatus {
	b.reviewGateMu.Lock()
	defer b.reviewGateMu.Unlock()
	if b.reviewGateByAgent == nil {
		b.reviewGateByAgent = make(map[string]reviewGateStatus)
	}
	return b.reviewGateByAgent[agentID]
}

func (b *Orchestrator) setReviewGateStatus(agentID string, status reviewGateStatus) {
	b.reviewGateMu.Lock()
	defer b.reviewGateMu.Unlock()
	if b.reviewGateByAgent == nil {
		b.reviewGateByAgent = make(map[string]reviewGateStatus)
	}
	b.reviewGateByAgent[agentID] = status
}

func (b *Orchestrator) clearReviewGateTracking(agentID string) {
	b.reviewGateMu.Lock()
	defer b.reviewGateMu.Unlock()
	if b.reviewGateByAgent == nil {
		return
	}
	delete(b.reviewGateByAgent, agentID)
}

func (b *Orchestrator) inputWaitAlertThreshold() time.Duration {
	delay := 2 * b.cfg.PollInterval
	if delay < inputWaitMinimumDuration {
		return inputWaitMinimumDuration
	}
	return delay
}

func (b *Orchestrator) runtimeLoopThreshold(agent Agent) time.Duration {
	if agent.Role == RoleReviewer {
		return reviewLoopMinimumDuration
	}
	return coderLoopMinimumDuration
}

func (b *Orchestrator) getInputWaitStatus(agentID string) inputWaitStatus {
	b.inputWaitMu.Lock()
	defer b.inputWaitMu.Unlock()
	if b.inputWaitByAgent == nil {
		b.inputWaitByAgent = make(map[string]inputWaitStatus)
	}
	return b.inputWaitByAgent[agentID]
}

func (b *Orchestrator) setInputWaitStatus(agentID string, status inputWaitStatus) {
	b.inputWaitMu.Lock()
	defer b.inputWaitMu.Unlock()
	if b.inputWaitByAgent == nil {
		b.inputWaitByAgent = make(map[string]inputWaitStatus)
	}
	b.inputWaitByAgent[agentID] = status
}

func (b *Orchestrator) clearInputWaitAlert(agentID string) {
	b.inputWaitMu.Lock()
	if b.inputWaitByAgent != nil {
		if status, ok := b.inputWaitByAgent[agentID]; ok {
			status.alerted = false
			status.lastChange = time.Now()
			b.inputWaitByAgent[agentID] = status
		}
	}
	b.inputWaitMu.Unlock()
	b.clearRuntimeLoopTracking(agentID)
}

func (b *Orchestrator) clearInputWaitTracking(agentID string) {
	b.inputWaitMu.Lock()
	defer b.inputWaitMu.Unlock()
	if b.inputWaitByAgent == nil {
		return
	}
	delete(b.inputWaitByAgent, agentID)
}

func (b *Orchestrator) getRuntimeLoopStatus(agentID string) runtimeLoopStatus {
	b.runtimeLoopMu.Lock()
	defer b.runtimeLoopMu.Unlock()
	if b.runtimeLoopByAgent == nil {
		b.runtimeLoopByAgent = make(map[string]runtimeLoopStatus)
	}
	return b.runtimeLoopByAgent[agentID]
}

func (b *Orchestrator) setRuntimeLoopStatus(agentID string, status runtimeLoopStatus) {
	b.runtimeLoopMu.Lock()
	defer b.runtimeLoopMu.Unlock()
	if b.runtimeLoopByAgent == nil {
		b.runtimeLoopByAgent = make(map[string]runtimeLoopStatus)
	}
	b.runtimeLoopByAgent[agentID] = status
}

func (b *Orchestrator) clearRuntimeLoopTracking(agentID string) {
	b.runtimeLoopMu.Lock()
	defer b.runtimeLoopMu.Unlock()
	if b.runtimeLoopByAgent == nil {
		return
	}
	delete(b.runtimeLoopByAgent, agentID)
}

func (b *Orchestrator) handleStalledReviewAgent(ctx context.Context, reviewer Agent, pane string) error {
	currentReviewer, ok := b.agents.Get(reviewer.ID)
	if !ok || currentReviewer.Stopped {
		b.clearRuntimeLoopTracking(reviewer.ID)
		b.clearReviewGateTracking(reviewer.ID)
		return nil
	}
	terminalizationErrs := make([]string, 0)
	result, lifecycleErr := b.transitionReviewCoordinatorLifecycle(
		ctx,
		currentReviewer.ID,
		reviewCoordinatorLifecycleRequest{
			Intent:                     ReviewCoordinatorLifecycleCleanup,
			FinalState:                 StateErrored,
			ReleaseCoordinatorWorktree: true,
		},
	)
	if strings.TrimSpace(result.Agent.ID) != "" {
		currentReviewer = result.Agent
	}
	terminalizationErrs = appendCleanupError(
		terminalizationErrs,
		lifecycleErr,
	)
	b.clearRuntimeLoopTracking(currentReviewer.ID)
	b.clearReviewGateTracking(currentReviewer.ID)

	parentID := strings.TrimSpace(currentReviewer.ParentAgentID)
	var coder Agent
	var hasCoder bool
	if parentID != "" {
		_ = b.agents.ClearActiveReviewAgentIfMatches(parentID, currentReviewer.ID)
		coder, hasCoder = b.agents.Get(parentID)
	}

	b.notify(
		ctx,
		fmt.Sprintf(
			"Repository Agent Orchestrator: review agent `%s` appears stuck in a working loop on PR %s; stopping it and requesting a fresh review run.\n\nRecent runtime output:\n%s",
			currentReviewer.ID,
			fallback(strings.TrimSpace(currentReviewer.PRURL), fmt.Sprintf("#%d", currentReviewer.PRNumber)),
			formatRuntimePaneExcerpt(pane, 10),
		),
	)
	log.Printf(
		"review agent stalled reviewer=%s coder=%s pr=%d head=%s pane=%q",
		currentReviewer.ID,
		parentID,
		currentReviewer.PRNumber,
		abbreviateSHA(currentReviewer.ObservedPRHeadSHA),
		abbreviateLogText(normalizeInputWaitSearchText(pane), 160),
	)

	if hasCoder && !coder.Stopped {
		if err := b.ensureReviewAgentForCoder(ctx, coder); err != nil {
			if len(terminalizationErrs) > 0 {
				return fmt.Errorf(
					"failed to terminalize stalled review coordinator %s: %s (relaunch error: %v)",
					currentReviewer.ID,
					strings.Join(terminalizationErrs, "; "),
					err,
				)
			}
			return fmt.Errorf("failed to relaunch review after stalled reviewer %s: %w", currentReviewer.ID, err)
		}
	}
	if len(terminalizationErrs) > 0 {
		return fmt.Errorf(
			"failed to terminalize stalled review coordinator %s: %s",
			currentReviewer.ID,
			strings.Join(terminalizationErrs, "; "),
		)
	}
	return nil
}

func (b *Orchestrator) handleStalledCoderAgent(ctx context.Context, coder Agent, pane string) error {
	current, ok := b.agents.Get(coder.ID)
	if !ok || current.Stopped || current.Role != RoleCoder {
		b.clearRuntimeLoopTracking(coder.ID)
		return nil
	}

	if err := b.stopRuntime(current); err != nil {
		return fmt.Errorf("failed to stop stalled coding runtime for %s: %w", current.ID, err)
	}

	reason := "automatic recovery from a stalled working loop"
	restarted, err := b.restartRuntimeWithPrompt(ctx, current, formatAutonomousRecoveryPrompt(current, reason))
	if err != nil {
		b.agents.SetState(current.ID, StateErrored, false)
		b.notify(
			ctx,
			fmt.Sprintf(
				"Repository Agent Orchestrator: coding agent `%s` stalled on issue #%d and its runtime could not be restarted; state set to errored so the failure is visible. The worktree was preserved.\n\nFailure: %s",
				current.ID,
				current.IssueNumber,
				b.safeError(err),
			),
		)
		return fmt.Errorf("failed to restart stalled coding runtime for %s: %w", current.ID, err)
	}

	b.clearRuntimeLoopTracking(current.ID)
	b.notify(
		ctx,
		fmt.Sprintf(
			"Repository Agent Orchestrator: coding agent `%s` was stuck in a working loop on issue #%d; replaced runtime session `%s` with `%s` and preserved the worktree.\n\nRecent stalled runtime output:\n%s",
			current.ID,
			current.IssueNumber,
			current.RuntimeHandle.Session,
			fallback(strings.TrimSpace(restarted.RuntimeHandle.Session), "(unknown)"),
			formatRuntimePaneExcerpt(pane, 10),
		),
	)
	log.Printf(
		"coding agent stalled and restarted agent=%s issue=%d pr=%d old_session=%s new_session=%s pane=%q",
		current.ID,
		current.IssueNumber,
		current.PRNumber,
		current.RuntimeHandle.Session,
		strings.TrimSpace(restarted.RuntimeHandle.Session),
		abbreviateLogText(normalizeInputWaitSearchText(pane), 160),
	)
	return nil
}

func (b *Orchestrator) handleStalledRuntimeLessReviewer(ctx context.Context, reviewer Agent, now time.Time, stage string) (Agent, bool, error) {
	currentReviewer, ok := b.agents.Get(reviewer.ID)
	if !ok || currentReviewer.Stopped {
		b.clearReviewGateTracking(reviewer.ID)
		return reviewer, false, nil
	}
	if currentReviewer.State == StateDone || currentReviewer.State == StateErrored || currentReviewer.State == StateStopped {
		b.clearReviewGateTracking(reviewer.ID)
		return currentReviewer, false, nil
	}

	parentID := strings.TrimSpace(currentReviewer.ParentAgentID)
	if parentID != "" && !b.reviewLaunchStillOwned(currentReviewer.ID, parentID) {
		if err := b.retireReviewer(currentReviewer, StateErrored, false, "stale review launch retirement"); err != nil {
			log.Printf("non-fatal: stale review cleanup failed reviewer=%s path=%s: %s", currentReviewer.ID, currentReviewer.WorktreePath, b.safeError(err))
		}
		log.Printf(
			"stale review launch retired reviewer=%s coder=%s pr=%d head=%s stage=%s",
			currentReviewer.ID,
			parentID,
			currentReviewer.PRNumber,
			abbreviateSHA(currentReviewer.ObservedPRHeadSHA),
			stage,
		)
		return currentReviewer, false, nil
	}

	terminalizationErrs := make([]string, 0)
	result, lifecycleErr := b.transitionReviewCoordinatorLifecycle(
		ctx,
		currentReviewer.ID,
		reviewCoordinatorLifecycleRequest{
			Intent:                     ReviewCoordinatorLifecycleCleanup,
			FinalState:                 StateErrored,
			ReleaseCoordinatorWorktree: true,
		},
	)
	if strings.TrimSpace(result.Agent.ID) != "" {
		currentReviewer = result.Agent
	}
	terminalizationErrs = appendCleanupError(
		terminalizationErrs,
		lifecycleErr,
	)
	b.clearReviewGateTracking(currentReviewer.ID)

	var coder Agent
	var hasCoder bool
	if parentID != "" {
		_ = b.agents.ClearActiveReviewAgentIfMatches(parentID, currentReviewer.ID)
		coder, hasCoder = b.agents.Get(parentID)
	}

	b.notify(
		ctx,
		fmt.Sprintf(
			"Repository Agent Orchestrator: review agent `%s` stalled before runtime launch during `%s` on PR %s after `%s`; stopping it and requesting a fresh review run.\n\nHead SHA: `%s`",
			currentReviewer.ID,
			stage,
			fallback(strings.TrimSpace(currentReviewer.PRURL), fmt.Sprintf("#%d", currentReviewer.PRNumber)),
			formatElapsedDuration(now.Sub(currentReviewer.LastActivityTime)),
			fallback(strings.TrimSpace(currentReviewer.ObservedPRHeadSHA), "unknown"),
		),
	)
	log.Printf(
		"review launch stalled reviewer=%s coder=%s pr=%d head=%s stage=%s elapsed=%s",
		currentReviewer.ID,
		parentID,
		currentReviewer.PRNumber,
		abbreviateSHA(currentReviewer.ObservedPRHeadSHA),
		stage,
		formatElapsedDuration(now.Sub(currentReviewer.LastActivityTime)),
	)

	if hasCoder && !coder.Stopped {
		if err := b.ensureReviewAgentForCoder(ctx, coder); err != nil {
			if len(terminalizationErrs) > 0 {
				return currentReviewer, false, fmt.Errorf(
					"failed to terminalize stalled review coordinator %s: %s (relaunch error: %v)",
					currentReviewer.ID,
					strings.Join(terminalizationErrs, "; "),
					err,
				)
			}
			return currentReviewer, false, fmt.Errorf("failed to relaunch review after stalled reviewer %s: %w", currentReviewer.ID, err)
		}
	}
	if len(terminalizationErrs) > 0 {
		return currentReviewer, false, fmt.Errorf(
			"failed to terminalize stalled review coordinator %s: %s",
			currentReviewer.ID,
			strings.Join(terminalizationErrs, "; "),
		)
	}
	return currentReviewer, false, nil
}

func (b *Orchestrator) stopRuntime(agent Agent) error {
	if agent.RuntimeHandle.Session == "" {
		if b.agents != nil {
			_ = b.agents.SetRuntimeHandle(agent.ID, RuntimeHandle{})
		}
		b.clearRuntimeLoopTracking(agent.ID)
		b.clearInputWaitTracking(agent.ID)
		return nil
	}
	if err := b.runner.Stop(agent.RuntimeHandle); err != nil {
		return fmt.Errorf("failed to stop runtime for agent %s: %w", agent.ID, err)
	}
	if b.agents != nil {
		_ = b.agents.SetRuntimeHandle(agent.ID, RuntimeHandle{})
	}
	b.clearRuntimeLoopTracking(agent.ID)
	b.clearInputWaitTracking(agent.ID)
	return nil
}

func (b *Orchestrator) relatedReviewersForCoder(agent Agent) []Agent {
	if b == nil || b.agents == nil || agent.Role != RoleCoder {
		return nil
	}
	coderID := strings.TrimSpace(agent.ID)
	activeReviewerID := strings.TrimSpace(agent.ActiveReviewAgentID)
	related := make([]Agent, 0)
	seen := make(map[string]struct{})
	for _, reviewer := range b.agents.List() {
		if reviewer.Role != RoleReviewer {
			continue
		}
		if activeReviewerID != "" && strings.EqualFold(strings.TrimSpace(reviewer.ID), activeReviewerID) {
			if _, exists := seen[reviewer.ID]; !exists {
				seen[reviewer.ID] = struct{}{}
				related = append(related, reviewer)
			}
			continue
		}
		if coderID != "" && strings.EqualFold(strings.TrimSpace(reviewer.ParentAgentID), coderID) {
			if _, exists := seen[reviewer.ID]; !exists {
				seen[reviewer.ID] = struct{}{}
				related = append(related, reviewer)
			}
			continue
		}
		if agent.PRNumber > 0 && reviewer.PRNumber == agent.PRNumber {
			if _, exists := seen[reviewer.ID]; !exists {
				seen[reviewer.ID] = struct{}{}
				related = append(related, reviewer)
			}
		}
	}
	sort.Slice(related, func(i, j int) bool {
		return related[i].LastActivityTime.After(related[j].LastActivityTime)
	})
	return related
}

func (b *Orchestrator) retireReviewer(reviewer Agent, finalState AgentState, captureHandoff bool, reason string) error {
	return b.retireReviewerWithPersistedHandoffPolicy(reviewer, finalState, captureHandoff, false, reason)
}

func (b *Orchestrator) retireReviewerPreservingPersistedHandoff(reviewer Agent, finalState AgentState, captureHandoff bool, reason string) error {
	return b.retireReviewerWithPersistedHandoffPolicy(reviewer, finalState, captureHandoff, true, reason)
}

func (b *Orchestrator) retireReviewerWithPersistedHandoffPolicy(reviewer Agent, finalState AgentState, captureHandoff bool, preservePersistedHandoff bool, reason string) error {
	if b == nil || b.agents == nil || reviewer.Role != RoleReviewer {
		return nil
	}
	b.cancelReviewGate(reviewer.ID)
	b.clearReviewGateTracking(reviewer.ID)
	result, lifecycleErr := b.transitionReviewCoordinatorLifecycle(
		context.Background(),
		reviewer.ID,
		reviewCoordinatorLifecycleRequest{
			Intent:                     ReviewCoordinatorLifecycleCleanup,
			FinalState:                 finalState,
			ReleaseCoordinatorWorktree: true,
			ReleaseRuntimeArtifacts:    true,
			PreservePersistedHandoff:   preservePersistedHandoff,
			CaptureHandoff:             captureHandoff,
		},
	)
	if strings.TrimSpace(result.Agent.ID) != "" {
		reviewer = result.Agent
	}
	errs := make([]string, 0)
	errs = appendCleanupError(errs, lifecycleErr)
	if lifecycleErr == nil {
		errs = appendCleanupError(
			errs,
			b.cancelLinkedReviewLaunchOnRetirement(reviewer),
		)
	}
	if parentID := strings.TrimSpace(reviewer.ParentAgentID); parentID != "" {
		_ = b.agents.ClearActiveReviewAgentIfMatches(parentID, reviewer.ID)
	}
	b.clearInputWaitTracking(reviewer.ID)
	persistedState := reviewer.State
	log.Printf("reviewer retired reviewer=%s coder=%s pr=%d state=%s reason=%s", reviewer.ID, strings.TrimSpace(reviewer.ParentAgentID), reviewer.PRNumber, persistedState, reason)
	if len(errs) > 0 {
		return fmt.Errorf("reviewer cleanup completed with errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (b *Orchestrator) cleanupLinkedReviewer(ctx context.Context, agent Agent) error {
	if b == nil || b.agents == nil || agent.Role != RoleCoder {
		return nil
	}
	errs := make([]string, 0)
	for _, reviewer := range b.relatedReviewersForCoder(agent) {
		errs = appendCleanupError(errs, b.retireReviewer(reviewer, StateDone, false, "linked reviewer cleanup"))
	}
	if activeReviewerID := strings.TrimSpace(agent.ActiveReviewAgentID); activeReviewerID != "" {
		_ = b.agents.ClearActiveReviewAgentIfMatches(agent.ID, activeReviewerID)
	}
	if len(errs) > 0 {
		return fmt.Errorf("linked review cleanup completed with errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (b *Orchestrator) closeAgentPR(ctx context.Context, agent Agent) error {
	if b == nil || b.github == nil || agent.Role == RoleReviewer || agent.AdoptedPR || agent.PRNumber <= 0 {
		return nil
	}
	pr, _, err := b.github.PullRequests.Get(ctx, b.cfg.RepoOwner, b.cfg.RepoName, agent.PRNumber)
	if err != nil {
		return fmt.Errorf("failed to fetch PR #%d: %w", agent.PRNumber, err)
	}
	if strings.EqualFold(pr.GetState(), "closed") || pr.GetMerged() {
		return nil
	}
	closed := "closed"
	if _, _, err := b.github.PullRequests.Edit(ctx, b.cfg.RepoOwner, b.cfg.RepoName, agent.PRNumber, &github.PullRequest{State: &closed}); err != nil {
		return fmt.Errorf("failed to close PR #%d: %w", agent.PRNumber, err)
	}
	return nil
}

func (b *Orchestrator) deleteAgentRemoteBranch(ctx context.Context, agent Agent) error {
	if b == nil || agent.Role == RoleReviewer || agent.AdoptedPR {
		return nil
	}
	branch := strings.TrimSpace(agent.BranchName)
	if branch == "" {
		return nil
	}
	output, err := outputCommand(ctx, b.cfg.RepoPath, "git", "ls-remote", "--heads", "origin", branch)
	if err != nil {
		return fmt.Errorf("failed to inspect remote branch %s: %w", branch, err)
	}
	if strings.TrimSpace(string(output)) == "" {
		return nil
	}
	if err := b.execCommand(ctx, b.cfg.RepoPath, "git", "push", "origin", "--delete", branch); err != nil {
		return fmt.Errorf("failed to delete remote branch %s: %w", branch, err)
	}
	return nil
}

func (b *Orchestrator) removeAgentRuntimeLog(agent Agent) error {
	logPath := strings.TrimSpace(agent.RuntimeHandle.LogPath)
	if logPath == "" {
		derivedLogPath, err := runtimeLogPath(agent)
		if err == nil {
			logPath = strings.TrimSpace(derivedLogPath)
		}
	}
	if logPath == "" {
		return nil
	}
	if err := os.Remove(logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove runtime log %q: %w", logPath, err)
	}
	return nil
}

func (b *Orchestrator) removeAgentRuntimeState(agent Agent) error {
	if b == nil || b.runner == nil {
		return nil
	}
	cleaner, ok := b.runner.(RuntimeStateCleaner)
	if !ok {
		// Non-production runners predate isolated runtime state and have
		// nothing to remove.
		return nil
	}
	if err := cleaner.RemoveRuntimeState(agent); err != nil {
		return fmt.Errorf(
			"failed to remove isolated runtime state for agent %s: %w",
			agent.ID,
			err,
		)
	}
	return nil
}

func (b *Orchestrator) removeAgentMandatoryTestLog(agent Agent) error {
	logPath, err := mandatoryTestLogPath(agent)
	if err != nil {
		return nil
	}
	if err := os.Remove(logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove mandatory test log %q: %w", logPath, err)
	}
	return nil
}

func cleanupBranchName(agent Agent) string {
	if agent.Role == RoleReviewer {
		return ""
	}
	return agent.BranchName
}

func prHeadBranch(agent Agent) string {
	if branch := strings.TrimSpace(agent.PRHeadBranch); branch != "" {
		return branch
	}
	return strings.TrimSpace(agent.BranchName)
}

func appendCleanupError(errs []string, err error) []string {
	if err == nil {
		return errs
	}
	return append(errs, err.Error())
}

func (b *Orchestrator) defaultBaseBranch() string {
	baseBranch := strings.TrimSpace(b.cfg.BaseBranch)
	if baseBranch == "" {
		baseBranch = "main"
	}
	return baseBranch
}

func (b *Orchestrator) prepareCoderWorktree(ctx context.Context, agent Agent) error {
	repoPath := strings.TrimSpace(b.cfg.RepoPath)
	if repoPath == "" {
		return errors.New("repo path is empty")
	}
	if strings.TrimSpace(agent.WorktreePath) == "" {
		return errors.New("agent worktree path is empty")
	}
	if strings.TrimSpace(agent.BranchName) == "" {
		return errors.New("agent branch name is empty")
	}

	baseBranch := b.defaultBaseBranch()
	// Use an explicit refspec so origin/<baseBranch> is refreshed even if remote.fetch is customized.
	baseRefspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", baseBranch, baseBranch)
	if err := b.execCommand(ctx, repoPath, "git", "fetch", "--prune", "origin", baseRefspec); err != nil {
		return fmt.Errorf("git fetch failed for base branch %s: %w", baseBranch, err)
	}
	if err := b.runGitWorktreeMutation(ctx, func() error {
		return b.execCommand(ctx, repoPath, "git", "worktree", "add", "-b", agent.BranchName, agent.WorktreePath, fmt.Sprintf("origin/%s", baseBranch))
	}); err != nil {
		return fmt.Errorf("git worktree add failed for %s: %w", agent.WorktreePath, err)
	}
	return nil
}

func (b *Orchestrator) prepareContinuationWorktree(ctx context.Context, agent Agent) error {
	repoPath := strings.TrimSpace(b.cfg.RepoPath)
	if repoPath == "" {
		return errors.New("repo path is empty")
	}
	if strings.TrimSpace(agent.WorktreePath) == "" {
		return errors.New("agent worktree path is empty")
	}
	if strings.TrimSpace(agent.BranchName) == "" {
		return errors.New("agent branch name is empty")
	}
	headBranch := strings.TrimSpace(agent.PRHeadBranch)
	if headBranch == "" {
		return errors.New("PR head branch is empty")
	}

	baseBranch := b.defaultBaseBranch()
	baseRefspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", baseBranch, baseBranch)
	headRefspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", headBranch, headBranch)
	if err := b.execCommand(ctx, repoPath, "git", "fetch", "--prune", "origin", baseRefspec, headRefspec); err != nil {
		return fmt.Errorf("git fetch failed for PR head branch %s: %w", headBranch, err)
	}
	if err := b.runGitWorktreeMutation(ctx, func() error {
		return b.execCommand(ctx, repoPath, "git", "worktree", "add", "-b", agent.BranchName, agent.WorktreePath, fmt.Sprintf("origin/%s", headBranch))
	}); err != nil {
		return fmt.Errorf("git worktree add failed for continuation %s: %w", agent.WorktreePath, err)
	}
	return nil
}

func (b *Orchestrator) prepareReviewerWorktree(ctx context.Context, reviewer Agent) error {
	repoPath := strings.TrimSpace(b.cfg.RepoPath)
	if repoPath == "" {
		return errors.New("repo path is empty")
	}
	if strings.TrimSpace(reviewer.WorktreePath) == "" {
		return errors.New("reviewer worktree path is empty")
	}
	sha := strings.TrimSpace(reviewer.ObservedPRHeadSHA)
	if sha == "" {
		return errors.New("reviewed head sha is empty")
	}
	branch := strings.TrimSpace(reviewer.BranchName)
	if branch != "" {
		if err := b.execCommand(ctx, repoPath, "git", "fetch", "--prune", "origin", branch); err != nil {
			return fmt.Errorf("git fetch failed for review branch %s: %w", branch, err)
		}
	}
	if err := b.runGitWorktreeMutation(ctx, func() error {
		return b.execCommand(ctx, repoPath, "git", "worktree", "add", "--detach", reviewer.WorktreePath, sha)
	}); err != nil {
		return fmt.Errorf("git worktree add failed for reviewer %s: %w", reviewer.WorktreePath, err)
	}
	return nil
}

func (b *Orchestrator) cleanupWorktree(ctx context.Context, worktreePath, branchName string) error {
	path := strings.TrimSpace(worktreePath)
	if path == "" {
		return nil
	}
	branch := strings.TrimSpace(branchName)
	repoPath := strings.TrimSpace(b.cfg.RepoPath)

	if b != nil && b.cleanupWorktreeFunc != nil {
		return b.cleanupWorktreeFunc(ctx, repoPath, path, branch)
	}

	if err := b.runGitWorktreeMutation(ctx, func() error {
		if err := b.execCommand(ctx, repoPath, "git", "worktree", "remove", path, "--force"); err != nil {
			if !pathWithin(strings.TrimSpace(b.cfg.WorktreeDir), path) {
				return fmt.Errorf("git worktree remove failed: %w", err)
			}
			if removeErr := os.RemoveAll(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return fmt.Errorf("git worktree remove failed: %w (fallback remove failed: %v)", err, removeErr)
			}
			if pruneErr := b.execCommand(ctx, repoPath, "git", "worktree", "prune", "--expire", "now"); pruneErr != nil {
				log.Printf("non-fatal: git worktree prune failed after fallback cleanup path=%s: %s", path, b.safeError(pruneErr))
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// git worktree remove (and the fallback os.RemoveAll above) can report
	// success while the directory is, in fact, still present on disk --
	// e.g. a mandatory test left a mount point, loop device, or another
	// process's open file handle somewhere underneath it, which can cause
	// a recursive delete to stop short without producing a fatal-looking
	// error for either git or os.RemoveAll to surface. Confirmed live: a
	// reviewer can retire cleanly with no error logged anywhere, yet its
	// review workers' multi-hundred-MB-to-multi-GB worktrees remain on
	// disk indefinitely, since nothing else ever checks for or retries
	// this once the owning reviewer is gone. Verify removal actually
	// happened instead of trusting either exit code, and try one more
	// forceful removal before giving up and surfacing a real error.
	if pathWithin(strings.TrimSpace(b.cfg.WorktreeDir), path) {
		if _, statErr := os.Stat(path); statErr == nil {
			if removeErr := os.RemoveAll(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return fmt.Errorf("worktree %q still exists after cleanup and could not be forcibly removed: %w", path, removeErr)
			}
			if _, statErr := os.Stat(path); statErr == nil {
				return fmt.Errorf("worktree %q still exists after cleanup despite no reported error (likely a busy mount point or open file handle left underneath it)", path)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("failed to verify worktree %q was removed: %w", path, statErr)
		}
	}

	if branch != "" {
		if err := b.execCommand(ctx, repoPath, "git", "branch", "-D", branch); err != nil {
			log.Printf("non-fatal: local branch deletion failed for %s: %s", branch, b.safeError(err))
		}
	}
	return nil
}

func (b *Orchestrator) gitWorktreeMutationSemaphore() chan struct{} {
	if b == nil {
		return nil
	}
	b.gitWorktreeMutationInitMu.Lock()
	defer b.gitWorktreeMutationInitMu.Unlock()
	if b.gitWorktreeMutationSlot == nil {
		b.gitWorktreeMutationSlot = make(chan struct{}, 1)
		b.gitWorktreeMutationSlot <- struct{}{}
	}
	return b.gitWorktreeMutationSlot
}

func (b *Orchestrator) runGitWorktreeMutation(
	ctx context.Context,
	run func() error,
) error {
	if b == nil {
		return errors.New("orchestrator is not configured")
	}
	if run == nil {
		return errors.New("git worktree mutation is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	semaphore := b.gitWorktreeMutationSemaphore()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-semaphore:
	}
	defer func() { semaphore <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	return run()
}

func (b *Orchestrator) syncBaseBranch(ctx context.Context) error {
	repoPath := strings.TrimSpace(b.cfg.RepoPath)
	if repoPath == "" {
		return errors.New("repo path is empty")
	}

	baseBranch := b.defaultBaseBranch()
	baseRefspec := fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", baseBranch, baseBranch)
	if err := b.execCommand(ctx, repoPath, "git", "fetch", "--prune", "origin", baseRefspec); err != nil {
		return fmt.Errorf("git fetch failed for base branch %s: %w", baseBranch, err)
	}

	currentBranchOutput, err := outputCommand(ctx, repoPath, "git", "branch", "--show-current")
	if err != nil {
		return fmt.Errorf("failed to detect current branch for repo sync: %w", err)
	}
	currentBranch := strings.TrimSpace(string(currentBranchOutput))
	if currentBranch == baseBranch {
		if err := b.execCommand(ctx, repoPath, "git", "merge", "--ff-only", fmt.Sprintf("origin/%s", baseBranch)); err != nil {
			return fmt.Errorf("git fast-forward failed for base branch %s: %w", baseBranch, err)
		}
		return nil
	}

	if err := b.execCommand(ctx, repoPath, "git", "branch", "-f", baseBranch, fmt.Sprintf("origin/%s", baseBranch)); err != nil {
		return fmt.Errorf("git branch update failed for base branch %s: %w", baseBranch, err)
	}

	return nil
}

func looksLikeInputWait(pane string) bool {
	if looksLikeUserInputWait(pane) {
		return true
	}

	tail := tailNonEmptyLines(stripANSIEscapeSequences(pane), 20)
	if len(tail) == 0 {
		return false
	}

	joined := normalizeInputWaitSearchText(strings.Join(tail, "\n"))
	markers := []string{
		"waiting for input",
		"awaiting input",
	}
	for _, marker := range markers {
		if strings.Contains(joined, marker) {
			return true
		}
	}
	if strings.Contains(joined, "context left") && strings.Contains(joined, "for shortcuts") {
		return true
	}

	return false
}

func looksLikeUserInputWait(pane string) bool {
	tail := tailNonEmptyLines(stripANSIEscapeSequences(pane), 20)
	if len(tail) == 0 {
		return false
	}

	joined := normalizeInputWaitSearchText(strings.Join(tail, "\n"))
	markers := []string{
		"awaiting your input",
		"need your input",
		"please provide",
		"choose an option",
		"select an option",
		"which option",
		"what should i do",
		"would you like to run the following command",
		"press enter to confirm or esc to cancel",
		"yes proceed y",
		"no and tell codex what to do differently",
		"do you want me to run",
	}
	for _, marker := range markers {
		if strings.Contains(joined, marker) {
			return true
		}
	}

	last := strings.ToLower(strings.TrimSpace(stripANSIEscapeSequences(tail[len(tail)-1])))
	if strings.HasSuffix(last, "?") {
		return strings.Contains(last, "which") || strings.Contains(last, "what") || strings.Contains(last, "confirm")
	}
	return false
}

func looksLikeWorkingLoop(pane string) bool {
	tail := tailNonEmptyLines(stripANSIEscapeSequences(pane), 20)
	if len(tail) == 0 {
		return false
	}

	for _, line := range tail {
		normalized := normalizeInputWaitSearchText(line)
		if strings.Contains(normalized, "working") && strings.Contains(normalized, "esc to interrupt") {
			return true
		}
	}
	return false
}

func runtimeLoopFingerprint(pane string) string {
	tail := tailNonEmptyLines(stripANSIEscapeSequences(pane), 20)
	if len(tail) == 0 {
		return hashPane("")
	}

	filtered := make([]string, 0, len(tail))
	for _, line := range tail {
		normalized := normalizeInputWaitSearchText(line)
		if strings.Contains(normalized, "working") && strings.Contains(normalized, "esc to interrupt") {
			continue
		}
		filtered = append(filtered, line)
	}
	if len(filtered) == 0 {
		filtered = tail
	}
	normalized := normalizeInputWaitSearchText(strings.Join(filtered, "\n"))
	return hashPane(normalized)
}

func formatRuntimePaneExcerpt(pane string, maxLines int) string {
	lines := tailNonEmptyLines(stripANSIEscapeSequences(pane), maxLines)
	if len(lines) == 0 {
		return "> (no recent output captured)"
	}
	for i, line := range lines {
		lines[i] = "> " + abbreviateLogText(line, 200)
	}
	return strings.Join(lines, "\n")
}

func normalizeInputWaitSearchText(text string) string {
	cleaned := strings.ToLower(stripANSIEscapeSequences(text))
	cleaned = inputWaitNormalizationPattern.ReplaceAllString(cleaned, " ")
	return strings.TrimSpace(cleaned)
}

func stripANSIEscapeSequences(text string) string {
	cleaned := ansiOSCSequencePattern.ReplaceAllString(text, "")
	return ansiControlSequencePattern.ReplaceAllString(cleaned, "")
}

func tailNonEmptyLines(text string, n int) []string {
	if n <= 0 {
		return nil
	}

	normalized := strings.ReplaceAll(text, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	lines := strings.Split(normalized, "\n")
	nonEmpty := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		nonEmpty = append(nonEmpty, trimmed)
	}
	if len(nonEmpty) <= n {
		return nonEmpty
	}
	return nonEmpty[len(nonEmpty)-n:]
}

func hashPane(pane string) string {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(pane))
	return strconv.FormatUint(hasher.Sum64(), 16)
}

func abbreviateLogText(text string, maxLen int) string {
	trimmed := strings.TrimSpace(text)
	if maxLen <= 0 || len(trimmed) <= maxLen {
		return trimmed
	}
	if maxLen <= 3 {
		return trimmed[:maxLen]
	}
	return trimmed[:maxLen-3] + "..."
}

type cappedTailBuffer struct {
	maxBytes  int
	buf       []byte
	truncated bool
}

func (b *cappedTailBuffer) Write(p []byte) (int, error) {
	if b == nil || b.maxBytes <= 0 {
		return len(p), nil
	}
	if len(p) >= b.maxBytes {
		b.truncated = true
		b.buf = append(b.buf[:0], p[len(p)-b.maxBytes:]...)
		return len(p), nil
	}
	total := len(b.buf) + len(p)
	if total > b.maxBytes {
		drop := total - b.maxBytes
		if drop >= len(b.buf) {
			b.buf = b.buf[:0]
		} else {
			copy(b.buf, b.buf[drop:])
			b.buf = b.buf[:len(b.buf)-drop]
		}
		b.truncated = true
	}
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *cappedTailBuffer) output() capturedOutput {
	if b == nil {
		return capturedOutput{}
	}
	return capturedOutput{text: string(b.buf), truncated: b.truncated}
}

type capturedOutput struct {
	text      string
	truncated bool
}

type synchronizedWriter struct {
	mu  sync.Mutex
	dst io.Writer
}

func (w *synchronizedWriter) Write(p []byte) (int, error) {
	if w == nil || w.dst == nil {
		return len(p), nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dst.Write(p)
}

type commandExecutionError struct {
	command string
	cause   error
	detail  string
}

type sanitizedCommandError struct {
	message string
	cause   error
}

func (e *sanitizedCommandError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func (e *sanitizedCommandError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *commandExecutionError) Error() string {
	if e == nil {
		return ""
	}
	message := fmt.Sprintf("command failed: %s", e.command)
	if detail := strings.TrimSpace(e.Detail()); detail != "" {
		return message + "\n" + detail
	}
	return message
}

func (e *commandExecutionError) Detail() string {
	if e == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	if e.cause != nil {
		if cause := strings.TrimSpace(e.cause.Error()); cause != "" {
			parts = append(parts, "Cause: "+cause)
		}
	}
	if detail := strings.TrimSpace(e.detail); detail != "" {
		parts = append(parts, detail)
	}
	return strings.Join(parts, "\n")
}

func (e *commandExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func formatCommandFailureDetail(stdout, stderr capturedOutput) string {
	sections := make([]string, 0, 2)
	if section := formatCommandFailureSection("stdout", stdout); section != "" {
		sections = append(sections, section)
	}
	if section := formatCommandFailureSection("stderr", stderr); section != "" {
		sections = append(sections, section)
	}
	return strings.Join(sections, "\n")
}

func formatCommandFailureSection(label string, output capturedOutput) string {
	lines := tailNonEmptyLines(stripANSIEscapeSequences(output.text), commandFailureOutputLines)
	if len(lines) == 0 {
		return ""
	}
	header := label + ":"
	if output.truncated {
		header = label + " (truncated):"
	}
	return header + "\n" + strings.Join(lines, "\n")
}

func formatCommandLine(name string, args ...string) string {
	command := strings.TrimSpace(name)
	if len(args) == 0 {
		return command
	}
	return strings.TrimSpace(command + " " + strings.Join(args, " "))
}

func (b *Orchestrator) execCommand(ctx context.Context, dir string, name string, args ...string) error {
	// git has no built-in bound on how long it can block (e.g. an SSH
	// passphrase or HTTPS credential prompt with nothing able to answer
	// it), so cap it here rather than letting a caller-supplied ctx with
	// no deadline hang indefinitely. Non-interactive credential env vars
	// (see nonInteractiveGitEnvEntries) make that specific hang fail fast
	// instead; this timeout is a backstop for any other stall (e.g.
	// network or lock contention).
	if name == "git" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, gitCommandTimeout)
		defer cancel()
	}
	var err error
	if b != nil && b.cmdRunner != nil {
		err = b.cmdRunner(ctx, dir, name, args...)
	} else {
		err = runCommandWithOutput(ctx, dir, name, args...)
	}
	if err == nil || b == nil {
		return err
	}
	return &sanitizedCommandError{
		message: b.safeError(err),
		cause:   err,
	}
}

func (b *Orchestrator) TailAgentLog(agentID string) error {
	agent, err := findTailableAgent(b, agentID)
	if err != nil {
		return err
	}
	runner := runAgentTailCommand
	if b != nil && b.tailRunner != nil {
		runner = b.tailRunner
	}
	return runner(agent.RuntimeHandle.LogPath)
}

func runAgentTailCommand(logPath string) error {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" {
		return errors.New("agent runtime log path is empty")
	}

	stdoutWriter := io.Writer(os.Stdout)
	stderrWriter := io.Writer(os.Stderr)
	if os.Stdout != nil && term.IsTerminal(int(os.Stdout.Fd())) {
		stdoutWriter = &carriageReturnNormalizingWriter{writer: os.Stdout}
	}
	if os.Stderr != nil && term.IsTerminal(int(os.Stderr.Fd())) {
		stderrWriter = &carriageReturnNormalizingWriter{writer: os.Stderr}
	}

	cmd := exec.Command("tail", "-F", logPath)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start tail -F for %s: %w", logPath, err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	interrupt := func() error {
		if cmd.Process == nil {
			return nil
		}
		if pgid := cmd.Process.Pid; pgid > 0 {
			if err := syscall.Kill(-pgid, syscall.SIGINT); err != nil && !errors.Is(err, syscall.ESRCH) {
				return err
			}
		}
		return nil
	}

	if os.Stdin != nil && term.IsTerminal(int(os.Stdin.Fd())) {
		input, err := newTerminalTailInputSource()
		if err == nil {
			return waitForInteractiveTailExit(logPath, os.Stdout, input, done, interrupt)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	for {
		select {
		case <-sigCh:
			if err := interrupt(); err != nil {
				return fmt.Errorf("failed to interrupt tail -F for %s: %w", logPath, err)
			}
		case err := <-done:
			return normalizeTailCommandError(logPath, err)
		}
	}
}

var errTailInputUnavailable = errors.New("tail input unavailable")

type tailInputSource interface {
	NextByte() (byte, error)
	Close() error
}

type carriageReturnNormalizingWriter struct {
	writer io.Writer
}

func (w *carriageReturnNormalizingWriter) Write(p []byte) (int, error) {
	if w == nil || w.writer == nil || len(p) == 0 {
		return len(p), nil
	}

	buf := make([]byte, 0, len(p)*2)
	prevCR := false
	for _, b := range p {
		if b == '\n' && !prevCR {
			buf = append(buf, '\r')
		}
		buf = append(buf, b)
		prevCR = b == '\r'
	}
	if _, err := w.writer.Write(buf); err != nil {
		return 0, err
	}
	return len(p), nil
}

type terminalTailInputSource struct {
	input *os.File
	fd    int
	state *term.State
}

func newTerminalTailInputSource() (*terminalTailInputSource, error) {
	input, err := os.OpenFile("/dev/tty", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	fd := int(input.Fd())
	state, err := term.MakeRaw(fd)
	if err != nil {
		_ = input.Close()
		return nil, err
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = term.Restore(fd, state)
		_ = input.Close()
		return nil, err
	}

	return &terminalTailInputSource{input: input, fd: fd, state: state}, nil
}

func (s *terminalTailInputSource) NextByte() (byte, error) {
	if s == nil {
		return 0, io.EOF
	}

	var buf [1]byte
	n, err := syscall.Read(s.fd, buf[:])
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EINTR) {
			return 0, errTailInputUnavailable
		}
		return 0, err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return buf[0], nil
}

func (s *terminalTailInputSource) Close() error {
	if s == nil {
		return nil
	}

	var err error
	if restoreErr := term.Restore(s.fd, s.state); restoreErr != nil {
		err = restoreErr
	}
	if nonblockErr := syscall.SetNonblock(s.fd, false); nonblockErr != nil && err == nil {
		err = nonblockErr
	}
	if s.input != nil {
		if closeErr := s.input.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func waitForInteractiveTailExit(logPath string, stdout io.Writer, input tailInputSource, done <-chan error, interrupt func() error) error {
	if input == nil {
		return errors.New("tail input source is not available")
	}
	defer input.Close()

	for {
		select {
		case err := <-done:
			return normalizeTailCommandError(logPath, err)
		default:
		}

		b, err := input.NextByte()
		switch {
		case err == nil:
			if b != 3 {
				continue
			}
			if stdout != nil {
				if _, writeErr := fmt.Fprint(stdout, "^C\r\n"); writeErr != nil {
					return writeErr
				}
			}
			if err := interrupt(); err != nil {
				return fmt.Errorf("failed to interrupt tail -F for %s: %w", logPath, err)
			}
		case errors.Is(err, errTailInputUnavailable), errors.Is(err, io.EOF):
			time.Sleep(tailInputPollInterval)
		default:
			return err
		}
	}
}

func normalizeTailCommandError(logPath string, err error) error {
	if err == nil {
		return nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGINT {
			return nil
		}
	}
	return fmt.Errorf("tail -F failed for %s: %w", logPath, err)
}

func runCommandWithOutput(ctx context.Context, dir string, name string, args ...string) error {
	cmd := newManagedCommand(dir, name, args...)

	stdout := &cappedTailBuffer{maxBytes: commandFailureOutputBytes}
	stderr := &cappedTailBuffer{maxBytes: commandFailureOutputBytes}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := runManagedCommand(ctx, cmd); err != nil {
		return &commandExecutionError{
			command: formatCommandLine(name, args...),
			cause:   err,
			detail:  formatCommandFailureDetail(stdout.output(), stderr.output()),
		}
	}
	return nil
}

func runCommandWithOutputAndLiveLog(ctx context.Context, dir string, logPath string, name string, args ...string) error {
	if strings.TrimSpace(logPath) == "" {
		return runCommandWithOutput(ctx, dir, name, args...)
	}

	cmd := newManagedCommand(dir, name, args...)

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open command log %q: %w", logPath, err)
	}
	defer file.Close()

	command := formatCommandLine(name, args...)
	if err := writeCommandLogHeader(file, command); err != nil {
		return fmt.Errorf("failed to write command log header %q: %w", logPath, err)
	}

	stdout := &cappedTailBuffer{maxBytes: commandFailureOutputBytes}
	stderr := &cappedTailBuffer{maxBytes: commandFailureOutputBytes}
	logWriter := &synchronizedWriter{dst: file}
	cmd.Stdout = io.MultiWriter(stdout, newRuntimeLogSanitizerWriter(logWriter))
	cmd.Stderr = io.MultiWriter(stderr, newRuntimeLogSanitizerWriter(logWriter))

	if err := runManagedCommand(ctx, cmd); err != nil {
		_ = writeCommandLogFooter(file, command, false, err.Error())
		return &commandExecutionError{
			command: command,
			cause:   err,
			detail:  formatCommandFailureDetail(stdout.output(), stderr.output()),
		}
	}
	if err := writeCommandLogFooter(file, command, true, ""); err != nil {
		return fmt.Errorf("failed to finalize command log %q: %w", logPath, err)
	}
	return nil
}

func newManagedCommand(dir string, name string, args ...string) *exec.Cmd {
	cmd := newCommand(name, args...)
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

func runManagedCommand(ctx context.Context, cmd *exec.Cmd) error {
	if cmd == nil {
		return errors.New("command is required")
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		return err
	default:
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		select {
		case err := <-done:
			return err
		default:
		}
		killManagedCommandProcessGroup(cmd)
		<-done
		return ctx.Err()
	}
}

func killManagedCommandProcessGroup(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pgid := cmd.Process.Pid
	if pgid <= 0 {
		return
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		log.Printf("non-fatal: failed to kill command process group pid=%d: %s", pgid, err.Error())
	}
}

func writeCommandLogHeader(dst io.Writer, command string) error {
	if dst == nil {
		return nil
	}
	_, err := fmt.Fprintf(dst, "[%s] command started: %s\n", time.Now().UTC().Format(time.RFC3339), strings.TrimSpace(command))
	return err
}

func writeCommandLogFooter(dst io.Writer, command string, success bool, detail string) error {
	if dst == nil {
		return nil
	}
	status := "succeeded"
	if !success {
		status = "failed"
	}
	if _, err := fmt.Fprintf(dst, "\n[%s] command %s: %s\n", time.Now().UTC().Format(time.RFC3339), status, strings.TrimSpace(command)); err != nil {
		return err
	}
	if detail = strings.TrimSpace(detail); detail != "" {
		if _, err := fmt.Fprintf(dst, "[%s] detail: %s\n", time.Now().UTC().Format(time.RFC3339), detail); err != nil {
			return err
		}
	}
	return nil
}

func resetCommandLog(logPath string) error {
	logPath = strings.TrimSpace(logPath)
	if logPath == "" {
		return nil
	}
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(logPath, nil, 0o644)
}

func appendCommandLogNote(logPath string, note string) error {
	logPath = strings.TrimSpace(logPath)
	note = strings.TrimSpace(note)
	if logPath == "" || note == "" {
		return nil
	}
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "[%s] %s\n", time.Now().UTC().Format(time.RFC3339), note)
	return err
}

func ensureDirExists(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("path is not a directory")
	}
	return nil
}

func ensureDirWritable(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(path, ".repository-agent-orchestrator-write-test-")
	if err != nil {
		return err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Remove(name); err != nil {
		return err
	}
	return nil
}

func isGitWorktreePath(path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	_, err := os.Stat(filepath.Join(path, ".git"))
	return err == nil
}

func isGitRepo(ctx context.Context, repoPath string) (bool, error) {
	cmd := newCommandContext(ctx, "git", "-C", repoPath, "rev-parse", "--is-inside-work-tree")
	output, err := cmd.Output()
	if err != nil {
		return false, nil
	}
	return strings.TrimSpace(string(output)) == "true", nil
}

func getGitHubTokenFromGH(ctx context.Context) (string, error) {
	cmd := newCommandContext(ctx, "gh", "auth", "token")
	output, err := cmd.Output()
	if err != nil {
		return "", errors.New("failed to read GitHub auth token from gh; run `gh auth login`")
	}
	token := strings.TrimSpace(string(output))
	if token == "" {
		return "", errors.New("gh auth token is empty; run `gh auth login`")
	}
	return token, nil
}

func runtimeCommandName(raw string) string {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return "codex"
	}
	return parts[0]
}

func repoPathMatchesTargetRepo(ctx context.Context, repoPath, owner, repo string) (bool, string, error) {
	actual, err := originRepoSlug(ctx, repoPath)
	if err != nil {
		return false, "", err
	}
	expected := fmt.Sprintf("%s/%s", strings.ToLower(strings.TrimSpace(owner)), strings.ToLower(strings.TrimSpace(repo)))
	return actual == expected, actual, nil
}

func originRepoSlug(ctx context.Context, repoPath string) (string, error) {
	cmd := newCommandContext(ctx, "git", "-C", repoPath, "remote", "get-url", "origin")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to read git origin remote")
	}
	owner, repo, ok := parseRemoteRepoSlug(string(out))
	if !ok {
		return "", fmt.Errorf("unable to parse git origin remote")
	}
	return fmt.Sprintf("%s/%s", strings.ToLower(owner), strings.ToLower(repo)), nil
}

func parseRemoteRepoSlug(raw string) (string, string, bool) {
	remote := strings.TrimSpace(raw)
	if remote == "" {
		return "", "", false
	}

	trimmed := strings.TrimSuffix(strings.TrimSuffix(remote, "/"), ".git")
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err == nil {
			pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
			if len(pathParts) >= 2 {
				return pathParts[len(pathParts)-2], pathParts[len(pathParts)-1], true
			}
		}
	}

	if strings.Contains(trimmed, "@") && strings.Contains(trimmed, ":") {
		parts := strings.SplitN(trimmed, ":", 2)
		scpPath := strings.Trim(parts[1], "/")
		pathParts := strings.Split(scpPath, "/")
		if len(pathParts) >= 2 {
			return pathParts[len(pathParts)-2], pathParts[len(pathParts)-1], true
		}
	}

	pathParts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(pathParts) >= 2 {
		return pathParts[len(pathParts)-2], pathParts[len(pathParts)-1], true
	}
	return "", "", false
}

var replHelpLines = []string{
	"Available commands:",
	"",
	"Core commands:",
	"  help",
	"  status",
	"  tech-support",
	"  exit | quit",
	"",
	"Issue workflow:",
	"  agent list",
	"  agent continue <issueNumber> <prNumber>",
	"  agent start <issueNumber>",
	"  agent start next",
	"  agent index repo",
	"",
	"Review tools:",
	"  agent review <prNumber>",
	"",
	"Agent control:",
	"  agent help",
	"",
	"Notes:",
	"  `status`'s final section, Review Cycle Health, gives one diagnosis line per active reviewer (healthy or a named known-failure pattern) instead of requiring manual inference from the rest of status",
	"  `tech-support` writes a single tar.gz (build version, effective config, status, agent details, budget analysis, persisted state, and a daemon log tail) for handoff/transfer without needing terminal copy/paste",
	"  issue numbers are the primary way to target Repository Agent Orchestrator workflow state; agent control defaults to the coder for that issue",
	"  `agent review <prNumber>` is PR-based so it can review standalone PRs while still rejoining tracked Repository Agent Orchestrator review flow when applicable",
	"  use `agent help` for stop/pause/steer/tail/cleanup syntax and role-targeted commands",
	"  unique command prefixes are accepted when unambiguous (for example: ag li, agent rev)",
}

var replAgentHelpLines = []string{
	"Agent commands:",
	"  agent help",
	"  agent list",
	"  agent index repo",
	"  agent continue <issueNumber> <prNumber>",
	"  agent start <issueNumber>",
	"  agent start next",
	"  agent review <prNumber>",
	"  agent cleanup [coder|reviewer] <issueOrPRNumber>",
	"  agent pause [coder|reviewer] <issueNumber>",
	"  agent unpause [coder|reviewer] <issueNumber>",
	"  agent stop [coder|reviewer] <issueOrPRNumber>",
	"  agent steer [coder|reviewer] <issueNumber> <text...>",
	"  agent tail",
	"  agent tail [coder|reviewer] <issueNumber>",
	"",
	"Notes:",
	"  omitted role defaults to `coder`",
	"  stop and cleanup accept a tracked or manual reviewer PR when no matching issue-scoped agent exists",
	"  `agent continue` starts from an existing same-repository PR head and leaves the PR and remote branch under human ownership",
	"  `agent review <prNumber>` uses tracked review semantics for Repository Agent Orchestrator PRs and detached one-shot review for standalone PRs",
	"  pause and unpause apply to coding and review agents; repo indexing agents cannot be paused",
	"  `agent steer` also resumes a paused agent before sending the steering message",
	"  `agent stop` prompts to clean up; if declined, the stopped agent remains available to a later `agent cleanup`",
	"  `agent tail` without a selector opens an interactive chooser on interactive terminals; press Ctrl-C while tailing to return to the REPL",
}

func printHelp() {
	for _, line := range replHelpLines {
		replPrintln(line)
	}
}

func printAgentHelp() {
	for _, line := range replAgentHelpLines {
		replPrintln(line)
	}
}

func printStatus(bot *Orchestrator) {
	active := bot.agents.Active()
	lastPoll := bot.agents.LastPoll()
	issues, manualReviewers, indexers := activeIssueSummaries(bot)
	reviewGates := make([]Agent, 0)
	for _, a := range active {
		if a.Role == RoleReviewer && a.State == StateReviewGate {
			reviewGates = append(reviewGates, a)
		}
	}

	replPrintln("Build")
	replPrintf("  %s\n", buildVersionString())
	replPrintln("")

	replPrintln("Repository")
	replPrintf("  repo: %s/%s\n", bot.cfg.RepoOwner, bot.cfg.RepoName)
	replPrintf("  path: %s\n", bot.cfg.RepoPath)
	replPrintf("  base branch: %s\n", bot.cfg.BaseBranch)
	replPrintf("  worktrees: %s\n", bot.cfg.WorktreeDir)
	replPrintf("  logs: %s\n", bot.cfg.LogDir)
	replPrintln("")

	replPrintln("Overview")
	replPrintf("  active issues: %d\n", len(issues))
	replPrintf("  manual reviews: %d\n", len(manualReviewers))
	replPrintf("  active agents: %d\n", len(active))
	replPrintf("  review gates: %d\n", len(reviewGates))
	replPrintf("  poll interval: %ds\n", bot.cfg.PollIntervalSeconds)
	if lastPoll.IsZero() {
		replPrintln("  last poll: never")
	} else {
		replPrintf("  last poll: %s\n", lastPoll.Format(time.RFC3339))
	}

	replPrintln("")
	replPrintf("%s", formatReviewPolicyStatus(bot.cfg.ReviewPolicy))

	replPrintln("")
	replPrintf("%s", formatReviewCyclesStatus(bot.agents.List()))

	replPrintln("")
	replPrintln("Issue Status")
	if len(issues) == 0 {
		replPrintln("  none")
	} else {
		for i, summary := range issues {
			if i > 0 {
				replPrintln("")
			}
			printIssueSummaryCard(bot, summary, false)
		}
	}

	if len(manualReviewers) > 0 {
		replPrintln("")
		replPrintln("Manual Reviews")
		for i, reviewer := range manualReviewers {
			if i > 0 {
				replPrintln("")
			}
			printManualReviewCard(bot, reviewer, false)
		}
	}

	if len(indexers) > 0 {
		replPrintln("")
		replPrintln("Repo Indexers")
		for _, agent := range indexers {
			replPrintf("  - %s\n", agent.ID)
			replPrintf("    state: %s\n", formatAgentStateSummary(&agent))
		}
	}

	now := time.Now()
	if len(reviewGates) > 0 {
		replPrintln("")
		replPrintln("Review Gates")
		for _, a := range reviewGates {
			replPrintf("  - reviewer: %s\n", a.ID)
			replPrintf("    pr: #%d\n", a.PRNumber)
			replPrintf("    head: %s\n", abbreviateSHA(a.ObservedPRHeadSHA))
			gateStatus := bot.getReviewGateStatus(a.ID)
			if gateStatus.started {
				replPrintf("    elapsed: %s\n", formatElapsedDuration(now.Sub(gateStatus.startedAt)))
			} else {
				replPrintln("    elapsed: (gate not started yet)")
			}
			if gateLogPath, err := mandatoryTestLogPath(a); err == nil {
				replPrintf("    gate log: %s\n", gateLogPath)
			}
		}
	}

	replPrintln("")
	replPrintln("Review Cycle Health")
	replPrintf("%s", formatReviewCycleHealthSummary(active, now))
}

// formatReviewCycleHealthSummary renders one diagnosis line per active
// reviewer (there may be zero, one, or many at once -- never assume a
// single active review), so the operator sees at a glance which of
// several simultaneous reviews, if any, needs attention, without having
// to re-derive it from the more detailed sections above.
func formatReviewCycleHealthSummary(agents []Agent, now time.Time) string {
	reviewers := make([]Agent, 0)
	for _, agent := range agents {
		if agent.Role == RoleReviewer {
			reviewers = append(reviewers, agent)
		}
	}
	if len(reviewers) == 0 {
		return "  none\n"
	}
	sort.Slice(reviewers, func(i, j int) bool {
		if reviewers[i].PRNumber != reviewers[j].PRNumber {
			return reviewers[i].PRNumber < reviewers[j].PRNumber
		}
		return reviewers[i].ID < reviewers[j].ID
	})
	var summary strings.Builder
	for _, reviewer := range reviewers {
		diagnosis := diagnoseReviewCycle(reviewer, now)
		fmt.Fprintln(&summary, formatReviewCycleDiagnosisLine(reviewer, diagnosis))
	}
	return summary.String()
}

func printAgents(bot *Orchestrator) {
	issues, manualReviewers, indexers := activeIssueSummaries(bot)
	failed := failedAgents(bot)
	if len(issues) == 0 && len(manualReviewers) == 0 && len(indexers) == 0 &&
		len(failed) == 0 {
		replPrintln("No agents.")
		return
	}

	if len(issues) > 0 || len(manualReviewers) > 0 || len(indexers) > 0 {
		replPrintln("Active Agents")
		for _, summary := range issues {
			printIssueSummaryCard(bot, summary, true)
			replPrintln("")
		}
	}

	if len(manualReviewers) > 0 {
		replPrintln("Manual Reviews")
		for idx, reviewer := range manualReviewers {
			printManualReviewCard(bot, reviewer, true)
			if idx < len(manualReviewers)-1 || len(indexers) > 0 {
				replPrintln("")
			}
		}
	}

	if len(indexers) > 0 {
		replPrintln("Repo Indexers")
		for idx, agent := range indexers {
			replPrintf("  - %s\n", agent.ID)
			replPrintf("    state: %s\n", formatAgentStateSummary(&agent))
			printAgentDetails(bot, "repo indexer", agent)
			if idx < len(indexers)-1 {
				replPrintln("")
			}
		}
	}

	if len(failed) > 0 {
		if len(issues) > 0 || len(manualReviewers) > 0 || len(indexers) > 0 {
			replPrintln("")
		}
		replPrintln("Failed Agents")
		for index, agent := range failed {
			replPrintf("  - %s: %s\n", selectorRoleLabel(agent.Role), formatAgentHeadline(&agent))
			if agent.IssueNumber > 0 {
				replPrintf("    issue: #%d\n", agent.IssueNumber)
			}
			if agent.PRNumber > 0 {
				replPrintf("    pr: #%d\n", agent.PRNumber)
			}
			if strings.TrimSpace(agent.ObservedPRHeadSHA) != "" {
				replPrintf("    head: %s\n", abbreviateSHA(agent.ObservedPRHeadSHA))
			}
			if strings.TrimSpace(agent.FailureMessage) != "" {
				replPrintf("    failure: %s\n", agent.FailureMessage)
			}
			replPrintf("    last activity: %s\n", agent.LastActivityTime.Local().Format(time.RFC3339))
			if index < len(failed)-1 {
				replPrintln("")
			}
		}
	}
}

func failedAgents(bot *Orchestrator) []Agent {
	if bot == nil || bot.agents == nil {
		return nil
	}
	failed := make([]Agent, 0)
	for _, agent := range bot.agents.List() {
		if agent.State == StateErrored {
			failed = append(failed, agent)
		}
	}
	return failed
}

func printManualReviewCard(bot *Orchestrator, reviewer Agent, includeAgentDetails bool) {
	replPrintf("  [%s]\n", formatManualReviewHeadline(reviewer))
	replPrintln("    mode: detached manual review")
	if strings.TrimSpace(reviewer.PRURL) != "" {
		replPrintf("    pr url: %s\n", reviewer.PRURL)
	}
	replPrintf("    reviewer: %s\n", formatAgentHeadline(&reviewer))
	if !includeAgentDetails {
		return
	}
	printAgentDetails(bot, "reviewer", reviewer)
}

func printIssueSummaryCard(bot *Orchestrator, summary issueContextSummary, includeAgentDetails bool) {
	replPrintf("  [%s]\n", formatIssueHeadline(summary))
	replPrintf("    workflow: %s\n", formatWorkflowState(summary))
	replPrintf("    pr: %s\n", formatPRSummary(summary))
	if strings.TrimSpace(summary.PRURL) != "" {
		replPrintf("    pr url: %s\n", summary.PRURL)
	}
	replPrintf("    coder: %s\n", formatAgentHeadline(summary.Coder))
	replPrintf("    reviewer: %s\n", formatReviewerHeadline(summary))
	if !includeAgentDetails {
		return
	}
	if summary.Coder != nil {
		printAgentDetails(bot, "coder", *summary.Coder)
	}
	if summary.Reviewer != nil {
		printAgentDetails(bot, "reviewer", *summary.Reviewer)
	}
}

func formatWorkflowState(summary issueContextSummary) string {
	switch {
	case summary.Reviewer != nil && summary.Reviewer.State == StateReviewGate:
		return "review gate"
	case summary.Reviewer != nil:
		return "reviewing"
	case summary.Coder != nil:
		switch summary.Coder.State {
		case StateWaiting:
			if summary.PRNumber > 0 {
				return "waiting on review"
			}
		case StateApproved:
			return "approved"
		case StateWorking:
			return "coding"
		}
		return strings.ReplaceAll(string(summary.Coder.State), "_", " ")
	default:
		return "idle"
	}
}

// formatReviewerHeadline reports "none" for an issue with no active
// reviewer, same as formatAgentHeadline -- unless the coder's current
// head is only marked reviewed via holdReviewOnInactiveReviewer's manual
// hold (its previous reviewer went inactive before a verdict), or its most
// recent review-coordinator launch attempt for that head ended terminally
// with automatic retry disabled (isNonRetryableReviewBlockForHead), either
// of which otherwise looks identical to a genuinely idle, nothing-to-review
// issue in this summary. Surfacing it here means an operator scanning
// `status` doesn't have to already know to inspect raw agent state to
// discover a PR is silently waiting on a manual `agent review <pr>`.
func formatReviewerHeadline(summary issueContextSummary) string {
	if summary.Reviewer != nil {
		return formatAgentHeadline(summary.Reviewer)
	}
	if summary.Coder != nil && summary.PRNumber > 0 &&
		isManualReviewHoldForHead(*summary.Coder, summary.Coder.ObservedPRHeadSHA) {
		return fmt.Sprintf("none (manual hold on %s; run `agent review %d` to resume)", abbreviateSHA(summary.Coder.ObservedPRHeadSHA), summary.PRNumber)
	}
	if summary.Coder != nil && summary.PRNumber > 0 &&
		isNonRetryableReviewBlockForHead(*summary.Coder, summary.Coder.ObservedPRHeadSHA) {
		return fmt.Sprintf("none (automatic retry disabled for %s; run `agent review %d` to resume)", abbreviateSHA(summary.Coder.ObservedPRHeadSHA), summary.PRNumber)
	}
	return formatAgentHeadline(summary.Reviewer)
}

func formatAgentHeadline(agent *Agent) string {
	if agent == nil {
		return "none"
	}
	return fmt.Sprintf("%s (%s)", formatAgentStateSummary(agent), agent.ID)
}

func formatManualReviewHeadline(reviewer Agent) string {
	if reviewer.PRNumber <= 0 {
		return reviewer.ID
	}
	if strings.TrimSpace(reviewer.PRTitle) != "" {
		return fmt.Sprintf("pr #%d: %s", reviewer.PRNumber, strings.TrimSpace(reviewer.PRTitle))
	}
	return fmt.Sprintf("pr #%d", reviewer.PRNumber)
}

func printAgentDetails(bot *Orchestrator, label string, agent Agent) {
	if agent.AdoptedPR {
		replPrintf("    %s mode: existing PR continuation\n", label)
	}
	replPrintf("    %s branch: %s\n", label, agent.BranchName)
	if agent.AdoptedPR {
		replPrintf("    %s PR head branch: %s\n", label, agent.PRHeadBranch)
	}
	if agent.Role == RoleReviewer && agent.State == StateReviewGate {
		replPrintf("    %s phase: mandatory-tests\n", label)
		replPrintf("    %s head: %s\n", label, abbreviateSHA(agent.ObservedPRHeadSHA))
		replPrintf("    %s elapsed: %s\n", label, formatElapsedDuration(time.Since(agent.LastActivityTime)))
		if gateLogPath, err := mandatoryTestLogPath(agent); err == nil {
			replPrintf("    %s gate log: %s\n", label, gateLogPath)
		}
	}
	if agent.RuntimeHandle.Session == "" {
		replPrintf("    %s runtime: none\n", label)
	} else {
		alive := false
		errText := ""
		if bot != nil && bot.runner != nil {
			var err error
			alive, err = bot.runner.IsAlive(agent.RuntimeHandle)
			if err != nil {
				errText = bot.safeError(err)
			}
		}
		if errText != "" {
			replPrintf("    %s runtime: %s session=%s alive=unknown err=%s\n", label, agent.RuntimeHandle.Kind, agent.RuntimeHandle.Session, errText)
		} else {
			replPrintf("    %s runtime: %s session=%s alive=%t\n", label, agent.RuntimeHandle.Kind, agent.RuntimeHandle.Session, alive)
		}
		if strings.TrimSpace(agent.RuntimeHandle.TmuxServer) != "" {
			replPrintf(
				"    %s tmux server: %s\n",
				label,
				agent.RuntimeHandle.TmuxServer,
			)
			replPrintf(
				"    %s attach (read-only): tmux -L %s attach-session -r -t %s\n",
				label,
				agent.RuntimeHandle.TmuxServer,
				agent.RuntimeHandle.Session,
			)
		}
		if agent.RuntimeHandle.Scope.Version != 0 {
			replPrintf(
				"    %s scope: %s\n",
				label,
				runtimeScopeSummary(agent.RuntimeHandle.Scope),
			)
		}
		if strings.TrimSpace(agent.RuntimeHandle.LogPath) != "" {
			replPrintf("    %s log: %s\n", label, agent.RuntimeHandle.LogPath)
		}
	}
	replPrintf("    %s worktree: %s\n", label, agent.WorktreePath)
	replPrintf("    %s last activity: %s\n", label, agent.LastActivityTime.Local().Format(time.RFC3339))
}

func tailableAgents(bot *Orchestrator) []Agent {
	agents := bot.agents.Active()
	sort.Slice(agents, func(i, j int) bool {
		return agents[i].LastActivityTime.After(agents[j].LastActivityTime)
	})

	filtered := make([]Agent, 0, len(agents))
	for _, agent := range agents {
		if strings.TrimSpace(agent.RuntimeHandle.LogPath) == "" {
			continue
		}
		filtered = append(filtered, agent)
	}
	return filtered
}

func findTailableAgent(bot *Orchestrator, agentID string) (Agent, error) {
	if bot == nil || bot.agents == nil {
		return Agent{}, fmt.Errorf("agent not found: %s", agentID)
	}
	agent, ok := bot.agents.Get(agentID)
	if !ok || agent.Stopped || agent.State == StateDone || agent.State == StateErrored || agent.State == StateStopped {
		return Agent{}, fmt.Errorf("agent not found: %s", agentID)
	}
	if strings.TrimSpace(agent.RuntimeHandle.LogPath) == "" {
		return Agent{}, fmt.Errorf("agent %s does not have a runtime log", agentID)
	}
	return agent, nil
}

func formatTailSelectorOption(agent Agent) string {
	parts := []string{
		agent.ID,
		fmt.Sprintf("role=%s", fallback(strings.TrimSpace(string(agent.Role)), string(RoleCoder))),
		fmt.Sprintf("state=%s", agent.State),
	}
	if agent.IssueNumber > 0 {
		parts = append(parts, fmt.Sprintf("issue=%d", agent.IssueNumber))
	}
	if agent.PRNumber > 0 {
		parts = append(parts, fmt.Sprintf("pr=%d", agent.PRNumber))
	}
	return strings.Join(parts, " ")
}

func formatElapsedDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return d.Truncate(time.Second).String()
}

func formatMandatoryTestList(commands []string) string {
	if len(commands) == 0 {
		return "- (none configured)"
	}
	lines := make([]string, 0, len(commands))
	for _, raw := range commands {
		command := strings.TrimSpace(raw)
		if command == "" {
			continue
		}
		lines = append(lines, "- `"+command+"`")
	}
	if len(lines) == 0 {
		return "- (none configured)"
	}
	return strings.Join(lines, "\n")
}

func normalizeREPLNewlines(text string) string {
	normalized := strings.ReplaceAll(text, "\r\n", "\n")
	return strings.ReplaceAll(normalized, "\n", "\r\n")
}

// replOutputOverride lets captureREPLOutput (tech_support.go) redirect
// replPrintf/replPrintln's output into an in-memory buffer instead of the
// real os.Stdout, without touching the process-global os.Stdout file
// descriptor itself (other code, like the interactive tail chooser, needs
// a real *os.File there). nil means "write to os.Stdout as normal," which
// is also what every existing test that reassigns os.Stdout directly
// already relies on.
//
// replOutputMu guards both reading replOutputOverride *and* the write
// itself (held for the whole Fprint call, not just the lookup): whatever
// replOutputOverride points to during a capture is a single shared
// *bytes.Buffer, which is not safe for concurrent use, so a concurrent
// replPrintf call from another goroutine (an operator's interactive
// command racing an automatic tech-support capture) writing to that same
// buffer without serialization would be a genuine data race, not just
// misdirected output. Serializing the write itself here closes that. It
// does not change where the output ends up going, though: if a capture is
// in progress when the write happens, it still lands in the capture's
// buffer instead of the terminal (see captureREPLOutput's comment in
// tech_support.go for why that narrower, non-corrupting misdirection is
// accepted rather than solved here).
var (
	replOutputMu       sync.Mutex
	replOutputOverride io.Writer
)

func replPrintf(format string, args ...any) {
	replOutputMu.Lock()
	defer replOutputMu.Unlock()
	w := io.Writer(os.Stdout)
	if replOutputOverride != nil {
		w = replOutputOverride
	}
	_, _ = fmt.Fprint(w, normalizeREPLNewlines(fmt.Sprintf(format, args...)))
}

func replPrintln(text string) {
	replPrintf("%s\n", text)
}

type replLineReader interface {
	ReadLine(prompt string) (string, error)
	Close() error
}

type replAgentSelector interface {
	SelectAgent(prompt string, agents []Agent) (Agent, error)
}

type scannerLineReader struct {
	scanner *bufio.Scanner
	writer  io.Writer
	input   *os.File
	closeMu sync.Once
}

func (r *scannerLineReader) ReadLine(prompt string) (string, error) {
	if _, err := fmt.Fprint(r.writer, prompt); err != nil {
		return "", err
	}
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return "", err
		}
		return "", io.EOF
	}
	return r.scanner.Text(), nil
}

func (r *scannerLineReader) Close() error {
	if r == nil {
		return nil
	}

	var err error
	r.closeMu.Do(func() {
		if r.input != nil {
			err = r.input.Close()
		}
	})
	return err
}

type terminalLineReader struct {
	reader  *bufio.Reader
	writer  io.Writer
	fd      int
	state   *term.State
	history []string
	input   *os.File
	closeMu sync.Once
}

type terminalMenuRenderer struct {
	writer      io.Writer
	rendered    bool
	renderedLen int
}

func (r *terminalLineReader) ReadLine(prompt string) (string, error) {
	if r.state != nil {
		previousState, err := term.MakeRaw(r.fd)
		if err != nil {
			return "", fmt.Errorf("failed to configure terminal input: %w", err)
		}
		defer term.Restore(r.fd, previousState)
	}

	if _, err := fmt.Fprint(r.writer, prompt); err != nil {
		return "", err
	}

	current := make([]byte, 0, 64)
	historyPos := len(r.history)
	draft := ""

	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(current) == 0 {
					return "", io.EOF
				}
				if _, writeErr := fmt.Fprint(r.writer, "\r\n"); writeErr != nil {
					return "", writeErr
				}
				line := string(current)
				r.addHistory(line)
				return line, nil
			}
			return "", err
		}

		switch b {
		case '\r', '\n':
			if _, err := fmt.Fprint(r.writer, "\r\n"); err != nil {
				return "", err
			}
			line := string(current)
			r.addHistory(line)
			return line, nil
		case 127, 8:
			if len(current) == 0 {
				continue
			}
			current = current[:len(current)-1]
			if err := r.redraw(prompt, string(current)); err != nil {
				return "", err
			}
		case 27:
			seqType, err := r.reader.ReadByte()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return "", io.EOF
				}
				return "", err
			}
			if seqType != '[' {
				continue
			}
			seqCode, err := r.reader.ReadByte()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return "", io.EOF
				}
				return "", err
			}

			// Ignore extended control sequences (e.g. delete/home/end).
			if seqCode >= '0' && seqCode <= '9' {
				for {
					terminal, err := r.reader.ReadByte()
					if err != nil {
						if errors.Is(err, io.EOF) {
							return "", io.EOF
						}
						return "", err
					}
					if terminal == '~' || (terminal >= 'A' && terminal <= 'Z') {
						break
					}
				}
				continue
			}

			switch seqCode {
			case 'A': // up
				if len(r.history) == 0 {
					continue
				}
				if historyPos == len(r.history) {
					draft = string(current)
				}
				if historyPos > 0 {
					historyPos--
				}
				current = append(current[:0], r.history[historyPos]...)
				if err := r.redraw(prompt, string(current)); err != nil {
					return "", err
				}
			case 'B': // down
				if len(r.history) == 0 {
					continue
				}
				switch {
				case historyPos < len(r.history)-1:
					historyPos++
					current = append(current[:0], r.history[historyPos]...)
				case historyPos == len(r.history)-1:
					historyPos = len(r.history)
					current = append(current[:0], draft...)
				default:
					continue
				}
				if err := r.redraw(prompt, string(current)); err != nil {
					return "", err
				}
			}
		case 3:
			if _, err := fmt.Fprint(r.writer, "^C\r\n"); err != nil {
				return "", err
			}
			return "", errREPLInterrupted
		default:
			if b < 32 {
				continue
			}
			current = append(current, b)
			if _, err := r.writer.Write([]byte{b}); err != nil {
				return "", err
			}
		}
	}
}

func (r *terminalLineReader) SelectAgent(prompt string, agents []Agent) (Agent, error) {
	if len(agents) == 0 {
		return Agent{}, errors.New("no agents available")
	}

	if r.state != nil {
		previousState, err := term.MakeRaw(r.fd)
		if err != nil {
			return Agent{}, fmt.Errorf("failed to configure terminal input: %w", err)
		}
		defer term.Restore(r.fd, previousState)
	}

	index := 0
	renderer := terminalMenuRenderer{writer: r.writer}
	if err := renderer.Render(prompt, agents, index); err != nil {
		return Agent{}, err
	}

	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return Agent{}, io.EOF
			}
			return Agent{}, err
		}

		switch b {
		case '\r', '\n':
			if err := renderer.Clear(); err != nil {
				return Agent{}, err
			}
			if _, err := fmt.Fprint(r.writer, "\r\n"); err != nil {
				return Agent{}, err
			}
			return agents[index], nil
		case 3:
			if err := renderer.Clear(); err != nil {
				return Agent{}, err
			}
			if _, err := fmt.Fprint(r.writer, "^C\r\n"); err != nil {
				return Agent{}, err
			}
			return Agent{}, errREPLInterrupted
		case 27:
			seqType, err := r.reader.ReadByte()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return Agent{}, io.EOF
				}
				return Agent{}, err
			}
			if seqType != '[' {
				continue
			}
			seqCode, err := r.reader.ReadByte()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return Agent{}, io.EOF
				}
				return Agent{}, err
			}
			switch seqCode {
			case 'A':
				if index > 0 {
					index--
				}
			case 'B':
				if index < len(agents)-1 {
					index++
				}
			default:
				continue
			}
			if err := renderer.Render(prompt, agents, index); err != nil {
				return Agent{}, err
			}
		}
	}
}

func (r *terminalLineReader) redraw(prompt string, line string) error {
	_, err := fmt.Fprintf(r.writer, "\r%s%s\x1b[K", prompt, line)
	return err
}

func (r *terminalLineReader) addHistory(line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	r.history = append(r.history, line)
}

func (r *terminalLineReader) Close() error {
	if r == nil {
		return nil
	}

	var err error
	r.closeMu.Do(func() {
		if r.state != nil {
			if restoreErr := term.Restore(r.fd, r.state); restoreErr != nil {
				err = restoreErr
			}
		}
		if r.input != nil {
			if closeErr := r.input.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}
	})
	return err
}

func (r *terminalMenuRenderer) Render(prompt string, agents []Agent, selected int) error {
	lines := make([]string, 0, len(agents)+2)
	lines = append(lines, prompt)
	lines = append(lines, "Use up/down to select an agent log, then press Enter to tail it.")
	for i, agent := range agents {
		prefix := "  "
		if i == selected {
			prefix = "> "
		}
		lines = append(lines, prefix+formatTailSelectorOption(agent))
	}
	if err := r.clear(); err != nil {
		return err
	}
	if _, err := fmt.Fprint(r.writer, strings.Join(lines, "\r\n")); err != nil {
		return err
	}
	r.rendered = true
	r.renderedLen = len(lines)
	return nil
}

func (r *terminalMenuRenderer) Clear() error {
	return r.clear()
}

func (r *terminalMenuRenderer) clear() error {
	if !r.rendered || r.renderedLen == 0 {
		return nil
	}
	for i := 0; i < r.renderedLen; i++ {
		if _, err := fmt.Fprint(r.writer, "\r\x1b[2K"); err != nil {
			return err
		}
		if i < r.renderedLen-1 {
			if _, err := fmt.Fprint(r.writer, "\x1b[1A"); err != nil {
				return err
			}
		}
	}
	r.rendered = false
	r.renderedLen = 0
	return nil
}

func newREPLLineReader(stdin *os.File, stdout *os.File) (replLineReader, error) {
	if term.IsTerminal(int(stdin.Fd())) && term.IsTerminal(int(stdout.Fd())) {
		state, err := term.GetState(int(stdin.Fd()))
		if err != nil {
			return nil, fmt.Errorf("failed to configure terminal input: %w", err)
		}
		return &terminalLineReader{
			reader: bufio.NewReader(stdin),
			writer: stdout,
			fd:     int(stdin.Fd()),
			state:  state,
			input:  stdin,
		}, nil
	}
	return &scannerLineReader{
		scanner: bufio.NewScanner(stdin),
		writer:  stdout,
		input:   stdin,
	}, nil
}

type commandOption struct {
	name      string
	canonical string
}

type unknownCommandError struct {
	token string
}

func (e unknownCommandError) Error() string {
	return fmt.Sprintf("unknown command %q", e.token)
}

type ambiguousCommandError struct {
	token   string
	matches []string
}

func (e ambiguousCommandError) Error() string {
	return fmt.Sprintf("ambiguous command %q", e.token)
}

var replRootCommandOptions = []commandOption{
	{name: "help", canonical: "help"},
	{name: "agent", canonical: "agent"},
	{name: "status", canonical: "status"},
	{name: "tech-support", canonical: "tech-support"},
	{name: "quit", canonical: "quit"},
	{name: "exit", canonical: "exit"},
}

var replAgentCommandOptions = []commandOption{
	{name: "cleanup", canonical: "cleanup"},
	{name: "continue", canonical: "continue"},
	{name: "help", canonical: "help"},
	{name: "index", canonical: "index"},
	{name: "list", canonical: "list"},
	{name: "pause", canonical: "pause"},
	{name: "tail", canonical: "tail"},
	{name: "start", canonical: "start"},
	{name: "review", canonical: "review"},
	{name: "stop", canonical: "stop"},
	{name: "steer", canonical: "steer"},
	{name: "unpause", canonical: "unpause"},
}

var replAgentRoleOptions = []commandOption{
	{name: "coder", canonical: string(RoleCoder)},
	{name: "reviewer", canonical: string(RoleReviewer)},
}

type issueSelectorParseError struct {
	message string
}

func (e issueSelectorParseError) Error() string {
	return e.message
}

func resolveAgentRoleToken(token string) (AgentRole, error) {
	resolved, err := resolveCommandToken(token, replAgentRoleOptions)
	if err != nil {
		return "", err
	}
	return AgentRole(resolved), nil
}

func resolveIssueScopedAgent(bot *Orchestrator, parts []string, defaultRole AgentRole) (Agent, int, error) {
	return resolveIssueScopedAgentForCommand(bot, parts, defaultRole, false, false)
}

func resolveIssueScopedAgentForCleanup(bot *Orchestrator, parts []string, defaultRole AgentRole) (Agent, int, error) {
	return resolveIssueScopedAgentForCommand(bot, parts, defaultRole, true, true)
}

func resolveAgentForStop(bot *Orchestrator, parts []string, defaultRole AgentRole) (Agent, int, error) {
	return resolveIssueScopedAgentForCommand(bot, parts, defaultRole, false, true)
}

func resolveIssueScopedAgentForCommand(bot *Orchestrator, parts []string, defaultRole AgentRole, includeStopped bool, allowReviewPR bool) (Agent, int, error) {
	if bot == nil || bot.agents == nil {
		return Agent{}, 0, errors.New("agent manager is not configured")
	}
	if len(parts) == 0 {
		return Agent{}, 0, issueSelectorParseError{message: "missing issue selector"}
	}

	role := defaultRole
	roleExplicit := false
	selectorToken := parts[0]
	consumed := 1
	if resolvedRole, err := resolveAgentRoleToken(parts[0]); err == nil {
		if len(parts) < 2 {
			return Agent{}, 0, issueSelectorParseError{message: "missing issue number"}
		}
		role = resolvedRole
		roleExplicit = true
		selectorToken = parts[1]
		consumed = 2
	} else {
		switch err.(type) {
		case ambiguousCommandError:
			return Agent{}, 0, err
		case unknownCommandError:
			// Treat as an issue number token.
		default:
			return Agent{}, 0, err
		}
	}

	selectorNumber, err := strconv.Atoi(selectorToken)
	if err != nil || selectorNumber <= 0 {
		return Agent{}, 0, issueSelectorParseError{message: "invalid issue or PR number"}
	}
	var agent Agent
	if includeStopped {
		agent, err = bot.agents.ResolveIssueAgentForCleanup(selectorNumber, role)
	} else {
		agent, err = bot.agents.ResolveActiveIssueAgent(selectorNumber, role)
	}
	issueErr := err
	var notFound *agentSelectorNotFoundError
	if err != nil && allowReviewPR && (!roleExplicit || role == RoleReviewer) && errors.As(err, &notFound) {
		if includeStopped {
			agent, err = bot.agents.ResolveReviewAgentForCleanup(selectorNumber)
		} else {
			agent, err = bot.agents.ResolveActiveReviewAgent(selectorNumber)
		}
		if err != nil {
			err = issueErr
		}
	}
	if err != nil {
		return Agent{}, 0, err
	}
	return agent, consumed, nil
}

func formatAgentControlTarget(agent Agent) string {
	if normalizedAgentRole(agent.Role) == RoleReviewer && agent.PRNumber > 0 {
		if agent.IssueNumber > 0 {
			return fmt.Sprintf("issue #%d (PR #%d)", agent.IssueNumber, agent.PRNumber)
		}
		return fmt.Sprintf("PR #%d", agent.PRNumber)
	}
	return fmt.Sprintf("issue #%d", agent.IssueNumber)
}

func deferredAgentCleanupCommand(agent Agent) string {
	if normalizedAgentRole(agent.Role) == RoleReviewer && agent.PRNumber > 0 {
		return fmt.Sprintf("agent cleanup reviewer %d", agent.PRNumber)
	}
	return fmt.Sprintf("agent cleanup %s %d", selectorRoleLabel(agent.Role), agent.IssueNumber)
}

func promptCleanupAfterStop(lineReader replLineReader, agent Agent) (bool, error) {
	if lineReader == nil {
		return false, errors.New("REPL line reader is not configured")
	}

	cleanupEffect := "remove its worktree and runtime artifacts"
	if agent.Role != RoleReviewer {
		cleanupEffect = "close any open PR and remove its worktree, branches, and runtime artifacts"
	}
	prompt := fmt.Sprintf(
		"Clean up %s for %s now (%s)? [y/N] ",
		selectorRoleLabel(agent.Role),
		formatAgentControlTarget(agent),
		cleanupEffect,
	)
	for {
		answer, err := lineReader.ReadLine(prompt)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, errREPLInterrupted) {
				return false, nil
			}
			return false, err
		}
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "y", "yes":
			return true, nil
		case "", "n", "no":
			return false, nil
		default:
			replPrintln("Please answer yes or no.")
		}
	}
}

func resolveCommandToken(token string, options []commandOption) (string, error) {
	token = strings.ToLower(strings.TrimSpace(token))
	if token == "" {
		return "", unknownCommandError{token: token}
	}

	for _, option := range options {
		if token == option.name {
			return option.canonical, nil
		}
	}

	matchSet := make(map[string]struct{})
	for _, option := range options {
		if strings.HasPrefix(option.name, token) {
			matchSet[option.canonical] = struct{}{}
		}
	}
	if len(matchSet) == 0 {
		return "", unknownCommandError{token: token}
	}
	if len(matchSet) == 1 {
		for match := range matchSet {
			return match, nil
		}
	}

	matches := make([]string, 0, len(matchSet))
	for match := range matchSet {
		matches = append(matches, match)
	}
	sort.Strings(matches)
	return "", ambiguousCommandError{
		token:   token,
		matches: matches,
	}
}

func runREPL(ctx context.Context, bot *Orchestrator, cancel context.CancelFunc) error {
	lineReader, err := newREPLLineReader(os.Stdin, os.Stdout)
	if err != nil {
		return err
	}
	defer lineReader.Close()

	return runREPLWithLineReader(ctx, bot, cancel, lineReader)
}

func runREPLWithLineReader(ctx context.Context, bot *Orchestrator, cancel context.CancelFunc, lineReader replLineReader) error {
	printHelp()

	for {
		line, err := lineReader.ReadLine("repository-agent-orchestrator> ")
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, errREPLInterrupted) {
				cancel()
				return nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)

		command, err := resolveCommandToken(parts[0], replRootCommandOptions)
		if err != nil {
			ambiguous, ok := err.(ambiguousCommandError)
			if ok {
				replPrintf("ambiguous command %q; matches: %s\n", ambiguous.token, strings.Join(ambiguous.matches, ", "))
				continue
			}
			replPrintln("unknown command; type 'help'")
			continue
		}

		switch command {
		case "help":
			printHelp()
		case "agent":
			if len(parts) < 2 {
				replPrintln("usage: agent <help|cleanup|continue|index|list|pause|review|start|steer|stop|tail|unpause> ...")
				continue
			}
			agentCommand, err := resolveCommandToken(parts[1], replAgentCommandOptions)
			if err != nil {
				ambiguous, ok := err.(ambiguousCommandError)
				if ok {
					replPrintf("ambiguous agent command %q; matches: %s\n", ambiguous.token, strings.Join(ambiguous.matches, ", "))
					continue
				}
				replPrintln("usage: agent <help|cleanup|continue|index|list|pause|review|start|steer|stop|tail|unpause> ...")
				continue
			}
			switch agentCommand {
			case "help":
				if len(parts) != 2 {
					replPrintln("usage: agent help")
					continue
				}
				printAgentHelp()
			case "cleanup":
				agent, consumed, err := resolveIssueScopedAgentForCleanup(bot, parts[2:], RoleCoder)
				if err != nil {
					if _, ok := err.(issueSelectorParseError); ok {
						replPrintln("usage: agent cleanup [coder|reviewer] <issueOrPRNumber>")
						continue
					}
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				if len(parts) != 2+consumed {
					replPrintln("usage: agent cleanup [coder|reviewer] <issueOrPRNumber>")
					continue
				}
				if err := bot.CleanupAgent(ctx, agent.ID); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("%s cleaned up for %s\n", selectorRoleLabel(agent.Role), formatAgentControlTarget(agent))
			case "continue":
				if len(parts) != 4 {
					replPrintln("usage: agent continue <issueNumber> <prNumber>")
					continue
				}
				issueNumber, issueErr := strconv.Atoi(parts[2])
				prNumber, prErr := strconv.Atoi(parts[3])
				if issueErr != nil || issueNumber <= 0 || prErr != nil || prNumber <= 0 {
					replPrintln("usage: agent continue <issueNumber> <prNumber>")
					continue
				}
				if err := bot.ContinueAgentForIssuePR(ctx, issueNumber, prNumber); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("agent continued for issue #%d on PR #%d\n", issueNumber, prNumber)
			case "index":
				if len(parts) != 3 || !strings.EqualFold(parts[2], "repo") {
					replPrintln("usage: agent index repo")
					continue
				}
				agentID, err := bot.LaunchRepoIndexAgent(ctx)
				if err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("repo indexing agent started: %s\n", agentID)
			case "list":
				if len(parts) != 2 {
					replPrintln("usage: agent list")
					continue
				}
				printAgents(bot)
			case "tail":
				switch len(parts) {
				case 2:
					agents := tailableAgents(bot)
					if len(agents) == 0 {
						replPrintln("No agents with runtime logs.")
						continue
					}
					selector, ok := lineReader.(replAgentSelector)
					if !ok {
						replPrintln("agent tail without an id requires an interactive terminal")
						continue
					}
					selected, err := selector.SelectAgent("Select an agent log to tail:", agents)
					if err != nil {
						if errors.Is(err, errREPLInterrupted) || errors.Is(err, io.EOF) {
							continue
						}
						replPrintf("error: %s\n", bot.safeError(err))
						continue
					}
					if err := bot.TailAgentLog(selected.ID); err != nil {
						replPrintf("error: %s\n", bot.safeError(err))
					}
				default:
					agent, consumed, err := resolveIssueScopedAgent(bot, parts[2:], RoleCoder)
					if err != nil {
						if _, ok := err.(issueSelectorParseError); ok {
							replPrintln("usage: agent tail")
							replPrintln("usage: agent tail [coder|reviewer] <issueNumber>")
							continue
						}
						replPrintf("error: %s\n", bot.safeError(err))
						continue
					}
					if len(parts) != 2+consumed {
						replPrintln("usage: agent tail")
						replPrintln("usage: agent tail [coder|reviewer] <issueNumber>")
						continue
					}
					if err := bot.TailAgentLog(agent.ID); err != nil {
						replPrintf("error: %s\n", bot.safeError(err))
						continue
					}
				}
			case "start":
				if len(parts) != 3 {
					replPrintln("usage: agent start <issueNumber>")
					replPrintln("usage: agent start next")
					continue
				}
				if parts[2] == "next" {
					issueNumber, title, err := bot.FindNextOpenIssue(ctx)
					if err != nil {
						replPrintf("error: %s\n", bot.safeError(err))
						continue
					}
					replPrintf("selected issue #%d: %s\n", issueNumber, title)
					if err := bot.InitAgentForIssue(ctx, issueNumber); err != nil {
						replPrintf("error: %s\n", bot.safeError(err))
						continue
					}
					replPrintf("agent started for issue #%d\n", issueNumber)
					continue
				}
				issueNum, err := strconv.Atoi(parts[2])
				if err != nil || issueNum <= 0 {
					replPrintln("usage: agent start <issueNumber>")
					continue
				}
				if err := bot.InitAgentForIssue(ctx, issueNum); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("agent started for issue #%d\n", issueNum)
			case "review":
				if len(parts) != 3 {
					replPrintln("usage: agent review <prNumber>")
					continue
				}
				prNumber, err := strconv.Atoi(parts[2])
				if err != nil || prNumber <= 0 {
					replPrintln("usage: agent review <prNumber>")
					continue
				}
				if err := bot.LaunchReviewAgent(ctx, prNumber); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("review agent launch started for PR #%d (hard gate and coordinator startup continue in the background; check `status` or notifications for progress)\n", prNumber)
			case "pause":
				agent, consumed, err := resolveIssueScopedAgent(bot, parts[2:], RoleCoder)
				if err != nil {
					if _, ok := err.(issueSelectorParseError); ok {
						replPrintln("usage: agent pause [coder|reviewer] <issueNumber>")
						continue
					}
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				if len(parts) != 2+consumed {
					replPrintln("usage: agent pause [coder|reviewer] <issueNumber>")
					continue
				}
				if err := bot.PauseAgent(ctx, agent.ID); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("%s paused for issue #%d\n", selectorRoleLabel(agent.Role), agent.IssueNumber)
			case "unpause":
				agent, consumed, err := resolveIssueScopedAgent(bot, parts[2:], RoleCoder)
				if err != nil {
					if _, ok := err.(issueSelectorParseError); ok {
						replPrintln("usage: agent unpause [coder|reviewer] <issueNumber>")
						continue
					}
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				if len(parts) != 2+consumed {
					replPrintln("usage: agent unpause [coder|reviewer] <issueNumber>")
					continue
				}
				if err := bot.UnpauseAgent(ctx, agent.ID); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("%s unpaused for issue #%d\n", selectorRoleLabel(agent.Role), agent.IssueNumber)
			case "stop":
				agent, consumed, err := resolveAgentForStop(bot, parts[2:], RoleCoder)
				if err != nil {
					if _, ok := err.(issueSelectorParseError); ok {
						replPrintln("usage: agent stop [coder|reviewer] <issueOrPRNumber>")
						continue
					}
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				if len(parts) != 2+consumed {
					replPrintln("usage: agent stop [coder|reviewer] <issueOrPRNumber>")
					continue
				}
				if err := bot.StopAgent(ctx, agent.ID); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("%s stopped for %s\n", selectorRoleLabel(agent.Role), formatAgentControlTarget(agent))
				cleanup, err := promptCleanupAfterStop(lineReader, agent)
				if err != nil {
					return err
				}
				if !cleanup {
					replPrintf(
						"cleanup skipped; run `%s` later\n",
						deferredAgentCleanupCommand(agent),
					)
					continue
				}
				if err := bot.CleanupAgent(ctx, agent.ID); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("%s cleaned up for %s\n", selectorRoleLabel(agent.Role), formatAgentControlTarget(agent))
			case "steer":
				agent, consumed, err := resolveIssueScopedAgent(bot, parts[2:], RoleCoder)
				if err != nil {
					if _, ok := err.(issueSelectorParseError); ok {
						replPrintln("usage: agent steer [coder|reviewer] <issueNumber> <text...>")
						continue
					}
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				if len(parts) < 2+consumed+1 {
					replPrintln("usage: agent steer [coder|reviewer] <issueNumber> <text...>")
					continue
				}
				message := strings.TrimSpace(strings.Join(parts[2+consumed:], " "))
				if message == "" {
					replPrintln("usage: agent steer [coder|reviewer] <issueNumber> <text...>")
					continue
				}
				if err := bot.SteerAgent(ctx, agent.ID, message); err != nil {
					replPrintf("error: %s\n", bot.safeError(err))
					continue
				}
				replPrintf("message sent to %s for issue #%d\n", selectorRoleLabel(agent.Role), agent.IssueNumber)
			default:
				replPrintln("usage: agent <help|cleanup|continue|index|list|pause|review|start|steer|stop|tail|unpause> ...")
			}
		case "status":
			printStatus(bot)
		case "tech-support":
			bundlePath, secretScan, err := bot.generateTechSupportBundle()
			if err != nil {
				replPrintf("error: %s\n", bot.safeError(err))
				continue
			}
			replPrintf("tech-support bundle written to %s\n", bundlePath)
			replPrintf("%s", formatTechSupportSecretScanWarning(secretScan))
		case "quit", "exit":
			cancel()
			return nil
		default:
			replPrintln("unknown command; type 'help'")
		}
	}
}

func setupProcessLogger(repoLogDir string) (*os.File, error) {
	repoLogDir = strings.TrimSpace(repoLogDir)
	if repoLogDir == "" {
		return nil, errors.New("repo log directory is empty")
	}
	if err := os.MkdirAll(repoLogDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %q: %w", repoLogDir, err)
	}
	logPath := filepath.Join(repoLogDir, orchestratorDaemonLogName)
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %q: %w", logPath, err)
	}
	log.SetOutput(f)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	return f, nil
}

type cliOptions struct {
	configPath             string
	showHelp               bool
	showVersion            bool
	cleanStart             bool
	runtimeProfileOverride agentProfileOverride
}

func parseCLIOptions(args []string) (cliOptions, error) {
	opts := cliOptions{}
	for i := 1; i < len(args); i++ {
		token := strings.TrimSpace(args[i])
		switch {
		case token == "--help" || token == "-h":
			opts.showHelp = true
		case token == "--version":
			opts.showVersion = true
		case token == "--clean":
			opts.cleanStart = true
		case token == "--config":
			if i+1 >= len(args) {
				return cliOptions{}, errors.New("missing value for --config")
			}
			i++
			opts.configPath = strings.TrimSpace(args[i])
		case strings.HasPrefix(token, "--config="):
			opts.configPath = strings.TrimSpace(strings.TrimPrefix(token, "--config="))
		case token == "--model" || strings.HasPrefix(token, "--model="):
			if opts.runtimeProfileOverride.HasModel {
				return cliOptions{}, errors.New("--model may only be specified once")
			}
			value, next, err := cliOptionValue(args, i, "--model")
			if err != nil {
				return cliOptions{}, err
			}
			i = next
			opts.runtimeProfileOverride.Model = value
			opts.runtimeProfileOverride.HasModel = true
		case token == "--reasoning-effort" || strings.HasPrefix(token, "--reasoning-effort="):
			if opts.runtimeProfileOverride.HasReasoningEffort {
				return cliOptions{}, errors.New("--reasoning-effort may only be specified once")
			}
			value, next, err := cliOptionValue(args, i, "--reasoning-effort")
			if err != nil {
				return cliOptions{}, err
			}
			i = next
			opts.runtimeProfileOverride.ReasoningEffort = value
			opts.runtimeProfileOverride.HasReasoningEffort = true
		default:
			return cliOptions{}, fmt.Errorf("unknown argument %q", token)
		}
	}
	if opts.showHelp || opts.showVersion {
		return opts, nil
	}
	if strings.TrimSpace(opts.configPath) == "" {
		return cliOptions{}, errors.New("missing required --config <path> argument")
	}
	return opts, nil
}

func cliOptionValue(args []string, index int, name string) (string, int, error) {
	token := strings.TrimSpace(args[index])
	if token == name {
		if index+1 >= len(args) {
			return "", index, fmt.Errorf("missing value for %s", name)
		}
		index++
		value := strings.TrimSpace(args[index])
		if value == "" {
			return "", index, fmt.Errorf("missing value for %s", name)
		}
		return value, index, nil
	}
	value := strings.TrimSpace(strings.TrimPrefix(token, name+"="))
	if value == "" {
		return "", index, fmt.Errorf("missing value for %s", name)
	}
	return value, index, nil
}

func printCLIUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: repository-agent-orchestrator --config <path> [--clean] [--model <model>] [--reasoning-effort <effort>]")
	fmt.Fprintln(w, "       repository-agent-orchestrator --version")
}

func Run(parentCtx context.Context, args []string) int {
	if len(args) > 1 && strings.TrimSpace(args[1]) == runtimeLogSanitizerSubcommand {
		return runRuntimeLogSanitizer(args[2:])
	}

	signalCtx, stopSignals := signal.NotifyContext(parentCtx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	options, err := parseCLIOptions(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "argument error: %s\n", err.Error())
		printCLIUsage(os.Stderr)
		return 1
	}
	if options.showHelp {
		printCLIUsage(os.Stdout)
		return 0
	}
	if options.showVersion {
		fmt.Fprintln(os.Stdout, buildVersionString())
		return 0
	}

	cfg, err := loadConfigWithRuntimeProfileOverride(options.configPath, options.runtimeProfileOverride)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %s\n", err.Error())
		return 1
	}

	if err := cfg.ValidateRuntime(signalCtx); err != nil {
		fmt.Fprintf(os.Stderr, "runtime validation failed: %s\n", err.Error())
		return 1
	}
	if options.cleanStart {
		if err := cleanStartupState(signalCtx, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "startup cleanup failed: %s\n", err.Error())
			return 1
		}
	}

	lockPath := filepath.Join(cfg.LogDir, orchestratorLockFileName)
	processLock, err := acquireProcessLock(lockPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "startup lock failed: repo=%s lock=%s error=%s\n", cfg.RepoName, lockPath, err.Error())
		return 1
	}
	defer func() {
		if err := processLock.Release(); err != nil {
			log.Printf("failed to release process lock: %s", err.Error())
		}
	}()

	logFile, err := setupProcessLogger(cfg.LogDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log initialization failed: %s\n", err.Error())
		return 1
	}
	defer logFile.Close()
	log.Print(buildVersionString())
	logEffectiveReviewPolicy(cfg.ReviewPolicy)
	for _, warning := range analyzeReviewPolicyBudget(cfg.ReviewPolicy) {
		log.Printf("review policy budget warning: %s", warning)
	}
	if err := killLegacyOrchestratorTmuxSessions(cfg); err != nil {
		fmt.Fprintf(
			os.Stderr,
			"legacy runtime quarantine failed: repo=%s error=%s\n",
			cfg.RepoName,
			err.Error(),
		)
		return 1
	}

	bot, err := NewOrchestrator(signalCtx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator initialization failed: %s\n", err.Error())
		return 1
	}
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	bot.notify(ctx, fmt.Sprintf("Repository Agent Orchestrator started for `%s/%s` (repo path `%s`)", cfg.RepoOwner, cfg.RepoName, cfg.RepoPath))
	log.Printf("orchestrator started repo=%s/%s repoPath=%s", cfg.RepoOwner, cfg.RepoName, cfg.RepoPath)

	var wg sync.WaitGroup
	wg.Add(1)
	go bot.PollLoop(ctx, &wg)

	lineReader, lineReaderErr := newREPLLineReader(os.Stdin, os.Stdout)
	if lineReaderErr != nil {
		log.Printf("repl exited with error: %s", bot.safeError(lineReaderErr))
	} else {
		defer lineReader.Close()

		replDone := make(chan error, 1)
		go func() {
			replDone <- runREPLWithLineReader(ctx, bot, cancel, lineReader)
		}()

		select {
		case err := <-replDone:
			if err != nil {
				log.Printf("repl exited with error: %s", bot.safeError(err))
			}
		case <-ctx.Done():
			_ = lineReader.Close()
		}
	}

	cancel()
	wg.Wait()
	bot.Shutdown(ctx)
	log.Println("orchestrator exited")
	return 0
}
