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
	"os"
	"sort"
	"strconv"
	"strings"
)

const agentProfileEnvironmentPrefix = "RAO_PROFILE_"

var supportedReasoningEfforts = map[string]struct{}{
	"none":    {},
	"minimal": {},
	"low":     {},
	"medium":  {},
	"high":    {},
	"xhigh":   {},
	"max":     {},
	"ultra":   {},
}

type ModelCapability struct {
	ReasoningEfforts []string `json:"reasoning_efforts"`
	Available        bool     `json:"available"`
}

type agentProfileOverride struct {
	Model              string
	ReasoningEffort    string
	HasModel           bool
	HasReasoningEffort bool
}

func builtInAgentProfile(role AgentProfileRole) AgentProfile {
	switch role {
	case AgentProfileRoleCoder:
		return AgentProfile{Name: string(role), Model: "gpt-5.6-sol", ReasoningEffort: "high"}
	case AgentProfileRoleIndexer:
		return AgentProfile{Name: string(role), Model: "gpt-5.6-luna", ReasoningEffort: "medium"}
	case AgentProfileRoleDiscovery:
		return AgentProfile{Name: string(role), Model: "gpt-5.6-terra", ReasoningEffort: "high"}
	case AgentProfileRoleVerifier:
		return AgentProfile{Name: string(role), Model: "gpt-5.6-terra", ReasoningEffort: "high"}
	case AgentProfileRoleChallenge:
		return AgentProfile{Name: string(role), Model: "gpt-5.6-sol", ReasoningEffort: "xhigh"}
	case AgentProfileRoleEscalation:
		return AgentProfile{Name: string(role), Model: "gpt-5.6-sol", ReasoningEffort: "max"}
	default:
		return AgentProfile{}
	}
}

func defaultModelCatalog() map[string]ModelCapability {
	efforts := []string{"none", "low", "medium", "high", "xhigh", "max"}
	return map[string]ModelCapability{
		"gpt-5.6-sol":   {ReasoningEfforts: append([]string(nil), efforts...), Available: true},
		"gpt-5.6-terra": {ReasoningEfforts: append([]string(nil), efforts...), Available: true},
		"gpt-5.6-luna":  {ReasoningEfforts: append([]string(nil), efforts...), Available: true},
	}
}

func normalizeModelCatalog(raw map[string]modelCapabilityFile, defaults map[string]ModelCapability) (map[string]ModelCapability, error) {
	catalog := make(map[string]ModelCapability, len(defaults)+len(raw))
	for model, capability := range defaults {
		catalog[model] = ModelCapability{
			ReasoningEfforts: append([]string(nil), capability.ReasoningEfforts...),
			Available:        capability.Available,
		}
	}
	if raw == nil {
		return catalog, nil
	}

	models := make([]string, 0, len(raw))
	for model := range raw {
		models = append(models, model)
	}
	sort.Strings(models)
	normalizedModels := make(map[string]string, len(models))
	for _, rawModel := range models {
		model := strings.TrimSpace(rawModel)
		if model == "" {
			return nil, fmt.Errorf("REVIEW_POLICY.MODEL_CATALOG contains an empty model name")
		}
		if previous, ok := normalizedModels[model]; ok {
			return nil, fmt.Errorf(
				"REVIEW_POLICY.MODEL_CATALOG contains duplicate model %q after trimming whitespace from %q and %q",
				model,
				previous,
				rawModel,
			)
		}
		normalizedModels[model] = rawModel
		if !safeCodexModelName(model) {
			return nil, fmt.Errorf("REVIEW_POLICY.MODEL_CATALOG model %q is not a safe Codex model or alias", model)
		}
		efforts, err := normalizeReasoningEffortList(raw[rawModel].ReasoningEfforts)
		if err != nil {
			return nil, fmt.Errorf("REVIEW_POLICY.MODEL_CATALOG.%s.REASONING_EFFORTS: %w", model, err)
		}
		if len(efforts) == 0 {
			return nil, fmt.Errorf("REVIEW_POLICY.MODEL_CATALOG.%s.REASONING_EFFORTS must not be empty", model)
		}
		available := true
		if raw[rawModel].Available != nil {
			available = *raw[rawModel].Available
		}
		catalog[model] = ModelCapability{ReasoningEfforts: efforts, Available: available}
	}
	return catalog, nil
}

func normalizeReasoningEffortList(raw []string) ([]string, error) {
	seen := make(map[string]struct{}, len(raw))
	efforts := make([]string, 0, len(raw))
	for _, value := range raw {
		effort := normalizeReasoningEffort(value)
		if _, ok := supportedReasoningEfforts[effort]; !ok {
			return nil, fmt.Errorf("unsupported reasoning effort %q", value)
		}
		if _, ok := seen[effort]; ok {
			continue
		}
		seen[effort] = struct{}{}
		efforts = append(efforts, effort)
	}
	sort.Strings(efforts)
	return efforts, nil
}

func applyAgentProfileEnvironmentOverrides(policy *ReviewPolicy) error {
	if policy == nil {
		return nil
	}
	names := make([]string, 0, len(policy.AgentProfiles))
	for name := range policy.AgentProfiles {
		names = append(names, name)
	}
	sort.Strings(names)

	environmentNames := make(map[string]string, len(names))
	for _, name := range names {
		environmentName := sanitizeAgentProfileEnvironmentName(name)
		prefix := agentProfileEnvironmentPrefix + environmentName
		model, hasModel := os.LookupEnv(prefix + "_MODEL")
		effort, hasEffort := os.LookupEnv(prefix + "_REASONING_EFFORT")
		inheritRaw, hasInherit := os.LookupEnv(prefix + "_INHERIT_GLOBAL")
		if !hasModel && !hasEffort && !hasInherit {
			continue
		}
		if previous, ok := environmentNames[environmentName]; ok {
			return fmt.Errorf(
				"deployment profile override %s matches both profiles %q and %q; rename one profile",
				prefix,
				previous,
				name,
			)
		}
		environmentNames[environmentName] = name

		profile := policy.AgentProfiles[name]
		if hasInherit {
			inherit, err := strconv.ParseBool(strings.TrimSpace(inheritRaw))
			if err != nil {
				return fmt.Errorf("%s_INHERIT_GLOBAL must be a boolean", prefix)
			}
			if inherit && (hasModel || hasEffort) {
				return fmt.Errorf(
					"%s_INHERIT_GLOBAL cannot be true with deployment MODEL or REASONING_EFFORT overrides",
					prefix,
				)
			}
			profile.InheritGlobal = inherit
			if inherit {
				profile.Model = ""
				profile.ReasoningEffort = ""
			}
		}
		if hasModel {
			profile.Model = strings.TrimSpace(model)
			profile.InheritGlobal = false
		}
		if hasEffort {
			profile.ReasoningEffort = normalizeReasoningEffort(effort)
			profile.InheritGlobal = false
		}
		policy.AgentProfiles[name] = profile
	}
	return nil
}

func sanitizeAgentProfileEnvironmentName(value string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(strings.TrimSpace(value)) {
		switch {
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-':
			b.WriteRune('_')
		}
	}
	return b.String()
}

func validateAgentProfiles(policy ReviewPolicy) error {
	selectedRepositoryProfiles := make(map[string]struct{}, len(requiredAgentProfileRoles))
	for _, role := range requiredAgentProfileRoles {
		profile, err := policy.effectiveProfileForRole(role)
		if err != nil {
			return err
		}
		if _, repositoryProfile := policy.repositoryProfileNames[profile.Name]; repositoryProfile {
			selectedRepositoryProfiles[profile.Name] = struct{}{}
		}
	}

	names := make([]string, 0, len(policy.repositoryProfileNames))
	for name := range policy.repositoryProfileNames {
		if _, selected := selectedRepositoryProfiles[name]; selected {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateAgentProfile(policy.AgentProfiles[name], policy.activeModelCatalog()); err != nil {
			return fmt.Errorf("REVIEW_POLICY.AGENT_PROFILES.%s: %w", name, err)
		}
	}
	return nil
}

func validateConvergentReviewWorkerProfiles(policy ReviewPolicy) error {
	requireExplicit := func(label string, profile AgentProfile) error {
		if !profile.InheritGlobal {
			return nil
		}
		return fmt.Errorf(
			"%s selects profile %q with INHERIT_GLOBAL; convergent review workers require explicit effective MODEL and REASONING_EFFORT values",
			label,
			profile.Name,
		)
	}
	for _, role := range []AgentProfileRole{
		AgentProfileRoleDiscovery,
		AgentProfileRoleVerifier,
		AgentProfileRoleChallenge,
		AgentProfileRoleEscalation,
	} {
		profile, err := policy.effectiveProfileForRole(role)
		if err != nil {
			return err
		}
		if err := requireExplicit(
			fmt.Sprintf(
				"REVIEW_POLICY.ROLE_PROFILES.%s",
				strings.ToUpper(string(role)),
			),
			profile,
		); err != nil {
			return err
		}
	}
	for _, lane := range policy.Swarm.Lanes {
		profile, err := policy.effectiveNamedProfileForReviewWorker(
			strings.TrimSpace(lane.Profile),
			fmt.Sprintf("discovery lane %q", lane.Name),
		)
		if err != nil {
			return err
		}
		if err := requireExplicit(
			fmt.Sprintf(
				"REVIEW_POLICY.REVIEW_SWARM lane %q",
				lane.Name,
			),
			profile,
		); err != nil {
			return err
		}
	}
	profile, err := policy.effectiveNamedProfileForReviewWorker(
		strings.TrimSpace(policy.Escalation.Profile),
		"review escalation",
	)
	if err != nil {
		return err
	}
	return requireExplicit(
		"REVIEW_POLICY.ESCALATION.PROFILE",
		profile,
	)
}

func applyAgentProfileOverride(profile AgentProfile, override agentProfileOverride) (AgentProfile, error) {
	if !override.HasModel && !override.HasReasoningEffort {
		return profile, nil
	}
	if override.HasModel && strings.TrimSpace(override.Model) == "" {
		return AgentProfile{}, fmt.Errorf("CLI --model must not be empty")
	}
	if override.HasReasoningEffort && normalizeReasoningEffort(override.ReasoningEffort) == "" {
		return AgentProfile{}, fmt.Errorf("CLI --reasoning-effort must not be empty")
	}
	if profile.InheritGlobal && (!override.HasModel || !override.HasReasoningEffort) {
		return AgentProfile{}, fmt.Errorf(
			"selected profile %q inherits global Codex settings; set both CLI --model and --reasoning-effort to override it",
			profile.Name,
		)
	}
	if override.HasModel {
		profile.Model = strings.TrimSpace(override.Model)
		profile.InheritGlobal = false
	}
	if override.HasReasoningEffort {
		profile.ReasoningEffort = normalizeReasoningEffort(override.ReasoningEffort)
		profile.InheritGlobal = false
	}
	return profile, nil
}

func validateAgentProfile(profile AgentProfile, catalog map[string]ModelCapability) error {
	if profile.InheritGlobal {
		if strings.TrimSpace(profile.Model) != "" || strings.TrimSpace(profile.ReasoningEffort) != "" {
			return fmt.Errorf("inherited profile must not set MODEL or REASONING_EFFORT")
		}
		return nil
	}
	model := strings.TrimSpace(profile.Model)
	effort := normalizeReasoningEffort(profile.ReasoningEffort)
	if model == "" || effort == "" {
		return fmt.Errorf("MODEL and REASONING_EFFORT are required unless INHERIT_GLOBAL is true")
	}
	if !safeCodexModelName(model) {
		return fmt.Errorf("model %q is not a safe Codex model or alias", model)
	}
	capability, ok := catalog[model]
	if !ok {
		return fmt.Errorf(
			"model %q is not present in REVIEW_POLICY.MODEL_CATALOG; add the model and its supported REASONING_EFFORTS",
			model,
		)
	}
	if !capability.Available {
		return fmt.Errorf("model %q is configured as unavailable", model)
	}
	for _, supported := range capability.ReasoningEfforts {
		if effort == supported {
			return nil
		}
	}
	return fmt.Errorf(
		"model %q does not support reasoning effort %q; supported efforts: %s",
		model,
		effort,
		strings.Join(capability.ReasoningEfforts, ", "),
	)
}

func (p ReviewPolicy) effectiveProfileForRole(role AgentProfileRole) (AgentProfile, error) {
	if !isRequiredAgentProfileRole(role) {
		return AgentProfile{}, fmt.Errorf("unknown agent profile role %q", role)
	}
	var profile AgentProfile
	if len(p.RoleProfiles) == 0 && len(p.AgentProfiles) == 0 {
		profile = builtInAgentProfile(role)
	} else {
		var err error
		profile, err = p.ProfileForRole(role)
		if err != nil {
			return AgentProfile{}, err
		}
	}
	profile, err := applyAgentProfileOverride(profile, p.runtimeProfileCLIOverride)
	if err != nil {
		return AgentProfile{}, fmt.Errorf("agent profile role %q: %w", role, err)
	}
	if err := validateAgentProfile(profile, p.activeModelCatalog()); err != nil {
		return AgentProfile{}, fmt.Errorf("agent profile role %q selected invalid profile %q: %w", role, profile.Name, err)
	}
	return profile, nil
}

func (p ReviewPolicy) activeModelCatalog() map[string]ModelCapability {
	if len(p.ModelCatalog) == 0 {
		return defaultModelCatalog()
	}
	return p.ModelCatalog
}

func (c Config) runtimeProfileForRole(role AgentProfileRole) (AgentProfile, error) {
	return c.ReviewPolicy.effectiveProfileForRole(role)
}

func isRequiredAgentProfileRole(role AgentProfileRole) bool {
	for _, required := range requiredAgentProfileRoles {
		if role == required {
			return true
		}
	}
	return false
}

func normalizeReasoningEffort(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func safeCodexModelName(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if i == 0 && !isASCIIAlphaNumeric(r) {
			return false
		}
		if isASCIIAlphaNumeric(r) || r == '.' || r == '_' || r == ':' || r == '/' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func isASCIIAlphaNumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
