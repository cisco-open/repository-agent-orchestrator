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
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

const inheritedGlobalPolicyValue = "inherited-global"

type reviewPolicyMetadata struct {
	State        string                           `json:"state"`
	Version      int                              `json:"version"`
	Fingerprint  string                           `json:"fingerprint"`
	Roles        []reviewPolicyRoleMetadata       `json:"roles"`
	Swarm        reviewPolicySwarmMetadata        `json:"swarm"`
	Lanes        []reviewPolicyLaneMetadata       `json:"lanes"`
	Artifacts    reviewPolicyArtifactMetadata     `json:"artifacts"`
	Verification reviewPolicyVerificationMetadata `json:"verification"`
	Convergence  reviewPolicyConvergenceMetadata  `json:"convergence"`
	Escalation   reviewPolicyEscalationMetadata   `json:"escalation"`
}

type reviewPolicyRoleMetadata struct {
	Role            AgentProfileRole `json:"role"`
	Profile         string           `json:"profile"`
	Model           string           `json:"model"`
	ReasoningEffort string           `json:"reasoning_effort"`
	InheritGlobal   bool             `json:"inherit_global"`
}

type reviewPolicySwarmMetadata struct {
	MinReviewers         int `json:"min_reviewers"`
	MaxReviewers         int `json:"max_reviewers"`
	MaxParallelReviewers int `json:"max_parallel_reviewers"`
	TimeoutMinutes       int `json:"timeout_minutes"`
	Retries              int `json:"retries"`
}

type reviewPolicyLaneMetadata struct {
	Name            string `json:"name"`
	Required        bool   `json:"required"`
	Profile         string `json:"profile"`
	Model           string `json:"model"`
	ReasoningEffort string `json:"reasoning_effort"`
	InheritGlobal   bool   `json:"inherit_global"`
}

type reviewPolicyArtifactMetadata struct {
	MaxBytes       int `json:"max_bytes"`
	TimeoutSeconds int `json:"timeout_seconds"`
	Retries        int `json:"retries"`
}

type reviewPolicyVerificationMetadata struct {
	MinVerifiers         int `json:"min_verifiers"`
	MaxVerifiers         int `json:"max_verifiers"`
	MaxParallelVerifiers int `json:"max_parallel_verifiers"`
	TimeoutMinutes       int `json:"timeout_minutes"`
	Retries              int `json:"retries"`
}

type reviewPolicyConvergenceMetadata struct {
	QuietRoundsRequired   int `json:"quiet_rounds_required"`
	MaxRounds             int `json:"max_rounds"`
	MaxReviewAgentsPerSHA int `json:"max_review_agents_per_sha"`
	MaxWallTimeMinutes    int `json:"max_wall_time_minutes"`
	MaxUsageTokens        int `json:"max_usage_tokens"`
}

type reviewPolicyEscalationMetadata struct {
	AfterNonConvergingRounds int    `json:"after_non_converging_rounds"`
	AfterCorrectionRounds    int    `json:"after_correction_rounds"`
	Profile                  string `json:"profile"`
}

type reviewPolicyLogRecord struct {
	Timestamp    time.Time            `json:"timestamp"`
	Event        string               `json:"event"`
	ReviewPolicy reviewPolicyMetadata `json:"review_policy"`
}

// safeReviewPolicyMetadata is an explicit allowlist. Keep credentials, command
// strings, environment values, and unused profile or catalog entries out of it.
func safeReviewPolicyMetadata(policy ReviewPolicy) reviewPolicyMetadata {
	roles := make([]reviewPolicyRoleMetadata, 0, len(requiredAgentProfileRoles))
	for _, role := range requiredAgentProfileRoles {
		profile, err := policy.effectiveProfileForRole(role)
		if err != nil {
			roles = append(roles, reviewPolicyRoleMetadata{
				Role:            role,
				Profile:         "unavailable",
				Model:           "unavailable",
				ReasoningEffort: "unavailable",
			})
			continue
		}
		model := profile.Model
		effort := profile.ReasoningEffort
		if profile.InheritGlobal {
			model = inheritedGlobalPolicyValue
			effort = inheritedGlobalPolicyValue
		}
		roles = append(roles, reviewPolicyRoleMetadata{
			Role:            role,
			Profile:         profile.Name,
			Model:           model,
			ReasoningEffort: effort,
			InheritGlobal:   profile.InheritGlobal,
		})
	}

	lanes := make([]reviewPolicyLaneMetadata, 0, len(policy.Swarm.Lanes))
	for _, lane := range policy.Swarm.Lanes {
		metadata := reviewPolicyLaneMetadata{
			Name:            lane.Name,
			Required:        lane.Required,
			Profile:         lane.Profile,
			Model:           "unavailable",
			ReasoningEffort: "unavailable",
		}
		profile, err := policy.effectiveNamedProfileForReviewWorker(
			strings.TrimSpace(lane.Profile),
			"observable discovery lane",
		)
		if err == nil {
			metadata.Profile = profile.Name
			metadata.Model = profile.Model
			metadata.ReasoningEffort = profile.ReasoningEffort
			metadata.InheritGlobal = profile.InheritGlobal
			if profile.InheritGlobal {
				metadata.Model = inheritedGlobalPolicyValue
				metadata.ReasoningEffort =
					inheritedGlobalPolicyValue
			}
		}
		lanes = append(lanes, metadata)
	}

	return reviewPolicyMetadata{
		State:       "convergent",
		Version:     policy.Version,
		Fingerprint: policy.Fingerprint,
		Roles:       roles,
		Swarm: reviewPolicySwarmMetadata{
			MinReviewers:         policy.Swarm.MinReviewers,
			MaxReviewers:         policy.Swarm.MaxReviewers,
			MaxParallelReviewers: policy.Swarm.MaxParallelReviewers,
			TimeoutMinutes:       policy.Swarm.TimeoutMinutes,
			Retries:              policy.Swarm.Retries,
		},
		Lanes: lanes,
		Artifacts: reviewPolicyArtifactMetadata{
			MaxBytes:       policy.Artifacts.MaxBytes,
			TimeoutSeconds: policy.Artifacts.TimeoutSeconds,
			Retries:        policy.Artifacts.Retries,
		},
		Verification: reviewPolicyVerificationMetadata{
			MinVerifiers: policy.Verification.MinVerifiers,
			MaxVerifiers: policy.Verification.MaxVerifiers,
			MaxParallelVerifiers: policy.Verification.
				MaxParallelVerifiers,
			TimeoutMinutes: policy.Verification.TimeoutMinutes,
			Retries:        policy.Verification.Retries,
		},
		Convergence: reviewPolicyConvergenceMetadata{
			QuietRoundsRequired:   policy.Convergence.QuietRoundsRequired,
			MaxRounds:             policy.Convergence.MaxRounds,
			MaxReviewAgentsPerSHA: policy.Convergence.MaxReviewAgentsPerSHA,
			MaxWallTimeMinutes:    policy.Convergence.MaxWallTimeMinutes,
			MaxUsageTokens:        policy.Convergence.MaxUsageTokens,
		},
		Escalation: reviewPolicyEscalationMetadata{
			AfterNonConvergingRounds: policy.Escalation.AfterNonConvergingRounds,
			AfterCorrectionRounds:    policy.Escalation.AfterCorrectionRounds,
			Profile:                  policy.Escalation.Profile,
		},
	}
}

func formatReviewPolicyStatus(policy ReviewPolicy) string {
	metadata := safeReviewPolicyMetadata(policy)
	var status strings.Builder

	status.WriteString("Review Policy\n")
	fmt.Fprintf(&status, "  state: %s\n", metadata.State)
	fmt.Fprintf(&status, "  version: %d\n", metadata.Version)
	fmt.Fprintf(&status, "  fingerprint: %s\n", metadata.Fingerprint)
	status.WriteString("  roles:\n")
	for _, role := range metadata.Roles {
		fmt.Fprintf(
			&status,
			"    - %s: profile=%q model=%q effort=%q inherit_global=%t\n",
			role.Role,
			role.Profile,
			role.Model,
			role.ReasoningEffort,
			role.InheritGlobal,
		)
	}
	fmt.Fprintf(
		&status,
		"  swarm: min=%d max=%d max_parallel=%d timeout_minutes=%d retries=%d\n",
		metadata.Swarm.MinReviewers,
		metadata.Swarm.MaxReviewers,
		metadata.Swarm.MaxParallelReviewers,
		metadata.Swarm.TimeoutMinutes,
		metadata.Swarm.Retries,
	)
	status.WriteString("  lanes:\n")
	for _, lane := range metadata.Lanes {
		fmt.Fprintf(
			&status,
			"    - %s: required=%t profile=%q model=%q effort=%q inherit_global=%t\n",
			lane.Name,
			lane.Required,
			lane.Profile,
			lane.Model,
			lane.ReasoningEffort,
			lane.InheritGlobal,
		)
	}
	fmt.Fprintf(
		&status,
		"  artifacts: max_bytes=%d timeout_seconds=%d retries=%d\n",
		metadata.Artifacts.MaxBytes,
		metadata.Artifacts.TimeoutSeconds,
		metadata.Artifacts.Retries,
	)
	fmt.Fprintf(
		&status,
		"  verification: min=%d max=%d max_parallel=%d timeout_minutes=%d retries=%d\n",
		metadata.Verification.MinVerifiers,
		metadata.Verification.MaxVerifiers,
		metadata.Verification.MaxParallelVerifiers,
		metadata.Verification.TimeoutMinutes,
		metadata.Verification.Retries,
	)
	fmt.Fprintf(
		&status,
		"  convergence: quiet_rounds=%d max_rounds=%d max_agents_per_sha=%d max_wall_time_minutes=%d max_usage_tokens=%d\n",
		metadata.Convergence.QuietRoundsRequired,
		metadata.Convergence.MaxRounds,
		metadata.Convergence.MaxReviewAgentsPerSHA,
		metadata.Convergence.MaxWallTimeMinutes,
		metadata.Convergence.MaxUsageTokens,
	)
	fmt.Fprintf(
		&status,
		"  escalation: after_non_converging_rounds=%d after_correction_rounds=%d profile=%q\n",
		metadata.Escalation.AfterNonConvergingRounds,
		metadata.Escalation.AfterCorrectionRounds,
		metadata.Escalation.Profile,
	)
	return status.String()
}

func formatReviewPolicyLogRecord(policy ReviewPolicy, timestamp time.Time) string {
	body, err := json.Marshal(reviewPolicyLogRecord{
		Timestamp:    timestamp.UTC(),
		Event:        "effective_review_policy",
		ReviewPolicy: safeReviewPolicyMetadata(policy),
	})
	if err != nil {
		return fmt.Sprintf(
			`{"timestamp":%q,"event":"effective_review_policy","error":"metadata_unavailable"}`,
			timestamp.UTC().Format(time.RFC3339Nano),
		)
	}
	return string(body)
}

func logEffectiveReviewPolicy(policy ReviewPolicy) {
	structuredLogger := log.New(log.Writer(), "", 0)
	structuredLogger.Print(formatReviewPolicyLogRecord(policy, time.Now()))
}
