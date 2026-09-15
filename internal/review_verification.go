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

import "fmt"

const (
	defaultReviewMinVerifiers      = 1
	defaultReviewMaxVerifiers      = 3
	defaultReviewParallelVerifiers = 2
	defaultReviewVerifierTimeout   = 30
	defaultReviewVerifierRetries   = 1
	defaultReviewRequireEvidence   = true
	defaultReviewQuietRounds       = 1
	defaultReviewMaxRounds         = 2
	// The built-in policy's two discovery/synthesis passes and verification
	// rounds can consume six sequential 30-minute runtime windows, each with
	// one retry. Ten hours preserves useful headroom without shortening any
	// worker's configured runtime.
	defaultReviewMaxAgentsPerSHA = 50
	defaultReviewMaxWallTime     = 600
	defaultReviewMaxUsageTokens  = 2_000_000
)

type verificationConfigFile struct {
	MinVerifiers         *int    `yaml:"MIN_VERIFIERS"`
	MaxVerifiers         *int    `yaml:"MAX_VERIFIERS"`
	MaxParallelVerifiers *int    `yaml:"MAX_PARALLEL_VERIFIERS"`
	TimeoutMinutes       *int    `yaml:"TIMEOUT_MINUTES"`
	Retries              *int    `yaml:"RETRIES"`
	RequireEvidence      *bool   `yaml:"REQUIRE_EVIDENCE"`
	InconclusiveAction   *string `yaml:"INCONCLUSIVE_ACTION"`
}

type convergenceConfigFile struct {
	QuietRoundsRequired   *int `yaml:"QUIET_ROUNDS_REQUIRED"`
	MaxRounds             *int `yaml:"MAX_ROUNDS"`
	MaxReviewAgentsPerSHA *int `yaml:"MAX_REVIEW_AGENTS_PER_SHA"`
	MaxWallTimeMinutes    *int `yaml:"MAX_WALL_TIME_MINUTES"`
	MaxUsageTokens        *int `yaml:"MAX_USAGE_TOKENS"`
}

type VerificationPolicy struct {
	MinVerifiers         int          `json:"min_verifiers" yaml:"MIN_VERIFIERS"`
	MaxVerifiers         int          `json:"max_verifiers" yaml:"MAX_VERIFIERS"`
	MaxParallelVerifiers int          `json:"max_parallel_verifiers" yaml:"MAX_PARALLEL_VERIFIERS"`
	TimeoutMinutes       int          `json:"timeout_minutes" yaml:"TIMEOUT_MINUTES"`
	Retries              int          `json:"retries" yaml:"RETRIES"`
	RequireEvidence      bool         `json:"require_evidence" yaml:"REQUIRE_EVIDENCE"`
	InconclusiveAction   ReviewAction `json:"inconclusive_action" yaml:"INCONCLUSIVE_ACTION"`
}

type ConvergencePolicy struct {
	QuietRoundsRequired   int `json:"quiet_rounds_required" yaml:"QUIET_ROUNDS_REQUIRED"`
	MaxRounds             int `json:"max_rounds" yaml:"MAX_ROUNDS"`
	MaxReviewAgentsPerSHA int `json:"max_review_agents_per_sha" yaml:"MAX_REVIEW_AGENTS_PER_SHA"`
	MaxWallTimeMinutes    int `json:"max_wall_time_minutes" yaml:"MAX_WALL_TIME_MINUTES"`
	MaxUsageTokens        int `json:"max_usage_tokens" yaml:"MAX_USAGE_TOKENS"`
}

func defaultVerificationPolicy() VerificationPolicy {
	return VerificationPolicy{
		MinVerifiers:         defaultReviewMinVerifiers,
		MaxVerifiers:         defaultReviewMaxVerifiers,
		MaxParallelVerifiers: defaultReviewParallelVerifiers,
		TimeoutMinutes:       defaultReviewVerifierTimeout,
		Retries:              defaultReviewVerifierRetries,
		RequireEvidence:      defaultReviewRequireEvidence,
		InconclusiveAction:   ReviewActionEscalate,
	}
}

func defaultConvergencePolicy() ConvergencePolicy {
	return ConvergencePolicy{
		QuietRoundsRequired:   defaultReviewQuietRounds,
		MaxRounds:             defaultReviewMaxRounds,
		MaxReviewAgentsPerSHA: defaultReviewMaxAgentsPerSHA,
		MaxWallTimeMinutes:    defaultReviewMaxWallTime,
		MaxUsageTokens:        defaultReviewMaxUsageTokens,
	}
}

func normalizeVerificationPolicy(raw *verificationConfigFile, defaults VerificationPolicy) VerificationPolicy {
	policy := defaults
	if raw == nil {
		return policy
	}
	if raw.MinVerifiers != nil {
		policy.MinVerifiers = *raw.MinVerifiers
	}
	if raw.MaxVerifiers != nil {
		policy.MaxVerifiers = *raw.MaxVerifiers
	}
	if raw.MaxParallelVerifiers != nil {
		policy.MaxParallelVerifiers = *raw.MaxParallelVerifiers
	}
	if raw.TimeoutMinutes != nil {
		policy.TimeoutMinutes = *raw.TimeoutMinutes
	}
	if raw.Retries != nil {
		policy.Retries = *raw.Retries
	}
	if raw.RequireEvidence != nil {
		policy.RequireEvidence = *raw.RequireEvidence
	}
	if raw.InconclusiveAction != nil {
		policy.InconclusiveAction = normalizeReviewAction(*raw.InconclusiveAction)
	}
	return policy
}

func normalizeConvergencePolicy(raw *convergenceConfigFile, defaults ConvergencePolicy) ConvergencePolicy {
	policy := defaults
	if raw == nil {
		return policy
	}
	if raw.QuietRoundsRequired != nil {
		policy.QuietRoundsRequired = *raw.QuietRoundsRequired
	}
	if raw.MaxRounds != nil {
		policy.MaxRounds = *raw.MaxRounds
	}
	if raw.MaxReviewAgentsPerSHA != nil {
		policy.MaxReviewAgentsPerSHA = *raw.MaxReviewAgentsPerSHA
	}
	if raw.MaxWallTimeMinutes != nil {
		policy.MaxWallTimeMinutes = *raw.MaxWallTimeMinutes
	}
	if raw.MaxUsageTokens != nil {
		policy.MaxUsageTokens = *raw.MaxUsageTokens
	}
	return policy
}

func validateVerificationPolicy(policy VerificationPolicy) error {
	if policy.MinVerifiers <= 0 {
		return fmt.Errorf("REVIEW_POLICY.VERIFICATION.MIN_VERIFIERS must be greater than zero")
	}
	if policy.MaxVerifiers <= 0 {
		return fmt.Errorf("REVIEW_POLICY.VERIFICATION.MAX_VERIFIERS must be greater than zero")
	}
	if policy.MaxParallelVerifiers <= 0 {
		return fmt.Errorf("REVIEW_POLICY.VERIFICATION.MAX_PARALLEL_VERIFIERS must be greater than zero")
	}
	if policy.TimeoutMinutes <= 0 {
		return fmt.Errorf("REVIEW_POLICY.VERIFICATION.TIMEOUT_MINUTES must be greater than zero")
	}
	if policy.Retries < 0 {
		return fmt.Errorf("REVIEW_POLICY.VERIFICATION.RETRIES must not be negative")
	}
	if policy.MinVerifiers > policy.MaxVerifiers {
		return fmt.Errorf("REVIEW_POLICY.VERIFICATION.MIN_VERIFIERS must be <= MAX_VERIFIERS")
	}
	if policy.MaxParallelVerifiers > policy.MaxVerifiers {
		return fmt.Errorf("REVIEW_POLICY.VERIFICATION.MAX_PARALLEL_VERIFIERS must be <= MAX_VERIFIERS")
	}
	if err := validateReviewAction("REVIEW_POLICY.VERIFICATION.INCONCLUSIVE_ACTION", policy.InconclusiveAction); err != nil {
		return err
	}
	return nil
}

func validateConvergencePolicy(policy ConvergencePolicy) error {
	if policy.QuietRoundsRequired <= 0 {
		return fmt.Errorf("REVIEW_POLICY.CONVERGENCE.QUIET_ROUNDS_REQUIRED must be greater than zero")
	}
	if policy.MaxRounds <= 0 {
		return fmt.Errorf("REVIEW_POLICY.CONVERGENCE.MAX_ROUNDS must be greater than zero")
	}
	if policy.QuietRoundsRequired > policy.MaxRounds {
		return fmt.Errorf("REVIEW_POLICY.CONVERGENCE.QUIET_ROUNDS_REQUIRED must be <= MAX_ROUNDS")
	}
	if policy.MaxReviewAgentsPerSHA <= 0 {
		return fmt.Errorf("REVIEW_POLICY.CONVERGENCE.MAX_REVIEW_AGENTS_PER_SHA must be greater than zero")
	}
	if policy.MaxWallTimeMinutes <= 0 {
		return fmt.Errorf("REVIEW_POLICY.CONVERGENCE.MAX_WALL_TIME_MINUTES must be greater than zero")
	}
	if policy.MaxUsageTokens <= 0 {
		return fmt.Errorf("REVIEW_POLICY.CONVERGENCE.MAX_USAGE_TOKENS must be greater than zero")
	}
	return nil
}
