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
	"fmt"
	"strings"
)

const (
	defaultReviewEscalationRound = 1
	defaultReviewCorrectionRound = 2
)

type ReviewAction string

const (
	ReviewActionEscalate     ReviewAction = "escalate"
	ReviewActionFail         ReviewAction = "fail"
	ReviewActionInconclusive ReviewAction = "inconclusive"
)

type escalationConfigFile struct {
	AfterNonConvergingRounds *int    `yaml:"AFTER_NON_CONVERGING_ROUNDS"`
	AfterCorrectionRounds    *int    `yaml:"AFTER_CORRECTION_ROUNDS"`
	Profile                  *string `yaml:"PROFILE"`
}

type reviewFailureActionsConfigFile struct {
	RequiredLaneFailure *string `yaml:"REQUIRED_LANE_FAILURE"`
	VerificationFailure *string `yaml:"VERIFICATION_FAILURE"`
	BudgetExhaustion    *string `yaml:"BUDGET_EXHAUSTION"`
}

type EscalationPolicy struct {
	AfterNonConvergingRounds int    `json:"after_non_converging_rounds" yaml:"AFTER_NON_CONVERGING_ROUNDS"`
	AfterCorrectionRounds    int    `json:"after_correction_rounds" yaml:"AFTER_CORRECTION_ROUNDS"`
	Profile                  string `json:"profile" yaml:"PROFILE"`
}

type ReviewFailureActions struct {
	RequiredLaneFailure ReviewAction `json:"required_lane_failure" yaml:"REQUIRED_LANE_FAILURE"`
	VerificationFailure ReviewAction `json:"verification_failure" yaml:"VERIFICATION_FAILURE"`
	BudgetExhaustion    ReviewAction `json:"budget_exhaustion" yaml:"BUDGET_EXHAUSTION"`
}

func defaultEscalationPolicy(profile string) EscalationPolicy {
	return EscalationPolicy{
		AfterNonConvergingRounds: defaultReviewEscalationRound,
		AfterCorrectionRounds:    defaultReviewCorrectionRound,
		Profile:                  profile,
	}
}

func defaultReviewFailureActions() ReviewFailureActions {
	return ReviewFailureActions{
		RequiredLaneFailure: ReviewActionEscalate,
		VerificationFailure: ReviewActionEscalate,
		BudgetExhaustion:    ReviewActionInconclusive,
	}
}

func normalizeEscalationPolicy(raw *escalationConfigFile, defaults EscalationPolicy) EscalationPolicy {
	policy := defaults
	if raw == nil {
		return policy
	}
	if raw.AfterNonConvergingRounds != nil {
		policy.AfterNonConvergingRounds = *raw.AfterNonConvergingRounds
	}
	if raw.AfterCorrectionRounds != nil {
		policy.AfterCorrectionRounds = *raw.AfterCorrectionRounds
	}
	if raw.Profile != nil {
		policy.Profile = strings.TrimSpace(*raw.Profile)
	}
	return policy
}

func normalizeReviewFailureActions(
	raw *reviewFailureActionsConfigFile,
	defaults ReviewFailureActions,
) ReviewFailureActions {
	actions := defaults
	if raw == nil {
		return actions
	}
	if raw.RequiredLaneFailure != nil {
		actions.RequiredLaneFailure = normalizeReviewAction(*raw.RequiredLaneFailure)
	}
	if raw.VerificationFailure != nil {
		actions.VerificationFailure = normalizeReviewAction(*raw.VerificationFailure)
	}
	if raw.BudgetExhaustion != nil {
		actions.BudgetExhaustion = normalizeReviewAction(*raw.BudgetExhaustion)
	}
	return actions
}

func normalizeReviewAction(raw string) ReviewAction {
	return ReviewAction(strings.ToLower(strings.TrimSpace(raw)))
}

func validateReviewLimits(policy ReviewPolicy) error {
	minimumWorkers, err := minimumReviewLifecycleCapacity(policy)
	if err != nil {
		return fmt.Errorf(
			"REVIEW_POLICY.CONVERGENCE.MAX_REVIEW_AGENTS_PER_SHA is invalid: %w",
			err,
		)
	}
	if uint64(policy.Convergence.MaxReviewAgentsPerSHA) < minimumWorkers {
		return fmt.Errorf(
			"REVIEW_POLICY.CONVERGENCE.MAX_REVIEW_AGENTS_PER_SHA must be at least %d "+
				"for discovery, synthesis, retries, verifier fan-out, and rediscovery",
			minimumWorkers,
		)
	}
	if policy.Escalation.AfterNonConvergingRounds <= 0 {
		return fmt.Errorf("REVIEW_POLICY.ESCALATION.AFTER_NON_CONVERGING_ROUNDS must be greater than zero")
	}
	if policy.Escalation.AfterNonConvergingRounds > policy.Convergence.MaxRounds {
		return fmt.Errorf(
			"REVIEW_POLICY.ESCALATION.AFTER_NON_CONVERGING_ROUNDS must be <= " +
				"REVIEW_POLICY.CONVERGENCE.MAX_ROUNDS",
		)
	}
	if policy.Escalation.AfterCorrectionRounds <= 0 {
		return fmt.Errorf("REVIEW_POLICY.ESCALATION.AFTER_CORRECTION_ROUNDS must be greater than zero")
	}
	if policy.Escalation.Profile == "" {
		return fmt.Errorf("REVIEW_POLICY.ESCALATION.PROFILE must reference a named profile")
	}
	if _, ok := policy.AgentProfiles[policy.Escalation.Profile]; !ok {
		return fmt.Errorf(
			"REVIEW_POLICY.ESCALATION.PROFILE references unknown profile %q",
			policy.Escalation.Profile,
		)
	}
	for _, item := range []struct {
		field  string
		action ReviewAction
	}{
		{field: "REVIEW_POLICY.FAILURE_ACTIONS.REQUIRED_LANE_FAILURE", action: policy.FailureActions.RequiredLaneFailure},
		{field: "REVIEW_POLICY.FAILURE_ACTIONS.VERIFICATION_FAILURE", action: policy.FailureActions.VerificationFailure},
		{field: "REVIEW_POLICY.FAILURE_ACTIONS.BUDGET_EXHAUSTION", action: policy.FailureActions.BudgetExhaustion},
	} {
		if err := validateReviewAction(item.field, item.action); err != nil {
			return err
		}
	}
	return nil
}

func validateReviewAction(field string, action ReviewAction) error {
	switch action {
	case ReviewActionEscalate, ReviewActionFail, ReviewActionInconclusive:
		return nil
	default:
		return fmt.Errorf(
			"%s has unsupported action %q; supported actions are escalate, fail, and inconclusive",
			field,
			action,
		)
	}
}
