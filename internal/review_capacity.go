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
	"errors"
	"fmt"
)

type ReviewCapacityPlan struct {
	ConfiguredMaximum    int `json:"configured_maximum"`
	RequiredMaximum      int `json:"required_maximum"`
	EffectiveMaximum     int `json:"effective_maximum"`
	DiscoveryPasses      int `json:"discovery_passes"`
	DiscoveryAttempts    int `json:"discovery_attempts"`
	VerificationAttempts int `json:"verification_attempts"`
	ChallengeAttempts    int `json:"challenge_attempts"`
}

func checkedReviewCapacityProduct(values ...uint64) (uint64, error) {
	product := uint64(1)
	for _, value := range values {
		if value != 0 && product > ^uint64(0)/value {
			return 0, errors.New("review capacity calculation overflowed")
		}
		product *= value
	}
	return product, nil
}

func checkedReviewCapacitySum(values ...uint64) (uint64, error) {
	total := uint64(0)
	for _, value := range values {
		if total > ^uint64(0)-value {
			return 0, errors.New("review capacity calculation overflowed")
		}
		total += value
	}
	return total, nil
}

func minimumReviewLifecycleCapacity(policy ReviewPolicy) (uint64, error) {
	requiredLanes := 0
	for _, lane := range policy.Swarm.Lanes {
		if lane.Required {
			requiredLanes++
		}
	}
	discoveryLanes := max(policy.Swarm.MinReviewers, requiredLanes) + 1
	discovery, err := checkedReviewCapacityProduct(
		uint64(discoveryLanes),
		uint64(max(1, policy.Swarm.Retries+1)),
		uint64(max(1, policy.Convergence.QuietRoundsRequired)),
	)
	if err != nil {
		return 0, err
	}
	verification, err := checkedReviewCapacityProduct(
		uint64(policy.Verification.MinVerifiers),
		uint64(max(1, policy.Verification.Retries+1)),
	)
	if err != nil {
		return 0, err
	}
	return checkedReviewCapacitySum(discovery, verification)
}

func plannedReviewDiscoveryPasses(cycle *ReviewCycleState) int {
	if cycle == nil {
		return 0
	}
	passes := len(cycle.DiscoveryPasses)
	if reviewCycleHasTerminalVerdict(cycle) {
		return passes
	}
	rounds := 0
	quiet := 0
	if cycle.Convergence != nil {
		rounds = len(cycle.Convergence.Rounds)
		quiet = cycle.Convergence.QuietRounds
	}
	unassessedPasses := passes - rounds
	if unassessedPasses < 0 {
		unassessedPasses = 0
	}
	remainingQuiet := cycle.Policy.Convergence.QuietRoundsRequired - quiet - unassessedPasses
	if remainingQuiet < 0 {
		remainingQuiet = 0
	}
	planned := passes + remainingQuiet
	if planned == 0 {
		planned = cycle.Policy.Convergence.QuietRoundsRequired
	}
	if planned > cycle.Policy.Convergence.MaxRounds {
		planned = cycle.Policy.Convergence.MaxRounds
	}
	return planned
}

func currentReviewVerificationCapacity(cycle *ReviewCycleState) int {
	if cycle == nil {
		return 0
	}
	logicalAssignments := 0
	currentByFinding := make(map[string]int)
	if cycle.Convergence != nil {
		logicalAssignments = len(cycle.Convergence.VerificationAssignments)
		for _, assignment := range cycle.Convergence.VerificationAssignments {
			if assignment.Superseded {
				continue
			}
			finding, ok := reviewCanonicalFindingByID(cycle, assignment.FindingID)
			if !ok {
				continue
			}
			revision, err := reviewFindingCandidateRevision(*finding)
			if err == nil && assignment.CandidateRevision == revision {
				currentByFinding[finding.ID]++
			}
		}
	}
	for _, finding := range cycle.CanonicalFindings {
		if finding.ExactSHA != cycle.HeadSHA {
			continue
		}
		missing := cycle.Policy.Verification.MaxVerifiers - currentByFinding[finding.ID]
		if missing > 0 {
			logicalAssignments += missing
		}
	}
	return logicalAssignments
}

func currentReviewChallengeCapacity(cycle *ReviewCycleState) int {
	if cycle == nil {
		return 0
	}
	assignments := 0
	attempted := make(map[string]struct{})
	if cycle.Convergence != nil {
		assignments = len(cycle.Convergence.ChallengeAssignments)
		for _, assignment := range cycle.Convergence.ChallengeAssignments {
			if assignment.Superseded {
				continue
			}
			attempted[string(assignment.Target.Kind)+"\x00"+assignment.Target.ID] = struct{}{}
		}
	}
	for _, target := range buildReviewChallengeTargets(cycle) {
		key := string(target.Kind) + "\x00" + target.ID
		if _, ok := attempted[key]; !ok {
			assignments++
		}
	}
	return assignments
}

func buildReviewCapacityPlan(cycle *ReviewCycleState) (ReviewCapacityPlan, error) {
	if cycle == nil || cycle.Plan == nil {
		return ReviewCapacityPlan{}, errors.New("review capacity requires an exact-SHA plan")
	}
	passes := plannedReviewDiscoveryPasses(cycle)
	discoveryAttempts, err := checkedReviewCapacityProduct(
		uint64(len(cycle.Plan.SelectedLanes)+1),
		uint64(cycle.Policy.Swarm.Retries+1),
		uint64(passes),
	)
	if err != nil {
		return ReviewCapacityPlan{}, err
	}
	verificationAttempts, err := checkedReviewCapacityProduct(
		uint64(currentReviewVerificationCapacity(cycle)),
		uint64(cycle.Policy.Verification.Retries+1),
	)
	if err != nil {
		return ReviewCapacityPlan{}, err
	}
	challengeAttempts, err := checkedReviewCapacityProduct(
		uint64(currentReviewChallengeCapacity(cycle)),
		uint64(cycle.Policy.Swarm.Retries+1),
	)
	if err != nil {
		return ReviewCapacityPlan{}, err
	}
	required, err := checkedReviewCapacitySum(
		discoveryAttempts,
		verificationAttempts,
		challengeAttempts,
	)
	if err != nil {
		return ReviewCapacityPlan{}, err
	}
	if required < uint64(len(cycle.WorkerOwnerships)) {
		required = uint64(len(cycle.WorkerOwnerships))
	}
	if cycle.LimitTransition != nil &&
		cycle.LimitTransition.Kind == ReviewLimitAgentCount &&
		required < cycle.LimitTransition.Maximum {
		required = cycle.LimitTransition.Maximum
	}
	maximumInt := uint64(^uint(0) >> 1)
	if required > maximumInt {
		return ReviewCapacityPlan{}, fmt.Errorf(
			"required review capacity %d exceeds the platform integer limit",
			required,
		)
	}
	requiredMaximum := int(required)
	effectiveMaximum := cycle.Policy.Convergence.MaxReviewAgentsPerSHA
	return ReviewCapacityPlan{
		ConfiguredMaximum:    cycle.Policy.Convergence.MaxReviewAgentsPerSHA,
		RequiredMaximum:      requiredMaximum,
		EffectiveMaximum:     effectiveMaximum,
		DiscoveryPasses:      passes,
		DiscoveryAttempts:    int(discoveryAttempts),
		VerificationAttempts: int(verificationAttempts),
		ChallengeAttempts:    int(challengeAttempts),
	}, nil
}

func effectiveReviewAgentCapacity(cycle *ReviewCycleState) int {
	if cycle == nil {
		return 0
	}
	if cycle.Plan == nil {
		return cycle.Policy.Convergence.MaxReviewAgentsPerSHA
	}
	plan, err := buildReviewCapacityPlan(cycle)
	if err != nil {
		return cycle.Policy.Convergence.MaxReviewAgentsPerSHA
	}
	return plan.EffectiveMaximum
}
