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
	"time"
)

// reviewBudgetEstimate is a *realistic* worst-case estimate of what a fully
// healthy convergence can need, as distinct from minimumReviewLifecycleCapacity
// (internal/review_capacity.go), which is a bare-survival floor already
// enforced at config-load time (validateReviewLimits) and is far below what
// most real reviews actually use: it only accounts for the required lanes
// and one quiet round, not the full configured lane list run out to
// MaxRounds. This estimate is intentionally a soft advisory, not a hard
// validation error -- it can be wrong in either direction for an unusual
// policy or a genuinely lightweight repository, so it is reported as a
// warning with its reasoning shown, never a config rejection.
type reviewBudgetEstimate struct {
	DiscoveryPasses          int
	DiscoveryPassMinutes     int
	ConvergenceRounds        int
	VerificationRoundMinutes int
	LaneCount                int
	VerifiersPerFinding      int
	AssumedContestedFindings int
	DiscoveryAgents          int
	EstimatedMinutes         int
	EstimatedAgents          int
	// MaxSupportedContestedFindings is how many concurrently-contested
	// findings per round the *configured* MaxReviewAgentsPerSHA can
	// actually absorb, after discovery's share of the budget. It is always
	// reported (not just when a warning fires) because the number of
	// findings a real review surfaces is a repository property, not a
	// policy one -- no policy-only estimate can know it, so the honest
	// thing to expose is the budget's actual capacity in those terms
	// rather than a single guessed "the" agent count.
	MaxSupportedContestedFindings int
}

// estimateReviewPolicyBudget cannot know how many distinct findings a real
// review will surface -- that depends on the repository/PR content, not
// the policy -- so verifier fan-out (MaxVerifiers is a per-finding bound,
// not a per-round one) is estimated against an assumed number of
// concurrently-contested findings per round, AssumedContestedFindings, tied
// to an existing policy knob (SWARM.MIN_REVIEWERS) rather than an
// unexplainable made-up constant. This is deliberately visible in
// reviewBudgetEstimate and every warning message: a review that surfaces
// more contested findings per round than that will need more agents than
// this estimate, which is why MaxSupportedContestedFindings is always
// reported too, so the assumption's consequence is never silent.
func estimateReviewPolicyBudget(policy ReviewPolicy) reviewBudgetEstimate {
	laneCount := len(policy.Swarm.Lanes)
	swarmAttempts := max(1, policy.Swarm.Retries+1)
	// One discovery pass is a parallel lane batch (wall time bounded by the
	// slowest lane's timeout, including its own retries) followed by a
	// strictly sequential synthesis lane (internal/review_discovery_scheduler.go
	// only runs synthesis after every other lane succeeds) -- so a pass
	// costs up to two full timeout windows, not one.
	discoveryPassMinutes := policy.Swarm.TimeoutMinutes * swarmAttempts * 2
	// Rediscovery reruns a full pass before each non-quiet round
	// (internal/review_coordinator.go's convergent loop), so worst case is
	// one pass per round.
	passes := max(1, policy.Convergence.MaxRounds)

	verificationAttempts := max(1, policy.Verification.Retries+1)
	verificationRoundMinutes := policy.Verification.TimeoutMinutes * verificationAttempts
	rounds := max(1, policy.Convergence.MaxRounds)

	estimatedMinutes := passes*discoveryPassMinutes + rounds*verificationRoundMinutes

	assumedContestedFindings := max(1, policy.Swarm.MinReviewers)
	discoveryAgents := passes * (laneCount + 1) // +1 per pass for synthesis
	verificationAgents := rounds * assumedContestedFindings * policy.Verification.MaxVerifiers
	estimatedAgents := discoveryAgents + verificationAgents

	perFindingCost := max(1, rounds*policy.Verification.MaxVerifiers)
	maxSupportedContestedFindings := (policy.Convergence.MaxReviewAgentsPerSHA - discoveryAgents) / perFindingCost
	if maxSupportedContestedFindings < 0 {
		maxSupportedContestedFindings = 0
	}

	return reviewBudgetEstimate{
		DiscoveryPasses:               passes,
		DiscoveryPassMinutes:          discoveryPassMinutes,
		ConvergenceRounds:             rounds,
		VerificationRoundMinutes:      verificationRoundMinutes,
		LaneCount:                     laneCount,
		VerifiersPerFinding:           policy.Verification.MaxVerifiers,
		AssumedContestedFindings:      assumedContestedFindings,
		DiscoveryAgents:               discoveryAgents,
		EstimatedMinutes:              estimatedMinutes,
		EstimatedAgents:               estimatedAgents,
		MaxSupportedContestedFindings: maxSupportedContestedFindings,
	}
}

// reviewBudgetSuggestedValue rounds a raw estimate up with headroom (50%),
// then to the nearest 5, so a suggested config value doesn't read as
// falsely precise.
func reviewBudgetSuggestedValue(estimate int) int {
	withHeadroom := estimate + estimate/2
	const step = 5
	rounded := ((withHeadroom + step - 1) / step) * step
	if rounded < step {
		rounded = step
	}
	return rounded
}

// analyzeReviewPolicyBudget compares the configured CONVERGENCE budgets
// against estimateReviewPolicyBudget and returns one human-readable warning
// per budget that looks too tight, each with a concrete suggested value and
// the reasoning behind the estimate.
func analyzeReviewPolicyBudget(policy ReviewPolicy) []string {
	estimate := estimateReviewPolicyBudget(policy)
	var warnings []string

	if policy.Convergence.MaxWallTimeMinutes < estimate.EstimatedMinutes {
		suggested := reviewBudgetSuggestedValue(estimate.EstimatedMinutes)
		warnings = append(warnings, fmt.Sprintf(
			"REVIEW_POLICY.CONVERGENCE.MAX_WALL_TIME_MINUTES=%d is below the "+
				"~%d minutes a fully healthy convergence can realistically need "+
				"(%d discovery pass(es) x ~%d min [parallel lane batch + "+
				"sequential synthesis, SWARM.TIMEOUT_MINUTES=%d x %d attempt(s) "+
				"x 2] + %d verification round(s) x ~%d min "+
				"[VERIFICATION.TIMEOUT_MINUTES=%d x %d attempt(s)]); consider "+
				"raising it to at least %d",
			policy.Convergence.MaxWallTimeMinutes,
			estimate.EstimatedMinutes,
			estimate.DiscoveryPasses,
			estimate.DiscoveryPassMinutes,
			policy.Swarm.TimeoutMinutes,
			max(1, policy.Swarm.Retries+1),
			estimate.ConvergenceRounds,
			estimate.VerificationRoundMinutes,
			policy.Verification.TimeoutMinutes,
			max(1, policy.Verification.Retries+1),
			suggested,
		))
	}

	if policy.Convergence.MaxReviewAgentsPerSHA < estimate.EstimatedAgents {
		suggested := reviewBudgetSuggestedValue(estimate.EstimatedAgents)
		warnings = append(warnings, fmt.Sprintf(
			"REVIEW_POLICY.CONVERGENCE.MAX_REVIEW_AGENTS_PER_SHA=%d is below "+
				"the ~%d agents a fully healthy convergence can realistically "+
				"need (%d discovery pass(es) x (%d configured lane(s) + 1 "+
				"synthesis) + %d verification round(s) x an assumed %d "+
				"concurrently-contested finding(s) per round [from "+
				"SWARM.MIN_REVIEWERS=%d] x up to %d verifier(s) per finding); "+
				"a review surfacing more contested findings per round than "+
				"that assumption will need proportionally more agents than "+
				"even this estimate; consider raising it to at least %d",
			policy.Convergence.MaxReviewAgentsPerSHA,
			estimate.EstimatedAgents,
			estimate.DiscoveryPasses,
			estimate.LaneCount,
			estimate.ConvergenceRounds,
			estimate.AssumedContestedFindings,
			policy.Swarm.MinReviewers,
			estimate.VerifiersPerFinding,
			suggested,
		))
	}

	return warnings
}

// reviewLimitKindMaxRoundsExhausted is not one of the ReviewLimitKind
// values ReviewLimitTransition uses (internal/review_limits.go) -- round
// exhaustion is tracked separately, as cycle.Convergence.Status ==
// ReviewConvergenceMaxRounds (internal/review_convergence.go) -- but it is
// reported through the same reviewBudgetLimitHit shape since it is the
// same kind of fact from an operator's point of view: a configured
// convergence budget was actually exhausted.
const reviewLimitKindMaxRoundsExhausted ReviewLimitKind = "max_rounds_exhausted"

// reviewBudgetLimitHit is one persisted, actually-observed budget exhaustion
// -- forensic confirmation (or refutation) of analyzeReviewPolicyBudget's
// static estimate, from real review cycles rather than policy arithmetic.
type reviewBudgetLimitHit struct {
	ReviewerID string
	PRNumber   int
	Kind       ReviewLimitKind
	Actual     uint64
	Maximum    uint64
	Unit       string
	At         time.Time
}

// findReviewBudgetLimitHits scans currently loaded agents for durable,
// already-persisted evidence that a configured convergence budget was
// actually exhausted:
//   - ReviewLimitTransition (internal/review_limits.go), covering all
//     three ReviewLimitKind values -- agent count and wall time (both
//     proactively estimated by analyzeReviewPolicyBudget above) and
//     supported usage/tokens (deliberately *not* proactively estimated:
//     unlike wall time or agent count, nothing in ReviewPolicy bounds
//     per-agent token consumption -- AgentProfile carries no token/context
//     budget field -- so any numeric token estimate would be fabricated,
//     not derived; this forensic scan is the only detection available for
//     it today)
//   - cycle.Convergence.Status == ReviewConvergenceMaxRounds, covering
//     round exhaustion, which has no ReviewLimitTransition of its own.
//     Also not proactively flagged as its own warning: MaxRounds is
//     already a direct multiplier of both the wall-time and agent-count
//     estimates above (passes/rounds), so a MaxRounds that's badly out of
//     proportion to the wall-time/agent budgets is already reflected
//     through those two warnings; whether MaxRounds itself is high enough
//     for real convergence to succeed is a review-quality/product tuning
//     question, not a resource-budget misconfiguration in the same sense.
func findReviewBudgetLimitHits(agents []Agent) []reviewBudgetLimitHit {
	hits := make([]reviewBudgetLimitHit, 0)
	for _, agent := range agents {
		if agent.Role != RoleReviewer || agent.ReviewCycle == nil {
			continue
		}
		cycle := agent.ReviewCycle
		if transition := cycle.LimitTransition; transition != nil {
			hits = append(hits, reviewBudgetLimitHit{
				ReviewerID: agent.ID,
				PRNumber:   agent.PRNumber,
				Kind:       transition.Kind,
				Actual:     transition.Actual,
				Maximum:    transition.Maximum,
				Unit:       transition.Unit,
				At:         transition.TransitionedAt,
			})
		}
		if cycle.Convergence != nil &&
			cycle.Convergence.Status == ReviewConvergenceMaxRounds {
			var at time.Time
			if rounds := cycle.Convergence.Rounds; len(rounds) > 0 {
				at = rounds[len(rounds)-1].CompletedAt
			}
			hits = append(hits, reviewBudgetLimitHit{
				ReviewerID: agent.ID,
				PRNumber:   agent.PRNumber,
				Kind:       reviewLimitKindMaxRoundsExhausted,
				Actual:     uint64(len(cycle.Convergence.Rounds)),
				Maximum:    uint64(cycle.Policy.Convergence.MaxRounds),
				Unit:       "rounds",
				At:         at,
			})
		}
	}
	return hits
}

// formatReviewBudgetReport combines the static policy estimate with any
// forensic evidence of budgets actually having been exhausted, for
// inclusion in a tech-support bundle or REPL output.
func formatReviewBudgetReport(policy ReviewPolicy, agents []Agent) string {
	var report string
	report += "Scope: proactively estimated below are " +
		"MAX_WALL_TIME_MINUTES and MAX_REVIEW_AGENTS_PER_SHA. " +
		"MAX_USAGE_TOKENS is not (no ReviewPolicy field bounds an agent's " +
		"token/context usage, so any numeric estimate would be fabricated, " +
		"not derived) and MAX_ROUNDS exhaustion is not flagged as its own " +
		"warning (it is already a direct multiplier of the two estimates " +
		"below, and whether it is high enough for real convergence to " +
		"succeed is a review-quality tuning question, not a resource-budget " +
		"misconfiguration in the same sense). Both are still covered by the " +
		"forensic scan below, which reports an actual exhaustion of any " +
		"kind regardless of whether it was estimated in advance.\n\n"
	estimate := estimateReviewPolicyBudget(policy)
	warnings := analyzeReviewPolicyBudget(policy)
	if len(warnings) == 0 {
		report += "No budget warnings: configured CONVERGENCE limits are at or above the realistic estimate for this policy.\n"
	} else {
		report += "Budget warnings (estimate, not a guarantee -- see reasoning below):\n"
		for _, warning := range warnings {
			report += "  - " + warning + "\n"
		}
	}
	// The number of findings a review surfaces is a repository property,
	// not a policy one, so no estimate above can know it -- this is always
	// shown (not just when a warning fires) so that consequence is never
	// silent: even a policy with no warning can still exhaust
	// MAX_REVIEW_AGENTS_PER_SHA if a review surfaces more contested
	// findings per round than the budget actually has room for.
	report += fmt.Sprintf(
		"At the configured MAX_REVIEW_AGENTS_PER_SHA=%d, this budget has "+
			"room for up to %d concurrently-contested finding(s) per round "+
			"needing full verifier fan-out (up to %d verifier(s) each) after "+
			"discovery's share (%d agent(s)); a review surfacing more "+
			"contested findings than that per round will exhaust the budget "+
			"regardless of the warnings above.\n",
		policy.Convergence.MaxReviewAgentsPerSHA,
		estimate.MaxSupportedContestedFindings,
		estimate.VerifiersPerFinding,
		estimate.DiscoveryAgents,
	)

	hits := findReviewBudgetLimitHits(agents)
	report += "\n"
	if len(hits) == 0 {
		report += "No currently loaded reviewer has recorded an actual budget exhaustion.\n"
		return report
	}
	report += fmt.Sprintf("%d currently loaded reviewer(s) have actually exhausted a budget:\n", len(hits))
	for _, hit := range hits {
		report += fmt.Sprintf(
			"  - reviewer=%s pr=%d kind=%s actual=%d maximum=%d unit=%s at=%s\n",
			hit.ReviewerID,
			hit.PRNumber,
			hit.Kind,
			hit.Actual,
			hit.Maximum,
			hit.Unit,
			hit.At.UTC().Format(time.RFC3339),
		)
	}
	return report
}
