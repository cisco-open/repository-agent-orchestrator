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
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// reviewCycleDiagnosisPattern names a known review-cycle failure signature.
// Each one maps to an entry in docs/TROUBLESHOOTING.md; adding a newly
// diagnosed failure mode to that doc without adding a matching check here
// is exactly the kind of drift this mechanism exists to avoid, so the two
// should be kept in step deliberately, not by convention alone.
type reviewCycleDiagnosisPattern string

const (
	reviewCycleDiagnosisHealthy reviewCycleDiagnosisPattern = "healthy"
	// reviewCycleDiagnosisPaused: Agent.Paused -- an operator deliberately
	// paused this reviewer (agent pause). AgentManager.Active() does not
	// exclude paused agents, so without this a paused reviewer's
	// intentional silence would eventually be misread as unknown_stall
	// (and trigger an automatic tech-support capture for a non-problem).
	// Checked after staleness and structural validity (both facts about
	// the cycle's data, independent of pause state) but before every
	// other, time-elapsed-dependent check -- a pause explains away
	// silence, but never an existing data problem that pausing didn't
	// cause and unpausing won't fix.
	reviewCycleDiagnosisPaused reviewCycleDiagnosisPattern = "paused"
	// reviewCycleDiagnosisSuperseded: ReviewCycle.Stale -- the PR's head
	// SHA has advanced past what this reviewer is pinned to. Retired and
	// non-progressing by design (see ReviewCycle.Stale's other checks in
	// app.go and review_workflow.go); not a problem needing attention.
	reviewCycleDiagnosisSuperseded reviewCycleDiagnosisPattern = "superseded_stale_head"
	// reviewCycleDiagnosisAwaitingVerdictPublication: convergence reached a
	// *publishable* terminal result (reviewCycleHasPublishableReport)
	// and a verdict genuinely has not been posted yet
	// (reviewCycleNeedsReportPublication). Escalates to STUCK past
	// reviewCycleVerdictPublicationQuietWindow. Does not cover an
	// already-published verdict (reviewCycleDiagnosisVerdictPublishedAwaitingCleanup)
	// or a cycle with no trusted result to publish
	// (reviewCycleDiagnosisBudgetExhausted/reviewCycleDiagnosisTerminalNonPublishableStall).
	reviewCycleDiagnosisAwaitingVerdictPublication reviewCycleDiagnosisPattern = "awaiting_verdict_publication"
	// reviewCycleDiagnosisVerdictPublishedAwaitingCleanup: the verdict was
	// already published, but the reviewer remains active awaiting the
	// coordinator's post-publication cleanup.
	reviewCycleDiagnosisVerdictPublishedAwaitingCleanup reviewCycleDiagnosisPattern = "verdict_published_awaiting_cleanup"
	// reviewCycleDiagnosisTerminalNonPublishableStall: convergence reached
	// a terminal decision with no trusted accepted result and the
	// reviewer remains active awaiting the coordinator's non-publishable-
	// verdict cleanup (publishAndSnapshotConvergentReview). See issue #131.
	reviewCycleDiagnosisTerminalNonPublishableStall reviewCycleDiagnosisPattern = "terminal_non_publishable_stall"
	// reviewCycleDiagnosisReviewGateWedge: a reviewer whose launch was
	// interrupted (e.g. a restart mid-hard-gate) is still in
	// StateInitializing/StateReviewGate with no runtime session past its
	// stall window. Previously this was permanent -- pollActiveAgent
	// (internal/app.go) intercepted any reviewer with a non-nil
	// ReviewCycle before reconcilePendingReviewLaunch (the only code path
	// that resumes/terminalizes a stalled pre-gate launch) ever ran, so
	// recovery could never happen; fixed in issue #158 by gating that
	// pollActiveAgent branch on StateWorking. Seeing this pattern now
	// means recovery is attempted but not completing within the stall
	// window, not that it structurally cannot happen. See
	// docs/TROUBLESHOOTING.md.
	reviewCycleDiagnosisReviewGateWedge reviewCycleDiagnosisPattern = "review_gate_wedge_after_restart"
	// reviewCycleDiagnosisPersistedStateInvalid: the persisted ReviewCycle
	// fails its own structural validation, so every future mutation
	// attempt will hit the identical rollback and it can never advance
	// (e.g. issue #128's orphaned-verification-assignment class).
	reviewCycleDiagnosisPersistedStateInvalid reviewCycleDiagnosisPattern = "persisted_convergence_invalid"
	// reviewCycleDiagnosisBudgetExhausted: a convergence budget (agent
	// count, wall time, or usage) has been exhausted
	// (ReviewCycleState.LimitTransition != nil), and the reviewer remains
	// active awaiting the coordinator's cleanup. Never publishable
	// (ApprovalEligible rejects any cycle with a LimitTransition; see
	// classifyReviewCycleResult's partial/no-review classification).
	reviewCycleDiagnosisBudgetExhausted reviewCycleDiagnosisPattern = "budget_exhausted"
	// reviewCycleDiagnosisUnknownStall: none of the known patterns match,
	// but the cycle has been quiet longer than the current phase should
	// plausibly take. This is the catch-all for a genuinely novel stall.
	reviewCycleDiagnosisUnknownStall reviewCycleDiagnosisPattern = "unknown_stall"
)

// reviewCycleDiagnosisFallbackQuietWindow bounds the "nothing in particular
// is happening yet" gap between settled phases (e.g. a round just settled
// and the next hasn't started, or discovery just completed and convergence
// hasn't begun) -- these transitions are synchronous and fast in the
// scheduler, so a generous fixed window here is meant to catch a genuine
// stall, not to model real expected latency.
const reviewCycleDiagnosisFallbackQuietWindow = 2 * time.Minute

// reviewCycleCleanupGraceWindow bounds how long a reviewer may plausibly
// remain active after its ReviewCycle reached a state that requires the
// coordinator to retire it (a terminal, never-publishable decision, or an
// already-published verdict) before that itself counts as stuck. This is
// a separate constant from reviewCycleDiagnosisFallbackQuietWindow even
// though they currently share a value: one bounds an internal
// housekeeping step, the other bounds a scheduler quiet gap, and they
// should be free to diverge independently later.
const reviewCycleCleanupGraceWindow = 2 * time.Minute

// reviewCycleVerdictPublicationQuietWindow bounds how long a cycle that has
// already reached a terminal convergence decision may plausibly sit
// awaiting verdict publication (a GitHub API call plus its own retry
// backoff, not a multi-minute worker timeout) before that itself counts as
// stuck.
const reviewCycleVerdictPublicationQuietWindow = 5 * time.Minute

// reviewCycleDiagnosis is the result of diagnoseReviewCycle: one of the
// known patterns above, or Healthy with a compact human-readable progress
// summary. Headline is the word status renders when Healthy is true (set
// by whichever code path constructs the diagnosis, so there is one place
// that decides it, not a second switch elsewhere trying to stay in sync);
// it's ignored when Healthy is false, since STUCK diagnoses always render
// with their Pattern name instead.
type reviewCycleDiagnosis struct {
	Pattern  reviewCycleDiagnosisPattern
	Healthy  bool
	Headline string
	Detail   string
}

// reviewCycleAwaitingCleanupDiagnosis is the shared shape for every
// pattern above that means "this ReviewCycle has reached a state where it
// will never advance further on its own -- it needs the coordinator to
// retire the reviewer, and nothing else is going to happen first." All
// such patterns behave identically: report plainly while the wait is
// still plausibly brief, escalate to STUCK once it exceeds
// reviewCycleCleanupGraceWindow.
func reviewCycleAwaitingCleanupDiagnosis(
	pattern reviewCycleDiagnosisPattern,
	elapsed time.Duration,
	statusPhrase string,
) reviewCycleDiagnosis {
	if elapsed > reviewCycleCleanupGraceWindow {
		return reviewCycleDiagnosis{
			Pattern: pattern,
			Detail: fmt.Sprintf(
				"%s %s ago but the reviewer was never retired -- the coordinator's cleanup either hasn't run or already failed",
				statusPhrase, formatElapsedDuration(elapsed),
			),
		}
	}
	return reviewCycleDiagnosis{
		Pattern:  pattern,
		Healthy:  true,
		Headline: "awaiting cleanup",
		Detail:   fmt.Sprintf("%s, awaiting coordinator cleanup", statusPhrase),
	}
}

// diagnoseReviewCycle checks a reviewer's ReviewCycle against every known
// structural failure signature, in order, first match wins. Each check is
// either a direct re-derivation of an existing, already-authoritative
// signal (running the real validator, reading the real terminal-decision
// field) rather than a heuristic guess, or -- only as the final fallback --
// a policy-derived "this has been quiet longer than this phase should take"
// bound.
func diagnoseReviewCycle(agent Agent, now time.Time) reviewCycleDiagnosis {
	if agent.Role != RoleReviewer {
		return reviewCycleDiagnosis{Pattern: reviewCycleDiagnosisHealthy, Healthy: true}
	}
	cycle := agent.ReviewCycle

	if (agent.State == StateInitializing || agent.State == StateReviewGate) &&
		strings.TrimSpace(agent.RuntimeHandle.Session) == "" &&
		cycle != nil && !reviewCycleHasTerminalVerdict(cycle) {
		stallWindow := reviewInitStallTimeout
		if agent.State == StateReviewGate {
			stallWindow = reviewLaunchStallTimeout
		}
		if elapsed := now.Sub(agent.LastActivityTime); elapsed > stallWindow {
			return reviewCycleDiagnosis{
				Pattern: reviewCycleDiagnosisReviewGateWedge,
				Detail: fmt.Sprintf(
					"stuck in %s for %s with no runtime session yet (see docs/TROUBLESHOOTING.md: "+
						"\"Reviewer wedged in StateReviewGate after a restart mid-hard-gate\")",
					agent.State, formatElapsedDuration(elapsed),
				),
			}
		}
	}

	if cycle == nil {
		return reviewCycleDiagnosis{Pattern: reviewCycleDiagnosisHealthy, Healthy: true}
	}

	// Staleness is a deliberate, well-understood terminal condition the
	// rest of the workflow already treats specially -- check it before
	// anything else so a retired-but-not-yet-cleaned-up cycle is reported
	// as such, not as "healthy" (misleading: nothing is progressing) or
	// routed into checks meant for cycles that are still supposed to be
	// advancing.
	if cycle.Stale {
		detail := fmt.Sprintf(
			"head SHA advanced past this reviewer's pinned %s; retired, awaiting cleanup in favor of a fresh reviewer",
			abbreviateSHA(cycle.HeadSHA),
		)
		if strings.TrimSpace(cycle.SupersededByHeadSHA) != "" {
			detail = fmt.Sprintf(
				"superseded by %s (was reviewing %s); retired, awaiting cleanup in favor of a fresh reviewer",
				abbreviateSHA(cycle.SupersededByHeadSHA), abbreviateSHA(cycle.HeadSHA),
			)
		}
		return reviewCycleDiagnosis{
			Pattern:  reviewCycleDiagnosisSuperseded,
			Healthy:  true,
			Headline: "superseded",
			Detail:   detail,
		}
	}

	if err := validatePersistedReviewCycleSnapshot(cycle); err != nil {
		return reviewCycleDiagnosis{
			Pattern: reviewCycleDiagnosisPersistedStateInvalid,
			Detail: fmt.Sprintf(
				"persisted review cycle is structurally invalid and cannot advance: %s",
				err.Error(),
			),
		}
	}

	// An intentional pause explains any amount of silence in every check
	// below this point on its own -- but only below this point: staleness
	// and structural validity are facts about the cycle's data, true or
	// not regardless of pause state, and must never be hidden behind
	// "paused" (a paused-but-invalid cycle is still invalid, and will
	// still be unable to advance once unpaused).
	if agent.Paused {
		return reviewCycleDiagnosis{
			Pattern:  reviewCycleDiagnosisPaused,
			Healthy:  true,
			Headline: "paused",
			Detail:   "reviewer is paused by an operator (agent unpause to resume)",
		}
	}

	// A limit blocks approval, but accepted work still produces a partial
	// report. Only a limit reached before any trusted result awaits cleanup
	// without publication.
	if cycle.LimitTransition != nil &&
		!reviewCycleHasPublishableReport(cycle) {
		limit := cycle.LimitTransition
		status := fmt.Sprintf("%s budget exhausted (actual=%d maximum=%d %s)", limit.Kind, limit.Actual, limit.Maximum, limit.Unit)
		return reviewCycleAwaitingCleanupDiagnosis(reviewCycleDiagnosisBudgetExhausted, now.Sub(agent.LastActivityTime), status)
	}

	// reviewCycleHasTerminalVerdict is also true whenever LimitTransition is
	// set (handled above); reaching here means it's true for one of the
	// other terminal convergence statuses instead. Not every terminal
	// status is publishable: only a terminal cycle with no trusted result
	// fails reviewCycleHasPublishableReport, and it needs the same
	// coordinator cleanup as a budget exhaustion, not a verdict
	// publication (see issue #131).
	if reviewCycleHasTerminalVerdict(cycle) {
		if !reviewCycleHasPublishableReport(cycle) {
			status := "a terminal decision"
			if cycle.Convergence != nil {
				status = fmt.Sprintf("a terminal, non-publishable decision (%s)", cycle.Convergence.Status)
			}
			return reviewCycleAwaitingCleanupDiagnosis(reviewCycleDiagnosisTerminalNonPublishableStall, now.Sub(agent.LastActivityTime), status)
		}

		// Publishable, but is a verdict actually still needed?
		// publishReviewCycleVerdict may have already succeeded
		// (VerdictPublication.Status == Published) while the reviewer
		// itself remains active until a later step retires it -- that
		// needs the same coordinator-cleanup treatment, not "awaiting
		// verdict publication" (which would contradict the cycle's own
		// persisted publication state).
		if cycle.VerdictPublication != nil &&
			cycle.VerdictPublication.Status == ReviewVerdictPublicationPublished ||
			!reviewCycleNeedsReportPublication(cycle) {
			return reviewCycleAwaitingCleanupDiagnosis(reviewCycleDiagnosisVerdictPublishedAwaitingCleanup, now.Sub(agent.LastActivityTime), "verdict was already published")
		}

		detail := fmt.Sprintf("convergence reached a terminal decision (%s), awaiting verdict publication", cycle.Convergence.Status)
		if elapsed := now.Sub(agent.LastActivityTime); elapsed > reviewCycleVerdictPublicationQuietWindow {
			return reviewCycleDiagnosis{
				Pattern: reviewCycleDiagnosisAwaitingVerdictPublication,
				Detail: fmt.Sprintf(
					"%s, but quiet for %s past the ~%s this should plausibly take -- publication itself may be stuck",
					detail, formatElapsedDuration(elapsed), formatElapsedDuration(reviewCycleVerdictPublicationQuietWindow),
				),
			}
		}
		return reviewCycleDiagnosis{Pattern: reviewCycleDiagnosisAwaitingVerdictPublication, Healthy: true, Headline: "awaiting verdict", Detail: detail}
	}

	if elapsed, expected := now.Sub(agent.LastActivityTime), reviewCycleExpectedMaxQuietDuration(cycle); elapsed > expected {
		return reviewCycleDiagnosis{
			Pattern: reviewCycleDiagnosisUnknownStall,
			Detail: fmt.Sprintf(
				"quiet for %s, past the ~%s this phase should plausibly take, and no known pattern matched",
				formatElapsedDuration(elapsed), formatElapsedDuration(expected),
			),
		}
	}

	return reviewCycleDiagnosis{
		Pattern: reviewCycleDiagnosisHealthy,
		Healthy: true,
		Detail:  summarizeReviewCycleProgress(cycle),
	}
}

// reviewCycleExpectedMaxQuietDuration approximates the maximum time the
// current phase of cycle should plausibly take, from policy alone, so a
// health check can compare against actual elapsed quiet time rather than a
// guessed constant. This deliberately does not attempt to exactly replicate
// the scheduler's own timeout computation (reviewWorkerRuntimeTimeoutAt,
// which additionally depends on live worker ownership state) -- it only
// needs to be roughly right to separate "still plausible" from "long past
// due," and errs generous (an overestimate delays a legitimate diagnosis,
// never produces a false alarm).
func reviewCycleExpectedMaxQuietDuration(cycle *ReviewCycleState) time.Duration {
	swarmAttempts := max(1, cycle.Policy.Swarm.Retries+1)
	// A discovery pass is a parallel lane batch followed by a strictly
	// sequential synthesis lane (see review_budget_advisor.go's identical
	// reasoning), so it can cost up to two full timeout windows.
	discoveryBudget := time.Duration(cycle.Policy.Swarm.TimeoutMinutes) *
		time.Duration(swarmAttempts) * 2 * time.Minute

	verificationAttempts := max(1, cycle.Policy.Verification.Retries+1)
	verificationBudget := time.Duration(cycle.Policy.Verification.TimeoutMinutes) *
		time.Duration(verificationAttempts) * time.Minute

	if len(cycle.DiscoveryPasses) == 0 {
		return discoveryBudget
	}
	latestPass := cycle.DiscoveryPasses[len(cycle.DiscoveryPasses)-1]
	if latestPass.CompletedAt.IsZero() {
		return discoveryBudget
	}
	if cycle.Convergence == nil || len(cycle.Convergence.Rounds) == 0 {
		return reviewCycleDiagnosisFallbackQuietWindow
	}
	latestRound := cycle.Convergence.Rounds[len(cycle.Convergence.Rounds)-1]
	if latestRound.Outcome != ReviewConvergenceRoundActive {
		return reviewCycleDiagnosisFallbackQuietWindow
	}
	// An active round may be running fresh (re)discovery or verification/
	// challenge workers (which share the Swarm timeout bucket); use the
	// larger of the two plausible budgets.
	if discoveryBudget > verificationBudget {
		return discoveryBudget
	}
	return verificationBudget
}

// summarizeReviewCycleProgress renders a compact, single-line progress
// description for a healthy cycle -- enough to see real numbers (which
// round, how many findings, how much is still in flight) without the full
// verbosity of formatReviewCyclesStatus.
func summarizeReviewCycleProgress(cycle *ReviewCycleState) string {
	if len(cycle.DiscoveryPasses) == 0 {
		return "discovery not yet started"
	}
	latestPass := cycle.DiscoveryPasses[len(cycle.DiscoveryPasses)-1]
	if latestPass.CompletedAt.IsZero() {
		done := 0
		for _, lane := range latestPass.Lanes {
			if lane.Status == ReviewDiscoveryLaneCompleted || lane.Status == ReviewDiscoveryLaneFailed {
				done++
			}
		}
		return fmt.Sprintf("discovery pass %d running (%d/%d lanes done)", latestPass.Pass, done, len(latestPass.Lanes))
	}
	if cycle.Convergence == nil || len(cycle.Convergence.Rounds) == 0 {
		return fmt.Sprintf("%d finding(s) discovered, convergence not started", len(cycle.CanonicalFindings))
	}
	round := cycle.Convergence.Rounds[len(cycle.Convergence.Rounds)-1]
	activeVerifications := 0
	for _, assignment := range cycle.Convergence.VerificationAssignments {
		// A superseded assignment can still carry a live Queued/Running
		// status (its worker, if any, hasn't reported back yet), but it is
		// retired -- its own eventual completion will be rejected as a
		// conflict (see reviewVerificationAssignmentMatchesCurrentCandidate)
		// and every other consumer in review_convergence.go already
		// excludes it from "active" accounting. Match that here so this
		// summary doesn't overcount work that has already moved on.
		if !assignment.Superseded &&
			(assignment.Status == ReviewVerificationQueued || assignment.Status == ReviewVerificationRunning) {
			activeVerifications++
		}
	}
	activeChallenges := 0
	for _, assignment := range cycle.Convergence.ChallengeAssignments {
		if !assignment.Superseded &&
			(assignment.Status == ReviewChallengeQueued || assignment.Status == ReviewChallengeRunning) {
			activeChallenges++
		}
	}
	return fmt.Sprintf(
		"round %d/%d (%s), %d finding(s), %d verification(s) + %d challenge(s) active",
		round.Round, cycle.Policy.Convergence.MaxRounds, round.Outcome,
		len(cycle.CanonicalFindings), activeVerifications, activeChallenges,
	)
}

// formatReviewCycleDiagnosisLine renders one reviewer's diagnosis as a
// single line, identified by PR/issue so it stays unambiguous with
// multiple reviews active at once.
func formatReviewCycleDiagnosisLine(agent Agent, diagnosis reviewCycleDiagnosis) string {
	label := fmt.Sprintf("reviewer=%s", agent.ID)
	if agent.PRNumber > 0 {
		label = fmt.Sprintf("pr=#%d %s", agent.PRNumber, label)
	}
	if diagnosis.Healthy {
		headline := diagnosis.Headline
		if headline == "" {
			headline = "healthy"
		}
		return fmt.Sprintf("  %s: %s -- %s", label, headline, diagnosis.Detail)
	}
	return fmt.Sprintf("  %s: STUCK (%s) -- %s", label, diagnosis.Pattern, diagnosis.Detail)
}

// reviewCycleDiagnosisRecord is the tracked "last checked" state for a
// reviewer's health diagnosis. It must carry both the pattern and whether
// that check was healthy, not just the pattern alone: several patterns
// (e.g. budget_exhausted, awaiting_verdict_publication) are reported while
// still healthy during their grace window and later, unchanged in name,
// once that window elapses and they become STUCK. Comparing pattern alone
// would treat that transition as "no change" and silently skip the
// notification for a reviewer that just became genuinely stuck.
type reviewCycleDiagnosisRecord struct {
	Pattern reviewCycleDiagnosisPattern
	Healthy bool
}

// reviewCycleLastDiagnosisRecord/setReviewCycleLastDiagnosisRecord follow
// the same lazy-init, mutex-guarded map convention as
// getReviewGateStatus/setReviewGateStatus above.
func (b *Orchestrator) reviewCycleLastDiagnosisRecord(agentID string) (reviewCycleDiagnosisRecord, bool) {
	b.reviewCycleDiagnosisMu.Lock()
	defer b.reviewCycleDiagnosisMu.Unlock()
	record, ok := b.reviewCycleLastDiagnosis[agentID]
	return record, ok
}

func (b *Orchestrator) setReviewCycleLastDiagnosisRecord(agentID string, record reviewCycleDiagnosisRecord) {
	b.reviewCycleDiagnosisMu.Lock()
	defer b.reviewCycleDiagnosisMu.Unlock()
	if b.reviewCycleLastDiagnosis == nil {
		b.reviewCycleLastDiagnosis = make(map[string]reviewCycleDiagnosisRecord)
	}
	b.reviewCycleLastDiagnosis[agentID] = record
}

// pruneReviewCycleDiagnosisTracking drops tracked reviewers that are no
// longer active, so this map doesn't grow without bound over the life of
// a long-running orchestrator process.
func (b *Orchestrator) pruneReviewCycleDiagnosisTracking(activeReviewerIDs map[string]struct{}) {
	b.reviewCycleDiagnosisMu.Lock()
	defer b.reviewCycleDiagnosisMu.Unlock()
	for id := range b.reviewCycleLastDiagnosis {
		if _, ok := activeReviewerIDs[id]; !ok {
			delete(b.reviewCycleLastDiagnosis, id)
		}
	}
}

// checkReviewCycleHealth runs diagnoseReviewCycle for every active
// reviewer and, on an edge transition (a diagnosis pattern different from
// the last one observed for that reviewer -- including first observation
// of a non-healthy pattern), logs it and notifies. It never re-notifies
// every poll tick for a diagnosis that hasn't changed, so a genuinely
// long-lived stall doesn't spam. reviewCycleDiagnosisUnknownStall
// additionally triggers an automatic tech-support capture the moment it's
// first observed for a reviewer, since by definition nothing else here
// already explains what's happening -- capturing state immediately, before
// any mitigation, is exactly the discipline docs/TROUBLESHOOTING.md asks
// for, applied automatically instead of depending on a human noticing.
func (b *Orchestrator) checkReviewCycleHealth(ctx context.Context, agents []Agent, now time.Time) {
	activeReviewerIDs := make(map[string]struct{})
	for _, agent := range agents {
		if agent.Role != RoleReviewer {
			continue
		}
		activeReviewerIDs[agent.ID] = struct{}{}
		diagnosis := diagnoseReviewCycle(agent, now)
		previous, hadPrevious := b.reviewCycleLastDiagnosisRecord(agent.ID)
		b.setReviewCycleLastDiagnosisRecord(agent.ID, reviewCycleDiagnosisRecord{
			Pattern: diagnosis.Pattern,
			Healthy: diagnosis.Healthy,
		})
		if diagnosis.Healthy {
			continue
		}
		if hadPrevious && !previous.Healthy && previous.Pattern == diagnosis.Pattern {
			// Already reported this exact reviewer as STUCK on this same
			// pattern; suppress the repeat notification. A transition into
			// STUCK from healthy (even under the same pattern name) or from
			// a different STUCK pattern always falls through and notifies.
			continue
		}
		line := formatReviewCycleDiagnosisLine(agent, diagnosis)
		log.Printf("review cycle health transition: %s", strings.TrimSpace(line))
		b.notify(ctx, fmt.Sprintf(
			"Repository Agent Orchestrator: %s",
			strings.TrimSpace(line),
		))
		if diagnosis.Pattern == reviewCycleDiagnosisUnknownStall {
			bundlePath, secretScan, err := b.generateTechSupportBundle()
			if err != nil {
				log.Printf(
					"automatic tech-support capture failed for unmatched stall agent=%s: %s",
					agent.ID, b.safeError(err),
				)
				continue
			}
			log.Printf(
				"automatic tech-support bundle captured for unmatched stall agent=%s: %s",
				agent.ID, bundlePath,
			)
			if secretScan.AnyMatches() {
				log.Printf(
					"automatic tech-support bundle for agent=%s may contain secrets: %s",
					agent.ID, strings.ReplaceAll(strings.TrimSpace(formatTechSupportSecretScanWarning(secretScan)), "\n", "; "),
				)
			}
		}
	}
	b.pruneReviewCycleDiagnosisTracking(activeReviewerIDs)
}
