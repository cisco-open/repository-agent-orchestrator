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
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v90/github"
)

type reviewCoordinatorGit interface {
	Output(
		ctx context.Context,
		dir string,
		args ...string,
	) ([]byte, error)
}

type osReviewCoordinatorGit struct{}

func (osReviewCoordinatorGit) Output(
	ctx context.Context,
	dir string,
	args ...string,
) ([]byte, error) {
	return outputCommand(ctx, dir, "git", args...)
}

type reviewCoordinatorPullRequestGetter interface {
	Get(
		ctx context.Context,
		owner string,
		repo string,
		number int,
	) (*github.PullRequest, *github.Response, error)
}

type reviewCoordinatorBoundary struct {
	BaseSHA string
	HeadSHA string
}

var errConvergentReviewPullRequestTerminal = errors.New(
	"convergent review pull request is closed or merged",
)

var errReviewBaseChanged = errors.New("review base SHA changed")

type convergentReviewPullRequestTerminalError struct {
	prNumber int
	merged   bool
}

func (err *convergentReviewPullRequestTerminalError) Error() string {
	if err == nil {
		return errConvergentReviewPullRequestTerminal.Error()
	}
	state := "closed"
	if err.merged {
		state = "merged"
	}
	return fmt.Sprintf(
		"convergent review PR #%d is %s",
		err.prNumber,
		state,
	)
}

func (err *convergentReviewPullRequestTerminalError) Unwrap() error {
	return errConvergentReviewPullRequestTerminal
}

// RunConvergentReviewCycle is the single production entry point for starting
// or resuming an exact-SHA convergent review.
func (b *Orchestrator) RunConvergentReviewCycle(
	ctx context.Context,
	reviewerID string,
) (ReviewCycleState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ReviewCycleState{}, err
	}
	if b == nil || b.agents == nil {
		return ReviewCycleState{}, errors.New(
			"convergent review coordinator is not configured",
		)
	}
	reviewerID = strings.TrimSpace(reviewerID)
	if !b.beginConvergentReviewCoordinatorRun(reviewerID) {
		return ReviewCycleState{}, fmt.Errorf(
			"convergent review coordinator %q is already running",
			reviewerID,
		)
	}
	defer b.finishConvergentReviewCoordinatorRun(reviewerID)

	reviewer, boundary, err := b.validateConvergentReviewBoundary(
		ctx,
		reviewerID,
	)
	if err != nil {
		var staleErr *reviewCycleStaleError
		if errors.As(err, &staleErr) {
			invalidateErr := b.invalidateStaleReviewCycle(
				ctx,
				staleErr.reviewerID,
				staleErr.liveHead,
			)
			return b.reviewCycleSnapshot(
				reviewerID,
				errors.Join(err, invalidateErr),
			)
		}
		var terminalErr *convergentReviewPullRequestTerminalError
		if errors.As(err, &terminalErr) {
			_, reconcileErr :=
				b.reconcileConvergentReviewPullRequest(
					ctx,
					reviewerID,
				)
			return b.reviewCycleSnapshot(
				reviewerID,
				errors.Join(err, reconcileErr),
			)
		}
		return ReviewCycleState{}, err
	}
	b.logReviewCycleObservation(reviewer.ID, "cycle_running")
	var failures []error
	if reviewer.ReviewCycle.Inputs == nil {
		if err := b.syncConvergentReviewGitObjects(
			ctx,
			reviewer.WorktreePath,
			boundary.BaseSHA,
			boundary.HeadSHA,
		); err != nil {
			return ReviewCycleState{}, fmt.Errorf(
				"failed to synchronize exact-SHA review boundary: %w",
				err,
			)
		}
		inputs, err := collectReviewPlanInputs(
			ctx,
			reviewer.WorktreePath,
			boundary.BaseSHA,
			boundary.HeadSHA,
			reviewer.IssueBody,
		)
		if err != nil {
			return ReviewCycleState{}, fmt.Errorf(
				"failed to collect exact-SHA review-plan inputs: %w",
				err,
			)
		}
		if err := b.persistReviewPlanInputs(reviewer.ID, inputs); err != nil {
			return ReviewCycleState{}, err
		}
		var ok bool
		reviewer, ok = b.agents.Get(reviewer.ID)
		if !ok || reviewer.ReviewCycle == nil ||
			reviewer.ReviewCycle.Inputs == nil {
			return ReviewCycleState{}, errors.New(
				"convergent review coordinator disappeared after input planning",
			)
		}
	}
	if reviewer.ReviewCycle.Inputs.BaseSHA != boundary.BaseSHA {
		return ReviewCycleState{}, fmt.Errorf(
			"%w: convergent review base SHA mismatch: persisted=%s live=%s",
			errReviewBaseChanged,
			abbreviateSHA(reviewer.ReviewCycle.Inputs.BaseSHA),
			abbreviateSHA(boundary.BaseSHA),
		)
	}
	if reviewer.ReviewCycle.Plan == nil {
		plan, err := buildReviewPlan(
			*reviewer.ReviewCycle.Inputs,
			reviewer.ReviewCycle.Policy,
		)
		if err != nil {
			return ReviewCycleState{}, fmt.Errorf(
				"failed to build exact-SHA review plan: %w",
				err,
			)
		}
		if err := b.persistReviewPlan(reviewer.ID, plan); err != nil {
			return b.reviewCycleSnapshot(reviewer.ID, err)
		}
	}

	reviewer, _ = b.agents.Get(reviewer.ID)
	priorLedger := reviewer.ReviewCycle.PriorReviewLedger
	ledgerHeadAlreadyRecorded := priorLedger != nil &&
		priorLedger.HeadSHA == boundary.HeadSHA
	if reviewer.ReviewCycle.ReviewLedgerInputs == nil &&
		!ledgerHeadAlreadyRecorded {
		ledgerInputs := *reviewer.ReviewCycle.Inputs
		ledgerPlan := *reviewer.ReviewCycle.Plan
		if prior := reviewer.ReviewCycle.PriorReviewLedger; prior != nil {
			if err := b.syncConvergentReviewGitObjects(
				ctx,
				reviewer.WorktreePath,
				prior.HeadSHA,
				boundary.HeadSHA,
			); err != nil {
				return ReviewCycleState{}, fmt.Errorf(
					"failed to synchronize cross-SHA review ledger boundary: %w",
					err,
				)
			}
			ledgerInputs, err = collectReviewPlanInputs(
				ctx,
				reviewer.WorktreePath,
				prior.HeadSHA,
				boundary.HeadSHA,
				reviewer.IssueBody,
			)
			if err != nil {
				return ReviewCycleState{}, fmt.Errorf(
					"failed to collect cross-SHA review ledger delta: %w",
					err,
				)
			}
			ledgerPlan, err = buildReviewPlan(ledgerInputs, reviewer.ReviewCycle.Policy)
			if err != nil {
				return ReviewCycleState{}, fmt.Errorf(
					"failed to build cross-SHA review ledger plan: %w",
					err,
				)
			}
		}
		if err := b.persistReviewLedgerCycleContext(
			reviewer.ID,
			ledgerInputs,
			ledgerPlan,
		); err != nil {
			return b.reviewCycleSnapshot(reviewer.ID, err)
		}
	}

	current, ok := b.agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		return ReviewCycleState{}, errors.New(
			"convergent review coordinator disappeared after planning",
		)
	}
	if reviewCycleHasTerminalVerdict(current.ReviewCycle) {
		return b.publishAndSnapshotConvergentReview(
			ctx,
			current.ID,
			failures,
		)
	}

	recoveryWork, err := collectReviewCycleRecoveryWork(current.ReviewCycle)
	if err != nil {
		return ReviewCycleState{}, err
	}
	if len(recoveryWork) != 0 ||
		reviewCycleHasActiveRound(current.ReviewCycle) {
		if err := b.recoverPersistedReviewCycle(
			ctx,
			current.ID,
		); err != nil {
			failures = append(failures, err)
		}
		current, ok = b.agents.Get(current.ID)
		if !ok || current.ReviewCycle == nil {
			return ReviewCycleState{}, errors.Join(
				append(
					failures,
					errors.New(
						"convergent review coordinator disappeared during recovery",
					),
				)...,
			)
		}
		if len(failures) != 0 {
			return b.reviewCycleSnapshot(
				current.ID,
				errors.Join(failures...),
			)
		}
	}

	if len(current.ReviewCycle.DiscoveryPasses) == 0 {
		if _, err := b.runReviewDiscoveryPass(
			ctx,
			current.ID,
			1,
		); err != nil {
			failures = append(failures, err)
		}
		current, ok = b.agents.Get(current.ID)
		if !ok || current.ReviewCycle == nil {
			return ReviewCycleState{}, errors.Join(
				append(
					failures,
					errors.New(
						"convergent review coordinator disappeared after discovery",
					),
				)...,
			)
		}
		if len(failures) != 0 {
			return b.failAndSnapshotConvergentReview(
				ctx,
				current,
				failures,
			)
		}
	}
	latest, hasLatest := latestReviewDiscoveryPass(current.ReviewCycle)
	if !hasLatest || latest.CompletedAt.IsZero() {
		return b.reviewCycleSnapshot(
			current.ID,
			errors.Join(
				append(
					failures,
					errors.New(
						"convergent review discovery did not reach a durable terminal pass",
					),
				)...,
			),
		)
	}
	if reviewCycleHasTerminalVerdict(current.ReviewCycle) {
		return b.publishAndSnapshotConvergentReview(
			ctx,
			current.ID,
			failures,
		)
	}

	for ctx.Err() == nil {
		current, ok = b.agents.Get(current.ID)
		if !ok || current.ReviewCycle == nil {
			return ReviewCycleState{}, errors.Join(
				append(
					failures,
					errors.New(
						"convergent review coordinator disappeared before rediscovery",
					),
				)...,
			)
		}
		passCount := len(current.ReviewCycle.DiscoveryPasses)
		roundCount := 0
		if current.ReviewCycle.Convergence != nil {
			roundCount = len(current.ReviewCycle.Convergence.Rounds)
		}
		if passCount == roundCount {
			if _, err := b.runReviewDiscoveryPass(
				ctx,
				current.ID,
				passCount+1,
			); err != nil {
				failures = append(failures, err)
			}
		} else if passCount != roundCount+1 {
			failures = append(failures, fmt.Errorf(
				"convergent review discovery/round sequence is invalid: passes=%d rounds=%d",
				passCount,
				roundCount,
			))
			return b.failAndSnapshotConvergentReview(
				ctx,
				current,
				failures,
			)
		}
		current, ok = b.agents.Get(current.ID)
		if !ok || current.ReviewCycle == nil {
			return ReviewCycleState{}, errors.Join(
				append(
					failures,
					errors.New(
						"convergent review coordinator disappeared after rediscovery",
					),
				)...,
			)
		}
		if len(failures) != 0 {
			return b.failAndSnapshotConvergentReview(
				ctx,
				current,
				failures,
			)
		}
		latest, hasLatest = latestReviewDiscoveryPass(current.ReviewCycle)
		if !hasLatest || latest.CompletedAt.IsZero() {
			failures = append(
				failures,
				errors.New(
					"convergent review discovery did not reach a durable terminal pass",
				),
			)
			return b.failAndSnapshotConvergentReview(
				ctx,
				current,
				failures,
			)
		}
		if _, err := b.runReviewConvergentRound(
			ctx,
			current.ID,
		); err != nil {
			failures = append(failures, err)
		}
		current, ok = b.agents.Get(current.ID)
		if !ok || current.ReviewCycle == nil {
			return ReviewCycleState{}, errors.Join(
				append(
					failures,
					errors.New(
						"convergent review coordinator disappeared during convergence",
					),
				)...,
			)
		}
		if len(failures) != 0 {
			return b.failAndSnapshotConvergentReview(
				ctx,
				current,
				failures,
			)
		}
		if reviewCycleHasTerminalVerdict(current.ReviewCycle) {
			return b.publishAndSnapshotConvergentReview(
				ctx,
				current.ID,
				failures,
			)
		}
	}
	failures = append(failures, ctx.Err())
	return b.reviewCycleSnapshot(
		current.ID,
		errors.Join(failures...),
	)
}

func (b *Orchestrator) syncConvergentReviewGitObjects(
	ctx context.Context,
	repositoryPath string,
	objectIDs ...string,
) error {
	if b == nil || b.reviewCoordinatorGit == nil {
		return errors.New("convergent review Git boundary is not configured")
	}
	repositoryPath = strings.TrimSpace(repositoryPath)
	if repositoryPath == "" {
		return errors.New("convergent review repository path is empty")
	}

	unique := make([]string, 0, len(objectIDs))
	seen := make(map[string]struct{}, len(objectIDs))
	for _, objectID := range objectIDs {
		objectID = strings.TrimSpace(objectID)
		if err := validateCanonicalGitObjectID(objectID); err != nil {
			return fmt.Errorf("convergent review Git object ID is invalid: %w", err)
		}
		if _, ok := seen[objectID]; ok {
			continue
		}
		seen[objectID] = struct{}{}
		unique = append(unique, objectID)
	}
	if len(unique) == 0 {
		return errors.New("convergent review Git boundary is empty")
	}

	args := []string{
		"fetch",
		"--no-tags",
		"--no-write-fetch-head",
		"--no-recurse-submodules",
		"origin",
	}
	args = append(args, unique...)
	if _, err := b.reviewCoordinatorGit.Output(
		ctx,
		repositoryPath,
		args...,
	); err != nil {
		return fmt.Errorf("failed to fetch exact review Git objects: %w", err)
	}

	for _, objectID := range unique {
		if _, err := b.reviewCoordinatorGit.Output(
			ctx,
			repositoryPath,
			"cat-file",
			"-e",
			objectID+"^{commit}",
		); err != nil {
			return fmt.Errorf(
				"exact review Git object %s is unavailable after fetch: %w",
				abbreviateSHA(objectID),
				err,
			)
		}
	}
	return nil
}

func (b *Orchestrator) beginConvergentReviewCoordinatorRun(
	reviewerID string,
) bool {
	if b == nil || strings.TrimSpace(reviewerID) == "" {
		return false
	}
	b.reviewCoordinatorRunMu.Lock()
	defer b.reviewCoordinatorRunMu.Unlock()
	if b.reviewCoordinatorRuns == nil {
		b.reviewCoordinatorRuns = make(map[string]struct{})
	}
	if _, running := b.reviewCoordinatorRuns[reviewerID]; running {
		return false
	}
	b.reviewCoordinatorRuns[reviewerID] = struct{}{}
	return true
}

func (b *Orchestrator) finishConvergentReviewCoordinatorRun(
	reviewerID string,
) {
	if b == nil {
		return
	}
	b.reviewCoordinatorRunMu.Lock()
	delete(b.reviewCoordinatorRuns, strings.TrimSpace(reviewerID))
	b.reviewCoordinatorRunMu.Unlock()
}

func (b *Orchestrator) reviewCycleSnapshot(
	reviewerID string,
	err error,
) (ReviewCycleState, error) {
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok || reviewer.ReviewCycle == nil {
		return ReviewCycleState{}, errors.Join(
			err,
			errors.New("convergent review cycle snapshot is unavailable"),
		)
	}
	return *cloneReviewCycle(reviewer.ReviewCycle), err
}

func (b *Orchestrator) failAndSnapshotConvergentReview(
	ctx context.Context,
	reviewer Agent,
	failures []error,
) (ReviewCycleState, error) {
	failure := errors.Join(failures...)
	var staleErr *reviewCycleStaleError
	if errors.As(failure, &staleErr) {
		invalidateErr := b.invalidateStaleReviewCycle(
			ctx,
			staleErr.reviewerID,
			staleErr.liveHead,
		)
		return b.reviewCycleSnapshot(
			reviewer.ID,
			errors.Join(failure, invalidateErr),
		)
	}
	var terminalErr *convergentReviewPullRequestTerminalError
	if errors.As(failure, &terminalErr) {
		_, reconcileErr := b.reconcileConvergentReviewPullRequest(
			ctx,
			reviewer.ID,
		)
		return b.reviewCycleSnapshot(
			reviewer.ID,
			errors.Join(failure, reconcileErr),
		)
	}
	if reviewCycleHasTerminalVerdict(reviewer.ReviewCycle) &&
		(reviewer.ReviewCycle.LimitTransition != nil ||
			reviewCycleHasTrustedResult(reviewer.ReviewCycle)) &&
		!errors.Is(failure, errReviewBaseChanged) {
		return b.publishAndSnapshotConvergentReview(
			ctx,
			reviewer.ID,
			failures,
		)
	}
	if err := b.recordReviewIncompleteForCoder(
		ctx,
		reviewer,
		"convergent review discovery failed",
		true,
		true,
	); err != nil {
		failures = append(failures, err)
	}
	if err := b.retireReviewer(
		reviewer,
		StateErrored,
		false,
		"convergent review discovery failed",
	); err != nil {
		failures = append(failures, err)
	}
	return b.reviewCycleSnapshot(
		reviewer.ID,
		errors.Join(failures...),
	)
}

// recordReviewIncompleteForCoder durably terminalizes a coder-linked review.
// Only failures that happened before a trusted review result may schedule an
// automatic same-head retry. A completed, non-convergent review is
// deterministic for the same code and policy and must stay terminal.
func (b *Orchestrator) recordReviewIncompleteForCoder(
	ctx context.Context,
	reviewer Agent,
	reason string,
	postNotice bool,
	retryable bool,
) error {
	if reviewer.ReviewCycle == nil {
		return nil
	}
	headSHA := strings.TrimSpace(reviewer.ReviewCycle.HeadSHA)
	if headSHA == "" {
		return nil
	}
	coderID := strings.TrimSpace(reviewer.ParentAgentID)
	if coderID == "" {
		if postNotice {
			b.postReviewIncompleteComment(ctx, reviewer, headSHA, reason, nil)
		}
		return nil
	}
	retryAfter := time.Time{}
	if retryable {
		retryAfter = time.Now().UTC().Add(reviewSameHeadRetryDelay)
	}
	retry, err := b.transitionAndPersistCoderLaunchAttempt(
		coderID,
		reviewer.ReviewCycle.ID,
		DurableLaunchFailed,
		&DurableLaunchFailure{
			Kind:      DurableLaunchFailureIncomplete,
			Retryable: retryable,
		},
		retryAfter,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to terminalize review attempt for coder %s cycle %s: %w",
			coderID,
			reviewer.ReviewCycle.ID,
			err,
		)
	}
	if postNotice {
		b.postReviewIncompleteComment(ctx, reviewer, headSHA, reason, &retry)
	}
	return nil
}

func (b *Orchestrator) finalizePartialNoFindingsReview(
	ctx context.Context,
	reviewerID string,
) error {
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok {
		return nil
	}
	// postNotice is false here: publishReviewCycleVerdictLocked already
	// posted the partial-no-findings verdict comment to the PR before
	// calling this, so a second postReviewIncompleteComment would be a
	// confusing duplicate. This call exists purely to terminalize the
	// coder's DurableLaunchAttempt as non-retryable bookkeeping.
	retryErr := b.recordReviewIncompleteForCoder(
		ctx, reviewer, "", false, false,
	)
	retireErr := b.retireReviewer(
		reviewer,
		StateErrored,
		false,
		"partial review completed without mandatory coverage",
	)
	return errors.Join(retryErr, retireErr)
}

// postReviewIncompleteComment is a best-effort, public-facing notice -- it
// deliberately carries only the retire reason category, not the joined
// internal failure detail already sent to the operator via Webex, so it
// never leaks internal convergence/discovery state to the PR.
func (b *Orchestrator) postReviewIncompleteComment(
	ctx context.Context,
	reviewer Agent,
	headSHA string,
	reason string,
	retry *DurableLaunchAttempt,
) {
	if b == nil || b.github == nil || reviewer.PRNumber <= 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	body := fmt.Sprintf(
		"Repository Agent Orchestrator: automated review could not reach a "+
			"verdict for exact commit `%s` (%s). This is not a clean-review result.",
		abbreviateSHA(headSHA),
		reason,
	)
	if retry == nil {
		body += " Run another review for this exact commit to retry."
	} else if retry.RetryAfter.IsZero() {
		body += " Automatic retry is disabled for this completed result; an operator can run another review explicitly."
	} else if retry.Attempt < reviewSameHeadAttemptLimit {
		body += fmt.Sprintf(
			" Same-head review attempt %d of %d will start automatically after %s.",
			retry.Attempt+1,
			reviewSameHeadAttemptLimit,
			retry.RetryAfter.UTC().Format(time.RFC3339),
		)
	} else {
		body += fmt.Sprintf(
			" The automatic same-head limit of %d attempts is exhausted; an operator can run another review explicitly without changing Git history.",
			reviewSameHeadAttemptLimit,
		)
	}
	if _, _, err := b.github.Issues.CreateComment(
		ctx,
		b.cfg.RepoOwner,
		b.cfg.RepoName,
		reviewer.PRNumber,
		&github.IssueComment{Body: github.String(body)},
	); err != nil {
		log.Printf(
			"failed to post review-incomplete notice reviewer=%s pr=%d: %s",
			reviewer.ID,
			reviewer.PRNumber,
			b.safeError(err),
		)
	}
}

func (b *Orchestrator) publishAndSnapshotConvergentReview(
	ctx context.Context,
	reviewerID string,
	failures []error,
) (ReviewCycleState, error) {
	reviewer, ok := b.agents.Get(reviewerID)
	if ok {
		if reviewCycleHasTerminalVerdict(reviewer.ReviewCycle) &&
			strings.TrimSpace(reviewer.ParentAgentID) != "" {
			if err := b.persistCompletedReviewLedger(reviewer); err != nil {
				failures = append(failures, err)
				// Deliberately fall through instead of returning here: this
				// reviewer must still be published/retired
				// below even though its ledger update failed, or it would
				// be left permanently stuck -- nothing else ever retries
				// this exact step on its own schedule, and a deterministic
				// failure (e.g. a genuine carried-coverage conflict) would
				// reproduce identically on every future poll tick anyway.
				// The cost is confined to this round's coverage/finding
				// continuity for the coder, not correctness: the worst
				// case is a later round re-verifying something this round
				// already covered, not anything incorrect being published.
				b.notify(
					ctx,
					fmt.Sprintf(
						"Repository Agent Orchestrator: failed to persist the completed review ledger for `%s` (coder `%s`, PR %s) -- proceeding to publish/retire anyway rather than leaving the reviewer stuck, but this round's coverage/finding continuity may not have been fully recorded. Failure: %s",
						reviewer.ID,
						reviewer.ParentAgentID,
						fallback(
							strings.TrimSpace(reviewer.PRURL),
							fmt.Sprintf("#%d", reviewer.PRNumber),
						),
						b.safeError(err),
					),
				)
			}
		}
		switch {
		case reviewCycleHasPublishableReport(reviewer.ReviewCycle):
			if _, err := b.publishReviewCycleVerdict(ctx, reviewerID); err != nil {
				failures = append(failures, err)
			}
		case reviewCycleHasTerminalVerdict(reviewer.ReviewCycle):
			result, _ := reviewCycleResultForReviewer(reviewer)
			if len(failures) == 0 {
				failures = append(failures, fmt.Errorf(
					"manual review ended with result %s and no confirmed patch-caused findings",
					result,
				))
			}
			b.notify(
				ctx,
				fmt.Sprintf(
					"Repository Agent Orchestrator: manual review coordinator `%s` ended with result `%s` on PR %s and no confirmed patch-caused findings; no GitHub verdict was posted. Failure details: %s",
					reviewer.ID,
					result,
					fallback(
						strings.TrimSpace(reviewer.PRURL),
						fmt.Sprintf("#%d", reviewer.PRNumber),
					),
					b.safeError(errors.Join(failures...)),
				),
			)
			if err := b.recordReviewIncompleteForCoder(
				ctx,
				reviewer,
				"convergent review ended without a publishable verdict",
				true,
				false,
			); err != nil {
				failures = append(failures, err)
			}
			if err := b.retireReviewer(
				reviewer,
				StateErrored,
				false,
				"convergent review ended without a publishable verdict",
			); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return b.reviewCycleSnapshot(
		reviewerID,
		errors.Join(failures...),
	)
}

func (b *Orchestrator) validateConvergentReviewBoundary(
	ctx context.Context,
	reviewerID string,
) (Agent, reviewCoordinatorBoundary, error) {
	if b == nil || b.agents == nil ||
		b.reviewCoordinatorGit == nil ||
		b.reviewCoordinatorPullRequests == nil {
		return Agent{}, reviewCoordinatorBoundary{}, errors.New(
			"convergent review repository and GitHub boundaries are not configured",
		)
	}
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return Agent{}, reviewCoordinatorBoundary{}, fmt.Errorf(
			"review coordinator %q was not found",
			strings.TrimSpace(reviewerID),
		)
	}
	if reviewer.State != StateWorking || reviewer.Paused ||
		agentLifecycleTerminal(&reviewer) ||
		reviewer.ReviewCycle.Stale {
		return Agent{}, reviewCoordinatorBoundary{}, errors.New(
			"convergent review coordinator is not active",
		)
	}
	if reviewer.PRNumber <= 0 {
		return Agent{}, reviewCoordinatorBoundary{}, errors.New(
			"convergent review PR identity is missing",
		)
	}
	if reviewer.ObservedPRHeadSHA != reviewer.ReviewCycle.HeadSHA {
		return Agent{}, reviewCoordinatorBoundary{}, errors.New(
			"convergent review coordinator and cycle head SHAs do not match",
		)
	}
	if err := validatePersistedReviewCycleSnapshot(
		reviewer.ReviewCycle,
	); err != nil {
		return Agent{}, reviewCoordinatorBoundary{}, fmt.Errorf(
			"convergent review policy checkpoint is invalid: %w",
			err,
		)
	}
	repoOwner := strings.TrimSpace(b.cfg.RepoOwner)
	repoName := strings.TrimSpace(b.cfg.RepoName)
	repoPath := filepath.Clean(strings.TrimSpace(b.cfg.RepoPath))
	worktreeDir := filepath.Clean(strings.TrimSpace(b.cfg.WorktreeDir))
	if repoOwner == "" || repoName == "" ||
		repoPath == "." || worktreeDir == "." {
		return Agent{}, reviewCoordinatorBoundary{}, errors.New(
			"convergent review configured repository identity is incomplete",
		)
	}
	if !reviewWorkerPathWithin(worktreeDir, reviewer.WorktreePath) {
		return Agent{}, reviewCoordinatorBoundary{}, errors.New(
			"convergent review coordinator worktree is outside the configured worktree directory",
		)
	}

	expectedRepository := strings.ToLower(repoOwner + "/" + repoName)
	for _, repositoryPath := range []string{
		repoPath,
		reviewer.WorktreePath,
	} {
		if err := b.validateConvergentReviewRepository(
			ctx,
			repositoryPath,
			expectedRepository,
		); err != nil {
			return Agent{}, reviewCoordinatorBoundary{}, err
		}
	}
	checkedOut, err := b.reviewCoordinatorGit.Output(
		ctx,
		reviewer.WorktreePath,
		"rev-parse",
		"--verify",
		"HEAD",
	)
	if err != nil {
		return Agent{}, reviewCoordinatorBoundary{}, fmt.Errorf(
			"failed to validate convergent review coordinator checkout: %w",
			err,
		)
	}
	checkedOutSHA := strings.ToLower(strings.TrimSpace(string(checkedOut)))
	if checkedOutSHA != reviewer.ReviewCycle.HeadSHA {
		return Agent{}, reviewCoordinatorBoundary{}, fmt.Errorf(
			"convergent review checkout SHA mismatch: cycle=%s checkout=%s",
			abbreviateSHA(reviewer.ReviewCycle.HeadSHA),
			abbreviateSHA(checkedOutSHA),
		)
	}

	pullRequest, _, err := b.reviewCoordinatorPullRequests.Get(
		ctx,
		repoOwner,
		repoName,
		reviewer.PRNumber,
	)
	if err != nil {
		return Agent{}, reviewCoordinatorBoundary{}, fmt.Errorf(
			"failed to validate convergent review PR #%d: %w",
			reviewer.PRNumber,
			err,
		)
	}
	boundary, err := validateConvergentReviewPullRequestBoundary(
		reviewer,
		expectedRepository,
		pullRequest,
	)
	if err != nil {
		return Agent{}, reviewCoordinatorBoundary{}, err
	}
	if boundary.HeadSHA != reviewer.ReviewCycle.HeadSHA {
		return Agent{}, reviewCoordinatorBoundary{},
			&reviewCycleStaleError{
				reviewerID: reviewer.ID,
				staleHead:  reviewer.ReviewCycle.HeadSHA,
				liveHead:   boundary.HeadSHA,
			}
	}
	return reviewer, boundary, nil
}

func (b *Orchestrator) reconcileConvergentReviewPullRequest(
	ctx context.Context,
	reviewerID string,
) (Agent, error) {
	if b == nil || b.agents == nil {
		return Agent{}, errors.New(
			"convergent review coordinator is not configured",
		)
	}
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return Agent{}, fmt.Errorf(
			"review coordinator %q was not found",
			strings.TrimSpace(reviewerID),
		)
	}
	if reviewer.ReviewCycle.Stale || agentLifecycleTerminal(&reviewer) {
		return reviewer, nil
	}
	var pullRequests reviewCoordinatorPullRequestGetter
	switch {
	case b.reviewCoordinatorPullRequests != nil:
		pullRequests = b.reviewCoordinatorPullRequests
	case b.github != nil:
		pullRequests = b.github.PullRequests
	default:
		return Agent{}, errors.New(
			"convergent review live PR boundary is not configured",
		)
	}
	pullRequest, _, err := pullRequests.Get(
		ctx,
		strings.TrimSpace(b.cfg.RepoOwner),
		strings.TrimSpace(b.cfg.RepoName),
		reviewer.PRNumber,
	)
	if err != nil {
		return Agent{}, fmt.Errorf(
			"failed to reconcile convergent review PR #%d: %w",
			reviewer.PRNumber,
			err,
		)
	}
	expectedRepository := strings.ToLower(
		strings.TrimSpace(b.cfg.RepoOwner) + "/" +
			strings.TrimSpace(b.cfg.RepoName),
	)
	boundary, boundaryErr :=
		validateConvergentReviewPullRequestBoundary(
			reviewer,
			expectedRepository,
			pullRequest,
		)
	var terminalErr *convergentReviewPullRequestTerminalError
	if errors.As(boundaryErr, &terminalErr) {
		passDone := b.cancelReviewCycleWork(reviewer.ID)
		if passDone != nil {
			b.scheduleTerminalReviewCleanup(
				ctx,
				reviewer.ID,
				pullRequest,
				passDone,
			)
			return reviewer, nil
		}
		if err := b.cleanupTerminalConvergentReview(
			ctx,
			reviewer.ID,
			pullRequest,
		); err != nil {
			return Agent{}, err
		}
		refreshed, found := b.agents.Get(reviewer.ID)
		if !found {
			return reviewer, nil
		}
		return refreshed, nil
	}
	if boundaryErr != nil {
		return Agent{}, boundaryErr
	}
	if boundary.HeadSHA != reviewer.ReviewCycle.HeadSHA {
		if err := b.invalidateStaleReviewCycle(
			ctx,
			reviewer.ID,
			boundary.HeadSHA,
		); err != nil {
			return Agent{}, err
		}
		refreshed, found := b.agents.Get(reviewer.ID)
		if !found {
			return reviewer, nil
		}
		return refreshed, nil
	}
	return reviewer, nil
}

func (b *Orchestrator) cleanupTerminalConvergentReview(
	ctx context.Context,
	reviewerID string,
	pullRequest *github.PullRequest,
) error {
	reviewer, found := b.agents.Get(strings.TrimSpace(reviewerID))
	if !found || reviewer.Role != RoleReviewer {
		return nil
	}
	if parentID := strings.TrimSpace(
		reviewer.ParentAgentID,
	); parentID != "" {
		parent, parentFound := b.agents.Get(parentID)
		if !parentFound || parent.Role != RoleCoder {
			return fmt.Errorf(
				"failed to reconcile terminal review PR #%d: parent coder %q was not found",
				reviewer.PRNumber,
				parentID,
			)
		}
		if agentLifecycleTerminal(&parent) {
			return nil
		}
		return b.handleTerminalPRSnapshot(
			ctx,
			parent,
			pullRequest,
		)
	}
	return b.handleTerminalManualReviewerPRSnapshot(
		ctx,
		reviewer,
		pullRequest,
	)
}

func (b *Orchestrator) scheduleTerminalReviewCleanup(
	ctx context.Context,
	reviewerID string,
	pullRequest *github.PullRequest,
	passDone <-chan struct{},
) {
	if !b.beginReviewCleanup() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cleanupCtx := context.WithoutCancel(ctx)
	pullRequest = terminalPullRequestSnapshot(pullRequest)
	go func() {
		defer b.reviewCleanupWG.Done()
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		select {
		case <-passDone:
		case <-timer.C:
		}
		if err := b.cleanupTerminalConvergentReview(
			cleanupCtx,
			reviewerID,
			pullRequest,
		); err != nil {
			log.Printf(
				"non-fatal: terminal review cleanup failed reviewer=%s pr=%d: %s",
				reviewerID,
				pullRequest.GetNumber(),
				b.safeError(err),
			)
		}
	}()
}

func terminalPullRequestSnapshot(
	pullRequest *github.PullRequest,
) *github.PullRequest {
	if pullRequest == nil {
		return &github.PullRequest{}
	}
	snapshot := &github.PullRequest{
		Number: github.Int(pullRequest.GetNumber()),
		State:  github.String(pullRequest.GetState()),
		Merged: github.Bool(pullRequest.GetMerged()),
	}
	if mergedBy := pullRequest.GetMergedBy(); mergedBy != nil {
		if login := strings.TrimSpace(mergedBy.GetLogin()); login != "" {
			snapshot.MergedBy = &github.User{
				Login: github.String(login),
			}
		}
	}
	return snapshot
}

func validateConvergentReviewPullRequestBoundary(
	reviewer Agent,
	expectedRepository string,
	pullRequest *github.PullRequest,
) (reviewCoordinatorBoundary, error) {
	if pullRequest == nil ||
		(pullRequest.GetNumber() != 0 &&
			pullRequest.GetNumber() != reviewer.PRNumber) {
		return reviewCoordinatorBoundary{}, errors.New(
			"convergent review PR identity mismatch",
		)
	}
	headRepository := reviewCoordinatorRepositoryName(
		pullRequest.GetHead().GetRepo(),
	)
	baseRepository := reviewCoordinatorRepositoryName(
		pullRequest.GetBase().GetRepo(),
	)
	if headRepository != expectedRepository ||
		baseRepository != expectedRepository {
		return reviewCoordinatorBoundary{}, fmt.Errorf(
			"convergent review repository mismatch for PR #%d: configured=%s head=%s base=%s",
			reviewer.PRNumber,
			expectedRepository,
			headRepository,
			baseRepository,
		)
	}
	if pullRequest.GetMerged() ||
		strings.EqualFold(
			strings.TrimSpace(pullRequest.GetState()),
			"closed",
		) {
		return reviewCoordinatorBoundary{},
			&convergentReviewPullRequestTerminalError{
				prNumber: reviewer.PRNumber,
				merged:   pullRequest.GetMerged(),
			}
	}
	headSHA := strings.ToLower(strings.TrimSpace(
		pullRequest.GetHead().GetSHA(),
	))
	baseSHA := strings.ToLower(strings.TrimSpace(
		pullRequest.GetBase().GetSHA(),
	))
	if err := validateCanonicalGitObjectID(headSHA); err != nil {
		return reviewCoordinatorBoundary{}, fmt.Errorf(
			"convergent review PR head SHA is invalid: %w",
			err,
		)
	}
	if err := validateCanonicalGitObjectID(baseSHA); err != nil {
		return reviewCoordinatorBoundary{}, fmt.Errorf(
			"convergent review PR base SHA is invalid: %w",
			err,
		)
	}
	return reviewCoordinatorBoundary{
		BaseSHA: baseSHA,
		HeadSHA: headSHA,
	}, nil
}

func (b *Orchestrator) validateConvergentReviewRepository(
	ctx context.Context,
	repositoryPath string,
	expectedRepository string,
) error {
	if b == nil || b.reviewCoordinatorGit == nil {
		return errors.New(
			"convergent review repository boundary is not configured",
		)
	}
	origin, err := b.reviewCoordinatorGit.Output(
		ctx,
		repositoryPath,
		"remote",
		"get-url",
		"origin",
	)
	if err != nil {
		return fmt.Errorf(
			"failed to validate convergent review repository origin: %w",
			err,
		)
	}
	actualOwner, actualRepo, ok := parseCanonicalGitHubRemoteRepository(
		string(origin),
	)
	actualRepository := strings.ToLower(
		strings.TrimSpace(actualOwner) + "/" +
			strings.TrimSpace(actualRepo),
	)
	if !ok || actualRepository != expectedRepository {
		return fmt.Errorf(
			"convergent review repository mismatch: configured=%s actual=%s",
			expectedRepository,
			strings.Trim(actualRepository, "/"),
		)
	}
	return nil
}

func parseCanonicalGitHubRemoteRepository(
	raw string,
) (string, string, bool) {
	remote := strings.TrimSpace(raw)
	if remote == "" {
		return "", "", false
	}
	trimmed := strings.TrimSuffix(
		strings.TrimSuffix(remote, "/"),
		".git",
	)

	if strings.Contains(trimmed, "://") {
		parsed, err := url.Parse(trimmed)
		if err != nil ||
			!strings.EqualFold(parsed.Hostname(), "github.com") ||
			parsed.Port() != "" ||
			parsed.RawQuery != "" ||
			parsed.Fragment != "" {
			return "", "", false
		}
		switch strings.ToLower(parsed.Scheme) {
		case "https":
			if parsed.User != nil {
				return "", "", false
			}
		case "ssh":
			if parsed.User == nil ||
				parsed.User.Username() != "git" ||
				parsed.User.String() != "git" {
				return "", "", false
			}
		default:
			return "", "", false
		}
		return canonicalGitHubRepositoryPath(parsed.Path)
	}

	const scpPrefix = "git@github.com:"
	if !strings.HasPrefix(strings.ToLower(trimmed), scpPrefix) {
		return "", "", false
	}
	return canonicalGitHubRepositoryPath(trimmed[len(scpPrefix):])
}

func canonicalGitHubRepositoryPath(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 {
		return "", "", false
	}
	owner := strings.TrimSpace(parts[0])
	repository := strings.TrimSpace(parts[1])
	if owner == "" || repository == "" ||
		owner == "." || owner == ".." ||
		repository == "." || repository == ".." {
		return "", "", false
	}
	return owner, repository, true
}

func reviewCoordinatorRepositoryName(repository *github.Repository) string {
	if repository == nil {
		return ""
	}
	name := strings.TrimSpace(repository.GetFullName())
	if name == "" {
		name = strings.Trim(
			strings.TrimSpace(repository.GetOwner().GetLogin())+"/"+
				strings.TrimSpace(repository.GetName()),
			"/",
		)
	}
	return strings.ToLower(name)
}

func reviewWorkerPRNumberEnvironmentValue(prNumber int) (string, error) {
	if prNumber <= 0 {
		return "", errors.New("review worker PR number is invalid")
	}
	return strconv.Itoa(prNumber), nil
}
