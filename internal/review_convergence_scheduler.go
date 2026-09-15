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
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type reviewVerificationAssignmentRunnerFunc func(
	context.Context,
	string,
	ReviewVerificationAssignment,
) error

type reviewChallengeAssignmentRunnerFunc func(
	context.Context,
	string,
	ReviewChallengeAssignment,
) error

type reviewConvergenceMutation struct {
	reviewerID               string
	previousState            *ReviewConvergenceState
	previousLastActivityTime time.Time
	updatedAt                time.Time
}

type reviewAssignedWorkerRunError struct {
	code      ReviewVerificationFailureCode
	retryable bool
	err       error
}

type reviewWorkerRuntimeTimeoutError struct {
	timeout time.Duration
}

func (failure *reviewWorkerRuntimeTimeoutError) Error() string {
	if failure == nil || failure.timeout <= 0 {
		return "review worker runtime exceeded its deadline"
	}
	return fmt.Sprintf(
		"review worker runtime exceeded its %s deadline",
		failure.timeout,
	)
}

func (failure *reviewAssignedWorkerRunError) Error() string {
	if failure == nil || failure.err == nil {
		return "assigned review worker failed"
	}
	return failure.err.Error()
}

func (failure *reviewAssignedWorkerRunError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.err
}

func reviewWorkerRunWasCancelled(err error) bool {
	if err == nil {
		return false
	}
	var discoveryFailure *reviewDiscoveryLaneRunError
	if errors.As(err, &discoveryFailure) {
		return discoveryFailure.code == ReviewDiscoveryFailureCanceled
	}
	var assignmentFailure *reviewAssignedWorkerRunError
	if errors.As(err, &assignmentFailure) {
		return assignmentFailure.code == ReviewVerificationFailureCanceled
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

func reviewWorkerFailureRequiresCoordinatorAbort(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errReviewBaseChanged) {
		return true
	}
	var boundaryErr *reviewWorkerBoundaryError
	if errors.As(err, &boundaryErr) {
		return true
	}
	var staleErr *reviewCycleStaleError
	if errors.As(err, &staleErr) {
		return true
	}
	var terminalErr *convergentReviewPullRequestTerminalError
	return errors.As(err, &terminalErr)
}

func newReviewAssignedWorkerRunError(
	code ReviewVerificationFailureCode,
	err error,
) error {
	if err == nil {
		err = errors.New("assigned review worker failed")
	}
	return &reviewAssignedWorkerRunError{code: code, err: err}
}

func newRetryableReviewAssignedWorkerRunError(
	code ReviewVerificationFailureCode,
	err error,
) error {
	if err == nil {
		err = errors.New("assigned review worker failed")
	}
	return &reviewAssignedWorkerRunError{
		code:      code,
		retryable: true,
		err:       err,
	}
}

func reviewAssignedWorkerFailureCode(
	err error,
) ReviewVerificationFailureCode {
	var workerErr *reviewAssignedWorkerRunError
	if errors.As(err, &workerErr) &&
		supportedReviewVerificationFailureCode(workerErr.code) {
		return workerErr.code
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return ReviewVerificationFailureCanceled
	}
	return ReviewVerificationFailureRuntime
}

func supportedReviewVerificationFailureCode(
	code ReviewVerificationFailureCode,
) bool {
	switch code {
	case ReviewVerificationFailureCanceled,
		ReviewVerificationFailureLaunch,
		ReviewVerificationFailureRuntime,
		ReviewVerificationFailureArtifact,
		ReviewVerificationFailureState,
		ReviewVerificationFailureEvidence:
		return true
	default:
		return false
	}
}

func (m *AgentManager) mutateReviewConvergence(
	reviewerID string,
	mutate func(*ReviewCycleState, time.Time) error,
) (reviewConvergenceMutation, *ReviewConvergenceState, error) {
	if m == nil {
		return reviewConvergenceMutation{}, nil,
			errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer {
		return reviewConvergenceMutation{}, nil, fmt.Errorf(
			"review coordinator %q was not found",
			strings.TrimSpace(reviewerID),
		)
	}
	if agentLifecycleTerminal(reviewer) || reviewer.Paused ||
		reviewer.ReviewCycle == nil {
		return reviewConvergenceMutation{}, nil, fmt.Errorf(
			"review coordinator %q is stopped, paused, terminal, or unplanned",
			reviewer.ID,
		)
	}
	if reviewer.ReviewCycle.Stale {
		return reviewConvergenceMutation{}, nil, errReviewCycleStale
	}
	mutation := reviewConvergenceMutation{
		reviewerID: reviewer.ID,
		previousState: cloneReviewConvergenceState(
			reviewer.ReviewCycle.Convergence,
		),
		previousLastActivityTime: reviewer.LastActivityTime,
		updatedAt:                time.Now().UTC(),
	}
	if err := mutate(reviewer.ReviewCycle, mutation.updatedAt); err != nil {
		return reviewConvergenceMutation{}, nil, err
	}
	if err := validatePersistedReviewCycleSnapshot(
		reviewer.ReviewCycle,
	); err != nil {
		reviewer.ReviewCycle.Convergence = mutation.previousState
		return reviewConvergenceMutation{}, nil, fmt.Errorf(
			"review convergence mutation is invalid: %w",
			err,
		)
	}
	reviewer.LastActivityTime = mutation.updatedAt
	return mutation,
		cloneReviewConvergenceState(reviewer.ReviewCycle.Convergence),
		nil
}

func (m *AgentManager) rollbackReviewConvergenceMutation(
	mutation reviewConvergenceMutation,
) bool {
	if m == nil || mutation.reviewerID == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[mutation.reviewerID]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return false
	}
	reviewer.ReviewCycle.Convergence =
		cloneReviewConvergenceState(mutation.previousState)
	if reviewer.LastActivityTime.Equal(mutation.updatedAt) {
		reviewer.LastActivityTime =
			mutation.previousLastActivityTime
	}
	return true
}

func (b *Orchestrator) mutateAndPersistReviewConvergence(
	reviewerID string,
	mutate func(*ReviewCycleState, time.Time) error,
) (*ReviewConvergenceState, error) {
	if b == nil || b.agents == nil {
		return nil, errors.New(
			"orchestrator agent manager is not configured",
		)
	}
	reviewer, unlockLifecycle, err :=
		b.agents.lockActiveReviewCoordinatorForWorkerLaunch(reviewerID)
	if err != nil {
		return nil, err
	}
	liveHeadSHA, err := b.resolveLiveReviewHead(
		context.Background(),
		reviewer,
	)
	if err != nil {
		unlockLifecycle()
		return nil, fmt.Errorf(
			"failed to recheck live head before review convergence mutation: %w",
			err,
		)
	}
	if liveHeadSHA != reviewer.ReviewCycle.HeadSHA {
		unlockLifecycle()
		invalidateErr := b.invalidateStaleReviewCycle(
			context.Background(),
			reviewer.ID,
			liveHeadSHA,
		)
		return nil, errors.Join(
			&reviewCycleStaleError{
				reviewerID: reviewer.ID,
				staleHead:  reviewer.ReviewCycle.HeadSHA,
				liveHead:   liveHeadSHA,
			},
			invalidateErr,
		)
	}
	defer unlockLifecycle()
	b.reviewArtifactMu.Lock()
	defer b.reviewArtifactMu.Unlock()
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	mutation, state, err := b.agents.mutateReviewConvergence(
		reviewerID,
		mutate,
	)
	if err != nil {
		return nil, err
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if !b.agents.rollbackReviewConvergenceMutation(mutation) {
			return nil, fmt.Errorf(
				"failed to persist review convergence and failed to roll it back: %w",
				err,
			)
		}
		return nil, fmt.Errorf(
			"failed to persist review convergence: %w",
			err,
		)
	}
	return state, nil
}

func (b *Orchestrator) beginReviewConvergenceRound(
	reviewerID string,
) (ReviewConvergenceRoundState, error) {
	var round ReviewConvergenceRoundState
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			var err error
			round, err = beginReviewConvergenceRoundState(cycle, now)
			return err
		},
	)
	return round, err
}

func (b *Orchestrator) completeReviewConvergenceRound(
	reviewerID string,
) (ReviewConvergenceRoundState, error) {
	var round ReviewConvergenceRoundState
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			var err error
			round, err = completeReviewConvergenceRoundState(cycle, now)
			return err
		},
	)
	return round, err
}

func (b *Orchestrator) markReviewConvergenceRoundUnresolved(
	reviewerID string,
	action ReviewAction,
) error {
	if err := validateReviewAction(
		"review convergence unresolved action",
		action,
	); err != nil {
		return err
	}
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, _ time.Time) error {
			if cycle.Convergence == nil ||
				len(cycle.Convergence.Rounds) == 0 {
				return errors.New(
					"review convergence has no active round",
				)
			}
			round := &cycle.Convergence.Rounds[len(cycle.Convergence.Rounds)-1]
			if round.Outcome != ReviewConvergenceRoundActive {
				return errors.New(
					"review convergence has no active round",
				)
			}
			round.Action = action
			return nil
		},
	)
	return err
}

func (b *Orchestrator) queueReviewVerificationRound(
	reviewerID string,
	round int,
) ([]ReviewVerificationAssignment, error) {
	var assignments []ReviewVerificationAssignment
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			if !reviewConvergenceRoundIsActive(cycle, round) {
				return fmt.Errorf(
					"review convergence round %d is not active",
					round,
				)
			}
			var err error
			assignments, err = queueReviewVerificationAssignments(
				cycle,
				round,
				now,
			)
			return err
		},
	)
	return assignments, err
}

func reviewConvergenceRoundIsActive(
	cycle *ReviewCycleState,
	round int,
) bool {
	if cycle == nil || cycle.Convergence == nil ||
		len(cycle.Convergence.Rounds) == 0 {
		return false
	}
	latest := cycle.Convergence.Rounds[len(cycle.Convergence.Rounds)-1]
	return latest.Round == round &&
		latest.Outcome == ReviewConvergenceRoundActive
}

func (b *Orchestrator) markReviewVerificationRunning(
	reviewerID string,
	assignmentID string,
	ownership *ReviewWorkerOwnership,
) error {
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			assignment, ok := reviewVerificationAssignmentByID(
				cycle,
				assignmentID,
			)
			if !ok {
				return fmt.Errorf(
					"review verification assignment %q is not scheduled",
					assignmentID,
				)
			}
			if !reviewVerificationAssignmentMatchesCurrentCandidate(
				cycle,
				*assignment,
			) {
				return fmt.Errorf(
					"review verification assignment %q targets a stale candidate revision",
					assignmentID,
				)
			}
			switch {
			case assignment.Status == ReviewVerificationQueued &&
				ownership == nil:
				assignment.Status = ReviewVerificationRunning
				assignment.StartedAt = now
			case assignment.Status == ReviewVerificationRunning &&
				ownership != nil:
				if !reviewVerificationOwnershipMatches(
					cycle,
					*assignment,
					*ownership,
				) {
					return errors.New(
						"review verifier is not independent or does not own the assignment",
					)
				}
				if assignment.WorkerID != "" &&
					ownership.Attempt <= assignment.Attempt {
					return errors.New(
						"review verification retry does not have fresh ownership",
					)
				}
				assignment.WorkerID = ownership.OwnerID
				assignment.Attempt = ownership.Attempt
			default:
				return fmt.Errorf(
					"review verification assignment %q cannot run from %q",
					assignmentID,
					assignment.Status,
				)
			}
			return syncReviewFindingVerifications(cycle)
		},
	)
	return err
}

func reviewVerificationAssignmentByID(
	cycle *ReviewCycleState,
	assignmentID string,
) (*ReviewVerificationAssignment, bool) {
	if cycle == nil || cycle.Convergence == nil {
		return nil, false
	}
	for index := range cycle.Convergence.VerificationAssignments {
		assignment := &cycle.Convergence.VerificationAssignments[index]
		if assignment.ID == assignmentID {
			return assignment, true
		}
	}
	return nil, false
}

func (b *Orchestrator) failReviewVerificationAssignment(
	reviewerID string,
	assignmentID string,
	code ReviewVerificationFailureCode,
) error {
	if !supportedReviewVerificationFailureCode(code) {
		return fmt.Errorf(
			"review verification failure code %q is unsupported",
			code,
		)
	}
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			assignment, ok := reviewVerificationAssignmentByID(
				cycle,
				assignmentID,
			)
			if !ok {
				return fmt.Errorf(
					"review verification assignment %q is not scheduled",
					assignmentID,
				)
			}
			if assignment.Superseded {
				return nil
			}
			if assignment.Status != ReviewVerificationQueued &&
				assignment.Status != ReviewVerificationRunning {
				return fmt.Errorf(
					"review verification assignment %q cannot fail from %q",
					assignmentID,
					assignment.Status,
				)
			}
			assignment.Status = ReviewVerificationFailed
			assignment.CompletedAt = now
			assignment.FailureCode = code
			assignment.Action =
				cycle.Policy.FailureActions.VerificationFailure
			return syncReviewFindingVerifications(cycle)
		},
	)
	return err
}

func (b *Orchestrator) queueTargetedReviewChallenge(
	reviewerID string,
	round int,
	target ReviewChallengeTarget,
) (ReviewChallengeAssignment, error) {
	var assignment ReviewChallengeAssignment
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			if !reviewConvergenceRoundIsActive(cycle, round) {
				return fmt.Errorf(
					"review convergence round %d is not active",
					round,
				)
			}
			var err error
			assignment, err = queueTargetedReviewChallenge(
				cycle,
				round,
				target,
				now,
			)
			return err
		},
	)
	return assignment, err
}

func (b *Orchestrator) markReviewChallengeRunning(
	reviewerID string,
	assignmentID string,
	ownership *ReviewWorkerOwnership,
) error {
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			assignment, ok := reviewChallengeAssignmentByID(
				cycle,
				assignmentID,
			)
			if !ok {
				return fmt.Errorf(
					"review challenge assignment %q is not scheduled",
					assignmentID,
				)
			}
			if assignment.Superseded {
				return fmt.Errorf(
					"review challenge assignment %q is superseded",
					assignmentID,
				)
			}
			switch {
			case assignment.Status == ReviewChallengeQueued &&
				ownership == nil:
				assignment.Status = ReviewChallengeRunning
				assignment.StartedAt = now
			case assignment.Status == ReviewChallengeRunning &&
				ownership != nil:
				if !reviewChallengeOwnershipMatches(
					cycle,
					*assignment,
					*ownership,
				) {
					return errors.New(
						"review challenge worker does not own the assignment",
					)
				}
				if assignment.WorkerID != "" &&
					ownership.Attempt <= assignment.Attempt {
					return errors.New(
						"review challenge retry does not have fresh ownership",
					)
				}
				assignment.WorkerID = ownership.OwnerID
				assignment.Attempt = ownership.Attempt
			default:
				return fmt.Errorf(
					"review challenge assignment %q cannot run from %q",
					assignmentID,
					assignment.Status,
				)
			}
			return nil
		},
	)
	return err
}

func reviewChallengeAssignmentByID(
	cycle *ReviewCycleState,
	assignmentID string,
) (*ReviewChallengeAssignment, bool) {
	if cycle == nil || cycle.Convergence == nil {
		return nil, false
	}
	for index := range cycle.Convergence.ChallengeAssignments {
		assignment := &cycle.Convergence.ChallengeAssignments[index]
		if assignment.ID == assignmentID {
			return assignment, true
		}
	}
	return nil, false
}

func (b *Orchestrator) failReviewChallengeAssignment(
	reviewerID string,
	assignmentID string,
	action ReviewAction,
) error {
	if err := validateReviewAction(
		"review challenge failure action",
		action,
	); err != nil {
		return err
	}
	_, err := b.mutateAndPersistReviewConvergence(
		reviewerID,
		func(cycle *ReviewCycleState, now time.Time) error {
			assignment, ok := reviewChallengeAssignmentByID(
				cycle,
				assignmentID,
			)
			if !ok {
				return fmt.Errorf(
					"review challenge assignment %q is not scheduled",
					assignmentID,
				)
			}
			if assignment.Superseded {
				return nil
			}
			if assignment.Status != ReviewChallengeQueued &&
				assignment.Status != ReviewChallengeRunning {
				return fmt.Errorf(
					"review challenge assignment %q cannot fail from %q",
					assignmentID,
					assignment.Status,
				)
			}
			assignment.Status = ReviewChallengeFailed
			assignment.CompletedAt = now
			assignment.Action = action
			return nil
		},
	)
	return err
}

// runReviewConvergentRound executes one exact-SHA coordinator round. The
// Every review cycle calls it through the convergent coordinator.
func (b *Orchestrator) runReviewConvergentRound(
	ctx context.Context,
	reviewerID string,
) (ReviewConvergenceRoundState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	roundCtx, finishRound, err := b.registerReviewDiscoveryPass(
		ctx,
		reviewerID,
	)
	if err != nil {
		return ReviewConvergenceRoundState{}, err
	}
	defer finishRound()
	round, err := b.beginReviewConvergenceRound(reviewerID)
	if err != nil {
		return ReviewConvergenceRoundState{}, err
	}
	var failures []error
	if _, err := b.runReviewVerificationRound(
		roundCtx,
		reviewerID,
		round.Round,
	); err != nil {
		failures = append(failures, err)
	}
	for roundCtx.Err() == nil {
		current, ok := b.agents.Get(reviewerID)
		if !ok || current.ReviewCycle == nil {
			failures = append(
				failures,
				errors.New("review coordinator disappeared before challenge selection"),
			)
			break
		}
		if reviewCycleReadyForChangesRequiredVerdict(current.ReviewCycle) {
			break
		}
		assignments, challengeErr := b.runTargetedReviewChallengeRound(
			roundCtx,
			reviewerID,
			round.Round,
		)
		if challengeErr != nil {
			failures = append(failures, challengeErr)
			break
		}
		if len(assignments) == 0 {
			break
		}
		if _, verificationErr := b.runReviewVerificationRound(
			roundCtx,
			reviewerID,
			round.Round,
		); verificationErr != nil {
			failures = append(failures, verificationErr)
			break
		}
	}
	if roundCtx.Err() != nil {
		failures = append(failures, roundCtx.Err())
	}
	if len(failures) != 0 {
		reviewer, ok := b.agents.Get(reviewerID)
		if !ok || reviewer.ReviewCycle == nil {
			failures = append(
				failures,
				errors.New(
					"review coordinator disappeared before round failure was persisted",
				),
			)
		} else if err := b.markReviewConvergenceRoundUnresolved(
			reviewerID,
			reviewer.ReviewCycle.Policy.FailureActions.VerificationFailure,
		); err != nil {
			failures = append(failures, err)
		}
	}
	completed, completeErr := b.completeReviewConvergenceRound(
		reviewerID,
	)
	if completeErr != nil {
		failures = append(failures, completeErr)
		return round, errors.Join(failures...)
	}
	return completed, errors.Join(failures...)
}

// runReviewVerificationRound executes the verifier assignments for one
// exact-SHA round.
func (b *Orchestrator) runReviewVerificationRound(
	ctx context.Context,
	reviewerID string,
	round int,
) ([]ReviewVerificationAssignment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	all := make([]ReviewVerificationAssignment, 0)
	var failures []error
	for {
		assignments, err := b.queueReviewVerificationRound(
			reviewerID,
			round,
		)
		if err != nil {
			return all, errors.Join(append(failures, err)...)
		}
		all = append(all, assignments...)
		queued := make([]ReviewVerificationAssignment, 0)
		for _, assignment := range assignments {
			if assignment.Status == ReviewVerificationQueued {
				queued = append(queued, assignment)
			}
		}
		if len(queued) == 0 {
			break
		}
		reviewer, ok := b.agents.Get(reviewerID)
		if !ok || reviewer.ReviewCycle == nil {
			return all, fmt.Errorf(
				"review coordinator %q disappeared before verification",
				reviewerID,
			)
		}
		parallelism :=
			reviewer.ReviewCycle.Policy.Verification.MaxParallelVerifiers
		slots := make(chan struct{}, parallelism)
		results := make(chan error, len(queued))
		var workers sync.WaitGroup
		for _, value := range queued {
			assignment := value
			workers.Add(1)
			go func() {
				defer workers.Done()
				select {
				case slots <- struct{}{}:
					defer func() { <-slots }()
				case <-ctx.Done():
					failErr := b.failReviewVerificationAssignment(
						reviewerID,
						assignment.ID,
						ReviewVerificationFailureCanceled,
					)
					results <- errors.Join(ctx.Err(), failErr)
					return
				}
				results <- b.runOneReviewVerification(
					ctx,
					reviewerID,
					assignment,
				)
			}()
		}
		workers.Wait()
		close(results)
		for result := range results {
			if result != nil {
				failures = append(failures, result)
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	return all, errors.Join(failures...)
}

func (b *Orchestrator) runOneReviewVerification(
	ctx context.Context,
	reviewerID string,
	assignment ReviewVerificationAssignment,
) error {
	if err := b.markReviewVerificationRunning(
		reviewerID,
		assignment.ID,
		nil,
	); err != nil {
		return err
	}
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return errors.New(
			"review coordinator disappeared during verification",
		)
	}
	retries := reviewer.ReviewCycle.Policy.Verification.Retries
	runErr := runReviewConvergenceWorkerWithRetries(
		retries,
		func() error {
			return b.executeReviewVerificationAssignment(
				ctx,
				reviewerID,
				assignment,
			)
		},
	)
	current, found := b.agents.Get(reviewerID)
	if found && current.ReviewCycle != nil {
		stored, scheduled := reviewVerificationAssignmentByID(
			current.ReviewCycle,
			assignment.ID,
		)
		if scheduled && stored.Superseded {
			return nil
		}
		if scheduled && stored.Status == ReviewVerificationCompleted {
			return nil
		}
	}
	if runErr == nil {
		runErr = newReviewAssignedWorkerRunError(
			ReviewVerificationFailureArtifact,
			errors.New(
				"review verifier returned without a trusted artifact",
			),
		)
	}
	code := reviewAssignedWorkerFailureCode(runErr)
	failErr := b.failReviewVerificationAssignment(
		reviewerID,
		assignment.ID,
		code,
	)
	if reviewWorkerFailureRequiresCoordinatorAbort(runErr) {
		return errors.Join(runErr, failErr)
	}
	return failErr
}

func runReviewConvergenceWorkerWithRetries(
	retries int,
	run func() error,
) error {
	var runErr error
	for attempt := 0; ; attempt++ {
		runErr = run()
		if runErr == nil ||
			!reviewConvergenceWorkerFailureIsRetryable(runErr) ||
			attempt >= retries {
			return runErr
		}
	}
}

func reviewConvergenceWorkerFailureIsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var workerErr *reviewAssignedWorkerRunError
	if errors.As(err, &workerErr) && workerErr.retryable {
		return true
	}
	var runtimeTimeout *reviewWorkerRuntimeTimeoutError
	if errors.As(err, &runtimeTimeout) {
		return true
	}
	return reviewArtifactFailureAllowsFreshWorkerRetry(err)
}

func (b *Orchestrator) executeReviewVerificationAssignment(
	ctx context.Context,
	reviewerID string,
	assignment ReviewVerificationAssignment,
) error {
	if b.reviewVerificationRunner != nil {
		return b.reviewVerificationRunner(
			ctx,
			reviewerID,
			assignment,
		)
	}
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			errors.New("review coordinator was not found"),
		)
	}
	identity, err := reviewWorkerIdentityForCycle(
		reviewer.ReviewCycle,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
	)
	if err != nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			err,
		)
	}
	prompt, err := reviewVerificationPrompt(
		reviewer,
		assignment,
	)
	if err != nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			err,
		)
	}
	return b.runReviewAssignedWorker(
		ctx,
		reviewerID,
		identity,
		prompt,
		ReviewArtifactPhaseVerification,
		func(ownership ReviewWorkerOwnership) error {
			return b.markReviewVerificationRunning(
				reviewerID,
				assignment.ID,
				&ownership,
			)
		},
	)
}

func reviewVerificationPrompt(
	reviewer Agent,
	assignment ReviewVerificationAssignment,
) (string, error) {
	if reviewer.ReviewCycle == nil || reviewer.ReviewCycle.Plan == nil {
		return "", errors.New(
			"review verification prompt requires an exact-SHA cycle",
		)
	}
	if !reviewVerificationAssignmentMatchesCurrentCandidate(
		reviewer.ReviewCycle,
		assignment,
	) {
		return "", errors.New(
			"review verification prompt targets a stale candidate revision",
		)
	}
	finding := assignment.CandidateSnapshot
	findingBody, err := json.Marshal(finding)
	if err != nil {
		return "", err
	}
	intentBody, err := json.Marshal(struct {
		Scope              []string `json:"scope"`
		NonGoals           []string `json:"non_goals"`
		AcceptanceCriteria []string `json:"acceptance_criteria"`
	}{
		Scope: nonNilReviewTaskIntentItems(
			reviewer.ReviewCycle.Plan.TaskScope,
		),
		NonGoals: nonNilReviewTaskIntentItems(
			reviewer.ReviewCycle.Plan.NonGoals,
		),
		AcceptanceCriteria: nonNilReviewTaskIntentItems(
			reviewer.ReviewCycle.Plan.AcceptanceCriteria,
		),
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"Independently verify finding %s against exact base %s and head %s. "+
			"%s The immutable task intent is %s. A confirmed outcome is valid only "+
			"when independent evidence proves both that the finding is in scope "+
			"and that the base-to-head patch introduced or worsened the defect. "+
			"A defect already present at base, an unchanged contract mismatch, a "+
			"general hardening idea, or a non-goal must be rejected as non-blocking. "+
			"You must not rely on or impersonate any originating worker: %s. "+
			"Assigned finding: %s. Publish one strict verification artifact "+
			"to RAO_REVIEW_ARTIFACT_DIR with finding_id, outcome, summary, "+
			"scope_disposition, patch_disposition, the exact location, "+
			"behavioral_path, independent evidence, causal_evidence that names "+
			"the exact changed path and line range responsible for an "+
			"introduced/worsened defect, and "+
			"test_evidence; when a test or reproduction is not practical, "+
			"provide test_not_practical_reason. Do not publish a GitHub comment.",
		assignment.FindingID,
		reviewer.ReviewCycle.Plan.BaseSHA,
		reviewer.ReviewCycle.HeadSHA,
		reviewPullRequestDiffInstruction(
			reviewer.ReviewCycle.Plan.BaseSHA,
			reviewer.ReviewCycle.Plan.HeadSHA,
		),
		string(intentBody),
		strings.Join(assignment.ExcludedWorkerIDs, ", "),
		string(findingBody),
	), nil
}

// runTargetedReviewChallengeRound schedules every currently actionable target
// before starting workers, then runs the batch concurrently. New evidence from
// the batch is verified together by the caller.
func (b *Orchestrator) runTargetedReviewChallengeRound(
	ctx context.Context,
	reviewerID string,
	round int,
) ([]ReviewChallengeAssignment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return nil, errors.New(
			"review coordinator disappeared before challenge scheduling",
		)
	}
	capacity := reviewConvergenceRemainingAgentCapacity(reviewer.ReviewCycle)
	assignments := make([]ReviewChallengeAssignment, 0, capacity)
	for ctx.Err() == nil && len(assignments) < capacity {
		assignment, err := b.queueNextTargetedReviewChallenge(
			reviewerID,
			round,
		)
		if err != nil {
			if strings.Contains(
				err.Error(),
				"identical review challenge context",
			) {
				break
			}
			return assignments, err
		}
		if assignment.ID == "" {
			break
		}
		assignments = append(assignments, assignment)
	}
	if len(assignments) == 0 {
		return assignments, ctx.Err()
	}
	reviewer, ok = b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return assignments, errors.New(
			"review coordinator disappeared before challenge execution",
		)
	}
	parallelism := reviewer.ReviewCycle.Policy.Swarm.MaxParallelReviewers
	if parallelism < 1 {
		parallelism = 1
	}
	slots := make(chan struct{}, parallelism)
	results := make(chan error, len(assignments))
	var workers sync.WaitGroup
	for _, value := range assignments {
		assignment := value
		workers.Add(1)
		go func() {
			defer workers.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				failErr := b.failReviewChallengeAssignment(
					reviewerID,
					assignment.ID,
					reviewer.ReviewCycle.Policy.FailureActions.VerificationFailure,
				)
				results <- errors.Join(ctx.Err(), failErr)
				return
			}
			results <- b.runOneTargetedReviewChallenge(
				ctx,
				reviewerID,
				assignment,
			)
		}()
	}
	workers.Wait()
	close(results)
	var failures []error
	for result := range results {
		if result != nil {
			failures = append(failures, result)
		}
	}
	return assignments, errors.Join(failures...)
}

// runNextTargetedReviewChallenge preserves the single-target operation used by
// focused tests and callers that intentionally request one challenge.
func (b *Orchestrator) runNextTargetedReviewChallenge(
	ctx context.Context,
	reviewerID string,
	round int,
) (ReviewChallengeAssignment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	assignment, err := b.queueNextTargetedReviewChallenge(reviewerID, round)
	if err != nil || assignment.ID == "" {
		return assignment, err
	}
	return assignment, b.runOneTargetedReviewChallenge(
		ctx,
		reviewerID,
		assignment,
	)
}

func (b *Orchestrator) queueNextTargetedReviewChallenge(
	reviewerID string,
	round int,
) (ReviewChallengeAssignment, error) {
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return ReviewChallengeAssignment{}, fmt.Errorf(
			"review coordinator %q was not found",
			reviewerID,
		)
	}
	targets := buildReviewChallengeTargets(reviewer.ReviewCycle)
	if len(targets) == 0 {
		return ReviewChallengeAssignment{}, nil
	}
	var assignment ReviewChallengeAssignment
	var err error
	for _, target := range targets {
		if reviewChallengeTargetAttemptedInRound(
			reviewer.ReviewCycle,
			round,
			target,
		) {
			continue
		}
		assignment, err = b.queueTargetedReviewChallenge(
			reviewerID,
			round,
			target,
		)
		if err == nil {
			break
		}
		if !strings.Contains(
			err.Error(),
			"identical review challenge context",
		) {
			return ReviewChallengeAssignment{}, err
		}
	}
	if assignment.ID == "" {
		if err == nil {
			return ReviewChallengeAssignment{}, nil
		}
		return ReviewChallengeAssignment{}, err
	}
	if assignment.Status == ReviewChallengeFailed {
		return assignment, errors.New(
			"review challenge has no remaining worker capacity",
		)
	}
	return assignment, nil
}

func (b *Orchestrator) runOneTargetedReviewChallenge(
	ctx context.Context,
	reviewerID string,
	assignment ReviewChallengeAssignment,
) error {
	if err := b.markReviewChallengeRunning(
		reviewerID,
		assignment.ID,
		nil,
	); err != nil {
		return err
	}
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return errors.New(
			"review coordinator disappeared during challenge",
		)
	}
	retries := reviewer.ReviewCycle.Policy.Swarm.Retries
	runErr := runReviewConvergenceWorkerWithRetries(
		retries,
		func() error {
			return b.executeReviewChallengeAssignment(
				ctx,
				reviewerID,
				assignment,
			)
		},
	)
	current, found := b.agents.Get(reviewerID)
	if found && current.ReviewCycle != nil {
		stored, scheduled := reviewChallengeAssignmentByID(
			current.ReviewCycle,
			assignment.ID,
		)
		if scheduled && stored.Superseded {
			return nil
		}
		if scheduled && stored.Status == ReviewChallengeCompleted {
			return nil
		}
	}
	if runErr == nil {
		runErr = errors.New(
			"review challenge returned without a trusted artifact",
		)
	}
	failErr := b.failReviewChallengeAssignment(
		reviewerID,
		assignment.ID,
		reviewer.ReviewCycle.Policy.FailureActions.VerificationFailure,
	)
	if reviewWorkerFailureRequiresCoordinatorAbort(runErr) {
		return errors.Join(runErr, failErr)
	}
	return failErr
}

func (b *Orchestrator) executeReviewChallengeAssignment(
	ctx context.Context,
	reviewerID string,
	assignment ReviewChallengeAssignment,
) error {
	if b.reviewChallengeRunner != nil {
		return b.reviewChallengeRunner(
			ctx,
			reviewerID,
			assignment,
		)
	}
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			errors.New("review coordinator was not found"),
		)
	}
	identity, err := reviewWorkerIdentityForCycle(
		reviewer.ReviewCycle,
		AgentProfileRoleChallenge,
		assignment.Round,
		assignment.Lane,
	)
	if err != nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			err,
		)
	}
	diff, err := collectReviewChallengeDiff(
		ctx,
		reviewer,
	)
	if err != nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			err,
		)
	}
	prompt, err := reviewChallengePrompt(
		reviewer,
		assignment,
		diff,
	)
	if err != nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			err,
		)
	}
	return b.runReviewAssignedWorker(
		ctx,
		reviewerID,
		identity,
		prompt,
		ReviewArtifactPhaseChallenge,
		func(ownership ReviewWorkerOwnership) error {
			return b.markReviewChallengeRunning(
				reviewerID,
				assignment.ID,
				&ownership,
			)
		},
	)
}

func collectReviewChallengeDiff(
	ctx context.Context,
	reviewer Agent,
) (string, error) {
	if reviewer.ReviewCycle == nil ||
		reviewer.ReviewCycle.Inputs == nil {
		return "", errors.New(
			"review challenge requires exact diff inputs",
		)
	}
	body, err := outputReviewPlanGitCommand(
		ctx,
		reviewer.WorktreePath,
		nil,
		"-c",
		"diff.algorithm="+reviewPlanDiffAlgorithm,
		"diff",
		"--no-ext-diff",
		"--no-textconv",
		"--find-renames="+reviewPlanDiffRenameThreshold,
		reviewPullRequestDiffRange(
			reviewer.ReviewCycle.Inputs.BaseSHA,
			reviewer.ReviewCycle.HeadSHA,
		),
		"--",
	)
	if err != nil {
		return "", fmt.Errorf(
			"failed to collect full exact-SHA challenge diff: %w",
			err,
		)
	}
	return string(body), nil
}

func reviewChallengePrompt(
	reviewer Agent,
	assignment ReviewChallengeAssignment,
	fullDiff string,
) (string, error) {
	if reviewer.ReviewCycle == nil ||
		reviewer.ReviewCycle.Plan == nil ||
		reviewer.ReviewCycle.Convergence == nil {
		return "", errors.New(
			"review challenge prompt requires planned convergence state",
		)
	}
	contextBody, err := json.Marshal(struct {
		Plan          *ReviewPlan                 `json:"plan"`
		Coverage      []ReviewCoverageGap         `json:"coverage_gaps"`
		Findings      []ReviewCanonicalFinding    `json:"findings"`
		Verifications []ReviewFindingVerification `json:"verifications"`
	}{
		Plan:          reviewer.ReviewCycle.Plan,
		Coverage:      reviewer.ReviewCycle.UnresolvedCoverage,
		Findings:      reviewer.ReviewCycle.CanonicalFindings,
		Verifications: reviewer.ReviewCycle.Convergence.FindingVerifications,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"Challenge only the named %s %q (%s) at exact head %s. "+
			"%s Do not run a generic review. Full plan, coverage, and verified "+
			"finding summary: %s\n\nFull exact diff:\n%s\n\nPublish one strict "+
			"challenge artifact to RAO_REVIEW_ARTIFACT_DIR with "+
			"assignment_id=%q, target_kind=%q, target_id=%q, candidates, "+
			"coverage, outcome, and summary. Any candidate "+
			"will be independently verified. A coverage gap closes only with "+
			"a covered claim containing evidence. A conclusive competing-hypothesis "+
			"result must include a candidate reproducing the targeted finding so "+
			"the new evidence returns through independent verification. Do not "+
			"publish a GitHub comment.",
		assignment.Target.Kind,
		assignment.Target.ID,
		assignment.Target.Description,
		reviewer.ReviewCycle.HeadSHA,
		reviewPullRequestDiffInstruction(
			reviewer.ReviewCycle.Plan.BaseSHA,
			reviewer.ReviewCycle.Plan.HeadSHA,
		),
		string(contextBody),
		fullDiff,
		assignment.ID,
		assignment.Target.Kind,
		assignment.Target.ID,
	), nil
}

func (b *Orchestrator) runReviewAssignedWorker(
	ctx context.Context,
	reviewerID string,
	identity ReviewWorkerIdentity,
	prompt string,
	phase ReviewArtifactPhase,
	bindOwnership func(ReviewWorkerOwnership) error,
) (runErr error) {
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			errors.New("review coordinator was not found"),
		)
	}
	timeout := reviewWorkerRuntimeTimeout(
		reviewer.ReviewCycle,
		identity.Role,
	)
	if timeout <= 0 {
		if err := b.enforceReviewWorkerDispatchLimits(reviewerID); err != nil {
			return newReviewAssignedWorkerRunError(
				ReviewVerificationFailureLaunch,
				err,
			)
		}
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			errors.New("assigned review worker timeout is invalid"),
		)
	}
	ownership, handle, err := b.launchReviewWorker(
		ctx,
		reviewerID,
		reviewWorkerLaunchRequest{
			Identity: identity,
			Prompt:   prompt,
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return newReviewAssignedWorkerRunError(
				ReviewVerificationFailureCanceled,
				err,
			)
		}
		var setupErr *reviewWorkerWorktreeSetupError
		if errors.As(err, &setupErr) {
			return newReviewAssignedWorkerRunError(
				ReviewVerificationFailureLaunch,
				err,
			)
		}
		if errors.Is(err, context.DeadlineExceeded) &&
			ctx.Err() == nil {
			return newRetryableReviewAssignedWorkerRunError(
				ReviewVerificationFailureLaunch,
				err,
			)
		}
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureLaunch,
			err,
		)
	}
	defer func() {
		next := ReviewWorkerCompleted
		var failure *DurableLaunchFailure
		if runErr != nil {
			next = ReviewWorkerFailed
			failure = &DurableLaunchFailure{
				Kind: DurableLaunchFailureRuntime,
				Retryable: reviewConvergenceWorkerFailureIsRetryable(
					runErr,
				),
			}
			if reviewWorkerRunWasCancelled(runErr) {
				next = ReviewWorkerCancelled
				failure = nil
			}
		}
		if _, lifecycleErr :=
			b.transitionAndPersistReviewWorkerLifecycle(
				reviewerID,
				ownership.OwnerID,
				next,
				failure,
			); lifecycleErr != nil {
			runErr = errors.Join(runErr, lifecycleErr)
		}
	}()
	runtimeDeadline := reviewWorkerRuntimeDeadline(
		reviewer.ReviewCycle,
		identity.Role,
		ownership.StartedAt,
	)
	if runtimeDeadline.IsZero() {
		_ = b.runner.Stop(handle)
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureState,
			errors.New("assigned review worker runtime deadline is invalid"),
		)
	}
	runtimeAllowance := runtimeDeadline.Sub(ownership.StartedAt)
	runtimeCtx, cancelRuntime := context.WithDeadline(ctx, runtimeDeadline)
	defer cancelRuntime()
	if err := bindOwnership(ownership); err != nil {
		_ = b.runner.Stop(handle)
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureState,
			err,
		)
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		_ = b.runner.Stop(handle)
		return newReviewAssignedWorkerRunError(
			ReviewVerificationFailureArtifact,
			err,
		)
	}
	artifactPath := filepath.Join(
		artifactDirectory,
		reviewArtifactFilename(ownership),
	)
	ticker := time.NewTicker(reviewDiscoveryWorkerPollInterval)
	defer ticker.Stop()
	artifactCorrections := 0
	for {
		if err := runtimeCtx.Err(); err != nil {
			stopErr := b.runner.Stop(handle)
			if ctx.Err() == nil {
				return newRetryableReviewAssignedWorkerRunError(
					ReviewVerificationFailureRuntime,
					errors.Join(
						&reviewWorkerRuntimeTimeoutError{timeout: runtimeAllowance},
						stopErr,
					),
				)
			}
			return newReviewAssignedWorkerRunError(
				ReviewVerificationFailureCanceled,
				errors.Join(err, stopErr),
			)
		}
		published, publishedErr :=
			reviewDiscoveryArtifactIsPublished(artifactPath)
		if publishedErr != nil {
			artifactErr := b.recordReviewArtifactRejection(
				reviewerID,
				&ownership,
				publishedErr,
			)
			stopErr := b.runner.Stop(handle)
			return newReviewAssignedWorkerRunError(
				ReviewVerificationFailureArtifact,
				errors.Join(artifactErr, stopErr),
			)
		}
		if published {
			if _, err := b.intakeReviewWorkerArtifactWithMalformedCorrection(
				ctx,
				reviewerID,
				ownership.OwnerID,
				artifactPath,
				phase,
				true,
			); err != nil {
				corrected, correctionErr := b.handleMalformedReviewArtifact(
					reviewerID,
					ownership,
					handle,
					artifactPath,
					artifactCorrections,
					err,
				)
				if corrected {
					artifactCorrections++
					continue
				}
				stopErr := b.runner.Stop(handle)
				return newReviewAssignedWorkerRunError(
					ReviewVerificationFailureArtifact,
					errors.Join(correctionErr, stopErr),
				)
			}
			if err := b.runner.Stop(handle); err != nil {
				return newReviewAssignedWorkerRunError(
					ReviewVerificationFailureRuntime,
					err,
				)
			}
			return nil
		}
		alive, aliveErr := b.runner.IsAlive(handle)
		if aliveErr != nil {
			stopErr := b.runner.Stop(handle)
			return newReviewAssignedWorkerRunError(
				ReviewVerificationFailureRuntime,
				errors.Join(aliveErr, stopErr),
			)
		}
		if !alive {
			if _, err := b.intakeReviewWorkerArtifact(
				ctx,
				reviewerID,
				ownership.OwnerID,
				artifactPath,
				phase,
			); err != nil {
				return newReviewAssignedWorkerRunError(
					ReviewVerificationFailureArtifact,
					err,
				)
			}
			return nil
		}
		select {
		case <-runtimeCtx.Done():
		case <-ticker.C:
		}
	}
}
