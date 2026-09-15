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
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var errPersistedReviewWorkerMissing = errors.New(
	"persisted review worker runtime is missing",
)

type reviewCycleRecoveryWork struct {
	Identity     ReviewWorkerIdentity
	AssignmentID string
	OwnerID      string
	Attempt      int
}

func reviewCycleRecoveryWorkKey(identity ReviewWorkerIdentity) string {
	return fmt.Sprintf(
		"%s\x00%06d\x00%s",
		identity.Role,
		identity.Pass,
		identity.Lane,
	)
}

func collectReviewCycleRecoveryWork(
	cycle *ReviewCycleState,
) ([]reviewCycleRecoveryWork, error) {
	if cycle == nil {
		return nil, nil
	}
	byIdentity := make(map[string]reviewCycleRecoveryWork)
	add := func(work reviewCycleRecoveryWork) error {
		if err := validateReviewWorkerIdentity(work.Identity); err != nil {
			return err
		}
		key := reviewCycleRecoveryWorkKey(work.Identity)
		if _, duplicate := byIdentity[key]; duplicate {
			return fmt.Errorf(
				"persisted review logical work %q is duplicated",
				key,
			)
		}
		byIdentity[key] = work
		return nil
	}

	for _, pass := range cycle.DiscoveryPasses {
		for _, lane := range pass.Lanes {
			if lane.Status != ReviewDiscoveryLaneQueued &&
				lane.Status != ReviewDiscoveryLaneRunning {
				continue
			}
			identity, err := reviewWorkerIdentityForCycle(
				cycle,
				AgentProfileRoleDiscovery,
				pass.Pass,
				lane.Lane,
			)
			if err != nil {
				return nil, err
			}
			if err := add(reviewCycleRecoveryWork{
				Identity: identity,
				OwnerID:  lane.WorkerID,
				Attempt:  lane.Attempt,
			}); err != nil {
				return nil, err
			}
		}
	}
	if cycle.Convergence != nil {
		for _, assignment := range cycle.Convergence.VerificationAssignments {
			if assignment.Superseded {
				continue
			}
			if assignment.Status != ReviewVerificationQueued &&
				assignment.Status != ReviewVerificationRunning {
				continue
			}
			identity, err := reviewWorkerIdentityForCycle(
				cycle,
				AgentProfileRoleVerifier,
				assignment.Round,
				assignment.Lane,
			)
			if err != nil {
				return nil, err
			}
			if err := add(reviewCycleRecoveryWork{
				Identity:     identity,
				AssignmentID: assignment.ID,
				OwnerID:      assignment.WorkerID,
				Attempt:      assignment.Attempt,
			}); err != nil {
				return nil, err
			}
		}
		for _, assignment := range cycle.Convergence.ChallengeAssignments {
			if assignment.Superseded {
				continue
			}
			if assignment.Status != ReviewChallengeQueued &&
				assignment.Status != ReviewChallengeRunning {
				continue
			}
			identity, err := reviewWorkerIdentityForCycle(
				cycle,
				AgentProfileRoleChallenge,
				assignment.Round,
				assignment.Lane,
			)
			if err != nil {
				return nil, err
			}
			if err := add(reviewCycleRecoveryWork{
				Identity:     identity,
				AssignmentID: assignment.ID,
				OwnerID:      assignment.WorkerID,
				Attempt:      assignment.Attempt,
			}); err != nil {
				return nil, err
			}
		}
	}

	keys := make([]string, 0, len(byIdentity))
	for key := range byIdentity {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	work := make([]reviewCycleRecoveryWork, 0, len(keys))
	for _, key := range keys {
		work = append(work, byIdentity[key])
	}
	return work, nil
}

func reviewCycleHasActiveRound(cycle *ReviewCycleState) bool {
	if cycle == nil || cycle.Convergence == nil ||
		len(cycle.Convergence.Rounds) == 0 {
		return false
	}
	last := len(cycle.Convergence.Rounds) - 1
	return cycle.Convergence.Rounds[last].Outcome ==
		ReviewConvergenceRoundActive
}

func reviewCycleRecoveryOwnership(
	cycle *ReviewCycleState,
	work reviewCycleRecoveryWork,
) (ReviewWorkerOwnership, bool) {
	if cycle == nil {
		return ReviewWorkerOwnership{}, false
	}
	var current ReviewWorkerOwnership
	for _, ownership := range cycle.WorkerOwnerships {
		if sameReviewWorkerLogicalIdentity(
			ownership.Identity,
			work.Identity,
		) &&
			(current.OwnerID == "" ||
				ownership.Attempt > current.Attempt) {
			current = ownership
		}
	}
	return current, current.OwnerID != ""
}

func reviewCycleRecoveryCanRetry(
	cycle *ReviewCycleState,
	ownership ReviewWorkerOwnership,
) bool {
	if cycle == nil || ownership.Attempt <= 0 {
		return false
	}
	var retries int
	switch ownership.Identity.Role {
	case AgentProfileRoleDiscovery, AgentProfileRoleChallenge:
		retries = cycle.Policy.Swarm.Retries
	case AgentProfileRoleVerifier:
		retries = cycle.Policy.Verification.Retries
	default:
		return false
	}
	return ownership.Attempt <= retries
}

func (b *Orchestrator) reconcilePersistedReviewCycles(
	ctx context.Context,
) error {
	if b == nil || b.agents == nil {
		return nil
	}
	b.reviewCycleRecoveryInitMu.Lock()
	defer b.reviewCycleRecoveryInitMu.Unlock()
	if b.reviewCycleRecoveryReady {
		return nil
	}

	agents := b.agents.List()
	ownedWorkers := make(map[string]struct{})
	for _, agent := range agents {
		if agent.Role != RoleReviewer || agent.ReviewCycle == nil {
			continue
		}
		for _, ownership := range agent.ReviewCycle.WorkerOwnerships {
			ownedWorkers[ownership.OwnerID] = struct{}{}
		}
	}
	if err := b.cleanupOrphanedReviewWorkerArtifacts(
		ownedWorkers,
	); err != nil {
		return err
	}

	sort.Slice(agents, func(i, j int) bool {
		return agents[i].ID < agents[j].ID
	})
	for _, agent := range agents {
		if agent.Role != RoleReviewer || agent.ReviewCycle == nil {
			continue
		}
		if !agent.Paused &&
			!agentLifecycleTerminal(&agent) &&
			agent.State == StateWorking &&
			(b.reviewCoordinatorPullRequests != nil ||
				b.github != nil) {
			reconciled, err :=
				b.reconcileConvergentReviewPullRequest(
					ctx,
					agent.ID,
				)
			if err != nil {
				return fmt.Errorf(
					"failed to reconcile persisted review cycle %s PR boundary: %w",
					agent.ID,
					err,
				)
			}
			agent = reconciled
		}
		shouldRecover := false
		switch {
		case agent.ReviewCoordinatorLifecycle != nil &&
			agent.ReviewCoordinatorLifecycle.CompletedAt.IsZero():
			shouldRecover = true
		case agent.Paused || agentLifecycleTerminal(&agent):
			shouldRecover = false
		case agent.State == StateWorking:
			// The coordinator owns the complete path, including a crash
			// before inputs, a plan, or ownership could be checkpointed.
			shouldRecover = true
		}
		if shouldRecover {
			b.queuePersistedReviewCycleRecovery(ctx, agent.ID)
		}
	}
	b.reviewCycleRecoveryReady = true
	return nil
}

func (b *Orchestrator) beginPersistedReviewCycleRecovery(
	reviewerID string,
) bool {
	b.reviewCycleRecoveryMu.Lock()
	defer b.reviewCycleRecoveryMu.Unlock()
	if b.reviewCycleRecoveryClosing {
		return false
	}
	if b.reviewCycleRecoveryPending == nil {
		b.reviewCycleRecoveryPending = make(map[string]struct{})
	}
	if _, pending := b.reviewCycleRecoveryPending[reviewerID]; pending {
		return false
	}
	b.reviewCycleRecoveryPending[reviewerID] = struct{}{}
	b.reviewCycleRecoveryWG.Add(1)
	return true
}

func (b *Orchestrator) finishPersistedReviewCycleRecovery(
	reviewerID string,
) {
	b.reviewCycleRecoveryMu.Lock()
	delete(b.reviewCycleRecoveryPending, reviewerID)
	b.reviewCycleRecoveryMu.Unlock()
	b.reviewCycleRecoveryWG.Done()
}

func (b *Orchestrator) queuePersistedReviewCycleRecovery(
	ctx context.Context,
	reviewerID string,
) {
	if !b.beginPersistedReviewCycleRecovery(reviewerID) {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		defer b.finishPersistedReviewCycleRecovery(reviewerID)
		err := b.resumeConfiguredReviewCycle(ctx, reviewerID)
		if err != nil && ctx.Err() == nil {
			failure := b.safeError(err)
			_ = b.agents.SetFailureMessage(reviewerID, failure)
			log.Printf(
				"persisted review cycle recovery failed reviewer=%s: %s",
				reviewerID,
				failure,
			)
		}
	}()
}

func (b *Orchestrator) resumeConfiguredReviewCycle(
	ctx context.Context,
	reviewerID string,
) error {
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return nil
	}
	if lifecycle := reviewer.ReviewCoordinatorLifecycle; lifecycle != nil &&
		lifecycle.CompletedAt.IsZero() {
		return b.recoverPersistedReviewCycle(ctx, reviewer.ID)
	}
	// In production both boundaries are configured. The nil pair is used by
	// focused durable-worker lifecycle tests.
	if b.reviewCoordinatorGit == nil &&
		b.reviewCoordinatorPullRequests == nil {
		return b.recoverPersistedReviewCycle(ctx, reviewer.ID)
	}
	_, err := b.RunConvergentReviewCycle(ctx, reviewer.ID)
	return err
}

func (b *Orchestrator) waitForPersistedReviewCycleRecoveries() {
	if b == nil {
		return
	}
	b.reviewCycleRecoveryMu.Lock()
	b.reviewCycleRecoveryClosing = true
	b.reviewCycleRecoveryMu.Unlock()
	b.reviewCycleRecoveryWG.Wait()
}

func reviewCoordinatorLifecycleRequestFromState(
	state *ReviewCoordinatorLifecycleState,
) (reviewCoordinatorLifecycleRequest, error) {
	if state == nil {
		return reviewCoordinatorLifecycleRequest{},
			errors.New("review coordinator lifecycle state is missing")
	}
	request := reviewCoordinatorLifecycleRequest{
		Intent:                     state.Intent,
		FinalState:                 state.FinalState,
		ReleaseCoordinatorWorktree: state.ReleaseCoordinatorWorktree,
		ReleaseRuntimeArtifacts:    state.ReleaseRuntimeArtifacts,
		PreservePersistedHandoff:   state.PreservePersistedHandoff,
		CaptureHandoff:             state.CaptureHandoff,
	}
	if err := validateReviewCoordinatorLifecycleRequest(request); err != nil {
		return reviewCoordinatorLifecycleRequest{}, err
	}
	return request, nil
}

func (b *Orchestrator) recoverPersistedReviewCycle(
	ctx context.Context,
	reviewerID string,
) error {
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return nil
	}
	if lifecycle := reviewer.ReviewCoordinatorLifecycle; lifecycle != nil {
		if !lifecycle.CompletedAt.IsZero() {
			return nil
		}
		request, err :=
			reviewCoordinatorLifecycleRequestFromState(lifecycle)
		if err != nil {
			return err
		}
		_, err = b.transitionReviewCoordinatorLifecycle(
			ctx,
			reviewer.ID,
			request,
		)
		return err
	}
	if reviewer.Paused || agentLifecycleTerminal(&reviewer) ||
		reviewer.State != StateWorking {
		return nil
	}

	recoveryCtx, finish, err := b.registerReviewDiscoveryPass(
		ctx,
		reviewer.ID,
	)
	if err != nil {
		return err
	}
	defer finish()

	work, err := collectReviewCycleRecoveryWork(reviewer.ReviewCycle)
	if err != nil {
		return err
	}
	currentOwners := make(map[string]struct{}, len(work))
	for _, item := range work {
		if ownership, found := reviewCycleRecoveryOwnership(
			reviewer.ReviewCycle,
			item,
		); found {
			currentOwners[ownership.OwnerID] = struct{}{}
		}
	}
	for _, ownership := range reviewer.ReviewCycle.WorkerOwnerships {
		if _, current := currentOwners[ownership.OwnerID]; current {
			continue
		}
		stopErr := b.stopPersistedReviewWorkerIfLive(
			ownership,
		)
		if stopErr != nil {
			return stopErr
		}
		switch ownership.Lifecycle {
		case ReviewWorkerReserved, ReviewWorkerRunning:
			if _, err := b.transitionAndPersistReviewWorkerLifecycle(
				reviewer.ID,
				ownership.OwnerID,
				ReviewWorkerCancelled,
				nil,
			); err != nil {
				return err
			}
		}
	}

	var failures []error
	for _, item := range work {
		if err := recoveryCtx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		if err := b.recoverPersistedReviewWork(
			recoveryCtx,
			reviewer.ID,
			item,
		); err != nil {
			failures = append(failures, err)
			current, ok := b.agents.Get(reviewer.ID)
			if ok && current.ReviewCycle != nil {
				ownership, found := reviewCycleRecoveryOwnership(
					current.ReviewCycle,
					item,
				)
				if found && !durableLaunchTerminal(
					ownership.Lifecycle,
				) {
					return errors.Join(failures...)
				}
			}
		}
	}

	current, ok := b.agents.Get(reviewer.ID)
	if !ok || current.ReviewCycle == nil {
		return errors.Join(
			append(
				failures,
				errors.New(
					"review coordinator disappeared during restart recovery",
				),
			)...,
		)
	}
	if reviewCycleHasActiveRound(current.ReviewCycle) {
		last := len(current.ReviewCycle.Convergence.Rounds) - 1
		round := current.ReviewCycle.Convergence.Rounds[last].Round
		roundErr := b.resumePersistedReviewConvergenceRound(
			recoveryCtx,
			reviewer.ID,
			round,
			errors.Join(failures...),
		)
		return roundErr
	}
	if reviewCycleNeedsReportPublication(current.ReviewCycle) {
		if _, err := b.publishReviewCycleVerdict(
			recoveryCtx,
			reviewer.ID,
		); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (b *Orchestrator) stopPersistedReviewWorkerIfLive(
	ownership ReviewWorkerOwnership,
) error {
	if b.runner == nil {
		return errors.New("review worker runtime is not configured")
	}
	handle := RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: ownership.SessionName,
	}
	alive, err := b.runner.IsAlive(handle)
	if err != nil {
		return fmt.Errorf(
			"failed to inspect persisted review worker %s: %w",
			ownership.OwnerID,
			err,
		)
	}
	if !alive {
		return nil
	}
	if err := b.runner.Stop(handle); err != nil {
		return fmt.Errorf(
			"failed to stop superseded persisted review worker %s: %w",
			ownership.OwnerID,
			err,
		)
	}
	return nil
}

func reviewCycleRecoveryPhase(
	role AgentProfileRole,
) (ReviewArtifactPhase, error) {
	switch role {
	case AgentProfileRoleDiscovery:
		return ReviewArtifactPhaseDiscovery, nil
	case AgentProfileRoleVerifier:
		return ReviewArtifactPhaseVerification, nil
	case AgentProfileRoleChallenge:
		return ReviewArtifactPhaseChallenge, nil
	default:
		return "", fmt.Errorf(
			"review recovery role %q is unsupported",
			role,
		)
	}
}

func (b *Orchestrator) intakePersistedReviewWorkerArtifactIfPublished(
	ctx context.Context,
	reviewerID string,
	ownership ReviewWorkerOwnership,
	artifactPath string,
	phase ReviewArtifactPhase,
) (bool, error) {
	published, err := reviewDiscoveryArtifactIsPublished(artifactPath)
	if err != nil {
		return false, err
	}
	if !published {
		return false, nil
	}
	if _, err := b.intakeReviewWorkerArtifact(
		ctx,
		reviewerID,
		ownership.OwnerID,
		artifactPath,
		phase,
	); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Orchestrator) monitorPersistedReviewWorker(
	ctx context.Context,
	reviewerID string,
	work reviewCycleRecoveryWork,
	ownership ReviewWorkerOwnership,
) error {
	phase, err := reviewCycleRecoveryPhase(work.Identity.Role)
	if err != nil {
		return err
	}
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return errors.New("review coordinator disappeared during recovery")
	}
	// reviewWorkerRuntimeTimeoutAt folds the cycle's *remaining* wall-time
	// budget into the per-worker timeout it returns, so once the overall
	// budget (Convergence.MaxWallTimeMinutes) is exhausted -- easy to
	// happen across a long post-restart recovery, or simply because a
	// couple of lanes were genuinely slow -- it returns zero regardless
	// of a perfectly valid per-worker configuration. Surface that the
	// same way the live (non-recovery) reservation path already does
	// (reserveReviewWorkerOwnershipForActiveCoordinator) if this cycle
	// has already recorded a limit transition, instead of a generic,
	// unhandled "timeout is invalid" error that doesn't get recognized
	// as budget exhaustion anywhere and can loop indefinitely without
	// ever reaching a final review result.
	if err := existingReviewLimitReachedError(reviewer.ReviewCycle); err != nil {
		return err
	}
	handle := RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: ownership.SessionName,
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		return err
	}
	artifactPath := filepath.Join(
		artifactDirectory,
		reviewArtifactFilename(ownership),
	)
	deadline := time.Time{}
	ticker := time.NewTicker(reviewDiscoveryWorkerPollInterval)
	defer ticker.Stop()
	for {
		published, err :=
			b.intakePersistedReviewWorkerArtifactIfPublished(
				ctx,
				reviewerID,
				ownership,
				artifactPath,
				phase,
			)
		if err != nil {
			return err
		}
		if published {
			return b.runner.Stop(handle)
		}
		alive, err := b.runner.IsAlive(handle)
		if err != nil {
			return fmt.Errorf(
				"failed to inspect persisted review worker %s: %w",
				ownership.OwnerID,
				err,
			)
		}
		if !alive {
			published, err =
				b.intakePersistedReviewWorkerArtifactIfPublished(
					ctx,
					reviewerID,
					ownership,
					artifactPath,
					phase,
				)
			if err != nil {
				return err
			}
			if published {
				return nil
			}
			return errPersistedReviewWorkerMissing
		}
		if deadline.IsZero() {
			if ownership.StartedAt.IsZero() {
				ownerID := ownership.OwnerID
				ownership, err =
					b.transitionAndPersistReviewWorkerLifecycle(
						reviewerID,
						ownership.OwnerID,
						ReviewWorkerRunning,
						nil,
					)
				if err != nil {
					return fmt.Errorf(
						"failed to reconcile late-starting persisted review worker %s: %w",
						ownerID,
						err,
					)
				}
			}
			deadline = reviewWorkerRuntimeDeadline(
				reviewer.ReviewCycle,
				work.Identity.Role,
				ownership.StartedAt,
			)
			if deadline.IsZero() {
				return errors.New(
					"persisted review worker runtime deadline is invalid",
				)
			}
		}
		if !time.Now().Before(deadline) {
			if err := b.runner.Stop(handle); err != nil {
				return err
			}
			published, err =
				b.intakePersistedReviewWorkerArtifactIfPublished(
					ctx,
					reviewerID,
					ownership,
					artifactPath,
					phase,
				)
			if err != nil {
				return err
			}
			if published {
				return nil
			}
			return errPersistedReviewWorkerMissing
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Orchestrator) reconcileLivePersistedReviewWorkerLifecycle(
	reviewerID string,
	ownership ReviewWorkerOwnership,
) (ReviewWorkerOwnership, error) {
	if ownership.Lifecycle != ReviewWorkerReserved {
		return ownership, nil
	}
	if b.runner == nil {
		return ReviewWorkerOwnership{},
			errors.New("review worker runtime is not configured")
	}
	handle := RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: ownership.SessionName,
	}
	alive, err := b.runner.IsAlive(handle)
	if err != nil {
		return ReviewWorkerOwnership{}, fmt.Errorf(
			"failed to inspect persisted review worker %s: %w",
			ownership.OwnerID,
			err,
		)
	}
	if !alive {
		return ownership, nil
	}
	running, err := b.transitionAndPersistReviewWorkerLifecycle(
		reviewerID,
		ownership.OwnerID,
		ReviewWorkerRunning,
		nil,
	)
	if err != nil {
		return ReviewWorkerOwnership{}, fmt.Errorf(
			"failed to reconcile live persisted review worker %s: %w",
			ownership.OwnerID,
			err,
		)
	}
	return running, nil
}

func (b *Orchestrator) recoverPersistedReviewWork(
	ctx context.Context,
	reviewerID string,
	work reviewCycleRecoveryWork,
) error {
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCycle == nil {
		return nil
	}
	if ownership, found := reviewCycleRecoveryOwnership(
		reviewer.ReviewCycle,
		work,
	); found {
		if durableLaunchTerminal(ownership.Lifecycle) {
			if err := b.bindPersistedReviewWorkerOwnership(
				reviewerID,
				work,
				ownership,
			); err != nil {
				return err
			}
			switch ownership.Lifecycle {
			case ReviewWorkerCompleted:
				return nil
			case ReviewWorkerCancelled:
				return b.failPersistedReviewWork(
					reviewerID,
					work,
					DurableLaunchFailureRuntime,
					true,
				)
			case ReviewWorkerFailed:
				failureKind := DurableLaunchFailureRuntime
				if ownership.Failure != nil {
					failureKind = ownership.Failure.Kind
					if ownership.Failure.Retryable &&
						reviewCycleRecoveryCanRetry(
							reviewer.ReviewCycle,
							ownership,
						) {
						return b.launchPersistedReviewWorkReplacement(
							ctx,
							reviewerID,
							work,
						)
					}
				}
				return b.failPersistedReviewWork(
					reviewerID,
					work,
					failureKind,
					false,
				)
			}
		}
		ownership, err := b.reconcileLivePersistedReviewWorkerLifecycle(
			reviewerID,
			ownership,
		)
		if err != nil {
			return err
		}
		if err := b.bindPersistedReviewWorkerOwnership(
			reviewerID,
			work,
			ownership,
		); err != nil {
			return err
		}
		err = b.monitorPersistedReviewWorker(
			ctx,
			reviewerID,
			work,
			ownership,
		)
		if err != nil {
			stopErr := b.stopPersistedReviewWorkerIfLive(ownership)
			if stopErr != nil {
				return errors.Join(err, stopErr)
			}
			current, found := b.agents.Get(reviewerID)
			if found && reviewCycleRecoveryWorkCompleted(
				current.ReviewCycle,
				work,
			) {
				err = nil
			}
		}
		next := ReviewWorkerCompleted
		var launchFailure *DurableLaunchFailure
		if err != nil {
			next = ReviewWorkerFailed
			failureKind := DurableLaunchFailureRuntime
			artifactFailure := asReviewArtifactError(err)
			if errors.Is(err, errPersistedReviewWorkerMissing) {
				failureKind = DurableLaunchFailureRuntimeMissing
			} else if artifactFailure != nil {
				failureKind = DurableLaunchFailureArtifact
			}
			launchFailure = &DurableLaunchFailure{
				Kind: failureKind,
				Retryable: errors.Is(
					err,
					errPersistedReviewWorkerMissing,
				) || (artifactFailure != nil &&
					artifactFailure.Class == ReviewArtifactFailureTransient),
			}
			if reviewWorkerRunWasCancelled(err) {
				next = ReviewWorkerCancelled
				launchFailure = nil
			}
		}
		if _, lifecycleErr :=
			b.transitionAndPersistReviewWorkerLifecycle(
				reviewerID,
				ownership.OwnerID,
				next,
				launchFailure,
			); lifecycleErr != nil {
			return errors.Join(err, lifecycleErr)
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, errPersistedReviewWorkerMissing) {
			return b.launchPersistedReviewWorkReplacement(
				ctx,
				reviewerID,
				work,
			)
		}
		artifactErr := asReviewArtifactError(err)
		if artifactErr == nil {
			failErr := b.failPersistedReviewWork(
				reviewerID,
				work,
				DurableLaunchFailureRuntime,
				reviewWorkerRunWasCancelled(err),
			)
			if reviewWorkerFailureRequiresCoordinatorAbort(err) {
				return errors.Join(err, failErr)
			}
			return failErr
		}
		switch artifactErr.Class {
		case ReviewArtifactFailureTransient:
			return b.launchPersistedReviewWorkReplacement(
				ctx,
				reviewerID,
				work,
			)
		case ReviewArtifactFailureTerminal:
			failErr := b.failPersistedReviewWork(
				reviewerID,
				work,
				DurableLaunchFailureArtifact,
				false,
			)
			return failErr
		default:
			return b.failPersistedReviewWork(
				reviewerID,
				work,
				DurableLaunchFailureArtifact,
				false,
			)
		}
	}
	return b.launchPersistedReviewWorkReplacement(
		ctx,
		reviewerID,
		work,
	)
}

func reviewCycleRecoveryWorkCompleted(
	cycle *ReviewCycleState,
	work reviewCycleRecoveryWork,
) bool {
	switch work.Identity.Role {
	case AgentProfileRoleDiscovery:
		lane, found := scheduledReviewDiscoveryLane(
			cycle,
			work.Identity.Pass,
			work.Identity.Lane,
		)
		return found && lane.Status == ReviewDiscoveryLaneCompleted
	case AgentProfileRoleVerifier:
		assignment, found := reviewVerificationAssignmentByID(
			cycle,
			work.AssignmentID,
		)
		return found && assignment.Status == ReviewVerificationCompleted
	case AgentProfileRoleChallenge:
		assignment, found := reviewChallengeAssignmentByID(
			cycle,
			work.AssignmentID,
		)
		return found && assignment.Status == ReviewChallengeCompleted
	default:
		return false
	}
}

func (b *Orchestrator) bindPersistedReviewWorkerOwnership(
	reviewerID string,
	work reviewCycleRecoveryWork,
	ownership ReviewWorkerOwnership,
) error {
	if work.OwnerID == ownership.OwnerID &&
		work.Attempt == ownership.Attempt {
		return nil
	}
	switch work.Identity.Role {
	case AgentProfileRoleDiscovery:
		return b.markReviewDiscoveryLaneRunning(
			reviewerID,
			work.Identity.Pass,
			work.Identity.Lane,
			&ownership,
		)
	case AgentProfileRoleVerifier:
		return b.markReviewVerificationRunning(
			reviewerID,
			work.AssignmentID,
			&ownership,
		)
	case AgentProfileRoleChallenge:
		return b.markReviewChallengeRunning(
			reviewerID,
			work.AssignmentID,
			&ownership,
		)
	default:
		return fmt.Errorf(
			"persisted review role %q cannot bind recovered ownership",
			work.Identity.Role,
		)
	}
}

func (b *Orchestrator) failPersistedReviewWork(
	reviewerID string,
	work reviewCycleRecoveryWork,
	failureKind DurableLaunchFailureKind,
	cancelled bool,
) error {
	switch work.Identity.Role {
	case AgentProfileRoleDiscovery:
		code := ReviewDiscoveryFailureRuntime
		if cancelled {
			code = ReviewDiscoveryFailureCanceled
		} else {
			switch failureKind {
			case DurableLaunchFailureSetup,
				DurableLaunchFailureStart:
				code = ReviewDiscoveryFailureLaunch
			case DurableLaunchFailureArtifact:
				code = ReviewDiscoveryFailureArtifact
			case DurableLaunchFailureState:
				code = ReviewDiscoveryFailureState
			}
		}
		return b.failReviewDiscoveryLane(
			reviewerID,
			work.Identity.Pass,
			work.Identity.Lane,
			code,
		)
	case AgentProfileRoleVerifier:
		code := ReviewVerificationFailureRuntime
		if cancelled {
			code = ReviewVerificationFailureCanceled
		} else {
			switch failureKind {
			case DurableLaunchFailureSetup,
				DurableLaunchFailureStart:
				code = ReviewVerificationFailureLaunch
			case DurableLaunchFailureArtifact:
				code = ReviewVerificationFailureArtifact
			case DurableLaunchFailureState:
				code = ReviewVerificationFailureState
			}
		}
		return b.failReviewVerificationAssignment(
			reviewerID,
			work.AssignmentID,
			code,
		)
	case AgentProfileRoleChallenge:
		reviewer, ok := b.agents.Get(reviewerID)
		if !ok || reviewer.ReviewCycle == nil {
			return errors.New(
				"review coordinator disappeared before persisted challenge failure",
			)
		}
		return b.failReviewChallengeAssignment(
			reviewerID,
			work.AssignmentID,
			reviewer.ReviewCycle.Policy.
				FailureActions.VerificationFailure,
		)
	default:
		return fmt.Errorf(
			"persisted review role %q cannot fail recovered work",
			work.Identity.Role,
		)
	}
}

func (b *Orchestrator) launchPersistedReviewWorkReplacement(
	ctx context.Context,
	reviewerID string,
	work reviewCycleRecoveryWork,
) error {
	switch work.Identity.Role {
	case AgentProfileRoleDiscovery:
		reviewer, ok := b.agents.Get(reviewerID)
		if !ok || reviewer.ReviewCycle == nil {
			return errors.New(
				"review coordinator disappeared before discovery recovery",
			)
		}
		lane, scheduled := scheduledReviewDiscoveryLane(
			reviewer.ReviewCycle,
			work.Identity.Pass,
			work.Identity.Lane,
		)
		if !scheduled {
			return errors.New(
				"persisted discovery lane disappeared before recovery",
			)
		}
		if lane.Status == ReviewDiscoveryLaneQueued {
			if err := b.markReviewDiscoveryLaneRunning(
				reviewerID,
				work.Identity.Pass,
				work.Identity.Lane,
				nil,
			); err != nil {
				return err
			}
		}
		runErr := b.executeReviewDiscoveryLane(
			ctx,
			reviewerID,
			work.Identity.Pass,
			work.Identity.Lane,
		)
		current, found := b.agents.Get(reviewerID)
		if found && current.ReviewCycle != nil {
			stored, ok := scheduledReviewDiscoveryLane(
				current.ReviewCycle,
				work.Identity.Pass,
				work.Identity.Lane,
			)
			if ok && stored.Status == ReviewDiscoveryLaneCompleted {
				return nil
			}
		}
		if runErr == nil {
			runErr = errors.New(
				"recovered discovery worker returned without trusted evidence",
			)
		}
		failErr := b.failReviewDiscoveryLane(
			reviewerID,
			work.Identity.Pass,
			work.Identity.Lane,
			reviewDiscoveryFailureCodeForError(runErr),
		)
		if reviewWorkerFailureRequiresCoordinatorAbort(runErr) {
			return errors.Join(runErr, failErr)
		}
		return failErr

	case AgentProfileRoleVerifier:
		reviewer, ok := b.agents.Get(reviewerID)
		if !ok || reviewer.ReviewCycle == nil {
			return errors.New(
				"review coordinator disappeared before verifier recovery",
			)
		}
		assignment, scheduled := reviewVerificationAssignmentByID(
			reviewer.ReviewCycle,
			work.AssignmentID,
		)
		if !scheduled {
			return errors.New(
				"persisted verification assignment disappeared before recovery",
			)
		}
		if assignment.Status == ReviewVerificationQueued {
			if err := b.markReviewVerificationRunning(
				reviewerID,
				assignment.ID,
				nil,
			); err != nil {
				return err
			}
		}
		runErr := b.executeReviewVerificationAssignment(
			ctx,
			reviewerID,
			*assignment,
		)
		current, found := b.agents.Get(reviewerID)
		if found && current.ReviewCycle != nil {
			stored, ok := reviewVerificationAssignmentByID(
				current.ReviewCycle,
				work.AssignmentID,
			)
			if ok && stored.Status == ReviewVerificationCompleted {
				return nil
			}
		}
		if runErr == nil {
			runErr = errors.New(
				"recovered verifier returned without trusted evidence",
			)
		}
		failErr := b.failReviewVerificationAssignment(
			reviewerID,
			work.AssignmentID,
			reviewAssignedWorkerFailureCode(runErr),
		)
		if reviewWorkerFailureRequiresCoordinatorAbort(runErr) {
			return errors.Join(runErr, failErr)
		}
		return failErr

	case AgentProfileRoleChallenge:
		reviewer, ok := b.agents.Get(reviewerID)
		if !ok || reviewer.ReviewCycle == nil {
			return errors.New(
				"review coordinator disappeared before challenge recovery",
			)
		}
		assignment, scheduled := reviewChallengeAssignmentByID(
			reviewer.ReviewCycle,
			work.AssignmentID,
		)
		if !scheduled {
			return errors.New(
				"persisted challenge assignment disappeared before recovery",
			)
		}
		if assignment.Status == ReviewChallengeQueued {
			if err := b.markReviewChallengeRunning(
				reviewerID,
				assignment.ID,
				nil,
			); err != nil {
				return err
			}
		}
		runErr := b.executeReviewChallengeAssignment(
			ctx,
			reviewerID,
			*assignment,
		)
		current, found := b.agents.Get(reviewerID)
		if found && current.ReviewCycle != nil {
			stored, ok := reviewChallengeAssignmentByID(
				current.ReviewCycle,
				work.AssignmentID,
			)
			if ok && stored.Status == ReviewChallengeCompleted {
				return nil
			}
		}
		if runErr == nil {
			runErr = errors.New(
				"recovered challenge returned without trusted evidence",
			)
		}
		failErr := b.failReviewChallengeAssignment(
			reviewerID,
			work.AssignmentID,
			reviewer.ReviewCycle.Policy.
				FailureActions.VerificationFailure,
		)
		if reviewWorkerFailureRequiresCoordinatorAbort(runErr) {
			return errors.Join(runErr, failErr)
		}
		return failErr
	default:
		return fmt.Errorf(
			"persisted review role %q cannot be recovered",
			work.Identity.Role,
		)
	}
}

func (b *Orchestrator) resumePersistedReviewConvergenceRound(
	ctx context.Context,
	reviewerID string,
	round int,
	recoveryErr error,
) error {
	failures := make([]error, 0)
	if recoveryErr != nil {
		failures = append(failures, recoveryErr)
	}
	if _, err := b.runReviewVerificationRound(
		ctx,
		reviewerID,
		round,
	); err != nil {
		failures = append(failures, err)
	}
	for ctx.Err() == nil {
		assignment, err := b.runNextTargetedReviewChallenge(
			ctx,
			reviewerID,
			round,
		)
		if err != nil {
			if assignment.ID != "" ||
				!strings.Contains(
					err.Error(),
					"identical review challenge context",
				) {
				failures = append(failures, err)
			}
			break
		}
		if assignment.ID == "" {
			break
		}
		if _, err := b.runReviewVerificationRound(
			ctx,
			reviewerID,
			round,
		); err != nil {
			failures = append(failures, err)
			break
		}
	}
	if ctx.Err() != nil {
		failures = append(failures, ctx.Err())
	}
	if len(failures) != 0 {
		reviewer, ok := b.agents.Get(reviewerID)
		if ok && reviewer.ReviewCycle != nil {
			if err := b.markReviewConvergenceRoundUnresolved(
				reviewerID,
				reviewer.ReviewCycle.Policy.
					FailureActions.VerificationFailure,
			); err != nil {
				failures = append(failures, err)
			}
		}
	}
	if _, err := b.completeReviewConvergenceRound(
		reviewerID,
	); err != nil {
		failures = append(failures, err)
	}
	current, ok := b.agents.Get(reviewerID)
	if ok && reviewCycleNeedsReportPublication(current.ReviewCycle) {
		if _, err := b.publishReviewCycleVerdict(
			ctx,
			reviewerID,
		); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (b *Orchestrator) cleanupOrphanedReviewWorkerArtifacts(
	ownedWorkers map[string]struct{},
) error {
	worktreeDir := filepath.Clean(strings.TrimSpace(b.cfg.WorktreeDir))
	if worktreeDir == "." || worktreeDir == string(filepath.Separator) ||
		!filepath.IsAbs(worktreeDir) {
		return nil
	}
	entries, err := os.ReadDir(worktreeDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"failed to inspect review worker artifacts: %w",
			err,
		)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() ||
			!strings.HasPrefix(
				name,
				reviewWorkerArtifactDirPrefix+"-",
			) ||
			!strings.HasSuffix(
				name,
				reviewWorkerArtifactOwnerSuffix,
			) {
			continue
		}
		markerPath := filepath.Join(worktreeDir, name)
		ownerID, valid, err :=
			readReviewWorkerArtifactOwnerMarker(markerPath)
		if err != nil {
			return err
		}
		if !valid {
			continue
		}
		if _, owned := ownedWorkers[ownerID]; owned {
			continue
		}
		artifactDirectory := strings.TrimSuffix(
			markerPath,
			reviewWorkerArtifactOwnerSuffix,
		)
		if !reviewWorkerPathWithin(
			worktreeDir,
			artifactDirectory,
		) || filepath.Dir(artifactDirectory) != worktreeDir {
			continue
		}
		if err := os.RemoveAll(artifactDirectory); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(
				"failed to remove orphaned review artifact directory: %w",
				err,
			)
		}
		if err := os.Remove(markerPath); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(
				"failed to remove orphaned review artifact marker: %w",
				err,
			)
		}
		if err := syncReviewArtifactDirectory(worktreeDir); err != nil {
			return fmt.Errorf(
				"failed to sync orphaned review artifact cleanup: %w",
				err,
			)
		}
	}
	return nil
}

func readReviewWorkerArtifactOwnerMarker(
	markerPath string,
) (string, bool, error) {
	info, err := os.Lstat(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Size() <= int64(len(reviewWorkerArtifactOwnerV1)) ||
		info.Size() > int64(
			len(reviewWorkerArtifactOwnerV1)+
				reviewWorkerOwnerNameLimit+1,
		) {
		return "", false, nil
	}
	marker, err := os.Open(markerPath)
	if err != nil {
		return "", false, err
	}
	defer marker.Close()
	openedInfo, err := marker.Stat()
	if err != nil || !os.SameFile(info, openedInfo) {
		return "", false, errors.New(
			"review worker artifact owner marker changed while opening",
		)
	}
	body, err := io.ReadAll(io.LimitReader(
		marker,
		int64(
			len(reviewWorkerArtifactOwnerV1)+
				reviewWorkerOwnerNameLimit+2,
		),
	))
	if err != nil {
		return "", false, err
	}
	text := string(body)
	if !strings.HasPrefix(text, reviewWorkerArtifactOwnerV1) ||
		!strings.HasSuffix(text, "\n") {
		return "", false, nil
	}
	ownerID := strings.TrimSuffix(
		strings.TrimPrefix(
			text,
			reviewWorkerArtifactOwnerV1,
		),
		"\n",
	)
	if ownerID == "" ||
		len(ownerID) > reviewWorkerOwnerNameLimit ||
		sanitizeSessionPart(ownerID) != ownerID ||
		strings.Contains(ownerID, "\n") {
		return "", false, nil
	}
	return ownerID, true, nil
}
