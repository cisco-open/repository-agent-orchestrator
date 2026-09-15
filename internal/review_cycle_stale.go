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
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

var errReviewCycleStale = errors.New("review cycle head is stale")

type reviewCycleStaleError struct {
	reviewerID string
	staleHead  string
	liveHead   string
}

func (err *reviewCycleStaleError) Error() string {
	if err == nil {
		return errReviewCycleStale.Error()
	}
	return fmt.Sprintf(
		"%s: reviewer=%s stale_head=%s live_head=%s",
		errReviewCycleStale,
		err.reviewerID,
		abbreviateSHA(err.staleHead),
		abbreviateSHA(err.liveHead),
	)
}

func (err *reviewCycleStaleError) Unwrap() error {
	return errReviewCycleStale
}

type staleReviewCycleMutation struct {
	reviewerID                       string
	previousStale                    bool
	previousStaleAt                  time.Time
	previousSupersededByHeadSHA      string
	supersedingHeadSHA               string
	previousWorkerOwnerships         []ReviewWorkerOwnership
	previousReviewerState            AgentState
	previousReviewerStopped          bool
	previousReviewerPaused           bool
	previousReviewerLastActivityTime time.Time
	coderID                          string
	previousCoderHeadSHA             string
	previousActiveReviewAgentID      string
	previousGateFailureHeadSHA       string
	previousGateFailureAt            time.Time
	previousCoderLastActivityTime    time.Time
	updatedAt                        time.Time
}

func (m *AgentManager) markReviewCycleStale(
	reviewerID string,
	liveHeadSHA string,
	observedAt time.Time,
) (Agent, staleReviewCycleMutation, bool, error) {
	if m == nil {
		return Agent{}, staleReviewCycleMutation{}, false,
			errors.New("agent manager is not configured")
	}
	reviewerID = strings.TrimSpace(reviewerID)
	liveHeadSHA = strings.ToLower(strings.TrimSpace(liveHeadSHA))
	if err := validateCanonicalGitObjectID(liveHeadSHA); err != nil {
		return Agent{}, staleReviewCycleMutation{}, false,
			fmt.Errorf("live review head SHA is invalid: %w", err)
	}
	if observedAt.IsZero() {
		return Agent{}, staleReviewCycleMutation{}, false,
			errors.New("stale review observation time is missing")
	}
	observedAt = observedAt.UTC()

	parentID := ""
	if reviewer, ok := m.Get(reviewerID); ok {
		parentID = strings.TrimSpace(reviewer.ParentAgentID)
	}
	unlockScopes := m.lockRuntimeScopeMutations(reviewerID, parentID)
	defer unlockScopes()

	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[reviewerID]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return Agent{}, staleReviewCycleMutation{}, false,
			fmt.Errorf("review coordinator %q was not found", reviewerID)
	}
	cycle := reviewer.ReviewCycle
	if cycle.Stale {
		if cycle.SupersededByHeadSHA != liveHeadSHA {
			return Agent{}, staleReviewCycleMutation{}, false,
				fmt.Errorf(
					"review cycle %s is already stale for head %s",
					cycle.ID,
					abbreviateSHA(cycle.SupersededByHeadSHA),
				)
		}
		return cloneAgent(reviewer), staleReviewCycleMutation{}, false, nil
	}
	if cycle.HeadSHA == liveHeadSHA {
		return cloneAgent(reviewer), staleReviewCycleMutation{}, false, nil
	}

	mutation := staleReviewCycleMutation{
		reviewerID:                  reviewer.ID,
		previousStale:               cycle.Stale,
		previousStaleAt:             cycle.StaleAt,
		previousSupersededByHeadSHA: cycle.SupersededByHeadSHA,
		supersedingHeadSHA:          liveHeadSHA,
		previousWorkerOwnerships: append(
			[]ReviewWorkerOwnership(nil),
			cycle.WorkerOwnerships...,
		),
		previousReviewerState:            reviewer.State,
		previousReviewerStopped:          reviewer.Stopped,
		previousReviewerPaused:           reviewer.Paused,
		previousReviewerLastActivityTime: reviewer.LastActivityTime,
		updatedAt:                        observedAt.UTC(),
	}
	cycle.Stale = true
	cycle.StaleAt = mutation.updatedAt
	cycle.SupersededByHeadSHA = liveHeadSHA
	for index := range cycle.WorkerOwnerships {
		ownership := &cycle.WorkerOwnerships[index]
		switch ownership.Lifecycle {
		case ReviewWorkerReserved, ReviewWorkerRunning:
			ownership.Lifecycle = ReviewWorkerCancelled
			ownership.FinishedAt = mutation.updatedAt
		}
	}
	reviewer.State = StateStopped
	reviewer.Stopped = true
	reviewer.Paused = false
	reviewer.LastActivityTime = mutation.updatedAt

	parentID = strings.TrimSpace(reviewer.ParentAgentID)
	if parentID != "" {
		if coder := m.agents[parentID]; coder != nil &&
			coder.Role == RoleCoder {
			mutation.coderID = coder.ID
			mutation.previousCoderHeadSHA = coder.ObservedPRHeadSHA
			mutation.previousActiveReviewAgentID =
				coder.ActiveReviewAgentID
			mutation.previousGateFailureHeadSHA =
				coder.LastPreReviewGateFailureHeadSHA
			mutation.previousGateFailureAt =
				coder.LastPreReviewGateFailureAt
			mutation.previousCoderLastActivityTime =
				coder.LastActivityTime
			coder.ObservedPRHeadSHA = liveHeadSHA
			if strings.EqualFold(
				strings.TrimSpace(coder.ActiveReviewAgentID),
				reviewer.ID,
			) {
				coder.ActiveReviewAgentID = ""
			}
			if !strings.EqualFold(
				coder.LastPreReviewGateFailureHeadSHA,
				liveHeadSHA,
			) {
				coder.LastPreReviewGateFailureHeadSHA = ""
				coder.LastPreReviewGateFailureAt = time.Time{}
			}
			coder.LastActivityTime = mutation.updatedAt
		}
	}
	if err := validatePersistedReviewCycleSnapshot(cycle); err != nil {
		m.rollbackStaleReviewCycleMutationLocked(mutation)
		return Agent{}, staleReviewCycleMutation{}, false,
			fmt.Errorf("stale review cycle audit state is invalid: %w", err)
	}
	return cloneAgent(reviewer), mutation, true, nil
}

func (m *AgentManager) rollbackStaleReviewCycleMutation(
	mutation staleReviewCycleMutation,
) bool {
	if m == nil || mutation.reviewerID == "" {
		return false
	}
	unlockScopes := m.lockRuntimeScopeMutations(
		mutation.reviewerID,
		mutation.coderID,
	)
	defer unlockScopes()

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rollbackStaleReviewCycleMutationLocked(mutation)
}

func (m *AgentManager) rollbackStaleReviewCycleMutationLocked(
	mutation staleReviewCycleMutation,
) bool {
	reviewer := m.agents[mutation.reviewerID]
	if reviewer == nil || reviewer.ReviewCycle == nil ||
		!reviewer.ReviewCycle.Stale ||
		!reviewer.ReviewCycle.StaleAt.Equal(mutation.updatedAt) {
		return false
	}
	reviewer.ReviewCycle.Stale = mutation.previousStale
	reviewer.ReviewCycle.StaleAt = mutation.previousStaleAt
	reviewer.ReviewCycle.SupersededByHeadSHA =
		mutation.previousSupersededByHeadSHA
	reviewer.ReviewCycle.WorkerOwnerships = append(
		[]ReviewWorkerOwnership(nil),
		mutation.previousWorkerOwnerships...,
	)
	reviewer.State = mutation.previousReviewerState
	reviewer.Stopped = mutation.previousReviewerStopped
	reviewer.Paused = mutation.previousReviewerPaused
	reviewer.LastActivityTime =
		mutation.previousReviewerLastActivityTime
	if mutation.coderID != "" {
		if coder := m.agents[mutation.coderID]; coder != nil &&
			coder.ObservedPRHeadSHA ==
				mutation.supersedingHeadSHA {
			coder.ObservedPRHeadSHA = mutation.previousCoderHeadSHA
			coder.ActiveReviewAgentID =
				mutation.previousActiveReviewAgentID
			coder.LastPreReviewGateFailureHeadSHA =
				mutation.previousGateFailureHeadSHA
			coder.LastPreReviewGateFailureAt =
				mutation.previousGateFailureAt
			coder.LastActivityTime =
				mutation.previousCoderLastActivityTime
		}
	}
	return true
}

func (b *Orchestrator) resolveLiveReviewHead(
	ctx context.Context,
	reviewer Agent,
) (string, error) {
	if reviewer.ReviewCycle == nil {
		return "", errors.New("review coordinator has no review cycle")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var (
		headSHA string
		err     error
	)
	switch {
	case b != nil && b.reviewHeadResolver != nil:
		headSHA, err = b.reviewHeadResolver(ctx, reviewer.PRNumber)
	case b != nil && b.github != nil && reviewer.PRNumber > 0:
		headSHA, err = b.getPRHeadSHA(ctx, reviewer.PRNumber)
	default:
		return reviewer.ReviewCycle.HeadSHA, nil
	}
	if err != nil {
		return "", err
	}
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	if err := validateCanonicalGitObjectID(headSHA); err != nil {
		return "", fmt.Errorf("live review head SHA is invalid: %w", err)
	}
	return headSHA, nil
}

func (b *Orchestrator) ensureReviewCycleHeadCurrent(
	ctx context.Context,
	reviewerID string,
) (Agent, error) {
	if b == nil || b.agents == nil {
		return Agent{}, errors.New(
			"orchestrator agent manager is not configured",
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
	if reviewer.ReviewCycle.Stale {
		return reviewer, &reviewCycleStaleError{
			reviewerID: reviewer.ID,
			staleHead:  reviewer.ReviewCycle.HeadSHA,
			liveHead:   reviewer.ReviewCycle.SupersededByHeadSHA,
		}
	}
	liveHeadSHA, err := b.resolveLiveReviewHead(ctx, reviewer)
	if err != nil {
		return Agent{}, fmt.Errorf(
			"failed to recheck live head for review coordinator %s: %w",
			reviewer.ID,
			err,
		)
	}
	if liveHeadSHA == reviewer.ReviewCycle.HeadSHA {
		return reviewer, nil
	}
	invalidateErr := b.invalidateStaleReviewCycle(
		ctx,
		reviewer.ID,
		liveHeadSHA,
	)
	staleErr := &reviewCycleStaleError{
		reviewerID: reviewer.ID,
		staleHead:  reviewer.ReviewCycle.HeadSHA,
		liveHead:   liveHeadSHA,
	}
	if invalidateErr != nil {
		return Agent{}, errors.Join(staleErr, invalidateErr)
	}
	return Agent{}, staleErr
}

func (b *Orchestrator) invalidateStaleReviewCycle(
	ctx context.Context,
	reviewerID string,
	liveHeadSHA string,
) error {
	if b == nil || b.agents == nil {
		return errors.New("orchestrator agent manager is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	liveHeadSHA = strings.ToLower(strings.TrimSpace(liveHeadSHA))
	if err := validateCanonicalGitObjectID(liveHeadSHA); err != nil {
		return fmt.Errorf("live review head SHA is invalid: %w", err)
	}

	passDone := b.cancelReviewCycleWork(reviewerID)
	unlockLifecycle :=
		b.agents.lockReviewCoordinatorTerminalization(reviewerID)
	b.statePersistenceMu.Lock()
	staleReviewer, mutation, changed, err :=
		b.agents.markReviewCycleStale(
			reviewerID,
			liveHeadSHA,
			time.Now().UTC(),
		)
	if err == nil && changed {
		if persistErr := b.persistAgentStateLocked(); persistErr != nil {
			if !b.agents.rollbackStaleReviewCycleMutation(mutation) {
				err = fmt.Errorf(
					"failed to persist stale review cycle and failed to roll back its lifecycle transition: %w",
					persistErr,
				)
			} else {
				err = fmt.Errorf(
					"failed to persist stale review cycle: %w",
					persistErr,
				)
			}
		}
	}
	b.statePersistenceMu.Unlock()
	unlockLifecycle()
	if err != nil {
		b.allowReviewDiscoveryPass(reviewerID)
		return err
	}
	if !changed {
		return nil
	}

	b.logReviewCycleObservation(staleReviewer.ID, "cycle_stale_head")
	b.cancelReviewGate(staleReviewer.ID)
	b.clearReviewGateTracking(staleReviewer.ID)
	b.scheduleStaleReviewCleanup(staleReviewer, passDone)
	log.Printf(
		"review cycle invalidated reviewer=%s coder=%s pr=%d stale_head=%s live_head=%s",
		staleReviewer.ID,
		strings.TrimSpace(staleReviewer.ParentAgentID),
		staleReviewer.PRNumber,
		abbreviateSHA(staleReviewer.ReviewCycle.HeadSHA),
		abbreviateSHA(liveHeadSHA),
	)
	b.notify(
		ctx,
		fmt.Sprintf(
			"Repository Agent Orchestrator: review cycle `%s` on PR %s became stale after the head moved from `%s` to `%s`; its work was canceled and will be ignored",
			staleReviewer.ID,
			fallback(
				strings.TrimSpace(staleReviewer.PRURL),
				fmt.Sprintf("#%d", staleReviewer.PRNumber),
			),
			abbreviateSHA(staleReviewer.ReviewCycle.HeadSHA),
			abbreviateSHA(liveHeadSHA),
		),
	)

	successorCtx := context.WithoutCancel(ctx)
	if err := b.launchReviewCycleSuccessor(
		successorCtx,
		staleReviewer,
		liveHeadSHA,
	); err != nil {
		return fmt.Errorf(
			"failed to launch successor review cycle for head %s: %w",
			abbreviateSHA(liveHeadSHA),
			err,
		)
	}
	return nil
}

func (b *Orchestrator) launchReviewCycleSuccessor(
	ctx context.Context,
	staleReviewer Agent,
	headSHA string,
) error {
	if b.reviewSuccessorLauncher != nil {
		return b.reviewSuccessorLauncher(ctx, staleReviewer, headSHA)
	}
	parentID := strings.TrimSpace(staleReviewer.ParentAgentID)
	if parentID != "" {
		coder, ok := b.agents.Get(parentID)
		if !ok || coder.Stopped {
			return nil
		}
		return b.ensureReviewAgentForCoder(ctx, coder)
	}

	target := reviewLaunchTargetFromReviewer(staleReviewer)
	if !b.reserveReviewLaunch(target.LaunchKey, headSHA) {
		return nil
	}
	go func() {
		defer b.releaseReviewLaunch(target.LaunchKey, headSHA)
		if err := b.startReviewAgentForTarget(
			ctx,
			target,
			headSHA,
		); err != nil {
			log.Printf(
				"successor review launch error pr=%d head=%s: %s",
				target.PRNumber,
				abbreviateSHA(headSHA),
				b.safeError(err),
			)
		}
	}()
	return nil
}

func (b *Orchestrator) beginReviewCleanup() bool {
	b.reviewCleanupMu.Lock()
	defer b.reviewCleanupMu.Unlock()
	if b.reviewCleanupClosing {
		return false
	}
	b.reviewCleanupWG.Add(1)
	return true
}

func (b *Orchestrator) scheduleStaleReviewCleanup(
	reviewer Agent,
	passDone <-chan struct{},
) {
	if !b.beginReviewCleanup() {
		return
	}
	go func() {
		defer b.reviewCleanupWG.Done()
		if passDone != nil {
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			select {
			case <-passDone:
			case <-timer.C:
			}
		}
		if err := b.retireReviewer(
			reviewer,
			StateStopped,
			false,
			"stale review cycle cleanup",
		); err != nil {
			log.Printf(
				"non-fatal: stale review cycle cleanup failed reviewer=%s pr=%d: %s",
				reviewer.ID,
				reviewer.PRNumber,
				b.safeError(err),
			)
		}
	}()
}

func (b *Orchestrator) waitForReviewCleanups() {
	if b == nil {
		return
	}
	b.reviewCleanupMu.Lock()
	b.reviewCleanupClosing = true
	b.reviewCleanupMu.Unlock()
	b.reviewCleanupWG.Wait()
}

func (b *Orchestrator) waitForStaleReviewCleanups() {
	b.waitForReviewCleanups()
}

func (b *Orchestrator) reviewCycleApprovalEligible(
	ctx context.Context,
	reviewerID string,
) (bool, error) {
	reviewer, err := b.ensureReviewCycleHeadCurrent(ctx, reviewerID)
	if errors.Is(err, errReviewCycleStale) &&
		b.reviewCycleMarkedStale(reviewerID) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return reviewer.ReviewCycle.ApprovalEligible(), nil
}

func (b *Orchestrator) reviewCycleMarkedStale(reviewerID string) bool {
	if b == nil || b.agents == nil {
		return false
	}
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	return ok && reviewer.ReviewCycle != nil && reviewer.ReviewCycle.Stale
}
