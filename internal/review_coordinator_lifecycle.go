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
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ReviewCoordinatorLifecycleIntent string

const (
	ReviewCoordinatorLifecyclePause   ReviewCoordinatorLifecycleIntent = "pause"
	ReviewCoordinatorLifecycleStop    ReviewCoordinatorLifecycleIntent = "stop"
	ReviewCoordinatorLifecycleCleanup ReviewCoordinatorLifecycleIntent = "cleanup"
)

type ReviewWorkerCleanupState struct {
	OwnerID                     string    `json:"owner_id"`
	StopAcknowledgedAt          time.Time `json:"stop_acknowledged_at,omitempty"`
	CodexHomeReleasedAt         time.Time `json:"codex_home_released_at,omitempty"`
	WorktreeReleasedAt          time.Time `json:"worktree_released_at,omitempty"`
	GitHubConfigReleasedAt      time.Time `json:"github_config_released_at,omitempty"`
	ArtifactDirectoryReleasedAt time.Time `json:"artifact_directory_released_at,omitempty"`
}

type ReviewCoordinatorLifecycleState struct {
	Generation                    int                              `json:"generation"`
	Intent                        ReviewCoordinatorLifecycleIntent `json:"intent"`
	FinalState                    AgentState                       `json:"final_state"`
	ReleaseCoordinatorWorktree    bool                             `json:"release_coordinator_worktree"`
	ReleaseRuntimeArtifacts       bool                             `json:"release_runtime_artifacts"`
	PreservePersistedHandoff      bool                             `json:"preserve_persisted_handoff"`
	CaptureHandoff                bool                             `json:"capture_handoff"`
	RuntimeLogPath                string                           `json:"runtime_log_path,omitempty"`
	RequestedAt                   time.Time                        `json:"requested_at"`
	RuntimeStopAcknowledgedAt     time.Time                        `json:"runtime_stop_acknowledged_at,omitempty"`
	WorkerStopsAcknowledgedAt     time.Time                        `json:"worker_stops_acknowledged_at,omitempty"`
	CodexHomeReleasedAt           time.Time                        `json:"codex_home_released_at,omitempty"`
	CoordinatorWorktreeReleasedAt time.Time                        `json:"coordinator_worktree_released_at,omitempty"`
	RuntimeLogReleasedAt          time.Time                        `json:"runtime_log_released_at,omitempty"`
	MandatoryTestLogReleasedAt    time.Time                        `json:"mandatory_test_log_released_at,omitempty"`
	PersistedHandoffsReleasedAt   time.Time                        `json:"persisted_handoffs_released_at,omitempty"`
	CompletedAt                   time.Time                        `json:"completed_at,omitempty"`
	WorkerCleanups                []ReviewWorkerCleanupState       `json:"worker_cleanups,omitempty"`
}

type reviewCoordinatorLifecycleRequest struct {
	Intent                     ReviewCoordinatorLifecycleIntent
	FinalState                 AgentState
	ReleaseCoordinatorWorktree bool
	ReleaseRuntimeArtifacts    bool
	PreservePersistedHandoff   bool
	CaptureHandoff             bool
	RuntimeStopAcknowledged    bool
}

type reviewCoordinatorLifecycleResult struct {
	Agent   Agent
	Changed bool
}

type reviewCoordinatorLifecycleMutation struct {
	reviewerID               string
	previousLifecycle        *ReviewCoordinatorLifecycleState
	previousState            AgentState
	previousStopped          bool
	previousPaused           bool
	previousLastActivityTime time.Time
	previousWorkerOwnerships []ReviewWorkerOwnership
	workerLifecycleChanged   bool
	appliedGeneration        int
	appliedRequestedAt       time.Time
}

func cloneReviewCoordinatorLifecycle(
	state *ReviewCoordinatorLifecycleState,
) *ReviewCoordinatorLifecycleState {
	if state == nil {
		return nil
	}
	clone := *state
	clone.WorkerCleanups = append(
		[]ReviewWorkerCleanupState(nil),
		state.WorkerCleanups...,
	)
	return &clone
}

func reviewCoordinatorLifecycleIntentRank(
	intent ReviewCoordinatorLifecycleIntent,
) int {
	switch intent {
	case ReviewCoordinatorLifecyclePause:
		return 1
	case ReviewCoordinatorLifecycleStop:
		return 2
	case ReviewCoordinatorLifecycleCleanup:
		return 3
	default:
		return 0
	}
}

func reviewCoordinatorFinalStateRank(state AgentState) int {
	switch state {
	case StateDone:
		return 3
	case StateErrored:
		return 2
	case StateStopped:
		return 1
	default:
		return 0
	}
}

func validateReviewCoordinatorLifecycleRequest(
	request reviewCoordinatorLifecycleRequest,
) error {
	if reviewCoordinatorLifecycleIntentRank(request.Intent) == 0 {
		return fmt.Errorf(
			"unsupported review coordinator lifecycle intent %q",
			request.Intent,
		)
	}
	if request.PreservePersistedHandoff &&
		!request.ReleaseRuntimeArtifacts {
		return errors.New(
			"persisted handoff policy requires runtime artifact cleanup",
		)
	}
	switch request.Intent {
	case ReviewCoordinatorLifecyclePause:
		if request.ReleaseCoordinatorWorktree ||
			request.ReleaseRuntimeArtifacts {
			return errors.New(
				"paused review coordinator cannot release durable resources",
			)
		}
		if terminalAgentState(request.FinalState) {
			return errors.New(
				"paused review coordinator cannot request a terminal final state",
			)
		}
	case ReviewCoordinatorLifecycleStop,
		ReviewCoordinatorLifecycleCleanup:
		if !request.ReleaseCoordinatorWorktree {
			return errors.New(
				"terminal review coordinator must release its worktree",
			)
		}
		if !terminalAgentState(request.FinalState) {
			return errors.New(
				"terminal review coordinator final state is not terminal",
			)
		}
	}
	return nil
}

func reviewCoordinatorLifecycleAlreadyCompleted(
	reviewer Agent,
	request reviewCoordinatorLifecycleRequest,
) bool {
	state := reviewer.ReviewCoordinatorLifecycle
	if state == nil || state.CompletedAt.IsZero() {
		return false
	}
	if reviewCoordinatorLifecycleIntentRank(state.Intent) <
		reviewCoordinatorLifecycleIntentRank(request.Intent) {
		return false
	}
	if request.ReleaseRuntimeArtifacts &&
		!state.ReleaseRuntimeArtifacts {
		return false
	}
	if request.ReleaseRuntimeArtifacts &&
		!request.PreservePersistedHandoff &&
		state.PreservePersistedHandoff {
		return false
	}
	if request.Intent == ReviewCoordinatorLifecyclePause {
		return reviewer.Paused || agentLifecycleTerminal(&reviewer)
	}
	return agentLifecycleTerminal(&reviewer)
}

func reviewCoordinatorLifecycleSatisfiesRequest(
	state *ReviewCoordinatorLifecycleState,
	request reviewCoordinatorLifecycleRequest,
) bool {
	if state == nil {
		return false
	}
	stateRank := reviewCoordinatorLifecycleIntentRank(state.Intent)
	requestRank := reviewCoordinatorLifecycleIntentRank(request.Intent)
	if stateRank > requestRank {
		return true
	}
	if stateRank != requestRank {
		return false
	}
	if request.ReleaseCoordinatorWorktree &&
		!state.ReleaseCoordinatorWorktree {
		return false
	}
	if request.ReleaseRuntimeArtifacts &&
		!state.ReleaseRuntimeArtifacts {
		return false
	}
	if request.ReleaseRuntimeArtifacts &&
		!request.PreservePersistedHandoff &&
		state.PreservePersistedHandoff {
		return false
	}
	if request.CaptureHandoff && !state.CaptureHandoff {
		return false
	}
	if request.RuntimeStopAcknowledged &&
		state.RuntimeStopAcknowledgedAt.IsZero() {
		return false
	}
	if reviewCoordinatorFinalStateRank(request.FinalState) >
		reviewCoordinatorFinalStateRank(state.FinalState) {
		return false
	}
	return true
}

func reviewWorkerCleanupStatesForRequest(
	reviewer *Agent,
) []ReviewWorkerCleanupState {
	existing := make(map[string]ReviewWorkerCleanupState)
	if reviewer.ReviewCoordinatorLifecycle != nil {
		for _, cleanup := range reviewer.ReviewCoordinatorLifecycle.WorkerCleanups {
			existing[cleanup.OwnerID] = cleanup
		}
	}
	if reviewer.ReviewCycle == nil {
		return nil
	}
	cleanups := make(
		[]ReviewWorkerCleanupState,
		0,
		len(reviewer.ReviewCycle.WorkerOwnerships),
	)
	for _, ownership := range reviewer.ReviewCycle.WorkerOwnerships {
		cleanup := existing[ownership.OwnerID]
		cleanup.OwnerID = ownership.OwnerID
		cleanups = append(cleanups, cleanup)
	}
	return cleanups
}

func (m *AgentManager) beginReviewCoordinatorLifecycle(
	reviewerID string,
	request reviewCoordinatorLifecycleRequest,
	requestedAt time.Time,
) (
	Agent,
	reviewCoordinatorLifecycleMutation,
	bool,
	error,
) {
	if m == nil {
		return Agent{}, reviewCoordinatorLifecycleMutation{}, false,
			errors.New("agent manager is not configured")
	}
	if err := validateReviewCoordinatorLifecycleRequest(request); err != nil {
		return Agent{}, reviewCoordinatorLifecycleMutation{}, false, err
	}
	requestedAt = requestedAt.UTC()
	if requestedAt.IsZero() {
		return Agent{}, reviewCoordinatorLifecycleMutation{}, false,
			errors.New("review coordinator lifecycle request time is missing")
	}

	unlockScope := m.lockRuntimeScopeMutation(reviewerID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer {
		return Agent{}, reviewCoordinatorLifecycleMutation{}, false,
			fmt.Errorf(
				"review coordinator %q was not found",
				strings.TrimSpace(reviewerID),
			)
	}
	if reviewCoordinatorLifecycleAlreadyCompleted(*reviewer, request) {
		return cloneAgent(reviewer), reviewCoordinatorLifecycleMutation{}, true, nil
	}

	current := reviewer.ReviewCoordinatorLifecycle
	if current != nil && current.CompletedAt.IsZero() &&
		reviewCoordinatorLifecycleSatisfiesRequest(current, request) {
		return cloneAgent(reviewer), reviewCoordinatorLifecycleMutation{}, false, nil
	}

	mutation := reviewCoordinatorLifecycleMutation{
		reviewerID:               reviewer.ID,
		previousLifecycle:        cloneReviewCoordinatorLifecycle(current),
		previousState:            reviewer.State,
		previousStopped:          reviewer.Stopped,
		previousPaused:           reviewer.Paused,
		previousLastActivityTime: reviewer.LastActivityTime,
	}
	if reviewer.ReviewCycle != nil {
		mutation.previousWorkerOwnerships = append(
			[]ReviewWorkerOwnership(nil),
			reviewer.ReviewCycle.WorkerOwnerships...,
		)
	}
	generation := 1
	if current != nil {
		generation = current.Generation + 1
	}
	effectiveRequest := request
	if current != nil &&
		current.Intent == request.Intent {
		effectiveRequest.ReleaseCoordinatorWorktree =
			current.ReleaseCoordinatorWorktree ||
				request.ReleaseCoordinatorWorktree
		effectiveRequest.ReleaseRuntimeArtifacts =
			current.ReleaseRuntimeArtifacts ||
				request.ReleaseRuntimeArtifacts
		deletePersistedHandoff :=
			(current.ReleaseRuntimeArtifacts &&
				!current.PreservePersistedHandoff) ||
				(request.ReleaseRuntimeArtifacts &&
					!request.PreservePersistedHandoff)
		effectiveRequest.PreservePersistedHandoff =
			effectiveRequest.ReleaseRuntimeArtifacts &&
				!deletePersistedHandoff
		effectiveRequest.CaptureHandoff =
			current.CaptureHandoff || request.CaptureHandoff
		if reviewCoordinatorFinalStateRank(current.FinalState) >
			reviewCoordinatorFinalStateRank(request.FinalState) {
			effectiveRequest.FinalState = current.FinalState
		}
	}
	reviewer.ReviewCoordinatorLifecycle = &ReviewCoordinatorLifecycleState{
		Generation:                 generation,
		Intent:                     effectiveRequest.Intent,
		FinalState:                 effectiveRequest.FinalState,
		ReleaseCoordinatorWorktree: effectiveRequest.ReleaseCoordinatorWorktree,
		ReleaseRuntimeArtifacts:    effectiveRequest.ReleaseRuntimeArtifacts,
		PreservePersistedHandoff:   effectiveRequest.PreservePersistedHandoff,
		CaptureHandoff:             effectiveRequest.CaptureHandoff,
		RequestedAt:                requestedAt,
		WorkerCleanups:             reviewWorkerCleanupStatesForRequest(reviewer),
	}
	if current != nil {
		reviewer.ReviewCoordinatorLifecycle.RuntimeLogPath =
			current.RuntimeLogPath
		if strings.TrimSpace(reviewer.RuntimeHandle.Session) == "" {
			reviewer.ReviewCoordinatorLifecycle.RuntimeStopAcknowledgedAt =
				current.RuntimeStopAcknowledgedAt
		}
		allWorkerStopsAcknowledged := true
		for _, cleanup := range reviewer.ReviewCoordinatorLifecycle.WorkerCleanups {
			if cleanup.StopAcknowledgedAt.IsZero() {
				allWorkerStopsAcknowledged = false
				break
			}
		}
		if allWorkerStopsAcknowledged {
			reviewer.ReviewCoordinatorLifecycle.WorkerStopsAcknowledgedAt =
				current.WorkerStopsAcknowledgedAt
		}
		reviewer.ReviewCoordinatorLifecycle.CoordinatorWorktreeReleasedAt =
			current.CoordinatorWorktreeReleasedAt
		reviewer.ReviewCoordinatorLifecycle.CodexHomeReleasedAt =
			current.CodexHomeReleasedAt
		reviewer.ReviewCoordinatorLifecycle.RuntimeLogReleasedAt =
			current.RuntimeLogReleasedAt
		reviewer.ReviewCoordinatorLifecycle.MandatoryTestLogReleasedAt =
			current.MandatoryTestLogReleasedAt
		reviewer.ReviewCoordinatorLifecycle.PersistedHandoffsReleasedAt =
			current.PersistedHandoffsReleasedAt
	}
	if logPath := strings.TrimSpace(reviewer.RuntimeHandle.LogPath); logPath != "" {
		reviewer.ReviewCoordinatorLifecycle.RuntimeLogPath = logPath
	}
	if request.RuntimeStopAcknowledged {
		reviewer.ReviewCoordinatorLifecycle.RuntimeStopAcknowledgedAt =
			requestedAt
		reviewer.RuntimeHandle = RuntimeHandle{}
	}
	mutation.appliedGeneration = generation
	mutation.appliedRequestedAt = requestedAt
	if request.Intent == ReviewCoordinatorLifecyclePause {
		reviewer.Paused = true
	} else {
		reviewer.State = StateStopped
		reviewer.Stopped = true
		reviewer.Paused = false
	}
	if reviewer.ReviewCycle != nil &&
		request.Intent != ReviewCoordinatorLifecyclePause {
		for index := range reviewer.ReviewCycle.WorkerOwnerships {
			ownership := &reviewer.ReviewCycle.WorkerOwnerships[index]
			switch ownership.Lifecycle {
			case ReviewWorkerReserved, ReviewWorkerRunning:
				ownership.Lifecycle = ReviewWorkerCancelled
				ownership.FinishedAt = requestedAt
				mutation.workerLifecycleChanged = true
			}
		}
	}
	reviewer.LastActivityTime = requestedAt.UTC()
	return cloneAgent(reviewer), mutation, false, nil
}

func (m *AgentManager) rollbackReviewCoordinatorLifecycle(
	mutation reviewCoordinatorLifecycleMutation,
) bool {
	if m == nil || strings.TrimSpace(mutation.reviewerID) == "" {
		return false
	}
	unlockScope := m.lockRuntimeScopeMutation(mutation.reviewerID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer := m.agents[mutation.reviewerID]
	if reviewer == nil || reviewer.ReviewCoordinatorLifecycle == nil ||
		reviewer.ReviewCoordinatorLifecycle.Generation !=
			mutation.appliedGeneration ||
		!reviewer.ReviewCoordinatorLifecycle.RequestedAt.Equal(
			mutation.appliedRequestedAt,
		) {
		return false
	}
	reviewer.ReviewCoordinatorLifecycle =
		cloneReviewCoordinatorLifecycle(mutation.previousLifecycle)
	reviewer.State = mutation.previousState
	reviewer.Stopped = mutation.previousStopped
	reviewer.Paused = mutation.previousPaused
	if mutation.workerLifecycleChanged && reviewer.ReviewCycle != nil {
		reviewer.ReviewCycle.WorkerOwnerships = append(
			[]ReviewWorkerOwnership(nil),
			mutation.previousWorkerOwnerships...,
		)
	}
	reviewer.LastActivityTime = mutation.previousLastActivityTime
	return true
}

func (m *AgentManager) updateReviewCoordinatorLifecycle(
	reviewerID string,
	generation int,
	update func(*Agent, *ReviewCoordinatorLifecycleState) error,
) (
	Agent,
	reviewCoordinatorLifecycleMutation,
	error,
) {
	if m == nil {
		return Agent{}, reviewCoordinatorLifecycleMutation{},
			errors.New("agent manager is not configured")
	}
	unlockScope := m.lockRuntimeScopeMutation(reviewerID)
	defer unlockScope()

	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCoordinatorLifecycle == nil ||
		reviewer.ReviewCoordinatorLifecycle.Generation != generation {
		return Agent{}, reviewCoordinatorLifecycleMutation{},
			fmt.Errorf(
				"review coordinator %q lifecycle generation %d was not found",
				strings.TrimSpace(reviewerID),
				generation,
			)
	}
	mutation := reviewCoordinatorLifecycleMutation{
		reviewerID:               reviewer.ID,
		previousLifecycle:        cloneReviewCoordinatorLifecycle(reviewer.ReviewCoordinatorLifecycle),
		previousState:            reviewer.State,
		previousStopped:          reviewer.Stopped,
		previousPaused:           reviewer.Paused,
		previousLastActivityTime: reviewer.LastActivityTime,
		appliedGeneration:        generation,
		appliedRequestedAt:       reviewer.ReviewCoordinatorLifecycle.RequestedAt,
	}
	if err := update(reviewer, reviewer.ReviewCoordinatorLifecycle); err != nil {
		return Agent{}, reviewCoordinatorLifecycleMutation{}, err
	}
	reviewer.LastActivityTime = time.Now().UTC()
	return cloneAgent(reviewer), mutation, nil
}

func (b *Orchestrator) persistReviewCoordinatorLifecycleUpdate(
	reviewerID string,
	generation int,
	rollbackOnFailure bool,
	update func(*Agent, *ReviewCoordinatorLifecycleState) error,
) (Agent, error) {
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	reviewer, mutation, err := b.agents.updateReviewCoordinatorLifecycle(
		reviewerID,
		generation,
		update,
	)
	if err != nil {
		return Agent{}, err
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if rollbackOnFailure &&
			!b.agents.rollbackReviewCoordinatorLifecycle(mutation) {
			return Agent{}, fmt.Errorf(
				"failed to persist review coordinator lifecycle and failed to roll back its in-memory transition: %w",
				err,
			)
		}
		return Agent{}, fmt.Errorf(
			"failed to persist review coordinator lifecycle: %w",
			err,
		)
	}
	return reviewer, nil
}

func (b *Orchestrator) beginPersistedReviewCoordinatorLifecycle(
	reviewerID string,
	request reviewCoordinatorLifecycleRequest,
) (Agent, bool, error) {
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	reviewer, mutation, completed, err :=
		b.agents.beginReviewCoordinatorLifecycle(
			reviewerID,
			request,
			time.Now().UTC(),
		)
	if err != nil || completed ||
		mutation.appliedGeneration == 0 {
		return reviewer, completed, err
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if !b.agents.rollbackReviewCoordinatorLifecycle(mutation) {
			return Agent{}, false, fmt.Errorf(
				"failed to persist review coordinator cleanup intent and failed to roll it back: %w",
				err,
			)
		}
		return Agent{}, false, fmt.Errorf(
			"failed to persist review coordinator cleanup intent: %w",
			err,
		)
	}
	return reviewer, false, nil
}

func reviewWorkerCleanupForOwner(
	state *ReviewCoordinatorLifecycleState,
	ownerID string,
) (*ReviewWorkerCleanupState, error) {
	for index := range state.WorkerCleanups {
		if state.WorkerCleanups[index].OwnerID == ownerID {
			return &state.WorkerCleanups[index], nil
		}
	}
	return nil, fmt.Errorf(
		"review worker %q is missing from the persisted cleanup intent",
		ownerID,
	)
}

func reviewWorkerOwnershipForCleanup(
	reviewer Agent,
	ownerID string,
) (ReviewWorkerOwnership, bool) {
	if reviewer.ReviewCycle == nil {
		return ReviewWorkerOwnership{}, false
	}
	for _, ownership := range reviewer.ReviewCycle.WorkerOwnerships {
		if ownership.OwnerID == ownerID {
			return ownership, true
		}
	}
	return ReviewWorkerOwnership{}, false
}

func (b *Orchestrator) cleanupOwnedReviewWorkersForLifecycle(
	ctx context.Context,
	reviewerID string,
	generation int,
) error {
	reviewer, ok := b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCoordinatorLifecycle == nil ||
		reviewer.ReviewCoordinatorLifecycle.Generation != generation {
		return fmt.Errorf(
			"review coordinator %q cleanup intent is unavailable",
			reviewerID,
		)
	}
	state := reviewer.ReviewCoordinatorLifecycle
	if len(state.WorkerCleanups) == 0 {
		if state.WorkerStopsAcknowledgedAt.IsZero() {
			_, err := b.persistReviewCoordinatorLifecycleUpdate(
				reviewerID,
				generation,
				false,
				func(
					_ *Agent,
					lifecycle *ReviewCoordinatorLifecycleState,
				) error {
					lifecycle.WorkerStopsAcknowledgedAt = time.Now().UTC()
					return nil
				},
			)
			return err
		}
		return nil
	}
	if b.runner == nil {
		return errors.New("review worker cleanup runtime is not configured")
	}

	errs := make([]string, 0)
	for _, cleanup := range state.WorkerCleanups {
		if !cleanup.StopAcknowledgedAt.IsZero() {
			continue
		}
		ownership, found := reviewWorkerOwnershipForCleanup(
			reviewer,
			cleanup.OwnerID,
		)
		if !found {
			errs = appendCleanupError(
				errs,
				fmt.Errorf(
					"review worker %q cleanup ownership was not found",
					cleanup.OwnerID,
				),
			)
			continue
		}
		if err := validateReviewWorkerOwnership(ownership); err != nil {
			errs = appendCleanupError(
				errs,
				fmt.Errorf(
					"review worker %q has invalid cleanup ownership: %w",
					ownership.OwnerID,
					err,
				),
			)
			continue
		}
		if err := b.runner.Stop(RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: ownership.SessionName,
		}); err != nil {
			errs = appendCleanupError(errs, err)
			continue
		}
		_, err := b.persistReviewCoordinatorLifecycleUpdate(
			reviewerID,
			generation,
			false,
			func(
				_ *Agent,
				lifecycle *ReviewCoordinatorLifecycleState,
			) error {
				current, err := reviewWorkerCleanupForOwner(
					lifecycle,
					ownership.OwnerID,
				)
				if err != nil {
					return err
				}
				if current.StopAcknowledgedAt.IsZero() {
					current.StopAcknowledgedAt = time.Now().UTC()
				}
				return nil
			},
		)
		errs = appendCleanupError(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf(
			"review worker stop completed with errors: %s",
			strings.Join(errs, "; "),
		)
	}

	reviewer, ok = b.agents.Get(reviewerID)
	if !ok || reviewer.ReviewCoordinatorLifecycle == nil {
		return fmt.Errorf(
			"review coordinator %q disappeared during worker cleanup",
			reviewerID,
		)
	}
	if reviewer.ReviewCoordinatorLifecycle.WorkerStopsAcknowledgedAt.IsZero() {
		var err error
		reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
			reviewerID,
			generation,
			false,
			func(
				_ *Agent,
				lifecycle *ReviewCoordinatorLifecycleState,
			) error {
				for _, cleanup := range lifecycle.WorkerCleanups {
					if cleanup.StopAcknowledgedAt.IsZero() {
						return fmt.Errorf(
							"review worker %q stop is not acknowledged",
							cleanup.OwnerID,
						)
					}
				}
				lifecycle.WorkerStopsAcknowledgedAt = time.Now().UTC()
				return nil
			},
		)
		if err != nil {
			return err
		}
	}

	worktreeDir := filepath.Clean(strings.TrimSpace(b.cfg.WorktreeDir))
	errs = errs[:0]
	for _, cleanup := range reviewer.ReviewCoordinatorLifecycle.WorkerCleanups {
		ownership, found := reviewWorkerOwnershipForCleanup(
			reviewer,
			cleanup.OwnerID,
		)
		if !found {
			errs = appendCleanupError(
				errs,
				fmt.Errorf(
					"review worker %q cleanup ownership was not found",
					cleanup.OwnerID,
				),
			)
			continue
		}
		if worktreeDir == "." ||
			worktreeDir == string(filepath.Separator) ||
			!filepath.IsAbs(worktreeDir) ||
			!reviewWorkerPathWithin(worktreeDir, ownership.WorktreePath) ||
			!reviewWorkerPathWithin(worktreeDir, ownership.GitHubConfigPath) {
			errs = appendCleanupError(
				errs,
				fmt.Errorf(
					"review worker %q cleanup paths escaped the configured worktree directory",
					ownership.OwnerID,
				),
			)
			continue
		}

		if cleanup.CodexHomeReleasedAt.IsZero() {
			err := b.removeAgentRuntimeState(Agent{
				ID:           ownership.OwnerID,
				WorktreePath: ownership.WorktreePath,
			})
			if err == nil {
				_, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewerID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						current, err := reviewWorkerCleanupForOwner(
							lifecycle,
							ownership.OwnerID,
						)
						if err != nil {
							return err
						}
						current.CodexHomeReleasedAt = time.Now().UTC()
						return nil
					},
				)
			}
			errs = appendCleanupError(errs, err)
		}
		if cleanup.WorktreeReleasedAt.IsZero() {
			err := b.cleanupWorktree(ctx, ownership.WorktreePath, "")
			if err == nil {
				_, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewerID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						current, err := reviewWorkerCleanupForOwner(
							lifecycle,
							ownership.OwnerID,
						)
						if err != nil {
							return err
						}
						current.WorktreeReleasedAt = time.Now().UTC()
						return nil
					},
				)
			}
			errs = appendCleanupError(errs, err)
		}
		if cleanup.GitHubConfigReleasedAt.IsZero() {
			err := os.RemoveAll(ownership.GitHubConfigPath)
			if err != nil && errors.Is(err, os.ErrNotExist) {
				err = nil
			}
			if err == nil {
				_, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewerID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						current, err := reviewWorkerCleanupForOwner(
							lifecycle,
							ownership.OwnerID,
						)
						if err != nil {
							return err
						}
						current.GitHubConfigReleasedAt = time.Now().UTC()
						return nil
					},
				)
			}
			if err != nil {
				err = fmt.Errorf(
					"failed to remove review worker GitHub config %q: %w",
					ownership.GitHubConfigPath,
					err,
				)
			}
			errs = appendCleanupError(errs, err)
		}
		if cleanup.ArtifactDirectoryReleasedAt.IsZero() {
			err := cleanupReviewWorkerArtifactDirectory(
				worktreeDir,
				ownership,
			)
			if err == nil {
				_, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewerID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						current, err := reviewWorkerCleanupForOwner(
							lifecycle,
							ownership.OwnerID,
						)
						if err != nil {
							return err
						}
						current.ArtifactDirectoryReleasedAt =
							time.Now().UTC()
						return nil
					},
				)
			}
			errs = appendCleanupError(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(
			"review worker cleanup completed with errors: %s",
			strings.Join(errs, "; "),
		)
	}
	return nil
}

func (b *Orchestrator) transitionReviewCoordinatorLifecycle(
	ctx context.Context,
	reviewerID string,
	request reviewCoordinatorLifecycleRequest,
) (reviewCoordinatorLifecycleResult, error) {
	if b == nil || b.agents == nil {
		return reviewCoordinatorLifecycleResult{},
			errors.New("orchestrator agent manager is not configured")
	}
	if err := validateReviewCoordinatorLifecycleRequest(request); err != nil {
		return reviewCoordinatorLifecycleResult{}, err
	}
	reviewer, ok := b.agents.Get(strings.TrimSpace(reviewerID))
	if !ok || reviewer.Role != RoleReviewer {
		return reviewCoordinatorLifecycleResult{},
			fmt.Errorf(
				"review coordinator %q was not found",
				strings.TrimSpace(reviewerID),
			)
	}
	if reviewCoordinatorLifecycleAlreadyCompleted(reviewer, request) {
		return reviewCoordinatorLifecycleResult{Agent: reviewer}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	drainCtx, drainCancel := context.WithTimeout(ctx, 30*time.Second)
	drainErr := b.blockAndDrainReviewDiscoveryPass(
		drainCtx,
		reviewer.ID,
	)
	drainCancel()
	if drainErr != nil {
		b.allowReviewDiscoveryPass(reviewer.ID)
		return reviewCoordinatorLifecycleResult{},
			fmt.Errorf(
				"failed to drain review coordinator %s before %s: %w",
				reviewer.ID,
				request.Intent,
				drainErr,
			)
	}

	unlockLifecycle := b.agents.lockReviewCoordinatorTerminalization(
		reviewer.ID,
	)
	defer unlockLifecycle()
	reviewer, completed, err :=
		b.beginPersistedReviewCoordinatorLifecycle(
			reviewer.ID,
			request,
		)
	if err != nil {
		b.allowReviewDiscoveryPass(reviewer.ID)
		return reviewCoordinatorLifecycleResult{}, err
	}
	if completed {
		if reviewer.ReviewCoordinatorLifecycle != nil &&
			reviewer.ReviewCoordinatorLifecycle.Intent ==
				ReviewCoordinatorLifecyclePause &&
			reviewer.Paused &&
			!agentLifecycleTerminal(&reviewer) {
			b.allowReviewDiscoveryPass(reviewer.ID)
		}
		return reviewCoordinatorLifecycleResult{Agent: reviewer}, nil
	}
	b.logReviewCycleObservation(
		reviewer.ID,
		"cycle_"+string(request.Intent),
	)
	state := reviewer.ReviewCoordinatorLifecycle
	if state == nil {
		return reviewCoordinatorLifecycleResult{},
			errors.New("persisted review coordinator cleanup intent is missing")
	}
	generation := state.Generation
	request = reviewCoordinatorLifecycleRequest{
		Intent:                     state.Intent,
		FinalState:                 state.FinalState,
		ReleaseCoordinatorWorktree: state.ReleaseCoordinatorWorktree,
		ReleaseRuntimeArtifacts:    state.ReleaseRuntimeArtifacts,
		PreservePersistedHandoff:   state.PreservePersistedHandoff,
		CaptureHandoff:             state.CaptureHandoff,
	}

	if state.RuntimeStopAcknowledgedAt.IsZero() {
		if err := b.stopRuntime(reviewer); err != nil {
			return reviewCoordinatorLifecycleResult{Agent: reviewer, Changed: true},
				err
		}
		reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
			reviewer.ID,
			generation,
			false,
			func(
				_ *Agent,
				lifecycle *ReviewCoordinatorLifecycleState,
			) error {
				lifecycle.RuntimeStopAcknowledgedAt = time.Now().UTC()
				return nil
			},
		)
		if err != nil {
			return reviewCoordinatorLifecycleResult{Agent: reviewer, Changed: true},
				err
		}
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	err = b.cleanupOwnedReviewWorkersForLifecycle(
		cleanupCtx,
		reviewer.ID,
		generation,
	)
	cleanupCancel()
	if err != nil {
		if current, found := b.agents.Get(reviewer.ID); found {
			reviewer = current
		}
		return reviewCoordinatorLifecycleResult{Agent: reviewer, Changed: true},
			err
	}

	if request.Intent != ReviewCoordinatorLifecyclePause {
		reviewer, _ = b.agents.Get(reviewer.ID)
		state = reviewer.ReviewCoordinatorLifecycle
		if state.CodexHomeReleasedAt.IsZero() {
			err = b.removeAgentRuntimeState(reviewer)
			if err == nil {
				reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewer.ID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						lifecycle.CodexHomeReleasedAt = time.Now().UTC()
						return nil
					},
				)
			}
			if err != nil {
				return reviewCoordinatorLifecycleResult{
					Agent:   reviewer,
					Changed: true,
				}, err
			}
		}
	}

	if request.ReleaseCoordinatorWorktree {
		reviewer, _ = b.agents.Get(reviewer.ID)
		state = reviewer.ReviewCoordinatorLifecycle
		if state.CoordinatorWorktreeReleasedAt.IsZero() {
			if request.CaptureHandoff {
				b.captureAgentHandoffNonFatal(reviewer)
			}
			worktreePath := strings.TrimSpace(reviewer.WorktreePath)
			if worktreePath != "" {
				cleanupCtx, cleanupCancel =
					context.WithTimeout(
						context.Background(),
						30*time.Second,
					)
				err = b.cleanupWorktree(
					cleanupCtx,
					worktreePath,
					"",
				)
				cleanupCancel()
			}
			if err != nil {
				return reviewCoordinatorLifecycleResult{
					Agent:   reviewer,
					Changed: true,
				}, err
			}
			reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
				reviewer.ID,
				generation,
				false,
				func(
					_ *Agent,
					lifecycle *ReviewCoordinatorLifecycleState,
				) error {
					if lifecycle.WorkerStopsAcknowledgedAt.IsZero() {
						return errors.New(
							"worker stops are not durably acknowledged",
						)
					}
					lifecycle.CoordinatorWorktreeReleasedAt =
						time.Now().UTC()
					return nil
				},
			)
			if err != nil {
				return reviewCoordinatorLifecycleResult{
					Agent:   reviewer,
					Changed: true,
				}, err
			}
		}
	}

	if request.ReleaseRuntimeArtifacts {
		reviewer, _ = b.agents.Get(reviewer.ID)
		state = reviewer.ReviewCoordinatorLifecycle
		resourceOwner := reviewer
		resourceOwner.RuntimeHandle.LogPath = state.RuntimeLogPath
		if state.RuntimeLogReleasedAt.IsZero() {
			err = b.removeAgentRuntimeLog(resourceOwner)
			if err == nil {
				reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewer.ID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						lifecycle.RuntimeLogReleasedAt = time.Now().UTC()
						return nil
					},
				)
			}
			if err != nil {
				return reviewCoordinatorLifecycleResult{
					Agent:   reviewer,
					Changed: true,
				}, err
			}
		}
		state = reviewer.ReviewCoordinatorLifecycle
		if state.MandatoryTestLogReleasedAt.IsZero() {
			err = b.removeAgentMandatoryTestLog(resourceOwner)
			if err == nil {
				reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewer.ID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						lifecycle.MandatoryTestLogReleasedAt =
							time.Now().UTC()
						return nil
					},
				)
			}
			if err != nil {
				return reviewCoordinatorLifecycleResult{
					Agent:   reviewer,
					Changed: true,
				}, err
			}
		}
		state = reviewer.ReviewCoordinatorLifecycle
		if !request.PreservePersistedHandoff &&
			state.PersistedHandoffsReleasedAt.IsZero() {
			err = b.removePersistedHandoffs(reviewer.ID)
			if err == nil {
				reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
					reviewer.ID,
					generation,
					false,
					func(
						_ *Agent,
						lifecycle *ReviewCoordinatorLifecycleState,
					) error {
						lifecycle.PersistedHandoffsReleasedAt =
							time.Now().UTC()
						return nil
					},
				)
			}
			if err != nil {
				return reviewCoordinatorLifecycleResult{
					Agent:   reviewer,
					Changed: true,
				}, err
			}
		}
	}

	reviewer, err = b.persistReviewCoordinatorLifecycleUpdate(
		reviewer.ID,
		generation,
		true,
		func(
			agent *Agent,
			lifecycle *ReviewCoordinatorLifecycleState,
		) error {
			if lifecycle.RuntimeStopAcknowledgedAt.IsZero() ||
				lifecycle.WorkerStopsAcknowledgedAt.IsZero() {
				return errors.New(
					"review coordinator stop acknowledgements are incomplete",
				)
			}
			if lifecycle.ReleaseCoordinatorWorktree &&
				lifecycle.CoordinatorWorktreeReleasedAt.IsZero() {
				return errors.New(
					"review coordinator worktree release is incomplete",
				)
			}
			if lifecycle.Intent != ReviewCoordinatorLifecyclePause &&
				lifecycle.CodexHomeReleasedAt.IsZero() {
				return errors.New(
					"review coordinator Codex home release is incomplete",
				)
			}
			if lifecycle.ReleaseRuntimeArtifacts &&
				(lifecycle.RuntimeLogReleasedAt.IsZero() ||
					lifecycle.MandatoryTestLogReleasedAt.IsZero() ||
					(!lifecycle.PreservePersistedHandoff &&
						lifecycle.PersistedHandoffsReleasedAt.IsZero())) {
				return errors.New(
					"review coordinator runtime artifact release is incomplete",
				)
			}
			lifecycle.CompletedAt = time.Now().UTC()
			if lifecycle.Intent == ReviewCoordinatorLifecyclePause {
				agent.Paused = true
				agent.Stopped = false
			} else {
				agent.State = lifecycle.FinalState
				agent.Stopped = true
				agent.Paused = false
			}
			return nil
		},
	)
	if err != nil {
		return reviewCoordinatorLifecycleResult{Agent: reviewer, Changed: true},
			err
	}
	if reviewer.ReviewCoordinatorLifecycle != nil &&
		reviewer.ReviewCoordinatorLifecycle.Intent ==
			ReviewCoordinatorLifecyclePause &&
		reviewer.Paused &&
		!agentLifecycleTerminal(&reviewer) {
		b.allowReviewDiscoveryPass(reviewer.ID)
	}
	return reviewCoordinatorLifecycleResult{
		Agent:   reviewer,
		Changed: true,
	}, nil
}

func validateReviewCoordinatorLifecycleState(
	reviewer Agent,
) error {
	state := reviewer.ReviewCoordinatorLifecycle
	if state == nil {
		return nil
	}
	if reviewer.Role != RoleReviewer {
		return errors.New(
			"non-review agent has review coordinator lifecycle state",
		)
	}
	if state.Generation <= 0 ||
		reviewCoordinatorLifecycleIntentRank(state.Intent) == 0 ||
		state.RequestedAt.IsZero() {
		return errors.New(
			"review coordinator lifecycle identity or intent is invalid",
		)
	}
	if err := validateReviewCoordinatorLifecycleRequest(
		reviewCoordinatorLifecycleRequest{
			Intent:                     state.Intent,
			FinalState:                 state.FinalState,
			ReleaseCoordinatorWorktree: state.ReleaseCoordinatorWorktree,
			ReleaseRuntimeArtifacts:    state.ReleaseRuntimeArtifacts,
			PreservePersistedHandoff:   state.PreservePersistedHandoff,
		},
	); err != nil {
		return err
	}
	owners := make(map[string]struct{}, len(state.WorkerCleanups))
	ownerships := make(map[string]struct{})
	if reviewer.ReviewCycle != nil {
		for _, ownership := range reviewer.ReviewCycle.WorkerOwnerships {
			ownerships[ownership.OwnerID] = struct{}{}
		}
	}
	for _, cleanup := range state.WorkerCleanups {
		if strings.TrimSpace(cleanup.OwnerID) == "" {
			return errors.New(
				"review worker cleanup owner identity is missing",
			)
		}
		if _, duplicate := owners[cleanup.OwnerID]; duplicate {
			return fmt.Errorf(
				"review worker cleanup owner %q is duplicated",
				cleanup.OwnerID,
			)
		}
		owners[cleanup.OwnerID] = struct{}{}
		if _, exists := ownerships[cleanup.OwnerID]; !exists {
			return fmt.Errorf(
				"review worker cleanup owner %q has no ownership record",
				cleanup.OwnerID,
			)
		}
		if (!cleanup.WorktreeReleasedAt.IsZero() ||
			!cleanup.CodexHomeReleasedAt.IsZero() ||
			!cleanup.GitHubConfigReleasedAt.IsZero() ||
			!cleanup.ArtifactDirectoryReleasedAt.IsZero()) &&
			cleanup.StopAcknowledgedAt.IsZero() {
			return fmt.Errorf(
				"review worker %q resources were released before stop acknowledgement",
				cleanup.OwnerID,
			)
		}
	}
	if !state.WorkerStopsAcknowledgedAt.IsZero() {
		for _, cleanup := range state.WorkerCleanups {
			if cleanup.StopAcknowledgedAt.IsZero() {
				return fmt.Errorf(
					"review worker %q stop aggregate is premature",
					cleanup.OwnerID,
				)
			}
		}
	}
	if !state.CoordinatorWorktreeReleasedAt.IsZero() &&
		(state.RuntimeStopAcknowledgedAt.IsZero() ||
			state.WorkerStopsAcknowledgedAt.IsZero()) {
		return errors.New(
			"review coordinator worktree was released before stop acknowledgement",
		)
	}
	if !state.CompletedAt.IsZero() {
		if state.RuntimeStopAcknowledgedAt.IsZero() ||
			state.WorkerStopsAcknowledgedAt.IsZero() {
			return errors.New(
				"completed review coordinator lifecycle lacks stop acknowledgement",
			)
		}
		if state.ReleaseCoordinatorWorktree &&
			state.CoordinatorWorktreeReleasedAt.IsZero() {
			return errors.New(
				"completed review coordinator lifecycle retained its worktree",
			)
		}
		if state.ReleaseRuntimeArtifacts &&
			(state.RuntimeLogReleasedAt.IsZero() ||
				state.MandatoryTestLogReleasedAt.IsZero() ||
				(!state.PreservePersistedHandoff &&
					state.PersistedHandoffsReleasedAt.IsZero())) {
			return errors.New(
				"completed review coordinator lifecycle retained runtime artifacts",
			)
		}
		for _, cleanup := range state.WorkerCleanups {
			if cleanup.CodexHomeReleasedAt.IsZero() ||
				cleanup.WorktreeReleasedAt.IsZero() ||
				cleanup.GitHubConfigReleasedAt.IsZero() ||
				cleanup.ArtifactDirectoryReleasedAt.IsZero() {
				return fmt.Errorf(
					"completed review worker %q cleanup is incomplete",
					cleanup.OwnerID,
				)
			}
		}
		if state.Intent != ReviewCoordinatorLifecyclePause &&
			state.CodexHomeReleasedAt.IsZero() {
			return errors.New(
				"completed review coordinator lifecycle retained its Codex home",
			)
		}
	}
	return nil
}
