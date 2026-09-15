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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type reviewCycleRecoveryMatrixRunner struct {
	mu        sync.Mutex
	bot       *Orchestrator
	live      map[string]bool
	started   []ReviewWorkerOwnership
	stopped   []string
	stopError error
	isAlive   func(RuntimeHandle) (bool, error)
}

func (runner *reviewCycleRecoveryMatrixRunner) Start(
	agent Agent,
	_ string,
) (RuntimeHandle, error) {
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: tmuxSessionName(agent),
	}, nil
}

func (runner *reviewCycleRecoveryMatrixRunner) StartReviewWorkerContext(
	ctx context.Context,
	agent Agent,
	_ string,
	_ []string,
) (RuntimeHandle, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeHandle{}, err
	}
	reviewer, ok := runner.bot.agents.Get(agent.ParentAgentID)
	if !ok || reviewer.ReviewCycle == nil {
		return RuntimeHandle{}, errors.New(
			"review coordinator disappeared during recovery test",
		)
	}
	var ownership ReviewWorkerOwnership
	for _, candidate := range reviewer.ReviewCycle.WorkerOwnerships {
		if candidate.OwnerID == agent.ID {
			ownership = candidate
			break
		}
	}
	if ownership.OwnerID == "" {
		return RuntimeHandle{}, errors.New(
			"recovery ownership disappeared before test launch",
		)
	}
	if err := publishReviewCycleRecoveryMatrixArtifact(
		ctx,
		runner.bot,
		reviewer,
		ownership,
	); err != nil {
		return RuntimeHandle{}, err
	}
	runner.mu.Lock()
	runner.started = append(runner.started, ownership)
	if runner.live == nil {
		runner.live = make(map[string]bool)
	}
	runner.live[ownership.SessionName] = true
	runner.mu.Unlock()
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: ownership.SessionName,
	}, nil
}

func (runner *reviewCycleRecoveryMatrixRunner) ValidateReviewWorkerIsolation(
	[]string,
) error {
	return nil
}

func (runner *reviewCycleRecoveryMatrixRunner) Send(
	RuntimeHandle,
	string,
) error {
	return nil
}

func (runner *reviewCycleRecoveryMatrixRunner) Capture(
	RuntimeHandle,
	int,
) (string, error) {
	return "", nil
}

func (runner *reviewCycleRecoveryMatrixRunner) Stop(
	handle RuntimeHandle,
) error {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.stopError != nil {
		return runner.stopError
	}
	if runner.live == nil {
		runner.live = make(map[string]bool)
	}
	runner.live[handle.Session] = false
	runner.stopped = append(runner.stopped, handle.Session)
	return nil
}

func (runner *reviewCycleRecoveryMatrixRunner) IsAlive(
	handle RuntimeHandle,
) (bool, error) {
	runner.mu.Lock()
	isAlive := runner.isAlive
	if isAlive == nil {
		defer runner.mu.Unlock()
		return runner.live[handle.Session], nil
	}
	runner.mu.Unlock()
	return isAlive(handle)
}

func (runner *reviewCycleRecoveryMatrixRunner) startSnapshot() []ReviewWorkerOwnership {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return append([]ReviewWorkerOwnership(nil), runner.started...)
}

func (runner *reviewCycleRecoveryMatrixRunner) stopSnapshot() []string {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return append([]string(nil), runner.stopped...)
}

func publishReviewCycleRecoveryMatrixArtifact(
	ctx context.Context,
	bot *Orchestrator,
	reviewer Agent,
	ownership ReviewWorkerOwnership,
) error {
	directory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		return err
	}
	store, err := newReviewArtifactStore(
		directory,
		reviewer.ReviewCycle.Policy.Artifacts,
	)
	if err != nil {
		return err
	}
	var phase ReviewArtifactPhase
	var payload ReviewArtifactPayload
	switch ownership.Identity.Role {
	case AgentProfileRoleDiscovery:
		phase = ReviewArtifactPhaseDiscovery
		payload = ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadDiscovery,
			Discovery: &ReviewDiscoveryPayload{
				Summary:         "recovered discovery lane",
				Candidates:      []ReviewFindingCandidate{},
				Coverage:        []ReviewCoverageClaim{},
				UnreviewedAreas: []string{},
			},
		}
	case AgentProfileRoleVerifier:
		phase = ReviewArtifactPhaseVerification
		assignment, ok := scheduledReviewVerificationAssignment(
			reviewer.ReviewCycle,
			ownership.Identity.Pass,
			ownership.Identity.Lane,
		)
		if !ok {
			return errors.New(
				"recovered verifier assignment was not found",
			)
		}
		finding, ok := reviewCanonicalFindingByID(
			reviewer.ReviewCycle,
			assignment.FindingID,
		)
		if !ok {
			return errors.New(
				"recovered verifier finding was not found",
			)
		}
		location := finding.Location
		payload = ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadVerification,
			Verification: &ReviewVerificationPayload{
				FindingID:        finding.ID,
				Outcome:          ReviewVerificationRejected,
				Summary:          "independent recovery verification",
				ScopeDisposition: ReviewScopeInScope,
				PatchDisposition: ReviewPatchNotReproduced,
				Location:         &location,
				BehavioralPath:   finding.BehavioralPath,
				Evidence: []ReviewEvidence{{
					Summary:   "independent exact-SHA inspection",
					Path:      "tracked.txt",
					StartLine: 1,
					EndLine:   1,
				}},
				CausalEvidence: []ReviewEvidence{},
				TestEvidence: []ReviewEvidence{{
					Summary: "focused recovery reproduction",
					Path:    "tracked.txt",
				}},
			},
		}
	default:
		return fmt.Errorf(
			"unsupported recovery test role %q",
			ownership.Identity.Role,
		)
	}
	envelope, err := newReviewArtifactEnvelope(
		reviewer.ReviewCycle.HeadSHA,
		ownership,
		phase,
		payload,
	)
	if err != nil {
		return err
	}
	_, err = store.publish(
		ctx,
		reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	)
	return err
}

func prepareReviewCycleRecoveryWorkerCheckout(
	ctx context.Context,
	repoPath string,
	worker Agent,
) error {
	command := newCommandContext(
		ctx,
		"git",
		"clone",
		"--quiet",
		repoPath,
		worker.WorktreePath,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"failed to clone recovery worker checkout: %w: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}
	return nil
}

func restartReviewCycleRecoveryMatrixBot(
	t *testing.T,
	source *Orchestrator,
	runner *reviewCycleRecoveryMatrixRunner,
) *Orchestrator {
	t.Helper()
	restarted := &Orchestrator{
		cfg:                        source.cfg,
		agents:                     NewAgentManager(),
		runner:                     runner,
		reviewCycleRecoveryPending: make(map[string]struct{}),
	}
	runner.bot = restarted
	restarted.prepareReviewWorkerWorktreeFunc = func(
		ctx context.Context,
		worker Agent,
	) error {
		return prepareReviewCycleRecoveryWorkerCheckout(
			ctx,
			restarted.cfg.RepoPath,
			worker,
		)
	}
	restarted.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		return os.RemoveAll(worktreePath)
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	return restarted
}

func reconcileReviewCycleRecoveryMatrixBot(
	t *testing.T,
	bot *Orchestrator,
) {
	t.Helper()
	if err := bot.reconcilePersistedReviewCycles(
		context.Background(),
	); err != nil {
		t.Fatalf("reconcilePersistedReviewCycles() error = %v", err)
	}
	bot.waitForPersistedReviewCycleRecoveries()
}

func TestReviewCycleRecoveryRetryBudgetIsPolicyDriven(t *testing.T) {
	cycle := &ReviewCycleState{Policy: builtInReviewPolicy()}
	cycle.Policy.Verification.Retries = 4
	ownership := ReviewWorkerOwnership{
		DurableLaunchAttempt: DurableLaunchAttempt{Attempt: 4},
		Identity: ReviewWorkerIdentity{
			Role: AgentProfileRoleVerifier,
		},
	}
	if !reviewCycleRecoveryCanRetry(cycle, ownership) {
		t.Fatal("fourth verifier attempt should retain a fifth total attempt")
	}
	ownership.Attempt = 5
	if reviewCycleRecoveryCanRetry(cycle, ownership) {
		t.Fatal("fifth verifier attempt should exhaust four configured retries")
	}
}

func createReviewCycleRecoveryOrphan(
	t *testing.T,
	bot *Orchestrator,
	cycle *ReviewCycleState,
) (string, string) {
	t.Helper()
	identity, err := reviewWorkerIdentityForCycle(
		cycle,
		AgentProfileRoleDiscovery,
		cycle.Revision+90,
		"restart-orphan",
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle(orphan) error = %v", err)
	}
	ownership, err := allocateReviewWorkerOwnership(
		identity,
		1,
		bot.cfg.WorktreeDir,
		"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership(orphan) error = %v", err)
	}
	directory, err := prepareReviewWorkerArtifactDirectory(ownership)
	if err != nil {
		t.Fatalf("prepareReviewWorkerArtifactDirectory(orphan) error = %v", err)
	}
	marker, err := reviewWorkerArtifactOwnerMarkerPath(directory)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactOwnerMarkerPath(orphan) error = %v", err)
	}
	return directory, marker
}

func assertReviewCycleRecoveryOrphanRemoved(
	t *testing.T,
	directory string,
	marker string,
) {
	t.Helper()
	for _, path := range []string{directory, marker} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphaned review resource %q survived: %v", path, err)
		}
	}
}

func persistReviewCycleRecoveryMatrixSource(
	t *testing.T,
	source *Orchestrator,
) {
	t.Helper()
	if err := source.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}
}

func prepareActiveReviewCycleRecoveryDiscovery(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
) (ReviewDiscoveryLaneState, ReviewWorkerOwnership, *reviewArtifactStore) {
	t.Helper()
	state, err := harness.bot.beginReviewDiscoveryPass(
		harness.reviewer.ID,
		2,
	)
	if err != nil {
		t.Fatalf("beginReviewDiscoveryPass(recovery) error = %v", err)
	}
	lane := state.Lanes[0]
	if err := harness.bot.markReviewDiscoveryLaneRunning(
		harness.reviewer.ID,
		state.Pass,
		lane.Lane,
		nil,
	); err != nil {
		t.Fatalf("markReviewDiscoveryLaneRunning(recovery) error = %v", err)
	}
	for _, other := range state.Lanes[1:] {
		if err := harness.bot.failReviewDiscoveryLane(
			harness.reviewer.ID,
			state.Pass,
			other.Lane,
			ReviewDiscoveryFailureRuntime,
		); err != nil {
			t.Fatalf(
				"failReviewDiscoveryLane(%s) error = %v",
				other.Lane,
				err,
			)
		}
	}
	ownership, store := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleDiscovery,
		state.Pass,
		lane.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewDiscoveryLaneRunning(
				harness.reviewer.ID,
				state.Pass,
				lane.Lane,
				&ownership,
			)
		},
	)
	return lane, ownership, store
}

func TestPersistedReviewCycleRecoveryMatrix(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "checkpointed but not launched",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)
				orphanDir, orphanMarker :=
					createReviewCycleRecoveryOrphan(
						t,
						harness.bot,
						harness.reviewer.ReviewCycle,
					)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: make(map[string]bool),
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)
				assertReviewCycleRecoveryOrphanRemoved(
					t,
					orphanDir,
					orphanMarker,
				)

				started := runner.startSnapshot()
				if len(started) != 1 ||
					started[0].Identity.Role !=
						AgentProfileRoleVerifier ||
					started[0].Identity.Pass != assignment.Round ||
					started[0].Identity.Lane != assignment.Lane ||
					started[0].Attempt != 1 {
					t.Fatalf(
						"checkpointed recovery launches = %#v, want one verifier attempt",
						started,
					)
				}
				assertCompletedRecoveryCycleIsNotRelaunched(
					t,
					restarted,
					harness.reviewer.ID,
				)
			},
		},
		{
			name: "active with live owned runtime resources",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				ownership, store := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				finding := currentReviewFinding(
					t,
					harness,
					assignment.FindingID,
				)
				envelope := verificationEnvelope(
					t,
					harness,
					ownership,
					finding,
					ReviewVerificationRejected,
				)
				if _, err := store.publish(
					context.Background(),
					harness.reviewer.ReviewCycle.HeadSHA,
					ownership,
					envelope,
				); err != nil {
					t.Fatalf("publish(live recovery artifact) error = %v", err)
				}
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)
				orphanDir, orphanMarker :=
					createReviewCycleRecoveryOrphan(
						t,
						harness.bot,
						harness.reviewer.ReviewCycle,
					)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: map[string]bool{
						ownership.SessionName: true,
					},
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)
				assertReviewCycleRecoveryOrphanRemoved(
					t,
					orphanDir,
					orphanMarker,
				)
				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"live owned recovery relaunched work: %#v",
						started,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				stored, ok := reviewVerificationAssignmentByID(
					current.ReviewCycle,
					assignment.ID,
				)
				if !ok ||
					stored.Status != ReviewVerificationCompleted ||
					stored.WorkerID != ownership.OwnerID ||
					stored.Attempt != ownership.Attempt {
					t.Fatalf(
						"live owned verifier recovery = %#v",
						stored,
					)
				}
				recoveredOwnership, found :=
					reviewWorkerOwnershipByOwnerID(
						current.ReviewCycle,
						ownership.OwnerID,
					)
				if !found ||
					recoveredOwnership.Lifecycle !=
						ReviewWorkerCompleted {
					t.Fatalf(
						"live owned worker lifecycle = %#v",
						recoveredOwnership,
					)
				}
				assertCompletedRecoveryCycleIsNotRelaunched(
					t,
					restarted,
					harness.reviewer.ID,
				)
			},
		},
		{
			name: "active with live retry ownership",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				first, _ := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				retry, store := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				if retry.Attempt != first.Attempt+1 {
					t.Fatalf(
						"retry attempt = %d, want %d",
						retry.Attempt,
						first.Attempt+1,
					)
				}
				current, _ := harness.agents.Get(harness.reviewer.ID)
				timeout := reviewWorkerConfiguredRuntimeTimeout(
					current.ReviewCycle,
					AgentProfileRoleVerifier,
				)
				if timeout <= 0 {
					t.Fatal("recovery verifier timeout is invalid")
				}
				if _, err := harness.bot.mutateAndPersistReviewConvergence(
					harness.reviewer.ID,
					func(cycle *ReviewCycleState, _ time.Time) error {
						stored, ok := reviewVerificationAssignmentByID(
							cycle,
							assignment.ID,
						)
						if !ok {
							return errors.New(
								"retry assignment disappeared",
							)
						}
						stored.StartedAt = retry.AllocatedAt.Add(
							-timeout - time.Minute,
						)
						return nil
					},
				); err != nil {
					t.Fatalf(
						"persist expired logical start error = %v",
						err,
					)
				}
				finding := currentReviewFinding(
					t,
					harness,
					assignment.FindingID,
				)
				envelope := verificationEnvelope(
					t,
					harness,
					retry,
					finding,
					ReviewVerificationRejected,
				)
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				polls := 0
				runner := &reviewCycleRecoveryMatrixRunner{
					live: make(map[string]bool),
				}
				runner.isAlive = func(
					handle RuntimeHandle,
				) (bool, error) {
					if handle.Session != retry.SessionName {
						return false, nil
					}
					polls++
					if polls == 2 {
						_, err := store.publish(
							context.Background(),
							harness.reviewer.ReviewCycle.HeadSHA,
							retry,
							envelope,
						)
						if err != nil {
							return false, err
						}
					}
					return true, nil
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"live retry recovery relaunched work: %#v",
						started,
					)
				}
				if polls < 2 {
					t.Fatalf(
						"live retry runtime probes = %d, want at least 2",
						polls,
					)
				}
				current, _ = restarted.agents.Get(harness.reviewer.ID)
				stored, ok := reviewVerificationAssignmentByID(
					current.ReviewCycle,
					assignment.ID,
				)
				if !ok ||
					stored.Status != ReviewVerificationCompleted ||
					stored.WorkerID != retry.OwnerID ||
					stored.Attempt != retry.Attempt {
					t.Fatalf(
						"live retry recovery = %#v",
						stored,
					)
				}
			},
		},
		{
			name: "active with live owner reserved before binding",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				ownership, store := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ReviewWorkerOwnership) error {
						return nil
					},
				)
				finding := currentReviewFinding(
					t,
					harness,
					assignment.FindingID,
				)
				envelope := verificationEnvelope(
					t,
					harness,
					ownership,
					finding,
					ReviewVerificationRejected,
				)
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				polls := 0
				liveStatusChecked := false
				var liveStatusErr error
				runner := &reviewCycleRecoveryMatrixRunner{
					live: make(map[string]bool),
				}
				runner.isAlive = func(
					handle RuntimeHandle,
				) (bool, error) {
					if handle.Session != ownership.SessionName {
						return false, nil
					}
					polls++
					if polls == 2 {
						liveStatusChecked = true
						current, ok := runner.bot.agents.Get(
							harness.reviewer.ID,
						)
						if !ok || current.ReviewCycle == nil {
							liveStatusErr = errors.New(
								"review coordinator disappeared while observing recovered live owner",
							)
						} else {
							observation, err :=
								observableReviewCycle(current)
							if err != nil {
								liveStatusErr = err
							} else {
								workerVisible := false
								for _, worker := range observation.Workers {
									if worker.Role ==
										ownership.Identity.Role &&
										worker.Pass ==
											ownership.Identity.Pass &&
										worker.Lane ==
											ownership.Identity.Lane &&
										worker.Attempt ==
											ownership.Attempt &&
										worker.Lifecycle ==
											ReviewWorkerRunning &&
										!worker.StartedAt.IsZero() {
										workerVisible = true
										break
									}
								}
								if observation.Counts.Running != 1 ||
									observation.Counts.MaxParallelObserved != 1 ||
									observation.Counts.MaxVerifierParallelObserved != 1 ||
									!workerVisible {
									liveStatusErr = fmt.Errorf(
										"recovered live worker observation = %#v",
										observation,
									)
								}
							}
						}
						_, err := store.publish(
							context.Background(),
							harness.reviewer.ReviewCycle.HeadSHA,
							ownership,
							envelope,
						)
						if err != nil {
							return false, err
						}
					}
					return true, nil
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"unbound live owner recovery relaunched work: %#v",
						started,
					)
				}
				if !liveStatusChecked || liveStatusErr != nil {
					t.Fatalf(
						"recovered live owner status checked=%t error=%v",
						liveStatusChecked,
						liveStatusErr,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				stored, ok := reviewVerificationAssignmentByID(
					current.ReviewCycle,
					assignment.ID,
				)
				if !ok ||
					stored.Status != ReviewVerificationCompleted ||
					stored.WorkerID != ownership.OwnerID ||
					stored.Attempt != ownership.Attempt {
					t.Fatalf(
						"unbound live owner recovery = %#v",
						stored,
					)
				}
				recovered, found := reviewWorkerOwnershipByOwnerID(
					current.ReviewCycle,
					ownership.OwnerID,
				)
				if !found ||
					recovered.Lifecycle != ReviewWorkerCompleted ||
					recovered.StartedAt.IsZero() ||
					recovered.FinishedAt.IsZero() {
					t.Fatalf(
						"recovered live owner lifecycle = %#v",
						recovered,
					)
				}
				observation, err := observableReviewCycle(current)
				if err != nil {
					t.Fatalf(
						"observableReviewCycle(recovered) error = %v",
						err,
					)
				}
				if observation.Counts.MaxParallelObserved != 1 ||
					observation.Counts.MaxVerifierParallelObserved != 1 {
					t.Fatalf(
						"recovered parallel accounting = %#v",
						observation.Counts,
					)
				}
			},
		},
		{
			name: "active with newer published owner reserved before rebinding",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				first, _ := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				newest, store := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ReviewWorkerOwnership) error {
						return nil
					},
				)
				finding := currentReviewFinding(
					t,
					harness,
					assignment.FindingID,
				)
				envelope := verificationEnvelope(
					t,
					harness,
					newest,
					finding,
					ReviewVerificationRejected,
				)
				if _, err := store.publish(
					context.Background(),
					harness.reviewer.ReviewCycle.HeadSHA,
					newest,
					envelope,
				); err != nil {
					t.Fatalf(
						"publish(unbound newer recovery artifact) error = %v",
						err,
					)
				}
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: map[string]bool{
						first.SessionName:  false,
						newest.SessionName: false,
					},
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"unbound newer owner recovery relaunched work: %#v",
						started,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				stored, ok := reviewVerificationAssignmentByID(
					current.ReviewCycle,
					assignment.ID,
				)
				if !ok ||
					stored.Status != ReviewVerificationCompleted ||
					stored.WorkerID != newest.OwnerID ||
					stored.Attempt != newest.Attempt {
					t.Fatalf(
						"unbound newer owner recovery = %#v",
						stored,
					)
				}
			},
		},
		{
			name: "active with completed discovery checkpoint",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 0, true)
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: map[string]bool{
						harness.ownership.SessionName: true,
					},
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"completed discovery recovery relaunched work: %#v",
						started,
					)
				}
				if stopped := runner.stopSnapshot(); !stringSliceContains(
					stopped,
					harness.ownership.SessionName,
				) {
					t.Fatalf(
						"completed discovery owner was not stopped: %#v",
						stopped,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				pass, ok := latestReviewDiscoveryPassSnapshot(
					current.ReviewCycle,
				)
				if !ok || pass.CompletedAt.IsZero() ||
					pass.Lanes[0].Status != ReviewDiscoveryLaneCompleted ||
					pass.Lanes[0].WorkerID != harness.ownership.OwnerID {
					t.Fatalf(
						"completed discovery checkpoint changed: %#v",
						pass,
					)
				}
			},
		},
		{
			name: "terminal setup failure is not relaunched",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				failed, _ := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				if _, err :=
					harness.bot.transitionAndPersistReviewWorkerLifecycle(
						harness.reviewer.ID,
						failed.OwnerID,
						ReviewWorkerFailed,
						&DurableLaunchFailure{
							Kind:   DurableLaunchFailureSetup,
							Detail: "git worktree setup exhausted",
						},
					); err != nil {
					t.Fatalf(
						"transitionAndPersistReviewWorkerLifecycle() error = %v",
						err,
					)
				}
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: map[string]bool{failed.SessionName: false},
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"terminal setup failure relaunched work: %#v",
						started,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				stored, ok := reviewVerificationAssignmentByID(
					current.ReviewCycle,
					assignment.ID,
				)
				if !ok || stored.Status != ReviewVerificationFailed ||
					stored.FailureCode != ReviewVerificationFailureLaunch {
					t.Fatalf(
						"terminal setup recovery assignment = %#v",
						stored,
					)
				}
			},
		},
		{
			name: "active with missing or exited owned runtime resources",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				missing, _ := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				beforeRecovery, _ := harness.bot.agents.Get(
					harness.reviewer.ID,
				)
				ownershipsBeforeRecovery := len(
					beforeRecovery.ReviewCycle.WorkerOwnerships,
				)
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)
				orphanDir, orphanMarker :=
					createReviewCycleRecoveryOrphan(
						t,
						harness.bot,
						harness.reviewer.ReviewCycle,
					)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: map[string]bool{
						missing.SessionName: false,
					},
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)
				assertReviewCycleRecoveryOrphanRemoved(
					t,
					orphanDir,
					orphanMarker,
				)

				started := runner.startSnapshot()
				if len(started) != 1 ||
					started[0].Identity.Role !=
						AgentProfileRoleVerifier ||
					started[0].Identity.Pass != assignment.Round ||
					started[0].Identity.Lane != assignment.Lane ||
					started[0].Attempt != missing.Attempt+1 ||
					started[0].OwnerID == missing.OwnerID {
					t.Fatalf(
						"missing owned recovery launches = %#v, want one fresh logical replacement",
						started,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				if got := len(
					current.ReviewCycle.WorkerOwnerships,
				); got != ownershipsBeforeRecovery+1 {
					t.Fatalf(
						"worker ownership count = %d, want %d completed discovery, missing, and replacement records",
						got,
						ownershipsBeforeRecovery+1,
					)
				}
				missingOwnership, missingFound :=
					reviewWorkerOwnershipByOwnerID(
						current.ReviewCycle,
						missing.OwnerID,
					)
				replacementOwnership, replacementFound :=
					reviewWorkerOwnershipByOwnerID(
						current.ReviewCycle,
						started[0].OwnerID,
					)
				if !missingFound ||
					missingOwnership.Lifecycle !=
						ReviewWorkerFailed ||
					!replacementFound ||
					replacementOwnership.Lifecycle !=
						ReviewWorkerCompleted {
					t.Fatalf(
						"recovered worker lifecycles = missing:%#v replacement:%#v",
						missingOwnership,
						replacementOwnership,
					)
				}
				assertCompletedRecoveryCycleIsNotRelaunched(
					t,
					restarted,
					harness.reviewer.ID,
				)
			},
		},
		{
			name: "active with worker publishing while exiting",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				ownership, _ := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				var restarted *Orchestrator
				var publishOnce sync.Once
				var publishErr error
				runner := &reviewCycleRecoveryMatrixRunner{
					live: make(map[string]bool),
				}
				runner.isAlive = func(
					handle RuntimeHandle,
				) (bool, error) {
					if handle.Session != ownership.SessionName {
						return false, nil
					}
					publishOnce.Do(func() {
						current, ok := restarted.agents.Get(
							harness.reviewer.ID,
						)
						if !ok {
							publishErr = errors.New(
								"review coordinator disappeared",
							)
							return
						}
						publishErr =
							publishReviewCycleRecoveryMatrixArtifact(
								context.Background(),
								restarted,
								current,
								ownership,
							)
					})
					return false, publishErr
				}
				restarted = restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"publish-and-exit recovery relaunched work: %#v",
						started,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				stored, ok := reviewVerificationAssignmentByID(
					current.ReviewCycle,
					assignment.ID,
				)
				if !ok ||
					stored.Status != ReviewVerificationCompleted ||
					stored.WorkerID != ownership.OwnerID ||
					stored.Attempt != ownership.Attempt {
					t.Fatalf(
						"publish-and-exit recovery = %#v",
						stored,
					)
				}
			},
		},
		{
			name: "active with transiently unreadable published artifact",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 1, true)
				assignment := queueOneReviewVerification(t, harness)
				ownership, store := bindConvergenceWorker(
					t,
					harness,
					AgentProfileRoleVerifier,
					assignment.Round,
					assignment.Lane,
					func(ownership ReviewWorkerOwnership) error {
						return harness.bot.markReviewVerificationRunning(
							harness.reviewer.ID,
							assignment.ID,
							&ownership,
						)
					},
				)
				finding := currentReviewFinding(
					t,
					harness,
					assignment.FindingID,
				)
				envelope := verificationEnvelope(
					t,
					harness,
					ownership,
					finding,
					ReviewVerificationRejected,
				)
				artifactPath, err := store.publish(
					context.Background(),
					harness.reviewer.ReviewCycle.HeadSHA,
					ownership,
					envelope,
				)
				if err != nil {
					t.Fatalf(
						"publish(transient recovery artifact) error = %v",
						err,
					)
				}
				if err := os.Chmod(artifactPath, 0); err != nil {
					t.Fatalf(
						"Chmod(transient recovery artifact) error = %v",
						err,
					)
				}
				t.Cleanup(func() {
					_ = os.Chmod(artifactPath, 0o600)
				})
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: map[string]bool{
						ownership.SessionName: true,
					},
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				started := runner.startSnapshot()
				if len(started) != 1 ||
					started[0].Attempt != ownership.Attempt+1 ||
					started[0].OwnerID == ownership.OwnerID {
					t.Fatalf(
						"transient intake recovery launches = %#v, want one replacement",
						started,
					)
				}
				if stopped := runner.stopSnapshot(); !stringSliceContains(
					stopped,
					ownership.SessionName,
				) {
					t.Fatalf(
						"transient intake owner was not stopped: %#v",
						stopped,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				stored, ok := reviewVerificationAssignmentByID(
					current.ReviewCycle,
					assignment.ID,
				)
				if !ok ||
					stored.Status != ReviewVerificationCompleted ||
					stored.WorkerID != started[0].OwnerID ||
					stored.Attempt != started[0].Attempt {
					t.Fatalf(
						"transient intake recovery = %#v",
						stored,
					)
				}
			},
		},
		{
			name: "active with terminally invalid published artifact",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 0, true)
				lane, ownership, store :=
					prepareActiveReviewCycleRecoveryDiscovery(
						t,
						harness,
					)
				artifactPath := filepath.Join(
					store.directory,
					reviewArtifactFilename(ownership),
				)
				if err := os.WriteFile(
					artifactPath,
					[]byte("{"),
					0o600,
				); err != nil {
					t.Fatalf(
						"WriteFile(terminal recovery artifact) error = %v",
						err,
					)
				}
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: map[string]bool{
						ownership.SessionName: true,
					},
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)

				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"terminal intake recovery relaunched work: %#v",
						started,
					)
				}
				if stopped := runner.stopSnapshot(); !stringSliceContains(
					stopped,
					ownership.SessionName,
				) {
					t.Fatalf(
						"terminal intake owner was not stopped: %#v",
						stopped,
					)
				}
				current, _ := restarted.agents.Get(harness.reviewer.ID)
				stored, ok := scheduledReviewDiscoveryLane(
					current.ReviewCycle,
					2,
					lane.Lane,
				)
				if !ok ||
					stored.Status != ReviewDiscoveryLaneFailed ||
					stored.FailureCode != ReviewDiscoveryFailureArtifact {
					t.Fatalf(
						"terminal intake recovery = %#v",
						stored,
					)
				}
			},
		},
		{
			name: "paused",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 0, true)
				harness.bot.runner =
					&reviewCycleRecoveryMatrixRunner{
						bot:  harness.bot,
						live: make(map[string]bool),
					}
				harness.bot.cleanupWorktreeFunc = func(
					_ context.Context,
					_ string,
					worktreePath string,
					_ string,
				) error {
					return os.RemoveAll(worktreePath)
				}
				if err := harness.bot.PauseAgent(
					context.Background(),
					harness.reviewer.ID,
				); err != nil {
					t.Fatalf("PauseAgent() error = %v", err)
				}
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)
				orphanDir, orphanMarker :=
					createReviewCycleRecoveryOrphan(
						t,
						harness.bot,
						harness.reviewer.ReviewCycle,
					)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: make(map[string]bool),
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)
				assertReviewCycleRecoveryOrphanRemoved(
					t,
					orphanDir,
					orphanMarker,
				)
				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"paused cycle relaunched work: %#v",
						started,
					)
				}
				current, ok :=
					restarted.agents.Get(harness.reviewer.ID)
				if !ok || !current.Paused || current.Stopped ||
					current.ReviewCoordinatorLifecycle == nil ||
					current.ReviewCoordinatorLifecycle.
						CompletedAt.IsZero() {
					t.Fatalf(
						"paused recovery state = %#v",
						current,
					)
				}
			},
		},
		{
			name: "cleanup pending or terminal",
			run: func(t *testing.T) {
				harness := newReviewConvergenceTestHarness(t, 0, true)
				failingRunner := &reviewCycleRecoveryMatrixRunner{
					bot:       harness.bot,
					live:      make(map[string]bool),
					stopError: errors.New("injected stop failure"),
				}
				harness.bot.runner = failingRunner
				harness.bot.cleanupWorktreeFunc = func(
					_ context.Context,
					_ string,
					worktreePath string,
					_ string,
				) error {
					return os.RemoveAll(worktreePath)
				}
				_, err := harness.bot.transitionReviewCoordinatorLifecycle(
					context.Background(),
					harness.reviewer.ID,
					reviewCoordinatorLifecycleRequest{
						Intent:                     ReviewCoordinatorLifecycleCleanup,
						FinalState:                 StateDone,
						ReleaseCoordinatorWorktree: true,
						ReleaseRuntimeArtifacts:    true,
					},
				)
				if err == nil {
					t.Fatal(
						"cleanup setup unexpectedly completed",
					)
				}
				persistReviewCycleRecoveryMatrixSource(t, harness.bot)
				orphanDir, orphanMarker :=
					createReviewCycleRecoveryOrphan(
						t,
						harness.bot,
						harness.reviewer.ReviewCycle,
					)

				runner := &reviewCycleRecoveryMatrixRunner{
					live: make(map[string]bool),
				}
				restarted := restartReviewCycleRecoveryMatrixBot(
					t,
					harness.bot,
					runner,
				)
				reconcileReviewCycleRecoveryMatrixBot(t, restarted)
				assertReviewCycleRecoveryOrphanRemoved(
					t,
					orphanDir,
					orphanMarker,
				)
				if started := runner.startSnapshot(); len(started) != 0 {
					t.Fatalf(
						"cleanup-pending cycle relaunched work: %#v",
						started,
					)
				}
				current, ok :=
					restarted.agents.Get(harness.reviewer.ID)
				if !ok || current.State != StateDone ||
					!current.Stopped ||
					current.ReviewCoordinatorLifecycle == nil ||
					current.ReviewCoordinatorLifecycle.
						CompletedAt.IsZero() {
					t.Fatalf(
						"cleanup recovery state = %#v",
						current,
					)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}

func TestPersistedReviewCycleRecoveryDoesNotReplaceLiveWorkerWhenStopFails(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	lane, ownership, store := prepareActiveReviewCycleRecoveryDiscovery(
		t,
		harness,
	)
	if err := publishReviewCycleRecoveryMatrixArtifact(
		context.Background(),
		harness.bot,
		harness.reviewer,
		ownership,
	); err != nil {
		t.Fatalf("publish(transient recovery artifact) error = %v", err)
	}
	artifactPath := filepath.Join(
		store.directory,
		reviewArtifactFilename(ownership),
	)
	if err := os.Chmod(artifactPath, 0); err != nil {
		t.Fatalf("Chmod(transient recovery artifact) error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(artifactPath, 0o600) })
	persistReviewCycleRecoveryMatrixSource(t, harness.bot)

	stopFailure := errors.New("injected stop failure")
	runner := &reviewCycleRecoveryMatrixRunner{
		live: map[string]bool{
			ownership.SessionName: true,
		},
		stopError: stopFailure,
	}
	restarted := restartReviewCycleRecoveryMatrixBot(
		t,
		harness.bot,
		runner,
	)

	err := restarted.recoverPersistedReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if !errors.Is(err, stopFailure) {
		t.Fatalf("recoverPersistedReviewCycle() error = %v", err)
	}
	artifactFailure := asReviewArtifactError(err)
	if artifactFailure == nil ||
		artifactFailure.Class != ReviewArtifactFailureTransient {
		t.Fatalf("recovery did not preserve artifact failure: %v", err)
	}
	if started := runner.startSnapshot(); len(started) != 0 {
		t.Fatalf("stop failure launched replacement work: %#v", started)
	}
	current, _ := restarted.agents.Get(harness.reviewer.ID)
	storedLane, ok := scheduledReviewDiscoveryLane(
		current.ReviewCycle,
		2,
		lane.Lane,
	)
	if !ok || storedLane.Status != ReviewDiscoveryLaneRunning {
		t.Fatalf("lane after stop failure = %#v", storedLane)
	}
	storedOwnership, ok := reviewWorkerOwnershipByOwnerID(
		current.ReviewCycle,
		ownership.OwnerID,
	)
	if !ok || storedOwnership.Lifecycle != ReviewWorkerRunning {
		t.Fatalf("ownership after stop failure = %#v", storedOwnership)
	}

	runner.mu.Lock()
	runner.stopError = nil
	runner.mu.Unlock()
	work := reviewCycleRecoveryWork{
		Identity: ownership.Identity,
		OwnerID:  ownership.OwnerID,
		Attempt:  ownership.Attempt,
	}
	if err := restarted.recoverPersistedReviewWork(
		context.Background(),
		harness.reviewer.ID,
		work,
	); err != nil {
		t.Fatalf("recoverPersistedReviewWork(retry) error = %v", err)
	}
	current, _ = restarted.agents.Get(harness.reviewer.ID)
	storedLane, ok = scheduledReviewDiscoveryLane(
		current.ReviewCycle,
		2,
		lane.Lane,
	)
	if !ok || storedLane.Status != ReviewDiscoveryLaneCompleted {
		t.Fatalf("lane after successful stop retry = %#v", storedLane)
	}
	started := runner.startSnapshot()
	if len(started) != 1 ||
		started[0].OwnerID == ownership.OwnerID ||
		started[0].Attempt != ownership.Attempt+1 {
		t.Fatalf("replacement after successful stop retry = %#v", started)
	}
	storedOwnership, ok = reviewWorkerOwnershipByOwnerID(
		current.ReviewCycle,
		ownership.OwnerID,
	)
	if !ok || storedOwnership.Lifecycle != ReviewWorkerFailed {
		t.Fatalf(
			"ownership after successful stop retry = %#v",
			storedOwnership,
		)
	}
}

func TestPersistedReviewCycleRecoveryPreservesAcceptedArtifactWhenStopFails(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	ownership, store := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewVerificationRunning(
				harness.reviewer.ID,
				assignment.ID,
				&ownership,
			)
		},
	)
	finding := currentReviewFinding(t, harness, assignment.FindingID)
	envelope := verificationEnvelope(
		t,
		harness,
		ownership,
		finding,
		ReviewVerificationRejected,
	)
	if _, err := store.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	); err != nil {
		t.Fatalf("publish(accepted recovery artifact) error = %v", err)
	}
	persistReviewCycleRecoveryMatrixSource(t, harness.bot)

	stopFailure := errors.New("injected stop failure")
	runner := &reviewCycleRecoveryMatrixRunner{
		live: map[string]bool{
			ownership.SessionName: true,
		},
		stopError: stopFailure,
	}
	restarted := restartReviewCycleRecoveryMatrixBot(
		t,
		harness.bot,
		runner,
	)

	err := restarted.recoverPersistedReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if !errors.Is(err, stopFailure) {
		t.Fatalf("recoverPersistedReviewCycle() error = %v", err)
	}
	if started := runner.startSnapshot(); len(started) != 0 {
		t.Fatalf("stop failure launched replacement work: %#v", started)
	}
	current, _ := restarted.agents.Get(harness.reviewer.ID)
	storedAssignment, ok := reviewVerificationAssignmentByID(
		current.ReviewCycle,
		assignment.ID,
	)
	if !ok || storedAssignment.Status != ReviewVerificationCompleted {
		t.Fatalf("accepted assignment after stop failure = %#v", storedAssignment)
	}
	storedOwnership, ok := reviewWorkerOwnershipByOwnerID(
		current.ReviewCycle,
		ownership.OwnerID,
	)
	if !ok || storedOwnership.Lifecycle != ReviewWorkerRunning {
		t.Fatalf("ownership after stop failure = %#v", storedOwnership)
	}
	if !reviewCycleHasActiveRound(current.ReviewCycle) {
		t.Fatal("stop failure completed the active convergence round")
	}
}

func reviewWorkerOwnershipByOwnerID(
	cycle *ReviewCycleState,
	ownerID string,
) (ReviewWorkerOwnership, bool) {
	if cycle == nil {
		return ReviewWorkerOwnership{}, false
	}
	for _, ownership := range cycle.WorkerOwnerships {
		if ownership.OwnerID == ownerID {
			return ownership, true
		}
	}
	return ReviewWorkerOwnership{}, false
}

func assertCompletedRecoveryCycleIsNotRelaunched(
	t *testing.T,
	source *Orchestrator,
	reviewerID string,
) {
	t.Helper()
	if err := source.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState(completed recovery) error = %v", err)
	}
	runner := &reviewCycleRecoveryMatrixRunner{
		live: make(map[string]bool),
	}
	restarted := restartReviewCycleRecoveryMatrixBot(
		t,
		source,
		runner,
	)
	reconcileReviewCycleRecoveryMatrixBot(t, restarted)
	if started := runner.startSnapshot(); len(started) != 0 {
		t.Fatalf(
			"completed lane, verifier, or round was relaunched: %#v",
			started,
		)
	}
	current, ok := restarted.agents.Get(reviewerID)
	if !ok || current.ReviewCycle == nil ||
		current.ReviewCycle.Convergence == nil ||
		len(current.ReviewCycle.Convergence.Rounds) != 1 {
		t.Fatalf(
			"completed recovery cycle was not restored exactly: %#v",
			current,
		)
	}
	round := current.ReviewCycle.Convergence.Rounds[0]
	if round.CompletedAt.IsZero() ||
		round.Outcome == ReviewConvergenceRoundActive {
		t.Fatalf("completed recovery round = %#v", round)
	}
}

// TestMonitorPersistedReviewWorkerSurfacesExistingLimitInsteadOfGenericInvalidTimeout
// is the regression test for a real production incident on issue
// #982/PR #992: reviewWorkerRuntimeTimeoutAt folds a cycle's *remaining*
// wall-time budget into the per-worker timeout it returns, so once the
// overall budget (Convergence.MaxWallTimeMinutes) is exhausted -- easy
// to happen across a long post-restart recovery -- it returns zero
// regardless of a perfectly valid per-worker configuration.
// monitorPersistedReviewWorker treated that zero as a generic,
// unrecognized "persisted review worker timeout is invalid" error
// rather than the same durable budget-exhaustion signal
// (reviewLimitReachedError, wrapping errReviewLimitReached) the live
// (non-recovery) reservation path already produces -- observed live as
// an indefinite loop of "persisted review cycle recovery failed ...
// persisted review worker timeout is invalid" every ~20s, all night,
// never reaching a final review result.
func TestMonitorPersistedReviewWorkerSurfacesExistingLimitInsteadOfGenericInvalidTimeout(t *testing.T) {
	policy := builtInReviewPolicy()
	policy.Convergence.MaxWallTimeMinutes = 60
	cycle, err := newReviewCycleState(testReviewHeadSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	cycle.LimitTransition = &ReviewLimitTransition{
		SchemaVersion:     reviewLimitTransitionSchemaVersion,
		Kind:              ReviewLimitWallTime,
		TransitionedAt:    time.Now().UTC(),
		Actual:            uint64((61 * time.Minute).Milliseconds()),
		Maximum:           uint64((60 * time.Minute).Milliseconds()),
		Unit:              "milliseconds",
		Action:            policy.FailureActions.BudgetExhaustion,
		Outcome:           reviewLimitOutcomeForAction(policy.FailureActions.BudgetExhaustion),
		Reason:            "review wall time 3660000ms reached snapshotted maximum 3600000ms",
		MetricEventCount:  1,
		MetricEventDigest: cycle.PolicyFingerprint,
	}

	agents := NewAgentManager()
	reviewer := &Agent{
		ID:          "review-agent-982-1",
		Role:        RoleReviewer,
		IssueNumber: 982,
		PRNumber:    992,
		State:       StateWorking,
		ReviewCycle: cycle,
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	bot := &Orchestrator{agents: agents}

	work := reviewCycleRecoveryWork{
		Identity: ReviewWorkerIdentity{
			CycleID: cycle.ID,
			Role:    AgentProfileRoleDiscovery,
			Pass:    1,
			Lane:    "contract",
		},
		OwnerID: "review-worker-1",
		Attempt: 1,
	}
	ownership := ReviewWorkerOwnership{
		DurableLaunchAttempt: DurableLaunchAttempt{
			OwnerID: work.OwnerID,
		},
		Identity: work.Identity,
	}

	err = bot.monitorPersistedReviewWorker(
		context.Background(),
		reviewer.ID,
		work,
		ownership,
	)
	if err == nil {
		t.Fatal("monitorPersistedReviewWorker() error = nil, want the existing limit transition surfaced")
	}
	if !errors.Is(err, errReviewLimitReached) {
		t.Fatalf("monitorPersistedReviewWorker() error = %v, want errReviewLimitReached", err)
	}
	if strings.Contains(err.Error(), "persisted review worker timeout is invalid") {
		t.Fatalf("monitorPersistedReviewWorker() regressed to the generic invalid-timeout error: %v", err)
	}
}
