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
	"strings"
	"time"
	"unicode/utf8"
)

const durableLaunchDiagnosticBytes = 16 * 1024

// DurableLaunchKind identifies the external work authorized by an attempt.
// The attempt is persisted before its external side effect is allowed to run.
type DurableLaunchKind string

const (
	DurableLaunchReviewCoordinator DurableLaunchKind = "review_coordinator"
	DurableLaunchCorrectionRuntime DurableLaunchKind = "correction_runtime"
	DurableLaunchReviewWorker      DurableLaunchKind = "review_worker"
)

type DurableLaunchLifecycle string

const (
	DurableLaunchReserved  DurableLaunchLifecycle = "reserved"
	DurableLaunchRunning   DurableLaunchLifecycle = "running"
	DurableLaunchCompleted DurableLaunchLifecycle = "completed"
	DurableLaunchFailed    DurableLaunchLifecycle = "failed"
	DurableLaunchCancelled DurableLaunchLifecycle = "cancelled"
)

type DurableLaunchFailureKind string

const (
	DurableLaunchFailureSetup          DurableLaunchFailureKind = "setup"
	DurableLaunchFailureStart          DurableLaunchFailureKind = "start"
	DurableLaunchFailureRuntime        DurableLaunchFailureKind = "runtime"
	DurableLaunchFailureRuntimeMissing DurableLaunchFailureKind = "runtime_missing"
	DurableLaunchFailureArtifact       DurableLaunchFailureKind = "artifact"
	DurableLaunchFailureIncomplete     DurableLaunchFailureKind = "incomplete"
	DurableLaunchFailureState          DurableLaunchFailureKind = "state"
)

type DurableLaunchFailure struct {
	Kind      DurableLaunchFailureKind `json:"kind"`
	Detail    string                   `json:"detail,omitempty"`
	Retryable bool                     `json:"retryable,omitempty"`
}

// DurableLaunchAttempt is the sole durable authorization for starting an
// external runtime or coordinator. Attempt is charged at reservation time.
// Running is written before invoking the external launcher, eliminating the
// ambiguous "did the process start before the crash?" window.
type DurableLaunchAttempt struct {
	ID          string                 `json:"id"`
	Kind        DurableLaunchKind      `json:"kind"`
	Scope       string                 `json:"scope"`
	Attempt     int                    `json:"attempt"`
	OwnerID     string                 `json:"owner_id"`
	Lifecycle   DurableLaunchLifecycle `json:"lifecycle"`
	SessionName string                 `json:"session_name,omitempty"`
	AllocatedAt time.Time              `json:"allocated_at"`
	StartedAt   time.Time              `json:"started_at,omitempty"`
	FinishedAt  time.Time              `json:"finished_at,omitempty"`
	RetryAfter  time.Time              `json:"retry_after,omitempty"`
	Failure     *DurableLaunchFailure  `json:"failure,omitempty"`
}

func newDurableLaunchAttempt(
	id string,
	kind DurableLaunchKind,
	scope string,
	attempt int,
	ownerID string,
	sessionName string,
	allocatedAt time.Time,
) (DurableLaunchAttempt, error) {
	launch := DurableLaunchAttempt{
		ID:          strings.TrimSpace(id),
		Kind:        kind,
		Scope:       strings.TrimSpace(scope),
		Attempt:     attempt,
		OwnerID:     strings.TrimSpace(ownerID),
		Lifecycle:   DurableLaunchReserved,
		SessionName: strings.TrimSpace(sessionName),
		AllocatedAt: allocatedAt.UTC(),
	}
	if err := validateDurableLaunchAttempt(launch); err != nil {
		return DurableLaunchAttempt{}, err
	}
	return launch, nil
}

func cloneDurableLaunchAttempts(
	attempts []DurableLaunchAttempt,
) []DurableLaunchAttempt {
	cloned := append([]DurableLaunchAttempt(nil), attempts...)
	for index := range cloned {
		if cloned[index].Failure != nil {
			failure := *cloned[index].Failure
			cloned[index].Failure = &failure
		}
	}
	return cloned
}

func durableLaunchAttemptByID(
	attempts []DurableLaunchAttempt,
	id string,
) (DurableLaunchAttempt, bool) {
	id = strings.TrimSpace(id)
	for _, attempt := range attempts {
		if attempt.ID == id {
			return attempt, true
		}
	}
	return DurableLaunchAttempt{}, false
}

func durableLaunchAttemptCount(
	attempts []DurableLaunchAttempt,
	kind DurableLaunchKind,
) int {
	count := 0
	for _, attempt := range attempts {
		if attempt.Kind == kind {
			count++
		}
	}
	return count
}

func durableLaunchTerminal(lifecycle DurableLaunchLifecycle) bool {
	switch lifecycle {
	case DurableLaunchCompleted,
		DurableLaunchFailed,
		DurableLaunchCancelled:
		return true
	default:
		return false
	}
}

func validateDurableLaunchAttempt(attempt DurableLaunchAttempt) error {
	if strings.TrimSpace(attempt.ID) == "" {
		return errors.New("durable launch attempt identity is missing")
	}
	if sanitizeSessionPart(attempt.ID) != attempt.ID ||
		len(attempt.ID) > reviewWorkerOwnerNameLimit {
		return errors.New("durable launch attempt identity is unsafe")
	}
	switch attempt.Kind {
	case DurableLaunchReviewCoordinator,
		DurableLaunchCorrectionRuntime,
		DurableLaunchReviewWorker:
	default:
		return fmt.Errorf("durable launch kind %q is unsupported", attempt.Kind)
	}
	if strings.TrimSpace(attempt.Scope) == "" {
		return errors.New("durable launch attempt scope is missing")
	}
	if attempt.Attempt <= 0 {
		return errors.New("durable launch attempt number must be greater than zero")
	}
	if strings.TrimSpace(attempt.OwnerID) == "" {
		return errors.New("durable launch attempt owner is missing")
	}
	if sanitizeSessionPart(attempt.OwnerID) != attempt.OwnerID ||
		len(attempt.OwnerID) > reviewWorkerOwnerNameLimit {
		return errors.New("durable launch attempt owner is unsafe")
	}
	if attempt.AllocatedAt.IsZero() {
		return errors.New("durable launch reservation time is missing")
	}
	switch attempt.Lifecycle {
	case DurableLaunchReserved:
		if !attempt.StartedAt.IsZero() || !attempt.FinishedAt.IsZero() ||
			attempt.Failure != nil {
			return errors.New("reserved durable launch has terminal state")
		}
	case DurableLaunchRunning:
		if attempt.StartedAt.IsZero() || !attempt.FinishedAt.IsZero() ||
			attempt.Failure != nil {
			return errors.New("running durable launch lifecycle is invalid")
		}
	case DurableLaunchCompleted:
		if attempt.FinishedAt.IsZero() || attempt.Failure != nil {
			return errors.New("completed durable launch lifecycle is invalid")
		}
	case DurableLaunchFailed:
		if attempt.FinishedAt.IsZero() || attempt.Failure == nil {
			return errors.New("failed durable launch is missing its failure")
		}
		switch attempt.Failure.Kind {
		case DurableLaunchFailureSetup,
			DurableLaunchFailureStart,
			DurableLaunchFailureRuntime,
			DurableLaunchFailureRuntimeMissing,
			DurableLaunchFailureArtifact,
			DurableLaunchFailureIncomplete,
			DurableLaunchFailureState:
		default:
			return fmt.Errorf(
				"durable launch failure kind %q is unsupported",
				attempt.Failure.Kind,
			)
		}
		if len(attempt.Failure.Detail) > durableLaunchDiagnosticBytes ||
			!utf8.ValidString(attempt.Failure.Detail) {
			return errors.New("durable launch failure detail is invalid")
		}
	case DurableLaunchCancelled:
		if attempt.FinishedAt.IsZero() || attempt.Failure != nil {
			return errors.New("cancelled durable launch lifecycle is invalid")
		}
	default:
		return fmt.Errorf(
			"durable launch lifecycle %q is unsupported",
			attempt.Lifecycle,
		)
	}
	if !attempt.StartedAt.IsZero() &&
		attempt.StartedAt.Before(attempt.AllocatedAt) {
		return errors.New("durable launch start precedes its reservation")
	}
	if !attempt.FinishedAt.IsZero() &&
		attempt.FinishedAt.Before(attempt.AllocatedAt) {
		return errors.New("durable launch finish precedes its reservation")
	}
	if !attempt.StartedAt.IsZero() && !attempt.FinishedAt.IsZero() &&
		attempt.FinishedAt.Before(attempt.StartedAt) {
		return errors.New("durable launch finish precedes its start")
	}
	if !attempt.RetryAfter.IsZero() {
		if !durableLaunchTerminal(attempt.Lifecycle) {
			return errors.New("non-terminal durable launch has a retry time")
		}
		if attempt.RetryAfter.Before(attempt.FinishedAt) {
			return errors.New("durable launch retry precedes its finish")
		}
	}
	return nil
}

func transitionDurableLaunchAttempt(
	attempt *DurableLaunchAttempt,
	next DurableLaunchLifecycle,
	failure *DurableLaunchFailure,
	retryAfter time.Time,
	observedAt time.Time,
) (bool, error) {
	if attempt == nil {
		return false, errors.New("durable launch attempt is missing")
	}
	if observedAt.IsZero() {
		return false, errors.New("durable launch observation time is missing")
	}
	if attempt.Lifecycle == next || durableLaunchTerminal(attempt.Lifecycle) {
		return false, nil
	}
	valid := false
	switch next {
	case DurableLaunchRunning:
		valid = attempt.Lifecycle == DurableLaunchReserved && failure == nil
	case DurableLaunchCompleted:
		valid = attempt.Lifecycle == DurableLaunchReserved ||
			attempt.Lifecycle == DurableLaunchRunning
		valid = valid && failure == nil
	case DurableLaunchFailed:
		valid = (attempt.Lifecycle == DurableLaunchReserved ||
			attempt.Lifecycle == DurableLaunchRunning) && failure != nil
	case DurableLaunchCancelled:
		valid = (attempt.Lifecycle == DurableLaunchReserved ||
			attempt.Lifecycle == DurableLaunchRunning) && failure == nil
	}
	if !valid {
		return false, fmt.Errorf(
			"durable launch %q cannot transition from %q to %q",
			attempt.ID,
			attempt.Lifecycle,
			next,
		)
	}

	previous := *attempt
	if attempt.Failure != nil {
		previousFailure := *attempt.Failure
		previous.Failure = &previousFailure
	}
	observedAt = observedAt.UTC()
	attempt.Lifecycle = next
	attempt.RetryAfter = retryAfter.UTC()
	attempt.Failure = nil
	if failure != nil {
		copied := *failure
		copied.Detail = strings.TrimSpace(copied.Detail)
		attempt.Failure = &copied
	}
	if next == DurableLaunchRunning {
		attempt.StartedAt = observedAt
	} else {
		attempt.FinishedAt = observedAt
	}
	if err := validateDurableLaunchAttempt(*attempt); err != nil {
		*attempt = previous
		return false, err
	}
	return true, nil
}

type coderLaunchAttemptMutation struct {
	coderID                  string
	previousAttempts         []DurableLaunchAttempt
	previousPendingVerdict   *PendingReviewVerdictApplication
	previousRuntimeHandle    RuntimeHandle
	previousLastActivityTime time.Time
	updatedAt                time.Time
	changed                  bool
}

func reviewLaunchAttemptForHead(
	agent Agent,
	headSHA string,
) (DurableLaunchAttempt, bool) {
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	var latest DurableLaunchAttempt
	for _, attempt := range agent.LaunchAttempts {
		if attempt.Kind != DurableLaunchReviewCoordinator ||
			!strings.EqualFold(attempt.Scope, headSHA) ||
			(latest.ID != "" && attempt.Attempt <= latest.Attempt) {
			continue
		}
		latest = attempt
	}
	return latest, latest.ID != ""
}

func reviewLaunchAttemptCountForHead(agent Agent, headSHA string) int {
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	count := 0
	for _, attempt := range agent.LaunchAttempts {
		if attempt.Kind == DurableLaunchReviewCoordinator &&
			strings.EqualFold(attempt.Scope, headSHA) {
			count++
		}
	}
	return count
}

func correctionLaunchAttemptCount(agent Agent) int {
	return durableLaunchAttemptCount(
		agent.LaunchAttempts,
		DurableLaunchCorrectionRuntime,
	)
}

func activeCorrectionLaunchAttempt(
	agent Agent,
) (DurableLaunchAttempt, bool) {
	for _, attempt := range agent.LaunchAttempts {
		if attempt.Kind == DurableLaunchCorrectionRuntime &&
			!durableLaunchTerminal(attempt.Lifecycle) {
			return attempt, true
		}
	}
	return DurableLaunchAttempt{}, false
}

func (m *AgentManager) reserveReviewLaunchAttempt(
	coderID string,
	headSHA string,
	attemptID string,
	reviewerID string,
	automatic bool,
	reservedAt time.Time,
) (coderLaunchAttemptMutation, DurableLaunchAttempt, bool) {
	if m == nil || validateCanonicalGitObjectID(headSHA) != nil {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	coder := m.agents[strings.TrimSpace(coderID)]
	if coder == nil || coder.Role != RoleCoder || agentLifecycleTerminal(coder) {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	if latest, found := reviewLaunchAttemptForHead(*coder, headSHA); found {
		if !durableLaunchTerminal(latest.Lifecycle) {
			return coderLaunchAttemptMutation{}, latest, true
		}
		if automatic && (latest.RetryAfter.IsZero() ||
			latest.Attempt >= reviewSameHeadAttemptLimit ||
			reservedAt.Before(latest.RetryAfter)) {
			return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
		}
	}
	attemptNumber := reviewLaunchAttemptCountForHead(*coder, headSHA) + 1
	if automatic && attemptNumber > reviewSameHeadAttemptLimit {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	attempt, err := newDurableLaunchAttempt(
		attemptID,
		DurableLaunchReviewCoordinator,
		strings.ToLower(strings.TrimSpace(headSHA)),
		attemptNumber,
		reviewerID,
		"",
		reservedAt,
	)
	if err != nil {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	mutation := coderLaunchAttemptMutation{
		coderID:                  coder.ID,
		previousAttempts:         cloneDurableLaunchAttempts(coder.LaunchAttempts),
		previousPendingVerdict:   clonePendingReviewVerdictApplication(coder.PendingReviewVerdict),
		previousRuntimeHandle:    coder.RuntimeHandle,
		previousLastActivityTime: coder.LastActivityTime,
		updatedAt:                reservedAt.UTC(),
		changed:                  true,
	}
	coder.LaunchAttempts = append(coder.LaunchAttempts, attempt)
	coder.LastActivityTime = mutation.updatedAt
	return mutation, attempt, true
}

func (m *AgentManager) reserveCorrectionLaunchAttempt(
	coderID string,
	attemptID string,
	scope string,
	sessionName string,
	limit int,
	reservedAt time.Time,
) (coderLaunchAttemptMutation, DurableLaunchAttempt, bool) {
	if m == nil || limit <= 0 {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	coder := m.agents[strings.TrimSpace(coderID)]
	if coder == nil || coder.Role != RoleCoder || coder.Stopped ||
		coder.PendingReviewVerdict == nil ||
		coder.PendingReviewVerdict.Verdict != ReviewVerdictNeedsChanges {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	if pendingID := strings.TrimSpace(
		coder.PendingReviewVerdict.CorrectionAttemptID,
	); pendingID != "" {
		attempt, found := durableLaunchAttemptByID(
			coder.LaunchAttempts,
			pendingID,
		)
		if found && !durableLaunchTerminal(attempt.Lifecycle) {
			return coderLaunchAttemptMutation{}, attempt, true
		}
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	attemptNumber := correctionLaunchAttemptCount(*coder) + 1
	if attemptNumber > limit {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	attempt, err := newDurableLaunchAttempt(
		attemptID,
		DurableLaunchCorrectionRuntime,
		scope,
		attemptNumber,
		coder.ID,
		sessionName,
		reservedAt,
	)
	if err != nil {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, false
	}
	mutation := coderLaunchAttemptMutation{
		coderID:                  coder.ID,
		previousAttempts:         cloneDurableLaunchAttempts(coder.LaunchAttempts),
		previousPendingVerdict:   clonePendingReviewVerdictApplication(coder.PendingReviewVerdict),
		previousRuntimeHandle:    coder.RuntimeHandle,
		previousLastActivityTime: coder.LastActivityTime,
		updatedAt:                reservedAt.UTC(),
		changed:                  true,
	}
	// A new correction runtime replaces the previous one. Once its stop has
	// completed, close the prior running authorization before reserving the
	// next one so only one correction attempt can remain active.
	for index := range coder.LaunchAttempts {
		candidate := &coder.LaunchAttempts[index]
		if candidate.Kind != DurableLaunchCorrectionRuntime ||
			durableLaunchTerminal(candidate.Lifecycle) {
			continue
		}
		if _, err := transitionDurableLaunchAttempt(
			candidate,
			DurableLaunchCompleted,
			nil,
			time.Time{},
			reservedAt,
		); err != nil {
			coder.LaunchAttempts = cloneDurableLaunchAttempts(
				mutation.previousAttempts,
			)
			coder.PendingReviewVerdict =
				clonePendingReviewVerdictApplication(
					mutation.previousPendingVerdict,
				)
			coder.RuntimeHandle = mutation.previousRuntimeHandle
			coder.LastActivityTime = mutation.previousLastActivityTime
			return coderLaunchAttemptMutation{},
				DurableLaunchAttempt{},
				false
		}
	}
	coder.LaunchAttempts = append(coder.LaunchAttempts, attempt)
	coder.PendingReviewVerdict.CorrectionAttemptID = attempt.ID
	coder.RuntimeHandle = RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: attempt.SessionName,
	}
	coder.LastActivityTime = mutation.updatedAt
	return mutation, attempt, true
}

func (m *AgentManager) transitionCoderLaunchAttempt(
	coderID string,
	attemptID string,
	next DurableLaunchLifecycle,
	failure *DurableLaunchFailure,
	retryAfter time.Time,
	observedAt time.Time,
) (coderLaunchAttemptMutation, DurableLaunchAttempt, error) {
	if m == nil {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{},
			errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	coder := m.agents[strings.TrimSpace(coderID)]
	if coder == nil || coder.Role != RoleCoder {
		return coderLaunchAttemptMutation{}, DurableLaunchAttempt{},
			fmt.Errorf("coder %q was not found", strings.TrimSpace(coderID))
	}
	for index := range coder.LaunchAttempts {
		attempt := &coder.LaunchAttempts[index]
		if attempt.ID != strings.TrimSpace(attemptID) {
			continue
		}
		mutation := coderLaunchAttemptMutation{
			coderID:                  coder.ID,
			previousAttempts:         cloneDurableLaunchAttempts(coder.LaunchAttempts),
			previousPendingVerdict:   clonePendingReviewVerdictApplication(coder.PendingReviewVerdict),
			previousRuntimeHandle:    coder.RuntimeHandle,
			previousLastActivityTime: coder.LastActivityTime,
			updatedAt:                observedAt.UTC(),
		}
		changed, err := transitionDurableLaunchAttempt(
			attempt,
			next,
			failure,
			retryAfter,
			observedAt,
		)
		if err != nil {
			return coderLaunchAttemptMutation{}, DurableLaunchAttempt{}, err
		}
		if !changed {
			return coderLaunchAttemptMutation{}, *attempt, nil
		}
		mutation.changed = true
		if attempt.Kind == DurableLaunchCorrectionRuntime &&
			durableLaunchTerminal(attempt.Lifecycle) &&
			coder.PendingReviewVerdict != nil &&
			coder.PendingReviewVerdict.CorrectionAttemptID == attempt.ID {
			coder.PendingReviewVerdict.CorrectionAttemptID = ""
			coder.RuntimeHandle = RuntimeHandle{}
		}
		coder.LastActivityTime = mutation.updatedAt
		return mutation, *attempt, nil
	}
	return coderLaunchAttemptMutation{}, DurableLaunchAttempt{},
		fmt.Errorf("coder launch attempt %q was not found", attemptID)
}

func (m *AgentManager) rollbackCoderLaunchAttemptMutation(
	mutation coderLaunchAttemptMutation,
) bool {
	if m == nil || !mutation.changed {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	coder := m.agents[mutation.coderID]
	if coder == nil || !coder.LastActivityTime.Equal(mutation.updatedAt) {
		return false
	}
	coder.LaunchAttempts = cloneDurableLaunchAttempts(
		mutation.previousAttempts,
	)
	coder.PendingReviewVerdict = clonePendingReviewVerdictApplication(
		mutation.previousPendingVerdict,
	)
	coder.RuntimeHandle = mutation.previousRuntimeHandle
	coder.LastActivityTime = mutation.previousLastActivityTime
	return true
}

func (b *Orchestrator) persistCoderLaunchAttemptMutation(
	mutation coderLaunchAttemptMutation,
	description string,
) error {
	if !mutation.changed {
		return nil
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if !b.agents.rollbackCoderLaunchAttemptMutation(mutation) {
			return fmt.Errorf(
				"failed to persist %s and failed to roll it back: %w",
				description,
				err,
			)
		}
		return fmt.Errorf("failed to persist %s: %w", description, err)
	}
	return nil
}

func (b *Orchestrator) transitionAndPersistCoderLaunchAttempt(
	coderID string,
	attemptID string,
	next DurableLaunchLifecycle,
	failure *DurableLaunchFailure,
	retryAfter time.Time,
) (DurableLaunchAttempt, error) {
	if b == nil || b.agents == nil {
		return DurableLaunchAttempt{},
			errors.New("orchestrator agent manager is not configured")
	}
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	mutation, attempt, err := b.agents.transitionCoderLaunchAttempt(
		coderID,
		attemptID,
		next,
		failure,
		retryAfter,
		time.Now().UTC(),
	)
	if err != nil {
		return DurableLaunchAttempt{}, err
	}
	if err := b.persistCoderLaunchAttemptMutation(
		mutation,
		"coder launch attempt lifecycle",
	); err != nil {
		return DurableLaunchAttempt{}, err
	}
	return attempt, nil
}

// cancelLinkedReviewLaunchOnRetirement closes the authorization owned by a
// linked review coordinator after coordinator cleanup succeeds. Outcome paths
// terminalize the attempt before retirement; this only closes an attempt that
// would otherwise remain active after stale-head or parent cleanup.
func (b *Orchestrator) cancelLinkedReviewLaunchOnRetirement(
	reviewer Agent,
) error {
	if b == nil || b.agents == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return nil
	}
	coderID := strings.TrimSpace(reviewer.ParentAgentID)
	attemptID := strings.TrimSpace(reviewer.ReviewCycle.ID)
	if coderID == "" || attemptID == "" {
		return nil
	}
	coder, found := b.agents.Get(coderID)
	if !found {
		return nil
	}
	attempt, found := durableLaunchAttemptByID(coder.LaunchAttempts, attemptID)
	if !found || attempt.Kind != DurableLaunchReviewCoordinator ||
		durableLaunchTerminal(attempt.Lifecycle) {
		return nil
	}
	_, err := b.transitionAndPersistCoderLaunchAttempt(
		coderID,
		attemptID,
		DurableLaunchCancelled,
		nil,
		time.Time{},
	)
	if err != nil {
		return fmt.Errorf(
			"failed to cancel retired review launch %s for coder %s: %w",
			attemptID,
			coderID,
			err,
		)
	}
	return nil
}
