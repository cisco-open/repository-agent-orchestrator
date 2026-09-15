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

import "errors"

type RuntimeKind string

const (
	RuntimeKindTmux RuntimeKind = "tmux"
)

type RuntimeHandle struct {
	// Preserve the original JSON field names for persisted-state compatibility.
	Kind       RuntimeKind
	Session    string
	LogPath    string
	TmuxServer string
	CodexHome  string
	Scope      RuntimeScope
}

type Runner interface {
	Start(agent Agent, initialPrompt string) (RuntimeHandle, error)
	Send(handle RuntimeHandle, text string) error
	Capture(handle RuntimeHandle, lines int) (string, error)
	Stop(handle RuntimeHandle) error
	IsAlive(handle RuntimeHandle) (bool, error)
}

// ScopedRuntimeSender is implemented by production runners that bind every
// runtime message to the immutable repository and agent identity that owns the
// target pane. The base Runner.Send method remains for test doubles and legacy
// implementations; production callers must prefer this interface.
type ScopedRuntimeSender interface {
	BindRuntimeHandle(agent Agent, handle RuntimeHandle) (RuntimeHandle, error)
	SendScoped(agent Agent, handle RuntimeHandle, text string) error
}

// RuntimeLaunchReconciler removes a deterministic runtime session whose
// creation may have outlived the durable handle update that owns it.
type RuntimeLaunchReconciler interface {
	ReconcileRuntimeLaunch(agent Agent) error
}

// RuntimeStateCleaner removes per-agent runtime state after terminal cleanup.
type RuntimeStateCleaner interface {
	RemoveRuntimeState(agent Agent) error
}

// runtimeDeliveryError records whether a runtime payload was accepted before
// the delivery channel failed closed. Callers use this outcome to avoid
// replaying an already-delivered PR comment.
type runtimeDeliveryError struct {
	err         error
	delivered   bool
	quarantined bool
}

func (e *runtimeDeliveryError) Error() string {
	if e == nil || e.err == nil {
		return "runtime delivery failed"
	}
	return e.err.Error()
}

func (e *runtimeDeliveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func runtimePayloadDelivered(err error) bool {
	var deliveryErr *runtimeDeliveryError
	return errors.As(err, &deliveryErr) && deliveryErr.delivered
}
