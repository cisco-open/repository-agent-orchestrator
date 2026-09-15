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
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type callbackScopedRunner struct {
	bind func(Agent, RuntimeHandle) (RuntimeHandle, error)
	send func(Agent, RuntimeHandle, string) error
}

type blockingResurrectionRunner struct {
	mu              sync.Mutex
	startOnce       sync.Once
	startEntered    chan struct{}
	releaseStart    chan struct{}
	starts          int
	sends           int
	startComplete   bool
	sentBeforeStart bool
}

func (r *blockingResurrectionRunner) Start(
	Agent,
	string,
) (RuntimeHandle, error) {
	r.mu.Lock()
	r.starts++
	r.mu.Unlock()
	r.startOnce.Do(func() { close(r.startEntered) })
	<-r.releaseStart
	r.mu.Lock()
	r.startComplete = true
	r.mu.Unlock()
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: "resurrected-runtime",
	}, nil
}

func (r *blockingResurrectionRunner) Send(
	RuntimeHandle,
	string,
) error {
	return errors.New("unexpected unscoped Send call")
}

func (r *blockingResurrectionRunner) Capture(
	RuntimeHandle,
	int,
) (string, error) {
	return "", nil
}

func (r *blockingResurrectionRunner) Stop(RuntimeHandle) error {
	return nil
}

func (r *blockingResurrectionRunner) IsAlive(
	handle RuntimeHandle,
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.startComplete && handle.Session == "resurrected-runtime", nil
}

func (r *blockingResurrectionRunner) BindRuntimeHandle(
	_ Agent,
	handle RuntimeHandle,
) (RuntimeHandle, error) {
	return handle, nil
}

func (r *blockingResurrectionRunner) SendScoped(
	_ Agent,
	_ RuntimeHandle,
	_ string,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sends++
	if !r.startComplete {
		r.sentBeforeStart = true
	}
	return nil
}

func (r *callbackScopedRunner) Start(
	Agent,
	string,
) (RuntimeHandle, error) {
	return RuntimeHandle{}, errors.New("unexpected Start call")
}

func (r *callbackScopedRunner) Send(RuntimeHandle, string) error {
	return errors.New("unexpected unscoped Send call")
}

func (r *callbackScopedRunner) Capture(
	RuntimeHandle,
	int,
) (string, error) {
	return "", nil
}

func (r *callbackScopedRunner) Stop(RuntimeHandle) error {
	return nil
}

func (r *callbackScopedRunner) IsAlive(RuntimeHandle) (bool, error) {
	return true, nil
}

func (r *callbackScopedRunner) BindRuntimeHandle(
	agent Agent,
	handle RuntimeHandle,
) (RuntimeHandle, error) {
	return r.bind(agent, handle)
}

func (r *callbackScopedRunner) SendScoped(
	agent Agent,
	handle RuntimeHandle,
	text string,
) error {
	return r.send(agent, handle, text)
}

func TestRepositoryTmuxRunnerIsolatesServerCodexStateAndPrompt(t *testing.T) {
	baseCodexHome := t.TempDir()
	for _, name := range []string{"auth.json", "config.toml"} {
		if err := os.WriteFile(
			filepath.Join(baseCodexHome, name),
			[]byte(name+"\n"),
			0o600,
		); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}
	if err := os.WriteFile(
		filepath.Join(baseCodexHome, "history.jsonl"),
		[]byte("foreign prompt\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(history.jsonl) error = %v", err)
	}
	t.Setenv("CODEX_HOME", baseCodexHome)

	cfgA := runtimeIsolationTestConfig(t, "acme", "example-repository")
	cfgB := runtimeIsolationTestConfig(t, "acme", "other-repository")
	agentA := runtimeIsolationTestAgent(t, cfgA, "review-agent-156", 156, "aaaa")
	agentB := runtimeIsolationTestAgent(t, cfgB, "review-agent-156", 156, "bbbb")
	execA := &fakeCommandExecutor{outs: managedStartOutputs(0)}
	execB := &fakeCommandExecutor{outs: managedStartOutputs(0)}
	runnerA, err := NewRepositoryTmuxRunner("codex", cfgA, execA)
	if err != nil {
		t.Fatalf("NewRepositoryTmuxRunner(A) error = %v", err)
	}
	runnerB, err := NewRepositoryTmuxRunner("codex", cfgB, execB)
	if err != nil {
		t.Fatalf("NewRepositoryTmuxRunner(B) error = %v", err)
	}

	handleA, err := runnerA.Start(agentA, "review Example Repository PR #156")
	if err != nil {
		t.Fatalf("Start(A) error = %v", err)
	}
	handleB, err := runnerB.Start(agentB, "review Other Repository PR #156")
	if err != nil {
		t.Fatalf("Start(B) error = %v", err)
	}

	if handleA.TmuxServer == "" || handleB.TmuxServer == "" {
		t.Fatalf(
			"tmux servers = %q, %q, want repository-specific servers",
			handleA.TmuxServer,
			handleB.TmuxServer,
		)
	}
	if handleA.TmuxServer == handleB.TmuxServer {
		t.Fatalf("repository tmux servers collided: %q", handleA.TmuxServer)
	}
	if handleA.CodexHome == "" || handleA.CodexHome == handleB.CodexHome {
		t.Fatalf(
			"isolated Codex homes = %q, %q, want distinct non-empty paths",
			handleA.CodexHome,
			handleB.CodexHome,
		)
	}
	for _, handle := range []RuntimeHandle{handleA, handleB} {
		if _, err := os.Stat(filepath.Join(handle.CodexHome, "sessions")); err != nil {
			t.Fatalf("isolated sessions directory missing for %q: %v", handle.CodexHome, err)
		}
		if _, err := os.Lstat(filepath.Join(handle.CodexHome, "history.jsonl")); !os.IsNotExist(err) {
			t.Fatalf(
				"isolated history for %q is shared or pre-populated: err=%v",
				handle.CodexHome,
				err,
			)
		}
		for _, name := range []string{"auth.json", "config.toml"} {
			info, err := os.Lstat(filepath.Join(handle.CodexHome, name))
			if err != nil {
				t.Fatalf("shared Codex entry %s missing: %v", name, err)
			}
			if info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("shared Codex entry %s is not a symlink", name)
			}
		}
	}

	assertRepositoryTmuxCalls(t, execA.calls, handleA)
	assertRepositoryTmuxCalls(t, execB.calls, handleB)
	scope, body := scopedPromptFromCalls(t, execA)
	if err := validateRuntimeScopeExact(handleA.Scope, scope); err != nil {
		t.Fatalf("initial prompt scope mismatch: %v", err)
	}
	if !strings.Contains(body, "review Example Repository PR #156") {
		t.Fatalf("initial prompt body = %q, want example-repository task", body)
	}
	if strings.Contains(body, "Other Repository") {
		t.Fatalf("initial prompt body contains foreign repository task: %q", body)
	}
}

func TestRepositoryTmuxRunnerRejectsForeignRuntimeScope(t *testing.T) {
	t.Setenv("CODEX_HOME", t.TempDir())
	cfg := runtimeIsolationTestConfig(t, "acme", "example-repository")
	agent := runtimeIsolationTestAgent(t, cfg, "review-agent-156", 156, "aaaa")
	executor := &fakeCommandExecutor{outs: managedStartOutputs(0)}
	runner, err := NewRepositoryTmuxRunner("codex", cfg, executor)
	if err != nil {
		t.Fatalf("NewRepositoryTmuxRunner() error = %v", err)
	}
	handle, err := runner.Start(agent, "review")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := runner.Send(handle, "unscoped"); err == nil {
		t.Fatal("Send() accepted an unscoped production message")
	}

	tests := []struct {
		name   string
		mutate func(*RuntimeHandle)
	}{
		{
			name: "tmux server",
			mutate: func(candidate *RuntimeHandle) {
				candidate.TmuxServer = "rao-acme-other-repository-foreign"
			},
		},
		{
			name: "instance",
			mutate: func(candidate *RuntimeHandle) {
				candidate.Scope.InstanceID = "foreign-instance"
			},
		},
		{
			name: "agent",
			mutate: func(candidate *RuntimeHandle) {
				candidate.Scope.AgentID = "review-agent-133"
			},
		},
		{
			name: "repository",
			mutate: func(candidate *RuntimeHandle) {
				candidate.Scope.RepoName = "other-repository"
			},
		},
		{
			name: "repository path",
			mutate: func(candidate *RuntimeHandle) {
				candidate.Scope.RepoPath = t.TempDir()
			},
		},
		{
			name: "worktree",
			mutate: func(candidate *RuntimeHandle) {
				candidate.Scope.WorktreePath = t.TempDir()
			},
		},
		{
			name: "PR",
			mutate: func(candidate *RuntimeHandle) {
				candidate.Scope.PRNumber = 133
			},
		},
		{
			name: "SHA",
			mutate: func(candidate *RuntimeHandle) {
				candidate.Scope.HeadSHA = "bbbb"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := handle
			test.mutate(&candidate)
			executor.calls = nil
			if err := runner.SendScoped(agent, candidate, "foreign message"); err == nil {
				t.Fatal("SendScoped() accepted foreign runtime scope")
			}
			if len(executor.calls) != 0 {
				t.Fatalf("rejected message made tmux calls: %#v", executor.calls)
			}
		})
	}

	executor.calls = nil
	executor.outs = managedSendOutputs()
	if err := runner.SendScoped(agent, handle, "authorized message"); err != nil {
		t.Fatalf("SendScoped() authorized message error = %v", err)
	}
	if len(executor.calls) != 8 {
		t.Fatalf("authorized message tmux calls = %d, want 8", len(executor.calls))
	}
	for _, call := range executor.calls {
		if len(call.args) < 2 ||
			call.args[0] != "-L" ||
			call.args[1] != handle.TmuxServer {
			t.Fatalf("authorized message escaped repository tmux server: %#v", call)
		}
	}
	wantEnable := []string{
		"-L", handle.TmuxServer,
		"select-pane", "-e", "-t", handle.Session,
	}
	wantDisable := []string{
		"-L", handle.TmuxServer,
		"select-pane", "-d", "-t", handle.Session,
	}
	if !reflect.DeepEqual(executor.calls[0].args, wantEnable) ||
		!reflect.DeepEqual(executor.calls[len(executor.calls)-1].args, wantDisable) {
		t.Fatalf(
			"managed input calls = first:%#v last:%#v, want %#v/%#v",
			executor.calls[0].args,
			executor.calls[len(executor.calls)-1].args,
			wantEnable,
			wantDisable,
		)
	}
	scope, body := scopedPromptFromCalls(t, executor)
	if !reflect.DeepEqual(scope, handle.Scope) {
		t.Fatalf("authorized scope = %#v, want %#v", scope, handle.Scope)
	}
	if !strings.Contains(body, "authorized message") {
		t.Fatalf("authorized body = %q", body)
	}

	executor.calls = nil
	executor.outs = managedSendOutputs()
	executor.errs = map[int]error{2: errors.New("injected send failure")}
	if err := runner.SendScoped(agent, handle, "failed message"); err == nil {
		t.Fatal("SendScoped() error = nil for injected send failure")
	}
	if len(executor.calls) != 4 ||
		!reflect.DeepEqual(
			executor.calls[len(executor.calls)-1].args,
			wantDisable,
		) {
		t.Fatalf(
			"failed delivery did not restore disabled input: %#v",
			executor.calls,
		)
	}
	executor.errs = nil

	executor.calls = nil
	executor.outs = managedSendOutputs()
	executor.errs = map[int]error{7: errors.New("injected disable failure")}
	err = runner.SendScoped(agent, handle, "delivered before quarantine")
	if err == nil {
		t.Fatal("SendScoped() error = nil for injected disable failure")
	}
	var deliveryErr *runtimeDeliveryError
	if !errors.As(err, &deliveryErr) {
		t.Fatalf("SendScoped() error = %T, want *runtimeDeliveryError", err)
	}
	if !deliveryErr.delivered {
		t.Fatal("SendScoped() did not preserve successful payload outcome")
	}
	if !deliveryErr.quarantined {
		t.Fatal("SendScoped() did not record successful runtime quarantine")
	}
	wantStop := []string{
		"-L", handle.TmuxServer,
		"kill-session", "-t", handle.Session,
	}
	if len(executor.calls) != 9 ||
		!reflect.DeepEqual(executor.calls[len(executor.calls)-1].args, wantStop) {
		t.Fatalf(
			"post-delivery isolation failure did not quarantine runtime: %#v",
			executor.calls,
		)
	}
	executor.errs = nil

	updated := agent
	updated.PRNumber = 157
	updated.ObservedPRHeadSHA = "cccc"
	executor.calls = nil
	if err := runner.SendScoped(updated, handle, "stale target"); err == nil {
		t.Fatal("SendScoped() accepted stale PR and SHA without authoritative rebind")
	}
	if len(executor.calls) != 0 {
		t.Fatalf("stale target made tmux calls: %#v", executor.calls)
	}
	bound, err := runner.BindRuntimeHandle(updated, handle)
	if err != nil {
		t.Fatalf("BindRuntimeHandle() verified transition error = %v", err)
	}
	if bound.Scope.PRNumber != updated.PRNumber ||
		bound.Scope.HeadSHA != updated.ObservedPRHeadSHA {
		t.Fatalf(
			"rebound scope = %#v, want PR=%d SHA=%s",
			bound.Scope,
			updated.PRNumber,
			updated.ObservedPRHeadSHA,
		)
	}
	if err := runner.SendScoped(updated, bound, "verified new target"); err != nil {
		t.Fatalf("SendScoped() rebound target error = %v", err)
	}
}

func TestScopedRuntimeDeliveryRejectsScopeInvalidatedDuringBinding(
	t *testing.T,
) {
	manager := NewAgentManager()
	agent := Agent{
		ID:                "coding-agent-bind-race",
		WorktreePath:      t.TempDir(),
		PRNumber:          156,
		ObservedPRHeadSHA: "aaaaaaaa",
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "binding-race",
		},
		State: StateWorking,
	}
	if err := manager.Add(&agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	bindEntered := make(chan struct{})
	releaseBind := make(chan struct{})
	sendCalls := 0
	runner := &callbackScopedRunner{
		bind: func(
			_ Agent,
			handle RuntimeHandle,
		) (RuntimeHandle, error) {
			close(bindEntered)
			<-releaseBind
			return handle, nil
		},
		send: func(Agent, RuntimeHandle, string) error {
			sendCalls++
			return nil
		},
	}
	bot := &Orchestrator{agents: manager, runner: runner}
	result := make(chan error, 1)
	go func() {
		_, err := bot.sendScopedRuntimeMessage(agent, "follow-up")
		result <- err
	}()
	<-bindEntered
	if !manager.SetPRHeadSHA(agent.ID, "bbbbbbbb") {
		t.Fatal("SetPRHeadSHA() = false")
	}
	close(releaseBind)
	err := <-result
	if err == nil || !strings.Contains(err.Error(), "scope changed while binding") {
		t.Fatalf("sendScopedRuntimeMessage() error = %v", err)
	}
	if sendCalls != 0 {
		t.Fatalf("scope-invalidated delivery calls = %d, want 0", sendCalls)
	}
}

func TestScopedRuntimeDeliverySerializesConcurrentSends(t *testing.T) {
	manager := NewAgentManager()
	agent := Agent{
		ID:                "coding-agent-send-serialization",
		WorktreePath:      t.TempDir(),
		PRNumber:          156,
		ObservedPRHeadSHA: "aaaaaaaa",
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "send-serialization",
		},
		State: StateWorking,
	}
	if err := manager.Add(&agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var (
		mu        sync.Mutex
		active    int
		maxActive int
		sendCalls int
	)
	runner := &callbackScopedRunner{
		bind: func(
			_ Agent,
			handle RuntimeHandle,
		) (RuntimeHandle, error) {
			return handle, nil
		},
		send: func(Agent, RuntimeHandle, string) error {
			mu.Lock()
			active++
			sendCalls++
			if active > maxActive {
				maxActive = active
			}
			call := sendCalls
			mu.Unlock()
			if call == 1 {
				close(firstEntered)
				<-releaseFirst
			}
			mu.Lock()
			active--
			mu.Unlock()
			return nil
		},
	}
	bot := &Orchestrator{agents: manager, runner: runner}
	var wait sync.WaitGroup
	errs := make(chan error, 2)
	for _, text := range []string{"first", "second"} {
		wait.Add(1)
		go func(text string) {
			defer wait.Done()
			_, err := bot.sendScopedRuntimeMessage(agent, text)
			errs <- err
		}(text)
	}
	<-firstEntered
	close(releaseFirst)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("sendScopedRuntimeMessage() error = %v", err)
		}
	}
	if sendCalls != 2 || maxActive != 1 {
		t.Fatalf(
			"serialized sends = calls:%d max-active:%d, want 2/1",
			sendCalls,
			maxActive,
		)
	}
}

func TestRuntimeResurrectionSerializesFollowupDelivery(t *testing.T) {
	manager := NewAgentManager()
	agent := Agent{
		ID:           "coding-agent-resurrection-race",
		Role:         RoleCoder,
		WorktreePath: t.TempDir(),
		State:        StateWorking,
	}
	if err := manager.Add(&agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	runtimeAgent, ok := manager.Get(agent.ID)
	if !ok {
		t.Fatalf("agent %s not found", agent.ID)
	}
	runner := &blockingResurrectionRunner{
		startEntered: make(chan struct{}),
		releaseStart: make(chan struct{}),
	}
	bot := &Orchestrator{agents: manager, runner: runner}

	results := make(chan error, 2)
	go func() {
		_, err := bot.sendRuntimeMessage(
			context.Background(),
			runtimeAgent,
			"first follow-up",
			"first",
		)
		results <- err
	}()
	<-runner.startEntered
	go func() {
		_, err := bot.sendRuntimeMessage(
			context.Background(),
			runtimeAgent,
			"second follow-up",
			"second",
		)
		results <- err
	}()
	close(runner.releaseStart)

	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("sendRuntimeMessage() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for serialized runtime sends")
		}
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.starts != 1 {
		t.Fatalf("runtime starts = %d, want 1", runner.starts)
	}
	if runner.sends != 2 {
		t.Fatalf("runtime sends = %d, want 2", runner.sends)
	}
	if runner.sentBeforeStart {
		t.Fatal("follow-up reached runtime before resurrection completed")
	}
}

func TestLaunchRuntimeCheckpointsOwnershipBeforeStart(t *testing.T) {
	cfg := runtimeIsolationTestConfig(t, "acme", "launch-checkpoint")
	manager := NewAgentManager()
	agent := Agent{
		ID:           "coding-agent-launch-checkpoint",
		Role:         RoleCoder,
		WorktreePath: t.TempDir(),
		RuntimeCWD:   t.TempDir(),
		LogDir:       cfg.LogDir,
		State:        StateInitializing,
	}
	if err := manager.Add(&agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	runner := &stubRunner{
		startHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "checkpointed-runtime",
		},
	}
	bot := &Orchestrator{cfg: cfg, agents: manager, runner: runner}
	runner.startHook = func() {
		body, err := os.ReadFile(bot.agentStateFilePath())
		if err != nil {
			t.Fatalf("ReadFile(state checkpoint) error = %v", err)
		}
		var payload persistedStateFile
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("json.Unmarshal(state checkpoint) error = %v", err)
		}
		if len(payload.Agents) != 1 {
			t.Fatalf("checkpoint agents = %d, want 1", len(payload.Agents))
		}
		persisted := payload.Agents[0]
		if persisted.ID != agent.ID ||
			persisted.State != StateInitializing ||
			persisted.RuntimeHandle.Session != "" {
			t.Fatalf(
				"checkpointed ownership = %#v, want initializing agent %s without handle",
				persisted,
				agent.ID,
			)
		}
	}

	if _, err := bot.launchRuntime(agent, "initial prompt", true); err != nil {
		t.Fatalf("launchRuntime() error = %v", err)
	}
}

func TestRepositoryTmuxRunnerRemovesOnlyOwnedCodexState(t *testing.T) {
	t.Setenv("CODEX_HOME", t.TempDir())
	cfg := runtimeIsolationTestConfig(t, "acme", "cleanup")
	agent := runtimeIsolationTestAgent(
		t,
		cfg,
		"coding-agent-cleanup",
		156,
		"aaaaaaaa",
	)
	runner, err := NewRepositoryTmuxRunner("codex", cfg, &fakeCommandExecutor{})
	if err != nil {
		t.Fatalf("NewRepositoryTmuxRunner() error = %v", err)
	}
	codexHome, err := runner.isolation.codexHomeForAgent(agent)
	if err != nil {
		t.Fatalf("codexHomeForAgent() error = %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(codexHome, "history.jsonl"),
		[]byte("owned history\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(history) error = %v", err)
	}
	if err := runner.RemoveRuntimeState(agent); err != nil {
		t.Fatalf("RemoveRuntimeState() error = %v", err)
	}
	if _, err := os.Stat(codexHome); !os.IsNotExist(err) {
		t.Fatalf("owned Codex home still exists: err=%v", err)
	}

	foreign := t.TempDir()
	marker := filepath.Join(foreign, "preserve")
	if err := os.WriteFile(marker, []byte("preserve\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(foreign marker) error = %v", err)
	}
	root := filepath.Join(cfg.LogDir, runtimeCodexStateDirName)
	if err := os.Remove(root); err != nil {
		t.Fatalf("Remove(empty owned root) error = %v", err)
	}
	if err := os.Symlink(foreign, root); err != nil {
		t.Fatalf("Symlink(foreign root) error = %v", err)
	}
	if err := runner.RemoveRuntimeState(agent); err == nil {
		t.Fatal("RemoveRuntimeState() accepted a symlinked ownership root")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("foreign marker was removed: %v", err)
	}
}

func TestReconcileRuntimeHealthMigratesLegacyHandles(t *testing.T) {
	t.Setenv("CODEX_HOME", t.TempDir())
	cfg := runtimeIsolationTestConfig(t, "acme", "example-repository")

	t.Run("working runtime is relaunched in place", func(t *testing.T) {
		agent := runtimeIsolationTestAgent(
			t,
			cfg,
			"coding-agent-156",
			156,
			"aaaa",
		)
		agent.Role = RoleCoder
		agent.State = StateWorking
		agent.RuntimeHandle = RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: tmuxSessionName(agent),
			LogPath: filepath.Join(cfg.LogDir, agent.ID+".log"),
		}
		manager := NewAgentManager()
		if err := manager.Add(&agent); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		executor := &fakeCommandExecutor{
			outs: managedStartOutputs(1),
		}
		runner, err := NewRepositoryTmuxRunner("codex", cfg, executor)
		if err != nil {
			t.Fatalf("NewRepositoryTmuxRunner() error = %v", err)
		}
		bot := &Orchestrator{cfg: cfg, agents: manager, runner: runner}

		updated, shouldContinue, err := bot.reconcileRuntimeHealth(
			context.Background(),
			agent,
		)
		if err != nil {
			t.Fatalf("reconcileRuntimeHealth() error = %v", err)
		}
		if !shouldContinue {
			t.Fatal("reconcileRuntimeHealth() shouldContinue = false")
		}
		if runtimeHandleNeedsIsolationMigration(updated.RuntimeHandle) {
			t.Fatalf(
				"migrated runtime handle remains legacy: %#v",
				updated.RuntimeHandle,
			)
		}
		if updated.RuntimeHandle.TmuxServer != runner.isolation.tmuxServer {
			t.Fatalf(
				"migrated tmux server = %q, want %q",
				updated.RuntimeHandle.TmuxServer,
				runner.isolation.tmuxServer,
			)
		}
		if len(executor.calls) != 14 {
			t.Fatalf(
				"migration tmux calls = %d, want 14: %#v",
				len(executor.calls),
				executor.calls,
			)
		}
		wantReconcile := []string{
			"-L",
			updated.RuntimeHandle.TmuxServer,
			"kill-session",
			"-t",
			tmuxSessionName(agent),
		}
		if !reflect.DeepEqual(executor.calls[0].args, wantReconcile) {
			t.Fatalf(
				"migration reconciliation = %#v, want %#v",
				executor.calls[0].args,
				wantReconcile,
			)
		}
		assertRepositoryTmuxCalls(
			t,
			executor.calls[1:],
			updated.RuntimeHandle,
		)
	})

	t.Run("waiting runtime is cleared until needed", func(t *testing.T) {
		agent := runtimeIsolationTestAgent(
			t,
			cfg,
			"coding-agent-157",
			157,
			"bbbb",
		)
		agent.Role = RoleCoder
		agent.State = StateWaiting
		agent.RuntimeHandle = RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: tmuxSessionName(agent),
		}
		manager := NewAgentManager()
		if err := manager.Add(&agent); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		executor := &fakeCommandExecutor{}
		runner, err := NewRepositoryTmuxRunner("codex", cfg, executor)
		if err != nil {
			t.Fatalf("NewRepositoryTmuxRunner() error = %v", err)
		}
		bot := &Orchestrator{cfg: cfg, agents: manager, runner: runner}

		updated, shouldContinue, err := bot.reconcileRuntimeHealth(
			context.Background(),
			agent,
		)
		if err != nil {
			t.Fatalf("reconcileRuntimeHealth() error = %v", err)
		}
		if !shouldContinue {
			t.Fatal("reconcileRuntimeHealth() shouldContinue = false")
		}
		if updated.RuntimeHandle.Session != "" {
			t.Fatalf(
				"waiting runtime session = %q, want cleared",
				updated.RuntimeHandle.Session,
			)
		}
		if len(executor.calls) != 0 {
			t.Fatalf("waiting migration made tmux calls: %#v", executor.calls)
		}
	})
}

func TestRepositoryTmuxRunnerDeliversInitialAndFollowupOnRealServer(
	t *testing.T,
) {
	requireTmux(t)
	t.Setenv("CODEX_HOME", t.TempDir())
	cfg := runtimeIsolationTestConfig(t, "acme", "delivery")
	agent := runtimeIsolationTestAgent(
		t,
		cfg,
		"review-agent-delivery",
		156,
		"aaaaaaaa",
	)
	runner := realRepositoryTmuxRunner(t, cfg)

	handle, err := runner.Start(agent, "INITIAL_RUNTIME_MARKER")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = runner.Stop(handle)
	})
	if err := runner.SendScoped(
		agent,
		handle,
		"FOLLOWUP_RUNTIME_MARKER",
	); err != nil {
		t.Fatalf("SendScoped() error = %v", err)
	}
	waitForRuntimeMarkers(
		t,
		runner,
		handle,
		"INITIAL_RUNTIME_MARKER",
		"FOLLOWUP_RUNTIME_MARKER",
	)
	assertPaneInputDisabled(t, runner, handle)
}

func TestRuntimeIsolationMigrationReconcilesInterruptedRealSessions(
	t *testing.T,
) {
	requireTmux(t)
	t.Setenv("CODEX_HOME", t.TempDir())

	for _, crashWindow := range []string{"session-created", "prompt-delivered"} {
		t.Run(crashWindow, func(t *testing.T) {
			cfg := runtimeIsolationTestConfig(t, "acme", crashWindow)
			agent := runtimeIsolationTestAgent(
				t,
				cfg,
				"coding-agent-"+crashWindow,
				156,
				"aaaaaaaa",
			)
			agent.Role = RoleCoder
			agent.State = StateWorking
			agent.RuntimeHandle = RuntimeHandle{
				Kind:    RuntimeKindTmux,
				Session: tmuxSessionName(agent),
			}
			manager := NewAgentManager()
			if err := manager.Add(&agent); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			runner := realRepositoryTmuxRunner(t, cfg)

			switch crashWindow {
			case "session-created":
				if err := runner.runTmuxCommandContext(
					context.Background(),
					"new-session",
					"-d",
					"-s",
					tmuxSessionName(agent),
					"-c",
					agent.WorktreePath,
				); err != nil {
					t.Fatalf("create interrupted session error = %v", err)
				}
			case "prompt-delivered":
				orphan, err := runner.Start(agent, "ORPHAN_PROMPT_MARKER")
				if err != nil {
					t.Fatalf("create prompted orphan error = %v", err)
				}
				if orphan.Session != tmuxSessionName(agent) {
					t.Fatalf(
						"orphan session = %q, want %q",
						orphan.Session,
						tmuxSessionName(agent),
					)
				}
			}

			bot := &Orchestrator{
				cfg:    cfg,
				agents: manager,
				runner: runner,
			}
			updated, shouldContinue, err := bot.reconcileRuntimeHealth(
				context.Background(),
				agent,
			)
			if err != nil {
				t.Fatalf("reconcileRuntimeHealth() error = %v", err)
			}
			if !shouldContinue {
				t.Fatal("reconcileRuntimeHealth() shouldContinue = false")
			}
			t.Cleanup(func() {
				_ = runner.Stop(updated.RuntimeHandle)
			})
			if runtimeHandleNeedsIsolationMigration(updated.RuntimeHandle) {
				t.Fatalf(
					"reconciled handle remains legacy: %#v",
					updated.RuntimeHandle,
				)
			}
			waitForRuntimeMarkers(
				t,
				runner,
				updated.RuntimeHandle,
				"mandatory runtime isolation migration",
			)
			if crashWindow == "prompt-delivered" {
				pane, captureErr := runner.Capture(
					updated.RuntimeHandle,
					200,
				)
				if captureErr != nil {
					t.Fatalf("Capture() error = %v", captureErr)
				}
				if strings.Contains(pane, "ORPHAN_PROMPT_MARKER") {
					t.Fatalf(
						"reconciled pane retained orphan prompt: %q",
						pane,
					)
				}
			}
		})
	}
}

func TestStartupReconcilesCheckpointedNonReviewLaunches(t *testing.T) {
	requireTmux(t)
	t.Setenv("CODEX_HOME", t.TempDir())

	for _, crashWindow := range []string{"session-created", "prompt-delivered"} {
		t.Run(crashWindow, func(t *testing.T) {
			cfg := runtimeIsolationTestConfig(
				t,
				"acme",
				"startup-"+crashWindow,
			)
			agent := runtimeIsolationTestAgent(
				t,
				cfg,
				"coding-agent-startup-"+crashWindow,
				156,
				"aaaaaaaa",
			)
			agent.Role = RoleCoder
			agent.State = StateInitializing
			manager := NewAgentManager()
			if err := manager.Add(&agent); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			runner := realRepositoryTmuxRunner(t, cfg)
			checkpointing := &Orchestrator{
				cfg:    cfg,
				agents: manager,
				runner: runner,
			}
			if err := checkpointing.persistAgentState(); err != nil {
				t.Fatalf("persistAgentState() error = %v", err)
			}
			codexHome, err := runner.isolation.codexHomeForAgent(agent)
			if err != nil {
				t.Fatalf("codexHomeForAgent() error = %v", err)
			}
			orphanHandle := RuntimeHandle{
				Kind:       RuntimeKindTmux,
				Session:    tmuxSessionName(agent),
				TmuxServer: runner.isolation.tmuxServer,
			}
			t.Cleanup(func() {
				_ = runner.Stop(orphanHandle)
			})

			switch crashWindow {
			case "session-created":
				if err := runner.runTmuxCommandContext(
					context.Background(),
					"new-session",
					"-d",
					"-s",
					orphanHandle.Session,
					"-c",
					agent.WorktreePath,
				); err != nil {
					t.Fatalf("create interrupted session error = %v", err)
				}
			case "prompt-delivered":
				started, err := runner.Start(
					agent,
					"ORPHAN_STARTUP_PROMPT_MARKER",
				)
				if err != nil {
					t.Fatalf("create prompted orphan error = %v", err)
				}
				orphanHandle = started
			}

			restarted := &Orchestrator{
				cfg:    cfg,
				agents: NewAgentManager(),
				runner: runner,
				cleanupWorktreeFunc: func(
					context.Context,
					string,
					string,
					string,
				) error {
					return os.RemoveAll(agent.WorktreePath)
				},
			}
			if err := restarted.loadPersistedAgentState(); err != nil {
				t.Fatalf("loadPersistedAgentState() error = %v", err)
			}
			if got := len(restarted.agents.List()); got != 0 {
				t.Fatalf("restored agents = %d, want 0", got)
			}
			alive, err := runner.IsAlive(orphanHandle)
			if err != nil {
				t.Fatalf("IsAlive(orphan) error = %v", err)
			}
			if alive {
				t.Fatal("checkpointed orphan runtime remained alive")
			}
			if _, err := os.Stat(codexHome); !os.IsNotExist(err) {
				t.Fatalf("checkpointed Codex home still exists: %v", err)
			}
			if _, err := os.Stat(agent.WorktreePath); !os.IsNotExist(err) {
				t.Fatalf("checkpointed worktree still exists: %v", err)
			}
		})
	}
}

func requireTmux(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is unavailable")
	}
}

func realRepositoryTmuxRunner(
	t *testing.T,
	cfg Config,
) *TmuxRunner {
	t.Helper()
	fakeCodex := filepath.Join(t.TempDir(), "fake-codex")
	body := "#!/bin/sh\n" +
		"printf 'OpenAI Codex\\n'\n" +
		"while IFS= read -r line; do\n" +
		"  printf 'RECEIVED:%s\\n' \"$line\"\n" +
		"done\n"
	if err := os.WriteFile(fakeCodex, []byte(body), 0o700); err != nil {
		t.Fatalf("WriteFile(fake Codex) error = %v", err)
	}
	runner, err := NewRepositoryTmuxRunner(
		fakeCodex,
		cfg,
		osCommandExecutor{},
	)
	if err != nil {
		t.Fatalf("NewRepositoryTmuxRunner() error = %v", err)
	}
	return runner
}

func waitForRuntimeMarkers(
	t *testing.T,
	runner *TmuxRunner,
	handle RuntimeHandle,
	markers ...string,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		pane, err := runner.Capture(handle, 300)
		if err != nil {
			t.Fatalf("Capture() error = %v", err)
		}
		allFound := true
		for _, marker := range markers {
			if !strings.Contains(pane, marker) {
				allFound = false
				break
			}
		}
		if allFound && strings.Count(pane, "RECEIVED:") >= len(markers) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"runtime pane missing markers %q: %q",
				markers,
				pane,
			)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func assertPaneInputDisabled(
	t *testing.T,
	runner *TmuxRunner,
	handle RuntimeHandle,
) {
	t.Helper()
	out, err := runner.outputTmuxCommandContext(
		context.Background(),
		"display-message",
		"-p",
		"-t",
		handle.Session,
		"#{pane_input_off}",
	)
	if err != nil {
		t.Fatalf("display pane input state error = %v", err)
	}
	if strings.TrimSpace(string(out)) != "1" {
		t.Fatalf("pane input state = %q, want disabled", out)
	}
}

func runtimeIsolationTestConfig(
	t *testing.T,
	owner string,
	name string,
) Config {
	t.Helper()
	repoPath := filepath.Join(t.TempDir(), name)
	logDir := filepath.Join(repoPath, ".repository-agent-orchestrator")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", logDir, err)
	}
	return Config{
		RepoOwner: owner,
		RepoName:  name,
		RepoPath:  repoPath,
		LogDir:    logDir,
	}
}

func runtimeIsolationTestAgent(
	t *testing.T,
	cfg Config,
	id string,
	prNumber int,
	headSHA string,
) Agent {
	t.Helper()
	worktree := filepath.Join(cfg.RepoPath, ".worktrees", id)
	if err := os.MkdirAll(worktree, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", worktree, err)
	}
	return Agent{
		ID:                id,
		WorktreePath:      worktree,
		RuntimeCWD:        worktree,
		LogDir:            cfg.LogDir,
		PRNumber:          prNumber,
		ObservedPRHeadSHA: headSHA,
	}
}

func assertRepositoryTmuxCalls(
	t *testing.T,
	calls []fakeCommandCall,
	handle RuntimeHandle,
) {
	t.Helper()
	if len(calls) != 13 {
		t.Fatalf("tmux calls = %d, want 13: %#v", len(calls), calls)
	}
	for _, call := range calls {
		if call.name != "tmux" ||
			len(call.args) < 2 ||
			call.args[0] != "-L" ||
			call.args[1] != handle.TmuxServer {
			t.Fatalf("call escaped repository tmux server: %#v", call)
		}
	}
	launch := calls[2]
	if !strings.Contains(
		strings.Join(launch.args, " "),
		"CODEX_HOME="+shellSingleQuote(handle.CodexHome),
	) {
		t.Fatalf("runtime launch omitted isolated CODEX_HOME: %#v", launch)
	}
	wantInputDisabled := []string{
		"-L",
		handle.TmuxServer,
		"select-pane",
		"-d",
		"-t",
		handle.Session,
	}
	if !reflect.DeepEqual(calls[len(calls)-1].args, wantInputDisabled) {
		t.Fatalf(
			"last tmux call = %#v, want direct input disabled with %#v",
			calls[len(calls)-1].args,
			wantInputDisabled,
		)
	}
}

// managedStartOutputs scripts capture-pane responses for a full managed
// Start() launch: two consecutive idle readiness polls, a "beforePaste"
// snapshot, a poll confirming the paste-buffer delivery changed the pane,
// and a poll confirming the submit Enter changed the pane again. offset
// shifts every index by the number of tmux calls that precede the launch
// sequence (e.g. an isolation-migration reconciliation call).
func managedStartOutputs(offset int) map[int][]byte {
	return map[int][]byte{
		offset + 4:  []byte("OpenAI Codex"),
		offset + 5:  []byte("OpenAI Codex"),
		offset + 6:  []byte("idle"),
		offset + 9:  []byte("prompt pasted"),
		offset + 11: []byte("prompt submitted"),
	}
}

func managedSendOutputs() map[int][]byte {
	return map[int][]byte{
		1: []byte("idle"),
		4: []byte("prompt pasted"),
		6: []byte("prompt submitted"),
	}
}

func scopedPromptFromCalls(
	t *testing.T,
	executor *fakeCommandExecutor,
) (RuntimeScope, string) {
	t.Helper()
	for _, call := range executor.calls {
		if call.name != "tmux" {
			continue
		}
		loadIdx := -1
		for i, arg := range call.args {
			if arg == "load-buffer" {
				loadIdx = i
				break
			}
		}
		if loadIdx == -1 ||
			loadIdx+2 >= len(call.args) ||
			call.args[loadIdx+1] != "-b" {
			continue
		}
		bufferName := call.args[loadIdx+2]
		payload, ok := executor.loadedBuffers[bufferName]
		if !ok || !strings.HasPrefix(payload, runtimeScopeEnvelopePrefix) {
			continue
		}
		scope, body, err := parseScopedRuntimeMessage(payload)
		if err != nil {
			t.Fatalf("parseScopedRuntimeMessage() error = %v", err)
		}
		return scope, body
	}
	t.Fatalf("scoped prompt not found in calls: %#v", executor.calls)
	return RuntimeScope{}, ""
}
