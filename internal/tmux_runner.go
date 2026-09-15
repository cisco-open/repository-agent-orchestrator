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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode"
)

type commandExecutor interface {
	Run(name string, args ...string) error
	Output(name string, args ...string) ([]byte, error)
}

type contextCommandExecutor interface {
	RunContext(ctx context.Context, name string, args ...string) error
	OutputContext(
		ctx context.Context,
		name string,
		args ...string,
	) ([]byte, error)
}

type osCommandExecutor struct{}

const reviewWorkerEnvironmentErrorRedaction = "[review-worker-environment-redacted]"

func (e osCommandExecutor) Run(name string, args ...string) error {
	cmd := newCommand(name, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		stderrMsg := strings.TrimSpace(stderr.String())
		command, stderrMsg := safeCommandErrorDetails(name, args, stderrMsg)
		if stderrMsg == "" {
			return fmt.Errorf("%s: %w", command, err)
		}
		return fmt.Errorf("%s: %w: %s", command, err, stderrMsg)
	}
	return nil
}

func (e osCommandExecutor) Output(name string, args ...string) ([]byte, error) {
	cmd := newCommand(name, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		stderrMsg := strings.TrimSpace(stderr.String())
		command, stderrMsg := safeCommandErrorDetails(name, args, stderrMsg)
		if stderrMsg == "" {
			return nil, fmt.Errorf("%s: %w", command, err)
		}
		return nil, fmt.Errorf("%s: %w: %s", command, err, stderrMsg)
	}
	return out, nil
}

func (e osCommandExecutor) RunContext(
	ctx context.Context,
	name string,
	args ...string,
) error {
	cmd := newCommandContext(ctx, name, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		stderrMsg := strings.TrimSpace(stderr.String())
		command, stderrMsg := safeCommandErrorDetails(name, args, stderrMsg)
		if stderrMsg == "" {
			return fmt.Errorf("%s: %w", command, err)
		}
		return fmt.Errorf("%s: %w: %s", command, err, stderrMsg)
	}
	return nil
}

func (e osCommandExecutor) OutputContext(
	ctx context.Context,
	name string,
	args ...string,
) ([]byte, error) {
	cmd := newCommandContext(ctx, name, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		stderrMsg := strings.TrimSpace(stderr.String())
		command, stderrMsg := safeCommandErrorDetails(name, args, stderrMsg)
		if stderrMsg == "" {
			return nil, fmt.Errorf("%s: %w", command, err)
		}
		return nil, fmt.Errorf("%s: %w: %s", command, err, stderrMsg)
	}
	return out, nil
}

func safeCommandErrorDetails(name string, args []string, stderr string) (string, string) {
	safeArgs := append([]string(nil), args...)
	containsReviewWorkerEnvironment := false
	for index, arg := range safeArgs {
		if !isReviewWorkerIsolatedShellCommand(arg) {
			continue
		}
		containsReviewWorkerEnvironment = true
		safeArgs[index] = reviewWorkerEnvironmentErrorRedaction
	}
	if containsReviewWorkerEnvironment && strings.TrimSpace(stderr) != "" {
		// The isolated shell argument contains allowlisted environment values.
		// tmux and wrappers may echo any transformed fragment of that argument,
		// so no stderr from its creation boundary is safe to forward.
		stderr = reviewWorkerEnvironmentErrorRedaction
	}
	return strings.TrimSpace(name + " " + strings.Join(safeArgs, " ")), stderr
}

func isReviewWorkerIsolatedShellCommand(arg string) bool {
	arg = strings.TrimSpace(arg)
	return strings.HasPrefix(arg, "/usr/bin/env -i ") &&
		strings.HasSuffix(arg, " /bin/sh")
}

type TmuxRunner struct {
	codexCmd             string
	exec                 commandExecutor
	isolation            *runtimeIsolation
	managedPromptTimeout time.Duration
	managedPromptPoll    time.Duration
}

const (
	codexReadyProbeLines   = "-200"
	codexReadyPollInterval = 100 * time.Millisecond
	codexReadyTimeout      = 20 * time.Second
	// codexReadyConsecutivePolls requires the pane to look idle across this
	// many consecutive polls before Codex is considered ready. A single
	// clean-looking poll isn't enough: Codex's busy indicator (e.g. while
	// booting a configured MCP server) can transiently disappear and
	// reappear, and sending the initial prompt into that gap leaves it
	// pasted-but-unsubmitted with no automatic recovery.
	codexReadyConsecutivePolls = 2
	managedPromptTimeout       = 3 * time.Second
	managedPromptPoll          = 25 * time.Millisecond
	managedPromptAttempts      = 2
	tmuxPasteCleanupTimeout    = 3 * time.Second
	maxRuntimeAgentLogs        = 10
)

func NewTmuxRunner(codexCmd string, exec commandExecutor) *TmuxRunner {
	if exec == nil {
		exec = osCommandExecutor{}
	}
	cmd := strings.TrimSpace(codexCmd)
	if cmd == "" {
		cmd = "codex"
	}
	return &TmuxRunner{
		codexCmd:             cmd,
		exec:                 exec,
		managedPromptTimeout: managedPromptTimeout,
		managedPromptPoll:    managedPromptPoll,
	}
}

func NewRepositoryTmuxRunner(
	codexCmd string,
	cfg Config,
	exec commandExecutor,
) (*TmuxRunner, error) {
	isolation, err := newRuntimeIsolation(cfg)
	if err != nil {
		return nil, err
	}
	runner := NewTmuxRunner(codexCmd, exec)
	runner.isolation = isolation
	return runner, nil
}

func (r *TmuxRunner) Start(agent Agent, initialPrompt string) (RuntimeHandle, error) {
	return r.start(context.Background(), agent, initialPrompt, nil, false)
}

func (r *TmuxRunner) ValidateReviewWorkerIsolation(environment []string) error {
	if r == nil || r.exec == nil {
		return errors.New("review worker command executor is not configured")
	}
	if err := validateReviewWorkerEnvironment(environment); err != nil {
		return err
	}
	for _, path := range []string{"/usr/bin/env", "/bin/sh"} {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("review worker isolation executable %s is unavailable: %w", path, err)
		}
		if info.IsDir() || info.Mode()&0o111 == 0 {
			return fmt.Errorf("review worker isolation executable %s is not executable", path)
		}
	}
	return nil
}

func (r *TmuxRunner) StartReviewWorker(
	agent Agent,
	initialPrompt string,
	environment []string,
) (RuntimeHandle, error) {
	return r.StartReviewWorkerContext(
		context.Background(),
		agent,
		initialPrompt,
		environment,
	)
}

func (r *TmuxRunner) StartReviewWorkerContext(
	ctx context.Context,
	agent Agent,
	initialPrompt string,
	environment []string,
) (RuntimeHandle, error) {
	if err := r.ValidateReviewWorkerIsolation(environment); err != nil {
		return RuntimeHandle{}, fmt.Errorf("review worker isolation is unavailable: %w", err)
	}
	return r.start(
		ctx,
		agent,
		initialPrompt,
		append([]string(nil), environment...),
		true,
	)
}

func (r *TmuxRunner) start(
	ctx context.Context,
	agent Agent,
	initialPrompt string,
	isolatedEnvironment []string,
	reviewWorker bool,
) (RuntimeHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RuntimeHandle{}, err
	}
	if strings.TrimSpace(agent.WorktreePath) == "" {
		return RuntimeHandle{}, errors.New("agent worktree path is empty")
	}
	startDir := strings.TrimSpace(agent.RuntimeCWD)
	if startDir == "" {
		startDir = strings.TrimSpace(agent.WorktreePath)
	}
	if startDir == "" {
		return RuntimeHandle{}, errors.New("runtime start directory is empty")
	}
	runtimeCommand, err := codexCommandForAgentProfile(r.codexCmd, agent.RuntimeProfile)
	if err != nil {
		return RuntimeHandle{}, fmt.Errorf("invalid runtime profile for agent %s: %w", agent.ID, err)
	}
	codexHome := ""
	scope := RuntimeScope{}
	if r.isolation != nil {
		codexHome, err = r.isolation.codexHomeForAgent(agent)
		if err != nil {
			return RuntimeHandle{}, err
		}
		scope = r.isolation.scopeForAgent(agent)
		initialPrompt, err = formatScopedRuntimeMessage(scope, initialPrompt)
		if err != nil {
			return RuntimeHandle{}, fmt.Errorf("failed to scope initial runtime prompt: %w", err)
		}
	}

	logPath, err := runtimeLogPath(agent)
	if err != nil {
		return RuntimeHandle{}, err
	}
	if err := pruneRuntimeLogsForNewSession(logPath, maxRuntimeAgentLogs); err != nil {
		log.Printf("non-fatal: failed to prune runtime logs dir=%s keep=%d: %s", filepath.Dir(logPath), maxRuntimeAgentLogs, err.Error())
	}
	if err := ensureRuntimeLogFile(logPath); err != nil {
		return RuntimeHandle{}, fmt.Errorf("failed to initialize runtime log file %q: %w", logPath, err)
	}

	session := tmuxSessionName(agent)
	if reviewWorker {
		session = reviewWorkerTmuxSessionName(agent)
	}
	newSessionArgs := []string{"new-session", "-d", "-s", session, "-c", startDir}
	if reviewWorker {
		newSessionArgs = append(
			newSessionArgs,
			reviewWorkerIsolatedShellCommand(isolatedEnvironment),
		)
	}
	if err := r.runTmuxCommandContext(ctx, newSessionArgs...); err != nil {
		if isTmuxServerExitedUnexpectedly(err) {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return RuntimeHandle{}, ctx.Err()
			case <-timer.C:
			}
			if retryErr := r.runTmuxCommandContext(
				ctx,
				newSessionArgs...,
			); retryErr != nil {
				return RuntimeHandle{}, fmt.Errorf("failed to create tmux session (after retry): %w", retryErr)
			}
		} else {
			return RuntimeHandle{}, fmt.Errorf("failed to create tmux session: %w", err)
		}
	}

	handle := RuntimeHandle{
		Kind:      RuntimeKindTmux,
		Session:   session,
		LogPath:   logPath,
		CodexHome: codexHome,
		Scope:     scope,
	}
	if r.isolation != nil {
		handle.TmuxServer = r.isolation.tmuxServer
	}
	pipeCommand, err := runtimeLogPipeCommand(logPath)
	if err != nil {
		_ = r.Stop(handle)
		return RuntimeHandle{}, fmt.Errorf("failed to build tmux log pipe command for session %q: %w", session, err)
	}
	if err := r.runTmuxCommandContext(
		ctx,
		"pipe-pane",
		"-o",
		"-t",
		session,
		pipeCommand,
	); err != nil {
		_ = r.Stop(handle)
		return RuntimeHandle{}, fmt.Errorf("failed to configure tmux log piping for session %q: %w", session, err)
	}

	if err := r.sendRuntimeCommandModeContext(
		ctx,
		session,
		runtimeCommand,
		codexHome,
		reviewWorker,
	); err != nil {
		_ = r.Stop(handle)
		return RuntimeHandle{}, fmt.Errorf("failed to launch codex in tmux session: %w", err)
	}

	if err := r.waitForCodexReadyContext(ctx, session); err != nil {
		_ = r.Stop(handle)
		return RuntimeHandle{}, err
	}

	var promptErr error
	if r.isolation == nil {
		promptErr = r.sendPromptContext(ctx, session, initialPrompt)
	} else {
		promptErr = r.sendManagedPromptContext(ctx, session, initialPrompt)
	}
	if promptErr != nil {
		_ = r.Stop(handle)
		return RuntimeHandle{}, fmt.Errorf("failed to send initial prompt to codex: %w", promptErr)
	}
	if r.isolation != nil {
		if err := r.setPaneInputContext(ctx, session, false); err != nil {
			_ = r.Stop(handle)
			return RuntimeHandle{}, fmt.Errorf(
				"failed to disable direct input for tmux session %q: %w",
				session,
				err,
			)
		}
	}

	return handle, nil
}

func reviewWorkerIsolatedShellCommand(environment []string) string {
	parts := []string{"/usr/bin/env", "-i"}
	for _, entry := range environment {
		parts = append(parts, shellSingleQuote(entry))
	}
	parts = append(parts, "/bin/sh")
	return strings.Join(parts, " ")
}

func (r *TmuxRunner) Send(handle RuntimeHandle, text string) error {
	if r != nil && r.isolation != nil {
		return errors.New("unscoped runtime messages are disabled")
	}
	if handle.Kind != RuntimeKindTmux {
		return fmt.Errorf("unsupported runtime kind: %s", handle.Kind)
	}
	if strings.TrimSpace(handle.Session) == "" {
		return errors.New("runtime session is empty")
	}
	return r.sendPrompt(handle.Session, text)
}

func (r *TmuxRunner) BindRuntimeHandle(
	agent Agent,
	handle RuntimeHandle,
) (RuntimeHandle, error) {
	if r == nil || r.isolation == nil {
		return handle, errors.New("runtime scope binding is unavailable")
	}
	if handle.Kind != RuntimeKindTmux {
		return handle, fmt.Errorf("unsupported runtime kind: %s", handle.Kind)
	}
	if strings.TrimSpace(handle.Session) == "" {
		return handle, errors.New("runtime session is empty")
	}
	if strings.TrimSpace(handle.TmuxServer) != r.isolation.tmuxServer {
		return handle, fmt.Errorf(
			"runtime tmux server mismatch: got %q, want %q",
			handle.TmuxServer,
			r.isolation.tmuxServer,
		)
	}
	expected := r.isolation.scopeForAgent(agent)
	if err := validateRuntimeScopeImmutable(expected, handle.Scope); err != nil {
		return handle, err
	}
	handle.Scope = expected
	return handle, nil
}

func (r *TmuxRunner) SendScoped(
	agent Agent,
	handle RuntimeHandle,
	text string,
) error {
	if r == nil || r.isolation == nil {
		return errors.New("runtime scope enforcement is unavailable")
	}
	if handle.Kind != RuntimeKindTmux {
		return fmt.Errorf("runtime message rejected: unsupported runtime kind: %s", handle.Kind)
	}
	if strings.TrimSpace(handle.Session) == "" {
		return errors.New("runtime message rejected: runtime session is empty")
	}
	if strings.TrimSpace(handle.TmuxServer) != r.isolation.tmuxServer {
		return fmt.Errorf(
			"runtime message rejected: runtime tmux server mismatch: got %q, want %q",
			handle.TmuxServer,
			r.isolation.tmuxServer,
		)
	}
	if err := validateRuntimeScopeExact(
		r.isolation.scopeForAgent(agent),
		handle.Scope,
	); err != nil {
		return fmt.Errorf("runtime message rejected: %w", err)
	}
	message, err := formatScopedRuntimeMessage(handle.Scope, text)
	if err != nil {
		return fmt.Errorf("runtime message rejected: %w", err)
	}
	parsed, _, err := parseScopedRuntimeMessage(message)
	if err != nil {
		return fmt.Errorf("runtime message rejected: %w", err)
	}
	if err := validateRuntimeScopeExact(handle.Scope, parsed); err != nil {
		return fmt.Errorf("runtime message rejected: %w", err)
	}
	return r.sendScopedPrompt(handle, message)
}

func (r *TmuxRunner) sendScopedPrompt(
	handle RuntimeHandle,
	text string,
) error {
	if r == nil || r.isolation == nil {
		return errors.New("managed pane input is unavailable")
	}
	if err := r.setPaneInputContext(
		context.Background(),
		handle.Session,
		true,
	); err != nil {
		return fmt.Errorf(
			"failed to enable managed input for tmux session %q: %w",
			handle.Session,
			err,
		)
	}
	sendErr := r.sendManagedPromptContext(
		context.Background(),
		handle.Session,
		text,
	)
	disableErr := r.setPaneInputContext(
		context.Background(),
		handle.Session,
		false,
	)
	if disableErr == nil {
		return sendErr
	}

	stopErr := r.Stop(handle)
	return &runtimeDeliveryError{
		err: errors.Join(
			sendErr,
			fmt.Errorf(
				"failed to restore disabled input for tmux session %q: %w",
				handle.Session,
				disableErr,
			),
			stopErr,
		),
		delivered:   sendErr == nil,
		quarantined: stopErr == nil,
	}
}

func (r *TmuxRunner) setPaneInputContext(
	ctx context.Context,
	session string,
	enabled bool,
) error {
	flag := "-d"
	if enabled {
		flag = "-e"
	}
	return r.runTmuxCommandContext(
		ctx,
		"select-pane",
		flag,
		"-t",
		session,
	)
}

func (r *TmuxRunner) Stop(handle RuntimeHandle) error {
	if handle.Kind != RuntimeKindTmux {
		return fmt.Errorf("unsupported runtime kind: %s", handle.Kind)
	}
	if strings.TrimSpace(handle.Session) == "" {
		return nil
	}
	args, err := r.tmuxArgsForHandle(handle, "kill-session", "-t", handle.Session)
	if err != nil {
		return err
	}
	err = r.exec.Run("tmux", args...)
	if err == nil || isTmuxSessionMissing(err) {
		return nil
	}
	return fmt.Errorf("failed to stop tmux session %q: %w", handle.Session, err)
}

// ReconcileRuntimeLaunch removes any deterministic repository-scoped session
// that may have been created before its handle was durably persisted. It is
// deliberately idempotent so startup recovery can retry after any crash
// window in Start.
func (r *TmuxRunner) ReconcileRuntimeLaunch(agent Agent) error {
	if r == nil || r.isolation == nil {
		return errors.New("runtime launch reconciliation is unavailable")
	}
	return r.Stop(RuntimeHandle{
		Kind:       RuntimeKindTmux,
		Session:    tmuxSessionName(agent),
		TmuxServer: r.isolation.tmuxServer,
	})
}

func (r *TmuxRunner) RemoveRuntimeState(agent Agent) error {
	if r == nil || r.isolation == nil {
		return errors.New("runtime state cleanup is unavailable")
	}
	return r.isolation.removeCodexHomeForAgent(agent)
}

func (r *TmuxRunner) Capture(handle RuntimeHandle, lines int) (string, error) {
	if handle.Kind != RuntimeKindTmux {
		return "", fmt.Errorf("unsupported runtime kind: %s", handle.Kind)
	}
	if strings.TrimSpace(handle.Session) == "" {
		return "", nil
	}
	if lines <= 0 {
		lines = 200
	}
	start := fmt.Sprintf("-%d", lines)
	args, err := r.tmuxArgsForHandle(
		handle,
		"capture-pane",
		"-p",
		"-J",
		"-t",
		handle.Session,
		"-S",
		start,
	)
	if err != nil {
		return "", err
	}
	out, err := r.exec.Output("tmux", args...)
	if err != nil {
		if isTmuxSessionMissing(err) {
			return "", nil
		}
		return "", fmt.Errorf("failed to capture tmux output for session %q: %w", handle.Session, err)
	}
	return string(out), nil
}

func (r *TmuxRunner) IsAlive(handle RuntimeHandle) (bool, error) {
	if handle.Kind != RuntimeKindTmux {
		return false, fmt.Errorf("unsupported runtime kind: %s", handle.Kind)
	}
	if strings.TrimSpace(handle.Session) == "" {
		return false, nil
	}
	args, err := r.tmuxArgsForHandle(handle, "has-session", "-t", handle.Session)
	if err != nil {
		return false, err
	}
	err = r.exec.Run("tmux", args...)
	if err == nil {
		return true, nil
	}
	if isTmuxSessionMissing(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to check tmux session %q: %w", handle.Session, err)
}

func codexCommandForAgentProfile(baseCommand string, profile AgentProfile) (string, error) {
	baseCommand = strings.TrimSpace(baseCommand)
	if baseCommand == "" {
		return "", errors.New("runtime command is empty")
	}
	if strings.TrimSpace(profile.Name) == "" &&
		strings.TrimSpace(profile.Model) == "" &&
		strings.TrimSpace(profile.ReasoningEffort) == "" {
		profile.InheritGlobal = true
	}
	if profile.InheritGlobal {
		if strings.TrimSpace(profile.Model) != "" || strings.TrimSpace(profile.ReasoningEffort) != "" {
			return "", errors.New("inherited runtime profile cannot set model or reasoning effort")
		}
		return baseCommand, nil
	}
	model := strings.TrimSpace(profile.Model)
	effort := normalizeReasoningEffort(profile.ReasoningEffort)
	if model == "" || effort == "" {
		return "", errors.New("model and reasoning effort are required")
	}
	if !safeCodexModelName(model) {
		return "", fmt.Errorf("unsafe model name %q", model)
	}
	if _, ok := supportedReasoningEfforts[effort]; !ok {
		return "", fmt.Errorf("unsupported reasoning effort %q", effort)
	}
	baseCommand, err := removeTopLevelCodexModelArguments(baseCommand)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s --model %s -c %s",
		baseCommand,
		shellSingleQuote(model),
		shellSingleQuote(fmt.Sprintf("model_reasoning_effort=%q", effort)),
	), nil
}

type shellCommandWord struct {
	raw   string
	value string
}

func removeTopLevelCodexModelArguments(command string) (string, error) {
	words, err := parseShellCommandWords(command)
	if err != nil {
		return "", fmt.Errorf("failed to parse CODEX_CMD: %w", err)
	}
	filtered := make([]string, 0, len(words))
	for i := 0; i < len(words); i++ {
		switch {
		case words[i].value == "--model" || words[i].value == "-m":
			if i+1 >= len(words) {
				return "", fmt.Errorf("CODEX_CMD %s is missing its model value", words[i].value)
			}
			i++
		case strings.HasPrefix(words[i].value, "--model="),
			strings.HasPrefix(words[i].value, "-m="),
			strings.HasPrefix(words[i].value, "-m") && len(words[i].value) > len("-m"):
			continue
		default:
			filtered = append(filtered, words[i].raw)
		}
	}
	if len(filtered) == 0 {
		return "", errors.New("runtime command is empty after replacing CODEX_CMD model arguments")
	}
	return strings.Join(filtered, " "), nil
}

func parseShellCommandWords(command string) ([]shellCommandWord, error) {
	words := make([]shellCommandWord, 0)
	for i := 0; i < len(command); {
		for i < len(command) && isShellCommandWhitespace(command[i]) {
			i++
		}
		if i >= len(command) {
			break
		}

		start := i
		var value strings.Builder
		var quote byte
		for i < len(command) {
			current := command[i]
			if quote == 0 {
				if isShellCommandWhitespace(current) {
					break
				}
				switch current {
				case '\'', '"':
					quote = current
					i++
				case '\\':
					if i+1 >= len(command) {
						return nil, errors.New("trailing escape")
					}
					value.WriteByte(command[i+1])
					i += 2
				default:
					value.WriteByte(current)
					i++
				}
				continue
			}

			if current == quote {
				quote = 0
				i++
				continue
			}
			if quote == '"' && current == '\\' {
				if i+1 >= len(command) {
					return nil, errors.New("trailing escape in double-quoted value")
				}
				value.WriteByte(command[i+1])
				i += 2
				continue
			}
			value.WriteByte(current)
			i++
		}
		if quote != 0 {
			return nil, errors.New("unterminated quoted value")
		}
		words = append(words, shellCommandWord{
			raw:   command[start:i],
			value: value.String(),
		})
	}
	return words, nil
}

func isShellCommandWhitespace(value byte) bool {
	switch value {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func (r *TmuxRunner) sendRuntimeCommand(session string, text string) error {
	return r.sendRuntimeCommandContext(context.Background(), session, text, "")
}

func (r *TmuxRunner) sendRuntimeCommandContext(
	ctx context.Context,
	session string,
	text string,
	codexHome string,
) error {
	return r.sendRuntimeCommandModeContext(
		ctx,
		session,
		text,
		codexHome,
		false,
	)
}

func (r *TmuxRunner) sendRuntimeCommandModeContext(
	ctx context.Context,
	session string,
	text string,
	codexHome string,
	replaceShell bool,
) error {
	normalized := strings.ReplaceAll(text, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	normalized = strings.TrimSpace(strings.ReplaceAll(normalized, "\n", " "))
	if normalized == "" {
		return errors.New("runtime command is empty")
	}
	normalized = runtimeLaunchCommand(normalized, runtime.GOOS, codexHome)
	if replaceShell {
		normalized = "exec " + normalized
	}

	if err := r.runTmuxCommandContext(
		ctx,
		"send-keys",
		"-t",
		session,
		"-l",
		"--",
		normalized,
	); err != nil {
		return fmt.Errorf("failed to send runtime command to tmux session %q: %w", session, err)
	}

	if err := r.runTmuxCommandContext(
		ctx,
		"send-keys",
		"-t",
		session,
		"Enter",
	); err != nil {
		return fmt.Errorf("failed to send enter for runtime command to tmux session %q: %w", session, err)
	}
	return nil
}

func (r *TmuxRunner) sendPrompt(session string, text string) error {
	return r.sendPromptContext(context.Background(), session, text)
}

func (r *TmuxRunner) sendPromptContext(
	ctx context.Context,
	session string,
	text string,
) error {
	if err := r.sendPromptTextContext(ctx, session, text); err != nil {
		return err
	}
	return r.sendPromptEnterContext(ctx, session)
}

func (r *TmuxRunner) sendManagedPromptContext(
	ctx context.Context,
	session string,
	text string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	beforePaste, err := r.captureManagedPromptPane(ctx, session)
	if err != nil {
		return fmt.Errorf(
			"failed to capture tmux session %q before prompt delivery: %w",
			session,
			err,
		)
	}
	if err := r.sendPromptTextContext(ctx, session, text); err != nil {
		return err
	}
	afterPaste, err := r.waitForManagedPromptPaneChange(
		ctx,
		session,
		beforePaste,
	)
	if err != nil {
		return fmt.Errorf(
			"prompt paste was not observed in tmux session %q: %w",
			session,
			err,
		)
	}

	var acknowledgementErr error
	for attempt := 1; attempt <= managedPromptAttempts; attempt++ {
		if err := r.sendPromptEnterContext(ctx, session); err != nil {
			return err
		}
		if _, err := r.waitForManagedPromptPaneChange(
			ctx,
			session,
			afterPaste,
		); err == nil {
			return nil
		} else {
			acknowledgementErr = err
		}
	}
	return fmt.Errorf(
		"prompt submission was not acknowledged in tmux session %q after %d Enter attempts: %w",
		session,
		managedPromptAttempts,
		acknowledgementErr,
	)
}

// sendPromptTextContext delivers prompt text into the pane via a tmux paste
// buffer rather than simulated keystrokes. A tmux paste buffer is loaded
// from a temporary file (avoiding any argv-length concerns for very large
// prompts) and pasted as a single, atomic, bracketed-paste operation, in
// place of splitting the text across many separate "send-keys -l"
// invocations. That chunked-keystroke approach could race the receiving
// application's own paste processing on large prompts, and required many
// sequential tmux subprocess invocations that could stall under PTY
// backpressure.
func (r *TmuxRunner) sendPromptTextContext(
	ctx context.Context,
	session string,
	text string,
) error {
	normalized := strings.ReplaceAll(text, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")

	bufferPath, cleanup, err := writeTmuxPasteBufferFile(normalized)
	if err != nil {
		return fmt.Errorf(
			"failed to prepare prompt paste buffer for tmux session %q: %w",
			session,
			err,
		)
	}
	defer cleanup()

	bufferName := tmuxPasteBufferName(session)
	if err := r.runTmuxCommandContext(
		ctx,
		"load-buffer",
		"-b",
		bufferName,
		bufferPath,
	); err != nil {
		return fmt.Errorf(
			"failed to load prompt text into tmux buffer for session %q: %w",
			session,
			err,
		)
	}
	if pasteErr := r.runTmuxCommandContext(
		ctx,
		"paste-buffer",
		"-d",
		"-p",
		"-t",
		session,
		"-b",
		bufferName,
	); pasteErr != nil {
		pasteErr = fmt.Errorf(
			"failed to paste prompt text into tmux session %q: %w",
			session,
			pasteErr,
		)
		// paste-buffer -d removes the buffer on success. Use an independent,
		// bounded context on failure so canceled delivery cannot retain the prompt.
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			tmuxPasteCleanupTimeout,
		)
		cleanupErr := r.runTmuxCommandContext(
			cleanupCtx,
			"delete-buffer",
			"-b",
			bufferName,
		)
		cancel()
		if cleanupErr != nil {
			return errors.Join(
				pasteErr,
				fmt.Errorf(
					"failed to delete prompt text from tmux buffer %q: %w",
					bufferName,
					cleanupErr,
				),
			)
		}
		return pasteErr
	}
	return nil
}

// writeTmuxPasteBufferFile writes content to a temporary file suitable for
// "tmux load-buffer" and returns a cleanup function that removes it.
func writeTmuxPasteBufferFile(content string) (string, func(), error) {
	f, err := os.CreateTemp("", "rao-tmux-paste-*.txt")
	if err != nil {
		return "", nil, err
	}
	path := f.Name()
	cleanup := func() { _ = os.Remove(path) }
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		cleanup()
		return "", nil, err
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

// tmuxPasteBufferName derives a tmux buffer name scoped to the target
// session, so concurrent prompt deliveries to different sessions on the
// same (possibly shared, isolated-per-repo) tmux server never collide.
func tmuxPasteBufferName(session string) string {
	return "rao-prompt-" + sanitizeSessionPart(session)
}

func (r *TmuxRunner) sendPromptEnterContext(
	ctx context.Context,
	session string,
) error {
	if err := r.runTmuxCommandContext(
		ctx,
		"send-keys",
		"-t",
		session,
		"Enter",
	); err != nil {
		return fmt.Errorf(
			"failed to send enter for prompt to tmux session %q: %w",
			session,
			err,
		)
	}
	return nil
}

func (r *TmuxRunner) captureManagedPromptPane(
	ctx context.Context,
	session string,
) (string, error) {
	out, err := r.outputTmuxCommandContext(
		ctx,
		"capture-pane",
		"-p",
		"-J",
		"-t",
		session,
		"-S",
		codexReadyProbeLines,
	)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func normalizeManagedPromptPane(pane string) string {
	lines := strings.Split(pane, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.Contains(line, "esc to interrupt") {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n")
}

func (r *TmuxRunner) waitForManagedPromptPaneChange(
	ctx context.Context,
	session string,
	previous string,
) (string, error) {
	timeout := r.managedPromptTimeout
	if timeout <= 0 {
		timeout = managedPromptTimeout
	}
	poll := r.managedPromptPoll
	if poll <= 0 {
		poll = managedPromptPoll
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	normalizedPrevious := normalizeManagedPromptPane(previous)
	for {
		pane, err := r.captureManagedPromptPane(waitCtx, session)
		if err != nil {
			if waitErr := waitCtx.Err(); waitErr != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return "", ctxErr
				}
				return "", waitErr
			}
			return "", err
		}
		if normalizeManagedPromptPane(pane) != normalizedPrevious {
			return pane, nil
		}
		select {
		case <-waitCtx.Done():
			if ctxErr := ctx.Err(); ctxErr != nil {
				return "", ctxErr
			}
			return "", waitCtx.Err()
		case <-ticker.C:
		}
	}
}

func (r *TmuxRunner) waitForCodexReady(session string) error {
	return r.waitForCodexReadyContext(
		context.Background(),
		session,
	)
}

func (r *TmuxRunner) waitForCodexReadyContext(
	ctx context.Context,
	session string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	readyCtx, cancel := context.WithTimeout(ctx, codexReadyTimeout)
	defer cancel()
	ticker := time.NewTicker(codexReadyPollInterval)
	defer ticker.Stop()
	sentContinue := false
	consecutiveReadyPolls := 0
	for {
		out, err := r.outputTmuxCommandContext(
			readyCtx,
			"capture-pane",
			"-p",
			"-t",
			session,
			"-S",
			codexReadyProbeLines,
		)
		if err != nil {
			if readyErr := readyCtx.Err(); readyErr != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return fmt.Errorf(
					"timed out waiting for codex prompt in tmux session %q: %w",
					session,
					readyErr,
				)
			}
			return fmt.Errorf("failed to detect codex readiness for session %q: %w", session, err)
		}
		pane := string(out)
		lower := strings.ToLower(pane)
		if strings.Contains(lower, "press enter to continue") && !sentContinue {
			if err := r.runTmuxCommandContext(
				readyCtx,
				"send-keys",
				"-t",
				session,
				"Enter",
			); err != nil {
				return fmt.Errorf("failed to continue codex startup in session %q: %w", session, err)
			}
			sentContinue = true
		}
		if codexReady(pane) {
			consecutiveReadyPolls++
			if consecutiveReadyPolls >= codexReadyConsecutivePolls {
				return nil
			}
		} else {
			consecutiveReadyPolls = 0
		}
		select {
		case <-readyCtx.Done():
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf(
				"timed out waiting for codex prompt in tmux session %q: %w",
				session,
				readyCtx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func (r *TmuxRunner) runCommandContext(
	ctx context.Context,
	name string,
	args ...string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if executor, ok := r.exec.(contextCommandExecutor); ok {
		return executor.RunContext(ctx, name, args...)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := r.exec.Run(name, args...)
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (r *TmuxRunner) outputCommandContext(
	ctx context.Context,
	name string,
	args ...string,
) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if executor, ok := r.exec.(contextCommandExecutor); ok {
		return executor.OutputContext(ctx, name, args...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := r.exec.Output(name, args...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *TmuxRunner) runTmuxCommandContext(
	ctx context.Context,
	args ...string,
) error {
	server := ""
	if r != nil && r.isolation != nil {
		server = r.isolation.tmuxServer
	}
	return r.runCommandContext(ctx, "tmux", tmuxServerArgs(server, args...)...)
}

func (r *TmuxRunner) outputTmuxCommandContext(
	ctx context.Context,
	args ...string,
) ([]byte, error) {
	server := ""
	if r != nil && r.isolation != nil {
		server = r.isolation.tmuxServer
	}
	return r.outputCommandContext(ctx, "tmux", tmuxServerArgs(server, args...)...)
}

func (r *TmuxRunner) tmuxArgsForHandle(
	handle RuntimeHandle,
	args ...string,
) ([]string, error) {
	server := strings.TrimSpace(handle.TmuxServer)
	if r != nil && r.isolation != nil {
		if server == "" {
			server = r.isolation.tmuxServer
		} else if server != r.isolation.tmuxServer {
			return nil, fmt.Errorf(
				"runtime tmux server mismatch: got %q, want %q",
				server,
				r.isolation.tmuxServer,
			)
		}
	}
	return tmuxServerArgs(server, args...), nil
}

func runtimeLaunchCommand(
	command string,
	goos string,
	codexHomes ...string,
) string {
	codexHome := ""
	if len(codexHomes) > 0 {
		codexHome = strings.TrimSpace(codexHomes[0])
	}
	parts := []string{}
	if goos == "darwin" {
		parts = append(
			parts,
			"env",
			"-u",
			"MallocStackLogging",
			"-u",
			"MallocStackLoggingNoCompact",
		)
	} else if codexHome != "" {
		parts = append(parts, "/usr/bin/env")
	}
	if codexHome != "" {
		parts = append(parts, "CODEX_HOME="+shellSingleQuote(codexHome))
	}
	if len(parts) == 0 {
		return command
	}
	return strings.Join(parts, " ") + " " + command
}

func codexReady(pane string) bool {
	return strings.Contains(pane, "OpenAI Codex") && !codexBusy(pane)
}

// codexBusy reports whether Codex is still doing asynchronous startup work
// rather than sitting idle at the composer. The banner ("OpenAI Codex")
// can render well before Codex has finished settling in; sending the
// initial prompt during that window has been observed to leave Codex
// stuck later on (e.g. an MCP server boot that never completes and never
// recovers), not merely to be queued and processed late. Readiness must
// wait for Codex to be fully settled, not just for the banner to appear.
func codexBusy(pane string) bool {
	if strings.Contains(pane, "esc to interrupt") {
		return true
	}
	return codexBannerStillResolving(pane)
}

// codexBannerStillResolving reports whether Codex's own startup banner box
// is still resolving its fields (e.g. "model:     loading" or
// "directory: loading") rather than showing their final values. The
// "OpenAI Codex" banner text is present from the very first frame, before
// these fields resolve.
func codexBannerStillResolving(pane string) bool {
	for _, line := range strings.Split(pane, "\n") {
		if strings.Contains(line, "loading") &&
			(strings.Contains(line, "model:") || strings.Contains(line, "directory:")) {
			return true
		}
	}
	return false
}

func tmuxSessionName(agent Agent) string {
	name := sanitizeSessionPart(agent.ID)
	if name == "" {
		name = fmt.Sprintf("issue-%d", agent.IssueNumber)
	}
	session := "repository-agent-orchestrator-" + name
	if len(session) > 64 {
		return session[:64]
	}
	return session
}

func reviewWorkerTmuxSessionName(agent Agent) string {
	name := sanitizeSessionPart(agent.ID)
	if name == "" {
		name = fmt.Sprintf("issue-%d", agent.IssueNumber)
	}
	session := "repository-agent-orchestrator-" + name
	return boundedTmuxSessionName(session)
}

func boundedTmuxSessionName(session string) string {
	if len(session) <= 64 {
		return session
	}
	digest := sha256.Sum256([]byte(session))
	suffix := fmt.Sprintf("-%x", digest[:6])
	return session[:64-len(suffix)] + suffix
}

func runtimeLogPath(agent Agent) (string, error) {
	logName := strings.TrimSpace(agent.ID)
	if logName == "" && strings.TrimSpace(agent.WorktreePath) != "" {
		logName = strings.TrimSpace(filepath.Base(filepath.Clean(agent.WorktreePath)))
	}
	if logName == "" && agent.IssueNumber > 0 {
		logName = fmt.Sprintf("issue-%d", agent.IssueNumber)
	}
	if logName == "" {
		return "", errors.New("cannot derive runtime log file name")
	}

	safeName := sanitizeSessionPart(logName)
	if safeName == "" {
		return "", fmt.Errorf("cannot derive safe runtime log file name from %q", logName)
	}

	logDir := strings.TrimSpace(agent.LogDir)
	if logDir == "" {
		logDir = orchestratorLogDir
	}
	return filepath.Join(logDir, safeName+".log"), nil
}

func mandatoryTestLogPath(agent Agent) (string, error) {
	logName := strings.TrimSpace(agent.ID)
	if logName == "" && strings.TrimSpace(agent.WorktreePath) != "" {
		logName = strings.TrimSpace(filepath.Base(filepath.Clean(agent.WorktreePath)))
	}
	if logName == "" && agent.IssueNumber > 0 {
		logName = fmt.Sprintf("issue-%d", agent.IssueNumber)
	}
	if logName == "" {
		return "", errors.New("cannot derive mandatory test log file name")
	}

	safeName := sanitizeSessionPart(logName)
	if safeName == "" {
		return "", fmt.Errorf("cannot derive safe mandatory test log file name from %q", logName)
	}

	logDir := strings.TrimSpace(agent.LogDir)
	if logDir == "" {
		logDir = orchestratorLogDir
	}
	return filepath.Join(logDir, safeName+"-gate.log"), nil
}

func ensureRuntimeLogFile(logPath string) error {
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

type runtimeLogFile struct {
	path    string
	modTime time.Time
}

// reviewWorkerLogFilePrefix identifies a review worker's runtime log by
// filename (see review_worker.go's ownerID derivation, "review-worker-"+
// trace). Only review workers mint a genuinely new, one-off log file per
// session: a coder or reviewer mints its agent ID once at initial launch,
// and every later restart reuses that same ID -- and therefore the same
// log path, opened in append mode -- so it never accumulates additional
// files the way a burst of short-lived review workers does.
const reviewWorkerLogFilePrefix = "review-worker-"

func pruneRuntimeLogsForNewSession(logPath string, maxTotalLogs int) error {
	if maxTotalLogs <= 0 {
		return nil
	}
	// Scope the cap to review workers only. Applying it to
	// every session start regardless of role made it a cap on the whole
	// shared log directory across every agent the orchestrator is
	// running: a single review round can start well over ten worker
	// sessions in a burst, and each one re-running this prune could -- and
	// in production did -- delete a completely unrelated, still-active
	// coder's or reviewer's own log purely because enough newer worker
	// logs had been created around it in the meantime.
	if !strings.HasPrefix(filepath.Base(logPath), reviewWorkerLogFilePrefix) {
		return nil
	}
	return pruneRuntimeLogFiles(filepath.Dir(logPath), logPath, maxTotalLogs-1)
}

func pruneRuntimeLogFiles(logDir string, preservePath string, maxLogs int) error {
	logDir = strings.TrimSpace(logDir)
	if logDir == "" || maxLogs < 0 {
		return nil
	}

	entries, err := os.ReadDir(logDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	preservePath = filepath.Clean(strings.TrimSpace(preservePath))
	candidates := make([]runtimeLogFile, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".log") || name == orchestratorDaemonLogName {
			continue
		}
		// Defense in depth alongside pruneRuntimeLogsForNewSession's own
		// guard above: even if this is ever called directly, never treat a
		// coder's or reviewer's own log as a pruning candidate.
		if !strings.HasPrefix(name, reviewWorkerLogFilePrefix) {
			continue
		}
		fullPath := filepath.Join(logDir, name)
		if preservePath != "" && filepath.Clean(fullPath) == preservePath {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		candidates = append(candidates, runtimeLogFile{path: fullPath, modTime: info.ModTime()})
	}
	if len(candidates) <= maxLogs {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].path > candidates[j].path
		}
		return candidates[i].modTime.After(candidates[j].modTime)
	})
	for _, stale := range candidates[maxLogs:] {
		if err := os.Remove(stale.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func shellSingleQuote(text string) string {
	if text == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(text, "'", "'\\''") + "'"
}

func sanitizeSessionPart(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteRune('-')
	}
	return strings.Trim(b.String(), "-")
}

func isTmuxSessionMissing(err error) bool {
	type exitCoder interface {
		ExitCode() int
	}
	var ec exitCoder
	return errors.As(err, &ec) && ec.ExitCode() == 1
}

func isTmuxServerExitedUnexpectedly(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "server exited unexpectedly")
}
