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
	"os"
	"os/exec"
	"runtime"
	"strings"
)

func newCommand(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	cmd.Env = commandEnv(name)
	return cmd
}

func newCommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = commandEnv(name)
	return cmd
}

func commandEnv(name string) []string {
	env := sanitizedCommandEnv()
	if name == "git" {
		env = nonInteractiveGitEnvEntries(env)
	}
	return env
}

func sanitizedCommandEnv() []string {
	return sanitizeCommandEnvEntries(os.Environ(), runtime.GOOS)
}

// nonInteractiveGitEnvEntries overrides the git/ssh credential-prompt
// variables so a `git` subprocess fails fast instead of hanging forever
// waiting on a TTY that will never answer (e.g. "Enter passphrase for key
// ...", an SSH host-key confirmation, or an HTTPS username/password
// prompt). This matters most right after something disrupts the ambient
// credential setup (an expired ssh-agent, a dead credential cache after a
// crash/restart) -- exactly when nothing is watching a terminal to answer
// the prompt.
//
// GIT_SSH_COMMAND is preserved if already customized (e.g. a pinned
// identity file or port) and only gets `-o BatchMode=yes` appended, so SSH
// itself refuses to prompt rather than blocking.
func nonInteractiveGitEnvEntries(env []string) []string {
	overrides := map[string]string{
		"GIT_TERMINAL_PROMPT": "0",
		"GIT_ASKPASS":         "",
		"SSH_ASKPASS":         "",
		"GIT_SSH_COMMAND":     nonInteractiveGitSSHCommand(env),
	}
	filtered := make([]string, 0, len(env)+len(overrides))
	for _, entry := range env {
		key, _, found := strings.Cut(entry, "=")
		if found {
			if _, overridden := overrides[key]; overridden {
				continue
			}
		}
		filtered = append(filtered, entry)
	}
	for key, value := range overrides {
		filtered = append(filtered, key+"="+value)
	}
	return filtered
}

// nonInteractiveGitSSHCommand appends "-o BatchMode=yes" to an existing
// GIT_SSH_COMMAND, replacing rather than merely supplementing any prior
// BatchMode option. OpenSSH honors the first "-o BatchMode=<value>" it
// sees on the command line, so a stale or attacker/operator-supplied
// "-o BatchMode=no" earlier in the string would otherwise silently
// survive appending "-o BatchMode=yes" after it -- defeating the whole
// fail-fast guarantee this function exists for. Stripping every
// existing BatchMode option first, so the appended one is unambiguously
// the only (and therefore effective) one, closes that gap.
func nonInteractiveGitSSHCommand(env []string) string {
	existing := ""
	for _, entry := range env {
		key, value, found := strings.Cut(entry, "=")
		if found && key == "GIT_SSH_COMMAND" {
			existing = strings.TrimSpace(value)
		}
	}
	if existing == "" {
		existing = "ssh"
	}
	existing = strings.TrimSpace(stripSSHBatchModeOptions(existing))
	if existing == "" {
		existing = "ssh"
	}
	return existing + " -o BatchMode=yes"
}

// stripSSHBatchModeOptions removes every "-o BatchMode=<value>" and
// "-oBatchMode=<value>" token from a raw ssh command-line string,
// tolerating either spacing.
func stripSSHBatchModeOptions(command string) string {
	fields := strings.Fields(command)
	kept := make([]string, 0, len(fields))
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		switch {
		case strings.HasPrefix(field, "-oBatchMode="):
			continue
		case field == "-o" && i+1 < len(fields) &&
			strings.HasPrefix(fields[i+1], "BatchMode="):
			i++
			continue
		default:
			kept = append(kept, field)
		}
	}
	return strings.Join(kept, " ")
}

func sanitizeCommandEnvEntries(env []string, goos string) []string {
	if goos != "darwin" {
		return append([]string(nil), env...)
	}

	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		key, value, found := strings.Cut(entry, "=")
		if !found {
			filtered = append(filtered, entry)
			continue
		}
		if shouldDropDarwinMallocEnv(key, value) {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func shouldDropDarwinMallocEnv(key, value string) bool {
	switch key {
	case "MallocStackLogging", "MallocStackLoggingNoCompact":
	default:
		return false
	}

	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "", "0", "false", "no", "off":
		return true
	default:
		return false
	}
}
