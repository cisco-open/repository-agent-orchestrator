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
	"slices"
	"strings"
	"testing"
)

func TestSanitizeCommandEnvEntriesDarwinStripsDisabledMallocStackLogging(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"MallocStackLogging=0",
		"MallocStackLoggingNoCompact=false",
		"MallocNanoZone=0",
	}

	got := sanitizeCommandEnvEntries(env, "darwin")
	if slices.Contains(got, "MallocStackLogging=0") {
		t.Fatalf("sanitizeCommandEnvEntries() retained disabled MallocStackLogging: %v", got)
	}
	if slices.Contains(got, "MallocStackLoggingNoCompact=false") {
		t.Fatalf("sanitizeCommandEnvEntries() retained disabled MallocStackLoggingNoCompact: %v", got)
	}
	if !slices.Contains(got, "PATH=/usr/bin") {
		t.Fatalf("sanitizeCommandEnvEntries() dropped unrelated PATH entry: %v", got)
	}
	if !slices.Contains(got, "MallocNanoZone=0") {
		t.Fatalf("sanitizeCommandEnvEntries() dropped unrelated malloc entry: %v", got)
	}
}

func TestSanitizeCommandEnvEntriesDarwinKeepsEnabledMallocStackLogging(t *testing.T) {
	env := []string{
		"MallocStackLogging=1",
		"MallocStackLoggingNoCompact=yes",
	}

	got := sanitizeCommandEnvEntries(env, "darwin")
	if !slices.Contains(got, "MallocStackLogging=1") {
		t.Fatalf("sanitizeCommandEnvEntries() dropped enabled MallocStackLogging: %v", got)
	}
	if !slices.Contains(got, "MallocStackLoggingNoCompact=yes") {
		t.Fatalf("sanitizeCommandEnvEntries() dropped enabled MallocStackLoggingNoCompact: %v", got)
	}
}

func TestSanitizeCommandEnvEntriesNonDarwinNoFiltering(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"MallocStackLogging=0",
	}

	got := sanitizeCommandEnvEntries(env, "linux")
	if !slices.Equal(got, env) {
		t.Fatalf("sanitizeCommandEnvEntries() = %v, want %v", got, env)
	}
}

func TestNonInteractiveGitEnvEntriesDisablesPrompts(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"GIT_TERMINAL_PROMPT=1",
		"GIT_ASKPASS=/usr/bin/some-gui-prompt",
		"SSH_ASKPASS=/usr/bin/some-gui-prompt",
	}

	got := nonInteractiveGitEnvEntries(env)

	want := map[string]string{
		"GIT_TERMINAL_PROMPT": "0",
		"GIT_ASKPASS":         "",
		"SSH_ASKPASS":         "",
		"GIT_SSH_COMMAND":     "ssh -o BatchMode=yes",
	}
	seen := make(map[string]string, len(got))
	for _, entry := range got {
		key, value, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		if _, dup := seen[key]; dup {
			t.Fatalf("nonInteractiveGitEnvEntries() duplicated key %q: %v", key, got)
		}
		seen[key] = value
	}
	for key, value := range want {
		if seen[key] != value {
			t.Fatalf("nonInteractiveGitEnvEntries()[%s] = %q, want %q (full env: %v)", key, seen[key], value, got)
		}
	}
	if !slices.Contains(got, "PATH=/usr/bin") {
		t.Fatalf("nonInteractiveGitEnvEntries() dropped unrelated PATH entry: %v", got)
	}
}

func TestNonInteractiveGitEnvEntriesPreservesCustomSSHCommand(t *testing.T) {
	env := []string{
		"GIT_SSH_COMMAND=ssh -i /opt/deploy/id_ed25519 -p 2222",
	}

	got := nonInteractiveGitEnvEntries(env)

	want := "ssh -i /opt/deploy/id_ed25519 -p 2222 -o BatchMode=yes"
	found := false
	for _, entry := range got {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key == "GIT_SSH_COMMAND" {
			found = true
			if value != want {
				t.Fatalf("GIT_SSH_COMMAND = %q, want %q", value, want)
			}
		}
	}
	if !found {
		t.Fatal("nonInteractiveGitEnvEntries() dropped GIT_SSH_COMMAND")
	}
}

// TestNonInteractiveGitEnvEntriesNormalizesExistingBatchModeToYes is the
// regression test for review feedback: OpenSSH honors
// the first "-o BatchMode=<value>" it sees on the command line, so
// merely appending "-o BatchMode=yes" after an existing
// "-o BatchMode=no" does not override it -- ssh -G confirms
// "-o BatchMode=no -o BatchMode=yes" still resolves to "batchmode no".
// The effective value must always be "yes" regardless of what a
// pre-existing GIT_SSH_COMMAND already specified.
func TestNonInteractiveGitEnvEntriesNormalizesExistingBatchModeToYes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing string
		want     string
	}{
		{
			name:     "spaced option set to no",
			existing: "ssh -o BatchMode=no",
			want:     "ssh -o BatchMode=yes",
		},
		{
			name:     "compact option set to no",
			existing: "ssh -oBatchMode=no",
			want:     "ssh -o BatchMode=yes",
		},
		{
			name:     "existing option already yes",
			existing: "ssh -o BatchMode=yes",
			want:     "ssh -o BatchMode=yes",
		},
		{
			name:     "batchmode option alongside other flags",
			existing: "ssh -i /opt/deploy/id_ed25519 -o BatchMode=no -p 2222",
			want:     "ssh -i /opt/deploy/id_ed25519 -p 2222 -o BatchMode=yes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := nonInteractiveGitEnvEntries([]string{"GIT_SSH_COMMAND=" + tc.existing})
			found := false
			for _, entry := range got {
				key, value, ok := strings.Cut(entry, "=")
				if ok && key == "GIT_SSH_COMMAND" {
					found = true
					if value != tc.want {
						t.Fatalf("GIT_SSH_COMMAND = %q, want %q", value, tc.want)
					}
					if strings.Count(value, "BatchMode=") != 1 {
						t.Fatalf("GIT_SSH_COMMAND = %q, want exactly one BatchMode option", value)
					}
				}
			}
			if !found {
				t.Fatal("nonInteractiveGitEnvEntries() dropped GIT_SSH_COMMAND")
			}
		})
	}
}

func TestCommandEnvOnlyHardensGit(t *testing.T) {
	t.Setenv("GIT_TERMINAL_PROMPT", "sentinel")

	if !slices.Contains(commandEnv("git"), "GIT_TERMINAL_PROMPT=0") {
		t.Fatal("commandEnv(\"git\") does not disable interactive git prompts")
	}
	if !slices.Contains(commandEnv("make"), "GIT_TERMINAL_PROMPT=sentinel") {
		t.Fatal("commandEnv(\"make\") unexpectedly altered an ambient, unrelated GIT_TERMINAL_PROMPT")
	}
}
