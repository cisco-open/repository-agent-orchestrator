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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestOSCommandExecutorRunAndOutput(t *testing.T) {
	exec := osCommandExecutor{}

	if err := exec.Run("sh", "-c", "exit 0"); err != nil {
		t.Fatalf("Run(success) error = %v", err)
	}

	if err := exec.Run("sh", "-c", "exit 1"); err == nil {
		t.Fatal("Run(exit 1) error = nil, want error")
	}

	err := exec.Run("sh", "-c", "echo boom >&2; exit 1")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Run(stderr failure) error = %v, want stderr in error", err)
	}

	out, err := exec.Output("sh", "-c", "printf 'ready'")
	if err != nil {
		t.Fatalf("Output(success) error = %v", err)
	}
	if string(out) != "ready" {
		t.Fatalf("Output(success) = %q, want %q", string(out), "ready")
	}

	_, err = exec.Output("sh", "-c", "echo output-fail >&2; exit 1")
	if err == nil || !strings.Contains(err.Error(), "output-fail") {
		t.Fatalf("Output(stderr failure) error = %v, want stderr in error", err)
	}
}

func TestOSCommandExecutorRedactsReviewWorkerEnvironmentOnTmuxCreationFailure(
	t *testing.T,
) {
	fakeTmux := filepath.Join(t.TempDir(), "tmux")
	if err := os.WriteFile(
		fakeTmux,
		[]byte("#!/bin/sh\nprintf '%s' \"$*\" >&2\nexit 1\n"),
		0o755,
	); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", fakeTmux, err)
	}
	environment := []string{
		"HOME=/sensitive/home",
		"PATH=/sensitive/bin",
		"TMPDIR=/sensitive/tmp",
	}
	isolatedShell := reviewWorkerIsolatedShellCommand(environment)

	err := (osCommandExecutor{}).Run(
		fakeTmux,
		"new-session",
		"-d",
		"-s",
		"review-worker",
		"-c",
		"/safe/worktree",
		isolatedShell,
	)
	if err == nil {
		t.Fatal("Run(tmux creation failure) error = nil, want error")
	}
	loggable := err.Error()
	for _, entry := range environment {
		if strings.Contains(loggable, entry) {
			t.Fatalf("loggable tmux error leaked environment assignment %q", entry)
		}
	}
	for _, value := range []string{
		"/sensitive/home",
		"/sensitive/bin",
		"/sensitive/tmp",
	} {
		if strings.Contains(loggable, value) {
			t.Fatalf("loggable tmux error leaked environment value %q", value)
		}
	}
	if !strings.Contains(loggable, reviewWorkerEnvironmentErrorRedaction) {
		t.Fatalf("loggable tmux error = %q, want environment redaction marker", loggable)
	}
	if !strings.Contains(loggable, "new-session") {
		t.Fatalf("loggable tmux error = %q, want safe command context", loggable)
	}
}

func TestOSCommandExecutorRedactsPartialReviewWorkerEnvironmentStderr(
	t *testing.T,
) {
	environment := []string{
		"HOME=/sensitive/home",
		"PATH=/sensitive/bin",
	}
	isolatedShell := reviewWorkerIsolatedShellCommand(environment)
	tests := []struct {
		name   string
		script string
		leak   string
	}{
		{
			name:   "assignment",
			script: "#!/bin/sh\nprintf 'HOME=/sensitive/home' >&2\nexit 1\n",
			leak:   "HOME=/sensitive/home",
		},
		{
			name:   "value",
			script: "#!/bin/sh\nprintf '/sensitive/bin' >&2\nexit 1\n",
			leak:   "/sensitive/bin",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeTmux := filepath.Join(t.TempDir(), "tmux")
			if err := os.WriteFile(
				fakeTmux,
				[]byte(test.script),
				0o755,
			); err != nil {
				t.Fatalf("WriteFile(%s) error = %v", fakeTmux, err)
			}

			err := (osCommandExecutor{}).Run(
				fakeTmux,
				"new-session",
				"-d",
				"-s",
				"review-worker",
				isolatedShell,
			)
			if err == nil {
				t.Fatal("Run(tmux creation failure) error = nil, want error")
			}
			loggable := err.Error()
			if strings.Contains(loggable, test.leak) {
				t.Fatalf(
					"loggable tmux error leaked partial environment %q",
					test.leak,
				)
			}
			if !strings.Contains(
				loggable,
				reviewWorkerEnvironmentErrorRedaction,
			) {
				t.Fatalf(
					"loggable tmux error = %q, want environment redaction marker",
					loggable,
				)
			}
		})
	}
}

func TestOSCommandExecutorRunStripsDisabledMallocStackLoggingOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin-specific malloc logging behavior")
	}

	t.Setenv("MallocStackLogging", "0")
	t.Setenv("MallocStackLoggingNoCompact", "false")
	exec := osCommandExecutor{}

	if err := exec.Run("sh", "-c", "test -z \"$MallocStackLogging\" -a -z \"$MallocStackLoggingNoCompact\""); err != nil {
		t.Fatalf("Run(darwin malloc env sanitization) error = %v", err)
	}
}
