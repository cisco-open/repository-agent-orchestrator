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
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimeLogSanitizerWriterStripsTerminalControlSequences(t *testing.T) {
	var dst bytes.Buffer
	writer := newRuntimeLogSanitizerWriter(&dst)

	chunks := [][]byte{
		[]byte("hello\a"),
		[]byte("\x1b[31mred"),
		[]byte("\x1b[0m\r\n"),
		[]byte("\x1b]0;title\a"),
		[]byte("plain"),
		[]byte("\x1bPignored"),
		[]byte("\x1b\\done"),
		[]byte("\x7f"),
	}
	for _, chunk := range chunks {
		if _, err := writer.Write(chunk); err != nil {
			t.Fatalf("Write(%q) error = %v", chunk, err)
		}
	}

	if got, want := dst.String(), "hellored\nplaindone"; got != want {
		t.Fatalf("sanitized log = %q, want %q", got, want)
	}
}

func TestRunRuntimeLogSanitizerSubcommandWritesSafeLogFile(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "runtime.log")

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	oldStdin := os.Stdin
	os.Stdin = inR
	defer func() { os.Stdin = oldStdin }()

	if _, err := inW.Write([]byte("start\a\x1b[32mok\x1b[0m\r\n\x1b]0;title\aend")); err != nil {
		t.Fatalf("Write(stdin) error = %v", err)
	}
	if err := inW.Close(); err != nil {
		t.Fatalf("stdin close error = %v", err)
	}
	defer inR.Close()

	stdout, stderr, code := captureStdoutStderr(t, func() int {
		return Run(context.Background(), []string{"repository-agent-orchestrator", runtimeLogSanitizerSubcommand, logPath})
	})
	if code != 0 {
		t.Fatalf("Run(runtime log sanitizer) code = %d, want 0\nstdout=%q\nstderr=%q", code, stdout, stderr)
	}
	if stdout != "" {
		t.Fatalf("Run(runtime log sanitizer) stdout = %q, want empty", stdout)
	}
	if stderr != "" {
		t.Fatalf("Run(runtime log sanitizer) stderr = %q, want empty", stderr)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", logPath, err)
	}
	if got, want := string(data), "startok\nend"; got != want {
		t.Fatalf("sanitized file = %q, want %q", got, want)
	}
}
