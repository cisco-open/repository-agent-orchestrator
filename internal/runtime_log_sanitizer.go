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
	"fmt"
	"io"
	"os"
	"strings"
)

const runtimeLogSanitizerSubcommand = "__sanitize-runtime-log"

type runtimeLogSanitizerState uint8

const (
	runtimeLogStateNormal runtimeLogSanitizerState = iota
	runtimeLogStateEscape
	runtimeLogStateCSI
	runtimeLogStateOSC
	runtimeLogStateOSCEscape
	runtimeLogStateString
	runtimeLogStateStringEscape
)

type runtimeLogSanitizerWriter struct {
	dst   io.Writer
	state runtimeLogSanitizerState
	sawCR bool
}

func newRuntimeLogSanitizerWriter(dst io.Writer) *runtimeLogSanitizerWriter {
	return &runtimeLogSanitizerWriter{dst: dst}
}

func (w *runtimeLogSanitizerWriter) Write(p []byte) (int, error) {
	if w == nil || w.dst == nil {
		return len(p), nil
	}

	var out bytes.Buffer
	for _, b := range p {
		if w.sawCR {
			if b == '\n' {
				w.sawCR = false
				continue
			}
			w.sawCR = false
		}

		switch w.state {
		case runtimeLogStateNormal:
			switch b {
			case 0x1b:
				w.state = runtimeLogStateEscape
			case '\a':
				continue
			case '\r':
				out.WriteByte('\n')
				w.sawCR = true
			case '\n', '\t':
				out.WriteByte(b)
			default:
				if b >= 0x20 && b != 0x7f {
					out.WriteByte(b)
				}
			}
		case runtimeLogStateEscape:
			switch b {
			case '[':
				w.state = runtimeLogStateCSI
			case ']':
				w.state = runtimeLogStateOSC
			case 'P', 'X', '^', '_':
				w.state = runtimeLogStateString
			default:
				w.state = runtimeLogStateNormal
			}
		case runtimeLogStateCSI:
			if b >= 0x40 && b <= 0x7e {
				w.state = runtimeLogStateNormal
			}
		case runtimeLogStateOSC:
			switch b {
			case '\a':
				w.state = runtimeLogStateNormal
			case 0x1b:
				w.state = runtimeLogStateOSCEscape
			}
		case runtimeLogStateOSCEscape:
			if b == '\\' {
				w.state = runtimeLogStateNormal
			} else {
				w.state = runtimeLogStateOSC
			}
		case runtimeLogStateString:
			if b == 0x1b {
				w.state = runtimeLogStateStringEscape
			}
		case runtimeLogStateStringEscape:
			if b == '\\' {
				w.state = runtimeLogStateNormal
			} else {
				w.state = runtimeLogStateString
			}
		}
	}

	if out.Len() == 0 {
		return len(p), nil
	}
	if _, err := w.dst.Write(out.Bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func sanitizeRuntimeLogStream(src io.Reader, dst io.Writer) error {
	if src == nil {
		return fmt.Errorf("runtime log sanitizer source is nil")
	}
	if dst == nil {
		return fmt.Errorf("runtime log sanitizer destination is nil")
	}

	sanitizer := newRuntimeLogSanitizerWriter(dst)
	if _, err := io.CopyBuffer(sanitizer, src, make([]byte, 32*1024)); err != nil {
		return err
	}
	return nil
}

func runtimeLogPipeCommand(logPath string) (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("failed to determine orchestrator executable: %w", err)
	}
	return strings.Join([]string{
		shellSingleQuote(executable),
		shellSingleQuote(runtimeLogSanitizerSubcommand),
		shellSingleQuote(logPath),
	}, " "), nil
}

func runRuntimeLogSanitizer(args []string) int {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		fmt.Fprintln(os.Stderr, "runtime log sanitizer error: missing required <log-path> argument")
		return 1
	}

	logPath := strings.TrimSpace(args[0])
	if err := ensureRuntimeLogFile(logPath); err != nil {
		fmt.Fprintf(os.Stderr, "runtime log sanitizer error: %s\n", err.Error())
		return 1
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runtime log sanitizer error: %s\n", err.Error())
		return 1
	}
	defer file.Close()

	if err := sanitizeRuntimeLogStream(os.Stdin, file); err != nil {
		fmt.Fprintf(os.Stderr, "runtime log sanitizer error: %s\n", err.Error())
		return 1
	}
	return 0
}
