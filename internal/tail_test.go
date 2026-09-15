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
	"testing"
)

type fakeTailInputResult struct {
	b   byte
	err error
}

type fakeTailInputSource struct {
	results []fakeTailInputResult
	closed  bool
}

func (f *fakeTailInputSource) NextByte() (byte, error) {
	if len(f.results) == 0 {
		return 0, errTailInputUnavailable
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result.b, result.err
}

func (f *fakeTailInputSource) Close() error {
	f.closed = true
	return nil
}

func TestWaitForInteractiveTailExitCtrlCStopsTail(t *testing.T) {
	done := make(chan error, 1)
	input := &fakeTailInputSource{
		results: []fakeTailInputResult{
			{b: 3},
		},
	}

	var output bytes.Buffer
	interrupts := 0
	err := waitForInteractiveTailExit("/tmp/repository-agent-orchestrator/agent-17.log", &output, input, done, func() error {
		interrupts++
		done <- nil
		return nil
	})
	if err != nil {
		t.Fatalf("waitForInteractiveTailExit() error = %v", err)
	}
	if interrupts != 1 {
		t.Fatalf("interrupt count = %d, want 1", interrupts)
	}
	if !input.closed {
		t.Fatal("waitForInteractiveTailExit() did not close the input source")
	}
	if got := output.String(); got != "^C\r\n" {
		t.Fatalf("interrupt output = %q, want %q", got, "^C\r\n")
	}
}

func TestCarriageReturnNormalizingWriterAddsCarriageReturns(t *testing.T) {
	var output bytes.Buffer
	writer := &carriageReturnNormalizingWriter{writer: &output}

	written, err := writer.Write([]byte("alpha\nbeta\r\ngamma\n"))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written != len("alpha\nbeta\r\ngamma\n") {
		t.Fatalf("Write() bytes = %d, want %d", written, len("alpha\nbeta\r\ngamma\n"))
	}

	want := "alpha\r\nbeta\r\ngamma\r\n"
	if got := output.String(); got != want {
		t.Fatalf("normalized output = %q, want %q", got, want)
	}
}
