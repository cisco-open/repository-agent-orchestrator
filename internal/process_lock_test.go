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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const processLockHelperEnv = "ORCHESTRATOR_LOCK_HELPER"

func TestAcquireProcessLockBlocksConcurrentProcess(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "instance.lock")
	readyPath := filepath.Join(t.TempDir(), "ready")

	cmd := exec.Command(os.Args[0], "-test.run=TestProcessLockHelper", "--", lockPath, readyPath)
	cmd.Env = append(os.Environ(), processLockHelperEnv+"=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start helper process: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper did not signal readiness within timeout")
		}
		time.Sleep(25 * time.Millisecond)
	}

	lock, err := acquireProcessLock(lockPath)
	if err == nil {
		_ = lock.Release()
		t.Fatalf("expected lock acquisition to fail while helper process holds lock")
	}
	if !strings.Contains(err.Error(), "already holds lock file") {
		t.Fatalf("lock error = %q, want message mentioning existing lock holder", err.Error())
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("failed to kill helper process: %v", err)
	}
	if err := cmd.Wait(); err != nil && !strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("helper wait error = %v", err)
	}

	lock, err = acquireProcessLock(lockPath)
	if err != nil {
		t.Fatalf("expected lock acquisition to succeed after helper exits: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestProcessLockHelper(t *testing.T) {
	if os.Getenv(processLockHelperEnv) != "1" {
		t.Skip("helper process test")
	}
	if len(os.Args) < 3 {
		t.Fatalf("missing helper arguments")
	}
	lockPath := os.Args[len(os.Args)-2]
	readyPath := os.Args[len(os.Args)-1]

	lock, err := acquireProcessLock(lockPath)
	if err != nil {
		t.Fatalf("helper failed to acquire lock: %v", err)
	}
	defer func() {
		_ = lock.Release()
	}()
	if err := os.WriteFile(readyPath, []byte("ready"), 0o644); err != nil {
		t.Fatalf("helper failed to write ready file: %v", err)
	}
	time.Sleep(30 * time.Second)
}
