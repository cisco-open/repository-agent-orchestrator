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
	"strings"
	"testing"
)

func TestBuildVersionStringIsSafeAndNonEmpty(t *testing.T) {
	version := buildVersionString()
	if strings.TrimSpace(version) == "" {
		t.Fatal("buildVersionString() = empty string")
	}
	if !strings.HasPrefix(version, "repository-agent-orchestrator") {
		t.Fatalf("buildVersionString() = %q, want it to identify the binary", version)
	}
	// go test does not build via `go build`, so VCS stamping is normally
	// unavailable here; either the "version unknown" fallback or a real
	// commit= line is acceptable, but it must never be empty or panic.
	if !strings.Contains(version, "commit=") &&
		!strings.Contains(version, "version unknown") {
		t.Fatalf("buildVersionString() = %q, want commit= or a clear fallback", version)
	}
}

// TestFormatBuildVersionStringPreservesFullCommit is the regression test
// for independent review feedback: this used to truncate the revision to
// 12 characters, but docs/TROUBLESHOOTING.md documents version.txt as
// containing "the running build's exact commit" -- a short hash is not
// exact, so the full revision must be preserved.
func TestFormatBuildVersionStringPreservesFullCommit(t *testing.T) {
	fullSHA := "0123456789abcdef0123456789abcdef01234567"
	info := buildInfo{
		Revision:  fullSHA,
		Time:      "2026-01-01T00:00:00Z",
		GoVersion: "go1.99",
	}
	version := formatBuildVersionString(info, true)
	if !strings.Contains(version, "commit="+fullSHA) {
		t.Fatalf("formatBuildVersionString() = %q, want the full untruncated commit %q", version, fullSHA)
	}
}
