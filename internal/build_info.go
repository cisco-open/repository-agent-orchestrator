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
	"fmt"
	"runtime/debug"
)

// buildInfo is serviceability-101 material: every deployed binary must be
// able to report exactly what source commit it was built from, without
// requiring the operator to know (or trust) which checkout produced it. Go
// embeds this automatically for binaries built with the standard toolchain
// from a VCS checkout (`go build`, no ldflags required), via
// runtime/debug.ReadBuildInfo. It is unavailable only for binaries built
// with VCS stamping explicitly disabled (`-buildvcs=false`) or built outside
// any recognized VCS checkout.
type buildInfo struct {
	Revision  string
	Time      string
	Modified  bool
	GoVersion string
}

func readBuildInfo() (buildInfo, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return buildInfo{}, false
	}
	result := buildInfo{GoVersion: info.GoVersion}
	found := false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			result.Revision = setting.Value
			found = true
		case "vcs.time":
			result.Time = setting.Value
		case "vcs.modified":
			result.Modified = setting.Value == "true"
		}
	}
	return result, found
}

// buildVersionString is safe to print to stdout/stderr/logs: it contains no
// repository configuration, credentials, or user data, only VCS metadata
// about the binary itself.
func buildVersionString() string {
	info, ok := readBuildInfo()
	return formatBuildVersionString(info, ok)
}

// formatBuildVersionString is buildVersionString's pure formatting step,
// split out so it's testable without needing a real `go build`-produced
// binary's VCS stamping (readBuildInfo's result is normally unavailable
// under `go test`).
func formatBuildVersionString(info buildInfo, ok bool) string {
	if !ok {
		return "repository-agent-orchestrator version unknown " +
			"(binary was not built with VCS stamping; rebuild with " +
			"`go build` from a git checkout, not `-buildvcs=false`)"
	}
	// Deliberately not truncated: this is a forensic/serviceability value
	// (docs/TROUBLESHOOTING.md documents version.txt as "the running
	// build's exact commit"), and a short hash is not exact -- git's
	// abbreviation length is usually, but not always, unambiguous.
	revision := info.Revision
	if revision == "" {
		revision = "unknown"
	}
	dirty := ""
	if info.Modified {
		dirty = "-dirty"
	}
	buildTime := info.Time
	if buildTime == "" {
		buildTime = "unknown"
	}
	return fmt.Sprintf(
		"repository-agent-orchestrator commit=%s%s built=%s go=%s",
		revision,
		dirty,
		buildTime,
		info.GoVersion,
	)
}
