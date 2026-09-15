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
	"os/exec"
	"path/filepath"
	"testing"
)

func TestParseRemoteRepoSlug(t *testing.T) {
	tests := []struct {
		name   string
		remote string
		owner  string
		repo   string
		ok     bool
	}{
		{
			name:   "https",
			remote: "https://github.com/acme/widget.git",
			owner:  "acme",
			repo:   "widget",
			ok:     true,
		},
		{
			name:   "ssh",
			remote: "ssh://git@github.com/acme/widget.git",
			owner:  "acme",
			repo:   "widget",
			ok:     true,
		},
		{
			name:   "scp style",
			remote: "git@github.com:acme/widget.git",
			owner:  "acme",
			repo:   "widget",
			ok:     true,
		},
		{
			name:   "plain owner repo path",
			remote: "acme/widget",
			owner:  "acme",
			repo:   "widget",
			ok:     true,
		},
		{
			name:   "invalid",
			remote: "not-a-repo",
			ok:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repo, ok := parseRemoteRepoSlug(tt.remote)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if owner != tt.owner {
				t.Fatalf("owner = %q, want %q", owner, tt.owner)
			}
			if repo != tt.repo {
				t.Fatalf("repo = %q, want %q", repo, tt.repo)
			}
		})
	}
}

func TestRepoPathMatchesTargetRepo(t *testing.T) {
	ctx := context.Background()
	repoPath := t.TempDir()

	run := func(name string, args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, name, args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("command failed: %s %v: %v: %s", name, args, err, string(out))
		}
	}

	run("git", "-C", repoPath, "init")
	run("git", "-C", repoPath, "remote", "add", "origin", "git@github.com:acme/widget.git")

	match, actual, err := repoPathMatchesTargetRepo(ctx, repoPath, "acme", "widget")
	if err != nil {
		t.Fatalf("repoPathMatchesTargetRepo() error = %v", err)
	}
	if !match {
		t.Fatalf("match = false, want true (actual %q)", actual)
	}

	match, actual, err = repoPathMatchesTargetRepo(ctx, repoPath, "acme", "different")
	if err != nil {
		t.Fatalf("repoPathMatchesTargetRepo() mismatch check error = %v", err)
	}
	if match {
		t.Fatalf("match = true, want false")
	}
	if actual != "acme/widget" {
		t.Fatalf("actual = %q, want %q", actual, "acme/widget")
	}
}

func TestRepoPathMatchesTargetRepoMissingOrigin(t *testing.T) {
	ctx := context.Background()
	repoPath := t.TempDir()

	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "init")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init failed: %v: %s", err, string(out))
	}

	_, _, err := repoPathMatchesTargetRepo(ctx, repoPath, "acme", "widget")
	if err == nil {
		t.Fatal("expected error when origin remote is missing")
	}
}

func TestOriginRepoSlugNormalizesCase(t *testing.T) {
	ctx := context.Background()
	repoPath := t.TempDir()

	run := func(name string, args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, name, args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("command failed: %s %v: %v: %s", name, args, err, string(out))
		}
	}

	run("git", "-C", repoPath, "init")
	run("git", "-C", repoPath, "remote", "add", "origin", "https://github.com/AcMe/WIDGET.git")

	slug, err := originRepoSlug(ctx, filepath.Clean(repoPath))
	if err != nil {
		t.Fatalf("originRepoSlug() error = %v", err)
	}
	if slug != "acme/widget" {
		t.Fatalf("slug = %q, want %q", slug, "acme/widget")
	}
}
