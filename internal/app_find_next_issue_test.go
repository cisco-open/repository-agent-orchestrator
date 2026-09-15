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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFindNextOpenIssueSkipsActiveIssues(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{
			{"number": 1, "title": "issue one"},
			{"number": 2, "title": "issue two"},
			{"number": 5, "title": "issue five"},
		})
	}))
	defer srv.Close()

	ghClient := mustNewGitHubClientForTest(t, srv.Client(), srv.URL+"/")

	agents := NewAgentManager()
	if err := agents.Add(testAgent("agent-1", 1)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
		github: ghClient,
		agents: agents,
	}

	issueNumber, title, err := bot.FindNextOpenIssue(context.Background())
	if err != nil {
		t.Fatalf("FindNextOpenIssue() error = %v", err)
	}
	if issueNumber != 2 {
		t.Fatalf("issueNumber = %d, want 2", issueNumber)
	}
	if title != "issue two" {
		t.Fatalf("title = %q, want %q", title, "issue two")
	}
}

func TestFindNextOpenIssueReturnsErrorWhenOnlyActiveIssuesRemain(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/widget/issues" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]interface{}{
			{"number": 9, "title": "issue nine"},
		})
	}))
	defer srv.Close()

	ghClient := mustNewGitHubClientForTest(t, srv.Client(), srv.URL+"/")

	agents := NewAgentManager()
	if err := agents.Add(testAgent("agent-9", 9)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
		github: ghClient,
		agents: agents,
	}

	_, _, err := bot.FindNextOpenIssue(context.Background())
	if err == nil {
		t.Fatal("expected error when all open issues are already active")
	}
	if !strings.Contains(err.Error(), "no open issues found") {
		t.Fatalf("unexpected error: %v", err)
	}
}
