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
	"sync"
	"testing"
)

func TestInitAgentForIssueUsesCodingAgentIDAndLaunchMessage(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/43":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 43,
				"title":  "Fix launch notification text",
				"body":   "Ensure runtime launch notifications include agent type.",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	var (
		mu            sync.Mutex
		notifications []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	ghClient := mustNewGitHubClientForTest(t, gh.Client(), gh.URL+"/")

	agents := NewAgentManager()
	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "stub-session"}}
	commands := make([]string, 0)
	contextWrites := make([]string, 0)
	handoffWrites := make([]string, 0)
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			RepoPath:        "/tmp/repo",
			BaseBranch:      "main",
			WorktreeDir:     "/tmp/repo/.worktrees",
			LogDir:          t.TempDir(),
			WebexWebhookURL: webex.URL,
			ReviewPolicy:    builtInReviewPolicy(),
		},
		github:   ghClient,
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		messenger: &recordingTaskMessenger{
			contexts: &contextWrites,
			handoffs: &handoffWrites,
		},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			commands = append(commands, strings.TrimSpace(dir+"::"+name+" "+strings.Join(args, " ")))
			return nil
		},
	}

	if err := bot.InitAgentForIssue(context.Background(), 43); err != nil {
		t.Fatalf("InitAgentForIssue() error = %v", err)
	}

	if got, want := len(runner.started), 1; got != want {
		t.Fatalf("runner starts = %d, want %d", got, want)
	}
	agentID := runner.started[0].ID
	if !strings.HasPrefix(agentID, "coding-agent-43-") {
		t.Fatalf("started agent ID = %q, want coding-agent prefix", agentID)
	}
	if runner.started[0].RuntimeCWD != runner.started[0].WorktreePath {
		t.Fatalf("runtime cwd = %q, want worktree path %q", runner.started[0].RuntimeCWD, runner.started[0].WorktreePath)
	}
	if got, want := runner.started[0].RuntimeProfile, builtInAgentProfile(AgentProfileRoleCoder); got != want {
		t.Fatalf("runtime profile = %+v, want %+v", got, want)
	}
	if got := len(commands); got != 2 {
		t.Fatalf("setup command count = %d, want 2", got)
	}
	if got := len(contextWrites); got != 1 {
		t.Fatalf("context write count = %d, want 1", got)
	}
	if !strings.Contains(contextWrites[0], "# Repository Agent Orchestrator Agent Context") {
		t.Fatalf("context markdown missing header: %q", contextWrites[0])
	}
	if !strings.Contains(contextWrites[0], "## Completion State") {
		t.Fatalf("context markdown missing completion state: %q", contextWrites[0])
	}
	if got := len(handoffWrites); got != 1 {
		t.Fatalf("handoff write count = %d, want 1", got)
	}
	if !strings.Contains(handoffWrites[0], "schema_version: 1") {
		t.Fatalf("handoff template missing schema version: %q", handoffWrites[0])
	}
	if !strings.Contains(handoffWrites[0], "status: in_progress") {
		t.Fatalf("handoff template missing status: %q", handoffWrites[0])
	}
	if !strings.Contains(commands[0], "git fetch --prune origin +refs/heads/main:refs/remotes/origin/main") {
		t.Fatalf("first setup command = %q, want git fetch base branch", commands[0])
	}
	if !strings.Contains(commands[1], "git worktree add -b repository-agent-orchestrator/issue-43") {
		t.Fatalf("second setup command = %q, want git worktree add branch", commands[1])
	}

	mu.Lock()
	defer mu.Unlock()
	if countMessagesContaining(notifications, "runtime launched for coding agent") != 1 {
		t.Fatalf("expected one coding runtime launch notification, got %v", notifications)
	}
	var runtimeLaunch string
	for _, msg := range notifications {
		if strings.Contains(msg, "runtime launched for coding agent") {
			runtimeLaunch = msg
			break
		}
	}
	if runtimeLaunch == "" {
		t.Fatalf("missing runtime launch notification in %v", notifications)
	}
	if !strings.Contains(runtimeLaunch, agentID) {
		t.Fatalf("runtime launch notification %q missing agent ID %q", runtimeLaunch, agentID)
	}
}

func TestContinueAgentForIssuePRLaunchesFromExistingHead(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/42":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 42,
				"title":  "Finish the in-progress change",
				"body":   "Continue from the existing pull request.",
			})
		case "/repos/acme/widget/pulls/57":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number":   57,
				"title":    "Partial implementation",
				"body":     "Closes #42",
				"state":    "open",
				"merged":   false,
				"html_url": "https://github.com/acme/widget/pull/57",
				"base":     map[string]any{"ref": "main"},
				"head": map[string]any{
					"ref": "feature/in-progress",
					"sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"repo": map[string]any{
						"full_name": "acme/widget",
						"name":      "widget",
						"owner":     map[string]any{"login": "acme"},
					},
				},
			})
		case "/repos/acme/widget/pulls/57/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 101, "body": "Existing inline feedback"}})
		case "/repos/acme/widget/issues/57/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 202, "body": "Existing conversation"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "continuation-session"}}
	commands := make([]string, 0)
	tasks := make([]string, 0)
	agents := NewAgentManager()
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:      "acme",
			RepoName:       "widget",
			RepoPath:       "/tmp/repo",
			BaseBranch:     "main",
			WorktreeDir:    t.TempDir(),
			LogDir:         t.TempDir(),
			MandatoryTests: []string{"make test"},
		},
		github:    newGitHubClientForTest(t, gh),
		agents:    agents,
		runner:    runner,
		messenger: &recordingTaskMessenger{tasks: &tasks},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			commands = append(commands, strings.TrimSpace(dir+"::"+name+" "+strings.Join(args, " ")))
			return nil
		},
	}

	if err := bot.ContinueAgentForIssuePR(context.Background(), 42, 57); err != nil {
		t.Fatalf("ContinueAgentForIssuePR() error = %v", err)
	}
	if got := len(runner.started); got != 1 {
		t.Fatalf("runner starts = %d, want 1", got)
	}
	started := runner.started[0]
	if !started.AdoptedPR || started.PRHeadBranch != "feature/in-progress" {
		t.Fatalf("started adopted PR metadata = (adopted=%v, head=%q)", started.AdoptedPR, started.PRHeadBranch)
	}
	if started.BranchName != "repository-agent-orchestrator/continue-issue-42-pr-57" {
		t.Fatalf("managed branch = %q", started.BranchName)
	}
	current, ok := agents.Get(started.ID)
	if !ok || current.State != StateWorking || current.PRNumber != 57 {
		t.Fatalf("continued agent state = (%v, %s, pr=%d)", ok, current.State, current.PRNumber)
	}
	if current.LastReviewedHeadSHA != current.ObservedPRHeadSHA {
		t.Fatalf("initial head was not baselined: reviewed=%q observed=%q", current.LastReviewedHeadSHA, current.ObservedPRHeadSHA)
	}
	if !agents.HasSeenReviewComment(started.ID, 101) || !agents.HasSeenIssueComment(started.ID, 202) {
		t.Fatal("existing PR comments were not baselined")
	}
	if got := len(commands); got != 2 {
		t.Fatalf("setup commands = %d, want 2: %v", got, commands)
	}
	if !strings.Contains(commands[0], "+refs/heads/feature/in-progress:refs/remotes/origin/feature/in-progress") {
		t.Fatalf("fetch command missing PR head refspec: %q", commands[0])
	}
	if !strings.Contains(commands[1], "git worktree add -b repository-agent-orchestrator/continue-issue-42-pr-57") || !strings.Contains(commands[1], "origin/feature/in-progress") {
		t.Fatalf("worktree command does not start from PR head: %q", commands[1])
	}
	for _, text := range append(tasks, runner.prompts...) {
		if !strings.Contains(text, "git push origin 'HEAD:feature/in-progress'") || strings.Contains(text, "open a draft pull request") {
			t.Fatalf("continuation instructions do not target existing PR head: %q", text)
		}
	}
}

func TestContinueAgentForIssuePRRejectsForkHead(t *testing.T) {
	t.Parallel()

	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/widget/issues/42":
			_ = json.NewEncoder(w).Encode(map[string]any{"number": 42, "title": "Issue"})
		case "/repos/acme/widget/pulls/57":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"number": 57,
				"state":  "open",
				"base":   map[string]any{"ref": "main"},
				"head": map[string]any{
					"ref":  "feature/fork",
					"sha":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"repo": map[string]any{"full_name": "contributor/widget"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()

	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget", BaseBranch: "main"},
		github: newGitHubClientForTest(t, gh),
		agents: NewAgentManager(),
		runner: &stubRunner{},
	}
	err := bot.ContinueAgentForIssuePR(context.Background(), 42, 57)
	if err == nil || !strings.Contains(err.Error(), "fork PR continuation is unsupported") {
		t.Fatalf("ContinueAgentForIssuePR() error = %v, want fork rejection", err)
	}
	if got := len(bot.agents.List()); got != 0 {
		t.Fatalf("agents created after validation failure = %d", got)
	}
}

func TestContinueAgentForIssuePRValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      string
		merged     bool
		baseBranch string
		headBranch string
		headSHA    string
		wantError  string
	}{
		{name: "closed", state: "closed", baseBranch: "main", headBranch: "feature/work", headSHA: "abc123", wantError: "because it is closed"},
		{name: "merged", state: "closed", merged: true, baseBranch: "main", headBranch: "feature/work", headSHA: "abc123", wantError: "cannot continue merged PR"},
		{name: "wrong base", state: "open", baseBranch: "release", headBranch: "feature/work", headSHA: "abc123", wantError: "does not match configured base branch"},
		{name: "missing head branch", state: "open", baseBranch: "main", headSHA: "abc123", wantError: "head branch is empty"},
		{name: "missing head sha", state: "open", baseBranch: "main", headBranch: "feature/work", wantError: "head SHA is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/repos/acme/widget/issues/42":
					_ = json.NewEncoder(w).Encode(map[string]any{"number": 42, "title": "Issue"})
				case "/repos/acme/widget/pulls/57":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"number": 57,
						"state":  tt.state,
						"merged": tt.merged,
						"base":   map[string]any{"ref": tt.baseBranch},
						"head": map[string]any{
							"ref":  tt.headBranch,
							"sha":  tt.headSHA,
							"repo": map[string]any{"full_name": "acme/widget"},
						},
					})
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			bot := &Orchestrator{
				cfg:    Config{RepoOwner: "acme", RepoName: "widget", BaseBranch: "main"},
				github: newGitHubClientForTest(t, srv),
				agents: NewAgentManager(),
				runner: &stubRunner{},
			}
			err := bot.ContinueAgentForIssuePR(context.Background(), 42, 57)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("ContinueAgentForIssuePR() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestLaunchRepoIndexAgentUsesIndexerIDAndPrompt(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		notifications []string
	)
	webex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		payload := map[string]string{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		notifications = append(notifications, payload["markdown"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webex.Close()

	agents := NewAgentManager()
	runner := &stubRunner{startHandle: RuntimeHandle{Kind: RuntimeKindTmux, Session: "index-session"}}
	commands := make([]string, 0)
	taskWrites := make([]string, 0)
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:       "acme",
			RepoName:        "widget",
			RepoPath:        "/tmp/repo",
			BaseBranch:      "main",
			WorktreeDir:     "/tmp/repo/.worktrees",
			LogDir:          t.TempDir(),
			WebexWebhookURL: webex.URL,
			ReviewPolicy:    builtInReviewPolicy(),
		},
		notifier: NewWebexNotifier(webex.URL),
		agents:   agents,
		runner:   runner,
		messenger: &recordingTaskMessenger{
			tasks: &taskWrites,
		},
		cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
			commands = append(commands, strings.TrimSpace(dir+"::"+name+" "+strings.Join(args, " ")))
			return nil
		},
	}

	agentID, err := bot.LaunchRepoIndexAgent(context.Background())
	if err != nil {
		t.Fatalf("LaunchRepoIndexAgent() error = %v", err)
	}
	if !strings.HasPrefix(agentID, "index-agent-") {
		t.Fatalf("agent ID = %q, want index-agent prefix", agentID)
	}
	if got, want := len(runner.started), 1; got != want {
		t.Fatalf("runner starts = %d, want %d", got, want)
	}
	if runner.started[0].Role != RoleIndexer {
		t.Fatalf("started agent role = %s, want %s", runner.started[0].Role, RoleIndexer)
	}
	if got, want := runner.started[0].RuntimeProfile, builtInAgentProfile(AgentProfileRoleIndexer); got != want {
		t.Fatalf("runtime profile = %+v, want %+v", got, want)
	}
	if !strings.HasPrefix(runner.started[0].BranchName, "repository-agent-orchestrator/repo-index-") {
		t.Fatalf("branch = %q, want repo index branch", runner.started[0].BranchName)
	}
	if got := len(commands); got != 2 {
		t.Fatalf("setup command count = %d, want 2", got)
	}
	if !strings.Contains(commands[1], "git worktree add -b repository-agent-orchestrator/repo-index-") {
		t.Fatalf("second setup command = %q, want git worktree add for repo index branch", commands[1])
	}
	if got := len(taskWrites); got != 1 {
		t.Fatalf("task write count = %d, want 1", got)
	}
	if !strings.Contains(taskWrites[0], "Canonical source: agent_index.yaml") {
		t.Fatalf("task markdown missing agent_index.yaml output: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "Generated output: INDEX.md") {
		t.Fatalf("task markdown missing INDEX.md output: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "`components`, `concerns`, and `artifacts` must use lowercase `snake_case` identifiers.") {
		t.Fatalf("task markdown missing normalized identifier guidance: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "preferred starting points") {
		t.Fatalf("task markdown missing repository routing hints guidance: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "Define top-level groups as a mapping from group name to a mapping with a `summary` field") {
		t.Fatalf("task markdown missing top-level groups guidance: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "`read_when` and `usually_skip_when` must stay as arrays, even for a single item.") {
		t.Fatalf("task markdown missing array-shape guidance: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "Use repo structure as a source of candidate names") {
		t.Fatalf("task markdown missing repo-structure guidance: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "Use real newlines in the commit body rather than literal `\\n` sequences.") {
		t.Fatalf("task markdown missing commit newline guidance: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "Never stage or commit Repository Agent Orchestrator runtime artifacts under `.repository-agent-orchestrator/` or `.worktrees/`; `.repository-agent-orchestrator/HANDOFF.yaml` is for Repository Agent Orchestrator state capture only.") {
		t.Fatalf("task markdown missing runtime artifact commit rule: %q", taskWrites[0])
	}
	if !strings.Contains(taskWrites[0], "open a pull request against `main`") {
		t.Fatalf("task markdown missing PR instruction: %q", taskWrites[0])
	}
	if got := len(runner.prompts); got != 1 {
		t.Fatalf("runner prompts = %d, want 1", got)
	}
	if !strings.Contains(runner.prompts[0], "repository indexing agent") {
		t.Fatalf("initial prompt missing indexing role: %q", runner.prompts[0])
	}
	if !strings.Contains(runner.prompts[0], "agent_index.yaml") {
		t.Fatalf("initial prompt missing agent_index.yaml guidance: %q", runner.prompts[0])
	}
	if !strings.Contains(runner.prompts[0], "Define top-level repository routing hints") {
		t.Fatalf("initial prompt missing repository routing hints guidance: %q", runner.prompts[0])
	}
	if !strings.Contains(runner.prompts[0], "Define top-level groups as a mapping from group name to a mapping with a `summary` field") {
		t.Fatalf("initial prompt missing top-level groups guidance: %q", runner.prompts[0])
	}
	if !strings.Contains(runner.prompts[0], "Use repo structure only as a hint for candidate names") {
		t.Fatalf("initial prompt missing repo-structure hint guidance: %q", runner.prompts[0])
	}
	if !strings.Contains(runner.prompts[0], "Include per-document `priority` using only `foundational`, `important`, `targeted`, or `situational`.") {
		t.Fatalf("initial prompt missing priority guidance: %q", runner.prompts[0])
	}
	if !strings.Contains(runner.prompts[0], "Use real newlines in the commit body rather than literal `\\n` sequences.") {
		t.Fatalf("initial prompt missing commit newline guidance: %q", runner.prompts[0])
	}
	if !strings.Contains(runner.prompts[0], "Never stage or commit Repository Agent Orchestrator runtime artifacts under `.repository-agent-orchestrator/` or `.worktrees/`; `.repository-agent-orchestrator/HANDOFF.yaml` is for Repository Agent Orchestrator state capture only.") {
		t.Fatalf("initial prompt missing runtime artifact commit rule: %q", runner.prompts[0])
	}

	mu.Lock()
	defer mu.Unlock()
	if countMessagesContaining(notifications, "runtime launched for repo indexing agent") != 1 {
		t.Fatalf("expected one repo indexing runtime launch notification, got %v", notifications)
	}
}

type recordingTaskMessenger struct {
	tasks    *[]string
	contexts *[]string
	handoffs *[]string
}

func (m *recordingTaskMessenger) SendMessage(agent Agent, text string) error {
	return nil
}

func (m *recordingTaskMessenger) SendTask(agent Agent, markdown string) error {
	if m.tasks != nil {
		*m.tasks = append(*m.tasks, markdown)
	}
	return nil
}

func (m *recordingTaskMessenger) SendContext(agent Agent, markdown string) error {
	if m.contexts != nil {
		*m.contexts = append(*m.contexts, markdown)
	}
	return nil
}

func (m *recordingTaskMessenger) SendHandoff(agent Agent, content string) error {
	if m.handoffs != nil {
		*m.handoffs = append(*m.handoffs, content)
	}
	return nil
}
