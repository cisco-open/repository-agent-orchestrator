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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-github/v90/github"
)

func TestGetAuthenticatedUserLoginUsesConfiguredWebexUID(t *testing.T) {
	tests := []struct {
		name string
		uid  string
		want string
	}{
		{name: "omitted"},
		{
			name: "configured email",
			uid:  " maintainer@example.com ",
			want: "<@personEmail:maintainer@example.com>",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bot := &Orchestrator{cfg: Config{WebexUID: test.uid}}
			if got := bot.getAuthenticatedUserLogin(context.Background()); got != test.want {
				t.Fatalf("getAuthenticatedUserLogin() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSelectTrackedPR(t *testing.T) {
	t.Parallel()

	mainPR := &github.PullRequest{
		Number: github.Int(11),
		Base:   &github.PullRequestBranch{Ref: github.String("main")},
	}
	releasePR := &github.PullRequest{
		Number: github.Int(12),
		Base:   &github.PullRequestBranch{Ref: github.String("release")},
	}
	noBasePR := &github.PullRequest{
		Number: github.Int(13),
	}

	cases := []struct {
		name       string
		prs        []*github.PullRequest
		baseBranch string
		wantNumber int
		wantNil    bool
	}{
		{
			name:       "selects matching base branch",
			prs:        []*github.PullRequest{releasePR, mainPR},
			baseBranch: "main",
			wantNumber: 11,
		},
		{
			name:       "uses single fallback when base ref missing",
			prs:        []*github.PullRequest{noBasePR},
			baseBranch: "main",
			wantNumber: 13,
		},
		{
			name:       "returns nil for mismatched base branch",
			prs:        []*github.PullRequest{releasePR},
			baseBranch: "main",
			wantNil:    true,
		},
		{
			name:       "returns nil for multiple missing-base candidates",
			prs:        []*github.PullRequest{noBasePR, &github.PullRequest{Number: github.Int(14)}},
			baseBranch: "main",
			wantNil:    true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := selectTrackedPR(tc.prs, tc.baseBranch)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("selectTrackedPR() = PR #%d, want nil", got.GetNumber())
				}
				return
			}
			if got == nil {
				t.Fatal("selectTrackedPR() = nil, want PR")
			}
			if got.GetNumber() != tc.wantNumber {
				t.Fatalf("selectTrackedPR() = PR #%d, want #%d", got.GetNumber(), tc.wantNumber)
			}
		})
	}
}

func TestIsInternalReviewVerdictComment(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		c    *github.IssueComment
		want bool
	}{
		{
			name: "matching metadata",
			c: &github.IssueComment{
				Body: github.String("CODEX_AGENT_ID: review-agent-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: abcdef123456\nCODEX_VERDICT: THUMBS_UP"),
			},
			want: true,
		},
		{
			name: "missing agent id",
			c: &github.IssueComment{
				Body: github.String("CODEX_REVIEWED_SHA: abcdef123456\nCODEX_VERDICT: THUMBS_UP"),
			},
			want: false,
		},
		{
			name: "missing sha",
			c: &github.IssueComment{
				Body: github.String("CODEX_AGENT_ID: review-agent-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_VERDICT: THUMBS_UP"),
			},
			want: false,
		},
		{
			name: "missing verdict",
			c: &github.IssueComment{
				Body: github.String("CODEX_AGENT_ID: review-agent-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: abcdef123456"),
			},
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isInternalReviewVerdictComment(tc.c); got != tc.want {
				t.Fatalf("isInternalReviewVerdictComment() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCommentClassification(t *testing.T) {
	t.Parallel()

	t.Run("recognizes marked orchestrator agent comment", func(t *testing.T) {
		t.Parallel()

		body := "Handled the feedback.\n\nCODEX_AGENT_ID: coding-agent-1\nCODEX_AGENT_ROLE: coder"
		if !isOrchestratorInternalComment(body) {
			t.Fatal("isOrchestratorInternalComment() = false, want true")
		}
		if isHumanFeedbackComment(body) {
			t.Fatal("isHumanFeedbackComment() = true, want false")
		}
	})

	t.Run("recognizes review verdict as internal control comment", func(t *testing.T) {
		t.Parallel()

		body := "CODEX_AGENT_ID: review-agent-1\nCODEX_AGENT_ROLE: reviewer\nCODEX_REVIEWED_SHA: abcdef123456\nCODEX_VERDICT: NEEDS_CHANGES"
		if !isInternalReviewVerdictBody(body) {
			t.Fatal("isInternalReviewVerdictBody() = false, want true")
		}
		if isHumanFeedbackComment(body) {
			t.Fatal("isHumanFeedbackComment() = true, want false")
		}
	})

	t.Run("treats unmarked comment as human feedback", func(t *testing.T) {
		t.Parallel()

		body := "Please fix the nil case before merge."
		if isOrchestratorInternalComment(body) {
			t.Fatal("isOrchestratorInternalComment() = true, want false")
		}
		if !isHumanFeedbackComment(body) {
			t.Fatal("isHumanFeedbackComment() = false, want true")
		}
	})
}

func TestEnsurePRReadyForReview(t *testing.T) {
	t.Parallel()

	t.Run("marks draft PR ready", func(t *testing.T) {
		t.Parallel()

		var commandCalls []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/42":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"number": 42,
					"draft":  true,
				})
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				commandCalls = append(commandCalls, name+" "+strings.Join(args, " "))
				return nil
			},
		}
		if err := bot.ensurePRReadyForReview(context.Background(), 42); err != nil {
			t.Fatalf("ensurePRReadyForReview() error = %v", err)
		}
		if len(commandCalls) != 1 {
			t.Fatalf("gh ready calls = %d, want 1", len(commandCalls))
		}
		if got, want := commandCalls[0], "gh pr ready 42 -R acme/widget"; got != want {
			t.Fatalf("gh ready command = %q, want %q", got, want)
		}
	})

	t.Run("skips non-draft PR", func(t *testing.T) {
		t.Parallel()

		var commandCalls []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/42":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"number": 42,
					"draft":  false,
				})
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				commandCalls = append(commandCalls, name+" "+strings.Join(args, " "))
				return nil
			},
		}
		if err := bot.ensurePRReadyForReview(context.Background(), 42); err != nil {
			t.Fatalf("ensurePRReadyForReview() error = %v", err)
		}
		if len(commandCalls) != 0 {
			t.Fatalf("gh ready calls = %d, want 0", len(commandCalls))
		}
	})

	t.Run("returns gh error when gh pr ready fails", func(t *testing.T) {
		t.Parallel()

		var commandCalls []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/repos/acme/widget/pulls/42":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"number": 42,
					"draft":  true,
				})
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		bot := &Orchestrator{
			cfg:    Config{RepoOwner: "acme", RepoName: "widget"},
			github: newGitHubClientForTest(t, srv),
			cmdRunner: func(ctx context.Context, dir string, name string, args ...string) error {
				commandCalls = append(commandCalls, name+" "+strings.Join(args, " "))
				return fmt.Errorf("gh failed")
			},
		}
		err := bot.ensurePRReadyForReview(context.Background(), 42)
		if err == nil || !strings.Contains(err.Error(), "gh pr ready failed for PR #42") {
			t.Fatalf("ensurePRReadyForReview() error = %v, want gh failure", err)
		}
		if len(commandCalls) != 1 {
			t.Fatalf("gh ready calls = %d, want 1", len(commandCalls))
		}
		if got, want := commandCalls[0], "gh pr ready 42 -R acme/widget"; got != want {
			t.Fatalf("gh ready command = %q, want %q", got, want)
		}
	})
}
