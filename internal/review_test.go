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
	"errors"
	"strings"
	"testing"

	"github.com/google/go-github/v90/github"
)

func review(user, state string) *github.PullRequestReview {
	return &github.PullRequestReview{
		State: github.String(state),
		User:  &github.User{Login: github.String(user)},
	}
}

func TestIsApprovedFromReviews(t *testing.T) {
	cases := []struct {
		name    string
		reviews []*github.PullRequestReview
		want    bool
	}{
		{
			name:    "no reviews",
			reviews: nil,
			want:    false,
		},
		{
			name: "single approval",
			reviews: []*github.PullRequestReview{
				review("alice", "APPROVED"),
			},
			want: true,
		},
		{
			name: "approval with later changes requested by same reviewer",
			reviews: []*github.PullRequestReview{
				review("alice", "APPROVED"),
				review("alice", "CHANGES_REQUESTED"),
			},
			want: false,
		},
		{
			name: "approval and changes requested by different reviewers",
			reviews: []*github.PullRequestReview{
				review("alice", "APPROVED"),
				review("bob", "CHANGES_REQUESTED"),
			},
			want: false,
		},
		{
			name: "changes requested then approval by same reviewer",
			reviews: []*github.PullRequestReview{
				review("alice", "CHANGES_REQUESTED"),
				review("alice", "APPROVED"),
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isApprovedFromReviews(tc.reviews)
			if got != tc.want {
				t.Fatalf("isApprovedFromReviews() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSafeErrorRedactsSecrets(t *testing.T) {
	bot := &Orchestrator{cfg: Config{
		WebexWebhookURL: "https://example.test/webhook/secret",
	}}
	bot.token = "gh-secret-token"

	err := errors.New("request failed with gh-secret-token and https://example.test/webhook/secret")
	msg := bot.safeError(err)

	if strings.Contains(msg, "gh-secret-token") {
		t.Fatal("safeError leaked github token")
	}
	if strings.Contains(msg, "https://example.test/webhook/secret") {
		t.Fatal("safeError leaked webex webhook")
	}
	if !strings.Contains(msg, "[REDACTED]") {
		t.Fatal("safeError should include redaction marker")
	}
}
