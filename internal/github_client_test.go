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
	"net/http"
	"testing"

	"github.com/google/go-github/v90/github"
)

func mustNewGitHubClientForTest(
	t *testing.T,
	httpClient *http.Client,
	baseURL string,
) *github.Client {
	t.Helper()
	client, err := github.NewClient(
		github.WithHTTPClient(httpClient),
		github.WithURLs(&baseURL, nil),
	)
	if err != nil {
		t.Fatalf("github.NewClient() error = %v", err)
	}
	return client
}

func TestNewGitHubClientSetsTimeout(t *testing.T) {
	client, err := newGitHubClient("token-test")
	if err != nil {
		t.Fatalf("newGitHubClient() error = %v", err)
	}

	got := client.Client().Timeout
	if got != defaultGitHubHTTPTimeout {
		t.Fatalf("GitHub HTTP timeout = %s, want %s", got, defaultGitHubHTTPTimeout)
	}
}
