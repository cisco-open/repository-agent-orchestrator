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

func TestFormatEffectiveConfigReportShowsDefaultedFields(t *testing.T) {
	// Deliberately leave HardGateMode/ReviewGateAlertThreshold at their zero
	// values, the way a config.yaml that omits them entirely would --
	// effectiveHardGateMode()/effectiveReviewGateAlertThreshold() must
	// resolve those to the same defaults the running orchestrator actually
	// uses, not report an empty/zero value.
	cfg := Config{
		RepoOwner:            "acme",
		RepoName:             "widget",
		RepoPath:             "/home/user/widget",
		LogDir:               "/home/user/widget/.repository-agent-orchestrator",
		WorktreeDir:          "/home/user/widget/.worktrees",
		BaseBranch:           "main",
		MergeMethod:          "squash",
		PollIntervalSeconds:  20,
		MaxStoredHandoffs:    100,
		MaxHandoffsInContext: 5,
		MandatoryTests:       []string{"make review-gate"},
	}

	report := formatEffectiveConfigReport(cfg)

	if !strings.Contains(report, "acme/widget") {
		t.Fatalf("report missing repo identity: %q", report)
	}
	if !strings.Contains(report, "make review-gate") {
		t.Fatalf("report missing mandatory test: %q", report)
	}
	if !strings.Contains(report, "hard_gate_mode: "+string(HardGateModeParallel)) {
		t.Fatalf("report did not resolve the defaulted hard gate mode: %q", report)
	}
	if !strings.Contains(report, "review_gate_alert_threshold: "+reviewGateAlertThreshold.String()) {
		t.Fatalf("report did not resolve the defaulted review gate alert threshold: %q", report)
	}
}

func TestFormatEffectiveConfigReportNeverPrintsWebexCredentials(t *testing.T) {
	cfg := Config{
		RepoOwner:       "acme",
		RepoName:        "widget",
		WebexWebhookURL: "https://webexapis.com/v1/webhooks/incoming/" + observabilitySecretMarker,
		WebexUID:        observabilitySecretMarker,
	}

	report := formatEffectiveConfigReport(cfg)

	if strings.Contains(report, observabilitySecretMarker) {
		t.Fatalf("report leaked a Webex credential: %q", report)
	}
	if !strings.Contains(report, "webex_webhook_url: (configured)") {
		t.Fatalf("report missing webhook presence indicator: %q", report)
	}
	if !strings.Contains(report, "webex_uid: (configured)") {
		t.Fatalf("report missing webex_uid presence indicator: %q", report)
	}
}

func TestFormatEffectiveConfigReportRedactsCredentialBearingCommands(t *testing.T) {
	cfg := Config{
		RepoOwner:      "acme",
		RepoName:       "widget",
		CodexCmd:       "codex --token " + observabilitySecretMarker,
		MandatoryTests: []string{"curl --authorization " + observabilitySecretMarker, "make test"},
	}

	report := formatEffectiveConfigReport(cfg)

	if strings.Contains(report, observabilitySecretMarker) {
		t.Fatalf("report leaked a credential-bearing flag value: %q", report)
	}
	if strings.Contains(report, "--token") || strings.Contains(report, "--authorization") {
		t.Fatalf("report exposed a command containing credentials: %q", report)
	}
	if !strings.Contains(report, "make test") {
		t.Fatalf("report should still show the non-credential-bearing mandatory test: %q", report)
	}
}

func TestRedactIfCredentialBearingLeavesOrdinaryValuesUnchanged(t *testing.T) {
	value := "codex --ask-for-approval never --sandbox danger-full-access"
	if got := redactIfCredentialBearing(value); got != value {
		t.Fatalf("redactIfCredentialBearing() = %q, want unchanged %q", got, value)
	}
}

// TestRedactIfCredentialBearingCatchesAssignmentForms is the regression
// test for independent review feedback: the original marker list only
// caught "--flag"/"word "-style forms and missed the equally common
// KEY=value / KEY:value forms (env-style prefixes in a shell command, or a
// bare "token=..." in a mandatory test entry).
func TestRedactIfCredentialBearingCatchesAssignmentForms(t *testing.T) {
	cases := []string{
		"GH_TOKEN=ghp_abc123 codex --ask-for-approval never",
		"GITHUB_TOKEN=abc123 make test",
		"token=abc123 curl https://example.test",
		"DB_PASSWORD=hunter2 ./run.sh",
		"API_KEY=abc123 make test",
		"Authorization=Bearer abc123",
		// Bare "*_KEY"/"*_KEY_ID" forms, independent of any "api" prefix
		// -- an equally common credential-variable naming convention
		// (access keys, private keys, SSH keys).
		"AWS_ACCESS_KEY_ID=AKIAABCDEF make deploy",
		"AWS_SECRET_ACCESS_KEY=abc123 make deploy",
		"PRIVATE_KEY=-----BEGIN----- make test",
		"SSH_KEY=/home/user/.ssh/id_rsa make test",
	}
	for _, value := range cases {
		if got := redactIfCredentialBearing(value); got == value {
			t.Fatalf("redactIfCredentialBearing(%q) left the value unchanged, want redacted", value)
		}
	}
}

func TestConfiguredOrNot(t *testing.T) {
	if got := configuredOrNot(""); got != "(not configured)" {
		t.Fatalf("configuredOrNot(empty) = %q", got)
	}
	if got := configuredOrNot("  "); got != "(not configured)" {
		t.Fatalf("configuredOrNot(whitespace) = %q", got)
	}
	if got := configuredOrNot("value"); got != "(configured)" {
		t.Fatalf("configuredOrNot(value) = %q", got)
	}
}
