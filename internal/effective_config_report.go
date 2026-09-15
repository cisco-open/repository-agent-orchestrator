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
	"regexp"
	"strings"
)

// credentialBearingFlagMarkers are substrings that, if present in a
// user-configured command (CodexCmd, a mandatory test entry), suggest it
// carries a credential rather than a plain command. This mirrors the
// existing guard in review_policy_observability.go
// (TestReviewPolicyObservabilityOmitsSecretsAndCommands already asserts
// "--token"/"--authorization" never reach status/log output); the intent
// here is the same, applied to a new place these values are surfaced.
var credentialBearingFlagMarkers = []string{
	"--token",
	"--authorization",
	"--password",
	"--secret",
	"authorization:",
	"bearer ",
	"basic ",
}

// credentialBearingAssignmentPattern catches the other common shape these
// values take in a shell command or env-style entry: KEY=value or
// KEY:value where KEY itself looks credential-bearing (GH_TOKEN=,
// GITHUB_TOKEN=, token=, DB_PASSWORD=, API_KEY=, Authorization=,
// AWS_ACCESS_KEY_ID=, PRIVATE_KEY=, etc). credentialBearingFlagMarkers
// alone misses all of these since none of them use a "--flag" or
// trailing-space form. "key" is matched bare (not just "api_key") since
// *_KEY/*_KEY_ID is itself an extremely common credential-variable
// naming convention (access keys, private keys, encryption keys, ssh
// keys) independent of any "api" prefix; the resulting occasional
// false positive on a non-secret "*_KEY" name is the intended, safer
// failure mode for a whole-value redaction (see
// redactIfCredentialBearing's own reasoning below).
var credentialBearingAssignmentPattern = regexp.MustCompile(
	`(?i)[a-z0-9_.-]*(token|password|passwd|secret|key|authorization)[a-z0-9_.-]*\s*[:=]`,
)

// redactIfCredentialBearing returns value unchanged unless it looks like it
// might carry a credential, in which case it returns a fixed placeholder
// instead of attempting to surgically redact just the credential part --
// a whole-value placeholder can't accidentally leave part of a secret
// visible the way a partial-match redaction could.
func redactIfCredentialBearing(value string) string {
	lower := strings.ToLower(value)
	for _, marker := range credentialBearingFlagMarkers {
		if strings.Contains(lower, marker) {
			return "[REDACTED: contains a possible credential-bearing flag]"
		}
	}
	if credentialBearingAssignmentPattern.MatchString(value) {
		return "[REDACTED: contains a possible credential-bearing flag]"
	}
	return value
}

// configuredOrNot reports presence without ever printing the value itself,
// for fields that are secrets by the project's own established definition
// (README "Security Notes": GitHub tokens, Webex webhook URLs).
func configuredOrNot(value string) string {
	if strings.TrimSpace(value) == "" {
		return "(not configured)"
	}
	return "(configured)"
}

// formatEffectiveConfigReport renders the fully-resolved Config -- every
// default already applied by loadConfig, not just what a user's config.yaml
// happened to set explicitly -- with secrets and possible credentials
// never printed in the clear. The REVIEW_POLICY sub-config already has its
// own dedicated, separately-tested-safe renderer
// (formatReviewPolicyStatus, included in status.txt); this covers the
// top-level fields that aren't part of REVIEW_POLICY and today appear
// nowhere in status/tech-support output at all.
func formatEffectiveConfigReport(cfg Config) string {
	var report strings.Builder
	report.WriteString("Effective Configuration\n")
	fmt.Fprintf(&report, "  repo: %s/%s\n", cfg.RepoOwner, cfg.RepoName)
	fmt.Fprintf(&report, "  repo_path: %s\n", cfg.RepoPath)
	fmt.Fprintf(&report, "  log_dir: %s\n", cfg.LogDir)
	fmt.Fprintf(&report, "  worktree_dir: %s\n", cfg.WorktreeDir)
	fmt.Fprintf(&report, "  base_branch: %s\n", cfg.BaseBranch)
	fmt.Fprintf(&report, "  merge_method: %s\n", cfg.MergeMethod)
	fmt.Fprintf(&report, "  poll_interval_seconds: %d\n", cfg.PollIntervalSeconds)
	fmt.Fprintf(&report, "  hard_gate_mode: %s\n", cfg.effectiveHardGateMode())
	fmt.Fprintf(&report, "  review_gate_alert_threshold: %s\n", cfg.effectiveReviewGateAlertThreshold())
	fmt.Fprintf(&report, "  max_stored_handoffs: %d\n", cfg.MaxStoredHandoffs)
	fmt.Fprintf(&report, "  max_handoffs_in_context: %d\n", cfg.MaxHandoffsInContext)
	fmt.Fprintf(&report, "  config_path: %s\n", cfg.ConfigPath)
	fmt.Fprintf(&report, "  webex_webhook_url: %s\n", configuredOrNot(cfg.WebexWebhookURL))
	fmt.Fprintf(&report, "  webex_uid: %s\n", configuredOrNot(cfg.WebexUID))
	fmt.Fprintf(&report, "  codex_cmd: %s\n", redactIfCredentialBearing(cfg.CodexCmd))
	report.WriteString("  mandatory_tests:\n")
	if len(cfg.MandatoryTests) == 0 {
		report.WriteString("    (none)\n")
	}
	for _, test := range cfg.MandatoryTests {
		fmt.Fprintf(&report, "    - %s\n", redactIfCredentialBearing(test))
	}
	report.WriteString(
		"\nSee status.txt for the resolved REVIEW_POLICY (every default already applied).\n",
	)
	return report.String()
}
