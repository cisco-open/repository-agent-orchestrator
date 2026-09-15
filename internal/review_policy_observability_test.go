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
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const observabilitySecretMarker = "TOP-SECRET-OBSERVABILITY-MARKER"

func TestReviewPolicyStatusGolden(t *testing.T) {
	policy := observabilityTestPolicy()
	got := formatReviewPolicyStatus(policy)

	assertGoldenFile(t, "review_policy_status.golden", got)
	if !strings.Contains(got, "state: convergent") {
		t.Fatalf("convergent policy is not obvious in status: %q", got)
	}
}

func TestReviewPolicyStructuredLogGolden(t *testing.T) {
	policy := observabilityTestPolicy()
	timestamp := time.Date(2026, time.July, 28, 12, 34, 56, 789000000, time.UTC)
	got := formatReviewPolicyLogRecord(policy, timestamp) + "\n"

	assertGoldenFile(t, "review_policy_log.golden", got)
}

func TestReviewPolicyStructuredDaemonLogIsNDJSON(t *testing.T) {
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	defer log.SetOutput(oldWriter)
	defer log.SetFlags(oldFlags)

	logDir := filepath.Join(t.TempDir(), "logs")
	logFile, err := setupProcessLogger(logDir)
	if err != nil {
		t.Fatalf("setupProcessLogger(%s) error = %v", logDir, err)
	}

	logEffectiveReviewPolicy(observabilityTestPolicy())
	if err := logFile.Close(); err != nil {
		t.Fatalf("logFile.Close() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(logDir, orchestratorDaemonLogName))
	if err != nil {
		t.Fatalf("ReadFile(daemon log) error = %v", err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 1 {
		t.Fatalf("daemon log line count = %d, want 1: %q", len(lines), string(data))
	}

	var record reviewPolicyLogRecord
	if err := json.Unmarshal(lines[0], &record); err != nil {
		t.Fatalf("daemon log event is not valid NDJSON: %v: %q", err, string(lines[0]))
	}
	if record.Timestamp.IsZero() {
		t.Fatalf("daemon log event has zero timestamp: %q", string(lines[0]))
	}
	if record.Event != "effective_review_policy" {
		t.Fatalf("daemon log event = %q, want effective_review_policy", record.Event)
	}
	if record.ReviewPolicy.Fingerprint != strings.Repeat("a", 64) {
		t.Fatalf("daemon log policy fingerprint = %q", record.ReviewPolicy.Fingerprint)
	}
}

func TestReviewPolicyStatusShowsConvergentPolicy(t *testing.T) {
	policy := builtInReviewPolicy()
	policy.Fingerprint = strings.Repeat("b", 64)

	status := formatReviewPolicyStatus(policy)
	logRecord := formatReviewPolicyLogRecord(policy, time.Now())
	for name, output := range map[string]string{"status": status, "log": logRecord} {
		if !strings.Contains(output, "convergent") {
			t.Fatalf("%s output does not show convergent policy: %q", name, output)
		}
		if strings.Contains(output, "DISABLED") || strings.Contains(output, "enabled") {
			t.Fatalf("%s output exposes an obsolete review mode: %q", name, output)
		}
	}
}

func TestReviewPolicyObservabilityOmitsSecretsAndCommands(t *testing.T) {
	t.Setenv("RAW_POLICY_TEST_VALUE", observabilitySecretMarker)
	policy := builtInReviewPolicy()
	policy.Fingerprint = strings.Repeat("a", 64)
	policy.AgentProfiles["unused-sensitive-profile"] = AgentProfile{
		Name:            "unused-sensitive-profile",
		Model:           "unused-" + observabilitySecretMarker,
		ReasoningEffort: "high",
	}
	bot := &Orchestrator{
		cfg: Config{
			WebexWebhookURL: "https://example.test/" + observabilitySecretMarker,
			CodexCmd:        "codex --token " + observabilitySecretMarker,
			MandatoryTests:  []string{"curl --authorization " + observabilitySecretMarker},
			ReviewPolicy:    policy,
		},
		agents: NewAgentManager(),
	}

	status := captureStdout(t, func() { printStatus(bot) })

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	defer log.SetOutput(oldWriter)
	defer log.SetFlags(oldFlags)
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	logEffectiveReviewPolicy(policy)

	for name, output := range map[string]string{
		"status": status,
		"log":    logs.String(),
	} {
		if strings.Contains(output, observabilitySecretMarker) {
			t.Fatalf("%s output exposed secret-bearing configuration: %q", name, output)
		}
		if strings.Contains(output, "--token") || strings.Contains(output, "--authorization") {
			t.Fatalf("%s output exposed a command containing credentials: %q", name, output)
		}
	}
}

func observabilityTestPolicy() ReviewPolicy {
	policy := builtInReviewPolicy()
	policy.Fingerprint = strings.Repeat("a", 64)
	return policy
}

func assertGoldenFile(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	if got != string(want) {
		t.Fatalf("%s mismatch\n--- want ---\n%s--- got ---\n%s", name, want, got)
	}
}
