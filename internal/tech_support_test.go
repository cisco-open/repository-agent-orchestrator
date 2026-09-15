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
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func readTarGzEntries(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%s) error = %v", path, err)
	}
	defer file.Close()
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer gzReader.Close()
	tarReader := tar.NewReader(gzReader)
	entries := make(map[string]string)
	for {
		header, err := tarReader.Next()
		if err != nil {
			break
		}
		body, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatalf("read tar entry %s error = %v", header.Name, err)
		}
		entries[header.Name] = string(body)
	}
	return entries
}

// TestCaptureREPLOutputHandlesLargeOutputWithoutDeadlock is the regression
// test for independent review feedback: captureREPLOutput used to redirect
// os.Stdout through a real os.Pipe and only read it back after fn()
// returned. A pipe's kernel buffer is bounded (commonly 64KiB on Linux);
// writing more than that with nobody reading yet blocks the write
// forever. This writes well over that (roughly 5 MiB) and requires the
// call to complete within a generous but bounded deadline, so a
// regression reintroducing the pipe-based approach fails this test
// instead of hanging the whole suite.
// TestCaptureREPLOutputRestoresOverrideOnPanic is the regression test for
// independent review feedback: the redirect-then-restore around fn() had
// no defer, so a panic inside fn() (printStatus, printAgents, or any
// future caller) would leave the process-global replOutputOverride
// pointing at an about-to-be-discarded buffer forever -- silently
// swallowing every later replPrintf call in the process, including in the
// interactive REPL, instead of writing to the terminal.
func TestCaptureREPLOutputRestoresOverrideOnPanic(t *testing.T) {
	func() {
		defer func() { _ = recover() }()
		captureREPLOutput(func() { panic("boom") })
	}()

	replOutputMu.Lock()
	override := replOutputOverride
	replOutputMu.Unlock()
	if override != nil {
		t.Fatal("replOutputOverride was not restored to nil after a panic inside captureREPLOutput's fn")
	}

	// And a concrete symptom check: output after the panic must reach a
	// fresh, uncontaminated capture rather than silently vanishing into
	// the stale buffer from the panicking call.
	output := captureREPLOutput(func() { replPrintln("after-panic") })
	if !strings.Contains(output, "after-panic") {
		t.Fatalf("output after a panicking capture = %q, want it to still be capturable normally", output)
	}
}

func TestCaptureREPLOutputHandlesLargeOutputWithoutDeadlock(t *testing.T) {
	done := make(chan string, 1)
	go func() {
		done <- captureREPLOutput(func() {
			line := strings.Repeat("x", 1024)
			for i := 0; i < 5000; i++ {
				replPrintln(line)
			}
		})
	}()
	select {
	case output := <-done:
		if got, want := strings.Count(output, "\n"), 5000; got != want {
			t.Fatalf("captured %d lines, want %d", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("captureREPLOutput did not return within 10s -- likely deadlocked on large output")
	}
}

// TestGenerateTechSupportBundleSerializesConcurrentCalls is the
// regression test for independent review feedback: generateTechSupportBundle
// can be triggered both interactively (REPL `tech-support`) and
// automatically from the poll loop (checkReviewCycleHealth on
// unknown_stall), so two calls can genuinely overlap. The bundle filename
// used to have only second-precision, and os.Create truncates, so two
// overlapping calls starting in the same second could compute the same
// path and race on (or silently clobber) the same archive.
// techSupportBundleMu now serializes the whole call, and the filename
// also carries a random suffix independent of that serialization; this
// drives real concurrent callers and asserts every one produced its own
// intact, independently readable archive.
func TestGenerateTechSupportBundleSerializesConcurrentCalls(t *testing.T) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget", LogDir: logDir},
		agents: agents,
	}

	const goroutines = 8
	paths := make(chan string, goroutines)
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			path, _, err := bot.generateTechSupportBundle()
			if err != nil {
				errs <- err
				return
			}
			paths <- path
		}()
	}

	seen := make(map[string]struct{}, goroutines)
	timeout := time.After(10 * time.Second)
	for i := 0; i < goroutines; i++ {
		select {
		case err := <-errs:
			t.Fatalf("generateTechSupportBundle() error = %v", err)
		case path := <-paths:
			if _, dup := seen[path]; dup {
				t.Fatalf("two concurrent calls produced the same bundle path: %s", path)
			}
			seen[path] = struct{}{}
			entries := readTarGzEntries(t, path)
			if len(entries) == 0 {
				t.Fatalf("bundle %s has no readable entries -- likely clobbered/corrupted", path)
			}
		case <-timeout:
			t.Fatal("concurrent generateTechSupportBundle calls did not all complete within 10s")
		}
	}
	if len(seen) != goroutines {
		t.Fatalf("got %d distinct bundle paths, want %d", len(seen), goroutines)
	}
}

// TestReplPrintfDuringCaptureHasNoDataRace is the regression test for
// independent review feedback: replPrintf used to release replOutputMu
// before the actual Fprint call, so a concurrent replPrintf from another
// goroutine (an operator's interactive command racing an automatic
// tech-support capture) could write to the same shared *bytes.Buffer at
// the same time -- a genuine data race, since bytes.Buffer isn't safe for
// concurrent use, not just misdirected output.
//
// This must be run with -race to be meaningful (that's what actually
// proves there's no unsynchronized concurrent write); it also checks that
// every line in the captured output is intact -- either the marker or the
// concurrent writer's line, in full, never a garbled partial mix of both
// bytes, since interleaving one goroutine's *whole* lines into the other's
// buffer is the documented, accepted behavior here (see captureREPLOutput's
// comment), but a corrupted/partial line would mean the write itself
// wasn't atomic and something is still unsynchronized.
func TestReplPrintfDuringCaptureHasNoDataRace(t *testing.T) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				replPrintln("concurrent-uncaptured-write")
			}
		}
	}()

	const marker = "captured-line"
	output := captureREPLOutput(func() {
		for i := 0; i < 200; i++ {
			replPrintln(marker)
		}
	})

	close(stop)
	wg.Wait()

	// replPrintln normalizes "\n" to "\r\n" (normalizeREPLNewlines), so
	// split on that rather than bare "\n".
	for _, line := range strings.Split(strings.TrimRight(output, "\r\n"), "\r\n") {
		if line != "" && line != marker && line != "concurrent-uncaptured-write" {
			t.Fatalf("capture buffer contains a garbled/partial line, not a clean interleave: %q", line)
		}
	}
}

// TestScanForSuspectedSecretsCategorizesFoundAndSuspected is the
// regression test for independent review feedback: the
// tech-support bundle warning must scan for secret-like content and
// report, by category, how many were found (a specific, well-known
// credential format) versus merely suspected (a generic
// assignment-shaped heuristic).
func TestScanForSuspectedSecretsCategorizesFoundAndSuspected(t *testing.T) {
	// The AWS-shaped value is built by concatenation, not written as one
	// contiguous literal, so it doesn't look like a real credential to
	// GitHub's own push-protection secret scanner while still matching
	// techSupportSecretScanPatterns' AWS regex at runtime once joined.
	fakeAWSKeyID := "AKIA" + "ABCDEFGHIJKLMNOP"
	content := []byte(strings.Join([]string{
		"issue body: deploying with AWS_ACCESS_KEY_ID=" + fakeAWSKeyID + " set in the environment",
		"a github token ghp_" + strings.Repeat("a", 40) + " was pasted into a review comment",
		"-----BEGIN RSA PRIVATE KEY-----",
		"MIIBogIBAAJBAK...",
		"-----END RSA PRIVATE KEY-----",
		"the customer's api_key: abcdef123456 needs rotating", // gitleaks:allow
		"unrelated prose about a cache_key field, no assignment here",
	}, "\n"))

	summary := scanForSuspectedSecrets(content)

	byName := make(map[string]techSupportSecretScanCategory)
	for _, category := range summary.Categories {
		byName[category.Name] = category
	}

	if got := byName["AWS access keys"]; got.Found != 1 {
		t.Fatalf("AWS access keys = %+v, want Found=1", got)
	}
	if got := byName["GitHub tokens"]; got.Found != 1 {
		t.Fatalf("GitHub tokens = %+v, want Found=1", got)
	}
	if got := byName["Private key material"]; got.Found != 1 {
		t.Fatalf("Private key material = %+v, want Found=1", got)
	}
	if got := byName["Generic credential-style assignment"]; got.Suspected < 1 {
		t.Fatalf("Generic credential-style assignment = %+v, want Suspected >= 1", got)
	}
	if got := byName["Slack tokens/webhooks"]; got.Found != 0 || got.Suspected != 0 {
		t.Fatalf("Slack tokens/webhooks = %+v, want no matches", got)
	}
	if !summary.AnyMatches() {
		t.Fatal("AnyMatches() = false, want true")
	}
}

func TestScanForSuspectedSecretsNoMatches(t *testing.T) {
	summary := scanForSuspectedSecrets([]byte("just an ordinary issue body with no credentials in it"))
	if summary.AnyMatches() {
		t.Fatalf("AnyMatches() = true, want false for content with no secret-like patterns")
	}
	warning := formatTechSupportSecretScanWarning(summary)
	if !strings.Contains(warning, "no matches in any scanned category") {
		t.Fatalf("formatTechSupportSecretScanWarning(no matches) = %q, want it to say so explicitly", warning)
	}
}

func TestFormatTechSupportSecretScanWarningReportsFoundAndSuspectedSeparately(t *testing.T) {
	summary := techSupportSecretScanSummary{Categories: []techSupportSecretScanCategory{
		{Name: "AWS access keys", Found: 2},
		{Name: "Generic credential-style assignment", Suspected: 3},
		{Name: "GitHub tokens"}, // no matches; must not appear in the warning
	}}
	warning := formatTechSupportSecretScanWarning(summary)
	if !strings.Contains(warning, "AWS access keys: 2 found") {
		t.Fatalf("warning missing AWS found count: %q", warning)
	}
	if !strings.Contains(warning, "Generic credential-style assignment: 3 suspected") {
		t.Fatalf("warning missing generic suspected count: %q", warning)
	}
	if strings.Contains(warning, "GitHub tokens") {
		t.Fatalf("warning should omit categories with zero matches: %q", warning)
	}
	if !strings.Contains(warning, "agents_state.json") {
		t.Fatalf("warning should name the file that carries raw content: %q", warning)
	}
}

// TestGenerateTechSupportBundleReturnsSecretScanForPlantedCredential is an
// end-to-end check that generateTechSupportBundle actually scans
// agents_state.json (not some other file) and returns a non-empty
// summary when a coder's issue body -- which lands in agents_state.json
// verbatim -- contains something secret-like.
func TestGenerateTechSupportBundleReturnsSecretScanForPlantedCredential(t *testing.T) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	coder := &Agent{
		ID:          "coding-agent-980",
		Role:        RoleCoder,
		IssueNumber: 980,
		// Concatenated, not a contiguous literal, for the same reason as
		// TestScanForSuspectedSecretsCategorizesFoundAndSuspected above.
		IssueBody:        "deploy notes: AWS_ACCESS_KEY_ID=" + "AKIA" + "ABCDEFGHIJKLMNOP",
		PRNumber:         990,
		State:            StateWorking,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget", LogDir: logDir},
		agents: agents,
	}
	if err := bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}

	_, secretScan, err := bot.generateTechSupportBundle()
	if err != nil {
		t.Fatalf("generateTechSupportBundle() error = %v", err)
	}
	if !secretScan.AnyMatches() {
		t.Fatal("secretScan.AnyMatches() = false, want true for a planted AWS access key in the coder's issue body")
	}
}

func TestGenerateTechSupportBundleContainsExpectedEntries(t *testing.T) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	coder := &Agent{
		ID:               "coding-agent-980",
		Role:             RoleCoder,
		IssueNumber:      980,
		PRNumber:         990,
		State:            StateWorking,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
			LogDir:    logDir,
		},
		agents: agents,
	}
	if err := bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}
	daemonLogPath := filepath.Join(logDir, orchestratorDaemonLogName)
	if err := os.WriteFile(daemonLogPath, []byte("daemon log line one\ndaemon log line two\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(daemon log) error = %v", err)
	}

	bundlePath, _, err := bot.generateTechSupportBundle()
	if err != nil {
		t.Fatalf("generateTechSupportBundle() error = %v", err)
	}
	if !strings.HasSuffix(bundlePath, ".tar.gz") {
		t.Fatalf("bundlePath = %q, want a .tar.gz path", bundlePath)
	}
	if _, err := os.Stat(bundlePath); err != nil {
		t.Fatalf("Stat(%s) error = %v", bundlePath, err)
	}

	entries := readTarGzEntries(t, bundlePath)
	for _, name := range []string{
		"version.txt",
		"config.txt",
		"status.txt",
		"agents.txt",
		"budget-analysis.txt",
		"agents_state.json",
		"daemon.log.tail",
	} {
		if _, ok := entries[name]; !ok {
			t.Fatalf("bundle missing entry %q; got entries=%v", name, mapKeys(entries))
		}
	}
	if !strings.Contains(entries["version.txt"], "repository-agent-orchestrator") {
		t.Fatalf("version.txt = %q, want it to identify the binary", entries["version.txt"])
	}
	if !strings.Contains(entries["status.txt"], "acme/widget") {
		t.Fatalf("status.txt missing repo identity: %q", entries["status.txt"])
	}
	if !strings.Contains(entries["agents_state.json"], "coding-agent-980") {
		t.Fatalf("agents_state.json missing persisted agent: %q", entries["agents_state.json"])
	}
	if !strings.Contains(entries["daemon.log.tail"], "daemon log line two") {
		t.Fatalf("daemon.log.tail missing expected content: %q", entries["daemon.log.tail"])
	}
	if !strings.Contains(entries["budget-analysis.txt"], "Budget warnings") {
		t.Fatalf("budget-analysis.txt missing content: %q", entries["budget-analysis.txt"])
	}
	if !strings.Contains(entries["config.txt"], "acme/widget") {
		t.Fatalf("config.txt missing effective config: %q", entries["config.txt"])
	}
}

func TestDeduplicateLogLinesCollapsesRepeatedMessage(t *testing.T) {
	text := "2026/08/21 08:18:21.397806 persisted review cycle recovery failed reviewer=review-agent-980: convergent review coordinator is not active\n" +
		"2026/08/21 08:18:41.397806 persisted review cycle recovery failed reviewer=review-agent-980: convergent review coordinator is not active\n" +
		"2026/08/21 08:19:01.397806 persisted review cycle recovery failed reviewer=review-agent-980: convergent review coordinator is not active\n" +
		"2026/08/21 16:02:44.112233 review launch skipped coder=coding-agent-980 pr=990: active reviewer review-agent-980 still owns head\n"

	got := deduplicateLogLines(text)

	if strings.Count(got, "convergent review coordinator is not active") != 1 {
		t.Fatalf("deduplicateLogLines() did not collapse the repeated line: %q", got)
	}
	if !strings.Contains(got, "repeated 2 more time(s)") {
		t.Fatalf("deduplicateLogLines() missing repeat count: %q", got)
	}
	if !strings.Contains(got, "08:18:21.397806") || !strings.Contains(got, "08:19:01.397806") {
		t.Fatalf("deduplicateLogLines() missing time span of the repeated run: %q", got)
	}
	if !strings.Contains(got, "still owns head") {
		t.Fatalf("deduplicateLogLines() dropped the differing final line: %q", got)
	}
}

func TestDeduplicateLogLinesLeavesNonRepeatingLinesUnchanged(t *testing.T) {
	text := "2026/08/21 08:18:21.397806 first message\n" +
		"2026/08/21 08:18:41.397806 second message\n" +
		"2026/08/21 08:19:01.397806 third message\n"

	if got := deduplicateLogLines(text); got != text {
		t.Fatalf("deduplicateLogLines() = %q, want unchanged %q", got, text)
	}
}

func TestDeduplicateLogLinesHandlesNonTimestampedLines(t *testing.T) {
	text := `{"timestamp":"2026-08-21T08:18:21Z","event":"cycle_running"}` + "\n" +
		`{"timestamp":"2026-08-21T08:18:21Z","event":"cycle_running"}` + "\n"

	got := deduplicateLogLines(text)
	if !strings.Contains(got, "repeated 1 more time(s)") {
		t.Fatalf("deduplicateLogLines() did not collapse identical non-timestamped lines: %q", got)
	}
}

func TestSummarizedLogTailCompressesAnOvernightSpamLoop(t *testing.T) {
	logDir := t.TempDir()
	daemonLogPath := filepath.Join(logDir, orchestratorDaemonLogName)

	var builder strings.Builder
	start := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	// ~8 hours of the exact same message every 20 seconds -- roughly the
	// scenario reported: leave it running overnight, come back to the same
	// error every few dozen seconds for hours.
	for i := 0; i < 1440; i++ {
		ts := start.Add(time.Duration(i) * 20 * time.Second)
		builder.WriteString(fmt.Sprintf(
			"%s persisted review cycle recovery failed reviewer=review-agent-980: convergent review coordinator is not active\n",
			ts.Format("2006/01/02 15:04:05.000000"),
		))
	}
	builder.WriteString(fmt.Sprintf(
		"%s review launch skipped coder=coding-agent-980 pr=990: active reviewer review-agent-980 still owns head\n",
		start.Add(1440*20*time.Second).Format("2006/01/02 15:04:05.000000"),
	))
	if err := os.WriteFile(daemonLogPath, []byte(builder.String()), 0o644); err != nil {
		t.Fatalf("WriteFile(daemon log) error = %v", err)
	}

	tail, err := summarizedLogTail(daemonLogPath, techSupportDaemonLogRawReadBytes, techSupportDaemonLogTailBytes)
	if err != nil {
		t.Fatalf("summarizedLogTail() error = %v", err)
	}
	if int64(len(tail)) >= techSupportDaemonLogRawReadBytes {
		t.Fatalf("summarizedLogTail() did not compress the spam loop: %d bytes", len(tail))
	}
	if strings.Count(string(tail), "convergent review coordinator is not active") != 1 {
		t.Fatalf("summarizedLogTail() should mention the repeated message exactly once: %q", tail)
	}
	if !strings.Contains(string(tail), "repeated 1439 more time(s)") {
		t.Fatalf("summarizedLogTail() missing accurate repeat count: %q", tail)
	}
	if !strings.Contains(string(tail), "still owns head") {
		t.Fatalf("summarizedLogTail() should still reach the one differing line: %q", tail)
	}
}

func TestGenerateTechSupportBundleRequiresLogDir(t *testing.T) {
	bot := &Orchestrator{agents: NewAgentManager()}
	if _, _, err := bot.generateTechSupportBundle(); err == nil ||
		!strings.Contains(err.Error(), "log directory is not configured") {
		t.Fatalf("generateTechSupportBundle() error = %v, want missing log directory", err)
	}
}

// TestGenerateTechSupportBundleRemovesPartialArchiveOnFailure is the
// regression test for independent review feedback: most error returns
// after os.Create (a write failure, or a non-"doesn't exist" failure
// reading agents_state.json/the daemon log) left the partially-written
// archive behind at its now-uniquely-named, timestamped path -- easy to
// mistake for a real, complete capture later. Forces exactly that: the
// persisted-state path exists but is a directory, so os.ReadFile fails
// with something other than IsNotExist after version.txt/config.txt/
// status.txt/agents.txt/budget-analysis.txt have already been written to
// the tar stream.
func TestGenerateTechSupportBundleRemovesPartialArchiveOnFailure(t *testing.T) {
	logDir := t.TempDir()
	agents := NewAgentManager()
	bot := &Orchestrator{
		cfg:    Config{RepoOwner: "acme", RepoName: "widget", LogDir: logDir},
		agents: agents,
	}

	statePath := bot.agentStateFilePath()
	if err := os.MkdirAll(statePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", statePath, err)
	}

	if _, _, err := bot.generateTechSupportBundle(); err == nil ||
		!strings.Contains(err.Error(), "failed to read persisted agent state") {
		t.Fatalf("generateTechSupportBundle() error = %v, want a persisted-agent-state read failure", err)
	}

	bundleDir := filepath.Join(logDir, "tech-support")
	entries, err := os.ReadDir(bundleDir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", bundleDir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("tech-support directory has %d leftover entries after a failed capture, want 0: %v", len(entries), entries)
	}
}

func TestRunREPLTechSupportCommand(t *testing.T) {
	logDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner: "acme",
			RepoName:  "widget",
			LogDir:    logDir,
		},
		agents: NewAgentManager(),
	}

	output, err := runREPLForTest(t, "tech-support\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "tech-support bundle written to") {
		t.Fatalf("runREPL output missing bundle confirmation: %q", output)
	}
	if !strings.Contains(output, filepath.Join(logDir, "tech-support")) {
		t.Fatalf("runREPL output missing bundle path under log dir: %q", output)
	}
}

func TestRunREPLTechSupportCommandReportsError(t *testing.T) {
	bot := &Orchestrator{agents: NewAgentManager()}

	output, err := runREPLForTest(t, "tech-support\nquit\n", bot)
	if err != nil {
		t.Fatalf("runREPL() error = %v", err)
	}
	if !strings.Contains(output, "error:") {
		t.Fatalf("runREPL output missing error: %q", output)
	}
}

func mapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
