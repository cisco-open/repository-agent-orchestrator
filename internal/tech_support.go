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
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// techSupportDaemonLogTailBytes bounds the *deduplicated* daemon log excerpt
// a bundle captures, so a long-running deployment's log history never makes
// the bundle unpredictably large.
const techSupportDaemonLogTailBytes = 2 << 20 // 2 MiB

// techSupportDaemonLogRawReadBytes bounds the raw (pre-deduplication) read
// window. It is deliberately much larger than the final output budget: a
// long-running deployment that logs the same failure every few seconds for
// hours produces a raw tail that is almost entirely one repeated line, which
// deduplicateLogLines collapses to a handful of lines -- reading a larger
// raw window before deduplicating is what lets the final output actually
// reach back to the last *interesting* (non-repeated) content instead of
// spending its whole budget on one repeated message.
const techSupportDaemonLogRawReadBytes = 32 << 20 // 32 MiB

// logLineTimestampPattern matches the log package's LstdFlags|Lmicroseconds
// prefix (e.g. "2026/08/21 08:18:21.397806 "), which setupProcessLogger
// configures for the daemon log. Lines that don't match (for example the
// raw structured JSON event lines written directly to the log file) are
// compared and deduplicated as whole lines instead.
var logLineTimestampPattern = regexp.MustCompile(
	`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} `,
)

// logLineMessage strips a std-log timestamp prefix, if present, so two
// occurrences of the same message at different times compare equal.
func logLineMessage(line string) string {
	if loc := logLineTimestampPattern.FindStringIndex(line); loc != nil {
		return line[loc[1]:]
	}
	return line
}

// logLineTimestamp returns the leading timestamp text, or "" if the line
// doesn't have one.
func logLineTimestamp(line string) string {
	match := logLineTimestampPattern.FindString(line)
	return strings.TrimSpace(match)
}

// deduplicateLogLines collapses runs of consecutive lines that carry the
// same message (ignoring each line's own timestamp) into the first
// occurrence plus a one-line summary of how many more followed and over
// what time span -- the same idea as syslog's "last message repeated N
// times". This is what makes an overnight run that logs one recovery
// failure every few dozen seconds still fit a useful amount of history
// into a bounded excerpt, instead of the excerpt being nothing but that one
// repeated line.
func deduplicateLogLines(text string) string {
	if text == "" {
		return text
	}
	trailingNewline := strings.HasSuffix(text, "\n")
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); {
		message := logLineMessage(lines[i])
		j := i + 1
		for j < len(lines) && logLineMessage(lines[j]) == message {
			j++
		}
		out = append(out, lines[i])
		repeats := j - i - 1
		if repeats > 0 {
			firstTS := logLineTimestamp(lines[i])
			lastTS := logLineTimestamp(lines[j-1])
			switch {
			case firstTS != "" && lastTS != "":
				out = append(out, fmt.Sprintf(
					"[... previous line repeated %d more time(s), %s .. %s ...]",
					repeats, firstTS, lastTS,
				))
			default:
				out = append(out, fmt.Sprintf(
					"[... previous line repeated %d more time(s) ...]",
					repeats,
				))
			}
		}
		i = j
	}
	result := strings.Join(out, "\n")
	if trailingNewline {
		result += "\n"
	}
	return result
}

// tailTextBytes returns the last maxBytes of text, aligned to a line
// boundary where possible so the excerpt doesn't open mid-line.
func tailTextBytes(text string, maxBytes int) string {
	if len(text) <= maxBytes {
		return text
	}
	start := len(text) - maxBytes
	if next := strings.IndexByte(text[start:], '\n'); next >= 0 {
		start += next + 1
	}
	return fmt.Sprintf(
		"[... truncated %d earlier bytes ...]\n%s",
		start, text[start:],
	)
}

// captureREPLOutput runs fn with the REPL's print output (replPrintf/
// replPrintln) redirected into an in-memory buffer and returns everything
// it wrote. The REPL's rendering functions (printStatus, printAgents, ...)
// write through replPrintf; this lets a tech-support bundle reuse that
// exact rendering without duplicating it.
//
// This used to redirect the real os.Stdout through an os.Pipe and read it
// back with io.ReadAll after fn() returned. That has a real deadlock risk:
// a pipe's kernel buffer is bounded (commonly 64KiB on Linux), and nothing
// was reading from it while fn() was still writing, so any renderer
// producing more output than that -- plausible for printStatus/printAgents
// with many active agents and the Review Cycle Health section -- would
// block on the write forever, hanging generateTechSupportBundle
// permanently. Swapping in a *bytes.Buffer instead has no such limit (it
// just grows) and needs no concurrent reader.
//
// replOutputOverride is a single, process-global slot: this function does
// not serialize concurrent calls to itself, because its only caller
// (generateTechSupportBundle) already holds techSupportBundleMu for its
// entire duration, including both of its captureREPLOutput calls. Do not
// call this from anywhere that isn't already holding that same
// serialization guarantee.
//
// This does not fully serialize against *all* REPL output: an interactive
// command's replPrintf calls issued while a capture is holding
// replOutputOverride will still be redirected into that capture's buffer
// rather than the terminal (the operator's command echo would be missing
// from their screen momentarily, and show up in the tech-support bundle
// instead). That narrow residual is accepted rather than solved here --
// fully closing it would mean making every REPL command's dispatch, not
// just this capture, participate in the same lock, which is a much larger
// change to the REPL's command loop unrelated to tech-support itself.
// Accepting it is safe specifically because replPrintf (app.go) holds
// replOutputMu for the write itself, not just the lookup: a stray
// interactive write during a capture lands in the wrong place
// (misdirected output, a UX annoyance), but it can never race the
// capture's own writes to the same *bytes.Buffer (which would otherwise be
// a real data race, since bytes.Buffer isn't safe for concurrent use).
//
// The restore is deferred so a panic inside fn() can never leave
// replOutputOverride pointing at this (about-to-be-discarded) buffer
// forever -- without that, every later replPrintf call in the process,
// including in the interactive REPL, would silently keep writing into a
// buffer nobody reads instead of the terminal.
func captureREPLOutput(fn func()) string {
	var buf bytes.Buffer
	replOutputMu.Lock()
	replOutputOverride = &buf
	replOutputMu.Unlock()

	defer func() {
		replOutputMu.Lock()
		replOutputOverride = nil
		replOutputMu.Unlock()
	}()

	fn()

	// Read buf.String() while holding replOutputMu, matching every
	// replPrintf call's own lock around its write to this same buffer
	// (see replPrintf in app.go) -- otherwise this read races a concurrent
	// replPrintf's write to the shared *bytes.Buffer, which isn't safe for
	// concurrent use. The override itself is only cleared afterward, by
	// the deferred unlock above, so a write landing between this read and
	// that clear is possible but benign (same accepted trade-off as the
	// "narrow residual" documented above: at worst it's a write that
	// arrived too late to be included in the captured string, never a
	// corrupted read).
	replOutputMu.Lock()
	result := buf.String()
	replOutputMu.Unlock()
	return result
}

// tailFileBytes returns up to maxBytes from the end of the file at path. It
// is not exact-line-aligned; the first partial line of the result may be
// truncated, which is an acceptable tradeoff for a bounded, cheap tail read
// that never loads an entire (potentially large) log file into memory.
func tailFileBytes(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	start := int64(0)
	if size > maxBytes {
		start = size - maxBytes
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	if start > 0 {
		header := fmt.Sprintf(
			"[... truncated %d earlier bytes ...]\n",
			start,
		)
		return append([]byte(header), data...), nil
	}
	return data, nil
}

// summarizedLogTail reads a bounded raw window from the end of the file at
// path, collapses repeated consecutive messages, and returns a
// deduplicated excerpt bounded to maxOutputBytes. See
// techSupportDaemonLogRawReadBytes for why the raw read window is much
// larger than the final output bound.
func summarizedLogTail(path string, rawWindowBytes, maxOutputBytes int64) ([]byte, error) {
	raw, err := tailFileBytes(path, rawWindowBytes)
	if err != nil {
		return nil, err
	}
	deduped := deduplicateLogLines(string(raw))
	return []byte(tailTextBytes(deduped, int(maxOutputBytes))), nil
}

// generateTechSupportBundle gathers everything needed to diagnose a stalled
// or misbehaving deployment into a single tar.gz, mirroring `show
// tech-support` on Cisco platforms: one artifact an operator can hand off or
// transfer (e.g. via scp) without needing interactive copy/paste from the
// terminal running the orchestrator, and without needing to remember which
// several files and commands matter.
//
// The bundle contains: the effective (fully-defaulted, not just what
// config.yaml set explicitly) configuration and REVIEW_POLICY
// (formatEffectiveConfigReport, formatReviewPolicyStatus via status.txt --
// both already verified secret-safe by
// TestReviewPolicyObservabilityOmitsSecretsAndCommands, with
// formatEffectiveConfigReport additionally redacting any CodexCmd/
// MandatoryTests entry that looks credential-bearing rather than printing
// it verbatim), what printStatus/printAgents already render, the persisted
// state file (agents_state.json), and a bounded daemon log tail.
//
// The orchestrator itself never embeds a credential it manages in any of
// this (see README "Security Notes" -- GitHub/Webex auth is read from the
// environment or `gh auth token`, never persisted to agent state or
// logged). That is not, however, a guarantee that the bundle is free of
// sensitive content: agents_state.json is included verbatim, and it
// carries raw, unredacted, human-authored text -- issue titles/bodies,
// human review guidance, PR titles, and finding/verification/ledger
// summaries pulled from GitHub -- none of which the orchestrator
// validates or sanitizes. If a person pasted a credential, customer data,
// or other sensitive material into an issue or comment, it is present
// here exactly as written. Treat a generated bundle with the same care as
// the raw agents_state.json file it's built from: fine to move between
// trusted operators/systems (e.g. scp to another host you control), but
// not something to attach to a third-party support ticket or otherwise
// send outside that trust boundary without reviewing its contents first.
//
// Collision-safe against overlapping calls (this can be triggered both
// interactively via the REPL `tech-support` command and automatically by
// checkReviewCycleHealth on an unknown_stall, so overlap is a real
// possibility, not just a theoretical one): techSupportBundleMu
// serializes the entire call end to end, and the bundle filename includes
// a random suffix in addition to a second-precision timestamp, so even if
// that serialization were ever removed or bypassed, two calls can never
// compute the same path and race on os.Create (which truncates).
// techSupportSecretScanCategory is one category of secret-like content a
// tech-support bundle was scanned for, split into two confidence tiers:
// Found (matched a specific, well-known credential format -- AWS access
// key IDs, GitHub tokens, PEM private key blocks, etc. -- essentially
// unambiguous) and Suspected (matched only a generic "this looks like a
// KEY=value/token: value assignment" heuristic, which is far more prone to
// false positives on ordinary prose, e.g. a sentence discussing a
// "cache_key" field). Reported separately, rather than merged into one
// count, so an operator can tell "there is almost certainly a real
// credential in here" from "there might be, take a closer look" at a
// glance instead of every hit reading the same regardless of confidence.
type techSupportSecretScanCategory struct {
	Name      string
	Found     int
	Suspected int
}

// techSupportSecretScanSummary is the result of scanning a bundle's raw,
// unredacted content (agents_state.json -- see generateTechSupportBundle's
// documentation of why that file specifically carries raw human-authored
// text) for secret-like patterns. It exists to make the risk documented
// there visible to whoever actually generates a bundle, not just to
// whoever reads the code or docs/TROUBLESHOOTING.md.
type techSupportSecretScanSummary struct {
	Categories []techSupportSecretScanCategory
}

// AnyMatches reports whether any category found or suspected at least one
// match, so callers can decide whether a scan-results warning is worth
// printing at all.
func (s techSupportSecretScanSummary) AnyMatches() bool {
	for _, category := range s.Categories {
		if category.Found > 0 || category.Suspected > 0 {
			return true
		}
	}
	return false
}

// techSupportSecretScanPatterns defines each category's Found (specific,
// well-known format) and, where a meaningfully distinct heuristic exists,
// Suspected (generic assignment-shaped) patterns. A category with no
// Suspected pattern relies on Found alone being specific enough that a
// separate, noisier heuristic wouldn't add useful signal (a PEM private
// key header, for instance, is not the kind of thing that shows up by
// coincidence).
var techSupportSecretScanPatterns = []struct {
	Name      string
	Found     *regexp.Regexp
	Suspected *regexp.Regexp
}{
	{
		Name:  "AWS access keys",
		Found: regexp.MustCompile(`\b(AKIA|ASIA)[0-9A-Z]{16}\b`),
	},
	{
		Name:  "GitHub tokens",
		Found: regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}\b`),
	},
	{
		Name:  "Slack tokens/webhooks",
		Found: regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]+\b|hooks\.slack\.com/services/\S+`),
	},
	{
		Name:  "Private key material",
		Found: regexp.MustCompile(`-----BEGIN (RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----`),
	},
	{
		Name:  "JWT-looking tokens",
		Found: regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`),
	},
	{
		// Deliberately no Found pattern -- there is no well-known format for
		// a generic API key, only the shape of the assignment around it
		// (mirrors credentialBearingAssignmentPattern in
		// effective_config_report.go, applied here to free-form prose
		// rather than a single config value).
		Name: "Generic credential-style assignment",
		Suspected: regexp.MustCompile(
			`(?i)\b[a-z0-9_.-]*(token|password|passwd|secret|api[_-]?key|private[_-]?key)[a-z0-9_.-]*\s*[:=]\s*\S+`,
		),
	},
}

// scanForSuspectedSecrets scans content (a bundle file's raw bytes) for
// secret-like patterns, returning per-category Found/Suspected counts. It
// does not redact or otherwise alter content -- see the type documentation
// above for why this exists purely to make an already-accepted risk
// visible, not to close it.
func scanForSuspectedSecrets(content []byte) techSupportSecretScanSummary {
	categories := make([]techSupportSecretScanCategory, 0, len(techSupportSecretScanPatterns))
	for _, pattern := range techSupportSecretScanPatterns {
		category := techSupportSecretScanCategory{Name: pattern.Name}
		if pattern.Found != nil {
			category.Found = len(pattern.Found.FindAll(content, -1))
		}
		if pattern.Suspected != nil {
			category.Suspected = len(pattern.Suspected.FindAll(content, -1))
		}
		categories = append(categories, category)
	}
	return techSupportSecretScanSummary{Categories: categories}
}

// formatTechSupportSecretScanWarning renders summary as the human-readable
// warning printed when a bundle is generated (see the "tech-support" REPL
// command and its automatic-capture caller in checkReviewCycleHealth).
// Always states that a scan happened, even when nothing matched, so an
// operator never has to wonder whether the absence of a warning meant "no
// scan ran" versus "the scan found nothing."
func formatTechSupportSecretScanWarning(summary techSupportSecretScanSummary) string {
	var b strings.Builder
	b.WriteString("This bundle includes agents_state.json, which carries raw, unredacted, human-authored text (issue bodies, review guidance, PR titles, finding/verification/ledger summaries) -- see docs/TROUBLESHOOTING.md before sharing it outside this environment.\n")
	b.WriteString("Its content was scanned for common secret-like patterns:\n")
	if !summary.AnyMatches() {
		b.WriteString("  no matches in any scanned category\n")
		return b.String()
	}
	for _, category := range summary.Categories {
		if category.Found == 0 && category.Suspected == 0 {
			continue
		}
		switch {
		case category.Found > 0 && category.Suspected > 0:
			fmt.Fprintf(&b, "  %s: %d found, %d suspected\n", category.Name, category.Found, category.Suspected)
		case category.Found > 0:
			fmt.Fprintf(&b, "  %s: %d found\n", category.Name, category.Found)
		default:
			fmt.Fprintf(&b, "  %s: %d suspected\n", category.Name, category.Suspected)
		}
	}
	return b.String()
}

func (b *Orchestrator) generateTechSupportBundle() (string, techSupportSecretScanSummary, error) {
	if b == nil || b.agents == nil {
		return "", techSupportSecretScanSummary{}, errors.New("orchestrator is not initialized")
	}
	b.techSupportBundleMu.Lock()
	defer b.techSupportBundleMu.Unlock()

	logDir := strings.TrimSpace(b.cfg.LogDir)
	if logDir == "" {
		return "", techSupportSecretScanSummary{}, errors.New("log directory is not configured")
	}
	bundleDir := filepath.Join(logDir, "tech-support")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to create tech-support directory: %w", err)
	}
	suffix, err := randomReviewWorkerToken()
	if err != nil {
		return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to allocate a unique tech-support bundle name: %w", err)
	}
	bundlePath := filepath.Join(
		bundleDir,
		fmt.Sprintf("tech-support-%s-%s.tar.gz", time.Now().UTC().Format("20060102T150405Z"), suffix),
	)

	statusText := captureREPLOutput(func() { printStatus(b) })
	agentsText := captureREPLOutput(func() { printAgents(b) })
	budgetText := formatReviewBudgetReport(b.cfg.ReviewPolicy, b.agents.List())

	file, err := os.Create(bundlePath)
	if err != nil {
		return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to create tech-support bundle: %w", err)
	}
	// Any return below before succeeded is set true (a write failure while
	// adding a file, or a failure reading agents_state.json/the daemon log
	// for a reason other than "doesn't exist yet") must not leave a
	// partial archive behind at this now-uniquely-named, easy-to-mistake-
	// for-a-real-capture path. Registered before the file/gzip/tar Close
	// defers so it runs last (LIFO) -- only after everything is actually
	// closed, not while still open.
	succeeded := false
	defer func() {
		if !succeeded {
			_ = os.Remove(bundlePath)
		}
	}()
	defer file.Close()
	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	addFile := func(name string, contents []byte) error {
		header := &tar.Header{
			Name:    name,
			Mode:    0o600,
			Size:    int64(len(contents)),
			ModTime: time.Now().UTC(),
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write %s header: %w", name, err)
		}
		if _, err := tarWriter.Write(contents); err != nil {
			return fmt.Errorf("failed to write %s: %w", name, err)
		}
		return nil
	}

	if err := addFile("version.txt", []byte(buildVersionString()+"\n")); err != nil {
		return "", techSupportSecretScanSummary{}, err
	}
	if err := addFile("config.txt", []byte(formatEffectiveConfigReport(b.cfg))); err != nil {
		return "", techSupportSecretScanSummary{}, err
	}
	if err := addFile("status.txt", []byte(statusText)); err != nil {
		return "", techSupportSecretScanSummary{}, err
	}
	if err := addFile("agents.txt", []byte(agentsText)); err != nil {
		return "", techSupportSecretScanSummary{}, err
	}
	if err := addFile("budget-analysis.txt", []byte(budgetText)); err != nil {
		return "", techSupportSecretScanSummary{}, err
	}

	var secretScan techSupportSecretScanSummary
	if statePath := b.agentStateFilePath(); statePath != "" {
		data, err := os.ReadFile(statePath)
		switch {
		case err == nil:
			// Scanned before archiving, not after: this is the one file in
			// the bundle documented above as carrying raw, unredacted
			// human-authored text, so it's the one worth telling the
			// operator about up front (see formatTechSupportSecretScanWarning).
			secretScan = scanForSuspectedSecrets(data)
			if err := addFile("agents_state.json", data); err != nil {
				return "", techSupportSecretScanSummary{}, err
			}
		case os.IsNotExist(err):
			// No persisted state yet (e.g. a fresh deployment); not an
			// error, just nothing to include.
		default:
			return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to read persisted agent state: %w", err)
		}
	}

	daemonLogPath := filepath.Join(logDir, orchestratorDaemonLogName)
	tail, err := summarizedLogTail(
		daemonLogPath,
		techSupportDaemonLogRawReadBytes,
		techSupportDaemonLogTailBytes,
	)
	switch {
	case err == nil:
		if err := addFile("daemon.log.tail", tail); err != nil {
			return "", techSupportSecretScanSummary{}, err
		}
	case os.IsNotExist(err):
		// No daemon log yet.
	default:
		return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to read daemon log: %w", err)
	}

	// gzip.Writer.Close and tar.Writer.Close are where the final
	// bytes/footers actually get written (tar's end-of-archive marker,
	// gzip's trailer/flush) -- a disk-full or writeback error surfaces
	// here, not at any earlier Write call. The deferred bare Close calls
	// above exist only as a safety net for the error-return paths already
	// taken; checking (and closing in the correct tar-then-gzip-then-file
	// order) explicitly here is what keeps a late failure from being
	// silently swallowed on the success path. A failure at this point
	// means bundlePath is a truncated/corrupt archive, not a usable one;
	// the deferred cleanup above removes it since succeeded is never set.
	if err := tarWriter.Close(); err != nil {
		return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to finalize tech-support archive (tar): %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to finalize tech-support archive (gzip): %w", err)
	}
	if err := file.Close(); err != nil {
		return "", techSupportSecretScanSummary{}, fmt.Errorf("failed to finalize tech-support archive (file): %w", err)
	}

	succeeded = true
	return bundlePath, secretScan, nil
}
