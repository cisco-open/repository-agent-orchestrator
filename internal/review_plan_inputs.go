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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	reviewPlanInputsSchemaVersion  = 1
	reviewPlanDiffBigFileThreshold = "512m"
	reviewPlanDiffAlgorithm        = "myers"
	reviewPlanDiffRenameThreshold  = "50%"
)

// ReviewPlanInputs is the normalized, classification-free input record for one
// exact diff.
type ReviewPlanInputs struct {
	SchemaVersion      int                     `json:"schema_version"`
	BaseSHA            string                  `json:"base_sha"`
	HeadSHA            string                  `json:"head_sha"`
	ChangedFileCount   int                     `json:"changed_file_count"`
	ChangedLineCount   int64                   `json:"changed_line_count"`
	ChangedFiles       []ReviewPlanChangedFile `json:"changed_files"`
	TaskScope          []string                `json:"task_scope"`
	NonGoals           []string                `json:"non_goals"`
	AcceptanceCriteria []string                `json:"acceptance_criteria"`
}

type ReviewPlanChangedFile struct {
	Path          string            `json:"path"`
	PreviousPath  string            `json:"previous_path,omitempty"`
	Additions     int64             `json:"additions"`
	Deletions     int64             `json:"deletions"`
	Binary        bool              `json:"binary"`
	ChangedRanges []ReviewLineRange `json:"changed_ranges"`
}

type ReviewLineRange struct {
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
	Symbol    string `json:"symbol,omitempty"`
}

// collectReviewPlanInputs reads only an exact base/head diff. The checked-out
// HEAD must still match the requested head so collection cannot silently use a
// stale or incorrectly prepared worktree.
func collectReviewPlanInputs(
	ctx context.Context,
	repoPath string,
	baseSHA string,
	headSHA string,
	taskBody string,
) (ReviewPlanInputs, error) {
	repoPath = strings.TrimSpace(repoPath)
	if repoPath == "" {
		return ReviewPlanInputs{}, errors.New("review-plan input repo path is empty")
	}
	baseSHA, err := canonicalReviewPlanSHA("base", baseSHA)
	if err != nil {
		return ReviewPlanInputs{}, err
	}
	headSHA, err = canonicalReviewPlanSHA("head", headSHA)
	if err != nil {
		return ReviewPlanInputs{}, err
	}

	checkedOut, err := outputReviewPlanGitCommand(ctx, repoPath, nil, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return ReviewPlanInputs{}, fmt.Errorf("failed to resolve checked-out review head: %w", err)
	}
	actualHead := strings.ToLower(strings.TrimSpace(string(checkedOut)))
	if err := validateCanonicalGitObjectID(actualHead); err != nil {
		return ReviewPlanInputs{}, fmt.Errorf("checked-out review head is invalid: %w", err)
	}
	if actualHead != headSHA {
		return ReviewPlanInputs{}, fmt.Errorf(
			"review-plan head mismatch: expected %s, checked out %s",
			abbreviateSHA(headSHA),
			abbreviateSHA(actualHead),
		)
	}

	numstat, ranges, err := collectReviewPlanDiff(ctx, repoPath, baseSHA, headSHA)
	if err != nil {
		return ReviewPlanInputs{}, fmt.Errorf(
			"failed to collect exact review diff %s..%s: %w",
			abbreviateSHA(baseSHA),
			abbreviateSHA(headSHA),
			err,
		)
	}
	files, changedLines, err := parseReviewPlanNumstat(numstat)
	if err != nil {
		return ReviewPlanInputs{}, fmt.Errorf("failed to parse exact review diff: %w", err)
	}
	for index := range files {
		files[index].ChangedRanges = ranges[files[index].Path]
		if files[index].ChangedRanges == nil {
			files[index].ChangedRanges = []ReviewLineRange{}
		}
	}

	taskScope, nonGoals, acceptanceCriteria := extractReviewTaskIntent(taskBody)
	inputs := ReviewPlanInputs{
		SchemaVersion:      reviewPlanInputsSchemaVersion,
		BaseSHA:            baseSHA,
		HeadSHA:            headSHA,
		ChangedFileCount:   len(files),
		ChangedLineCount:   changedLines,
		ChangedFiles:       files,
		TaskScope:          taskScope,
		NonGoals:           nonGoals,
		AcceptanceCriteria: acceptanceCriteria,
	}
	if err := validateReviewPlanInputs(inputs); err != nil {
		return ReviewPlanInputs{}, fmt.Errorf("collected review-plan inputs are invalid: %w", err)
	}
	return inputs, nil
}

// collectReviewPlanDiff runs the diff with only SHA-bound attributes.
// A temporary index supplies committed .gitattributes from headSHA while the
// isolated Git directory excludes the source repository's info/attributes.
// Global and system attributes are disabled by reviewPlanGitEnvironment.
func collectReviewPlanDiff(
	ctx context.Context,
	repoPath string,
	baseSHA string,
	headSHA string,
) ([]byte, map[string][]ReviewLineRange, error) {
	objectDirOutput, err := outputReviewPlanGitCommand(
		ctx,
		repoPath,
		nil,
		"rev-parse",
		"--git-path",
		"objects",
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve review object directory: %w", err)
	}
	objectDir := strings.TrimSpace(string(objectDirOutput))
	if objectDir == "" {
		return nil, nil, errors.New("resolved review object directory is empty")
	}
	if !filepath.IsAbs(objectDir) {
		objectDir = filepath.Join(repoPath, objectDir)
	}
	objectDir = filepath.Clean(objectDir)
	info, err := os.Stat(objectDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to inspect review object directory: %w", err)
	}
	if !info.IsDir() {
		return nil, nil, errors.New("resolved review object directory is not a directory")
	}

	scratchPath, err := os.MkdirTemp("", "repository-agent-orchestrator-review-plan-*")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create isolated review diff directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(scratchPath)
	}()

	repoTemplatePath := filepath.Join(scratchPath, "templates")
	isolatedRepoPath := filepath.Join(scratchPath, "repo")
	if err := os.MkdirAll(repoTemplatePath, 0o700); err != nil {
		return nil, nil, fmt.Errorf("failed to create isolated review Git template directory: %w", err)
	}
	if _, err := outputReviewPlanGitCommand(
		ctx,
		scratchPath,
		nil,
		"init",
		"--quiet",
		"--object-format=sha1",
		"--template="+repoTemplatePath,
		isolatedRepoPath,
	); err != nil {
		return nil, nil, fmt.Errorf("failed to initialize isolated review Git directory: %w", err)
	}

	isolatedGitPath := filepath.Join(isolatedRepoPath, ".git")
	isolatedEnv := []string{
		"GIT_DIR=" + isolatedGitPath,
		"GIT_WORK_TREE=" + isolatedRepoPath,
		"GIT_INDEX_FILE=" + filepath.Join(isolatedGitPath, "review-plan-index"),
		"GIT_OBJECT_DIRECTORY=" + objectDir,
	}
	if _, err := outputReviewPlanGitCommand(
		ctx,
		isolatedRepoPath,
		isolatedEnv,
		"read-tree",
		headSHA,
	); err != nil {
		return nil, nil, fmt.Errorf("failed to load exact review attributes: %w", err)
	}
	if err := materializeReviewPlanAttributes(ctx, isolatedRepoPath, isolatedEnv); err != nil {
		return nil, nil, err
	}

	commonArgs := []string{
		"-c",
		"core.attributesFile=" + os.DevNull,
		"-c",
		"core.bigFileThreshold=" + reviewPlanDiffBigFileThreshold,
		"-c",
		"diff.algorithm=" + reviewPlanDiffAlgorithm,
		"diff",
		"--find-renames=" + reviewPlanDiffRenameThreshold,
		"-l0",
		"--ignore-submodules=none",
		"--submodule=short",
		"--no-ext-diff",
		"--no-textconv",
	}
	prDiff := reviewPullRequestDiffRange(baseSHA, headSHA)
	numstatArgs := append(append([]string(nil), commonArgs...), "--numstat", "-z")
	numstatArgs = append(numstatArgs, prDiff, "--")
	numstat, err := outputReviewPlanGitCommand(
		ctx,
		isolatedRepoPath,
		isolatedEnv,
		numstatArgs...,
	)
	if err != nil {
		return nil, nil, err
	}
	patchArgs := append(
		append([]string(nil), commonArgs...),
		"--unified=0",
		"--no-color",
		"--no-prefix",
	)
	patchArgs = append(patchArgs, prDiff, "--")
	ranges, err := collectReviewPlanChangedRangesGitCommand(
		ctx,
		isolatedRepoPath,
		isolatedEnv,
		patchArgs...,
	)
	if err != nil {
		return nil, nil, err
	}
	return numstat, ranges, nil
}

func reviewPullRequestDiffRange(baseSHA, headSHA string) string {
	return baseSHA + "..." + headSHA
}

func reviewPullRequestDiffInstruction(baseSHA, headSHA string) string {
	diffRange := reviewPullRequestDiffRange(baseSHA, headSHA)
	return fmt.Sprintf(
		"The review change set is GitHub's merge-base diff `%s`; inspect it with "+
			"`git diff %s --`. Do not use a direct two-endpoint diff, because it "+
			"can include changes that are not part of the pull request.",
		diffRange,
		diffRange,
	)
}

func collectReviewPlanChangedRangesGitCommand(
	ctx context.Context,
	dir string,
	extraEnv []string,
	args ...string,
) (map[string][]ReviewLineRange, error) {
	cmd := newCommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = reviewPlanGitEnvironment(extraEnv)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.New("failed to capture exact review patch")
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("command failed: git %s", strings.Join(args, " "))
	}
	ranges, parseErr := parseReviewPlanChangedRangesReader(stdout)
	waitErr := cmd.Wait()
	if parseErr != nil {
		return nil, parseErr
	}
	if waitErr != nil {
		return nil, fmt.Errorf("command failed: git %s", strings.Join(args, " "))
	}
	return ranges, nil
}

func materializeReviewPlanAttributes(
	ctx context.Context,
	isolatedRepoPath string,
	isolatedEnv []string,
) error {
	paths, err := outputReviewPlanGitCommand(
		ctx,
		isolatedRepoPath,
		isolatedEnv,
		"ls-files",
		"-z",
		"--",
		".gitattributes",
		":(glob)**/.gitattributes",
	)
	if err != nil {
		return fmt.Errorf("failed to list exact review attributes: %w", err)
	}
	if len(paths) == 0 {
		return nil
	}

	cmd := newCommandContext(
		ctx,
		"git",
		"-c",
		"core.attributesFile="+os.DevNull,
		"checkout-index",
		"--force",
		"-z",
		"--stdin",
	)
	cmd.Dir = isolatedRepoPath
	cmd.Env = reviewPlanGitEnvironment(isolatedEnv)
	cmd.Stdin = bytes.NewReader(paths)
	if err := cmd.Run(); err != nil {
		return errors.New("failed to materialize exact review attributes")
	}
	return nil
}

func outputReviewPlanGitCommand(
	ctx context.Context,
	dir string,
	extraEnv []string,
	args ...string,
) ([]byte, error) {
	cmd := newCommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = reviewPlanGitEnvironment(extraEnv)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("command failed: git %s", strings.Join(args, " "))
	}
	return output, nil
}

func reviewPlanGitEnvironment(extraEnv []string) []string {
	baseEnv := sanitizedCommandEnv()
	filtered := make([]string, 0, len(baseEnv)+len(extraEnv)+4)
	for _, entry := range baseEnv {
		key, _, found := strings.Cut(entry, "=")
		if found && isReviewPlanGitEnvironmentOverride(key) {
			continue
		}
		filtered = append(filtered, entry)
	}
	filtered = append(
		filtered,
		"GIT_ATTR_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_NO_REPLACE_OBJECTS=1",
	)
	return append(filtered, extraEnv...)
}

func isReviewPlanGitEnvironmentOverride(key string) bool {
	switch key {
	case "GIT_ALTERNATE_OBJECT_DIRECTORIES",
		"GIT_ATTR_NOSYSTEM",
		"GIT_ATTR_SOURCE",
		"GIT_COMMON_DIR",
		"GIT_CONFIG",
		"GIT_CONFIG_COUNT",
		"GIT_CONFIG_GLOBAL",
		"GIT_CONFIG_NOSYSTEM",
		"GIT_CONFIG_PARAMETERS",
		"GIT_CONFIG_SYSTEM",
		"GIT_DIFF_OPTS",
		"GIT_DIR",
		"GIT_EXTERNAL_DIFF",
		"GIT_GLOB_PATHSPECS",
		"GIT_ICASE_PATHSPECS",
		"GIT_INDEX_FILE",
		"GIT_LITERAL_PATHSPECS",
		"GIT_NAMESPACE",
		"GIT_NOGLOB_PATHSPECS",
		"GIT_NO_REPLACE_OBJECTS",
		"GIT_OBJECT_DIRECTORY",
		"GIT_REPLACE_REF_BASE",
		"GIT_TEMPLATE_DIR",
		"GIT_WORK_TREE":
		return true
	default:
		return strings.HasPrefix(key, "GIT_CONFIG_KEY_") ||
			strings.HasPrefix(key, "GIT_CONFIG_VALUE_")
	}
}

func canonicalReviewPlanSHA(label, sha string) (string, error) {
	sha = strings.ToLower(strings.TrimSpace(sha))
	if sha == "" {
		return "", fmt.Errorf("review-plan exact %s SHA is missing", label)
	}
	if err := validateCanonicalGitObjectID(sha); err != nil {
		return "", fmt.Errorf("review-plan exact %s SHA is invalid: %w", label, err)
	}
	return sha, nil
}

func parseReviewPlanNumstat(raw []byte) ([]ReviewPlanChangedFile, int64, error) {
	if len(raw) == 0 {
		return []ReviewPlanChangedFile{}, 0, nil
	}
	if raw[len(raw)-1] != 0 {
		return nil, 0, errors.New("numstat output is not NUL-terminated")
	}

	records := bytes.Split(raw, []byte{0})
	files := make([]ReviewPlanChangedFile, 0, len(records)-1)
	var changedLines int64
	for index := 0; index < len(records)-1; index++ {
		record := records[index]
		fields := bytes.SplitN(record, []byte{'\t'}, 3)
		if len(fields) != 3 {
			return nil, 0, fmt.Errorf("numstat record %d has %d fields; expected 3", index+1, len(fields))
		}

		file := ReviewPlanChangedFile{}
		if len(fields[2]) == 0 {
			if index+2 >= len(records)-1 {
				return nil, 0, fmt.Errorf("renamed numstat record %d is missing paths", index+1)
			}
			file.PreviousPath = string(records[index+1])
			file.Path = string(records[index+2])
			index += 2
		} else {
			file.Path = string(fields[2])
		}
		if err := validateReviewPlanPath("path", file.Path); err != nil {
			return nil, 0, fmt.Errorf("numstat record %d: %w", index+1, err)
		}
		if file.PreviousPath != "" {
			if err := validateReviewPlanPath("previous path", file.PreviousPath); err != nil {
				return nil, 0, fmt.Errorf("numstat record %d: %w", index+1, err)
			}
		}

		additions, deletions, binary, err := parseReviewPlanLineCounts(fields[0], fields[1])
		if err != nil {
			return nil, 0, fmt.Errorf("numstat record for %q: %w", file.Path, err)
		}
		file.Additions = additions
		file.Deletions = deletions
		file.Binary = binary
		if additions > math.MaxInt64-deletions || changedLines > math.MaxInt64-additions-deletions {
			return nil, 0, fmt.Errorf("changed-line count overflows for %q", file.Path)
		}
		changedLines += additions + deletions
		files = append(files, file)
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].Path == files[j].Path {
			return files[i].PreviousPath < files[j].PreviousPath
		}
		return files[i].Path < files[j].Path
	})
	return files, changedLines, nil
}

func parseReviewPlanChangedRanges(
	raw []byte,
) (map[string][]ReviewLineRange, error) {
	return parseReviewPlanChangedRangesReader(bytes.NewReader(raw))
}

func parseReviewPlanChangedRangesReader(
	input io.Reader,
) (map[string][]ReviewLineRange, error) {
	ranges := make(map[string][]ReviewLineRange)
	currentPath := ""
	awaitingPath := false
	consume := func(line string) error {
		switch {
		case strings.HasPrefix(line, "diff --git "):
			currentPath = ""
			awaitingPath = true
		case awaitingPath && strings.HasPrefix(line, "+++ "):
			path, err := parseReviewPlanPatchPath(strings.TrimPrefix(line, "+++ "))
			if err != nil {
				return err
			}
			currentPath = path
			awaitingPath = false
		case strings.HasPrefix(line, "@@ "):
			if currentPath == "" {
				return nil
			}
			fields := strings.Fields(line)
			if len(fields) < 3 || !strings.HasPrefix(fields[2], "+") {
				return fmt.Errorf("invalid patch hunk header %q", line)
			}
			changed, err := parseReviewPlanNewLineRange(fields[2])
			if err != nil {
				return fmt.Errorf("invalid patch hunk header %q: %w", line, err)
			}
			changed.Symbol = parseReviewPlanHunkSymbol(line)
			ranges[currentPath] = appendReviewLineRange(
				ranges[currentPath],
				changed,
			)
		}
		return nil
	}
	reader := bufio.NewReader(input)
	oversizedLine := false
	var parseErr error
	for {
		fragment, err := reader.ReadSlice('\n')
		if !oversizedLine && err != bufio.ErrBufferFull && parseErr == nil {
			line := strings.TrimSuffix(string(fragment), "\n")
			line = strings.TrimSuffix(line, "\r")
			parseErr = consume(line)
		}
		switch err {
		case nil:
			oversizedLine = false
		case bufio.ErrBufferFull:
			oversizedLine = true
		case io.EOF:
			if parseErr != nil {
				return nil, parseErr
			}
			return ranges, nil
		default:
			return nil, fmt.Errorf("failed to read exact review patch: %w", err)
		}
	}
}

func parseReviewPlanHunkSymbol(header string) string {
	first := strings.Index(header, "@@")
	if first < 0 {
		return ""
	}
	remainder := header[first+2:]
	second := strings.Index(remainder, "@@")
	if second < 0 {
		return ""
	}
	symbol := strings.Join(strings.Fields(remainder[second+2:]), " ")
	if len(symbol) > 256 {
		symbol = symbol[:256]
	}
	return symbol
}

func parseReviewPlanPatchPath(raw string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "/dev/null" {
		return "", nil
	}
	if strings.HasPrefix(path, `"`) {
		decoded, err := strconv.Unquote(path)
		if err != nil {
			return "", fmt.Errorf("invalid quoted patch path %q", path)
		}
		path = decoded
	}
	if err := validateReviewPlanPath("patch path", path); err != nil {
		return "", err
	}
	return path, nil
}

func parseReviewPlanNewLineRange(raw string) (ReviewLineRange, error) {
	value := strings.TrimPrefix(raw, "+")
	startText, countText, hasCount := strings.Cut(value, ",")
	start, err := strconv.Atoi(startText)
	if err != nil || start < 0 {
		return ReviewLineRange{}, errors.New("new-line start is invalid")
	}
	count := 1
	if hasCount {
		count, err = strconv.Atoi(countText)
		if err != nil || count < 0 {
			return ReviewLineRange{}, errors.New("new-line count is invalid")
		}
	}
	if start == 0 {
		start = 1
	}
	end := start
	if count > 0 {
		maximumInt := int(^uint(0) >> 1)
		if start > maximumInt-count+1 {
			return ReviewLineRange{}, errors.New("new-line range overflows")
		}
		end = start + count - 1
	}
	return ReviewLineRange{StartLine: start, EndLine: end}, nil
}

func appendReviewLineRange(
	ranges []ReviewLineRange,
	added ReviewLineRange,
) []ReviewLineRange {
	if len(ranges) == 0 || added.Symbol != ranges[len(ranges)-1].Symbol ||
		added.StartLine > ranges[len(ranges)-1].EndLine+1 {
		return append(ranges, added)
	}
	if added.EndLine > ranges[len(ranges)-1].EndLine {
		ranges[len(ranges)-1].EndLine = added.EndLine
	}
	return ranges
}

func parseReviewPlanLineCounts(additionsRaw, deletionsRaw []byte) (int64, int64, bool, error) {
	additionsText := string(additionsRaw)
	deletionsText := string(deletionsRaw)
	if additionsText == "-" || deletionsText == "-" {
		if additionsText == "-" && deletionsText == "-" {
			return 0, 0, true, nil
		}
		return 0, 0, false, errors.New("binary line counts must both be '-'")
	}
	additions, err := strconv.ParseInt(additionsText, 10, 64)
	if err != nil || additions < 0 {
		return 0, 0, false, fmt.Errorf("invalid additions count %q", additionsText)
	}
	deletions, err := strconv.ParseInt(deletionsText, 10, 64)
	if err != nil || deletions < 0 {
		return 0, 0, false, fmt.Errorf("invalid deletions count %q", deletionsText)
	}
	return additions, deletions, false, nil
}

func validateReviewPlanPath(label, path string) error {
	if path == "" {
		return fmt.Errorf("%s is empty", label)
	}
	if !utf8.ValidString(path) {
		return fmt.Errorf("%s is not valid UTF-8", label)
	}
	return nil
}

func extractAcceptanceCriteria(taskBody string) []string {
	_, _, criteria := extractReviewTaskIntent(taskBody)
	return criteria
}

func extractReviewTaskIntent(taskBody string) ([]string, []string, []string) {
	taskScope := extractReviewTaskSection(taskBody, reviewTaskScopeHeading)
	nonGoals := extractReviewTaskSection(taskBody, reviewTaskNonGoalsHeading)
	criteria := extractReviewTaskSection(
		taskBody,
		reviewTaskAcceptanceCriteriaHeading,
	)
	return taskScope, nonGoals, criteria
}

type reviewTaskHeadingMatcher func(string) bool

func extractReviewTaskSection(
	taskBody string,
	matches reviewTaskHeadingMatcher,
) []string {
	lines := strings.Split(strings.ReplaceAll(taskBody, "\r\n", "\n"), "\n")
	sectionLevel := 0
	items := make([]string, 0)
	seen := make(map[string]struct{})
	for _, line := range lines {
		if level, title, ok := markdownHeading(line); ok {
			if sectionLevel == 0 {
				if matches(title) {
					sectionLevel = level
				}
				continue
			}
			if level <= sectionLevel {
				break
			}
			continue
		}
		if sectionLevel == 0 {
			continue
		}
		item, ok := markdownListItem(line)
		if !ok {
			continue
		}
		item = normalizeReviewTaskIntentItem(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		items = append(items, item)
	}
	sort.Strings(items)
	return items
}

func normalizedReviewTaskHeading(title string) string {
	title = strings.ToLower(strings.Join(strings.Fields(title), " "))
	title = strings.TrimSpace(strings.TrimSuffix(title, ":"))
	title = strings.NewReplacer("–", "-", "—", "-").Replace(title)
	return title
}

func reviewTaskScopeHeading(title string) bool {
	switch normalizedReviewTaskHeading(title) {
	case "scope", "summary", "impact":
		return true
	default:
		return false
	}
}

func reviewTaskNonGoalsHeading(title string) bool {
	switch normalizedReviewTaskHeading(title) {
	case "non-goals", "non goals", "out of scope", "non-goals / out of scope",
		"non goals / out of scope":
		return true
	default:
		return false
	}
}

func reviewTaskAcceptanceCriteriaHeading(title string) bool {
	title = normalizedReviewTaskHeading(title)
	return title == "validation" ||
		title == "acceptance criteria" ||
		strings.HasSuffix(title, " acceptance criteria")
}

func markdownHeading(line string) (int, string, bool) {
	line = strings.TrimSpace(line)
	level := 0
	for level < len(line) && level < 6 && line[level] == '#' {
		level++
	}
	if level == 0 || level >= len(line) || (line[level] != ' ' && line[level] != '\t') {
		return 0, "", false
	}
	title := strings.TrimSpace(line[level:])
	title = strings.TrimSpace(strings.TrimRight(title, "#"))
	return level, title, title != ""
}

func markdownListItem(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if len(line) < 2 {
		return "", false
	}
	start := 0
	switch line[0] {
	case '-', '*', '+':
		if line[1] != ' ' && line[1] != '\t' {
			return "", false
		}
		start = 2
	default:
		digits := 0
		for digits < len(line) && line[digits] >= '0' && line[digits] <= '9' {
			digits++
		}
		if digits == 0 || digits+1 >= len(line) || (line[digits] != '.' && line[digits] != ')') {
			return "", false
		}
		if line[digits+1] != ' ' && line[digits+1] != '\t' {
			return "", false
		}
		start = digits + 2
	}
	item := strings.TrimSpace(line[start:])
	if len(item) >= 3 && item[0] == '[' && item[2] == ']' {
		switch item[1] {
		case ' ', 'x', 'X':
			item = strings.TrimSpace(item[3:])
		}
	}
	return item, item != ""
}

func normalizeAcceptanceCriterion(criterion string) string {
	return normalizeReviewTaskIntentItem(criterion)
}

func normalizeReviewTaskIntentItem(item string) string {
	return strings.Join(strings.Fields(item), " ")
}

func validateReviewPlanInputs(inputs ReviewPlanInputs) error {
	if inputs.SchemaVersion != reviewPlanInputsSchemaVersion {
		return fmt.Errorf(
			"review-plan input schema version %d is unsupported; expected %d",
			inputs.SchemaVersion,
			reviewPlanInputsSchemaVersion,
		)
	}
	canonicalBase, err := canonicalReviewPlanSHA("base", inputs.BaseSHA)
	if err != nil {
		return err
	}
	if canonicalBase != inputs.BaseSHA {
		return errors.New("review-plan exact base SHA is not normalized")
	}
	canonicalHead, err := canonicalReviewPlanSHA("head", inputs.HeadSHA)
	if err != nil {
		return err
	}
	if canonicalHead != inputs.HeadSHA {
		return errors.New("review-plan exact head SHA is not normalized")
	}
	if inputs.ChangedFiles == nil {
		return errors.New("changed files are not initialized")
	}
	if inputs.AcceptanceCriteria == nil {
		return errors.New("acceptance criteria are not initialized")
	}
	if inputs.ChangedFileCount != len(inputs.ChangedFiles) {
		return fmt.Errorf(
			"changed file count mismatch: record=%d files=%d",
			inputs.ChangedFileCount,
			len(inputs.ChangedFiles),
		)
	}

	var changedLines int64
	previousKey := ""
	for index, file := range inputs.ChangedFiles {
		if err := validateReviewPlanPath("changed file path", file.Path); err != nil {
			return err
		}
		if file.PreviousPath != "" {
			if err := validateReviewPlanPath("changed file previous path", file.PreviousPath); err != nil {
				return err
			}
		}
		if file.Additions < 0 || file.Deletions < 0 {
			return fmt.Errorf("changed file %q has a negative line count", file.Path)
		}
		if file.Binary && (file.Additions != 0 || file.Deletions != 0) {
			return fmt.Errorf("binary changed file %q has textual line counts", file.Path)
		}
		if file.Binary && len(file.ChangedRanges) != 0 {
			return fmt.Errorf("binary changed file %q has changed-line ranges", file.Path)
		}
		previousRangeEnd := 0
		for _, changedRange := range file.ChangedRanges {
			if changedRange.StartLine <= 0 ||
				changedRange.EndLine < changedRange.StartLine ||
				changedRange.StartLine <= previousRangeEnd ||
				strings.TrimSpace(changedRange.Symbol) != changedRange.Symbol {
				return fmt.Errorf(
					"changed file %q has invalid changed-line ranges",
					file.Path,
				)
			}
			previousRangeEnd = changedRange.EndLine
		}
		key := file.Path + "\x00" + file.PreviousPath
		if index > 0 && key <= previousKey {
			return errors.New("changed files are not in deterministic path order")
		}
		previousKey = key
		if file.Additions > math.MaxInt64-file.Deletions ||
			changedLines > math.MaxInt64-file.Additions-file.Deletions {
			return fmt.Errorf("changed-line count overflows for %q", file.Path)
		}
		changedLines += file.Additions + file.Deletions
	}
	if inputs.ChangedLineCount != changedLines {
		return fmt.Errorf(
			"changed line count mismatch: record=%d files=%d",
			inputs.ChangedLineCount,
			changedLines,
		)
	}

	for _, group := range []struct {
		label string
		items []string
	}{
		{label: "task scope", items: inputs.TaskScope},
		{label: "non-goals", items: inputs.NonGoals},
		{label: "acceptance criteria", items: inputs.AcceptanceCriteria},
	} {
		if err := validateReviewTaskIntentItems(group.label, group.items); err != nil {
			return err
		}
	}
	return nil
}

func validateReviewTaskIntentItems(label string, items []string) error {
	previous := ""
	for index, item := range items {
		if item == "" || item != normalizeReviewTaskIntentItem(item) {
			return fmt.Errorf("%s are not normalized", label)
		}
		if index > 0 && item <= previous {
			return fmt.Errorf("%s are not in deterministic order", label)
		}
		previous = item
	}
	return nil
}

func attachReviewPlanInputs(cycle *ReviewCycleState, inputs ReviewPlanInputs) error {
	if cycle == nil {
		return errors.New("review cycle is missing")
	}
	if err := validateReviewPlanInputs(inputs); err != nil {
		return err
	}
	if cycle.HeadSHA != inputs.HeadSHA {
		return fmt.Errorf(
			"review-plan input head mismatch: cycle=%s inputs=%s",
			abbreviateSHA(cycle.HeadSHA),
			abbreviateSHA(inputs.HeadSHA),
		)
	}
	cloned := cloneReviewPlanInputs(inputs)
	cycle.Inputs = &cloned
	return nil
}

func cloneReviewPlanInputs(inputs ReviewPlanInputs) ReviewPlanInputs {
	if inputs.ChangedFiles != nil {
		files := make([]ReviewPlanChangedFile, len(inputs.ChangedFiles))
		copy(files, inputs.ChangedFiles)
		for index := range files {
			if files[index].ChangedRanges != nil {
				files[index].ChangedRanges = append(
					[]ReviewLineRange(nil),
					files[index].ChangedRanges...,
				)
			}
		}
		inputs.ChangedFiles = files
	}
	if inputs.AcceptanceCriteria != nil {
		criteria := make([]string, len(inputs.AcceptanceCriteria))
		copy(criteria, inputs.AcceptanceCriteria)
		inputs.AcceptanceCriteria = criteria
	}
	inputs.TaskScope = cloneReviewPlanStrings(inputs.TaskScope)
	inputs.NonGoals = cloneReviewPlanStrings(inputs.NonGoals)
	return inputs
}

// SetReviewPlanInputs makes the complete normalized record visible through the
// same durable review-cycle checkpoint that later classification will consume.
func (m *AgentManager) SetReviewPlanInputs(agentID string, inputs ReviewPlanInputs) error {
	if m == nil {
		return errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	agent, ok := m.agents[strings.TrimSpace(agentID)]
	if !ok {
		return fmt.Errorf("review agent %q was not found", strings.TrimSpace(agentID))
	}
	if agent.Role != RoleReviewer {
		return fmt.Errorf("agent %q is not a review agent", agent.ID)
	}
	if agent.ReviewCycle == nil {
		return fmt.Errorf("review agent %q has no review cycle", agent.ID)
	}
	if agent.ReviewCycle.Stale {
		return errReviewCycleStale
	}
	if agent.ReviewCycle.Inputs != nil {
		if reflect.DeepEqual(*agent.ReviewCycle.Inputs, inputs) {
			return nil
		}
		return fmt.Errorf("review-plan inputs for %s are already recorded", agent.ID)
	}
	candidate := cloneReviewCycle(agent.ReviewCycle)
	if err := attachReviewPlanInputs(candidate, inputs); err != nil {
		return fmt.Errorf("failed to set review-plan inputs for %s: %w", agent.ID, err)
	}
	if err := validatePersistedReviewCycleSnapshot(candidate); err != nil {
		return fmt.Errorf("failed to validate review-plan inputs for %s: %w", agent.ID, err)
	}
	agent.ReviewCycle = candidate
	agent.LastActivityTime = time.Now().UTC()
	return nil
}

func (b *Orchestrator) persistReviewPlanInputs(agentID string, inputs ReviewPlanInputs) error {
	if b == nil || b.agents == nil {
		return errors.New("orchestrator agent manager is not configured")
	}
	if _, err := b.ensureReviewCycleHeadCurrent(
		context.Background(),
		agentID,
	); err != nil {
		return err
	}
	if err := b.agents.SetReviewPlanInputs(agentID, inputs); err != nil {
		return err
	}
	if err := b.persistAgentState(); err != nil {
		return fmt.Errorf("failed to persist review-plan inputs for %s: %w", agentID, err)
	}
	return nil
}
