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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func setupReviewPlanRepo(t *testing.T) (string, string) {
	t.Helper()
	repoPath := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", repoPath, err)
	}
	runGit(t, repoPath, "init")
	runGit(t, repoPath, "config", "user.email", "tester@example.com")
	runGit(t, repoPath, "config", "user.name", "Tester")
	runGit(t, repoPath, "checkout", "-b", "main")
	if err := os.WriteFile(filepath.Join(repoPath, "old name.txt"), []byte("stable\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(base) error = %v", err)
	}
	runGit(t, repoPath, "add", ".")
	runGit(t, repoPath, "commit", "-m", "base")
	return repoPath, strings.TrimSpace(runGit(t, repoPath, "rev-parse", "HEAD"))
}

func commitReviewPlanChanges(t *testing.T, repoPath, message string) string {
	t.Helper()
	runGit(t, repoPath, "add", "-A")
	runGit(t, repoPath, "commit", "-m", message)
	return strings.TrimSpace(runGit(t, repoPath, "rev-parse", "HEAD"))
}

func TestCollectReviewPlanInputsEmptyDiff(t *testing.T) {
	repoPath, headSHA := setupReviewPlanRepo(t)
	taskBody := strings.Join([]string{
		"## Outcome",
		"Keep the result stable.",
		"",
		"## Acceptance criteria",
		"- [ ] Empty diffs remain empty.",
		"- [x]   Whitespace is   normalized. ",
		"",
		"## Delivery",
		"- This is not an acceptance criterion.",
	}, "\n")

	inputs, err := collectReviewPlanInputs(
		context.Background(),
		repoPath,
		headSHA,
		headSHA,
		taskBody,
	)
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}
	if inputs.SchemaVersion != reviewPlanInputsSchemaVersion {
		t.Fatalf("schema version = %d, want %d", inputs.SchemaVersion, reviewPlanInputsSchemaVersion)
	}
	if inputs.BaseSHA != headSHA || inputs.HeadSHA != headSHA {
		t.Fatalf("diff SHAs = %s..%s, want %s..%s", inputs.BaseSHA, inputs.HeadSHA, headSHA, headSHA)
	}
	if inputs.ChangedFileCount != 0 || inputs.ChangedLineCount != 0 || len(inputs.ChangedFiles) != 0 {
		t.Fatalf("empty diff inputs = %+v", inputs)
	}
	wantCriteria := []string{
		"Empty diffs remain empty.",
		"Whitespace is normalized.",
	}
	if got := strings.Join(inputs.AcceptanceCriteria, "|"); got != strings.Join(wantCriteria, "|") {
		t.Fatalf("acceptance criteria = %#v, want %#v", inputs.AcceptanceCriteria, wantCriteria)
	}
}

func TestCollectReviewPlanInputsMatchesPullRequestDiff(t *testing.T) {
	repoPath, _ := setupReviewPlanRepo(t)
	runGit(t, repoPath, "checkout", "-b", "feature")
	if err := os.WriteFile(
		filepath.Join(repoPath, "branch.txt"),
		[]byte("branch change\n"),
		0o644,
	); err != nil {
		t.Fatalf("WriteFile(branch) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "branch change")

	runGit(t, repoPath, "checkout", "main")
	if err := os.WriteFile(
		filepath.Join(repoPath, "main.txt"),
		[]byte("unrelated main change\n"),
		0o644,
	); err != nil {
		t.Fatalf("WriteFile(main) error = %v", err)
	}
	baseSHA := commitReviewPlanChanges(t, repoPath, "main change")
	runGit(t, repoPath, "checkout", "feature")

	inputs, err := collectReviewPlanInputs(
		context.Background(),
		repoPath,
		baseSHA,
		headSHA,
		"",
	)
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}
	if inputs.ChangedFileCount != 1 ||
		len(inputs.ChangedFiles) != 1 ||
		inputs.ChangedFiles[0].Path != "branch.txt" {
		t.Fatalf("pull request changed files = %#v, want only branch.txt", inputs.ChangedFiles)
	}
	reviewer := Agent{
		WorktreePath: repoPath,
		ReviewCycle: &ReviewCycleState{
			Attempt: 1,
			HeadSHA: headSHA,
			Inputs:  &inputs,
		},
	}
	challengeDiff, err := collectReviewChallengeDiff(
		context.Background(),
		reviewer,
	)
	if err != nil {
		t.Fatalf("collectReviewChallengeDiff() error = %v", err)
	}
	if !strings.Contains(challengeDiff, "branch.txt") ||
		strings.Contains(challengeDiff, "main.txt") {
		t.Fatalf(
			"challenge diff does not match the pull request diff:\n%s",
			challengeDiff,
		)
	}
}

func TestCollectReviewPlanInputsRenamedFile(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	runGit(t, repoPath, "mv", "old name.txt", "new name.txt")
	headSHA := commitReviewPlanChanges(t, repoPath, "rename file")

	inputs, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}
	if inputs.ChangedFileCount != 1 || len(inputs.ChangedFiles) != 1 {
		t.Fatalf("renamed diff changed files = %+v", inputs.ChangedFiles)
	}
	file := inputs.ChangedFiles[0]
	if file.Path != "new name.txt" || file.PreviousPath != "old name.txt" {
		t.Fatalf("renamed file = %+v", file)
	}
	if file.Additions != 0 || file.Deletions != 0 || file.Binary {
		t.Fatalf("renamed file counts = %+v", file)
	}
	if file.ChangedRanges == nil || len(file.ChangedRanges) != 0 {
		t.Fatalf("pure rename changed ranges = %#v, want initialized empty", file.ChangedRanges)
	}
}

func TestCollectReviewPlanInputsRecordsExactChangedLineRanges(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	if err := os.WriteFile(
		filepath.Join(repoPath, "old name.txt"),
		[]byte("stable\nadded\n"),
		0o644,
	); err != nil {
		t.Fatalf("WriteFile(changed lines) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "add second line")
	inputs, err := collectReviewPlanInputs(
		context.Background(),
		repoPath,
		baseSHA,
		headSHA,
		"",
	)
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}
	if len(inputs.ChangedFiles) != 1 {
		t.Fatalf("changed files = %#v", inputs.ChangedFiles)
	}
	want := []ReviewLineRange{{StartLine: 2, EndLine: 2, Symbol: "stable"}}
	if got := inputs.ChangedFiles[0].ChangedRanges; !reflect.DeepEqual(got, want) {
		t.Fatalf("changed ranges = %#v, want %#v", got, want)
	}
}

func TestParseReviewPlanChangedRangesDoesNotTreatAddedContentAsHeader(
	t *testing.T,
) {
	patch := strings.Join([]string{
		"diff --git source.go source.go",
		"--- source.go",
		"+++ source.go",
		"@@ -1,0 +2 @@ func first()",
		"+++ fake-header.go",
		"@@ -8,0 +10,2 @@ func second()",
		"+first",
		"+second",
	}, "\n")
	ranges, err := parseReviewPlanChangedRanges([]byte(patch))
	if err != nil {
		t.Fatalf("parseReviewPlanChangedRanges() error = %v", err)
	}
	want := []ReviewLineRange{
		{StartLine: 2, EndLine: 2, Symbol: "func first()"},
		{StartLine: 10, EndLine: 11, Symbol: "func second()"},
	}
	if got := ranges["source.go"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("changed ranges = %#v, want %#v", got, want)
	}
}

func TestCollectReviewPlanInputsBinaryDiff(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	binaryPath := filepath.Join(repoPath, "asset.bin")
	if err := os.WriteFile(binaryPath, []byte{0x00, 0x01, 0x02, 0xff}, 0o644); err != nil {
		t.Fatalf("WriteFile(binary) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "add binary")

	inputs, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}
	if inputs.ChangedFileCount != 1 || len(inputs.ChangedFiles) != 1 {
		t.Fatalf("binary diff changed files = %+v", inputs.ChangedFiles)
	}
	file := inputs.ChangedFiles[0]
	if file.Path != "asset.bin" || !file.Binary {
		t.Fatalf("binary file = %+v", file)
	}
	if file.Additions != 0 || file.Deletions != 0 || inputs.ChangedLineCount != 0 {
		t.Fatalf("binary line counts = file=%+v total=%d", file, inputs.ChangedLineCount)
	}
}

func TestCollectReviewPlanInputsLargeFileDiff(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	const lineCount = 100_001
	largePath := filepath.Join(repoPath, "large.txt")
	if err := os.WriteFile(largePath, []byte(strings.Repeat("large line\n", lineCount)), 0o644); err != nil {
		t.Fatalf("WriteFile(large) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "add large file")

	inputs, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}
	if inputs.ChangedFileCount != 1 || len(inputs.ChangedFiles) != 1 {
		t.Fatalf("large diff changed files = %+v", inputs.ChangedFiles)
	}
	file := inputs.ChangedFiles[0]
	if file.Path != "large.txt" || file.Additions != lineCount || file.Deletions != 0 || file.Binary {
		t.Fatalf("large file counts = %+v", file)
	}
	if inputs.ChangedLineCount != lineCount {
		t.Fatalf("changed line count = %d, want %d", inputs.ChangedLineCount, lineCount)
	}
}

func TestCollectReviewPlanInputsIgnoresAmbientDiffConfig(t *testing.T) {
	repoPath, _ := setupReviewPlanRepo(t)
	algorithmPath := filepath.Join(repoPath, "algorithm.txt")
	baseLines := []string{"C", "E", "B", "A", "D", "A", "D", "D"}
	if err := os.WriteFile(algorithmPath, []byte(strings.Join(baseLines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(algorithm base) error = %v", err)
	}
	baseSHA := commitReviewPlanChanges(t, repoPath, "add algorithm fixture")

	headLines := []string{"C", "C", "B", "C", "D", "D", "E", "B"}
	if err := os.WriteFile(algorithmPath, []byte(strings.Join(headLines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(algorithm head) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "text.txt"), []byte("plain text\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(text) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "add deterministic diff fixtures")

	reference, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs(reference) error = %v", err)
	}
	if len(reference.ChangedFiles) != 2 {
		t.Fatalf("reference changed files = %+v", reference.ChangedFiles)
	}
	if file := reference.ChangedFiles[0]; file.Path != "algorithm.txt" ||
		file.Additions != 4 || file.Deletions != 4 || file.Binary {
		t.Fatalf("reference algorithm counts = %+v", file)
	}

	assertSameRecord := func(label string) {
		t.Helper()
		got, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
		if err != nil {
			t.Fatalf("collectReviewPlanInputs(%s) error = %v", label, err)
		}
		referenceJSON, err := json.Marshal(reference)
		if err != nil {
			t.Fatalf("json.Marshal(reference) error = %v", err)
		}
		gotJSON, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("json.Marshal(%s) error = %v", label, err)
		}
		if string(gotJSON) != string(referenceJSON) {
			t.Fatalf("%s config changed record:\nreference: %s\ngot:       %s", label, referenceJSON, gotJSON)
		}
	}

	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.bigFileThreshold")
	t.Setenv("GIT_CONFIG_VALUE_0", "1")
	assertSameRecord("core.bigFileThreshold")

	t.Setenv("GIT_CONFIG_KEY_0", "diff.algorithm")
	t.Setenv("GIT_CONFIG_VALUE_0", "histogram")
	assertSameRecord("diff.algorithm")

	attributesPath := filepath.Join(t.TempDir(), "attributes")
	if err := os.WriteFile(attributesPath, []byte("*.txt binary\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(global attributes) error = %v", err)
	}
	globalConfigPath := filepath.Join(t.TempDir(), "gitconfig")
	globalConfig := "[core]\n\tattributesFile = " + attributesPath + "\n"
	if err := os.WriteFile(globalConfigPath, []byte(globalConfig), 0o644); err != nil {
		t.Fatalf("WriteFile(global config) error = %v", err)
	}
	t.Setenv("GIT_CONFIG_COUNT", "0")
	t.Setenv("GIT_CONFIG_GLOBAL", globalConfigPath)
	assertSameRecord("core.attributesFile")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)

	infoAttributesPath := strings.TrimSpace(runGit(t, repoPath, "rev-parse", "--git-path", "info/attributes"))
	if !filepath.IsAbs(infoAttributesPath) {
		infoAttributesPath = filepath.Join(repoPath, infoAttributesPath)
	}
	if err := os.MkdirAll(filepath.Dir(infoAttributesPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(info attributes) error = %v", err)
	}
	if err := os.WriteFile(infoAttributesPath, []byte("*.txt binary\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(info attributes) error = %v", err)
	}
	assertSameRecord("info/attributes")
	if err := os.Remove(infoAttributesPath); err != nil {
		t.Fatalf("Remove(info attributes) error = %v", err)
	}

	dirtyAttributesPath := filepath.Join(repoPath, ".gitattributes")
	if err := os.WriteFile(dirtyAttributesPath, []byte("*.txt binary\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(dirty attributes) error = %v", err)
	}
	assertSameRecord("dirty worktree attributes")
	if err := os.Remove(dirtyAttributesPath); err != nil {
		t.Fatalf("Remove(dirty attributes) error = %v", err)
	}
}

func TestCollectReviewPlanInputsUsesExactHeadAttributes(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	if err := os.WriteFile(filepath.Join(repoPath, ".gitattributes"), []byte("*.txt binary\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(attributes) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "old name.txt"), []byte("changed text\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(changed text) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "add exact head attributes")
	t.Setenv("GIT_ATTR_SOURCE", baseSHA)

	inputs, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}
	for _, file := range inputs.ChangedFiles {
		if file.Path == "old name.txt" {
			if !file.Binary || file.Additions != 0 || file.Deletions != 0 {
				t.Fatalf("exact-head attributed file = %+v", file)
			}
			return
		}
	}
	t.Fatalf("exact-head attributed file missing from %+v", inputs.ChangedFiles)
}

func TestCollectReviewPlanInputsPinsRenameLimit(t *testing.T) {
	repoPath, _ := setupReviewPlanRepo(t)
	for _, prefix := range []string{"a", "b"} {
		lines := make([]string, 20)
		for index := range lines {
			lines[index] = prefix + strings.Repeat("x", index+1)
		}
		path := filepath.Join(repoPath, "old-"+prefix+".txt")
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", path, err)
		}
	}
	baseSHA := commitReviewPlanChanges(t, repoPath, "add rename limit fixtures")

	for _, prefix := range []string{"a", "b"} {
		oldPath := filepath.Join(repoPath, "old-"+prefix+".txt")
		newPath := filepath.Join(repoPath, "new-"+prefix+".txt")
		if err := os.Rename(oldPath, newPath); err != nil {
			t.Fatalf("Rename(%s) error = %v", prefix, err)
		}
		content, err := os.ReadFile(newPath)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", newPath, err)
		}
		if err := os.WriteFile(newPath, append(content, []byte("modified\n")...), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", newPath, err)
		}
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "rename limit fixtures")

	reference, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs(reference) error = %v", err)
	}
	if len(reference.ChangedFiles) != 2 {
		t.Fatalf("reference changed files = %+v", reference.ChangedFiles)
	}
	for _, file := range reference.ChangedFiles {
		if file.PreviousPath == "" {
			t.Fatalf("reference rename missing previous path: %+v", file)
		}
	}

	runGit(t, repoPath, "config", "diff.renameLimit", "1")
	got, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs(rename limit) error = %v", err)
	}
	referenceJSON, err := json.Marshal(reference)
	if err != nil {
		t.Fatalf("json.Marshal(reference) error = %v", err)
	}
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json.Marshal(got) error = %v", err)
	}
	if string(gotJSON) != string(referenceJSON) {
		t.Fatalf("rename limit changed record:\nreference: %s\ngot:       %s", referenceJSON, gotJSON)
	}
}

func TestCollectReviewPlanInputsPinsSubmoduleHandling(t *testing.T) {
	repoPath, initialSHA := setupReviewPlanRepo(t)
	runGit(t, repoPath, "update-index", "--add", "--cacheinfo", "160000", initialSHA, "module")
	runGit(t, repoPath, "commit", "-m", "add gitlink")
	baseSHA := strings.TrimSpace(runGit(t, repoPath, "rev-parse", "HEAD"))
	runGit(t, repoPath, "update-index", "--cacheinfo", "160000", baseSHA, "module")
	runGit(t, repoPath, "commit", "-m", "update gitlink")
	headSHA := strings.TrimSpace(runGit(t, repoPath, "rev-parse", "HEAD"))

	reference, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs(reference) error = %v", err)
	}
	if len(reference.ChangedFiles) != 1 || reference.ChangedFiles[0].Path != "module" {
		t.Fatalf("reference gitlink diff = %+v", reference.ChangedFiles)
	}

	runGit(t, repoPath, "config", "diff.ignoreSubmodules", "all")
	runGit(t, repoPath, "config", "diff.submodule", "log")
	got, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs(submodule config) error = %v", err)
	}
	referenceJSON, err := json.Marshal(reference)
	if err != nil {
		t.Fatalf("json.Marshal(reference) error = %v", err)
	}
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json.Marshal(got) error = %v", err)
	}
	if string(gotJSON) != string(referenceJSON) {
		t.Fatalf("submodule config changed record:\nreference: %s\ngot:       %s", referenceJSON, gotJSON)
	}
}

func TestCollectReviewPlanInputsOrdersFilesDeterministically(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	for _, name := range []string{"z-last.txt", "a-first.txt", "m-middle.txt"} {
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte(name+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "add unordered files")

	first, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs(first) error = %v", err)
	}
	second, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, headSHA, "")
	if err != nil {
		t.Fatalf("collectReviewPlanInputs(second) error = %v", err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("json.Marshal(first) error = %v", err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("json.Marshal(second) error = %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("repeated collection differs:\nfirst:  %s\nsecond: %s", firstJSON, secondJSON)
	}
	gotPaths := make([]string, 0, len(first.ChangedFiles))
	for _, file := range first.ChangedFiles {
		gotPaths = append(gotPaths, file.Path)
	}
	wantPaths := []string{"a-first.txt", "m-middle.txt", "z-last.txt"}
	if strings.Join(gotPaths, "|") != strings.Join(wantPaths, "|") {
		t.Fatalf("changed file order = %#v, want %#v", gotPaths, wantPaths)
	}
}

func TestCollectReviewPlanInputsAbortsOnHeadMismatch(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	if err := os.WriteFile(filepath.Join(repoPath, "change.txt"), []byte("new\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(change) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "advance head")
	if baseSHA == headSHA {
		t.Fatal("test repository head did not advance")
	}

	_, err := collectReviewPlanInputs(context.Background(), repoPath, baseSHA, baseSHA, "")
	if err == nil || !strings.Contains(err.Error(), "review-plan head mismatch") {
		t.Fatalf("collectReviewPlanInputs() error = %v, want head mismatch", err)
	}
}

func TestExtractAcceptanceCriteriaUsesOnlyTaskSection(t *testing.T) {
	taskBody := strings.Join([]string{
		"# Task",
		"- [ ] outside",
		"",
		"### ACCEPTANCE CRITERIA ###",
		"1. [X] Second   criterion",
		"2) First criterion",
		"- [ ] First criterion",
		"",
		"### Out of scope",
		"- [ ] after",
	}, "\n")
	want := []string{
		"First criterion",
		"Second criterion",
	}
	got := extractAcceptanceCriteria(taskBody)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("extractAcceptanceCriteria() = %#v, want %#v", got, want)
	}
}

func TestExtractAcceptanceCriteriaIncludesNestedCategorySections(t *testing.T) {
	taskBody := strings.Join([]string{
		"## Acceptance criteria",
		"",
		"### Classify risk tags (#29)",
		"",
		"- [ ] Fixture diffs produce stable expected tags.",
		"- [ ] No model preference is used as a classification reason.",
		"",
		"### Select review lanes and initial swarm size (#30)",
		"",
		"- [ ] Small and multi-domain inputs choose different plans where policy requires.",
		"- [ ] Selection never violates min/max bounds.",
		"- [ ] Required lanes cannot be removed to meet a budget.",
		"",
		"## Delivery constraint",
		"",
		"- This is not an acceptance criterion.",
	}, "\n")
	want := []string{
		"Fixture diffs produce stable expected tags.",
		"No model preference is used as a classification reason.",
		"Required lanes cannot be removed to meet a budget.",
		"Selection never violates min/max bounds.",
		"Small and multi-domain inputs choose different plans where policy requires.",
	}
	got := extractAcceptanceCriteria(taskBody)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("extractAcceptanceCriteria() = %#v, want %#v", got, want)
	}
}

func TestExtractReviewTaskIntentPreservesScopeNonGoalsAndFlexibleCriteriaHeading(
	t *testing.T,
) {
	taskBody := strings.Join([]string{
		"## Scope",
		"- Route JSON handlers through the bounded decoder.",
		"- Keep the decoder strict.",
		"",
		"## Non-goals",
		"- Change multipart upload policy.",
		"- Redesign response DTOs.",
		"",
		"## TDD acceptance criteria",
		"- [ ] Oversized JSON returns the documented error.",
		"- [ ] Existing multipart limits remain separate.",
	}, "\n")
	scope, nonGoals, criteria := extractReviewTaskIntent(taskBody)
	if got, want := strings.Join(scope, "|"),
		"Keep the decoder strict.|Route JSON handlers through the bounded decoder."; got != want {
		t.Fatalf("task scope = %q, want %q", got, want)
	}
	if got, want := strings.Join(nonGoals, "|"),
		"Change multipart upload policy.|Redesign response DTOs."; got != want {
		t.Fatalf("non-goals = %q, want %q", got, want)
	}
	if got, want := strings.Join(criteria, "|"),
		"Existing multipart limits remain separate.|Oversized JSON returns the documented error."; got != want {
		t.Fatalf("acceptance criteria = %q, want %q", got, want)
	}
}

func TestExtractReviewTaskIntentUnderstandsStandardPRSections(t *testing.T) {
	taskBody := strings.Join([]string{
		"## Summary",
		"- Fix prompt delivery startup.",
		"- Keep review publication deterministic.",
		"",
		"## Why",
		"Review workers must start reliably.",
		"",
		"## Validation",
		"- `go test ./...`",
	}, "\n")
	scope, nonGoals, criteria := extractReviewTaskIntent(taskBody)
	if got, want := strings.Join(scope, "|"),
		"Fix prompt delivery startup.|Keep review publication deterministic."; got != want {
		t.Fatalf("task scope = %q, want %q", got, want)
	}
	if len(nonGoals) != 0 {
		t.Fatalf("non-goals = %#v, want none", nonGoals)
	}
	if got, want := strings.Join(criteria, "|"), "`go test ./...`"; got != want {
		t.Fatalf("validation criteria = %q, want %q", got, want)
	}
}

func TestReviewPlanInputsPersistWithReviewCycle(t *testing.T) {
	repoPath, baseSHA := setupReviewPlanRepo(t)
	if err := os.WriteFile(filepath.Join(repoPath, "change.txt"), []byte("new\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(change) error = %v", err)
	}
	headSHA := commitReviewPlanChanges(t, repoPath, "add change")
	inputs, err := collectReviewPlanInputs(
		context.Background(),
		repoPath,
		baseSHA,
		headSHA,
		"## Acceptance criteria\n- [ ] Persist the normalized inputs.\n",
	)
	if err != nil {
		t.Fatalf("collectReviewPlanInputs() error = %v", err)
	}

	policy := builtInReviewPolicy()
	cycle, err := newReviewCycleState(headSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	runtimeProfile, err := cycle.Policy.effectiveProfileForRole(AgentProfileRoleChallenge)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                "review-agent-plan-inputs",
		Role:              RoleReviewer,
		ObservedPRHeadSHA: headSHA,
		RuntimeProfile:    runtimeProfile,
		RuntimeHandle:     RuntimeHandle{Kind: RuntimeKindTmux, Session: "review-plan-inputs"},
		ReviewCycle:       cycle,
		State:             StateWorking,
		LastActivityTime:  time.Now(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	logDir := t.TempDir()
	bot := &Orchestrator{
		cfg: Config{
			LogDir:       logDir,
			ReviewPolicy: cycle.Policy,
		},
		agents: agents,
	}
	if err := bot.persistReviewPlanInputs(reviewer.ID, inputs); err != nil {
		t.Fatalf("persistReviewPlanInputs() error = %v", err)
	}
	inputs.ChangedFiles[0].Path = "mutated-after-set"
	inputs.AcceptanceCriteria[0] = "mutated after set"

	restarted := &Orchestrator{
		cfg: Config{
			LogDir:       logDir,
			ReviewPolicy: cycle.Policy,
		},
		agents: NewAgentManager(),
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	loaded, ok := restarted.agents.Get(reviewer.ID)
	if !ok {
		t.Fatal("persisted review agent was not restored")
	}
	if loaded.ReviewCycle == nil || loaded.ReviewCycle.Inputs == nil {
		t.Fatal("review-plan inputs were not persisted with the review cycle")
	}
	persistedInputs := loaded.ReviewCycle.Inputs
	if persistedInputs.HeadSHA != headSHA || persistedInputs.BaseSHA != baseSHA {
		t.Fatalf("persisted input SHAs = %s..%s, want %s..%s", persistedInputs.BaseSHA, persistedInputs.HeadSHA, baseSHA, headSHA)
	}
	if got := persistedInputs.ChangedFiles[0].Path; got != "change.txt" {
		t.Fatalf("persisted changed path = %q, want %q", got, "change.txt")
	}
	if got := persistedInputs.AcceptanceCriteria[0]; got != "Persist the normalized inputs." {
		t.Fatalf("persisted acceptance criterion = %q", got)
	}
}

func TestAttachReviewPlanInputsRejectsCycleHeadMismatch(t *testing.T) {
	cycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	inputs := ReviewPlanInputs{
		SchemaVersion:      reviewPlanInputsSchemaVersion,
		BaseSHA:            testReviewHeadSHA,
		HeadSHA:            testOtherReviewHeadSHA,
		ChangedFileCount:   0,
		ChangedLineCount:   0,
		ChangedFiles:       []ReviewPlanChangedFile{},
		AcceptanceCriteria: []string{},
	}
	err = attachReviewPlanInputs(cycle, inputs)
	if err == nil || !strings.Contains(err.Error(), "input head mismatch") {
		t.Fatalf("attachReviewPlanInputs() error = %v, want head mismatch", err)
	}
	if cycle.Inputs != nil {
		t.Fatal("mismatched review-plan inputs were attached")
	}
}

func TestReviewPlanInputsCannotBeReplaced(t *testing.T) {
	cycle, err := newReviewCycleState(testReviewHeadSHA, builtInReviewPolicy())
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := &Agent{
		ID:                "review-agent-stable-inputs",
		Role:              RoleReviewer,
		ObservedPRHeadSHA: testReviewHeadSHA,
		ReviewCycle:       cycle,
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	inputs := ReviewPlanInputs{
		SchemaVersion:      reviewPlanInputsSchemaVersion,
		BaseSHA:            testOtherReviewHeadSHA,
		HeadSHA:            testReviewHeadSHA,
		ChangedFileCount:   0,
		ChangedLineCount:   0,
		ChangedFiles:       []ReviewPlanChangedFile{},
		AcceptanceCriteria: []string{},
	}
	if err := agents.SetReviewPlanInputs(reviewer.ID, inputs); err != nil {
		t.Fatalf("SetReviewPlanInputs(first) error = %v", err)
	}
	if err := agents.SetReviewPlanInputs(reviewer.ID, cloneReviewPlanInputs(inputs)); err != nil {
		t.Fatalf("SetReviewPlanInputs(idempotent) error = %v", err)
	}
	replacement := cloneReviewPlanInputs(inputs)
	replacement.BaseSHA = testThirdReviewHeadSHA
	err = agents.SetReviewPlanInputs(reviewer.ID, replacement)
	if err == nil || !strings.Contains(err.Error(), "already recorded") {
		t.Fatalf("SetReviewPlanInputs(replacement) error = %v, want immutable input error", err)
	}
	loaded, ok := agents.Get(reviewer.ID)
	if !ok || loaded.ReviewCycle == nil || loaded.ReviewCycle.Inputs == nil {
		t.Fatal("reviewer inputs missing after replacement attempt")
	}
	if loaded.ReviewCycle.Inputs.BaseSHA != testOtherReviewHeadSHA {
		t.Fatalf("persisted base SHA = %s, want original %s", loaded.ReviewCycle.Inputs.BaseSHA, testOtherReviewHeadSHA)
	}
}
