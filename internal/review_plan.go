// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2026 Cisco Systems, Inc. and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	reviewPlanSchemaVersion = 5
	reviewPlanRulesVersion  = 6
)

const (
	reviewRiskLifecycle    = "lifecycle"
	reviewRiskConcurrency  = "concurrency"
	reviewRiskSecurity     = "security"
	reviewRiskPersistence  = "persistence"
	reviewRiskTestCoverage = "test-coverage"
)

var supportedReviewRiskTags = map[string]struct{}{
	reviewRiskLifecycle:    {},
	reviewRiskConcurrency:  {},
	reviewRiskSecurity:     {},
	reviewRiskPersistence:  {},
	reviewRiskTestCoverage: {},
}

var reviewPlanKeywordRules = []struct {
	tag      string
	keywords []string
}{
	{
		tag: reviewRiskLifecycle,
		keywords: []string{
			"cancel", "cancellation", "cleanup", "continue", "exit", "launch",
			"lifecycle", "pause", "recovery", "restart", "resume", "shutdown",
			"startup", "stop", "stopped", "termination", "worktree",
		},
	},
	{
		tag: reviewRiskConcurrency,
		keywords: []string{
			"atomic", "channel", "concurrency", "concurrent", "context", "deadlock",
			"goroutine", "lock", "mutex", "parallel", "poll", "polling", "queue",
			"race", "runner", "synchronization", "tmux", "worker", "workers",
		},
	},
	{
		tag: reviewRiskSecurity,
		keywords: []string{
			"auth", "authentication", "authorization", "credential", "credentials",
			"crypto", "oauth", "permission", "permissions", "redact", "redaction",
			"sanitize", "secret", "secrets", "security", "token", "tokens", "webhook",
		},
	},
	{
		tag: reviewRiskPersistence,
		keywords: []string{
			"backend", "database", "db", "handoff", "ledger", "migration", "persist",
			"persisted", "persistence", "restore", "state", "storage", "store",
		},
	},
	{
		tag: reviewRiskTestCoverage,
		keywords: []string{
			"coverage", "fixture", "fixtures", "regression", "test", "testing", "tests",
		},
	},
}

var reviewPlanRiskLaneRules = []struct {
	tag  string
	lane string
}{
	{tag: reviewRiskLifecycle, lane: "lifecycle"},
	{tag: reviewRiskConcurrency, lane: "concurrency-ordering"},
	{tag: reviewRiskSecurity, lane: "contract"},
	{tag: reviewRiskPersistence, lane: "persistence-recovery"},
	{tag: reviewRiskTestCoverage, lane: "operations-tests"},
}

// ReviewPlan is the deterministic, explainable planning result for one exact
// input record and one effective policy snapshot.
type ReviewPlan struct {
	SchemaVersion        int                         `json:"schema_version"`
	RulesVersion         int                         `json:"rules_version"`
	BaseSHA              string                      `json:"base_sha"`
	HeadSHA              string                      `json:"head_sha"`
	PolicyVersion        int                         `json:"policy_version"`
	PolicyFingerprint    string                      `json:"policy_fingerprint"`
	ChangedFileCount     int                         `json:"changed_file_count"`
	ChangedLineCount     int64                       `json:"changed_line_count"`
	TaskScope            []string                    `json:"task_scope"`
	NonGoals             []string                    `json:"non_goals"`
	AcceptanceCriteria   []string                    `json:"acceptance_criteria"`
	RiskTags             []string                    `json:"risk_tags"`
	RiskReasons          []ReviewPlanRiskReason      `json:"risk_reasons"`
	RequiredLanes        []string                    `json:"required_lanes"`
	SelectedLanes        []string                    `json:"selected_lanes"`
	InitialReviewerCount int                         `json:"initial_reviewer_count"`
	SelectionReasons     []string                    `json:"selection_reasons"`
	CoverageRequirements []ReviewCoverageRequirement `json:"coverage_requirements"`
}

type ReviewPlanRiskReason struct {
	Tag     string   `json:"tag"`
	Reasons []string `json:"reasons"`
}

type reviewPlanAggregateMatch struct {
	items    map[string]struct{}
	keywords map[string]struct{}
}

func buildReviewPlan(inputs ReviewPlanInputs, policy ReviewPolicy) (ReviewPlan, error) {
	if err := validateReviewPlanInputs(inputs); err != nil {
		return ReviewPlan{}, fmt.Errorf("review-plan inputs are invalid: %w", err)
	}
	if err := validateReviewPolicyFingerprint(policy.Fingerprint); err != nil {
		return ReviewPlan{}, fmt.Errorf("review-plan policy is not an effective snapshot: %w", err)
	}
	if err := validateReviewPolicyConfiguration(policy); err != nil {
		return ReviewPlan{}, fmt.Errorf("review-plan policy snapshot is invalid: %w", err)
	}

	riskTags, riskReasons := classifyReviewPlanRisks(inputs)
	requiredLanes, selectedLanes, selectionReasons := selectReviewPlanLanes(
		riskTags,
		policy.Swarm,
	)
	plan := ReviewPlan{
		SchemaVersion:        reviewPlanSchemaVersion,
		RulesVersion:         reviewPlanRulesVersion,
		BaseSHA:              inputs.BaseSHA,
		HeadSHA:              inputs.HeadSHA,
		PolicyVersion:        policy.Version,
		PolicyFingerprint:    policy.Fingerprint,
		ChangedFileCount:     inputs.ChangedFileCount,
		ChangedLineCount:     inputs.ChangedLineCount,
		TaskScope:            nonNilReviewTaskIntentItems(inputs.TaskScope),
		NonGoals:             nonNilReviewTaskIntentItems(inputs.NonGoals),
		AcceptanceCriteria:   nonNilReviewTaskIntentItems(inputs.AcceptanceCriteria),
		RiskTags:             riskTags,
		RiskReasons:          riskReasons,
		RequiredLanes:        requiredLanes,
		SelectedLanes:        selectedLanes,
		InitialReviewerCount: len(selectedLanes),
		SelectionReasons:     selectionReasons,
		CoverageRequirements: buildReviewCoverageRequirements(
			inputs,
			riskTags,
			selectedLanes,
		),
	}
	if err := validateReviewPlan(plan, policy); err != nil {
		return ReviewPlan{}, fmt.Errorf("built review plan is invalid: %w", err)
	}
	return plan, nil
}

func classifyReviewPlanRisks(
	inputs ReviewPlanInputs,
) ([]string, []ReviewPlanRiskReason) {
	pathMatches := make(map[string]*reviewPlanAggregateMatch)
	criterionMatches := make(map[string]*reviewPlanAggregateMatch)
	riskReasons := make(map[string][]string)
	implementationPaths := make(map[string]struct{})
	testPaths := make(map[string]struct{})

	for _, file := range inputs.ChangedFiles {
		paths := []string{file.Path}
		if file.PreviousPath != "" {
			paths = append(paths, file.PreviousPath)
		}
		for _, path := range paths {
			if isReviewPlanTestPath(path) {
				testPaths[path] = struct{}{}
				continue
			}
			collectReviewPlanKeywordMatches(pathMatches, path, reviewPlanTokens(path))
			if isReviewPlanImplementationPath(path) {
				implementationPaths[path] = struct{}{}
			}
		}
	}
	for index, criterion := range inputs.AcceptanceCriteria {
		key := fmt.Sprintf("criterion-%06d", index+1)
		collectReviewPlanKeywordMatches(
			criterionMatches,
			key,
			reviewPlanTokens(criterion),
		)
	}

	appendReviewPlanMatchReasons(riskReasons, "path-keyword", pathMatches)
	appendReviewPlanMatchReasons(riskReasons, "acceptance-criterion", criterionMatches)
	if len(implementationPaths) > 0 && len(testPaths) == 0 {
		addReviewPlanRiskReason(
			riskReasons,
			reviewRiskTestCoverage,
			fmt.Sprintf(
				"coverage-gap rule found %d implementation path(s) and no changed test path",
				len(implementationPaths),
			),
		)
	}
	tags := make([]string, 0, len(riskReasons))
	for tag, reasons := range riskReasons {
		if len(reasons) > 0 {
			tags = append(tags, tag)
		}
	}
	sort.Strings(tags)
	classifications := make([]ReviewPlanRiskReason, 0, len(tags))
	for _, tag := range tags {
		reasons := uniqueNonEmptySortedStrings(riskReasons[tag])
		classifications = append(classifications, ReviewPlanRiskReason{
			Tag:     tag,
			Reasons: reasons,
		})
	}
	return nonNilStrings(tags), nonNilRiskReasons(classifications)
}

func collectReviewPlanKeywordMatches(
	matches map[string]*reviewPlanAggregateMatch,
	item string,
	tokens []string,
) {
	tokenSet := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		tokenSet[token] = struct{}{}
	}
	for _, rule := range reviewPlanKeywordRules {
		matchedKeywords := make([]string, 0)
		for _, keyword := range rule.keywords {
			if _, ok := tokenSet[keyword]; ok {
				matchedKeywords = append(matchedKeywords, keyword)
			}
		}
		if len(matchedKeywords) == 0 {
			continue
		}
		aggregate := matches[rule.tag]
		if aggregate == nil {
			aggregate = &reviewPlanAggregateMatch{
				items:    make(map[string]struct{}),
				keywords: make(map[string]struct{}),
			}
			matches[rule.tag] = aggregate
		}
		aggregate.items[item] = struct{}{}
		for _, keyword := range matchedKeywords {
			aggregate.keywords[keyword] = struct{}{}
		}
	}
}

func appendReviewPlanMatchReasons(
	reasons map[string][]string,
	ruleName string,
	matches map[string]*reviewPlanAggregateMatch,
) {
	for tag, aggregate := range matches {
		keywords := make([]string, 0, len(aggregate.keywords))
		for keyword := range aggregate.keywords {
			keywords = append(keywords, keyword)
		}
		sort.Strings(keywords)
		addReviewPlanRiskReason(
			reasons,
			tag,
			fmt.Sprintf(
				"%s rule matched %d item(s) using keywords: %s",
				ruleName,
				len(aggregate.items),
				strings.Join(keywords, ", "),
			),
		)
	}
}

func addReviewPlanRiskReason(reasons map[string][]string, tag, reason string) {
	reasons[tag] = append(reasons[tag], strings.TrimSpace(reason))
}

func reviewPlanTokens(value string) []string {
	return strings.FieldsFunc(strings.ToLower(value), func(character rune) bool {
		return !unicode.IsLetter(character) && !unicode.IsNumber(character)
	})
}

func isReviewPlanImplementationPath(path string) bool {
	switch strings.ToLower(filepath.Ext(filepath.ToSlash(path))) {
	case ".c", ".cc", ".cpp", ".cs", ".go", ".java", ".js", ".jsx", ".kt", ".kts",
		".lua", ".m", ".mm", ".php", ".proto", ".py", ".rb", ".rs", ".swift", ".ts",
		".tsx":
		return !isReviewPlanTestPath(path)
	default:
		return false
	}
}

func isReviewPlanTestPath(path string) bool {
	normalized := filepath.ToSlash(path)
	lower := strings.ToLower(normalized)
	segments := strings.Split(lower, "/")
	for _, segment := range segments[:max(0, len(segments)-1)] {
		switch segment {
		case "__test__", "__tests__", "fixture", "fixtures", "spec", "specs",
			"test", "testdata", "tests":
			return true
		}
		prefixes := []string{
			"test_", "test-", "test.", "tests_", "tests-", "tests.",
			"spec_", "spec-", "spec.", "specs_", "specs-", "specs.",
		}
		for _, prefix := range prefixes {
			if strings.HasPrefix(segment, prefix) {
				return true
			}
		}
		suffixes := []string{
			"_test", "-test", ".test", "_tests", "-tests", ".tests",
			"_spec", "-spec", ".spec", "_specs", "-specs", ".specs",
		}
		for _, suffix := range suffixes {
			if strings.HasSuffix(segment, suffix) {
				return true
			}
		}
	}
	name := filepath.Base(lower)
	testPrefixes := []string{
		"test_", "test-", "test.", "tests_", "tests-", "tests.",
		"spec_", "spec-", "spec.", "specs_", "specs-", "specs.",
	}
	for _, prefix := range testPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	testSuffixMarkers := []string{
		"_test.", ".test.", "-test.", "_tests.", ".tests.", "-tests.",
		"_spec.", ".spec.", "-spec.", "_specs.", ".specs.", "-specs.",
	}
	for _, marker := range testSuffixMarkers {
		if strings.Contains(name, marker) {
			return true
		}
	}

	stem := strings.TrimSuffix(filepath.Base(normalized), filepath.Ext(normalized))
	camelPrefixes := []string{"Test", "Tests", "Spec", "Specs"}
	for _, prefix := range camelPrefixes {
		if strings.HasPrefix(stem, prefix) &&
			len(stem) > len(prefix) &&
			unicode.IsUpper(rune(stem[len(prefix)])) {
			return true
		}
	}
	camelSuffixes := []string{"Test", "Tests", "Spec", "Specs"}
	for _, suffix := range camelSuffixes {
		if strings.HasSuffix(stem, suffix) && len(stem) > len(suffix) {
			return true
		}
	}
	return false
}

func selectReviewPlanLanes(
	riskTags []string,
	swarm ReviewSwarmPolicy,
) ([]string, []string, []string) {
	configured := make(map[string]ReviewLane, len(swarm.Lanes))
	for _, lane := range swarm.Lanes {
		configured[lane.Name] = lane
	}
	selected := make(map[string]struct{})
	required := make([]string, 0)
	reasons := make([]string, 0)
	for _, lane := range swarm.Lanes {
		if !lane.Required {
			continue
		}
		required = append(required, lane.Name)
		selected[lane.Name] = struct{}{}
		reasons = append(
			reasons,
			fmt.Sprintf("explicit required-lane rule preserved %s", lane.Name),
		)
	}

	riskSet := make(map[string]struct{}, len(riskTags))
	for _, tag := range riskTags {
		riskSet[tag] = struct{}{}
	}
	desiredRiskLanes := make([]string, 0)
	riskLaneTags := make(map[string][]string)
	for _, rule := range reviewPlanRiskLaneRules {
		if _, ok := riskSet[rule.tag]; !ok {
			continue
		}
		if _, ok := configured[rule.lane]; !ok {
			reasons = append(
				reasons,
				fmt.Sprintf(
					"risk-to-lane rule could not map %s because %s is not configured",
					rule.tag,
					rule.lane,
				),
			)
			continue
		}
		if _, seen := riskLaneTags[rule.lane]; !seen {
			desiredRiskLanes = append(desiredRiskLanes, rule.lane)
		}
		riskLaneTags[rule.lane] = append(riskLaneTags[rule.lane], rule.tag)
	}

	target := swarm.MinReviewers
	desiredTarget := len(selected)
	for _, lane := range desiredRiskLanes {
		if _, ok := selected[lane]; !ok {
			desiredTarget++
		}
	}
	if desiredTarget > target {
		target = desiredTarget
	}
	if target > swarm.MaxReviewers {
		target = swarm.MaxReviewers
	}
	if target > len(swarm.Lanes) {
		target = len(swarm.Lanes)
		reasons = append(
			reasons,
			fmt.Sprintf(
				"available-lane rule limited the initial swarm to %d configured lanes",
				target,
			),
		)
	}

	for _, lane := range desiredRiskLanes {
		tags := uniqueNonEmptySortedStrings(riskLaneTags[lane])
		if _, ok := selected[lane]; ok {
			reasons = append(
				reasons,
				fmt.Sprintf(
					"risk-to-lane rule mapped %s to already-required %s",
					strings.Join(tags, ", "),
					lane,
				),
			)
			continue
		}
		if len(selected) >= target || len(selected) >= swarm.MaxReviewers {
			reasons = append(
				reasons,
				fmt.Sprintf(
					"risk-to-lane rule mapped %s to %s but the configured maximum left no capacity",
					strings.Join(tags, ", "),
					lane,
				),
			)
			continue
		}
		selected[lane] = struct{}{}
		reasons = append(
			reasons,
			fmt.Sprintf(
				"risk-to-lane rule mapped %s to selected %s",
				strings.Join(tags, ", "),
				lane,
			),
		)
	}
	for _, lane := range swarm.Lanes {
		if len(selected) >= target {
			break
		}
		selected[lane.Name] = struct{}{}
	}

	selectedLanes := make([]string, 0, len(selected))
	for _, lane := range swarm.Lanes {
		if _, ok := selected[lane.Name]; ok {
			selectedLanes = append(selectedLanes, lane.Name)
		}
	}
	reasons = append(
		reasons,
		fmt.Sprintf(
			"swarm-bounds rule selected %d reviewers within configured bounds %d..%d",
			len(selectedLanes),
			swarm.MinReviewers,
			swarm.MaxReviewers,
		),
	)
	return nonNilStrings(required),
		nonNilStrings(selectedLanes),
		uniqueNonEmptySortedStrings(reasons)
}

func validateReviewPlan(plan ReviewPlan, policy ReviewPolicy) error {
	if plan.SchemaVersion != reviewPlanSchemaVersion {
		return fmt.Errorf(
			"review-plan schema version %d is unsupported; expected %d",
			plan.SchemaVersion,
			reviewPlanSchemaVersion,
		)
	}
	if plan.RulesVersion != reviewPlanRulesVersion {
		return fmt.Errorf(
			"review-plan rules version %d is unsupported; expected %d",
			plan.RulesVersion,
			reviewPlanRulesVersion,
		)
	}
	if _, err := canonicalReviewPlanSHA("base", plan.BaseSHA); err != nil {
		return err
	}
	if _, err := canonicalReviewPlanSHA("head", plan.HeadSHA); err != nil {
		return err
	}
	if plan.PolicyVersion != policy.Version {
		return fmt.Errorf(
			"review-plan policy version mismatch: plan=%d snapshot=%d",
			plan.PolicyVersion,
			policy.Version,
		)
	}
	if plan.PolicyFingerprint != policy.Fingerprint {
		return errors.New("review-plan policy fingerprint does not match snapshot")
	}
	if plan.ChangedFileCount < 0 || plan.ChangedLineCount < 0 {
		return errors.New("review-plan change counts must not be negative")
	}
	for _, group := range []struct {
		label string
		items []string
	}{
		{label: "review-plan task scope", items: plan.TaskScope},
		{label: "review-plan non-goals", items: plan.NonGoals},
		{label: "review-plan acceptance criteria", items: plan.AcceptanceCriteria},
	} {
		if err := validateReviewTaskIntentItems(group.label, group.items); err != nil {
			return err
		}
	}
	if plan.RiskTags == nil || plan.RiskReasons == nil {
		return errors.New("review-plan risk classification is not initialized")
	}
	if err := validateStrictlySortedStrings("review-plan risk tags", plan.RiskTags); err != nil {
		return err
	}
	if len(plan.RiskReasons) != len(plan.RiskTags) {
		return errors.New("review-plan must record reasons for every risk tag")
	}
	for index, item := range plan.RiskReasons {
		if _, ok := supportedReviewRiskTags[item.Tag]; !ok {
			return fmt.Errorf("review-plan contains unsupported risk tag %q", item.Tag)
		}
		if item.Tag != plan.RiskTags[index] {
			return errors.New("review-plan risk reasons do not match risk tags")
		}
		if err := validateNonEmptySortedReasons(
			"review-plan risk "+item.Tag,
			item.Reasons,
		); err != nil {
			return err
		}
	}
	if plan.RequiredLanes == nil ||
		plan.SelectedLanes == nil ||
		plan.SelectionReasons == nil ||
		plan.CoverageRequirements == nil {
		return errors.New("review-plan lane selection is not initialized")
	}
	if err := validateNonEmptySortedReasons(
		"review-plan selection",
		plan.SelectionReasons,
	); err != nil {
		return err
	}
	configured := make(map[string]ReviewLane, len(policy.Swarm.Lanes))
	selected := make(map[string]struct{}, len(plan.SelectedLanes))
	required := make(map[string]struct{}, len(plan.RequiredLanes))
	for _, lane := range policy.Swarm.Lanes {
		configured[lane.Name] = lane
	}
	for _, lane := range plan.RequiredLanes {
		configuredLane, ok := configured[lane]
		if !ok || !configuredLane.Required {
			return fmt.Errorf("review-plan required lane %q is not explicitly required", lane)
		}
		if _, duplicate := required[lane]; duplicate {
			return fmt.Errorf("review-plan required lane %q is duplicated", lane)
		}
		required[lane] = struct{}{}
	}
	for _, lane := range policy.Swarm.Lanes {
		_, recorded := required[lane.Name]
		if lane.Required != recorded {
			return fmt.Errorf("review-plan did not preserve required lane %q", lane.Name)
		}
	}
	for _, lane := range plan.SelectedLanes {
		if _, ok := configured[lane]; !ok {
			return fmt.Errorf("review-plan selected unknown lane %q", lane)
		}
		if _, duplicate := selected[lane]; duplicate {
			return fmt.Errorf("review-plan selected lane %q is duplicated", lane)
		}
		selected[lane] = struct{}{}
	}
	for lane := range required {
		if _, ok := selected[lane]; !ok {
			return fmt.Errorf("review-plan removed required lane %q", lane)
		}
	}
	if plan.InitialReviewerCount != len(plan.SelectedLanes) {
		return fmt.Errorf(
			"review-plan initial reviewer count mismatch: record=%d lanes=%d",
			plan.InitialReviewerCount,
			len(plan.SelectedLanes),
		)
	}
	if plan.InitialReviewerCount < policy.Swarm.MinReviewers ||
		plan.InitialReviewerCount > policy.Swarm.MaxReviewers {
		return fmt.Errorf(
			"review-plan initial reviewer count %d violates configured bounds %d..%d",
			plan.InitialReviewerCount,
			policy.Swarm.MinReviewers,
			policy.Swarm.MaxReviewers,
		)
	}
	if err := validateReviewCoverageRequirements(
		plan.CoverageRequirements,
	); err != nil {
		return err
	}
	for _, requirement := range plan.CoverageRequirements {
		if !requirement.Critical {
			continue
		}
		if _, ok := selected[requirement.AccountableLane]; !ok && requirement.AccountableLane != reviewSynthesisLane {
			return fmt.Errorf(
				"critical review-plan coverage requirement %q is assigned to unselected lane %q",
				requirement.ID,
				requirement.AccountableLane,
			)
		}
	}
	return nil
}

func validateReviewPlanAgainstCycle(cycle *ReviewCycleState) error {
	if cycle == nil || cycle.Plan == nil {
		return nil
	}
	if cycle.Inputs == nil {
		return errors.New("persisted review plan has no exact-SHA inputs")
	}
	if err := validateReviewPlan(*cycle.Plan, cycle.Policy); err != nil {
		return err
	}
	expected, err := buildReviewPlan(*cycle.Inputs, cycle.Policy)
	if err != nil {
		return fmt.Errorf("failed to rebuild persisted review plan: %w", err)
	}
	if !reflect.DeepEqual(expected, *cycle.Plan) {
		return errors.New("persisted review plan does not match exact-SHA inputs and policy")
	}
	return nil
}

func validateStrictlySortedStrings(label string, values []string) error {
	previous := ""
	for index, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s contains an empty value", label)
		}
		if index > 0 && value <= previous {
			return fmt.Errorf("%s are not in deterministic order", label)
		}
		previous = value
	}
	return nil
}

func validateNonEmptySortedReasons(label string, reasons []string) error {
	if len(reasons) == 0 {
		return fmt.Errorf("%s reasons are empty", label)
	}
	return validateStrictlySortedStrings(label+" reasons", reasons)
}

func uniqueNonEmptySortedStrings(values []string) []string {
	result := uniqueSortedStrings(values)
	if result == nil {
		return []string{}
	}
	return result
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func nonNilRiskReasons(values []ReviewPlanRiskReason) []ReviewPlanRiskReason {
	if values == nil {
		return []ReviewPlanRiskReason{}
	}
	return values
}

func cloneReviewPlan(plan ReviewPlan) ReviewPlan {
	plan.TaskScope = cloneReviewPlanStrings(plan.TaskScope)
	plan.NonGoals = cloneReviewPlanStrings(plan.NonGoals)
	plan.AcceptanceCriteria = cloneReviewPlanStrings(plan.AcceptanceCriteria)
	plan.RiskTags = cloneReviewPlanStrings(plan.RiskTags)
	plan.RiskReasons = cloneReviewPlanRiskReasons(plan.RiskReasons)
	for index := range plan.RiskReasons {
		plan.RiskReasons[index].Reasons = cloneReviewPlanStrings(
			plan.RiskReasons[index].Reasons,
		)
	}
	plan.RequiredLanes = cloneReviewPlanStrings(plan.RequiredLanes)
	plan.SelectedLanes = cloneReviewPlanStrings(plan.SelectedLanes)
	plan.SelectionReasons = cloneReviewPlanStrings(plan.SelectionReasons)
	plan.CoverageRequirements = append(
		[]ReviewCoverageRequirement(nil),
		plan.CoverageRequirements...,
	)
	for index := range plan.CoverageRequirements {
		plan.CoverageRequirements[index].ChangedTargets = append(
			[]ReviewCoverageTarget(nil),
			plan.CoverageRequirements[index].ChangedTargets...,
		)
	}
	return plan
}

func nonNilReviewTaskIntentItems(values []string) []string {
	if values == nil {
		return []string{}
	}
	return cloneReviewPlanStrings(values)
}

func cloneReviewPlanStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneReviewPlanRiskReasons(
	values []ReviewPlanRiskReason,
) []ReviewPlanRiskReason {
	if values == nil {
		return nil
	}
	cloned := make([]ReviewPlanRiskReason, len(values))
	copy(cloned, values)
	return cloned
}

func attachReviewPlan(cycle *ReviewCycleState, plan ReviewPlan) error {
	if cycle == nil {
		return errors.New("review cycle is missing")
	}
	if cycle.Inputs == nil {
		return errors.New("review-plan exact-SHA inputs are missing")
	}
	if err := validateReviewPlan(plan, cycle.Policy); err != nil {
		return err
	}
	expected, err := buildReviewPlan(*cycle.Inputs, cycle.Policy)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, plan) {
		return errors.New("review plan does not match exact-SHA inputs and policy")
	}
	cloned := cloneReviewPlan(plan)
	cycle.Plan = &cloned
	return nil
}

// SetReviewPlan makes the immutable exact-SHA planning result visible through
// the durable review-cycle checkpoint without enabling reviewer launch.
func (m *AgentManager) SetReviewPlan(agentID string, plan ReviewPlan) error {
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
	if agent.ReviewCycle.Plan != nil {
		if reflect.DeepEqual(*agent.ReviewCycle.Plan, plan) {
			return nil
		}
		return fmt.Errorf("review plan for %s is already recorded", agent.ID)
	}
	candidate := cloneReviewCycle(agent.ReviewCycle)
	if err := attachReviewPlan(candidate, plan); err != nil {
		return fmt.Errorf("failed to set review plan for %s: %w", agent.ID, err)
	}
	if err := validatePersistedReviewCycleSnapshot(candidate); err != nil {
		return fmt.Errorf("failed to validate review plan for %s: %w", agent.ID, err)
	}
	agent.ReviewCycle = candidate
	agent.LastActivityTime = time.Now().UTC()
	return nil
}

func (b *Orchestrator) persistReviewPlan(agentID string, plan ReviewPlan) error {
	if b == nil || b.agents == nil {
		return errors.New("orchestrator agent manager is not configured")
	}
	if _, err := b.ensureReviewCycleHeadCurrent(
		context.Background(),
		agentID,
	); err != nil {
		return err
	}
	if err := b.agents.SetReviewPlan(agentID, plan); err != nil {
		return err
	}
	if err := b.persistAgentState(); err != nil {
		return fmt.Errorf("failed to persist review plan for %s: %w", agentID, err)
	}
	return nil
}
