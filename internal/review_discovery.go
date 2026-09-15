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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
)

const reviewSynthesisLane = "synthesis"

type ReviewCoverageKind string

const (
	ReviewCoverageAcceptanceCriterion ReviewCoverageKind = "acceptance_criterion"
	ReviewCoverageChangedSymbol       ReviewCoverageKind = "changed_symbol"
	ReviewCoverageChangedBranch       ReviewCoverageKind = "changed_branch"
	ReviewCoverageCallPath            ReviewCoverageKind = "call_path"
	ReviewCoverageStateTransition     ReviewCoverageKind = "state_transition"
	ReviewCoveragePersistenceBoundary ReviewCoverageKind = "persistence_boundary"
	ReviewCoverageRiskDomain          ReviewCoverageKind = "risk_domain"
)

type ReviewCoverageRequirement struct {
	ID              string                 `json:"id"`
	Kind            ReviewCoverageKind     `json:"kind"`
	Description     string                 `json:"description"`
	Critical        bool                   `json:"critical,omitempty"`
	AccountableLane string                 `json:"accountable_lane,omitempty"`
	ChangedTargets  []ReviewCoverageTarget `json:"changed_targets,omitempty"`
}

type ReviewCoverageTarget struct {
	Path      string `json:"path"`
	Symbol    string `json:"symbol,omitempty"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
}

type ReviewCoverageStatus string

const (
	ReviewCoverageCovered       ReviewCoverageStatus = "covered"
	ReviewCoveragePartial       ReviewCoverageStatus = "partial"
	ReviewCoverageNotCovered    ReviewCoverageStatus = "not_covered"
	ReviewCoverageNotApplicable ReviewCoverageStatus = "not_applicable"
)

type ReviewFindingSeverity string

const (
	ReviewFindingSeverityCritical ReviewFindingSeverity = "critical"
	ReviewFindingSeverityHigh     ReviewFindingSeverity = "high"
	ReviewFindingSeverityMedium   ReviewFindingSeverity = "medium"
	ReviewFindingSeverityLow      ReviewFindingSeverity = "low"
)

type ReviewFindingConfidence string

const (
	ReviewFindingConfidenceHigh   ReviewFindingConfidence = "high"
	ReviewFindingConfidenceMedium ReviewFindingConfidence = "medium"
	ReviewFindingConfidenceLow    ReviewFindingConfidence = "low"
)

type ReviewEvidence struct {
	Summary   string `json:"summary"`
	Path      string `json:"path,omitempty"`
	StartLine int    `json:"start_line,omitempty"`
	EndLine   int    `json:"end_line,omitempty"`
}

type ReviewFindingLocation struct {
	Path      string `json:"path"`
	Symbol    string `json:"symbol"`
	StartLine int    `json:"start_line,omitempty"`
	EndLine   int    `json:"end_line,omitempty"`
}

// ReviewFindingCandidate is worker-owned, exact-SHA evidence. CandidateID is
// only provenance; the coordinator derives the logical finding identity.
type ReviewFindingCandidate struct {
	CandidateID       string                  `json:"candidate_id"`
	Summary           string                  `json:"summary"`
	Location          ReviewFindingLocation   `json:"location"`
	BehavioralPath    string                  `json:"behavioral_path"`
	ViolatedInvariant string                  `json:"violated_invariant"`
	Severity          ReviewFindingSeverity   `json:"severity"`
	Confidence        ReviewFindingConfidence `json:"confidence"`
	Evidence          []ReviewEvidence        `json:"evidence"`
}

type ReviewCoverageClaim struct {
	RequirementID string               `json:"requirement_id"`
	Kind          ReviewCoverageKind   `json:"kind"`
	Status        ReviewCoverageStatus `json:"status"`
	Evidence      []ReviewEvidence     `json:"evidence"`
}

type ReviewFindingProvenance struct {
	WorkerID    string                  `json:"worker_id"`
	Lane        string                  `json:"lane"`
	Pass        int                     `json:"pass"`
	CandidateID string                  `json:"candidate_id"`
	Severity    ReviewFindingSeverity   `json:"severity"`
	Confidence  ReviewFindingConfidence `json:"confidence"`
}

type ReviewCanonicalFinding struct {
	ID                string                    `json:"id"`
	Fingerprint       string                    `json:"fingerprint"`
	ExactSHA          string                    `json:"exact_sha"`
	Summary           string                    `json:"summary"`
	Location          ReviewFindingLocation     `json:"location"`
	BehavioralPath    string                    `json:"behavioral_path"`
	ViolatedInvariant string                    `json:"violated_invariant"`
	Severity          ReviewFindingSeverity     `json:"severity"`
	Confidence        ReviewFindingConfidence   `json:"confidence"`
	Evidence          []ReviewEvidence          `json:"evidence"`
	Provenance        []ReviewFindingProvenance `json:"provenance"`
}

type ReviewCoverageGapStatus string

const (
	ReviewCoverageGapMissing     ReviewCoverageGapStatus = "missing"
	ReviewCoverageGapPartial     ReviewCoverageGapStatus = "partial"
	ReviewCoverageGapUncovered   ReviewCoverageGapStatus = "not_covered"
	ReviewCoverageGapConflicting ReviewCoverageGapStatus = "conflicting"
)

type ReviewCoverageGap struct {
	ID            string                  `json:"id"`
	RequirementID string                  `json:"requirement_id"`
	Kind          ReviewCoverageKind      `json:"kind"`
	Description   string                  `json:"description"`
	Status        ReviewCoverageGapStatus `json:"status"`
	Evidence      []ReviewEvidence        `json:"evidence"`
}

type ReviewDiscoveryLaneStatus string

const (
	ReviewDiscoveryLaneQueued    ReviewDiscoveryLaneStatus = "queued"
	ReviewDiscoveryLaneRunning   ReviewDiscoveryLaneStatus = "running"
	ReviewDiscoveryLaneCompleted ReviewDiscoveryLaneStatus = "completed"
	ReviewDiscoveryLaneFailed    ReviewDiscoveryLaneStatus = "failed"
)

type ReviewDiscoveryFailureCode string

const (
	ReviewDiscoveryFailureCanceled ReviewDiscoveryFailureCode = "canceled"
	ReviewDiscoveryFailureLaunch   ReviewDiscoveryFailureCode = "launch_failed"
	ReviewDiscoveryFailureRuntime  ReviewDiscoveryFailureCode = "runtime_failed"
	ReviewDiscoveryFailureArtifact ReviewDiscoveryFailureCode = "artifact_rejected"
	ReviewDiscoveryFailureState    ReviewDiscoveryFailureCode = "state_persistence_failed"
)

type ReviewDiscoveryLaneState struct {
	Lane                 string                     `json:"lane"`
	Required             bool                       `json:"required"`
	Status               ReviewDiscoveryLaneStatus  `json:"status"`
	WorkerID             string                     `json:"worker_id,omitempty"`
	Attempt              int                        `json:"attempt,omitempty"`
	QueuedAt             time.Time                  `json:"queued_at"`
	StartedAt            time.Time                  `json:"started_at,omitempty"`
	CompletedAt          time.Time                  `json:"completed_at,omitempty"`
	FailureCode          ReviewDiscoveryFailureCode `json:"failure_code,omitempty"`
	RecoveredBySynthesis bool                       `json:"recovered_by_synthesis,omitempty"`
}

type ReviewDiscoveryPassState struct {
	Pass        int                        `json:"pass"`
	HeadSHA     string                     `json:"head_sha"`
	QueuedAt    time.Time                  `json:"queued_at"`
	CompletedAt time.Time                  `json:"completed_at,omitempty"`
	Lanes       []ReviewDiscoveryLaneState `json:"lanes"`
}

func (state ReviewDiscoveryPassState) ApprovalEligible(
	gaps []ReviewCoverageGap,
) bool {
	if state.CompletedAt.IsZero() || len(state.Lanes) == 0 ||
		len(gaps) != 0 {
		return false
	}
	for _, lane := range state.Lanes {
		if lane.Required && lane.Status != ReviewDiscoveryLaneCompleted &&
			!(lane.Status == ReviewDiscoveryLaneFailed &&
				lane.RecoveredBySynthesis) {
			return false
		}
		if lane.Status == ReviewDiscoveryLaneQueued ||
			lane.Status == ReviewDiscoveryLaneRunning {
			return false
		}
	}
	return true
}

func buildReviewCoverageRequirements(
	inputs ReviewPlanInputs,
	riskTags []string,
	selectedLanes []string,
) []ReviewCoverageRequirement {
	requirements := make([]ReviewCoverageRequirement, 0,
		len(inputs.AcceptanceCriteria)+len(inputs.ChangedFiles)*3+len(riskTags)*2)
	allTargets := reviewCoverageChangedTargets(inputs)
	for index, criterion := range inputs.AcceptanceCriteria {
		requirements = append(requirements, ReviewCoverageRequirement{
			ID:          fmt.Sprintf("acceptance-criterion:%06d", index+1),
			Kind:        ReviewCoverageAcceptanceCriterion,
			Description: strings.Join(strings.Fields(criterion), " "),
			Critical:    len(allTargets) != 0,
			AccountableLane: reviewAcceptanceCriterionAccountableLane(
				criterion,
				selectedLanes,
			),
			ChangedTargets: append([]ReviewCoverageTarget(nil), allTargets...),
		})
	}
	if len(allTargets) != 0 {
		requirements = append(requirements, ReviewCoverageRequirement{
			ID:          reviewCoverageRequirementID("changed-branches", allTargets),
			Kind:        ReviewCoverageChangedBranch,
			Description: "changed branches across the exact base-to-head diff",
			Critical:    true,
			AccountableLane: reviewCoverageAccountableLane(
				"operations-tests",
				selectedLanes,
			),
			ChangedTargets: append([]ReviewCoverageTarget(nil), allTargets...),
		})
	}
	implementationTargets := reviewCoverageImplementationTargets(inputs)
	if len(implementationTargets) != 0 {
		requirements = append(requirements,
			ReviewCoverageRequirement{
				ID:          reviewCoverageRequirementID("changed-behavior", implementationTargets),
				Kind:        ReviewCoverageChangedSymbol,
				Description: "changed implementation behavior across the exact base-to-head diff",
				Critical:    true,
				AccountableLane: reviewCoverageAccountableLane(
					"contract",
					selectedLanes,
				),
				ChangedTargets: append([]ReviewCoverageTarget(nil), implementationTargets...),
			},
			ReviewCoverageRequirement{
				ID:          reviewCoverageRequirementID("call-paths", implementationTargets),
				Kind:        ReviewCoverageCallPath,
				Description: "callers and downstream effects of changed implementation behavior",
				Critical:    true,
				AccountableLane: reviewCoverageAccountableLane(
					"callers",
					selectedLanes,
				),
				ChangedTargets: append([]ReviewCoverageTarget(nil), implementationTargets...),
			},
		)
	}
	for _, tag := range riskTags {
		switch tag {
		case reviewRiskLifecycle:
			requirements = append(requirements, ReviewCoverageRequirement{
				ID: "state-transition:" + tag, Kind: ReviewCoverageStateTransition,
				Description: tag + " state transitions through changed branches",
				Critical:    len(allTargets) != 0, AccountableLane: reviewCoverageAccountableLane(
					"lifecycle",
					selectedLanes,
				),
				ChangedTargets: append([]ReviewCoverageTarget(nil), allTargets...),
			})
		case reviewRiskConcurrency:
			requirements = append(requirements, ReviewCoverageRequirement{
				ID: "state-transition:" + tag, Kind: ReviewCoverageStateTransition,
				Description: tag + " ordering transitions through changed branches",
				Critical:    len(allTargets) != 0, AccountableLane: reviewCoverageAccountableLane(
					"concurrency-ordering",
					selectedLanes,
				),
				ChangedTargets: append([]ReviewCoverageTarget(nil), allTargets...),
			})
		case reviewRiskPersistence:
			requirements = append(requirements, ReviewCoverageRequirement{
				ID: "persistence-boundary:" + tag, Kind: ReviewCoveragePersistenceBoundary,
				Description: "durable write, restore, and replay boundaries through changed branches",
				Critical:    len(allTargets) != 0, AccountableLane: reviewCoverageAccountableLane(
					"persistence-recovery",
					selectedLanes,
				),
				ChangedTargets: append([]ReviewCoverageTarget(nil), allTargets...),
			})
		}
		requirements = append(requirements, ReviewCoverageRequirement{
			ID:          "risk-domain:" + tag,
			Kind:        ReviewCoverageRiskDomain,
			Description: tag,
		})
	}
	sort.Slice(requirements, func(i, j int) bool {
		if requirements[i].Kind != requirements[j].Kind {
			return requirements[i].Kind < requirements[j].Kind
		}
		return requirements[i].ID < requirements[j].ID
	})
	if requirements == nil {
		return []ReviewCoverageRequirement{}
	}
	return requirements
}

func reviewAcceptanceCriterionAccountableLane(
	criterion string,
	selectedLanes []string,
) string {
	tokens := reviewPlanTokens(criterion)
	for _, token := range tokens {
		switch token {
		case "coverage", "test", "testing", "tests":
			return reviewCoverageAccountableLane(
				"operations-tests",
				selectedLanes,
			)
		}
	}
	return reviewCoverageAccountableLane("contract", selectedLanes)
}

func reviewCoverageAccountableLane(
	preferred string,
	selectedLanes []string,
) string {
	for _, lane := range selectedLanes {
		if lane == preferred {
			return preferred
		}
	}
	return reviewSynthesisLane
}

func reviewCoverageChangedTargets(inputs ReviewPlanInputs) []ReviewCoverageTarget {
	targets := make([]ReviewCoverageTarget, 0)
	for _, file := range inputs.ChangedFiles {
		for _, changed := range file.ChangedRanges {
			targets = append(targets, ReviewCoverageTarget{
				Path: file.Path, Symbol: changed.Symbol,
				StartLine: changed.StartLine, EndLine: changed.EndLine,
			})
		}
	}
	return targets
}

func reviewCoverageImplementationTargets(
	inputs ReviewPlanInputs,
) []ReviewCoverageTarget {
	targets := make([]ReviewCoverageTarget, 0)
	for _, file := range inputs.ChangedFiles {
		if !isReviewPlanImplementationPath(file.Path) {
			continue
		}
		for _, changed := range file.ChangedRanges {
			targets = append(targets, ReviewCoverageTarget{
				Path: file.Path, Symbol: changed.Symbol,
				StartLine: changed.StartLine, EndLine: changed.EndLine,
			})
		}
	}
	return targets
}

func reviewCoverageRequirementID(
	prefix string,
	targets []ReviewCoverageTarget,
) string {
	body, _ := json.Marshal(targets)
	sum := sha256.Sum256(body)
	return prefix + ":" + hex.EncodeToString(sum[:8])
}

func validateReviewCoverageRequirements(
	requirements []ReviewCoverageRequirement,
) error {
	seen := make(map[string]struct{}, len(requirements))
	previous := ""
	for _, requirement := range requirements {
		if !supportedReviewCoverageKind(requirement.Kind) ||
			strings.TrimSpace(requirement.ID) != requirement.ID ||
			requirement.ID == "" ||
			strings.TrimSpace(requirement.Description) == "" {
			return errors.New("review-plan coverage requirement is invalid")
		}
		if requirement.Critical &&
			(strings.TrimSpace(requirement.AccountableLane) == "" ||
				len(requirement.ChangedTargets) == 0) {
			return fmt.Errorf(
				"critical review-plan coverage requirement %q has no accountable lane or changed target",
				requirement.ID,
			)
		}
		for _, target := range requirement.ChangedTargets {
			if !safeReviewRelativePath(target.Path) ||
				target.StartLine <= 0 || target.EndLine < target.StartLine ||
				strings.TrimSpace(target.Symbol) != target.Symbol {
				return fmt.Errorf("review-plan coverage requirement %q has an invalid changed target", requirement.ID)
			}
		}
		key := string(requirement.Kind) + "\x00" + requirement.ID
		if key <= previous {
			return errors.New(
				"review-plan coverage requirements are not in deterministic order",
			)
		}
		if _, duplicate := seen[requirement.ID]; duplicate {
			return fmt.Errorf(
				"review-plan coverage requirement %q is duplicated",
				requirement.ID,
			)
		}
		seen[requirement.ID] = struct{}{}
		previous = key
	}
	return nil
}

func supportedReviewCoverageKind(kind ReviewCoverageKind) bool {
	switch kind {
	case ReviewCoverageAcceptanceCriterion,
		ReviewCoverageChangedSymbol,
		ReviewCoverageChangedBranch,
		ReviewCoverageCallPath,
		ReviewCoverageStateTransition,
		ReviewCoveragePersistenceBoundary,
		ReviewCoverageRiskDomain:
		return true
	default:
		return false
	}
}

func validateReviewFindingCandidate(candidate ReviewFindingCandidate) error {
	if !safeReviewCandidateID(candidate.CandidateID) {
		return errors.New("candidate_id is missing or invalid")
	}
	if strings.TrimSpace(candidate.Summary) == "" {
		return errors.New("summary is required")
	}
	if strings.TrimSpace(candidate.BehavioralPath) == "" {
		return errors.New("behavioral_path is required")
	}
	if strings.TrimSpace(candidate.ViolatedInvariant) == "" {
		return errors.New("violated_invariant is required")
	}
	if !supportedReviewFindingSeverity(candidate.Severity) {
		return errors.New("severity is unsupported")
	}
	if !supportedReviewFindingConfidence(candidate.Confidence) {
		return errors.New("confidence is unsupported")
	}
	if candidate.Evidence == nil || len(candidate.Evidence) == 0 {
		return errors.New("evidence must contain at least one item")
	}
	if err := validateReviewFindingLocation(candidate.Location); err != nil {
		return err
	}
	for index, evidence := range candidate.Evidence {
		if err := validateReviewEvidence(evidence); err != nil {
			return fmt.Errorf("evidence[%d]: %w", index, err)
		}
	}
	return nil
}

func validateReviewFindingCandidates(
	candidates []ReviewFindingCandidate,
) error {
	seen := make(map[string]struct{}, len(candidates))
	for index, candidate := range candidates {
		if err := validateReviewFindingCandidate(candidate); err != nil {
			return fmt.Errorf("candidates[%d]: %w", index, err)
		}
		if _, duplicate := seen[candidate.CandidateID]; duplicate {
			return fmt.Errorf(
				"review finding candidate %q is duplicated",
				candidate.CandidateID,
			)
		}
		seen[candidate.CandidateID] = struct{}{}
	}
	return nil
}

func safeReviewCandidateID(value string) bool {
	if value == "" || len(value) > 128 ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if unicode.IsLetter(char) || unicode.IsDigit(char) ||
			char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func validateReviewFindingLocation(location ReviewFindingLocation) error {
	if !safeReviewRelativePath(location.Path) {
		return errors.New("location.path is missing or invalid")
	}
	if strings.TrimSpace(location.Symbol) == "" {
		return errors.New("location.symbol is required")
	}
	if (location.StartLine == 0) != (location.EndLine == 0) ||
		location.StartLine < 0 || location.EndLine < location.StartLine {
		return errors.New("location line range is invalid")
	}
	return nil
}

func validateReviewEvidence(evidence ReviewEvidence) error {
	if strings.TrimSpace(evidence.Summary) == "" {
		return errors.New("summary is required")
	}
	if (evidence.StartLine == 0) != (evidence.EndLine == 0) ||
		evidence.StartLine < 0 || evidence.EndLine < evidence.StartLine {
		return errors.New("line range is invalid")
	}
	if evidence.Path == "" {
		if evidence.StartLine != 0 || evidence.EndLine != 0 {
			return errors.New("review evidence lines require a path")
		}
		return nil
	}
	if !safeReviewRelativePath(evidence.Path) {
		return errors.New("review evidence path is invalid")
	}
	return nil
}

func safeReviewRelativePath(path string) bool {
	_, ok := canonicalReviewRelativePath(path)
	return ok
}

// canonicalReviewRelativePath validates the worker-facing path syntax and
// returns the one repository-relative representation used after artifact
// intake. A leading "./" is harmless, but traversal segments are rejected
// instead of being cleaned into a different path.
func canonicalReviewRelativePath(path string) (string, bool) {
	if path == "" || strings.TrimSpace(path) != path ||
		filepath.IsAbs(path) || strings.Contains(path, `\`) {
		return "", false
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == ".." {
			return "", false
		}
	}
	canonical := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	if canonical == "." || canonical == ".." ||
		strings.HasPrefix(canonical, "../") {
		return "", false
	}
	return canonical, true
}

func supportedReviewFindingSeverity(severity ReviewFindingSeverity) bool {
	switch severity {
	case ReviewFindingSeverityCritical,
		ReviewFindingSeverityHigh,
		ReviewFindingSeverityMedium,
		ReviewFindingSeverityLow:
		return true
	default:
		return false
	}
}

func supportedReviewFindingConfidence(confidence ReviewFindingConfidence) bool {
	switch confidence {
	case ReviewFindingConfidenceHigh,
		ReviewFindingConfidenceMedium,
		ReviewFindingConfidenceLow:
		return true
	default:
		return false
	}
}

func validateReviewCoverageClaims(
	claims []ReviewCoverageClaim,
	requirements []ReviewCoverageRequirement,
) error {
	return validateReviewCoverageClaimSet(claims, requirements, false)
}

func validateReviewCoverageClaimSet(
	claims []ReviewCoverageClaim,
	requirements []ReviewCoverageRequirement,
	allowDuplicateRequirements bool,
) error {
	supported := make(map[string]ReviewCoverageRequirement, len(requirements))
	for _, requirement := range requirements {
		supported[requirement.ID] = requirement
	}
	seen := make(map[string]struct{}, len(claims))
	for index, claim := range claims {
		if strings.TrimSpace(claim.RequirementID) != claim.RequirementID ||
			claim.RequirementID == "" {
			return fmt.Errorf("coverage[%d].requirement_id is missing or invalid", index)
		}
		if !supportedReviewCoverageKind(claim.Kind) {
			return fmt.Errorf("coverage[%d].kind is unsupported", index)
		}
		if claim.Evidence == nil {
			return fmt.Errorf("coverage[%d].evidence is required", index)
		}
		switch claim.Status {
		case ReviewCoverageCovered,
			ReviewCoveragePartial,
			ReviewCoverageNotCovered,
			ReviewCoverageNotApplicable:
		default:
			return fmt.Errorf("coverage[%d].status is unsupported", index)
		}
		if claim.Status != ReviewCoverageNotCovered &&
			len(claim.Evidence) == 0 {
			return fmt.Errorf(
				"coverage[%d].evidence must contain at least one item for status %q",
				index,
				claim.Status,
			)
		}
		if _, duplicate := seen[claim.RequirementID]; duplicate &&
			!allowDuplicateRequirements {
			return fmt.Errorf(
				"review coverage claim %q is duplicated",
				claim.RequirementID,
			)
		}
		seen[claim.RequirementID] = struct{}{}
		if requirements != nil {
			requirement, ok := supported[claim.RequirementID]
			if !ok || requirement.Kind != claim.Kind {
				return fmt.Errorf(
					"review coverage claim %q is not required by the exact-SHA plan",
					claim.RequirementID,
				)
			}
			if claim.Status == ReviewCoverageNotApplicable &&
				requirement.Kind == ReviewCoverageAcceptanceCriterion {
				return fmt.Errorf(
					"review coverage claim %q cannot mark an acceptance criterion not applicable",
					claim.RequirementID,
				)
			}
		}
		for evidenceIndex, evidence := range claim.Evidence {
			if err := validateReviewEvidence(evidence); err != nil {
				return fmt.Errorf(
					"coverage[%d].evidence[%d]: %w",
					index,
					evidenceIndex,
					err,
				)
			}
		}
	}
	return nil
}

func reviewCoverageClaimHasCausalChangedLine(
	claim ReviewCoverageClaim,
	requirement ReviewCoverageRequirement,
) bool {
	for _, evidence := range claim.Evidence {
		if evidence.Path == "" || evidence.StartLine <= 0 {
			continue
		}
		for _, target := range requirement.ChangedTargets {
			if evidence.Path == target.Path &&
				evidence.StartLine <= target.EndLine &&
				target.StartLine <= evidence.EndLine {
				return true
			}
		}
	}
	return false
}

func reviewCoverageClaimCoversRequirement(
	claim ReviewCoverageClaim,
	requirement ReviewCoverageRequirement,
) bool {
	if claim.Status != ReviewCoverageCovered &&
		claim.Status != ReviewCoverageNotApplicable {
		return false
	}
	if claim.Status == ReviewCoverageNotApplicable &&
		requirement.Kind == ReviewCoverageAcceptanceCriterion {
		return false
	}
	if !requirement.Critical ||
		requirement.Kind == ReviewCoverageAcceptanceCriterion {
		return true
	}
	return reviewCoverageClaimHasCausalChangedLine(claim, requirement)
}

func reviewCoverageClaimsForLane(
	claims []ReviewCoverageClaim,
	requirements []ReviewCoverageRequirement,
	lane string,
) []ReviewCoverageClaim {
	byID := make(map[string]ReviewCoverageRequirement, len(requirements))
	for _, requirement := range requirements {
		byID[requirement.ID] = requirement
	}
	result := make([]ReviewCoverageClaim, 0, len(claims))
	for _, claim := range claims {
		requirement := byID[claim.RequirementID]
		if requirement.Critical && lane != reviewSynthesisLane &&
			lane != requirement.AccountableLane {
			continue
		}
		result = append(result, claim)
	}
	return result
}

func reviewCoverageRequirementsForLane(
	requirements []ReviewCoverageRequirement,
	lane string,
) []ReviewCoverageRequirement {
	result := make([]ReviewCoverageRequirement, 0, len(requirements))
	for _, requirement := range requirements {
		if requirement.Critical && lane != reviewSynthesisLane &&
			lane != requirement.AccountableLane {
			continue
		}
		result = append(result, requirement)
	}
	return result
}

type reviewCandidateReport struct {
	ExactSHA  string
	WorkerID  string
	Lane      string
	Pass      int
	Candidate ReviewFindingCandidate
}

func canonicalizeReviewFindingReports(
	reports []reviewCandidateReport,
) ([]ReviewCanonicalFinding, error) {
	type fingerprintedReport struct {
		report      reviewCandidateReport
		fingerprint string
	}
	items := make([]fingerprintedReport, 0, len(reports))
	for _, report := range reports {
		if validateCanonicalGitObjectID(report.ExactSHA) != nil ||
			strings.TrimSpace(report.WorkerID) == "" ||
			strings.TrimSpace(report.Lane) == "" ||
			report.Pass <= 0 {
			return nil, errors.New("review candidate origin is invalid")
		}
		if err := validateReviewFindingCandidate(report.Candidate); err != nil {
			return nil, err
		}
		fingerprint, err := reviewFindingFingerprint(report.Candidate)
		if err != nil {
			return nil, err
		}
		items = append(items, fingerprintedReport{
			report:      report,
			fingerprint: fingerprint,
		})
	}
	parents := make([]int, len(items))
	for index := range parents {
		parents[index] = index
	}
	var root func(int) int
	root = func(index int) int {
		if parents[index] != index {
			parents[index] = root(parents[index])
		}
		return parents[index]
	}
	for left := range items {
		for right := left + 1; right < len(items); right++ {
			if items[left].report.ExactSHA != items[right].report.ExactSHA ||
				!reviewFindingCandidatesSemanticallyEquivalent(
					items[left].report.Candidate,
					items[right].report.Candidate,
				) {
				continue
			}
			leftRoot := root(left)
			rightRoot := root(right)
			if leftRoot < rightRoot {
				parents[rightRoot] = leftRoot
			} else {
				parents[leftRoot] = rightRoot
			}
		}
	}
	grouped := make(map[int][]fingerprintedReport)
	for index, item := range items {
		grouped[root(index)] = append(grouped[root(index)], item)
	}

	type canonicalGroup struct {
		fingerprint string
		reports     []reviewCandidateReport
	}
	groups := make([]canonicalGroup, 0, len(grouped))
	for _, members := range grouped {
		sort.Slice(members, func(i, j int) bool {
			leftKey := reviewCanonicalRepresentativeKey(members[i].report)
			rightKey := reviewCanonicalRepresentativeKey(members[j].report)
			if leftKey != rightKey {
				return leftKey < rightKey
			}
			return members[i].fingerprint < members[j].fingerprint
		})
		group := canonicalGroup{fingerprint: members[0].fingerprint}
		for _, member := range members {
			group.reports = append(group.reports, member.report)
		}
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].fingerprint < groups[j].fingerprint
	})
	findings := make([]ReviewCanonicalFinding, 0, len(groups))
	for _, group := range groups {
		finding, err := buildCanonicalReviewFinding(
			group.fingerprint,
			group.reports,
		)
		if err != nil {
			return nil, err
		}
		findings = append(findings, finding)
	}
	if findings == nil {
		return []ReviewCanonicalFinding{}, nil
	}
	return findings, nil
}

func reviewCanonicalRepresentativeKey(report reviewCandidateReport) string {
	return fmt.Sprintf(
		"%06d\x00%s\x00%s\x00%s",
		report.Pass,
		report.Lane,
		report.WorkerID,
		report.Candidate.CandidateID,
	)
}

func reviewFindingCandidatesSemanticallyEquivalent(
	left ReviewFindingCandidate,
	right ReviewFindingCandidate,
) bool {
	leftFingerprint, leftErr := reviewFindingFingerprint(left)
	rightFingerprint, rightErr := reviewFindingFingerprint(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	if leftFingerprint == rightFingerprint {
		return true
	}
	leftLocation := normalizedReviewFindingLocation(left.Location)
	rightLocation := normalizedReviewFindingLocation(right.Location)
	if leftLocation.Path != rightLocation.Path {
		return false
	}
	invariantThreshold := 0.3
	behaviorThreshold := 0.55
	if leftLocation.Symbol != rightLocation.Symbol {
		if !reviewFindingLocationsAreNestedAndOverlapping(
			leftLocation,
			rightLocation,
		) {
			return false
		}
		invariantThreshold = 0.3
		behaviorThreshold = 0.15
	}
	if !reflect.DeepEqual(
		reviewFindingNumericTokens(left.ViolatedInvariant),
		reviewFindingNumericTokens(right.ViolatedInvariant),
	) {
		return false
	}
	return reviewFindingTokenSimilarity(
		left.ViolatedInvariant,
		right.ViolatedInvariant,
	) >= invariantThreshold && reviewFindingTokenSimilarity(
		left.BehavioralPath,
		right.BehavioralPath,
	) >= behaviorThreshold
}

func reviewFindingLocationsAreNestedAndOverlapping(
	left ReviewFindingLocation,
	right ReviewFindingLocation,
) bool {
	leftSymbol := strings.TrimSpace(left.Symbol)
	rightSymbol := strings.TrimSpace(right.Symbol)
	nested := strings.HasPrefix(leftSymbol, rightSymbol+".") ||
		strings.HasPrefix(rightSymbol, leftSymbol+".")
	return nested &&
		left.StartLine <= right.EndLine &&
		right.StartLine <= left.EndLine
}

func reviewFindingNumericTokens(value string) []string {
	tokens := make([]string, 0)
	for _, token := range strings.FieldsFunc(value, func(char rune) bool {
		return !unicode.IsDigit(char)
	}) {
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return uniqueSortedStrings(tokens)
}

func reviewFindingTokenSimilarity(left, right string) float64 {
	leftTokens := reviewFindingSemanticTokens(left)
	rightTokens := reviewFindingSemanticTokens(right)
	if len(leftTokens) == 0 || len(rightTokens) == 0 {
		return 0
	}
	intersection := 0
	union := make(map[string]struct{}, len(leftTokens)+len(rightTokens))
	for token := range leftTokens {
		union[token] = struct{}{}
		if _, ok := rightTokens[token]; ok {
			intersection++
		}
	}
	for token := range rightTokens {
		union[token] = struct{}{}
	}
	return float64(intersection) / float64(len(union))
}

func reviewFindingSemanticTokens(value string) map[string]struct{} {
	stopWords := map[string]struct{}{
		"a": {}, "an": {}, "and": {}, "are": {}, "be": {}, "by": {},
		"each": {}, "every": {}, "for": {}, "from": {}, "is": {},
		"must": {}, "of": {}, "or": {}, "that": {}, "the": {},
		"this": {}, "to": {}, "when": {}, "while": {}, "with": {},
	}
	tokens := make(map[string]struct{})
	for _, token := range strings.FieldsFunc(
		strings.ToLower(value),
		func(char rune) bool {
			return !unicode.IsLetter(char) && !unicode.IsDigit(char)
		},
	) {
		if _, skip := stopWords[token]; skip || token == "" {
			continue
		}
		for _, suffix := range []string{"ing", "ed", "es", "s"} {
			if len(token) > len(suffix)+3 && strings.HasSuffix(token, suffix) {
				token = strings.TrimSuffix(token, suffix)
				break
			}
		}
		tokens[token] = struct{}{}
	}
	return tokens
}

func reviewFindingFingerprint(candidate ReviewFindingCandidate) (string, error) {
	if err := validateReviewFindingCandidate(candidate); err != nil {
		return "", err
	}
	identity := struct {
		Path              string `json:"path"`
		Symbol            string `json:"symbol"`
		BehavioralPath    string `json:"behavioral_path"`
		ViolatedInvariant string `json:"violated_invariant"`
	}{
		Path:              candidate.Location.Path,
		Symbol:            normalizedReviewFindingText(candidate.Location.Symbol),
		BehavioralPath:    normalizedReviewFindingText(candidate.BehavioralPath),
		ViolatedInvariant: normalizedReviewFindingText(candidate.ViolatedInvariant),
	}
	body, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize review finding: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func normalizedReviewFindingText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func displayReviewFindingText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func buildCanonicalReviewFinding(
	fingerprint string,
	reports []reviewCandidateReport,
) (ReviewCanonicalFinding, error) {
	if len(reports) == 0 {
		return ReviewCanonicalFinding{}, errors.New("review finding has no reports")
	}
	headSHA := reports[0].ExactSHA
	representative := reports[0].Candidate
	for _, report := range reports {
		candidateFingerprint, err := reviewFindingFingerprint(report.Candidate)
		if err != nil {
			return ReviewCanonicalFinding{}, err
		}
		if candidateFingerprint == fingerprint {
			representative = report.Candidate
			break
		}
	}
	summaries := make([]string, 0, len(reports))
	evidence := make([]ReviewEvidence, 0)
	provenance := make([]ReviewFindingProvenance, 0, len(reports))
	severity := ReviewFindingSeverityLow
	confidence := ReviewFindingConfidenceLow
	location := normalizedReviewFindingLocation(representative.Location)
	behavior := normalizedReviewFindingText(representative.BehavioralPath)
	invariant := normalizedReviewFindingText(representative.ViolatedInvariant)
	for _, report := range reports {
		if report.ExactSHA != headSHA {
			return ReviewCanonicalFinding{}, errors.New(
				"equivalent review candidates span different exact SHAs",
			)
		}
		summaries = append(
			summaries,
			displayReviewFindingText(report.Candidate.Summary),
		)
		if reviewFindingSeverityRank(report.Candidate.Severity) >
			reviewFindingSeverityRank(severity) {
			severity = report.Candidate.Severity
		}
		if reviewFindingConfidenceRank(report.Candidate.Confidence) >
			reviewFindingConfidenceRank(confidence) {
			confidence = report.Candidate.Confidence
		}
		for _, item := range report.Candidate.Evidence {
			evidence = append(evidence, normalizedReviewEvidence(item))
		}
		provenance = append(provenance, ReviewFindingProvenance{
			WorkerID:    report.WorkerID,
			Lane:        report.Lane,
			Pass:        report.Pass,
			CandidateID: report.Candidate.CandidateID,
			Severity:    report.Candidate.Severity,
			Confidence:  report.Candidate.Confidence,
		})
	}
	sort.Strings(summaries)
	evidence = uniqueSortedReviewEvidence(evidence)
	sort.Slice(provenance, func(i, j int) bool {
		return reviewFindingProvenanceKey(provenance[i]) <
			reviewFindingProvenanceKey(provenance[j])
	})
	return ReviewCanonicalFinding{
		ID:                "finding-" + fingerprint,
		Fingerprint:       fingerprint,
		ExactSHA:          headSHA,
		Summary:           summaries[0],
		Location:          location,
		BehavioralPath:    behavior,
		ViolatedInvariant: invariant,
		Severity:          severity,
		Confidence:        confidence,
		Evidence:          evidence,
		Provenance:        provenance,
	}, nil
}

func normalizedReviewFindingLocation(
	location ReviewFindingLocation,
) ReviewFindingLocation {
	return ReviewFindingLocation{
		Path:      location.Path,
		Symbol:    normalizedReviewFindingText(location.Symbol),
		StartLine: location.StartLine,
		EndLine:   location.EndLine,
	}
}

func reviewFindingLocationLess(
	left ReviewFindingLocation,
	right ReviewFindingLocation,
) bool {
	leftKey := fmt.Sprintf(
		"%s\x00%s\x00%010d\x00%010d",
		left.Path,
		left.Symbol,
		left.StartLine,
		left.EndLine,
	)
	rightKey := fmt.Sprintf(
		"%s\x00%s\x00%010d\x00%010d",
		right.Path,
		right.Symbol,
		right.StartLine,
		right.EndLine,
	)
	return leftKey < rightKey
}

func reviewFindingSeverityRank(severity ReviewFindingSeverity) int {
	switch severity {
	case ReviewFindingSeverityCritical:
		return 4
	case ReviewFindingSeverityHigh:
		return 3
	case ReviewFindingSeverityMedium:
		return 2
	case ReviewFindingSeverityLow:
		return 1
	default:
		return 0
	}
}

func reviewFindingConfidenceRank(confidence ReviewFindingConfidence) int {
	switch confidence {
	case ReviewFindingConfidenceHigh:
		return 3
	case ReviewFindingConfidenceMedium:
		return 2
	case ReviewFindingConfidenceLow:
		return 1
	default:
		return 0
	}
}

func normalizedReviewEvidence(evidence ReviewEvidence) ReviewEvidence {
	return ReviewEvidence{
		Summary:   displayReviewFindingText(evidence.Summary),
		Path:      evidence.Path,
		StartLine: evidence.StartLine,
		EndLine:   evidence.EndLine,
	}
}

func reviewEvidenceKey(evidence ReviewEvidence) string {
	return fmt.Sprintf(
		"%s\x00%010d\x00%010d\x00%s",
		evidence.Path,
		evidence.StartLine,
		evidence.EndLine,
		evidence.Summary,
	)
}

func uniqueSortedReviewEvidence(values []ReviewEvidence) []ReviewEvidence {
	unique := make(map[string]ReviewEvidence, len(values))
	for _, evidence := range values {
		unique[reviewEvidenceKey(evidence)] = evidence
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ReviewEvidence, 0, len(keys))
	for _, key := range keys {
		result = append(result, unique[key])
	}
	if result == nil {
		return []ReviewEvidence{}
	}
	return result
}

func reviewFindingProvenanceKey(provenance ReviewFindingProvenance) string {
	return fmt.Sprintf(
		"%s\x00%06d\x00%s\x00%s\x00%s\x00%s",
		provenance.Lane,
		provenance.Pass,
		provenance.WorkerID,
		provenance.CandidateID,
		provenance.Severity,
		provenance.Confidence,
	)
}

func aggregateReviewCoverageGaps(
	requirements []ReviewCoverageRequirement,
	claims []ReviewCoverageClaim,
) ([]ReviewCoverageGap, error) {
	if err := validateReviewCoverageRequirements(requirements); err != nil {
		return nil, err
	}
	if err := validateReviewCoverageClaimSet(
		claims,
		requirements,
		true,
	); err != nil {
		return nil, err
	}
	byRequirement := make(map[string][]ReviewCoverageClaim)
	for _, claim := range claims {
		byRequirement[claim.RequirementID] = append(
			byRequirement[claim.RequirementID],
			claim,
		)
	}
	gaps := make([]ReviewCoverageGap, 0)
	for _, requirement := range requirements {
		requirementClaims := byRequirement[requirement.ID]
		covered := false
		disputed := false
		terminalStatus := ReviewCoverageStatus("")
		status := ReviewCoverageGapUncovered
		evidence := make([]ReviewEvidence, 0)
		for _, claim := range requirementClaims {
			coversRequirement := reviewCoverageClaimCoversRequirement(
				claim,
				requirement,
			)
			if coversRequirement {
				covered = true
				if terminalStatus != "" && terminalStatus != claim.Status {
					disputed = true
				} else {
					terminalStatus = claim.Status
				}
			} else {
				disputed = true
			}
			if claim.Status == ReviewCoveragePartial ||
				((claim.Status == ReviewCoverageCovered ||
					claim.Status == ReviewCoverageNotApplicable) &&
					!coversRequirement) {
				status = ReviewCoverageGapPartial
			}
			for _, item := range claim.Evidence {
				evidence = append(evidence, normalizedReviewEvidence(item))
			}
		}
		if covered && !disputed {
			continue
		}
		if covered && disputed {
			status = ReviewCoverageGapConflicting
		}
		if len(requirementClaims) == 0 {
			status = ReviewCoverageGapMissing
		}
		fingerprintBody, _ := json.Marshal(struct {
			Kind ReviewCoverageKind `json:"kind"`
			ID   string             `json:"id"`
		}{
			Kind: requirement.Kind,
			ID:   requirement.ID,
		})
		sum := sha256.Sum256(fingerprintBody)
		gaps = append(gaps, ReviewCoverageGap{
			ID:            "coverage-gap-" + hex.EncodeToString(sum[:]),
			RequirementID: requirement.ID,
			Kind:          requirement.Kind,
			Description:   requirement.Description,
			Status:        status,
			Evidence:      uniqueSortedReviewEvidence(evidence),
		})
	}
	if gaps == nil {
		return []ReviewCoverageGap{}, nil
	}
	return gaps, nil
}

func cloneReviewDiscoveryPasses(
	values []ReviewDiscoveryPassState,
) []ReviewDiscoveryPassState {
	if values == nil {
		return nil
	}
	cloned := make([]ReviewDiscoveryPassState, len(values))
	copy(cloned, values)
	for index := range cloned {
		cloned[index].Lanes = append(
			[]ReviewDiscoveryLaneState(nil),
			values[index].Lanes...,
		)
	}
	return cloned
}

func latestReviewDiscoveryPass(
	cycle *ReviewCycleState,
) (*ReviewDiscoveryPassState, bool) {
	if cycle == nil || len(cycle.DiscoveryPasses) == 0 {
		return nil, false
	}
	latestIndex := 0
	for index := range cycle.DiscoveryPasses {
		if cycle.DiscoveryPasses[index].Pass >
			cycle.DiscoveryPasses[latestIndex].Pass {
			latestIndex = index
		}
	}
	return &cycle.DiscoveryPasses[latestIndex], true
}

func reviewDiscoveryPassByNumber(
	cycle *ReviewCycleState,
	pass int,
) (*ReviewDiscoveryPassState, bool) {
	if cycle == nil || pass <= 0 {
		return nil, false
	}
	for index := range cycle.DiscoveryPasses {
		if cycle.DiscoveryPasses[index].Pass == pass {
			return &cycle.DiscoveryPasses[index], true
		}
	}
	return nil, false
}

func scheduledReviewDiscoveryLane(
	cycle *ReviewCycleState,
	pass int,
	lane string,
) (*ReviewDiscoveryLaneState, bool) {
	if cycle == nil {
		return nil, false
	}
	for passIndex := range cycle.DiscoveryPasses {
		if cycle.DiscoveryPasses[passIndex].Pass != pass {
			continue
		}
		for laneIndex := range cycle.DiscoveryPasses[passIndex].Lanes {
			state := &cycle.DiscoveryPasses[passIndex].Lanes[laneIndex]
			if state.Lane == lane {
				return state, true
			}
		}
	}
	return nil, false
}

func completeReviewDiscoveryLaneFromReceipt(
	cycle *ReviewCycleState,
	ownership ReviewWorkerOwnership,
	envelope ReviewArtifactEnvelope,
	completedAt time.Time,
) error {
	if envelope.Phase != ReviewArtifactPhaseDiscovery ||
		envelope.Payload.Discovery == nil {
		return nil
	}
	laneState, scheduled := scheduledReviewDiscoveryLane(
		cycle,
		envelope.Pass,
		envelope.Lane,
	)
	if !scheduled {
		return nil
	}
	if laneState.Status != ReviewDiscoveryLaneRunning ||
		(laneState.WorkerID != "" &&
			(laneState.WorkerID != ownership.OwnerID ||
				laneState.Attempt != ownership.Attempt)) {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureConflict,
		)
	}
	if err := validateReviewCoverageClaims(
		envelope.Payload.Discovery.Coverage,
		cycle.Plan.CoverageRequirements,
	); err != nil {
		return malformedReviewArtifactError(err.Error())
	}
	laneState.WorkerID = ownership.OwnerID
	laneState.Attempt = ownership.Attempt
	laneState.Status = ReviewDiscoveryLaneCompleted
	laneState.CompletedAt = completedAt.UTC()
	laneState.FailureCode = ""
	laneState.RecoveredBySynthesis = false
	if envelope.Lane == reviewSynthesisLane {
		for passIndex := range cycle.DiscoveryPasses {
			if cycle.DiscoveryPasses[passIndex].Pass != envelope.Pass {
				continue
			}
			for laneIndex := range cycle.DiscoveryPasses[passIndex].Lanes {
				candidate := &cycle.DiscoveryPasses[passIndex].Lanes[laneIndex]
				if candidate.Lane != reviewSynthesisLane &&
					candidate.Status == ReviewDiscoveryLaneFailed {
					candidate.RecoveredBySynthesis = true
				}
			}
		}
	}
	updateReviewDiscoveryPassCompletion(cycle, envelope.Pass)
	return rebuildReviewDiscoveryDerivedState(cycle)
}

func updateReviewDiscoveryPassCompletion(
	cycle *ReviewCycleState,
	pass int,
) {
	for passIndex := range cycle.DiscoveryPasses {
		state := &cycle.DiscoveryPasses[passIndex]
		if state.Pass != pass {
			continue
		}
		latest := time.Time{}
		for _, lane := range state.Lanes {
			if lane.Status != ReviewDiscoveryLaneCompleted &&
				lane.Status != ReviewDiscoveryLaneFailed {
				state.CompletedAt = time.Time{}
				return
			}
			if lane.CompletedAt.After(latest) {
				latest = lane.CompletedAt
			}
		}
		state.CompletedAt = latest
		return
	}
}

func rebuildReviewDiscoveryDerivedState(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("review cycle is missing")
	}
	if len(cycle.DiscoveryPasses) == 0 {
		cycle.CanonicalFindings = nil
		cycle.UnresolvedCoverage = nil
		return reconcileReviewFindingAssignments(cycle)
	}
	if cycle.Plan == nil {
		return errors.New("review discovery has no exact-SHA plan")
	}
	latest, _ := latestReviewDiscoveryPass(cycle)
	synthesisWorkerID, synthesisAttempt, synthesisAuthoritative :=
		authoritativeReviewSynthesisReceipt(cycle, latest)
	reports := make([]reviewCandidateReport, 0)
	coverage := make([]ReviewCoverageClaim, 0)
	type challengeCoverageReport struct {
		acceptedAt time.Time
		workerID   string
		claims     []ReviewCoverageClaim
	}
	challengeCoverage := make([]challengeCoverageReport, 0)
	for _, receipt := range cycle.ArtifactReceipts {
		switch receipt.Phase {
		case ReviewArtifactPhaseDiscovery:
			if receipt.Envelope.Payload.Discovery == nil {
				continue
			}
			laneState, scheduled := scheduledReviewDiscoveryLane(
				cycle,
				receipt.Pass,
				receipt.Lane,
			)
			if !scheduled ||
				laneState.Status != ReviewDiscoveryLaneCompleted ||
				laneState.WorkerID != receipt.WorkerID ||
				laneState.Attempt != receipt.Attempt {
				continue
			}
			payload := receipt.Envelope.Payload.Discovery
			if err := validateReviewCoverageClaims(
				payload.Coverage,
				cycle.Plan.CoverageRequirements,
			); err != nil {
				return err
			}
			includeCandidates := !synthesisAuthoritative ||
				(receipt.Pass == latest.Pass &&
					receipt.Lane == reviewSynthesisLane &&
					receipt.WorkerID == synthesisWorkerID &&
					receipt.Attempt == synthesisAttempt)
			if includeCandidates {
				for _, candidate := range payload.Candidates {
					reports = append(reports, reviewCandidateReport{
						ExactSHA:  receipt.Envelope.ExactSHA,
						WorkerID:  receipt.WorkerID,
						Lane:      receipt.Lane,
						Pass:      receipt.Pass,
						Candidate: candidate,
					})
				}
			}
			if latest != nil && receipt.Pass == latest.Pass {
				coverage = append(coverage, reviewCoverageClaimsForLane(
					payload.Coverage,
					cycle.Plan.CoverageRequirements,
					receipt.Lane,
				)...)
			}
		case ReviewArtifactPhaseChallenge:
			if receipt.Envelope.Payload.Challenge == nil {
				continue
			}
			assignment, scheduled :=
				scheduledReviewChallengeAssignment(
					cycle,
					receipt.Pass,
					receipt.Lane,
				)
			if !scheduled ||
				assignment.Status != ReviewChallengeCompleted ||
				assignment.WorkerID != receipt.WorkerID ||
				assignment.Attempt != receipt.Attempt {
				continue
			}
			payload := receipt.Envelope.Payload.Challenge
			if synthesisAuthoritative && receipt.Pass < latest.Pass {
				continue
			}
			if err := validateReviewCoverageClaims(
				payload.Coverage,
				cycle.Plan.CoverageRequirements,
			); err != nil {
				return err
			}
			for _, candidate := range payload.Candidates {
				reports = append(reports, reviewCandidateReport{
					ExactSHA:  receipt.Envelope.ExactSHA,
					WorkerID:  receipt.WorkerID,
					Lane:      receipt.Lane,
					Pass:      receipt.Pass,
					Candidate: candidate,
				})
			}
			challengeCoverage = append(
				challengeCoverage,
				challengeCoverageReport{
					acceptedAt: receipt.AcceptedAt,
					workerID:   receipt.WorkerID,
					claims:     payload.Coverage,
				},
			)
		}
	}
	coverageByRequirement := make(
		map[string][]ReviewCoverageClaim,
		len(cycle.Plan.CoverageRequirements),
	)
	for _, claim := range coverage {
		coverageByRequirement[claim.RequirementID] = append(
			coverageByRequirement[claim.RequirementID],
			claim,
		)
	}
	sort.Slice(challengeCoverage, func(i, j int) bool {
		if challengeCoverage[i].acceptedAt.Equal(
			challengeCoverage[j].acceptedAt,
		) {
			return challengeCoverage[i].workerID <
				challengeCoverage[j].workerID
		}
		return challengeCoverage[i].acceptedAt.Before(
			challengeCoverage[j].acceptedAt,
		)
	})
	for _, report := range challengeCoverage {
		for _, claim := range report.claims {
			coverageByRequirement[claim.RequirementID] =
				[]ReviewCoverageClaim{claim}
		}
	}
	coverage = coverage[:0]
	for _, requirement := range cycle.Plan.CoverageRequirements {
		coverage = append(
			coverage,
			coverageByRequirement[requirement.ID]...,
		)
	}
	findings, err := canonicalizeReviewFindingReports(reports)
	if err != nil {
		return err
	}
	findings = mergeReviewLedgerSeedFindings(
		findings,
		reviewLedgerSeedFindings(cycle),
	)
	gaps, err := aggregateReviewCoverageGaps(
		cycle.Plan.CoverageRequirements,
		coverage,
	)
	if err != nil {
		return err
	}
	cycle.CanonicalFindings = findings
	cycle.UnresolvedCoverage = gaps
	return reconcileReviewFindingAssignments(cycle)
}

func authoritativeReviewSynthesisReceipt(
	cycle *ReviewCycleState,
	latest *ReviewDiscoveryPassState,
) (string, int, bool) {
	if cycle == nil || latest == nil {
		return "", 0, false
	}
	for _, receipt := range cycle.ArtifactReceipts {
		if receipt.Phase != ReviewArtifactPhaseDiscovery ||
			receipt.Pass != latest.Pass ||
			receipt.Lane != reviewSynthesisLane ||
			receipt.Envelope.Payload.Discovery == nil {
			continue
		}
		lane, scheduled := scheduledReviewDiscoveryLane(
			cycle,
			receipt.Pass,
			receipt.Lane,
		)
		if scheduled && lane.Status == ReviewDiscoveryLaneCompleted &&
			lane.WorkerID == receipt.WorkerID &&
			lane.Attempt == receipt.Attempt {
			return receipt.WorkerID, receipt.Attempt, true
		}
	}
	return "", 0, false
}

func validatePersistedReviewDiscovery(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("persisted review cycle is missing")
	}
	if len(cycle.DiscoveryPasses) == 0 {
		if len(cycle.CanonicalFindings) != 0 ||
			len(cycle.UnresolvedCoverage) != 0 {
			return errors.New(
				"persisted discovery results have no scheduled pass",
			)
		}
		return nil
	}
	if cycle.Plan == nil {
		return errors.New("persisted discovery passes have no review plan")
	}
	required := make(map[string]struct{}, len(cycle.Plan.RequiredLanes))
	for _, lane := range cycle.Plan.RequiredLanes {
		required[lane] = struct{}{}
	}
	seenPasses := make(map[int]struct{}, len(cycle.DiscoveryPasses))
	for passIndex, pass := range cycle.DiscoveryPasses {
		if pass.Pass != passIndex+1 ||
			pass.HeadSHA != cycle.HeadSHA ||
			pass.QueuedAt.IsZero() ||
			len(pass.Lanes) != len(cycle.Plan.SelectedLanes)+1 {
			return errors.New("persisted discovery pass is invalid")
		}
		if _, duplicate := seenPasses[pass.Pass]; duplicate {
			return errors.New("persisted discovery pass is duplicated")
		}
		seenPasses[pass.Pass] = struct{}{}
		allTerminal := true
		latest := time.Time{}
		for laneIndex, lane := range pass.Lanes {
			if laneIndex == len(cycle.Plan.SelectedLanes) {
				if lane.Lane != reviewSynthesisLane || !lane.Required {
					return errors.New(
						"persisted discovery synthesis does not match the plan",
					)
				}
			} else {
				_, requiredLane := required[lane.Lane]
				if lane.Lane != cycle.Plan.SelectedLanes[laneIndex] ||
					lane.Required != requiredLane {
					return errors.New(
						"persisted discovery lane does not match the plan",
					)
				}
			}
			if lane.QueuedAt.IsZero() ||
				lane.QueuedAt.Before(pass.QueuedAt) {
				return errors.New("persisted discovery lane does not match the plan")
			}
			switch lane.Status {
			case ReviewDiscoveryLaneQueued:
				allTerminal = false
				if !lane.StartedAt.IsZero() || !lane.CompletedAt.IsZero() ||
					lane.WorkerID != "" || lane.Attempt != 0 ||
					lane.FailureCode != "" || lane.RecoveredBySynthesis {
					return errors.New("persisted queued discovery lane is invalid")
				}
			case ReviewDiscoveryLaneRunning:
				allTerminal = false
				if lane.StartedAt.IsZero() || !lane.CompletedAt.IsZero() ||
					lane.FailureCode != "" || lane.RecoveredBySynthesis ||
					lane.StartedAt.Before(lane.QueuedAt) ||
					(lane.WorkerID == "") != (lane.Attempt == 0) ||
					(lane.WorkerID != "" &&
						!reviewDiscoveryPersistedOwnershipMatches(
							cycle,
							pass.Pass,
							lane,
						)) {
					return errors.New("persisted running discovery lane is invalid")
				}
			case ReviewDiscoveryLaneCompleted:
				if lane.StartedAt.IsZero() || lane.CompletedAt.IsZero() ||
					lane.WorkerID == "" || lane.Attempt <= 0 ||
					lane.FailureCode != "" || lane.RecoveredBySynthesis ||
					lane.StartedAt.Before(lane.QueuedAt) ||
					lane.CompletedAt.Before(lane.StartedAt) ||
					!reviewDiscoveryPersistedOwnershipMatches(
						cycle,
						pass.Pass,
						lane,
					) {
					return errors.New("persisted completed discovery lane is invalid")
				}
				if !reviewDiscoveryLaneHasTrustedCompletion(cycle, pass.Pass, lane) {
					return errors.New(
						"persisted completed discovery lane has no trusted artifact",
					)
				}
			case ReviewDiscoveryLaneFailed:
				if lane.CompletedAt.IsZero() ||
					lane.CompletedAt.Before(lane.QueuedAt) ||
					!supportedReviewDiscoveryFailureCode(lane.FailureCode) ||
					(lane.WorkerID == "") != (lane.Attempt == 0) ||
					(lane.WorkerID != "" &&
						!reviewDiscoveryPersistedOwnershipMatches(
							cycle,
							pass.Pass,
							lane,
						)) {
					return errors.New("persisted failed discovery lane is invalid")
				}
			default:
				return errors.New("persisted discovery lane status is unsupported")
			}
			if lane.CompletedAt.After(latest) {
				latest = lane.CompletedAt
			}
		}
		if allTerminal {
			synthesisRecoveredFailures := false
			for _, lane := range pass.Lanes {
				if lane.Lane == reviewSynthesisLane &&
					lane.Status == ReviewDiscoveryLaneCompleted {
					synthesisRecoveredFailures = true
				}
			}
			for _, lane := range pass.Lanes {
				if lane.RecoveredBySynthesis && !synthesisRecoveredFailures {
					return errors.New(
						"persisted discovery lane recovery has no completed synthesis",
					)
				}
			}
			if pass.CompletedAt.IsZero() || !pass.CompletedAt.Equal(latest) {
				return errors.New("persisted discovery pass completion is invalid")
			}
		} else if !pass.CompletedAt.IsZero() {
			return errors.New("persisted incomplete discovery pass is terminal")
		}
	}
	expected := cloneReviewCycle(cycle)
	expected.CanonicalFindings = nil
	expected.UnresolvedCoverage = nil
	if err := rebuildReviewDiscoveryDerivedState(expected); err != nil {
		return err
	}
	actualFindings := cycle.CanonicalFindings
	if actualFindings == nil {
		actualFindings = []ReviewCanonicalFinding{}
	}
	actualGaps := cycle.UnresolvedCoverage
	if actualGaps == nil {
		actualGaps = []ReviewCoverageGap{}
	}
	if !reflect.DeepEqual(
		expected.CanonicalFindings,
		actualFindings,
	) || !reflect.DeepEqual(
		expected.UnresolvedCoverage,
		actualGaps,
	) {
		return errors.New(
			"persisted discovery results do not match trusted exact-SHA artifacts",
		)
	}
	return nil
}

func reviewDiscoveryPersistedOwnershipMatches(
	cycle *ReviewCycleState,
	pass int,
	lane ReviewDiscoveryLaneState,
) bool {
	for _, ownership := range cycle.WorkerOwnerships {
		if ownership.OwnerID == lane.WorkerID &&
			ownership.Attempt == lane.Attempt &&
			reviewDiscoveryOwnershipMatches(
				cycle,
				pass,
				lane.Lane,
				ownership,
			) {
			return true
		}
	}
	return false
}

func reviewDiscoveryLaneHasTrustedCompletion(
	cycle *ReviewCycleState,
	pass int,
	lane ReviewDiscoveryLaneState,
) bool {
	for _, completion := range cycle.LaneCompletions {
		if completion.WorkerID == lane.WorkerID &&
			completion.Attempt == lane.Attempt &&
			completion.Pass == pass &&
			completion.Lane == lane.Lane &&
			completion.Phase == ReviewArtifactPhaseDiscovery {
			return true
		}
	}
	return false
}

func supportedReviewDiscoveryFailureCode(
	code ReviewDiscoveryFailureCode,
) bool {
	switch code {
	case ReviewDiscoveryFailureCanceled,
		ReviewDiscoveryFailureLaunch,
		ReviewDiscoveryFailureRuntime,
		ReviewDiscoveryFailureArtifact,
		ReviewDiscoveryFailureState:
		return true
	default:
		return false
	}
}

func latestReviewDiscoveryPassSnapshot(
	cycle *ReviewCycleState,
) (ReviewDiscoveryPassState, bool) {
	latest, ok := latestReviewDiscoveryPass(cycle)
	if !ok {
		return ReviewDiscoveryPassState{}, false
	}
	snapshot := *latest
	snapshot.Lanes = append(
		[]ReviewDiscoveryLaneState(nil),
		latest.Lanes...,
	)
	return snapshot, true
}
