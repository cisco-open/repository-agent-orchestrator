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
	"reflect"
	"sort"
	"strings"
	"time"
)

const reviewLedgerSchemaVersion = 5

type ReviewLedgerFindingStatus string

const (
	ReviewLedgerFindingReported ReviewLedgerFindingStatus = "reported"
	ReviewLedgerFindingFixed    ReviewLedgerFindingStatus = "fixed"
	ReviewLedgerFindingRejected ReviewLedgerFindingStatus = "rejected"
	ReviewLedgerFindingMissed   ReviewLedgerFindingStatus = "missed"
)

type ReviewLedgerFindingProvenance string

const (
	ReviewLedgerFindingPreExisting        ReviewLedgerFindingProvenance = "pre_existing"
	ReviewLedgerFindingOriginalPR         ReviewLedgerFindingProvenance = "original_pr"
	ReviewLedgerFindingFixIntroduced      ReviewLedgerFindingProvenance = "fix_introduced"
	ReviewLedgerFindingPreviouslyReported ReviewLedgerFindingProvenance = "previously_reported"
	ReviewLedgerFindingPreviouslyMissed   ReviewLedgerFindingProvenance = "previously_missed"
)

type ReviewLedgerFindingTransitionReason string

const (
	ReviewLedgerFindingDiscovered         ReviewLedgerFindingTransitionReason = "discovered"
	ReviewLedgerFindingImpacted           ReviewLedgerFindingTransitionReason = "impacted_by_delta"
	ReviewLedgerFindingRechecked          ReviewLedgerFindingTransitionReason = "rechecked"
	ReviewLedgerFindingEvidenceChanged    ReviewLedgerFindingTransitionReason = "new_evidence"
	ReviewLedgerFindingVerifiedFixed      ReviewLedgerFindingTransitionReason = "verified_fixed"
	ReviewLedgerFindingRenamed            ReviewLedgerFindingTransitionReason = "identity_renamed"
	ReviewLedgerFindingRejectedSuppressed ReviewLedgerFindingTransitionReason = "rejected_suppressed"
	ReviewLedgerFindingReportedSuppressed ReviewLedgerFindingTransitionReason = "reported_suppressed"
)

type ReviewLedgerFindingTransition struct {
	SourceSHA                  string                              `json:"source_sha"`
	FromStatus                 ReviewLedgerFindingStatus           `json:"from_status,omitempty"`
	ToStatus                   ReviewLedgerFindingStatus           `json:"to_status"`
	FromProvenance             ReviewLedgerFindingProvenance       `json:"from_provenance,omitempty"`
	ToProvenance               ReviewLedgerFindingProvenance       `json:"to_provenance"`
	FromFindingID              string                              `json:"from_finding_id,omitempty"`
	ToFindingID                string                              `json:"to_finding_id"`
	FromEvidenceDigest         string                              `json:"from_evidence_digest,omitempty"`
	EvidenceDigest             string                              `json:"evidence_digest"`
	FromMaterialEvidenceDigest string                              `json:"from_material_evidence_digest,omitempty"`
	MaterialEvidenceDigest     string                              `json:"material_evidence_digest"`
	PresentInDeltaBase         *bool                               `json:"present_in_delta_base,omitempty"`
	Reason                     ReviewLedgerFindingTransitionReason `json:"reason"`
}

// ReviewLedgerVerificationReceipt is the durable projection of the exact-SHA
// verifier assignment that produced one finding disposition.
type ReviewLedgerVerificationReceipt struct {
	FindingID         string                    `json:"finding_id"`
	ExactSHA          string                    `json:"exact_sha"`
	AssignmentID      string                    `json:"assignment_id"`
	CandidateRevision string                    `json:"candidate_revision"`
	VerifierWorkerID  string                    `json:"verifier_worker_id"`
	Outcome           ReviewVerificationOutcome `json:"outcome"`
}

type ReviewLedgerFinding struct {
	ID                     string                          `json:"id"`
	Fingerprint            string                          `json:"fingerprint"`
	Status                 ReviewLedgerFindingStatus       `json:"status"`
	Provenance             ReviewLedgerFindingProvenance   `json:"provenance"`
	SourceSHA              string                          `json:"source_sha"`
	FirstSeenSHA           string                          `json:"first_seen_sha"`
	LastObservedSHA        string                          `json:"last_observed_sha"`
	ReportedSHA            string                          `json:"reported_sha,omitempty"`
	FixedSHA               string                          `json:"fixed_sha,omitempty"`
	Finding                ReviewCanonicalFinding          `json:"finding"`
	VerificationEvidence   []ReviewEvidence                `json:"verification_evidence"`
	VerificationReceipt    ReviewLedgerVerificationReceipt `json:"verification_receipt"`
	EvidenceDigest         string                          `json:"evidence_digest"`
	MaterialEvidenceDigest string                          `json:"material_evidence_digest"`
	NeedsRecheck           bool                            `json:"needs_recheck,omitempty"`
	History                []ReviewLedgerFindingTransition `json:"history"`
}

type ReviewLedgerCoverageTransitionReason string

const (
	ReviewLedgerCoverageRecorded  ReviewLedgerCoverageTransitionReason = "recorded"
	ReviewLedgerCoverageImpacted  ReviewLedgerCoverageTransitionReason = "impacted_by_delta"
	ReviewLedgerCoverageRechecked ReviewLedgerCoverageTransitionReason = "rechecked"
)

type ReviewLedgerCoverageTransition struct {
	SourceSHA            string                               `json:"source_sha"`
	FromStatus           ReviewCoverageStatus                 `json:"from_status,omitempty"`
	ToStatus             ReviewCoverageStatus                 `json:"to_status"`
	RequirementDigest    string                               `json:"requirement_digest"`
	RequirementSetDigest string                               `json:"requirement_set_digest,omitempty"`
	FromEvidenceDigest   string                               `json:"from_evidence_digest,omitempty"`
	EvidenceDigest       string                               `json:"evidence_digest"`
	Reason               ReviewLedgerCoverageTransitionReason `json:"reason"`
}

type ReviewLedgerCoverage struct {
	RequirementID        string                           `json:"requirement_id"`
	Kind                 ReviewCoverageKind               `json:"kind"`
	Description          string                           `json:"description"`
	Critical             bool                             `json:"critical,omitempty"`
	AccountableLane      string                           `json:"accountable_lane,omitempty"`
	ChangedTargets       []ReviewCoverageTarget           `json:"changed_targets,omitempty"`
	RequirementDigest    string                           `json:"requirement_digest"`
	RequirementSetDigest string                           `json:"requirement_set_digest"`
	Status               ReviewCoverageStatus             `json:"status"`
	Evidence             []ReviewEvidence                 `json:"evidence"`
	EvidenceDigest       string                           `json:"evidence_digest"`
	SourceSHA            string                           `json:"source_sha"`
	LastObservedSHA      string                           `json:"last_observed_sha"`
	NeedsRecheck         bool                             `json:"needs_recheck,omitempty"`
	History              []ReviewLedgerCoverageTransition `json:"history"`
}

// ReviewLedgerHeadBinding audit-binds one exact delta to the complete immutable
// coverage requirement set accepted for that head. The requirement set includes
// current-plan requirements plus any exact persisted recheck bindings.
type ReviewLedgerHeadBinding struct {
	Delta                      ReviewPlanInputs            `json:"delta"`
	CoverageRequirements       []ReviewCoverageRequirement `json:"coverage_requirements"`
	CoverageRequirementsDigest string                      `json:"coverage_requirements_digest"`
}

// ReviewLedgerTransitionSnapshot is manager-owned input for one persistence
// boundary. The successor ledger cannot authoritatively supply its own delta or
// exact-SHA plan requirement set.
type ReviewLedgerTransitionSnapshot struct {
	Inputs ReviewPlanInputs
	Plan   ReviewPlan
}

// ReviewLedger is the coder-owned, cross-SHA review knowledge checkpoint.
// Findings and coverage are kept in deterministic identity order. Per-item
// histories retain every status, provenance, evidence, and impact transition.
type ReviewLedger struct {
	SchemaVersion   int                       `json:"schema_version"`
	BaseSHA         string                    `json:"base_sha"`
	PreviousHeadSHA string                    `json:"previous_head_sha"`
	HeadSHA         string                    `json:"head_sha"`
	HeadBindings    []ReviewLedgerHeadBinding `json:"head_bindings"`
	Findings        []ReviewLedgerFinding     `json:"findings"`
	Coverage        []ReviewLedgerCoverage    `json:"coverage"`
}

// ReviewLedgerFindingObservation is one exact-SHA disposition produced after
// independent review and bound to the verifier assignment receipt that
// produced it. PresentInDeltaBase is required for a newly observed identity:
// on the first PR head it distinguishes pre-existing from original PR
// findings, and on later heads it distinguishes previously missed from
// fix-introduced findings.
type ReviewLedgerFindingObservation struct {
	Finding              ReviewCanonicalFinding          `json:"finding"`
	Status               ReviewLedgerFindingStatus       `json:"status"`
	VerificationEvidence []ReviewEvidence                `json:"verification_evidence"`
	VerificationReceipt  ReviewLedgerVerificationReceipt `json:"verification_receipt"`
	PresentInDeltaBase   *bool                           `json:"present_in_delta_base,omitempty"`
}

type ReviewLedgerCoverageObservation struct {
	Requirement ReviewCoverageRequirement `json:"requirement"`
	Claim       ReviewCoverageClaim       `json:"claim"`
}

type ReviewLedgerDeltaResult struct {
	Ledger               ReviewLedger `json:"ledger"`
	RecheckFindingIDs    []string     `json:"recheck_finding_ids"`
	ImpactedCoverageIDs  []string     `json:"impacted_coverage_ids"`
	NovelFindingIDs      []string     `json:"novel_finding_ids"`
	SuppressedFindingIDs []string     `json:"suppressed_finding_ids"`
	FixedFindingIDs      []string     `json:"fixed_finding_ids"`
}

func cloneReviewLedger(ledger *ReviewLedger) *ReviewLedger {
	if ledger == nil {
		return nil
	}
	body, err := json.Marshal(ledger)
	if err != nil {
		clone := *ledger
		return &clone
	}
	var clone ReviewLedger
	if err := json.Unmarshal(body, &clone); err != nil {
		fallback := *ledger
		return &fallback
	}
	return &clone
}

func supportedReviewLedgerFindingStatus(status ReviewLedgerFindingStatus) bool {
	switch status {
	case ReviewLedgerFindingReported,
		ReviewLedgerFindingFixed,
		ReviewLedgerFindingRejected,
		ReviewLedgerFindingMissed:
		return true
	default:
		return false
	}
}

func supportedReviewLedgerFindingProvenance(
	provenance ReviewLedgerFindingProvenance,
) bool {
	switch provenance {
	case ReviewLedgerFindingPreExisting,
		ReviewLedgerFindingOriginalPR,
		ReviewLedgerFindingFixIntroduced,
		ReviewLedgerFindingPreviouslyReported,
		ReviewLedgerFindingPreviouslyMissed:
		return true
	default:
		return false
	}
}

func supportedReviewLedgerFindingTransitionReason(
	reason ReviewLedgerFindingTransitionReason,
) bool {
	switch reason {
	case ReviewLedgerFindingDiscovered,
		ReviewLedgerFindingImpacted,
		ReviewLedgerFindingRechecked,
		ReviewLedgerFindingEvidenceChanged,
		ReviewLedgerFindingVerifiedFixed,
		ReviewLedgerFindingRenamed,
		ReviewLedgerFindingRejectedSuppressed,
		ReviewLedgerFindingReportedSuppressed:
		return true
	default:
		return false
	}
}

func supportedReviewLedgerCoverageTransitionReason(
	reason ReviewLedgerCoverageTransitionReason,
) bool {
	switch reason {
	case ReviewLedgerCoverageRecorded,
		ReviewLedgerCoverageImpacted,
		ReviewLedgerCoverageRechecked:
		return true
	default:
		return false
	}
}

func reviewLedgerCoverageRequirementsDigestFor(
	requirements []ReviewCoverageRequirement,
) (string, error) {
	if requirements == nil {
		return "", errors.New(
			"review ledger coverage requirements are not initialized",
		)
	}
	body, err := json.Marshal(requirements)
	if err != nil {
		return "", fmt.Errorf(
			"failed to canonicalize review ledger coverage requirements: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func validateReviewLedgerHeadBindings(
	ledger *ReviewLedger,
) error {
	if ledger.HeadBindings == nil || len(ledger.HeadBindings) == 0 {
		return errors.New("review ledger head bindings are missing")
	}
	expectedBaseSHA := ledger.BaseSHA
	seenHeads := make(map[string]struct{}, len(ledger.HeadBindings))
	for index, binding := range ledger.HeadBindings {
		if err := validateReviewPlanInputs(binding.Delta); err != nil {
			return fmt.Errorf(
				"review ledger head binding %d delta is invalid: %w",
				index,
				err,
			)
		}
		if binding.Delta.BaseSHA != expectedBaseSHA {
			return fmt.Errorf(
				"review ledger head binding %d does not continue the exact delta chain",
				index,
			)
		}
		if _, duplicate := seenHeads[binding.Delta.HeadSHA]; duplicate {
			return fmt.Errorf(
				"review ledger head binding %d repeats an exact head SHA",
				index,
			)
		}
		seenHeads[binding.Delta.HeadSHA] = struct{}{}
		if binding.CoverageRequirements == nil {
			return fmt.Errorf(
				"review ledger head binding %d coverage requirements are not initialized",
				index,
			)
		}
		if err := validateReviewCoverageRequirements(
			binding.CoverageRequirements,
		); err != nil {
			return fmt.Errorf(
				"review ledger head binding %d coverage requirements are invalid: %w",
				index,
				err,
			)
		}
		expectedDigest, err := reviewLedgerCoverageRequirementsDigestFor(
			binding.CoverageRequirements,
		)
		if err != nil {
			return err
		}
		if err := validateLowerHexDigest(
			binding.CoverageRequirementsDigest,
			sha256.Size,
		); err != nil {
			return fmt.Errorf(
				"review ledger head binding %d coverage requirements digest is invalid: %w",
				index,
				err,
			)
		}
		if binding.CoverageRequirementsDigest != expectedDigest {
			return fmt.Errorf(
				"review ledger head binding %d coverage requirements digest does not match the immutable requirement set",
				index,
			)
		}
		expectedBaseSHA = binding.Delta.HeadSHA
	}
	latest := ledger.HeadBindings[len(ledger.HeadBindings)-1]
	if latest.Delta.BaseSHA != ledger.PreviousHeadSHA ||
		latest.Delta.HeadSHA != ledger.HeadSHA {
		return errors.New(
			"review ledger latest head binding does not match the ledger head transition",
		)
	}
	return nil
}

func reviewLedgerHeadBindingIndex(
	bindings []ReviewLedgerHeadBinding,
	headSHA string,
) int {
	for index := range bindings {
		if bindings[index].Delta.HeadSHA == headSHA {
			return index
		}
	}
	return -1
}

// reviewLedgerRequirementInHeadBinding reports whether requirement is one
// of the requirements bound to this exact-SHA head binding, comparing only
// true identity fields (see reviewCoverageRequirementIdentityEqual) rather
// than the whole struct. A coverage entry's own Critical/AccountableLane/
// ChangedTargets are only ever refreshed to the latest round's values (see
// applyReviewLedgerCoverageObservation), while this function is also used
// to validate *historical* transitions recorded against *earlier* head
// bindings, whose own stored requirement snapshot legitimately had
// different diff-derived field values for the exact same logical
// requirement. Comparing the whole struct made every legitimate recheck
// of a stable-ID coverage kind (acceptance_criterion, state_transition,
// persistence_boundary) across rounds fail this check -- the same root
// cause as the carried-coverage conflict false positive, just
// surfacing in ledger-consistency validation instead of the carry-forward
// conflict check.
func reviewLedgerRequirementInHeadBinding(
	binding ReviewLedgerHeadBinding,
	requirement ReviewCoverageRequirement,
) bool {
	for _, planned := range binding.CoverageRequirements {
		if reviewCoverageRequirementIdentityEqual(planned, requirement) {
			return true
		}
	}
	return false
}

func reviewLedgerCoverageRequirement(
	coverage ReviewLedgerCoverage,
) ReviewCoverageRequirement {
	return ReviewCoverageRequirement{
		ID: coverage.RequirementID, Kind: coverage.Kind,
		Description: coverage.Description, Critical: coverage.Critical,
		AccountableLane: coverage.AccountableLane,
		ChangedTargets:  append([]ReviewCoverageTarget(nil), coverage.ChangedTargets...),
	}
}

func validateReviewLedger(ledger *ReviewLedger) error {
	if ledger == nil {
		return errors.New("review ledger is missing")
	}
	if ledger.SchemaVersion != reviewLedgerSchemaVersion {
		return fmt.Errorf(
			"review ledger schema version %d is unsupported; expected %d",
			ledger.SchemaVersion,
			reviewLedgerSchemaVersion,
		)
	}
	if err := validateCanonicalGitObjectID(ledger.BaseSHA); err != nil {
		return fmt.Errorf("review ledger base SHA is invalid: %w", err)
	}
	if err := validateCanonicalGitObjectID(
		ledger.PreviousHeadSHA,
	); err != nil {
		return fmt.Errorf(
			"review ledger previous head SHA is invalid: %w",
			err,
		)
	}
	if err := validateCanonicalGitObjectID(ledger.HeadSHA); err != nil {
		return fmt.Errorf("review ledger head SHA is invalid: %w", err)
	}
	if ledger.PreviousHeadSHA == ledger.HeadSHA {
		return errors.New(
			"review ledger previous and current head SHAs are identical",
		)
	}
	if ledger.BaseSHA == ledger.HeadSHA {
		return errors.New(
			"review ledger base and current head SHAs are identical",
		)
	}
	if ledger.Findings == nil {
		return errors.New("review ledger findings are not initialized")
	}
	if ledger.Coverage == nil {
		return errors.New("review ledger coverage is not initialized")
	}
	if err := validateReviewLedgerHeadBindings(ledger); err != nil {
		return err
	}

	previousFindingID := ""
	for index := range ledger.Findings {
		finding := &ledger.Findings[index]
		if index > 0 && finding.ID <= previousFindingID {
			return errors.New(
				"review ledger findings are not in deterministic identity order",
			)
		}
		previousFindingID = finding.ID
		if err := validateReviewLedgerFinding(
			*finding,
			ledger.HeadBindings,
		); err != nil {
			return fmt.Errorf(
				"review ledger finding %q is invalid: %w",
				finding.ID,
				err,
			)
		}
	}

	previousCoverageID := ""
	for index := range ledger.Coverage {
		coverage := &ledger.Coverage[index]
		if index > 0 && coverage.RequirementID <= previousCoverageID {
			return errors.New(
				"review ledger coverage is not in deterministic requirement order",
			)
		}
		previousCoverageID = coverage.RequirementID
		if err := validateReviewLedgerCoverage(
			*coverage,
			ledger.HeadBindings,
		); err != nil {
			return fmt.Errorf(
				"review ledger coverage %q is invalid: %w",
				coverage.RequirementID,
				err,
			)
		}
	}
	return nil
}

func validateReviewLedgerFinding(
	finding ReviewLedgerFinding,
	headBindings []ReviewLedgerHeadBinding,
) error {
	if finding.ID == "" ||
		finding.Fingerprint == "" ||
		finding.ID != "finding-"+finding.Fingerprint {
		return errors.New("identity does not match the canonical fingerprint")
	}
	if err := validateLowerHexDigest(
		finding.Fingerprint,
		sha256.Size,
	); err != nil {
		return fmt.Errorf("fingerprint is invalid: %w", err)
	}
	if !supportedReviewLedgerFindingStatus(finding.Status) {
		return errors.New("status is unsupported")
	}
	if !supportedReviewLedgerFindingProvenance(finding.Provenance) {
		return errors.New("provenance is unsupported")
	}
	for label, sha := range map[string]string{
		"source":        finding.SourceSHA,
		"first-seen":    finding.FirstSeenSHA,
		"last-observed": finding.LastObservedSHA,
	} {
		if err := validateCanonicalGitObjectID(sha); err != nil {
			return fmt.Errorf("%s SHA is invalid: %w", label, err)
		}
	}
	if finding.ReportedSHA != "" {
		if err := validateCanonicalGitObjectID(finding.ReportedSHA); err != nil {
			return fmt.Errorf("reported SHA is invalid: %w", err)
		}
	}
	if finding.FixedSHA != "" {
		if err := validateCanonicalGitObjectID(finding.FixedSHA); err != nil {
			return fmt.Errorf("fixed SHA is invalid: %w", err)
		}
	}
	if finding.Status == ReviewLedgerFindingReported &&
		finding.ReportedSHA == "" {
		return errors.New("reported status has no reported SHA")
	}
	if finding.Status == ReviewLedgerFindingFixed &&
		finding.FixedSHA == "" {
		return errors.New("fixed status has no fixed SHA")
	}
	if finding.Finding.ID != finding.ID ||
		finding.Finding.Fingerprint != finding.Fingerprint {
		return errors.New("canonical finding identity does not match")
	}
	if err := validateReviewLedgerCanonicalFinding(finding.Finding); err != nil {
		return err
	}
	if finding.Finding.ExactSHA != finding.SourceSHA {
		return errors.New(
			"canonical finding exact SHA does not match its source SHA",
		)
	}
	if finding.VerificationEvidence == nil ||
		len(finding.VerificationEvidence) == 0 {
		return errors.New("verification evidence is missing")
	}
	for _, evidence := range finding.VerificationEvidence {
		if err := validateReviewEvidence(evidence); err != nil {
			return err
		}
	}
	if err := validateReviewLedgerVerificationReceiptFields(
		finding.VerificationReceipt,
	); err != nil {
		return err
	}
	if finding.VerificationReceipt.ExactSHA != finding.LastObservedSHA {
		return errors.New(
			"verification receipt does not match the last observed SHA",
		)
	}
	if finding.Status == ReviewLedgerFindingFixed &&
		finding.VerificationReceipt.Outcome !=
			ReviewVerificationRejected {
		return errors.New(
			"fixed finding has no rejecting verifier receipt",
		)
	}
	expectedDigest, err := reviewLedgerFindingEvidenceDigestFor(
		finding.Finding,
		finding.VerificationEvidence,
		finding.VerificationReceipt,
	)
	if err != nil {
		return err
	}
	if err := validateLowerHexDigest(
		finding.EvidenceDigest,
		sha256.Size,
	); err != nil {
		return fmt.Errorf("evidence digest is invalid: %w", err)
	}
	if finding.EvidenceDigest != expectedDigest {
		return errors.New(
			"evidence digest does not match the stored finding evidence",
		)
	}
	expectedMaterialDigest, err :=
		reviewLedgerFindingMaterialEvidenceDigestFor(
			finding.Finding,
			finding.VerificationEvidence,
		)
	if err != nil {
		return err
	}
	if err := validateLowerHexDigest(
		finding.MaterialEvidenceDigest,
		sha256.Size,
	); err != nil {
		return fmt.Errorf(
			"material evidence digest is invalid: %w",
			err,
		)
	}
	if finding.MaterialEvidenceDigest != expectedMaterialDigest {
		return errors.New(
			"material evidence digest does not match the stored finding evidence",
		)
	}
	if finding.History == nil || len(finding.History) == 0 {
		return errors.New("transition history is missing")
	}
	var previous *ReviewLedgerFindingTransition
	expectedSourceSHA := ""
	expectedLastObservedSHA := ""
	expectedReportedSHA := ""
	expectedFixedSHA := ""
	expectedNeedsRecheck := false
	var verificationReceiptTransition *ReviewLedgerFindingTransition
	previousHeadBindingIndex := -1
	for index, transition := range finding.History {
		if err := validateReviewLedgerFindingTransition(
			transition,
		); err != nil {
			return err
		}
		headBindingIndex := reviewLedgerHeadBindingIndex(
			headBindings,
			transition.SourceSHA,
		)
		if headBindingIndex < 0 {
			return errors.New(
				"finding transition is not bound to a persisted exact delta",
			)
		}
		if headBindingIndex < previousHeadBindingIndex {
			return errors.New(
				"finding transition regresses persisted head order",
			)
		}
		previousHeadBindingIndex = headBindingIndex
		if transition.SourceSHA ==
			finding.VerificationReceipt.ExactSHA &&
			transition.ToFindingID ==
				finding.VerificationReceipt.FindingID &&
			transition.Reason != ReviewLedgerFindingImpacted &&
			transition.Reason != ReviewLedgerFindingRenamed {
			verificationReceiptTransition =
				&finding.History[index]
		}
		if index == 0 &&
			(transition.FromStatus != "" ||
				transition.FromProvenance != "" ||
				transition.Reason != ReviewLedgerFindingDiscovered) {
			return errors.New(
				"initial finding transition is not a discovery",
			)
		}
		if index == 0 {
			if transition.FromFindingID != "" ||
				transition.ToFindingID == "" {
				return errors.New(
					"initial finding transition identity is invalid",
				)
			}
			if transition.FromEvidenceDigest != "" {
				return errors.New(
					"initial finding transition has prior evidence",
				)
			}
			if transition.FromMaterialEvidenceDigest != "" {
				return errors.New(
					"initial finding transition has prior material evidence",
				)
			}
			if transition.SourceSHA != finding.FirstSeenSHA {
				return errors.New(
					"initial finding transition does not match first-seen SHA",
				)
			}
			if transition.ToStatus == ReviewLedgerFindingFixed {
				return errors.New(
					"initial finding transition cannot begin fixed",
				)
			}
			expectedProvenance, err :=
				reviewLedgerNewFindingProvenance(
					headBindingIndex > 0,
					transition.PresentInDeltaBase,
				)
			if err != nil {
				return fmt.Errorf(
					"initial finding transition provenance evidence is invalid: %w",
					err,
				)
			}
			if transition.ToProvenance != expectedProvenance {
				return errors.New(
					"initial finding transition provenance does not match delta-base presence evidence",
				)
			}
			expectedSourceSHA = transition.SourceSHA
			expectedLastObservedSHA = transition.SourceSHA
		} else {
			if transition.Reason == ReviewLedgerFindingDiscovered {
				return errors.New(
					"finding discovery appears after the initial transition",
				)
			}
			if transition.FromStatus != previous.ToStatus ||
				transition.FromProvenance != previous.ToProvenance {
				return errors.New(
					"finding transition status or provenance history is discontinuous",
				)
			}
			if transition.FromFindingID != previous.ToFindingID ||
				transition.FromEvidenceDigest != previous.EvidenceDigest ||
				transition.FromMaterialEvidenceDigest !=
					previous.MaterialEvidenceDigest {
				return errors.New(
					"finding transition identity or evidence history is discontinuous",
				)
			}
			reopensClassifiedIdentity :=
				(previous.ToStatus == ReviewLedgerFindingFixed &&
					transition.ToStatus !=
						ReviewLedgerFindingFixed) ||
					(previous.ToStatus ==
						ReviewLedgerFindingRejected &&
						transition.ToStatus !=
							ReviewLedgerFindingRejected)
			if previous.ToStatus == ReviewLedgerFindingRejected &&
				transition.ToStatus !=
					ReviewLedgerFindingRejected &&
				(transition.Reason !=
					ReviewLedgerFindingEvidenceChanged ||
					transition.FromMaterialEvidenceDigest ==
						transition.MaterialEvidenceDigest) {
				return errors.New(
					"rejected finding cannot reopen without materially new evidence",
				)
			}
			if reopensClassifiedIdentity {
				expectedProvenance, err :=
					reviewLedgerNewFindingProvenance(
						true,
						transition.PresentInDeltaBase,
					)
				if err != nil {
					return fmt.Errorf(
						"reopened finding transition provenance evidence is invalid: %w",
						err,
					)
				}
				if transition.ToProvenance !=
					expectedProvenance {
					return errors.New(
						"reopened finding transition provenance does not match delta-base presence evidence",
					)
				}
			} else if transition.PresentInDeltaBase != nil {
				return errors.New(
					"finding transition has unexpected delta-base presence evidence",
				)
			}
			identityChanged := transition.FromFindingID !=
				transition.ToFindingID
			switch transition.Reason {
			case ReviewLedgerFindingImpacted,
				ReviewLedgerFindingRechecked,
				ReviewLedgerFindingRenamed,
				ReviewLedgerFindingRejectedSuppressed,
				ReviewLedgerFindingReportedSuppressed:
				if !identityChanged &&
					transition.FromMaterialEvidenceDigest !=
						transition.MaterialEvidenceDigest {
					return errors.New(
						"finding transition changes material evidence without an evidence-change reason",
					)
				}
			case ReviewLedgerFindingEvidenceChanged:
				if transition.FromMaterialEvidenceDigest ==
					transition.MaterialEvidenceDigest {
					return errors.New(
						"finding evidence-change transition preserves the prior material evidence",
					)
				}
			case ReviewLedgerFindingVerifiedFixed:
				if transition.FromEvidenceDigest ==
					transition.EvidenceDigest {
					return errors.New(
						"finding verified-fixed transition preserves the prior verification receipt",
					)
				}
			}
			switch transition.Reason {
			case ReviewLedgerFindingImpacted:
				if transition.FromStatus != transition.ToStatus ||
					transition.FromProvenance !=
						transition.ToProvenance ||
					(transition.FromStatus !=
						ReviewLedgerFindingReported &&
						transition.FromStatus !=
							ReviewLedgerFindingMissed) {
					return errors.New(
						"finding impact transition has illegal state preconditions",
					)
				}
				expectedNeedsRecheck = true
			case ReviewLedgerFindingRenamed:
				if !identityChanged ||
					(transition.FromStatus !=
						ReviewLedgerFindingRejected &&
						transition.FromStatus !=
							ReviewLedgerFindingFixed) ||
					transition.ToStatus !=
						transition.FromStatus ||
					expectedNeedsRecheck ||
					transition.FromProvenance !=
						transition.ToProvenance {
					return errors.New(
						"finding rename transition has illegal state preconditions",
					)
				}
			case ReviewLedgerFindingVerifiedFixed:
				if !expectedNeedsRecheck {
					return errors.New(
						"finding cannot be fixed without a preceding impacted state",
					)
				}
				if transition.ToStatus != ReviewLedgerFindingFixed ||
					(transition.FromStatus !=
						ReviewLedgerFindingReported &&
						transition.FromStatus !=
							ReviewLedgerFindingMissed) {
					return errors.New(
						"finding fixed transition has illegal state preconditions",
					)
				}
				expectedNeedsRecheck = false
			case ReviewLedgerFindingRechecked,
				ReviewLedgerFindingEvidenceChanged:
				expectedNeedsRecheck = false
			case ReviewLedgerFindingRejectedSuppressed:
				if expectedNeedsRecheck ||
					identityChanged ||
					transition.FromStatus !=
						ReviewLedgerFindingRejected ||
					transition.ToStatus !=
						ReviewLedgerFindingRejected ||
					transition.FromProvenance !=
						transition.ToProvenance {
					return errors.New(
						"finding rejected-suppression transition has illegal state preconditions",
					)
				}
				expectedNeedsRecheck = false
			case ReviewLedgerFindingReportedSuppressed:
				if identityChanged ||
					transition.FromStatus !=
						ReviewLedgerFindingReported ||
					transition.ToStatus !=
						ReviewLedgerFindingReported ||
					transition.ToProvenance !=
						ReviewLedgerFindingPreviouslyReported {
					return errors.New(
						"finding reported-suppression transition has illegal state preconditions",
					)
				}
				expectedNeedsRecheck = false
			}
			if transition.ToStatus == ReviewLedgerFindingFixed &&
				transition.Reason != ReviewLedgerFindingVerifiedFixed &&
				!(transition.Reason ==
					ReviewLedgerFindingRenamed &&
					transition.FromStatus ==
						ReviewLedgerFindingFixed) {
				return errors.New(
					"finding reaches fixed without verified-fixed evidence",
				)
			}
			if transition.Reason != ReviewLedgerFindingImpacted &&
				transition.Reason != ReviewLedgerFindingRenamed {
				expectedLastObservedSHA = transition.SourceSHA
			}
			if identityChanged ||
				transition.FromEvidenceDigest !=
					transition.EvidenceDigest ||
				transition.Reason == ReviewLedgerFindingRechecked ||
				transition.Reason == ReviewLedgerFindingEvidenceChanged ||
				transition.Reason == ReviewLedgerFindingVerifiedFixed {
				expectedSourceSHA = transition.SourceSHA
			}
		}
		if index == 0 {
			expectedNeedsRecheck = false
		}
		if expectedReportedSHA == "" &&
			transition.ToStatus == ReviewLedgerFindingReported {
			expectedReportedSHA = transition.SourceSHA
		}
		if transition.Reason == ReviewLedgerFindingVerifiedFixed {
			expectedFixedSHA = transition.SourceSHA
		}
		previous = &finding.History[index]
	}
	latest := finding.History[len(finding.History)-1]
	if latest.ToFindingID != finding.ID {
		return errors.New(
			"latest finding transition identity does not match current state",
		)
	}
	if latest.ToStatus != finding.Status ||
		latest.ToProvenance != finding.Provenance ||
		latest.EvidenceDigest != finding.EvidenceDigest ||
		latest.MaterialEvidenceDigest !=
			finding.MaterialEvidenceDigest {
		return errors.New(
			"latest finding transition does not match current state",
		)
	}
	if finding.NeedsRecheck != expectedNeedsRecheck {
		return errors.New(
			"finding recheck history does not match current state",
		)
	}
	if verificationReceiptTransition == nil {
		return errors.New(
			"verification receipt is not bound to an observed finding transition",
		)
	}
	if verificationReceiptTransition.Reason !=
		ReviewLedgerFindingReportedSuppressed {
		expectedReceiptOutcome, err :=
			reviewLedgerVerificationOutcomeForStatus(
				verificationReceiptTransition.ToStatus,
			)
		if err != nil {
			return err
		}
		if verificationReceiptTransition.Reason ==
			ReviewLedgerFindingRejectedSuppressed {
			expectedReceiptOutcome =
				ReviewVerificationConfirmed
		}
		if finding.VerificationReceipt.Outcome !=
			expectedReceiptOutcome {
			return errors.New(
				"verification receipt outcome does not match its observed finding transition",
			)
		}
	}
	verifiedFinding, err :=
		reviewLedgerFindingAtVerificationReceipt(
			finding,
			headBindings,
		)
	if err != nil {
		return err
	}
	candidateRevision, err := reviewFindingCandidateRevision(
		verifiedFinding,
	)
	if err != nil {
		return err
	}
	if finding.VerificationReceipt.CandidateRevision !=
		candidateRevision {
		return errors.New(
			"verification receipt does not match the stored finding revision",
		)
	}
	for _, provenance := range verifiedFinding.Provenance {
		if provenance.WorkerID ==
			finding.VerificationReceipt.VerifierWorkerID {
			return errors.New(
				"verification receipt is not independent from finding discovery",
			)
		}
	}
	if finding.SourceSHA != expectedSourceSHA ||
		finding.LastObservedSHA != expectedLastObservedSHA ||
		finding.ReportedSHA != expectedReportedSHA ||
		finding.FixedSHA != expectedFixedSHA {
		return errors.New(
			"finding transition SHA history does not match current state",
		)
	}
	return nil
}

func reviewLedgerFindingAtVerificationReceipt(
	finding ReviewLedgerFinding,
	headBindings []ReviewLedgerHeadBinding,
) (ReviewCanonicalFinding, error) {
	receipt := finding.VerificationReceipt
	canonical := cloneReviewCanonicalFinding(finding.Finding)
	currentID := finding.ID
	currentPath := canonical.Location.Path
	for index := len(finding.History) - 1; index >= 0 && currentID != receipt.FindingID; index-- {
		transition := finding.History[index]
		if transition.ToFindingID != currentID ||
			transition.FromFindingID == transition.ToFindingID {
			continue
		}
		if transition.Reason != ReviewLedgerFindingImpacted &&
			transition.Reason != ReviewLedgerFindingRenamed {
			return ReviewCanonicalFinding{}, errors.New(
				"verification receipt finding revision crosses a non-rename identity transition",
			)
		}
		bindingIndex := reviewLedgerHeadBindingIndex(
			headBindings,
			transition.SourceSHA,
		)
		if bindingIndex < 0 {
			return ReviewCanonicalFinding{}, errors.New(
				"verification receipt finding revision has no exact-diff binding",
			)
		}
		previousPath := ""
		for _, file := range headBindings[bindingIndex].Delta.ChangedFiles {
			if file.Path != currentPath ||
				file.PreviousPath == "" ||
				file.PreviousPath == file.Path {
				continue
			}
			if previousPath != "" &&
				previousPath != file.PreviousPath {
				return ReviewCanonicalFinding{}, errors.New(
					"verification receipt finding revision has ambiguous rename provenance",
				)
			}
			previousPath = file.PreviousPath
		}
		if previousPath == "" {
			return ReviewCanonicalFinding{}, errors.New(
				"verification receipt finding revision is not linked by an exact-diff rename",
			)
		}
		if canonical.Location.Path == currentPath {
			canonical.Location.Path = previousPath
		}
		for evidenceIndex := range canonical.Evidence {
			if canonical.Evidence[evidenceIndex].Path ==
				currentPath {
				canonical.Evidence[evidenceIndex].Path =
					previousPath
			}
		}
		currentPath = previousPath
		currentID = transition.FromFindingID
	}
	if currentID != receipt.FindingID {
		return ReviewCanonicalFinding{}, errors.New(
			"verification receipt finding identity is not in the durable rename chain",
		)
	}
	canonical.ID = receipt.FindingID
	canonical.Fingerprint = strings.TrimPrefix(
		receipt.FindingID,
		"finding-",
	)
	canonical.ExactSHA = receipt.ExactSHA
	fingerprint, err := reviewFindingFingerprint(
		ReviewFindingCandidate{
			CandidateID:       "review-ledger-receipt",
			Summary:           canonical.Summary,
			Location:          canonical.Location,
			BehavioralPath:    canonical.BehavioralPath,
			ViolatedInvariant: canonical.ViolatedInvariant,
			Severity:          canonical.Severity,
			Confidence:        canonical.Confidence,
			Evidence:          canonical.Evidence,
		},
	)
	if err != nil {
		return ReviewCanonicalFinding{}, err
	}
	if receipt.FindingID != "finding-"+fingerprint {
		return ReviewCanonicalFinding{}, errors.New(
			"verification receipt finding identity is not canonical",
		)
	}
	return canonical, nil
}

func validateReviewLedgerCanonicalFinding(
	finding ReviewCanonicalFinding,
) error {
	if finding.ID == "" ||
		finding.Fingerprint == "" ||
		finding.ID != "finding-"+finding.Fingerprint {
		return errors.New("canonical finding identity is invalid")
	}
	if err := validateCanonicalGitObjectID(finding.ExactSHA); err != nil {
		return fmt.Errorf("canonical finding exact SHA is invalid: %w", err)
	}
	if strings.TrimSpace(finding.Summary) == "" ||
		strings.TrimSpace(finding.BehavioralPath) == "" ||
		strings.TrimSpace(finding.ViolatedInvariant) == "" ||
		!supportedReviewFindingSeverity(finding.Severity) ||
		!supportedReviewFindingConfidence(finding.Confidence) ||
		finding.Evidence == nil ||
		len(finding.Evidence) == 0 {
		return errors.New("canonical finding is incomplete")
	}
	if err := validateReviewFindingLocation(finding.Location); err != nil {
		return err
	}
	for _, evidence := range finding.Evidence {
		if err := validateReviewEvidence(evidence); err != nil {
			return err
		}
	}
	if finding.Provenance == nil || len(finding.Provenance) == 0 {
		return errors.New("canonical finding worker provenance is missing")
	}
	previousProvenanceKey := ""
	maxSeverity := ReviewFindingSeverityLow
	maxConfidence := ReviewFindingConfidenceLow
	for index, provenance := range finding.Provenance {
		if strings.TrimSpace(provenance.WorkerID) == "" ||
			strings.TrimSpace(provenance.Lane) == "" ||
			strings.TrimSpace(provenance.CandidateID) == "" ||
			provenance.Pass <= 0 ||
			!supportedReviewFindingSeverity(provenance.Severity) ||
			!supportedReviewFindingConfidence(provenance.Confidence) {
			return errors.New(
				"canonical finding worker provenance is invalid",
			)
		}
		key := reviewFindingProvenanceKey(provenance)
		if index > 0 && key <= previousProvenanceKey {
			return errors.New(
				"canonical finding worker provenance is not in deterministic order",
			)
		}
		previousProvenanceKey = key
		if reviewFindingSeverityRank(provenance.Severity) >
			reviewFindingSeverityRank(maxSeverity) {
			maxSeverity = provenance.Severity
		}
		if reviewFindingConfidenceRank(provenance.Confidence) >
			reviewFindingConfidenceRank(maxConfidence) {
			maxConfidence = provenance.Confidence
		}
	}
	if finding.Severity != maxSeverity ||
		finding.Confidence != maxConfidence {
		return errors.New(
			"canonical finding severity or confidence does not match worker provenance",
		)
	}
	expectedFingerprint, err := reviewFindingFingerprint(
		ReviewFindingCandidate{
			CandidateID:       "ledger-validation",
			Summary:           finding.Summary,
			Location:          finding.Location,
			BehavioralPath:    finding.BehavioralPath,
			ViolatedInvariant: finding.ViolatedInvariant,
			Severity:          finding.Severity,
			Confidence:        finding.Confidence,
			Evidence:          finding.Evidence,
		},
	)
	if err != nil {
		return err
	}
	if finding.Fingerprint != expectedFingerprint {
		return errors.New(
			"canonical finding fingerprint does not match its canonical fields",
		)
	}
	return nil
}

func validateReviewLedgerFindingTransition(
	transition ReviewLedgerFindingTransition,
) error {
	if err := validateCanonicalGitObjectID(transition.SourceSHA); err != nil {
		return fmt.Errorf("finding transition source SHA is invalid: %w", err)
	}
	if transition.FromStatus != "" &&
		!supportedReviewLedgerFindingStatus(transition.FromStatus) {
		return errors.New("finding transition prior status is unsupported")
	}
	if !supportedReviewLedgerFindingStatus(transition.ToStatus) {
		return errors.New("finding transition status is unsupported")
	}
	if transition.FromProvenance != "" &&
		!supportedReviewLedgerFindingProvenance(
			transition.FromProvenance,
		) {
		return errors.New(
			"finding transition prior provenance is unsupported",
		)
	}
	if !supportedReviewLedgerFindingProvenance(
		transition.ToProvenance,
	) {
		return errors.New("finding transition provenance is unsupported")
	}
	if transition.FromFindingID != "" {
		if err := validateReviewLedgerFindingID(
			transition.FromFindingID,
		); err != nil {
			return fmt.Errorf(
				"finding transition prior identity is invalid: %w",
				err,
			)
		}
		if transition.Reason == ReviewLedgerFindingDiscovered {
			return errors.New(
				"finding discovery cannot have a prior identity",
			)
		}
	}
	if err := validateReviewLedgerFindingID(
		transition.ToFindingID,
	); err != nil {
		return fmt.Errorf(
			"finding transition identity is invalid: %w",
			err,
		)
	}
	if transition.FromEvidenceDigest != "" {
		if err := validateLowerHexDigest(
			transition.FromEvidenceDigest,
			sha256.Size,
		); err != nil {
			return fmt.Errorf(
				"finding transition prior evidence digest is invalid: %w",
				err,
			)
		}
	}
	if err := validateLowerHexDigest(
		transition.EvidenceDigest,
		sha256.Size,
	); err != nil {
		return fmt.Errorf("finding transition evidence digest is invalid: %w", err)
	}
	if transition.FromMaterialEvidenceDigest != "" {
		if err := validateLowerHexDigest(
			transition.FromMaterialEvidenceDigest,
			sha256.Size,
		); err != nil {
			return fmt.Errorf(
				"finding transition prior material evidence digest is invalid: %w",
				err,
			)
		}
	}
	if err := validateLowerHexDigest(
		transition.MaterialEvidenceDigest,
		sha256.Size,
	); err != nil {
		return fmt.Errorf(
			"finding transition material evidence digest is invalid: %w",
			err,
		)
	}
	if !supportedReviewLedgerFindingTransitionReason(transition.Reason) {
		return errors.New("finding transition reason is unsupported")
	}
	return nil
}

func validateReviewLedgerFindingID(id string) error {
	fingerprint, found := strings.CutPrefix(id, "finding-")
	if !found {
		return errors.New(
			"must contain a canonical finding fingerprint",
		)
	}
	return validateLowerHexDigest(fingerprint, sha256.Size)
}

func validateReviewLedgerCoverage(
	coverage ReviewLedgerCoverage,
	headBindings []ReviewLedgerHeadBinding,
) error {
	if strings.TrimSpace(coverage.RequirementID) == "" ||
		strings.TrimSpace(coverage.RequirementID) != coverage.RequirementID ||
		!supportedReviewCoverageKind(coverage.Kind) ||
		strings.TrimSpace(coverage.Description) == "" {
		return errors.New("coverage identity is incomplete")
	}
	if err := validateReviewCoverageRequirements(
		[]ReviewCoverageRequirement{reviewLedgerCoverageRequirement(coverage)},
	); err != nil {
		return fmt.Errorf("coverage requirement is invalid: %w", err)
	}
	expectedRequirementDigest, err :=
		reviewLedgerCoverageRequirementDigestFor(
			reviewLedgerCoverageRequirement(coverage),
		)
	if err != nil {
		return err
	}
	if err := validateLowerHexDigest(
		coverage.RequirementDigest,
		sha256.Size,
	); err != nil {
		return fmt.Errorf("coverage requirement digest is invalid: %w", err)
	}
	if coverage.RequirementDigest != expectedRequirementDigest {
		return errors.New(
			"coverage requirement digest does not match the stored requirement",
		)
	}
	if err := validateLowerHexDigest(
		coverage.RequirementSetDigest,
		sha256.Size,
	); err != nil {
		return fmt.Errorf(
			"coverage requirement-set digest is invalid: %w",
			err,
		)
	}
	switch coverage.Status {
	case ReviewCoverageCovered,
		ReviewCoveragePartial,
		ReviewCoverageNotCovered,
		ReviewCoverageNotApplicable:
	default:
		return errors.New("coverage status is unsupported")
	}
	if coverage.Evidence == nil || len(coverage.Evidence) == 0 {
		return errors.New("coverage evidence is missing")
	}
	for _, evidence := range coverage.Evidence {
		if err := validateReviewEvidence(evidence); err != nil {
			return err
		}
	}
	expectedDigest, err := reviewLedgerCoverageEvidenceDigestFor(
		ReviewCoverageClaim{
			RequirementID: coverage.RequirementID,
			Kind:          coverage.Kind,
			Status:        coverage.Status,
			Evidence:      coverage.Evidence,
		},
	)
	if err != nil {
		return err
	}
	if err := validateLowerHexDigest(
		coverage.EvidenceDigest,
		sha256.Size,
	); err != nil {
		return fmt.Errorf("coverage evidence digest is invalid: %w", err)
	}
	if coverage.EvidenceDigest != expectedDigest {
		return errors.New(
			"evidence digest does not match the stored coverage evidence",
		)
	}
	for label, sha := range map[string]string{
		"source":        coverage.SourceSHA,
		"last-observed": coverage.LastObservedSHA,
	} {
		if err := validateCanonicalGitObjectID(sha); err != nil {
			return fmt.Errorf("coverage %s SHA is invalid: %w", label, err)
		}
	}
	if coverage.History == nil || len(coverage.History) == 0 {
		return errors.New("coverage transition history is missing")
	}
	var previous *ReviewLedgerCoverageTransition
	expectedSourceSHA := ""
	expectedLastObservedSHA := ""
	expectedRequirementSetDigest := ""
	expectedNeedsRecheck := false
	previousHeadBindingIndex := -1
	for index, transition := range coverage.History {
		if err := validateCanonicalGitObjectID(
			transition.SourceSHA,
		); err != nil {
			return fmt.Errorf(
				"coverage transition source SHA is invalid: %w",
				err,
			)
		}
		switch transition.FromStatus {
		case "",
			ReviewCoverageCovered,
			ReviewCoveragePartial,
			ReviewCoverageNotCovered,
			ReviewCoverageNotApplicable:
		default:
			return errors.New(
				"coverage transition prior status is unsupported",
			)
		}
		switch transition.ToStatus {
		case ReviewCoverageCovered,
			ReviewCoveragePartial,
			ReviewCoverageNotCovered,
			ReviewCoverageNotApplicable:
		default:
			return errors.New(
				"coverage transition status is unsupported",
			)
		}
		if err := validateLowerHexDigest(
			transition.RequirementDigest,
			sha256.Size,
		); err != nil {
			return fmt.Errorf(
				"coverage transition requirement digest is invalid: %w",
				err,
			)
		}
		if transition.RequirementDigest !=
			coverage.RequirementDigest {
			return errors.New(
				"coverage transition requirement digest does not match the stored requirement",
			)
		}
		if err := validateLowerHexDigest(
			transition.EvidenceDigest,
			sha256.Size,
		); err != nil {
			return fmt.Errorf(
				"coverage transition evidence digest is invalid: %w",
				err,
			)
		}
		if transition.FromEvidenceDigest != "" {
			if err := validateLowerHexDigest(
				transition.FromEvidenceDigest,
				sha256.Size,
			); err != nil {
				return fmt.Errorf(
					"coverage transition prior evidence digest is invalid: %w",
					err,
				)
			}
		}
		if !supportedReviewLedgerCoverageTransitionReason(
			transition.Reason,
		) {
			return errors.New(
				"coverage transition reason is unsupported",
			)
		}
		headBindingIndex := reviewLedgerHeadBindingIndex(
			headBindings,
			transition.SourceSHA,
		)
		if headBindingIndex < 0 {
			return errors.New(
				"coverage transition is not bound to a persisted exact delta",
			)
		}
		if headBindingIndex < previousHeadBindingIndex {
			return errors.New(
				"coverage transition regresses persisted head order",
			)
		}
		previousHeadBindingIndex = headBindingIndex
		if index == 0 &&
			(transition.FromStatus != "" ||
				transition.Reason != ReviewLedgerCoverageRecorded) {
			return errors.New(
				"initial coverage transition is not a recording",
			)
		}
		if index == 0 {
			if transition.FromEvidenceDigest != "" {
				return errors.New(
					"initial coverage transition has prior evidence",
				)
			}
			expectedSourceSHA = transition.SourceSHA
			expectedLastObservedSHA = transition.SourceSHA
		} else {
			if transition.Reason == ReviewLedgerCoverageRecorded {
				return errors.New(
					"coverage recording appears after the initial transition",
				)
			}
			if transition.FromStatus != previous.ToStatus ||
				transition.FromEvidenceDigest != previous.EvidenceDigest {
				return errors.New(
					"coverage transition status or evidence history is discontinuous",
				)
			}
		}
		switch transition.Reason {
		case ReviewLedgerCoverageImpacted:
			if transition.RequirementSetDigest != "" {
				return errors.New(
					"coverage impact transition has an unexpected requirement-set binding",
				)
			}
			if transition.FromStatus != transition.ToStatus {
				return errors.New(
					"coverage impact transition changes status",
				)
			}
			if transition.FromEvidenceDigest !=
				transition.EvidenceDigest {
				return errors.New(
					"coverage impact transition changes evidence",
				)
			}
			expectedNeedsRecheck = true
		case ReviewLedgerCoverageRecorded,
			ReviewLedgerCoverageRechecked:
			if err := validateLowerHexDigest(
				transition.RequirementSetDigest,
				sha256.Size,
			); err != nil {
				return fmt.Errorf(
					"coverage transition requirement-set digest is invalid: %w",
					err,
				)
			}
			binding := headBindings[headBindingIndex]
			if transition.RequirementSetDigest !=
				binding.CoverageRequirementsDigest ||
				!reviewLedgerRequirementInHeadBinding(
					binding,
					reviewLedgerCoverageRequirement(coverage),
				) {
				return errors.New(
					"coverage recording is not bound to its immutable exact-SHA requirement set",
				)
			}
			expectedSourceSHA = transition.SourceSHA
			expectedLastObservedSHA = transition.SourceSHA
			expectedRequirementSetDigest =
				transition.RequirementSetDigest
			expectedNeedsRecheck = false
		}
		previous = &coverage.History[index]
	}
	latest := coverage.History[len(coverage.History)-1]
	if latest.ToStatus != coverage.Status ||
		latest.EvidenceDigest != coverage.EvidenceDigest {
		return errors.New(
			"latest coverage transition does not match current state",
		)
	}
	if coverage.RequirementSetDigest !=
		expectedRequirementSetDigest {
		return errors.New(
			"coverage requirement-set history does not match current state",
		)
	}
	if coverage.SourceSHA != expectedSourceSHA ||
		coverage.LastObservedSHA != expectedLastObservedSHA ||
		coverage.NeedsRecheck != expectedNeedsRecheck {
		return errors.New(
			"coverage transition SHA or recheck history does not match current state",
		)
	}
	return nil
}

func validateLowerHexDigest(value string, byteLength int) error {
	if len(value) != byteLength*2 ||
		value != strings.ToLower(value) {
		return fmt.Errorf(
			"must be a %d-character lowercase hexadecimal value",
			byteLength*2,
		)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != byteLength {
		return fmt.Errorf(
			"must be a %d-character lowercase hexadecimal value",
			byteLength*2,
		)
	}
	return nil
}

func reviewLedgerFindingEvidenceDigest(
	observation ReviewLedgerFindingObservation,
) (string, error) {
	return reviewLedgerFindingEvidenceDigestFor(
		observation.Finding,
		observation.VerificationEvidence,
		observation.VerificationReceipt,
	)
}

func reviewLedgerFindingEvidenceDigestFor(
	finding ReviewCanonicalFinding,
	verificationEvidence []ReviewEvidence,
	verificationReceipt ReviewLedgerVerificationReceipt,
) (string, error) {
	evidence := uniqueSortedReviewEvidence(append(
		append(
			[]ReviewEvidence(nil),
			finding.Evidence...,
		),
		verificationEvidence...,
	))
	record := struct {
		ExactSHA            string                          `json:"exact_sha"`
		Summary             string                          `json:"summary"`
		Location            ReviewFindingLocation           `json:"location"`
		BehavioralPath      string                          `json:"behavioral_path"`
		ViolatedInvariant   string                          `json:"violated_invariant"`
		Evidence            []ReviewEvidence                `json:"evidence"`
		Provenance          []ReviewFindingProvenance       `json:"provenance"`
		VerificationReceipt ReviewLedgerVerificationReceipt `json:"verification_receipt"`
	}{
		ExactSHA: finding.ExactSHA,
		Summary: strings.Join(
			strings.Fields(finding.Summary),
			" ",
		),
		Location:          finding.Location,
		BehavioralPath:    normalizedReviewFindingText(finding.BehavioralPath),
		ViolatedInvariant: normalizedReviewFindingText(finding.ViolatedInvariant),
		Evidence:          evidence,
		Provenance: append(
			[]ReviewFindingProvenance(nil),
			finding.Provenance...,
		),
		VerificationReceipt: verificationReceipt,
	}
	body, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf(
			"failed to canonicalize review ledger finding evidence: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func reviewLedgerFindingMaterialEvidenceDigest(
	observation ReviewLedgerFindingObservation,
) (string, error) {
	return reviewLedgerFindingMaterialEvidenceDigestFor(
		observation.Finding,
		observation.VerificationEvidence,
	)
}

func reviewLedgerFindingMaterialEvidenceDigestFor(
	finding ReviewCanonicalFinding,
	verificationEvidence []ReviewEvidence,
) (string, error) {
	evidence := uniqueSortedReviewEvidence(append(
		append(
			[]ReviewEvidence(nil),
			finding.Evidence...,
		),
		verificationEvidence...,
	))
	record := struct {
		Summary           string                `json:"summary"`
		Location          ReviewFindingLocation `json:"location"`
		BehavioralPath    string                `json:"behavioral_path"`
		ViolatedInvariant string                `json:"violated_invariant"`
		Evidence          []ReviewEvidence      `json:"evidence"`
	}{
		Summary: strings.Join(
			strings.Fields(finding.Summary),
			" ",
		),
		Location:          finding.Location,
		BehavioralPath:    normalizedReviewFindingText(finding.BehavioralPath),
		ViolatedInvariant: normalizedReviewFindingText(finding.ViolatedInvariant),
		Evidence:          evidence,
	}
	body, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf(
			"failed to canonicalize review ledger material finding evidence: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func reviewLedgerCoverageRequirementDigestFor(
	requirement ReviewCoverageRequirement,
) (string, error) {
	record := struct {
		RequirementID string             `json:"requirement_id"`
		Kind          ReviewCoverageKind `json:"kind"`
		Description   string             `json:"description"`
	}{
		RequirementID: requirement.ID,
		Kind:          requirement.Kind,
		Description:   requirement.Description,
	}
	body, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf(
			"failed to canonicalize review ledger coverage requirement: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func reviewLedgerCoverageEvidenceDigest(
	observation ReviewLedgerCoverageObservation,
) (string, error) {
	return reviewLedgerCoverageEvidenceDigestFor(observation.Claim)
}

func reviewLedgerCoverageEvidenceDigestFor(
	claim ReviewCoverageClaim,
) (string, error) {
	record := struct {
		RequirementID string               `json:"requirement_id"`
		Kind          ReviewCoverageKind   `json:"kind"`
		Status        ReviewCoverageStatus `json:"status"`
		Evidence      []ReviewEvidence     `json:"evidence"`
	}{
		RequirementID: claim.RequirementID,
		Kind:          claim.Kind,
		Status:        claim.Status,
		Evidence: uniqueSortedReviewEvidence(
			claim.Evidence,
		),
	}
	body, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf(
			"failed to canonicalize review ledger coverage evidence: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func reviewLedgerVerificationOutcomeForStatus(
	status ReviewLedgerFindingStatus,
) (ReviewVerificationOutcome, error) {
	switch status {
	case ReviewLedgerFindingReported, ReviewLedgerFindingMissed:
		return ReviewVerificationConfirmed, nil
	case ReviewLedgerFindingRejected, ReviewLedgerFindingFixed:
		return ReviewVerificationRejected, nil
	default:
		return "", errors.New(
			"finding disposition has no verifier outcome",
		)
	}
}

func validateReviewLedgerVerificationReceiptFields(
	receipt ReviewLedgerVerificationReceipt,
) error {
	for label, value := range map[string]string{
		"finding ID":         receipt.FindingID,
		"assignment ID":      receipt.AssignmentID,
		"verifier worker ID": receipt.VerifierWorkerID,
	} {
		if strings.TrimSpace(value) == "" ||
			strings.TrimSpace(value) != value {
			return fmt.Errorf(
				"verification receipt %s is invalid",
				label,
			)
		}
	}
	if !strings.HasPrefix(
		receipt.AssignmentID,
		"verification:"+receipt.FindingID+":",
	) {
		return errors.New(
			"verification receipt assignment does not match the finding identity",
		)
	}
	if err := validateCanonicalGitObjectID(receipt.ExactSHA); err != nil {
		return fmt.Errorf(
			"verification receipt exact SHA is invalid: %w",
			err,
		)
	}
	if err := validateLowerHexDigest(
		receipt.CandidateRevision,
		sha256.Size,
	); err != nil {
		return fmt.Errorf(
			"verification receipt candidate revision is invalid: %w",
			err,
		)
	}
	switch receipt.Outcome {
	case ReviewVerificationConfirmed, ReviewVerificationRejected:
		return nil
	default:
		return errors.New(
			"verification receipt outcome is not a durable disposition",
		)
	}
}

func validateReviewLedgerObservationVerificationReceipt(
	observation ReviewLedgerFindingObservation,
	headSHA string,
) error {
	receipt := observation.VerificationReceipt
	if err := validateReviewLedgerVerificationReceiptFields(
		receipt,
	); err != nil {
		return err
	}
	if receipt.FindingID != observation.Finding.ID {
		return errors.New(
			"verification receipt does not match the finding identity",
		)
	}
	if receipt.ExactSHA != headSHA {
		return errors.New(
			"verification receipt does not match the delta head",
		)
	}
	candidateRevision, err := reviewFindingCandidateRevision(
		observation.Finding,
	)
	if err != nil {
		return err
	}
	if receipt.CandidateRevision != candidateRevision {
		return errors.New(
			"verification receipt does not match the finding revision",
		)
	}
	expectedOutcome, err := reviewLedgerVerificationOutcomeForStatus(
		observation.Status,
	)
	if err != nil {
		return err
	}
	if receipt.Outcome != expectedOutcome {
		return errors.New(
			"verification receipt outcome does not match the finding disposition",
		)
	}
	for _, provenance := range observation.Finding.Provenance {
		if provenance.WorkerID == receipt.VerifierWorkerID {
			return errors.New(
				"verification receipt is not independent from finding discovery",
			)
		}
	}
	return nil
}

func validateReviewLedgerFindingObservation(
	observation ReviewLedgerFindingObservation,
	headSHA string,
) error {
	if err := validateReviewLedgerCanonicalFinding(
		observation.Finding,
	); err != nil {
		return err
	}
	if observation.Finding.ExactSHA != headSHA {
		return errors.New(
			"finding observation does not match the delta head",
		)
	}
	if !supportedReviewLedgerFindingStatus(observation.Status) {
		return errors.New("finding observation status is unsupported")
	}
	if observation.VerificationEvidence == nil ||
		len(observation.VerificationEvidence) == 0 {
		return errors.New(
			"finding observation has no independent verification evidence",
		)
	}
	for _, evidence := range observation.VerificationEvidence {
		if err := validateReviewEvidence(evidence); err != nil {
			return err
		}
	}
	return validateReviewLedgerObservationVerificationReceipt(
		observation,
		headSHA,
	)
}

func validateReviewLedgerCoverageObservation(
	observation ReviewLedgerCoverageObservation,
	planRequirements []ReviewCoverageRequirement,
) error {
	if strings.TrimSpace(observation.Requirement.ID) == "" ||
		strings.TrimSpace(observation.Requirement.Description) == "" ||
		!supportedReviewCoverageKind(observation.Requirement.Kind) {
		return errors.New("coverage observation requirement is invalid")
	}
	if observation.Requirement.ID != observation.Claim.RequirementID ||
		observation.Requirement.Kind != observation.Claim.Kind {
		return errors.New(
			"coverage observation claim does not match its requirement",
		)
	}
	if err := validateReviewCoverageClaims(
		[]ReviewCoverageClaim{observation.Claim},
		planRequirements,
	); err != nil {
		return err
	}
	for _, requirement := range planRequirements {
		if requirement.ID != observation.Requirement.ID {
			continue
		}
		if !reflect.DeepEqual(requirement, observation.Requirement) {
			return errors.New(
				"coverage observation requirement does not match the exact-SHA plan",
			)
		}
		return nil
	}
	return errors.New(
		"coverage observation requirement is not in the exact-SHA plan",
	)
}

func reviewLedgerChangedPaths(
	delta ReviewPlanInputs,
) map[string]struct{} {
	paths := make(map[string]struct{}, len(delta.ChangedFiles)*2)
	for _, file := range delta.ChangedFiles {
		paths[file.Path] = struct{}{}
		if file.PreviousPath != "" {
			paths[file.PreviousPath] = struct{}{}
		}
	}
	return paths
}

func reviewLedgerFindingImpacted(
	finding ReviewLedgerFinding,
	changed map[string]struct{},
) bool {
	if _, ok := changed[finding.Finding.Location.Path]; ok {
		return true
	}
	for _, evidence := range finding.Finding.Evidence {
		if _, ok := changed[evidence.Path]; ok && evidence.Path != "" {
			return true
		}
	}
	for _, evidence := range finding.VerificationEvidence {
		if _, ok := changed[evidence.Path]; ok && evidence.Path != "" {
			return true
		}
	}
	return false
}

func reviewLedgerCoverageImpacted(
	coverage ReviewLedgerCoverage,
	changed map[string]struct{},
) bool {
	for _, target := range coverage.ChangedTargets {
		if _, ok := changed[target.Path]; ok {
			return true
		}
	}
	for _, evidence := range coverage.Evidence {
		if _, ok := changed[evidence.Path]; ok && evidence.Path != "" {
			return true
		}
	}
	if coverage.Kind == ReviewCoverageCallPath {
		path := strings.TrimPrefix(
			coverage.RequirementID,
			"call-path:",
		)
		if _, ok := changed[path]; ok {
			return true
		}
		if _, ok := changed[coverage.Description]; ok {
			return true
		}
	}
	return false
}

func reviewLedgerNewFindingProvenance(
	hasPriorHead bool,
	presentInDeltaBase *bool,
) (ReviewLedgerFindingProvenance, error) {
	if presentInDeltaBase == nil {
		return "", errors.New(
			"new finding is missing delta-base presence evidence",
		)
	}
	if hasPriorHead {
		if *presentInDeltaBase {
			return ReviewLedgerFindingPreviouslyMissed, nil
		}
		return ReviewLedgerFindingFixIntroduced, nil
	}
	if *presentInDeltaBase {
		return ReviewLedgerFindingPreExisting, nil
	}
	return ReviewLedgerFindingOriginalPR, nil
}

func reviewLedgerPriorFindingProvenance(
	prior ReviewLedgerFinding,
	observation ReviewLedgerFindingObservation,
) (ReviewLedgerFindingProvenance, error) {
	switch prior.Status {
	case ReviewLedgerFindingReported:
		return ReviewLedgerFindingPreviouslyReported, nil
	case ReviewLedgerFindingMissed:
		return ReviewLedgerFindingPreviouslyMissed, nil
	case ReviewLedgerFindingRejected:
		if observation.Status == ReviewLedgerFindingRejected {
			return prior.Provenance, nil
		}
		return reviewLedgerNewFindingProvenance(
			true,
			observation.PresentInDeltaBase,
		)
	case ReviewLedgerFindingFixed:
		return reviewLedgerNewFindingProvenance(
			true,
			observation.PresentInDeltaBase,
		)
	default:
		return "", errors.New("prior finding status is unsupported")
	}
}

func reviewLedgerVerificationReceiptIsFresh(
	prior ReviewLedgerVerificationReceipt,
	current ReviewLedgerVerificationReceipt,
) bool {
	return current.ExactSHA != prior.ExactSHA &&
		current.CandidateRevision != prior.CandidateRevision
}

func appendReviewLedgerFindingTransition(
	finding *ReviewLedgerFinding,
	sourceSHA string,
	previous *ReviewLedgerFinding,
	reason ReviewLedgerFindingTransitionReason,
	presentInDeltaBase *bool,
) {
	var presence *bool
	if presentInDeltaBase != nil {
		value := *presentInDeltaBase
		presence = &value
	}
	finding.History = append(
		finding.History,
		ReviewLedgerFindingTransition{
			SourceSHA:              sourceSHA,
			ToStatus:               finding.Status,
			ToProvenance:           finding.Provenance,
			ToFindingID:            finding.ID,
			EvidenceDigest:         finding.EvidenceDigest,
			MaterialEvidenceDigest: finding.MaterialEvidenceDigest,
			PresentInDeltaBase:     presence,
			Reason:                 reason,
		},
	)
	if previous == nil {
		return
	}
	latest := &finding.History[len(finding.History)-1]
	latest.FromStatus = previous.Status
	latest.FromProvenance = previous.Provenance
	latest.FromFindingID = previous.ID
	latest.FromEvidenceDigest = previous.EvidenceDigest
	latest.FromMaterialEvidenceDigest =
		previous.MaterialEvidenceDigest
}

func appendReviewLedgerCoverageTransition(
	coverage *ReviewLedgerCoverage,
	sourceSHA string,
	previous *ReviewLedgerCoverage,
	reason ReviewLedgerCoverageTransitionReason,
	requirementSetDigest string,
) {
	transition := ReviewLedgerCoverageTransition{
		SourceSHA:            sourceSHA,
		ToStatus:             coverage.Status,
		RequirementDigest:    coverage.RequirementDigest,
		RequirementSetDigest: requirementSetDigest,
		EvidenceDigest:       coverage.EvidenceDigest,
		Reason:               reason,
	}
	if previous != nil {
		transition.FromStatus = previous.Status
		transition.FromEvidenceDigest = previous.EvidenceDigest
	}
	coverage.History = append(coverage.History, transition)
}

type reviewLedgerFindingObservationMatch struct {
	Index        int
	PreviousID   string
	PreviousPath string
}

func reviewLedgerFindingIndex(
	findings []ReviewLedgerFinding,
	id string,
) int {
	for index := range findings {
		if findings[index].ID == id {
			return index
		}
	}
	return -1
}

func reviewLedgerFindingIDAtPath(
	finding ReviewCanonicalFinding,
	path string,
) (string, error) {
	fingerprint, err := reviewFindingFingerprint(
		ReviewFindingCandidate{
			CandidateID: "review-ledger-rename",
			Summary:     finding.Summary,
			Location: ReviewFindingLocation{
				Path:      path,
				Symbol:    finding.Location.Symbol,
				StartLine: finding.Location.StartLine,
				EndLine:   finding.Location.EndLine,
			},
			BehavioralPath:    finding.BehavioralPath,
			ViolatedInvariant: finding.ViolatedInvariant,
			Severity:          finding.Severity,
			Confidence:        finding.Confidence,
			Evidence:          finding.Evidence,
		},
	)
	if err != nil {
		return "", err
	}
	return "finding-" + fingerprint, nil
}

func matchReviewLedgerFindingObservation(
	ledger *ReviewLedger,
	observation ReviewLedgerFindingObservation,
	delta ReviewPlanInputs,
) (reviewLedgerFindingObservationMatch, error) {
	exactIndex := reviewLedgerFindingIndex(
		ledger.Findings,
		observation.Finding.ID,
	)
	match := reviewLedgerFindingObservationMatch{Index: exactIndex}
	for _, file := range delta.ChangedFiles {
		if file.PreviousPath == "" ||
			file.PreviousPath == file.Path ||
			file.Path != observation.Finding.Location.Path {
			continue
		}
		previousID, err := reviewLedgerFindingIDAtPath(
			observation.Finding,
			file.PreviousPath,
		)
		if err != nil {
			return reviewLedgerFindingObservationMatch{}, err
		}
		previousIndex := reviewLedgerFindingIndex(
			ledger.Findings,
			previousID,
		)
		if previousIndex < 0 {
			continue
		}
		if exactIndex >= 0 && previousIndex != exactIndex {
			return reviewLedgerFindingObservationMatch{}, errors.New(
				"renamed finding collides with an existing current-path identity",
			)
		}
		if match.PreviousID != "" &&
			match.PreviousID != previousID {
			return reviewLedgerFindingObservationMatch{}, errors.New(
				"renamed finding matches multiple prior identities",
			)
		}
		match = reviewLedgerFindingObservationMatch{
			Index:        previousIndex,
			PreviousID:   previousID,
			PreviousPath: file.PreviousPath,
		}
	}
	return match, nil
}

func reviewLedgerFindingEvidenceDigestAfterRename(
	finding ReviewLedgerFinding,
	previousPath string,
	currentPath string,
	headSHA string,
) (string, error) {
	canonical := cloneReviewCanonicalFinding(finding.Finding)
	canonical.ExactSHA = headSHA
	if canonical.Location.Path == previousPath {
		canonical.Location.Path = currentPath
	}
	for index := range canonical.Evidence {
		if canonical.Evidence[index].Path == previousPath {
			canonical.Evidence[index].Path = currentPath
		}
	}
	verification := append(
		[]ReviewEvidence(nil),
		finding.VerificationEvidence...,
	)
	for index := range verification {
		if verification[index].Path == previousPath {
			verification[index].Path = currentPath
		}
	}
	return reviewLedgerFindingEvidenceDigestFor(
		canonical,
		verification,
		finding.VerificationReceipt,
	)
}

func reviewLedgerFindingMaterialEvidenceDigestAfterRename(
	finding ReviewLedgerFinding,
	previousPath string,
	currentPath string,
) (string, error) {
	canonical := cloneReviewCanonicalFinding(finding.Finding)
	if canonical.Location.Path == previousPath {
		canonical.Location.Path = currentPath
	}
	for index := range canonical.Evidence {
		if canonical.Evidence[index].Path == previousPath {
			canonical.Evidence[index].Path = currentPath
		}
	}
	verification := append(
		[]ReviewEvidence(nil),
		finding.VerificationEvidence...,
	)
	for index := range verification {
		if verification[index].Path == previousPath {
			verification[index].Path = currentPath
		}
	}
	return reviewLedgerFindingMaterialEvidenceDigestFor(
		canonical,
		verification,
	)
}

func reconcileReviewLedgerFindingRenames(
	ledger *ReviewLedger,
	delta ReviewPlanInputs,
) (map[string]ReviewLedgerFinding, error) {
	renames := make(map[string]string)
	for _, file := range delta.ChangedFiles {
		if file.PreviousPath == "" ||
			file.PreviousPath == file.Path {
			continue
		}
		if current, exists := renames[file.PreviousPath]; exists &&
			current != file.Path {
			return nil, fmt.Errorf(
				"review ledger rename path %q has multiple destinations",
				file.PreviousPath,
			)
		}
		renames[file.PreviousPath] = file.Path
	}
	if len(renames) == 0 {
		return map[string]ReviewLedgerFinding{}, nil
	}

	occupied := make(map[string]struct{}, len(ledger.Findings))
	for _, finding := range ledger.Findings {
		occupied[finding.ID] = struct{}{}
	}
	previousByCurrentID := make(
		map[string]ReviewLedgerFinding,
		len(renames),
	)
	for index := range ledger.Findings {
		finding := &ledger.Findings[index]
		if finding.Status != ReviewLedgerFindingReported &&
			finding.Status != ReviewLedgerFindingMissed &&
			finding.Status != ReviewLedgerFindingRejected &&
			finding.Status != ReviewLedgerFindingFixed {
			continue
		}
		currentPath, renamed := renames[finding.Finding.Location.Path]
		if !renamed {
			continue
		}

		previous := *finding
		canonical := cloneReviewCanonicalFinding(finding.Finding)
		previousPath := canonical.Location.Path
		canonical.Location.Path = currentPath
		canonical.ExactSHA = delta.HeadSHA
		for evidenceIndex := range canonical.Evidence {
			if canonical.Evidence[evidenceIndex].Path == previousPath {
				canonical.Evidence[evidenceIndex].Path = currentPath
			}
		}
		verification := append(
			[]ReviewEvidence(nil),
			finding.VerificationEvidence...,
		)
		for evidenceIndex := range verification {
			if verification[evidenceIndex].Path == previousPath {
				verification[evidenceIndex].Path = currentPath
			}
		}
		fingerprint, err := reviewFindingFingerprint(
			ReviewFindingCandidate{
				CandidateID:       "review-ledger-rename",
				Summary:           canonical.Summary,
				Location:          canonical.Location,
				BehavioralPath:    canonical.BehavioralPath,
				ViolatedInvariant: canonical.ViolatedInvariant,
				Severity:          canonical.Severity,
				Confidence:        canonical.Confidence,
				Evidence:          canonical.Evidence,
			},
		)
		if err != nil {
			return nil, err
		}
		currentID := "finding-" + fingerprint
		delete(occupied, finding.ID)
		if _, collision := occupied[currentID]; collision {
			return nil, fmt.Errorf(
				"renamed finding %q collides with an existing current-path identity",
				finding.ID,
			)
		}
		occupied[currentID] = struct{}{}
		canonical.ID = currentID
		canonical.Fingerprint = fingerprint
		digest, err := reviewLedgerFindingEvidenceDigestFor(
			canonical,
			verification,
			finding.VerificationReceipt,
		)
		if err != nil {
			return nil, err
		}
		materialDigest, err :=
			reviewLedgerFindingMaterialEvidenceDigestFor(
				canonical,
				verification,
			)
		if err != nil {
			return nil, err
		}

		finding.ID = currentID
		finding.Fingerprint = fingerprint
		finding.SourceSHA = delta.HeadSHA
		finding.Finding = canonical
		finding.VerificationEvidence =
			uniqueSortedReviewEvidence(verification)
		finding.EvidenceDigest = digest
		finding.MaterialEvidenceDigest = materialDigest
		previousByCurrentID[currentID] = previous
		if finding.Status == ReviewLedgerFindingRejected ||
			finding.Status == ReviewLedgerFindingFixed {
			appendReviewLedgerFindingTransition(
				finding,
				delta.HeadSHA,
				&previous,
				ReviewLedgerFindingRenamed,
				nil,
			)
		}
	}
	sort.Slice(ledger.Findings, func(i, j int) bool {
		return ledger.Findings[i].ID < ledger.Findings[j].ID
	})
	return previousByCurrentID, nil
}

func updateReviewLedgerFindingObservationEvidence(
	finding *ReviewLedgerFinding,
	observation ReviewLedgerFindingObservation,
	digest string,
	materialDigest string,
	headSHA string,
) {
	finding.ID = observation.Finding.ID
	finding.Fingerprint = observation.Finding.Fingerprint
	finding.SourceSHA = headSHA
	finding.LastObservedSHA = headSHA
	finding.Finding = cloneReviewCanonicalFinding(observation.Finding)
	finding.VerificationEvidence = uniqueSortedReviewEvidence(
		observation.VerificationEvidence,
	)
	finding.VerificationReceipt = observation.VerificationReceipt
	finding.EvidenceDigest = digest
	finding.MaterialEvidenceDigest = materialDigest
	finding.NeedsRecheck = false
}

// reviewCoverageRequirementIdentityEqual compares only the fields that
// define a coverage requirement's true identity (ID, Kind, Description),
// not Critical, AccountableLane, or ChangedTargets -- all three of which
// are recomputed fresh from the current diff/lane-selection every round
// and are expected to legitimately differ round to round for the exact
// same logical requirement.
//
// Comparing the whole ReviewCoverageRequirement via reflect.DeepEqual
// treated that expected, legitimate per-round variation
// as a conflict, firing on nearly every multi-round PR for any coverage
// kind whose ID is a stable tag/index rather than a content hash
// (acceptance_criterion, state_transition, persistence_boundary) --
// exactly the kinds where a same-ID, different-round comparison is
// actually expected to happen as the plan's diff changes round to round.
// Kinds with content-hashed IDs (changed_symbol, changed_branch,
// call_path) never relied on comparing these fields in the first place: a
// genuinely different diff already produces a different ID for them
// (the ID *is* derived from the same content these fields carry), so
// there is never a same-ID-different-content case here for a full-struct
// comparison to have been protecting against for those kinds either.
func reviewCoverageRequirementIdentityEqual(
	a, b ReviewCoverageRequirement,
) bool {
	return a.ID == b.ID &&
		a.Kind == b.Kind &&
		a.Description == b.Description
}

func reviewLedgerExactCoverageRequirements(
	previous *ReviewLedger,
	delta ReviewPlanInputs,
	planRequirements []ReviewCoverageRequirement,
) ([]ReviewCoverageRequirement, error) {
	// NeedsRecheck is itself an exact persisted binding. Carry only unresolved
	// or currently impacted requirements; unrelated prior coverage cannot be
	// claimed unless the current immutable plan independently requires it.
	requirements := append(
		[]ReviewCoverageRequirement(nil),
		planRequirements...,
	)
	byID := make(map[string]ReviewCoverageRequirement, len(requirements))
	for _, requirement := range requirements {
		byID[requirement.ID] = requirement
	}
	if previous != nil {
		changed := reviewLedgerChangedPaths(delta)
		for _, coverage := range previous.Coverage {
			requirement := reviewLedgerCoverageRequirement(coverage)
			if planned, exists := byID[requirement.ID]; exists &&
				!reviewCoverageRequirementIdentityEqual(planned, requirement) {
				return nil, fmt.Errorf(
					"carried coverage requirement %q conflicts with the exact-SHA plan",
					requirement.ID,
				)
			}
			if !coverage.NeedsRecheck &&
				!reviewLedgerCoverageImpacted(coverage, changed) {
				continue
			}
			if _, exists := byID[requirement.ID]; exists {
				continue
			}
			byID[requirement.ID] = requirement
			requirements = append(requirements, requirement)
		}
	}
	sort.Slice(requirements, func(i, j int) bool {
		if requirements[i].Kind != requirements[j].Kind {
			return requirements[i].Kind < requirements[j].Kind
		}
		return requirements[i].ID < requirements[j].ID
	})
	if err := validateReviewCoverageRequirements(requirements); err != nil {
		return nil, err
	}
	if requirements == nil {
		return []ReviewCoverageRequirement{}, nil
	}
	return requirements, nil
}

func buildReviewLedgerHeadBinding(
	previous *ReviewLedger,
	delta ReviewPlanInputs,
	planRequirements []ReviewCoverageRequirement,
) (ReviewLedgerHeadBinding, error) {
	if err := validateReviewPlanInputs(delta); err != nil {
		return ReviewLedgerHeadBinding{}, fmt.Errorf(
			"review ledger exact delta is invalid: %w",
			err,
		)
	}
	if planRequirements == nil {
		return ReviewLedgerHeadBinding{}, errors.New(
			"review ledger exact-SHA plan requirements are not initialized",
		)
	}
	if err := validateReviewCoverageRequirements(
		planRequirements,
	); err != nil {
		return ReviewLedgerHeadBinding{}, fmt.Errorf(
			"review ledger exact-SHA plan requirements are invalid: %w",
			err,
		)
	}
	exactCoverageRequirements, err := reviewLedgerExactCoverageRequirements(
		previous,
		delta,
		planRequirements,
	)
	if err != nil {
		return ReviewLedgerHeadBinding{}, fmt.Errorf(
			"review ledger exact-SHA coverage binding is invalid: %w",
			err,
		)
	}
	requirementsDigest, err :=
		reviewLedgerCoverageRequirementsDigestFor(
			exactCoverageRequirements,
		)
	if err != nil {
		return ReviewLedgerHeadBinding{}, fmt.Errorf(
			"review ledger exact-SHA coverage digest is invalid: %w",
			err,
		)
	}
	boundDelta := cloneReviewPlanInputs(delta)
	return ReviewLedgerHeadBinding{
		Delta: boundDelta,
		CoverageRequirements: append(
			[]ReviewCoverageRequirement{},
			exactCoverageRequirements...,
		),
		CoverageRequirementsDigest: requirementsDigest,
	}, nil
}

func reviewLedgerHeadBindingFromSnapshot(
	previous *ReviewLedger,
	snapshot ReviewLedgerTransitionSnapshot,
) (ReviewLedgerHeadBinding, error) {
	if snapshot.Plan.BaseSHA != snapshot.Inputs.BaseSHA ||
		snapshot.Plan.HeadSHA != snapshot.Inputs.HeadSHA {
		return ReviewLedgerHeadBinding{}, errors.New(
			"review ledger trusted plan does not match its exact delta",
		)
	}
	return buildReviewLedgerHeadBinding(
		previous,
		snapshot.Inputs,
		snapshot.Plan.CoverageRequirements,
	)
}

func applyReviewLedgerFindingObservation(
	ledger *ReviewLedger,
	observation ReviewLedgerFindingObservation,
	hasPriorHead bool,
	delta ReviewPlanInputs,
	result *ReviewLedgerDeltaResult,
) error {
	headSHA := ledger.HeadSHA
	if err := validateReviewLedgerFindingObservation(
		observation,
		headSHA,
	); err != nil {
		return err
	}
	digest, err := reviewLedgerFindingEvidenceDigest(observation)
	if err != nil {
		return err
	}
	materialDigest, err :=
		reviewLedgerFindingMaterialEvidenceDigest(observation)
	if err != nil {
		return err
	}
	match, err := matchReviewLedgerFindingObservation(
		ledger,
		observation,
		delta,
	)
	if err != nil {
		return err
	}
	if match.Index < 0 {
		if observation.Status == ReviewLedgerFindingFixed {
			return errors.New(
				"new finding cannot begin in the fixed state",
			)
		}
		provenance, err := reviewLedgerNewFindingProvenance(
			hasPriorHead,
			observation.PresentInDeltaBase,
		)
		if err != nil {
			return err
		}
		finding := ReviewLedgerFinding{
			ID:                     observation.Finding.ID,
			Fingerprint:            observation.Finding.Fingerprint,
			Status:                 observation.Status,
			Provenance:             provenance,
			SourceSHA:              headSHA,
			FirstSeenSHA:           headSHA,
			LastObservedSHA:        headSHA,
			Finding:                cloneReviewCanonicalFinding(observation.Finding),
			VerificationEvidence:   uniqueSortedReviewEvidence(observation.VerificationEvidence),
			VerificationReceipt:    observation.VerificationReceipt,
			EvidenceDigest:         digest,
			MaterialEvidenceDigest: materialDigest,
			History:                []ReviewLedgerFindingTransition{},
		}
		if finding.Status == ReviewLedgerFindingReported {
			finding.ReportedSHA = headSHA
		}
		appendReviewLedgerFindingTransition(
			&finding,
			headSHA,
			nil,
			ReviewLedgerFindingDiscovered,
			observation.PresentInDeltaBase,
		)
		index := sort.Search(
			len(ledger.Findings),
			func(index int) bool {
				return ledger.Findings[index].ID >= finding.ID
			},
		)
		ledger.Findings = append(
			ledger.Findings,
			ReviewLedgerFinding{},
		)
		copy(ledger.Findings[index+1:], ledger.Findings[index:])
		ledger.Findings[index] = finding
		if finding.Status == ReviewLedgerFindingReported ||
			finding.Status == ReviewLedgerFindingMissed {
			result.NovelFindingIDs = append(
				result.NovelFindingIDs,
				finding.ID,
			)
		}
		return nil
	}

	finding := &ledger.Findings[match.Index]
	renamed := match.PreviousID != ""
	if !renamed &&
		finding.Fingerprint != observation.Finding.Fingerprint {
		return errors.New(
			"finding observation identity collides with another fingerprint",
		)
	}
	if renamed {
		defer sort.Slice(ledger.Findings, func(i, j int) bool {
			return ledger.Findings[i].ID < ledger.Findings[j].ID
		})
	}
	previous := *finding
	sameEvidence := finding.MaterialEvidenceDigest == materialDigest
	if renamed {
		renamedDigest, err :=
			reviewLedgerFindingMaterialEvidenceDigestAfterRename(
				*finding,
				match.PreviousPath,
				observation.Finding.Location.Path,
			)
		if err != nil {
			return err
		}
		sameEvidence = renamedDigest == materialDigest
	}
	if observation.Status == ReviewLedgerFindingFixed &&
		(!finding.NeedsRecheck ||
			!reviewLedgerVerificationReceiptIsFresh(
				finding.VerificationReceipt,
				observation.VerificationReceipt,
			)) {
		return errors.New(
			"finding cannot be fixed without impacted, fresh exact-SHA verification receipt",
		)
	}

	if sameEvidence &&
		finding.Status == ReviewLedgerFindingRejected &&
		observation.Status != ReviewLedgerFindingRejected {
		updateReviewLedgerFindingObservationEvidence(
			finding,
			observation,
			digest,
			materialDigest,
			headSHA,
		)
		appendReviewLedgerFindingTransition(
			finding,
			headSHA,
			&previous,
			ReviewLedgerFindingRejectedSuppressed,
			nil,
		)
		result.SuppressedFindingIDs = append(
			result.SuppressedFindingIDs,
			finding.ID,
		)
		return nil
	}
	if sameEvidence &&
		finding.Status == ReviewLedgerFindingReported &&
		observation.Status != ReviewLedgerFindingFixed {
		finding.Provenance = ReviewLedgerFindingPreviouslyReported
		updateReviewLedgerFindingObservationEvidence(
			finding,
			observation,
			digest,
			materialDigest,
			headSHA,
		)
		appendReviewLedgerFindingTransition(
			finding,
			headSHA,
			&previous,
			ReviewLedgerFindingReportedSuppressed,
			nil,
		)
		result.SuppressedFindingIDs = append(
			result.SuppressedFindingIDs,
			finding.ID,
		)
		return nil
	}

	provenance, err := reviewLedgerPriorFindingProvenance(
		*finding,
		observation,
	)
	if err != nil {
		return err
	}
	status := observation.Status
	if finding.Status == ReviewLedgerFindingReported &&
		status == ReviewLedgerFindingMissed {
		status = ReviewLedgerFindingReported
	}
	finding.Status = status
	finding.Provenance = provenance
	updateReviewLedgerFindingObservationEvidence(
		finding,
		observation,
		digest,
		materialDigest,
		headSHA,
	)
	if finding.Status == ReviewLedgerFindingReported &&
		finding.ReportedSHA == "" {
		finding.ReportedSHA = headSHA
	}
	reason := ReviewLedgerFindingRechecked
	if !sameEvidence {
		reason = ReviewLedgerFindingEvidenceChanged
	}
	if finding.Status == ReviewLedgerFindingFixed {
		finding.FixedSHA = headSHA
		reason = ReviewLedgerFindingVerifiedFixed
		result.FixedFindingIDs = append(
			result.FixedFindingIDs,
			finding.ID,
		)
	}
	var provenancePresence *bool
	if (previous.Status == ReviewLedgerFindingFixed &&
		finding.Status != ReviewLedgerFindingFixed) ||
		(previous.Status == ReviewLedgerFindingRejected &&
			finding.Status != ReviewLedgerFindingRejected) {
		provenancePresence = observation.PresentInDeltaBase
	}
	appendReviewLedgerFindingTransition(
		finding,
		headSHA,
		&previous,
		reason,
		provenancePresence,
	)
	if finding.Status == ReviewLedgerFindingMissed ||
		(finding.Status == ReviewLedgerFindingReported && !sameEvidence) {
		result.NovelFindingIDs = append(
			result.NovelFindingIDs,
			finding.ID,
		)
	}
	return nil
}

func applyReviewLedgerCoverageObservation(
	ledger *ReviewLedger,
	observation ReviewLedgerCoverageObservation,
	planRequirements []ReviewCoverageRequirement,
) error {
	if err := validateReviewLedgerCoverageObservation(
		observation,
		planRequirements,
	); err != nil {
		return err
	}
	digest, err := reviewLedgerCoverageEvidenceDigest(observation)
	if err != nil {
		return err
	}
	requirementDigest, err := reviewLedgerCoverageRequirementDigestFor(
		observation.Requirement,
	)
	if err != nil {
		return err
	}
	requirementSetDigest, err :=
		reviewLedgerCoverageRequirementsDigestFor(planRequirements)
	if err != nil {
		return err
	}
	headSHA := ledger.HeadSHA
	index := sort.Search(len(ledger.Coverage), func(index int) bool {
		return ledger.Coverage[index].RequirementID >=
			observation.Requirement.ID
	})
	if index == len(ledger.Coverage) ||
		ledger.Coverage[index].RequirementID !=
			observation.Requirement.ID {
		coverage := ReviewLedgerCoverage{
			RequirementID:        observation.Requirement.ID,
			Kind:                 observation.Requirement.Kind,
			Description:          observation.Requirement.Description,
			Critical:             observation.Requirement.Critical,
			AccountableLane:      observation.Requirement.AccountableLane,
			ChangedTargets:       append([]ReviewCoverageTarget(nil), observation.Requirement.ChangedTargets...),
			RequirementDigest:    requirementDigest,
			RequirementSetDigest: requirementSetDigest,
			Status:               observation.Claim.Status,
			Evidence:             uniqueSortedReviewEvidence(observation.Claim.Evidence),
			EvidenceDigest:       digest,
			SourceSHA:            headSHA,
			LastObservedSHA:      headSHA,
			History:              []ReviewLedgerCoverageTransition{},
		}
		appendReviewLedgerCoverageTransition(
			&coverage,
			headSHA,
			nil,
			ReviewLedgerCoverageRecorded,
			requirementSetDigest,
		)
		ledger.Coverage = append(
			ledger.Coverage,
			ReviewLedgerCoverage{},
		)
		copy(ledger.Coverage[index+1:], ledger.Coverage[index:])
		ledger.Coverage[index] = coverage
		return nil
	}
	coverage := &ledger.Coverage[index]
	if coverage.Kind != observation.Requirement.Kind {
		return errors.New(
			"coverage observation changes the requirement kind",
		)
	}
	if coverage.Description != observation.Requirement.Description ||
		coverage.RequirementDigest != requirementDigest {
		return errors.New(
			"coverage observation changes the durable requirement binding",
		)
	}
	previous := *coverage
	coverage.Status = observation.Claim.Status
	coverage.Evidence = uniqueSortedReviewEvidence(
		observation.Claim.Evidence,
	)
	coverage.EvidenceDigest = digest
	coverage.RequirementSetDigest = requirementSetDigest
	coverage.SourceSHA = headSHA
	coverage.LastObservedSHA = headSHA
	coverage.NeedsRecheck = false
	// Refresh the diff-derived fields to this round's requirement, not
	// just the recording metadata above. Kind/Description/RequirementDigest
	// are this coverage entry's true, immutable identity (already checked
	// unchanged above) and never get touched here, but Critical/
	// AccountableLane/ChangedTargets are recomputed fresh from the current
	// diff/lane-selection every round for a stable-ID requirement (see
	// reviewCoverageRequirementIdentityEqual) and are expected to
	// legitimately differ round to round. Leaving them frozen at
	// whatever round first created this entry would mean every later
	// consumer of this coverage record's requirement snapshot (e.g.
	// reviewLedgerCoverageImpacted, or reviewLedgerRequirementInHeadBinding
	// itself on the *next* recheck) keeps evaluating against a
	// permanently stale diff instead of the one this round actually
	// rechecked against.
	coverage.Critical = observation.Requirement.Critical
	coverage.AccountableLane = observation.Requirement.AccountableLane
	coverage.ChangedTargets = append(
		[]ReviewCoverageTarget(nil),
		observation.Requirement.ChangedTargets...,
	)
	appendReviewLedgerCoverageTransition(
		coverage,
		headSHA,
		&previous,
		ReviewLedgerCoverageRechecked,
		requirementSetDigest,
	)
	return nil
}

// transitionReviewLedger deterministically applies one previous-head..new-head
// delta and exact-SHA observations. It never infers a fix from absence: changed
// paths mark prior knowledge for recheck, and only an explicit fixed
// observation with new verification evidence resolves a finding.
func transitionReviewLedger(
	previous *ReviewLedger,
	delta ReviewPlanInputs,
	planRequirements []ReviewCoverageRequirement,
	findingObservations []ReviewLedgerFindingObservation,
	coverageObservations []ReviewLedgerCoverageObservation,
) (ReviewLedgerDeltaResult, error) {
	if err := validateReviewPlanInputs(delta); err != nil {
		return ReviewLedgerDeltaResult{}, fmt.Errorf(
			"review ledger delta is invalid: %w",
			err,
		)
	}
	if planRequirements == nil {
		return ReviewLedgerDeltaResult{}, errors.New(
			"review ledger exact-SHA plan requirements are not initialized",
		)
	}
	if err := validateReviewCoverageRequirements(
		planRequirements,
	); err != nil {
		return ReviewLedgerDeltaResult{}, fmt.Errorf(
			"review ledger exact-SHA plan requirements are invalid: %w",
			err,
		)
	}
	hasPriorHead := previous != nil
	var ledger ReviewLedger
	if previous == nil {
		ledger = ReviewLedger{
			SchemaVersion:   reviewLedgerSchemaVersion,
			BaseSHA:         delta.BaseSHA,
			PreviousHeadSHA: delta.BaseSHA,
			HeadSHA:         delta.HeadSHA,
			HeadBindings:    []ReviewLedgerHeadBinding{},
			Findings:        []ReviewLedgerFinding{},
			Coverage:        []ReviewLedgerCoverage{},
		}
	} else {
		if err := validateReviewLedger(previous); err != nil {
			return ReviewLedgerDeltaResult{}, fmt.Errorf(
				"prior review ledger is invalid: %w",
				err,
			)
		}
		if previous.HeadSHA != delta.BaseSHA {
			return ReviewLedgerDeltaResult{}, fmt.Errorf(
				"review ledger delta base %s does not match prior head %s",
				abbreviateSHA(delta.BaseSHA),
				abbreviateSHA(previous.HeadSHA),
			)
		}
		ledger = *cloneReviewLedger(previous)
		ledger.PreviousHeadSHA = previous.HeadSHA
		ledger.HeadSHA = delta.HeadSHA
	}
	headBinding, err := buildReviewLedgerHeadBinding(
		previous,
		delta,
		planRequirements,
	)
	if err != nil {
		return ReviewLedgerDeltaResult{}, err
	}
	exactCoverageRequirements := headBinding.CoverageRequirements
	ledger.HeadBindings = append(
		ledger.HeadBindings,
		headBinding,
	)
	result := ReviewLedgerDeltaResult{Ledger: ledger}
	renamedFindings, err := reconcileReviewLedgerFindingRenames(
		&result.Ledger,
		delta,
	)
	if err != nil {
		return ReviewLedgerDeltaResult{}, fmt.Errorf(
			"review ledger rename reconciliation failed: %w",
			err,
		)
	}
	changed := reviewLedgerChangedPaths(delta)
	for index := range result.Ledger.Findings {
		finding := &result.Ledger.Findings[index]
		if finding.Status != ReviewLedgerFindingReported &&
			finding.Status != ReviewLedgerFindingMissed {
			continue
		}
		if !reviewLedgerFindingImpacted(*finding, changed) {
			continue
		}
		previous := *finding
		if renamed, ok := renamedFindings[finding.ID]; ok {
			previous = renamed
		}
		finding.NeedsRecheck = true
		appendReviewLedgerFindingTransition(
			finding,
			delta.HeadSHA,
			&previous,
			ReviewLedgerFindingImpacted,
			nil,
		)
	}
	for index := range result.Ledger.Coverage {
		coverage := &result.Ledger.Coverage[index]
		if !reviewLedgerCoverageImpacted(*coverage, changed) {
			continue
		}
		previous := *coverage
		coverage.NeedsRecheck = true
		appendReviewLedgerCoverageTransition(
			coverage,
			delta.HeadSHA,
			&previous,
			ReviewLedgerCoverageImpacted,
			"",
		)
	}

	findings := append(
		[]ReviewLedgerFindingObservation(nil),
		findingObservations...,
	)
	sort.Slice(findings, func(i, j int) bool {
		return findings[i].Finding.ID < findings[j].Finding.ID
	})
	for index, observation := range findings {
		if index > 0 &&
			observation.Finding.ID == findings[index-1].Finding.ID {
			return ReviewLedgerDeltaResult{}, fmt.Errorf(
				"review ledger finding observation %q is duplicated",
				observation.Finding.ID,
			)
		}
		if err := applyReviewLedgerFindingObservation(
			&result.Ledger,
			observation,
			hasPriorHead,
			delta,
			&result,
		); err != nil {
			return ReviewLedgerDeltaResult{}, fmt.Errorf(
				"review ledger finding observation %q is invalid: %w",
				observation.Finding.ID,
				err,
			)
		}
	}

	coverage := append(
		[]ReviewLedgerCoverageObservation(nil),
		coverageObservations...,
	)
	sort.Slice(coverage, func(i, j int) bool {
		return coverage[i].Requirement.ID <
			coverage[j].Requirement.ID
	})
	for index, observation := range coverage {
		if index > 0 &&
			observation.Requirement.ID ==
				coverage[index-1].Requirement.ID {
			return ReviewLedgerDeltaResult{}, fmt.Errorf(
				"review ledger coverage observation %q is duplicated",
				observation.Requirement.ID,
			)
		}
		if err := applyReviewLedgerCoverageObservation(
			&result.Ledger,
			observation,
			exactCoverageRequirements,
		); err != nil {
			return ReviewLedgerDeltaResult{}, fmt.Errorf(
				"review ledger coverage observation %q is invalid: %w",
				observation.Requirement.ID,
				err,
			)
		}
	}

	for _, finding := range result.Ledger.Findings {
		if finding.NeedsRecheck {
			result.RecheckFindingIDs = append(
				result.RecheckFindingIDs,
				finding.ID,
			)
		}
	}
	for _, coverage := range result.Ledger.Coverage {
		if coverage.NeedsRecheck {
			result.ImpactedCoverageIDs = append(
				result.ImpactedCoverageIDs,
				coverage.RequirementID,
			)
		}
	}
	result.RecheckFindingIDs =
		uniqueSortedStrings(result.RecheckFindingIDs)
	result.ImpactedCoverageIDs =
		uniqueSortedStrings(result.ImpactedCoverageIDs)
	result.NovelFindingIDs =
		uniqueSortedStrings(result.NovelFindingIDs)
	result.SuppressedFindingIDs =
		uniqueSortedStrings(result.SuppressedFindingIDs)
	result.FixedFindingIDs =
		uniqueSortedStrings(result.FixedFindingIDs)
	if err := validateReviewLedger(&result.Ledger); err != nil {
		return ReviewLedgerDeltaResult{}, fmt.Errorf(
			"transitioned review ledger is invalid: %w",
			err,
		)
	}
	return result, nil
}

func reviewLedgerFindingHistoryHasPrefix(
	history []ReviewLedgerFindingTransition,
	prefix []ReviewLedgerFindingTransition,
) bool {
	return len(history) >= len(prefix) &&
		reflect.DeepEqual(history[:len(prefix)], prefix)
}

func reviewLedgerCoverageHistoryHasPrefix(
	history []ReviewLedgerCoverageTransition,
	prefix []ReviewLedgerCoverageTransition,
) bool {
	return len(history) >= len(prefix) &&
		reflect.DeepEqual(history[:len(prefix)], prefix)
}

func reviewLedgerFindingIdentityTransitioned(
	finding ReviewLedgerFinding,
	priorHistoryLength int,
	fromID string,
) bool {
	if len(finding.History) <= priorHistoryLength {
		return false
	}
	transition := finding.History[priorHistoryLength]
	return transition.FromFindingID == fromID &&
		transition.ToFindingID == finding.ID &&
		(transition.Reason == ReviewLedgerFindingImpacted ||
			transition.Reason == ReviewLedgerFindingRenamed)
}

func reviewLedgerSuccessorFindingIndex(
	prior ReviewLedgerFinding,
	successor ReviewLedger,
) (int, error) {
	exactIndex := reviewLedgerFindingIndex(
		successor.Findings,
		prior.ID,
	)
	if exactIndex >= 0 {
		if !reviewLedgerFindingHistoryHasPrefix(
			successor.Findings[exactIndex].History,
			prior.History,
		) {
			return -1, fmt.Errorf(
				"review ledger successor rewrites finding %q transition history",
				prior.ID,
			)
		}
		return exactIndex, nil
	}

	matchIndex := -1
	for index, finding := range successor.Findings {
		if !reviewLedgerFindingHistoryHasPrefix(
			finding.History,
			prior.History,
		) || !reviewLedgerFindingIdentityTransitioned(
			finding,
			len(prior.History),
			prior.ID,
		) {
			continue
		}
		if matchIndex >= 0 {
			return -1, fmt.Errorf(
				"review ledger successor ambiguously replaces finding %q",
				prior.ID,
			)
		}
		matchIndex = index
	}
	if matchIndex < 0 {
		return -1, fmt.Errorf(
			"review ledger successor drops finding %q",
			prior.ID,
		)
	}
	return matchIndex, nil
}

func validateReviewLedgerSuccessorFinding(
	prior ReviewLedgerFinding,
	successor ReviewLedgerFinding,
	headBinding ReviewLedgerHeadBinding,
) error {
	if !reviewLedgerFindingHistoryHasPrefix(
		successor.History,
		prior.History,
	) {
		return fmt.Errorf(
			"review ledger successor rewrites finding %q transition history",
			prior.ID,
		)
	}
	if len(successor.History) == len(prior.History) {
		if !reflect.DeepEqual(successor, prior) {
			return fmt.Errorf(
				"review ledger successor rewrites finding %q without an audit transition",
				prior.ID,
			)
		}
		return nil
	}
	if successor.FirstSeenSHA != prior.FirstSeenSHA {
		return fmt.Errorf(
			"review ledger successor rewrites finding %q first-seen SHA",
			prior.ID,
		)
	}
	if prior.ReportedSHA != "" &&
		successor.ReportedSHA != prior.ReportedSHA {
		return fmt.Errorf(
			"review ledger successor rewrites finding %q reported SHA",
			prior.ID,
		)
	}
	if prior.FixedSHA != "" && successor.FixedSHA == "" {
		return fmt.Errorf(
			"review ledger successor drops finding %q fixed SHA",
			prior.ID,
		)
	}
	if successor.ID != prior.ID {
		if err := validateReviewLedgerSuccessorFindingRename(
			prior,
			successor,
			headBinding,
		); err != nil {
			return err
		}
	}
	for _, transition := range successor.History[len(prior.History):] {
		if transition.SourceSHA != headBinding.Delta.HeadSHA {
			return fmt.Errorf(
				"review ledger successor finding %q appends a transition for another head",
				successor.ID,
			)
		}
	}
	return nil
}

func validateReviewLedgerSuccessorFindingRename(
	prior ReviewLedgerFinding,
	successor ReviewLedgerFinding,
	headBinding ReviewLedgerHeadBinding,
) error {
	previousPath := prior.Finding.Location.Path
	currentPath := successor.Finding.Location.Path
	exactRename := false
	for _, file := range headBinding.Delta.ChangedFiles {
		if file.PreviousPath == previousPath &&
			file.Path == currentPath &&
			previousPath != currentPath {
			exactRename = true
			break
		}
	}
	if !exactRename {
		return fmt.Errorf(
			"review ledger successor replaces finding %q without an exact-diff rename",
			prior.ID,
		)
	}
	expectedID, err := reviewLedgerFindingIDAtPath(
		prior.Finding,
		currentPath,
	)
	if err != nil {
		return err
	}
	if successor.ID != expectedID {
		return fmt.Errorf(
			"review ledger successor replaces finding %q with a non-path identity",
			prior.ID,
		)
	}
	expectedDigest, err :=
		reviewLedgerFindingEvidenceDigestAfterRename(
			prior,
			previousPath,
			currentPath,
			headBinding.Delta.HeadSHA,
		)
	if err != nil {
		return err
	}
	expectedMaterialDigest, err :=
		reviewLedgerFindingMaterialEvidenceDigestAfterRename(
			prior,
			previousPath,
			currentPath,
		)
	if err != nil {
		return err
	}
	transition := successor.History[len(prior.History)]
	expectedReason := ReviewLedgerFindingImpacted
	if prior.Status == ReviewLedgerFindingRejected ||
		prior.Status == ReviewLedgerFindingFixed {
		expectedReason = ReviewLedgerFindingRenamed
	}
	if transition.FromFindingID != prior.ID ||
		transition.ToFindingID != successor.ID ||
		transition.Reason != expectedReason ||
		transition.EvidenceDigest != expectedDigest ||
		transition.MaterialEvidenceDigest !=
			expectedMaterialDigest {
		return fmt.Errorf(
			"review ledger successor finding %q is not a path-only exact-diff rename",
			successor.ID,
		)
	}
	return nil
}

func validateReviewLedgerSuccessorCoverage(
	prior ReviewLedgerCoverage,
	successor ReviewLedgerCoverage,
	headSHA string,
) error {
	if !reviewLedgerCoverageHistoryHasPrefix(
		successor.History,
		prior.History,
	) {
		return fmt.Errorf(
			"review ledger successor rewrites coverage %q transition history",
			prior.RequirementID,
		)
	}
	if len(successor.History) == len(prior.History) {
		if !reflect.DeepEqual(successor, prior) {
			return fmt.Errorf(
				"review ledger successor rewrites coverage %q without an audit transition",
				prior.RequirementID,
			)
		}
		return nil
	}
	if successor.Kind != prior.Kind {
		return fmt.Errorf(
			"review ledger successor changes coverage %q kind",
			prior.RequirementID,
		)
	}
	if successor.Description != prior.Description ||
		successor.RequirementDigest != prior.RequirementDigest {
		return fmt.Errorf(
			"review ledger successor changes coverage %q description",
			prior.RequirementID,
		)
	}
	for _, transition := range successor.History[len(prior.History):] {
		if transition.SourceSHA != headSHA {
			return fmt.Errorf(
				"review ledger successor coverage %q appends a transition for another head",
				successor.RequirementID,
			)
		}
	}
	return nil
}

func validateReviewLedgerSuccessor(
	prior ReviewLedger,
	successor ReviewLedger,
) error {
	if prior.BaseSHA != successor.BaseSHA {
		return errors.New(
			"review ledger successor changes the durable base SHA",
		)
	}
	if len(successor.HeadBindings) != len(prior.HeadBindings)+1 ||
		!reflect.DeepEqual(
			successor.HeadBindings[:len(prior.HeadBindings)],
			prior.HeadBindings,
		) {
		return errors.New(
			"review ledger successor does not append exactly one immutable head binding",
		)
	}
	if prior.HeadSHA != successor.PreviousHeadSHA {
		return fmt.Errorf(
			"review ledger update previous head %s does not match coder ledger head %s",
			abbreviateSHA(successor.PreviousHeadSHA),
			abbreviateSHA(prior.HeadSHA),
		)
	}

	matchedFindings := make(map[int]struct{}, len(prior.Findings))
	headBinding := successor.HeadBindings[len(successor.HeadBindings)-1]
	for _, priorFinding := range prior.Findings {
		index, err := reviewLedgerSuccessorFindingIndex(
			priorFinding,
			successor,
		)
		if err != nil {
			return err
		}
		if _, exists := matchedFindings[index]; exists {
			return errors.New(
				"review ledger successor combines distinct prior findings",
			)
		}
		if err := validateReviewLedgerSuccessorFinding(
			priorFinding,
			successor.Findings[index],
			headBinding,
		); err != nil {
			return err
		}
		matchedFindings[index] = struct{}{}
	}
	for index, finding := range successor.Findings {
		if _, matched := matchedFindings[index]; matched {
			continue
		}
		if finding.FirstSeenSHA != successor.HeadSHA ||
			finding.SourceSHA != successor.HeadSHA ||
			finding.History[0].SourceSHA != successor.HeadSHA {
			return fmt.Errorf(
				"review ledger successor imports finding %q without prior history",
				finding.ID,
			)
		}
	}

	priorCoverageByID := make(
		map[string]ReviewLedgerCoverage,
		len(prior.Coverage),
	)
	for _, coverage := range prior.Coverage {
		priorCoverageByID[coverage.RequirementID] = coverage
	}
	for _, priorCoverage := range prior.Coverage {
		index := sort.Search(
			len(successor.Coverage),
			func(index int) bool {
				return successor.Coverage[index].RequirementID >=
					priorCoverage.RequirementID
			},
		)
		if index == len(successor.Coverage) ||
			successor.Coverage[index].RequirementID !=
				priorCoverage.RequirementID {
			return fmt.Errorf(
				"review ledger successor drops coverage %q",
				priorCoverage.RequirementID,
			)
		}
		if err := validateReviewLedgerSuccessorCoverage(
			priorCoverage,
			successor.Coverage[index],
			successor.HeadSHA,
		); err != nil {
			return err
		}
	}
	for _, coverage := range successor.Coverage {
		if _, existed := priorCoverageByID[coverage.RequirementID]; existed {
			continue
		}
		if coverage.SourceSHA != successor.HeadSHA ||
			coverage.LastObservedSHA != successor.HeadSHA ||
			coverage.History[0].SourceSHA != successor.HeadSHA {
			return fmt.Errorf(
				"review ledger successor imports coverage %q without prior history",
				coverage.RequirementID,
			)
		}
	}
	return nil
}

func (m *AgentManager) SetReviewLedger(
	agentID string,
	ledger ReviewLedger,
	snapshot ReviewLedgerTransitionSnapshot,
) error {
	if m == nil {
		return errors.New("agent manager is not configured")
	}
	if err := validateReviewLedger(&ledger); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	agent, ok := m.agents[strings.TrimSpace(agentID)]
	if !ok {
		return fmt.Errorf(
			"coding agent %q was not found",
			strings.TrimSpace(agentID),
		)
	}
	if agent.Role != RoleCoder {
		return fmt.Errorf("agent %q is not a coding agent", agent.ID)
	}
	if agent.ReviewLedger != nil &&
		agent.ReviewLedger.HeadSHA == ledger.HeadSHA {
		if !reflect.DeepEqual(*agent.ReviewLedger, ledger) {
			return fmt.Errorf(
				"review ledger for head %s is already recorded",
				abbreviateSHA(ledger.HeadSHA),
			)
		}
		latest := ledger.HeadBindings[len(ledger.HeadBindings)-1]
		if snapshot.Plan.BaseSHA != snapshot.Inputs.BaseSHA ||
			snapshot.Plan.HeadSHA != snapshot.Inputs.HeadSHA ||
			!reflect.DeepEqual(latest.Delta, snapshot.Inputs) {
			return errors.New(
				"review ledger idempotent update does not match the manager-owned exact delta and plan",
			)
		}
		return nil
	}
	if agent.ReviewLedger == nil && len(ledger.HeadBindings) != 1 {
		return errors.New(
			"review ledger initial update imports unowned prior head bindings",
		)
	}
	expectedBinding, err := reviewLedgerHeadBindingFromSnapshot(
		agent.ReviewLedger,
		snapshot,
	)
	if err != nil {
		return err
	}
	latestBinding := ledger.HeadBindings[len(ledger.HeadBindings)-1]
	if !reflect.DeepEqual(latestBinding, expectedBinding) {
		return errors.New(
			"review ledger successor head binding does not match the manager-owned exact delta and plan",
		)
	}
	if agent.ReviewLedger != nil {
		if err := validateReviewLedgerSuccessor(
			*agent.ReviewLedger,
			ledger,
		); err != nil {
			return err
		}
	}
	agent.ReviewLedger = cloneReviewLedger(&ledger)
	agent.LastActivityTime = time.Now().UTC()
	return nil
}

func (b *Orchestrator) persistReviewLedger(
	agentID string,
	ledger ReviewLedger,
	snapshot ReviewLedgerTransitionSnapshot,
) error {
	if b == nil || b.agents == nil {
		return errors.New(
			"orchestrator agent manager is not configured",
		)
	}
	if err := b.agents.SetReviewLedger(
		agentID,
		ledger,
		snapshot,
	); err != nil {
		return err
	}
	if err := b.persistAgentState(); err != nil {
		return fmt.Errorf(
			"failed to persist review ledger for %s: %w",
			agentID,
			err,
		)
	}
	return nil
}
