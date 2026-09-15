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
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	testLedgerBaseSHA   = "1111111111111111111111111111111111111111"
	testLedgerFirstSHA  = "2222222222222222222222222222222222222222"
	testLedgerSecondSHA = "3333333333333333333333333333333333333333"
	testLedgerThirdSHA  = "4444444444444444444444444444444444444444"
	testLedgerFourthSHA = "5555555555555555555555555555555555555555"
)

func reviewLedgerBool(value bool) *bool {
	return &value
}

func testReviewLedgerFinding(
	t *testing.T,
	headSHA string,
	path string,
	symbol string,
	invariant string,
) ReviewCanonicalFinding {
	t.Helper()
	candidate := ReviewFindingCandidate{
		CandidateID:       "candidate-" + strings.ReplaceAll(symbol, " ", "-"),
		Summary:           "Finding for " + symbol,
		Location:          ReviewFindingLocation{Path: path, Symbol: symbol},
		BehavioralPath:    "entry -> " + symbol + " -> failure",
		ViolatedInvariant: invariant,
		Severity:          ReviewFindingSeverityHigh,
		Confidence:        ReviewFindingConfidenceHigh,
		Evidence: []ReviewEvidence{{
			Summary: "discovery evidence for " + symbol,
			Path:    path,
		}},
	}
	fingerprint, err := reviewFindingFingerprint(candidate)
	if err != nil {
		t.Fatalf("reviewFindingFingerprint() error = %v", err)
	}
	return ReviewCanonicalFinding{
		ID:                "finding-" + fingerprint,
		Fingerprint:       fingerprint,
		ExactSHA:          headSHA,
		Summary:           candidate.Summary,
		Location:          candidate.Location,
		BehavioralPath:    candidate.BehavioralPath,
		ViolatedInvariant: candidate.ViolatedInvariant,
		Severity:          candidate.Severity,
		Confidence:        candidate.Confidence,
		Evidence:          candidate.Evidence,
		Provenance: []ReviewFindingProvenance{{
			WorkerID:    "discovery-worker",
			Lane:        "contract",
			Pass:        1,
			CandidateID: candidate.CandidateID,
			Severity:    candidate.Severity,
			Confidence:  candidate.Confidence,
		}},
	}
}

func testReviewLedgerFindingObservation(
	finding ReviewCanonicalFinding,
	status ReviewLedgerFindingStatus,
	verificationSummary string,
	presentInDeltaBase *bool,
) ReviewLedgerFindingObservation {
	candidateRevision, _ := reviewFindingCandidateRevision(finding)
	outcome, _ := reviewLedgerVerificationOutcomeForStatus(status)
	return ReviewLedgerFindingObservation{
		Finding: finding,
		Status:  status,
		VerificationEvidence: []ReviewEvidence{{
			Summary: verificationSummary,
			Path:    finding.Location.Path,
		}},
		VerificationReceipt: ReviewLedgerVerificationReceipt{
			FindingID:         finding.ID,
			ExactSHA:          finding.ExactSHA,
			AssignmentID:      "verification:" + finding.ID + ":001",
			CandidateRevision: candidateRevision,
			VerifierWorkerID:  "verifier-" + finding.ExactSHA,
			Outcome:           outcome,
		},
		PresentInDeltaBase: presentInDeltaBase,
	}
}

func testReviewLedgerCoverageObservation(
	path string,
	status ReviewCoverageStatus,
	summary string,
) ReviewLedgerCoverageObservation {
	requirement := ReviewCoverageRequirement{
		ID:          "call-path:" + path,
		Kind:        ReviewCoverageCallPath,
		Description: path,
	}
	return ReviewLedgerCoverageObservation{
		Requirement: requirement,
		Claim: ReviewCoverageClaim{
			RequirementID: requirement.ID,
			Kind:          requirement.Kind,
			Status:        status,
			Evidence: []ReviewEvidence{{
				Summary: summary,
				Path:    path,
			}},
		},
	}
}

func testReviewLedgerCoverageRequirements(
	paths ...string,
) []ReviewCoverageRequirement {
	requirements := make([]ReviewCoverageRequirement, 0, len(paths))
	for _, path := range paths {
		requirements = append(requirements, ReviewCoverageRequirement{
			ID:          "call-path:" + path,
			Kind:        ReviewCoverageCallPath,
			Description: path,
		})
	}
	sort.Slice(requirements, func(i, j int) bool {
		if requirements[i].Kind != requirements[j].Kind {
			return requirements[i].Kind < requirements[j].Kind
		}
		return requirements[i].ID < requirements[j].ID
	})
	return requirements
}

func TestTransitionReviewLedgerPersistsNotApplicableCoverage(t *testing.T) {
	const path = "internal/example_test.go"
	requirements := testReviewLedgerCoverageRequirements(path)
	result, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(testLedgerBaseSHA, testLedgerFirstSHA, path),
		requirements,
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				path,
				ReviewCoverageNotApplicable,
				"test-fixture error handling does not change product behavior",
			),
		},
	)
	if err != nil {
		t.Fatalf("transitionReviewLedger() error = %v", err)
	}
	coverage := coverageByLedgerID(t, result.Ledger, "call-path:"+path)
	if coverage.Status != ReviewCoverageNotApplicable {
		t.Fatalf("persisted coverage status = %q, want not_applicable", coverage.Status)
	}
	if err := validateReviewLedger(&result.Ledger); err != nil {
		t.Fatalf("validateReviewLedger() error = %v", err)
	}
}

func testReviewLedgerDelta(
	baseSHA string,
	headSHA string,
	paths ...string,
) ReviewPlanInputs {
	files := make([]ReviewPlanChangedFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, ReviewPlanChangedFile{
			Path:      path,
			Additions: 1,
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return ReviewPlanInputs{
		SchemaVersion:      reviewPlanInputsSchemaVersion,
		BaseSHA:            baseSHA,
		HeadSHA:            headSHA,
		ChangedFileCount:   len(files),
		ChangedLineCount:   int64(len(files)),
		ChangedFiles:       files,
		AcceptanceCriteria: []string{},
	}
}

func testReviewLedgerRenameDelta(
	baseSHA string,
	headSHA string,
	previousPath string,
	path string,
) ReviewPlanInputs {
	delta := testReviewLedgerDelta(baseSHA, headSHA, path)
	delta.ChangedFiles[0].PreviousPath = previousPath
	delta.ChangedFiles[0].Deletions = 1
	delta.ChangedLineCount++
	return delta
}

func testReviewLedgerTransitionSnapshotFromLedger(
	ledger ReviewLedger,
) ReviewLedgerTransitionSnapshot {
	binding := ledger.HeadBindings[len(ledger.HeadBindings)-1]
	return ReviewLedgerTransitionSnapshot{
		Inputs: cloneReviewPlanInputs(binding.Delta),
		Plan: ReviewPlan{
			BaseSHA: binding.Delta.BaseSHA,
			HeadSHA: binding.Delta.HeadSHA,
			CoverageRequirements: append(
				[]ReviewCoverageRequirement{},
				binding.CoverageRequirements...,
			),
		},
	}
}

func findingByLedgerID(
	t *testing.T,
	ledger ReviewLedger,
	id string,
) ReviewLedgerFinding {
	t.Helper()
	for _, finding := range ledger.Findings {
		if finding.ID == id {
			return finding
		}
	}
	t.Fatalf("review ledger finding %q was not found", id)
	return ReviewLedgerFinding{}
}

func coverageByLedgerID(
	t *testing.T,
	ledger ReviewLedger,
	id string,
) ReviewLedgerCoverage {
	t.Helper()
	for _, coverage := range ledger.Coverage {
		if coverage.RequirementID == id {
			return coverage
		}
	}
	t.Fatalf("review ledger coverage %q was not found", id)
	return ReviewLedgerCoverage{}
}

func loadPersistedReviewLedgerForTest(
	t *testing.T,
	ledger *ReviewLedger,
) error {
	t.Helper()
	logDir := t.TempDir()
	restored := &Orchestrator{
		cfg:    Config{LogDir: logDir},
		agents: NewAgentManager(),
	}
	payload := persistedStateFile{
		Version: persistedStateVersion,
		Agents: []persistedAgent{{
			ID:           "coding-agent-persisted-ledger-test",
			Role:         RoleCoder,
			State:        StateWorking,
			ReviewLedger: ledger,
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := os.MkdirAll(
		orchestratorStateDir(logDir),
		0o755,
	); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(
		restored.agentStateFilePath(),
		body,
		0o600,
	); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return restored.loadPersistedAgentState()
}

func TestTransitionReviewLedgerClassifiesInitialFindingProvenance(t *testing.T) {
	preExisting := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/preexisting.go",
		"PreExisting",
		"pre-existing state remains valid",
	)
	originalPR := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/original.go",
		"OriginalPR",
		"new behavior remains valid",
	)
	result, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/original.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/original.go",
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				originalPR,
				ReviewLedgerFindingMissed,
				"independently confirmed original PR behavior",
				reviewLedgerBool(false),
			),
			testReviewLedgerFindingObservation(
				preExisting,
				ReviewLedgerFindingReported,
				"independently confirmed pre-existing behavior",
				reviewLedgerBool(true),
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				"internal/original.go",
				ReviewCoverageCovered,
				"inspected original PR path",
			),
		},
	)
	if err != nil {
		t.Fatalf("transitionReviewLedger() error = %v", err)
	}
	if result.Ledger.BaseSHA != testLedgerBaseSHA ||
		result.Ledger.PreviousHeadSHA != testLedgerBaseSHA ||
		result.Ledger.HeadSHA != testLedgerFirstSHA {
		t.Fatalf("ledger SHA lineage = %#v", result.Ledger)
	}
	if got := findingByLedgerID(
		t,
		result.Ledger,
		preExisting.ID,
	).Provenance; got != ReviewLedgerFindingPreExisting {
		t.Fatalf("pre-existing provenance = %q", got)
	}
	if got := findingByLedgerID(
		t,
		result.Ledger,
		originalPR.ID,
	).Provenance; got != ReviewLedgerFindingOriginalPR {
		t.Fatalf("original PR provenance = %q", got)
	}
	wantNovel := []string{originalPR.ID, preExisting.ID}
	sort.Strings(wantNovel)
	if !reflect.DeepEqual(result.NovelFindingIDs, wantNovel) {
		t.Fatalf(
			"novel finding IDs = %#v, want both deterministic IDs",
			result.NovelFindingIDs,
		)
	}
	if err := validateReviewLedger(&result.Ledger); err != nil {
		t.Fatalf("validateReviewLedger() error = %v", err)
	}
}

func TestTransitionReviewLedgerPreservesUnaffectedAndRequiresVerifiedFix(
	t *testing.T,
) {
	impacted := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/impacted.go",
		"Impacted",
		"impacted state remains valid",
	)
	unaffected := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/unaffected.go",
		"Unaffected",
		"unaffected state remains valid",
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/impacted.go",
			"internal/unaffected.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/impacted.go",
			"internal/unaffected.go",
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				impacted,
				ReviewLedgerFindingReported,
				"independently confirmed impacted issue",
				reviewLedgerBool(false),
			),
			testReviewLedgerFindingObservation(
				unaffected,
				ReviewLedgerFindingReported,
				"independently confirmed unaffected issue",
				reviewLedgerBool(false),
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				"internal/impacted.go",
				ReviewCoverageCovered,
				"inspected impacted path",
			),
			testReviewLedgerCoverageObservation(
				"internal/unaffected.go",
				ReviewCoverageCovered,
				"inspected unaffected path",
			),
		},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	unaffectedBefore := findingByLedgerID(
		t,
		first.Ledger,
		unaffected.ID,
	)
	unaffectedCoverageBefore := coverageByLedgerID(
		t,
		first.Ledger,
		"call-path:internal/unaffected.go",
	)

	secondWithoutResolution, err := transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/impacted.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/impacted.go",
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("delta-only transitionReviewLedger() error = %v", err)
	}
	impactedPending := findingByLedgerID(
		t,
		secondWithoutResolution.Ledger,
		impacted.ID,
	)
	if impactedPending.Status != ReviewLedgerFindingReported ||
		!impactedPending.NeedsRecheck ||
		impactedPending.FixedSHA != "" {
		t.Fatalf(
			"impacted finding without verifier resolution = %#v",
			impactedPending,
		)
	}
	if !reflect.DeepEqual(
		secondWithoutResolution.RecheckFindingIDs,
		[]string{impacted.ID},
	) {
		t.Fatalf(
			"recheck finding IDs = %#v",
			secondWithoutResolution.RecheckFindingIDs,
		)
	}
	if !reflect.DeepEqual(
		secondWithoutResolution.ImpactedCoverageIDs,
		[]string{"call-path:internal/impacted.go"},
	) {
		t.Fatalf(
			"impacted coverage IDs = %#v",
			secondWithoutResolution.ImpactedCoverageIDs,
		)
	}
	if got := findingByLedgerID(
		t,
		secondWithoutResolution.Ledger,
		unaffected.ID,
	); !reflect.DeepEqual(got, unaffectedBefore) {
		t.Fatalf("unaffected finding changed:\n got %#v\nwant %#v", got, unaffectedBefore)
	}
	if got := coverageByLedgerID(
		t,
		secondWithoutResolution.Ledger,
		"call-path:internal/unaffected.go",
	); !reflect.DeepEqual(got, unaffectedCoverageBefore) {
		t.Fatalf("unaffected coverage changed:\n got %#v\nwant %#v", got, unaffectedCoverageBefore)
	}

	thirdWithUnrelatedDelta, err := transitionReviewLedger(
		&secondWithoutResolution.Ledger,
		testReviewLedgerDelta(
			testLedgerSecondSHA,
			testLedgerThirdSHA,
			"internal/unrelated.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/unrelated.go",
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("later unrelated transitionReviewLedger() error = %v", err)
	}
	if !reflect.DeepEqual(
		thirdWithUnrelatedDelta.RecheckFindingIDs,
		[]string{impacted.ID},
	) {
		t.Fatalf(
			"carried recheck finding IDs = %#v",
			thirdWithUnrelatedDelta.RecheckFindingIDs,
		)
	}
	if !reflect.DeepEqual(
		thirdWithUnrelatedDelta.ImpactedCoverageIDs,
		[]string{"call-path:internal/impacted.go"},
	) {
		t.Fatalf(
			"carried impacted coverage IDs = %#v",
			thirdWithUnrelatedDelta.ImpactedCoverageIDs,
		)
	}

	impactedAtSecond := cloneReviewCanonicalFinding(impacted)
	impactedAtSecond.ExactSHA = testLedgerSecondSHA
	fixed, err := transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/impacted.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/impacted.go",
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				impactedAtSecond,
				ReviewLedgerFindingFixed,
				"exact-SHA regression passes and the faulty behavior is absent",
				nil,
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				"internal/impacted.go",
				ReviewCoverageCovered,
				"rechecked the fixed path at the new head",
			),
		},
	)
	if err != nil {
		t.Fatalf("verified-fix transitionReviewLedger() error = %v", err)
	}
	resolved := findingByLedgerID(t, fixed.Ledger, impacted.ID)
	if resolved.Status != ReviewLedgerFindingFixed ||
		resolved.FixedSHA != testLedgerSecondSHA ||
		resolved.NeedsRecheck {
		t.Fatalf("verified fixed finding = %#v", resolved)
	}
	if !reflect.DeepEqual(fixed.FixedFindingIDs, []string{impacted.ID}) {
		t.Fatalf("fixed finding IDs = %#v", fixed.FixedFindingIDs)
	}
	if len(fixed.RecheckFindingIDs) != 0 {
		t.Fatalf(
			"resolved finding remained actionable: %#v",
			fixed.RecheckFindingIDs,
		)
	}
	if len(fixed.ImpactedCoverageIDs) != 0 {
		t.Fatalf(
			"rechecked coverage remained actionable: %#v",
			fixed.ImpactedCoverageIDs,
		)
	}
	recheckedCoverage := coverageByLedgerID(
		t,
		fixed.Ledger,
		"call-path:internal/impacted.go",
	)
	if recheckedCoverage.NeedsRecheck ||
		recheckedCoverage.SourceSHA != testLedgerSecondSHA {
		t.Fatalf("rechecked coverage = %#v", recheckedCoverage)
	}
}

func TestTransitionReviewLedgerCarriesReportedFindingAcrossRename(
	t *testing.T,
) {
	const (
		previousPath = "internal/old.go"
		currentPath  = "internal/new.go"
	)
	priorFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		previousPath,
		"Renamed",
		"renamed behavior remains valid",
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				priorFinding,
				ReviewLedgerFindingReported,
				"independently confirmed renamed issue",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	renamedFinding := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		currentPath,
		"Renamed",
		"renamed behavior remains valid",
	)
	successor, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerRenameDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			previousPath,
			currentPath,
		),
		testReviewLedgerCoverageRequirements(
			currentPath,
			previousPath,
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				renamedFinding,
				ReviewLedgerFindingReported,
				"independently confirmed renamed issue",
				nil,
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("rename transitionReviewLedger() error = %v", err)
	}
	if len(successor.Ledger.Findings) != 1 {
		t.Fatalf(
			"renamed ledger findings = %d, want one carried identity",
			len(successor.Ledger.Findings),
		)
	}
	carried := successor.Ledger.Findings[0]
	if carried.ID != renamedFinding.ID ||
		carried.Finding.Location.Path != currentPath ||
		carried.Status != ReviewLedgerFindingReported ||
		carried.Provenance != ReviewLedgerFindingPreviouslyReported ||
		carried.FirstSeenSHA != testLedgerFirstSHA ||
		carried.ReportedSHA != testLedgerFirstSHA ||
		carried.NeedsRecheck {
		t.Fatalf("carried renamed finding = %#v", carried)
	}
	if !reviewLedgerFindingHistoryHasPrefix(
		carried.History,
		prior.Ledger.Findings[0].History,
	) {
		t.Fatal("renamed finding did not preserve its audit history")
	}
	identityTransition := carried.History[len(prior.Ledger.Findings[0].History)]
	if identityTransition.FromFindingID != priorFinding.ID ||
		identityTransition.ToFindingID != renamedFinding.ID {
		t.Fatalf(
			"rename identity audit transition = %#v",
			identityTransition,
		)
	}
	if len(successor.NovelFindingIDs) != 0 ||
		len(successor.RecheckFindingIDs) != 0 {
		t.Fatalf(
			"renamed novel=%#v recheck=%#v, want neither",
			successor.NovelFindingIDs,
			successor.RecheckFindingIDs,
		)
	}
	if !reflect.DeepEqual(
		successor.SuppressedFindingIDs,
		[]string{renamedFinding.ID},
	) {
		t.Fatalf(
			"renamed suppressed finding IDs = %#v",
			successor.SuppressedFindingIDs,
		)
	}

	agents := NewAgentManager()
	if err := agents.Add(&Agent{
		ID:   "coding-agent-renamed-ledger",
		Role: RoleCoder,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agents.SetReviewLedger(
		"coding-agent-renamed-ledger",
		prior.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(prior) error = %v", err)
	}
	if err := agents.SetReviewLedger(
		"coding-agent-renamed-ledger",
		successor.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(successor.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(rename successor) error = %v", err)
	}
}

func TestTransitionReviewLedgerPersistsUnobservedFindingRename(
	t *testing.T,
) {
	const (
		previousPath = "internal/old.go"
		currentPath  = "internal/new.go"
		unrelated    = "internal/unrelated.go"
	)
	priorFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		previousPath,
		"Renamed later",
		"renamed behavior remains valid across later heads",
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				priorFinding,
				ReviewLedgerFindingReported,
				"independently confirmed issue before rename",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	renamed, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerRenameDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			previousPath,
			currentPath,
		),
		testReviewLedgerCoverageRequirements(
			currentPath,
			previousPath,
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("unobserved rename transitionReviewLedger() error = %v", err)
	}
	currentFinding := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		currentPath,
		"Renamed later",
		"renamed behavior remains valid across later heads",
	)
	if len(renamed.Ledger.Findings) != 1 ||
		renamed.Ledger.Findings[0].ID != currentFinding.ID ||
		!renamed.Ledger.Findings[0].NeedsRecheck {
		t.Fatalf(
			"unobserved renamed ledger findings = %#v",
			renamed.Ledger.Findings,
		)
	}
	if !reflect.DeepEqual(
		renamed.RecheckFindingIDs,
		[]string{currentFinding.ID},
	) {
		t.Fatalf(
			"unobserved rename recheck IDs = %#v",
			renamed.RecheckFindingIDs,
		)
	}
	identityTransition :=
		renamed.Ledger.Findings[0].History[len(prior.Ledger.Findings[0].History)]
	if identityTransition.FromFindingID != priorFinding.ID ||
		identityTransition.ToFindingID != currentFinding.ID {
		t.Fatalf(
			"unobserved rename identity transition = %#v",
			identityTransition,
		)
	}

	observedAtThird := testReviewLedgerFinding(
		t,
		testLedgerThirdSHA,
		currentPath,
		"Renamed later",
		"renamed behavior remains valid across later heads",
	)
	resolved, err := transitionReviewLedger(
		&renamed.Ledger,
		testReviewLedgerDelta(
			testLedgerSecondSHA,
			testLedgerThirdSHA,
			unrelated,
		),
		testReviewLedgerCoverageRequirements(unrelated),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				observedAtThird,
				ReviewLedgerFindingReported,
				"independently confirmed issue before rename",
				nil,
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("later rename recheck transitionReviewLedger() error = %v", err)
	}
	if len(resolved.Ledger.Findings) != 1 {
		t.Fatalf(
			"later recheck findings = %d, want one carried identity",
			len(resolved.Ledger.Findings),
		)
	}
	carried := resolved.Ledger.Findings[0]
	if carried.ID != observedAtThird.ID ||
		carried.Finding.Location.Path != currentPath ||
		carried.NeedsRecheck {
		t.Fatalf("later rechecked renamed finding = %#v", carried)
	}
	if len(resolved.NovelFindingIDs) != 0 ||
		len(resolved.RecheckFindingIDs) != 0 {
		t.Fatalf(
			"later renamed novel=%#v recheck=%#v, want neither",
			resolved.NovelFindingIDs,
			resolved.RecheckFindingIDs,
		)
	}
	if !reflect.DeepEqual(
		resolved.SuppressedFindingIDs,
		[]string{observedAtThird.ID},
	) {
		t.Fatalf(
			"later renamed suppressed IDs = %#v",
			resolved.SuppressedFindingIDs,
		)
	}
}

func TestTransitionReviewLedgerPersistsRejectedFindingRename(
	t *testing.T,
) {
	const (
		previousPath = "internal/rejected-old.go"
		currentPath  = "internal/rejected-new.go"
		unrelated    = "internal/unrelated.go"
	)
	priorFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		previousPath,
		"Rejected rename",
		"rejected renamed behavior remains suppressed",
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				priorFinding,
				ReviewLedgerFindingRejected,
				"independent verifier rejected renamed candidate",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	renamed, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerRenameDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			previousPath,
			currentPath,
		),
		testReviewLedgerCoverageRequirements(
			currentPath,
			previousPath,
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("rejected rename transitionReviewLedger() error = %v", err)
	}
	currentFinding := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		currentPath,
		"Rejected rename",
		"rejected renamed behavior remains suppressed",
	)
	if len(renamed.Ledger.Findings) != 1 {
		t.Fatalf(
			"rejected renamed ledger findings = %d, want one",
			len(renamed.Ledger.Findings),
		)
	}
	carried := renamed.Ledger.Findings[0]
	if carried.ID != currentFinding.ID ||
		carried.Status != ReviewLedgerFindingRejected ||
		carried.NeedsRecheck {
		t.Fatalf("rejected renamed finding = %#v", carried)
	}
	renameTransition := carried.History[len(carried.History)-1]
	if renameTransition.Reason != ReviewLedgerFindingRenamed ||
		renameTransition.FromFindingID != priorFinding.ID ||
		renameTransition.ToFindingID != currentFinding.ID {
		t.Fatalf(
			"rejected rename audit transition = %#v",
			renameTransition,
		)
	}
	if len(renamed.NovelFindingIDs) != 0 ||
		len(renamed.RecheckFindingIDs) != 0 {
		t.Fatalf(
			"rejected rename novel=%#v recheck=%#v, want neither",
			renamed.NovelFindingIDs,
			renamed.RecheckFindingIDs,
		)
	}

	observedAtThird := testReviewLedgerFinding(
		t,
		testLedgerThirdSHA,
		currentPath,
		"Rejected rename",
		"rejected renamed behavior remains suppressed",
	)
	suppressed, err := transitionReviewLedger(
		&renamed.Ledger,
		testReviewLedgerDelta(
			testLedgerSecondSHA,
			testLedgerThirdSHA,
			unrelated,
		),
		testReviewLedgerCoverageRequirements(unrelated),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				observedAtThird,
				ReviewLedgerFindingReported,
				"independent verifier rejected renamed candidate",
				nil,
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("later rejected transitionReviewLedger() error = %v", err)
	}
	if len(suppressed.Ledger.Findings) != 1 {
		t.Fatalf(
			"later rejected ledger findings = %d, want one",
			len(suppressed.Ledger.Findings),
		)
	}
	rejected := suppressed.Ledger.Findings[0]
	if rejected.ID != observedAtThird.ID ||
		rejected.Status != ReviewLedgerFindingRejected ||
		rejected.NeedsRecheck {
		t.Fatalf("later suppressed rejected finding = %#v", rejected)
	}
	if len(suppressed.NovelFindingIDs) != 0 ||
		!reflect.DeepEqual(
			suppressed.SuppressedFindingIDs,
			[]string{observedAtThird.ID},
		) {
		t.Fatalf(
			"later rejected novel=%#v suppressed=%#v",
			suppressed.NovelFindingIDs,
			suppressed.SuppressedFindingIDs,
		)
	}
}

func TestTransitionReviewLedgerReopensFixedFindingOnRenameHead(
	t *testing.T,
) {
	const (
		previousPath = "internal/fixed-observed-old.go"
		currentPath  = "internal/fixed-observed-new.go"
	)
	priorFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		previousPath,
		"Observed fixed rename",
		"fixed rename regressions retain one identity",
	)
	recorded, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				priorFinding,
				ReviewLedgerFindingReported,
				"independently confirmed issue before the fix",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	fixedAtSecond := cloneReviewCanonicalFinding(priorFinding)
	fixedAtSecond.ExactSHA = testLedgerSecondSHA
	fixed, err := transitionReviewLedger(
		&recorded.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				fixedAtSecond,
				ReviewLedgerFindingFixed,
				"fresh verifier confirms the reproduction no longer fails",
				nil,
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("fixed transitionReviewLedger() error = %v", err)
	}

	regression := testReviewLedgerFinding(
		t,
		testLedgerThirdSHA,
		currentPath,
		"Observed fixed rename",
		"fixed rename regressions retain one identity",
	)
	reopened, err := transitionReviewLedger(
		&fixed.Ledger,
		testReviewLedgerRenameDelta(
			testLedgerSecondSHA,
			testLedgerThirdSHA,
			previousPath,
			currentPath,
		),
		testReviewLedgerCoverageRequirements(
			currentPath,
			previousPath,
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				regression,
				ReviewLedgerFindingMissed,
				"new verifier reproduces the regression after the rename",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf(
			"observed rename transitionReviewLedger() error = %v",
			err,
		)
	}
	if len(reopened.Ledger.Findings) != 1 {
		t.Fatalf(
			"observed fixed rename findings = %d, want one",
			len(reopened.Ledger.Findings),
		)
	}
	carried := reopened.Ledger.Findings[0]
	if carried.ID != regression.ID ||
		carried.Status != ReviewLedgerFindingMissed ||
		carried.Provenance != ReviewLedgerFindingFixIntroduced ||
		carried.FirstSeenSHA != testLedgerFirstSHA ||
		carried.FixedSHA != testLedgerSecondSHA ||
		carried.NeedsRecheck {
		t.Fatalf("observed fixed rename finding = %#v", carried)
	}
	if !reviewLedgerFindingHistoryHasPrefix(
		carried.History,
		fixed.Ledger.Findings[0].History,
	) {
		t.Fatal("observed fixed rename rewrote prior audit history")
	}
	renameIndex := len(fixed.Ledger.Findings[0].History)
	if len(carried.History) != renameIndex+2 {
		t.Fatalf(
			"observed fixed rename history = %#v",
			carried.History,
		)
	}
	renamed := carried.History[renameIndex]
	observed := carried.History[renameIndex+1]
	if renamed.Reason != ReviewLedgerFindingRenamed ||
		renamed.FromFindingID != priorFinding.ID ||
		renamed.ToFindingID != regression.ID ||
		observed.FromFindingID != regression.ID ||
		observed.ToFindingID != regression.ID ||
		observed.PresentInDeltaBase == nil ||
		*observed.PresentInDeltaBase {
		t.Fatalf(
			"observed fixed rename audit transitions = %#v",
			carried.History[renameIndex:],
		)
	}

	agents := NewAgentManager()
	const agentID = "coding-agent-observed-fixed-rename-ledger"
	if err := agents.Add(&Agent{
		ID:   agentID,
		Role: RoleCoder,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		recorded.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(recorded.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(recorded) error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		fixed.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(fixed.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(fixed) error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		reopened.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(reopened.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(observed rename) error = %v", err)
	}
}

func TestTransitionReviewLedgerReopensUnobservedFixedRenameOnLaterHead(
	t *testing.T,
) {
	const (
		previousPath = "internal/fixed-unobserved-old.go"
		currentPath  = "internal/fixed-unobserved-new.go"
		unrelated    = "internal/fixed-unobserved-unrelated.go"
	)
	priorFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		previousPath,
		"Unobserved fixed rename",
		"unobserved fixed renames retain one identity",
	)
	recorded, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				priorFinding,
				ReviewLedgerFindingReported,
				"independently confirmed issue before the fix",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	fixedAtSecond := cloneReviewCanonicalFinding(priorFinding)
	fixedAtSecond.ExactSHA = testLedgerSecondSHA
	fixed, err := transitionReviewLedger(
		&recorded.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				fixedAtSecond,
				ReviewLedgerFindingFixed,
				"fresh verifier confirms the reproduction no longer fails",
				nil,
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("fixed transitionReviewLedger() error = %v", err)
	}

	renamed, err := transitionReviewLedger(
		&fixed.Ledger,
		testReviewLedgerRenameDelta(
			testLedgerSecondSHA,
			testLedgerThirdSHA,
			previousPath,
			currentPath,
		),
		testReviewLedgerCoverageRequirements(
			currentPath,
			previousPath,
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"unobserved fixed rename transitionReviewLedger() error = %v",
			err,
		)
	}
	currentFinding := testReviewLedgerFinding(
		t,
		testLedgerThirdSHA,
		currentPath,
		"Unobserved fixed rename",
		"unobserved fixed renames retain one identity",
	)
	if len(renamed.Ledger.Findings) != 1 {
		t.Fatalf(
			"unobserved fixed rename findings = %d, want one",
			len(renamed.Ledger.Findings),
		)
	}
	carriedFixed := renamed.Ledger.Findings[0]
	if carriedFixed.ID != currentFinding.ID ||
		carriedFixed.Status != ReviewLedgerFindingFixed ||
		carriedFixed.SourceSHA != testLedgerThirdSHA ||
		carriedFixed.LastObservedSHA != testLedgerSecondSHA ||
		carriedFixed.FixedSHA != testLedgerSecondSHA ||
		carriedFixed.VerificationReceipt.FindingID != priorFinding.ID ||
		carriedFixed.VerificationReceipt.ExactSHA !=
			testLedgerSecondSHA {
		t.Fatalf(
			"unobserved fixed rename finding = %#v",
			carriedFixed,
		)
	}
	renameTransition :=
		carriedFixed.History[len(carriedFixed.History)-1]
	if renameTransition.Reason != ReviewLedgerFindingRenamed ||
		renameTransition.FromFindingID != priorFinding.ID ||
		renameTransition.ToFindingID != currentFinding.ID {
		t.Fatalf(
			"unobserved fixed rename audit = %#v",
			renameTransition,
		)
	}

	regression := cloneReviewCanonicalFinding(currentFinding)
	regression.ExactSHA = testLedgerFourthSHA
	reopened, err := transitionReviewLedger(
		&renamed.Ledger,
		testReviewLedgerDelta(
			testLedgerThirdSHA,
			testLedgerFourthSHA,
			unrelated,
		),
		testReviewLedgerCoverageRequirements(unrelated),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				regression,
				ReviewLedgerFindingMissed,
				"later verifier reproduces the pre-existing regression",
				reviewLedgerBool(true),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf(
			"later fixed rename transitionReviewLedger() error = %v",
			err,
		)
	}
	if len(reopened.Ledger.Findings) != 1 {
		t.Fatalf(
			"later fixed rename findings = %d, want one",
			len(reopened.Ledger.Findings),
		)
	}
	carried := reopened.Ledger.Findings[0]
	if carried.ID != regression.ID ||
		carried.Status != ReviewLedgerFindingMissed ||
		carried.Provenance != ReviewLedgerFindingPreviouslyMissed ||
		carried.FirstSeenSHA != testLedgerFirstSHA ||
		carried.FixedSHA != testLedgerSecondSHA ||
		carried.VerificationReceipt.FindingID != regression.ID ||
		carried.VerificationReceipt.ExactSHA !=
			testLedgerFourthSHA {
		t.Fatalf("later fixed rename finding = %#v", carried)
	}

	agents := NewAgentManager()
	const agentID = "coding-agent-unobserved-fixed-rename-ledger"
	if err := agents.Add(&Agent{
		ID:   agentID,
		Role: RoleCoder,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	for _, checkpoint := range []struct {
		label  string
		ledger ReviewLedger
	}{
		{label: "recorded", ledger: recorded.Ledger},
		{label: "fixed", ledger: fixed.Ledger},
		{label: "renamed", ledger: renamed.Ledger},
		{label: "reopened", ledger: reopened.Ledger},
	} {
		if err := agents.SetReviewLedger(
			agentID,
			checkpoint.ledger,
			testReviewLedgerTransitionSnapshotFromLedger(
				checkpoint.ledger,
			),
		); err != nil {
			t.Fatalf(
				"SetReviewLedger(%s) error = %v",
				checkpoint.label,
				err,
			)
		}
	}
}

func TestTransitionReviewLedgerResolvesCarriedCoverageOnLaterHead(
	t *testing.T,
) {
	const (
		impacted  = "internal/impacted.go"
		unrelated = "internal/unrelated.go"
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			impacted,
		),
		testReviewLedgerCoverageRequirements(impacted),
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				impacted,
				ReviewCoverageCovered,
				"covered impacted path before the fix",
			),
		},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	_, err = transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			unrelated,
		),
		testReviewLedgerCoverageRequirements(unrelated),
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				impacted,
				ReviewCoverageCovered,
				"unplanned recheck of unaffected persisted coverage",
			),
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "not required by the exact-SHA plan") {
		t.Fatalf(
			"unaffected persisted coverage error = %v, want exact-SHA rejection",
			err,
		)
	}
	second, err := transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			impacted,
		),
		testReviewLedgerCoverageRequirements(impacted),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("impacted transitionReviewLedger() error = %v", err)
	}
	if !reflect.DeepEqual(
		second.ImpactedCoverageIDs,
		[]string{"call-path:" + impacted},
	) {
		t.Fatalf(
			"impacted coverage IDs = %#v",
			second.ImpactedCoverageIDs,
		)
	}

	resolved, err := transitionReviewLedger(
		&second.Ledger,
		testReviewLedgerDelta(
			testLedgerSecondSHA,
			testLedgerThirdSHA,
			unrelated,
		),
		testReviewLedgerCoverageRequirements(unrelated),
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				impacted,
				ReviewCoverageCovered,
				"rechecked carried impacted coverage at the later head",
			),
		},
	)
	if err != nil {
		t.Fatalf(
			"carried coverage transitionReviewLedger() error = %v",
			err,
		)
	}
	if len(resolved.ImpactedCoverageIDs) != 0 {
		t.Fatalf(
			"resolved carried coverage remained actionable: %#v",
			resolved.ImpactedCoverageIDs,
		)
	}
	coverage := coverageByLedgerID(
		t,
		resolved.Ledger,
		"call-path:"+impacted,
	)
	if coverage.NeedsRecheck ||
		coverage.SourceSHA != testLedgerThirdSHA ||
		coverage.LastObservedSHA != testLedgerThirdSHA {
		t.Fatalf("resolved carried coverage = %#v", coverage)
	}
}

func TestTransitionReviewLedgerRejectedCandidateNeedsNewEvidence(t *testing.T) {
	rejected := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/rejected.go",
		"Rejected",
		"candidate invariant",
	)
	initialObservation := testReviewLedgerFindingObservation(
		rejected,
		ReviewLedgerFindingRejected,
		"independent verification disproved the candidate",
		reviewLedgerBool(false),
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/rejected.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/rejected.go",
		),
		[]ReviewLedgerFindingObservation{initialObservation},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	rejectedAtSecond := cloneReviewCanonicalFinding(rejected)
	rejectedAtSecond.ExactSHA = testLedgerSecondSHA
	rejectedAtSecond.Provenance[0].WorkerID =
		"discovery-worker-second-head"
	sameEvidence := testReviewLedgerFindingObservation(
		rejectedAtSecond,
		ReviewLedgerFindingMissed,
		"independent verification disproved the candidate",
		nil,
	)
	second, err := transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/rejected.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/rejected.go",
		),
		[]ReviewLedgerFindingObservation{sameEvidence},
		nil,
	)
	if err != nil {
		t.Fatalf("same-evidence transitionReviewLedger() error = %v", err)
	}
	carried := findingByLedgerID(t, second.Ledger, rejected.ID)
	if carried.Status != ReviewLedgerFindingRejected {
		t.Fatalf("same-evidence rejected status = %q", carried.Status)
	}
	priorFinding := findingByLedgerID(t, first.Ledger, rejected.ID)
	if carried.EvidenceDigest == priorFinding.EvidenceDigest {
		t.Fatal("head and worker provenance were not audit-bound")
	}
	if carried.MaterialEvidenceDigest !=
		priorFinding.MaterialEvidenceDigest {
		t.Fatal("worker-only change altered material evidence identity")
	}
	if got := carried.Finding.Provenance[0].WorkerID; got !=
		"discovery-worker-second-head" {
		t.Fatalf("latest audited worker provenance = %q", got)
	}
	if !reflect.DeepEqual(
		second.SuppressedFindingIDs,
		[]string{rejected.ID},
	) || len(second.NovelFindingIDs) != 0 {
		t.Fatalf(
			"suppressed=%#v novel=%#v",
			second.SuppressedFindingIDs,
			second.NovelFindingIDs,
		)
	}
	if got := carried.History[len(carried.History)-1].Reason; got != ReviewLedgerFindingRejectedSuppressed {
		t.Fatalf("last rejected history reason = %q", got)
	}

	rejectedAtThird := cloneReviewCanonicalFinding(rejected)
	rejectedAtThird.ExactSHA = testLedgerThirdSHA
	tests := []struct {
		name           string
		presence       *bool
		wantProvenance ReviewLedgerFindingProvenance
		wantError      string
	}{
		{
			name:           "absent at delta base",
			presence:       reviewLedgerBool(false),
			wantProvenance: ReviewLedgerFindingFixIntroduced,
		},
		{
			name:           "present at delta base",
			presence:       reviewLedgerBool(true),
			wantProvenance: ReviewLedgerFindingPreviouslyMissed,
		},
		{
			name:      "missing presence evidence",
			wantError: "missing delta-base presence evidence",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			third, err := transitionReviewLedger(
				&second.Ledger,
				testReviewLedgerDelta(
					testLedgerSecondSHA,
					testLedgerThirdSHA,
					"internal/rejected.go",
				),
				testReviewLedgerCoverageRequirements(
					"internal/rejected.go",
				),
				[]ReviewLedgerFindingObservation{
					testReviewLedgerFindingObservation(
						rejectedAtThird,
						ReviewLedgerFindingMissed,
						"new reproduction demonstrates the invariant violation",
						test.presence,
					),
				},
				nil,
			)
			if test.wantError != "" {
				if err == nil ||
					!strings.Contains(
						err.Error(),
						test.wantError,
					) {
					t.Fatalf(
						"transitionReviewLedger() error = %v, want containing %q",
						err,
						test.wantError,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf(
					"transitionReviewLedger() error = %v",
					err,
				)
			}
			reopened := findingByLedgerID(
				t,
				third.Ledger,
				rejected.ID,
			)
			if reopened.Status != ReviewLedgerFindingMissed ||
				reopened.Provenance != test.wantProvenance {
				t.Fatalf(
					"reopened rejected finding = %#v",
					reopened,
				)
			}
			latest := reopened.History[len(reopened.History)-1]
			if latest.PresentInDeltaBase == nil ||
				*latest.PresentInDeltaBase != *test.presence {
				t.Fatalf(
					"reopened presence audit = %#v",
					latest,
				)
			}
			if !reflect.DeepEqual(
				third.NovelFindingIDs,
				[]string{rejected.ID},
			) {
				t.Fatalf(
					"novel finding IDs = %#v",
					third.NovelFindingIDs,
				)
			}
		})
	}
}

func TestTransitionReviewLedgerUsesFreshReceiptToVerifyFix(
	t *testing.T,
) {
	const path = "internal/receipt-bound-fix.go"
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		path,
		"Receipt-bound fix",
		"a fix requires a fresh exact-SHA verifier receipt",
	)
	verificationSummary := "independent verifier inspected the same behavior"
	recorded, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingReported,
				verificationSummary,
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	sameContentAtSecond := cloneReviewCanonicalFinding(finding)
	sameContentAtSecond.ExactSHA = testLedgerSecondSHA
	fixed, err := transitionReviewLedger(
		&recorded.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				sameContentAtSecond,
				ReviewLedgerFindingFixed,
				verificationSummary,
				nil,
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf(
			"same-content fresh verification error = %v",
			err,
		)
	}
	fixedFinding := findingByLedgerID(t, fixed.Ledger, finding.ID)
	priorFinding := findingByLedgerID(t, recorded.Ledger, finding.ID)
	if fixedFinding.Status != ReviewLedgerFindingFixed ||
		fixedFinding.MaterialEvidenceDigest !=
			priorFinding.MaterialEvidenceDigest ||
		fixedFinding.VerificationReceipt.ExactSHA !=
			testLedgerSecondSHA ||
		fixedFinding.VerificationReceipt.CandidateRevision ==
			priorFinding.VerificationReceipt.CandidateRevision ||
		fixedFinding.VerificationReceipt.VerifierWorkerID ==
			priorFinding.VerificationReceipt.VerifierWorkerID {
		t.Fatalf(
			"same-content fixed finding = %#v",
			fixedFinding,
		)
	}

	textOnlyMutation := testReviewLedgerFindingObservation(
		sameContentAtSecond,
		ReviewLedgerFindingFixed,
		"summary-only edit claims freshness without another verifier",
		nil,
	)
	textOnlyMutation.VerificationReceipt =
		priorFinding.VerificationReceipt
	_, err = transitionReviewLedger(
		&recorded.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{textOnlyMutation},
		nil,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"verification receipt does not match the delta head",
		) {
		t.Fatalf(
			"text-only fixed transition error = %v, want stale-receipt rejection",
			err,
		)
	}
}

func TestTransitionReviewLedgerDistinguishesFixIntroducedAndPreviouslyMissed(
	t *testing.T,
) {
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/seed.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/seed.go",
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	missed := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		"internal/old.go",
		"OldMissed",
		"old behavior remains valid",
	)
	fixIntroduced := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		"internal/fix.go",
		"FixRegression",
		"fix behavior remains valid",
	)
	second, err := transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/fix.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/fix.go",
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				fixIntroduced,
				ReviewLedgerFindingMissed,
				"verified the regression was absent at the prior head",
				reviewLedgerBool(false),
			),
			testReviewLedgerFindingObservation(
				missed,
				ReviewLedgerFindingMissed,
				"verified the issue was present at the prior head",
				reviewLedgerBool(true),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("delta transitionReviewLedger() error = %v", err)
	}
	if got := findingByLedgerID(
		t,
		second.Ledger,
		fixIntroduced.ID,
	).Provenance; got != ReviewLedgerFindingFixIntroduced {
		t.Fatalf("fix-introduced provenance = %q", got)
	}
	if got := findingByLedgerID(
		t,
		second.Ledger,
		missed.ID,
	).Provenance; got != ReviewLedgerFindingPreviouslyMissed {
		t.Fatalf("previously missed provenance = %q", got)
	}
}

func TestTransitionReviewLedgerSuppressesAlreadyReportedWithoutNewEvidence(
	t *testing.T,
) {
	reported := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/reported.go",
		"Reported",
		"reported behavior remains valid",
	)
	observation := testReviewLedgerFindingObservation(
		reported,
		ReviewLedgerFindingReported,
		"independently confirmed reported issue",
		reviewLedgerBool(false),
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/reported.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/reported.go",
		),
		[]ReviewLedgerFindingObservation{observation},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	reportedAtSecond := cloneReviewCanonicalFinding(reported)
	reportedAtSecond.ExactSHA = testLedgerSecondSHA
	observation = testReviewLedgerFindingObservation(
		reportedAtSecond,
		ReviewLedgerFindingReported,
		"independently confirmed reported issue",
		nil,
	)
	second, err := transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/reported.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/reported.go",
		),
		[]ReviewLedgerFindingObservation{observation},
		nil,
	)
	if err != nil {
		t.Fatalf("second transitionReviewLedger() error = %v", err)
	}
	carried := findingByLedgerID(t, second.Ledger, reported.ID)
	if carried.Status != ReviewLedgerFindingReported ||
		carried.Provenance != ReviewLedgerFindingPreviouslyReported ||
		carried.ReportedSHA != testLedgerFirstSHA {
		t.Fatalf("carried reported finding = %#v", carried)
	}
	if !reflect.DeepEqual(
		second.SuppressedFindingIDs,
		[]string{reported.ID},
	) || len(second.NovelFindingIDs) != 0 {
		t.Fatalf(
			"suppressed=%#v novel=%#v",
			second.SuppressedFindingIDs,
			second.NovelFindingIDs,
		)
	}
	last := carried.History[len(carried.History)-1]
	if last.FromProvenance != ReviewLedgerFindingOriginalPR ||
		last.ToProvenance != ReviewLedgerFindingPreviouslyReported ||
		last.Reason != ReviewLedgerFindingReportedSuppressed {
		t.Fatalf("reported provenance audit transition = %#v", last)
	}
}

func TestTransitionReviewLedgerRejectsUnverifiedFixedDisposition(t *testing.T) {
	reported := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/fixed.go",
		"Fixed",
		"fixed behavior remains valid",
	)
	observation := testReviewLedgerFindingObservation(
		reported,
		ReviewLedgerFindingReported,
		"independently confirmed issue",
		reviewLedgerBool(false),
	)
	first, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/fixed.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/fixed.go",
		),
		[]ReviewLedgerFindingObservation{observation},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	reportedAtSecond := cloneReviewCanonicalFinding(reported)
	reportedAtSecond.ExactSHA = testLedgerSecondSHA
	observation = testReviewLedgerFindingObservation(
		reportedAtSecond,
		ReviewLedgerFindingFixed,
		"independently confirmed issue",
		nil,
	)
	_, err = transitionReviewLedger(
		&first.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/unrelated.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/unrelated.go",
		),
		[]ReviewLedgerFindingObservation{observation},
		nil,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"impacted, fresh exact-SHA verification receipt",
		) {
		t.Fatalf(
			"unverified fixed transition error = %v, want evidence guard",
			err,
		)
	}
}

func TestTransitionReviewLedgerRejectsCoverageOutsideExactSHAPlan(
	t *testing.T,
) {
	_, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/planned.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/planned.go",
		),
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				"internal/arbitrary.go",
				ReviewCoverageCovered,
				"evidence for an unplanned requirement",
			),
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "not required by the exact-SHA plan") {
		t.Fatalf(
			"out-of-plan coverage error = %v, want exact-SHA plan rejection",
			err,
		)
	}
}

func TestSetReviewLedgerRejectsNonAppendOnlySuccessors(t *testing.T) {
	const path = "internal/durable.go"
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		path,
		"Durable",
		"durable review knowledge is retained",
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingReported,
				"independently confirmed durable issue",
				reviewLedgerBool(false),
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				path,
				ReviewCoverageCovered,
				"covered durable path",
			),
		},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	successor, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("successor transitionReviewLedger() error = %v", err)
	}

	tests := []struct {
		name    string
		rewrite func(*ReviewLedger)
		want    string
	}{
		{
			name: "dropped finding",
			rewrite: func(ledger *ReviewLedger) {
				ledger.Findings = []ReviewLedgerFinding{}
			},
			want: "drops finding",
		},
		{
			name: "dropped coverage",
			rewrite: func(ledger *ReviewLedger) {
				ledger.Coverage = []ReviewLedgerCoverage{}
			},
			want: "drops coverage",
		},
		{
			name: "rewritten finding audit history",
			rewrite: func(ledger *ReviewLedger) {
				finding := &ledger.Findings[0]
				finding.Status = ReviewLedgerFindingMissed
				finding.ReportedSHA = ""
				finding.History[0].ToStatus =
					ReviewLedgerFindingMissed
				finding.History[1].FromStatus =
					ReviewLedgerFindingMissed
				finding.History[1].ToStatus =
					ReviewLedgerFindingMissed
			},
			want: "rewrites finding",
		},
		{
			name: "rewritten coverage audit history",
			rewrite: func(ledger *ReviewLedger) {
				coverage := &ledger.Coverage[0]
				coverage.Evidence[0].Summary =
					"rewritten covered durable path"
				digest, err :=
					reviewLedgerCoverageEvidenceDigestFor(
						ReviewCoverageClaim{
							RequirementID: coverage.RequirementID,
							Kind:          coverage.Kind,
							Status:        coverage.Status,
							Evidence:      coverage.Evidence,
						},
					)
				if err != nil {
					t.Fatalf(
						"reviewLedgerCoverageEvidenceDigestFor() error = %v",
						err,
					)
				}
				coverage.EvidenceDigest = digest
				coverage.History[0].EvidenceDigest = digest
				coverage.History[1].FromEvidenceDigest = digest
				coverage.History[1].EvidenceDigest = digest
			},
			want: "rewrites coverage",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneReviewLedger(&successor.Ledger)
			test.rewrite(candidate)
			if err := validateReviewLedger(candidate); err != nil {
				t.Fatalf(
					"rewritten successor should remain independently valid: %v",
					err,
				)
			}

			agents := NewAgentManager()
			if err := agents.Add(&Agent{
				ID:   "coding-agent-append-only-ledger",
				Role: RoleCoder,
			}); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			if err := agents.SetReviewLedger(
				"coding-agent-append-only-ledger",
				prior.Ledger,
				testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
			); err != nil {
				t.Fatalf("SetReviewLedger(prior) error = %v", err)
			}
			err := agents.SetReviewLedger(
				"coding-agent-append-only-ledger",
				*candidate,
				testReviewLedgerTransitionSnapshotFromLedger(successor.Ledger),
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"SetReviewLedger(rewritten) error = %v, want containing %q",
					err,
					test.want,
				)
			}
		})
	}
}

func TestSetReviewLedgerRejectsArbitraryFindingIdentityReplacement(
	t *testing.T,
) {
	const (
		previousPath = "internal/identity-old.go"
		currentPath  = "internal/identity-new.go"
	)
	priorFinding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		previousPath,
		"Original identity",
		"the original canonical behavior remains durable",
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			previousPath,
		),
		testReviewLedgerCoverageRequirements(previousPath),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				priorFinding,
				ReviewLedgerFindingReported,
				"independently confirmed original identity",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	renamed, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerRenameDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			previousPath,
			currentPath,
		),
		testReviewLedgerCoverageRequirements(
			currentPath,
			previousPath,
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("rename transitionReviewLedger() error = %v", err)
	}

	candidate := cloneReviewLedger(&renamed.Ledger)
	arbitrary := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		currentPath,
		"Unrelated replacement",
		"an unrelated canonical behavior is substituted",
	)
	verification := []ReviewEvidence{{
		Summary: "independent evidence for unrelated replacement",
		Path:    currentPath,
	}}
	previous := prior.Ledger.Findings[0]
	digest, err := reviewLedgerFindingEvidenceDigestFor(
		arbitrary,
		verification,
		previous.VerificationReceipt,
	)
	if err != nil {
		t.Fatalf("reviewLedgerFindingEvidenceDigestFor() error = %v", err)
	}
	materialDigest, err :=
		reviewLedgerFindingMaterialEvidenceDigestFor(
			arbitrary,
			verification,
		)
	if err != nil {
		t.Fatalf(
			"reviewLedgerFindingMaterialEvidenceDigestFor() error = %v",
			err,
		)
	}
	replacement := previous
	replacement.ID = arbitrary.ID
	replacement.Fingerprint = arbitrary.Fingerprint
	replacement.SourceSHA = testLedgerSecondSHA
	replacement.Finding = arbitrary
	replacement.VerificationEvidence = verification
	replacement.VerificationReceipt = previous.VerificationReceipt
	replacement.EvidenceDigest = digest
	replacement.MaterialEvidenceDigest = materialDigest
	replacement.NeedsRecheck = true
	replacement.History = append(
		[]ReviewLedgerFindingTransition{},
		previous.History...,
	)
	appendReviewLedgerFindingTransition(
		&replacement,
		testLedgerSecondSHA,
		&previous,
		ReviewLedgerFindingImpacted,
		nil,
	)
	impactedReplacement := replacement
	replacementObservation := testReviewLedgerFindingObservation(
		arbitrary,
		ReviewLedgerFindingReported,
		"fresh independent evidence for unrelated replacement",
		nil,
	)
	replacementDigest, err := reviewLedgerFindingEvidenceDigest(
		replacementObservation,
	)
	if err != nil {
		t.Fatalf("reviewLedgerFindingEvidenceDigest() error = %v", err)
	}
	replacementMaterialDigest, err :=
		reviewLedgerFindingMaterialEvidenceDigest(
			replacementObservation,
		)
	if err != nil {
		t.Fatalf(
			"reviewLedgerFindingMaterialEvidenceDigest() error = %v",
			err,
		)
	}
	replacement.Provenance =
		ReviewLedgerFindingPreviouslyReported
	updateReviewLedgerFindingObservationEvidence(
		&replacement,
		replacementObservation,
		replacementDigest,
		replacementMaterialDigest,
		testLedgerSecondSHA,
	)
	appendReviewLedgerFindingTransition(
		&replacement,
		testLedgerSecondSHA,
		&impactedReplacement,
		ReviewLedgerFindingEvidenceChanged,
		nil,
	)
	candidate.Findings[0] = replacement
	if err := validateReviewLedger(candidate); err != nil {
		t.Fatalf(
			"constructed identity replacement should remain independently valid: %v",
			err,
		)
	}

	agents := NewAgentManager()
	const agentID = "coding-agent-arbitrary-identity-ledger"
	if err := agents.Add(&Agent{
		ID:   agentID,
		Role: RoleCoder,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		prior.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(prior) error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		*candidate,
		testReviewLedgerTransitionSnapshotFromLedger(renamed.Ledger),
	); err == nil ||
		!strings.Contains(err.Error(), "non-path identity") {
		t.Fatalf(
			"SetReviewLedger(arbitrary identity) error = %v, want path-only rename rejection",
			err,
		)
	}
}

func TestSetReviewLedgerRejectsFixedFindingWithoutPriorImpact(
	t *testing.T,
) {
	const path = "internal/unimpacted.go"
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		path,
		"Unimpacted",
		"only impacted findings can be fixed",
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingReported,
				"independently confirmed unimpacted issue",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	legitimate, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			"internal/unrelated.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/unrelated.go",
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("successor transitionReviewLedger() error = %v", err)
	}
	successor := cloneReviewLedger(&legitimate.Ledger)
	fixedFinding := testReviewLedgerFinding(
		t,
		testLedgerSecondSHA,
		path,
		"Unimpacted",
		"only impacted findings can be fixed",
	)
	observation := testReviewLedgerFindingObservation(
		fixedFinding,
		ReviewLedgerFindingFixed,
		"fresh evidence claims the unrelated finding is fixed",
		nil,
	)
	digest, err := reviewLedgerFindingEvidenceDigest(observation)
	if err != nil {
		t.Fatalf("reviewLedgerFindingEvidenceDigest() error = %v", err)
	}
	materialDigest, err :=
		reviewLedgerFindingMaterialEvidenceDigest(observation)
	if err != nil {
		t.Fatalf(
			"reviewLedgerFindingMaterialEvidenceDigest() error = %v",
			err,
		)
	}
	current := &successor.Findings[0]
	previous := *current
	current.Status = ReviewLedgerFindingFixed
	current.Provenance = ReviewLedgerFindingPreviouslyReported
	updateReviewLedgerFindingObservationEvidence(
		current,
		observation,
		digest,
		materialDigest,
		testLedgerSecondSHA,
	)
	current.FixedSHA = testLedgerSecondSHA
	appendReviewLedgerFindingTransition(
		current,
		testLedgerSecondSHA,
		&previous,
		ReviewLedgerFindingVerifiedFixed,
		nil,
	)

	agents := NewAgentManager()
	const agentID = "coding-agent-unimpacted-fixed-ledger"
	if err := agents.Add(&Agent{
		ID:   agentID,
		Role: RoleCoder,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		prior.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(prior) error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		*successor,
		testReviewLedgerTransitionSnapshotFromLedger(legitimate.Ledger),
	); err == nil ||
		!strings.Contains(err.Error(), "preceding impacted state") {
		t.Fatalf(
			"SetReviewLedger(fixed without impact) error = %v, want impacted-state rejection",
			err,
		)
	}
}

func TestSetReviewLedgerRejectsCoverageDescriptionRewrite(
	t *testing.T,
) {
	const path = "internal/coverage-description.go"
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				path,
				ReviewCoverageCovered,
				"covered immutable requirement",
			),
		},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	successor, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("successor transitionReviewLedger() error = %v", err)
	}
	rewritten := cloneReviewLedger(&successor.Ledger)
	rewritten.Coverage[0].Description =
		"rewritten acceptance criterion meaning"

	agents := NewAgentManager()
	const agentID = "coding-agent-coverage-description-ledger"
	if err := agents.Add(&Agent{
		ID:   agentID,
		Role: RoleCoder,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		prior.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(prior) error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		*rewritten,
		testReviewLedgerTransitionSnapshotFromLedger(successor.Ledger),
	); err == nil ||
		!strings.Contains(err.Error(), "requirement digest") {
		t.Fatalf(
			"SetReviewLedger(description rewrite) error = %v, want requirement binding rejection",
			err,
		)
	}
}

func TestReviewLedgerRejectsUnboundSuccessorCoverage(
	t *testing.T,
) {
	const (
		seedPath      = "internal/coverage-seed.go"
		plannedPath   = "internal/coverage-planned.go"
		arbitraryPath = "internal/coverage-arbitrary.go"
		agentID       = "coding-agent-unbound-coverage-ledger"
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			seedPath,
		),
		testReviewLedgerCoverageRequirements(seedPath),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	successorDelta := testReviewLedgerDelta(
		testLedgerFirstSHA,
		testLedgerSecondSHA,
		plannedPath,
	)
	successor, err := transitionReviewLedger(
		&prior.Ledger,
		successorDelta,
		testReviewLedgerCoverageRequirements(plannedPath),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("successor transitionReviewLedger() error = %v", err)
	}
	forged, err := transitionReviewLedger(
		nil,
		successorDelta,
		testReviewLedgerCoverageRequirements(arbitraryPath),
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				arbitraryPath,
				ReviewCoverageCovered,
				"constructed coverage absent from the successor plan",
			),
		},
	)
	if err != nil {
		t.Fatalf("forged transitionReviewLedger() error = %v", err)
	}
	candidate := cloneReviewLedger(&successor.Ledger)
	candidate.Coverage = append(
		[]ReviewLedgerCoverage{},
		forged.Ledger.Coverage...,
	)

	agents := NewAgentManager()
	if err := agents.Add(&Agent{
		ID:   agentID,
		Role: RoleCoder,
	}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		prior.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger(prior) error = %v", err)
	}
	if err := agents.SetReviewLedger(
		agentID,
		*candidate,
		testReviewLedgerTransitionSnapshotFromLedger(successor.Ledger),
	); err == nil ||
		!strings.Contains(
			err.Error(),
			"not bound to its immutable exact-SHA requirement set",
		) {
		t.Fatalf(
			"SetReviewLedger(unbound coverage) error = %v, want plan-binding rejection",
			err,
		)
	}

	logDir := t.TempDir()
	restored := &Orchestrator{
		cfg:    Config{LogDir: logDir},
		agents: NewAgentManager(),
	}
	payload := persistedStateFile{
		Version: persistedStateVersion,
		Agents: []persistedAgent{{
			ID:           agentID,
			Role:         RoleCoder,
			State:        StateWorking,
			ReviewLedger: candidate,
		}},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if err := os.MkdirAll(
		orchestratorStateDir(logDir),
		0o755,
	); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(
		restored.agentStateFilePath(),
		body,
		0o600,
	); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := restored.loadPersistedAgentState(); err == nil ||
		!strings.Contains(
			err.Error(),
			"not bound to its immutable exact-SHA requirement set",
		) {
		t.Fatalf(
			"loadPersistedAgentState() error = %v, want plan-binding rejection",
			err,
		)
	}
}

func TestSetReviewLedgerRejectsSelfAuthoredHeadBindings(t *testing.T) {
	t.Run("coverage requirement and dependent claim", func(t *testing.T) {
		const (
			seedPath      = "internal/trusted-seed.go"
			plannedPath   = "internal/trusted-planned.go"
			arbitraryPath = "internal/trusted-arbitrary.go"
			agentID       = "coding-agent-trusted-coverage-ledger"
		)
		prior, err := transitionReviewLedger(
			nil,
			testReviewLedgerDelta(
				testLedgerBaseSHA,
				testLedgerFirstSHA,
				seedPath,
			),
			testReviewLedgerCoverageRequirements(seedPath),
			nil,
			nil,
		)
		if err != nil {
			t.Fatalf("initial transitionReviewLedger() error = %v", err)
		}
		delta := testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			plannedPath,
		)
		trusted, err := transitionReviewLedger(
			&prior.Ledger,
			delta,
			testReviewLedgerCoverageRequirements(plannedPath),
			nil,
			nil,
		)
		if err != nil {
			t.Fatalf("trusted transitionReviewLedger() error = %v", err)
		}
		forged, err := transitionReviewLedger(
			&prior.Ledger,
			delta,
			testReviewLedgerCoverageRequirements(
				arbitraryPath,
				plannedPath,
			),
			nil,
			[]ReviewLedgerCoverageObservation{
				testReviewLedgerCoverageObservation(
					arbitraryPath,
					ReviewCoverageCovered,
					"self-authored requirement and matching coverage",
				),
			},
		)
		if err != nil {
			t.Fatalf("forged transitionReviewLedger() error = %v", err)
		}
		if err := validateReviewLedger(&forged.Ledger); err != nil {
			t.Fatalf("self-consistent forged ledger is invalid: %v", err)
		}

		agents := NewAgentManager()
		if err := agents.Add(&Agent{
			ID:   agentID,
			Role: RoleCoder,
		}); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		if err := agents.SetReviewLedger(
			agentID,
			prior.Ledger,
			testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
		); err != nil {
			t.Fatalf("SetReviewLedger(prior) error = %v", err)
		}
		if err := agents.SetReviewLedger(
			agentID,
			forged.Ledger,
			testReviewLedgerTransitionSnapshotFromLedger(trusted.Ledger),
		); err == nil ||
			!strings.Contains(
				err.Error(),
				"manager-owned exact delta and plan",
			) {
			t.Fatalf(
				"SetReviewLedger(forged coverage binding) error = %v, want trusted binding rejection",
				err,
			)
		}
	})

	t.Run("rename delta and dependent identity", func(t *testing.T) {
		const (
			previousPath = "internal/trusted-rename-old.go"
			currentPath  = "internal/trusted-rename-new.go"
			agentID      = "coding-agent-trusted-rename-ledger"
		)
		finding := testReviewLedgerFinding(
			t,
			testLedgerFirstSHA,
			previousPath,
			"Trusted rename",
			"identity moves require a manager-owned exact rename",
		)
		prior, err := transitionReviewLedger(
			nil,
			testReviewLedgerDelta(
				testLedgerBaseSHA,
				testLedgerFirstSHA,
				previousPath,
			),
			testReviewLedgerCoverageRequirements(previousPath),
			[]ReviewLedgerFindingObservation{
				testReviewLedgerFindingObservation(
					finding,
					ReviewLedgerFindingReported,
					"independently confirmed durable identity",
					reviewLedgerBool(false),
				),
			},
			nil,
		)
		if err != nil {
			t.Fatalf("initial transitionReviewLedger() error = %v", err)
		}
		trusted, err := transitionReviewLedger(
			&prior.Ledger,
			testReviewLedgerDelta(
				testLedgerFirstSHA,
				testLedgerSecondSHA,
				currentPath,
			),
			testReviewLedgerCoverageRequirements(currentPath),
			nil,
			nil,
		)
		if err != nil {
			t.Fatalf("trusted transitionReviewLedger() error = %v", err)
		}
		forged, err := transitionReviewLedger(
			&prior.Ledger,
			testReviewLedgerRenameDelta(
				testLedgerFirstSHA,
				testLedgerSecondSHA,
				previousPath,
				currentPath,
			),
			testReviewLedgerCoverageRequirements(
				currentPath,
				previousPath,
			),
			nil,
			nil,
		)
		if err != nil {
			t.Fatalf("forged transitionReviewLedger() error = %v", err)
		}
		if err := validateReviewLedger(&forged.Ledger); err != nil {
			t.Fatalf("self-consistent forged rename ledger is invalid: %v", err)
		}

		agents := NewAgentManager()
		if err := agents.Add(&Agent{
			ID:   agentID,
			Role: RoleCoder,
		}); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
		if err := agents.SetReviewLedger(
			agentID,
			prior.Ledger,
			testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
		); err != nil {
			t.Fatalf("SetReviewLedger(prior) error = %v", err)
		}
		if err := agents.SetReviewLedger(
			agentID,
			forged.Ledger,
			testReviewLedgerTransitionSnapshotFromLedger(trusted.Ledger),
		); err == nil ||
			!strings.Contains(
				err.Error(),
				"manager-owned exact delta and plan",
			) {
			t.Fatalf(
				"SetReviewLedger(forged rename binding) error = %v, want trusted binding rejection",
				err,
			)
		}
	})
}

func TestTransitionReviewLedgerRejectsPlannedCoverageDescriptionRewrite(
	t *testing.T,
) {
	const (
		path      = "internal/planned-description.go"
		unrelated = "internal/unrelated.go"
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		nil,
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				path,
				ReviewCoverageCovered,
				"covered immutable planned requirement",
			),
		},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	requirements := testReviewLedgerCoverageRequirements(
		path,
		unrelated,
	)
	for index := range requirements {
		if requirements[index].ID == "call-path:"+path {
			requirements[index].Description =
				"rewritten exact-SHA plan meaning"
		}
	}
	if _, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			unrelated,
		),
		requirements,
		nil,
		nil,
	); err == nil ||
		!strings.Contains(
			err.Error(),
			"conflicts with the exact-SHA plan",
		) {
		t.Fatalf(
			"transitionReviewLedger(description rewrite) error = %v, want exact-plan conflict",
			err,
		)
	}
}

// TestTransitionReviewLedgerCarriesStableIDCoverageAcrossChangingDiffs is
// the regression test: reviewLedgerExactCoverageRequirements
// used to compare a carried coverage requirement against the new plan's
// same-ID requirement with reflect.DeepEqual on the whole struct, which
// includes ChangedTargets and Critical -- both recomputed fresh from the
// current diff every round, and expected to legitimately differ round to
// round for the exact same logical requirement. That fired a false
// "conflicts with the exact-SHA plan" error on nearly every multi-round PR
// for any coverage kind whose ID is a stable tag/index rather than a
// content hash (acceptance_criterion, state_transition,
// persistence_boundary -- reproduced here with state_transition, matching
// the production incident this was filed from).
//
// TestTransitionReviewLedgerRejectsPlannedCoverageDescriptionRewrite above
// already covers (and must keep covering) the case this must *not*
// weaken: a genuine identity change (a rewritten Description) is still a
// real conflict.
func TestTransitionReviewLedgerCarriesStableIDCoverageAcrossChangingDiffs(
	t *testing.T,
) {
	const (
		requirementID = "state-transition:lifecycle"
		firstPath     = "internal/lifecycle_first.go"
		secondPath    = "internal/lifecycle_second.go"
	)
	firstRoundRequirements := []ReviewCoverageRequirement{{
		ID:              requirementID,
		Kind:            ReviewCoverageStateTransition,
		Description:     "lifecycle state transitions through changed branches",
		Critical:        true,
		AccountableLane: "lifecycle",
		ChangedTargets: []ReviewCoverageTarget{
			{Path: firstPath, StartLine: 1, EndLine: 10},
		},
	}}
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			firstPath,
		),
		firstRoundRequirements,
		nil,
		[]ReviewLedgerCoverageObservation{{
			Requirement: firstRoundRequirements[0],
			Claim: ReviewCoverageClaim{
				RequirementID: requirementID,
				Kind:          ReviewCoverageStateTransition,
				Status:        ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary: "covered lifecycle transitions in the first round's diff",
					Path:    firstPath,
				}},
			},
		}},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	// Second round: the same logical requirement (same ID/Kind/Description)
	// is planned again, but ChangedTargets and Critical are recomputed from
	// a genuinely different diff -- a different changed path, a different
	// target range. This must not be treated as a conflict.
	secondRoundRequirements := []ReviewCoverageRequirement{{
		ID:              requirementID,
		Kind:            ReviewCoverageStateTransition,
		Description:     "lifecycle state transitions through changed branches",
		Critical:        false,
		AccountableLane: "lifecycle",
		ChangedTargets: []ReviewCoverageTarget{
			{Path: secondPath, StartLine: 20, EndLine: 30},
		},
	}}
	// A real second-round recheck, not a nil coverage observation: this is
	// A real second-round recheck, not a nil coverage observation: this is
	// what independent review feedback on the first version of this fix
	// caught -- passing nil here never exercised
	// reviewLedgerRequirementInHeadBinding (a *second*, separate
	// full-struct reflect.DeepEqual comparison, hit only when validating a
	// coverage entry's transition history against its own historical head
	// bindings) or applyReviewLedgerCoverageObservation's failure to
	// refresh a rechecked coverage entry's own stored Critical/
	// AccountableLane/ChangedTargets to the new round's values.
	next, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			secondPath,
		),
		secondRoundRequirements,
		nil,
		[]ReviewLedgerCoverageObservation{{
			Requirement: secondRoundRequirements[0],
			Claim: ReviewCoverageClaim{
				RequirementID: requirementID,
				Kind:          ReviewCoverageStateTransition,
				Status:        ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary: "covered lifecycle transitions in the second round's diff",
					Path:    secondPath,
				}},
			},
		}},
	)
	if err != nil {
		t.Fatalf(
			"transitionReviewLedger(same identity, changed diff-derived fields, real second-round recheck) error = %v, want no conflict",
			err,
		)
	}
	if err := validateReviewLedger(&next.Ledger); err != nil {
		t.Fatalf("resulting ledger is invalid: %v", err)
	}

	coverage := findLedgerCoverageByID(t, next.Ledger, requirementID)
	if coverage.Critical != false ||
		coverage.AccountableLane != "lifecycle" ||
		len(coverage.ChangedTargets) != 1 ||
		coverage.ChangedTargets[0].Path != secondPath {
		t.Fatalf(
			"coverage entry after second-round recheck = %#v, want refreshed to the second round's Critical/AccountableLane/ChangedTargets, not left frozen at the first round's",
			coverage,
		)
	}
}

func findLedgerCoverageByID(
	t *testing.T,
	ledger ReviewLedger,
	requirementID string,
) ReviewLedgerCoverage {
	t.Helper()
	for _, coverage := range ledger.Coverage {
		if coverage.RequirementID == requirementID {
			return coverage
		}
	}
	t.Fatalf("no coverage entry found for requirement %q", requirementID)
	return ReviewLedgerCoverage{}
}

func TestReviewLedgerRejectsDiscontinuousAuditHistories(t *testing.T) {
	const path = "internal/continuous.go"
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		path,
		"Continuous",
		"audit state transitions remain continuous",
	)
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingReported,
				"independently confirmed continuous issue",
				reviewLedgerBool(false),
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				path,
				ReviewCoverageCovered,
				"covered continuous path",
			),
		},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	successor, err := transitionReviewLedger(
		&prior.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("successor transitionReviewLedger() error = %v", err)
	}

	tests := []struct {
		name    string
		corrupt func(*ReviewLedger)
		want    string
	}{
		{
			name: "finding status",
			corrupt: func(ledger *ReviewLedger) {
				latest := &ledger.Findings[0].History[len(ledger.Findings[0].History)-1]
				latest.FromStatus = ReviewLedgerFindingRejected
			},
			want: "status or provenance history is discontinuous",
		},
		{
			name: "finding provenance",
			corrupt: func(ledger *ReviewLedger) {
				latest := &ledger.Findings[0].History[len(ledger.Findings[0].History)-1]
				latest.FromProvenance =
					ReviewLedgerFindingPreExisting
			},
			want: "status or provenance history is discontinuous",
		},
		{
			name: "finding identity",
			corrupt: func(ledger *ReviewLedger) {
				latest := &ledger.Findings[0].History[len(ledger.Findings[0].History)-1]
				latest.FromFindingID =
					"finding-" + strings.Repeat("a", 64)
			},
			want: "identity or evidence history is discontinuous",
		},
		{
			name: "finding evidence",
			corrupt: func(ledger *ReviewLedger) {
				latest := &ledger.Findings[0].History[len(ledger.Findings[0].History)-1]
				latest.FromEvidenceDigest = strings.Repeat("b", 64)
			},
			want: "identity or evidence history is discontinuous",
		},
		{
			name: "finding observed SHA",
			corrupt: func(ledger *ReviewLedger) {
				ledger.Findings[0].LastObservedSHA =
					testLedgerSecondSHA
			},
			want: "verification receipt does not match the last observed SHA",
		},
		{
			name: "coverage status",
			corrupt: func(ledger *ReviewLedger) {
				latest := &ledger.Coverage[0].History[len(ledger.Coverage[0].History)-1]
				latest.FromStatus = ReviewCoveragePartial
			},
			want: "status or evidence history is discontinuous",
		},
		{
			name: "coverage evidence",
			corrupt: func(ledger *ReviewLedger) {
				latest := &ledger.Coverage[0].History[len(ledger.Coverage[0].History)-1]
				latest.FromEvidenceDigest = strings.Repeat("c", 64)
			},
			want: "status or evidence history is discontinuous",
		},
		{
			name: "coverage observed SHA",
			corrupt: func(ledger *ReviewLedger) {
				ledger.Coverage[0].LastObservedSHA =
					testLedgerSecondSHA
			},
			want: "SHA or recheck history does not match current state",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneReviewLedger(&successor.Ledger)
			test.corrupt(candidate)
			if err := validateReviewLedger(candidate); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"validateReviewLedger() error = %v, want containing %q",
					err,
					test.want,
				)
			}

			agents := NewAgentManager()
			const agentID = "coding-agent-continuous-ledger"
			if err := agents.Add(&Agent{
				ID:   agentID,
				Role: RoleCoder,
			}); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			if err := agents.SetReviewLedger(
				agentID,
				prior.Ledger,
				testReviewLedgerTransitionSnapshotFromLedger(prior.Ledger),
			); err != nil {
				t.Fatalf("SetReviewLedger(prior) error = %v", err)
			}
			if err := agents.SetReviewLedger(
				agentID,
				*candidate,
				testReviewLedgerTransitionSnapshotFromLedger(successor.Ledger),
			); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"SetReviewLedger(discontinuous) error = %v, want containing %q",
					err,
					test.want,
				)
			}

			logDir := t.TempDir()
			restored := &Orchestrator{
				cfg:    Config{LogDir: logDir},
				agents: NewAgentManager(),
			}
			payload := persistedStateFile{
				Version: persistedStateVersion,
				Agents: []persistedAgent{{
					ID:           agentID,
					Role:         RoleCoder,
					State:        StateWorking,
					ReviewLedger: candidate,
				}},
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			if err := os.MkdirAll(
				orchestratorStateDir(logDir),
				0o755,
			); err != nil {
				t.Fatalf("MkdirAll() error = %v", err)
			}
			if err := os.WriteFile(
				restored.agentStateFilePath(),
				body,
				0o600,
			); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			if err := restored.loadPersistedAgentState(); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"loadPersistedAgentState() error = %v, want containing %q",
					err,
					test.want,
				)
			}
		})
	}
}

func TestLoadPersistedReviewLedgerRejectsTransitionOrderRegressions(
	t *testing.T,
) {
	const path = "internal/ordered-transition.go"
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		path,
		"Ordered transition",
		"rechecks cannot move backward through exact heads",
	)
	recorded, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingReported,
				"independently confirmed ordered transition",
				reviewLedgerBool(false),
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				path,
				ReviewCoverageCovered,
				"covered ordered transition",
			),
		},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	impacted, err := transitionReviewLedger(
		&recorded.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("impacted transitionReviewLedger() error = %v", err)
	}

	tests := []struct {
		name    string
		corrupt func(*ReviewLedger)
		want    string
	}{
		{
			name: "finding stale recheck",
			corrupt: func(ledger *ReviewLedger) {
				current := &ledger.Findings[0]
				previous := *current
				current.NeedsRecheck = false
				current.SourceSHA = testLedgerFirstSHA
				current.LastObservedSHA = testLedgerFirstSHA
				appendReviewLedgerFindingTransition(
					current,
					testLedgerFirstSHA,
					&previous,
					ReviewLedgerFindingRechecked,
					nil,
				)
			},
			want: "finding transition regresses persisted head order",
		},
		{
			name: "coverage stale recheck",
			corrupt: func(ledger *ReviewLedger) {
				current := &ledger.Coverage[0]
				previous := *current
				current.NeedsRecheck = false
				appendReviewLedgerCoverageTransition(
					current,
					testLedgerFirstSHA,
					&previous,
					ReviewLedgerCoverageRechecked,
					current.RequirementSetDigest,
				)
			},
			want: "coverage transition regresses persisted head order",
		},
		{
			name: "coverage impact status change",
			corrupt: func(ledger *ReviewLedger) {
				history := ledger.Coverage[0].History
				history[len(history)-1].ToStatus =
					ReviewCoveragePartial
			},
			want: "coverage impact transition changes status",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := cloneReviewLedger(&impacted.Ledger)
			test.corrupt(ledger)
			if err := loadPersistedReviewLedgerForTest(
				t,
				ledger,
			); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"loadPersistedAgentState() error = %v, want containing %q",
					err,
					test.want,
				)
			}
		})
	}
}

func TestReviewLedgerSurvivesAgentStateRestart(t *testing.T) {
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/persisted.go",
		"Persisted",
		"persisted behavior remains valid",
	)
	transitioned, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/persisted.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/persisted.go",
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingRejected,
				"independent verifier rejected the candidate",
				reviewLedgerBool(false),
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				"internal/persisted.go",
				ReviewCoveragePartial,
				"persisted partial coverage evidence",
			),
		},
	)
	if err != nil {
		t.Fatalf("transitionReviewLedger() error = %v", err)
	}

	logDir := t.TempDir()
	agents := NewAgentManager()
	coder := &Agent{
		ID:               "coding-agent-ledger",
		Role:             RoleCoder,
		IssueNumber:      45,
		State:            StateWorking,
		LastActivityTime: time.Unix(1700000000, 0).UTC(),
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "coding-agent-ledger",
		},
		RuntimeProfile: AgentProfile{
			Name:            "coder",
			Model:           "gpt-5.6-sol",
			ReasoningEffort: "high",
		},
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agents.SetReviewLedger(
		coder.ID,
		transitioned.Ledger,
		testReviewLedgerTransitionSnapshotFromLedger(transitioned.Ledger),
	); err != nil {
		t.Fatalf("SetReviewLedger() error = %v", err)
	}
	bot := &Orchestrator{cfg: Config{LogDir: logDir}, agents: agents}
	if err := bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState() error = %v", err)
	}

	restoredAgents := NewAgentManager()
	restored := &Orchestrator{
		cfg:    Config{LogDir: logDir},
		agents: restoredAgents,
	}
	if err := restored.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	loaded, ok := restoredAgents.Get(coder.ID)
	if !ok {
		t.Fatalf("restored coder %q was not found", coder.ID)
	}
	if loaded.ReviewLedger == nil ||
		!reflect.DeepEqual(*loaded.ReviewLedger, transitioned.Ledger) {
		t.Fatalf(
			"restored review ledger = %#v, want %#v",
			loaded.ReviewLedger,
			transitioned.Ledger,
		)
	}
	rejected := findingByLedgerID(
		t,
		*loaded.ReviewLedger,
		finding.ID,
	)
	if rejected.Status != ReviewLedgerFindingRejected {
		t.Fatalf("restored rejected status = %q", rejected.Status)
	}
}

func TestLoadPersistedAgentStateRejectsCorruptFindingRecheckState(
	t *testing.T,
) {
	const path = "internal/recheck-state.go"
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		path,
		"Recheck state",
		"pending finding work survives restart",
	)
	recorded, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingReported,
				"independently confirmed pending issue",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	impacted, err := transitionReviewLedger(
		&recorded.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("impacted transitionReviewLedger() error = %v", err)
	}

	tests := []struct {
		name   string
		ledger ReviewLedger
		value  bool
	}{
		{
			name:   "pending work removed",
			ledger: impacted.Ledger,
			value:  false,
		},
		{
			name:   "unimpacted work added",
			ledger: recorded.Ledger,
			value:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := cloneReviewLedger(&test.ledger)
			ledger.Findings[0].NeedsRecheck = test.value
			logDir := t.TempDir()
			restored := &Orchestrator{
				cfg:    Config{LogDir: logDir},
				agents: NewAgentManager(),
			}
			payload := persistedStateFile{
				Version: persistedStateVersion,
				Agents: []persistedAgent{{
					ID:           "coding-agent-corrupt-recheck",
					Role:         RoleCoder,
					State:        StateWorking,
					ReviewLedger: ledger,
				}},
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			if err := os.MkdirAll(
				orchestratorStateDir(logDir),
				0o755,
			); err != nil {
				t.Fatalf("MkdirAll() error = %v", err)
			}
			if err := os.WriteFile(
				restored.agentStateFilePath(),
				body,
				0o600,
			); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			if err := restored.loadPersistedAgentState(); err == nil ||
				!strings.Contains(err.Error(), "recheck history") {
				t.Fatalf(
					"loadPersistedAgentState() error = %v, want recheck-history rejection",
					err,
				)
			}
		})
	}
}

func TestLoadPersistedAgentStateRejectsCorruptReopenPresence(
	t *testing.T,
) {
	const (
		path      = "internal/reopen-presence.go"
		unrelated = "internal/reopen-unrelated.go"
	)
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		path,
		"Reopen presence",
		"reopened provenance remains audit-bound",
	)
	recorded, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingReported,
				"independently confirmed issue before the fix",
				reviewLedgerBool(false),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}
	fixedFinding := cloneReviewCanonicalFinding(finding)
	fixedFinding.ExactSHA = testLedgerSecondSHA
	fixed, err := transitionReviewLedger(
		&recorded.Ledger,
		testReviewLedgerDelta(
			testLedgerFirstSHA,
			testLedgerSecondSHA,
			path,
		),
		testReviewLedgerCoverageRequirements(path),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				fixedFinding,
				ReviewLedgerFindingFixed,
				"fresh exact-SHA evidence verifies the fix",
				nil,
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("fixed transitionReviewLedger() error = %v", err)
	}
	reopenedFinding := cloneReviewCanonicalFinding(fixedFinding)
	reopenedFinding.ExactSHA = testLedgerThirdSHA
	reopened, err := transitionReviewLedger(
		&fixed.Ledger,
		testReviewLedgerDelta(
			testLedgerSecondSHA,
			testLedgerThirdSHA,
			unrelated,
		),
		testReviewLedgerCoverageRequirements(unrelated),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				reopenedFinding,
				ReviewLedgerFindingMissed,
				"fresh evidence shows the issue existed at the delta base",
				reviewLedgerBool(true),
			),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("reopen transitionReviewLedger() error = %v", err)
	}
	reopenedRecord := reopened.Ledger.Findings[0]
	latest := reopenedRecord.History[len(reopenedRecord.History)-1]
	if latest.PresentInDeltaBase == nil ||
		!*latest.PresentInDeltaBase ||
		reopenedRecord.Provenance !=
			ReviewLedgerFindingPreviouslyMissed {
		t.Fatalf("reopened finding provenance audit = %#v", reopenedRecord)
	}

	tests := []struct {
		name     string
		presence *bool
		want     string
	}{
		{
			name:     "presence removed",
			presence: nil,
			want:     "missing delta-base presence evidence",
		},
		{
			name:     "presence flipped",
			presence: reviewLedgerBool(false),
			want:     "provenance does not match delta-base presence evidence",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneReviewLedger(&reopened.Ledger)
			history := candidate.Findings[0].History
			history[len(history)-1].PresentInDeltaBase =
				test.presence
			if err := loadPersistedReviewLedgerForTest(
				t,
				candidate,
			); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"loadPersistedAgentState() error = %v, want containing %q",
					err,
					test.want,
				)
			}
		})
	}
}

func TestLoadPersistedAgentStateRejectsCorruptReviewLedgerDerivedValues(
	t *testing.T,
) {
	finding := testReviewLedgerFinding(
		t,
		testLedgerFirstSHA,
		"internal/corrupt.go",
		"Corrupt",
		"persisted identity remains canonical",
	)
	transitioned, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(
			testLedgerBaseSHA,
			testLedgerFirstSHA,
			"internal/corrupt.go",
		),
		testReviewLedgerCoverageRequirements(
			"internal/corrupt.go",
		),
		[]ReviewLedgerFindingObservation{
			testReviewLedgerFindingObservation(
				finding,
				ReviewLedgerFindingRejected,
				"independent verifier rejected the candidate",
				reviewLedgerBool(false),
			),
		},
		[]ReviewLedgerCoverageObservation{
			testReviewLedgerCoverageObservation(
				"internal/corrupt.go",
				ReviewCoveragePartial,
				"persisted partial coverage evidence",
			),
		},
	)
	if err != nil {
		t.Fatalf("transitionReviewLedger() error = %v", err)
	}

	tests := []struct {
		name    string
		corrupt func(*ReviewLedger)
		want    string
	}{
		{
			name: "finding fingerprint",
			corrupt: func(ledger *ReviewLedger) {
				fingerprint := strings.Repeat("a", 64)
				ledger.Findings[0].ID = "finding-" + fingerprint
				ledger.Findings[0].Fingerprint = fingerprint
				ledger.Findings[0].Finding.ID = "finding-" + fingerprint
				ledger.Findings[0].Finding.Fingerprint = fingerprint
			},
			want: "fingerprint does not match its canonical fields",
		},
		{
			name: "finding evidence digest",
			corrupt: func(ledger *ReviewLedger) {
				digest := strings.Repeat("b", 64)
				ledger.Findings[0].EvidenceDigest = digest
				history := ledger.Findings[0].History
				history[len(history)-1].EvidenceDigest = digest
			},
			want: "digest does not match the stored finding evidence",
		},
		{
			name: "finding delta-base presence removed",
			corrupt: func(ledger *ReviewLedger) {
				ledger.Findings[0].History[0].PresentInDeltaBase = nil
			},
			want: "missing delta-base presence evidence",
		},
		{
			name: "finding delta-base presence flipped",
			corrupt: func(ledger *ReviewLedger) {
				ledger.Findings[0].History[0].PresentInDeltaBase =
					reviewLedgerBool(true)
			},
			want: "provenance does not match delta-base presence evidence",
		},
		{
			name: "finding worker provenance removed",
			corrupt: func(ledger *ReviewLedger) {
				ledger.Findings[0].Finding.Provenance = nil
			},
			want: "worker provenance is missing",
		},
		{
			name: "finding worker provenance rewritten",
			corrupt: func(ledger *ReviewLedger) {
				ledger.Findings[0].Finding.Provenance[0].WorkerID =
					"rewritten-discovery-worker"
			},
			want: "digest does not match the stored finding evidence",
		},
		{
			name: "coverage evidence digest",
			corrupt: func(ledger *ReviewLedger) {
				digest := strings.Repeat("c", 64)
				ledger.Coverage[0].EvidenceDigest = digest
				history := ledger.Coverage[0].History
				history[len(history)-1].EvidenceDigest = digest
			},
			want: "digest does not match the stored coverage evidence",
		},
		{
			name: "coverage requirement description",
			corrupt: func(ledger *ReviewLedger) {
				ledger.Coverage[0].Description =
					"rewritten persisted acceptance criterion"
			},
			want: "requirement digest does not match",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ledger := cloneReviewLedger(&transitioned.Ledger)
			test.corrupt(ledger)
			logDir := t.TempDir()
			restored := &Orchestrator{
				cfg:    Config{LogDir: logDir},
				agents: NewAgentManager(),
			}
			payload := persistedStateFile{
				Version: persistedStateVersion,
				Agents: []persistedAgent{{
					ID:           "coding-agent-corrupt-ledger",
					Role:         RoleCoder,
					State:        StateWorking,
					ReviewLedger: ledger,
				}},
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			if err := os.MkdirAll(
				orchestratorStateDir(logDir),
				0o755,
			); err != nil {
				t.Fatalf("MkdirAll() error = %v", err)
			}
			if err := os.WriteFile(
				restored.agentStateFilePath(),
				body,
				0o600,
			); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			err = restored.loadPersistedAgentState()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"loadPersistedAgentState() error = %v, want containing %q",
					err,
					test.want,
				)
			}
		})
	}
}
