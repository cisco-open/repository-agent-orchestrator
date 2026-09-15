// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2026 Cisco Systems, Inc. and its affiliates

package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

const reviewReplaySchemaVersion = 1

type reviewReplayCorpus struct {
	SchemaVersion int                    `json:"schema_version"`
	Ownership     string                 `json:"ownership"`
	Scenarios     []reviewReplayScenario `json:"scenarios"`
}

type reviewReplayScenario struct {
	ID              string                    `json:"id"`
	HeadSHA         string                    `json:"head_sha"`
	SeededFindings  []reviewReplaySeed        `json:"seeded_findings"`
	Observations    []reviewReplayObservation `json:"observations"`
	Rounds          []reviewReplayRound       `json:"rounds"`
	UnresolvedWork  []string                  `json:"unresolved_work"`
	BudgetExhausted bool                      `json:"budget_exhausted"`
	Workers         reviewReplayWorkers       `json:"workers"`
	ExpectedOutcome string                    `json:"expected_outcome"`
}

type reviewReplaySeed struct {
	ID     string `json:"id"`
	Origin string `json:"origin"`
}

type reviewReplayObservation struct {
	Pass       int      `json:"pass"`
	Lane       string   `json:"lane"`
	FindingIDs []string `json:"finding_ids"`
}

type reviewReplayRound struct {
	Pass                   int      `json:"pass"`
	Executed               bool     `json:"executed"`
	CompletedLanes         []string `json:"completed_lanes"`
	Outcome                string   `json:"outcome"`
	SynthesisAfterFindings bool     `json:"synthesis_after_findings"`
}

type reviewReplayWorkers struct {
	Allocated int `json:"allocated"`
	Completed int `json:"completed"`
}

type reviewReplayEvaluation struct {
	Scenario                    string   `json:"scenario"`
	Outcome                     string   `json:"outcome"`
	SeededMaterialFindings      int      `json:"seeded_material_findings"`
	FirstVerdictFindings        int      `json:"first_verdict_findings"`
	FirstVerdictRecallNumerator int      `json:"first_verdict_recall_numerator"`
	FirstVerdictRecallDenom     int      `json:"first_verdict_recall_denominator"`
	MissedFindingIDs            []string `json:"missed_finding_ids"`
	LaterMissedOriginalIDs      []string `json:"later_missed_original_ids"`
	CorrectionIntroducedMisses  []string `json:"correction_introduced_misses"`
	UnresolvedWork              []string `json:"unresolved_work"`
	WorkersAllocated            int      `json:"workers_allocated"`
	WorkersCompleted            int      `json:"workers_completed"`
}

func TestAntiDripReplayGate(t *testing.T) {
	raw, err := os.ReadFile("testdata/review_replay_cases.json")
	if err != nil {
		t.Fatalf("read replay corpus: %v", err)
	}
	for _, forbidden := range []string{
		"github.com", "http://", "https://", "/Users/", "internal-org",
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("replay corpus contains external identifying material %q", forbidden)
		}
	}
	var corpus reviewReplayCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("decode replay corpus: %v", err)
	}
	if corpus.SchemaVersion != reviewReplaySchemaVersion ||
		corpus.Ownership != "repository-agent-orchestrator" ||
		len(corpus.Scenarios) == 0 {
		t.Fatalf("invalid repository-owned replay corpus metadata: %#v", corpus)
	}
	productionBase := productionReplayBaseCycle(t)
	for _, scenario := range corpus.Scenarios {
		evaluation, err := evaluateReviewReplay(scenario)
		if err != nil {
			t.Fatalf("replay %q: %v", scenario.ID, err)
		}
		encoded, _ := json.Marshal(evaluation)
		t.Logf("anti-drip replay: %s", encoded)
		if evaluation.Outcome != scenario.ExpectedOutcome {
			t.Fatalf("replay %q outcome = %q, want %q: %s", scenario.ID, evaluation.Outcome, scenario.ExpectedOutcome, encoded)
		}
		assertProductionReplayOutcome(
			t,
			productionBase,
			scenario,
			evaluation,
		)
		if len(evaluation.MissedFindingIDs) != 0 ||
			len(evaluation.LaterMissedOriginalIDs) != 0 ||
			len(evaluation.CorrectionIntroducedMisses) != 0 {
			t.Fatalf("replay %q regressed first-verdict recall: %s", scenario.ID, encoded)
		}
	}
}

func TestAntiDripReplayMakesMissesAttributable(t *testing.T) {
	scenario := reviewReplayScenario{
		ID:      "attribution-check",
		HeadSHA: strings.Repeat("a", canonicalGitObjectIDLength),
		SeededFindings: []reviewReplaySeed{
			{ID: "original-miss", Origin: "original"},
			{ID: "correction-miss", Origin: "correction_introduced"},
		},
		Observations: []reviewReplayObservation{{
			Pass: 2, Lane: reviewSynthesisLane,
			FindingIDs: []string{"original-miss"},
		}},
		Rounds: []reviewReplayRound{{
			Pass: 2, Executed: true,
			CompletedLanes:         []string{reviewSynthesisLane},
			Outcome:                "material",
			SynthesisAfterFindings: true,
		}},
		Workers: reviewReplayWorkers{Allocated: 1, Completed: 1},
	}
	evaluation, err := evaluateReviewReplay(scenario)
	if err != nil {
		t.Fatalf("evaluate attribution replay: %v", err)
	}
	if strings.Join(evaluation.MissedFindingIDs, ",") !=
		"correction-miss,original-miss" ||
		strings.Join(evaluation.LaterMissedOriginalIDs, ",") !=
			"original-miss" ||
		strings.Join(evaluation.CorrectionIntroducedMisses, ",") !=
			"correction-miss" {
		t.Fatalf("unattributed replay misses: %#v", evaluation)
	}
}

func evaluateReviewReplay(
	scenario reviewReplayScenario,
) (reviewReplayEvaluation, error) {
	evaluation := reviewReplayEvaluation{
		Scenario:                   scenario.ID,
		SeededMaterialFindings:     len(scenario.SeededFindings),
		FirstVerdictRecallDenom:    len(scenario.SeededFindings),
		MissedFindingIDs:           []string{},
		LaterMissedOriginalIDs:     []string{},
		CorrectionIntroducedMisses: []string{},
		UnresolvedWork:             append([]string{}, scenario.UnresolvedWork...),
		WorkersAllocated:           scenario.Workers.Allocated,
		WorkersCompleted:           scenario.Workers.Completed,
	}
	if strings.TrimSpace(scenario.ID) == "" ||
		validateCanonicalGitObjectID(scenario.HeadSHA) != nil ||
		scenario.Workers.Allocated < 0 ||
		scenario.Workers.Completed < 0 ||
		scenario.Workers.Completed > scenario.Workers.Allocated {
		return evaluation, fmt.Errorf("scenario identity or worker accounting is invalid")
	}
	seeds := make(map[string]string, len(scenario.SeededFindings))
	for _, seed := range scenario.SeededFindings {
		if seed.ID == "" || (seed.Origin != "original" &&
			seed.Origin != "correction_introduced") {
			return evaluation, fmt.Errorf("seeded finding %#v is invalid", seed)
		}
		if _, duplicate := seeds[seed.ID]; duplicate {
			return evaluation, fmt.Errorf("seeded finding %q is duplicated", seed.ID)
		}
		seeds[seed.ID] = seed.Origin
	}
	firstVerdict := make(map[string]struct{})
	later := make(map[string]struct{})
	lastFindingPass := 0
	for _, observation := range scenario.Observations {
		if observation.Pass <= 0 || strings.TrimSpace(observation.Lane) == "" {
			return evaluation, fmt.Errorf("observation %#v is invalid", observation)
		}
		for _, findingID := range observation.FindingIDs {
			if _, seeded := seeds[findingID]; !seeded {
				return evaluation, fmt.Errorf("observation reports unseeded finding %q", findingID)
			}
			if observation.Pass == 1 {
				firstVerdict[findingID] = struct{}{}
			} else {
				later[findingID] = struct{}{}
			}
			if observation.Pass > lastFindingPass {
				lastFindingPass = observation.Pass
			}
		}
	}
	evaluation.FirstVerdictFindings = len(firstVerdict)
	for findingID, origin := range seeds {
		if _, found := firstVerdict[findingID]; found {
			evaluation.FirstVerdictRecallNumerator++
			continue
		}
		evaluation.MissedFindingIDs = append(evaluation.MissedFindingIDs, findingID)
		if _, foundLater := later[findingID]; foundLater && origin == "original" {
			evaluation.LaterMissedOriginalIDs = append(evaluation.LaterMissedOriginalIDs, findingID)
		}
		if origin == "correction_introduced" {
			evaluation.CorrectionIntroducedMisses = append(evaluation.CorrectionIntroducedMisses, findingID)
		}
	}
	hasValidQuietRound := false
	hasFreshSynthesis := lastFindingPass == 0
	for _, round := range scenario.Rounds {
		if round.Pass <= 0 {
			return evaluation, fmt.Errorf("round %#v is invalid", round)
		}
		hasSynthesis := stringSliceContains(round.CompletedLanes, reviewSynthesisLane)
		if round.Pass >= lastFindingPass && hasSynthesis &&
			round.SynthesisAfterFindings {
			hasFreshSynthesis = true
		}
		if round.Outcome != "quiet" {
			continue
		}
		if !round.Executed || len(round.CompletedLanes) == 0 || !hasSynthesis {
			evaluation.UnresolvedWork = append(
				evaluation.UnresolvedWork,
				fmt.Sprintf("round:%d:zero-work-quiet", round.Pass),
			)
			continue
		}
		hasValidQuietRound = true
	}
	if !hasFreshSynthesis {
		evaluation.UnresolvedWork = append(
			evaluation.UnresolvedWork,
			"synthesis:missing-after-last-finding",
		)
	}
	sort.Strings(evaluation.MissedFindingIDs)
	sort.Strings(evaluation.LaterMissedOriginalIDs)
	sort.Strings(evaluation.CorrectionIntroducedMisses)
	sort.Strings(evaluation.UnresolvedWork)
	switch {
	case scenario.BudgetExhausted || len(evaluation.UnresolvedWork) != 0:
		evaluation.Outcome = "incomplete"
	case len(firstVerdict) != 0:
		evaluation.Outcome = "changes_required"
	case hasValidQuietRound:
		evaluation.Outcome = "approved"
	default:
		evaluation.Outcome = "incomplete"
	}
	return evaluation, nil
}

func productionReplayBaseCycle(t *testing.T) *ReviewCycleState {
	t.Helper()
	harness := newTerminalReviewVerdictHarness(t)
	base := cloneReviewCycle(harness.reviewer.ReviewCycle)
	base.CanonicalFindings = nil
	base.Convergence.FindingVerifications = nil
	base.UnresolvedCoverage = nil
	base.Convergence.ChallengeAssignments = nil
	if !base.ApprovalEligible() {
		t.Fatal("production replay base cycle is not approval eligible")
	}
	return base
}

func assertProductionReplayOutcome(
	t *testing.T,
	base *ReviewCycleState,
	scenario reviewReplayScenario,
	evaluation reviewReplayEvaluation,
) {
	t.Helper()
	cycle := cloneReviewCycle(base)
	switch evaluation.Outcome {
	case "changes_required":
		for _, seed := range scenario.SeededFindings {
			cycle.CanonicalFindings = append(
				cycle.CanonicalFindings,
				ReviewCanonicalFinding{
					ID:                seed.ID,
					ExactSHA:          cycle.HeadSHA,
					Summary:           "seeded material defect",
					Location:          ReviewFindingLocation{Path: "synthetic.go", Symbol: seed.ID},
					BehavioralPath:    "synthetic replay path",
					ViolatedInvariant: "seeded blocker must be reported",
				},
			)
			cycle.Convergence.FindingVerifications = append(
				cycle.Convergence.FindingVerifications,
				ReviewFindingVerification{
					FindingID: seed.ID,
					ExactSHA:  cycle.HeadSHA,
					Status:    ReviewFindingVerificationConfirmed,
				},
			)
		}
	case "incomplete":
		cycle.Convergence.Status = ReviewConvergenceUnresolved
		if evaluation.WorkersCompleted == 0 {
			cycle.ArtifactReceipts = nil
		}
		unresolvedID := "replay-unresolved-work"
		if len(evaluation.UnresolvedWork) > 0 {
			unresolvedID = evaluation.UnresolvedWork[0]
		}
		cycle.UnresolvedCoverage = []ReviewCoverageGap{{
			ID:          unresolvedID,
			Description: "synthetic replay work remains unresolved",
			Status:      ReviewCoverageGapMissing,
		}}
	case "approved":
	default:
		t.Fatalf("replay %q has unsupported outcome %q", scenario.ID, evaluation.Outcome)
	}

	aggregation, err := buildReviewVerdictAggregation(cycle)
	if evaluation.Outcome == "incomplete" {
		if evaluation.WorkersCompleted == 0 {
			if !errors.Is(err, errReviewResultNotPublishable) {
				t.Fatalf("replay %q zero-work aggregation error = %v", scenario.ID, err)
			}
			return
		}
		if err != nil || aggregation.ResultState != ReviewCycleResultPartialNoFindings {
			t.Fatalf("replay %q partial aggregation = %#v, %v", scenario.ID, aggregation, err)
		}
		body, verdict := formatReviewVerdictPublicationBody(
			"replay-reviewer", "replay-publication", strings.Repeat("a", 64), aggregation,
		)
		if verdict != "" || !strings.Contains(body, "## Partial review report") {
			t.Fatalf("replay %q partial report = %q, %s", scenario.ID, verdict, body)
		}
		return
	}
	if err != nil {
		t.Fatalf("replay %q production verdict aggregation: %v", scenario.ID, err)
	}
	wantProductionResult := map[string]ReviewCycleResultState{
		"approved":         ReviewCycleResultCompleteClean,
		"changes_required": ReviewCycleResultCompleteWithFindings,
	}[evaluation.Outcome]
	body, verdict := formatReviewVerdictPublicationBody(
		"replay-reviewer",
		"replay-publication",
		strings.Repeat("a", 64),
		aggregation,
	)
	wantHeader := map[string]string{
		"approved":         "## Review approved",
		"changes_required": "## Review changes required",
	}[evaluation.Outcome]
	wantVerdict := ReviewVerdictNeedsChanges
	if evaluation.Outcome == "approved" {
		wantVerdict = ReviewVerdictThumbsUp
	}
	if aggregation.ResultState != wantProductionResult ||
		verdict != wantVerdict || !strings.Contains(body, wantHeader) {
		t.Fatalf(
			"replay %q diverged from production publication: result=%q verdict=%q body=%s",
			scenario.ID,
			aggregation.ResultState,
			verdict,
			body,
		)
	}
}
