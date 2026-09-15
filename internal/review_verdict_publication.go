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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v90/github"
)

const (
	reviewVerdictPublicationSchemaVersion = 4
	reviewVerdictPublicationIDPrefix      = "convergent-review-verdict-v4-"
	reviewVerdictPublicationMaxAttempts   = 3
	reviewVerdictPublicationMaxBodyBytes  = 65536
	reviewReportMaxListItems              = 32

	reviewVerdictPublicationIDMarker = "CODEX_REVIEW_PUBLICATION_ID"
	reviewVerdictAggregationMarker   = "CODEX_REVIEW_AGGREGATION_SHA256"
	reviewVerdictBodyDigestMarker    = "CODEX_REVIEW_BODY_SHA256"
)

var errReviewResultNotPublishable = errors.New(
	"review cycle has no publishable result",
)

type ReviewVerdictPublicationStatus string

const (
	ReviewVerdictPublicationPending   ReviewVerdictPublicationStatus = "pending"
	ReviewVerdictPublicationPublished ReviewVerdictPublicationStatus = "published"
)

// ReviewVerdictPublication is the durable publication intent for one exact
// PR head. The body is prepared and persisted before any GitHub write, so a
// restart always retries the same aggregation under the same identity.
type ReviewVerdictPublication struct {
	SchemaVersion     int                            `json:"schema_version"`
	ID                string                         `json:"id"`
	ReviewerID        string                         `json:"reviewer_id"`
	HeadSHA           string                         `json:"head_sha"`
	Verdict           ReviewVerdict                  `json:"verdict,omitempty"`
	ResultState       ReviewCycleResultState         `json:"result_state"`
	AggregationDigest string                         `json:"aggregation_digest"`
	BodyDigest        string                         `json:"body_digest"`
	Body              string                         `json:"body"`
	Status            ReviewVerdictPublicationStatus `json:"status"`
	PreparedAt        time.Time                      `json:"prepared_at"`
	PublishedAt       time.Time                      `json:"published_at,omitempty"`
	CommentID         int64                          `json:"comment_id,omitempty"`
	CommentURL        string                         `json:"comment_url,omitempty"`
}

type reviewVerdictFinding struct {
	ID                string `json:"id"`
	Summary           string `json:"summary"`
	Location          string `json:"location"`
	BehavioralPath    string `json:"behavioral_path"`
	ViolatedInvariant string `json:"violated_invariant"`
}

type reviewReportSection struct {
	Heading string   `json:"heading"`
	Items   []string `json:"items"`
}

type reviewVerdictAggregation struct {
	SchemaVersion   int                    `json:"schema_version"`
	HeadSHA         string                 `json:"head_sha"`
	ResultState     ReviewCycleResultState `json:"result_state"`
	AcceptedResults int                    `json:"accepted_results"`
	Findings        []reviewVerdictFinding `json:"findings"`
	Report          []reviewReportSection  `json:"report,omitempty"`
}

type reviewVerdictPublicationMutation struct {
	reviewerID               string
	previous                 *ReviewVerdictPublication
	previousLastActivityTime time.Time
	updatedAt                time.Time
}

func cloneReviewVerdictPublication(
	publication *ReviewVerdictPublication,
) *ReviewVerdictPublication {
	if publication == nil {
		return nil
	}
	clone := *publication
	return &clone
}

func reviewCycleHasTerminalVerdict(cycle *ReviewCycleState) bool {
	if cycle == nil {
		return false
	}
	if cycle.LimitTransition != nil {
		return true
	}
	if cycle.Plan == nil {
		return false
	}
	if cycle.Convergence == nil {
		return false
	}
	switch cycle.Convergence.Status {
	case ReviewConvergenceConverged,
		ReviewConvergenceChangesRequired,
		ReviewConvergenceMaxRounds,
		ReviewConvergenceUnresolved:
		return true
	default:
		return false
	}
}

func reviewCycleHasPublishableReport(cycle *ReviewCycleState) bool {
	if cycle == nil || cycle.Stale || cycle.Plan == nil ||
		!reviewCycleHasTerminalVerdict(cycle) {
		return false
	}
	return classifyReviewCycleResult(cycle) !=
		ReviewCycleResultNoReviewPossible
}

func reviewCycleNeedsReportPublication(cycle *ReviewCycleState) bool {
	if !reviewCycleHasPublishableReport(cycle) {
		return false
	}
	return cycle.VerdictPublication == nil ||
		cycle.VerdictPublication.Status != ReviewVerdictPublicationPublished ||
		cycle.VerdictPublication.ResultState ==
			ReviewCycleResultPartialNoFindings
}

// reviewVerdictPublicationIdentity computes the stable comment identity for
// one reviewer's verdict. It folds in aggregationDigest so two
// independent reviewer instances that terminate on the same head SHA but
// reach genuinely different verdicts (different confirmed findings -- LLM
// review is not guaranteed to reproduce identically across runs) get
// distinct identities instead of colliding on one derived purely from
// (repo, PR, head SHA). Callers that already hold a persisted publication
// for this reviewer must reuse its ID verbatim rather than calling this
// again: see prepareReviewVerdictPublication for why recomputing here on
// every attempt is unsafe once an ID has been persisted.
func reviewVerdictPublicationIdentity(
	repoOwner string,
	repoName string,
	prNumber int,
	headSHA string,
	aggregationDigest string,
) (string, error) {
	repoOwner = strings.ToLower(strings.TrimSpace(repoOwner))
	repoName = strings.ToLower(strings.TrimSpace(repoName))
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	aggregationDigest = strings.ToLower(strings.TrimSpace(aggregationDigest))
	if repoOwner == "" || repoName == "" || prNumber <= 0 {
		return "", errors.New(
			"review verdict publication repository and PR identity are incomplete",
		)
	}
	if err := validateCanonicalGitObjectID(headSHA); err != nil {
		return "", fmt.Errorf(
			"review verdict publication head SHA is invalid: %w",
			err,
		)
	}
	if err := validateReviewPolicyFingerprint(aggregationDigest); err != nil {
		return "", fmt.Errorf(
			"review verdict publication aggregation digest is invalid: %w",
			err,
		)
	}
	raw := strings.Join([]string{
		"repository-agent-orchestrator",
		"convergent-review-verdict",
		"v4",
		repoOwner,
		repoName,
		strconv.Itoa(prNumber),
		headSHA,
		aggregationDigest,
	}, "\n")
	sum := sha256.Sum256([]byte(raw))
	return reviewVerdictPublicationIDPrefix + hex.EncodeToString(sum[:]), nil
}

func reviewVerdictLocation(location ReviewFindingLocation) string {
	path := reviewVerdictText(location.Path)
	switch {
	case path == "":
		return "(unknown location)"
	case location.StartLine > 0 && location.EndLine > location.StartLine:
		return fmt.Sprintf(
			"%s:%d-%d",
			path,
			location.StartLine,
			location.EndLine,
		)
	case location.StartLine > 0:
		return fmt.Sprintf("%s:%d", path, location.StartLine)
	default:
		return path
	}
}

func reviewVerdictText(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func boundedReviewReportItems(items []string) []string {
	items = uniqueSortedStrings(items)
	if len(items) <= reviewReportMaxListItems {
		return items
	}
	omitted := len(items) - reviewReportMaxListItems + 1
	return append(
		items[:reviewReportMaxListItems-1],
		fmt.Sprintf("%d additional item(s) omitted", omitted),
	)
}

// reviewReportDetails exposes only validated scheduler identifiers and enums.
// Raw artifacts, prompts, evidence, worker IDs, and failure details stay private.
func reviewReportSections(
	cycle *ReviewCycleState,
) []reviewReportSection {
	var completed, unresolved, failed, diagnostics []string
	for _, receipt := range cycle.ArtifactReceipts {
		completed = append(completed, fmt.Sprintf(
			"%s pass %d / %s", receipt.Phase, receipt.Pass, receipt.Lane,
		))
	}
	for _, gap := range cycle.UnresolvedCoverage {
		unresolved = append(unresolved, fmt.Sprintf(
			"%s / %s: %s", gap.Kind, gap.RequirementID, gap.Status,
		))
	}
	for _, pass := range cycle.DiscoveryPasses {
		for _, lane := range pass.Lanes {
			if lane.Status != ReviewDiscoveryLaneFailed ||
				lane.RecoveredBySynthesis {
				continue
			}
			failed = append(failed, fmt.Sprintf(
				"discovery pass %d / %s: %s",
				pass.Pass,
				lane.Lane,
				lane.FailureCode,
			))
		}
	}
	if limit := cycle.LimitTransition; limit != nil {
		for _, item := range limit.UnresolvedEvidence {
			unresolved = append(unresolved, fmt.Sprintf(
				"%s / %s: %s", item.Kind, item.ID, item.Status,
			))
		}
		diagnostics = append(diagnostics, fmt.Sprintf(
			"%s limit reached (%d/%d %s); configured action: %s",
			limit.Kind, limit.Actual, limit.Maximum, limit.Unit, limit.Action,
		))
	}
	if cycle.Convergence != nil {
		status := cycle.Convergence.Status
		if status != ReviewConvergenceConverged &&
			status != ReviewConvergenceChangesRequired {
			unresolved = append(unresolved, "review convergence: "+string(status))
		}
		for _, assignment := range cycle.Convergence.VerificationAssignments {
			if assignment.Superseded ||
				assignment.Status != ReviewVerificationFailed {
				continue
			}
			unresolved = append(unresolved, fmt.Sprintf(
				"finding / %s: verification %s",
				assignment.FindingID,
				assignment.FailureCode,
			))
			failed = append(failed, fmt.Sprintf(
				"verification / %s: %s",
				assignment.FindingID,
				assignment.FailureCode,
			))
		}
		for _, assignment := range cycle.Convergence.ChallengeAssignments {
			if assignment.Superseded ||
				assignment.Status != ReviewChallengeFailed {
				continue
			}
			dependency := assignment.Target.ID
			if assignment.Target.RequirementID != "" {
				dependency = assignment.Target.RequirementID
			}
			unresolved = append(unresolved, fmt.Sprintf(
				"%s / %s: challenge failed",
				assignment.Target.Kind,
				dependency,
			))
			failed = append(failed, fmt.Sprintf(
				"challenge / %s / %s: failed",
				assignment.Target.Kind,
				assignment.Target.ID,
			))
		}
	}
	for _, worker := range cycle.WorkerOwnerships {
		if worker.Lifecycle != ReviewWorkerFailed {
			continue
		}
		status := string(worker.Lifecycle)
		if worker.Failure != nil {
			status += "/" + string(worker.Failure.Kind)
		}
		failed = append(failed, reviewWorkerLogicalAssignment(worker.Identity)+": "+status)
	}
	for _, failure := range cycle.ArtifactFailures {
		diagnostics = append(diagnostics, fmt.Sprintf(
			"artifact intake: %s/%s", failure.Class, failure.Code,
		))
	}
	diagnostics = append(
		diagnostics,
		"Retry unresolved or failed scopes on this exact SHA; if a failure recurs, inspect worker runtime and artifact-validation logs.",
	)
	return []reviewReportSection{
		{Heading: "Completed scopes", Items: boundedReviewReportItems(completed)},
		{Heading: "Unresolved scopes", Items: boundedReviewReportItems(unresolved)},
		{Heading: "Failed assignments", Items: boundedReviewReportItems(failed)},
		{Heading: "Infrastructure diagnostics", Items: boundedReviewReportItems(diagnostics)},
	}
}

func buildReviewVerdictAggregation(
	cycle *ReviewCycleState,
) (reviewVerdictAggregation, error) {
	if !reviewCycleHasPublishableReport(cycle) {
		return reviewVerdictAggregation{}, errReviewResultNotPublishable
	}
	aggregation := reviewVerdictAggregation{
		SchemaVersion:   reviewVerdictPublicationSchemaVersion,
		HeadSHA:         cycle.HeadSHA,
		ResultState:     classifyReviewCycleResult(cycle),
		AcceptedResults: len(cycle.ArtifactReceipts),
		Findings:        []reviewVerdictFinding{},
	}
	if aggregation.ResultState == ReviewCycleResultPartialNoFindings ||
		aggregation.ResultState == ReviewCycleResultPartialWithFindings {
		aggregation.Report = reviewReportSections(cycle)
	}

	decisions := make(
		map[string]ReviewFindingVerification,
		0,
	)
	if cycle.Convergence != nil {
		for _, decision := range cycle.Convergence.FindingVerifications {
			decisions[decision.FindingID] = decision
		}
	}
	for _, finding := range cycle.CanonicalFindings {
		if finding.ExactSHA != cycle.HeadSHA {
			continue
		}
		decision, ok := decisions[finding.ID]
		switch {
		case ok &&
			decision.ExactSHA == cycle.HeadSHA &&
			decision.Status == ReviewFindingVerificationConfirmed:
			aggregation.Findings = append(
				aggregation.Findings,
				reviewVerdictFinding{
					ID:                finding.ID,
					Summary:           reviewVerdictText(finding.Summary),
					Location:          reviewVerdictLocation(finding.Location),
					BehavioralPath:    reviewVerdictText(finding.BehavioralPath),
					ViolatedInvariant: reviewVerdictText(finding.ViolatedInvariant),
				},
			)
		case ok &&
			decision.ExactSHA == cycle.HeadSHA &&
			decision.Status == ReviewFindingVerificationRejected:
			continue
		default:
			continue
		}
	}
	sort.Slice(aggregation.Findings, func(i, j int) bool {
		return aggregation.Findings[i].ID < aggregation.Findings[j].ID
	})

	switch aggregation.ResultState {
	case ReviewCycleResultCompleteClean:
		if !cycle.ApprovalEligible() || len(aggregation.Findings) != 0 {
			return reviewVerdictAggregation{}, errReviewResultNotPublishable
		}
	case ReviewCycleResultCompleteWithFindings,
		ReviewCycleResultPartialWithFindings:
		if len(aggregation.Findings) == 0 {
			return reviewVerdictAggregation{}, errReviewResultNotPublishable
		}
	case ReviewCycleResultPartialNoFindings:
		if len(aggregation.Findings) != 0 {
			return reviewVerdictAggregation{}, errReviewResultNotPublishable
		}
	default:
		return reviewVerdictAggregation{}, errReviewResultNotPublishable
	}
	return aggregation, nil
}

func digestReviewVerdictAggregation(
	aggregation reviewVerdictAggregation,
) (string, error) {
	body, err := json.Marshal(aggregation)
	if err != nil {
		return "", fmt.Errorf(
			"failed to canonicalize review verdict aggregation: %w",
			err,
		)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func digestReviewVerdictBody(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func reviewVerdictForResult(result ReviewCycleResultState) ReviewVerdict {
	switch result {
	case ReviewCycleResultCompleteClean:
		return ReviewVerdictThumbsUp
	case ReviewCycleResultCompleteWithFindings,
		ReviewCycleResultPartialWithFindings:
		return ReviewVerdictNeedsChanges
	default:
		return ""
	}
}

func reviewVerdictBodyMatches(body string, expected ReviewVerdict) bool {
	if expected == "" {
		_, present := reviewVerdictMarkerValue(body, "CODEX_VERDICT")
		return !present
	}
	verdict, ok := parseReviewVerdict(body)
	return ok && verdict == expected
}

func renderReviewVerdictPublicationBody(
	reviewerID string,
	publicationID string,
	aggregationDigest string,
	aggregation reviewVerdictAggregation,
) (string, ReviewVerdict) {
	verdict := reviewVerdictForResult(aggregation.ResultState)

	var body strings.Builder
	body.WriteString("> Generated by Repository Agent Orchestrator.\n\n")
	heading := "Review changes required"
	var summary string
	switch aggregation.ResultState {
	case ReviewCycleResultCompleteClean:
		heading = "Review approved"
		summary = fmt.Sprintf(
			"No patch-caused findings were confirmed for `%s`.",
			aggregation.HeadSHA,
		)
	case ReviewCycleResultCompleteWithFindings:
		summary = fmt.Sprintf(
			"The review confirmed %d patch-caused issue(s) for `%s`.",
			len(aggregation.Findings),
			aggregation.HeadSHA,
		)
	case ReviewCycleResultPartialWithFindings:
		heading += " — partial coverage"
		summary = fmt.Sprintf(
			"The accepted review work independently confirmed %d patch-caused issue(s) for `%s`. Mandatory coverage is incomplete, but these findings still require correction.",
			len(aggregation.Findings),
			aggregation.HeadSHA,
		)
	case ReviewCycleResultPartialNoFindings:
		heading = "Partial review report"
		summary = fmt.Sprintf(
			"The review accepted %d trusted result(s) for `%s` without independently confirming a patch-caused finding. Mandatory coverage is incomplete; this is neither an approval nor a clean-review result.",
			aggregation.AcceptedResults,
			aggregation.HeadSHA,
		)
	}
	fmt.Fprintf(&body, "## %s\n\n%s", heading, summary)
	if verdict == ReviewVerdictNeedsChanges && len(aggregation.Findings) > 0 {
		body.WriteString("\n\n### Findings\n")
		for _, finding := range aggregation.Findings {
			fmt.Fprintf(
				&body,
				"\n- **%s** (`%s`)\n  - Behavioral path: %s\n  - Violated invariant: %s",
				finding.Summary,
				finding.Location,
				finding.BehavioralPath,
				finding.ViolatedInvariant,
			)
		}
	}
	if aggregation.ResultState == ReviewCycleResultPartialNoFindings ||
		aggregation.ResultState == ReviewCycleResultPartialWithFindings {
		for _, section := range aggregation.Report {
			fmt.Fprintf(&body, "\n\n### %s\n", section.Heading)
			if len(section.Items) == 0 {
				body.WriteString("\n- None recorded.")
			}
			for _, item := range section.Items {
				fmt.Fprintf(&body, "\n- %s", item)
			}
		}
	}
	fmt.Fprintf(
		&body,
		"\n\n<!--\nCODEX_AGENT_ID: %s\n"+
			"CODEX_AGENT_ROLE: reviewer\n"+
			"CODEX_REVIEWED_SHA: %s\n"+
			"%s: %s\n"+
			"%s: %s\n",
		reviewerID,
		aggregation.HeadSHA,
		reviewVerdictPublicationIDMarker,
		publicationID,
		reviewVerdictAggregationMarker,
		aggregationDigest,
	)
	if verdict != "" {
		fmt.Fprintf(&body, "CODEX_VERDICT: %s\n", verdict)
	}
	bodyDigest := digestReviewVerdictBody(body.String())
	fmt.Fprintf(
		&body,
		"%s: %s\n-->\n",
		reviewVerdictBodyDigestMarker,
		bodyDigest,
	)
	return body.String(), verdict
}

func formatReviewVerdictPublicationBody(
	reviewerID string,
	publicationID string,
	aggregationDigest string,
	aggregation reviewVerdictAggregation,
) (string, ReviewVerdict) {
	body, verdict := renderReviewVerdictPublicationBody(
		reviewerID,
		publicationID,
		aggregationDigest,
		aggregation,
	)
	return body, verdict
}

func prepareReviewVerdictPublication(
	reviewer Agent,
	repoOwner string,
	repoName string,
	now time.Time,
) (ReviewVerdictPublication, error) {
	if reviewer.Role != RoleReviewer || reviewer.ReviewCycle == nil ||
		reviewer.PRNumber <= 0 || strings.TrimSpace(reviewer.ID) == "" ||
		now.IsZero() {
		return ReviewVerdictPublication{}, errors.New(
			"review verdict publication requires an active reviewer, PR, and preparation time",
		)
	}
	aggregation, err := buildReviewVerdictAggregation(reviewer.ReviewCycle)
	if err != nil {
		return ReviewVerdictPublication{}, err
	}
	aggregationDigest, err := digestReviewVerdictAggregation(aggregation)
	if err != nil {
		return ReviewVerdictPublication{}, err
	}
	// If a publication has already been persisted for this reviewer, its ID
	// must be reused verbatim rather than recomputed here, regardless of
	// which identity formula (or which formula version) originally produced
	// it. This function runs fresh on every publication attempt, including
	// retries of an already-in-flight reviewer; prepareReviewVerdictPublication
	// (the AgentManager method) hard-fails if a freshly computed ID doesn't
	// match what's already persisted, on the theory that an ID drifting out
	// from under a reviewer mid-flight is corruption, not something to
	// silently paper over. That means any future change to
	// reviewVerdictPublicationIdentity's formula is only safe to apply to a
	// reviewer that has never yet
	// persisted a publication -- otherwise every reviewer already in flight
	// at deploy time would immediately and permanently fail this check on
	// its very next reconciliation attempt.
	publicationID := ""
	if existing := reviewer.ReviewCycle.VerdictPublication; existing != nil {
		publicationID = existing.ID
	} else {
		publicationID, err = reviewVerdictPublicationIdentity(
			repoOwner,
			repoName,
			reviewer.PRNumber,
			reviewer.ReviewCycle.HeadSHA,
			aggregationDigest,
		)
		if err != nil {
			return ReviewVerdictPublication{}, err
		}
	}
	body, verdict := formatReviewVerdictPublicationBody(
		reviewer.ID,
		publicationID,
		aggregationDigest,
		aggregation,
	)
	if len(body) > reviewVerdictPublicationMaxBodyBytes {
		return ReviewVerdictPublication{}, errors.New(
			"review verdict publication metadata exceeds the GitHub comment limit",
		)
	}
	return ReviewVerdictPublication{
		SchemaVersion:     reviewVerdictPublicationSchemaVersion,
		ID:                publicationID,
		ReviewerID:        reviewer.ID,
		HeadSHA:           reviewer.ReviewCycle.HeadSHA,
		Verdict:           verdict,
		ResultState:       aggregation.ResultState,
		AggregationDigest: aggregationDigest,
		BodyDigest:        digestReviewVerdictBody(body),
		Body:              body,
		Status:            ReviewVerdictPublicationPending,
		PreparedAt:        now.UTC(),
	}, nil
}

func reviewVerdictMarkerValue(
	body string,
	marker string,
) (string, bool) {
	prefix := marker + ":"
	value := ""
	found := false
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		candidate := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		if candidate == "" || (found && candidate != value) {
			return "", false
		}
		value = candidate
		found = true
	}
	return value, found
}

func validateReviewVerdictPublication(cycle *ReviewCycleState) error {
	if cycle == nil || cycle.VerdictPublication == nil {
		return nil
	}
	publication := cycle.VerdictPublication
	if !cycle.Stale && !reviewCycleHasPublishableReport(cycle) {
		return errors.New(
			"publication exists before a final review result",
		)
	}
	if publication.SchemaVersion != reviewVerdictPublicationSchemaVersion {
		return fmt.Errorf(
			"schema version %d is unsupported",
			publication.SchemaVersion,
		)
	}
	if !strings.HasPrefix(
		publication.ID,
		reviewVerdictPublicationIDPrefix,
	) || validateReviewPolicyFingerprint(
		strings.TrimPrefix(
			publication.ID,
			reviewVerdictPublicationIDPrefix,
		),
	) != nil {
		return errors.New("publication identity is invalid")
	}
	if strings.TrimSpace(publication.ReviewerID) == "" ||
		publication.HeadSHA != cycle.HeadSHA ||
		!supportedReviewCycleResultState(publication.ResultState) ||
		publication.ResultState == ReviewCycleResultNoReviewPossible ||
		publication.Verdict != reviewVerdictForResult(publication.ResultState) ||
		validateReviewPolicyFingerprint(publication.AggregationDigest) != nil ||
		validateReviewPolicyFingerprint(publication.BodyDigest) != nil ||
		strings.TrimSpace(publication.Body) == "" ||
		len(publication.Body) > reviewVerdictPublicationMaxBodyBytes ||
		publication.BodyDigest != digestReviewVerdictBody(publication.Body) ||
		publication.PreparedAt.IsZero() {
		return errors.New("publication intent is incomplete or inconsistent")
	}
	if !cycle.Stale {
		aggregation, err := buildReviewVerdictAggregation(cycle)
		if err != nil {
			return err
		}
		aggregationDigest, err :=
			digestReviewVerdictAggregation(aggregation)
		if err != nil {
			return err
		}
		expectedBody, expectedVerdict :=
			formatReviewVerdictPublicationBody(
				publication.ReviewerID,
				publication.ID,
				aggregationDigest,
				aggregation,
			)
		if publication.AggregationDigest != aggregationDigest ||
			publication.ResultState != aggregation.ResultState ||
			publication.Verdict != expectedVerdict ||
			publication.Body != expectedBody {
			return errors.New(
				"publication does not match the terminal review aggregation",
			)
		}
	}
	if marker, ok := reviewVerdictMarkerValue(
		publication.Body,
		reviewVerdictPublicationIDMarker,
	); !ok || marker != publication.ID {
		return errors.New("publication body identity does not match its intent")
	}
	if marker, ok := reviewVerdictMarkerValue(
		publication.Body,
		reviewVerdictAggregationMarker,
	); !ok || marker != publication.AggregationDigest {
		return errors.New(
			"publication body aggregation does not match its intent",
		)
	}
	if marker, ok := reviewVerdictMarkerValue(
		publication.Body,
		reviewVerdictBodyDigestMarker,
	); !ok || marker == "" {
		return errors.New("publication body digest marker is missing")
	}
	if reviewerID, ok := parseReviewAgentID(publication.Body); !ok ||
		reviewerID != publication.ReviewerID {
		return errors.New("publication body reviewer does not match its intent")
	}
	if role, ok := parseOrchestratorAgentRole(publication.Body); !ok ||
		role != RoleReviewer {
		return errors.New("publication body role is not reviewer")
	}
	if headSHA, ok := parseReviewedSHA(publication.Body); !ok ||
		headSHA != publication.HeadSHA {
		return errors.New("publication body head does not match its intent")
	}
	if !reviewVerdictBodyMatches(publication.Body, publication.Verdict) {
		return errors.New("publication body verdict does not match its intent")
	}
	switch publication.Status {
	case ReviewVerdictPublicationPending:
		if publication.CommentID != 0 ||
			strings.TrimSpace(publication.CommentURL) != "" ||
			!publication.PublishedAt.IsZero() {
			return errors.New(
				"pending publication contains a published comment",
			)
		}
	case ReviewVerdictPublicationPublished:
		if publication.CommentID <= 0 || publication.PublishedAt.IsZero() {
			return errors.New(
				"published publication is missing its comment",
			)
		}
	default:
		return fmt.Errorf(
			"publication status %q is unsupported",
			publication.Status,
		)
	}
	return nil
}

func (m *AgentManager) prepareReviewVerdictPublication(
	reviewerID string,
	prepared ReviewVerdictPublication,
) (reviewVerdictPublicationMutation, ReviewVerdictPublication, bool, error) {
	if m == nil {
		return reviewVerdictPublicationMutation{},
			ReviewVerdictPublication{},
			false,
			errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil || agentLifecycleTerminal(reviewer) ||
		reviewer.Paused {
		return reviewVerdictPublicationMutation{},
			ReviewVerdictPublication{},
			false,
			fmt.Errorf(
				"review coordinator %q is unavailable for verdict publication",
				strings.TrimSpace(reviewerID),
			)
	}
	if existing := reviewer.ReviewCycle.VerdictPublication; existing != nil {
		if existing.ID != prepared.ID ||
			existing.HeadSHA != prepared.HeadSHA ||
			existing.ReviewerID != prepared.ReviewerID {
			return reviewVerdictPublicationMutation{},
				ReviewVerdictPublication{},
				false,
				errors.New(
					"persisted review verdict publication identity changed",
				)
		}
		return reviewVerdictPublicationMutation{},
			*cloneReviewVerdictPublication(existing),
			false,
			nil
	}
	mutation := reviewVerdictPublicationMutation{
		reviewerID:               reviewer.ID,
		previousLastActivityTime: reviewer.LastActivityTime,
		updatedAt:                time.Now().UTC(),
	}
	reviewer.ReviewCycle.VerdictPublication =
		cloneReviewVerdictPublication(&prepared)
	reviewer.LastActivityTime = mutation.updatedAt
	if err := validatePersistedReviewCycleSnapshot(
		reviewer.ReviewCycle,
	); err != nil {
		reviewer.ReviewCycle.VerdictPublication = nil
		reviewer.LastActivityTime = mutation.previousLastActivityTime
		return reviewVerdictPublicationMutation{},
			ReviewVerdictPublication{},
			false,
			err
	}
	return mutation, prepared, true, nil
}

func (m *AgentManager) markReviewVerdictPublished(
	reviewerID string,
	commentID int64,
	commentURL string,
	publishedAt time.Time,
) (reviewVerdictPublicationMutation, ReviewVerdictPublication, error) {
	if m == nil || commentID <= 0 || publishedAt.IsZero() {
		return reviewVerdictPublicationMutation{},
			ReviewVerdictPublication{},
			errors.New(
				"published review verdict requires an agent manager, comment, and time",
			)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil ||
		reviewer.ReviewCycle.VerdictPublication == nil {
		return reviewVerdictPublicationMutation{},
			ReviewVerdictPublication{},
			fmt.Errorf(
				"review coordinator %q has no pending verdict publication",
				strings.TrimSpace(reviewerID),
			)
	}
	publication := reviewer.ReviewCycle.VerdictPublication
	if publication.Status == ReviewVerdictPublicationPublished {
		if publication.CommentID != commentID {
			return reviewVerdictPublicationMutation{},
				ReviewVerdictPublication{},
				errors.New(
					"review verdict publication resolved to a different comment",
				)
		}
		return reviewVerdictPublicationMutation{},
			*cloneReviewVerdictPublication(publication),
			nil
	}
	mutation := reviewVerdictPublicationMutation{
		reviewerID: reviewer.ID,
		previous: cloneReviewVerdictPublication(
			publication,
		),
		previousLastActivityTime: reviewer.LastActivityTime,
		updatedAt:                time.Now().UTC(),
	}
	publication.Status = ReviewVerdictPublicationPublished
	publication.CommentID = commentID
	publication.CommentURL = strings.TrimSpace(commentURL)
	publication.PublishedAt = publishedAt.UTC()
	reviewer.LastActivityTime = mutation.updatedAt
	if err := validatePersistedReviewCycleSnapshot(
		reviewer.ReviewCycle,
	); err != nil {
		reviewer.ReviewCycle.VerdictPublication =
			cloneReviewVerdictPublication(mutation.previous)
		reviewer.LastActivityTime = mutation.previousLastActivityTime
		return reviewVerdictPublicationMutation{},
			ReviewVerdictPublication{},
			err
	}
	return mutation, *cloneReviewVerdictPublication(publication), nil
}

func (m *AgentManager) rollbackReviewVerdictPublication(
	mutation reviewVerdictPublicationMutation,
) bool {
	if m == nil || mutation.reviewerID == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[mutation.reviewerID]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return false
	}
	reviewer.ReviewCycle.VerdictPublication =
		cloneReviewVerdictPublication(mutation.previous)
	if reviewer.LastActivityTime.Equal(mutation.updatedAt) {
		reviewer.LastActivityTime = mutation.previousLastActivityTime
	}
	return true
}

func (b *Orchestrator) persistPreparedReviewVerdict(
	reviewerID string,
	prepared ReviewVerdictPublication,
) (ReviewVerdictPublication, error) {
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	mutation, publication, changed, err :=
		b.agents.prepareReviewVerdictPublication(reviewerID, prepared)
	if err != nil || !changed {
		return publication, err
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if !b.agents.rollbackReviewVerdictPublication(mutation) {
			return ReviewVerdictPublication{}, fmt.Errorf(
				"failed to persist prepared review verdict and failed to roll it back: %w",
				err,
			)
		}
		return ReviewVerdictPublication{}, fmt.Errorf(
			"failed to persist prepared review verdict: %w",
			err,
		)
	}
	return publication, nil
}

func (b *Orchestrator) persistPublishedReviewVerdict(
	reviewerID string,
	comment *github.IssueComment,
) (ReviewVerdictPublication, error) {
	if comment == nil || comment.GetID() <= 0 {
		return ReviewVerdictPublication{}, errors.New(
			"GitHub returned no review verdict comment",
		)
	}
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	mutation, publication, err := b.agents.markReviewVerdictPublished(
		reviewerID,
		comment.GetID(),
		comment.GetHTMLURL(),
		time.Now().UTC(),
	)
	if err != nil {
		return ReviewVerdictPublication{}, err
	}
	if mutation.reviewerID == "" {
		return publication, nil
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if !b.agents.rollbackReviewVerdictPublication(mutation) {
			return ReviewVerdictPublication{}, fmt.Errorf(
				"failed to persist published review verdict and failed to restore its pending intent: %w",
				err,
			)
		}
		return ReviewVerdictPublication{}, fmt.Errorf(
			"failed to persist published review verdict: %w",
			err,
		)
	}
	return publication, nil
}

func validateExistingReviewVerdictComment(
	comment *github.IssueComment,
	publication ReviewVerdictPublication,
	trustedPublisherLogin string,
) error {
	if comment == nil || comment.GetID() <= 0 {
		return errors.New("matched review verdict comment is invalid")
	}
	if trustedPublisherLogin = strings.TrimSpace(
		trustedPublisherLogin,
	); trustedPublisherLogin == "" ||
		!strings.EqualFold(
			strings.TrimSpace(comment.GetUser().GetLogin()),
			trustedPublisherLogin,
		) {
		return errors.New(
			"matched review verdict comment is not from the trusted publisher",
		)
	}
	body := comment.GetBody()
	if body != publication.Body {
		return errors.New(
			"matched review verdict comment body differs from the persisted publication",
		)
	}
	if reviewerID, ok := parseReviewAgentID(body); !ok ||
		reviewerID != publication.ReviewerID {
		return errors.New(
			"matched review verdict comment has a different reviewer identity",
		)
	}
	if role, ok := parseOrchestratorAgentRole(body); !ok ||
		role != RoleReviewer {
		return errors.New(
			"matched review verdict comment has a different agent role",
		)
	}
	if headSHA, ok := parseReviewedSHA(body); !ok ||
		headSHA != publication.HeadSHA {
		return errors.New(
			"matched review verdict comment has a different exact head",
		)
	}
	if !reviewVerdictBodyMatches(body, publication.Verdict) {
		return errors.New(
			"matched review report comment has a different verdict",
		)
	}
	if digest, ok := reviewVerdictMarkerValue(
		body,
		reviewVerdictAggregationMarker,
	); !ok || digest != publication.AggregationDigest {
		return errors.New(
			"matched review verdict comment has a different aggregation",
		)
	}
	if digest, ok := reviewVerdictMarkerValue(
		body,
		reviewVerdictBodyDigestMarker,
	); !ok || digest == "" {
		return errors.New(
			"matched review verdict comment has no body digest",
		)
	}
	return nil
}

func findExistingReviewVerdictComment(
	comments []*github.IssueComment,
	publication ReviewVerdictPublication,
	trustedPublisherLogin string,
) (*github.IssueComment, error) {
	var matched *github.IssueComment
	for _, comment := range comments {
		identity, ok := reviewVerdictMarkerValue(
			comment.GetBody(),
			reviewVerdictPublicationIDMarker,
		)
		if !ok || identity != publication.ID {
			continue
		}
		if !strings.EqualFold(
			strings.TrimSpace(comment.GetUser().GetLogin()),
			strings.TrimSpace(trustedPublisherLogin),
		) {
			continue
		}
		// A comment carrying this exact publication ID but authored by a
		// different reviewer means the identity collided with another
		// reviewer's publication: an ID derived only from repo/PR/head SHA
		// lets two independent reviewers reaching
		// different verdicts for the same head compute the same ID) --
		// not that this is my own comment in some corrupted state. Skip
		// it rather than treating it as an unrecoverable validation
		// failure: it was never mine to reconcile against, so it can't be
		// allowed to permanently block me from publishing my own verdict.
		// validateExistingReviewVerdictComment below still independently
		// checks reviewer identity (among other things) for a comment
		// that passes this check, so a genuine self-inconsistency in my
		// *own* comment is still correctly treated as fatal.
		if reviewerID, ok := parseReviewAgentID(comment.GetBody()); !ok ||
			reviewerID != publication.ReviewerID {
			continue
		}
		if err := validateExistingReviewVerdictComment(
			comment,
			publication,
			trustedPublisherLogin,
		); err != nil {
			return nil, err
		}
		if matched != nil {
			return nil, fmt.Errorf(
				"review verdict publication %s has duplicate GitHub comments %d and %d",
				publication.ID,
				matched.GetID(),
				comment.GetID(),
			)
		}
		matched = comment
	}
	return matched, nil
}

func transientReviewVerdictGitHubError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var githubErr *github.ErrorResponse
	if errors.As(err, &githubErr) && githubErr.Response != nil {
		status := githubErr.Response.StatusCode
		if status == http.StatusRequestTimeout ||
			status == http.StatusTooManyRequests ||
			status >= http.StatusInternalServerError {
			return true
		}
		if status == http.StatusForbidden &&
			(githubErr.Response.Header.Get("Retry-After") != "" ||
				githubErr.Response.Header.Get(
					"X-RateLimit-Remaining",
				) == "0") {
			return true
		}
		return false
	}
	var networkErr net.Error
	return errors.As(err, &networkErr)
}

func waitReviewVerdictRetry(
	ctx context.Context,
	attempt int,
) error {
	delay := time.Duration(attempt+1) * 100 * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *Orchestrator) ensureReviewVerdictBoundaryCurrent(
	ctx context.Context,
	reviewer Agent,
) error {
	if reviewer.ReviewCycle == nil ||
		reviewer.ReviewCycle.Inputs == nil {
		return errors.New(
			"review verdict exact-SHA plan inputs are missing",
		)
	}
	var pullRequests reviewCoordinatorPullRequestGetter
	switch {
	case b != nil && b.reviewCoordinatorPullRequests != nil:
		pullRequests = b.reviewCoordinatorPullRequests
	case b != nil && b.github != nil:
		pullRequests = b.github.PullRequests
	default:
		return errors.New(
			"review verdict live PR boundary is not configured",
		)
	}
	repoOwner := strings.TrimSpace(b.cfg.RepoOwner)
	repoName := strings.TrimSpace(b.cfg.RepoName)
	pullRequest, _, err := pullRequests.Get(
		ctx,
		repoOwner,
		repoName,
		reviewer.PRNumber,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to recheck live PR boundary before review verdict publication: %w",
			err,
		)
	}
	expectedRepository := strings.ToLower(repoOwner + "/" + repoName)
	boundary, err := validateConvergentReviewPullRequestBoundary(
		reviewer,
		expectedRepository,
		pullRequest,
	)
	if err != nil {
		return fmt.Errorf(
			"review verdict PR boundary rejected publication: %w",
			err,
		)
	}
	if boundary.HeadSHA != reviewer.ReviewCycle.HeadSHA {
		return &reviewCycleStaleError{
			reviewerID: reviewer.ID,
			staleHead:  reviewer.ReviewCycle.HeadSHA,
			liveHead:   boundary.HeadSHA,
		}
	}
	if boundary.BaseSHA != reviewer.ReviewCycle.Inputs.BaseSHA {
		return fmt.Errorf(
			"%w: review verdict base SHA mismatch: persisted=%s live=%s",
			errReviewBaseChanged,
			abbreviateSHA(reviewer.ReviewCycle.Inputs.BaseSHA),
			abbreviateSHA(boundary.BaseSHA),
		)
	}
	return nil
}

func (b *Orchestrator) ensureReviewVerdictBoundaryCurrentWithRetry(
	ctx context.Context,
	reviewer Agent,
) error {
	var lastErr error
	for attempt := 0; attempt < reviewVerdictPublicationMaxAttempts; attempt++ {
		lastErr = b.ensureReviewVerdictBoundaryCurrent(ctx, reviewer)
		if lastErr == nil ||
			!transientReviewVerdictGitHubError(lastErr) {
			return lastErr
		}
		if attempt+1 == reviewVerdictPublicationMaxAttempts {
			break
		}
		if err := waitReviewVerdictRetry(ctx, attempt); err != nil {
			return errors.Join(lastErr, err)
		}
	}
	return lastErr
}

func (b *Orchestrator) resolveReviewVerdictPublisherLoginWithRetry(
	ctx context.Context,
) (string, error) {
	var lastErr error
	for attempt := 0; attempt < reviewVerdictPublicationMaxAttempts; attempt++ {
		publisher, _, err := b.github.Users.Get(ctx, "")
		login := ""
		if publisher != nil {
			login = strings.TrimSpace(publisher.GetLogin())
		}
		switch {
		case err == nil && login != "":
			return login, nil
		case err == nil:
			lastErr = errors.New(
				"authenticated GitHub publisher has no login",
			)
		default:
			lastErr = fmt.Errorf(
				"failed to resolve authenticated GitHub publisher: %w",
				err,
			)
		}
		if err == nil || !transientReviewVerdictGitHubError(err) ||
			attempt+1 == reviewVerdictPublicationMaxAttempts {
			return "", lastErr
		}
		if waitErr := waitReviewVerdictRetry(ctx, attempt); waitErr != nil {
			return "", errors.Join(lastErr, waitErr)
		}
	}
	return "", lastErr
}

// publishReviewCycleVerdict is the sole GitHub publication boundary for the
// convergent-review coordinator. Every attempt rechecks the live PR
// boundary, scans for the stable publication identity, and only then creates
// a comment.
func (b *Orchestrator) publishReviewCycleVerdict(
	ctx context.Context,
	reviewerID string,
) (ReviewVerdictPublication, error) {
	if b == nil || b.agents == nil || b.github == nil {
		return ReviewVerdictPublication{}, errors.New(
			"review verdict publication is not configured",
		)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	b.reviewVerdictPublicationMu.Lock()
	defer b.reviewVerdictPublicationMu.Unlock()

	publication, err := b.publishReviewCycleVerdictLocked(
		ctx,
		reviewerID,
	)
	b.logReviewCycleObservation(reviewerID, "publication_observed")
	var staleErr *reviewCycleStaleError
	if errors.As(err, &staleErr) {
		invalidateErr := b.invalidateStaleReviewCycle(
			ctx,
			staleErr.reviewerID,
			staleErr.liveHead,
		)
		return ReviewVerdictPublication{},
			errors.Join(err, invalidateErr)
	}
	var terminalErr *convergentReviewPullRequestTerminalError
	if errors.As(err, &terminalErr) {
		_, reconcileErr := b.reconcileConvergentReviewPullRequest(
			ctx,
			reviewerID,
		)
		return ReviewVerdictPublication{},
			errors.Join(err, reconcileErr)
	}
	if err == nil &&
		publication.ResultState == ReviewCycleResultPartialNoFindings {
		return publication,
			b.finalizePartialNoFindingsReview(ctx, reviewerID)
	}
	return publication, err
}

func (b *Orchestrator) publishReviewCycleVerdictLocked(
	ctx context.Context,
	reviewerID string,
) (ReviewVerdictPublication, error) {
	reviewer, unlockLifecycle, err :=
		b.agents.lockActiveReviewCoordinatorForWorkerLaunch(reviewerID)
	if err != nil {
		return ReviewVerdictPublication{}, err
	}
	defer unlockLifecycle()

	if !reviewCycleHasPublishableReport(reviewer.ReviewCycle) {
		return ReviewVerdictPublication{}, errReviewResultNotPublishable
	}
	if err := b.ensureReviewVerdictBoundaryCurrentWithRetry(
		ctx,
		reviewer,
	); err != nil {
		return ReviewVerdictPublication{}, err
	}
	prepared, err := prepareReviewVerdictPublication(
		reviewer,
		b.cfg.RepoOwner,
		b.cfg.RepoName,
		time.Now().UTC(),
	)
	if err != nil {
		return ReviewVerdictPublication{}, err
	}

	b.reviewArtifactMu.Lock()
	defer b.reviewArtifactMu.Unlock()
	publication, err := b.persistPreparedReviewVerdict(
		reviewer.ID,
		prepared,
	)
	if err != nil {
		return ReviewVerdictPublication{}, err
	}
	if publication.Status == ReviewVerdictPublicationPublished {
		return publication, nil
	}
	trustedPublisherLogin, err :=
		b.resolveReviewVerdictPublisherLoginWithRetry(ctx)
	if err != nil {
		return ReviewVerdictPublication{}, err
	}

	var lastErr error
	for attempt := 0; attempt < reviewVerdictPublicationMaxAttempts; attempt++ {
		if err := b.ensureReviewVerdictBoundaryCurrentWithRetry(
			ctx,
			reviewer,
		); err != nil {
			return ReviewVerdictPublication{}, err
		}
		comments, listErr := b.listAllIssueComments(
			ctx,
			reviewer.PRNumber,
		)
		if listErr != nil {
			lastErr = fmt.Errorf(
				"failed to list comments before review verdict publication: %w",
				listErr,
			)
			if !transientReviewVerdictGitHubError(listErr) ||
				attempt+1 == reviewVerdictPublicationMaxAttempts {
				return ReviewVerdictPublication{}, lastErr
			}
			if err := waitReviewVerdictRetry(ctx, attempt); err != nil {
				return ReviewVerdictPublication{}, errors.Join(lastErr, err)
			}
			continue
		}
		comment, findErr := findExistingReviewVerdictComment(
			comments,
			publication,
			trustedPublisherLogin,
		)
		if findErr != nil {
			return ReviewVerdictPublication{}, findErr
		}
		if comment == nil {
			if err := b.ensureReviewVerdictBoundaryCurrentWithRetry(
				ctx,
				reviewer,
			); err != nil {
				return ReviewVerdictPublication{}, err
			}
			comment, _, err = b.github.Issues.CreateComment(
				ctx,
				b.cfg.RepoOwner,
				b.cfg.RepoName,
				reviewer.PRNumber,
				&github.IssueComment{
					Body: github.String(publication.Body),
				},
			)
			if err != nil {
				lastErr = fmt.Errorf(
					"failed to create consolidated review verdict: %w",
					err,
				)
				if !transientReviewVerdictGitHubError(err) ||
					attempt+1 ==
						reviewVerdictPublicationMaxAttempts {
					return ReviewVerdictPublication{}, lastErr
				}
				if err := waitReviewVerdictRetry(
					ctx,
					attempt,
				); err != nil {
					return ReviewVerdictPublication{},
						errors.Join(lastErr, err)
				}
				continue
			}
		}
		if err := b.ensureReviewVerdictBoundaryCurrentWithRetry(
			ctx,
			reviewer,
		); err != nil {
			return ReviewVerdictPublication{}, err
		}
		return b.persistPublishedReviewVerdict(
			reviewer.ID,
			comment,
		)
	}
	return ReviewVerdictPublication{}, lastErr
}
