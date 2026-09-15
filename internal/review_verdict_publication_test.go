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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v90/github"
)

type reviewVerdictGitHubFixture struct {
	mu                      sync.Mutex
	baseSHA                 string
	headSHA                 string
	state                   string
	merged                  bool
	headSequence            []string
	headFailures            int
	headRequests            int
	listFailures            int
	ambiguousCreateFailures int
	listRequests            int
	createRequests          int
	comments                []*github.IssueComment
	publisherLogin          string
}

const (
	reviewVerdictFixturePublisherLogin      = "orchestrator-publisher"
	reviewVerdictFixtureMaxCommentBodyBytes = 65536
)

func (fixture *reviewVerdictGitHubFixture) trustedPublisherLogin() string {
	if login := strings.TrimSpace(fixture.publisherLogin); login != "" {
		return login
	}
	return reviewVerdictFixturePublisherLogin
}

func (fixture *reviewVerdictGitHubFixture) handler(
	w http.ResponseWriter,
	request *http.Request,
) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	switch {
	case request.Method == http.MethodGet &&
		request.URL.Path == "/user":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"login": fixture.trustedPublisherLogin(),
		})
	case request.Method == http.MethodGet &&
		request.URL.Path == "/repos/acme/widget/pulls/50":
		fixture.headRequests++
		if fixture.headFailures > 0 {
			fixture.headFailures--
			http.Error(
				w,
				`{"message":"temporary head failure"}`,
				http.StatusInternalServerError,
			)
			return
		}
		headSHA := fixture.headSHA
		if len(fixture.headSequence) > 0 {
			index := fixture.headRequests - 1
			if index >= len(fixture.headSequence) {
				index = len(fixture.headSequence) - 1
			}
			headSHA = fixture.headSequence[index]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number": 50,
			"state":  fixture.state,
			"merged": fixture.merged,
			"base": map[string]any{
				"sha": fixture.baseSHA,
				"repo": map[string]any{
					"full_name": "acme/widget",
				},
			},
			"head": map[string]any{
				"sha": headSHA,
				"repo": map[string]any{
					"full_name": "acme/widget",
				},
			},
		})
	case request.Method == http.MethodGet &&
		request.URL.Path == "/repos/acme/widget/issues/50/comments":
		fixture.listRequests++
		if fixture.listFailures > 0 {
			fixture.listFailures--
			http.Error(
				w,
				`{"message":"temporary list failure"}`,
				http.StatusInternalServerError,
			)
			return
		}
		_ = json.NewEncoder(w).Encode(fixture.comments)
	case request.Method == http.MethodPost &&
		request.URL.Path == "/repos/acme/widget/issues/50/comments":
		fixture.createRequests++
		var requested github.IssueComment
		if err := json.NewDecoder(request.Body).Decode(&requested); err != nil {
			http.Error(w, "invalid comment", http.StatusBadRequest)
			return
		}
		if len(requested.GetBody()) >
			reviewVerdictFixtureMaxCommentBodyBytes {
			http.Error(
				w,
				`{"message":"comment body is too long"}`,
				http.StatusUnprocessableEntity,
			)
			return
		}
		comment := &github.IssueComment{
			ID:   github.Int64(int64(1000 + len(fixture.comments))),
			Body: github.String(requested.GetBody()),
			User: &github.User{
				Login: github.String(
					fixture.trustedPublisherLogin(),
				),
			},
			HTMLURL: github.String(fmt.Sprintf(
				"https://example.test/comments/%d",
				1000+len(fixture.comments),
			)),
		}
		fixture.comments = append(fixture.comments, comment)
		if fixture.ambiguousCreateFailures > 0 {
			fixture.ambiguousCreateFailures--
			http.Error(
				w,
				`{"message":"response lost after create"}`,
				http.StatusInternalServerError,
			)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(comment)
	default:
		http.NotFound(w, request)
	}
}

func (fixture *reviewVerdictGitHubFixture) snapshot() (int, int, int, []*github.IssueComment) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return fixture.headRequests,
		fixture.listRequests,
		fixture.createRequests,
		append([]*github.IssueComment(nil), fixture.comments...)
}

func newTerminalReviewVerdictHarness(
	t *testing.T,
) *reviewArtifactIntakeHarness {
	t.Helper()
	harness := newReviewConvergenceTestHarness(t, 0, true)
	if !harness.agents.SetPR(
		harness.reviewer.ID,
		50,
		"Consolidated verdict",
		"https://example.test/pull/50",
		harness.reviewer.ReviewCycle.HeadSHA,
	) {
		t.Fatal("SetPR() = false")
	}
	for round := 0; round <
		harness.reviewer.ReviewCycle.Policy.Convergence.QuietRoundsRequired; round++ {
		if round > 0 {
			completeConvergenceRediscoveryPass(t, harness, 0)
		}
		if _, err := harness.bot.beginReviewConvergenceRound(
			harness.reviewer.ID,
		); err != nil {
			t.Fatalf(
				"beginReviewConvergenceRound() error = %v",
				err,
			)
		}
		if _, err := harness.bot.completeReviewConvergenceRound(
			harness.reviewer.ID,
		); err != nil {
			t.Fatalf(
				"completeReviewConvergenceRound() error = %v",
				err,
			)
		}
	}
	harness.reviewer, _ = harness.agents.Get(harness.reviewer.ID)
	if harness.reviewer.ReviewCycle.Convergence.Status !=
		ReviewConvergenceConverged {
		t.Fatalf(
			"terminal test convergence = %q, want %q",
			harness.reviewer.ReviewCycle.Convergence.Status,
			ReviewConvergenceConverged,
		)
	}
	return harness
}

func newPartialNoFindingsReviewHarness(
	t *testing.T,
) *reviewArtifactIntakeHarness {
	t.Helper()
	harness := newTerminalReviewVerdictHarness(t)
	cycle := harness.reviewer.ReviewCycle
	metrics, err := cycle.ReviewMetricsSnapshot()
	if err != nil {
		t.Fatalf("ReviewMetricsSnapshot() error = %v", err)
	}
	metrics.Cost.AgentCount = uint64(cycle.Policy.Convergence.MaxReviewAgentsPerSHA)
	evaluation, err := evaluateReviewLaunchLimits(cycle, metrics, time.Now().UTC())
	if err != nil || evaluation.Limit == nil {
		t.Fatalf("evaluateReviewLaunchLimits() = (%#v, %v)", evaluation, err)
	}
	harness.agents.mu.Lock()
	stored := harness.agents.agents[harness.reviewer.ID].ReviewCycle
	stored.LimitTransition = evaluation.Limit
	stored.ResultState = ReviewCycleResultPartialNoFindings
	harness.agents.mu.Unlock()
	harness.reviewer, _ = harness.agents.Get(harness.reviewer.ID)
	return harness
}

func attachReviewVerdictGitHub(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
	fixture *reviewVerdictGitHubFixture,
) *httptest.Server {
	t.Helper()
	if fixture.baseSHA == "" &&
		harness.reviewer.ReviewCycle != nil &&
		harness.reviewer.ReviewCycle.Inputs != nil {
		fixture.baseSHA = harness.reviewer.ReviewCycle.Inputs.BaseSHA
	}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	harness.bot.github = newGitHubClientForTest(t, server)
	return server
}

func allowReviewRetirement(harness *reviewArtifactIntakeHarness) {
	harness.bot.runner = &stubRunner{}
	harness.bot.cleanupWorktreeFunc = func(
		_ context.Context, _ string, worktreePath string, _ string,
	) error {
		return os.RemoveAll(worktreePath)
	}
}

func TestReviewVerdictIntermediateStateNeverPublishes(t *testing.T) {
	harness := newReviewConvergenceTestHarness(t, 0, true)
	if !harness.agents.SetPR(
		harness.reviewer.ID,
		50,
		"Intermediate",
		"https://example.test/pull/50",
		harness.reviewer.ReviewCycle.HeadSHA,
	) {
		t.Fatal("SetPR() = false")
	}
	fixture := &reviewVerdictGitHubFixture{
		headSHA: harness.reviewer.ReviewCycle.HeadSHA,
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	_, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	)
	if !errors.Is(err, errReviewResultNotPublishable) {
		t.Fatalf(
			"publishReviewCycleVerdict() error = %v, want non-terminal",
			err,
		)
	}
	headRequests, listRequests, createRequests, comments :=
		fixture.snapshot()
	if headRequests != 0 || listRequests != 0 || createRequests != 0 ||
		len(comments) != 0 {
		t.Fatalf(
			"intermediate publication requests = head:%d list:%d create:%d comments:%d",
			headRequests,
			listRequests,
			createRequests,
			len(comments),
		)
	}
}

func TestReviewVerdictTerminalPRNeverPublishes(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  string
		merged bool
	}{
		{name: "closed", state: "closed"},
		{name: "merged", state: "open", merged: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newTerminalReviewVerdictHarness(t)
			allowReviewRetirement(harness)
			fixture := &reviewVerdictGitHubFixture{
				headSHA: harness.reviewer.ReviewCycle.HeadSHA,
				state:   test.state,
				merged:  test.merged,
			}
			server := attachReviewVerdictGitHub(
				t,
				harness,
				fixture,
			)
			defer server.Close()

			_, err := harness.bot.publishReviewCycleVerdict(
				context.Background(),
				harness.reviewer.ID,
			)
			if !errors.Is(
				err,
				errConvergentReviewPullRequestTerminal,
			) {
				t.Fatalf(
					"publishReviewCycleVerdict() error = %v, want terminal PR rejection",
					err,
				)
			}
			reviewer, ok := harness.agents.Get(
				harness.reviewer.ID,
			)
			if !ok || !reviewer.Stopped ||
				reviewer.State != StateDone {
				t.Fatalf(
					"terminal publication reviewer = %#v",
					reviewer,
				)
			}
			_, listRequests, createRequests, comments :=
				fixture.snapshot()
			if listRequests != 0 ||
				createRequests != 0 ||
				len(comments) != 0 {
				t.Fatalf(
					"terminal publication writes = lists:%d creates:%d comments:%d",
					listRequests,
					createRequests,
					len(comments),
				)
			}
		})
	}
}

func TestPartialReviewAmbiguousCreateRetryProducesOneNeutralComment(t *testing.T) {
	harness := newPartialNoFindingsReviewHarness(t)
	allowReviewRetirement(harness)
	fixture := &reviewVerdictGitHubFixture{
		headSHA:                 harness.reviewer.ReviewCycle.HeadSHA,
		ambiguousCreateFailures: 1,
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	publication, err := harness.bot.publishReviewCycleVerdictLocked(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("publishReviewCycleVerdict() error = %v", err)
	}
	if publication.Status != ReviewVerdictPublicationPublished ||
		publication.ResultState != ReviewCycleResultPartialNoFindings ||
		publication.Verdict != "" || publication.CommentID <= 0 ||
		!strings.Contains(publication.Body, "### Completed scopes") ||
		!strings.Contains(publication.Body, "### Unresolved scopes") ||
		strings.Contains(publication.Body, "CODEX_VERDICT:") {
		t.Fatalf("published partial report = %#v", publication)
	}
	_, _, createRequests, comments := fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"ambiguous retry created %d requests and %d comments, want one each",
			createRequests,
			len(comments),
		)
	}
	if identity, ok := reviewVerdictMarkerValue(
		comments[0].GetBody(),
		reviewVerdictPublicationIDMarker,
	); !ok || identity != publication.ID {
		t.Fatalf(
			"visible verdict identity = %q, want %q",
			identity,
			publication.ID,
		)
	}

	recovered, err := harness.bot.publishReviewCycleVerdict(
		context.Background(), harness.reviewer.ID,
	)
	if err != nil || recovered.CommentID != publication.CommentID {
		t.Fatalf("recover published partial report = %#v, %v", recovered, err)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	if !current.Stopped || current.State != StateErrored {
		t.Fatalf("neutral partial reviewer was not retired: %#v", current)
	}
	_, _, createRequests, comments = fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf("idempotent replay created %d requests and %d comments", createRequests, len(comments))
	}
}

func TestSixAcceptedWorkersAndFailedChallengeBuildPartialReport(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	cycle := cloneReviewCycle(harness.reviewer.ReviewCycle)
	if len(cycle.ArtifactReceipts) < 6 {
		t.Fatalf("terminal fixture accepted %d workers, want at least six", len(cycle.ArtifactReceipts))
	}
	cycle.ArtifactReceipts = append(
		[]ReviewArtifactReceipt(nil),
		cycle.ArtifactReceipts[:6]...,
	)
	cycle.Convergence.Status = ReviewConvergenceUnresolved
	cycle.Convergence.ChallengeAssignments = append(
		cycle.Convergence.ChallengeAssignments,
		ReviewChallengeAssignment{
			ID:       "challenge-failed",
			ExactSHA: cycle.HeadSHA,
			Round:    1,
			Lane:     "challenge-contract",
			Target: ReviewChallengeTarget{
				Kind:          ReviewChallengeCoverageGap,
				ID:            "coverage-gap-contract",
				RequirementID: "requirement-contract",
			},
			Status: ReviewChallengeFailed,
			Action: ReviewActionEscalate,
		},
	)
	failedWorker := &cycle.WorkerOwnerships[0]
	failedWorker.Identity.Role = AgentProfileRoleChallenge
	failedWorker.Identity.Lane = "escalation-verifier"
	failedWorker.Lifecycle = ReviewWorkerFailed
	failedWorker.Failure = &DurableLaunchFailure{Kind: DurableLaunchFailureArtifact}
	cycle.ArtifactFailures = append(cycle.ArtifactFailures, ReviewArtifactIntakeFailure{
		Class:  ReviewArtifactFailureTransient,
		Code:   ReviewArtifactFailureTimeout,
		Detail: "SECRET RAW ARTIFACT AND INTERNAL PROMPT",
	})
	aggregation, err := buildReviewVerdictAggregation(cycle)
	if err != nil {
		t.Fatalf("buildReviewVerdictAggregation() error = %v", err)
	}
	body, verdict := formatReviewVerdictPublicationBody(
		harness.reviewer.ID,
		"six-worker-partial-report",
		strings.Repeat("a", 64),
		aggregation,
	)
	if aggregation.ResultState != ReviewCycleResultPartialNoFindings ||
		aggregation.AcceptedResults != 6 || verdict != "" ||
		!strings.Contains(body, "challenge:1:escalation-verifier") ||
		!strings.Contains(body, "coverage_gap / requirement-contract: challenge failed") ||
		!strings.Contains(body, "challenge / coverage_gap / coverage-gap-contract: failed") ||
		!strings.Contains(body, "transient/timeout") ||
		!strings.Contains(body, "### Failed assignments") ||
		!strings.Contains(body, "### Infrastructure diagnostics") ||
		strings.Contains(body, "## Review approved") ||
		strings.Contains(body, "SECRET RAW ARTIFACT") ||
		strings.Contains(body, "INTERNAL PROMPT") {
		t.Fatalf("six-worker partial report = %#v verdict:%q\n%s", aggregation, verdict, body)
	}
}

// reviewVerdictHarnessAggregationDigest replicates the aggregation digest
// prepareReviewVerdictPublication would compute for harness.reviewer's
// current ReviewCycle, so tests that need to precompute a colliding
// publication ID (via reviewVerdictPublicationIdentity directly) produce
// exactly the ID production code would.
func reviewVerdictHarnessAggregationDigest(
	t *testing.T,
	harness *reviewArtifactIntakeHarness,
) string {
	t.Helper()
	aggregation, err := buildReviewVerdictAggregation(
		harness.reviewer.ReviewCycle,
	)
	if err != nil {
		t.Fatalf("buildReviewVerdictAggregation() error = %v", err)
	}
	digest, err := digestReviewVerdictAggregation(aggregation)
	if err != nil {
		t.Fatalf("digestReviewVerdictAggregation() error = %v", err)
	}
	return digest
}

func TestReviewVerdictIgnoresForeignPublicationMarker(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	publicationID, err := reviewVerdictPublicationIdentity(
		harness.bot.cfg.RepoOwner,
		harness.bot.cfg.RepoName,
		harness.reviewer.PRNumber,
		harness.reviewer.ReviewCycle.HeadSHA,
		reviewVerdictHarnessAggregationDigest(t, harness),
	)
	if err != nil {
		t.Fatalf("reviewVerdictPublicationIdentity() error = %v", err)
	}
	foreign := &github.IssueComment{
		ID: github.Int64(900),
		Body: github.String(fmt.Sprintf(
			"spoofed marker\n%s: %s\n",
			reviewVerdictPublicationIDMarker,
			publicationID,
		)),
		User: &github.User{
			Login: github.String("untrusted-commenter"),
		},
	}
	fixture := &reviewVerdictGitHubFixture{
		headSHA: harness.reviewer.ReviewCycle.HeadSHA,
		comments: []*github.IssueComment{
			foreign,
		},
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	publication, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("publishReviewCycleVerdict() error = %v", err)
	}
	if publication.Status != ReviewVerdictPublicationPublished {
		t.Fatalf("published verdict = %#v", publication)
	}
	_, _, createRequests, comments := fixture.snapshot()
	if createRequests != 1 || len(comments) != 2 {
		t.Fatalf(
			"foreign marker publication created %d requests and left %d comments",
			createRequests,
			len(comments),
		)
	}
	if comments[0].GetID() != foreign.GetID() ||
		comments[1].GetBody() != publication.Body ||
		comments[1].GetUser().GetLogin() !=
			reviewVerdictFixturePublisherLogin {
		t.Fatalf("foreign marker adoption result = %#v", comments)
	}
}

// TestReviewVerdictSkipsCollidedPublicationFromDifferentReviewer is the
// regression test for issue #140: a comment carrying the *exact* same
// publication ID (derived only from repo/PR/head SHA, not reviewer
// identity or content) can already exist on the PR, posted by a
// *different* reviewer instance that reviewed the same head earlier.
// Unlike TestReviewVerdictIgnoresForeignPublicationMarker (a different
// *author* entirely), this comment is genuinely from the trusted
// publisher -- it just isn't *this* reviewer's own comment. That must not
// be treated as an unrecoverable "body differs from the persisted
// publication" validation failure (which would leave this reviewer
// permanently stuck, since the collision is deterministic and would
// reproduce identically on every future attempt): it should be skipped,
// and this reviewer should still publish its own comment normally.
func TestReviewVerdictSkipsCollidedPublicationFromDifferentReviewer(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	publicationID, err := reviewVerdictPublicationIdentity(
		harness.bot.cfg.RepoOwner,
		harness.bot.cfg.RepoName,
		harness.reviewer.PRNumber,
		harness.reviewer.ReviewCycle.HeadSHA,
		reviewVerdictHarnessAggregationDigest(t, harness),
	)
	if err != nil {
		t.Fatalf("reviewVerdictPublicationIdentity() error = %v", err)
	}
	collided := &github.IssueComment{
		ID: github.Int64(901),
		Body: github.String(fmt.Sprintf(
			"a prior reviewer's verdict for this same head\n"+
				"CODEX_AGENT_ID: review-agent-981-earlier-instance\n"+
				"CODEX_AGENT_ROLE: reviewer\n"+
				"%s: %s\n",
			reviewVerdictPublicationIDMarker,
			publicationID,
		)),
		User: &github.User{
			Login: github.String(reviewVerdictFixturePublisherLogin),
		},
	}
	fixture := &reviewVerdictGitHubFixture{
		headSHA: harness.reviewer.ReviewCycle.HeadSHA,
		comments: []*github.IssueComment{
			collided,
		},
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	publication, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("publishReviewCycleVerdict() error = %v, want it to skip the collided comment and publish its own", err)
	}
	if publication.Status != ReviewVerdictPublicationPublished {
		t.Fatalf("published verdict = %#v", publication)
	}
	_, _, createRequests, comments := fixture.snapshot()
	if createRequests != 1 || len(comments) != 2 {
		t.Fatalf(
			"collided publication created %d requests and left %d comments, want 1 and 2 (the earlier reviewer's comment plus this reviewer's own)",
			createRequests,
			len(comments),
		)
	}
	if comments[0].GetID() != collided.GetID() ||
		comments[1].GetBody() != publication.Body ||
		comments[1].GetUser().GetLogin() !=
			reviewVerdictFixturePublisherLogin {
		t.Fatalf("collided publication result = %#v", comments)
	}
}

// TestReviewVerdictPublicationIdentityDiffersForDistinctAggregationOnSameHead
// is the regression test for the still-open half of issue #140: the
// publication ID used to be derived purely from (repo, PR, head SHA), so
// two independent reviewer instances reaching genuinely different verdicts
// for the identical head SHA (LLM-driven discovery is not guaranteed to
// reproduce the same findings across independent runs) computed the exact
// same ID. #144 made a same-ID collision from a different reviewer
// non-fatal (skip and publish anyway), but left both comments claiming the
// same "canonical" ID -- confusing to a human reader even though nothing is
// stuck. This test asserts the actual root cause is now fixed: folding the
// aggregation digest into the identity means two different verdicts for the
// same head no longer collide in the first place.
func TestReviewVerdictPublicationIdentityDiffersForDistinctAggregationOnSameHead(t *testing.T) {
	const (
		owner   = "acme"
		repo    = "widget"
		pr      = 50
		headSHA = "c8d8b712dea27645eaec3c3d26a9a5275a9ae64e"
	)
	approved := reviewVerdictAggregation{
		SchemaVersion: reviewVerdictPublicationSchemaVersion,
		HeadSHA:       headSHA,
		ResultState:   ReviewCycleResultCompleteClean,
		Findings:      []reviewVerdictFinding{},
	}
	changesRequired := reviewVerdictAggregation{
		SchemaVersion: reviewVerdictPublicationSchemaVersion,
		HeadSHA:       headSHA,
		ResultState:   ReviewCycleResultCompleteWithFindings,
		Findings: []reviewVerdictFinding{{
			ID:      "finding-1",
			Summary: "a confirmed patch-caused issue",
		}},
	}

	approvedDigest, err := digestReviewVerdictAggregation(approved)
	if err != nil {
		t.Fatalf("digestReviewVerdictAggregation(approved) error = %v", err)
	}
	changesRequiredDigest, err := digestReviewVerdictAggregation(changesRequired)
	if err != nil {
		t.Fatalf("digestReviewVerdictAggregation(changesRequired) error = %v", err)
	}
	if approvedDigest == changesRequiredDigest {
		t.Fatal("test fixture aggregations must digest differently")
	}

	approvedID, err := reviewVerdictPublicationIdentity(
		owner, repo, pr, headSHA, approvedDigest,
	)
	if err != nil {
		t.Fatalf("reviewVerdictPublicationIdentity(approved) error = %v", err)
	}
	changesRequiredID, err := reviewVerdictPublicationIdentity(
		owner, repo, pr, headSHA, changesRequiredDigest,
	)
	if err != nil {
		t.Fatalf("reviewVerdictPublicationIdentity(changesRequired) error = %v", err)
	}
	if approvedID == changesRequiredID {
		t.Fatalf(
			"two reviewers with different verdicts for the same head SHA computed the same publication ID = %s, want distinct IDs",
			approvedID,
		)
	}
}

// TestPrepareReviewVerdictPublicationPreservesPersistedIdentity is the
// regression test for the rollout hazard called out on issue #140:
// prepareReviewVerdictPublication recomputes aggregation and identity fresh
// on every attempt, including retries of a reviewer that already persisted
// a publication (under whatever formula was in effect when it first did
// so). AgentManager.prepareReviewVerdictPublication hard-fails if a freshly
// computed ID doesn't match the persisted one, so if this function ever
// recomputed a new-formula ID for an already-in-flight reviewer, every such
// reviewer would permanently fail its very next reconciliation attempt.
// This asserts the already-persisted ID is reused verbatim rather than
// recomputed, regardless of what a fresh computation would now produce.
func TestPrepareReviewVerdictPublicationPreservesPersistedIdentity(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	reviewer, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", harness.reviewer.ID)
	}
	if reviewer.ReviewCycle.VerdictPublication != nil {
		t.Fatal("test fixture reviewer must not already have a persisted publication")
	}

	// Simulate a publication already persisted under a formula that would
	// no longer be recomputed identically today (a legacy head-only-derived
	// ID, distinct from anything digestReviewVerdictAggregation would ever
	// produce for this reviewer's actual aggregation).
	legacyID := reviewVerdictPublicationIDPrefix + strings.Repeat("f", 64)
	aggregation, err := buildReviewVerdictAggregation(reviewer.ReviewCycle)
	if err != nil {
		t.Fatalf("buildReviewVerdictAggregation() error = %v", err)
	}
	aggregationDigest, err := digestReviewVerdictAggregation(aggregation)
	if err != nil {
		t.Fatalf("digestReviewVerdictAggregation() error = %v", err)
	}
	body, verdict := formatReviewVerdictPublicationBody(
		reviewer.ID,
		legacyID,
		aggregationDigest,
		aggregation,
	)
	legacyPublication := &ReviewVerdictPublication{
		SchemaVersion:     reviewVerdictPublicationSchemaVersion,
		ID:                legacyID,
		ReviewerID:        reviewer.ID,
		HeadSHA:           reviewer.ReviewCycle.HeadSHA,
		Verdict:           verdict,
		ResultState:       aggregation.ResultState,
		AggregationDigest: aggregationDigest,
		BodyDigest:        digestReviewVerdictBody(body),
		Body:              body,
		Status:            ReviewVerdictPublicationPending,
		PreparedAt:        time.Now().UTC(),
	}
	harness.agents.mu.Lock()
	harness.agents.agents[reviewer.ID].ReviewCycle.VerdictPublication = legacyPublication
	harness.agents.mu.Unlock()

	reloaded, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found after update", harness.reviewer.ID)
	}
	prepared, err := prepareReviewVerdictPublication(
		reloaded,
		harness.bot.cfg.RepoOwner,
		harness.bot.cfg.RepoName,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("prepareReviewVerdictPublication() error = %v", err)
	}
	if prepared.ID != legacyID {
		t.Fatalf(
			"prepareReviewVerdictPublication() recomputed ID = %s, want the persisted legacy ID %s reused verbatim",
			prepared.ID,
			legacyID,
		)
	}
}

// TestPublishReviewCycleVerdictPublishesUnderPersistedLegacyIdentity is the
// end-to-end counterpart of
// TestPrepareReviewVerdictPublicationPreservesPersistedIdentity: a reviewer
// that already persisted a (pending, not-yet-published) publication under a
// pre-existing identity -- standing in for one still in flight when the
// identity formula changed -- must publish successfully under that exact
// identity via the full publishReviewCycleVerdict path, not fail or drift
// to a freshly recomputed one.
func TestPublishReviewCycleVerdictPublishesUnderPersistedLegacyIdentity(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	reviewer, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", harness.reviewer.ID)
	}

	legacyID := reviewVerdictPublicationIDPrefix + strings.Repeat("e", 64)
	aggregation, err := buildReviewVerdictAggregation(reviewer.ReviewCycle)
	if err != nil {
		t.Fatalf("buildReviewVerdictAggregation() error = %v", err)
	}
	aggregationDigest, err := digestReviewVerdictAggregation(aggregation)
	if err != nil {
		t.Fatalf("digestReviewVerdictAggregation() error = %v", err)
	}
	body, verdict := formatReviewVerdictPublicationBody(
		reviewer.ID,
		legacyID,
		aggregationDigest,
		aggregation,
	)
	legacyPublication := &ReviewVerdictPublication{
		SchemaVersion:     reviewVerdictPublicationSchemaVersion,
		ID:                legacyID,
		ReviewerID:        reviewer.ID,
		HeadSHA:           reviewer.ReviewCycle.HeadSHA,
		Verdict:           verdict,
		ResultState:       aggregation.ResultState,
		AggregationDigest: aggregationDigest,
		BodyDigest:        digestReviewVerdictBody(body),
		Body:              body,
		Status:            ReviewVerdictPublicationPending,
		PreparedAt:        time.Now().UTC(),
	}
	harness.agents.mu.Lock()
	harness.agents.agents[reviewer.ID].ReviewCycle.VerdictPublication = legacyPublication
	harness.agents.mu.Unlock()

	fixture := &reviewVerdictGitHubFixture{
		headSHA: harness.reviewer.ReviewCycle.HeadSHA,
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	publication, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("publishReviewCycleVerdict() error = %v, want the persisted legacy identity to publish cleanly", err)
	}
	if publication.ID != legacyID {
		t.Fatalf(
			"published verdict ID = %s, want the persisted legacy ID %s",
			publication.ID,
			legacyID,
		)
	}
	if publication.Status != ReviewVerdictPublicationPublished {
		t.Fatalf("published verdict = %#v", publication)
	}
	_, _, createRequests, comments := fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"legacy-identity publication created %d requests and left %d comments, want one each",
			createRequests,
			len(comments),
		)
	}
	if identity, ok := reviewVerdictMarkerValue(
		comments[0].GetBody(),
		reviewVerdictPublicationIDMarker,
	); !ok || identity != legacyID {
		t.Fatalf(
			"visible verdict identity = %q, want the persisted legacy ID %q",
			identity,
			legacyID,
		)
	}
}

func TestReviewVerdictRetriesTransientHeadLookup(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	fixture := &reviewVerdictGitHubFixture{
		headSHA:      harness.reviewer.ReviewCycle.HeadSHA,
		headFailures: 1,
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	if _, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	); err != nil {
		t.Fatalf("publishReviewCycleVerdict() error = %v", err)
	}
	headRequests, _, createRequests, comments := fixture.snapshot()
	if headRequests < 2 || createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"transient head retry requests = head:%d create:%d comments:%d",
			headRequests,
			createRequests,
			len(comments),
		)
	}
}

func TestPendingReviewVerdictRetriesOnPollWithoutRestart(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	fixture := &reviewVerdictGitHubFixture{
		headSHA:      harness.reviewer.ReviewCycle.HeadSHA,
		listFailures: reviewVerdictPublicationMaxAttempts,
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	if _, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	); err == nil || !strings.Contains(
		err.Error(),
		"failed to list comments",
	) {
		t.Fatalf(
			"publishReviewCycleVerdict(transient exhaustion) error = %v",
			err,
		)
	}
	harness.bot.reviewCycleRecoveryInitMu.Lock()
	harness.bot.reviewCycleRecoveryReady = true
	harness.bot.reviewCycleRecoveryInitMu.Unlock()

	if err := harness.bot.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce() error = %v", err)
	}
	current, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle.VerdictPublication == nil ||
		current.ReviewCycle.VerdictPublication.Status !=
			ReviewVerdictPublicationPublished {
		t.Fatalf(
			"poll-retried publication = %#v",
			current.ReviewCycle,
		)
	}
	_, listRequests, createRequests, comments := fixture.snapshot()
	if listRequests != reviewVerdictPublicationMaxAttempts+1 ||
		createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"poll retry requests = list:%d create:%d comments:%d",
			listRequests,
			createRequests,
			len(comments),
		)
	}
}

func TestPendingReviewVerdictPublishesOnceAfterRestart(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	fixture := &reviewVerdictGitHubFixture{
		headSHA:      harness.reviewer.ReviewCycle.HeadSHA,
		listFailures: reviewVerdictPublicationMaxAttempts,
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()

	if _, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	); err == nil || !strings.Contains(
		err.Error(),
		"failed to list comments",
	) {
		t.Fatalf(
			"publishReviewCycleVerdict(transient exhaustion) error = %v",
			err,
		)
	}
	pending, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || pending.ReviewCycle.VerdictPublication == nil ||
		pending.ReviewCycle.VerdictPublication.Status !=
			ReviewVerdictPublicationPending {
		t.Fatalf("pending publication = %#v", pending.ReviewCycle)
	}

	restarted := &Orchestrator{
		cfg:    harness.bot.cfg,
		github: harness.bot.github,
		agents: NewAgentManager(),
		runner: &stubRunner{aliveSet: true, alive: false},
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	if err := restarted.reconcilePersistedReviewCycles(
		context.Background(),
	); err != nil {
		t.Fatalf("reconcilePersistedReviewCycles() error = %v", err)
	}
	restarted.waitForPersistedReviewCycleRecoveries()

	recovered, ok := restarted.agents.Get(harness.reviewer.ID)
	if !ok || recovered.ReviewCycle.VerdictPublication == nil ||
		recovered.ReviewCycle.VerdictPublication.Status !=
			ReviewVerdictPublicationPublished {
		t.Fatalf(
			"recovered publication = %#v",
			recovered.ReviewCycle,
		)
	}
	_, _, createRequests, comments := fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"restart recovery created %d requests and %d comments, want one each",
			createRequests,
			len(comments),
		)
	}
}

func TestReviewVerdictRechecksHeadImmediatelyBeforeCreate(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	fixture := &reviewVerdictGitHubFixture{
		headSequence: []string{
			harness.reviewer.ReviewCycle.HeadSHA,
			harness.reviewer.ReviewCycle.HeadSHA,
			testOtherReviewHeadSHA,
		},
	}
	server := attachReviewVerdictGitHub(t, harness, fixture)
	defer server.Close()
	harness.bot.runner = &stubRunner{aliveSet: true, alive: false}
	harness.bot.reviewSuccessorLauncher = func(
		context.Context,
		Agent,
		string,
	) error {
		return nil
	}

	_, err := harness.bot.publishReviewCycleVerdict(
		context.Background(),
		harness.reviewer.ID,
	)
	if !errors.Is(err, errReviewCycleStale) {
		t.Fatalf(
			"publishReviewCycleVerdict(stale) error = %v, want stale",
			err,
		)
	}
	_, _, createRequests, comments := fixture.snapshot()
	if createRequests != 0 || len(comments) != 0 {
		t.Fatalf(
			"stale exact head created %d requests and %d comments",
			createRequests,
			len(comments),
		)
	}
	current, _ := harness.agents.Get(harness.reviewer.ID)
	if !current.ReviewCycle.Stale ||
		current.ReviewCycle.SupersededByHeadSHA != testOtherReviewHeadSHA {
		t.Fatalf("stale publication cycle = %#v", current.ReviewCycle)
	}
	harness.bot.waitForStaleReviewCleanups()
}

func TestReviewReportPublishesEveryTrustedFinalResult(t *testing.T) {
	harness := newTerminalReviewVerdictHarness(t)
	base := cloneReviewCycle(harness.reviewer.ReviewCycle)
	base.CanonicalFindings = nil
	base.Convergence.FindingVerifications = nil
	base.UnresolvedCoverage = nil
	base.Convergence.ChallengeAssignments = nil
	if !base.ApprovalEligible() {
		t.Fatal("fully resolved base cycle is not approval eligible")
	}

	tests := []struct {
		name        string
		mutate      func(*ReviewCycleState)
		publishable bool
	}{
		{name: "approved", mutate: func(*ReviewCycleState) {}, publishable: true},
		{
			name: "confirmed finding",
			mutate: func(cycle *ReviewCycleState) {
				finding := convergenceCandidate(90)
				cycle.CanonicalFindings = []ReviewCanonicalFinding{
					findingCandidateToCanonicalForVerdictTest(
						"finding-confirmed",
						cycle.HeadSHA,
						finding,
					),
				}
				cycle.Convergence.FindingVerifications =
					[]ReviewFindingVerification{{
						FindingID: "finding-confirmed",
						ExactSHA:  cycle.HeadSHA,
						Status:    ReviewFindingVerificationConfirmed,
					}}
			},
			publishable: true,
		},
		{
			name: "required lane",
			mutate: func(cycle *ReviewCycleState) {
				cycle.Convergence = nil
				latest := len(cycle.DiscoveryPasses) - 1
				cycle.DiscoveryPasses[latest].Lanes[0].Status =
					ReviewDiscoveryLaneFailed
			},
			publishable: false,
		},
		{
			name: "candidate",
			mutate: func(cycle *ReviewCycleState) {
				finding := convergenceCandidate(90)
				finding.CandidateID = "candidate-unresolved"
				cycle.CanonicalFindings = []ReviewCanonicalFinding{
					findingCandidateToCanonicalForVerdictTest(
						"finding-unresolved",
						cycle.HeadSHA,
						finding,
					),
				}
				cycle.Convergence.FindingVerifications =
					[]ReviewFindingVerification{{
						FindingID: "finding-unresolved",
						ExactSHA:  cycle.HeadSHA,
						Status:    ReviewFindingVerificationPending,
					}}
			},
			publishable: true,
		},
		{
			name: "coverage gap",
			mutate: func(cycle *ReviewCycleState) {
				cycle.UnresolvedCoverage = []ReviewCoverageGap{{
					ID:            "gap-required",
					RequirementID: "requirement-required",
					Status:        ReviewCoverageGapMissing,
					Description:   "required path is unreviewed",
				}}
			},
			publishable: true,
		},
		{
			name: "limit exhausted",
			mutate: func(cycle *ReviewCycleState) {
				cycle.LimitTransition = &ReviewLimitTransition{
					Kind: ReviewLimitWallTime,
				}
			},
			publishable: true,
		},
		{
			name: "stale head",
			mutate: func(cycle *ReviewCycleState) {
				cycle.Stale = true
			},
		},
		{
			name: "no trusted result",
			mutate: func(cycle *ReviewCycleState) {
				cycle.ArtifactReceipts = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cycle := cloneReviewCycle(base)
			test.mutate(cycle)
			if got := reviewCycleHasPublishableReport(cycle); got != test.publishable {
				t.Fatalf("reviewCycleHasPublishableReport() = %t, want %t", got, test.publishable)
			}
			_, err := buildReviewVerdictAggregation(cycle)
			if !test.publishable {
				if !errors.Is(err, errReviewResultNotPublishable) {
					t.Fatalf("buildReviewVerdictAggregation() error = %v, want non-publishable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildReviewVerdictAggregation() error = %v", err)
			}
		})
	}
}

func TestReviewVerdictBodyContainsOnlyPublicReviewResult(t *testing.T) {
	aggregation := reviewVerdictAggregation{
		SchemaVersion: reviewVerdictPublicationSchemaVersion,
		HeadSHA:       testReviewHeadSHA,
		ResultState:   ReviewCycleResultCompleteWithFindings,
		Findings: []reviewVerdictFinding{{
			ID:       "finding-confirmed",
			Summary:  "confirmed patch-caused defect",
			Location: "internal/review.go:42",
		}},
	}
	body, verdict := formatReviewVerdictPublicationBody(
		"reviewer-public",
		"publication-public",
		strings.Repeat("a", 64),
		aggregation,
	)
	if verdict != ReviewVerdictNeedsChanges ||
		!strings.Contains(body, "Generated by Repository Agent Orchestrator") ||
		!strings.Contains(body, "confirmed patch-caused defect") ||
		strings.Contains(body, "coverage_gap") ||
		strings.Contains(body, "mandatory work") ||
		strings.Contains(body, "unverified") {
		t.Fatalf("public review body leaked internal convergence state: verdict=%q\n%s", verdict, body)
	}
}

func findingCandidateToCanonicalForVerdictTest(
	findingID string,
	headSHA string,
	candidate ReviewFindingCandidate,
) ReviewCanonicalFinding {
	return ReviewCanonicalFinding{
		ID:                findingID,
		ExactSHA:          headSHA,
		Summary:           candidate.Summary,
		Location:          candidate.Location,
		BehavioralPath:    candidate.BehavioralPath,
		ViolatedInvariant: candidate.ViolatedInvariant,
		Severity:          candidate.Severity,
		Confidence:        candidate.Confidence,
		Evidence:          candidate.Evidence,
		Provenance:        []ReviewFindingProvenance{},
	}
}
