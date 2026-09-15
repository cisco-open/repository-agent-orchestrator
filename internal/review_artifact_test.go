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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReviewArtifactRelativePathsHaveOneCanonicalSafeForm(t *testing.T) {
	canonical, ok := canonicalReviewRelativePath("./internal/review.go")
	if !ok || canonical != "internal/review.go" {
		t.Fatalf("canonicalReviewRelativePath(./internal/review.go) = %q, %t", canonical, ok)
	}
	for _, path := range []string{
		"/tmp/review.go",
		"../review.go",
		"internal/../review.go",
		`internal\\review.go`,
		".",
	} {
		if safeReviewRelativePath(path) {
			t.Fatalf("safeReviewRelativePath(%q) = true, want false", path)
		}
	}
}

func TestReviewArtifactEnvelopeRoundTripsEveryPayloadKind(t *testing.T) {
	tests := []struct {
		name    string
		role    AgentProfileRole
		lane    string
		phase   ReviewArtifactPhase
		payload ReviewArtifactPayload
	}{
		{
			name:  "discovery",
			role:  AgentProfileRoleDiscovery,
			lane:  "contract",
			phase: ReviewArtifactPhaseDiscovery,
			payload: ReviewArtifactPayload{
				Kind: ReviewArtifactPayloadDiscovery,
				Discovery: &ReviewDiscoveryPayload{
					Summary:         "one finding",
					UnreviewedAreas: []string{},
					Candidates: []ReviewFindingCandidate{{
						CandidateID: "candidate-1",
						Summary:     "a lifecycle problem",
						Location: ReviewFindingLocation{
							Path:   "internal/app.go",
							Symbol: "pollReviewAgent",
						},
						BehavioralPath:    "poll exits while cleanup is running",
						ViolatedInvariant: "cleanup finishes before exit",
						Severity:          ReviewFindingSeverityHigh,
						Confidence:        ReviewFindingConfidenceLow,
						Evidence: []ReviewEvidence{{
							Summary: "exit can overtake cleanup",
							Path:    "internal/app.go",
						}},
					}},
					Coverage: []ReviewCoverageClaim{},
				},
			},
		},
		{
			name:  "verification",
			role:  AgentProfileRoleVerifier,
			lane:  "verification",
			phase: ReviewArtifactPhaseVerification,
			payload: ReviewArtifactPayload{
				Kind: ReviewArtifactPayloadVerification,
				Verification: &ReviewVerificationPayload{
					FindingID:        "finding-1",
					Outcome:          ReviewVerificationConfirmed,
					Summary:          "reproduced",
					ScopeDisposition: ReviewScopeInScope,
					PatchDisposition: ReviewPatchIntroduced,
					Evidence:         []ReviewEvidence{},
					CausalEvidence:   []ReviewEvidence{},
					TestEvidence:     []ReviewEvidence{},
				},
			},
		},
		{
			name:  "challenge",
			role:  AgentProfileRoleChallenge,
			lane:  "challenge",
			phase: ReviewArtifactPhaseChallenge,
			payload: ReviewArtifactPayload{
				Kind: ReviewArtifactPayloadChallenge,
				Challenge: &ReviewChallengePayload{
					Outcome:      ReviewChallengeUpheld,
					Summary:      "evidence survives challenge",
					AssignmentID: "challenge-1",
					TargetKind:   ReviewChallengeCoverageGap,
					TargetID:     "coverage-gap-1",
					Candidates:   []ReviewFindingCandidate{},
					Coverage:     []ReviewCoverageClaim{},
				},
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ownership := testReviewArtifactOwnership(
				t,
				test.role,
				test.lane,
				index+1,
				t.TempDir(),
			)
			envelope, err := newReviewArtifactEnvelope(
				testReviewHeadSHA,
				ownership,
				test.phase,
				test.payload,
			)
			if err != nil {
				t.Fatalf("newReviewArtifactEnvelope() error = %v", err)
			}
			body, err := marshalReviewArtifactEnvelope(envelope)
			if err != nil {
				t.Fatalf("marshalReviewArtifactEnvelope() error = %v", err)
			}
			roundTripped, err := unmarshalReviewArtifactEnvelope(body)
			if err != nil {
				t.Fatalf("unmarshalReviewArtifactEnvelope() error = %v", err)
			}
			if !reflect.DeepEqual(roundTripped, envelope) {
				t.Fatalf(
					"round-tripped envelope = %#v, want %#v",
					roundTripped,
					envelope,
				)
			}
		})
	}
}

func TestReviewArtifactEnvelopeRejectsOmissionsAndUnknownFields(
	t *testing.T,
) {
	ownership := testReviewArtifactOwnership(
		t,
		AgentProfileRoleDiscovery,
		"contract",
		1,
		t.TempDir(),
	)
	envelope := testDiscoveryArtifactEnvelope(t, ownership, "safe")
	body, err := marshalReviewArtifactEnvelope(envelope)
	if err != nil {
		t.Fatalf("marshalReviewArtifactEnvelope() error = %v", err)
	}

	tests := []struct {
		name     string
		mutate   func([]byte) []byte
		wantCode ReviewArtifactFailureCode
	}{
		{
			name: "missing exact SHA",
			mutate: func(body []byte) []byte {
				return bytes.Replace(
					body,
					[]byte(`"exact_sha":"`+testReviewHeadSHA+`",`),
					nil,
					1,
				)
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "missing worker identity",
			mutate: func(body []byte) []byte {
				return bytes.Replace(
					body,
					[]byte(`"worker_id":"`+ownership.OwnerID+`",`),
					nil,
					1,
				)
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "unknown envelope field",
			mutate: func(body []byte) []byte {
				return bytes.Replace(
					body,
					[]byte(`{"exact_sha":`),
					[]byte(`{"future":true,"exact_sha":`),
					1,
				)
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "unknown payload field",
			mutate: func(body []byte) []byte {
				return bytes.Replace(
					body,
					[]byte(`"kind":"discovery",`),
					[]byte(`"kind":"discovery","future":true,`),
					1,
				)
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "null required lists",
			mutate: func(body []byte) []byte {
				body = bytes.Replace(
					body,
					[]byte(`"candidates":[]`),
					[]byte(`"candidates":null`),
					1,
				)
				body = bytes.Replace(
					body,
					[]byte(`"coverage":[]`),
					[]byte(`"coverage":null`),
					1,
				)
				return body
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "missing unreviewed areas",
			mutate: func(body []byte) []byte {
				return bytes.Replace(
					body,
					[]byte(`,"unreviewed_areas":[]`),
					nil,
					1,
				)
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "invalid unreviewed area",
			mutate: func(body []byte) []byte {
				return bytes.Replace(
					body,
					[]byte(`"unreviewed_areas":[]`),
					[]byte(`"unreviewed_areas":[" incomplete "]`),
					1,
				)
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "obsolete schema field",
			mutate: func(body []byte) []byte {
				return bytes.Replace(
					body,
					[]byte(`{"exact_sha":`),
					[]byte(`{"schema_version":3,"exact_sha":`),
					1,
				)
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := unmarshalReviewArtifactEnvelope(test.mutate(body))
			assertReviewArtifactError(
				t,
				err,
				ReviewArtifactFailureTerminal,
				test.wantCode,
			)
		})
	}
}

func TestReviewArtifactPayloadHeaderDecodePreservesDiagnostic(t *testing.T) {
	_, err := unmarshalReviewArtifactPayload([]byte(`[]`))
	artifactErr := asReviewArtifactError(err)
	if artifactErr == nil ||
		artifactErr.Code != ReviewArtifactFailureMalformed ||
		!strings.Contains(artifactErr.Detail, "cannot unmarshal array") {
		t.Fatalf("payload header decode error = %#v", artifactErr)
	}
}

func TestReviewArtifactFailureAllowsOnlySafeFreshWorkerRetries(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "transient I/O",
			err: newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureIO,
			),
			want: true,
		},
		{
			name: "malformed worker output",
			err: newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureMalformed,
			),
			want: true,
		},
		{
			name: "secret material",
			err: newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureSecretMaterial,
			),
		},
		{
			name: "stale SHA",
			err: newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureStaleSHA,
			),
		},
		{name: "non-artifact failure", err: errors.New("runtime failed")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := reviewArtifactFailureAllowsFreshWorkerRetry(test.err); got != test.want {
				t.Fatalf("retryable = %v, want %v", got, test.want)
			}
		})
	}
}

func TestReviewArtifactPublicationIsAtomicBoundedAndRetryAware(t *testing.T) {
	t.Run("interrupted write never exposes partial artifact", func(t *testing.T) {
		ownership, envelope, store := newReviewArtifactPublicationTest(
			t,
			defaultReviewArtifactPolicy(),
		)
		store.policy.Retries = 0
		store.write = func(
			_ context.Context,
			target *os.File,
			body []byte,
		) error {
			if _, err := target.Write(body[:len(body)/2]); err != nil {
				return err
			}
			return errors.New("simulated crash")
		}

		_, err := store.publish(
			context.Background(),
			testReviewHeadSHA,
			ownership,
			envelope,
		)
		assertReviewArtifactError(
			t,
			err,
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
		published, err := store.published()
		if err != nil {
			t.Fatalf("Published() error = %v", err)
		}
		if len(published) != 0 {
			t.Fatalf("published artifacts = %v, want none", published)
		}
		if err := os.WriteFile(
			filepath.Join(
				store.directory,
				reviewArtifactTemporaryPrefix+"abandoned.json",
			),
			[]byte(`{"partial":true}`),
			0o600,
		); err != nil {
			t.Fatalf("WriteFile(abandoned partial) error = %v", err)
		}
		published, err = store.published()
		if err != nil {
			t.Fatalf("Published() after abandoned partial error = %v", err)
		}
		if len(published) != 0 {
			t.Fatalf(
				"published artifacts after abandoned partial = %v, want none",
				published,
			)
		}
	})

	t.Run("oversized artifact fails deterministically", func(t *testing.T) {
		policy := defaultReviewArtifactPolicy()
		policy.MaxBytes = 128
		ownership, envelope, store := newReviewArtifactPublicationTest(
			t,
			policy,
		)
		for attempt := 0; attempt < 2; attempt++ {
			_, err := store.publish(
				context.Background(),
				testReviewHeadSHA,
				ownership,
				envelope,
			)
			assertReviewArtifactError(
				t,
				err,
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureOversized,
			)
		}
		published, err := store.published()
		if err != nil {
			t.Fatalf("Published() error = %v", err)
		}
		if len(published) != 0 {
			t.Fatalf("published artifacts = %v, want none", published)
		}
	})

	t.Run("secret-bearing artifact is rejected before persistence", func(t *testing.T) {
		ownership, envelope, store := newReviewArtifactPublicationTest(
			t,
			defaultReviewArtifactPolicy(),
		)
		envelope.Payload.Discovery.Summary =
			"SECRET=must-not-reach-the-artifact-directory"
		_, err := store.publish(
			context.Background(),
			testReviewHeadSHA,
			ownership,
			envelope,
		)
		assertReviewArtifactError(
			t,
			err,
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureSecretMaterial,
		)
		published, err := store.published()
		if err != nil {
			t.Fatalf("published() error = %v", err)
		}
		if len(published) != 0 {
			t.Fatalf("published artifacts = %v, want none", published)
		}
	})

	t.Run("transient write retries within configured bound", func(t *testing.T) {
		policy := defaultReviewArtifactPolicy()
		policy.Retries = 1
		ownership, envelope, store := newReviewArtifactPublicationTest(
			t,
			policy,
		)
		calls := 0
		store.write = func(
			ctx context.Context,
			target *os.File,
			body []byte,
		) error {
			calls++
			if calls == 1 {
				return errors.New("temporary write failure")
			}
			return writeReviewArtifactBody(ctx, target, body)
		}
		path, err := store.publish(
			context.Background(),
			testReviewHeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		if calls != 2 {
			t.Fatalf("write calls = %d, want 2", calls)
		}
		if filepath.Base(path) != reviewArtifactFilename(ownership) {
			t.Fatalf("published path = %q", path)
		}
	})

	t.Run("publication timeout is classified as transient", func(t *testing.T) {
		policy := defaultReviewArtifactPolicy()
		policy.TimeoutSeconds = 1
		policy.Retries = 0
		ownership, envelope, store := newReviewArtifactPublicationTest(
			t,
			policy,
		)
		store.write = func(
			ctx context.Context,
			_ *os.File,
			_ []byte,
		) error {
			<-ctx.Done()
			return ctx.Err()
		}
		startedAt := time.Now()
		_, err := store.publish(
			context.Background(),
			testReviewHeadSHA,
			ownership,
			envelope,
		)
		assertReviewArtifactError(
			t,
			err,
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureTimeout,
		)
		if elapsed := time.Since(startedAt); elapsed < 900*time.Millisecond ||
			elapsed > 2*time.Second {
			t.Fatalf("publication timeout elapsed = %s, want about 1s", elapsed)
		}
	})

	t.Run("different attempts have immutable destinations", func(t *testing.T) {
		worktreeDir := t.TempDir()
		identity := ReviewWorkerIdentity{
			CycleID:  "review-cycle-abcdef123456-0123456789abcdef0123456789abcdef",
			Revision: 1,
			Role:     AgentProfileRoleDiscovery,
			Pass:     1,
			Lane:     "contract",
		}
		first := testReviewArtifactOwnershipForIdentity(
			t,
			identity,
			1,
			worktreeDir,
			"00000000000000000000000000000001",
		)
		second := testReviewArtifactOwnershipForIdentity(
			t,
			identity,
			2,
			worktreeDir,
			"00000000000000000000000000000002",
		)
		firstEnvelope := testDiscoveryArtifactEnvelope(t, first, "attempt one")
		secondEnvelope := testDiscoveryArtifactEnvelope(
			t,
			second,
			"attempt two",
		)
		firstDirectory, err := reviewWorkerArtifactDirectory(
			first.WorktreePath,
		)
		if err != nil {
			t.Fatalf("reviewWorkerArtifactDirectory(first) error = %v", err)
		}
		secondDirectory, err := reviewWorkerArtifactDirectory(
			second.WorktreePath,
		)
		if err != nil {
			t.Fatalf("reviewWorkerArtifactDirectory(second) error = %v", err)
		}
		firstStore, err := newReviewArtifactStore(
			firstDirectory,
			defaultReviewArtifactPolicy(),
		)
		if err != nil {
			t.Fatalf("newReviewArtifactStore(first) error = %v", err)
		}
		secondStore, err := newReviewArtifactStore(
			secondDirectory,
			defaultReviewArtifactPolicy(),
		)
		if err != nil {
			t.Fatalf("newReviewArtifactStore(second) error = %v", err)
		}
		firstPath, err := firstStore.publish(
			context.Background(),
			testReviewHeadSHA,
			first,
			firstEnvelope,
		)
		if err != nil {
			t.Fatalf("Publish(first) error = %v", err)
		}
		firstBefore, err := os.ReadFile(firstPath)
		if err != nil {
			t.Fatalf("ReadFile(first) error = %v", err)
		}
		secondPath, err := secondStore.publish(
			context.Background(),
			testReviewHeadSHA,
			second,
			secondEnvelope,
		)
		if err != nil {
			t.Fatalf("Publish(second) error = %v", err)
		}
		if firstPath == secondPath {
			t.Fatalf("attempt paths collided at %q", firstPath)
		}
		firstAfter, err := os.ReadFile(firstPath)
		if err != nil {
			t.Fatalf("ReadFile(first after retry) error = %v", err)
		}
		if !bytes.Equal(firstAfter, firstBefore) {
			t.Fatal("publishing a retry overwrote the prior attempt")
		}
		if _, err := os.Stat(secondPath); err != nil {
			t.Fatalf("second attempt artifact is missing: %v", err)
		}
	})

	t.Run("completed attempt cannot be overwritten", func(t *testing.T) {
		ownership, envelope, store := newReviewArtifactPublicationTest(
			t,
			defaultReviewArtifactPolicy(),
		)
		path, err := store.publish(
			context.Background(),
			testReviewHeadSHA,
			ownership,
			envelope,
		)
		if err != nil {
			t.Fatalf("Publish(first) error = %v", err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(first) error = %v", err)
		}
		replacement := testDiscoveryArtifactEnvelope(
			t,
			ownership,
			"replacement",
		)
		_, err = store.publish(
			context.Background(),
			testReviewHeadSHA,
			ownership,
			replacement,
		)
		assertReviewArtifactError(
			t,
			err,
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureConflict,
		)
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(after conflict) error = %v", err)
		}
		if !bytes.Equal(after, before) {
			t.Fatal("conflicting publication overwrote a completed attempt")
		}
	})

	t.Run("concurrent same-owner publication cannot replace winner", func(t *testing.T) {
		ownership, first, store := newReviewArtifactPublicationTest(
			t,
			defaultReviewArtifactPolicy(),
		)
		second := testDiscoveryArtifactEnvelope(
			t,
			ownership,
			"concurrent replacement",
		)
		if err := ensureReviewArtifactDirectory(store.directory); err != nil {
			t.Fatalf("ensureReviewArtifactDirectory() error = %v", err)
		}
		ready := make(chan struct{}, 2)
		release := make(chan struct{})
		store.write = func(
			ctx context.Context,
			target *os.File,
			body []byte,
		) error {
			if err := writeReviewArtifactBody(ctx, target, body); err != nil {
				return err
			}
			ready <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		type publicationResult struct {
			envelope ReviewArtifactEnvelope
			path     string
			err      error
		}
		results := make(chan publicationResult, 2)
		for _, envelope := range []ReviewArtifactEnvelope{first, second} {
			envelope := envelope
			go func() {
				path, err := store.publish(
					context.Background(),
					testReviewHeadSHA,
					ownership,
					envelope,
				)
				results <- publicationResult{
					envelope: envelope,
					path:     path,
					err:      err,
				}
			}()
		}
		for count := 0; count < 2; count++ {
			select {
			case <-ready:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("concurrent publishers did not reach publish barrier")
			}
		}
		close(release)

		var winner publicationResult
		successes := 0
		conflicts := 0
		for count := 0; count < 2; count++ {
			var result publicationResult
			select {
			case result = <-results:
			case <-time.After(time.Second):
				t.Fatal("concurrent publisher did not return")
			}
			if result.err == nil {
				successes++
				winner = result
				continue
			}
			artifactErr := asReviewArtifactError(result.err)
			if artifactErr != nil &&
				artifactErr.Class == ReviewArtifactFailureTerminal &&
				artifactErr.Code == ReviewArtifactFailureConflict {
				conflicts++
				continue
			}
			t.Fatalf("concurrent publication error = %v", result.err)
		}
		if successes != 1 || conflicts != 1 {
			t.Fatalf(
				"concurrent results = successes:%d conflicts:%d",
				successes,
				conflicts,
			)
		}
		body, err := os.ReadFile(winner.path)
		if err != nil {
			t.Fatalf("ReadFile(winning artifact) error = %v", err)
		}
		persisted, err := unmarshalReviewArtifactEnvelope(body)
		if err != nil {
			t.Fatalf("Unmarshal(winning artifact) error = %v", err)
		}
		if !reflect.DeepEqual(persisted, winner.envelope) {
			t.Fatalf(
				"persisted concurrent artifact = %#v, want winner %#v",
				persisted,
				winner.envelope,
			)
		}
	})
}

func TestReviewArtifactIntakeAcceptsOnlyTrustedExactSHACleanEvidence(
	t *testing.T,
) {
	t.Run("accepted artifact advances completion and persists evidence", func(t *testing.T) {
		harness := newReviewArtifactIntakeHarness(t)
		envelope := testDiscoveryArtifactEnvelope(
			t,
			harness.ownership,
			"safe evidence",
		)
		envelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
		envelope.Checkout.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
		path := harness.publish(t, envelope)

		accepted, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			harness.ownership.OwnerID,
			path,
			ReviewArtifactPhaseDiscovery,
		)
		if err != nil {
			t.Fatalf("intakeReviewWorkerArtifact() error = %v", err)
		}
		if !reflect.DeepEqual(accepted, envelope) {
			t.Fatalf("accepted envelope = %#v, want %#v", accepted, envelope)
		}
		stored, ok := harness.agents.Get(harness.reviewer.ID)
		if !ok {
			t.Fatalf("review coordinator %q disappeared", harness.reviewer.ID)
		}
		if len(stored.ReviewCycle.ArtifactReceipts) != 1 ||
			len(stored.ReviewCycle.LaneCompletions) != 1 ||
			len(stored.ReviewCycle.ArtifactFailures) != 0 {
			t.Fatalf(
				"artifact state = receipts:%d completions:%d failures:%d",
				len(stored.ReviewCycle.ArtifactReceipts),
				len(stored.ReviewCycle.LaneCompletions),
				len(stored.ReviewCycle.ArtifactFailures),
			)
		}
		if err := validatePersistedReviewCycleSnapshot(
			stored.ReviewCycle,
		); err != nil {
			t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
		}
		restarted := &Orchestrator{
			cfg:    harness.bot.cfg,
			agents: NewAgentManager(),
		}
		if err := restarted.loadPersistedAgentState(); err != nil {
			t.Fatalf("loadPersistedAgentState() error = %v", err)
		}
		restored, ok := restarted.agents.Get(harness.reviewer.ID)
		if !ok || restored.ReviewCycle == nil ||
			len(restored.ReviewCycle.ArtifactReceipts) != 1 ||
			len(restored.ReviewCycle.LaneCompletions) != 1 {
			t.Fatalf("restored artifact state = %#v", restored.ReviewCycle)
		}
	})

	t.Run("accepted pretty JSON uses canonical persisted digest", func(t *testing.T) {
		harness := newReviewArtifactIntakeHarness(t)
		envelope := testDiscoveryArtifactEnvelope(
			t,
			harness.ownership,
			"safe evidence",
		)
		envelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
		envelope.Checkout.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
		body, err := marshalReviewArtifactEnvelope(envelope)
		if err != nil {
			t.Fatalf("marshalReviewArtifactEnvelope() error = %v", err)
		}
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, body, "", "  "); err != nil {
			t.Fatalf("json.Indent() error = %v", err)
		}
		path := harness.writeRawArtifact(t, pretty.Bytes())

		if _, err := harness.bot.intakeReviewWorkerArtifact(
			context.Background(),
			harness.reviewer.ID,
			harness.ownership.OwnerID,
			path,
			ReviewArtifactPhaseDiscovery,
		); err != nil {
			t.Fatalf("intakeReviewWorkerArtifact() error = %v", err)
		}
		stored, ok := harness.agents.Get(harness.reviewer.ID)
		if !ok {
			t.Fatalf("review coordinator %q disappeared", harness.reviewer.ID)
		}
		if err := validatePersistedReviewCycleSnapshot(
			stored.ReviewCycle,
		); err != nil {
			t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
		}
	})

	tests := []struct {
		name    string
		prepare func(*testing.T, *reviewArtifactIntakeHarness) (
			string,
			string,
		)
		wantCode ReviewArtifactFailureCode
	}{
		{
			name: "dirty checkout",
			prepare: func(
				t *testing.T,
				harness *reviewArtifactIntakeHarness,
			) (string, string) {
				envelope := testDiscoveryArtifactEnvelope(
					t,
					harness.ownership,
					"safe evidence",
				)
				path := harness.publish(t, envelope)
				if err := os.WriteFile(
					filepath.Join(
						harness.ownership.WorktreePath,
						"tracked.txt",
					),
					[]byte("dirty"),
					0o600,
				); err != nil {
					t.Fatalf("WriteFile(dirty checkout) error = %v", err)
				}
				return harness.ownership.OwnerID, path
			},
			wantCode: ReviewArtifactFailureDirtyCheckout,
		},
		{
			name: "stale SHA",
			prepare: func(
				t *testing.T,
				harness *reviewArtifactIntakeHarness,
			) (string, string) {
				envelope := testDiscoveryArtifactEnvelope(
					t,
					harness.ownership,
					"safe evidence",
				)
				envelope.ExactSHA = testOtherReviewHeadSHA
				envelope.Checkout.ExactSHA = testOtherReviewHeadSHA
				body, err := marshalReviewArtifactEnvelope(envelope)
				if err != nil {
					t.Fatalf("marshalReviewArtifactEnvelope(stale) error = %v", err)
				}
				return harness.ownership.OwnerID,
					harness.writeRawArtifact(t, body)
			},
			wantCode: ReviewArtifactFailureStaleSHA,
		},
		{
			name: "unregistered worker",
			prepare: func(
				_ *testing.T,
				harness *reviewArtifactIntakeHarness,
			) (string, string) {
				return "review-worker-unregistered",
					filepath.Join(harness.store.directory, "missing.json")
			},
			wantCode: ReviewArtifactFailureUnregisteredWorker,
		},
		{
			name: "malformed artifact",
			prepare: func(
				t *testing.T,
				harness *reviewArtifactIntakeHarness,
			) (string, string) {
				return harness.ownership.OwnerID,
					harness.writeRawArtifact(t, []byte(`{"malformed":true}`))
			},
			wantCode: ReviewArtifactFailureMalformed,
		},
		{
			name: "secret-bearing artifact",
			prepare: func(
				t *testing.T,
				harness *reviewArtifactIntakeHarness,
			) (string, string) {
				envelope := testDiscoveryArtifactEnvelope(
					t,
					harness.ownership,
					"GH_TOKEN=ultra-private-marker",
				)
				envelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
				envelope.Checkout.ExactSHA =
					harness.reviewer.ReviewCycle.HeadSHA
				body, err := marshalReviewArtifactEnvelope(envelope)
				if err != nil {
					t.Fatalf(
						"marshalReviewArtifactEnvelope(secret) error = %v",
						err,
					)
				}
				return harness.ownership.OwnerID,
					harness.writeRawArtifact(t, body)
			},
			wantCode: ReviewArtifactFailureSecretMaterial,
		},
		{
			name: "invalid evidence path",
			prepare: func(
				t *testing.T,
				harness *reviewArtifactIntakeHarness,
			) (string, string) {
				envelope := testDiscoveryArtifactEnvelope(
					t,
					harness.ownership,
					"safe evidence",
				)
				envelope.Payload.Discovery.Coverage = []ReviewCoverageClaim{{
					RequirementID: "missing-path",
					Kind:          ReviewCoverageCallPath,
					Status:        ReviewCoverageCovered,
					Evidence: []ReviewEvidence{{
						Summary: "nonexistent evidence",
						Path:    "missing.txt",
					}},
				}}
				return harness.ownership.OwnerID, harness.publish(t, envelope)
			},
			wantCode: ReviewArtifactFailureInvalidPath,
		},
		{
			name: "unexpected phase",
			prepare: func(
				t *testing.T,
				harness *reviewArtifactIntakeHarness,
			) (string, string) {
				envelope := testDiscoveryArtifactEnvelope(
					t,
					harness.ownership,
					"safe evidence",
				)
				return harness.ownership.OwnerID, harness.publish(t, envelope)
			},
			wantCode: ReviewArtifactFailureUnexpectedPhase,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newReviewArtifactIntakeHarness(t)
			ownerID, path := test.prepare(t, harness)
			expectedPhase := ReviewArtifactPhaseDiscovery
			if test.name == "unexpected phase" {
				expectedPhase = ReviewArtifactPhaseVerification
			}
			_, err := harness.bot.intakeReviewWorkerArtifact(
				context.Background(),
				harness.reviewer.ID,
				ownerID,
				path,
				expectedPhase,
			)
			assertReviewArtifactError(
				t,
				err,
				ReviewArtifactFailureTerminal,
				test.wantCode,
			)
			stored, ok := harness.agents.Get(harness.reviewer.ID)
			if !ok {
				t.Fatalf(
					"review coordinator %q disappeared",
					harness.reviewer.ID,
				)
			}
			if len(stored.ReviewCycle.ArtifactReceipts) != 0 ||
				len(stored.ReviewCycle.LaneCompletions) != 0 {
				t.Fatalf(
					"rejected intake advanced state: receipts=%d completions=%d",
					len(stored.ReviewCycle.ArtifactReceipts),
					len(stored.ReviewCycle.LaneCompletions),
				)
			}
			if len(stored.ReviewCycle.ArtifactFailures) != 1 {
				t.Fatalf(
					"artifact failures = %d, want 1",
					len(stored.ReviewCycle.ArtifactFailures),
				)
			}
			failure := stored.ReviewCycle.ArtifactFailures[0]
			if failure.Code != test.wantCode ||
				failure.Class != ReviewArtifactFailureTerminal {
				t.Fatalf("recorded failure = %#v", failure)
			}
			stateBody, readErr := os.ReadFile(
				harness.bot.agentStateFilePath(),
			)
			if readErr != nil {
				t.Fatalf("ReadFile(agent state) error = %v", readErr)
			}
			if bytes.Contains(stateBody, []byte("ultra-private-marker")) ||
				strings.Contains(err.Error(), "ultra-private-marker") {
				t.Fatal("rejected artifact secret leaked into failure output")
			}
		})
	}
}

func TestReviewArtifactTerminalRejectionFinalizesAttempt(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	envelope := testDiscoveryArtifactEnvelope(
		t,
		harness.ownership,
		"safe evidence",
	)
	path := harness.publish(t, envelope)

	_, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseVerification,
	)
	assertReviewArtifactError(
		t,
		err,
		ReviewArtifactFailureTerminal,
		ReviewArtifactFailureUnexpectedPhase,
	)

	_, err = harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	)
	assertReviewArtifactError(
		t,
		err,
		ReviewArtifactFailureTerminal,
		ReviewArtifactFailureConflict,
	)
	stored, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after terminal rejection")
	}
	if len(stored.ReviewCycle.ArtifactReceipts) != 0 ||
		len(stored.ReviewCycle.LaneCompletions) != 0 {
		t.Fatalf(
			"terminally rejected attempt advanced state: receipts=%d completions=%d",
			len(stored.ReviewCycle.ArtifactReceipts),
			len(stored.ReviewCycle.LaneCompletions),
		)
	}
	if err := validatePersistedReviewCycleSnapshot(
		stored.ReviewCycle,
	); err != nil {
		t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
	}
}

func TestReviewArtifactIntakeRejectsLatePriorAttempt(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	envelope := testDiscoveryArtifactEnvelope(
		t,
		harness.ownership,
		"late evidence",
	)
	path := harness.publish(t, envelope)
	retry, _ := harness.allocateRetry(t)

	_, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	)
	assertReviewArtifactError(
		t,
		err,
		ReviewArtifactFailureTerminal,
		ReviewArtifactFailureConflict,
	)
	stored, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after late artifact")
	}
	if len(stored.ReviewCycle.ArtifactReceipts) != 0 ||
		len(stored.ReviewCycle.LaneCompletions) != 0 {
		t.Fatalf(
			"late attempt advanced state: receipts=%d completions=%d",
			len(stored.ReviewCycle.ArtifactReceipts),
			len(stored.ReviewCycle.LaneCompletions),
		)
	}
	got := stored.ReviewCycle.WorkerOwnerships[len(stored.ReviewCycle.WorkerOwnerships)-1]
	if got.OwnerID != retry.OwnerID || got.Attempt != 2 {
		t.Fatalf("current retry ownership = %#v, want %#v", got, retry)
	}
	if err := validatePersistedReviewCycleSnapshot(
		stored.ReviewCycle,
	); err != nil {
		t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
	}
}

func TestReviewArtifactCompletionTracksOnlyCurrentAttempt(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	firstEnvelope := testDiscoveryArtifactEnvelope(
		t,
		harness.ownership,
		"first attempt evidence",
	)
	firstPath := harness.publish(t, firstEnvelope)
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		firstPath,
		ReviewArtifactPhaseDiscovery,
	); err != nil {
		t.Fatalf("intakeReviewWorkerArtifact(first) error = %v", err)
	}

	retry, retryStore := harness.allocateRetry(t)
	afterRetry, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || afterRetry.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after retry allocation")
	}
	if len(afterRetry.ReviewCycle.ArtifactReceipts) != 1 ||
		len(afterRetry.ReviewCycle.LaneCompletions) != 0 {
		t.Fatalf(
			"superseded state = receipts:%d completions:%d",
			len(afterRetry.ReviewCycle.ArtifactReceipts),
			len(afterRetry.ReviewCycle.LaneCompletions),
		)
	}

	retryEnvelope := testDiscoveryArtifactEnvelope(
		t,
		retry,
		"retry evidence",
	)
	retryEnvelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
	retryEnvelope.Checkout.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
	retryPath, err := retryStore.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		retry,
		retryEnvelope,
	)
	if err != nil {
		t.Fatalf("Publish(retry) error = %v", err)
	}
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		retry.OwnerID,
		retryPath,
		ReviewArtifactPhaseDiscovery,
	); err != nil {
		t.Fatalf("intakeReviewWorkerArtifact(retry) error = %v", err)
	}

	stored, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after retry acceptance")
	}
	if len(stored.ReviewCycle.ArtifactReceipts) != 2 ||
		len(stored.ReviewCycle.LaneCompletions) != 1 {
		t.Fatalf(
			"retry state = receipts:%d completions:%d",
			len(stored.ReviewCycle.ArtifactReceipts),
			len(stored.ReviewCycle.LaneCompletions),
		)
	}
	completion := stored.ReviewCycle.LaneCompletions[0]
	if completion.WorkerID != retry.OwnerID ||
		completion.Attempt != retry.Attempt {
		t.Fatalf("current lane completion = %#v, want retry %#v", completion, retry)
	}
	if err := validatePersistedReviewCycleSnapshot(
		stored.ReviewCycle,
	); err != nil {
		t.Fatalf("validatePersistedReviewCycleSnapshot() error = %v", err)
	}
}

func TestReviewWorkerRetryReservationCheckpointFailureRollsBackCurrentAttempt(
	t *testing.T,
) {
	harness := newReviewArtifactIntakeHarness(t)
	envelope := testDiscoveryArtifactEnvelope(
		t,
		harness.ownership,
		"durable current attempt evidence",
	)
	path := harness.publish(t, envelope)
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	); err != nil {
		t.Fatalf("intakeReviewWorkerArtifact() error = %v", err)
	}
	before, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || before.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared before retry reservation")
	}
	if len(before.ReviewCycle.WorkerOwnerships) != 1 ||
		len(before.ReviewCycle.ArtifactReceipts) != 1 ||
		len(before.ReviewCycle.LaneCompletions) != 1 {
		t.Fatalf(
			"durable attempt state = owners:%d receipts:%d completions:%d, want 1/1/1",
			len(before.ReviewCycle.WorkerOwnerships),
			len(before.ReviewCycle.ArtifactReceipts),
			len(before.ReviewCycle.LaneCompletions),
		)
	}

	checkpointBlocker := harness.bot.agentStateFilePath() + ".tmp"
	if err := os.Mkdir(checkpointBlocker, 0o700); err != nil {
		t.Fatalf("Mkdir(checkpoint blocker) error = %v", err)
	}
	ownership, err := harness.bot.reserveReviewWorkerOwnership(
		harness.reviewer.ID,
		harness.ownership.Identity,
	)
	if err == nil || !strings.Contains(err.Error(), "before resource creation") {
		t.Fatalf(
			"reserveReviewWorkerOwnership() error = %v, want checkpoint failure",
			err,
		)
	}
	if ownership.OwnerID != "" {
		t.Fatalf(
			"failed retry reservation returned ghost ownership %q",
			ownership.OwnerID,
		)
	}
	afterFailure, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || afterFailure.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after retry reservation failure")
	}
	if !reflect.DeepEqual(
		afterFailure.ReviewCycle.WorkerOwnerships,
		before.ReviewCycle.WorkerOwnerships,
	) ||
		!reflect.DeepEqual(
			afterFailure.ReviewCycle.ArtifactReceipts,
			before.ReviewCycle.ArtifactReceipts,
		) ||
		!reflect.DeepEqual(
			afterFailure.ReviewCycle.LaneCompletions,
			before.ReviewCycle.LaneCompletions,
		) ||
		!afterFailure.LastActivityTime.Equal(before.LastActivityTime) {
		t.Fatalf(
			"failed reservation changed current attempt state: before=%#v after=%#v",
			before.ReviewCycle,
			afterFailure.ReviewCycle,
		)
	}

	if err := os.Remove(checkpointBlocker); err != nil {
		t.Fatalf("Remove(checkpoint blocker) error = %v", err)
	}
	if err := harness.bot.persistAgentState(); err != nil {
		t.Fatalf("persistAgentState(unrelated checkpoint) error = %v", err)
	}
	restartedAgents := NewAgentManager()
	restarted := &Orchestrator{
		cfg:    harness.bot.cfg,
		agents: restartedAgents,
	}
	if err := restarted.loadPersistedAgentState(); err != nil {
		t.Fatalf("loadPersistedAgentState() error = %v", err)
	}
	restored, ok := restartedAgents.Get(harness.reviewer.ID)
	if !ok || restored.ReviewCycle == nil {
		t.Fatal("review coordinator did not survive restart")
	}
	if !reflect.DeepEqual(
		restored.ReviewCycle.WorkerOwnerships,
		before.ReviewCycle.WorkerOwnerships,
	) ||
		!reflect.DeepEqual(
			restored.ReviewCycle.LaneCompletions,
			before.ReviewCycle.LaneCompletions,
		) {
		t.Fatalf(
			"unrelated checkpoint persisted failed retry mutation: %#v",
			restored.ReviewCycle,
		)
	}
	if _, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	); err != nil {
		t.Fatalf(
			"late durable-attempt artifact was rejected after rollback: %v",
			err,
		)
	}
}

func TestReviewArtifactIntakeRejectsEscapedConfiguredSecrets(t *testing.T) {
	tests := []struct {
		name      string
		secret    string
		escaped   string
		configure func(*reviewArtifactIntakeHarness, string)
	}{
		{
			name:    "GitHub token",
			secret:  "configured-token-private-marker",
			escaped: `configured-tok\u0065n-private-marker`,
			configure: func(
				harness *reviewArtifactIntakeHarness,
				secret string,
			) {
				harness.bot.token = secret
			},
		},
		{
			name: "Webex webhook",
			secret: "https://webexapis.com/v1/webhooks/incoming/" +
				"private-marker",
			escaped: `https://webex\u0061pis.com/v1/webhooks/incoming/` +
				`private-marker`,
			configure: func(
				harness *reviewArtifactIntakeHarness,
				secret string,
			) {
				harness.bot.cfg.WebexWebhookURL = secret
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newReviewArtifactIntakeHarness(t)
			test.configure(harness, test.secret)
			envelope := testDiscoveryArtifactEnvelope(
				t,
				harness.ownership,
				test.secret,
			)
			envelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
			envelope.Checkout.ExactSHA =
				harness.reviewer.ReviewCycle.HeadSHA
			body, err := marshalReviewArtifactEnvelope(envelope)
			if err != nil {
				t.Fatalf("marshalReviewArtifactEnvelope() error = %v", err)
			}
			body = bytes.Replace(
				body,
				[]byte(test.secret),
				[]byte(test.escaped),
				1,
			)
			if bytes.Contains(body, []byte(test.secret)) ||
				reviewArtifactContainsProhibitedSecret(body, test.secret) {
				t.Fatal("escaped-secret fixture was detected before JSON decoding")
			}
			path := harness.writeRawArtifact(t, body)

			_, err = harness.bot.intakeReviewWorkerArtifact(
				context.Background(),
				harness.reviewer.ID,
				harness.ownership.OwnerID,
				path,
				ReviewArtifactPhaseDiscovery,
			)
			assertReviewArtifactError(
				t,
				err,
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureSecretMaterial,
			)
			stored, ok := harness.agents.Get(harness.reviewer.ID)
			if !ok || stored.ReviewCycle == nil {
				t.Fatal("review coordinator disappeared after secret rejection")
			}
			if len(stored.ReviewCycle.ArtifactReceipts) != 0 ||
				len(stored.ReviewCycle.LaneCompletions) != 0 ||
				len(stored.ReviewCycle.ArtifactFailures) != 1 {
				t.Fatalf(
					"escaped-secret state = receipts:%d completions:%d failures:%d",
					len(stored.ReviewCycle.ArtifactReceipts),
					len(stored.ReviewCycle.LaneCompletions),
					len(stored.ReviewCycle.ArtifactFailures),
				)
			}
			stateBody, readErr := os.ReadFile(
				harness.bot.agentStateFilePath(),
			)
			if readErr != nil {
				t.Fatalf("ReadFile(agent state) error = %v", readErr)
			}
			if bytes.Contains(stateBody, []byte(test.secret)) ||
				bytes.Contains(stateBody, []byte(test.escaped)) ||
				strings.Contains(err.Error(), test.secret) {
				t.Fatal("escaped configured secret leaked into persisted state")
			}
		})
	}
}

func TestReviewArtifactRejectionSurfacesCheckpointFailure(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	const marker = "untrusted-artifact-body-private-marker"
	path := harness.writeRawArtifact(
		t,
		[]byte(`{"malformed":"`+marker+`"}`),
	)
	occupiedLogPath := filepath.Join(t.TempDir(), "occupied")
	if err := os.WriteFile(occupiedLogPath, []byte("occupied"), 0o600); err != nil {
		t.Fatalf("WriteFile(occupied log path) error = %v", err)
	}
	harness.bot.cfg.LogDir = occupiedLogPath

	_, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	)
	assertReviewArtifactError(
		t,
		err,
		ReviewArtifactFailureTransient,
		ReviewArtifactFailureIO,
	)
	if strings.Contains(err.Error(), marker) {
		t.Fatal("artifact content leaked through checkpoint failure")
	}
	stored, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || stored.ReviewCycle == nil ||
		len(stored.ReviewCycle.ArtifactFailures) != 1 {
		t.Fatalf("in-memory rejection state = %#v", stored.ReviewCycle)
	}
	if failure := stored.ReviewCycle.ArtifactFailures[0]; failure.Class != ReviewArtifactFailureTerminal ||
		failure.Code != ReviewArtifactFailureMalformed {
		t.Fatalf("in-memory rejection = %#v", failure)
	}
}

func TestReviewArtifactAcceptanceRollsBackCheckpointFailure(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	envelope := testDiscoveryArtifactEnvelope(
		t,
		harness.ownership,
		"safe evidence",
	)
	path := harness.publish(t, envelope)
	occupiedLogPath := filepath.Join(t.TempDir(), "occupied")
	if err := os.WriteFile(occupiedLogPath, []byte("occupied"), 0o600); err != nil {
		t.Fatalf("WriteFile(occupied log path) error = %v", err)
	}
	harness.bot.cfg.LogDir = occupiedLogPath

	_, err := harness.bot.intakeReviewWorkerArtifact(
		context.Background(),
		harness.reviewer.ID,
		harness.ownership.OwnerID,
		path,
		ReviewArtifactPhaseDiscovery,
	)
	assertReviewArtifactError(
		t,
		err,
		ReviewArtifactFailureTransient,
		ReviewArtifactFailureIO,
	)
	stored, ok := harness.agents.Get(harness.reviewer.ID)
	if !ok || stored.ReviewCycle == nil {
		t.Fatal("review coordinator disappeared after checkpoint failure")
	}
	if len(stored.ReviewCycle.ArtifactReceipts) != 0 ||
		len(stored.ReviewCycle.LaneCompletions) != 0 {
		t.Fatalf(
			"failed checkpoint retained acceptance: receipts=%d completions=%d",
			len(stored.ReviewCycle.ArtifactReceipts),
			len(stored.ReviewCycle.LaneCompletions),
		)
	}
}

func TestReviewArtifactPartialIntakeIsTransientAndIgnored(t *testing.T) {
	harness := newReviewArtifactIntakeHarness(t)
	partialPath := filepath.Join(
		harness.store.directory,
		reviewArtifactTemporaryPrefix+"still-writing",
	)
	if err := os.WriteFile(partialPath, []byte(`{"partial":true}`), 0o600); err != nil {
		t.Fatalf("WriteFile(partial) error = %v", err)
	}
	_, _, err := harness.store.read(
		context.Background(),
		partialPath,
	)
	assertReviewArtifactError(
		t,
		err,
		ReviewArtifactFailureTransient,
		ReviewArtifactFailurePartial,
	)
	published, err := harness.store.published()
	if err != nil {
		t.Fatalf("Published() error = %v", err)
	}
	if len(published) != 0 {
		t.Fatalf("Published() = %v, want no partial artifacts", published)
	}
}

func TestLoadConfigReviewArtifactLimits(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		setRequiredEnv(t)
		cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(""))
		cfg, err := loadConfig(cfgPath)
		if err != nil {
			t.Fatalf("loadConfig() error = %v", err)
		}
		if got, want := cfg.ReviewPolicy.Artifacts,
			defaultReviewArtifactPolicy(); got != want {
			t.Fatalf("ReviewPolicy.Artifacts = %#v, want %#v", got, want)
		}
	})

	t.Run("configured", func(t *testing.T) {
		setRequiredEnv(t)
		policy := validReviewPolicyConfig() + `  ARTIFACTS:
    MAX_BYTES: 4096
    TIMEOUT_SECONDS: 7
    RETRIES: 2
`
		cfgPath := writeRepoConfig(t, repoConfigWithReviewPolicy(policy))
		cfg, err := loadConfig(cfgPath)
		if err != nil {
			t.Fatalf("loadConfig() error = %v", err)
		}
		want := ReviewArtifactPolicy{
			MaxBytes:       4096,
			TimeoutSeconds: 7,
			Retries:        2,
		}
		if cfg.ReviewPolicy.Artifacts != want {
			t.Fatalf(
				"ReviewPolicy.Artifacts = %#v, want %#v",
				cfg.ReviewPolicy.Artifacts,
				want,
			)
		}
	})

	tests := []struct {
		name      string
		settings  string
		wantError string
	}{
		{
			name:      "zero size",
			settings:  "  ARTIFACTS:\n    MAX_BYTES: 0\n",
			wantError: "MAX_BYTES must be greater than zero",
		},
		{
			name:      "zero timeout",
			settings:  "  ARTIFACTS:\n    TIMEOUT_SECONDS: 0\n",
			wantError: "TIMEOUT_SECONDS must be greater than zero",
		},
		{
			name:      "negative retries",
			settings:  "  ARTIFACTS:\n    RETRIES: -1\n",
			wantError: "RETRIES must not be negative",
		},
		{
			name:      "unknown field",
			settings:  "  ARTIFACTS:\n    FORMAT: yaml\n",
			wantError: "failed to decode config YAML",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setRequiredEnv(t)
			cfgPath := writeRepoConfig(
				t,
				repoConfigWithReviewPolicy(
					validReviewPolicyConfig()+test.settings,
				),
			)
			_, err := loadConfig(cfgPath)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf(
					"loadConfig() error = %v, want containing %q",
					err,
					test.wantError,
				)
			}
		})
	}
}

func testReviewArtifactOwnership(
	t *testing.T,
	role AgentProfileRole,
	lane string,
	attempt int,
	worktreeDir string,
) ReviewWorkerOwnership {
	t.Helper()
	return testReviewArtifactOwnershipForIdentity(
		t,
		ReviewWorkerIdentity{
			CycleID:  "review-cycle-abcdef123456-0123456789abcdef0123456789abcdef",
			Revision: 1,
			Role:     role,
			Pass:     1,
			Lane:     lane,
		},
		attempt,
		worktreeDir,
		"0123456789abcdef0123456789abcdef",
	)
}

func testReviewArtifactOwnershipForIdentity(
	t *testing.T,
	identity ReviewWorkerIdentity,
	attempt int,
	worktreeDir string,
	token string,
) ReviewWorkerOwnership {
	t.Helper()
	ownership, err := allocateReviewWorkerOwnership(
		identity,
		attempt,
		worktreeDir,
		token,
		time.Unix(1_800_000_000+int64(attempt), 0).UTC(),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership() error = %v", err)
	}
	return ownership
}

func testDiscoveryArtifactEnvelope(
	t *testing.T,
	ownership ReviewWorkerOwnership,
	summary string,
) ReviewArtifactEnvelope {
	t.Helper()
	envelope, err := newReviewArtifactEnvelope(
		testReviewHeadSHA,
		ownership,
		ReviewArtifactPhaseDiscovery,
		ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadDiscovery,
			Discovery: &ReviewDiscoveryPayload{
				Summary:         summary,
				Candidates:      []ReviewFindingCandidate{},
				Coverage:        []ReviewCoverageClaim{},
				UnreviewedAreas: []string{},
			},
		},
	)
	if err != nil {
		t.Fatalf("newReviewArtifactEnvelope() error = %v", err)
	}
	return envelope
}

func newReviewArtifactPublicationTest(
	t *testing.T,
	policy ReviewArtifactPolicy,
) (ReviewWorkerOwnership, ReviewArtifactEnvelope, *reviewArtifactStore) {
	t.Helper()
	ownership := testReviewArtifactOwnership(
		t,
		AgentProfileRoleDiscovery,
		"contract",
		1,
		t.TempDir(),
	)
	envelope := testDiscoveryArtifactEnvelope(t, ownership, "safe evidence")
	directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	store, err := newReviewArtifactStore(directory, policy)
	if err != nil {
		t.Fatalf("newReviewArtifactStore() error = %v", err)
	}
	return ownership, envelope, store
}

type reviewArtifactIntakeHarness struct {
	bot       *Orchestrator
	agents    *AgentManager
	reviewer  Agent
	ownership ReviewWorkerOwnership
	store     *reviewArtifactStore
}

func newReviewArtifactIntakeHarness(
	t *testing.T,
) *reviewArtifactIntakeHarness {
	t.Helper()
	seedRepo := filepath.Join(t.TempDir(), "seed")
	if err := os.Mkdir(seedRepo, 0o755); err != nil {
		t.Fatalf("Mkdir(seed repo) error = %v", err)
	}
	runReviewArtifactGit(t, seedRepo, "init", "--initial-branch=main")
	runReviewArtifactGit(t, seedRepo, "config", "user.email", "test@example.com")
	runReviewArtifactGit(t, seedRepo, "config", "user.name", "Artifact Test")
	if err := os.WriteFile(
		filepath.Join(seedRepo, "tracked.txt"),
		[]byte("clean\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(seed tracked file) error = %v", err)
	}
	runReviewArtifactGit(t, seedRepo, "add", "tracked.txt")
	runReviewArtifactGit(t, seedRepo, "commit", "-m", "test: seed artifact repo")
	headSHA := strings.TrimSpace(
		runReviewArtifactGit(t, seedRepo, "rev-parse", "HEAD"),
	)

	policy := builtInReviewPolicy()
	cycle, err := newReviewCycleState(headSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	identity, err := reviewWorkerIdentityForCycle(
		cycle,
		AgentProfileRoleDiscovery,
		1,
		"contract",
	)
	if err != nil {
		t.Fatalf("reviewWorkerIdentityForCycle() error = %v", err)
	}
	runtimeProfile, err := cycle.Policy.effectiveProfileForRole(
		AgentProfileRoleChallenge,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := Agent{
		ID:                "review-agent-artifact-intake",
		Role:              RoleReviewer,
		IssueNumber:       34,
		IssueTitle:        "Validate review artifacts",
		PRNumber:          50,
		PRTitle:           "Validate review artifacts",
		PRURL:             "https://example.test/pull/50",
		ObservedPRHeadSHA: headSHA,
		RuntimeProfile:    runtimeProfile,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "review-agent-artifact-intake",
		},
		State:            StateWorking,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(&reviewer); err != nil {
		t.Fatalf("AgentManager.Add() error = %v", err)
	}
	worktreeDir := filepath.Join(t.TempDir(), "workers")
	if err := os.Mkdir(worktreeDir, 0o755); err != nil {
		t.Fatalf("Mkdir(worker root) error = %v", err)
	}
	ownership, _, err := agents.allocateReviewWorkerOwnership(
		reviewer.ID,
		identity,
		worktreeDir,
		"0123456789abcdef0123456789abcdef",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership() error = %v", err)
	}
	runReviewArtifactGit(
		t,
		worktreeDir,
		"clone",
		"--quiet",
		seedRepo,
		ownership.WorktreePath,
	)
	directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory() error = %v", err)
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("Mkdir(artifact directory) error = %v", err)
	}
	store, err := newReviewArtifactStore(directory, cycle.Policy.Artifacts)
	if err != nil {
		t.Fatalf("newReviewArtifactStore() error = %v", err)
	}
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     seedRepo,
			LogDir:       t.TempDir(),
			WorktreeDir:  worktreeDir,
			ReviewPolicy: cycle.Policy,
		},
		agents: agents,
	}
	storedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("review coordinator %q disappeared", reviewer.ID)
	}
	return &reviewArtifactIntakeHarness{
		bot:       bot,
		agents:    agents,
		reviewer:  storedReviewer,
		ownership: ownership,
		store:     store,
	}
}

func (harness *reviewArtifactIntakeHarness) publish(
	t *testing.T,
	envelope ReviewArtifactEnvelope,
) string {
	t.Helper()
	envelope.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
	envelope.Checkout.ExactSHA = harness.reviewer.ReviewCycle.HeadSHA
	path, err := harness.store.publish(
		context.Background(),
		harness.reviewer.ReviewCycle.HeadSHA,
		harness.ownership,
		envelope,
	)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	return path
}

func (harness *reviewArtifactIntakeHarness) writeRawArtifact(
	t *testing.T,
	body []byte,
) string {
	t.Helper()
	path := filepath.Join(
		harness.store.directory,
		reviewArtifactFilename(harness.ownership),
	)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("WriteFile(raw artifact) error = %v", err)
	}
	return path
}

func (harness *reviewArtifactIntakeHarness) allocateRetry(
	t *testing.T,
) (ReviewWorkerOwnership, *reviewArtifactStore) {
	t.Helper()
	ownership, _, err := harness.agents.allocateReviewWorkerOwnership(
		harness.reviewer.ID,
		harness.ownership.Identity,
		harness.bot.cfg.WorktreeDir,
		"fedcba9876543210fedcba9876543210",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("allocateReviewWorkerOwnership(retry) error = %v", err)
	}
	runReviewArtifactGit(
		t,
		harness.bot.cfg.WorktreeDir,
		"clone",
		"--quiet",
		harness.bot.cfg.RepoPath,
		ownership.WorktreePath,
	)
	directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
	if err != nil {
		t.Fatalf("reviewWorkerArtifactDirectory(retry) error = %v", err)
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("Mkdir(retry artifact directory) error = %v", err)
	}
	store, err := newReviewArtifactStore(
		directory,
		harness.reviewer.ReviewCycle.Policy.Artifacts,
	)
	if err != nil {
		t.Fatalf("newReviewArtifactStore(retry) error = %v", err)
	}
	return ownership, store
}

func runReviewArtifactGit(
	t *testing.T,
	directory string,
	args ...string,
) string {
	t.Helper()
	commandArgs := append([]string{"-C", directory}, args...)
	output, err := exec.Command("git", commandArgs...).CombinedOutput()
	if err != nil {
		t.Fatalf(
			"git %s error = %v, output = %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(string(output)),
		)
	}
	return string(output)
}

func assertReviewArtifactError(
	t *testing.T,
	err error,
	wantClass ReviewArtifactFailureClass,
	wantCode ReviewArtifactFailureCode,
) {
	t.Helper()
	artifactErr := asReviewArtifactError(err)
	if artifactErr == nil {
		t.Fatalf("error = %v, want ReviewArtifactError", err)
	}
	if artifactErr.Class != wantClass || artifactErr.Code != wantCode {
		t.Fatalf(
			"artifact error = (%s, %s), want (%s, %s)",
			artifactErr.Class,
			artifactErr.Code,
			wantClass,
			wantCode,
		)
	}
}
