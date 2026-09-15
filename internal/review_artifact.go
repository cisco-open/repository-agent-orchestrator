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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	defaultReviewArtifactMaxBytes       = 1 << 20
	defaultReviewArtifactTimeoutSeconds = 30
	defaultReviewArtifactRetries        = 1
	maxReviewArtifactErrorDetailRunes   = 512
	reviewWorkerArtifactDirPrefix       = "review-worker-artifacts"
	reviewArtifactTemporaryPrefix       = ".review-artifact-partial-"
	reviewArtifactFileExtension         = ".json"
)

type reviewArtifactConfigFile struct {
	MaxBytes       *int `yaml:"MAX_BYTES"`
	TimeoutSeconds *int `yaml:"TIMEOUT_SECONDS"`
	Retries        *int `yaml:"RETRIES"`
}

type ReviewArtifactPolicy struct {
	MaxBytes       int `json:"max_bytes" yaml:"MAX_BYTES"`
	TimeoutSeconds int `json:"timeout_seconds" yaml:"TIMEOUT_SECONDS"`
	Retries        int `json:"retries" yaml:"RETRIES"`
}

func defaultReviewArtifactPolicy() ReviewArtifactPolicy {
	return ReviewArtifactPolicy{
		MaxBytes:       defaultReviewArtifactMaxBytes,
		TimeoutSeconds: defaultReviewArtifactTimeoutSeconds,
		Retries:        defaultReviewArtifactRetries,
	}
}

func normalizeReviewArtifactPolicy(
	raw *reviewArtifactConfigFile,
	defaults ReviewArtifactPolicy,
) ReviewArtifactPolicy {
	policy := defaults
	if raw == nil {
		return policy
	}
	if raw.MaxBytes != nil {
		policy.MaxBytes = *raw.MaxBytes
	}
	if raw.TimeoutSeconds != nil {
		policy.TimeoutSeconds = *raw.TimeoutSeconds
	}
	if raw.Retries != nil {
		policy.Retries = *raw.Retries
	}
	return policy
}

func validateReviewArtifactPolicy(policy ReviewArtifactPolicy) error {
	if policy.MaxBytes <= 0 {
		return errors.New("REVIEW_POLICY.ARTIFACTS.MAX_BYTES must be greater than zero")
	}
	if policy.TimeoutSeconds <= 0 {
		return errors.New("REVIEW_POLICY.ARTIFACTS.TIMEOUT_SECONDS must be greater than zero")
	}
	if policy.Retries < 0 {
		return errors.New("REVIEW_POLICY.ARTIFACTS.RETRIES must not be negative")
	}
	return nil
}

type ReviewArtifactPhase string

const (
	ReviewArtifactPhaseDiscovery    ReviewArtifactPhase = "discovery"
	ReviewArtifactPhaseVerification ReviewArtifactPhase = "verification"
	ReviewArtifactPhaseChallenge    ReviewArtifactPhase = "challenge"
)

type ReviewArtifactPayloadKind string

const (
	ReviewArtifactPayloadDiscovery    ReviewArtifactPayloadKind = "discovery"
	ReviewArtifactPayloadVerification ReviewArtifactPayloadKind = "verification"
	ReviewArtifactPayloadChallenge    ReviewArtifactPayloadKind = "challenge"
)

type ReviewArtifactCheckoutAttestation struct {
	ExactSHA string `json:"exact_sha"`
	Clean    bool   `json:"clean"`
}

type ReviewDiscoveryPayload struct {
	Summary         string                   `json:"summary"`
	Candidates      []ReviewFindingCandidate `json:"candidates"`
	Coverage        []ReviewCoverageClaim    `json:"coverage"`
	UnreviewedAreas []string                 `json:"unreviewed_areas"`
}

type ReviewVerificationOutcome string

const (
	ReviewVerificationConfirmed    ReviewVerificationOutcome = "confirmed"
	ReviewVerificationRejected     ReviewVerificationOutcome = "rejected"
	ReviewVerificationInconclusive ReviewVerificationOutcome = "inconclusive"
)

type ReviewScopeDisposition string

const (
	ReviewScopeInScope    ReviewScopeDisposition = "in_scope"
	ReviewScopeOutOfScope ReviewScopeDisposition = "out_of_scope"
	ReviewScopeUnclear    ReviewScopeDisposition = "unclear"
)

type ReviewPatchDisposition string

const (
	ReviewPatchIntroduced    ReviewPatchDisposition = "introduced"
	ReviewPatchWorsened      ReviewPatchDisposition = "worsened"
	ReviewPatchPreExisting   ReviewPatchDisposition = "pre_existing"
	ReviewPatchUnrelated     ReviewPatchDisposition = "unrelated"
	ReviewPatchNotReproduced ReviewPatchDisposition = "not_reproduced"
	ReviewPatchUnclear       ReviewPatchDisposition = "unclear"
)

type ReviewVerificationPayload struct {
	FindingID              string                    `json:"finding_id"`
	Outcome                ReviewVerificationOutcome `json:"outcome"`
	Summary                string                    `json:"summary"`
	ScopeDisposition       ReviewScopeDisposition    `json:"scope_disposition"`
	PatchDisposition       ReviewPatchDisposition    `json:"patch_disposition"`
	Location               *ReviewFindingLocation    `json:"location,omitempty"`
	BehavioralPath         string                    `json:"behavioral_path,omitempty"`
	Evidence               []ReviewEvidence          `json:"evidence"`
	CausalEvidence         []ReviewEvidence          `json:"causal_evidence"`
	TestEvidence           []ReviewEvidence          `json:"test_evidence"`
	TestNotPracticalReason string                    `json:"test_not_practical_reason,omitempty"`
}

type ReviewChallengeOutcome string

const (
	ReviewChallengeUpheld       ReviewChallengeOutcome = "upheld"
	ReviewChallengeOverturned   ReviewChallengeOutcome = "overturned"
	ReviewChallengeInconclusive ReviewChallengeOutcome = "inconclusive"
)

type ReviewChallengePayload struct {
	Outcome      ReviewChallengeOutcome    `json:"outcome"`
	Summary      string                    `json:"summary"`
	AssignmentID string                    `json:"assignment_id"`
	TargetKind   ReviewChallengeTargetKind `json:"target_kind"`
	TargetID     string                    `json:"target_id"`
	Candidates   []ReviewFindingCandidate  `json:"candidates"`
	Coverage     []ReviewCoverageClaim     `json:"coverage"`
}

// ReviewArtifactPayload is a strict tagged union. Exactly one payload matching
// Kind must be present.
type ReviewArtifactPayload struct {
	Kind         ReviewArtifactPayloadKind
	Discovery    *ReviewDiscoveryPayload
	Verification *ReviewVerificationPayload
	Challenge    *ReviewChallengePayload
}

func (payload ReviewArtifactPayload) MarshalJSON() ([]byte, error) {
	if err := validateReviewArtifactPayload(payload); err != nil {
		return nil, err
	}
	switch payload.Kind {
	case ReviewArtifactPayloadDiscovery:
		return json.Marshal(struct {
			Kind            ReviewArtifactPayloadKind `json:"kind"`
			Summary         string                    `json:"summary"`
			Candidates      []ReviewFindingCandidate  `json:"candidates"`
			Coverage        []ReviewCoverageClaim     `json:"coverage"`
			UnreviewedAreas []string                  `json:"unreviewed_areas"`
		}{
			Kind:            payload.Kind,
			Summary:         payload.Discovery.Summary,
			Candidates:      payload.Discovery.Candidates,
			Coverage:        payload.Discovery.Coverage,
			UnreviewedAreas: payload.Discovery.UnreviewedAreas,
		})
	case ReviewArtifactPayloadVerification:
		return json.Marshal(struct {
			Kind ReviewArtifactPayloadKind `json:"kind"`
			*ReviewVerificationPayload
		}{
			Kind:                      payload.Kind,
			ReviewVerificationPayload: payload.Verification,
		})
	case ReviewArtifactPayloadChallenge:
		return json.Marshal(struct {
			Kind ReviewArtifactPayloadKind `json:"kind"`
			*ReviewChallengePayload
		}{
			Kind:                   payload.Kind,
			ReviewChallengePayload: payload.Challenge,
		})
	default:
		return nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureMalformed,
		)
	}
}

func (payload *ReviewArtifactPayload) UnmarshalJSON(body []byte) error {
	if payload == nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureMalformed,
		)
	}
	decoded, err := unmarshalReviewArtifactPayload(body)
	if err != nil {
		return err
	}
	*payload = decoded
	return nil
}

func unmarshalReviewArtifactPayload(
	body []byte,
) (ReviewArtifactPayload, error) {
	var header struct {
		Kind ReviewArtifactPayloadKind `json:"kind"`
	}
	if err := json.Unmarshal(body, &header); err != nil {
		return ReviewArtifactPayload{}, malformedReviewArtifactError(err.Error())
	}
	var decoded ReviewArtifactPayload
	switch header.Kind {
	case ReviewArtifactPayloadDiscovery:
		discovery, err := unmarshalReviewDiscoveryPayload(body)
		if err != nil {
			return ReviewArtifactPayload{}, err
		}
		decoded = ReviewArtifactPayload{
			Kind:      header.Kind,
			Discovery: discovery,
		}
	case ReviewArtifactPayloadVerification:
		var wire struct {
			Kind ReviewArtifactPayloadKind `json:"kind"`
			ReviewVerificationPayload
		}
		if err := decodeStrictReviewArtifactJSON(body, &wire); err != nil {
			return ReviewArtifactPayload{}, err
		}
		decoded = ReviewArtifactPayload{
			Kind:         wire.Kind,
			Verification: &wire.ReviewVerificationPayload,
		}
	case ReviewArtifactPayloadChallenge:
		var wire struct {
			Kind ReviewArtifactPayloadKind `json:"kind"`
			ReviewChallengePayload
		}
		if err := decodeStrictReviewArtifactJSON(body, &wire); err != nil {
			return ReviewArtifactPayload{}, err
		}
		decoded = ReviewArtifactPayload{
			Kind:      wire.Kind,
			Challenge: &wire.ReviewChallengePayload,
		}
	default:
		return ReviewArtifactPayload{}, malformedReviewArtifactError()
	}
	if err := validateReviewArtifactPayload(decoded); err != nil {
		return ReviewArtifactPayload{}, err
	}
	return decoded, nil
}

func unmarshalReviewDiscoveryPayload(
	body []byte,
) (*ReviewDiscoveryPayload, error) {
	var wire struct {
		Kind            ReviewArtifactPayloadKind `json:"kind"`
		Summary         string                    `json:"summary"`
		Candidates      *[]ReviewFindingCandidate `json:"candidates"`
		Coverage        *[]ReviewCoverageClaim    `json:"coverage"`
		UnreviewedAreas *[]string                 `json:"unreviewed_areas"`
	}
	if err := decodeStrictReviewArtifactJSON(body, &wire); err != nil {
		return nil, err
	}
	if wire.Candidates == nil || wire.Coverage == nil ||
		wire.UnreviewedAreas == nil {
		switch {
		case wire.Candidates == nil:
			return nil, malformedReviewArtifactError(
				"payload.candidates is required",
			)
		case wire.Coverage == nil:
			return nil, malformedReviewArtifactError(
				"payload.coverage is required",
			)
		default:
			return nil, malformedReviewArtifactError(
				"payload.unreviewed_areas is required",
			)
		}
	}
	return &ReviewDiscoveryPayload{
		Summary:         wire.Summary,
		Candidates:      *wire.Candidates,
		Coverage:        *wire.Coverage,
		UnreviewedAreas: *wire.UnreviewedAreas,
	}, nil
}

type ReviewArtifactEnvelope struct {
	ExactSHA      string                            `json:"exact_sha"`
	CycleID       string                            `json:"cycle_id"`
	CycleRevision int                               `json:"cycle_revision"`
	WorkerID      string                            `json:"worker_id"`
	Role          AgentProfileRole                  `json:"role"`
	Lane          string                            `json:"lane"`
	Phase         ReviewArtifactPhase               `json:"phase"`
	Pass          int                               `json:"pass"`
	Attempt       int                               `json:"attempt"`
	Checkout      ReviewArtifactCheckoutAttestation `json:"checkout"`
	Payload       ReviewArtifactPayload             `json:"payload"`
}

type reviewArtifactEnvelopeWire struct {
	ExactSHA      string                            `json:"exact_sha"`
	CycleID       string                            `json:"cycle_id"`
	CycleRevision int                               `json:"cycle_revision"`
	WorkerID      string                            `json:"worker_id"`
	Role          AgentProfileRole                  `json:"role"`
	Lane          string                            `json:"lane"`
	Phase         ReviewArtifactPhase               `json:"phase"`
	Pass          int                               `json:"pass"`
	Attempt       int                               `json:"attempt"`
	Checkout      ReviewArtifactCheckoutAttestation `json:"checkout"`
	Payload       json.RawMessage                   `json:"payload"`
}

func (envelope ReviewArtifactEnvelope) MarshalJSON() ([]byte, error) {
	if err := validateReviewArtifactEnvelope(envelope); err != nil {
		return nil, err
	}
	type wire ReviewArtifactEnvelope
	return json.Marshal(wire(envelope))
}

func (envelope *ReviewArtifactEnvelope) UnmarshalJSON(body []byte) error {
	if envelope == nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureMalformed,
		)
	}
	var decoded reviewArtifactEnvelopeWire
	if err := decodeStrictReviewArtifactJSON(body, &decoded); err != nil {
		return err
	}
	payload, err := unmarshalReviewArtifactPayload(decoded.Payload)
	if err != nil {
		return err
	}
	candidate := ReviewArtifactEnvelope{
		ExactSHA:      decoded.ExactSHA,
		CycleID:       decoded.CycleID,
		CycleRevision: decoded.CycleRevision,
		WorkerID:      decoded.WorkerID,
		Role:          decoded.Role,
		Lane:          decoded.Lane,
		Phase:         decoded.Phase,
		Pass:          decoded.Pass,
		Attempt:       decoded.Attempt,
		Checkout:      decoded.Checkout,
		Payload:       payload,
	}
	if err := validateReviewArtifactEnvelope(candidate); err != nil {
		return err
	}
	*envelope = candidate
	return nil
}

func newReviewArtifactEnvelope(
	exactSHA string,
	ownership ReviewWorkerOwnership,
	phase ReviewArtifactPhase,
	payload ReviewArtifactPayload,
) (ReviewArtifactEnvelope, error) {
	envelope := ReviewArtifactEnvelope{
		ExactSHA:      strings.ToLower(strings.TrimSpace(exactSHA)),
		CycleID:       ownership.Identity.CycleID,
		CycleRevision: ownership.Identity.Revision,
		WorkerID:      ownership.OwnerID,
		Role:          ownership.Identity.Role,
		Lane:          ownership.Identity.Lane,
		Phase:         phase,
		Pass:          ownership.Identity.Pass,
		Attempt:       ownership.Attempt,
		Checkout: ReviewArtifactCheckoutAttestation{
			ExactSHA: strings.ToLower(strings.TrimSpace(exactSHA)),
			Clean:    true,
		},
		Payload: payload,
	}
	if err := validateReviewArtifactEnvelope(envelope); err != nil {
		return ReviewArtifactEnvelope{}, err
	}
	if err := validateReviewArtifactAgainstOwnership(envelope, exactSHA, ownership); err != nil {
		return ReviewArtifactEnvelope{}, err
	}
	return envelope, nil
}

func marshalReviewArtifactEnvelope(envelope ReviewArtifactEnvelope) ([]byte, error) {
	body, err := json.Marshal(envelope)
	if err != nil {
		if artifactErr := asReviewArtifactError(err); artifactErr != nil {
			return nil, artifactErr
		}
		return nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureMalformed,
		)
	}
	return body, nil
}

func unmarshalReviewArtifactEnvelope(body []byte) (ReviewArtifactEnvelope, error) {
	var envelope ReviewArtifactEnvelope
	if err := decodeStrictReviewArtifactJSON(body, &envelope); err != nil {
		return ReviewArtifactEnvelope{}, err
	}
	return envelope, nil
}

func decodeStrictReviewArtifactJSON(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if artifactErr := asReviewArtifactError(err); artifactErr != nil {
			return artifactErr
		}
		return malformedReviewArtifactError(err.Error())
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return malformedReviewArtifactError(
			"artifact contains more than one JSON value",
		)
	}
	return nil
}

func validateReviewArtifactEnvelope(envelope ReviewArtifactEnvelope) error {
	if validateCanonicalGitObjectID(envelope.ExactSHA) != nil {
		return malformedReviewArtifactError(
			"exact_sha is missing or invalid",
		)
	}
	identity := ReviewWorkerIdentity{
		CycleID:  envelope.CycleID,
		Revision: envelope.CycleRevision,
		Role:     envelope.Role,
		Pass:     envelope.Pass,
		Lane:     envelope.Lane,
	}
	if validateReviewWorkerIdentity(identity) != nil ||
		strings.TrimSpace(envelope.WorkerID) == "" ||
		sanitizeSessionPart(envelope.WorkerID) != envelope.WorkerID ||
		envelope.Attempt <= 0 {
		return malformedReviewArtifactError(
			"cycle, worker, role, lane, pass, or attempt identity is missing or invalid",
		)
	}
	if !envelope.Checkout.Clean {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureDirtyCheckout,
		)
	}
	if envelope.Checkout.ExactSHA != envelope.ExactSHA {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureStaleSHA,
		)
	}
	if err := validateReviewArtifactPayload(envelope.Payload); err != nil {
		return err
	}
	if ReviewArtifactPayloadKind(envelope.Phase) != envelope.Payload.Kind {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnexpectedPhase,
		)
	}
	switch envelope.Phase {
	case ReviewArtifactPhaseDiscovery:
		if envelope.Role != AgentProfileRoleDiscovery {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureUnexpectedPhase,
			)
		}
	case ReviewArtifactPhaseVerification:
		if envelope.Role != AgentProfileRoleVerifier {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureUnexpectedPhase,
			)
		}
	case ReviewArtifactPhaseChallenge:
		if envelope.Role != AgentProfileRoleChallenge &&
			envelope.Role != AgentProfileRoleEscalation {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureUnexpectedPhase,
			)
		}
	default:
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnexpectedPhase,
		)
	}
	return nil
}

func validateReviewArtifactPayload(payload ReviewArtifactPayload) error {
	present := 0
	for _, configured := range []bool{
		payload.Discovery != nil,
		payload.Verification != nil,
		payload.Challenge != nil,
	} {
		if configured {
			present++
		}
	}
	if present != 1 {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureMalformed,
		)
	}
	switch payload.Kind {
	case ReviewArtifactPayloadDiscovery:
		if payload.Discovery == nil {
			return malformedReviewArtifactError("payload.discovery is required")
		}
		if strings.TrimSpace(payload.Discovery.Summary) == "" {
			return malformedReviewArtifactError("payload.summary is required")
		}
		if payload.Discovery.Candidates == nil {
			return malformedReviewArtifactError("payload.candidates is required")
		}
		if payload.Discovery.Coverage == nil {
			return malformedReviewArtifactError("payload.coverage is required")
		}
		if payload.Discovery.UnreviewedAreas == nil {
			return malformedReviewArtifactError(
				"payload.unreviewed_areas is required",
			)
		}
		for index, area := range payload.Discovery.UnreviewedAreas {
			if strings.TrimSpace(area) != area || area == "" {
				return malformedReviewArtifactError(fmt.Sprintf(
					"payload.unreviewed_areas[%d] is empty or has surrounding whitespace",
					index,
				))
			}
		}
		if err := validateReviewFindingCandidates(
			payload.Discovery.Candidates,
		); err != nil {
			return malformedReviewArtifactError(err.Error())
		}
		if err := validateReviewCoverageClaims(
			payload.Discovery.Coverage,
			nil,
		); err != nil {
			return malformedReviewArtifactError(err.Error())
		}
	case ReviewArtifactPayloadVerification:
		if payload.Verification == nil {
			return malformedReviewArtifactError("payload.verification is required")
		}
		if strings.TrimSpace(payload.Verification.FindingID) == "" {
			return malformedReviewArtifactError("payload.finding_id is required")
		}
		if strings.TrimSpace(payload.Verification.Summary) == "" {
			return malformedReviewArtifactError("payload.summary is required")
		}
		if payload.Verification.Evidence == nil {
			return malformedReviewArtifactError("payload.evidence is required")
		}
		if payload.Verification.CausalEvidence == nil {
			return malformedReviewArtifactError(
				"payload.causal_evidence is required",
			)
		}
		if payload.Verification.TestEvidence == nil {
			return malformedReviewArtifactError(
				"payload.test_evidence is required",
			)
		}
		switch payload.Verification.Outcome {
		case ReviewVerificationConfirmed,
			ReviewVerificationRejected,
			ReviewVerificationInconclusive:
		default:
			return malformedReviewArtifactError("payload.outcome is unsupported")
		}
		if !supportedReviewScopeDisposition(
			payload.Verification.ScopeDisposition,
		) {
			return malformedReviewArtifactError(
				"payload.scope_disposition is unsupported",
			)
		}
		if !supportedReviewPatchDisposition(
			payload.Verification.PatchDisposition,
		) {
			return malformedReviewArtifactError(
				"payload.patch_disposition is unsupported",
			)
		}
		if payload.Verification.Location != nil {
			if err := validateReviewFindingLocation(
				*payload.Verification.Location,
			); err != nil {
				return malformedReviewArtifactError(err.Error())
			}
		}
		for _, evidence := range append(
			append(
				append(
					[]ReviewEvidence(nil),
					payload.Verification.Evidence...,
				),
				payload.Verification.CausalEvidence...,
			),
			payload.Verification.TestEvidence...,
		) {
			if err := validateReviewEvidence(evidence); err != nil {
				return malformedReviewArtifactError(err.Error())
			}
		}
	case ReviewArtifactPayloadChallenge:
		if payload.Challenge == nil {
			return malformedReviewArtifactError("payload.challenge is required")
		}
		if strings.TrimSpace(payload.Challenge.Summary) == "" {
			return malformedReviewArtifactError("payload.summary is required")
		}
		if strings.TrimSpace(payload.Challenge.AssignmentID) !=
			payload.Challenge.AssignmentID ||
			payload.Challenge.AssignmentID == "" {
			return malformedReviewArtifactError(
				"payload.assignment_id is missing or invalid",
			)
		}
		if strings.TrimSpace(payload.Challenge.TargetID) !=
			payload.Challenge.TargetID ||
			payload.Challenge.TargetID == "" {
			return malformedReviewArtifactError(
				"payload.target_id is missing or invalid",
			)
		}
		if !supportedReviewChallengeTargetKind(payload.Challenge.TargetKind) {
			return malformedReviewArtifactError(
				"payload.target_kind is unsupported",
			)
		}
		if payload.Challenge.Candidates == nil {
			return malformedReviewArtifactError("payload.candidates is required")
		}
		if payload.Challenge.Coverage == nil {
			return malformedReviewArtifactError("payload.coverage is required")
		}
		switch payload.Challenge.Outcome {
		case ReviewChallengeUpheld,
			ReviewChallengeOverturned,
			ReviewChallengeInconclusive:
		default:
			return malformedReviewArtifactError("payload.outcome is unsupported")
		}
		if err := validateReviewFindingCandidates(
			payload.Challenge.Candidates,
		); err != nil {
			return malformedReviewArtifactError(err.Error())
		}
		if err := validateReviewCoverageClaims(
			payload.Challenge.Coverage,
			nil,
		); err != nil {
			return malformedReviewArtifactError(err.Error())
		}
	default:
		return malformedReviewArtifactError()
	}
	return nil
}

func supportedReviewScopeDisposition(value ReviewScopeDisposition) bool {
	switch value {
	case ReviewScopeInScope, ReviewScopeOutOfScope, ReviewScopeUnclear:
		return true
	default:
		return false
	}
}

func supportedReviewPatchDisposition(value ReviewPatchDisposition) bool {
	switch value {
	case ReviewPatchIntroduced,
		ReviewPatchWorsened,
		ReviewPatchPreExisting,
		ReviewPatchUnrelated,
		ReviewPatchNotReproduced,
		ReviewPatchUnclear:
		return true
	default:
		return false
	}
}

func malformedReviewArtifactError(details ...string) error {
	detail := "artifact does not match the required review schema"
	if len(details) != 0 && strings.TrimSpace(details[0]) != "" {
		detail = strings.TrimSpace(details[0])
	}
	return newReviewArtifactErrorWithDetail(
		ReviewArtifactFailureTerminal,
		ReviewArtifactFailureMalformed,
		detail,
	)
}

func validateReviewArtifactAgainstOwnership(
	envelope ReviewArtifactEnvelope,
	exactSHA string,
	ownership ReviewWorkerOwnership,
) error {
	if validateReviewWorkerOwnership(ownership) != nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnregisteredWorker,
		)
	}
	exactSHA = strings.ToLower(strings.TrimSpace(exactSHA))
	if envelope.ExactSHA != exactSHA {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureStaleSHA,
		)
	}
	if envelope.WorkerID != ownership.OwnerID ||
		envelope.CycleID != ownership.Identity.CycleID ||
		envelope.CycleRevision != ownership.Identity.Revision ||
		envelope.Role != ownership.Identity.Role ||
		envelope.Lane != ownership.Identity.Lane ||
		envelope.Pass != ownership.Identity.Pass ||
		envelope.Attempt != ownership.Attempt {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnregisteredWorker,
		)
	}
	return nil
}

type ReviewArtifactFailureClass string

const (
	ReviewArtifactFailureTransient ReviewArtifactFailureClass = "transient"
	ReviewArtifactFailureTerminal  ReviewArtifactFailureClass = "terminal"
)

type ReviewArtifactFailureCode string

const (
	ReviewArtifactFailureMalformed          ReviewArtifactFailureCode = "malformed"
	ReviewArtifactFailureOversized          ReviewArtifactFailureCode = "oversized"
	ReviewArtifactFailureTimeout            ReviewArtifactFailureCode = "timeout"
	ReviewArtifactFailurePartial            ReviewArtifactFailureCode = "partial"
	ReviewArtifactFailureIO                 ReviewArtifactFailureCode = "io"
	ReviewArtifactFailureConflict           ReviewArtifactFailureCode = "conflict"
	ReviewArtifactFailureStaleSHA           ReviewArtifactFailureCode = "stale_sha"
	ReviewArtifactFailureUnregisteredWorker ReviewArtifactFailureCode = "unregistered_worker"
	ReviewArtifactFailureUnexpectedPhase    ReviewArtifactFailureCode = "unexpected_phase"
	ReviewArtifactFailureDirtyCheckout      ReviewArtifactFailureCode = "dirty_checkout"
	ReviewArtifactFailureInvalidPath        ReviewArtifactFailureCode = "invalid_path"
	ReviewArtifactFailureSecretMaterial     ReviewArtifactFailureCode = "secret_material"
)

type ReviewArtifactError struct {
	Class  ReviewArtifactFailureClass
	Code   ReviewArtifactFailureCode
	Detail string
}

func (err *ReviewArtifactError) Error() string {
	if err == nil {
		return ""
	}
	message := fmt.Sprintf(
		"review artifact rejected: class=%s code=%s",
		err.Class,
		err.Code,
	)
	if err.Detail != "" {
		message += ": " + err.Detail
	}
	return message
}

func newReviewArtifactError(
	class ReviewArtifactFailureClass,
	code ReviewArtifactFailureCode,
) *ReviewArtifactError {
	return &ReviewArtifactError{Class: class, Code: code}
}

func newReviewArtifactErrorWithDetail(
	class ReviewArtifactFailureClass,
	code ReviewArtifactFailureCode,
	detail string,
) *ReviewArtifactError {
	return &ReviewArtifactError{
		Class:  class,
		Code:   code,
		Detail: boundedReviewArtifactErrorDetail(detail),
	}
}

func boundedReviewArtifactErrorDetail(detail string) string {
	detail = strings.Map(func(value rune) rune {
		if value < ' ' || value == '\u007f' {
			return ' '
		}
		return value
	}, strings.TrimSpace(detail))
	values := []rune(detail)
	if len(values) > maxReviewArtifactErrorDetailRunes {
		values = values[:maxReviewArtifactErrorDetailRunes]
	}
	return strings.TrimSpace(string(values))
}

func asReviewArtifactError(err error) *ReviewArtifactError {
	var artifactErr *ReviewArtifactError
	if errors.As(err, &artifactErr) {
		return artifactErr
	}
	return nil
}

func reviewArtifactFailureAllowsFreshWorkerRetry(err error) bool {
	artifactErr := asReviewArtifactError(err)
	if artifactErr == nil {
		return false
	}
	if artifactErr.Class == ReviewArtifactFailureTransient {
		return true
	}
	return artifactErr.Code == ReviewArtifactFailureMalformed
}

type reviewArtifactWriteFunc func(context.Context, *os.File, []byte) error

type reviewArtifactStore struct {
	directory string
	policy    ReviewArtifactPolicy
	write     reviewArtifactWriteFunc
}

func newReviewArtifactStore(
	directory string,
	policy ReviewArtifactPolicy,
) (*reviewArtifactStore, error) {
	directory = filepath.Clean(strings.TrimSpace(directory))
	if !filepath.IsAbs(directory) || directory == string(filepath.Separator) {
		return nil, errors.New("review artifact directory is unsafe")
	}
	if err := validateReviewArtifactPolicy(policy); err != nil {
		return nil, err
	}
	return &reviewArtifactStore{
		directory: directory,
		policy:    policy,
		write:     writeReviewArtifactBody,
	}, nil
}

func (store *reviewArtifactStore) publish(
	ctx context.Context,
	exactSHA string,
	ownership ReviewWorkerOwnership,
	envelope ReviewArtifactEnvelope,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil || store.write == nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureIO,
		)
	}
	if err := validateReviewArtifactPolicy(store.policy); err != nil {
		return "", err
	}
	expectedDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil || filepath.Clean(store.directory) != expectedDirectory {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	if err := validateReviewArtifactAgainstOwnership(
		envelope,
		exactSHA,
		ownership,
	); err != nil {
		return "", err
	}
	body, err := marshalReviewArtifactEnvelope(envelope)
	if err != nil {
		return "", err
	}
	if reviewArtifactContainsProhibitedSecret(body) {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureSecretMaterial,
		)
	}
	if len(body) > store.policy.MaxBytes {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureOversized,
		)
	}

	var lastErr error
	for retry := 0; retry <= store.policy.Retries; retry++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureTimeout,
			)
		}
		attemptCtx, cancel := context.WithTimeout(
			ctx,
			time.Duration(store.policy.TimeoutSeconds)*time.Second,
		)
		path, publishErr := store.publishOnce(
			attemptCtx,
			ownership,
			body,
		)
		cancel()
		if publishErr == nil {
			return path, nil
		}
		lastErr = publishErr
		artifactErr := asReviewArtifactError(publishErr)
		if artifactErr == nil ||
			artifactErr.Class == ReviewArtifactFailureTerminal {
			return "", publishErr
		}
	}
	return "", lastErr
}

func (store *reviewArtifactStore) publishOnce(
	ctx context.Context,
	ownership ReviewWorkerOwnership,
	body []byte,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureTimeout,
		)
	}
	if err := ensureReviewArtifactDirectory(store.directory); err != nil {
		return "", err
	}
	finalPath := filepath.Join(
		store.directory,
		reviewArtifactFilename(ownership),
	)
	if _, err := os.Lstat(finalPath); err == nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureConflict,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}

	temporary, err := os.CreateTemp(
		store.directory,
		reviewArtifactTemporaryPrefix,
	)
	if err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	temporaryPath := temporary.Name()
	complete := false
	defer func() {
		_ = temporary.Close()
		if !complete {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if err := store.write(ctx, temporary, body); err != nil {
		if ctx.Err() != nil {
			return "", newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureTimeout,
			)
		}
		if artifactErr := asReviewArtifactError(err); artifactErr != nil {
			return "", artifactErr
		}
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if err := temporary.Sync(); err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if err := temporary.Close(); err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if err := ctx.Err(); err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureTimeout,
		)
	}
	if err := os.Link(temporaryPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureConflict,
			)
		}
		return "", newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureIO,
		)
	}
	complete = true
	if err := syncReviewArtifactDirectory(store.directory); err != nil {
		return "", newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureIO,
		)
	}
	return finalPath, nil
}

func writeReviewArtifactBody(
	ctx context.Context,
	target *os.File,
	body []byte,
) error {
	for len(body) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := body
		if len(chunk) > 32*1024 {
			chunk = chunk[:32*1024]
		}
		written, err := target.Write(chunk)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		body = body[written:]
	}
	return nil
}

func ensureReviewArtifactDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureIO,
			)
		}
		return nil
	}
	if err != nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	return nil
}

func syncReviewArtifactDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}

func reviewArtifactFilename(ownership ReviewWorkerOwnership) string {
	return ownership.OwnerID + reviewArtifactFileExtension
}

func (store *reviewArtifactStore) published() ([]string, error) {
	if store == nil {
		return nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureIO,
		)
	}
	entries, err := os.ReadDir(store.directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() ||
			strings.HasPrefix(entry.Name(), ".") ||
			!strings.HasSuffix(entry.Name(), reviewArtifactFileExtension) {
			continue
		}
		paths = append(paths, filepath.Join(store.directory, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

func (store *reviewArtifactStore) read(
	ctx context.Context,
	path string,
	prohibitedValues ...string,
) (ReviewArtifactEnvelope, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = filepath.Clean(strings.TrimSpace(path))
	if strings.HasPrefix(filepath.Base(path), ".") {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailurePartial,
		)
	}
	if filepath.Ext(path) != reviewArtifactFileExtension ||
		!reviewWorkerPathWithin(store.directory, path) {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailurePartial,
		)
	}
	if err != nil {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	if info.Size() > int64(store.policy.MaxBytes) {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureOversized,
		)
	}
	file, err := os.Open(path)
	if err != nil {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil ||
		!openedInfo.Mode().IsRegular() ||
		!os.SameFile(info, openedInfo) {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	body, err := io.ReadAll(io.LimitReader(
		file,
		int64(store.policy.MaxBytes)+1,
	))
	if err != nil {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if err := ctx.Err(); err != nil {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureTimeout,
		)
	}
	if len(body) > store.policy.MaxBytes {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureOversized,
		)
	}
	if reviewArtifactContainsProhibitedSecret(body, prohibitedValues...) {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureSecretMaterial,
		)
	}
	envelope, err := unmarshalReviewArtifactEnvelope(body)
	if err != nil {
		return ReviewArtifactEnvelope{}, nil, err
	}
	canonicalBody, err := marshalReviewArtifactEnvelope(envelope)
	if err != nil {
		return ReviewArtifactEnvelope{}, nil, err
	}
	if reviewArtifactContainsProhibitedSecret(
		canonicalBody,
		prohibitedValues...,
	) {
		return ReviewArtifactEnvelope{}, nil, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureSecretMaterial,
		)
	}
	return envelope, canonicalBody, nil
}

var prohibitedReviewArtifactPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----`),
	regexp.MustCompile(`(?i)\bauthorization\s*[:=]\s*(?:bearer|basic)\s+\S+`),
	regexp.MustCompile(`(?i)\b(?:gh_token|github_token|webex_webhook_url|aws_secret_access_key|api[_-]?key|client_secret|password|secret)\s*[:=]\s*["']?[^\s"',}]+`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9_]{20,}\b`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}\b`),
	regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
	regexp.MustCompile(`(?i)https://webexapis\.com/v1/webhooks/incoming/\S+`),
}

func reviewArtifactContainsProhibitedSecret(
	body []byte,
	prohibitedValues ...string,
) bool {
	for _, value := range prohibitedValues {
		value = strings.TrimSpace(value)
		if value != "" && bytes.Contains(body, []byte(value)) {
			return true
		}
	}
	for _, pattern := range prohibitedReviewArtifactPatterns {
		if pattern.Match(body) {
			return true
		}
	}
	return false
}

func reviewArtifactEvidencePaths(
	payload ReviewArtifactPayload,
) []string {
	paths := make([]string, 0)
	switch payload.Kind {
	case ReviewArtifactPayloadDiscovery:
		for _, candidate := range payload.Discovery.Candidates {
			paths = append(paths, candidate.Location.Path)
			for _, evidence := range candidate.Evidence {
				if evidence.Path != "" {
					paths = append(paths, evidence.Path)
				}
			}
		}
		for _, claim := range payload.Discovery.Coverage {
			for _, evidence := range claim.Evidence {
				if evidence.Path != "" {
					paths = append(paths, evidence.Path)
				}
			}
		}
	case ReviewArtifactPayloadVerification:
		if payload.Verification.Location != nil {
			paths = append(paths, payload.Verification.Location.Path)
		}
		for _, evidence := range append(
			append(
				append(
					[]ReviewEvidence(nil),
					payload.Verification.Evidence...,
				),
				payload.Verification.CausalEvidence...,
			),
			payload.Verification.TestEvidence...,
		) {
			if evidence.Path != "" {
				paths = append(paths, evidence.Path)
			}
		}
	case ReviewArtifactPayloadChallenge:
		for _, candidate := range payload.Challenge.Candidates {
			paths = append(paths, candidate.Location.Path)
			for _, evidence := range candidate.Evidence {
				if evidence.Path != "" {
					paths = append(paths, evidence.Path)
				}
			}
		}
		for _, claim := range payload.Challenge.Coverage {
			for _, evidence := range claim.Evidence {
				if evidence.Path != "" {
					paths = append(paths, evidence.Path)
				}
			}
		}
	}
	return paths
}

func canonicalizeReviewArtifactPaths(
	envelope *ReviewArtifactEnvelope,
) error {
	if envelope == nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	canonicalizeLocation := func(location *ReviewFindingLocation) error {
		if location == nil {
			return nil
		}
		canonical, ok := canonicalReviewRelativePath(location.Path)
		if !ok {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureInvalidPath,
			)
		}
		location.Path = canonical
		return nil
	}
	canonicalizeEvidence := func(evidence []ReviewEvidence) error {
		for index := range evidence {
			if evidence[index].Path == "" {
				continue
			}
			canonical, ok := canonicalReviewRelativePath(evidence[index].Path)
			if !ok {
				return newReviewArtifactErrorWithDetail(
					ReviewArtifactFailureTerminal,
					ReviewArtifactFailureInvalidPath,
					fmt.Sprintf("evidence path %d is invalid", index),
				)
			}
			evidence[index].Path = canonical
		}
		return nil
	}
	canonicalizeCandidate := func(candidate *ReviewFindingCandidate) error {
		if err := canonicalizeLocation(&candidate.Location); err != nil {
			return err
		}
		return canonicalizeEvidence(candidate.Evidence)
	}

	switch envelope.Payload.Kind {
	case ReviewArtifactPayloadDiscovery:
		if envelope.Payload.Discovery == nil {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureMalformed,
			)
		}
		for index := range envelope.Payload.Discovery.Candidates {
			if err := canonicalizeCandidate(
				&envelope.Payload.Discovery.Candidates[index],
			); err != nil {
				return err
			}
		}
		for index := range envelope.Payload.Discovery.Coverage {
			if err := canonicalizeEvidence(
				envelope.Payload.Discovery.Coverage[index].Evidence,
			); err != nil {
				return err
			}
		}
	case ReviewArtifactPayloadVerification:
		if envelope.Payload.Verification == nil {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureMalformed,
			)
		}
		verification := envelope.Payload.Verification
		if err := canonicalizeLocation(verification.Location); err != nil {
			return err
		}
		for _, evidence := range [][]ReviewEvidence{
			verification.Evidence,
			verification.CausalEvidence,
			verification.TestEvidence,
		} {
			if err := canonicalizeEvidence(evidence); err != nil {
				return err
			}
		}
	case ReviewArtifactPayloadChallenge:
		if envelope.Payload.Challenge == nil {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureMalformed,
			)
		}
		for index := range envelope.Payload.Challenge.Candidates {
			if err := canonicalizeCandidate(
				&envelope.Payload.Challenge.Candidates[index],
			); err != nil {
				return err
			}
		}
		for index := range envelope.Payload.Challenge.Coverage {
			if err := canonicalizeEvidence(
				envelope.Payload.Challenge.Coverage[index].Evidence,
			); err != nil {
				return err
			}
		}
	default:
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureMalformed,
		)
	}
	return nil
}

func validateReviewArtifactEvidencePaths(
	checkoutRoot string,
	paths []string,
) error {
	checkoutRoot = filepath.Clean(strings.TrimSpace(checkoutRoot))
	if !filepath.IsAbs(checkoutRoot) ||
		checkoutRoot == string(filepath.Separator) {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	resolvedRoot, err := filepath.EvalSymlinks(checkoutRoot)
	if err != nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
	}
	for _, referenced := range paths {
		if strings.TrimSpace(referenced) != referenced ||
			referenced == "" ||
			filepath.IsAbs(referenced) ||
			filepath.Clean(referenced) != filepath.FromSlash(referenced) ||
			referenced == "." ||
			strings.Contains(referenced, `\`) {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureInvalidPath,
			)
		}
		target := filepath.Join(checkoutRoot, filepath.FromSlash(referenced))
		if !reviewWorkerPathWithin(checkoutRoot, target) {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureInvalidPath,
			)
		}
		resolvedTarget, err := filepath.EvalSymlinks(target)
		if err != nil || !reviewWorkerPathWithin(resolvedRoot, resolvedTarget) {
			return newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureInvalidPath,
			)
		}
	}
	return nil
}

func validateReviewArtifactCheckout(
	ctx context.Context,
	checkoutRoot string,
	exactSHA string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureTimeout,
		)
	}
	headOutput, err := newCommandContext(
		ctx,
		"git",
		"-C",
		checkoutRoot,
		"rev-parse",
		"--verify",
		"HEAD",
	).Output()
	if err != nil {
		if ctx.Err() != nil {
			return newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureTimeout,
			)
		}
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureStaleSHA,
		)
	}
	if strings.TrimSpace(string(headOutput)) != exactSHA {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureStaleSHA,
		)
	}
	statusOutput, err := newCommandContext(
		ctx,
		"git",
		"-C",
		checkoutRoot,
		"status",
		"--porcelain=v1",
		"--untracked-files=all",
	).Output()
	if err != nil {
		if ctx.Err() != nil {
			return newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureTimeout,
			)
		}
		return newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if len(bytes.TrimSpace(statusOutput)) != 0 {
		return newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureDirtyCheckout,
		)
	}
	return nil
}

type ReviewArtifactReceipt struct {
	WorkerID   string                    `json:"worker_id"`
	Attempt    int                       `json:"attempt"`
	Phase      ReviewArtifactPhase       `json:"phase"`
	Lane       string                    `json:"lane"`
	Pass       int                       `json:"pass"`
	Kind       ReviewArtifactPayloadKind `json:"kind"`
	Digest     string                    `json:"digest"`
	Envelope   ReviewArtifactEnvelope    `json:"envelope"`
	AcceptedAt time.Time                 `json:"accepted_at"`
}

type ReviewArtifactIntakeFailure struct {
	WorkerID   string                     `json:"worker_id,omitempty"`
	Attempt    int                        `json:"attempt,omitempty"`
	Class      ReviewArtifactFailureClass `json:"class"`
	Code       ReviewArtifactFailureCode  `json:"code"`
	Detail     string                     `json:"detail,omitempty"`
	RecordedAt time.Time                  `json:"recorded_at"`
}

type ReviewLaneCompletion struct {
	WorkerID    string              `json:"worker_id"`
	Attempt     int                 `json:"attempt"`
	Lane        string              `json:"lane"`
	Phase       ReviewArtifactPhase `json:"phase"`
	Pass        int                 `json:"pass"`
	CompletedAt time.Time           `json:"completed_at"`
}

type reviewArtifactReceiptMutation struct {
	reviewerID               string
	receipt                  ReviewArtifactReceipt
	completion               ReviewLaneCompletion
	receiptAdded             bool
	completionAdded          bool
	previousDiscoveryPasses  []ReviewDiscoveryPassState
	previousFindings         []ReviewCanonicalFinding
	previousCoverageGaps     []ReviewCoverageGap
	previousConvergence      *ReviewConvergenceState
	previousLastActivityTime time.Time
}

func reviewWorkerLogicalIdentityKey(identity ReviewWorkerIdentity) string {
	return fmt.Sprintf(
		"%s\x00%d\x00%s\x00%s\x00%d",
		identity.CycleID,
		identity.Revision,
		identity.Role,
		identity.Lane,
		identity.Pass,
	)
}

func reviewLaneCompletionLogicalKey(
	identity ReviewWorkerIdentity,
	phase ReviewArtifactPhase,
) string {
	return fmt.Sprintf(
		"%s\x00%s",
		reviewWorkerLogicalIdentityKey(identity),
		phase,
	)
}

func reviewArtifactAttemptKey(workerID string, attempt int) string {
	return fmt.Sprintf("%s\x00%d", workerID, attempt)
}

func reviewArtifactOwnershipForAttempt(
	cycle *ReviewCycleState,
	workerID string,
	attempt int,
) (ReviewWorkerOwnership, bool) {
	if cycle == nil {
		return ReviewWorkerOwnership{}, false
	}
	for _, ownership := range cycle.WorkerOwnerships {
		if ownership.OwnerID == workerID && ownership.Attempt == attempt {
			return ownership, true
		}
	}
	return ReviewWorkerOwnership{}, false
}

func removeReviewLaneCompletionsForIdentity(
	cycle *ReviewCycleState,
	identity ReviewWorkerIdentity,
) []ReviewLaneCompletion {
	if cycle == nil || len(cycle.LaneCompletions) == 0 {
		return nil
	}
	retained := make(
		[]ReviewLaneCompletion,
		0,
		len(cycle.LaneCompletions),
	)
	for _, completion := range cycle.LaneCompletions {
		ownership, ok := reviewArtifactOwnershipForAttempt(
			cycle,
			completion.WorkerID,
			completion.Attempt,
		)
		if ok && sameReviewWorkerLogicalIdentity(
			ownership.Identity,
			identity,
		) {
			continue
		}
		retained = append(retained, completion)
	}
	return retained
}

func (m *AgentManager) recordReviewArtifactReceipt(
	reviewerID string,
	ownership ReviewWorkerOwnership,
	envelope ReviewArtifactEnvelope,
	acceptedAt time.Time,
	prohibitedValues ...string,
) (reviewArtifactReceiptMutation, error) {
	if m == nil {
		return reviewArtifactReceiptMutation{},
			errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil {
		return reviewArtifactReceiptMutation{}, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnregisteredWorker,
		)
	}
	if reviewer.ReviewCycle.Stale {
		return reviewArtifactReceiptMutation{}, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureStaleSHA,
		)
	}
	if err := validateReviewArtifactAgainstOwnership(
		envelope,
		reviewer.ReviewCycle.HeadSHA,
		ownership,
	); err != nil {
		return reviewArtifactReceiptMutation{}, err
	}
	registered := false
	currentAttempt := 0
	for _, candidate := range reviewer.ReviewCycle.WorkerOwnerships {
		if candidate.OwnerID == ownership.OwnerID &&
			candidate.Attempt == ownership.Attempt {
			registered = true
		}
		if sameReviewWorkerLogicalIdentity(
			candidate.Identity,
			ownership.Identity,
		) && candidate.Attempt > currentAttempt {
			currentAttempt = candidate.Attempt
		}
	}
	if !registered {
		return reviewArtifactReceiptMutation{}, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnregisteredWorker,
		)
	}
	if ownership.Attempt != currentAttempt {
		return reviewArtifactReceiptMutation{}, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureConflict,
		)
	}
	for _, failure := range reviewer.ReviewCycle.ArtifactFailures {
		if failure.WorkerID == ownership.OwnerID &&
			failure.Attempt == ownership.Attempt &&
			failure.Class == ReviewArtifactFailureTerminal {
			return reviewArtifactReceiptMutation{}, newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureConflict,
			)
		}
	}
	canonicalBody, err := marshalReviewArtifactEnvelope(envelope)
	if err != nil {
		return reviewArtifactReceiptMutation{}, err
	}
	if reviewArtifactContainsProhibitedSecret(
		canonicalBody,
		prohibitedValues...,
	) {
		return reviewArtifactReceiptMutation{}, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureSecretMaterial,
		)
	}
	sum := sha256.Sum256(canonicalBody)
	digest := hex.EncodeToString(sum[:])
	for _, receipt := range reviewer.ReviewCycle.ArtifactReceipts {
		if receipt.WorkerID != envelope.WorkerID ||
			receipt.Attempt != envelope.Attempt {
			continue
		}
		if receipt.Digest == digest {
			return reviewArtifactReceiptMutation{}, nil
		}
		return reviewArtifactReceiptMutation{}, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureConflict,
		)
	}
	completionKey := reviewLaneCompletionLogicalKey(
		ownership.Identity,
		envelope.Phase,
	)
	for _, completion := range reviewer.ReviewCycle.LaneCompletions {
		completionOwnership, ok := reviewArtifactOwnershipForAttempt(
			reviewer.ReviewCycle,
			completion.WorkerID,
			completion.Attempt,
		)
		if ok && reviewLaneCompletionLogicalKey(
			completionOwnership.Identity,
			completion.Phase,
		) == completionKey {
			return reviewArtifactReceiptMutation{}, newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureConflict,
			)
		}
	}
	receipt := ReviewArtifactReceipt{
		WorkerID:   envelope.WorkerID,
		Attempt:    envelope.Attempt,
		Phase:      envelope.Phase,
		Lane:       envelope.Lane,
		Pass:       envelope.Pass,
		Kind:       envelope.Payload.Kind,
		Digest:     digest,
		Envelope:   envelope,
		AcceptedAt: acceptedAt.UTC(),
	}
	completion := ReviewLaneCompletion{
		WorkerID:    envelope.WorkerID,
		Attempt:     envelope.Attempt,
		Lane:        envelope.Lane,
		Phase:       envelope.Phase,
		Pass:        envelope.Pass,
		CompletedAt: acceptedAt.UTC(),
	}
	mutation := reviewArtifactReceiptMutation{
		reviewerID:      reviewer.ID,
		receipt:         receipt,
		completion:      completion,
		receiptAdded:    true,
		completionAdded: true,
		previousDiscoveryPasses: cloneReviewDiscoveryPasses(
			reviewer.ReviewCycle.DiscoveryPasses,
		),
		previousFindings: append(
			[]ReviewCanonicalFinding(nil),
			reviewer.ReviewCycle.CanonicalFindings...,
		),
		previousCoverageGaps: append(
			[]ReviewCoverageGap(nil),
			reviewer.ReviewCycle.UnresolvedCoverage...,
		),
		previousConvergence: cloneReviewConvergenceState(
			reviewer.ReviewCycle.Convergence,
		),
		previousLastActivityTime: reviewer.LastActivityTime,
	}
	reviewer.ReviewCycle.ArtifactReceipts = append(
		reviewer.ReviewCycle.ArtifactReceipts,
		receipt,
	)
	reviewer.ReviewCycle.LaneCompletions = append(
		reviewer.ReviewCycle.LaneCompletions,
		completion,
	)
	rollbackDerivedState := func() {
		reviewer.ReviewCycle.ArtifactReceipts =
			reviewer.ReviewCycle.ArtifactReceipts[:len(reviewer.ReviewCycle.ArtifactReceipts)-1]
		reviewer.ReviewCycle.LaneCompletions =
			reviewer.ReviewCycle.LaneCompletions[:len(reviewer.ReviewCycle.LaneCompletions)-1]
		reviewer.ReviewCycle.DiscoveryPasses =
			mutation.previousDiscoveryPasses
		reviewer.ReviewCycle.CanonicalFindings =
			mutation.previousFindings
		reviewer.ReviewCycle.UnresolvedCoverage =
			mutation.previousCoverageGaps
		reviewer.ReviewCycle.Convergence =
			mutation.previousConvergence
	}
	if err := completeReviewDiscoveryLaneFromReceipt(
		reviewer.ReviewCycle,
		ownership,
		envelope,
		acceptedAt,
	); err != nil {
		rollbackDerivedState()
		return reviewArtifactReceiptMutation{}, err
	}
	if err := completeReviewVerificationFromReceipt(
		reviewer.ReviewCycle,
		ownership,
		envelope,
		acceptedAt,
	); err != nil {
		rollbackDerivedState()
		return reviewArtifactReceiptMutation{}, err
	}
	if err := completeReviewChallengeFromReceipt(
		reviewer.ReviewCycle,
		ownership,
		envelope,
		acceptedAt,
	); err != nil {
		rollbackDerivedState()
		return reviewArtifactReceiptMutation{}, err
	}
	if envelope.Phase == ReviewArtifactPhaseChallenge {
		if err := rebuildReviewDiscoveryDerivedState(
			reviewer.ReviewCycle,
		); err != nil {
			rollbackDerivedState()
			return reviewArtifactReceiptMutation{}, err
		}
	}
	if err := syncReviewFindingVerifications(
		reviewer.ReviewCycle,
	); err != nil {
		rollbackDerivedState()
		return reviewArtifactReceiptMutation{}, err
	}
	reviewer.LastActivityTime = acceptedAt.UTC()
	return mutation, nil
}

func (m *AgentManager) rollbackReviewArtifactReceipt(
	mutation reviewArtifactReceiptMutation,
) {
	if m == nil || (!mutation.receiptAdded && !mutation.completionAdded) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	reviewer, ok := m.agents[mutation.reviewerID]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return
	}
	if mutation.receiptAdded {
		receipts := reviewer.ReviewCycle.ArtifactReceipts
		for index := len(receipts) - 1; index >= 0; index-- {
			if receipts[index].WorkerID == mutation.receipt.WorkerID &&
				receipts[index].Attempt == mutation.receipt.Attempt &&
				receipts[index].Digest == mutation.receipt.Digest {
				reviewer.ReviewCycle.ArtifactReceipts = append(
					receipts[:index],
					receipts[index+1:]...,
				)
				break
			}
		}
	}
	if mutation.completionAdded {
		completions := reviewer.ReviewCycle.LaneCompletions
		for index := len(completions) - 1; index >= 0; index-- {
			if completions[index] == mutation.completion {
				reviewer.ReviewCycle.LaneCompletions = append(
					completions[:index],
					completions[index+1:]...,
				)
				break
			}
		}
	}
	reviewer.ReviewCycle.DiscoveryPasses =
		cloneReviewDiscoveryPasses(mutation.previousDiscoveryPasses)
	reviewer.ReviewCycle.CanonicalFindings = append(
		[]ReviewCanonicalFinding(nil),
		mutation.previousFindings...,
	)
	reviewer.ReviewCycle.UnresolvedCoverage = append(
		[]ReviewCoverageGap(nil),
		mutation.previousCoverageGaps...,
	)
	reviewer.ReviewCycle.Convergence =
		cloneReviewConvergenceState(mutation.previousConvergence)
	if reviewer.LastActivityTime.Equal(mutation.receipt.AcceptedAt) {
		reviewer.LastActivityTime = mutation.previousLastActivityTime
	}
}

func (m *AgentManager) recordReviewArtifactFailure(
	reviewerID string,
	ownership *ReviewWorkerOwnership,
	artifactErr *ReviewArtifactError,
	recordedAt time.Time,
) {
	if m == nil || artifactErr == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil || reviewer.ReviewCycle.Stale ||
		agentLifecycleTerminal(reviewer) {
		return
	}
	failure := ReviewArtifactIntakeFailure{
		Class:      artifactErr.Class,
		Code:       artifactErr.Code,
		Detail:     artifactErr.Detail,
		RecordedAt: recordedAt.UTC(),
	}
	if ownership != nil {
		failure.WorkerID = ownership.OwnerID
		failure.Attempt = ownership.Attempt
	}
	if failure.Class == ReviewArtifactFailureTerminal {
		for _, receipt := range reviewer.ReviewCycle.ArtifactReceipts {
			if receipt.WorkerID == failure.WorkerID &&
				receipt.Attempt == failure.Attempt {
				return
			}
		}
	}
	for _, existing := range reviewer.ReviewCycle.ArtifactFailures {
		if existing.WorkerID == failure.WorkerID &&
			existing.Attempt == failure.Attempt &&
			existing.Class == failure.Class &&
			existing.Code == failure.Code {
			return
		}
	}
	reviewer.ReviewCycle.ArtifactFailures = append(
		reviewer.ReviewCycle.ArtifactFailures,
		failure,
	)
	reviewer.LastActivityTime = recordedAt.UTC()
}

func (b *Orchestrator) intakeReviewWorkerArtifact(
	ctx context.Context,
	reviewerID string,
	ownerID string,
	artifactPath string,
	expectedPhase ReviewArtifactPhase,
) (ReviewArtifactEnvelope, error) {
	return b.intakeReviewWorkerArtifactWithMalformedCorrection(
		ctx,
		reviewerID,
		ownerID,
		artifactPath,
		expectedPhase,
		false,
	)
}

func (b *Orchestrator) intakeReviewWorkerArtifactWithMalformedCorrection(
	ctx context.Context,
	reviewerID string,
	ownerID string,
	artifactPath string,
	expectedPhase ReviewArtifactPhase,
	allowMalformedCorrection bool,
) (ReviewArtifactEnvelope, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if b == nil || b.agents == nil {
		return ReviewArtifactEnvelope{}, errors.New(
			"review artifact intake is not configured",
		)
	}
	reviewer, unlockLifecycle, lockErr :=
		b.agents.lockActiveReviewCoordinatorForWorkerLaunch(reviewerID)
	if lockErr != nil {
		if errors.Is(lockErr, errReviewCycleStale) {
			return ReviewArtifactEnvelope{}, newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureStaleSHA,
			)
		}
		return ReviewArtifactEnvelope{}, newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnregisteredWorker,
		)
	}
	lifecycleLocked := true
	defer func() {
		if lifecycleLocked {
			unlockLifecycle()
		}
	}()
	recheckHead := func() error {
		liveHeadSHA, headErr := b.resolveLiveReviewHead(ctx, reviewer)
		if headErr != nil {
			return fmt.Errorf(
				"failed to recheck live head before review artifact intake: %w",
				headErr,
			)
		}
		if liveHeadSHA == reviewer.ReviewCycle.HeadSHA {
			return nil
		}
		lifecycleLocked = false
		unlockLifecycle()
		invalidateErr := b.invalidateStaleReviewCycle(
			ctx,
			reviewer.ID,
			liveHeadSHA,
		)
		return errors.Join(
			newReviewArtifactError(
				ReviewArtifactFailureTerminal,
				ReviewArtifactFailureStaleSHA,
			),
			invalidateErr,
		)
	}
	if err := recheckHead(); err != nil {
		return ReviewArtifactEnvelope{}, err
	}
	recordRejection := func(
		ownership *ReviewWorkerOwnership,
		artifactErr *ReviewArtifactError,
	) error {
		if err := recheckHead(); err != nil {
			return err
		}
		if allowMalformedCorrection &&
			artifactErr != nil &&
			artifactErr.Code == ReviewArtifactFailureMalformed {
			return artifactErr
		}
		return b.recordReviewArtifactRejection(
			reviewer.ID,
			ownership,
			artifactErr,
		)
	}
	var ownership *ReviewWorkerOwnership
	for index := range reviewer.ReviewCycle.WorkerOwnerships {
		if reviewer.ReviewCycle.WorkerOwnerships[index].OwnerID == ownerID {
			candidate := reviewer.ReviewCycle.WorkerOwnerships[index]
			ownership = &candidate
			break
		}
	}
	if ownership == nil {
		artifactErr := newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnregisteredWorker,
		)
		return ReviewArtifactEnvelope{}, recordRejection(
			nil,
			artifactErr,
		)
	}
	directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
	if err != nil {
		artifactErr := newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	store, err := newReviewArtifactStore(
		directory,
		reviewer.ReviewCycle.Policy.Artifacts,
	)
	if err != nil {
		return ReviewArtifactEnvelope{}, err
	}
	intakeCtx, cancel := context.WithTimeout(
		ctx,
		time.Duration(
			reviewer.ReviewCycle.Policy.Artifacts.TimeoutSeconds,
		)*time.Second,
	)
	defer cancel()
	if filepath.Clean(artifactPath) != filepath.Join(
		directory,
		reviewArtifactFilename(*ownership),
	) {
		artifactErr := newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureInvalidPath,
		)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	envelope, _, readErr := store.read(
		intakeCtx,
		artifactPath,
		b.token,
		b.cfg.WebexWebhookURL,
	)
	if readErr != nil {
		artifactErr := asReviewArtifactError(readErr)
		if artifactErr == nil {
			artifactErr = newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureIO,
			)
		}
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if err := canonicalizeReviewArtifactPaths(&envelope); err != nil {
		artifactErr := asReviewArtifactError(err)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if envelope.Phase != expectedPhase {
		artifactErr := newReviewArtifactError(
			ReviewArtifactFailureTerminal,
			ReviewArtifactFailureUnexpectedPhase,
		)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if err := validateReviewArtifactAgainstOwnership(
		envelope,
		reviewer.ReviewCycle.HeadSHA,
		*ownership,
	); err != nil {
		artifactErr := asReviewArtifactError(err)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if err := validateReviewArtifactCheckout(
		intakeCtx,
		ownership.WorktreePath,
		reviewer.ReviewCycle.HeadSHA,
	); err != nil {
		artifactErr := asReviewArtifactError(err)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if err := validateReviewArtifactEvidencePaths(
		ownership.WorktreePath,
		reviewArtifactEvidencePaths(envelope.Payload),
	); err != nil {
		artifactErr := asReviewArtifactError(err)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if intakeCtx.Err() != nil {
		artifactErr := newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureTimeout,
		)
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if err := recheckHead(); err != nil {
		return ReviewArtifactEnvelope{}, err
	}
	acceptedAt := time.Now().UTC()
	b.reviewArtifactMu.Lock()
	b.statePersistenceMu.Lock()
	mutation, err := b.agents.recordReviewArtifactReceipt(
		reviewer.ID,
		*ownership,
		envelope,
		acceptedAt,
		b.token,
		b.cfg.WebexWebhookURL,
	)
	if err != nil {
		b.statePersistenceMu.Unlock()
		b.reviewArtifactMu.Unlock()
		artifactErr := asReviewArtifactError(err)
		if artifactErr == nil {
			artifactErr = newReviewArtifactError(
				ReviewArtifactFailureTransient,
				ReviewArtifactFailureIO,
			)
		}
		return ReviewArtifactEnvelope{}, recordRejection(
			ownership,
			artifactErr,
		)
	}
	if err := b.persistAgentStateLocked(); err != nil {
		b.agents.rollbackReviewArtifactReceipt(mutation)
		b.statePersistenceMu.Unlock()
		b.reviewArtifactMu.Unlock()
		return ReviewArtifactEnvelope{}, newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	b.statePersistenceMu.Unlock()
	b.reviewArtifactMu.Unlock()
	return envelope, nil
}

func (b *Orchestrator) recordReviewArtifactRejection(
	reviewerID string,
	ownership *ReviewWorkerOwnership,
	artifactErr *ReviewArtifactError,
) *ReviewArtifactError {
	if artifactErr == nil {
		return newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	if b == nil || b.agents == nil {
		return artifactErr
	}
	b.reviewArtifactMu.Lock()
	b.statePersistenceMu.Lock()
	b.agents.recordReviewArtifactFailure(
		reviewerID,
		ownership,
		artifactErr,
		time.Now().UTC(),
	)
	if err := b.persistAgentStateLocked(); err != nil {
		b.statePersistenceMu.Unlock()
		b.reviewArtifactMu.Unlock()
		return newReviewArtifactError(
			ReviewArtifactFailureTransient,
			ReviewArtifactFailureIO,
		)
	}
	b.statePersistenceMu.Unlock()
	b.reviewArtifactMu.Unlock()
	return artifactErr
}

func validatePersistedReviewArtifacts(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("persisted review cycle is missing")
	}
	ownerships := make(map[string]ReviewWorkerOwnership)
	currentOwnerships := make(map[string]ReviewWorkerOwnership)
	for _, ownership := range cycle.WorkerOwnerships {
		ownerships[ownership.OwnerID] = ownership
		key := reviewWorkerLogicalIdentityKey(ownership.Identity)
		current, ok := currentOwnerships[key]
		if !ok || ownership.Attempt > current.Attempt {
			currentOwnerships[key] = ownership
		}
	}
	terminalFailures := make(map[string]time.Time)
	for _, failure := range cycle.ArtifactFailures {
		if failure.RecordedAt.IsZero() ||
			(failure.Class != ReviewArtifactFailureTransient &&
				failure.Class != ReviewArtifactFailureTerminal) ||
			!supportedReviewArtifactFailureCode(failure.Code) ||
			failure.Detail != boundedReviewArtifactErrorDetail(failure.Detail) {
			return errors.New("persisted review artifact failure is invalid")
		}
		if failure.WorkerID == "" {
			if failure.Attempt != 0 {
				return errors.New("persisted review artifact failure attempt has no worker")
			}
			continue
		}
		ownership, ok := ownerships[failure.WorkerID]
		if !ok || ownership.Attempt != failure.Attempt {
			return errors.New("persisted review artifact failure has an unknown worker")
		}
		if failure.Class == ReviewArtifactFailureTerminal {
			key := reviewArtifactAttemptKey(
				failure.WorkerID,
				failure.Attempt,
			)
			recordedAt, exists := terminalFailures[key]
			if !exists || failure.RecordedAt.Before(recordedAt) {
				terminalFailures[key] = failure.RecordedAt
			}
		}
	}
	receipts := make(map[string]ReviewArtifactReceipt)
	for _, receipt := range cycle.ArtifactReceipts {
		ownership, ok := ownerships[receipt.WorkerID]
		if !ok ||
			ownership.Attempt != receipt.Attempt ||
			receipt.AcceptedAt.IsZero() ||
			len(receipt.Digest) != sha256.Size*2 ||
			receipt.Phase != receipt.Envelope.Phase ||
			receipt.Lane != receipt.Envelope.Lane ||
			receipt.Pass != receipt.Envelope.Pass ||
			receipt.Kind != receipt.Envelope.Payload.Kind {
			return errors.New("persisted review artifact receipt is invalid")
		}
		if _, err := hex.DecodeString(receipt.Digest); err != nil {
			return errors.New("persisted review artifact receipt digest is invalid")
		}
		if err := validateReviewArtifactEnvelope(receipt.Envelope); err != nil {
			return errors.New("persisted review artifact envelope is invalid")
		}
		originalBody, err := marshalReviewArtifactEnvelope(receipt.Envelope)
		if err != nil {
			return errors.New("persisted review artifact envelope is invalid")
		}
		canonical, err := unmarshalReviewArtifactEnvelope(originalBody)
		if err != nil {
			return errors.New("persisted review artifact envelope is invalid")
		}
		if err := canonicalizeReviewArtifactPaths(&canonical); err != nil {
			return errors.New("persisted review artifact paths are invalid")
		}
		canonicalBody, err := marshalReviewArtifactEnvelope(canonical)
		if err != nil || !bytes.Equal(canonicalBody, originalBody) {
			return errors.New("persisted review artifact paths are not canonical")
		}
		if err := validateReviewArtifactAgainstOwnership(
			receipt.Envelope,
			cycle.HeadSHA,
			ownership,
		); err != nil {
			return errors.New("persisted review artifact receipt is untrusted")
		}
		body, err := marshalReviewArtifactEnvelope(receipt.Envelope)
		if err != nil ||
			reviewArtifactContainsProhibitedSecret(body) {
			return errors.New("persisted review artifact receipt is unsafe")
		}
		sum := sha256.Sum256(body)
		if receipt.Digest != hex.EncodeToString(sum[:]) {
			return errors.New("persisted review artifact receipt digest does not match")
		}
		key := reviewArtifactAttemptKey(receipt.WorkerID, receipt.Attempt)
		if _, duplicate := receipts[key]; duplicate {
			return errors.New("persisted review artifact receipt is duplicated")
		}
		if terminalAt, terminal := terminalFailures[key]; terminal &&
			!terminalAt.After(receipt.AcceptedAt) {
			return errors.New(
				"persisted review artifact receipt follows terminal attempt failure",
			)
		}
		current := currentOwnerships[reviewWorkerLogicalIdentityKey(ownership.Identity)]
		if current.Attempt > receipt.Attempt &&
			!receipt.AcceptedAt.Before(current.AllocatedAt) {
			return errors.New(
				"persisted review artifact receipt follows a newer attempt",
			)
		}
		receipts[key] = receipt
	}
	completions := make(map[string]ReviewLaneCompletion)
	for _, completion := range cycle.LaneCompletions {
		attemptKey := reviewArtifactAttemptKey(
			completion.WorkerID,
			completion.Attempt,
		)
		receipt, ok := receipts[attemptKey]
		if !ok ||
			completion.CompletedAt.IsZero() ||
			completion.Lane != receipt.Lane ||
			completion.Phase != receipt.Phase ||
			completion.Pass != receipt.Pass {
			return errors.New("persisted review lane completion has no trusted artifact")
		}
		ownership, ok := ownerships[completion.WorkerID]
		if !ok || ownership.Attempt != completion.Attempt {
			return errors.New(
				"persisted review lane completion has an unknown owner",
			)
		}
		current := currentOwnerships[reviewWorkerLogicalIdentityKey(ownership.Identity)]
		if current.OwnerID != ownership.OwnerID ||
			current.Attempt != ownership.Attempt {
			return errors.New(
				"persisted review lane completion belongs to a prior attempt",
			)
		}
		completionKey := reviewLaneCompletionLogicalKey(
			ownership.Identity,
			completion.Phase,
		)
		if _, duplicate := completions[completionKey]; duplicate {
			return errors.New("persisted review lane completion is duplicated")
		}
		completions[completionKey] = completion
	}
	for _, receipt := range receipts {
		ownership := ownerships[receipt.WorkerID]
		current := currentOwnerships[reviewWorkerLogicalIdentityKey(ownership.Identity)]
		if current.OwnerID != ownership.OwnerID ||
			current.Attempt != ownership.Attempt {
			continue
		}
		completion, ok := completions[reviewLaneCompletionLogicalKey(
			ownership.Identity,
			receipt.Phase,
		)]
		if !ok ||
			completion.WorkerID != receipt.WorkerID ||
			completion.Attempt != receipt.Attempt {
			return errors.New(
				"persisted current review artifact has no lane completion",
			)
		}
	}
	return nil
}

func supportedReviewArtifactFailureCode(code ReviewArtifactFailureCode) bool {
	switch code {
	case ReviewArtifactFailureMalformed,
		ReviewArtifactFailureOversized,
		ReviewArtifactFailureTimeout,
		ReviewArtifactFailurePartial,
		ReviewArtifactFailureIO,
		ReviewArtifactFailureConflict,
		ReviewArtifactFailureStaleSHA,
		ReviewArtifactFailureUnregisteredWorker,
		ReviewArtifactFailureUnexpectedPhase,
		ReviewArtifactFailureDirtyCheckout,
		ReviewArtifactFailureInvalidPath,
		ReviewArtifactFailureSecretMaterial:
		return true
	default:
		return false
	}
}
