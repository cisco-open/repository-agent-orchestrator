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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	reviewWorkerIdentityTokenBytes           = 16
	reviewWorkerOwnerNameLimit               = 160
	reviewWorkerWorktreeNameLimit            = 120
	reviewWorkerArtifactOwnerV1              = "repository-agent-orchestrator-review-worker-artifact-owner-v1\n"
	reviewWorkerArtifactOwnerSuffix          = ".owner"
	reviewWorkerWorktreeSetupAttempts        = 3
	reviewWorkerWorktreeSetupTimeout         = 2 * time.Minute
	reviewWorkerWorktreeSetupRetryDelay      = 100 * time.Millisecond
	reviewWorkerWorktreeSetupDiagnosticBytes = commandFailureOutputBytes
)

const (
	reviewWorkerEnvRepo        = "RAO_REVIEW_REPO"
	reviewWorkerEnvRepoOwner   = "RAO_REVIEW_REPO_OWNER"
	reviewWorkerEnvRepoName    = "RAO_REVIEW_REPO_NAME"
	reviewWorkerEnvPRNumber    = "RAO_REVIEW_PR_NUMBER"
	reviewWorkerEnvHeadSHA     = "RAO_REVIEW_HEAD_SHA"
	reviewWorkerEnvCycleID     = "RAO_REVIEW_CYCLE_ID"
	reviewWorkerEnvRevision    = "RAO_REVIEW_REVISION"
	reviewWorkerEnvRole        = "RAO_REVIEW_ROLE"
	reviewWorkerEnvPass        = "RAO_REVIEW_PASS"
	reviewWorkerEnvLane        = "RAO_REVIEW_LANE"
	reviewWorkerEnvAttempt     = "RAO_REVIEW_ATTEMPT"
	reviewWorkerEnvOwnerID     = "RAO_REVIEW_OWNER_ID"
	reviewWorkerEnvWorktree    = "RAO_REVIEW_WORKTREE"
	reviewWorkerEnvArtifactDir = "RAO_REVIEW_ARTIFACT_DIR"
	reviewWorkerGitConfigName  = "review-worker-gh"
)

var reviewWorkerAmbientEnvironmentAllowlist = map[string]struct{}{
	"HOME":   {},
	"LANG":   {},
	"LC_ALL": {},
	"PATH":   {},
	"TERM":   {},
	"TMPDIR": {},
}

var reviewWorkerFixedEnvironmentAllowlist = map[string]struct{}{
	"GH_CONFIG_DIR":       {},
	"GIT_ASKPASS":         {},
	"GIT_CONFIG_GLOBAL":   {},
	"GIT_CONFIG_NOSYSTEM": {},
	"GIT_TERMINAL_PROMPT": {},
	"SSH_ASKPASS":         {},
}

var reviewWorkerContextEnvironmentAllowlist = map[string]struct{}{
	reviewWorkerEnvRepo:        {},
	reviewWorkerEnvRepoOwner:   {},
	reviewWorkerEnvRepoName:    {},
	reviewWorkerEnvPRNumber:    {},
	reviewWorkerEnvHeadSHA:     {},
	reviewWorkerEnvCycleID:     {},
	reviewWorkerEnvRevision:    {},
	reviewWorkerEnvRole:        {},
	reviewWorkerEnvPass:        {},
	reviewWorkerEnvLane:        {},
	reviewWorkerEnvAttempt:     {},
	reviewWorkerEnvOwnerID:     {},
	reviewWorkerEnvWorktree:    {},
	reviewWorkerEnvArtifactDir: {},
}

type ReviewWorkerIdentity struct {
	CycleID  string           `json:"cycle_id"`
	Revision int              `json:"revision"`
	Role     AgentProfileRole `json:"role"`
	Pass     int              `json:"pass"`
	Lane     string           `json:"lane"`
}

type ReviewWorkerLifecycleState = DurableLaunchLifecycle

const (
	ReviewWorkerReserved  = DurableLaunchReserved
	ReviewWorkerRunning   = DurableLaunchRunning
	ReviewWorkerCompleted = DurableLaunchCompleted
	ReviewWorkerFailed    = DurableLaunchFailed
	ReviewWorkerCancelled = DurableLaunchCancelled
)

type ReviewWorkerOwnership struct {
	DurableLaunchAttempt
	Identity                 ReviewWorkerIdentity `json:"identity"`
	Profile                  string               `json:"profile"`
	Model                    string               `json:"model"`
	ReasoningEffort          string               `json:"reasoning_effort"`
	WorktreeName             string               `json:"worktree_name"`
	WorktreePath             string               `json:"worktree_path"`
	GitHubConfigPath         string               `json:"github_config_path"`
	ArtifactDirectoryClaimed bool                 `json:"artifact_directory_claimed,omitempty"`
}

type reviewWorkerWorktreeSetupError struct {
	attempts int
	err      error
}

type reviewWorkerBoundaryError struct {
	err error
}

func (failure *reviewWorkerBoundaryError) Error() string {
	if failure == nil || failure.err == nil {
		return "review worker repository boundary rejected operation"
	}
	return failure.err.Error()
}

func (failure *reviewWorkerBoundaryError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.err
}

func newReviewWorkerBoundaryError(err error) error {
	if err == nil {
		err = errors.New("review worker repository boundary rejected operation")
	}
	return &reviewWorkerBoundaryError{err: err}
}

func (failure *reviewWorkerWorktreeSetupError) Error() string {
	if failure == nil {
		return "review worker worktree setup failed"
	}
	return fmt.Sprintf(
		"review worker worktree setup failed after %d attempt(s): %v",
		failure.attempts,
		failure.err,
	)
}

func (failure *reviewWorkerWorktreeSetupError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.err
}

type reviewWorkerOwnershipMutation struct {
	reviewerID                string
	ownership                 ReviewWorkerOwnership
	previousLaneCompletions   []ReviewLaneCompletion
	previousLastActivityTime  time.Time
	reservationLastActivityAt time.Time
}

type reviewWorkerArtifactClaimMutation struct {
	reviewerID               string
	ownerID                  string
	previousClaimed          bool
	previousLastActivityTime time.Time
	claimLastActivityAt      time.Time
}

type reviewWorkerLifecycleMutation struct {
	reviewerID               string
	ownerID                  string
	previous                 ReviewWorkerOwnership
	previousLastActivityTime time.Time
	updatedAt                time.Time
	changed                  bool
}

type reviewWorkerLaunchRequest struct {
	Identity ReviewWorkerIdentity
	Prompt   string
}

type isolatedReviewWorkerRunner interface {
	ValidateReviewWorkerIsolation(environment []string) error
	StartReviewWorkerContext(
		ctx context.Context,
		agent Agent,
		initialPrompt string,
		environment []string,
	) (RuntimeHandle, error)
	Stop(handle RuntimeHandle) error
}

func reviewWorkerRuntimeTimeout(
	cycle *ReviewCycleState,
	role AgentProfileRole,
) time.Duration {
	return reviewWorkerRuntimeTimeoutAt(cycle, role, time.Now().UTC())
}

func reviewWorkerRuntimeTimeoutAt(
	cycle *ReviewCycleState,
	role AgentProfileRole,
	now time.Time,
) time.Duration {
	workerTimeout := reviewWorkerConfiguredRuntimeTimeout(cycle, role)
	if workerTimeout <= 0 {
		return 0
	}
	startedAt := reviewCycleMetricsStartedAt(cycle)
	if startedAt.IsZero() || now.IsZero() {
		return workerTimeout
	}
	remaining := startedAt.Add(
		time.Duration(cycle.Policy.Convergence.MaxWallTimeMinutes) * time.Minute,
	).Sub(now)
	if remaining <= 0 {
		return 0
	}
	if remaining < workerTimeout {
		return remaining
	}
	return workerTimeout
}

func reviewWorkerConfiguredRuntimeTimeout(
	cycle *ReviewCycleState,
	role AgentProfileRole,
) time.Duration {
	if cycle == nil {
		return 0
	}
	switch role {
	case AgentProfileRoleDiscovery, AgentProfileRoleChallenge:
		return time.Duration(cycle.Policy.Swarm.TimeoutMinutes) * time.Minute
	case AgentProfileRoleVerifier:
		return time.Duration(
			cycle.Policy.Verification.TimeoutMinutes,
		) * time.Minute
	default:
		return 0
	}
}

func reviewWorkerRuntimeDeadline(
	cycle *ReviewCycleState,
	role AgentProfileRole,
	workerStartedAt time.Time,
) time.Time {
	workerTimeout := reviewWorkerConfiguredRuntimeTimeout(cycle, role)
	if workerTimeout <= 0 || workerStartedAt.IsZero() {
		return time.Time{}
	}
	deadline := workerStartedAt.Add(workerTimeout)
	cycleStartedAt := reviewCycleMetricsStartedAt(cycle)
	if cycleStartedAt.IsZero() {
		return deadline
	}
	cycleDeadline := cycleStartedAt.Add(
		time.Duration(cycle.Policy.Convergence.MaxWallTimeMinutes) * time.Minute,
	)
	if cycleDeadline.Before(deadline) {
		return cycleDeadline
	}
	return deadline
}

func reviewWorkerArtifactContract(role AgentProfileRole) (string, error) {
	const candidateShape = `{"candidate_id":string,"summary":string,"location":{"path":string,"symbol":string,"start_line":integer,"end_line":integer},"behavioral_path":string,"violated_invariant":string,"severity":"critical|high|medium|low","confidence":"high|medium|low","evidence":[{"summary":string,"path":string,"start_line":integer,"end_line":integer}]}`
	const coverageShape = `{"requirement_id":string,"kind":"acceptance_criterion|changed_symbol|changed_branch|call_path|state_transition|persistence_boundary|risk_domain","status":"covered|partial|not_covered|not_applicable","evidence":[{"summary":string,"path":string,"start_line":integer,"end_line":integer}]}`
	var phase string
	var payload string
	var payloadRequirements string
	switch role {
	case AgentProfileRoleDiscovery:
		phase = string(ReviewArtifactPhaseDiscovery)
		payload = `{"kind":"discovery","summary":string,"candidates":[` +
			candidateShape + `],"coverage":[` + coverageShape +
			`],"unreviewed_areas":[string]}`
	case AgentProfileRoleVerifier:
		phase = string(ReviewArtifactPhaseVerification)
		payload = `{"kind":"verification","finding_id":string,"outcome":"confirmed|rejected|inconclusive","summary":string,"scope_disposition":"in_scope|out_of_scope|unclear","patch_disposition":"introduced|worsened|pre_existing|unrelated|not_reproduced|unclear","location":{"path":string,"symbol":string,"start_line":integer,"end_line":integer},"behavioral_path":string,"evidence":[{"summary":string,"path":string,"start_line":integer,"end_line":integer}],"causal_evidence":[{"summary":string,"path":string,"start_line":integer,"end_line":integer}],"test_evidence":[{"summary":string,"path":string,"start_line":integer,"end_line":integer}],"test_not_practical_reason":string}`
		payloadRequirements = " For a confirmed or rejected outcome, copy the " +
			"assigned finding's location and behavioral_path into those fields; " +
			"put independent wording in summary and evidence. At least one evidence " +
			"or test_evidence item must cite the assigned location path; use " +
			"test_evidence when the assigned location is a test. A confirmed outcome " +
			"requires in_scope plus introduced or worsened and causal_evidence on " +
			"the exact changed line."
	case AgentProfileRoleChallenge, AgentProfileRoleEscalation:
		phase = string(ReviewArtifactPhaseChallenge)
		payload = `{"kind":"challenge","outcome":"upheld|overturned|inconclusive","summary":string,"assignment_id":string,"target_kind":string,"target_id":string,"candidates":[` +
			candidateShape + `],"coverage":[` + coverageShape + `]}`
	default:
		return "", fmt.Errorf(
			"review worker role %q has no artifact contract",
			role,
		)
	}
	return fmt.Sprintf(
		"Artifact contract (required): publish exactly one UTF-8 JSON object "+
			"at \"$RAO_REVIEW_ARTIFACT_DIR/$RAO_REVIEW_OWNER_ID.json\"; the "+
			"coordinator observes no other filename. Write a temporary file in "+
			"RAO_REVIEW_ARTIFACT_DIR whose basename starts with "+
			"\".review-artifact-partial-\". Before the rename, validate the "+
			"temporary file with `jq -e .` or an equivalent strict JSON parser; "+
			"repair any validation failure, then atomically rename it to the "+
			"final path. Do not exit until the final path exists and contains the "+
			"validated object. For every coverage claim, copy requirement_id and "+
			"kind exactly from the supplied plan. Evidence must contain at least "+
			"one item for covered, partial, or not_applicable; use an empty array only for "+
			"not_covered. Use not_applicable only when the requirement does not describe changed product behavior, such as a test-fixture precondition or pre-existing behavior outside the patch; never use it for an acceptance criterion. Cite the relevant changed line and explain why it is not applicable. Use exactly these top-level fields: "+
			"{\"exact_sha\":$RAO_REVIEW_HEAD_SHA,"+
			"\"cycle_id\":$RAO_REVIEW_CYCLE_ID,\"cycle_revision\":"+
			"integer($RAO_REVIEW_REVISION),\"worker_id\":"+
			"$RAO_REVIEW_OWNER_ID,\"role\":$RAO_REVIEW_ROLE,"+
			"\"lane\":$RAO_REVIEW_LANE,\"phase\":%q,\"pass\":"+
			"integer($RAO_REVIEW_PASS),\"attempt\":"+
			"integer($RAO_REVIEW_ATTEMPT),\"checkout\":{"+
			"\"exact_sha\":$RAO_REVIEW_HEAD_SHA,\"clean\":true},"+
			"\"payload\":PAYLOAD}. Environment references describe string "+
			"values and must be emitted as JSON strings; integer(...) values "+
			"must be emitted as JSON integers. Verify HEAD and checkout cleanliness "+
			"before setting checkout.clean=true. Every non-empty evidence path must "+
			"still exist in the clean checkout when the final artifact is published. "+
			"Do not cite temporary files that you delete; cite the relevant tracked "+
			"source or test path, or leave the evidence path empty and summarize the "+
			"transient test result. The strict payload shape is %s. "+
			"Replace every descriptive type token with a real JSON value, keep "+
			"required arrays present even when empty, and do not add unknown fields.%s",
		phase,
		payload,
		payloadRequirements,
	), nil
}

func newReviewCycleID(headSHA string) (string, error) {
	token, err := randomReviewWorkerToken()
	if err != nil {
		return "", fmt.Errorf("failed to allocate review cycle identity: %w", err)
	}
	return fmt.Sprintf("review-cycle-%s-%s", abbreviateSHA(headSHA), token), nil
}

func randomReviewWorkerToken() (string, error) {
	body := make([]byte, reviewWorkerIdentityTokenBytes)
	if _, err := rand.Read(body); err != nil {
		return "", err
	}
	return hex.EncodeToString(body), nil
}

func reviewWorkerIdentityForCycle(
	cycle *ReviewCycleState,
	role AgentProfileRole,
	pass int,
	lane string,
) (ReviewWorkerIdentity, error) {
	if cycle == nil {
		return ReviewWorkerIdentity{}, errors.New("review worker cycle is missing")
	}
	identity := ReviewWorkerIdentity{
		CycleID:  strings.TrimSpace(cycle.ID),
		Revision: cycle.Revision,
		Role:     role,
		Pass:     pass,
		Lane:     strings.ToLower(strings.TrimSpace(lane)),
	}
	if err := validateReviewWorkerIdentity(identity); err != nil {
		return ReviewWorkerIdentity{}, err
	}
	return identity, nil
}

func validateReviewWorkerIdentity(identity ReviewWorkerIdentity) error {
	cycleID := strings.TrimSpace(identity.CycleID)
	if cycleID == "" || sanitizeSessionPart(cycleID) != cycleID {
		return errors.New("review worker cycle identity is missing or unsafe")
	}
	if identity.Revision <= 0 {
		return errors.New("review worker revision must be greater than zero")
	}
	switch identity.Role {
	case AgentProfileRoleDiscovery,
		AgentProfileRoleVerifier,
		AgentProfileRoleChallenge,
		AgentProfileRoleEscalation:
	default:
		return fmt.Errorf("review worker role %q is unsupported", identity.Role)
	}
	if identity.Pass <= 0 {
		return errors.New("review worker pass must be greater than zero")
	}
	lane := strings.TrimSpace(identity.Lane)
	if lane == "" || strings.ToLower(lane) != lane || sanitizeSessionPart(lane) != lane {
		return errors.New("review worker lane is missing or unsafe")
	}
	return nil
}

func sameReviewWorkerLogicalIdentity(left, right ReviewWorkerIdentity) bool {
	return left.CycleID == right.CycleID &&
		left.Revision == right.Revision &&
		left.Role == right.Role &&
		left.Pass == right.Pass &&
		left.Lane == right.Lane
}

func allocateReviewWorkerOwnership(
	identity ReviewWorkerIdentity,
	attempt int,
	worktreeDir string,
	token string,
	allocatedAt time.Time,
) (ReviewWorkerOwnership, error) {
	if err := validateReviewWorkerIdentity(identity); err != nil {
		return ReviewWorkerOwnership{}, err
	}
	if attempt <= 0 {
		return ReviewWorkerOwnership{}, errors.New("review worker attempt must be greater than zero")
	}
	if len(token) != reviewWorkerIdentityTokenBytes*2 {
		return ReviewWorkerOwnership{}, errors.New("review worker allocation token has an invalid length")
	}
	if _, err := hex.DecodeString(token); err != nil || strings.ToLower(token) != token {
		return ReviewWorkerOwnership{}, errors.New("review worker allocation token is not lowercase hexadecimal")
	}
	worktreeDir = filepath.Clean(strings.TrimSpace(worktreeDir))
	if worktreeDir == "." ||
		worktreeDir == string(filepath.Separator) ||
		!filepath.IsAbs(worktreeDir) {
		return ReviewWorkerOwnership{}, fmt.Errorf("review worker worktree directory %q is unsafe", worktreeDir)
	}
	if allocatedAt.IsZero() {
		return ReviewWorkerOwnership{}, errors.New("review worker allocation time is missing")
	}

	trace := fmt.Sprintf(
		"%s-r%d-%s-p%d-%s-a%d-%s",
		identity.CycleID,
		identity.Revision,
		identity.Role,
		identity.Pass,
		identity.Lane,
		attempt,
		token,
	)
	ownerID := boundedReviewWorkerResourceName("review-worker-"+trace, reviewWorkerOwnerNameLimit)
	if ownerID == "" {
		return ReviewWorkerOwnership{}, errors.New("failed to derive review worker owner identity")
	}
	sessionName := reviewWorkerTmuxSessionName(Agent{ID: ownerID})
	worktreeName := boundedReviewWorkerResourceName("review-"+trace, reviewWorkerWorktreeNameLimit)
	worktreePath := filepath.Join(worktreeDir, worktreeName)
	if !reviewWorkerPathWithin(worktreeDir, worktreePath) {
		return ReviewWorkerOwnership{}, errors.New("review worker worktree escaped the configured directory")
	}
	gitHubConfigPath, err := reviewWorkerGitHubConfigPath(worktreePath)
	if err != nil {
		return ReviewWorkerOwnership{}, err
	}
	launch, err := newDurableLaunchAttempt(
		ownerID,
		DurableLaunchReviewWorker,
		reviewWorkerLogicalAssignment(identity),
		attempt,
		ownerID,
		sessionName,
		allocatedAt,
	)
	if err != nil {
		return ReviewWorkerOwnership{}, err
	}
	return ReviewWorkerOwnership{
		DurableLaunchAttempt: launch,
		Identity:             identity,
		WorktreeName:         worktreeName,
		WorktreePath:         worktreePath,
		GitHubConfigPath:     gitHubConfigPath,
	}, nil
}

func boundedReviewWorkerResourceName(value string, limit int) string {
	safe := sanitizeSessionPart(value)
	if safe == "" || limit <= 0 {
		return ""
	}
	if len(safe) <= limit {
		return safe
	}
	sum := sha256.Sum256([]byte(safe))
	suffix := "-" + hex.EncodeToString(sum[:8])
	if limit <= len(suffix) {
		return suffix[len(suffix)-limit:]
	}
	return safe[:limit-len(suffix)] + suffix
}

func reviewWorkerPathWithin(basePath, targetPath string) bool {
	basePath, err := filepath.Abs(strings.TrimSpace(basePath))
	if err != nil {
		return false
	}
	targetPath, err = filepath.Abs(strings.TrimSpace(targetPath))
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(filepath.Clean(basePath), filepath.Clean(targetPath))
	if err != nil {
		return false
	}
	return relative == "." ||
		(relative != ".." &&
			!strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func reviewWorkerGitHubConfigPath(worktreePath string) (string, error) {
	worktreePath = filepath.Clean(strings.TrimSpace(worktreePath))
	if !filepath.IsAbs(worktreePath) ||
		worktreePath == string(filepath.Separator) {
		return "", errors.New("review worker worktree path is unsafe")
	}
	worktreeName := filepath.Base(worktreePath)
	if worktreeName == "" ||
		sanitizeSessionPart(worktreeName) != worktreeName {
		return "", errors.New("review worker worktree name is unsafe")
	}
	configName := boundedReviewWorkerResourceName(
		reviewWorkerGitConfigName+"-"+worktreeName,
		reviewWorkerWorktreeNameLimit,
	)
	if configName == "" {
		return "", errors.New("failed to derive review worker GitHub config path")
	}
	configPath := filepath.Join(filepath.Dir(worktreePath), configName)
	if configPath == worktreePath ||
		!reviewWorkerPathWithin(filepath.Dir(worktreePath), configPath) ||
		reviewWorkerPathWithin(worktreePath, configPath) {
		return "", errors.New("review worker GitHub config path is unsafe")
	}
	return configPath, nil
}

func reviewWorkerArtifactDirectory(worktreePath string) (string, error) {
	worktreePath = filepath.Clean(strings.TrimSpace(worktreePath))
	if !filepath.IsAbs(worktreePath) ||
		worktreePath == string(filepath.Separator) {
		return "", errors.New("review worker worktree path is unsafe")
	}
	worktreeName := filepath.Base(worktreePath)
	if worktreeName == "" ||
		sanitizeSessionPart(worktreeName) != worktreeName {
		return "", errors.New("review worker worktree name is unsafe")
	}
	artifactName := boundedReviewWorkerResourceName(
		reviewWorkerArtifactDirPrefix+"-"+worktreeName,
		reviewWorkerWorktreeNameLimit,
	)
	if artifactName == "" {
		return "", errors.New("failed to derive review worker artifact directory")
	}
	artifactPath := filepath.Join(filepath.Dir(worktreePath), artifactName)
	if artifactPath == worktreePath ||
		!reviewWorkerPathWithin(filepath.Dir(worktreePath), artifactPath) ||
		reviewWorkerPathWithin(worktreePath, artifactPath) {
		return "", errors.New("review worker artifact directory is unsafe")
	}
	return artifactPath, nil
}

func reviewWorkerArtifactOwnerMarkerPath(
	artifactDirectory string,
) (string, error) {
	artifactDirectory = filepath.Clean(strings.TrimSpace(artifactDirectory))
	if !filepath.IsAbs(artifactDirectory) ||
		artifactDirectory == string(filepath.Separator) {
		return "", errors.New("review worker artifact directory is unsafe")
	}
	markerPath := artifactDirectory + reviewWorkerArtifactOwnerSuffix
	if filepath.Dir(markerPath) != filepath.Dir(artifactDirectory) ||
		!reviewWorkerPathWithin(filepath.Dir(artifactDirectory), markerPath) {
		return "", errors.New("review worker artifact owner marker path is unsafe")
	}
	return markerPath, nil
}

func reviewWorkerArtifactOwnerMarkerBody(
	ownership ReviewWorkerOwnership,
) []byte {
	return []byte(reviewWorkerArtifactOwnerV1 + ownership.OwnerID + "\n")
}

func prepareReviewWorkerArtifactDirectory(
	ownership ReviewWorkerOwnership,
) (string, error) {
	if err := validateReviewWorkerOwnership(ownership); err != nil {
		return "", err
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		return "", err
	}
	markerPath, err := reviewWorkerArtifactOwnerMarkerPath(artifactDirectory)
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(artifactDirectory); err == nil {
		return "", errors.New("review worker artifact directory already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	// Persist ownership proof before Mkdir so a restart can safely clean the
	// directory even when the state-file claim checkpoint never completes.
	marker, err := os.OpenFile(
		markerPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return "", err
	}
	markerComplete := false
	defer func() {
		_ = marker.Close()
		if !markerComplete {
			_ = os.Remove(markerPath)
		}
	}()
	body := reviewWorkerArtifactOwnerMarkerBody(ownership)
	written, err := marker.Write(body)
	if err != nil {
		return "", err
	}
	if written != len(body) {
		return "", io.ErrShortWrite
	}
	if err := marker.Sync(); err != nil {
		return "", err
	}
	if err := marker.Close(); err != nil {
		return "", err
	}
	if err := syncReviewArtifactDirectory(
		filepath.Dir(artifactDirectory),
	); err != nil {
		return "", err
	}
	markerComplete = true

	if _, err := os.Lstat(artifactDirectory); err == nil {
		_ = os.Remove(markerPath)
		return "", errors.New("review worker artifact directory already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(markerPath)
		return "", err
	}
	if err := os.Mkdir(artifactDirectory, 0o700); err != nil {
		_ = os.Remove(markerPath)
		return "", err
	}
	if err := syncReviewArtifactDirectory(
		filepath.Dir(artifactDirectory),
	); err != nil {
		return "", err
	}
	return artifactDirectory, nil
}

func verifyReviewWorkerArtifactOwnerMarker(
	ownership ReviewWorkerOwnership,
) (bool, bool, error) {
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		return false, false, err
	}
	markerPath, err := reviewWorkerArtifactOwnerMarkerPath(artifactDirectory)
	if err != nil {
		return false, false, err
	}
	info, err := os.Lstat(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return true, false, errors.New(
			"review worker artifact owner marker is not a regular file",
		)
	}
	expected := reviewWorkerArtifactOwnerMarkerBody(ownership)
	if info.Size() != int64(len(expected)) {
		return true, false, nil
	}
	marker, err := os.Open(markerPath)
	if err != nil {
		return true, false, err
	}
	defer marker.Close()
	openedInfo, err := marker.Stat()
	if err != nil ||
		!openedInfo.Mode().IsRegular() ||
		!os.SameFile(info, openedInfo) {
		return true, false, errors.New(
			"review worker artifact owner marker changed while opening",
		)
	}
	body, err := io.ReadAll(io.LimitReader(
		marker,
		int64(len(expected))+1,
	))
	if err != nil {
		return true, false, err
	}
	return true, bytes.Equal(body, expected), nil
}

func validateReviewWorkerOwnership(ownership ReviewWorkerOwnership) error {
	if err := validateReviewWorkerIdentity(ownership.Identity); err != nil {
		return err
	}
	if err := validateDurableLaunchAttempt(
		ownership.DurableLaunchAttempt,
	); err != nil {
		return err
	}
	if ownership.Kind != DurableLaunchReviewWorker ||
		ownership.ID != ownership.OwnerID ||
		ownership.Scope != reviewWorkerLogicalAssignment(ownership.Identity) {
		return errors.New("review worker durable launch identity is invalid")
	}
	if ownership.Failure != nil &&
		(len(ownership.Failure.Detail) >
			reviewWorkerWorktreeSetupDiagnosticBytes ||
			!utf8.ValidString(ownership.Failure.Detail)) {
		return errors.New("review worker failure diagnostic is invalid")
	}
	if strings.TrimSpace(ownership.OwnerID) == "" ||
		sanitizeSessionPart(ownership.OwnerID) != ownership.OwnerID ||
		len(ownership.OwnerID) > reviewWorkerOwnerNameLimit {
		return errors.New("review worker owner identity is missing or unsafe")
	}
	if len(ownership.SessionName) > 64 ||
		ownership.SessionName != reviewWorkerTmuxSessionName(
			Agent{ID: ownership.OwnerID},
		) {
		return errors.New("review worker session name does not match its owner identity")
	}
	worktreePath := filepath.Clean(strings.TrimSpace(ownership.WorktreePath))
	if strings.TrimSpace(ownership.WorktreeName) == "" ||
		sanitizeSessionPart(ownership.WorktreeName) != ownership.WorktreeName ||
		len(ownership.WorktreeName) > reviewWorkerWorktreeNameLimit ||
		!filepath.IsAbs(worktreePath) ||
		worktreePath == string(filepath.Separator) ||
		filepath.Base(ownership.WorktreePath) != ownership.WorktreeName {
		return errors.New("review worker worktree does not match its owner identity")
	}
	gitHubConfigPath, err := reviewWorkerGitHubConfigPath(worktreePath)
	if err != nil ||
		filepath.Clean(strings.TrimSpace(ownership.GitHubConfigPath)) !=
			gitHubConfigPath {
		return errors.New("review worker GitHub config does not match its owned worktree")
	}
	return nil
}

func reviewWorkerRoutingValues(profile AgentProfile) (
	string,
	string,
	string,
) {
	model := strings.TrimSpace(profile.Model)
	effort := normalizeReasoningEffort(profile.ReasoningEffort)
	if profile.InheritGlobal {
		model = inheritedGlobalPolicyValue
		effort = inheritedGlobalPolicyValue
	}
	return strings.TrimSpace(profile.Name), model, effort
}

func populateReviewWorkerRouting(
	cycle *ReviewCycleState,
	ownership *ReviewWorkerOwnership,
) error {
	if cycle == nil || ownership == nil {
		return errors.New("review worker routing cycle and ownership are required")
	}
	var profile AgentProfile
	var err error
	profile, err = effectiveProfileForReviewLaunch(
		cycle,
		ownership.Identity,
	)
	if err != nil {
		return err
	}
	ownership.Profile,
		ownership.Model,
		ownership.ReasoningEffort =
		reviewWorkerRoutingValues(profile)
	return nil
}

func validateReviewWorkerRouting(
	cycle *ReviewCycleState,
	ownership ReviewWorkerOwnership,
) error {
	expected := ownership
	routingCycle := cycle
	if cycle != nil && cycle.EscalationTransition != nil &&
		ownership.AllocatedAt.Before(
			cycle.EscalationTransition.TransitionedAt,
		) {
		// An escalation changes routing for subsequent verifier and challenge
		// launches. Historical ownership remains bound to the profile that was
		// effective when it was allocated.
		historical := *cycle
		historical.EscalationTransition = nil
		routingCycle = &historical
	}
	if err := populateReviewWorkerRouting(routingCycle, &expected); err != nil {
		return fmt.Errorf("review worker routing is invalid: %w", err)
	}
	if strings.TrimSpace(ownership.Profile) == "" ||
		ownership.Profile != expected.Profile ||
		ownership.Model != expected.Model ||
		ownership.ReasoningEffort != expected.ReasoningEffort {
		return errors.New(
			"review worker routing does not match the snapshotted policy",
		)
	}
	return nil
}

func validateReviewCycleWorkerOwnerships(cycle *ReviewCycleState) error {
	if cycle == nil {
		return errors.New("persisted review cycle policy snapshot is missing")
	}
	if strings.TrimSpace(cycle.ID) == "" {
		if cycle.Revision != 0 || len(cycle.WorkerOwnerships) != 0 {
			return errors.New("persisted review cycle worker ownership has no cycle identity")
		}
		return nil
	}
	if sanitizeSessionPart(cycle.ID) != cycle.ID {
		return errors.New("persisted review cycle identity is unsafe")
	}
	if cycle.Revision <= 0 {
		return errors.New("persisted review cycle revision must be greater than zero")
	}

	owners := make(map[string]struct{}, len(cycle.WorkerOwnerships))
	sessions := make(map[string]struct{}, len(cycle.WorkerOwnerships))
	localPaths := make(map[string]struct{}, len(cycle.WorkerOwnerships)*2)
	attempts := make(map[string]struct{}, len(cycle.WorkerOwnerships))
	for _, ownership := range cycle.WorkerOwnerships {
		if err := validateReviewWorkerOwnership(ownership); err != nil {
			return fmt.Errorf("persisted review worker ownership is invalid: %w", err)
		}
		if err := validateReviewWorkerRouting(cycle, ownership); err != nil {
			return fmt.Errorf(
				"persisted review worker ownership is invalid: %w",
				err,
			)
		}
		if ownership.Identity.CycleID != cycle.ID {
			return errors.New("persisted review worker ownership belongs to another cycle")
		}
		if ownership.Identity.Revision > cycle.Revision {
			return errors.New("persisted review worker ownership belongs to a future revision")
		}
		attemptKey := fmt.Sprintf(
			"%s\x00%d\x00%s\x00%d\x00%s\x00%d",
			ownership.Identity.CycleID,
			ownership.Identity.Revision,
			ownership.Identity.Role,
			ownership.Identity.Pass,
			ownership.Identity.Lane,
			ownership.Attempt,
		)
		if _, duplicate := attempts[attemptKey]; duplicate {
			return errors.New("persisted review worker logical attempt is duplicated")
		}
		attempts[attemptKey] = struct{}{}
		for label, value := range map[string]string{
			"owner identity": ownership.OwnerID,
			"session":        ownership.SessionName,
		} {
			var seen map[string]struct{}
			switch label {
			case "owner identity":
				seen = owners
			default:
				seen = sessions
			}
			if _, duplicate := seen[value]; duplicate {
				return fmt.Errorf("persisted review worker %s %q is duplicated", label, value)
			}
			seen[value] = struct{}{}
		}
		artifactDirectory, err := reviewWorkerArtifactDirectory(
			ownership.WorktreePath,
		)
		if err != nil {
			return fmt.Errorf(
				"persisted review worker artifact directory is invalid: %w",
				err,
			)
		}
		for label, path := range map[string]string{
			"worktree":      ownership.WorktreePath,
			"GitHub config": ownership.GitHubConfigPath,
			"artifact":      artifactDirectory,
		} {
			path = filepath.Clean(path)
			if _, duplicate := localPaths[path]; duplicate {
				return fmt.Errorf("persisted review worker %s path %q is duplicated", label, path)
			}
			localPaths[path] = struct{}{}
		}
	}
	return nil
}

func (m *AgentManager) allocateReviewWorkerOwnership(
	reviewerID string,
	identity ReviewWorkerIdentity,
	worktreeDir string,
	token string,
	allocatedAt time.Time,
) (
	ReviewWorkerOwnership,
	reviewWorkerOwnershipMutation,
	error,
) {
	if m == nil {
		return ReviewWorkerOwnership{},
			reviewWorkerOwnershipMutation{},
			errors.New("agent manager is not configured")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer {
		return ReviewWorkerOwnership{},
			reviewWorkerOwnershipMutation{},
			fmt.Errorf(
				"review coordinator %q was not found",
				strings.TrimSpace(reviewerID),
			)
	}
	if agentLifecycleTerminal(reviewer) {
		return ReviewWorkerOwnership{},
			reviewWorkerOwnershipMutation{},
			fmt.Errorf(
				"review coordinator %q is stopped or terminal",
				reviewer.ID,
			)
	}
	if reviewer.ReviewCycle == nil {
		return ReviewWorkerOwnership{},
			reviewWorkerOwnershipMutation{},
			fmt.Errorf(
				"review coordinator %q has no review cycle",
				reviewer.ID,
			)
	}
	if reviewer.ReviewCycle.Stale {
		return ReviewWorkerOwnership{},
			reviewWorkerOwnershipMutation{},
			errReviewCycleStale
	}
	if identity.CycleID != reviewer.ReviewCycle.ID ||
		identity.Revision != reviewer.ReviewCycle.Revision {
		return ReviewWorkerOwnership{},
			reviewWorkerOwnershipMutation{},
			errors.New(
				"review worker identity does not match the active cycle revision",
			)
	}

	attempt := 1
	for _, existing := range reviewer.ReviewCycle.WorkerOwnerships {
		if sameReviewWorkerLogicalIdentity(existing.Identity, identity) &&
			existing.Attempt >= attempt {
			attempt = existing.Attempt + 1
		}
	}
	ownership, err := allocateReviewWorkerOwnership(
		identity,
		attempt,
		worktreeDir,
		token,
		allocatedAt,
	)
	if err != nil {
		return ReviewWorkerOwnership{}, reviewWorkerOwnershipMutation{}, err
	}
	if err := populateReviewWorkerRouting(
		reviewer.ReviewCycle,
		&ownership,
	); err != nil {
		return ReviewWorkerOwnership{},
			reviewWorkerOwnershipMutation{},
			fmt.Errorf(
				"failed to resolve review worker routing: %w",
				err,
			)
	}
	ownershipArtifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		return ReviewWorkerOwnership{}, reviewWorkerOwnershipMutation{}, err
	}
	ownershipPaths := []string{
		filepath.Clean(ownership.WorktreePath),
		filepath.Clean(ownership.GitHubConfigPath),
		filepath.Clean(ownershipArtifactDirectory),
	}
	for _, existing := range reviewer.ReviewCycle.WorkerOwnerships {
		existingArtifactDirectory, err := reviewWorkerArtifactDirectory(
			existing.WorktreePath,
		)
		if err != nil {
			return ReviewWorkerOwnership{},
				reviewWorkerOwnershipMutation{},
				errors.New(
					"existing review worker artifact ownership is invalid",
				)
		}
		if existing.OwnerID == ownership.OwnerID ||
			existing.SessionName == ownership.SessionName {
			return ReviewWorkerOwnership{},
				reviewWorkerOwnershipMutation{},
				errors.New(
					"review worker allocation collided with an existing owner",
				)
		}
		existingPaths := map[string]struct{}{
			filepath.Clean(existing.WorktreePath):     {},
			filepath.Clean(existing.GitHubConfigPath): {},
			filepath.Clean(existingArtifactDirectory): {},
		}
		for _, path := range ownershipPaths {
			if _, collision := existingPaths[path]; collision {
				return ReviewWorkerOwnership{},
					reviewWorkerOwnershipMutation{},
					errors.New(
						"review worker allocation collided with an existing owner",
					)
			}
		}
	}
	previousLaneCompletions := append(
		[]ReviewLaneCompletion(nil),
		reviewer.ReviewCycle.LaneCompletions...,
	)
	previousLastActivityTime := reviewer.LastActivityTime
	reviewer.ReviewCycle.WorkerOwnerships = append(
		reviewer.ReviewCycle.WorkerOwnerships,
		ownership,
	)
	reviewer.ReviewCycle.LaneCompletions =
		removeReviewLaneCompletionsForIdentity(
			reviewer.ReviewCycle,
			identity,
		)
	reservationLastActivityAt := time.Now().UTC()
	reviewer.LastActivityTime = reservationLastActivityAt
	return ownership, reviewWorkerOwnershipMutation{
		reviewerID:                reviewer.ID,
		ownership:                 ownership,
		previousLaneCompletions:   previousLaneCompletions,
		previousLastActivityTime:  previousLastActivityTime,
		reservationLastActivityAt: reservationLastActivityAt,
	}, nil
}

func (m *AgentManager) rollbackReviewWorkerOwnership(
	mutation reviewWorkerOwnershipMutation,
) bool {
	if m == nil || strings.TrimSpace(mutation.reviewerID) == "" ||
		strings.TrimSpace(mutation.ownership.OwnerID) == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	reviewer, ok := m.agents[mutation.reviewerID]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return false
	}
	ownerships := reviewer.ReviewCycle.WorkerOwnerships
	removed := false
	for index := len(ownerships) - 1; index >= 0; index-- {
		if ownerships[index].OwnerID != mutation.ownership.OwnerID ||
			ownerships[index].Attempt != mutation.ownership.Attempt {
			continue
		}
		reviewer.ReviewCycle.WorkerOwnerships = append(
			ownerships[:index],
			ownerships[index+1:]...,
		)
		removed = true
		break
	}
	if !removed {
		return false
	}
	reviewer.ReviewCycle.LaneCompletions = append(
		[]ReviewLaneCompletion(nil),
		mutation.previousLaneCompletions...,
	)
	if reviewer.LastActivityTime.Equal(
		mutation.reservationLastActivityAt,
	) {
		reviewer.LastActivityTime = mutation.previousLastActivityTime
	}
	return true
}

func (m *AgentManager) transitionReviewWorkerLifecycle(
	reviewerID string,
	ownerID string,
	next ReviewWorkerLifecycleState,
	failure *DurableLaunchFailure,
	observedAt time.Time,
) (reviewWorkerLifecycleMutation, ReviewWorkerOwnership, error) {
	if m == nil {
		return reviewWorkerLifecycleMutation{}, ReviewWorkerOwnership{},
			errors.New("agent manager is not configured")
	}
	if observedAt.IsZero() {
		return reviewWorkerLifecycleMutation{}, ReviewWorkerOwnership{},
			errors.New("review worker lifecycle observation time is missing")
	}
	observedAt = observedAt.UTC()
	m.mu.Lock()
	defer m.mu.Unlock()

	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return reviewWorkerLifecycleMutation{}, ReviewWorkerOwnership{},
			fmt.Errorf(
				"review coordinator %q was not found",
				strings.TrimSpace(reviewerID),
			)
	}
	for index := range reviewer.ReviewCycle.WorkerOwnerships {
		ownership := &reviewer.ReviewCycle.WorkerOwnerships[index]
		if ownership.OwnerID != strings.TrimSpace(ownerID) {
			continue
		}
		if ownership.Lifecycle == next ||
			durableLaunchTerminal(ownership.Lifecycle) {
			return reviewWorkerLifecycleMutation{},
				*ownership,
				nil
		}
		mutation := reviewWorkerLifecycleMutation{
			reviewerID:               reviewer.ID,
			ownerID:                  ownership.OwnerID,
			previous:                 *ownership,
			previousLastActivityTime: reviewer.LastActivityTime,
			updatedAt:                observedAt,
			changed:                  true,
		}
		changed, err := transitionDurableLaunchAttempt(
			&ownership.DurableLaunchAttempt,
			next,
			failure,
			time.Time{},
			observedAt,
		)
		if err != nil {
			return reviewWorkerLifecycleMutation{},
				ReviewWorkerOwnership{},
				err
		}
		if !changed {
			return reviewWorkerLifecycleMutation{}, *ownership, nil
		}
		if err := validateReviewWorkerOwnership(*ownership); err != nil {
			*ownership = mutation.previous
			return reviewWorkerLifecycleMutation{},
				ReviewWorkerOwnership{},
				err
		}
		reviewer.LastActivityTime = observedAt.UTC()
		return mutation, *ownership, nil
	}
	return reviewWorkerLifecycleMutation{}, ReviewWorkerOwnership{},
		fmt.Errorf("review worker %q was not found", strings.TrimSpace(ownerID))
}

func (m *AgentManager) rollbackReviewWorkerLifecycle(
	mutation reviewWorkerLifecycleMutation,
) bool {
	if m == nil || !mutation.changed {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	reviewer, ok := m.agents[mutation.reviewerID]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return false
	}
	for index := range reviewer.ReviewCycle.WorkerOwnerships {
		ownership := &reviewer.ReviewCycle.WorkerOwnerships[index]
		if ownership.OwnerID != mutation.ownerID {
			continue
		}
		*ownership = mutation.previous
		if reviewer.LastActivityTime.Equal(mutation.updatedAt) {
			reviewer.LastActivityTime =
				mutation.previousLastActivityTime
		}
		return true
	}
	return false
}

func (m *AgentManager) markReviewWorkerArtifactDirectoryClaimed(
	reviewerID string,
	ownerID string,
) bool {
	_, ok := m.claimReviewWorkerArtifactDirectory(reviewerID, ownerID)
	return ok
}

func (m *AgentManager) claimReviewWorkerArtifactDirectory(
	reviewerID string,
	ownerID string,
) (reviewWorkerArtifactClaimMutation, bool) {
	if m == nil {
		return reviewWorkerArtifactClaimMutation{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	reviewer, ok := m.agents[strings.TrimSpace(reviewerID)]
	if !ok || reviewer == nil || reviewer.Role != RoleReviewer ||
		reviewer.ReviewCycle == nil || reviewer.ReviewCycle.Stale ||
		agentLifecycleTerminal(reviewer) {
		return reviewWorkerArtifactClaimMutation{}, false
	}
	for index := range reviewer.ReviewCycle.WorkerOwnerships {
		ownership := &reviewer.ReviewCycle.WorkerOwnerships[index]
		if ownership.OwnerID != strings.TrimSpace(ownerID) {
			continue
		}
		previousLastActivityTime := reviewer.LastActivityTime
		previousClaimed := ownership.ArtifactDirectoryClaimed
		ownership.ArtifactDirectoryClaimed = true
		claimLastActivityAt := time.Now().UTC()
		reviewer.LastActivityTime = claimLastActivityAt
		return reviewWorkerArtifactClaimMutation{
			reviewerID:               reviewer.ID,
			ownerID:                  ownership.OwnerID,
			previousClaimed:          previousClaimed,
			previousLastActivityTime: previousLastActivityTime,
			claimLastActivityAt:      claimLastActivityAt,
		}, true
	}
	return reviewWorkerArtifactClaimMutation{}, false
}

func (m *AgentManager) rollbackReviewWorkerArtifactDirectoryClaim(
	mutation reviewWorkerArtifactClaimMutation,
) bool {
	if m == nil || strings.TrimSpace(mutation.reviewerID) == "" ||
		strings.TrimSpace(mutation.ownerID) == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	reviewer, ok := m.agents[mutation.reviewerID]
	if !ok || reviewer == nil || reviewer.ReviewCycle == nil {
		return false
	}
	for index := range reviewer.ReviewCycle.WorkerOwnerships {
		ownership := &reviewer.ReviewCycle.WorkerOwnerships[index]
		if ownership.OwnerID != mutation.ownerID {
			continue
		}
		ownership.ArtifactDirectoryClaimed = mutation.previousClaimed
		if reviewer.LastActivityTime.Equal(mutation.claimLastActivityAt) {
			reviewer.LastActivityTime = mutation.previousLastActivityTime
		}
		return true
	}
	return false
}

func (m *AgentManager) findReviewWorkerOwnership(
	ownerID string,
) (Agent, ReviewWorkerOwnership, bool) {
	if m == nil {
		return Agent{}, ReviewWorkerOwnership{}, false
	}
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return Agent{}, ReviewWorkerOwnership{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, reviewer := range m.agents {
		if reviewer == nil || reviewer.ReviewCycle == nil {
			continue
		}
		for _, ownership := range reviewer.ReviewCycle.WorkerOwnerships {
			if ownership.OwnerID == ownerID {
				return cloneAgent(reviewer), ownership, true
			}
		}
	}
	return Agent{}, ReviewWorkerOwnership{}, false
}

func (m *AgentManager) lockActiveReviewCoordinatorForWorkerLaunch(
	reviewerID string,
) (Agent, func(), error) {
	if m == nil {
		return Agent{}, nil, errors.New("agent manager is not configured")
	}
	reviewerID = strings.TrimSpace(reviewerID)
	m.mu.Lock()
	lifecycle, ok := m.reviewCoordinatorLifecycleLocked(reviewerID)
	m.mu.Unlock()
	if !ok {
		return Agent{}, nil, fmt.Errorf(
			"review coordinator %q was not found",
			reviewerID,
		)
	}

	lifecycle.RLock()
	m.mu.Lock()
	reviewer, ok := m.agents[reviewerID]
	var err error
	switch {
	case !ok || reviewer == nil || reviewer.Role != RoleReviewer:
		err = fmt.Errorf("review coordinator %q was not found", reviewerID)
	case reviewer.ReviewCycle == nil:
		err = fmt.Errorf("review coordinator %q has no review cycle", reviewerID)
	case reviewer.ReviewCycle.Stale:
		err = errReviewCycleStale
	case agentLifecycleTerminal(reviewer):
		err = fmt.Errorf(
			"review coordinator %q is stopped or terminal",
			reviewerID,
		)
	case reviewer.State != StateWorking || reviewer.Paused:
		err = fmt.Errorf(
			"review coordinator %q is not active for worker launch",
			reviewerID,
		)
	}
	if err != nil {
		m.mu.Unlock()
		lifecycle.RUnlock()
		return Agent{}, nil, err
	}
	snapshot := cloneAgent(reviewer)
	m.mu.Unlock()
	return snapshot, lifecycle.RUnlock, nil
}

func (b *Orchestrator) reserveReviewWorkerOwnership(
	reviewerID string,
	identity ReviewWorkerIdentity,
) (ReviewWorkerOwnership, error) {
	if b == nil || b.agents == nil {
		return ReviewWorkerOwnership{}, errors.New("orchestrator agent manager is not configured")
	}
	if _, err := b.ensureReviewCycleHeadCurrent(
		context.Background(),
		reviewerID,
	); err != nil {
		return ReviewWorkerOwnership{}, err
	}
	reviewer, unlockLifecycle, err :=
		b.agents.lockActiveReviewCoordinatorForWorkerLaunch(reviewerID)
	if err != nil {
		return ReviewWorkerOwnership{}, err
	}
	defer unlockLifecycle()
	return b.reserveReviewWorkerOwnershipForActiveCoordinator(
		reviewer.ID,
		identity,
	)
}

func (b *Orchestrator) reserveReviewWorkerOwnershipForActiveCoordinator(
	reviewerID string,
	identity ReviewWorkerIdentity,
) (ReviewWorkerOwnership, error) {
	token, err := randomReviewWorkerToken()
	if err != nil {
		return ReviewWorkerOwnership{}, fmt.Errorf("failed to allocate review worker identity: %w", err)
	}
	b.reviewArtifactMu.Lock()
	defer b.reviewArtifactMu.Unlock()
	b.statePersistenceMu.Lock()
	defer b.statePersistenceMu.Unlock()
	evaluation, limitMutation, err :=
		b.agents.evaluateReviewLaunchLimits(
			reviewerID,
			time.Now().UTC(),
			true,
		)
	if err != nil {
		return ReviewWorkerOwnership{}, err
	}
	if evaluation.Limit != nil {
		if limitMutation.changed {
			if err := b.persistAgentStateLocked(); err != nil {
				if !b.agents.rollbackReviewLaunchLimitMutation(
					limitMutation,
				) {
					return ReviewWorkerOwnership{}, fmt.Errorf(
						"failed to persist terminal review limit transition and failed to roll it back: %w",
						err,
					)
				}
				return ReviewWorkerOwnership{}, fmt.Errorf(
					"failed to persist terminal review limit transition: %w",
					err,
				)
			}
		}
		return ReviewWorkerOwnership{}, &reviewLimitReachedError{
			transition: *evaluation.Limit,
		}
	}
	ownership, mutation, err :=
		b.agents.allocateReviewWorkerOwnership(
			reviewerID,
			identity,
			b.cfg.WorktreeDir,
			token,
			time.Now().UTC(),
		)
	if err != nil {
		if limitMutation.changed {
			_ = b.agents.rollbackReviewLaunchLimitMutation(
				limitMutation,
			)
		}
		return ReviewWorkerOwnership{}, err
	}
	if err := b.persistAgentStateLocked(); err != nil {
		ownershipRolledBack :=
			b.agents.rollbackReviewWorkerOwnership(mutation)
		limitRolledBack := !limitMutation.changed ||
			b.agents.rollbackReviewLaunchLimitMutation(limitMutation)
		if !ownershipRolledBack || !limitRolledBack {
			return ReviewWorkerOwnership{}, fmt.Errorf(
				"failed to persist review worker owner %s before resource creation and failed to roll back its reservation: %w",
				ownership.OwnerID,
				err,
			)
		}
		return ReviewWorkerOwnership{}, fmt.Errorf(
			"failed to persist review worker owner %s before resource creation: %w",
			ownership.OwnerID,
			err,
		)
	}
	return ownership, nil
}

func (b *Orchestrator) transitionAndPersistReviewWorkerLifecycle(
	reviewerID string,
	ownerID string,
	next ReviewWorkerLifecycleState,
	failure *DurableLaunchFailure,
) (ReviewWorkerOwnership, error) {
	if b == nil || b.agents == nil {
		return ReviewWorkerOwnership{},
			errors.New("orchestrator agent manager is not configured")
	}
	b.statePersistenceMu.Lock()
	mutation, ownership, err :=
		b.agents.transitionReviewWorkerLifecycle(
			reviewerID,
			ownerID,
			next,
			failure,
			time.Now().UTC(),
		)
	if err != nil {
		b.statePersistenceMu.Unlock()
		return ReviewWorkerOwnership{}, err
	}
	if mutation.changed {
		if err := b.persistAgentStateLocked(); err != nil {
			if !b.agents.rollbackReviewWorkerLifecycle(mutation) {
				b.statePersistenceMu.Unlock()
				return ReviewWorkerOwnership{}, fmt.Errorf(
					"failed to persist review worker lifecycle and failed to roll it back: %w",
					err,
				)
			}
			b.statePersistenceMu.Unlock()
			return ReviewWorkerOwnership{}, fmt.Errorf(
				"failed to persist review worker lifecycle: %w",
				err,
			)
		}
	}
	b.statePersistenceMu.Unlock()
	return ownership, nil
}

func cleanupReviewWorkerArtifactDirectory(
	worktreeDir string,
	ownership ReviewWorkerOwnership,
) error {
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		return err
	}
	markerPath, err := reviewWorkerArtifactOwnerMarkerPath(
		artifactDirectory,
	)
	if err != nil {
		return err
	}
	if !reviewWorkerPathWithin(worktreeDir, artifactDirectory) ||
		!reviewWorkerPathWithin(worktreeDir, markerPath) {
		return fmt.Errorf(
			"review worker %q artifact cleanup path escaped the configured worktree directory",
			ownership.OwnerID,
		)
	}
	markerExists, markerMatches, markerErr :=
		verifyReviewWorkerArtifactOwnerMarker(ownership)
	// A matching durable marker is the only cleanup authority for an outbox
	// whose claimed bit did not reach the state checkpoint.
	if !ownership.ArtifactDirectoryClaimed && !markerMatches {
		return nil
	}
	if err := os.RemoveAll(artifactDirectory); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf(
			"failed to remove review worker artifact directory %q: %w",
			artifactDirectory,
			err,
		)
	}
	if markerMatches {
		if err := os.Remove(markerPath); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(
				"failed to remove review worker artifact owner marker %q: %w",
				markerPath,
				err,
			)
		}
		if err := syncReviewArtifactDirectory(
			filepath.Dir(artifactDirectory),
		); err != nil {
			return fmt.Errorf(
				"failed to sync review worker artifact cleanup: %w",
				err,
			)
		}
	}
	if ownership.ArtifactDirectoryClaimed && markerExists &&
		!markerMatches {
		if markerErr != nil {
			return fmt.Errorf(
				"review worker %q artifact owner marker could not be verified: %w",
				ownership.OwnerID,
				markerErr,
			)
		}
		return fmt.Errorf(
			"review worker %q artifact owner marker does not match its persisted owner",
			ownership.OwnerID,
		)
	}
	return nil
}

func (b *Orchestrator) persistReviewCoordinatorTerminalState(
	reviewerID string,
	state AgentState,
) error {
	if b == nil || b.agents == nil {
		return errors.New("orchestrator agent manager is not configured")
	}
	if !b.agents.setStateWithoutLifecycleGuard(reviewerID, state, true) {
		return fmt.Errorf(
			"review coordinator %q was not found during terminalization",
			reviewerID,
		)
	}
	if err := b.persistAgentState(); err != nil {
		return fmt.Errorf(
			"failed to persist terminal review coordinator %q: %w",
			reviewerID,
			err,
		)
	}
	return nil
}

func (b *Orchestrator) terminalizeReviewCoordinatorAfterWorkerCleanup(
	reviewerID string,
	state AgentState,
) (Agent, error) {
	result, err := b.transitionReviewCoordinatorLifecycle(
		context.Background(),
		reviewerID,
		reviewCoordinatorLifecycleRequest{
			Intent:                     ReviewCoordinatorLifecycleCleanup,
			FinalState:                 state,
			ReleaseCoordinatorWorktree: true,
			RuntimeStopAcknowledged:    true,
		},
	)
	return result.Agent, err
}

func reviewWorkerEnvironment(
	ambient []string,
	repoOwner string,
	repoName string,
	prNumber int,
	headSHA string,
	ownership ReviewWorkerOwnership,
) ([]string, error) {
	if err := validateReviewWorkerOwnership(ownership); err != nil {
		return nil, err
	}
	headSHA = strings.ToLower(strings.TrimSpace(headSHA))
	if err := validateCanonicalGitObjectID(headSHA); err != nil {
		return nil, fmt.Errorf("review worker exact head SHA is invalid: %w", err)
	}
	repoOwner = strings.TrimSpace(repoOwner)
	repoName = strings.TrimSpace(repoName)
	if !safeReviewWorkerEnvironmentValue(repoOwner) ||
		!safeReviewWorkerEnvironmentValue(repoName) ||
		repoOwner == "" ||
		repoName == "" {
		return nil, errors.New("review worker repository metadata is missing or unsafe")
	}

	values := make(map[string]string)
	for _, entry := range ambient {
		key, value, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		if _, ok := reviewWorkerAmbientEnvironmentAllowlist[key]; !ok {
			continue
		}
		if !safeReviewWorkerEnvironmentValue(value) {
			return nil, fmt.Errorf("review worker allowlisted environment variable %s is unsafe", key)
		}
		values[key] = value
	}
	if strings.TrimSpace(values["PATH"]) == "" {
		return nil, errors.New("review worker isolation requires an allowlisted PATH")
	}
	if strings.TrimSpace(values["HOME"]) == "" {
		return nil, errors.New("review worker isolation requires an allowlisted HOME")
	}

	values["GH_CONFIG_DIR"] = ownership.GitHubConfigPath
	values["GIT_ASKPASS"] = ""
	values["GIT_CONFIG_GLOBAL"] = os.DevNull
	values["GIT_CONFIG_NOSYSTEM"] = "1"
	values["GIT_TERMINAL_PROMPT"] = "0"
	values["SSH_ASKPASS"] = ""
	values[reviewWorkerEnvRepo] = repoOwner + "/" + repoName
	values[reviewWorkerEnvRepoOwner] = repoOwner
	values[reviewWorkerEnvRepoName] = repoName
	prNumberValue, err := reviewWorkerPRNumberEnvironmentValue(prNumber)
	if err != nil {
		return nil, err
	}
	values[reviewWorkerEnvPRNumber] = prNumberValue
	values[reviewWorkerEnvHeadSHA] = headSHA
	values[reviewWorkerEnvCycleID] = ownership.Identity.CycleID
	values[reviewWorkerEnvRevision] = strconv.Itoa(ownership.Identity.Revision)
	values[reviewWorkerEnvRole] = string(ownership.Identity.Role)
	values[reviewWorkerEnvPass] = strconv.Itoa(ownership.Identity.Pass)
	values[reviewWorkerEnvLane] = ownership.Identity.Lane
	values[reviewWorkerEnvAttempt] = strconv.Itoa(ownership.Attempt)
	values[reviewWorkerEnvOwnerID] = ownership.OwnerID
	values[reviewWorkerEnvWorktree] = ownership.WorktreePath
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		ownership.WorktreePath,
	)
	if err != nil {
		return nil, err
	}
	values[reviewWorkerEnvArtifactDir] = artifactDirectory

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	environment := make([]string, 0, len(keys))
	for _, key := range keys {
		environment = append(environment, key+"="+values[key])
	}
	if err := validateReviewWorkerEnvironment(environment); err != nil {
		return nil, err
	}
	return environment, nil
}

func validateReviewWorkerEnvironment(environment []string) error {
	if len(environment) == 0 {
		return errors.New("review worker environment is empty")
	}
	required := map[string]struct{}{
		"HOME":                     {},
		"PATH":                     {},
		"GH_CONFIG_DIR":            {},
		"GIT_CONFIG_GLOBAL":        {},
		"GIT_CONFIG_NOSYSTEM":      {},
		"GIT_TERMINAL_PROMPT":      {},
		"GIT_ASKPASS":              {},
		"SSH_ASKPASS":              {},
		reviewWorkerEnvRepo:        {},
		reviewWorkerEnvRepoOwner:   {},
		reviewWorkerEnvRepoName:    {},
		reviewWorkerEnvPRNumber:    {},
		reviewWorkerEnvHeadSHA:     {},
		reviewWorkerEnvCycleID:     {},
		reviewWorkerEnvRevision:    {},
		reviewWorkerEnvRole:        {},
		reviewWorkerEnvPass:        {},
		reviewWorkerEnvLane:        {},
		reviewWorkerEnvAttempt:     {},
		reviewWorkerEnvOwnerID:     {},
		reviewWorkerEnvWorktree:    {},
		reviewWorkerEnvArtifactDir: {},
	}
	seen := make(map[string]struct{}, len(environment))
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, found := strings.Cut(entry, "=")
		if !found || key == "" {
			return errors.New("review worker environment contains a malformed entry")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("review worker environment contains duplicate variable %s", key)
		}
		seen[key] = struct{}{}
		values[key] = value
		if _, ok := reviewWorkerAmbientEnvironmentAllowlist[key]; !ok {
			if _, ok := reviewWorkerFixedEnvironmentAllowlist[key]; !ok {
				if _, ok := reviewWorkerContextEnvironmentAllowlist[key]; !ok {
					return fmt.Errorf("review worker environment variable %s is not allowlisted", key)
				}
			}
		}
		if !safeReviewWorkerEnvironmentValue(value) {
			return fmt.Errorf("review worker environment variable %s is unsafe", key)
		}
	}
	for key := range required {
		if _, ok := seen[key]; !ok {
			return fmt.Errorf("review worker environment is missing required variable %s", key)
		}
	}
	if values["GIT_ASKPASS"] != "" ||
		values["SSH_ASKPASS"] != "" ||
		values["GIT_CONFIG_GLOBAL"] != os.DevNull ||
		values["GIT_CONFIG_NOSYSTEM"] != "1" ||
		values["GIT_TERMINAL_PROMPT"] != "0" {
		return errors.New("review worker environment does not disable interactive credentials")
	}
	gitHubConfigPath, err := reviewWorkerGitHubConfigPath(
		values[reviewWorkerEnvWorktree],
	)
	if err != nil ||
		filepath.Clean(values["GH_CONFIG_DIR"]) != gitHubConfigPath {
		return errors.New("review worker GitHub config is not an owned checkout-independent path")
	}
	artifactDirectory, err := reviewWorkerArtifactDirectory(
		values[reviewWorkerEnvWorktree],
	)
	if err != nil ||
		filepath.Clean(values[reviewWorkerEnvArtifactDir]) != artifactDirectory {
		return errors.New(
			"review worker artifact directory is not an owned checkout-independent path",
		)
	}
	if err := validateCanonicalGitObjectID(values[reviewWorkerEnvHeadSHA]); err != nil {
		return fmt.Errorf("review worker environment exact head SHA is invalid: %w", err)
	}
	for _, key := range []string{
		reviewWorkerEnvRevision,
		reviewWorkerEnvPass,
		reviewWorkerEnvAttempt,
	} {
		value, err := strconv.Atoi(values[key])
		if err != nil || value <= 0 {
			return fmt.Errorf("review worker environment variable %s must be a positive integer", key)
		}
	}
	identity := ReviewWorkerIdentity{
		CycleID: values[reviewWorkerEnvCycleID],
		Revision: mustReviewWorkerPositiveInteger(
			values[reviewWorkerEnvRevision],
		),
		Role: AgentProfileRole(values[reviewWorkerEnvRole]),
		Pass: mustReviewWorkerPositiveInteger(
			values[reviewWorkerEnvPass],
		),
		Lane: values[reviewWorkerEnvLane],
	}
	if err := validateReviewWorkerIdentity(identity); err != nil {
		return fmt.Errorf("review worker environment identity is invalid: %w", err)
	}
	if values[reviewWorkerEnvRepoOwner] == "" ||
		values[reviewWorkerEnvRepoName] == "" ||
		values[reviewWorkerEnvRepo] !=
			values[reviewWorkerEnvRepoOwner]+"/"+values[reviewWorkerEnvRepoName] {
		return errors.New("review worker environment repository identity is invalid")
	}
	prNumber, err := strconv.Atoi(values[reviewWorkerEnvPRNumber])
	if err != nil || prNumber <= 0 ||
		strconv.Itoa(prNumber) != values[reviewWorkerEnvPRNumber] {
		return errors.New("review worker environment PR identity is invalid")
	}
	if values[reviewWorkerEnvOwnerID] == "" ||
		sanitizeSessionPart(values[reviewWorkerEnvOwnerID]) !=
			values[reviewWorkerEnvOwnerID] {
		return errors.New("review worker environment owner identity is unsafe")
	}
	return nil
}

func mustReviewWorkerPositiveInteger(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func safeReviewWorkerEnvironmentValue(value string) bool {
	return !strings.ContainsAny(value, "\x00\r\n")
}

func (p ReviewPolicy) effectiveProfileForReviewWorker(
	identity ReviewWorkerIdentity,
) (AgentProfile, error) {
	switch identity.Role {
	case AgentProfileRoleDiscovery:
		for _, lane := range p.Swarm.Lanes {
			if lane.Name != identity.Lane {
				continue
			}
			return p.effectiveNamedProfileForReviewWorker(
				strings.TrimSpace(lane.Profile),
				fmt.Sprintf("discovery lane %q", identity.Lane),
			)
		}
		return AgentProfile{}, fmt.Errorf(
			"discovery lane %q is not configured in the snapshotted review swarm",
			identity.Lane,
		)
	case AgentProfileRoleEscalation:
		return p.effectiveNamedProfileForReviewWorker(
			strings.TrimSpace(p.Escalation.Profile),
			"review escalation",
		)
	default:
		return p.effectiveProfileForRole(identity.Role)
	}
}

func (p ReviewPolicy) effectiveNamedProfileForReviewWorker(
	profileName string,
	contextLabel string,
) (AgentProfile, error) {
	profile, ok := p.AgentProfiles[profileName]
	if !ok {
		return AgentProfile{}, fmt.Errorf(
			"%s references unknown profile %q",
			contextLabel,
			profileName,
		)
	}
	profile, err := applyAgentProfileOverride(profile, p.runtimeProfileCLIOverride)
	if err != nil {
		return AgentProfile{}, fmt.Errorf(
			"%s profile %q: %w",
			contextLabel,
			profileName,
			err,
		)
	}
	if err := validateAgentProfile(profile, p.activeModelCatalog()); err != nil {
		return AgentProfile{}, fmt.Errorf(
			"%s selected invalid profile %q: %w",
			contextLabel,
			profileName,
			err,
		)
	}
	return profile, nil
}

func (b *Orchestrator) launchReviewWorker(
	ctx context.Context,
	reviewerID string,
	request reviewWorkerLaunchRequest,
) (
	resultOwnership ReviewWorkerOwnership,
	resultHandle RuntimeHandle,
	launchErr error,
) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, err
	}
	if b == nil || b.agents == nil || b.runner == nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, errors.New("review worker runtime is not configured")
	}
	isolatedRunner, ok := b.runner.(isolatedReviewWorkerRunner)
	if !ok {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, errors.New(
			"review worker runtime cannot establish the required isolated environment",
		)
	}
	if err := validateReviewWorkerIdentity(request.Identity); err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, err
	}
	artifactContract, err := reviewWorkerArtifactContract(
		request.Identity.Role,
	)
	if err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, err
	}
	workerPrompt := strings.TrimSpace(request.Prompt)
	if workerPrompt != "" {
		workerPrompt += "\n\n"
	}
	workerPrompt += artifactContract
	if _, err := b.ensureReviewCycleHeadCurrent(
		ctx,
		reviewerID,
	); err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{},
			newReviewWorkerBoundaryError(err)
	}
	reviewer, unlockLifecycle, err :=
		b.agents.lockActiveReviewCoordinatorForWorkerLaunch(reviewerID)
	if err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, err
	}
	lifecycleLocked := true
	defer func() {
		if lifecycleLocked {
			unlockLifecycle()
		}
	}()
	invalidateMovedHead := func(liveHeadSHA string) error {
		lifecycleLocked = false
		unlockLifecycle()
		invalidateErr := b.invalidateStaleReviewCycle(
			ctx,
			reviewer.ID,
			liveHeadSHA,
		)
		staleErr := &reviewCycleStaleError{
			reviewerID: reviewer.ID,
			staleHead:  reviewer.ReviewCycle.HeadSHA,
			liveHead:   liveHeadSHA,
		}
		return errors.Join(staleErr, invalidateErr)
	}
	recheckHead := func() error {
		liveHeadSHA, headErr := b.resolveLiveReviewHead(ctx, reviewer)
		if headErr != nil {
			return fmt.Errorf(
				"failed to recheck live head before review worker dispatch: %w",
				headErr,
			)
		}
		if liveHeadSHA != reviewer.ReviewCycle.HeadSHA {
			return invalidateMovedHead(liveHeadSHA)
		}
		return nil
	}
	if request.Identity.CycleID != reviewer.ReviewCycle.ID ||
		request.Identity.Revision != reviewer.ReviewCycle.Revision {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, errors.New(
			"review worker launch identity does not match the active cycle revision",
		)
	}
	recheckBoundary := func() error {
		if b.reviewCoordinatorGit == nil &&
			b.reviewCoordinatorPullRequests == nil {
			return nil
		}
		if b.reviewCoordinatorGit == nil ||
			b.reviewCoordinatorPullRequests == nil {
			return errors.New(
				"review worker repository boundary is incompletely configured",
			)
		}
		current, boundary, err := b.validateConvergentReviewBoundary(
			ctx,
			reviewer.ID,
		)
		if err != nil {
			var staleErr *reviewCycleStaleError
			if errors.As(err, &staleErr) {
				return invalidateMovedHead(staleErr.liveHead)
			}
			var terminalErr *convergentReviewPullRequestTerminalError
			if errors.As(err, &terminalErr) {
				lifecycleLocked = false
				unlockLifecycle()
				_, reconcileErr :=
					b.reconcileConvergentReviewPullRequest(
						ctx,
						reviewer.ID,
					)
				return errors.Join(err, reconcileErr)
			}
			return fmt.Errorf(
				"review worker repository boundary rejected launch: %w",
				err,
			)
		}
		if current.ReviewCycle.Inputs == nil {
			return errors.New(
				"review worker repository boundary rejected launch: exact-SHA plan inputs are missing",
			)
		}
		if current.ReviewCycle.Inputs.BaseSHA != boundary.BaseSHA {
			return fmt.Errorf(
				"%w: review worker base SHA mismatch: persisted=%s live=%s",
				errReviewBaseChanged,
				abbreviateSHA(current.ReviewCycle.Inputs.BaseSHA),
				abbreviateSHA(boundary.BaseSHA),
			)
		}
		return nil
	}
	if err := recheckBoundary(); err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{},
			newReviewWorkerBoundaryError(err)
	}
	if _, err := effectiveProfileForReviewLaunch(
		reviewer.ReviewCycle,
		request.Identity,
	); err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, fmt.Errorf(
			"failed to resolve review worker runtime profile: %w",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, err
	}
	ownership, err := b.reserveReviewWorkerOwnershipForActiveCoordinator(
		reviewer.ID,
		request.Identity,
	)
	if err != nil {
		return ReviewWorkerOwnership{}, RuntimeHandle{}, err
	}
	defer func() {
		if launchErr == nil {
			return
		}
		next := ReviewWorkerFailed
		var failure *DurableLaunchFailure
		if ctx.Err() != nil && (errors.Is(launchErr, context.Canceled) ||
			errors.Is(launchErr, context.DeadlineExceeded)) {
			next = ReviewWorkerCancelled
		} else {
			failure = &DurableLaunchFailure{
				Kind: DurableLaunchFailureStart,
			}
			if diagnostic := b.reviewWorkerWorktreeSetupDiagnostic(
				launchErr,
			); diagnostic != "" {
				failure.Kind = DurableLaunchFailureSetup
				failure.Detail = diagnostic
			} else if request.Identity.Role !=
				AgentProfileRoleDiscovery &&
				ctx.Err() == nil &&
				errors.Is(launchErr, context.DeadlineExceeded) {
				failure.Retryable = true
			}
		}
		if _, lifecycleErr :=
			b.transitionAndPersistReviewWorkerLifecycle(
				reviewer.ID,
				ownership.OwnerID,
				next,
				failure,
			); lifecycleErr != nil {
			launchErr = errors.Join(launchErr, lifecycleErr)
		}
	}()
	currentReviewer, ok := b.agents.Get(reviewer.ID)
	if !ok || currentReviewer.ReviewCycle == nil {
		return ownership, RuntimeHandle{}, errors.New(
			"review coordinator disappeared after worker reservation",
		)
	}
	profile, err := effectiveProfileForReviewLaunch(
		currentReviewer.ReviewCycle,
		request.Identity,
	)
	if err != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"failed to resolve review worker runtime profile: %w",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return ownership, RuntimeHandle{}, err
	}
	environment, err := reviewWorkerEnvironment(
		os.Environ(),
		b.cfg.RepoOwner,
		b.cfg.RepoName,
		reviewer.PRNumber,
		reviewer.ReviewCycle.HeadSHA,
		ownership,
	)
	if err != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"review worker isolation could not be established: %w",
			err,
		)
	}
	if err := isolatedRunner.ValidateReviewWorkerIsolation(environment); err != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"review worker isolation could not be established: %w",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return ownership, RuntimeHandle{}, err
	}
	workerAgent := Agent{
		ID:                ownership.OwnerID,
		Role:              RoleReviewer,
		ParentAgentID:     reviewer.ID,
		IssueNumber:       reviewer.IssueNumber,
		IssueTitle:        reviewer.IssueTitle,
		IssueBody:         reviewer.IssueBody,
		WorktreePath:      ownership.WorktreePath,
		LogDir:            reviewer.LogDir,
		RuntimeCWD:        ownership.WorktreePath,
		BranchName:        reviewer.BranchName,
		PRNumber:          reviewer.PRNumber,
		PRTitle:           reviewer.PRTitle,
		PRURL:             reviewer.PRURL,
		ObservedPRHeadSHA: reviewer.ReviewCycle.HeadSHA,
		RuntimeProfile:    profile,
		State:             StateInitializing,
		LastActivityTime:  time.Now().UTC(),
	}
	if reviewWorkerTmuxSessionName(workerAgent) != ownership.SessionName {
		return ownership, RuntimeHandle{}, errors.New(
			"review worker planned session does not match persisted ownership",
		)
	}
	if err := ctx.Err(); err != nil {
		return ownership, RuntimeHandle{}, err
	}
	if err := b.prepareReviewWorkerWorktreeWithRetry(ctx, workerAgent); err != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"failed to prepare review worker worktree: %w",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return ownership, RuntimeHandle{}, err
	}
	if err := os.Mkdir(ownership.GitHubConfigPath, 0o700); err != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"failed to prepare fresh credentialless review worker GitHub config: %w",
			err,
		)
	}
	if _, err := prepareReviewWorkerArtifactDirectory(ownership); err != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"failed to prepare fresh review worker artifact directory: %w",
			err,
		)
	}
	if err := recheckHead(); err != nil {
		return ownership, RuntimeHandle{},
			newReviewWorkerBoundaryError(err)
	}
	b.reviewArtifactMu.Lock()
	b.statePersistenceMu.Lock()
	claimMutation, claimed :=
		b.agents.claimReviewWorkerArtifactDirectory(
			reviewer.ID,
			ownership.OwnerID,
		)
	if !claimed {
		b.statePersistenceMu.Unlock()
		b.reviewArtifactMu.Unlock()
		return ownership, RuntimeHandle{}, errors.New(
			"failed to claim review worker artifact directory",
		)
	}
	if err := b.persistAgentStateLocked(); err != nil {
		if !b.agents.rollbackReviewWorkerArtifactDirectoryClaim(
			claimMutation,
		) {
			b.statePersistenceMu.Unlock()
			b.reviewArtifactMu.Unlock()
			return ownership, RuntimeHandle{}, fmt.Errorf(
				"failed to persist review worker artifact directory claim and failed to roll it back: %w",
				err,
			)
		}
		b.statePersistenceMu.Unlock()
		b.reviewArtifactMu.Unlock()
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"failed to persist review worker artifact directory claim: %w",
			err,
		)
	}
	b.statePersistenceMu.Unlock()
	b.reviewArtifactMu.Unlock()
	ownership.ArtifactDirectoryClaimed = true
	if err := ctx.Err(); err != nil {
		return ownership, RuntimeHandle{}, err
	}
	if err := recheckHead(); err != nil {
		return ownership, RuntimeHandle{},
			newReviewWorkerBoundaryError(err)
	}
	if err := b.enforceReviewWorkerDispatchLimits(
		reviewer.ID,
	); err != nil {
		return ownership, RuntimeHandle{}, err
	}
	currentReviewer, ok = b.agents.Get(reviewer.ID)
	if !ok || currentReviewer.ReviewCycle == nil {
		return ownership, RuntimeHandle{}, errors.New(
			"review coordinator disappeared before worker dispatch",
		)
	}
	profile, err = effectiveProfileForReviewLaunch(
		currentReviewer.ReviewCycle,
		request.Identity,
	)
	if err != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"failed to resolve review worker runtime profile before dispatch: %w",
			err,
		)
	}
	workerAgent.RuntimeProfile = profile
	if b.reviewCoordinatorGit != nil {
		expectedRepository := strings.ToLower(
			strings.TrimSpace(b.cfg.RepoOwner) + "/" +
				strings.TrimSpace(b.cfg.RepoName),
		)
		if err := b.validateConvergentReviewWorkerCheckout(
			ctx,
			workerAgent.WorktreePath,
			expectedRepository,
			currentReviewer.ReviewCycle.HeadSHA,
		); err != nil {
			return ownership, RuntimeHandle{}, fmt.Errorf(
				"review worker checkout boundary rejected launch: %w",
				newReviewWorkerBoundaryError(err),
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return ownership, RuntimeHandle{}, err
	}
	if err := recheckBoundary(); err != nil {
		return ownership, RuntimeHandle{},
			newReviewWorkerBoundaryError(err)
	}
	runningOwnership, lifecycleErr :=
		b.transitionAndPersistReviewWorkerLifecycle(
			reviewer.ID,
			ownership.OwnerID,
			ReviewWorkerRunning,
			nil,
		)
	if lifecycleErr != nil {
		return ownership, RuntimeHandle{}, fmt.Errorf(
			"failed to checkpoint review worker launcher invocation: %w",
			lifecycleErr,
		)
	}
	ownership = runningOwnership
	handle, err := isolatedRunner.StartReviewWorkerContext(
		ctx,
		workerAgent,
		workerPrompt,
		environment,
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = errors.Join(ctxErr, err)
		}
		return ownership, RuntimeHandle{}, fmt.Errorf("failed to launch review worker: %w", err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if stopErr := isolatedRunner.Stop(handle); stopErr != nil {
			return ownership, RuntimeHandle{}, fmt.Errorf(
				"review worker launch canceled: %w; failed to stop newly launched runtime: %v",
				ctxErr,
				stopErr,
			)
		}
		return ownership, RuntimeHandle{}, ctxErr
	}
	if handle.Session != ownership.SessionName {
		_ = isolatedRunner.Stop(handle)
		return ownership, RuntimeHandle{}, errors.New(
			"review worker runtime returned a session not owned by the allocation",
		)
	}
	return ownership, handle, nil
}

func (b *Orchestrator) validateConvergentReviewWorkerCheckout(
	ctx context.Context,
	worktreePath string,
	expectedRepository string,
	expectedHeadSHA string,
) error {
	if err := b.validateConvergentReviewRepository(
		ctx,
		worktreePath,
		expectedRepository,
	); err != nil {
		return err
	}
	head, err := b.reviewCoordinatorGit.Output(
		ctx,
		worktreePath,
		"rev-parse",
		"--verify",
		"HEAD",
	)
	if err != nil {
		return fmt.Errorf(
			"failed to validate review worker checkout HEAD: %w",
			err,
		)
	}
	actualHeadSHA := strings.ToLower(strings.TrimSpace(string(head)))
	if actualHeadSHA != expectedHeadSHA {
		return fmt.Errorf(
			"review worker checkout SHA mismatch: cycle=%s checkout=%s",
			abbreviateSHA(expectedHeadSHA),
			abbreviateSHA(actualHeadSHA),
		)
	}
	ref, err := b.reviewCoordinatorGit.Output(
		ctx,
		worktreePath,
		"rev-parse",
		"--abbrev-ref",
		"HEAD",
	)
	if err != nil {
		return fmt.Errorf(
			"failed to validate detached review worker checkout: %w",
			err,
		)
	}
	if strings.TrimSpace(string(ref)) != "HEAD" {
		return errors.New("review worker checkout is not detached")
	}
	status, err := b.reviewCoordinatorGit.Output(
		ctx,
		worktreePath,
		"status",
		"--porcelain=v1",
		"--untracked-files=all",
	)
	if err != nil {
		return fmt.Errorf(
			"failed to validate clean review worker checkout: %w",
			err,
		)
	}
	if len(bytes.TrimSpace(status)) != 0 {
		return errors.New("review worker checkout is dirty")
	}
	return nil
}

func (b *Orchestrator) prepareReviewWorkerWorktree(ctx context.Context, worker Agent) error {
	if b != nil && b.prepareReviewWorkerWorktreeFunc != nil {
		return b.prepareReviewWorkerWorktreeFunc(ctx, worker)
	}
	return b.prepareReviewerWorktree(ctx, worker)
}

func (b *Orchestrator) prepareReviewWorkerWorktreeWithRetry(
	ctx context.Context,
	worker Agent,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	setupCtx, cancel := context.WithTimeout(ctx, reviewWorkerWorktreeSetupTimeout)
	defer cancel()

	var setupErr error
	for attempt := 1; attempt <= reviewWorkerWorktreeSetupAttempts; attempt++ {
		setupErr = b.prepareReviewWorkerWorktree(setupCtx, worker)
		if setupErr == nil {
			return nil
		}
		if ctxErr := setupCtx.Err(); ctxErr != nil {
			return &reviewWorkerWorktreeSetupError{
				attempts: attempt,
				err:      errors.Join(ctxErr, setupErr),
			}
		}
		if !reviewWorkerWorktreeSetupFailureIsRetryable(setupErr) ||
			attempt == reviewWorkerWorktreeSetupAttempts {
			return &reviewWorkerWorktreeSetupError{
				attempts: attempt,
				err:      setupErr,
			}
		}
		if cleanupErr := b.cleanupWorktree(
			setupCtx,
			worker.WorktreePath,
			"",
		); cleanupErr != nil {
			return &reviewWorkerWorktreeSetupError{
				attempts: attempt,
				err: errors.Join(
					setupErr,
					fmt.Errorf(
						"failed to clean owned worktree allocation before retry: %w",
						cleanupErr,
					),
				),
			}
		}
		if err := waitReviewWorkerWorktreeSetupRetry(setupCtx, attempt); err != nil {
			return &reviewWorkerWorktreeSetupError{
				attempts: attempt,
				err:      errors.Join(err, setupErr),
			}
		}
	}
	return &reviewWorkerWorktreeSetupError{
		attempts: reviewWorkerWorktreeSetupAttempts,
		err:      setupErr,
	}
}

func reviewWorkerWorktreeSetupFailureIsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return false
	}
	var commandErr *commandExecutionError
	if !errors.As(err, &commandErr) {
		return true
	}
	diagnostic := strings.ToLower(commandErr.Error())
	for _, deterministic := range []string{
		"invalid reference",
		"not a valid object name",
		"not a git repository",
		"permission denied",
		"operation not permitted",
		"unknown option",
		"usage: git worktree",
	} {
		if strings.Contains(diagnostic, deterministic) {
			return false
		}
	}
	return true
}

func waitReviewWorkerWorktreeSetupRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt) * reviewWorkerWorktreeSetupRetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *Orchestrator) reviewWorkerWorktreeSetupDiagnostic(err error) string {
	var setupErr *reviewWorkerWorktreeSetupError
	if !errors.As(err, &setupErr) {
		return ""
	}
	return boundedReviewWorkerWorktreeSetupDiagnostic(b.safeError(setupErr))
}

func boundedReviewWorkerWorktreeSetupDiagnostic(diagnostic string) string {
	diagnostic = strings.TrimSpace(strings.ToValidUTF8(
		diagnostic,
		string(utf8.RuneError),
	))
	if len(diagnostic) <= reviewWorkerWorktreeSetupDiagnosticBytes {
		return diagnostic
	}
	marker := "\n[... diagnostic truncated ...]\n"
	headBytes := reviewWorkerWorktreeSetupDiagnosticBytes / 4
	for headBytes > 0 && !utf8.RuneStart(diagnostic[headBytes]) {
		headBytes--
	}
	tailBytes := reviewWorkerWorktreeSetupDiagnosticBytes - headBytes - len(marker)
	tailStart := len(diagnostic) - tailBytes
	for tailStart < len(diagnostic) && !utf8.RuneStart(diagnostic[tailStart]) {
		tailStart++
	}
	return diagnostic[:headBytes] + marker + diagnostic[tailStart:]
}
