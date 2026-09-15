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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v90/github"
)

type reviewCoordinatorGitFixture struct {
	mu                sync.Mutex
	origin            string
	head              string
	checkoutOverrides map[string]reviewCoordinatorCheckoutFixture
	calls             []string
}

type reviewCoordinatorCheckoutFixture struct {
	head   string
	ref    string
	status string
}

func attachRunningReviewLaunchAttempt(
	t *testing.T,
	agents *AgentManager,
	coderID string,
	cycle *ReviewCycleState,
	reviewerID string,
) {
	t.Helper()
	now := time.Now().UTC()
	attempt, err := newDurableLaunchAttempt(
		cycle.ID,
		DurableLaunchReviewCoordinator,
		cycle.HeadSHA,
		cycle.Attempt,
		reviewerID,
		"",
		now,
	)
	if err != nil {
		t.Fatalf("newDurableLaunchAttempt() error = %v", err)
	}
	if _, err := transitionDurableLaunchAttempt(
		&attempt,
		DurableLaunchRunning,
		nil,
		time.Time{},
		now,
	); err != nil {
		t.Fatalf("transitionDurableLaunchAttempt() error = %v", err)
	}
	agents.mu.Lock()
	agents.agents[coderID].LaunchAttempts = append(
		agents.agents[coderID].LaunchAttempts,
		attempt,
	)
	agents.mu.Unlock()
}

func TestParseCanonicalGitHubRemoteRepository(t *testing.T) {
	tests := []struct {
		name   string
		remote string
		owner  string
		repo   string
		ok     bool
	}{
		{
			name:   "HTTPS",
			remote: "https://github.com/acme/widget.git",
			owner:  "acme",
			repo:   "widget",
			ok:     true,
		},
		{
			name:   "SSH URL",
			remote: "ssh://git@github.com/acme/widget.git",
			owner:  "acme",
			repo:   "widget",
			ok:     true,
		},
		{
			name:   "SSH SCP",
			remote: "git@github.com:acme/widget.git",
			owner:  "acme",
			repo:   "widget",
			ok:     true,
		},
		{
			name:   "same slug different host",
			remote: "https://gitlab.example/acme/widget.git",
		},
		{
			name:   "GitHub suffix host",
			remote: "https://github.com.example/acme/widget.git",
		},
		{
			name:   "extra path component",
			remote: "https://github.com/group/acme/widget.git",
		},
		{
			name:   "unqualified path",
			remote: "acme/widget",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner, repo, ok := parseCanonicalGitHubRemoteRepository(
				test.remote,
			)
			if owner != test.owner ||
				repo != test.repo ||
				ok != test.ok {
				t.Fatalf(
					"parseCanonicalGitHubRemoteRepository(%q) = (%q, %q, %t), want (%q, %q, %t)",
					test.remote,
					owner,
					repo,
					ok,
					test.owner,
					test.repo,
					test.ok,
				)
			}
		})
	}
}

func TestSyncConvergentReviewGitObjectsFetchesMissingLiveBase(t *testing.T) {
	root := t.TempDir()
	remotePath := filepath.Join(root, "origin.git")
	seedPath := filepath.Join(root, "seed")
	reviewPath := filepath.Join(root, "review")
	if err := os.Mkdir(seedPath, 0o755); err != nil {
		t.Fatalf("Mkdir(seed) error = %v", err)
	}
	runGit(t, root, "init", "--bare", remotePath)
	runGit(t, seedPath, "init", "--initial-branch=main")
	runGit(t, seedPath, "config", "user.email", "review@example.com")
	runGit(t, seedPath, "config", "user.name", "Review Test")
	if err := os.WriteFile(
		filepath.Join(seedPath, "shared.txt"),
		[]byte("shared\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(shared) error = %v", err)
	}
	runGit(t, seedPath, "add", "shared.txt")
	runGit(t, seedPath, "commit", "-m", "base")
	runGit(t, seedPath, "remote", "add", "origin", remotePath)
	runGit(t, seedPath, "push", "-u", "origin", "main")

	runGit(t, seedPath, "checkout", "-b", "feature")
	if err := os.WriteFile(
		filepath.Join(seedPath, "feature.txt"),
		[]byte("feature\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(feature) error = %v", err)
	}
	runGit(t, seedPath, "add", "feature.txt")
	runGit(t, seedPath, "commit", "-m", "feature")
	headSHA := strings.TrimSpace(runGit(t, seedPath, "rev-parse", "HEAD"))
	runGit(t, seedPath, "push", "-u", "origin", "feature")

	runGit(t, seedPath, "checkout", "main")
	if err := os.WriteFile(
		filepath.Join(seedPath, "main.txt"),
		[]byte("main\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(main) error = %v", err)
	}
	runGit(t, seedPath, "add", "main.txt")
	runGit(t, seedPath, "commit", "-m", "advance main")
	baseSHA := strings.TrimSpace(runGit(t, seedPath, "rev-parse", "HEAD"))
	runGit(t, seedPath, "push", "origin", "main")

	runGit(t, root, "clone", "--single-branch", "--branch", "feature", "--no-local", remotePath, reviewPath)
	missing := newCommandContext(
		context.Background(),
		"git",
		"-C",
		reviewPath,
		"cat-file",
		"-e",
		baseSHA+"^{commit}",
	)
	if err := missing.Run(); err == nil {
		t.Fatal("single-branch review clone unexpectedly contains live base")
	}

	bot := &Orchestrator{reviewCoordinatorGit: osReviewCoordinatorGit{}}
	if err := bot.syncConvergentReviewGitObjects(
		context.Background(),
		reviewPath,
		baseSHA,
		headSHA,
	); err != nil {
		t.Fatalf("syncConvergentReviewGitObjects() error = %v", err)
	}
	for _, sha := range []string{baseSHA, headSHA} {
		runGit(t, reviewPath, "cat-file", "-e", sha+"^{commit}")
	}
	if _, err := os.Stat(filepath.Join(reviewPath, ".git", "FETCH_HEAD")); !os.IsNotExist(err) {
		t.Fatalf("FETCH_HEAD was written by exact-object synchronization: %v", err)
	}
}

func TestSyncConvergentReviewGitObjectsRejectsNonCanonicalObjectID(t *testing.T) {
	fixture := &reviewCoordinatorGitFixture{}
	bot := &Orchestrator{reviewCoordinatorGit: fixture}
	err := bot.syncConvergentReviewGitObjects(
		context.Background(),
		t.TempDir(),
		"HEAD; touch unexpected",
	)
	if err == nil || !strings.Contains(err.Error(), "object ID is invalid") {
		t.Fatalf("syncConvergentReviewGitObjects() error = %v", err)
	}
	if len(fixture.calls) != 0 {
		t.Fatalf("invalid object ID reached Git: %#v", fixture.calls)
	}
}

func (fixture *reviewCoordinatorGitFixture) Output(
	_ context.Context,
	dir string,
	args ...string,
) ([]byte, error) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.calls = append(
		fixture.calls,
		filepath.Clean(dir)+"\x00"+strings.Join(args, "\x00"),
	)
	checkout := fixture.checkoutOverrides[filepath.Clean(dir)]
	switch strings.Join(args, " ") {
	case "remote get-url origin":
		return []byte(fixture.origin + "\n"), nil
	case "rev-parse --verify HEAD":
		head := fixture.head
		if checkout.head != "" {
			head = checkout.head
		}
		return []byte(head + "\n"), nil
	case "rev-parse --abbrev-ref HEAD":
		ref := checkout.ref
		if ref == "" {
			ref = "HEAD"
		}
		return []byte(ref + "\n"), nil
	case "status --porcelain=v1 --untracked-files=all":
		return []byte(checkout.status), nil
	default:
		if len(args) > 0 && args[0] == "fetch" {
			return nil, nil
		}
		if len(args) == 3 && args[0] == "cat-file" && args[1] == "-e" {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"unexpected injected git request: git %s",
			strings.Join(args, " "),
		)
	}
}

type reviewCoordinatorPullRequestFixture struct {
	mu          sync.Mutex
	pullRequest *github.PullRequest
	sequence    []*github.PullRequest
	err         error
	calls       int
}

func (fixture *reviewCoordinatorPullRequestFixture) Get(
	_ context.Context,
	owner string,
	repo string,
	number int,
) (*github.PullRequest, *github.Response, error) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.calls++
	if owner != "acme" || repo != "widget" || number != 50 {
		return nil, nil, fmt.Errorf(
			"unexpected injected pull request: %s/%s#%d",
			owner,
			repo,
			number,
		)
	}
	if len(fixture.sequence) != 0 {
		index := fixture.calls - 1
		if index >= len(fixture.sequence) {
			index = len(fixture.sequence) - 1
		}
		return fixture.sequence[index], nil, fixture.err
	}
	return fixture.pullRequest, nil, fixture.err
}

func coordinatorPullRequest(
	number int,
	baseSHA string,
	headSHA string,
	baseRepository string,
	headRepository string,
) *github.PullRequest {
	return &github.PullRequest{
		Number: github.Int(number),
		Base: &github.PullRequestBranch{
			SHA: github.String(baseSHA),
			Repo: &github.Repository{
				FullName: github.String(baseRepository),
			},
		},
		Head: &github.PullRequestBranch{
			SHA: github.String(headSHA),
			Repo: &github.Repository{
				FullName: github.String(headRepository),
			},
		},
	}
}

type reviewCoordinatorLaunch struct {
	Agent       Agent
	Environment []string
}

type productionReviewCoordinatorRunner struct {
	mu                  sync.Mutex
	bot                 *Orchestrator
	includeCandidate    bool
	candidateLane       string
	verificationOutcome ReviewVerificationOutcome
	leaveCoverageGap    bool
	failRole            AgentProfileRole
	starts              []reviewCoordinatorLaunch
	topLevelStarts      int
	activeStarts        int
	maxActiveStarts     int
	startEntered        chan struct{}
	releaseStart        chan struct{}
	startOnce           sync.Once
	afterArtifact       func(Agent)
}

func (runner *productionReviewCoordinatorRunner) Start(
	agent Agent,
	_ string,
) (RuntimeHandle, error) {
	runner.mu.Lock()
	runner.topLevelStarts++
	runner.mu.Unlock()
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: tmuxSessionName(agent),
	}, nil
}

func (runner *productionReviewCoordinatorRunner) StartReviewWorkerContext(
	ctx context.Context,
	agent Agent,
	_ string,
	environment []string,
) (RuntimeHandle, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeHandle{}, err
	}
	runner.mu.Lock()
	runner.starts = append(runner.starts, reviewCoordinatorLaunch{
		Agent:       agent,
		Environment: append([]string(nil), environment...),
	})
	runner.activeStarts++
	if runner.activeStarts > runner.maxActiveStarts {
		runner.maxActiveStarts = runner.activeStarts
	}
	runner.mu.Unlock()
	defer func() {
		runner.mu.Lock()
		runner.activeStarts--
		runner.mu.Unlock()
	}()
	if runner.failRole != "" {
		for _, entry := range environment {
			if entry == reviewWorkerEnvRole+"="+
				string(runner.failRole) {
				return RuntimeHandle{}, fmt.Errorf(
					"injected %s worker failure",
					runner.failRole,
				)
			}
		}
	}
	// Keep the production scheduler's launch semaphore observable.
	time.Sleep(10 * time.Millisecond)
	if runner.startEntered != nil {
		runner.startOnce.Do(func() {
			close(runner.startEntered)
		})
		select {
		case <-runner.releaseStart:
		case <-ctx.Done():
			return RuntimeHandle{}, ctx.Err()
		}
	}

	reviewer, ok := runner.bot.agents.Get(agent.ParentAgentID)
	if !ok || reviewer.ReviewCycle == nil {
		return RuntimeHandle{}, errors.New(
			"review coordinator disappeared from injected runtime",
		)
	}
	var ownership ReviewWorkerOwnership
	for _, candidate := range reviewer.ReviewCycle.WorkerOwnerships {
		if candidate.OwnerID == agent.ID {
			ownership = candidate
			break
		}
	}
	if ownership.OwnerID == "" {
		return RuntimeHandle{}, errors.New(
			"review worker ownership disappeared from injected runtime",
		)
	}
	phase, payload, err := runner.artifactFor(reviewer, ownership)
	if err != nil {
		return RuntimeHandle{}, err
	}
	directory, err := reviewWorkerArtifactDirectory(ownership.WorktreePath)
	if err != nil {
		return RuntimeHandle{}, err
	}
	store, err := newReviewArtifactStore(
		directory,
		reviewer.ReviewCycle.Policy.Artifacts,
	)
	if err != nil {
		return RuntimeHandle{}, err
	}
	envelope, err := newReviewArtifactEnvelope(
		reviewer.ReviewCycle.HeadSHA,
		ownership,
		phase,
		payload,
	)
	if err != nil {
		return RuntimeHandle{}, err
	}
	if _, err := store.publish(
		ctx,
		reviewer.ReviewCycle.HeadSHA,
		ownership,
		envelope,
	); err != nil {
		return RuntimeHandle{}, err
	}
	if runner.afterArtifact != nil {
		runner.afterArtifact(agent)
	}
	return RuntimeHandle{
		Kind:    RuntimeKindTmux,
		Session: ownership.SessionName,
	}, nil
}

func (runner *productionReviewCoordinatorRunner) artifactFor(
	reviewer Agent,
	ownership ReviewWorkerOwnership,
) (ReviewArtifactPhase, ReviewArtifactPayload, error) {
	cycle := reviewer.ReviewCycle
	switch ownership.Identity.Role {
	case AgentProfileRoleDiscovery:
		claims := make([]ReviewCoverageClaim, 0)
		for index, requirement := range cycle.Plan.CoverageRequirements {
			if runner.leaveCoverageGap && index == 0 {
				continue
			}
			claims = append(claims, ReviewCoverageClaim{
				RequirementID: requirement.ID,
				Kind:          requirement.Kind,
				Status:        ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary:   "production discovery inspected the changed path",
					Path:      "tracked.txt",
					StartLine: 1,
					EndLine:   1,
				}},
			})
		}
		candidates := make([]ReviewFindingCandidate, 0)
		candidateLane := runner.candidateLane
		if candidateLane == "" {
			candidateLane = "contract"
		}
		if runner.includeCandidate &&
			(ownership.Identity.Lane == candidateLane ||
				ownership.Identity.Lane == reviewSynthesisLane) {
			candidates = append(candidates, ReviewFindingCandidate{
				CandidateID: "production-candidate",
				Summary:     "The changed value can violate its invariant",
				Location: ReviewFindingLocation{
					Path:      "tracked.txt",
					Symbol:    "value",
					StartLine: 1,
					EndLine:   1,
				},
				BehavioralPath: "the changed value reaches its consumer",
				ViolatedInvariant: "the consumer requires the original " +
					"value",
				Severity:   ReviewFindingSeverityMedium,
				Confidence: ReviewFindingConfidenceMedium,
				Evidence: []ReviewEvidence{{
					Summary:   "the exact-SHA checkout contains the changed value",
					Path:      "tracked.txt",
					StartLine: 1,
					EndLine:   1,
				}},
			})
		}
		return ReviewArtifactPhaseDiscovery, ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadDiscovery,
			Discovery: &ReviewDiscoveryPayload{
				Summary:         "production discovery completed",
				Candidates:      candidates,
				Coverage:        claims,
				UnreviewedAreas: []string{},
			},
		}, nil
	case AgentProfileRoleVerifier:
		assignment, ok := scheduledReviewVerificationAssignment(
			cycle,
			ownership.Identity.Pass,
			ownership.Identity.Lane,
		)
		if !ok {
			return "", ReviewArtifactPayload{}, errors.New(
				"production verifier assignment disappeared",
			)
		}
		location := assignment.CandidateSnapshot.Location
		outcome := runner.verificationOutcome
		if outcome == "" {
			outcome = ReviewVerificationRejected
		}
		summary := "independent reproduction rejected the candidate"
		patchDisposition := ReviewPatchNotReproduced
		evidence := []ReviewEvidence{{
			Summary:   "independent inspection found the guard",
			Path:      location.Path,
			StartLine: location.StartLine,
			EndLine:   location.EndLine,
		}}
		causalEvidence := []ReviewEvidence{}
		testEvidence := []ReviewEvidence{{
			Summary: "the exact-SHA reproduction preserved the invariant",
			Path:    "tracked.txt",
		}}
		if outcome == ReviewVerificationConfirmed {
			summary = "independent reproduction confirmed the candidate"
			patchDisposition = ReviewPatchIntroduced
			evidence = []ReviewEvidence{{
				Summary:   "independent inspection reproduced the failure",
				Path:      location.Path,
				StartLine: location.StartLine,
				EndLine:   location.EndLine,
			}}
			causalEvidence = []ReviewEvidence{{
				Summary:   "the base-to-head change introduced the failure",
				Path:      location.Path,
				StartLine: location.StartLine,
				EndLine:   location.EndLine,
			}}
			testEvidence = []ReviewEvidence{{
				Summary:   "the exact-SHA reproduction triggered the failure",
				Path:      location.Path,
				StartLine: location.StartLine,
				EndLine:   location.EndLine,
			}}
		}
		return ReviewArtifactPhaseVerification, ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadVerification,
			Verification: &ReviewVerificationPayload{
				FindingID:        assignment.FindingID,
				Outcome:          outcome,
				Summary:          summary,
				ScopeDisposition: ReviewScopeInScope,
				PatchDisposition: patchDisposition,
				Location:         &location,
				BehavioralPath:   assignment.CandidateSnapshot.BehavioralPath,
				Evidence:         evidence,
				CausalEvidence:   causalEvidence,
				TestEvidence:     testEvidence,
			},
		}, nil
	case AgentProfileRoleChallenge:
		assignment, ok := scheduledReviewChallengeAssignment(
			cycle,
			ownership.Identity.Pass,
			ownership.Identity.Lane,
		)
		if !ok {
			return "", ReviewArtifactPayload{}, errors.New(
				"production challenge assignment disappeared",
			)
		}
		var requirement ReviewCoverageRequirement
		for _, candidate := range cycle.Plan.CoverageRequirements {
			if candidate.ID == assignment.Target.RequirementID {
				requirement = candidate
				break
			}
		}
		if requirement.ID == "" {
			return "", ReviewArtifactPayload{}, errors.New(
				"production challenge requirement disappeared",
			)
		}
		return ReviewArtifactPhaseChallenge, ReviewArtifactPayload{
			Kind: ReviewArtifactPayloadChallenge,
			Challenge: &ReviewChallengePayload{
				Outcome:      ReviewChallengeUpheld,
				Summary:      "targeted production evidence closed the gap",
				AssignmentID: assignment.ID,
				TargetKind:   assignment.Target.Kind,
				TargetID:     assignment.Target.ID,
				Candidates:   []ReviewFindingCandidate{},
				Coverage: []ReviewCoverageClaim{{
					RequirementID: requirement.ID,
					Kind:          requirement.Kind,
					Status:        ReviewCoverageCovered,
					Evidence: []ReviewEvidence{{
						Summary:   "targeted challenge exercised the changed path",
						Path:      "tracked.txt",
						StartLine: 1,
						EndLine:   1,
					}},
				}},
			},
		}, nil
	default:
		return "", ReviewArtifactPayload{}, fmt.Errorf(
			"unexpected production review role %q",
			ownership.Identity.Role,
		)
	}
}

func (runner *productionReviewCoordinatorRunner) ValidateReviewWorkerIsolation(
	environment []string,
) error {
	return validateReviewWorkerEnvironment(environment)
}

func (runner *productionReviewCoordinatorRunner) Send(
	RuntimeHandle,
	string,
) error {
	return nil
}

func (runner *productionReviewCoordinatorRunner) Capture(
	RuntimeHandle,
	int,
) (string, error) {
	return "", nil
}

func (runner *productionReviewCoordinatorRunner) Stop(
	RuntimeHandle,
) error {
	return nil
}

func (runner *productionReviewCoordinatorRunner) IsAlive(
	RuntimeHandle,
) (bool, error) {
	return false, nil
}

func (runner *productionReviewCoordinatorRunner) snapshot() ([]reviewCoordinatorLaunch, int) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return append([]reviewCoordinatorLaunch(nil), runner.starts...),
		runner.maxActiveStarts
}

func (runner *productionReviewCoordinatorRunner) topLevelStartCount() int {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.topLevelStarts
}

type productionReviewCoordinatorHarness struct {
	bot         *Orchestrator
	reviewer    Agent
	runner      *productionReviewCoordinatorRunner
	git         *reviewCoordinatorGitFixture
	pulls       *reviewCoordinatorPullRequestFixture
	fixture     *reviewVerdictGitHubFixture
	baseSHA     string
	headSHA     string
	server      *httptest.Server
	worktreeDir string
}

func newProductionReviewCoordinatorHarness(
	t *testing.T,
	includeCandidate bool,
	leaveCoverageGap bool,
) *productionReviewCoordinatorHarness {
	t.Helper()
	root := t.TempDir()
	seedRepo := filepath.Join(root, "seed")
	if err := os.Mkdir(seedRepo, 0o755); err != nil {
		t.Fatalf("Mkdir(seed repo) error = %v", err)
	}
	runReviewArtifactGit(t, seedRepo, "init", "--initial-branch=main")
	runReviewArtifactGit(
		t,
		seedRepo,
		"config",
		"user.email",
		"coordinator@example.com",
	)
	runReviewArtifactGit(
		t,
		seedRepo,
		"config",
		"user.name",
		"Coordinator Test",
	)
	if err := os.WriteFile(
		filepath.Join(seedRepo, "tracked.txt"),
		[]byte("before\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(base tracked file) error = %v", err)
	}
	runReviewArtifactGit(t, seedRepo, "add", "tracked.txt")
	runReviewArtifactGit(
		t,
		seedRepo,
		"commit",
		"-m",
		"test: create coordinator base",
	)
	baseSHA := strings.TrimSpace(
		runReviewArtifactGit(t, seedRepo, "rev-parse", "HEAD"),
	)
	if err := os.WriteFile(
		filepath.Join(seedRepo, "tracked.txt"),
		[]byte("after\n"),
		0o600,
	); err != nil {
		t.Fatalf("WriteFile(head tracked file) error = %v", err)
	}
	runReviewArtifactGit(t, seedRepo, "add", "tracked.txt")
	runReviewArtifactGit(
		t,
		seedRepo,
		"commit",
		"-m",
		"test: create coordinator head",
	)
	headSHA := strings.TrimSpace(
		runReviewArtifactGit(t, seedRepo, "rev-parse", "HEAD"),
	)

	worktreeDir := filepath.Join(root, "worktrees")
	if err := os.Mkdir(worktreeDir, 0o755); err != nil {
		t.Fatalf("Mkdir(worktree root) error = %v", err)
	}
	coordinatorWorktree := filepath.Join(worktreeDir, "coordinator")
	runReviewArtifactGit(
		t,
		root,
		"clone",
		"--quiet",
		seedRepo,
		coordinatorWorktree,
	)

	policy := productionReviewCoordinatorPolicy()
	cycle, err := newReviewCycleState(headSHA, policy)
	if err != nil {
		t.Fatalf("newReviewCycleState() error = %v", err)
	}
	inputs := ReviewPlanInputs{
		SchemaVersion:    reviewPlanInputsSchemaVersion,
		BaseSHA:          baseSHA,
		HeadSHA:          headSHA,
		ChangedFileCount: 1,
		ChangedLineCount: 2,
		ChangedFiles: []ReviewPlanChangedFile{{
			Path:      "tracked.txt",
			Additions: 1,
			Deletions: 1,
		}},
		AcceptanceCriteria: []string{
			"Preserve the changed value at its consumer",
		},
	}
	if err := validateReviewPlanInputs(inputs); err != nil {
		t.Fatalf("validateReviewPlanInputs() error = %v", err)
	}
	plan, err := buildReviewPlan(inputs, cycle.Policy)
	if err != nil {
		t.Fatalf("buildReviewPlan() error = %v", err)
	}
	if err := attachReviewPlanInputs(cycle, inputs); err != nil {
		t.Fatalf("attachReviewPlanInputs() error = %v", err)
	}
	if err := attachReviewPlan(cycle, plan); err != nil {
		t.Fatalf("attachReviewPlan() error = %v", err)
	}
	profile, err := cycle.Policy.effectiveProfileForRole(
		AgentProfileRoleChallenge,
	)
	if err != nil {
		t.Fatalf("effectiveProfileForRole(challenge) error = %v", err)
	}
	agents := NewAgentManager()
	reviewer := Agent{
		ID:                "production-review-coordinator",
		Role:              RoleReviewer,
		IssueNumber:       55,
		IssueTitle:        "Connect real coordinator to worker launches",
		IssueBody:         inputs.AcceptanceCriteria[0],
		WorktreePath:      coordinatorWorktree,
		LogDir:            filepath.Join(root, "logs"),
		PRNumber:          50,
		PRTitle:           "Production coordinator",
		PRURL:             "https://example.test/pull/50",
		ObservedPRHeadSHA: headSHA,
		RuntimeProfile:    profile,
		RuntimeHandle: RuntimeHandle{
			Kind:    RuntimeKindTmux,
			Session: "production-review-coordinator",
		},
		State:            StateWorking,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(&reviewer); err != nil {
		t.Fatalf("AgentManager.Add() error = %v", err)
	}
	gitFixture := &reviewCoordinatorGitFixture{
		origin:            "git@github.com:acme/widget.git",
		head:              headSHA,
		checkoutOverrides: make(map[string]reviewCoordinatorCheckoutFixture),
	}
	pullFixture := &reviewCoordinatorPullRequestFixture{
		pullRequest: coordinatorPullRequest(
			50,
			baseSHA,
			headSHA,
			"acme/widget",
			"acme/widget",
		),
	}
	verdictFixture := &reviewVerdictGitHubFixture{
		baseSHA: baseSHA,
		headSHA: headSHA,
	}
	server := httptest.NewServer(http.HandlerFunc(verdictFixture.handler))
	t.Cleanup(server.Close)
	bot := &Orchestrator{
		cfg: Config{
			RepoOwner:    "acme",
			RepoName:     "widget",
			RepoPath:     seedRepo,
			LogDir:       filepath.Join(root, "logs"),
			WorktreeDir:  worktreeDir,
			ReviewPolicy: cycle.Policy,
		},
		github:                        newGitHubClientForTest(t, server),
		agents:                        agents,
		reviewCoordinatorGit:          gitFixture,
		reviewCoordinatorPullRequests: pullFixture,
		reviewCoordinatorRuns:         make(map[string]struct{}),
		reviewHeadResolver: func(
			context.Context,
			int,
		) (string, error) {
			return headSHA, nil
		},
	}
	bot.prepareReviewWorkerWorktreeFunc = func(
		ctx context.Context,
		worker Agent,
	) error {
		command := newCommandContext(
			ctx,
			"git",
			"clone",
			"--quiet",
			seedRepo,
			worker.WorktreePath,
		)
		output, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf(
				"failed to clone coordinator worker checkout: %w: %s",
				err,
				strings.TrimSpace(string(output)),
			)
		}
		return nil
	}
	runner := &productionReviewCoordinatorRunner{
		bot:              bot,
		includeCandidate: includeCandidate,
		leaveCoverageGap: leaveCoverageGap,
	}
	bot.runner = runner
	t.Cleanup(bot.waitForReviewCleanups)
	t.Cleanup(bot.waitForPersistedReviewCycleRecoveries)
	return &productionReviewCoordinatorHarness{
		bot:         bot,
		reviewer:    reviewer,
		runner:      runner,
		git:         gitFixture,
		pulls:       pullFixture,
		fixture:     verdictFixture,
		baseSHA:     baseSHA,
		headSHA:     headSHA,
		server:      server,
		worktreeDir: worktreeDir,
	}
}

func productionReviewCoordinatorPolicy() ReviewPolicy {
	policy := builtInReviewPolicy()
	profiles := []AgentProfile{
		{
			Name:            "production-discovery",
			Model:           "gpt-5.6-terra",
			ReasoningEffort: "low",
		},
		{
			Name:            "production-verifier",
			Model:           "gpt-5.6-luna",
			ReasoningEffort: "medium",
		},
		{
			Name:            "production-challenge",
			Model:           "gpt-5.6-sol",
			ReasoningEffort: "xhigh",
		},
		{
			Name:            "production-escalation",
			Model:           "gpt-5.6-sol",
			ReasoningEffort: "max",
		},
		{
			Name:            "correctness-override",
			Model:           "gpt-5.6-luna",
			ReasoningEffort: "low",
		},
	}
	for _, profile := range profiles {
		policy.AgentProfiles[profile.Name] = profile
	}
	policy.RoleProfiles[AgentProfileRoleDiscovery] =
		"production-discovery"
	policy.RoleProfiles[AgentProfileRoleVerifier] =
		"production-verifier"
	policy.RoleProfiles[AgentProfileRoleChallenge] =
		"production-challenge"
	policy.RoleProfiles[AgentProfileRoleEscalation] =
		"production-escalation"
	policy.Swarm = ReviewSwarmPolicy{
		MinReviewers:         2,
		MaxReviewers:         2,
		MaxParallelReviewers: 1,
		TimeoutMinutes:       defaultReviewWorkerTimeout,
		Retries:              defaultReviewWorkerRetries,
		Lanes: []ReviewLane{
			{
				Name:     "contract",
				Required: true,
				Profile:  "correctness-override",
			},
			{
				Name:     "lifecycle",
				Required: true,
				Profile:  "production-discovery",
			},
		},
	}
	policy.Verification.MinVerifiers = 1
	policy.Verification.MaxVerifiers = 1
	policy.Verification.MaxParallelVerifiers = 1
	policy.Verification.TimeoutMinutes = 1
	policy.Verification.Retries = 0
	policy.Convergence.QuietRoundsRequired = 1
	policy.Convergence.MaxRounds = 2
	policy.Convergence.MaxReviewAgentsPerSHA = 20
	policy.Convergence.MaxWallTimeMinutes = 5
	policy.Escalation.AfterNonConvergingRounds = 2
	policy.Escalation.Profile = "production-escalation"
	policy.Artifacts.TimeoutSeconds = 2
	policy.Artifacts.Retries = 0
	policy.Swarm.Retries = 0
	return policy
}

func startTrackedReviewForCoordinatorHarness(
	t *testing.T,
	harness *productionReviewCoordinatorHarness,
	policy ReviewPolicy,
) (Agent, context.CancelFunc) {
	t.Helper()
	harness.bot.cfg.RepoPath = harness.reviewer.WorktreePath
	harness.bot.cfg.BaseBranch = "main"
	harness.bot.cfg.ReviewPolicy = policy
	if err := os.MkdirAll(harness.bot.cfg.LogDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(log dir) error = %v", err)
	}
	coder := &Agent{
		ID:                "tracked-coder",
		Role:              RoleCoder,
		IssueNumber:       58,
		IssueTitle:        "Integrate review swarm",
		IssueBody:         "Preserve the changed value at its consumer",
		WorktreePath:      t.TempDir(),
		BranchName:        "main",
		PRNumber:          50,
		PRTitle:           "Integrate review swarm",
		PRURL:             "https://example.test/pull/50",
		ObservedPRHeadSHA: harness.headSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now().UTC(),
	}
	if err := harness.bot.agents.Add(coder); err != nil {
		t.Fatalf("AgentManager.Add(coder) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := harness.bot.ensureReviewAgentForCoder(ctx, *coder); err != nil {
		cancel()
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	return *coder, cancel
}

func waitForTrackedReviewer(
	t *testing.T,
	harness *productionReviewCoordinatorHarness,
	coderID string,
) Agent {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		coder, ok := harness.bot.agents.Get(coderID)
		if ok && strings.TrimSpace(coder.ActiveReviewAgentID) != "" {
			if reviewer, found := harness.bot.agents.Get(
				coder.ActiveReviewAgentID,
			); found {
				return reviewer
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for tracked reviewer for coder %s", coderID)
	return Agent{}
}

func waitForTrackedReviewPublication(
	t *testing.T,
	harness *productionReviewCoordinatorHarness,
	reviewerID string,
) Agent {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		reviewer, ok := harness.bot.agents.Get(reviewerID)
		if ok && reviewer.ReviewCycle != nil &&
			reviewer.ReviewCycle.VerdictPublication != nil &&
			reviewer.ReviewCycle.VerdictPublication.Status ==
				ReviewVerdictPublicationPublished &&
			!trackedReviewLaunchPending(harness.bot, reviewer) {
			return reviewer
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf(
		"timed out waiting for tracked review publication from %s",
		reviewerID,
	)
	return Agent{}
}

func trackedReviewLaunchPending(bot *Orchestrator, reviewer Agent) bool {
	if bot == nil || reviewer.ReviewCycle == nil {
		return false
	}
	key := reviewLaunchKey(
		reviewLaunchKeyForPR(reviewer.PRNumber),
		reviewer.ReviewCycle.HeadSHA,
	)
	bot.reviewLaunchMu.Lock()
	defer bot.reviewLaunchMu.Unlock()
	_, pending := bot.pendingReviewLaunches[key]
	return pending
}

func attachTrackedCoderToCoordinator(
	t *testing.T,
	harness *productionReviewCoordinatorHarness,
) Agent {
	t.Helper()
	controlRoot := t.TempDir()
	controlRepo := filepath.Join(controlRoot, "control")
	origin := strings.TrimSpace(
		runReviewArtifactGit(
			t,
			harness.reviewer.WorktreePath,
			"remote",
			"get-url",
			"origin",
		),
	)
	runReviewArtifactGit(
		t,
		controlRoot,
		"clone",
		"--quiet",
		origin,
		controlRepo,
	)
	harness.bot.cfg.RepoPath = controlRepo
	harness.bot.cfg.BaseBranch = "main"
	harness.bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		return os.RemoveAll(worktreePath)
	}
	harness.bot.agents.mu.Lock()
	managedReviewer :=
		harness.bot.agents.agents[harness.reviewer.ID]
	managedReviewer.RuntimeHandle = RuntimeHandle{}
	harness.bot.agents.mu.Unlock()

	coder := Agent{
		ID:                  "terminal-tracked-coder",
		Role:                RoleCoder,
		IssueNumber:         58,
		WorktreePath:        t.TempDir(),
		BranchName:          "terminal-review",
		PRNumber:            50,
		PRURL:               harness.reviewer.PRURL,
		ObservedPRHeadSHA:   harness.headSHA,
		ActiveReviewAgentID: harness.reviewer.ID,
		AdoptedPR:           true,
		State:               StateWaiting,
		LastActivityTime:    time.Now().UTC(),
	}
	if err := harness.bot.agents.Add(&coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	harness.bot.agents.mu.Lock()
	harness.bot.agents.agents[harness.reviewer.ID].
		ParentAgentID = coder.ID
	harness.bot.agents.mu.Unlock()
	return coder
}

func waitForTrackedTerminalCleanup(
	t *testing.T,
	harness *productionReviewCoordinatorHarness,
	coderID string,
	reviewerID string,
) (Agent, Agent) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		coder, coderFound := harness.bot.agents.Get(coderID)
		reviewer, reviewerFound := harness.bot.agents.Get(reviewerID)
		if coderFound && reviewerFound &&
			coder.Stopped && coder.State == StateDone &&
			reviewer.Stopped && reviewer.State == StateDone {
			return coder, reviewer
		}
		time.Sleep(10 * time.Millisecond)
	}
	coder, _ := harness.bot.agents.Get(coderID)
	reviewer, _ := harness.bot.agents.Get(reviewerID)
	t.Fatalf(
		"timed out waiting for terminal cleanup: coder=%#v reviewer=%#v",
		coder,
		reviewer,
	)
	return Agent{}, Agent{}
}

func TestTrackedReviewTriggerInvokesProductionExactSHACoordinator(t *testing.T) {
	t.Run("tracked review", func(t *testing.T) {
		harness := newProductionReviewCoordinatorHarness(
			t,
			false,
			false,
		)
		coder, cancel := startTrackedReviewForCoordinatorHarness(
			t,
			harness,
			productionReviewCoordinatorPolicy(),
		)
		defer cancel()
		reviewer := waitForTrackedReviewer(t, harness, coder.ID)
		reviewer = waitForTrackedReviewPublication(
			t,
			harness,
			reviewer.ID,
		)
		if reviewer.ObservedPRHeadSHA != harness.headSHA ||
			reviewer.ReviewCycle.HeadSHA != harness.headSHA {
			t.Fatalf(
				"tracked exact SHA = reviewer:%s cycle:%s, want %s",
				reviewer.ObservedPRHeadSHA,
				reviewer.ReviewCycle.HeadSHA,
				harness.headSHA,
			)
		}
		if harness.runner.topLevelStartCount() != 0 ||
			strings.TrimSpace(reviewer.RuntimeHandle.Session) != "" {
			t.Fatalf(
				"coordinator launched top-level runtime: starts=%d handle=%#v",
				harness.runner.topLevelStartCount(),
				reviewer.RuntimeHandle,
			)
		}
		starts, _ := harness.runner.snapshot()
		if len(starts) != reviewer.ReviewCycle.Plan.InitialReviewerCount+1 {
			t.Fatalf(
				"production worker starts = %d, planned lanes plus synthesis %d",
				len(starts),
				reviewer.ReviewCycle.Plan.InitialReviewerCount+1,
			)
		}
		_, _, createRequests, comments := harness.fixture.snapshot()
		if createRequests != 1 || len(comments) != 1 {
			t.Fatalf(
				"consolidated publications = requests:%d comments:%d, want one",
				createRequests,
				len(comments),
			)
		}
	})
}

func TestConvergentReviewUnpauseReentersCoordinator(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	if err := harness.bot.PauseAgent(
		context.Background(),
		harness.reviewer.ID,
	); err != nil {
		t.Fatalf("PauseAgent() error = %v", err)
	}
	paused, ok := harness.bot.agents.Get(harness.reviewer.ID)
	if !ok || !paused.Paused ||
		strings.TrimSpace(paused.RuntimeHandle.Session) != "" {
		t.Fatalf("paused coordinator = %#v", paused)
	}

	if err := harness.bot.UnpauseAgent(
		context.Background(),
		harness.reviewer.ID,
	); err != nil {
		t.Fatalf("UnpauseAgent() error = %v", err)
	}
	harness.bot.waitForPersistedReviewCycleRecoveries()
	resumed := waitForTrackedReviewPublication(
		t,
		harness,
		harness.reviewer.ID,
	)
	if resumed.Paused || resumed.Stopped ||
		strings.TrimSpace(resumed.RuntimeHandle.Session) != "" {
		t.Fatalf("resumed coordinator = %#v", resumed)
	}
	if harness.runner.topLevelStartCount() != 0 {
		t.Fatalf(
			"enabled unpause launched %d top-level reviewer runtimes",
			harness.runner.topLevelStartCount(),
		)
	}
}

func TestConvergentReviewSteeringRejectsTopLevelRuntime(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	harness.bot.agents.mu.Lock()
	harness.bot.agents.agents[harness.reviewer.ID].
		RuntimeHandle = RuntimeHandle{}
	harness.bot.agents.mu.Unlock()

	err := harness.bot.SteerAgent(
		context.Background(),
		harness.reviewer.ID,
		"focus on lifecycle behavior",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "cannot be steered directly") {
		t.Fatalf(
			"SteerAgent() error = %v, want coordinator rejection",
			err,
		)
	}
	if harness.runner.topLevelStartCount() != 0 {
		t.Fatalf(
			"enabled steering launched %d top-level reviewer runtimes",
			harness.runner.topLevelStartCount(),
		)
	}
}

func TestConvergentReviewReconcilesTerminalPRs(
	t *testing.T,
) {
	tests := []struct {
		name    string
		tracked bool
		merged  bool
	}{
		{name: "manual closed"},
		{name: "manual merged", merged: true},
		{name: "tracked closed", tracked: true},
		{name: "tracked merged", tracked: true, merged: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newProductionReviewCoordinatorHarness(
				t,
				false,
				false,
			)
			harness.bot.cleanupWorktreeFunc = func(
				_ context.Context,
				_ string,
				worktreePath string,
				_ string,
			) error {
				return os.RemoveAll(worktreePath)
			}
			harness.bot.agents.mu.Lock()
			managedReviewer :=
				harness.bot.agents.agents[harness.reviewer.ID]
			managedReviewer.RuntimeHandle = RuntimeHandle{}
			harness.bot.agents.mu.Unlock()

			var coder *Agent
			if test.tracked {
				attached := attachTrackedCoderToCoordinator(t, harness)
				coder = &attached
			}

			state := "closed"
			if test.merged {
				state = "open"
			}
			terminalPR := coordinatorPullRequest(
				50,
				harness.baseSHA,
				harness.headSHA,
				"acme/widget",
				"acme/widget",
			)
			terminalPR.State = github.String(state)
			terminalPR.Merged = github.Bool(test.merged)
			harness.pulls.mu.Lock()
			harness.pulls.pullRequest = terminalPR
			harness.pulls.mu.Unlock()

			harness.bot.pollActiveAgent(
				context.Background(),
				harness.reviewer.ID,
				time.Now().UTC(),
			)

			reviewer, ok := harness.bot.agents.Get(
				harness.reviewer.ID,
			)
			if !ok || !reviewer.Stopped ||
				reviewer.State != StateDone {
				t.Fatalf(
					"terminal reviewer = %#v, want done and stopped",
					reviewer,
				)
			}
			if coder != nil {
				current, ok := harness.bot.agents.Get(coder.ID)
				if !ok || !current.Stopped ||
					current.State != StateDone {
					t.Fatalf(
						"terminal coder = %#v, want done and stopped",
						current,
					)
				}
			}
			starts, _ := harness.runner.snapshot()
			_, _, createRequests, comments :=
				harness.fixture.snapshot()
			if len(starts) != 0 ||
				harness.runner.topLevelStartCount() != 0 ||
				createRequests != 0 ||
				len(comments) != 0 {
				t.Fatalf(
					"terminal PR side effects = workers:%d top-level:%d writes:%d comments:%d",
					len(starts),
					harness.runner.topLevelStartCount(),
					createRequests,
					len(comments),
				)
			}
		})
	}
}

func TestTrackedConvergentReviewDefersTerminalCleanupDuringActivePass(
	t *testing.T,
) {
	tests := []struct {
		name        string
		publication bool
		merged      bool
	}{
		{name: "worker launch closed"},
		{name: "worker launch merged", merged: true},
		{name: "publication closed", publication: true},
		{name: "publication merged", publication: true, merged: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newProductionReviewCoordinatorHarness(
				t,
				false,
				false,
			)
			coder := attachTrackedCoderToCoordinator(t, harness)
			openPR := coordinatorPullRequest(
				50,
				harness.baseSHA,
				harness.headSHA,
				"acme/widget",
				"acme/widget",
			)
			openPR.State = github.String("open")
			terminalPR := coordinatorPullRequest(
				50,
				harness.baseSHA,
				harness.headSHA,
				"acme/widget",
				"acme/widget",
			)
			terminalPR.State = github.String("closed")
			if test.merged {
				terminalPR.State = github.String("open")
				terminalPR.Merged = github.Bool(true)
			}

			harness.pulls.mu.Lock()
			if test.publication {
				harness.pulls.pullRequest = openPR
				artifacts := 0
				harness.runner.afterArtifact = func(Agent) {
					artifacts++
					if artifacts != 2 {
						return
					}
					harness.pulls.mu.Lock()
					harness.pulls.pullRequest = terminalPR
					harness.pulls.mu.Unlock()
					harness.fixture.mu.Lock()
					harness.fixture.state =
						terminalPR.GetState()
					harness.fixture.merged =
						terminalPR.GetMerged()
					harness.fixture.mu.Unlock()
				}
			} else {
				harness.pulls.sequence = []*github.PullRequest{
					openPR,
					terminalPR,
				}
			}
			harness.pulls.mu.Unlock()

			runCtx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()
			_, err := harness.bot.RunConvergentReviewCycle(
				runCtx,
				harness.reviewer.ID,
			)
			if !errors.Is(
				err,
				errConvergentReviewPullRequestTerminal,
			) {
				t.Fatalf(
					"RunConvergentReviewCycle() error = %v, want terminal PR rejection",
					err,
				)
			}
			currentCoder, currentReviewer :=
				waitForTrackedTerminalCleanup(
					t,
					harness,
					coder.ID,
					harness.reviewer.ID,
				)
			if currentCoder.ActiveReviewAgentID != "" {
				t.Fatalf(
					"terminal coder retained active reviewer %q",
					currentCoder.ActiveReviewAgentID,
				)
			}
			if currentReviewer.ReviewCoordinatorLifecycle == nil ||
				currentReviewer.ReviewCoordinatorLifecycle.
					CompletedAt.IsZero() {
				t.Fatalf(
					"terminal reviewer lifecycle = %#v, want completed cleanup",
					currentReviewer.ReviewCoordinatorLifecycle,
				)
			}
			_, listRequests, createRequests, comments :=
				harness.fixture.snapshot()
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
			starts, _ := harness.runner.snapshot()
			if !test.publication && len(starts) != 0 {
				t.Fatalf(
					"terminal worker launch started %d workers",
					len(starts),
				)
			}
			if test.publication && len(starts) != 2 {
				t.Fatalf(
					"in-pass publication worker starts = %d, want 2",
					len(starts),
				)
			}
		})
	}
}

func TestTrackedReviewRejectsMismatchedRepositoryBeforeSideEffects(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	harness.pulls.pullRequest.Head.Repo.FullName =
		github.String("other/widget")
	coder, cancel := startTrackedReviewForCoordinatorHarness(
		t,
		harness,
		productionReviewCoordinatorPolicy(),
	)
	reviewer := waitForTrackedReviewer(t, harness, coder.ID)
	time.Sleep(100 * time.Millisecond)
	cancel()
	harness.bot.waitForPersistedReviewCycleRecoveries()

	reviewer, _ = harness.bot.agents.Get(reviewer.ID)
	if reviewer.ReviewCycle.Inputs != nil {
		t.Fatal("repository mismatch reached exact-SHA planning")
	}
	starts, _ := harness.runner.snapshot()
	_, _, createRequests, _ := harness.fixture.snapshot()
	if harness.runner.topLevelStartCount() != 0 ||
		len(starts) != 0 ||
		createRequests != 0 {
		t.Fatalf(
			"repository mismatch side effects = top-level:%d workers:%d writes:%d",
			harness.runner.topLevelStartCount(),
			len(starts),
			createRequests,
		)
	}
}

func TestTrackedConvergentReviewPublishesApproval(t *testing.T) {
	harness := newProductionReviewCoordinatorHarness(t, false, false)
	coder, cancel := startTrackedReviewForCoordinatorHarness(
		t,
		harness,
		productionReviewCoordinatorPolicy(),
	)
	defer cancel()
	reviewer := waitForTrackedReviewer(t, harness, coder.ID)
	reviewer = waitForTrackedReviewPublication(t, harness, reviewer.ID)
	publication := reviewer.ReviewCycle.VerdictPublication
	if publication.ResultState != ReviewCycleResultCompleteClean ||
		publication.Verdict != ReviewVerdictThumbsUp {
		t.Fatalf("approval publication = %#v", publication)
	}
	starts, _ := harness.runner.snapshot()
	if len(starts) == 0 {
		t.Fatal("review completed without starting workers")
	}
	_, _, createRequests, comments := harness.fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf("approval publications = requests:%d comments:%d, want one", createRequests, len(comments))
	}
}

func TestTrackedConvergentReviewStaleHeadNeverPublishes(t *testing.T) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	var headMu sync.Mutex
	liveHead := harness.headSHA
	successorHead := strings.Repeat("d", canonicalGitObjectIDLength)
	harness.bot.reviewHeadResolver = func(
		context.Context,
		int,
	) (string, error) {
		headMu.Lock()
		defer headMu.Unlock()
		return liveHead, nil
	}
	harness.bot.reviewSuccessorLauncher = func(
		context.Context,
		Agent,
		string,
	) error {
		return nil
	}
	var moveOnce sync.Once
	harness.runner.afterArtifact = func(Agent) {
		moveOnce.Do(func() {
			headMu.Lock()
			liveHead = successorHead
			headMu.Unlock()
		})
	}
	coder, cancel := startTrackedReviewForCoordinatorHarness(
		t,
		harness,
		productionReviewCoordinatorPolicy(),
	)
	defer cancel()
	reviewer := waitForTrackedReviewer(t, harness, coder.ID)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		reviewer, _ = harness.bot.agents.Get(reviewer.ID)
		if reviewer.ReviewCycle.Stale {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reviewer.ReviewCycle.Stale ||
		reviewer.ReviewCycle.SupersededByHeadSHA != successorHead {
		t.Fatalf(
			"stale cycle = stale:%t successor:%s, want %s",
			reviewer.ReviewCycle.Stale,
			reviewer.ReviewCycle.SupersededByHeadSHA,
			successorHead,
		)
	}
	staleObservation, err := observableReviewCycle(reviewer)
	if err != nil {
		t.Fatalf("observableReviewCycle(stale) error = %v", err)
	}
	if staleObservation.Publication.State !=
		"suppressed_stale_head" {
		t.Fatalf(
			"stale observation = result:%q publication:%#v",
			staleObservation.ResultState,
			staleObservation.Publication,
		)
	}
	harness.bot.waitForPersistedReviewCycleRecoveries()
	harness.bot.waitForStaleReviewCleanups()
	_, _, createRequests, comments := harness.fixture.snapshot()
	if createRequests != 0 || len(comments) != 0 {
		t.Fatalf(
			"stale head published requests:%d comments:%d",
			createRequests,
			len(comments),
		)
	}
}

func TestTrackedConvergentReviewPublicationRetriesOnPoll(t *testing.T) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	var failOnce sync.Once
	harness.runner.afterArtifact = func(Agent) {
		failOnce.Do(func() {
			harness.fixture.mu.Lock()
			harness.fixture.listFailures =
				reviewVerdictPublicationMaxAttempts
			harness.fixture.mu.Unlock()
		})
	}
	coder, cancel := startTrackedReviewForCoordinatorHarness(
		t,
		harness,
		productionReviewCoordinatorPolicy(),
	)
	defer cancel()
	reviewer := waitForTrackedReviewer(t, harness, coder.ID)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		reviewer, _ = harness.bot.agents.Get(reviewer.ID)
		if reviewer.ReviewCycle != nil &&
			reviewer.ReviewCycle.VerdictPublication != nil &&
			reviewer.ReviewCycle.VerdictPublication.Status ==
				ReviewVerdictPublicationPending {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if reviewer.ReviewCycle.VerdictPublication == nil ||
		reviewer.ReviewCycle.VerdictPublication.Status !=
			ReviewVerdictPublicationPending {
		t.Fatalf(
			"publication after in-call retry exhaustion = %#v, want pending",
			reviewer.ReviewCycle.VerdictPublication,
		)
	}
	harness.bot.pollActiveAgent(
		context.Background(),
		reviewer.ID,
		time.Now().UTC(),
	)
	reviewer = waitForTrackedReviewPublication(
		t,
		harness,
		reviewer.ID,
	)
	_, _, createRequests, comments := harness.fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"poll retry publications = requests:%d comments:%d, want one",
			createRequests,
			len(comments),
		)
	}
}

func TestTrackedConvergentReviewRestartBeforeFirstCheckpoint(t *testing.T) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	harness.bot.agents.mu.Lock()
	reviewer := harness.bot.agents.agents[harness.reviewer.ID]
	reviewer.RuntimeHandle = RuntimeHandle{}
	reviewer.ReviewCycle.Inputs = nil
	reviewer.ReviewCycle.Plan = nil
	harness.bot.agents.mu.Unlock()

	if err := harness.bot.reconcilePersistedReviewCycles(
		context.Background(),
	); err != nil {
		t.Fatalf("reconcilePersistedReviewCycles() error = %v", err)
	}
	reviewerSnapshot := waitForTrackedReviewPublication(
		t,
		harness,
		harness.reviewer.ID,
	)
	if reviewerSnapshot.ReviewCycle.Inputs == nil ||
		reviewerSnapshot.ReviewCycle.Plan == nil {
		t.Fatal("restart did not resume production planning checkpoints")
	}
	if harness.runner.topLevelStartCount() != 0 {
		t.Fatalf(
			"restart launched %d top-level reviewers",
			harness.runner.topLevelStartCount(),
		)
	}
}

func TestTrackedConvergentReviewRestartBeforeFirstCheckpointInvalidatesStaleHead(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	successorHead := strings.Repeat(
		"d",
		canonicalGitObjectIDLength,
	)
	harness.bot.agents.mu.Lock()
	reviewer := harness.bot.agents.agents[harness.reviewer.ID]
	reviewer.RuntimeHandle = RuntimeHandle{}
	reviewer.ReviewCycle.Inputs = nil
	reviewer.ReviewCycle.Plan = nil
	harness.bot.agents.mu.Unlock()
	harness.pulls.mu.Lock()
	harness.pulls.pullRequest.Head.SHA =
		github.String(successorHead)
	harness.pulls.mu.Unlock()
	harness.bot.cleanupWorktreeFunc = func(
		_ context.Context,
		_ string,
		worktreePath string,
		_ string,
	) error {
		return os.RemoveAll(worktreePath)
	}
	launchedSuccessorHead := ""
	harness.bot.reviewSuccessorLauncher = func(
		_ context.Context,
		_ Agent,
		headSHA string,
	) error {
		launchedSuccessorHead = headSHA
		return nil
	}

	if err := harness.bot.reconcilePersistedReviewCycles(
		context.Background(),
	); err != nil {
		t.Fatalf("reconcilePersistedReviewCycles() error = %v", err)
	}
	current, ok := harness.bot.agents.Get(harness.reviewer.ID)
	if !ok || current.ReviewCycle == nil ||
		!current.ReviewCycle.Stale ||
		current.ReviewCycle.SupersededByHeadSHA != successorHead ||
		!current.Stopped {
		t.Fatalf("stale restarted coordinator = %#v", current)
	}
	if launchedSuccessorHead != successorHead {
		t.Fatalf(
			"successor head = %q, want %q",
			launchedSuccessorHead,
			successorHead,
		)
	}
	starts, _ := harness.runner.snapshot()
	_, _, createRequests, comments := harness.fixture.snapshot()
	if len(starts) != 0 ||
		harness.runner.topLevelStartCount() != 0 ||
		createRequests != 0 ||
		len(comments) != 0 {
		t.Fatalf(
			"stale restart side effects = workers:%d top-level:%d writes:%d comments:%d",
			len(starts),
			harness.runner.topLevelStartCount(),
			createRequests,
			len(comments),
		)
	}
	harness.bot.waitForStaleReviewCleanups()
}

func reviewCoordinatorEnvironment(
	environment []string,
) map[string]string {
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, found := strings.Cut(entry, "=")
		if found {
			values[key] = value
		}
	}
	return values
}

func TestRunConvergentReviewCycleRoutesProductionWorkers(
	t *testing.T,
) {
	tests := []struct {
		name                 string
		includeCandidate     bool
		leaveCoverageGap     bool
		wantNondiscoveryRole AgentProfileRole
		wantNondiscovery     AgentProfile
		wantNondiscoveryRuns int
	}{
		{
			name:                 "verification",
			includeCandidate:     true,
			wantNondiscoveryRole: AgentProfileRoleVerifier,
			wantNondiscovery: AgentProfile{
				Name:            "production-verifier",
				Model:           "gpt-5.6-luna",
				ReasoningEffort: "medium",
			},
			wantNondiscoveryRuns: 1,
		},
		{
			name:                 "targeted challenge",
			leaveCoverageGap:     true,
			wantNondiscoveryRole: AgentProfileRoleChallenge,
			wantNondiscovery: AgentProfile{
				Name:            "production-challenge",
				Model:           "gpt-5.6-sol",
				ReasoningEffort: "xhigh",
			},
			wantNondiscoveryRuns: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newProductionReviewCoordinatorHarness(
				t,
				test.includeCandidate,
				test.leaveCoverageGap,
			)
			cycle, err := harness.bot.RunConvergentReviewCycle(
				context.Background(),
				harness.reviewer.ID,
			)
			if err != nil {
				t.Fatalf(
					"RunConvergentReviewCycle() error = %v",
					err,
				)
			}
			if cycle.Convergence == nil ||
				cycle.Convergence.Status != ReviewConvergenceConverged {
				t.Fatalf(
					"convergence = %#v, want converged",
					cycle.Convergence,
				)
			}
			if cycle.VerdictPublication == nil {
				t.Fatal("terminal production cycle has no verdict publication")
			}
			if cycle.EscalationTransition != nil {
				t.Fatalf(
					"unexpected escalation transition = %#v",
					cycle.EscalationTransition,
				)
			}

			starts, maxActive := harness.runner.snapshot()
			if maxActive > cycle.Policy.Swarm.MaxParallelReviewers {
				t.Fatalf(
					"maximum parallel starts = %d, configured maximum = %d",
					maxActive,
					cycle.Policy.Swarm.MaxParallelReviewers,
				)
			}
			discoveryStarts := 0
			nondiscoveryStarts := 0
			ordinaryStartsByPass := make(map[string]int)
			synthesisSeenByPass := make(map[string]bool)
			for _, start := range starts {
				environment := reviewCoordinatorEnvironment(
					start.Environment,
				)
				role := AgentProfileRole(
					environment[reviewWorkerEnvRole],
				)
				if got := environment[reviewWorkerEnvRepo]; got !=
					"acme/widget" {
					t.Fatalf(
						"worker repository = %q, want acme/widget",
						got,
					)
				}
				if got := environment[reviewWorkerEnvPRNumber]; got != "50" {
					t.Fatalf("worker PR = %q, want 50", got)
				}
				if got := environment[reviewWorkerEnvHeadSHA]; got !=
					harness.headSHA {
					t.Fatalf(
						"worker exact SHA = %q, want %q",
						got,
						harness.headSHA,
					)
				}
				if start.Agent.WorktreePath !=
					environment[reviewWorkerEnvWorktree] ||
					start.Agent.ID !=
						environment[reviewWorkerEnvOwnerID] ||
					environment[reviewWorkerEnvAttempt] != "1" {
					t.Fatalf(
						"worker ownership environment = %#v agent=%#v",
						environment,
						start.Agent,
					)
				}
				if _, found := environment["GH_TOKEN"]; found {
					t.Fatal("child worker received GH_TOKEN")
				}
				if _, found := environment["GITHUB_TOKEN"]; found {
					t.Fatal("child worker received GITHUB_TOKEN")
				}
				gitHubConfigEntries, err := os.ReadDir(
					environment["GH_CONFIG_DIR"],
				)
				if err != nil {
					t.Fatalf(
						"ReadDir(worker GitHub config) error = %v",
						err,
					)
				}
				if len(gitHubConfigEntries) != 0 {
					t.Fatalf(
						"worker GitHub config contains %d entries",
						len(gitHubConfigEntries),
					)
				}

				switch role {
				case AgentProfileRoleDiscovery:
					discoveryStarts++
					lane := environment[reviewWorkerEnvLane]
					pass := environment[reviewWorkerEnvPass]
					if lane == reviewSynthesisLane {
						if synthesisSeenByPass[pass] ||
							ordinaryStartsByPass[pass] != len(cycle.Plan.SelectedLanes) {
							t.Fatalf(
								"synthesis started before pass %s lanes completed: ordinary=%d selected=%d",
								pass,
								ordinaryStartsByPass[pass],
								len(cycle.Plan.SelectedLanes),
							)
						}
						synthesisSeenByPass[pass] = true
					} else {
						if synthesisSeenByPass[pass] {
							t.Fatalf("ordinary lane %q started after pass %s synthesis", lane, pass)
						}
						ordinaryStartsByPass[pass]++
					}
					want := AgentProfile{
						Name:            "production-discovery",
						Model:           "gpt-5.6-terra",
						ReasoningEffort: "low",
					}
					if lane == "contract" {
						want = AgentProfile{
							Name:            "correctness-override",
							Model:           "gpt-5.6-luna",
							ReasoningEffort: "low",
						}
					}
					if start.Agent.RuntimeProfile != want {
						t.Fatalf(
							"discovery lane %q profile = %#v, want %#v",
							lane,
							start.Agent.RuntimeProfile,
							want,
						)
					}
				case test.wantNondiscoveryRole:
					nondiscoveryStarts++
					if start.Agent.RuntimeProfile !=
						test.wantNondiscovery {
						t.Fatalf(
							"%s profile = %#v, want %#v",
							role,
							start.Agent.RuntimeProfile,
							test.wantNondiscovery,
						)
					}
				default:
					t.Fatalf("unexpected production worker role %q", role)
				}
			}
			wantDiscoveryStarts := len(cycle.Convergence.Rounds) *
				(len(cycle.Plan.SelectedLanes) + 1)
			if discoveryStarts != wantDiscoveryStarts {
				t.Fatalf(
					"discovery starts = %d, want %d across %d convergence rounds",
					discoveryStarts,
					wantDiscoveryStarts,
					len(cycle.Convergence.Rounds),
				)
			}
			if nondiscoveryStarts != test.wantNondiscoveryRuns {
				t.Fatalf(
					"%s starts = %d, want %d",
					test.wantNondiscoveryRole,
					nondiscoveryStarts,
					test.wantNondiscoveryRuns,
				)
			}
			for index, round := range cycle.Convergence.Rounds {
				if round.DiscoveryPass != index+1 {
					t.Fatalf(
						"convergence round %d discovery pass = %d",
						index+1,
						round.DiscoveryPass,
					)
				}
			}
			if len(cycle.WorkerOwnerships) != len(starts) {
				t.Fatalf(
					"durable reservations = %d, launches = %d",
					len(cycle.WorkerOwnerships),
					len(starts),
				)
			}
			currentReviewer, ok := harness.bot.agents.Get(
				harness.reviewer.ID,
			)
			if !ok {
				t.Fatal("production coordinator disappeared after launch")
			}
			observation, err := observableReviewCycle(currentReviewer)
			if err != nil {
				t.Fatalf(
					"observableReviewCycle() error = %v",
					err,
				)
			}
			if observation.Counts.Reserved != len(starts) ||
				observation.Counts.Completed != len(starts) ||
				observation.Counts.Running != 0 ||
				observation.Counts.Failed != 0 ||
				observation.Counts.Cancelled != 0 ||
				!observation.WithinBounds ||
				observation.Counts.MaxReviewerParallelObserved >
					cycle.Policy.Swarm.MaxParallelReviewers ||
				observation.Counts.MaxVerifierParallelObserved >
					cycle.Policy.Verification.
						MaxParallelVerifiers {
				t.Fatalf(
					"production worker accounting = %#v, starts=%d",
					observation.Counts,
					len(starts),
				)
			}
			if observation.ResultState !=
				ReviewCycleResultCompleteClean ||
				observation.Publication.State !=
					string(ReviewVerdictPublicationPublished) ||
				observation.Publication.Identity !=
					cycle.VerdictPublication.ID {
				t.Fatalf(
					"terminal observation = result:%q publication:%#v",
					observation.ResultState,
					observation.Publication,
				)
			}
			startedProfiles := make(
				map[string]AgentProfile,
				len(starts),
			)
			for _, start := range starts {
				startedProfiles[start.Agent.ID] =
					start.Agent.RuntimeProfile
			}
			for _, ownership := range cycle.WorkerOwnerships {
				started, found := startedProfiles[ownership.OwnerID]
				if !found {
					t.Fatalf(
						"durable worker %s did not reach the real launcher",
						ownership.OwnerID,
					)
				}
				profile, model, effort :=
					reviewWorkerRoutingValues(started)
				if ownership.Profile != profile ||
					ownership.Model != model ||
					ownership.ReasoningEffort != effort ||
					ownership.Lifecycle !=
						ReviewWorkerCompleted {
					t.Fatalf(
						"durable launch routing = %#v, launcher profile = %#v",
						ownership,
						started,
					)
				}
			}

			restarted := &Orchestrator{
				cfg:    harness.bot.cfg,
				agents: NewAgentManager(),
			}
			if err := restarted.loadPersistedAgentState(); err != nil {
				t.Fatalf(
					"loadPersistedAgentState() error = %v",
					err,
				)
			}
			restoredReviewer, ok := restarted.agents.Get(
				harness.reviewer.ID,
			)
			if !ok {
				t.Fatal("restart dropped production coordinator accounting")
			}
			restoredObservation, err :=
				observableReviewCycle(restoredReviewer)
			if err != nil {
				t.Fatalf(
					"observableReviewCycle(restart) error = %v",
					err,
				)
			}
			if !reflect.DeepEqual(
				restoredObservation,
				observation,
			) {
				t.Fatalf(
					"restart changed launch accounting:\n got: %#v\nwant: %#v",
					restoredObservation,
					observation,
				)
			}

			_, err = harness.bot.RunConvergentReviewCycle(
				context.Background(),
				harness.reviewer.ID,
			)
			if err != nil {
				t.Fatalf(
					"RunConvergentReviewCycle(restart) error = %v",
					err,
				)
			}
			afterRestart, _ := harness.runner.snapshot()
			if len(afterRestart) != len(starts) {
				t.Fatalf(
					"restart launches = %d, want unchanged %d",
					len(afterRestart),
					len(starts),
				)
			}
			_, _, createRequests, comments := harness.fixture.snapshot()
			if createRequests != 1 || len(comments) != 1 {
				t.Fatalf(
					"consolidated publications = requests:%d comments:%d, want one",
					createRequests,
					len(comments),
				)
			}
		})
	}
}

func TestRunConvergentReviewCycleExecutesRediscoveryForEveryQuietRound(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(t, true, false)

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("RunConvergentReviewCycle() error = %v", err)
	}
	if result.Convergence == nil ||
		result.Convergence.Status != ReviewConvergenceConverged ||
		len(result.Convergence.Rounds) != 2 {
		t.Fatalf("convergence = %#v, want material round then quiet round", result.Convergence)
	}
	for index, round := range result.Convergence.Rounds {
		wantOutcome := ReviewConvergenceRoundMaterial
		if index == 1 {
			wantOutcome = ReviewConvergenceRoundQuiet
		}
		if round.DiscoveryPass != index+1 || round.Outcome != wantOutcome {
			t.Fatalf("round %d = %#v", index+1, round)
		}
	}

	starts, _ := harness.runner.snapshot()
	discoveryStarts := 0
	for _, start := range starts {
		role := reviewCoordinatorEnvironment(start.Environment)[reviewWorkerEnvRole]
		if AgentProfileRole(role) == AgentProfileRoleDiscovery {
			discoveryStarts++
		}
	}
	wantDiscoveryStarts := 2 * (len(result.Plan.SelectedLanes) + 1)
	if discoveryStarts != wantDiscoveryStarts {
		t.Fatalf(
			"discovery starts = %d, want %d for two full-diff passes",
			discoveryStarts,
			wantDiscoveryStarts,
		)
	}
}

func TestRunConvergentReviewCycleDoesNotRediscoverAfterRecoveryStopFailure(
	t *testing.T,
) {
	harness := newReviewConvergenceTestHarness(t, 1, true)
	assignment := queueOneReviewVerification(t, harness)
	ownership, store := bindConvergenceWorker(
		t,
		harness,
		AgentProfileRoleVerifier,
		assignment.Round,
		assignment.Lane,
		func(ownership ReviewWorkerOwnership) error {
			return harness.bot.markReviewVerificationRunning(
				harness.reviewer.ID,
				assignment.ID,
				&ownership,
			)
		},
	)
	artifactPath := filepath.Join(
		store.directory,
		reviewArtifactFilename(ownership),
	)
	if err := os.WriteFile(artifactPath, []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile(invalid recovery artifact) error = %v", err)
	}

	harness.agents.mu.Lock()
	storedReviewer := harness.agents.agents[harness.reviewer.ID]
	storedReviewer.WorktreePath = harness.ownership.WorktreePath
	harness.agents.agents[harness.reviewer.ID] = storedReviewer
	harness.reviewer = cloneAgent(storedReviewer)
	harness.agents.mu.Unlock()
	current, _ := harness.agents.Get(harness.reviewer.ID)
	baseSHA := current.ReviewCycle.Inputs.BaseSHA
	headSHA := current.ReviewCycle.HeadSHA
	harness.bot.reviewCoordinatorGit = &reviewCoordinatorGitFixture{
		origin:            "git@github.com:acme/widget.git",
		head:              headSHA,
		checkoutOverrides: make(map[string]reviewCoordinatorCheckoutFixture),
	}
	harness.bot.reviewCoordinatorPullRequests =
		&reviewCoordinatorPullRequestFixture{
			pullRequest: coordinatorPullRequest(
				50,
				baseSHA,
				headSHA,
				"acme/widget",
				"acme/widget",
			),
		}
	harness.bot.reviewCoordinatorRuns = make(map[string]struct{})
	stopFailure := errors.New("injected stop failure")
	runner := &reviewCycleRecoveryMatrixRunner{
		bot: harness.bot,
		live: map[string]bool{
			ownership.SessionName: true,
		},
		stopError: stopFailure,
	}
	harness.bot.runner = runner

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if !errors.Is(err, stopFailure) {
		t.Fatalf("RunConvergentReviewCycle() error = %v", err)
	}
	if started := runner.startSnapshot(); len(started) != 0 {
		t.Fatalf("recovery stop failure launched rediscovery: %#v", started)
	}
	if len(result.DiscoveryPasses) != 1 {
		t.Fatalf(
			"discovery passes after recovery stop failure = %d, want 1",
			len(result.DiscoveryPasses),
		)
	}
	if !reviewCycleHasActiveRound(&result) {
		t.Fatal("recovery stop failure completed the active round")
	}
	storedOwnership, found := reviewWorkerOwnershipByOwnerID(
		&result,
		ownership.OwnerID,
	)
	if !found || storedOwnership.Lifecycle != ReviewWorkerRunning {
		t.Fatalf("ownership after recovery stop failure = %#v", storedOwnership)
	}
	current, _ = harness.agents.Get(harness.reviewer.ID)
	if current.State != StateWorking || current.Stopped {
		t.Fatalf("coordinator after recovery stop failure = %#v", current)
	}
}

func TestRunConvergentReviewCycleVerifiesSynthesisOnlyCandidate(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(t, true, false)
	harness.runner.candidateLane = reviewSynthesisLane
	harness.runner.verificationOutcome = ReviewVerificationConfirmed

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("RunConvergentReviewCycle() error = %v", err)
	}
	if result.Convergence == nil ||
		result.Convergence.Status != ReviewConvergenceChangesRequired ||
		len(result.CanonicalFindings) != 1 ||
		len(result.Convergence.FindingVerifications) != 1 {
		t.Fatalf(
			"synthesis candidate result = status:%v findings:%d verifications:%d",
			result.Convergence.Status,
			len(result.CanonicalFindings),
			len(result.Convergence.FindingVerifications),
		)
	}
	finding := result.CanonicalFindings[0]
	if len(finding.Provenance) == 0 ||
		finding.Provenance[0].Lane != reviewSynthesisLane {
		t.Fatalf("synthesis candidate provenance = %#v", finding.Provenance)
	}
	verification := result.Convergence.FindingVerifications[0]
	if verification.FindingID != finding.ID ||
		verification.Status != ReviewFindingVerificationConfirmed ||
		len(verification.VerifierWorkerIDs) != 1 {
		t.Fatalf("synthesis candidate verification = %#v", verification)
	}

	starts, _ := harness.runner.snapshot()
	synthesisStarts := 0
	for _, start := range starts {
		environment := reviewCoordinatorEnvironment(start.Environment)
		if environment[reviewWorkerEnvRole] == string(AgentProfileRoleDiscovery) &&
			environment[reviewWorkerEnvLane] == reviewSynthesisLane {
			synthesisStarts++
		}
	}
	if synthesisStarts != 1 {
		t.Fatalf("synthesis starts = %d, want one decisive pass", synthesisStarts)
	}
}

func TestRunConvergentReviewCyclePublishesPartialResultAfterManualDiscoveryFailure(t *testing.T) {
	harness := newProductionReviewCoordinatorHarness(t, false, false)
	harness.runner.failRole = AgentProfileRoleDiscovery

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("RunConvergentReviewCycle() error = %v", err)
	}
	if result.Convergence == nil || len(result.Convergence.Rounds) != 1 {
		t.Fatalf("failed discovery convergence = %#v, want one completed round", result.Convergence)
	}
	starts, _ := harness.runner.snapshot()
	challengeStarts := 0
	for _, start := range starts {
		role := AgentProfileRole(
			reviewCoordinatorEnvironment(start.Environment)[reviewWorkerEnvRole],
		)
		if role == AgentProfileRoleChallenge {
			challengeStarts++
		}
	}
	if challengeStarts != 1 {
		t.Fatalf("independent challenge starts = %d, want 1", challengeStarts)
	}
	_, _, _, comments := harness.fixture.snapshot()
	if len(comments) != 1 {
		t.Fatalf("partial review reports posted = %d, want 1: %#v", len(comments), comments)
	}
	body := comments[0].GetBody()
	if result.VerdictPublication == nil ||
		result.VerdictPublication.ResultState != ReviewCycleResultPartialNoFindings ||
		body != result.VerdictPublication.Body ||
		!strings.Contains(body, "Failed assignments") ||
		!strings.Contains(body, "discovery pass 1 / lifecycle") {
		t.Fatalf("partial discovery report = %#v, body %q", result.VerdictPublication, body)
	}
	if strings.Contains(body, "operations-tests") {
		t.Fatalf(
			"partial discovery report leaked worker profile detail: %q",
			body,
		)
	}
}

func TestRunConvergentReviewCyclePublishesAcceptedWorkAfterDiscoveryFailure(t *testing.T) {
	harness := newProductionReviewCoordinatorHarness(t, false, false)
	harness.runner.afterArtifact = func(Agent) {
		harness.runner.failRole = AgentProfileRoleDiscovery
	}

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(), harness.reviewer.ID,
	)
	if err != nil || result.VerdictPublication == nil {
		t.Fatalf("partial discovery result = %#v, %v", result, err)
	}
	publication := result.VerdictPublication
	if publication.ResultState != ReviewCycleResultPartialNoFindings ||
		publication.Verdict != "" ||
		!strings.Contains(publication.Body, "accepted 1 trusted result") ||
		!strings.Contains(publication.Body, ": failed/start") {
		t.Fatalf("partial discovery publication = %#v", publication)
	}
	_, _, _, comments := harness.fixture.snapshot()
	if len(comments) != 1 || comments[0].GetBody() != publication.Body {
		t.Fatalf("partial discovery comments = %#v", comments)
	}
}

// TestRunConvergentReviewCycleRecordsPartialResultForCoderAfterDiscoveryFailure
// guards against ensureReviewAgentForCoder relaunching a fresh coordinator for
// the same head forever: a completed partial result is deterministic for the
// same code and policy, so it must retire without scheduling an automatic retry.
func TestRunConvergentReviewCycleRecordsPartialResultForCoderAfterDiscoveryFailure(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(t, false, false)
	harness.runner.failRole = AgentProfileRoleDiscovery

	coder := &Agent{
		ID:                "production-review-coordinator-coder",
		Role:              RoleCoder,
		PRNumber:          harness.reviewer.PRNumber,
		ObservedPRHeadSHA: harness.headSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now().UTC(),
	}
	if err := harness.bot.agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	harness.bot.agents.mu.Lock()
	harness.bot.agents.agents[harness.reviewer.ID].ParentAgentID = coder.ID
	harness.bot.agents.mu.Unlock()
	attachRunningReviewLaunchAttempt(
		t,
		harness.bot.agents,
		coder.ID,
		harness.reviewer.ReviewCycle,
		harness.reviewer.ID,
	)

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("RunConvergentReviewCycle() error = %v", err)
	}
	if result.VerdictPublication == nil ||
		result.VerdictPublication.ResultState != ReviewCycleResultPartialNoFindings {
		t.Fatalf("partial discovery result = %#v", result.VerdictPublication)
	}

	updatedCoder, ok := harness.bot.agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	retry, found := reviewLaunchAttemptForHead(updatedCoder, harness.headSHA)
	if !found || retry.Attempt != 1 || !retry.RetryAfter.IsZero() ||
		retry.Failure == nil ||
		retry.Failure.Kind != DurableLaunchFailureIncomplete {
		t.Fatalf("review launch attempt = %#v, want terminal partial result", retry)
	}
	_, _, _, comments := harness.fixture.snapshot()
	if len(comments) != 1 {
		t.Fatalf("partial review reports posted = %d, want 1: %#v", len(comments), comments)
	}
	if comments[0].GetBody() != result.VerdictPublication.Body {
		t.Fatalf("partial review report body = %q", comments[0].GetBody())
	}
	if strings.Contains(comments[0].GetBody(), "operations-tests") {
		t.Fatalf(
			"partial review report leaked worker profile detail: %q",
			comments[0].GetBody(),
		)
	}
	if err := harness.bot.ensureReviewAgentForCoder(
		context.Background(),
		updatedCoder,
	); err != nil {
		t.Fatalf("ensureReviewAgentForCoder() error = %v", err)
	}
	starts, _ := harness.runner.snapshot()
	for _, start := range starts {
		if strings.TrimSpace(start.Agent.ParentAgentID) == coder.ID &&
			start.Agent.ID != harness.reviewer.ID {
			t.Fatalf(
				"ensureReviewAgentForCoder() relaunched a reviewer for the same head after an incomplete review: %s",
				start.Agent.ID,
			)
		}
	}
}

// TestPublishAndSnapshotConvergentReviewRetiresCoderLinkedReviewerOnFinalNonPublishableResult
// is the regression test for issue #131: a coder-linked reviewer whose
// convergence reaches a final result with no publishable verdict (a
// LimitTransition/budget exhaustion here; unresolved and max_rounds take
// the identical path through the same switch case in
// publishAndSnapshotConvergentReview) must still be retired and the coder
// unblocked -- not left active forever with no future poll path able to
// advance it. Confirmed this is already fixed on main (by 395397e/b7f882f,
// both predating #131's filing) rather than actually broken; this test
// closes the coverage gap so a regression can't silently reintroduce it.
// TestRunConvergentReviewCycleRecordsPartialResultForCoderAfterDiscoveryFailure
// above covers the sibling call site in failAndSnapshotConvergentReview
// (discovery failing outright); this one is specifically about the
// terminal-non-publishable branch inside publishAndSnapshotConvergentReview
// itself, which #131 describes.
func TestPublishAndSnapshotConvergentReviewRetiresCoderLinkedReviewerOnFinalNonPublishableResult(
	t *testing.T,
) {
	agents := NewAgentManager()
	cycle := newReviewLimitTestCycle(t, nil)
	maximum := uint64(time.Duration(cycle.Policy.Convergence.MaxWallTimeMinutes) * time.Minute / time.Millisecond)
	cycle.LimitTransition = &ReviewLimitTransition{
		SchemaVersion:     reviewLimitTransitionSchemaVersion,
		Kind:              ReviewLimitWallTime,
		TransitionedAt:    time.Now(),
		Actual:            maximum + 500,
		Maximum:           maximum,
		Unit:              "milliseconds",
		Action:            cycle.Policy.FailureActions.BudgetExhaustion,
		Outcome:           reviewLimitOutcomeForAction(cycle.Policy.FailureActions.BudgetExhaustion),
		Reason:            "review wall time reached snapshotted maximum",
		MetricEventCount:  1,
		MetricEventDigest: strings.Repeat("a", 64),
	}
	if reviewCycleHasPublishableReport(cycle) {
		t.Fatal("test fixture is publishable; a LimitTransition should never be, per ApprovalEligible()")
	}
	if !reviewCycleHasTerminalVerdict(cycle) {
		t.Fatal("test fixture is not terminal; a LimitTransition should always be")
	}

	coder := &Agent{
		ID:                "coder-terminal-non-publishable",
		Role:              RoleCoder,
		PRNumber:          131,
		ObservedPRHeadSHA: cycle.HeadSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now().UTC(),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}
	reviewer := &Agent{
		ID:               "reviewer-terminal-non-publishable",
		Role:             RoleReviewer,
		ParentAgentID:    coder.ID,
		PRNumber:         coder.PRNumber,
		State:            StateWorking,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	if !agents.SetActiveReviewAgent(coder.ID, reviewer.ID) {
		t.Fatal("SetActiveReviewAgent() = false")
	}
	attachRunningReviewLaunchAttempt(
		t,
		agents,
		coder.ID,
		cycle,
		reviewer.ID,
	)

	bot := &Orchestrator{agents: agents}

	if _, err := bot.publishAndSnapshotConvergentReview(
		context.Background(),
		reviewer.ID,
		nil,
	); err == nil {
		t.Fatal("publishAndSnapshotConvergentReview() error = nil, want the synthesized no-publishable-verdict error")
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateErrored {
		t.Fatalf(
			"reviewer state after terminal non-publishable outcome = %s, want %s (retired, not left active forever)",
			updatedReviewer.State, StateErrored,
		)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if strings.TrimSpace(updatedCoder.ActiveReviewAgentID) != "" {
		t.Fatalf(
			"coder ActiveReviewAgentID = %q after reviewer retirement, want cleared",
			updatedCoder.ActiveReviewAgentID,
		)
	}
	retry, found := reviewLaunchAttemptForHead(updatedCoder, cycle.HeadSHA)
	if !found || retry.Attempt != 1 || !retry.RetryAfter.IsZero() ||
		retry.Failure == nil ||
		retry.Failure.Kind != DurableLaunchFailureIncomplete ||
		retry.Failure.Retryable {
		t.Fatalf("coder review launch attempt = %#v, want terminal non-retryable result", retry)
	}
}

// TestPublishAndSnapshotConvergentReviewRetiresDespiteLedgerPersistFailure
// is the regression test for issue #133: publishAndSnapshotConvergentReview
// used to return immediately if persistCompletedReviewLedger failed,
// before ever reaching the switch statement that publishes a verdict or
// retires a non-publishable reviewer -- leaving the reviewer permanently
// stuck, since nothing else retries this exact step on its own schedule
// and a deterministic failure (like this one) reproduces identically on
// every future attempt.
//
// Uses a genuine ledger conflict (a rewritten Description for the same
// requirement ID -- a real identity change, still correctly rejected even
// after #134's fix, see TestTransitionReviewLedgerRejectsPlannedCoverageDescriptionRewrite)
// rather than a #134-style false positive, since the fix under test here
// must hold regardless of *why* the ledger update failed.
func TestPublishAndSnapshotConvergentReviewRetiresDespiteLedgerPersistFailure(
	t *testing.T,
) {
	const (
		requirementID = "call-path:internal/lifecycle.go"
		path          = "internal/lifecycle.go"
	)
	priorRequirements := []ReviewCoverageRequirement{{
		ID:          requirementID,
		Kind:        ReviewCoverageCallPath,
		Description: path,
	}}
	prior, err := transitionReviewLedger(
		nil,
		testReviewLedgerDelta(testLedgerBaseSHA, testLedgerFirstSHA, path),
		priorRequirements,
		nil,
		[]ReviewLedgerCoverageObservation{{
			Requirement: priorRequirements[0],
			Claim: ReviewCoverageClaim{
				RequirementID: requirementID,
				Kind:          ReviewCoverageCallPath,
				Status:        ReviewCoverageCovered,
				Evidence: []ReviewEvidence{{
					Summary: "covered immutable planned requirement",
					Path:    path,
				}},
			},
		}},
	)
	if err != nil {
		t.Fatalf("initial transitionReviewLedger() error = %v", err)
	}

	agents := NewAgentManager()
	coder := &Agent{
		ID:                "coder-ledger-persist-failure",
		Role:              RoleCoder,
		PRNumber:          133,
		ObservedPRHeadSHA: testLedgerSecondSHA,
		State:             StateWaiting,
		LastActivityTime:  time.Now().UTC(),
		ReviewLedger:      cloneReviewLedger(&prior.Ledger),
	}
	if err := agents.Add(coder); err != nil {
		t.Fatalf("Add(coder) error = %v", err)
	}

	cycle := newReviewLimitTestCycle(t, nil)
	cycle.HeadSHA = testLedgerSecondSHA
	cycle.PriorReviewLedger = cloneReviewLedger(&prior.Ledger)
	inputs := testReviewLedgerDelta(testLedgerFirstSHA, testLedgerSecondSHA, path)
	cycle.ReviewLedgerInputs = &inputs
	cycle.ReviewLedgerPlan = &ReviewPlan{
		BaseSHA: testLedgerFirstSHA,
		HeadSHA: testLedgerSecondSHA,
		CoverageRequirements: []ReviewCoverageRequirement{{
			ID:          requirementID,
			Kind:        ReviewCoverageCallPath,
			Description: "rewritten exact-SHA plan meaning",
		}},
	}
	cycle.Convergence = &ReviewConvergenceState{}
	maximum := uint64(time.Duration(cycle.Policy.Convergence.MaxWallTimeMinutes) * time.Minute / time.Millisecond)
	cycle.LimitTransition = &ReviewLimitTransition{
		SchemaVersion:     reviewLimitTransitionSchemaVersion,
		Kind:              ReviewLimitWallTime,
		TransitionedAt:    time.Now(),
		Actual:            maximum + 500,
		Maximum:           maximum,
		Unit:              "milliseconds",
		Action:            cycle.Policy.FailureActions.BudgetExhaustion,
		Outcome:           reviewLimitOutcomeForAction(cycle.Policy.FailureActions.BudgetExhaustion),
		Reason:            "review wall time reached snapshotted maximum",
		MetricEventCount:  1,
		MetricEventDigest: strings.Repeat("a", 64),
	}
	if reviewCycleHasPublishableReport(cycle) {
		t.Fatal("test fixture is publishable; a LimitTransition should never be")
	}
	if !reviewCycleHasTerminalVerdict(cycle) {
		t.Fatal("test fixture is not terminal")
	}

	reviewer := &Agent{
		ID:               "reviewer-ledger-persist-failure",
		Role:             RoleReviewer,
		ParentAgentID:    coder.ID,
		PRNumber:         coder.PRNumber,
		State:            StateWorking,
		ReviewCycle:      cycle,
		LastActivityTime: time.Now().UTC(),
	}
	if err := agents.Add(reviewer); err != nil {
		t.Fatalf("Add(reviewer) error = %v", err)
	}
	if !agents.SetActiveReviewAgent(coder.ID, reviewer.ID) {
		t.Fatal("SetActiveReviewAgent() = false")
	}
	attachRunningReviewLaunchAttempt(
		t,
		agents,
		coder.ID,
		cycle,
		reviewer.ID,
	)

	bot := &Orchestrator{agents: agents}

	_, err = bot.publishAndSnapshotConvergentReview(
		context.Background(),
		reviewer.ID,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "conflicts with the exact-SHA plan") {
		t.Fatalf(
			"publishAndSnapshotConvergentReview() error = %v, want a ledger conflict error",
			err,
		)
	}

	updatedReviewer, ok := agents.Get(reviewer.ID)
	if !ok {
		t.Fatalf("reviewer %s not found", reviewer.ID)
	}
	if updatedReviewer.State != StateErrored {
		t.Fatalf(
			"reviewer state after ledger persist failure = %s, want %s (retired despite the failure, not left stuck)",
			updatedReviewer.State, StateErrored,
		)
	}

	updatedCoder, ok := agents.Get(coder.ID)
	if !ok {
		t.Fatalf("coder %s not found", coder.ID)
	}
	if strings.TrimSpace(updatedCoder.ActiveReviewAgentID) != "" {
		t.Fatalf(
			"coder ActiveReviewAgentID = %q after reviewer retirement, want cleared",
			updatedCoder.ActiveReviewAgentID,
		)
	}
	retry, found := reviewLaunchAttemptForHead(
		updatedCoder,
		testLedgerSecondSHA,
	)
	if !found || retry.Failure == nil ||
		retry.Failure.Kind != DurableLaunchFailureIncomplete {
		t.Fatalf(
			"coder review launch attempt = %#v, want incomplete failure",
			retry,
		)
	}
	// The deliberate cost of this fix: continuity for this round is
	// sacrificed, so the coder's ledger must remain exactly at its
	// pre-failure state, not partially or incorrectly advanced.
	if updatedCoder.ReviewLedger == nil || updatedCoder.ReviewLedger.HeadSHA != testLedgerFirstSHA {
		t.Fatalf(
			"coder ReviewLedger after failed persist = %#v, want unchanged at head %q",
			updatedCoder.ReviewLedger, testLedgerFirstSHA,
		)
	}
}

func TestRunConvergentReviewCyclePersistsExactSHAPlanBeforeLaunch(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	harness.bot.agents.mu.Lock()
	cycle := harness.bot.agents.agents[harness.reviewer.ID].ReviewCycle
	cycle.Inputs = nil
	cycle.Plan = nil
	harness.bot.agents.mu.Unlock()

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err != nil {
		t.Fatalf("RunConvergentReviewCycle() error = %v", err)
	}
	if result.Inputs == nil ||
		result.Inputs.BaseSHA != harness.baseSHA ||
		result.Inputs.HeadSHA != harness.headSHA ||
		result.Plan == nil ||
		result.Plan.PolicyFingerprint != result.PolicyFingerprint {
		t.Fatalf(
			"persisted exact-SHA plan = inputs:%#v plan:%#v",
			result.Inputs,
			result.Plan,
		)
	}
	starts, _ := harness.runner.snapshot()
	if len(starts) != result.Plan.InitialReviewerCount+1 {
		t.Fatalf(
			"planned reviewer count plus synthesis = %d, production starts = %d",
			result.Plan.InitialReviewerCount+1,
			len(starts),
		)
	}
}

func TestRunConvergentReviewCycleFailsClosedBeforeWorkerStart(
	t *testing.T,
) {
	tests := []struct {
		name      string
		mutate    func(*productionReviewCoordinatorHarness)
		wantError string
	}{
		{
			name: "configured repository origin",
			mutate: func(harness *productionReviewCoordinatorHarness) {
				harness.git.origin =
					"git@github.com:other/widget.git"
			},
			wantError: "repository mismatch",
		},
		{
			name: "configured repository host",
			mutate: func(harness *productionReviewCoordinatorHarness) {
				harness.git.origin =
					"https://gitlab.example/acme/widget.git"
			},
			wantError: "repository mismatch",
		},
		{
			name: "coordinator checkout SHA",
			mutate: func(harness *productionReviewCoordinatorHarness) {
				harness.git.head = strings.Repeat("f", 40)
			},
			wantError: "checkout SHA mismatch",
		},
		{
			name: "pull request number",
			mutate: func(harness *productionReviewCoordinatorHarness) {
				harness.pulls.pullRequest.Number = github.Int(51)
			},
			wantError: "PR identity mismatch",
		},
		{
			name: "pull request head repository",
			mutate: func(harness *productionReviewCoordinatorHarness) {
				harness.pulls.pullRequest.Head.Repo.FullName =
					github.String("other/widget")
			},
			wantError: "repository mismatch",
		},
		{
			name: "pull request base repository",
			mutate: func(harness *productionReviewCoordinatorHarness) {
				harness.pulls.pullRequest.Base.Repo.FullName =
					github.String("other/widget")
			},
			wantError: "repository mismatch",
		},
		{
			name: "pull request head SHA",
			mutate: func(harness *productionReviewCoordinatorHarness) {
				harness.pulls.pullRequest.Head.SHA =
					github.String(strings.Repeat("e", 40))
			},
			wantError: "review cycle head is stale",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newProductionReviewCoordinatorHarness(
				t,
				false,
				false,
			)
			harness.bot.reviewSuccessorLauncher = func(
				context.Context,
				Agent,
				string,
			) error {
				return nil
			}
			defer harness.bot.waitForStaleReviewCleanups()
			test.mutate(harness)
			_, err := harness.bot.RunConvergentReviewCycle(
				context.Background(),
				harness.reviewer.ID,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantError) {
				t.Fatalf(
					"RunConvergentReviewCycle() error = %v, want %q",
					err,
					test.wantError,
				)
			}
			starts, _ := harness.runner.snapshot()
			if len(starts) != 0 {
				t.Fatalf(
					"boundary mismatch started %d workers",
					len(starts),
				)
			}
			current, _ := harness.bot.agents.Get(harness.reviewer.ID)
			if len(current.ReviewCycle.WorkerOwnerships) != 0 {
				t.Fatalf(
					"boundary mismatch reserved ownership = %#v",
					current.ReviewCycle.WorkerOwnerships,
				)
			}
			_, _, createRequests, comments :=
				harness.fixture.snapshot()
			if createRequests != 0 || len(comments) != 0 {
				t.Fatalf(
					"boundary mismatch published requests:%d comments:%d",
					createRequests,
					len(comments),
				)
			}
		})
	}
}

func TestRunConvergentReviewCycleValidatesPreparedWorkerCheckoutBeforeStart(
	t *testing.T,
) {
	tests := []struct {
		name      string
		mutate    func(*reviewCoordinatorCheckoutFixture)
		wantError string
	}{
		{
			name: "wrong exact SHA",
			mutate: func(checkout *reviewCoordinatorCheckoutFixture) {
				checkout.head = strings.Repeat("f", 40)
			},
			wantError: "checkout SHA mismatch",
		},
		{
			name: "attached checkout",
			mutate: func(checkout *reviewCoordinatorCheckoutFixture) {
				checkout.ref = "main"
			},
			wantError: "checkout is not detached",
		},
		{
			name: "dirty checkout",
			mutate: func(checkout *reviewCoordinatorCheckoutFixture) {
				checkout.status = " M tracked.txt\n"
			},
			wantError: "checkout is dirty",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newProductionReviewCoordinatorHarness(
				t,
				false,
				false,
			)
			prepare := harness.bot.prepareReviewWorkerWorktreeFunc
			harness.bot.prepareReviewWorkerWorktreeFunc = func(
				ctx context.Context,
				worker Agent,
			) error {
				if err := prepare(ctx, worker); err != nil {
					return err
				}
				checkout := reviewCoordinatorCheckoutFixture{}
				test.mutate(&checkout)
				harness.git.mu.Lock()
				harness.git.checkoutOverrides[filepath.Clean(worker.WorktreePath)] = checkout
				harness.git.mu.Unlock()
				return nil
			}

			_, err := harness.bot.RunConvergentReviewCycle(
				context.Background(),
				harness.reviewer.ID,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantError) {
				t.Fatalf(
					"RunConvergentReviewCycle() error = %v, want %q",
					err,
					test.wantError,
				)
			}
			starts, _ := harness.runner.snapshot()
			if len(starts) != 0 {
				t.Fatalf(
					"prepared checkout mismatch started %d workers",
					len(starts),
				)
			}
		})
	}
}

func TestRunConvergentReviewCycleRejectsBaseAdvanceDuringWorkerPreparation(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	movedBase := coordinatorPullRequest(
		50,
		strings.Repeat("c", 40),
		harness.headSHA,
		"acme/widget",
		"acme/widget",
	)
	prepare := harness.bot.prepareReviewWorkerWorktreeFunc
	harness.bot.prepareReviewWorkerWorktreeFunc = func(
		ctx context.Context,
		worker Agent,
	) error {
		if err := prepare(ctx, worker); err != nil {
			return err
		}
		harness.pulls.mu.Lock()
		harness.pulls.pullRequest = movedBase
		harness.pulls.mu.Unlock()
		return nil
	}

	_, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "base SHA mismatch") {
		t.Fatalf(
			"RunConvergentReviewCycle() error = %v, want preparation-time base rejection",
			err,
		)
	}
	starts, _ := harness.runner.snapshot()
	if len(starts) != 0 {
		t.Fatalf(
			"preparation-time base advance started %d workers, want zero",
			len(starts),
		)
	}
	_, _, createRequests, comments := harness.fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"preparation-time base advance publications = requests:%d comments:%d, want one incomplete notice and no code verdict",
			createRequests,
			len(comments),
		)
	}
	if body := comments[0].GetBody(); !strings.Contains(body, "convergent review discovery failed") ||
		!strings.Contains(body, "This is not a clean-review result") {
		t.Fatalf("preparation-time base advance incomplete notice body = %q", body)
	}
}

func TestRunConvergentReviewCycleBudgetStopsNewLaunches(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	harness.bot.agents.mu.Lock()
	cycle := harness.bot.agents.agents[harness.reviewer.ID].ReviewCycle
	metrics, err := newReviewMetricsState(
		cycle,
		time.Now().UTC().Add(
			-time.Duration(cycle.Policy.Convergence.MaxWallTimeMinutes)*time.Minute-
				time.Minute,
		),
	)
	if err != nil {
		harness.bot.agents.mu.Unlock()
		t.Fatalf("newReviewMetricsState() error = %v", err)
	}
	cycle.Metrics = metrics
	harness.bot.agents.mu.Unlock()

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err == nil {
		t.Fatal("RunConvergentReviewCycle() error = nil, want budget stop")
	}
	if result.LimitTransition == nil ||
		result.LimitTransition.Kind != ReviewLimitWallTime ||
		result.LimitTransition.Action !=
			result.Policy.FailureActions.BudgetExhaustion ||
		result.LimitTransition.Outcome !=
			ReviewLimitOutcomeInconclusive {
		t.Fatalf(
			"budget transition = %#v, want configured wall-time outcome",
			result.LimitTransition,
		)
	}
	starts, _ := harness.runner.snapshot()
	if len(starts) != 0 ||
		len(result.WorkerOwnerships) != 0 {
		t.Fatalf(
			"budget stop launches/reservations = %d/%d, want zero",
			len(starts),
			len(result.WorkerOwnerships),
		)
	}
	_, _, createRequests, comments := harness.fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"budget outcome publications = requests:%d comments:%d, want one incomplete notice and no code verdict",
			createRequests,
			len(comments),
		)
	}
	if body := comments[0].GetBody(); !strings.Contains(body, "convergent review ended without a publishable verdict") ||
		!strings.Contains(body, "This is not a clean-review result") {
		t.Fatalf("budget review-incomplete notice body = %q", body)
	}
	current, ok := harness.bot.agents.Get(harness.reviewer.ID)
	if !ok || current.State != StateErrored || !current.Stopped {
		t.Fatalf(
			"incomplete reviewer = (found=%t state=%s stopped=%t), want retired operational failure",
			ok,
			current.State,
			current.Stopped,
		)
	}
}

func TestRunConvergentReviewCycleRechecksBoundaryAtEveryLaunch(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	moved := coordinatorPullRequest(
		50,
		harness.baseSHA,
		strings.Repeat("d", 40),
		"acme/widget",
		"acme/widget",
	)
	harness.pulls.sequence = []*github.PullRequest{
		harness.pulls.pullRequest,
		moved,
	}
	harness.bot.reviewSuccessorLauncher = func(
		context.Context,
		Agent,
		string,
	) error {
		return nil
	}
	defer harness.bot.waitForStaleReviewCleanups()

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "review cycle head is stale") {
		t.Fatalf(
			"RunConvergentReviewCycle() error = %v, want launch boundary rejection",
			err,
		)
	}
	starts, _ := harness.runner.snapshot()
	if len(starts) != 0 ||
		len(result.WorkerOwnerships) != 0 {
		t.Fatalf(
			"launch-time boundary rejection starts/reservations = %d/%d",
			len(starts),
			len(result.WorkerOwnerships),
		)
	}
}

func TestRunConvergentReviewCycleRejectsBaseOnlyDriftAtLaunch(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	movedBase := coordinatorPullRequest(
		50,
		strings.Repeat("c", 40),
		harness.headSHA,
		"acme/widget",
		"acme/widget",
	)
	harness.pulls.sequence = []*github.PullRequest{
		harness.pulls.pullRequest,
		movedBase,
	}

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "base SHA mismatch") {
		t.Fatalf(
			"RunConvergentReviewCycle() error = %v, want base-only drift rejection",
			err,
		)
	}
	starts, _ := harness.runner.snapshot()
	if len(starts) != 0 ||
		len(result.WorkerOwnerships) != 0 {
		t.Fatalf(
			"base-only drift starts/reservations = %d/%d, want zero",
			len(starts),
			len(result.WorkerOwnerships),
		)
	}
}

func TestRunConvergentReviewCycleRejectsBaseAdvanceAfterFinalArtifact(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	movedBase := coordinatorPullRequest(
		50,
		strings.Repeat("c", 40),
		harness.headSHA,
		"acme/widget",
		"acme/widget",
	)
	var artifactMu sync.Mutex
	artifactCount := 0
	harness.runner.afterArtifact = func(Agent) {
		artifactMu.Lock()
		defer artifactMu.Unlock()
		artifactCount++
		if artifactCount != 2 {
			return
		}
		harness.pulls.mu.Lock()
		harness.pulls.pullRequest = movedBase
		harness.pulls.mu.Unlock()
	}

	result, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "base SHA mismatch") {
		t.Fatalf(
			"RunConvergentReviewCycle() error = %v, want post-artifact base rejection",
			err,
		)
	}
	artifactMu.Lock()
	gotArtifactCount := artifactCount
	artifactMu.Unlock()
	starts, _ := harness.runner.snapshot()
	if gotArtifactCount != 2 || len(starts) != 2 {
		t.Fatalf(
			"completed artifacts/starts = %d/%d, want final 2/2 before base advance",
			gotArtifactCount,
			len(starts),
		)
	}
	if result.VerdictPublication != nil {
		t.Fatalf(
			"post-artifact base advance prepared publication = %#v, want nil",
			result.VerdictPublication,
		)
	}
	_, _, createRequests, comments := harness.fixture.snapshot()
	if createRequests != 1 || len(comments) != 1 {
		t.Fatalf(
			"post-artifact base advance publications = requests:%d comments:%d, want one incomplete notice and no code verdict",
			createRequests,
			len(comments),
		)
	}
	if body := comments[0].GetBody(); !strings.Contains(body, "convergent review discovery failed") ||
		!strings.Contains(body, "This is not a clean-review result") {
		t.Fatalf("post-artifact base advance incomplete notice body = %q", body)
	}
}

func TestRunConvergentReviewCycleRejectsDuplicateActiveRun(
	t *testing.T,
) {
	harness := newProductionReviewCoordinatorHarness(
		t,
		false,
		false,
	)
	harness.runner.startEntered = make(chan struct{})
	harness.runner.releaseStart = make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := harness.bot.RunConvergentReviewCycle(
			context.Background(),
			harness.reviewer.ID,
		)
		firstDone <- err
	}()
	select {
	case <-harness.runner.startEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first production launch did not reach the runtime")
	}
	_, err := harness.bot.RunConvergentReviewCycle(
		context.Background(),
		harness.reviewer.ID,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "already running") {
		t.Fatalf(
			"duplicate RunConvergentReviewCycle() error = %v",
			err,
		)
	}
	close(harness.runner.releaseStart)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf(
				"first RunConvergentReviewCycle() error = %v",
				err,
			)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first production coordinator did not finish")
	}
	starts, _ := harness.runner.snapshot()
	want := harness.reviewer.ReviewCycle.Policy.Swarm.MinReviewers + 1
	if len(starts) != want {
		t.Fatalf(
			"duplicate run production starts = %d, want %d",
			len(starts),
			want,
		)
	}
}
