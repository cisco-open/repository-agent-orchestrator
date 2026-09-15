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

import "fmt"

type ReviewCycleResultState string

const (
	ReviewCycleResultCompleteClean        ReviewCycleResultState = "complete_clean"
	ReviewCycleResultCompleteWithFindings ReviewCycleResultState = "complete_with_findings"
	ReviewCycleResultPartialNoFindings    ReviewCycleResultState = "partial_no_findings"
	ReviewCycleResultPartialWithFindings  ReviewCycleResultState = "partial_with_findings"
	ReviewCycleResultNoReviewPossible     ReviewCycleResultState = "no_review_possible"
)

func supportedReviewCycleResultState(state ReviewCycleResultState) bool {
	switch state {
	case ReviewCycleResultCompleteClean,
		ReviewCycleResultCompleteWithFindings,
		ReviewCycleResultPartialNoFindings,
		ReviewCycleResultPartialWithFindings,
		ReviewCycleResultNoReviewPossible:
		return true
	default:
		return false
	}
}

func classifyReviewCycleResult(cycle *ReviewCycleState) ReviewCycleResultState {
	if !reviewCycleHasTrustedResult(cycle) {
		return ReviewCycleResultNoReviewPossible
	}
	hasFindings := len(cycle.PublishableFindings()) != 0
	if cycle.MandatoryCoverageComplete() {
		if hasFindings {
			return ReviewCycleResultCompleteWithFindings
		}
		return ReviewCycleResultCompleteClean
	}
	if hasFindings {
		return ReviewCycleResultPartialWithFindings
	}
	return ReviewCycleResultPartialNoFindings
}

func reviewCycleHasTrustedResult(cycle *ReviewCycleState) bool {
	return cycle != nil && len(cycle.ArtifactReceipts) != 0
}

func validateReviewCycleResult(cycle *ReviewCycleState) error {
	if cycle == nil || cycle.ResultState == "" {
		return nil
	}
	if !supportedReviewCycleResultState(cycle.ResultState) {
		return fmt.Errorf(
			"persisted review cycle result state %q is unsupported",
			cycle.ResultState,
		)
	}
	if expected := classifyReviewCycleResult(cycle); cycle.ResultState != expected {
		return fmt.Errorf(
			"persisted review cycle result state %q contradicts trusted state %q",
			cycle.ResultState,
			expected,
		)
	}
	return nil
}

func reviewCycleResultForReviewer(
	reviewer Agent,
) (ReviewCycleResultState, bool) {
	cycle := reviewer.ReviewCycle
	if cycle == nil {
		return "", false
	}
	final := cycle.Stale || reviewCycleHasTerminalVerdict(cycle) ||
		agentLifecycleTerminal(&reviewer) ||
		(reviewer.ReviewCoordinatorLifecycle != nil &&
			reviewer.ReviewCoordinatorLifecycle.Intent !=
				ReviewCoordinatorLifecyclePause)
	if !final {
		return "", false
	}
	return classifyReviewCycleResult(cycle), true
}
