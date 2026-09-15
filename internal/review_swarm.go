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
	"fmt"
	"strings"
)

const (
	defaultReviewMinReviewers      = 3
	defaultReviewMaxReviewers      = 6
	defaultReviewParallelReviewers = 6
	defaultReviewWorkerTimeout     = 30
	defaultReviewWorkerRetries     = 1
)

var supportedReviewLaneNames = map[string]struct{}{
	"contract":             {},
	"callers":              {},
	"lifecycle":            {},
	"persistence-recovery": {},
	"concurrency-ordering": {},
	"operations-tests":     {},
}

type reviewSwarmConfigFile struct {
	MinReviewers         *int                   `yaml:"MIN_REVIEWERS"`
	MaxReviewers         *int                   `yaml:"MAX_REVIEWERS"`
	MaxParallelReviewers *int                   `yaml:"MAX_PARALLEL_REVIEWERS"`
	TimeoutMinutes       *int                   `yaml:"TIMEOUT_MINUTES"`
	Retries              *int                   `yaml:"RETRIES"`
	Lanes                []reviewLaneConfigFile `yaml:"LANES"`
}

type reviewLaneConfigFile struct {
	Name     string `yaml:"NAME"`
	Required *bool  `yaml:"REQUIRED"`
	Profile  string `yaml:"PROFILE"`
}

type ReviewSwarmPolicy struct {
	MinReviewers         int          `json:"min_reviewers" yaml:"MIN_REVIEWERS"`
	MaxReviewers         int          `json:"max_reviewers" yaml:"MAX_REVIEWERS"`
	MaxParallelReviewers int          `json:"max_parallel_reviewers" yaml:"MAX_PARALLEL_REVIEWERS"`
	TimeoutMinutes       int          `json:"timeout_minutes" yaml:"TIMEOUT_MINUTES"`
	Retries              int          `json:"retries" yaml:"RETRIES"`
	Lanes                []ReviewLane `json:"lanes" yaml:"LANES"`
}

type ReviewLane struct {
	Name     string `json:"name" yaml:"NAME"`
	Required bool   `json:"required" yaml:"REQUIRED"`
	Profile  string `json:"profile" yaml:"PROFILE"`
}

func defaultReviewSwarmPolicy(discoveryProfile string) ReviewSwarmPolicy {
	return ReviewSwarmPolicy{
		MinReviewers:         defaultReviewMinReviewers,
		MaxReviewers:         defaultReviewMaxReviewers,
		MaxParallelReviewers: defaultReviewParallelReviewers,
		TimeoutMinutes:       defaultReviewWorkerTimeout,
		Retries:              defaultReviewWorkerRetries,
		Lanes: []ReviewLane{
			{Name: "contract", Required: true, Profile: discoveryProfile},
			{Name: "callers", Required: true, Profile: discoveryProfile},
			{Name: "lifecycle", Required: false, Profile: discoveryProfile},
			{Name: "persistence-recovery", Required: false, Profile: discoveryProfile},
			{Name: "concurrency-ordering", Required: false, Profile: discoveryProfile},
			{Name: "operations-tests", Required: true, Profile: discoveryProfile},
		},
	}
}

func normalizeReviewSwarm(raw *reviewSwarmConfigFile, defaults ReviewSwarmPolicy) (ReviewSwarmPolicy, error) {
	swarm := ReviewSwarmPolicy{
		MinReviewers:         defaults.MinReviewers,
		MaxReviewers:         defaults.MaxReviewers,
		MaxParallelReviewers: defaults.MaxParallelReviewers,
		TimeoutMinutes:       defaults.TimeoutMinutes,
		Retries:              defaults.Retries,
		Lanes:                append([]ReviewLane(nil), defaults.Lanes...),
	}
	if raw == nil {
		return swarm, nil
	}
	if raw.MinReviewers != nil {
		swarm.MinReviewers = *raw.MinReviewers
	}
	if raw.MaxReviewers != nil {
		swarm.MaxReviewers = *raw.MaxReviewers
	}
	if raw.MaxParallelReviewers != nil {
		swarm.MaxParallelReviewers = *raw.MaxParallelReviewers
	}
	if raw.TimeoutMinutes != nil {
		swarm.TimeoutMinutes = *raw.TimeoutMinutes
	}
	if raw.Retries != nil {
		swarm.Retries = *raw.Retries
	}
	if raw.Lanes != nil {
		lanes, err := normalizeReviewLanes(raw.Lanes)
		if err != nil {
			return ReviewSwarmPolicy{}, err
		}
		swarm.Lanes = lanes
	}
	return swarm, nil
}

func normalizeReviewLanes(raw []reviewLaneConfigFile) ([]ReviewLane, error) {
	seen := make(map[string]struct{}, len(raw))
	lanes := make([]ReviewLane, 0, len(raw))
	for _, laneRaw := range raw {
		name := strings.ToLower(strings.TrimSpace(laneRaw.Name))
		if _, ok := supportedReviewLaneNames[name]; !ok {
			return nil, fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.LANES contains unknown lane %q", laneRaw.Name)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.LANES contains duplicate lane %q", name)
		}
		if laneRaw.Required == nil {
			return nil, fmt.Errorf("REVIEW_POLICY lane %q must specify REQUIRED", name)
		}
		seen[name] = struct{}{}
		lanes = append(lanes, ReviewLane{
			Name:     name,
			Required: *laneRaw.Required,
			Profile:  strings.TrimSpace(laneRaw.Profile),
		})
	}
	return lanes, nil
}

func validateReviewPolicyConfiguration(policy ReviewPolicy) error {
	if err := validateAgentProfiles(policy); err != nil {
		return err
	}
	if err := validateReviewSwarm(policy); err != nil {
		return err
	}
	if err := validateReviewArtifactPolicy(policy.Artifacts); err != nil {
		return err
	}
	if err := validateVerificationPolicy(policy.Verification); err != nil {
		return err
	}
	if err := validateConvergencePolicy(policy.Convergence); err != nil {
		return err
	}
	if err := validateReviewLimits(policy); err != nil {
		return err
	}
	return validateConvergentReviewWorkerProfiles(policy)
}

func validateReviewSwarm(policy ReviewPolicy) error {
	swarm := policy.Swarm
	if swarm.MinReviewers <= 0 {
		return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.MIN_REVIEWERS must be greater than zero")
	}
	if swarm.MaxReviewers <= 0 {
		return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.MAX_REVIEWERS must be greater than zero")
	}
	if swarm.MaxParallelReviewers <= 0 {
		return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.MAX_PARALLEL_REVIEWERS must be greater than zero")
	}
	if swarm.TimeoutMinutes <= 0 {
		return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.TIMEOUT_MINUTES must be greater than zero")
	}
	if swarm.Retries < 0 {
		return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.RETRIES must not be negative")
	}
	if swarm.MinReviewers > swarm.MaxReviewers {
		return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.MIN_REVIEWERS must be <= MAX_REVIEWERS")
	}
	if swarm.MaxParallelReviewers > swarm.MaxReviewers {
		return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.MAX_PARALLEL_REVIEWERS must be <= MAX_REVIEWERS")
	}

	seen := make(map[string]struct{}, len(swarm.Lanes))
	required := 0
	for _, lane := range swarm.Lanes {
		if _, ok := supportedReviewLaneNames[lane.Name]; !ok {
			return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.LANES contains unknown lane %q", lane.Name)
		}
		if _, duplicate := seen[lane.Name]; duplicate {
			return fmt.Errorf("REVIEW_POLICY.REVIEW_SWARM.LANES contains duplicate lane %q", lane.Name)
		}
		seen[lane.Name] = struct{}{}
		if strings.TrimSpace(lane.Profile) == "" {
			return fmt.Errorf("REVIEW_POLICY lane %q must reference a named profile", lane.Name)
		}
		if _, ok := policy.AgentProfiles[lane.Profile]; !ok {
			return fmt.Errorf("REVIEW_POLICY lane %q references unknown profile %q", lane.Name, lane.Profile)
		}
		if lane.Required {
			required++
		}
	}
	if len(swarm.Lanes) < swarm.MinReviewers {
		return fmt.Errorf(
			"REVIEW_POLICY configures %d lanes but MIN_REVIEWERS is %d",
			len(swarm.Lanes),
			swarm.MinReviewers,
		)
	}
	if required > swarm.MaxReviewers {
		return fmt.Errorf(
			"REVIEW_POLICY has %d required lanes but MAX_REVIEWERS is %d",
			required,
			swarm.MaxReviewers,
		)
	}
	return nil
}
