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
	"testing"
	"time"
)

func TestAgentManagerListSortsByLastActivity(t *testing.T) {
	m := NewAgentManager()
	now := time.Now()

	old := testAgent("agent-old", 1)
	old.LastActivityTime = now.Add(-2 * time.Hour)
	mid := testAgent("agent-mid", 2)
	mid.LastActivityTime = now.Add(-time.Hour)
	newest := testAgent("agent-new", 3)
	newest.LastActivityTime = now

	if err := m.Add(old); err != nil {
		t.Fatalf("Add(old) error = %v", err)
	}
	if err := m.Add(mid); err != nil {
		t.Fatalf("Add(mid) error = %v", err)
	}
	if err := m.Add(newest); err != nil {
		t.Fatalf("Add(newest) error = %v", err)
	}

	listed := m.List()
	if len(listed) != 3 {
		t.Fatalf("len(List()) = %d, want 3", len(listed))
	}
	if listed[0].ID != "agent-new" || listed[1].ID != "agent-mid" || listed[2].ID != "agent-old" {
		t.Fatalf("List() order = [%s %s %s], want [agent-new agent-mid agent-old]", listed[0].ID, listed[1].ID, listed[2].ID)
	}
}

func TestAgentManagerNormalizesLastActivityTimeToUTC(t *testing.T) {
	m := NewAgentManager()
	offset := time.FixedZone("test-offset", 5*60*60+30*60)
	agent := testAgent("agent-timezone", 1)
	agent.LastActivityTime = time.Date(2026, time.August, 13, 12, 0, 0, 0, offset)

	if err := m.Add(agent); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	stored, ok := m.Get(agent.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", agent.ID)
	}
	if stored.LastActivityTime.Location() != time.UTC {
		t.Fatalf("LastActivityTime location = %s, want UTC", stored.LastActivityTime.Location())
	}

	if !m.Touch(agent.ID) {
		t.Fatalf("Touch(%s) = false, want true", agent.ID)
	}
	stored, _ = m.Get(agent.ID)
	if stored.LastActivityTime.Location() != time.UTC {
		t.Fatalf("touched LastActivityTime location = %s, want UTC", stored.LastActivityTime.Location())
	}
}

func TestAgentManagerSetPRAndPollTracking(t *testing.T) {
	m := NewAgentManager()
	if got := m.SetPR("missing", 10, "Example PR 10", "https://example.test/pr/10", " abc "); got {
		t.Fatal("SetPR(missing) = true, want false")
	}

	a := testAgent("agent-pr", 10)
	if err := m.Add(a); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if got := m.SetPR(a.ID, 10, "Example PR 10", "https://example.test/pr/10", " abc "); !got {
		t.Fatal("SetPR(existing) = false, want true")
	}

	updated, ok := m.Get(a.ID)
	if !ok {
		t.Fatalf("Get(%s) = not found", a.ID)
	}
	if updated.PRNumber != 10 {
		t.Fatalf("PRNumber = %d, want 10", updated.PRNumber)
	}
	if updated.PRURL != "https://example.test/pr/10" {
		t.Fatalf("PRURL = %q, want %q", updated.PRURL, "https://example.test/pr/10")
	}
	if updated.PRTitle != "Example PR 10" {
		t.Fatalf("PRTitle = %q, want %q", updated.PRTitle, "Example PR 10")
	}
	if updated.ObservedPRHeadSHA != "abc" {
		t.Fatalf("ObservedPRHeadSHA = %q, want %q", updated.ObservedPRHeadSHA, "abc")
	}

	now := time.Now().UTC().Truncate(time.Second)
	m.SetLastPoll(now)
	if got := m.LastPoll(); !got.Equal(now) {
		t.Fatalf("LastPoll() = %s, want %s", got.Format(time.RFC3339), now.Format(time.RFC3339))
	}
}

func TestAgentManagerStopAllActive(t *testing.T) {
	m := NewAgentManager()
	agents := []*Agent{
		{ID: "working", State: StateWorking, IssueNumber: 1, LastActivityTime: time.Now()},
		{ID: "done", State: StateDone, IssueNumber: 2, LastActivityTime: time.Now(), Stopped: true},
		{ID: "stopped", State: StateStopped, IssueNumber: 3, LastActivityTime: time.Now(), Stopped: true},
		{ID: "errored", State: StateErrored, IssueNumber: 4, LastActivityTime: time.Now()},
	}
	for _, a := range agents {
		a.BranchName = "repository-agent-orchestrator/issue-1"
		a.seenReviewCommentIDs = make(map[int64]struct{})
		a.seenIssueCommentIDs = make(map[int64]struct{})
		if err := m.Add(a); err != nil {
			t.Fatalf("Add(%s) error = %v", a.ID, err)
		}
	}

	stopped := m.StopAllActive()
	if len(stopped) != 2 {
		t.Fatalf("len(StopAllActive()) = %d, want 2", len(stopped))
	}

	for _, id := range []string{"working", "errored"} {
		a, ok := m.Get(id)
		if !ok {
			t.Fatalf("Get(%s) = not found", id)
		}
		if a.State != StateStopped || !a.Stopped {
			t.Fatalf("agent %s state = (%s, stopped=%v), want (%s, true)", id, a.State, a.Stopped, StateStopped)
		}
	}

	done, _ := m.Get("done")
	if done.State != StateDone || !done.Stopped {
		t.Fatalf("done agent mutated unexpectedly: (%s, stopped=%v)", done.State, done.Stopped)
	}
}
