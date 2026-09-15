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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	agentHandoffSchemaVersion = 1
	maxContextDocuments       = 5
)

var contextTokenPattern = regexp.MustCompile(`[a-z0-9]+`)

type agentIndexFile struct {
	Repository agentIndexRepository       `yaml:"repository"`
	Groups     map[string]agentIndexGroup `yaml:"groups"`
	Documents  []agentIndexDocument       `yaml:"documents"`
}

type agentIndexRepository struct {
	Name                    string             `yaml:"name"`
	Summary                 string             `yaml:"summary"`
	PreferredStartingPoints flexibleStringList `yaml:"preferred_starting_points"`
	StableComponents        flexibleStringList `yaml:"stable_components"`
	PrimaryConcerns         flexibleStringList `yaml:"primary_concerns"`
}

type agentIndexGroup struct {
	Summary string `yaml:"summary"`
}

// UnmarshalYAML tolerates a bare scalar (`some_group: "one-line summary"`)
// as shorthand for `some_group: {summary: "one-line summary"}`. The
// repo-indexer agent's own prompt (formatRepoIndexPrompt) says to "define
// top-level groups" without pinning down that each entry must be a
// mapping, and a plain string is the more natural reading of that
// instruction -- mirroring flexibleStringList's identical tolerance for
// the same class of AI-authored shape drift elsewhere in this file.
func (g *agentIndexGroup) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.ScalarNode {
		g.Summary = strings.TrimSpace(node.Value)
		return nil
	}
	var strict struct {
		Summary string `yaml:"summary"`
	}
	if err := node.Decode(&strict); err != nil {
		return err
	}
	g.Summary = strict.Summary
	return nil
}

type agentIndexDocument struct {
	Path            string   `yaml:"path"`
	Title           string   `yaml:"title"`
	Group           string   `yaml:"group"`
	Kind            string   `yaml:"kind"`
	Summary         string   `yaml:"summary"`
	Components      []string `yaml:"components"`
	Concerns        []string `yaml:"concerns"`
	Artifacts       []string `yaml:"artifacts"`
	ReadWhen        []string `yaml:"read_when"`
	UsuallySkipWhen []string `yaml:"usually_skip_when"`
	Priority        string   `yaml:"priority"`
}

type agentContextRequest struct {
	Role        AgentRole
	IssueNumber int
	IssueTitle  string
	IssueBody   string
	PRNumber    int
}

type scoredDocument struct {
	Document      agentIndexDocument
	Score         int
	MatchedTokens []string
}

type handoffFile struct {
	SchemaVersion int      `yaml:"schema_version"`
	Status        string   `yaml:"status"`
	Summary       string   `yaml:"summary"`
	Components    []string `yaml:"components"`
	Concerns      []string `yaml:"concerns"`
	Artifacts     []string `yaml:"artifacts"`
	DocsRead      []string `yaml:"docs_read"`
	FilesTouched  []string `yaml:"files_touched"`
	TestsRun      []string `yaml:"tests_run"`
	Decisions     []string `yaml:"decisions"`
	FollowUps     []string `yaml:"follow_ups"`
	Risks         []string `yaml:"risks"`
}

type persistedAgentHandoff struct {
	SchemaVersion int       `json:"schema_version"`
	Timestamp     time.Time `json:"timestamp"`
	AgentID       string    `json:"agent_id"`
	Role          AgentRole `json:"role"`
	IssueNumber   int       `json:"issue_number"`
	IssueTitle    string    `json:"issue_title"`
	PRNumber      int       `json:"pr_number"`
	BranchName    string    `json:"branch_name"`
	Status        string    `json:"status"`
	Summary       string    `json:"summary"`
	Components    []string  `json:"components"`
	Concerns      []string  `json:"concerns"`
	Artifacts     []string  `json:"artifacts"`
	DocsRead      []string  `json:"docs_read"`
	FilesTouched  []string  `json:"files_touched"`
	TestsRun      []string  `json:"tests_run"`
	Decisions     []string  `json:"decisions"`
	FollowUps     []string  `json:"follow_ups"`
	Risks         []string  `json:"risks"`
}

type scoredHandoff struct {
	Handoff       persistedAgentHandoff
	Score         int
	MatchedTokens []string
}

type flexibleStringList []string

func (l *flexibleStringList) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	items := make([]string, 0)
	switch node.Kind {
	case yaml.SequenceNode:
		for _, child := range node.Content {
			item := flexibleStringFromYAMLNode(child)
			if item != "" {
				items = append(items, item)
			}
		}
	default:
		item := flexibleStringFromYAMLNode(node)
		if item != "" {
			items = append(items, item)
		}
	}
	*l = items
	return nil
}

func flexibleStringFromYAMLNode(node *yaml.Node) string {
	if node == nil {
		return ""
	}
	switch node.Kind {
	case yaml.ScalarNode:
		return strings.TrimSpace(node.Value)
	case yaml.MappingNode:
		values := make(map[string]string, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := flexibleStringFromYAMLNode(node.Content[i+1])
			if key != "" && value != "" {
				values[key] = value
			}
		}
		for _, key := range []string{"path", "id", "name", "title", "summary", "reason", "description"} {
			if value := strings.TrimSpace(values[key]); value != "" {
				return value
			}
		}
	case yaml.SequenceNode:
		values := make([]string, 0, len(node.Content))
		for _, child := range node.Content {
			value := flexibleStringFromYAMLNode(child)
			if value != "" {
				values = append(values, value)
			}
		}
		return strings.Join(values, " ")
	}
	return ""
}

func (b *Orchestrator) repoDocIndexPath() string {
	if b == nil {
		return ""
	}
	repoPath := strings.TrimSpace(b.cfg.RepoPath)
	if repoPath == "" {
		return ""
	}
	return filepath.Join(repoPath, "agent_index.yaml")
}

func (b *Orchestrator) handoffStorePath() string {
	if b == nil {
		return ""
	}
	logDir := strings.TrimSpace(b.cfg.LogDir)
	if logDir == "" {
		return ""
	}
	return filepath.Join(orchestratorStateDir(logDir), "handoffs.json")
}

func (b *Orchestrator) legacyHandoffLogPath() string {
	if b == nil {
		return ""
	}
	logDir := strings.TrimSpace(b.cfg.LogDir)
	if logDir == "" {
		return ""
	}
	return filepath.Join(orchestratorStateDir(logDir), "handoffs.jsonl")
}

func orchestratorStateDir(logDir string) string {
	logDir = strings.TrimSpace(logDir)
	if logDir == "" {
		return ""
	}
	cleaned := filepath.Clean(logDir)
	if filepath.Base(cleaned) == orchestratorStateDirName {
		return cleaned
	}
	return filepath.Join(cleaned, orchestratorStateDirName)
}

func (b *Orchestrator) loadAgentIndex() (*agentIndexFile, error) {
	path := b.repoDocIndexPath()
	if path == "" {
		return nil, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var idx agentIndexFile
	if err := yaml.Unmarshal(body, &idx); err != nil {
		return nil, err
	}
	return &idx, nil
}

func (b *Orchestrator) loadPersistedHandoffs() ([]persistedAgentHandoff, error) {
	path := b.handoffStorePath()
	if path == "" {
		return nil, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return b.loadLegacyPersistedHandoffs()
		}
		return nil, err
	}
	handoffs := make([]persistedAgentHandoff, 0)
	if len(bytes.TrimSpace(body)) == 0 {
		return handoffs, nil
	}
	if err := json.Unmarshal(body, &handoffs); err != nil {
		return nil, err
	}
	return handoffs, nil
}

func (b *Orchestrator) loadLegacyPersistedHandoffs() ([]persistedAgentHandoff, error) {
	path := b.legacyHandoffLogPath()
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	handoffs := make([]persistedAgentHandoff, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var item persistedAgentHandoff
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			continue
		}
		handoffs = append(handoffs, item)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return handoffs, nil
}

func (b *Orchestrator) maxHandoffsInContext() int {
	if b == nil || b.cfg.MaxHandoffsInContext <= 0 {
		return defaultMaxHandoffsInContext
	}
	return b.cfg.MaxHandoffsInContext
}

func (b *Orchestrator) maxStoredHandoffs() int {
	if b == nil || b.cfg.MaxStoredHandoffs <= 0 {
		return defaultMaxStoredHandoffs
	}
	return b.cfg.MaxStoredHandoffs
}

func tokenizeForContext(text string) []string {
	normalized := strings.ToLower(text)
	normalized = strings.ReplaceAll(normalized, "_", " ")
	normalized = strings.ReplaceAll(normalized, "-", " ")
	return contextTokenPattern.FindAllString(normalized, -1)
}

func uniqueSortedStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	sort.Strings(result)
	return result
}

func priorityScore(priority string) int {
	switch strings.TrimSpace(priority) {
	case "foundational":
		return 50
	case "important":
		return 35
	case "targeted":
		return 20
	case "situational":
		return 5
	default:
		return 0
	}
}

func joinListTokens(items []string) string {
	return strings.Join(items, " ")
}

func buildContextQuery(request agentContextRequest) []string {
	parts := []string{request.IssueTitle, request.IssueBody}
	switch request.Role {
	case RoleReviewer:
		parts = append(parts, "review pull request feedback verdict")
	case RoleIndexer:
		parts = append(parts, "documentation index repository docs")
	default:
		parts = append(parts, "issue implementation coding")
	}
	return uniqueSortedStrings(tokenizeForContext(strings.Join(parts, " ")))
}

func scoreDocument(doc agentIndexDocument, queryTokens []string, preferred bool) scoredDocument {
	tokenSource := strings.Join([]string{
		doc.Path,
		doc.Title,
		doc.Group,
		doc.Kind,
		doc.Summary,
		joinListTokens(doc.Components),
		joinListTokens(doc.Concerns),
		joinListTokens(doc.Artifacts),
		joinListTokens(doc.ReadWhen),
	}, " ")
	docTokens := make(map[string]struct{})
	for _, token := range tokenizeForContext(tokenSource) {
		docTokens[token] = struct{}{}
	}
	matched := make([]string, 0)
	score := priorityScore(doc.Priority)
	if preferred {
		score += 80
	}
	for _, token := range queryTokens {
		if _, ok := docTokens[token]; ok {
			score += 8
			if len(matched) < 4 {
				matched = append(matched, token)
			}
		}
	}
	return scoredDocument{
		Document:      doc,
		Score:         score,
		MatchedTokens: matched,
	}
}

func scoreHandoff(item persistedAgentHandoff, queryTokens []string) scoredHandoff {
	tokenSource := strings.Join([]string{
		item.IssueTitle,
		item.Summary,
		joinListTokens(item.Components),
		joinListTokens(item.Concerns),
		joinListTokens(item.Artifacts),
		joinListTokens(item.Decisions),
		joinListTokens(item.FollowUps),
	}, " ")
	handoffTokens := make(map[string]struct{})
	for _, token := range tokenizeForContext(tokenSource) {
		handoffTokens[token] = struct{}{}
	}
	score := 0
	if !item.Timestamp.IsZero() {
		ageHours := time.Since(item.Timestamp).Hours()
		switch {
		case ageHours < 24:
			score += 15
		case ageHours < 24*7:
			score += 10
		case ageHours < 24*30:
			score += 5
		}
	}
	matched := make([]string, 0)
	for _, token := range queryTokens {
		if _, ok := handoffTokens[token]; ok {
			score += 6
			if len(matched) < 4 {
				matched = append(matched, token)
			}
		}
	}
	return scoredHandoff{Handoff: item, Score: score, MatchedTokens: matched}
}

func (b *Orchestrator) selectRelevantDocuments(idx *agentIndexFile, request agentContextRequest) []scoredDocument {
	if idx == nil {
		return nil
	}
	queryTokens := buildContextQuery(request)
	preferredSet := make(map[string]struct{}, len(idx.Repository.PreferredStartingPoints))
	for _, path := range idx.Repository.PreferredStartingPoints {
		preferredSet[strings.TrimSpace(path)] = struct{}{}
	}

	selected := make([]scoredDocument, 0)
	seen := make(map[string]struct{})

	for _, path := range idx.Repository.PreferredStartingPoints {
		if len(selected) >= maxContextDocuments {
			break
		}
		for _, doc := range idx.Documents {
			if strings.TrimSpace(doc.Path) != strings.TrimSpace(path) {
				continue
			}
			scored := scoreDocument(doc, queryTokens, true)
			selected = append(selected, scored)
			seen[doc.Path] = struct{}{}
			break
		}
	}

	scoredDocs := make([]scoredDocument, 0, len(idx.Documents))
	for _, doc := range idx.Documents {
		if _, ok := seen[doc.Path]; ok {
			continue
		}
		_, preferred := preferredSet[doc.Path]
		scored := scoreDocument(doc, queryTokens, preferred)
		scoredDocs = append(scoredDocs, scored)
	}
	sort.SliceStable(scoredDocs, func(i, j int) bool {
		if scoredDocs[i].Score == scoredDocs[j].Score {
			return scoredDocs[i].Document.Path < scoredDocs[j].Document.Path
		}
		return scoredDocs[i].Score > scoredDocs[j].Score
	})

	for _, doc := range scoredDocs {
		if len(selected) >= maxContextDocuments {
			break
		}
		if doc.Score <= 0 {
			continue
		}
		selected = append(selected, doc)
	}
	return selected
}

func (b *Orchestrator) selectRelevantHandoffs(request agentContextRequest) []scoredHandoff {
	handoffs, err := b.loadPersistedHandoffs()
	if err != nil {
		return nil
	}
	queryTokens := buildContextQuery(request)
	scored := make([]scoredHandoff, 0, len(handoffs))
	for _, item := range handoffs {
		entry := scoreHandoff(item, queryTokens)
		if entry.Score <= 0 {
			continue
		}
		scored = append(scored, entry)
	}
	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].Handoff.Timestamp.After(scored[j].Handoff.Timestamp)
		}
		return scored[i].Score > scored[j].Score
	})
	limit := b.maxHandoffsInContext()
	if len(scored) > limit {
		scored = scored[:limit]
	}
	return scored
}

func roleLabel(role AgentRole) string {
	switch role {
	case RoleReviewer:
		return "review"
	case RoleIndexer:
		return "repo indexing"
	default:
		return "coding"
	}
}

func (b *Orchestrator) buildAgentContextMarkdown(agent Agent, request agentContextRequest) (string, error) {
	idx, err := b.loadAgentIndex()
	if err != nil {
		return "", err
	}
	selectedDocs := b.selectRelevantDocuments(idx, request)
	selectedHandoffs := b.selectRelevantHandoffs(request)

	var out bytes.Buffer
	fmt.Fprintf(&out, "# Repository Agent Orchestrator Agent Context\n\n")
	fmt.Fprintf(&out, "- Role: `%s`\n", roleLabel(request.Role))
	if request.IssueNumber > 0 {
		fmt.Fprintf(&out, "- Issue: `#%d`\n", request.IssueNumber)
	}
	if strings.TrimSpace(request.IssueTitle) != "" {
		fmt.Fprintf(&out, "- Title: %s\n", strings.TrimSpace(request.IssueTitle))
	}
	if agent.PRNumber > 0 {
		fmt.Fprintf(&out, "- PR: `#%d`\n", agent.PRNumber)
	}
	if len(agent.HumanReviewGuidance) > 0 {
		fmt.Fprintf(&out, "\n## Human Review Guidance\n\n")
		fmt.Fprintf(&out, "These notes came from manual operator steering for this PR. Treat them as authoritative scope guidance for the current review cycle.\n\n")
		for i, note := range agent.HumanReviewGuidance {
			trimmed := strings.TrimSpace(note)
			if trimmed == "" {
				continue
			}
			fmt.Fprintf(&out, "%d. %s\n", i+1, trimmed)
		}
	}

	if idx != nil {
		fmt.Fprintf(&out, "\n## Repository Routing\n\n")
		if summary := strings.TrimSpace(idx.Repository.Summary); summary != "" {
			fmt.Fprintf(&out, "%s\n\n", summary)
		}
		if len(idx.Repository.PreferredStartingPoints) > 0 {
			fmt.Fprintf(&out, "- Preferred starting points: `%s`\n", strings.Join(idx.Repository.PreferredStartingPoints, "`, `"))
		}
		if len(idx.Repository.StableComponents) > 0 {
			fmt.Fprintf(&out, "- Stable components: `%s`\n", strings.Join(idx.Repository.StableComponents, "`, `"))
		}
		if len(idx.Repository.PrimaryConcerns) > 0 {
			fmt.Fprintf(&out, "- Primary concerns: `%s`\n", strings.Join(idx.Repository.PrimaryConcerns, "`, `"))
		}
	}

	fmt.Fprintf(&out, "\n## Read These Docs First\n\n")
	if len(selectedDocs) == 0 {
		fmt.Fprintf(&out, "- No `agent_index.yaml` routing data was available. Fall back to `README.md`, `ARCHITECTURE.md`, and `AGENTS.md` if they exist.\n")
	} else {
		for i, doc := range selectedDocs {
			fmt.Fprintf(&out, "%d. `%s`", i+1, doc.Document.Path)
			if strings.TrimSpace(doc.Document.Kind) != "" {
				fmt.Fprintf(&out, " (`%s`", doc.Document.Kind)
				if strings.TrimSpace(doc.Document.Priority) != "" {
					fmt.Fprintf(&out, ", `%s`", doc.Document.Priority)
				}
				fmt.Fprintf(&out, ")")
			}
			fmt.Fprintf(&out, ": %s\n", strings.TrimSpace(doc.Document.Summary))
			if len(doc.MatchedTokens) > 0 {
				fmt.Fprintf(&out, "   - matched: `%s`\n", strings.Join(doc.MatchedTokens, "`, `"))
			}
			if group := strings.TrimSpace(doc.Document.Group); group != "" && idx != nil {
				if meta, ok := idx.Groups[group]; ok && strings.TrimSpace(meta.Summary) != "" {
					fmt.Fprintf(&out, "   - group: `%s` (%s)\n", group, strings.TrimSpace(meta.Summary))
				}
			}
			if len(doc.Document.ReadWhen) > 0 {
				fmt.Fprintf(&out, "   - read when: %s\n", strings.TrimSpace(doc.Document.ReadWhen[0]))
			}
		}
	}

	fmt.Fprintf(&out, "\n## Recent Handoffs\n\n")
	if len(selectedHandoffs) == 0 {
		fmt.Fprintf(&out, "- No prior agent handoffs matched this task.\n")
	} else {
		for _, item := range selectedHandoffs {
			fmt.Fprintf(&out, "- `%s` `%s`", item.Handoff.Role, item.Handoff.AgentID)
			if item.Handoff.IssueNumber > 0 {
				fmt.Fprintf(&out, " on issue `#%d`", item.Handoff.IssueNumber)
			}
			if !item.Handoff.Timestamp.IsZero() {
				fmt.Fprintf(&out, " at `%s`", item.Handoff.Timestamp.UTC().Format(time.RFC3339))
			}
			fmt.Fprintf(&out, ": %s\n", strings.TrimSpace(item.Handoff.Summary))
			if len(item.MatchedTokens) > 0 {
				fmt.Fprintf(&out, "  - matched: `%s`\n", strings.Join(item.MatchedTokens, "`, `"))
			}
			if len(item.Handoff.FollowUps) > 0 {
				fmt.Fprintf(&out, "  - follow-up: %s\n", strings.TrimSpace(item.Handoff.FollowUps[0]))
			}
		}
	}

	fmt.Fprintf(&out, "\n## Completion State\n\n")
	fmt.Fprintf(&out, "Before you finish this run, update `.repository-agent-orchestrator/HANDOFF.yaml` with a concise machine-readable summary of what you changed, what mattered, and what follow-up work remains.\n")
	return out.String(), nil
}

func handoffTemplate(agent Agent) string {
	return fmt.Sprintf(
		"# Update this file before you finish the run.\n"+
			"schema_version: %d\n"+
			"status: in_progress\n"+
			"summary: \"\"\n"+
			"components: []\n"+
			"concerns: []\n"+
			"artifacts: []\n"+
			"docs_read: []\n"+
			"files_touched: []\n"+
			"tests_run: []\n"+
			"decisions: []\n"+
			"follow_ups: []\n"+
			"risks: []\n",
		agentHandoffSchemaVersion,
	)
}

func (b *Orchestrator) writeAgentContextArtifacts(agent Agent, request agentContextRequest) error {
	if b == nil || b.messenger == nil {
		return nil
	}
	contextMarkdown, err := b.buildAgentContextMarkdown(agent, request)
	if err != nil {
		return err
	}
	if err := b.messenger.SendContext(agent, contextMarkdown); err != nil {
		return err
	}
	if err := b.messenger.SendHandoff(agent, handoffTemplate(agent)); err != nil {
		return err
	}
	return nil
}

func (b *Orchestrator) captureAgentHandoffNonFatal(agent Agent) {
	if b == nil || b.agents == nil {
		return
	}
	current, ok := b.agents.Get(agent.ID)
	if !ok || current.HandoffCaptured {
		return
	}
	entry, ok, err := b.loadWorktreeHandoff(current)
	if err != nil {
		log.Printf("non-fatal: failed to load handoff agent=%s path=%s: %s", current.ID, filepath.Join(current.WorktreePath, ".repository-agent-orchestrator", "HANDOFF.yaml"), b.safeError(err))
		return
	}
	if !ok {
		return
	}
	if err := b.appendPersistedHandoff(entry); err != nil {
		log.Printf("non-fatal: failed to persist handoff agent=%s: %s", current.ID, b.safeError(err))
		return
	}
	_ = b.agents.SetHandoffCaptured(current.ID, true)
}

func (b *Orchestrator) loadWorktreeHandoff(agent Agent) (persistedAgentHandoff, bool, error) {
	path := filepath.Join(agent.WorktreePath, ".repository-agent-orchestrator", "HANDOFF.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return persistedAgentHandoff{}, false, nil
		}
		return persistedAgentHandoff{}, false, err
	}
	var handoff handoffFile
	if err := yaml.Unmarshal(body, &handoff); err != nil {
		return persistedAgentHandoff{}, false, err
	}
	if handoff.SchemaVersion != 0 && handoff.SchemaVersion != agentHandoffSchemaVersion {
		return persistedAgentHandoff{}, false, fmt.Errorf("unsupported handoff schema version %d", handoff.SchemaVersion)
	}
	if strings.TrimSpace(handoff.Status) == "in_progress" && strings.TrimSpace(handoff.Summary) == "" &&
		len(handoff.Components) == 0 && len(handoff.Concerns) == 0 && len(handoff.Artifacts) == 0 &&
		len(handoff.Decisions) == 0 && len(handoff.FollowUps) == 0 && len(handoff.Risks) == 0 &&
		len(handoff.FilesTouched) == 0 && len(handoff.TestsRun) == 0 && len(handoff.DocsRead) == 0 {
		return persistedAgentHandoff{}, false, nil
	}

	return persistedAgentHandoff{
		SchemaVersion: agentHandoffSchemaVersion,
		Timestamp:     time.Now().UTC(),
		AgentID:       agent.ID,
		Role:          agent.Role,
		IssueNumber:   agent.IssueNumber,
		IssueTitle:    strings.TrimSpace(agent.IssueTitle),
		PRNumber:      agent.PRNumber,
		BranchName:    strings.TrimSpace(agent.BranchName),
		Status:        strings.TrimSpace(handoff.Status),
		Summary:       strings.TrimSpace(handoff.Summary),
		Components:    uniqueSortedStrings(handoff.Components),
		Concerns:      uniqueSortedStrings(handoff.Concerns),
		Artifacts:     uniqueSortedStrings(handoff.Artifacts),
		DocsRead:      uniqueSortedStrings(handoff.DocsRead),
		FilesTouched:  uniqueSortedStrings(handoff.FilesTouched),
		TestsRun:      uniqueSortedStrings(handoff.TestsRun),
		Decisions:     uniqueSortedStrings(handoff.Decisions),
		FollowUps:     uniqueSortedStrings(handoff.FollowUps),
		Risks:         uniqueSortedStrings(handoff.Risks),
	}, true, nil
}

func (b *Orchestrator) appendPersistedHandoff(entry persistedAgentHandoff) error {
	path := b.handoffStorePath()
	if path == "" {
		return nil
	}
	handoffs, err := b.loadPersistedHandoffs()
	if err != nil {
		return err
	}
	handoffs = append(handoffs, entry)
	handoffs = compactPersistedHandoffs(handoffs, b.maxStoredHandoffs())
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(handoffs, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomically(path, append(body, '\n'))
}

func (b *Orchestrator) removePersistedHandoffs(agentIDs ...string) error {
	path := b.handoffStorePath()
	if path == "" {
		return nil
	}
	ids := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		ids[agentID] = struct{}{}
	}
	if len(ids) == 0 {
		return nil
	}

	handoffs, err := b.loadPersistedHandoffs()
	if err != nil {
		return err
	}
	filtered := make([]persistedAgentHandoff, 0, len(handoffs))
	changed := false
	for _, handoff := range handoffs {
		if _, remove := ids[strings.TrimSpace(handoff.AgentID)]; remove {
			changed = true
			continue
		}
		filtered = append(filtered, handoff)
	}
	if !changed {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(filtered, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomically(path, append(body, '\n'))
}

func compactPersistedHandoffs(items []persistedAgentHandoff, limit int) []persistedAgentHandoff {
	if limit <= 0 || len(items) == 0 {
		return nil
	}
	if len(items) <= limit {
		return items
	}

	selected := make(map[int]struct{}, limit)
	for i := len(items) - 1; i >= 0 && len(selected) < limit; i-- {
		if len(items[i].FollowUps) == 0 {
			continue
		}
		selected[i] = struct{}{}
	}
	for i := len(items) - 1; i >= 0 && len(selected) < limit; i-- {
		if _, ok := selected[i]; ok {
			continue
		}
		selected[i] = struct{}{}
	}

	compacted := make([]persistedAgentHandoff, 0, len(selected))
	for i := range items {
		if _, ok := selected[i]; !ok {
			continue
		}
		compacted = append(compacted, items[i])
	}
	return compacted
}

func writeFileAtomically(path string, body []byte) error {
	temp, err := os.CreateTemp(filepath.Dir(path), "handoffs-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	if _, err := temp.Write(body); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}
