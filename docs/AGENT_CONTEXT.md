# Agent Context

This document describes how Repository Agent Orchestrator chooses repository documents for coding and review agents, how it projects that context into worktrees, and how it captures machine-readable handoffs for later runs.

## Purpose

Repository Agent Orchestrator already tracks operational state such as agent lifecycle, PR metadata, and comment dedupe. Agent context adds a second layer:

- deterministic document routing from `agent_index.yaml`
- a local worktree context file at `.repository-agent-orchestrator/CONTEXT.md`
- a structured handoff template at `.repository-agent-orchestrator/HANDOFF.yaml`
- a persisted handoff log that later agents can reuse

The goal is to reduce repeated repo rediscovery without turning Repository Agent Orchestrator into a general-purpose memory system.

## Launch-Time Artifacts

For coding agents, Repository Agent Orchestrator writes:

- `.repository-agent-orchestrator/TASK.md`
- `.repository-agent-orchestrator/CONTEXT.md`
- `.repository-agent-orchestrator/HANDOFF.yaml`

For review agents, Repository Agent Orchestrator writes:

- `.repository-agent-orchestrator/CONTEXT.md`
- `.repository-agent-orchestrator/HANDOFF.yaml`

Review agents do not receive a separate task file today because the review prompt already defines the exact PR, SHA, and verdict contract.

Repo indexing agents do not participate in context routing today; they receive a task file, but not routed `CONTEXT.md` or `HANDOFF.yaml`.

## Inputs

Repository Agent Orchestrator builds context from two sources:

1. Top-level repository document routing metadata in `agent_index.yaml`
2. Prior structured handoffs persisted in Repository Agent Orchestrator's runtime state directory

If `agent_index.yaml` is missing, Repository Agent Orchestrator falls back to a minimal hint in `CONTEXT.md` telling the agent to use `README.md`, `ARCHITECTURE.md`, and `AGENTS.md` if present.

## Document Scoring

Repository Agent Orchestrator does not run model inference when choosing docs. It scores documents deterministically.

### Query construction

The query tokens come from:

- issue title
- issue body
- role-specific routing terms

Role-specific terms:

- coding agents add `issue implementation coding`
- review agents add `review pull request feedback verdict`

Tokens are normalized to lowercase alphanumeric words after splitting underscores and hyphens.

### Base priority

Each indexed document gets a base score from `priority`:

- `foundational`: `50`
- `important`: `35`
- `targeted`: `20`
- `situational`: `5`

### Preferred starting points

If a document is listed in `repository.preferred_starting_points`, Repository Agent Orchestrator gives it an additional `+80` score and also includes those preferred documents first in the listed order.

### Token overlap

For each query token found in the document's indexed metadata, Repository Agent Orchestrator adds `+8`.

The document token set is built from:

- `path`
- `title`
- `group`
- `kind`
- `summary`
- `components`
- `concerns`
- `artifacts`
- `read_when`

### Selection cap

Repository Agent Orchestrator includes:

- all preferred starting points that exist in the index
- then the highest-scoring additional documents
- up to `5` total documents

Documents with a non-positive score are skipped unless they are explicitly listed as preferred starting points.

## Handoff Scoring

Repository Agent Orchestrator also selects recent prior handoffs deterministically.

### Handoff inputs

Each persisted handoff contributes tokens from:

- issue title
- summary
- components
- concerns
- artifacts
- decisions
- follow-ups

### Recency bonus

Each handoff gets a recency bonus:

- less than 24 hours old: `+15`
- less than 7 days old: `+10`
- less than 30 days old: `+5`

### Token overlap

For each query token found in the handoff token set, Repository Agent Orchestrator adds `+6`.

### Selection cap

Repository Agent Orchestrator keeps the top `MAX_HANDOFFS_IN_CONTEXT` scored handoffs with positive scores.

Default:

- `MAX_HANDOFFS_IN_CONTEXT`: `5`

## Generated Context File

`.repository-agent-orchestrator/CONTEXT.md` includes:

- the current role, issue, title, and PR if known
- repository routing summary from `agent_index.yaml`
- preferred starting points, stable components, and primary concerns
- the selected repo docs with short scoring hints
- the selected recent handoffs
- an explicit instruction to update `.repository-agent-orchestrator/HANDOFF.yaml` before finishing

The file is meant to be inspectable by humans and small enough for agents to load first.

## Handoff Schema

Repository Agent Orchestrator writes a template file to `.repository-agent-orchestrator/HANDOFF.yaml` with this schema:

```yaml
schema_version: 1
status: in_progress
summary: ""
components: []
concerns: []
artifacts: []
docs_read: []
files_touched: []
tests_run: []
decisions: []
follow_ups: []
risks: []
```

Field intent:

- `status`: final run state such as `completed`, `needs_changes`, or another concise terminal label
- `summary`: short description of what changed or what the review concluded
- `components`: stable subsystem names
- `concerns`: problem or topic tags
- `artifacts`: concrete mechanisms such as files, CLIs, endpoints, fixtures, or named workflows
- `docs_read`: repo docs the agent actually used
- `files_touched`: code or docs changed during the run
- `tests_run`: verification commands actually executed
- `decisions`: noteworthy implementation or review decisions
- `follow_ups`: work that should happen later
- `risks`: remaining known risks or gaps

## Capture Rules

Repository Agent Orchestrator captures handoffs on the cleanup paths that end a run:

- coding agents: before terminal PR cleanup removes the worktree
- review agents: after the verdict is handled and before the review worktree is removed

The default untouched template is ignored. Repository Agent Orchestrator only persists a handoff if the file contains real content beyond the initial placeholder state.

## Persisted Handoff Store

Repository Agent Orchestrator stores captured handoffs in:

- `/tmp/repository-agent-orchestrator/<repo-name>/.repository-agent-orchestrator/handoffs.json` when `LOG_PATH` is omitted
- `<LOG_PATH>/.repository-agent-orchestrator/handoffs.json` when `LOG_PATH` is set

Each entry stores:

- schema version
- capture timestamp
- agent ID and role
- issue and PR metadata
- branch name
- the normalized handoff fields from `.repository-agent-orchestrator/HANDOFF.yaml`

The store is bounded by `MAX_STORED_HANDOFFS`.

Default:

- `MAX_STORED_HANDOFFS`: `100`

Compaction rule:

- preserve entries with non-empty `follow_ups` preferentially
- then keep the most recent remaining entries until the configured cap is reached

## Loading Rules For Future Agents

On the next coding or review launch, Repository Agent Orchestrator:

1. loads `agent_index.yaml`
2. scores the repository docs against the issue title, issue body, and agent role
3. loads persisted handoffs from `handoffs.json`
4. scores those handoffs against the same query
5. writes the selected results into `.repository-agent-orchestrator/CONTEXT.md`
6. writes a fresh `.repository-agent-orchestrator/HANDOFF.yaml` template for the new run

This keeps the canonical persisted state outside the prompt while still making the most relevant parts visible to the agent.

## Current Limitations

- handoffs are persisted under `LOG_DIR`, not yet repo-local under `REPO_PATH/.repository-agent-orchestrator`
- the scoring model is lexical and deterministic; it does not do semantic retrieval
- Repository Agent Orchestrator does not yet enforce validation of agent-written handoff tags against the repo index vocabulary
- only coding and review agents participate in context routing and handoff persistence today
