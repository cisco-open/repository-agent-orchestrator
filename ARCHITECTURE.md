# Repository Agent Orchestrator Architecture

This document describes the current Repository Agent Orchestrator architecture implemented in this repository.

## Purpose

Repository Agent Orchestrator is a local orchestration daemon for running Codex against one GitHub repository at a time. It is intentionally not a distributed scheduler or workflow engine. The design favors:

- deterministic local behavior
- isolated git worktrees per task
- GitHub as the workflow control plane
- `tmux` as the process wrapper for Codex
- small amounts of repo-scoped persisted state

Codex is the only runtime adapter implemented today, but the product direction
is runtime-pluggable rather than Codex-exclusive. The runner boundary and
role-specific profiles are extension seams; the current command construction,
readiness detection, home isolation, and model translation remain
Codex-specific and must not be presented as generic runtime compatibility.

## Top-Level Components

### Process And Config

- `cmd/main.go` starts the process and delegates to `orchestrator.Run`.
- `internal/app.go` owns config loading, runtime validation, REPL startup, poll-loop startup, and the main `Orchestrator` type.
- `config/sample.yaml` provides the minimal repo config shape.
- `LOG_PATH` can move Repository Agent Orchestrator runtime artifacts from `/tmp` into a repo-local `.repository-agent-orchestrator` directory.

Responsibilities:

- validate prerequisites
- resolve config and environment defaults
- acquire a repo-scoped process lock
- initialize GitHub, Webex, persistence, runtime, and messenger helpers

### Agent Manager And Persistence

- `AgentManager` tracks in-memory agent state
- `internal/state_persistence.go` snapshots active state to disk and reloads it on startup

Persisted state includes:

- role, issue, branch, worktree, and runtime handle
- PR metadata and review linkage
- coder-owned cross-SHA finding and coverage ledgers
- comment dedupe sets
- review baseline data
- repo-index output baselines
- captured coding and review handoffs

Persistence is currently stored under:

- `/tmp/repository-agent-orchestrator/<repo-name>/.repository-agent-orchestrator/agents_state.json` when `LOG_PATH` is omitted
- `/tmp/repository-agent-orchestrator/<repo-name>/.repository-agent-orchestrator/handoffs.json` when `LOG_PATH` is omitted
- `<LOG_PATH>/.repository-agent-orchestrator/agents_state.json` when `LOG_PATH` is set
- `<LOG_PATH>/.repository-agent-orchestrator/handoffs.json` when `LOG_PATH` is set

### GitHub Integration

GitHub is the system of record for:

- open issues
- PR creation
- PR review comments
- PR issue comments
- approvals
- mergeable state
- terminal PR state

Repository Agent Orchestrator uses the GitHub API to:

- discover the next open issue
- detect coder and repo-index PRs
- forward new comments exactly once
- detect merge conflicts
- determine approval state
- merge approved coder PRs
- detect terminal PRs for cleanup

### Runtime Layer

- `internal/tmux_runner.go` implements the `Runner` interface
- each agent runtime is a detached `tmux` session
- runtime stdout is piped through the internal log sanitizer before being appended to disk
- `internal/runtime_isolation.go` derives the repository server, private agent
  Codex home, and versioned runtime scope

Responsibilities:

- create, address, and stop sessions only through the repository-specific
  `tmux -L` server; keep direct pane input disabled outside serialized,
  scope-validated delivery windows
- create a private per-agent `CODEX_HOME` so history and sessions cannot cross
  repository or agent boundaries
- bind every prompt to the authoritative instance, agent, repository, path,
  worktree, PR, and exact-SHA scope; reject any mismatch before pane delivery
- replace any top-level model option in `CODEX_CMD`, then add one resolved
  role-specific Codex model and reasoning effort
- wait for the Codex UI banner before sending prompts
- send prompts and steering messages as bounded UTF-8 chunks within one logical bracketed paste
- capture recent pane content for monitoring
- report whether the runtime is still alive

Role profiles resolve once at configuration load in built-in, repository YAML,
deployment-override, then process CLI override order. The model catalog rejects
unavailable or unsupported effective model/effort combinations before a runtime
is created, without rejecting unused built-in profiles. Persisted explicit
profiles for non-review agents are revalidated against the active catalog after
restart. Each active reviewer instead requires a canonical full head SHA and a
matching review-cycle snapshot; its runtime profile is validated against, or
restored from, the snapshotted challenge profile and model catalog. A selected
profile inherits the user's global Codex configuration only through explicit
`INHERIT_GLOBAL`; disabling `REVIEW_POLICY` preserves the legacy global behavior
unless a complete CLI model/effort override is supplied.

Checkpointed reviewers also survive restarts during worktree preparation, the
hard gate, and runtime-handle persistence. Recovery validates the stored cycle
before restore filtering. A live deterministic tmux session without a durable
handle is stopped because liveness cannot distinguish session creation from
completed prompt delivery; recovery then retries launch with the same policy
snapshot and original hard-gate choice after cleaning any partial detached
worktree. Recovery inherits daemon cancellation and is joined before the final
shutdown checkpoint.

The same strict policy parser normalizes configured discovery lanes and validates
their profile references plus minimum, maximum, and parallel reviewer bounds. It
also validates verifier assignment, timeout, retry, and evidence settings plus
quiet-round and maximum-round convergence limits. Per-SHA agent, wall-time, and
token budgets and inconclusive, failure, and escalation actions are normalized
and cross-validated with the same policy.
The verifier, challenge, and convergence schedulers form the production
convergent-review boundary. An enabled snapshotted policy invokes them; the
legacy disabled reviewer path does not.

The enabled review-planning layer collects a classification-free input
record from one exact base/head SHA pair. It verifies the detached checkout
still matches the requested head, records deterministic per-file text or binary
line counts and normalized issue acceptance criteria, and can persist that
immutable record with the review-cycle checkpoint. From those inputs and the
same effective policy snapshot, it can also persist an explainable plan:

- path and acceptance-criterion keyword rules classify lifecycle, concurrency,
  security, persistence, scale, and test-coverage risk
- risk-to-lane rules preserve configured required lanes and select an initial
  swarm within the snapshotted minimum and maximum
- versioned coverage rules derive acceptance-criterion, changed call-path,
  relevant state-transition, and risk-domain requirements

The six discovery lanes have non-overlapping responsibilities and dedicated
hypotheses, checklists, and evidence requirements. After those lanes finish, a
required synthesis worker runs with the configured escalation profile. It
receives the complete diff and all lane artifacts, canonical candidates,
coverage gaps, and verification state; its candidates then follow the ordinary
verification path. Each convergence rediscovery pass repeats synthesis.

Every classification and selection records a safe rule explanation.

The convergent worker boundary allocates resources under the same checkpoint:

- every reservation evaluates agent count, elapsed wall time, supported total
  usage, and non-convergence from the coordinator-owned metrics snapshot; a
  hard-budget breach persists one immutable terminal transition before the
  launch is rejected, and dispatch rechecks time and usage after resource
  preparation without double-charging the reserved agent
- the transition snapshots required lanes and unresolved candidates, coverage,
  and challenges for the consolidated outcome; retries and restart recovery
  cannot allocate through it
- high-risk plans and the configured completed-round threshold persist an
  escalation transition, so subsequent verifier and challenge identities keep
  their assignment semantics while using the snapshotted escalation profile
- each cycle has a collision-resistant ID and a revision
- each logical worker identity records cycle, revision, role, pass, and lane
- retries retain that logical identity while receiving a monotonically
  increasing attempt and a fresh random owner ID
- worktree setup runs before the model-runtime clock, retries transient setup
  failures up to three times within a separate two-minute bound, and records
  exhaustion as the terminal `failed/setup` state of that exact durable launch
  attempt; restart recovery cannot reinterpret it as a missing runtime or
  allocate a replacement
- model runtime and artifact publication/intake have independent deadlines, so
  Git setup time and artifact polling cannot consume a worker's model budget or
  be misreported as a model timeout
- Git worktree add/remove/prune mutations share one context-aware repository
  semaphore; queued work abandons promptly when its context is cancelled
- bounded tmux and worktree names retain digest suffixes, so long shared
  prefixes cannot collide
- the ownership record contains the planned tmux session and worktree path and
  is durably persisted before either resource is created; cleanup can resolve
  those coordinates by owner ID after restart; failed retry checkpoints roll
  back both the tentative ownership and any superseded lane completion
- the tmux pane starts through `env -i`; only runtime essentials and explicit
  `RAO_REVIEW_*` repository, exact-SHA, identity, and lane context cross the
  boundary
- GitHub token variables, Webex webhook values, profile overrides, and all
  other coordinator environment entries are absent; GitHub CLI configuration
  is redirected to an empty, collision-resistant sibling of the worker
  checkout, and any pre-existing directory or symlink prevents launch
- missing isolation support or failed ownership persistence prevents worktree
  preparation and runtime launch
- each ownership derives a fresh artifact outbox outside the checkout; workers
  publish one strict exact-SHA JSON envelope through a temporary file and
  atomic no-replace link under snapshotted size, timeout, and retry bounds;
  version 2 discovery payloads carry evidence-complete candidates and
  plan-bound coverage claims, while version 1 remains restart-compatible; a
  flushed owner marker precedes directory creation so restart cleanup can
  reclaim the create-before-claim window without deleting unrelated paths
- coordinator intake accepts only registered, phase-matched workers with clean
  exact-SHA checkouts, checkout-contained referenced paths, and secret-free
  output; accepted envelopes and canonical digests advance lane completion,
  while rejected attempts persist only safe failure metadata
- the discovery scheduler checkpoints each selected lane as queued,
  running, completed, or failed, admits no more than the snapshotted parallel
  reviewer limit, polls immutable artifacts while workers remain live, retries
  transient artifact failures with fresh ownership up to the snapshotted
  limit, and stops and joins workers after acceptance or cancellation
- accepted scheduled discovery receipts atomically advance lane state,
  aggregate named coverage gaps, and derive coordinator-owned finding IDs from
  normalized behavior, location, and invariant fingerprints; severity and
  confidence remain independent and discovery state has no GitHub publication
  path
- convergent rounds persist independent verifier assignments before
  launch, bind origin exclusions and verifier decisions to the assignment-time
  candidate revision, reopen verification when equivalent challenge evidence
  changes that revision, apply exact-SHA evidence rules, and keep rejected,
  inconclusive, failed, or unverified candidates out of the publishable finding
  view
- challenge assignments bind to one coverage gap or competing
  hypothesis and include the full exact diff plus current plan, coverage, and
  verified-finding context; each target is attempted at most once per round,
  conclusive competing-hypothesis evidence must revise the targeted candidate
  and re-enter independent verification, gaps close only from evidenced
  plan-bound claims, and a completed inconclusive challenge remains unresolved
- assigned-worker attempt contexts bound production runtime commands and Codex
  readiness polling; a child launch deadline is retryable while the coordinator
  context remains active
- every convergence round binds to its own completed full discovery pass;
  failed, cancelled, or empty passes cannot count as quiet, and a newly
  discovered candidate, confirmed finding, or material coverage gap resets
  persisted quiet progress before the configured quiet-round threshold can
  converge
- terminal plan, converged, unresolved, and maximum-round outcomes persist one
  consolidated comment body and a stable repository/PR/exact-SHA publication
  identity before contacting GitHub; oversized details compact to deterministic
  result counts, and retries recheck the live head and scan all PR issue
  comments before creating anything
- a pending publication retries on normal poll ticks and adopts only the exact
  matching visible comment from the authenticated publisher before
  checkpointing its comment ID, including when the prior create succeeded but
  its response or final local checkpoint was lost
- THUMBS_UP is impossible while a planned required lane is missing or failed,
  an exact-SHA candidate is not explicitly rejected, a coverage gap remains, or
  a challenge remains unresolved
- the coder-owned review ledger records reported, fixed, rejected, and
  missed finding states plus exact-SHA evidence, source SHAs, coverage, and
  append-only status/provenance histories; rejected findings remain internal
- a deterministic previous-head-to-new-head diff marks intersecting findings
  and coverage for recheck while preserving unaffected entries byte-for-byte;
  absence never proves a fix, which requires fresh exact-SHA verification
  evidence, and unchanged rejected or already-reported evidence is suppressed
  as novel
- first-head presence evidence classifies pre-existing versus original-PR
  findings, while later-head presence evidence distinguishes previously missed
  from fix-introduced findings
- workers can launch only while their coordinator is actively working and
  unpaused, keeping prelaunch and hard-gate terminal paths ownership-free
- one persisted coordinator lifecycle transition owns pause, stop, and cleanup:
  it waits for in-flight worker launches, records the intent and exact owners,
  and serializes repeated or stronger concurrent requests
- worker sessions are stopped and durably acknowledged before any owned
  worktree, GitHub CLI configuration, or artifact directory is released; each
  release is checkpointed so retries skip work already completed
- cleanup failures leave the coordinator stopped with its incomplete lifecycle
  and original ownership records intact for an explicit retry
- pausing blocks new discovery passes, drains the active pass to durable
  terminal lane states, and releases worker resources through the same
  lifecycle boundary while retaining the coordinator worktree and the exact
  policy, SHA, assignment, and ownership snapshot needed to resume
- restart reconciliation inventories every restored cycle and its worker
  owners before removing marker-backed artifact orphans; queued work resumes
  from its checkpoint, live current owners are monitored in place, and a
  missing current owner receives one fresh attempt for the same persisted
  lane, verifier, or challenge identity
- completed logical identities are never relaunched, paused cycles remain
  paused, active convergence rounds continue from their persisted round, and
  incomplete stop or cleanup lifecycles resume resource release without
  scheduling review work
- linked review verdicts first persist a pending application with the stopped
  coordinator, then apply replay-safe coder, inbox, or GitHub effects and
  durably mark the head complete; restart recovery replays an unfinished
  application without consuming the verdict or retaining a duplicate runtime

The legacy reviewer path invokes neither the planner nor the discovery or
convergence schedulers. The automatic tracked-review trigger invokes the
production coordinator only when its exact-SHA policy snapshot is enabled.
That coordinator has no top-level reviewer Codex runtime; normal poll and
restart recovery re-enter the durable coordinator until its one exact-SHA
terminal verdict is published.

### File Messenger

The file messenger writes local agent artifacts into worktrees:

- coding agents: `.repository-agent-orchestrator/TASK.md`, `.repository-agent-orchestrator/CONTEXT.md`, `.repository-agent-orchestrator/HANDOFF.yaml`
- review agents: `.repository-agent-orchestrator/CONTEXT.md`, `.repository-agent-orchestrator/HANDOFF.yaml`
- repo indexing agents: `.repository-agent-orchestrator/TASK.md`
- forwarded comments: `.repository-agent-orchestrator/INBOX/<timestamp>.md` when Repository Agent Orchestrator has a worktree-backed runtime to notify

This creates a local, inspectable task surface for Codex beyond the initial prompt.

### Agent Context Routing

`internal/agent_context.go` adds a deterministic document-routing and handoff layer for coding and review agents.

Inputs:

- top-level `agent_index.yaml`
- persisted handoffs from the configured Repository Agent Orchestrator runtime-artifacts directory

Outputs per launch:

- `.repository-agent-orchestrator/CONTEXT.md`
- `.repository-agent-orchestrator/HANDOFF.yaml`

Behavior:

- tokenize issue title, issue body, and role-specific routing terms
- score indexed docs by preferred starting points, priority, and token overlap
- score recent handoffs by recency and token overlap
- project the selected docs and recent handoffs into `CONTEXT.md`
- capture a non-empty `HANDOFF.yaml` before review or terminal PR cleanup removes the worktree
- bound stored handoffs by config while preserving unresolved follow-ups preferentially

See [docs/AGENT_CONTEXT.md](docs/AGENT_CONTEXT.md) for the scoring rules and schema.

### Notifications

Repository Agent Orchestrator sends Markdown notifications to Webex for major lifecycle events. Notifications are formatted with repo context and sanitized before sending.

### Startup Cleanup

`internal/startup_cleanup.go` implements `--clean`.

It is intentionally destructive for the configured repo and is responsible for:

- terminating an existing Repository Agent Orchestrator process holding the repo lock
- killing Repository Agent Orchestrator `tmux` sessions associated with the repo
- removing managed worktrees
- deleting `repository-agent-orchestrator/*` branches
- removing the configured worktree directory
- removing the configured Repository Agent Orchestrator runtime-artifacts directory

## Agent Roles

Repository Agent Orchestrator currently has three roles.

### Coding Agent

Purpose:

- execute one GitHub issue in a writable worktree

Properties:

- one active coding agent per issue
- branch format: `repository-agent-orchestrator/issue-<N>`
- continuation branch format: `repository-agent-orchestrator/continue-issue-<N>-pr-<M>`, created from an explicitly selected same-repository PR head
- writable worktree rooted in `WORKTREE_DIR`
- creates commits and opens a PR
- can adopt an existing PR without taking ownership of that PR or its remote branch

### Review Agent

Purpose:

- review one specific PR head SHA for a coding agent

Properties:

- detached worktree at the reviewed commit
- disposable and SHA-scoped
- launched automatically for unreviewed coder heads or manually from the REPL
- posts a structured GitHub verdict comment

### Repo Indexing Agent

Purpose:

- build the repository documentation index

Properties:

- branch format: `repository-agent-orchestrator/repo-index-<unix>`
- writable worktree rooted in `WORKTREE_DIR`
- updates top-level `agent_index.yaml`
- generates top-level `INDEX.md`
- opens a PR for direct human review
- no Repository Agent Orchestrator review-agent cycle

Only one active repo indexing agent is allowed at a time.

## State Model

Shared agent states:

- `initializing`
- `working`
- `waiting_review`
- `approved`
- `merging`
- `done`
- `errored`
- `stopped`

State interpretation depends on role:

- coding agents use the full issue -> PR -> review -> merge path
- review agents use a shorter `initializing -> working -> done` path
- repo indexing agents use `initializing -> working -> waiting_review -> done`

## Main Flows

### 1. Start A Coding Agent

1. Repository Agent Orchestrator fetches `origin/<BASE_BRANCH>`.
2. It creates a new worktree and branch `repository-agent-orchestrator/issue-<N>`.
3. It writes `.repository-agent-orchestrator/TASK.md`.
4. It writes `.repository-agent-orchestrator/CONTEXT.md` and `.repository-agent-orchestrator/HANDOFF.yaml`.
5. It launches Codex in a detached `tmux` session rooted at that worktree.
6. It persists the runtime handle and moves the agent to `working`.

### 2. Detect A Coder PR

1. Polling detects an open PR whose head matches the coder branch.
2. Repository Agent Orchestrator records PR number, URL, and observed head SHA.
3. The coder moves to `waiting_review`.
4. Repository Agent Orchestrator launches a review agent for that exact head if it has not already been reviewed.

An explicit `agent continue <issueNumber> <prNumber>` takes a separate startup path. It validates the open PR and same-repository head, creates a managed local continuation branch at the PR head, and records the PR before launching Codex. Existing comments and the initial head are baselined so the coder first inspects and advances the in-progress change; automatic review begins after a new head is pushed to the existing PR branch. Continued PRs never auto-merge.

### 3. Review Loop

Review coordinators, review workers, and fresh correction runtimes share one
durable launch-attempt state machine. A launch authorization is persisted as
`reserved`, moved to `running` before the external launcher is invoked, and
then reaches exactly one terminal state with a typed failure when applicable.
Attempt limits count durable authorizations, not reconstructed process state.
After a crash, a live `running` runtime is adopted; a missing `running` runtime
is terminalized and any permitted replacement receives a new monotonically
increasing attempt. Terminal attempts are never monitored or relaunched.

Before a review runtime is launched, Repository Agent Orchestrator executes every configured mandatory test command in the detached review worktree as a hard gate.

If the hard gate fails:

- the review runtime is never launched
- the coder is treated as needing changes
- the coder runtime is notified
- same-head automatic retry is deferred for a short backoff instead of marking that SHA as fully reviewed

If the review runtime launches successfully, it must post one issue comment containing:

- `CODEX_REVIEW_AGENT_ID`
- `CODEX_REVIEWED_SHA`
- `CODEX_VERDICT`

Review agents also receive `.repository-agent-orchestrator/CONTEXT.md` and `.repository-agent-orchestrator/HANDOFF.yaml` in their detached worktrees.

Repository Agent Orchestrator ignores stale or mismatched verdict comments.

For `NEEDS_CHANGES`, Repository Agent Orchestrator stops the previous coder runtime and launches a fresh runtime on the same writable worktree. The complete verdict is the replacement runtime's first instruction. This bounds conversational context across repeated review cycles without discarding uncommitted work. Paused coders remain paused and receive the verdict through their inbox instead. Each durable fresh-runtime authorization consumes one correction attempt before invocation, so a crash cannot hide or repeat a physical launch under an old round number. At the configured limit, the PR remains draft and automated correction stops for human intervention.

A cycle that cannot produce a verdict terminalizes its exact-SHA launch attempt
and records a short retry cooldown. Polling then reserves a new coordinator
attempt for that same head without requiring a push or empty commit. Setup
failure and incomplete-review paths use the same ledger and permit three total
automatic attempts per SHA; manual review remains available after exhaustion.

### 4. Comment Forwarding

Repository Agent Orchestrator polls:

- PR review comments
- PR issue comments

Deduplication is by GitHub comment ID and survives restart through persisted state.

Forwarding behavior:

- write inbox Markdown when the worktree exists
- send the same content into the runtime
- track pending review feedback separately from issue comments

### 5. Approval And Merge

When a review agent returns `THUMBS_UP`:

- the review worktree is cleaned up
- Repository Agent Orchestrator marks the PR ready for review when it is still a draft
- the coder runtime is stopped
- the coder becomes `approved`

Repository Agent Orchestrator then waits for a human GitHub approval review and auto-merges using the configured merge method (`squash` today).

### 6. Terminal PR Cleanup

Repository Agent Orchestrator treats a PR as terminal when it is:

- merged
- closed without merge

Terminal cleanup applies to coding and repo-indexing agents.

Cleanup behavior:

- capture the agent handoff if `.repository-agent-orchestrator/HANDOFF.yaml` contains real content
- stop the main runtime
- if the agent is a coder, clean up any active review agent first
- delete the remote branch best-effort
- remove the worktree
- delete the local branch best-effort
- sync the local base branch
- mark the agent `done`

For an adopted continuation PR, cleanup skips closing the PR and deleting its remote head branch. Only the managed local branch, worktree, runtimes, and local artifacts are removed; a human remains responsible for merging or closing the PR.

This prevents stale worktrees and branches from accumulating even when a human closes a PR manually.

### 7. Repo Indexing

`agent index repo` launches a repo indexing agent with a task contract that requires:

- scanning tracked Markdown files across the repository
- updating top-level `agent_index.yaml`
- generating top-level `INDEX.md`
- keeping schema fields regular enough for deterministic reuse
- opening a PR for human review

Repository Agent Orchestrator monitors that worktree for:

- `agent_index.yaml`
- `INDEX.md`
- an open PR on the repo-index branch

Once both index files are present or updated and the PR is open, Repository Agent Orchestrator stops the runtime and leaves the agent in `waiting_review` until the PR is merged or closed.

## Worktree Model

Repository Agent Orchestrator uses one worktree per active work item:

- coder worktree: writable, branch-backed
- reviewer worktree: detached at reviewed SHA
- repo index worktree: writable, branch-backed

This keeps Codex sessions isolated while still letting Repository Agent Orchestrator inspect artifacts under `.repository-agent-orchestrator/`.

## Monitoring Model

Repository Agent Orchestrator continuously samples recent runtime output from each live `tmux` session.

It uses that output to detect:

- explicit input waits
- stalled or low-progress working loops
- merge conflict situations

Reactions include:

- Webex alerts for likely input waits
- automatic “continue autonomously” nudges
- two-stage coder-loop recovery: alert after ten unchanged minutes, then replace the runtime after a second unchanged ten-minute window while preserving its worktree
- conflict-resolution guidance for dirty PRs

## Important Invariants

- one active coding agent per issue
- one active repo indexing agent per repo
- review agents are tied to one coder and one PR head SHA
- comment forwarding is idempotent across restarts
- review verdicts must match both reviewer ID and reviewed SHA
- coding and repo-index branches are cleaned up when their PR becomes terminal
- notifications and logs must not leak tokens or webhook URLs

## Current Boundaries

Repository Agent Orchestrator is intentionally not:

- a database-backed workflow engine
- a distributed worker scheduler
- a generic CI replacement
- a multi-repo coordinator in one process

It is a local, repo-scoped orchestrator that relies on GitHub, local git worktrees, and `tmux` rather than introducing heavier infrastructure.
