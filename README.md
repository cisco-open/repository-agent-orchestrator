# Repository Agent Orchestrator

Repository Agent Orchestrator is a local Go daemon for orchestrating Codex-driven workflows against a single GitHub repository. It runs an interactive REPL in the foreground, polls GitHub in the background, launches Codex inside `tmux`, persists agent state across restarts, and sends Markdown notifications to Webex.

Repository Agent Orchestrator is intentionally simple and local:

- one configured repository per process
- local git worktrees for isolation
- GitHub as the source of issues, PRs, reviews, and comments
- `tmux` as the runtime wrapper for Codex sessions

For the system design and lifecycle details, see [ARCHITECTURE.md](ARCHITECTURE.md). For document routing and structured handoff persistence, see [docs/AGENT_CONTEXT.md](docs/AGENT_CONTEXT.md).

## Runtime Support

Codex is the only agent runtime supported by the current implementation. An
operator must install and authenticate the Codex CLI before starting Repository
Agent Orchestrator. `CODEX_CMD` can customize that CLI invocation, and model
profiles can route coding, indexing, discovery, verification, challenge, and
escalation roles to different Codex models and reasoning efforts. Those
features do not make other agent CLIs compatible today.

The project is intended to support pluggable agent runtimes in the future and
has no requirement that future adapters use Codex. Supporting another runtime,
such as Claude Code, will require runtime-specific launch, readiness, prompt
delivery, isolation, model selection, and shutdown behavior. See
[DEVELOPMENT.md](DEVELOPMENT.md#runtime-support-and-direction) for contribution
guidance.

## Prerequisites

To build and test the project, install:

- a supported Go toolchain compatible with [go.mod](go.mod) (currently Go
  1.26.7 or later)
- `git`
- `make`

To operate the daemon, also install and configure:

- the Codex CLI, installed, in `PATH`, and authenticated
- GitHub CLI (`gh`), in `PATH` and authenticated for the configured repository
- `tmux`
- a local clone whose `origin` matches the configured `REPO_OWNER/REPO_NAME`
- every executable named by `MANDATORY_TESTS`
- a Webex incoming-webhook URL supplied through `WEBEX_WEBHOOK_URL`

Startup validates these requirements and exits with an error when one is
missing. See [Runtime Validation](#runtime-validation) for the complete checks.

## What Repository Agent Orchestrator Does

Repository Agent Orchestrator currently manages three agent roles:

- Coding agent:
  - owns one GitHub issue
  - works in a writable worktree on branch `repository-agent-orchestrator/issue-<N>`
  - commits, pushes, and opens a PR
- Review agent:
  - reviews one specific PR head SHA for a coding agent
  - runs in a detached worktree at that exact commit
  - posts a structured verdict comment back to GitHub
- Repo indexing agent:
  - scans repository Markdown docs
  - writes top-level `agent_index.yaml`
  - generates top-level `INDEX.md`
  - opens a PR for human review without a Repository Agent Orchestrator review-agent cycle

Core behaviors:

- launches Codex in repo-specific worktrees under `WORKTREE_DIR`
- writes role-appropriate task, context, handoff, and inbox files under `<worktree>/.repository-agent-orchestrator/`
- polls GitHub for PR creation, comments, reviews, mergeability, approvals, and terminal PR state
- forwards new PR feedback exactly once per agent
- auto-launches fresh review agents for new coder PR heads
- auto-merges approved coder PRs
- cleans up Repository Agent Orchestrator-managed branches and worktrees when a PR is merged or closed

## Repository Layout

- `cmd/main.go`: process entrypoint
- `internal/`: runtime, REPL, polling, lifecycle, persistence, startup cleanup, `tmux` runner
- `config/sample.yaml`: minimal built-in convergent-review example
- `config/sample-swarm-minimal.yaml`: minimal explicit convergent-policy example
- `config/sample-swarm.yaml`: explicit role, lane, bound, budget, and action example
- `Makefile`: build and test helpers

## Build And Test

```bash
make build
make test
```

For the contributor workflow, see [DEVELOPMENT.md](DEVELOPMENT.md). For focused
tests, coverage output, and test-isolation rules, see [TESTING.md](TESTING.md).

Optional local guard:

```bash
git config core.hooksPath .githooks
```

This enables the tracked `commit-msg` hook, which currently rejects commit messages containing literal `\n` sequences.

Build output:

```text
./bin/repository-agent-orchestrator
```

## Configuration

Repository Agent Orchestrator requires a YAML config file:

```bash
repository-agent-orchestrator --config /absolute/path/to/config.yaml
```

Supported CLI flags:

- `--config <path>`
- `--config=<path>`
- `--clean`
- `--model <model>`
- `--model=<model>`
- `--reasoning-effort <effort>`
- `--reasoning-effort=<effort>`
- `--help`
- `-h`
- `--version` (prints the exact source commit, build time, and Go version the running binary was built from, then exits; no `--config` required)

Choose a tested public example as the starting point:

- [`config/sample.yaml`](config/sample.yaml) omits `REVIEW_POLICY` and uses the
  built-in convergent policy.
- [`config/sample-swarm-minimal.yaml`](config/sample-swarm-minimal.yaml) declares
  the built-in convergent policy explicitly.
- [`config/sample-swarm.yaml`](config/sample-swarm.yaml) declares the policy and
  shows explicit role models, lane overrides, concurrency bounds, budgets,
  escalation, and failure actions.

Copy the selected example outside the checkout before adding real repository
names, local paths, or deployment-specific settings. Real deployment configs
must not be committed.

Required YAML keys:

- `REPO_OWNER`
- `REPO_NAME`
- `REPO_PATH`
- `MANDATORY_TESTS`

Optional YAML keys:

- `LOG_PATH`
  - default: `/tmp/repository-agent-orchestrator/<repo-name>`
  - if set to `REPO_PATH`, Repository Agent Orchestrator runtime artifacts are stored under `<REPO_PATH>/.repository-agent-orchestrator`
- `HARD_GATE_MODE`
  - default: `PARALLEL`
  - supported values: `PARALLEL`, `SERIAL`
  - `SERIAL` runs reviewer hard gates one at a time inside this Repository Agent Orchestrator process
- `REVIEW_GATE_ALERT_MINUTES`
  - default: `15`
  - sends a one-time "still running" notification if mandatory review-gate tests exceed this duration
- `REVIEW_POLICY`
  - defaults to the built-in convergent policy when omitted
  - `VERSION` must be `1`
  - automatic tracked review always uses the production exact-SHA convergent
    coordinator
  - discovery, verification, challenge, convergence, and escalation output
    remains internal; the coder receives at most one consolidated terminal
    verdict for an exact SHA
  - the policy supports independent role profiles, lane profiles, model
    aliases, swarm/verifier bounds, artifact limits, convergence budgets,
    and failure/escalation actions
  - workers are confined to `REPO_OWNER/REPO_NAME`; repository or PR
    head/base mismatch fails before worker launch or GitHub write
  - policy must not contain secrets
  - see [docs/REVIEW_POLICY.md](docs/REVIEW_POLICY.md) for every field,
    default, precedence rule, validation error, model/effort mapping, lane,
    bound, budget, action, status signal, and troubleshooting path
  - see [docs/REVIEW_ARTIFACTS.md](docs/REVIEW_ARTIFACTS.md) for the strict
    internal exact-SHA evidence contract
- `MAX_STORED_HANDOFFS`
  - default: `100`
- `MAX_HANDOFFS_IN_CONTEXT`
  - default: `5`

All three public examples are loaded by the strict parser during tests. The
full swarm example is the canonical field reference in executable form; the
minimal example demonstrates that an explicit policy does not require copying every
default.

Runtime profile resolution is deterministic:

1. Built-in defaults provide a model and effort for every role.
2. Named repository profiles and role references override those defaults.
3. Deployment overrides named `RAO_PROFILE_<PROFILE>_MODEL`,
   `RAO_PROFILE_<PROFILE>_REASONING_EFFORT`, and
   `RAO_PROFILE_<PROFILE>_INHERIT_GLOBAL` override repository values.
4. Process-wide `--model` and `--reasoning-effort` CLI values take final
   precedence for every role.

Profile names are uppercased for environment lookup, and hyphens become
underscores. For example, profile `review-fast` uses
`RAO_PROFILE_REVIEW_FAST_MODEL`. Model-only or effort-only deployment overrides
combine with the other repository value. `INHERIT_GLOBAL=true` cannot be
combined with model or effort overrides.

Each CLI profile flag may combine with the other value selected by deployment,
repository, or built-in configuration. If the lower-tier profile uses
`INHERIT_GLOBAL`, both CLI flags are required because there is no explicit
lower-tier value to combine with. Repeating either CLI flag is an argument
error. CLI values win rather than conflict when the same field also has a
deployment override.

For explicit profiles, Repository Agent Orchestrator replaces any top-level
`--model`, `--model=...`, or `-m` setting in `CODEX_CMD` with one resolved
`--model` and appends the resolved `model_reasoning_effort` argument. The user's
global Codex model and effort are used only for a selected profile that
explicitly sets `INHERIT_GLOBAL: true`. Every
profile selected for a discovery, verifier, challenge, or escalation worker
must instead resolve to explicit model and reasoning-effort values so persisted
launch status reports the actual routing. Inherited profiles remain available
to coder and indexer roles.
Unsupported, unavailable, or unsafe model/effort selections fail configuration
loading before a runtime is created. Persisted explicit runtime profiles for
non-review agents are revalidated against the active model catalog after restart
before they can be launched again. Active reviewers must recover a canonical full
head SHA and matching review-cycle snapshot; their runtime profiles are validated
against, or restored from, the snapshotted challenge profile and model catalog.
Launch-phase coordinators with a valid checkpoint are retained across restart
and retry worktree preparation with the same snapshot and hard-gate choice.
Recovery inherits daemon
cancellation and completes before the final shutdown checkpoint.

### Environment Variables

Required:

- `WEBEX_WEBHOOK_URL`

Optional:

- `WORKTREE_DIR`
  - default: `<REPO_PATH>/.worktrees`
- `POLL_INTERVAL_SECONDS`
  - default: `20`
- `BASE_BRANCH`
  - default: `main`
- `CODEX_CMD`
  - default: `codex --ask-for-approval never --sandbox danger-full-access -c tui.animations=false -c check_for_update_on_startup=false`
- `RAO_PROFILE_<PROFILE>_MODEL`
  - deployment override for one named profile's model
- `RAO_PROFILE_<PROFILE>_REASONING_EFFORT`
  - deployment override for one named profile's reasoning effort
- `RAO_PROFILE_<PROFILE>_INHERIT_GLOBAL`
  - explicit boolean deployment override for global Codex inheritance
- `WEBEX_UID`
  - no default; omit it to disable user mentions
  - if set, used as the person email for Webex mentions in
    PR-ready-for-human-review notifications
  - format: a Webex-recognized email address

### Mandatory Test Command Rules

`MANDATORY_TESTS` entries are executed directly, not via a shell.

- at least one command is required
- blank entries are rejected
- each entry is split with `strings.Fields`
- shell syntax is not supported inside an entry

Do not rely on:

- pipes
- redirection
- shell quoting rules
- `cd && ...`

Use plain executable-plus-args commands only.

## Runtime Validation

Repository Agent Orchestrator validates these before it starts:

- `gh` in `PATH` and already authenticated
- `git` in `PATH`
- `make` in `PATH`
- `tmux` in `PATH`
- the executable named by the first token of `CODEX_CMD` in `PATH`
- a local git repository at `REPO_PATH`
- that repo's `origin` matches `REPO_OWNER/REPO_NAME`
- writable worktree and log directories
- valid `MANDATORY_TESTS` executables
- `WEBEX_WEBHOOK_URL` present in the environment

## Runtime Isolation

Every production runtime is isolated on three boundaries:

- all `tmux` operations use a deterministic repository-specific server via
  `tmux -L`; panes from another repository are unreachable even when agent or
  PR numbers collide
- every agent receives a private `CODEX_HOME` under the configured runtime
  artifact directory, including a private `sessions` directory and history;
  only the allowlisted authentication, configuration, skill, rule, and plugin
  entries are linked from the user's base Codex home
- every initial prompt and follow-up carries a versioned scope envelope for the
  orchestrator instance, agent, repository, repository path, worktree, PR, and
  exact head SHA; the runtime rejects a missing or mismatched scope before
  calling `tmux`

Direct keyboard input is disabled on managed panes. `agent list` prints the
repository server and a read-only attach command. Production follow-up delivery
temporarily enables pane input only inside the serialized, scope-validated send
path and restores the disabled state after every attempt. On the first upgraded
launch, unsafe legacy Repository Agent Orchestrator panes for that repository
are stopped on the default `tmux` server before recovery begins.

## Running Repository Agent Orchestrator

```bash
export WEBEX_WEBHOOK_URL="https://webexapis.com/v1/webhooks/incoming/<your-webhook-id>"

cp config/sample-swarm-minimal.yaml /absolute/path/to/your-config.yaml
# edit the config with your real repo owner/name/path and mandatory tests

./bin/repository-agent-orchestrator --config /absolute/path/to/your-config.yaml
```

## REPL Commands

Available commands:

- `help`
- `status`
- `exit`
- `quit`

Issue workflow:

- `agent list`
- `agent continue <issueNumber> <prNumber>`
- `agent start <issueNumber>`
- `agent start next`
- `agent index repo`

Review tools:

- `agent review <prNumber>`

Agent control:

- `agent help`
- `agent cleanup [coder|reviewer] <issueOrPRNumber>` resolves reviewers by either their tracked issue or PR
- `agent pause [coder|reviewer] <issueNumber>`
- `agent unpause [coder|reviewer] <issueNumber>`
- `agent stop [coder|reviewer] <issueOrPRNumber>` resolves reviewers by either their tracked issue or PR
- `agent steer [coder|reviewer] <issueNumber> <text...>`
- `agent tail`
- `agent tail [coder|reviewer] <issueNumber>`

REPL details:

- unique command prefixes are accepted when unambiguous
- `status` shows the effective review-policy state, fingerprint, role profiles,
  lanes, swarm and convergence bounds, and escalation settings without exposing
  credentials, command strings, or raw environment values
- issue numbers are the primary way to target Repository Agent Orchestrator workflow state; omitted role defaults to the coder for that issue
- bare `agent stop <number>` and `agent cleanup <number>` selectors fall back to the detached manual reviewer for PR `<number>` when no matching issue coder exists
- `agent continue <issueNumber> <prNumber>` continues an open, same-repository PR from its current head; fork PR heads are rejected
- `agent start next` selects the lowest-number open non-PR issue without an active coding agent
- `agent review <prNumber>` reviews any PR by number; Repository Agent Orchestrator-managed PRs rejoin the normal tracked review flow, while standalone PRs use a detached one-shot reviewer
- `agent pause` stops the current runtime without retiring the selected coding or review agent; repo indexing agents cannot be paused
- `agent unpause` explicitly restarts a paused coding or review agent runtime
- `agent steer` also resumes a paused agent before sending the steering message
- `agent stop` stops the selected runtime and prompts to clean up its managed artifacts; declining keeps the stopped agent available to a later `agent cleanup`
- `agent tail` opens an interactive selector for active agent runtime logs; `agent tail [coder|reviewer] <issueNumber>` tails that agent's log directly, and `Ctrl-C` returns to the REPL
- `agent index repo` allows one active repo indexing agent at a time

## Current Workflow Model

### Coding Flow

When you start a coding agent, Repository Agent Orchestrator:

- fetches `origin/<BASE_BRANCH>`
- creates a worktree under `WORKTREE_DIR`
- creates branch `repository-agent-orchestrator/issue-<N>`
- writes `<worktree>/.repository-agent-orchestrator/TASK.md`
- writes `<worktree>/.repository-agent-orchestrator/CONTEXT.md`
- writes `<worktree>/.repository-agent-orchestrator/HANDOFF.yaml`
- launches Codex in a detached `tmux` session rooted at that worktree
- waits for the Codex UI banner before sending the initial prompt
- sends each prompt as one logical bracketed paste split into bounded UTF-8 chunks so large issue bodies and steering messages do not exceed `tmux` command limits
- logs sanitized runtime output under the configured Repository Agent Orchestrator runtime-artifacts directory

When the coding agent opens a PR, Repository Agent Orchestrator:

- detects and records the draft PR
- moves the coder to `waiting_review`
- launches a review agent for the observed PR head

To continue an existing change, run `agent continue <issueNumber> <prNumber>`. Repository Agent Orchestrator validates that the PR is open, targets the configured base branch, and has a head in the configured repository. It creates a separate managed local branch from the existing PR head, baselines the current head and historical comments, and launches the coder to inspect the current diff and feedback. The coder updates the existing PR with `git push origin 'HEAD:<existing-head-branch>'`; the first new head enters the normal mandatory-test and review-agent loop.

Continued PRs remain human-owned. Repository Agent Orchestrator may mark a draft ready after agent approval, but it does not auto-merge, close, or delete the remote branch. Explicit or terminal cleanup removes only local orchestrator artifacts. Contributor-fork PR heads are not supported.

Review agents:

- run all configured mandatory tests before review as a hard gate
- back off same-head automatic review retry for a short cooldown when that hard gate fails
- review exactly one PR head SHA
- retry an incomplete review automatically against that same SHA after a short
  persisted cooldown, with three total cycle attempts and no empty commit
- receive `.repository-agent-orchestrator/CONTEXT.md` and `.repository-agent-orchestrator/HANDOFF.yaml`
- use the production discovery, verification, challenge, escalation, and
  convergence workers
- keep all intermediate evidence internal and publish one
  consolidated exact-SHA comment with `CODEX_AGENT_ID`,
  `CODEX_AGENT_ROLE: reviewer`, `CODEX_REVIEWED_SHA`, and `CODEX_VERDICT`

The coordinator snapshots the effective policy at the trigger. It
rechecks the configured repository and PR base/head repository before any
worker launch or GitHub write. Required lanes, unresolved candidates, coverage
gaps, or challenges make approval impossible. Stale heads are canceled and
superseded; restart and transient publication retries resume from durable
checkpoints. `status` and sanitized lifecycle log records expose the actual
worker routing, attempt states, bounded accounting, review result, and single
consolidated publication identity. Review results distinguish complete and
partial coverage from clean and finding dispositions. See
[docs/REVIEW_POLICY.md](docs/REVIEW_POLICY.md), including its same-repository
smoke-test procedure.

Verdicts:

- `NEEDS_CHANGES`
  - review worktree is cleaned up
  - coder returns to `working`
  - the previous coder runtime is replaced so each correction cycle starts with fresh model context
  - the preserved coder worktree and full verdict are supplied to the replacement runtime as its first instruction
  - paused coders remain paused and receive the verdict in their inbox
  - one durable correction attempt and deterministic runtime ownership are persisted before a fresh coder runtime is invoked; every replacement uses a new attempt, including recovery after a missing runtime
  - after the configured correction limit, the PR remains draft and waits for human correction
- `THUMBS_UP`
  - review worktree is cleaned up
  - Repository Agent Orchestrator marks the PR ready for review when it is still a draft
  - coder runtime is stopped
  - coder becomes `approved`
  - Repository Agent Orchestrator waits for a human GitHub approval review and then auto-merges

If a coding runtime remains on unchanged `working` output for ten minutes, Repository Agent Orchestrator alerts and starts a grace period. If the output is still unchanged after another ten minutes, it replaces the runtime while preserving the worktree and recorded review state. A failed replacement moves the coder to `errored` instead of leaving it indefinitely marked `working`.

### Comment Forwarding

Repository Agent Orchestrator polls both PR review comments and PR issue comments.

- new comments are deduplicated by GitHub comment ID
- forwarded comments are written to `<worktree>/.repository-agent-orchestrator/INBOX/<timestamp>.md` when the worktree exists
- the same content is sent into the runtime session
- comment dedupe survives restarts through persisted state
- coder forwarding is suspended while an automatic review is active, so
  internal review output cannot leak into the coding runtime
- internal verdict comments are never treated as human feedback; only the
  terminal verdict application reaches the coder

### Agent Context And Handoffs

Repository Agent Orchestrator uses top-level `agent_index.yaml` to route a small set of relevant docs into coding and review worktrees.

The two top-level index files describe the repository in which they are
committed; they are not example files:

- `agent_index.yaml` is the canonical, machine-readable routing metadata for
  this repository's tracked Markdown documentation
- `INDEX.md` is the generated, human-readable rendering of that YAML and should
  not be edited independently

Repository indexing agents keep these exact filenames when operating on other
repositories so each target repository owns its own routing metadata.

- selected docs and recent handoffs are written to `<worktree>/.repository-agent-orchestrator/CONTEXT.md`
- each coding or review run also gets `<worktree>/.repository-agent-orchestrator/HANDOFF.yaml`
- agents are expected to update the handoff file before they finish
- Repository Agent Orchestrator persists non-empty handoffs to the configured Repository Agent Orchestrator state directory as `handoffs.json`
- the stored set is bounded by `MAX_STORED_HANDOFFS` and unresolved follow-ups are preserved preferentially
- only the top `MAX_HANDOFFS_IN_CONTEXT` scored handoffs are rendered into agent context

Repo indexing agents do not currently receive routed `CONTEXT.md` or `HANDOFF.yaml` artifacts.

The scoring rules and schema are documented in [docs/AGENT_CONTEXT.md](docs/AGENT_CONTEXT.md).

### Repo Indexing Flow

`agent index repo` launches a repo indexing agent that:

- creates its own writable worktree on branch `repository-agent-orchestrator/repo-index-<unix>`
- reads tracked Markdown across the repo
- updates the target repository's canonical top-level `agent_index.yaml`
- generates the target repository's top-level `INDEX.md` from that YAML
- opens a PR for human review

Repository Agent Orchestrator does not launch a review agent for the indexing PR. Once both index files exist or are updated and the PR is open, Repository Agent Orchestrator stops the runtime and leaves the agent in `waiting_review` until the PR is merged or closed.

### Terminal PR Cleanup

For coding and repo-indexing agents, Repository Agent Orchestrator treats a PR as terminal when it is:

- merged
- closed without merge

On terminal PR cleanup, Repository Agent Orchestrator:

- stops the runtime
- cleans up any active review agent for the coder
- deletes the remote branch best-effort
- removes the worktree
- deletes the local branch best-effort
- syncs the local base branch
- marks the agent `done`

For a PR adopted with `agent continue`, remote-branch deletion is skipped. Explicit cleanup also leaves the existing PR open, and an approved continued PR waits for a human merge.

## Persistence And Paths

Repository Agent Orchestrator stores runtime artifacts under:

- log root: `/tmp/repository-agent-orchestrator/<repo-name>/` when `LOG_PATH` is omitted
- state root: `/tmp/repository-agent-orchestrator/<repo-name>/.repository-agent-orchestrator/` when `LOG_PATH` is omitted
- logs, state, locks, and runtime artifacts under `<LOG_PATH>/.repository-agent-orchestrator/` when `LOG_PATH` is set

Important paths:

- daemon log: `<log-root-or-log-path>/.repository-agent-orchestrator/repository-agent-orchestrator.log` when `LOG_PATH` is set, otherwise `/tmp/repository-agent-orchestrator/<repo-name>/repository-agent-orchestrator.log`
- runtime log: `<log-root-or-log-path>/.repository-agent-orchestrator/<agent-id>.log` when `LOG_PATH` is set, otherwise `/tmp/repository-agent-orchestrator/<repo-name>/<agent-id>.log`
- mandatory test gate log: `<log-root-or-log-path>/.repository-agent-orchestrator/<agent-id>-gate.log` when `LOG_PATH` is set, otherwise `/tmp/repository-agent-orchestrator/<repo-name>/<agent-id>-gate.log`
- persisted state: `<state-root>/agents_state.json`
- handoff store: `<state-root>/handoffs.json`
- process lock: `<log-root-or-log-path>/.repository-agent-orchestrator/instance.lock` when `LOG_PATH` is set, otherwise `/tmp/repository-agent-orchestrator/<repo-name>/instance.lock`

Worktree defaults:

- base worktree dir: `<REPO_PATH>/.worktrees`
- coder worktree: `<WORKTREE_DIR>/issue-<N>-<unix>`
- reviewer worktree: `<WORKTREE_DIR>/review-<N>-<unix-nanos>`
- repo index worktree: `<WORKTREE_DIR>/repo-index-<unix>`

Persisted state includes:

- agent role, issue, branch, worktree, runtime handle, and PR metadata
- review linkage and last reviewed SHA/verdict
- coder-owned cross-SHA finding and coverage ledgers
- comment dedupe sets
- repo-index baseline timestamps for `agent_index.yaml` and `INDEX.md`
- captured coding and review handoffs
- last poll timestamp

## Clean Start

```bash
./bin/repository-agent-orchestrator --config /absolute/path/to/your-config.yaml --clean
```

`--clean` is intentionally destructive for the configured repo. It:

- terminates any running Repository Agent Orchestrator process holding the repo lock
- kills Repository Agent Orchestrator `tmux` sessions rooted in the repo path or worktree dir
- removes Repository Agent Orchestrator-managed worktrees
- deletes local `repository-agent-orchestrator/*` branches
- removes the configured worktree directory
- removes the configured Repository Agent Orchestrator runtime-artifacts directory

Important caveats:

- cleanup still runs after normal runtime validation
- cleanup scope is keyed by sanitized `REPO_NAME`

## Notifications And Logs

Repository Agent Orchestrator sends Markdown Webex notifications for major lifecycle events, including:

- startup
- agent initialization and runtime launch
- PR detection
- forwarded comments
- review verdicts
- merge conflict guidance
- PR ready for human review
- merge or close cleanup
- stop and shutdown events

Runtime pane output is piped through an internal sanitizer before it is written to disk. This strips terminal control sequences and normalizes line endings so logs stay readable and do not leak raw terminal escape output.

See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for a first-response
checklist and a catalogue of known stall/loop failure signatures.

## Security Notes

- GitHub auth is read from `gh auth token`
- `WEBEX_WEBHOOK_URL` must come from the environment
- logs and surfaced errors are sanitized to avoid leaking:
  - GitHub tokens
  - Webex webhook URLs
  - authorization headers

## License

Repository Agent Orchestrator is licensed under the Apache License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for the full license text.

## Current Limitations

- logs, lock files, and persisted state are still scoped by sanitized `REPO_NAME` under `/tmp`
- runtime artifacts move under `LOG_PATH/.repository-agent-orchestrator` when `LOG_PATH` is set
- declining the cleanup prompt after `agent stop` leaves the coder's PR, worktree, and branch in place until a later `agent cleanup`
- `agent cleanup` stops the target agent and removes Repository Agent Orchestrator-managed artifacts for that agent, including linked reviewer worktrees for coders, local and remote Repository Agent Orchestrator branches, and an open PR when one exists
- `MANDATORY_TESTS` are not shell-evaluated
