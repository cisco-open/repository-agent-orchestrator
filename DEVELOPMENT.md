# Development Guide

This guide contains the technical rules for contributing code to Repository
Agent Orchestrator. Community expectations and the issue and pull-request
process are documented in [CONTRIBUTING.md](CONTRIBUTING.md). Test commands and
test-writing guidance are documented in [TESTING.md](TESTING.md).

## Development Prerequisites

Install:

- a supported Go toolchain compatible with [go.mod](go.mod) (currently Go
  1.26.7 or later)
- `git`
- `make`

Most automated tests use temporary repositories, fake command executors, and
local HTTP test servers. Work on production runtime behavior may also require
the operator prerequisites listed in [README.md](README.md#prerequisites),
including `gh`, `tmux`, and the Codex CLI.

Never use a real deployment config or real credentials in development tests.
Use temporary configs or the public `config/sample*.yaml` examples.

## Runtime Support And Direction

Codex is the only agent runtime implemented today. The runtime command,
readiness detection, prompt delivery, isolated home directory, and model-profile
translation are all currently Codex-specific. `CODEX_CMD` customizes the Codex
invocation; it is not a provider-neutral runtime contract.

The project is intended to support pluggable agent runtimes in the future. The
existing runner interface and role-specific model routing provide useful seams,
but another CLI such as Claude Code will require an explicit runtime adapter and
tests for its launch, readiness, input, isolation, model selection, error, and
shutdown behavior. Keep new orchestration logic independent of Codex where that
can be done without speculative abstraction.

## Getting Started

1. Fork or clone the repository and create a focused topic branch.
2. Run the baseline verification:

   ```bash
   make test
   make build
   ```

3. Make the smallest change that addresses the issue and add focused tests for
   behavior changes.
4. Run the relevant focused tests while iterating, then run the full required
   verification in [TESTING.md](TESTING.md#required-verification).

You can optionally enable the tracked local commit-message guard:

```bash
git config core.hooksPath .githooks
```

The hook rejects commit messages containing literal `\n` sequences.

## Project Scope

Repository Agent Orchestrator stays focused on local orchestration for one
GitHub repository per process. It is not a CI system, distributed scheduler,
database-backed workflow engine, or cluster manager.

Keep changes small and reviewable. Avoid unrelated refactors, broad renames,
and style-only churn. Do not implement automatic code-editing behavior unless
the issue explicitly requires editing agents.

## Implementation Rules

- Prefer the Go standard library unless a dependency is clearly justified.
- Keep packages cohesive and testable.
- Keep REPL logic separate from polling.
- Keep Webex notification logic behind an interface.
- Keep GitHub integration isolated and testable.
- Use interfaces around external services where they improve testability.
- Propagate cancellation with `context.Context`.
- Make poll loops stop cleanly and avoid goroutine leaks.
- Never block the REPL on network calls.
- Fail with a clear error and non-zero exit status when a prerequisite is
  missing.
- Keep notification text Markdown-compatible and free of secrets.
- Keep generated runtime artifacts such as `.repository-agent-orchestrator/`
  and `.worktrees/` out of commits unless a task explicitly targets tracked
  documentation or templates.
- Include the Apache 2.0 source header with
  `SPDX-License-Identifier: Apache-2.0` at the top of every source file.

Do not add merge behavior that bypasses approval detection, re-forward
duplicate PR comments, or forward secrets through PR comments or notifications.

## Secrets And Safe Logging

Never print or log secrets, including:

- the token returned by `gh auth token`
- `WEBEX_WEBHOOK_URL`
- authorization headers
- raw environment variables
- sanitized output that still contains credential material

Logs may include safe metadata such as repository, issue, pull request, agent
ID, branch, and worktree path.

## Code Style

Run `gofmt` on changed Go files. Prefer explicit, deterministic behavior over
clever abstractions, and follow the priority order in [AGENTS.md](AGENTS.md):
correctness, security, determinism, clarity, observability, then performance.

## Commit Messages

Use Conventional Commits:

```text
<type>(<scope>): <summary>
```

Allowed types are `feat`, `fix`, `chore`, `refactor`, `docs`, `test`, `build`,
`ci`, `perf`, and `revert`.

Use an imperative, present-tense summary of 72 characters or fewer. Add a body
when the reasoning is not obvious, and reference issues in the footer when
applicable.
