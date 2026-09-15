# Testing Guide

Repository Agent Orchestrator's automated suite exercises configuration,
command parsing, GitHub and Webex boundaries, polling, concurrency, persistence,
runtime isolation, and agent lifecycle behavior.

## Required Verification

Before submitting a code change, run:

```bash
make test
make build
```

`make test` runs `go test ./...`. `make build` writes the executable to
`bin/repository-agent-orchestrator`.

The required suite includes the repository-owned anti-drip replay gate. Run it
alone while changing review orchestration with:

```bash
make test-review-replay
```

Its deterministic report records first-verdict recall, later missed-original
findings, correction-introduced misses, unresolved work, and worker
utilization for synthetic scenarios under `internal/testdata/`.

Run `gofmt` on every changed Go file before these checks. Documentation-only
changes do not normally require new tests, but the repository should still
build and the existing suite should remain green.

## Focused Tests

During development, use a focused package or test name for a faster feedback
loop. For example:

```bash
go test ./internal -run TestName
```

The focused command supplements rather than replaces the required full suite.

## Coverage Report

To run the suite with coverage output:

```bash
make test-coverage
```

This writes machine-readable results and a coverage profile under `.coverage/`
and generates `.coverage/coverage.html`. Coverage is diagnostic; the required
verification remains `make test` and `make build`.

## When Tests Are Required

Add or update tests whenever behavior changes. Tests are especially important
for changes to:

- command parsing and configuration validation
- polling and cancellation
- concurrency and worker cleanup
- GitHub or Webex integration
- comment deduplication and secret filtering
- runtime launch, isolation, or agent lifecycle logic
- persistence and restart recovery
- review planning, evidence handling, and verdict publication

Test success, failure, cancellation, retry, and restart paths where they are
material to the change. Poll loops and worker schedulers must prove that they
stop cleanly without leaking goroutines.

## Test Isolation And Safety

- Use `t.TempDir()` for repositories, worktrees, configs, logs, and persisted
  state.
- Use fake command executors and `httptest` servers for external boundaries.
- Do not require a real GitHub repository, Webex webhook, Codex session, or
  deployment configuration in the automated suite.
- Never place tokens, webhook URLs, authorization headers, private keys, or
  other credentials in fixtures or failure output.
- Test configuration using temporary files or the public `config/sample*.yaml`
  examples, never an untracked deployment config.
- Keep tests deterministic and safe to run repeatedly and in parallel where
  supported.

If a prerequisite is intentionally absent, assert that the command fails with
a clear error and non-zero exit status.
