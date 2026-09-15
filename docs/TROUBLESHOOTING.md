# Troubleshooting

This is a runbook for diagnosing a stalled, looping, or misbehaving
Repository Agent Orchestrator deployment. Follow the first-response
checklist **before** taking any mutating action (stopping an agent,
restarting the orchestrator, editing config, clearing a marker). Mutating
actions destroy the exact state that made the problem reproducible; capture
evidence first, then act.

## Automated diagnosis (read this first)

Before doing any manual correlation of `status`'s many fields against known
failure modes, check the **"Review Cycle Health"** section at the end of
`status`'s output: one line per active reviewer (never assumes zero or one
-- multiple issues/PRs can be under review at once), each one of:
- `healthy` with a compact progress summary (still actively advancing),
- `paused`/`superseded`/`awaiting verdict`/`awaiting cleanup` (not a
  problem -- a known, expected reason nothing is progressing right now;
  see below),
- or `STUCK (<pattern>)` naming exactly which known failure signature
  matched (`internal/review_cycle_diagnosis.go`).

This is also checked automatically every poll tick; the first time a
reviewer's diagnosis changes to a `STUCK` pattern (or changes between two
different `STUCK` patterns), the orchestrator logs and notifies once (not
every tick), and if the pattern is `unknown_stall` (nothing named below
matched, but the cycle has been quiet longer than the current phase should
plausibly take), it automatically captures a `tech-support` bundle at that
moment -- before anyone has a chance to intervene and destroy the evidence.

Known `STUCK` patterns (each below has a corresponding check in
`diagnoseReviewCycle`; adding a newly diagnosed failure mode to this doc
without adding a matching check is exactly the drift this mechanism exists
to prevent -- keep the two in step deliberately):
- `review_gate_wedge_after_restart` -- see "Reviewer wedged in
  `StateReviewGate`" below.
- `persisted_convergence_invalid` -- see "Persisted-convergence-validation
  failure" below.
- `budget_exhausted` -- see "Resource/wall-time budget exhaustion" below.
  A `LimitTransition` blocks approval. If the cycle accepted trusted results,
  it publishes a partial report before cleanup; without one, the result is
  `no_review_possible` and only cleanup remains. Also reachable as a
  non-`STUCK`, informational `awaiting cleanup` line when the budget was
  only just exhausted and the coordinator hasn't had time to retire the
  reviewer yet.
- `terminal_non_publishable_stall` -- convergence reached a terminal
  decision without any trusted accepted result. The coordinator's non-publishable
  branch is supposed to retire the reviewer immediately
  (`recordReviewIncompleteForCoder` + `retireReviewer`); still seeing one
  active past a short grace period means that cleanup didn't run or
  already failed -- see the
  [issue tracker](https://github.com/cisco-open/repository-agent-orchestrator/issues).
  Deliberately not `awaiting_verdict_publication`: no report is possible,
  so reporting it that way would hide a real stall
  behind wording that means "this will resolve itself shortly."
- `unknown_stall` -- no known pattern matched; the tech-support bundle
  captured at detection time is the starting point for a fresh
  investigation, and a good candidate for a new pattern to add here once
  root-caused.

Some patterns above are only `STUCK` past a grace period; before that (and
for two statuses that are never `STUCK` at all) a cycle that isn't
progressing for a known, expected reason is reported as such rather than
as plain `healthy` (misleading) or routed into the checks above (irrelevant
to it):
- `paused` -- an operator ran `agent pause` on this reviewer
  (`Agent.Paused`). `AgentManager.Active()` does not exclude paused
  agents, so without this an operator-requested pause of any length would
  eventually be misread as `unknown_stall` and wrongly auto-trigger a
  tech-support capture. Never `STUCK`; `agent unpause` to resume. Checked
  only *after* staleness and structural validity, deliberately: a pause
  explains away silence, but never an existing data problem -- a
  paused-but-invalid cycle still reports `persisted_convergence_invalid`,
  since pausing didn't cause that and unpausing won't fix it.
- `superseded_stale_head` -- the PR's head SHA advanced past what this
  reviewer is pinned to (`ReviewCycle.Stale`); it's retired and will be
  cleaned up in favor of a fresh reviewer, not stuck. Never `STUCK`.
- `awaiting_verdict_publication` -- convergence reached a **publishable**
  terminal result (`reviewCycleHasPublishableReport`) and its report
  genuinely has not been posted yet (`reviewCycleNeedsReportPublication`
  is true); only becomes `STUCK` if that itself takes implausibly long.
  Deliberately does not cover a cycle whose verdict was already published,
  or one with no trusted result to publish (see
  `verdict_published_awaiting_cleanup` and `budget_exhausted`/
  `terminal_non_publishable_stall` below) -- conflating any of those with
  "still awaiting publication" would describe an event that provably
  isn't going to happen.
- `verdict_published_awaiting_cleanup` -- the verdict **was** already
  published (`VerdictPublication.Status == "published"`), so no further
  publication is needed, but the reviewer is still visible as active.
  Whatever is supposed to retire it after a successful publish hasn't run
  yet or already failed. Brief informational grace period before
  escalating to `STUCK`, same reasoning as `terminal_non_publishable_stall`.
- `terminal_non_publishable_stall` -- brief informational grace period
  before escalating to `STUCK` (see above), covering the moment between a
  non-publishable terminal decision and the coordinator's cleanup actually
  running.

## First-response checklist

Run this before doing anything else -- before it, not instead of thinking
about mitigation; it's fine to mitigate immediately afterward.

1. **Capture a tech-support bundle**, on demand from the REPL, with no
   restart needed:

   ```
   tech-support
   ```

   This writes a single `tech-support-<timestamp>-<random>.tar.gz`
   (collision-safe against an overlapping automatic capture) under
   `<log-dir>/tech-support/` containing:
   - `version.txt`: the running build's exact commit
   - `config.txt`: the fully-**effective** configuration -- every default
     `loadConfig` applied, not just what `config.yaml` set explicitly (e.g.
     `HARD_GATE_MODE`, `REVIEW_GATE_ALERT_MINUTES`, poll interval), with
     secrets never printed (`WEBEX_WEBHOOK_URL`/`WEBEX_UID` show only
     "(configured)"/"(not configured)") and `CODEX_CMD`/`MANDATORY_TESTS`
     entries redacted if they look credential-bearing rather than
     printed verbatim
   - `status.txt`/`agents.txt`: the same rendering as `status`/`agent list`,
     including the resolved `REVIEW_POLICY` with every default applied
   - `budget-analysis.txt`: the automated wall-time/agent-count budget
     estimate plus any actually-observed budget exhaustion (see "Resource/
     wall-time budget exhaustion" below)
   - `agents_state.json`: a **verbatim, unredacted** copy of the persisted
     state file -- see the security note below
   - `daemon.log.tail`: a deduplicated, bounded excerpt of the daemon log.
     The excerpt collapses runs of the same repeated message (e.g. a
     recovery failure logged every few seconds for hours overnight) into
     the first occurrence plus a repeat count and time span, the same idea
     as syslog's "last message repeated N times" -- a raw byte tail of a
     long spam loop would otherwise be nothing but that one message,
     pushing out whatever different content came before it.

   One artifact to transfer (e.g. `scp`) off the host, no interactive
   terminal copy/paste required. This directly replaces the older
   step-by-step manual capture (record version, back up state, capture
   logs, capture REPL status) with one command; use the manual steps below
   only if `tech-support` itself is unavailable (e.g. the deployed binary
   predates it -- check with `status` or `--version` first).

   **Security note on `agents_state.json`**: unlike `config.txt` (secrets
   never printed; credential-bearing commands redacted), `agents_state.json`
   is included verbatim. It carries raw, unredacted, human-authored
   content pulled from GitHub -- issue titles/bodies, human review
   guidance, PR titles, and finding/verification/ledger summaries -- none
   of which the orchestrator validates or sanitizes. The orchestrator
   itself never embeds a credential it manages anywhere in the bundle, but
   that is not a guarantee the *content* is credential-free: if someone
   pasted a secret, customer data, or other sensitive material into an
   issue or comment, it's present here exactly as written. Treat a
   generated bundle with the same care as the raw state file it's built
   from -- fine to move between trusted operators/systems, but review it
   before attaching it to a third-party support ticket or otherwise
   sending it outside that trust boundary.

   Both `tech-support` (the REPL command) and an automatic unknown-stall
   capture scan `agents_state.json` for common secret-like patterns (AWS
   access keys, GitHub tokens, Slack tokens/webhooks, PEM private key
   blocks, JWT-looking tokens, and generic `key=`/`token:`/`secret=`-style
   assignments) and report per-category counts, split into **found**
   (matched a specific, well-known credential format -- treat as a real
   hit) and **suspected** (matched only the generic assignment heuristic --
   worth a look, more prone to false positives on ordinary prose). This is
   a detection aid printed alongside the bundle, not a redaction step --
   it does not modify `agents_state.json`'s content, and a category with
   zero matches is not a guarantee the file is actually clean.

2. Only after the bundle is captured: reason about mitigation
   (stop/pause/restart) separately from root cause. Mutating actions
   (stopping an agent, restarting the orchestrator, editing config, clearing
   a marker) destroy the exact state that made the problem reproducible, so
   capture first, always.

### Manual capture (fallback if `tech-support` is unavailable)

- **Record the exact build**: `status` in the REPL (its `Build` section), the
  startup log line (the first `repository-agent-orchestrator commit=...
  built=... go=...` line after the most recent restart), or run the binary
  standalone with `--version`. Without this, no other evidence can be
  reliably correlated to a known code path -- a bug may already be fixed on
  `main` but not yet deployed, or a fix may not be the one actually running.
- **Back up the persisted state file**:
  ```bash
  STATE=<repo-path>/.repository-agent-orchestrator/agents_state.json
  cp "$STATE" ~/rao-incident-$(date +%s).json
  ```
- **Capture the recent log tail**, from the last known-good activity for the
  affected agent(s) through the current failure. Prefer over-capturing to
  under-capturing; log volume is cheap, a second occurrence of an
  intermittent bug is not.
- **Capture REPL `status`/`agent list` output** for the affected agent(s).

## Known stall/loop signatures

A catalogue of failure classes that have actually been observed and
characterized. Check these before assuming a new bug.

### Resource/wall-time budget exhaustion

**Symptom:** `review agent count N reached snapshotted maximum N` or
`review wall time <N>ms reached snapshotted maximum <N>ms`, then
a partial or `no_review_possible` result.

**Status:** not a bug, a config-tuning issue, and now detected
automatically rather than requiring manual arithmetic against the pipeline
shape. `analyzeReviewPolicyBudget` (`internal/review_budget_advisor.go`)
estimates the realistic worst-case wall time and agent count a fully
healthy convergence can need from the configured policy alone (lane count,
`SWARM.TIMEOUT_MINUTES`/`RETRIES`, `VERIFICATION.TIMEOUT_MINUTES`/`RETRIES`,
`CONVERGENCE.MAX_ROUNDS`), and compares it against the configured
`MAX_WALL_TIME_MINUTES`/`MAX_REVIEW_AGENTS_PER_SHA`:

- Logged as a `review policy budget warning: ...` line at every startup
  (non-fatal -- it's an estimate, not a hard validation error, so it can be
  wrong for an unusual policy or a genuinely lightweight repository).
- Included in every `tech-support` bundle (`budget-analysis.txt`), combined
  with forensic confirmation: a scan of currently loaded reviewers for an
  actually-persisted `ReviewLimitTransition` (recorded durably the moment a
  cycle really exhausts a budget) *and* for `Convergence.Status ==
  ReviewConvergenceMaxRounds` (round exhaustion, which has no
  `ReviewLimitTransition` of its own), so you can see the estimate and the
  real evidence side by side rather than only reasoning from one or the
  other.

**Scope, stated explicitly in `budget-analysis.txt` itself:** only
`MAX_WALL_TIME_MINUTES` and `MAX_REVIEW_AGENTS_PER_SHA` are proactively
estimated. `MAX_USAGE_TOKENS` is not -- no `REVIEW_POLICY` field bounds an
agent's token/context usage (`AgentProfile` carries no such field), so
unlike wall time or agent count, every input to a token estimate would have
to be fabricated rather than derived from policy; the forensic scan above
is the only detection available for it, and it fires just like the other
two if a cycle actually exhausts it. `MAX_ROUNDS` exhaustion isn't flagged
as its own warning either -- it's already a direct multiplier of both
estimates above (`passes`/`rounds`), so a `MAX_ROUNDS` badly out of
proportion to the wall-time/agent budgets already shows up through those,
and whether `MAX_ROUNDS` itself is high enough for real convergence to
succeed is a review-quality tuning question, not a resource-budget
misconfiguration in the same sense as the other three -- but it is still
forensically detected the same way.

Each warning names a concrete suggested value. This is distinct from
`minimumReviewLifecycleCapacity` (`internal/review_capacity.go`), which is a
bare-survival floor already enforced as a hard config-load validation error
-- the historical incident policy (`SWARM.TIMEOUT_MINUTES=8`,
`MAX_REVIEW_AGENTS_PER_SHA=12`, `MAX_WALL_TIME_MINUTES=20`) passed that floor
check while still being realistically too tight, which is exactly the gap this
advisor closes. Current built-in values are documented in
`docs/REVIEW_POLICY.md`.

The number of distinct findings a real review surfaces is a property of the
repository/PR, not the policy, so the agent-count estimate can't know it --
`MAX_VERIFIERS` bounds fan-out per contested finding, not per round. Rather
than silently assuming exactly one, the estimate assumes an explicit,
policy-grounded number of concurrently-contested findings per round
(`SWARM.MIN_REVIEWERS`) and always reports the budget's actual remaining
capacity in those terms (`budget-analysis.txt`'s "this budget has room for
up to N concurrently-contested finding(s) per round" line, shown even when
no warning fires) -- a review surfacing more contested findings per round
than that will still exhaust `MAX_REVIEW_AGENTS_PER_SHA` regardless of the
warnings. If you see this failure with no corresponding startup warning,
check that capacity line first; if it's already near zero, the review
simply surfaced more contested findings than the budget had room for. If
it's comfortably positive, either the advisor's estimate doesn't fit this
repository's actual review shape or the deployed binary predates it (check
`--version`).

### Reviewer wedged in `StateReviewGate` after a restart mid-hard-gate

**Fixed.** Historical symptom, kept for anyone diagnosing a
binary that predates the fix (check `--version`): `persisted review cycle
recovery failed reviewer=...: convergent review coordinator is not active`,
repeating on every poll tick forever, paired with `review launch skipped
coder=... : active reviewer ... still owns head` -- i.e. the coordinator
could neither resume nor be replaced.

**Root cause:** `pollActiveAgent` (`internal/app.go`) intercepted any
reviewer with a non-nil `ReviewCycle` and routed it straight to
`queuePersistedReviewCycleRecovery` before `reconcilePendingReviewLaunch`
(the only code path that knows how to resume `StateInitializing`/
`StateReviewGate` via `queueCheckpointedReviewLaunchRecovery`) ever ran.
Since every production reviewer has a `ReviewCycle` attached,
`reconcilePendingReviewLaunch`'s pre-`StateWorking` handling was effectively
dead code.

**Fix:** that `pollActiveAgent` branch is now gated on `agent.State ==
StateWorking`, so a reviewer still on its first, pre-gate launch attempt
falls through to `reconcilePendingReviewLaunch`/
`queueCheckpointedReviewLaunchRecovery` instead, which actually resumes or
terminalizes it (`handleStalledRuntimeLessReviewer` past
`reviewInitStallTimeout`/`reviewLaunchStallTimeout`).

If you still see `reviewCycleDiagnosisReviewGateWedge`
(`review_gate_wedge_after_restart` in `status`'s Review Cycle Health) on a
current binary, the reviewer is *attempting* recovery but not completing it
within the stall window -- that's a different, still-open problem (e.g. the
recovery attempt itself failing repeatedly; check the daemon log for
`persisted review cycle recovery failed` or `retrying checkpointed review
launch` around the same reviewer ID) and the same interim mitigation still
applies: stop the wedged reviewer directly; the coder will launch a fresh
one on the next poll tick.

### Persisted-convergence-validation failure blocking recovery

**Symptom:** `persisted review cycle recovery failed reviewer=...: review
discovery mutation is invalid: persisted review cycle convergence is
invalid: persisted review verification assignment is invalid` /
`review convergence requires a fresh completed discovery pass`, with all
review workers already in a completed lifecycle state.

**Status:** open, partially narrowed. `validatePersistedReviewConvergence`
(`internal/review_convergence.go`) rejected a persisted
`VerificationAssignment`. In the one occurrence investigated, the assignment
sourced from the `synthesis` discovery lane checked out perfectly (every
field, including an independently recomputed `reviewFindingCandidateRevision`
hash, matched exactly). The two assignments sourced from
`challenge-coverage_gap` workers could not be checked before the state file
was overwritten by the interim mitigation before a `tech-support` bundle
existed to capture it -- exactly the scenario it exists to prevent. Next
occurrence: run `tech-support` first, then check those two assignments'
`candidate_snapshot` the same way
(a scratch Go program recomputing `reviewFindingCandidateRevision` and
comparing to the stored `candidate_revision` is the fastest way to confirm
or rule out a hash mismatch). If those also check out clean, widen the
search to `ChallengeAssignments`/`FindingVerifications`/`Rounds` in the same
validation function, which haven't been examined yet.

**Interim mitigation:** same as the `StateReviewGate` case -- stop the
wedged reviewer; the coder will get a fresh one on the next poll tick.
