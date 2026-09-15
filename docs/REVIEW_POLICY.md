# Review Policy Operator Contract

`REVIEW_POLICY` is the strict, repository-scoped control surface for automatic
review. Omitting the block selects the built-in convergent policy. Supplying a
block overrides that policy; there is no alternate single-reviewer mode.

Each tracked PR head snapshots the effective policy and runs the production
convergent coordinator after the mandatory test gate. The
snapshot, its fingerprint, the exact 40-character lowercase head SHA, worker
ownership, evidence, convergence state, and terminal publication intent are
durable. A config change applies to later review cycles; it does not rewrite an
active exact-SHA snapshot.

Policy files must not contain secrets. In particular, do not put GitHub tokens,
`WEBEX_WEBHOOK_URL`, authorization headers, credentials, or private keys in a
profile, alias, lane, or other policy value. Internal workers start with an
empty allowlisted environment and an empty worker-specific GitHub CLI config.
Their configured repository, coordinator checkout, worker checkout, PR head
repository, and PR base repository must all match `REPO_OWNER/REPO_NAME`.
A mismatch fails closed before a worker launch or GitHub write.

## Activation And Visibility

The automatic tracked-review trigger always runs the configured mandatory tests
against the exact current PR head. No top-level reviewer Codex runtime is
launched. The coordinator plans discovery lanes, launches internal discovery,
verifier, challenge, or escalation workers, and converges on one terminal
result.
- Discovery, verification, challenge, convergence, escalation, and internal
  artifact output are never forwarded to the coding agent.
- The only coder-visible review output is the exact-SHA consolidated terminal
  verdict. Inconclusive, split, escalation, and failure actions are represented
  in that verdict and always map to `NEEDS_CHANGES`.
- `THUMBS_UP` requires a separately executed full discovery pass for every
  convergence round, every selected lane in those passes to complete, every
  candidate to be independently rejected, every plan-bound coverage gap to
  close, every challenge to resolve conclusively, and the configured
  quiet-round requirement to be met. Failed, cancelled, or empty passes never
  count as quiet.

Before creating the one terminal GitHub comment, publication rechecks the live
base and head repositories and SHAs. A stable repository/PR/SHA publication ID
deduplicates retries. A pending intent retries on normal poll ticks, and an
already-created exact comment is adopted only when it was posted by the
authenticated publisher and every digest matches.

## Complete Field Reference

YAML is decoded with unknown-field rejection. All names below are
case-sensitive YAML keys.

### Envelope

| Field | Default | Contract |
| --- | --- | --- |
| `VERSION` | `1` when the whole policy is omitted | Required when `REVIEW_POLICY` is present; the only supported value is `1`. |

Policy configuration with both profile sections omitted uses the built-in profiles
and model catalog below. `AGENT_PROFILES` and `ROLE_PROFILES` must otherwise be
configured together.

### `AGENT_PROFILES`

Each mapping key is a named profile. A profile has exactly one of these shapes:

```yaml
MODEL: gpt-5.6-terra
REASONING_EFFORT: high
```

or:

```yaml
INHERIT_GLOBAL: true
```

`MODEL` is a model name or deployment alias declared in `MODEL_CATALOG`.
`REASONING_EFFORT` is normalized to lowercase. Globally recognized effort
names are `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`, and
`ultra`; the selected model's catalog entry must also list the chosen effort.
`INHERIT_GLOBAL: true` cannot be combined with either explicit field. Omitting
one explicit field is invalid. Convergent review rejects
inheritance for profiles selected by discovery lanes, verifier, challenge, or
escalation workers because their effective model and reasoning effort must be
snapshotted and reported for every launch. Inheritance remains valid for coder
and indexer profiles.

### `ROLE_PROFILES`

All six fields are required when this section is present, and each must name an
`AGENT_PROFILES` entry.

| Field | Runtime use |
| --- | --- |
| `CODER` | New coding and correction runtimes. |
| `INDEXER` | Repository indexing runtimes. |
| `DISCOVERY` | Default discovery-worker profile. A lane-specific `PROFILE` overrides it for that lane. |
| `VERIFIER` | Independent candidate verification. |
| `CHALLENGE` | Targeted coverage-gap or competing-hypothesis challenges. |
| `ESCALATION` | Default source for `ESCALATION.PROFILE`. |

Built-in role defaults:

| Role/profile | Model alias | Effort |
| --- | --- | --- |
| `coder` | `gpt-5.6-sol` | `high` |
| `indexer` | `gpt-5.6-luna` | `medium` |
| `discovery` | `gpt-5.6-terra` | `high` |
| `verifier` | `gpt-5.6-terra` | `high` |
| `challenge` | `gpt-5.6-sol` | `xhigh` |
| `escalation` | `gpt-5.6-sol` | `max` |

### `MODEL_CATALOG`

Each key is a model name or alias and has:

| Field | Default | Contract |
| --- | --- | --- |
| `REASONING_EFFORTS` | none for a new alias | Non-empty list of recognized effort names. Duplicates are normalized away. |
| `AVAILABLE` | `true` | `false` prevents any selected profile from using the entry. |

Configured entries merge into the built-in catalog and replace a built-in
entry with the same key. Built-in aliases are `gpt-5.6-sol`,
`gpt-5.6-terra`, and `gpt-5.6-luna`; each is available and supports `none`,
`low`, `medium`, `high`, `xhigh`, and `max`.

An alias must begin with an ASCII letter or digit. Remaining characters may be
ASCII letters, digits, `.`, `_`, `:`, `/`, or `-`.

### `REVIEW_SWARM`

| Field | Default | Contract |
| --- | --- | --- |
| `MIN_REVIEWERS` | `3` | Minimum number of discovery lanes selected for an exact SHA. |
| `MAX_REVIEWERS` | `6` | Maximum selected discovery lanes. |
| `MAX_PARALLEL_REVIEWERS` | `6` | Maximum concurrently running discovery workers. |
| `TIMEOUT_MINUTES` | `30` | Model-runtime deadline for one discovery or challenge worker attempt. Worktree setup and artifact intake use separate bounds. |
| `RETRIES` | `1` | Additional discovery or challenge model-runtime attempts. Worktree setup has its own retry loop before a model starts. |
| `LANES` | built-in six-lane list | Ordered available discovery lanes. |

Every explicit lane requires:

| Field | Contract |
| --- | --- |
| `NAME` | One of `contract`, `callers`, `lifecycle`, `persistence-recovery`, `concurrency-ordering`, or `operations-tests`. |
| `REQUIRED` | Explicit `true` or `false`. Required lanes are always planned and must complete before approval. |
| `PROFILE` | Named profile used by this lane's discovery worker. |

The built-in lanes make `contract`, `callers`, and `operations-tests`
required and map all lanes to the discovery role profile. The deterministic
planner preserves required lanes, adds lanes relevant to changed subsystems,
then fills to the minimum without exceeding the maximum. Filename and task
signals select review coverage; they do not assign severity or trigger model
escalation. Changed test files and diff size do not increase perceived risk. Lane workers
receive the exact diff and plan-bound acceptance-criterion, changed call-path,
state-transition, and risk-domain coverage requirements.

Each lane receives its own responsibility, hypotheses, checklist, and evidence
requirements: contract and acceptance criteria; changed symbols and callers;
lifecycle and state transitions; persistence and recovery; concurrency and
ordering; or operations and tests. Every lane must continue after its first
finding, inspect its assigned scope without repeating other lanes, and declare
any unreviewed areas in its artifact.

After all planned lanes complete, one required `synthesis` discovery worker
runs sequentially with the discovery profile in the exact-SHA checkout. It
receives every lane artifact, the canonical candidate set, coverage gaps, and
current verification state. It reconciles cross-lane evidence, may identify
contradictions and omissions in the supplied evidence, and reports explicit
unreviewed areas. It does not independently repeat the lane reviews. Synthesis candidates enter the same
canonicalization and independent verification path as lane candidates. Every
new convergence discovery pass runs synthesis again, including the pass after
new findings are confirmed.

### `ARTIFACTS`

| Field | Default | Contract |
| --- | --- | --- |
| `MAX_BYTES` | `1048576` | Maximum encoded bytes for one worker artifact. |
| `TIMEOUT_SECONDS` | `30` | Timeout for each publication/intake attempt. |
| `RETRIES` | `1` | Additional attempts after the initial attempt; `0` disables retries. |

Artifacts use the strict exact-SHA envelope documented in
[REVIEW_ARTIFACTS.md](REVIEW_ARTIFACTS.md). Schema, identity, phase, ownership,
checkout, path, size, and secret-material validation happens before evidence is
accepted. Rejected evidence cannot complete a lane or assignment.

### `VERIFICATION`

| Field | Default | Contract |
| --- | --- | --- |
| `MIN_VERIFIERS` | `1` | Independent outcomes needed to decide a candidate. |
| `MAX_VERIFIERS` | `3` | Maximum assignments for one candidate revision. |
| `MAX_PARALLEL_VERIFIERS` | `2` | Maximum concurrent verifier workers. |
| `TIMEOUT_MINUTES` | `30` | Model-runtime deadline for one verifier attempt. Worktree setup and artifact intake use separate bounds. |
| `RETRIES` | `1` | Additional verifier model-runtime attempts. Worktree setup has its own retry loop before a model starts. |
| `REQUIRE_EVIDENCE` | `true` | Requires independent location/path evidence and test/reproduction evidence or an explanation that testing is impractical. |
| `INCONCLUSIVE_ACTION` | `escalate` | Terminal action for evidence-complete but inconclusive verification. |

Originating discovery and challenge workers are excluded from independent
verification. Configured capacity, timeout, retry, evidence, and action values
come from the exact-SHA snapshot.

### `CONVERGENCE`

| Field | Default | Contract |
| --- | --- | --- |
| `QUIET_ROUNDS_REQUIRED` | `1` | Persisted rounds with no new confirmed material finding or material coverage gap. |
| `MAX_ROUNDS` | `2` | Hard round bound before a terminal maximum-round action. |
| `MAX_REVIEW_AGENTS_PER_SHA` | `50` | Hard exact-SHA worker-reservation limit. The default includes headroom over the built-in policy's discovery, synthesis, verification, and challenge fan-out. |
| `MAX_WALL_TIME_MINUTES` | `600` | Hard elapsed-time budget from trusted cycle metrics. The default accommodates the built-in policy's 30-minute runtime windows and one retry across sequential discovery, synthesis, and verification phases. Active worker deadlines are capped by the remaining cycle time. |
| `MAX_USAGE_TOKENS` | `2000000` | Hard supported-total-token budget from trusted worker usage metrics. |

Every value must be positive. `QUIET_ROUNDS_REQUIRED` cannot exceed
`MAX_ROUNDS`. The configured agent limit must cover the minimum discovery
lanes plus synthesis for every required quiet pass and retry, followed by at
least one independent verifier and its retries. Configuration validation
rejects a limit that cannot complete this minimum lifecycle.

The configured per-SHA maximum never grows at runtime. Discovery, synthesis,
verification, retries, and challenges all consume the same reservation budget.
Verification queues are bounded by remaining capacity; work that cannot fit is
retained as unresolved evidence when the limit produces a partial or
`no_review_possible` result.

The coordinator evaluates the configured limit before ownership reservation and
again before dispatch where appropriate. Exhaustion persists one terminal
transition and the unresolved planned lanes, candidates, coverage gaps, and
challenges. Capacity exhaustion, worker failure, and unresolved mandatory work
record an explicit partial result and post a GitHub notice that the exact
commit did not receive a complete verdict. They cannot render as approval or
as `NEEDS_CHANGES` without confirmed patch-caused findings. A coder-linked
incomplete cycle terminalizes its exact durable coordinator attempt and
schedules a 30-second same-head retry. Coordinator setup failures use the same
ledger. Polling reserves a new monotonically increasing attempt without a push,
checkpoint commit, or changed head, bounded to three automatic attempts for
that SHA.

### `ESCALATION`

| Field | Default | Contract |
| --- | --- | --- |
| `AFTER_NON_CONVERGING_ROUNDS` | `1` | Completed non-converging round threshold for the escalation profile. |
| `AFTER_CORRECTION_ROUNDS` | `2` | Durable fresh coder-runtime authorizations allowed before remaining changes are escalated to human correction. |
| `PROFILE` | selected `ESCALATION` role profile | Named profile used after an escalation transition. |

The non-converging round threshold must be positive and no greater than
`MAX_ROUNDS`; the correction-round threshold must be positive.
Escalation changes the runtime profile; it does not bypass assignment,
evidence, coverage, convergence, or approval requirements.
Each fresh coder correction launch reserves its next attempt in durable coder
state, then persists `running` before invoking the runtime launcher. A known
launch failure terminalizes that charged attempt. Recovery adopts a live
`running` runtime, but a missing one is terminalized and any permitted
replacement receives a new attempt; it can never restart under the old ordinal.
Correction attempts are never inferred from review-ledger heads, GitHub
comments, tmux absence, or model output. At the limit, `NEEDS_CHANGES` leaves
or moves the PR to draft, stops automated correction, and waits for a human;
it never marks the PR ready.

Each pushed head records the exact previous-head-to-current-head delta before
worker launch. Unresolved findings and findings resolved on the immediately
preceding head are injected into the next cycle for independent verification.

### `FAILURE_ACTIONS`

| Field | Default | Runtime trigger |
| --- | --- | --- |
| `REQUIRED_LANE_FAILURE` | `escalate` | A planned required discovery lane is missing or failed. |
| `VERIFICATION_FAILURE` | `escalate` | Verifier or challenge capacity, launch, runtime, artifact, or state failure remains unresolved. |
| `BUDGET_EXHAUSTION` | `inconclusive` | Agent, wall-time, token, or maximum-round budget is exhausted. |

Every action-valued field (`VERIFICATION.INCONCLUSIVE_ACTION` and all failure
actions) accepts exactly:

- `escalate`: stop with an explicit escalation-required verdict;
- `fail`: stop with an explicit terminal failure;
- `inconclusive`: stop without approval and report unresolved evidence.

## Profile Precedence

Effective model and effort resolution is deterministic:

1. Built-in role profiles and model catalog provide the base.
2. Repository `AGENT_PROFILES`, `ROLE_PROFILES`, lane profiles, and catalog
   entries replace the corresponding base selections.
3. Deployment variables override one named profile:
   `RAO_PROFILE_<PROFILE>_MODEL`,
   `RAO_PROFILE_<PROFILE>_REASONING_EFFORT`, and
   `RAO_PROFILE_<PROFILE>_INHERIT_GLOBAL`.
4. Process `--model` and `--reasoning-effort` flags override every selected
   role profile.

For environment lookup, the profile name is uppercased and `-` becomes `_`;
characters other than ASCII letters, digits, `_`, and `-` are removed.
Profiles that collapse to the same environment name are rejected when an
override is present.

A model-only or effort-only deployment/CLI override combines with the other
lower-precedence explicit value. An inherited profile has no explicit lower
value, so both CLI flags are required to override it. `INHERIT_GLOBAL=true`
cannot coexist with deployment model or effort overrides.

For an explicit profile, Repository Agent Orchestrator replaces top-level
`--model`, `--model=...`, or `-m` values in `CODEX_CMD` and appends the resolved
`model_reasoning_effort`. Global Codex model and effort are used only by an
explicit inherited profile. Convergent worker profiles must resolve to explicit
values; supplying both CLI profile flags can materialize an otherwise inherited
profile before the policy is snapshotted.

## Validation Errors

Configuration loading fails before any runtime is created when any rule below
is violated. Errors name the affected `REVIEW_POLICY` field.

- YAML: an unknown key, wrong scalar/list/mapping type, duplicate mapping key
  rejected by YAML decoding, or unsupported `VERSION`.
- Profile sections: only one of `AGENT_PROFILES` and `ROLE_PROFILES` is
  present; the profile map is empty; a trimmed profile name is empty or
  duplicated; a profile mixes inheritance with explicit values; or an
  explicit profile omits model or effort.
- Role and lane references: a required role reference is empty or unknown; an
  escalation or lane profile is empty or unknown; a lane name is unsupported
  or duplicated; or an explicit lane omits `REQUIRED`.
- Deployment overrides: an inheritance value is not boolean; inheritance is
  combined with model/effort; or two profile names collide after environment
  normalization.
- Model catalog: an alias is empty, duplicated after trimming, unsafe, missing
  efforts, unavailable, or absent for a selected profile; an effort is
  globally unsupported or unsupported by the selected alias.
- CLI: a repeated or empty profile flag, a one-sided override of an inherited
  profile, or an effective model/effort combination rejected by the catalog.
- Swarm: a bound is not positive; minimum or parallel count exceeds maximum;
  lane count is below the minimum; or required-lane count exceeds the maximum.
- Artifacts: byte or timeout bounds are not positive, or retries are negative.
- Verification: count or timeout bounds are not positive; retries are
  negative; minimum or parallel count exceeds maximum; or the inconclusive
  action is unsupported.
- Convergence: a value is not positive; quiet rounds exceed maximum rounds; or
  the worker budget is below the minimum formula described above.
- Escalation: the round is not positive, exceeds maximum rounds, or references
  an unknown profile.
- Failure actions: any action is not one of `escalate`, `fail`,
  and `inconclusive`.

Persisted active cycles are also validated at startup. An invalid exact SHA,
policy version/fingerprint, plan, ownership, artifact, metric, limit,
convergence, or publication checkpoint stops startup with a clear error rather
than launching work from ambiguous state.

## Status And Troubleshooting

Run `status` to inspect the safe effective view:

- enabled/disabled state, version, and snapshot fingerprint;
- role-to-profile/model/effort mapping;
- discovery lanes and required flags;
- swarm and artifact bounds;
- convergence budgets; and
- escalation settings.

For every retained enabled cycle, the same command also shows:

- the cycle, full exact head SHA, logical assignment, role, lane, selected
  profile, model, reasoning effort, attempt, and worker lifecycle;
- planned and cumulative reserved attempts plus current running, completed,
  failed, and cancelled counts;
- the historical overall, reviewer, and verifier/challenge peak parallel
  worker counts and whether the observed reservation/parallel counts are
  within their respective snapshotted swarm and per-SHA budget bounds;
- one normalized review result when final: `complete_clean`,
  `complete_with_findings`, `partial_no_findings`,
  `partial_with_findings`, or `no_review_possible`; and
- the consolidated publication state, stable identity, and comment ID.

Startup emits one sanitized structured `effective_review_policy` log record.
Cycle start, stale-head invalidation, coordinator cancellation, and
publication emit sanitized structured cycle records from the same durable
checkpoint. Per-worker reservations and lifecycle transitions are persisted
for live `status` inspection without adding launch-path logging latency.
Neither surface includes command strings, prompts, diffs, source excerpts,
raw environment values, unused catalog entries, tokens, webhook URLs, or
authorization values.

Common operational diagnoses:

- No worker after a trigger: inspect the mandatory-test gate first,
  then the configured repository/origin, exact PR base/head repositories, and
  the policy validation error. Repository mismatch is intentionally fail
  closed.
- `partial_no_findings` or `partial_with_findings`: read the consolidated
  completed, unresolved, and failed scopes plus infrastructure diagnostics.
  Confirmed findings still request changes; a partial result without findings
  is a neutral report, never approval or a clean-review claim.
  `no_review_possible` means no trusted worker result was accepted, so there is
  no useful report to publish.
- A terminal intent is `pending`: GitHub publication exhausted its short retry
  window. Normal poll ticks and daemon restart both retry the same stable
  publication; do not post a replacement manually.
- A head moves during review: the old cycle is marked stale, its work is
  canceled and ignored, and a fresh snapshot is launched for the new exact
  head.
- Restart during planning or work: the coordinator resumes from the first
  missing durable checkpoint, adopts a live current owner when safe, or uses a
  fresh attempt for the same logical identity. Completed identities are not
  relaunched.

## Human-Supervised Same-Repository Smoke Test

This procedure is optional and must remain human supervised. Use only a
disposable draft PR in the repository named by `REPO_OWNER/REPO_NAME`; do not
point the daemon, a worker, or any validation command at another repository.
Do not paste, commit, or retain prompts, diffs, findings, credentials,
authorization data, webhook values, or raw environment output as evidence.

1. Stop the daemon. Make a protected local backup of its configuration without
   printing the file.
2. From an up-to-date `main` checkout of the configured repository, create a uniquely
   named disposable branch such as
   `smoke/convergent-review-YYYYMMDD-HHMMSS`. Make one harmless,
   human-authored documentation-only change, commit it, push it to the
   configured repository, and open a draft PR against `main`. Confirm both the
   head and base repositories shown by GitHub exactly match
   `REPO_OWNER/REPO_NAME`.
3. In the protected local configuration only, select explicit, already-approved profiles
   for discovery, verifier, challenge, and escalation roles; use conservative
   reviewer/verifier parallel bounds and a per-SHA agent budget. Do not change
   checked-in defaults based on this run.
4. Start the daemon in the foreground and keep the PR and terminal visible.
   At the REPL run `status`, record only the safe policy fingerprint, selected
   profile/model/effort mappings, and configured and calculated required
   bounds, then run
   `agent review <disposable-pr-number>`.
5. While the cycle runs, use `status` to verify each actual attempt has the
   same full head SHA and displays its logical assignment, role, lane, profile,
   model, effort, attempt, and lifecycle. Verify cumulative reservations do
   not exceed the configured `max_agents_per_sha`, `reviewer_parallel_observed` does not
   exceed `reviewer_parallel`, and `verifier_parallel_observed` does not
   exceed `verifier_parallel`. Stop the test if the head changes unexpectedly
   or any intermediate worker feedback appears on the PR.
6. At terminal state, verify `status` shows one of the documented normalized
   outcomes and exactly one stable consolidated publication identity. On the
   GitHub PR, verify there is at most one Repository Agent Orchestrator
   consolidated verdict for that exact head and no child-worker comments,
   reviews, or other coder-visible feedback. A pending publication may be
   allowed to retry; do not post a replacement manually.
7. Clean up before leaving the test: stop the daemon and restore the protected
   configuration backup. Close the
   disposable PR without merging, remove its disposable remote/local branch,
   and clean up its managed review agent/worktree through the normal agent
   cleanup command if it remains listed.

One smoke run is operational evidence only. It must not be used to claim
recall, precision, quality, or benchmark results, and it must not select new
defaults.

## Tested Examples

All public examples are continuously loaded with strict unknown-field checking:

- [`config/sample.yaml`](../config/sample.yaml) omits the policy and uses the
  built-in convergent policy;
- [`config/sample-swarm-minimal.yaml`](../config/sample-swarm-minimal.yaml)
  declares the built-in convergent-review defaults explicitly; and
- [`config/sample-swarm.yaml`](../config/sample-swarm.yaml) shows independent
  discovery, verifier, challenge, escalation, lane-specific profiles, bounds,
  budgets, and terminal actions.

Copy an example outside the checkout and adapt its repository paths, models,
bounds, and actions. Never commit a real deployment config.
