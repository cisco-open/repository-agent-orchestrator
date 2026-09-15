# Review Artifact Contract

Repository Agent Orchestrator uses one strict JSON contract for evidence passed
from an internal convergent-review worker to its coordinator. The production
scheduler invokes this boundary only for an exact-SHA policy snapshot with
the convergent review coordinator.

## Envelope

Every completed artifact is one JSON object:

```json
{
  "exact_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "cycle_id": "review-cycle-aaaaaaaaaaaa-0123456789abcdef0123456789abcdef",
  "cycle_revision": 1,
  "worker_id": "review-worker-review-cycle-aaaaaaaaaaaa-0123456789abcdef0123456789abcdef-r1-discovery-p1-contract-a1-0123456789abcdef0123456789abcdef",
  "role": "discovery",
  "lane": "contract",
  "phase": "discovery",
  "pass": 1,
  "attempt": 1,
  "checkout": {
    "exact_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "clean": true
  },
  "payload": {
    "kind": "discovery",
    "summary": "One lifecycle candidate",
    "candidates": [
      {
        "candidate_id": "candidate-lifecycle-1",
        "summary": "Cancellation can return before worker cleanup",
        "location": {
          "path": "internal/review_workflow.go",
          "symbol": "pollReviewAgent",
          "start_line": 140,
          "end_line": 148
        },
        "behavioral_path": "poll cancellation returns while cleanup is active",
        "violated_invariant": "owned workers finish cleanup before coordinator return",
        "severity": "high",
        "confidence": "low",
        "evidence": [
          {
            "summary": "the cancellation branch returns before the cleanup join",
            "path": "internal/review_workflow.go",
            "start_line": 140,
            "end_line": 148
          }
        ]
      }
    ],
    "coverage": [
      {
        "requirement_id": "risk-domain:lifecycle",
        "kind": "risk_domain",
        "status": "covered",
        "evidence": [
          {
            "summary": "inspected cancellation, exit, and cleanup paths",
            "path": "internal/review_workflow.go"
          }
        ]
      }
    ],
    "unreviewed_areas": []
  }
}
```

The decoder rejects unknown fields at every level, trailing JSON values,
missing identity or SHA fields, malformed canonical SHAs, and any payload that
does not match the current worker contract. Worker artifacts are ephemeral and
the workers and coordinator run from the same binary, so there is no artifact
schema version, compatibility path, fallback, or migration.

The coordinator requires all identity fields to match one registered,
persisted `ReviewWorkerOwnership` for the active cycle and attempt. The exact
SHA must also match the coordinator's cycle checkpoint and the clean checkout
attestation.

## Typed Payloads

The envelope phase and payload kind must match:

- `discovery`: summary, zero or more material candidates, plan-bound
  coverage claims, and an explicit list of unreviewed areas. Each candidate
  records its worker identifier, file and
  symbol, behavioral path, violated invariant, severity, confidence, and
  evidence. Each coverage claim records a requirement, coverage kind, status,
  and evidence.
- `verification`: finding ID, `confirmed`, `rejected`, or `inconclusive`
  outcome, summary, task-scope and patch dispositions, the exact candidate
  location and behavioral path, independent evidence, causal evidence,
  and test/reproduction evidence (or a reason it is not practical).
- `challenge`: `upheld`, `overturned`, or `inconclusive` outcome, summary,
  persisted assignment and named target identities, zero or more material
  candidates, and plan-bound coverage claims.

Discovery artifacts require a discovery worker, verification requires a
verifier, and challenge permits challenge or escalation workers. The required
`synthesis` worker publishes the same discovery payload, so synthesis-only
candidates are retained and verified without a second ingestion path.
The coordinator derives paths from candidate locations and evidence, then
validates them as clean relative paths that exist inside the worker checkout;
absolute paths, traversal, missing paths, and symlink escapes are rejected.
Workers must not cite temporary reproduction files that they delete before
publication. Transient test output belongs in the evidence summary with an
empty path, or the evidence should cite the relevant tracked source or test
path that remains in the clean checkout.

## Bounded Discovery And Normalization

The enabled discovery scheduler records every selected plan lane as `queued`
before launch. It admits at most
`REVIEW_SWARM.MAX_PARALLEL_REVIEWERS` running lanes, transitions each lane
through `running` to `completed` or `failed`, and checkpoints each transition.
A pass number can be scheduled only once, and the next pass cannot begin until
the prior pass is terminal. Cancellation stops live worker runtimes and joins
every scheduler goroutine before returning.

Review-plan schema version 2 derives deterministic requirements for:

- issue acceptance criteria
- changed call-path surfaces
- lifecycle, concurrency, and persistence state transitions
- classified risk domains

Claims outside that exact-SHA requirement set are rejected. A missing,
partial, or explicitly uncovered requirement becomes a stable named gap.
Aggregation sorts requirements and evidence so lane completion order cannot
change the result.

Candidate IDs remain worker-local provenance. The coordinator fingerprints
normalized behavior, file/symbol location, and violated invariant, then
derives a `finding-<sha256>` logical ID. Equivalent reports merge evidence and
provenance; distinct invariants or behavioral paths remain separate even when
they share a file. Severity and confidence remain independent fields.
Discovery receipts and normalized state stay internal and do not create a
GitHub comment.

## Independent Verification And Targeted Challenges

The convergence coordinator snapshots deterministic verifier
assignments before launch. Each assignment carries the complete set of
originating worker IDs as exclusions. A verifier ownership is accepted only
when it has the verifier role, owns that exact assignment, and is not one of
those origins. The configured minimum, maximum, parallel limit, timeout, and
retry count come from the exact-SHA policy snapshot. The per-SHA capacity grows
with every candidate and challenge before assignments launch, so none are
silently dropped. Exhausted effective capacity and worker or infrastructure
failures produce an explicit partial result with the configured action and
remaining mandatory work. Accepted results remain authoritative when an
unrelated assignment fails.

Confirmed and rejected outcomes require the exact assigned location and
behavioral path, independent evidence, and either test/reproduction evidence
or an explanation of why it is not practical. At least one independent or
test/reproduction evidence item must cite the assigned location path; test
evidence satisfies this requirement when the assigned location is a test.
Only confirmed exact-SHA findings enter the publishable finding view. Rejected,
inconclusive, pending, and failed candidates cannot enter that view or make the
cycle approval-eligible.

Every challenge assignment names either one current plan coverage gap or one
competing hypothesis. Its prompt includes the full exact diff, review plan,
coverage state, canonical findings, and verification summary. Identical
unchanged challenge contexts cannot be scheduled twice. Challenge candidates
join the canonical candidate intake and begin as pending independent
verification. A targeted coverage gap closes only when the accepted challenge
artifact contains a plan-bound `covered` claim with evidence for that exact
requirement.

## Verified Quiet-Round Convergence

Each internal round is bound to a separately executed, completed full discovery
pass and checkpointed before verifier or challenge workers launch. The round
ledger carries candidate observations across passes and records confirmed
findings and coverage-gap changes during active work; raw comments do not count.
A new candidate, a new confirmed finding, a new material gap, or any still-open
gap resets quiet progress. A failed or cancelled lane, an empty pass, and
verification-only work cannot count as quiet. Inconclusive, failed, and
capacity-blocked work records an explicit unresolved outcome. The cycle
converges only after the configured consecutive independently rediscovered quiet
rounds, and reaching `MAX_ROUNDS` first records an explicit maximum-round
action. The pass binding, complete round ledger, and quiet count are validated
and restored unchanged after restart.

## Cross-SHA Finding And Coverage Ledger

The cross-SHA ledger is owned by the coder rather than a disposable
exact-SHA coordinator, so accepted review knowledge survives both coordinator
cleanup and daemon restart. It stores deterministic finding identities,
reported, fixed, rejected, or missed dispositions, discovery and independent
verification evidence, source and observation SHAs, plan-bound coverage, and
append-only status and provenance histories. Rejected candidates stay in this
internal ledger and have no publication path. Ledger schema version 5 also
stores an append-only binding for every head transition: the normalized exact
delta, the complete immutable coverage requirement set accepted for that head,
and its canonical digest. The manager persistence boundary recomputes that
binding from its separately owned `ReviewPlanInputs` and `ReviewPlan` snapshots;
a successor cannot authorize coverage or an identity rename by supplying a
self-consistent delta and requirement set inside the ledger.

A ledger transition consumes normalized `ReviewPlanInputs` for the exact
previous-head-to-new-head diff and the immutable coverage requirements from
the current exact-SHA `ReviewPlan`. Changed and previous rename paths mark only
intersecting reported or missed findings and evidenced coverage for recheck;
unaffected entries are retained unchanged. When an exact-diff rename moves the
same canonical behavior to a new path, the ledger links the old and new
path-bound identities on the rename head even when no observation is produced.
That continuity includes rejected identities that must remain suppressed and
fixed identities whose original verification remains durable. It preserves the
prior history as an exact prefix and rechecks one carried reported or missed
finding instead of discovering a novel duplicate. An impacted finding is not
inferred fixed when it is absent from the next result. It reaches `fixed` only
through a fresh exact-SHA verifier assignment receipt after an audited impact
transition. Impacted coverage remains unresolved until a current-head claim
replaces it; a later plan binds that claim to the exact persisted unresolved
requirement in addition to its delta-derived requirements.

The coder-ledger persistence boundary accepts only an append-only successor of
the manager-owned checkpoint. Every prior finding and coverage identity must
remain present, each prior audit history must be an exact prefix, and a renamed
finding must include an explicit old-to-new identity transition whose canonical
identity is the path-only projection of the prior finding through a rename in
that head's exact delta. Independently valid replacement ledgers cannot drop
durable entries, rewrite their history, or relabel prior findings as unrelated
canonical behavior.
Each history is also validated as one continuous state machine: every entry's
prior status, provenance, identity, and evidence must match the preceding
result, and reconstructed recheck state, source SHA, and observation SHA must
match the final record. Transition head indexes must be nondecreasing, and
coverage impact transitions preserve their prior status. Coverage history
carries a canonical digest of the complete requirement ID, kind, and
description. Every coverage recording also carries the digest of its exact
immutable requirement set and is accepted only when that set contains the
complete requirement. These bindings prevent a persisted or successor ledger
from changing the meaning of a durable requirement or importing independently
constructed coverage.

Each finding has two evidence bindings. Its audit digest binds the exact head,
canonical worker provenance, discovery evidence, verification evidence, and a
durable verifier receipt containing the finding identity, exact SHA, assignment
identity, candidate revision, verifier worker identity, and outcome. Its
material digest excludes the exact head, worker identities, and verifier
receipt and is used only for semantic equality and suppression. Rediscovering
a rejected or already-reported identity with identical material evidence is
therefore suppressed even when the exact-SHA attempt has a new worker owner.
Verified-fix transitions instead require an impacted finding and a fresh
current-head assignment receipt. A new verifier can verify the fix with the
same reproduction evidence, while editing free-form evidence without a new
receipt cannot establish freshness.

Every newly observed identity includes evidence about whether it existed at
the delta base:

- on the first PR head, presence means `pre_existing` and absence means
  `original_pr`;
- on later heads, presence means `previously_missed` and absence means
  `fix_introduced`; and
- an already reported identity becomes `previously_reported`.

Discovery, post-fix reopen, and materially changed rejected-candidate reopen
transitions must carry the delta-base presence evidence used for that
classification. Other transition reasons cannot carry it. Restart validation
reconstructs the classification from the append-only head bindings and rejects
missing or contradictory presence evidence.

This ledger and transition layer runs only with the convergent review
scheduler.

## Atomic Publication

Each worker receives an allowlisted `RAO_REVIEW_ARTIFACT_DIR` outside the
reviewed checkout. The directory must be absent before launch and is created
empty with owner-only permissions. Before creating it, the coordinator writes
and flushes a sibling owner marker bound to the persisted worker owner ID.
That marker lets restart cleanup distinguish an outbox created before its
claimed bit was checkpointed from unrelated pre-existing content.

Publication:

1. validates the envelope against the exact worker ownership;
2. encodes it, rejects prohibited secret material, and enforces
   `REVIEW_POLICY.ARTIFACTS.MAX_BYTES`;
3. writes an owner-only temporary file in the artifact directory;
4. flushes and closes the complete file;
5. atomically links it to `<worker-owner-id>.json` without replacing an
   existing destination, then removes the temporary name; and
6. flushes the containing directory.

Temporary files begin with `.review-artifact-partial-` and are ignored by
intake. A completed destination is immutable. Every retry has a distinct
persisted owner ID, attempt number, worktree, artifact directory, and final
path, so it cannot overwrite evidence from another attempt.

Retry reservation and logical lane-completion replacement are one serialized
checkpoint mutation. If the checkpoint fails, the new ownership and completion
removal are rolled back before another checkpoint can observe them. Directory
claim checkpoint failure is rolled back in memory; the durable owner marker
allows restart cleanup to remove the unclaimed directory and marker safely.

`TIMEOUT_SECONDS` bounds each artifact publication or intake attempt. `RETRIES`
is the number of additional local publication attempts allowed after a
transient failure. Discovery and challenge runtime deadlines and retries are
configured separately under `REVIEW_SWARM`; verifier runtime bounds remain
under `VERIFICATION`. Validation, shape, size, path, identity, and
destination-conflict failures are terminal; timeout, partial-file, and
retryable local I/O failures are transient.

Each worker receives the explicit current envelope contract and must publish
by atomically renaming a partial file to the exact final path
`$RAO_REVIEW_ARTIFACT_DIR/$RAO_REVIEW_OWNER_ID.json`. The coordinator observes
no other filename; worker prompts include the strict role-specific payload
shape and the environment-bound identity fields required by intake.

Defaults:

- `MAX_BYTES`: `1048576`
- `TIMEOUT_SECONDS`: `30`
- `RETRIES`: `1`

## Trusted Intake

Before accepting or checkpointing evidence, the coordinator verifies:

- the final path and size bound;
- the strict current envelope and role-specific payload shape;
- the active cycle's exact SHA;
- the registered owner, cycle/revision, role, lane, phase, pass, and attempt;
- the clean-checkout attestation and the checkout's actual `HEAD` and status;
- every referenced path; and
- prohibited credential material, including configured coordinator secrets,
  authorization values, private keys, known GitHub token forms, and Webex
  incoming-webhook URLs. This scan runs on both the raw bytes and the decoded,
  canonical envelope so JSON escapes cannot conceal a credential.

An accepted artifact is checkpointed with a canonical SHA-256 digest and then
advances exactly one lane/phase/pass completion. A rejected artifact records
only safe failure class/code metadata. It never advances completion, and its
untrusted content is not copied into coordinator state or error text. Intake
surfaces a safe I/O failure if that rejection metadata cannot be checkpointed.

Active review cycles persisted before artifact limits existed are
accepted only when their original policy fingerprint is valid. On restart,
the coordinator migrates that snapshot to the default artifact limits and a
new fingerprint before recovering the reviewer and any plan or worker
ownerships. Legacy review-plan schema version 1 checkpoints are upgraded only
when they exactly rebuild from their persisted inputs and policy; the upgrade
adds deterministic coverage requirements and moves the plan to schema version
2.
