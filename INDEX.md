# Repository Index

## Summary

- Repository: `repository-agent-orchestrator`
- Summary: Local Go daemon that orchestrates Codex coding, review, and repo-indexing agents for one GitHub repository through isolated worktrees, tmux runtimes, persisted state, and human-reviewed pull requests.
- Preferred starting points: `README.md`, `AGENTS.md`, `ARCHITECTURE.md`, `docs/AGENT_CONTEXT.md`
- Stable components: `process_and_config`, `agent_lifecycle`, `github_control_plane`, `runtime_sessions`, `agent_context_routing`, `review_coordinator`
- Primary concerns: `deterministic_local_orchestration`, `worktree_isolation`, `review_convergence`, `document_routing`, `secret_safe_operations`, `human_review_gates`

## Groups

### `getting_started`

Repository overview, setup, configuration, commands, and operator workflow.

### `system_design`

Architecture, lifecycle behavior, persistence, worktrees, and runtime boundaries.

### `agent_context`

Deterministic document routing and structured handoff persistence.

### `agent_policy`

Mandatory operating, safety, testing, and contribution rules for agents and contributors.

### `review_contracts`

Exact-SHA review policy, worker evidence, verification, convergence, and publication contracts.

### `security_community`

Security reporting and community conduct expectations.

## Documents

### `README.md`

- Group: `getting_started`
- Kind: `overview`
- Priority: `foundational`
- Summary: Fast orientation to agent roles, prerequisites, configuration, REPL commands, workflow, persistence, and operational limitations.
- Components: `process_and_config`, `agent_lifecycle`, `github_control_plane`, `runtime_sessions`
- Concerns: `configuration`, `operator_workflow`, `agent_roles`, `runtime_prerequisites`
- Artifacts: `repository_agent_orchestrator_cli`, `config_sample_yaml`, `config_sample_swarm_yaml`, `mandatory_tests`, `codex_cmd`, `agent_index_yaml`
- Read when:
  - You need the fastest orientation to what the daemon does or how to operate it.
  - You need CLI flags, configuration keys, prerequisites, or workflow behavior.
- Usually skip when:
  - You need detailed lifecycle invariants, routing algorithms, or review field validation.

### `AGENTS.md`

- Group: `agent_policy`
- Kind: `agent_contract`
- Priority: `foundational`
- Summary: Stable repository contract covering scope, safety, verification, implementation rules, product guardrails, and commit conventions.
- Components: `agent_lifecycle`, `github_control_plane`, `runtime_sessions`
- Concerns: `secret_safety`, `scope_control`, `testing_requirements`, `logging_policy`, `commit_conventions`
- Artifacts: `gh_auth_token`, `webex_webhook_url`, `make_test`, `make_build`, `conventional_commits`
- Read when:
  - You are about to modify this repository and need its local operating rules.
  - You need required checks, logging constraints, or commit message format.
- Usually skip when:
  - You need product behavior or architecture details rather than contribution rules.

### `ARCHITECTURE.md`

- Group: `system_design`
- Kind: `architecture`
- Priority: `foundational`
- Summary: Current architecture for process startup, agent roles, GitHub polling, runtime isolation, review coordination, persistence, cleanup, and indexing.
- Components: `process_and_config`, `agent_lifecycle`, `github_control_plane`, `runtime_sessions`, `agent_context_routing`, `review_coordinator`
- Concerns: `state_persistence`, `worktree_isolation`, `review_convergence`, `comment_forwarding`, `terminal_cleanup`, `runtime_monitoring`
- Artifacts: `agent_manager`, `tmux_runner`, `agents_state_json`, `handoffs_json`, `review_cycle_checkpoint`, `codex_verdict`
- Read when:
  - You are changing polling, concurrency, worktree management, runtime control, or approval and merge flow.
  - You need lifecycle invariants, role boundaries, persistence, or cleanup semantics.
- Usually skip when:
  - You only need setup commands, contribution rules, or the exact review policy field reference.

### `docs/AGENT_CONTEXT.md`

- Group: `agent_context`
- Kind: `spec`
- Priority: `important`
- Summary: Defines deterministic document and handoff scoring, generated worktree context, handoff capture, persistence, and reload behavior.
- Components: `agent_context_routing`, `agent_lifecycle`
- Concerns: `document_routing`, `handoff_persistence`, `deterministic_scoring`, `context_projection`
- Artifacts: `agent_index_yaml`, `context_md`, `handoff_yaml`, `handoffs_json`, `max_handoffs_in_context`
- Read when:
  - You are changing how agents select repository docs or persist shared execution state.
  - You need scoring rules, handoff schema, or context load and capture lifecycle.
- Usually skip when:
  - You only need operator commands, broad architecture, or review evidence semantics.

### `docs/REVIEW_POLICY.md`

- Group: `review_contracts`
- Kind: `api_spec`
- Priority: `targeted`
- Summary: Operator contract for opt-in exact-SHA convergent review, profiles, lanes, budgets, failure actions, validation, status, and troubleshooting.
- Components: `review_coordinator`, `runtime_sessions`, `github_control_plane`
- Concerns: `review_convergence`, `policy_validation`, `worker_isolation`, `model_selection`, `approval_gates`
- Artifacts: `review_policy`, `agent_profiles`, `role_profiles`, `model_catalog`, `review_swarm`, `review_policy_status`
- Read when:
  - You are configuring or changing convergent review behavior.
  - You need policy defaults, field validation, profile precedence, budgets, or failure semantics.
- Usually skip when:
  - You need the internal artifact envelope rather than operator configuration.

### `docs/REVIEW_ARTIFACTS.md`

- Group: `review_contracts`
- Kind: `api_spec`
- Priority: `targeted`
- Summary: Strict internal worker artifact contract for exact-SHA envelopes, typed evidence, bounded publication, trusted intake, convergence, and atomic publication.
- Components: `review_coordinator`, `runtime_sessions`
- Concerns: `exact_sha_evidence`, `worker_isolation`, `trusted_intake`, `finding_provenance`, `review_convergence`
- Artifacts: `review_artifact_envelope`, `discovery_receipt`, `verifier_assignment`, `challenge_assignment`, `artifact_outbox`, `cross_sha_ledger`
- Read when:
  - You are changing worker evidence, artifact intake, verifier or challenge behavior, or review publication.
  - You need exact-SHA, ownership, path, secret, or atomic-write constraints.
- Usually skip when:
  - You only need configured policy fields or general daemon architecture.

### `TESTING.md`

- Group: `agent_policy`
- Kind: `testing_guide`
- Priority: `important`
- Summary: Verification guide covering required make targets, focused tests, coverage, change-triggered testing, and safe test isolation.
- Components: `process_and_config`, `agent_lifecycle`, `review_coordinator`
- Concerns: `testing_requirements`, `test_isolation`, `coverage`, `integration_testing`
- Artifacts: `make_test`, `make_build`, `go_test`, `coverage_html`, `sample_config`
- Read when:
  - You need the required verification commands or focused test guidance.
  - You are changing behavior and need to determine which tests or isolation rules apply.
- Usually skip when:
  - You need runtime behavior or policy semantics rather than test procedures.

### `DEVELOPMENT.md`

- Group: `agent_policy`
- Kind: `reference`
- Priority: `important`
- Summary: Contributor development guide for prerequisites, runtime direction, local setup, implementation rules, safe logging, style, and commits.
- Components: `process_and_config`, `runtime_sessions`, `agent_lifecycle`
- Concerns: `local_development`, `runtime_adapters`, `secret_safety`, `code_style`, `commit_conventions`
- Artifacts: `go_toolchain`, `codex_cli`, `tmux`, `webex_webhook_url`, `commit_msg_hook`
- Read when:
  - You are setting up a development environment or considering a runtime adapter.
  - You need contributor style, safe logging, or commit guidance.
- Usually skip when:
  - You need the binding repository contract or detailed test commands.

### `CONTRIBUTING.md`

- Group: `agent_policy`
- Kind: `reference`
- Priority: `situational`
- Summary: Short contributor guide for issue reports, pull requests, and other ways to participate.
- Components: none
- Concerns: `contribution_workflow`, `pull_requests`, `issue_reporting`
- Artifacts: `pull_request`, `github_issue`
- Read when:
  - You are preparing an issue, pull request, or other contribution.
- Usually skip when:
  - You are implementing code and need repository-specific technical rules.

### `SECURITY.md`

- Group: `security_community`
- Kind: `security_model`
- Priority: `situational`
- Summary: Security contact and process for reporting vulnerabilities, managing fixes, and suggesting security changes.
- Components: none
- Concerns: `vulnerability_reporting`, `security_maintenance`, `responsible_disclosure`
- Artifacts: `security_advisory`, `vulnerability_report`
- Read when:
  - You found or need to report a security vulnerability.
  - You are changing the repository security-reporting process.
- Usually skip when:
  - You need implementation-level secret handling or runtime isolation rules.

### `CODE_OF_CONDUCT.md`

- Group: `security_community`
- Kind: `reference`
- Priority: `situational`
- Summary: Contributor Covenant standards, scope, enforcement responsibilities, and response guidelines for community participation.
- Components: none
- Concerns: `community_conduct`, `moderation`, `inclusive_participation`
- Artifacts: `code_of_conduct`, `enforcement_guidelines`
- Read when:
  - You need community conduct or moderation expectations.
- Usually skip when:
  - You are working on code, operations, security reports, or review policy.
