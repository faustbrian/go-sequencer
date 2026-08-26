# Engineering Policy

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Scope And Authority

- This file is the canonical policy for the complete repository.
- Package policies MAY add stricter domain rules but MUST NOT weaken this file.
- `CLAUDE.md` and tool-specific files MUST point here rather than duplicate it.
- Historical implementation plans belong in repository history or issue
  tracking, not in the released source tree. Current checks MUST pass.

## Repository Structure

- The public root module MUST live at the repository root.
- Intentional optional or test modules MAY live in explicit nested directories.
- Commands MUST live under `cmd/`; private shared code MUST live under
  `internal/`; root automation MUST live under `scripts/`.
- Public module paths MUST match their repository-relative directories beneath
  the module path declared by the root `go.mod`.
- Every module MUST be declared in `modules.json`, and every package MUST be
  declared in `packages.json`.
- Independently releasable modules MUST retain independent `go.mod` files and
  directory-prefixed semantic-version tags.
- Cross-module dependencies MUST remain acyclic and MUST use public contracts.
- Permanent `replace` directives, sibling repositories, and absolute developer
  paths are forbidden in releasable modules.

## Design

- Prefer standard-library interfaces and explicit composition over hidden
  registration, global state, reflection-driven wiring, or service locators.
- Public APIs MUST make ownership, cancellation, retries, timeouts, resource
  limits, error semantics, and concurrency behavior observable.
- Interfaces SHOULD be defined by consumers and MUST remain narrowly scoped.
- Optional integrations SHOULD be adapters or nested modules, not mandatory
  dependencies of a core package.
- Breaking protocol or specification ambiguities MUST be documented as explicit
  decisions and covered by tests.

## Safety And Concurrency

- Shared mutable state MUST have one documented synchronization owner.
- Goroutines MUST have explicit lifetime, cancellation, shutdown, and leak
  tests. Fire-and-forget goroutines are forbidden.
- Channels MUST have documented ownership and closure rules.
- Locks MUST NOT be held across caller callbacks, network IO, blocking channel
  operations, or unbounded work.
- Every external operation MUST accept or derive a bounded `context.Context`.
- Response bodies, files, rows, transactions, timers, tickers, connections,
  and temporary resources MUST be closed on every path.
- Integer conversions, sizes, offsets, recursion, decompression, and allocation
  from untrusted input MUST be bounded before allocation or conversion.
- Secrets and credentials MUST NOT appear in errors, logs, traces, snapshots,
  fixtures, mutation reports, or generated artifacts.

## Testing

- Behavioral changes MUST include meaningful tests before completion.
- Tests MUST assert outcomes, invariants, errors, cleanup, and state transitions;
  line execution without behavioral assertions is not acceptable coverage.
- Every production package MUST have exact 100% statement coverage without
  rounding or aggregate masking.
- Every viable mutant MUST be killed. Mutation efficacy and mutant coverage
  MUST both be exactly 100%.
- Invalid or equivalent mutants require a narrow reviewed record containing a
  stable identifier, rationale, evidence, reviewer, date, and expiry.
- Parsers and hostile boundaries MUST have fuzz tests, corpus seeds, resource
  limits, and deterministic regression cases for every discovered failure.
- Concurrent code MUST pass `go test -race` and targeted stress/leak tests.
- Specification claims MUST be proven against pinned official fixtures and
  independent implementations where applicable.
- Benchmarks MUST compare equivalent behavior and publish latency, throughput,
  allocations, environment, corpus, and statistical method.

## Required Commands

- `make inventory` validates repository and package manifests.
- `make check` runs the exact contract for every repository module.
- `make ci` runs the complete repository contract.
- Local commands and CI MUST use the same scripts and thresholds.
- Missing tools, services, packages, profiles, mutants, or reports MUST fail.
- NilAway is advisory; its findings MUST remain visible and tracked against a
  no-regression baseline.

## Evidence Validity And Reuse

- Evidence validity MUST be determined by the complete set of inputs that can
  affect the gate result, not by a commit hash, branch name, timestamp, or
  repository-history shape alone.
- A gate fingerprint MUST include all applicable production code, tests,
  fixtures, generated files, module manifests and checksums, owned
  dependencies, shared gate scripts, gate configuration, pinned tool versions,
  required service images and configuration, and behavior-affecting
  environment inputs.
- Commit hashes MAY be recorded for traceability, but MUST NOT be the sole
  evidence cache key or invalidation condition.
- Go toolchain revision metadata MAY be retained when a build, test, profile,
  or diagnostic artifact requires it. That metadata is descriptive only and
  MUST NOT make an otherwise identical gate-input fingerprint stale.
- A history rewrite, rebase, squash, reset, repository reinitialization,
  metadata-only commit, or unrelated-file change MUST NOT invalidate evidence
  when the complete gate-input fingerprint is unchanged.
- Agents MUST NOT rerun an expensive gate solely to attach an already proven
  result to a new `HEAD`.
- Agents MUST NOT restart the complete package matrix after a force-push,
  rebase, squash, reset, or other history-only change. Previously verified
  package checkpoints MUST be reused, and only packages with changed complete
  gate-input fingerprints MAY be rerun.
- After a change, agents MUST rerun only the gates, modules, packages, and
  reverse dependants whose complete input fingerprints changed.
- Reused evidence MUST retain the original execution revision and result,
  record the revision at which it was revalidated, and include a
  machine-verifiable input fingerprint. Reuse MUST NOT rewrite history to
  pretend the gate executed again.
- Gate evidence MUST be written atomically as soon as the result is available
  and before another package, module, or gate begins. Agents and tooling MUST
  NOT defer completed evidence until the end of a long batch or lane.
- Long-running multi-package or multi-module gates MUST checkpoint each
  independently valid result. An interruption MUST preserve completed
  checkpoints and discard only the incomplete unit.
- Execution revision, input fingerprint, tool versions, and environment
  identity MUST be captured before the gate starts. Tooling MUST NOT attach
  whichever `HEAD` happens to exist when a later aggregate report is written.
- Aggregate reports MUST be derived incrementally from persisted checkpoints.
  They MUST NOT be the only durable record of results that were already
  received.
- Evidence MUST NOT be reused when input identity cannot be proven. Missing,
  incomplete, manually asserted, or ambiguous fingerprints make the evidence
  stale and require execution.
- Mutation evidence reuse MUST match the exact behavior-affecting verifier
  identity, including the upstream source checksum, semantic patches, enabled
  operators, coverage contract, and invocation policy. A tool version string
  alone MUST NOT authorize reuse. Executable hashes MUST remain recorded for
  traceability, but platform-specific binary bytes MUST NOT replace the
  portable semantic identity used for content-equivalent reuse.
- If repository tooling invalidates evidence solely because `HEAD` changed,
  agents MUST correct the evidence model instead of launching a repository-wide
  rerun with no changed gate inputs.
- A one-time history-reset migration MUST be pinned to the exact original
  module, package, execution revision, gate-input digest, tool version, and
  canonical report hash. It MUST also pin the exact replacement gate-input
  fingerprint for that module and package. It MUST preserve the original
  execution revision and MUST NOT depend on unrelated repository paths,
  worktree state, or repository-history availability.

## CI And Workflows

- `.github/workflows/ci.yml` is the only owned GitHub Actions workflow.
- Package-local workflows MUST NOT be added.
- Actions and external tools MUST be pinned to immutable versions.
- Every selected module MUST have an attributable result and evidence artifact.
- The stable required job MUST fail for failed, cancelled, skipped, or missing
  module results.
- Required checks MUST NOT use `continue-on-error`, `|| true`, permissive
  thresholds, or warning substitutions.

## Dependencies And Supply Chain

- Dependencies MUST be necessary, maintained, license-compatible, and pinned to
  reviewed current versions.
- Standard-library functionality MUST NOT be wrapped merely to create an owned
  abstraction; wrappers require a stable policy or portability boundary.
- Generated code and vendored corpora MUST record source, version, checksum,
  license, generation command, and update procedure.
- Vulnerability, secret, license, SBOM, provenance, and clean-consumer checks
  are release gates.

## Documentation

- Public identifiers MUST have useful Go documentation describing semantics,
  invariants, ownership, errors, concurrency, and caveats where relevant.
- Comments MUST explain why a constraint or non-obvious implementation exists;
  they MUST NOT narrate obvious syntax.
- Every public module MUST provide a quick start, API reference, examples,
  guidance on when to use it, explicit limitations, security notes, FAQ, and
  release notes.
- The root README MUST remain a concise entry point. Detailed guides,
  operations, audits, and maintainer material belong under `docs/` and MUST be
  linked through `docs/README.md`.
- Documentation and examples MUST compile and be checked in CI.

## Changelogs

- Every user-visible change MUST update the affected module `CHANGELOG.md` in
  the same commit.
- Entries MUST describe behavior and migration impact, not internal activity.
- Changes to multiple modules MUST update every affected changelog.
- Unreleased entries MUST NOT be silently rewritten or removed.
- Generated, dependency, security, compatibility, and deprecation changes are
  user-visible and require entries.

## Completion

- Run the narrowest affected gates during development and all affected release
  gates before declaring completion.
- Re-run affected gates after the final source, test, dependency, documentation,
  workflow, or generated-file change.
- Report exact commands and results. A skipped, blocked, stale, or warning-only
  gate is not a pass.
