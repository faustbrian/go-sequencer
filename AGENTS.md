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
- Historical `.ai/GOAL*.md` files are requirements and evidence, not proof of
  completion. Current executable evidence is REQUIRED.

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
- Documentation and metadata changes require only affected structural checks.
- Internal behavior changes require focused tests, affected package tests, and
  applicable format and static checks.
- Public API, lifecycle, security, persistence, and concurrency changes require
  observable regression or characterization tests, API compatibility where
  applicable, affected integration tests, and direct owned consumer checks.
- Exact statement coverage, mutation, fuzz, race, stress, leak, performance,
  conformance, and external-service checks MUST run only when they exercise a
  material changed risk or a public release boundary.
- Coverage and mutation percentages MUST NOT be universal repository gates.
- Invalid or equivalent mutants require a narrow reviewed record only when
  mutation testing is selected for the affected risk.
- Specification claims MUST be proven against pinned official fixtures and
  independent implementations where applicable.
- Benchmarks selected for a performance claim MUST compare equivalent behavior
  and publish latency, throughput, allocations, environment, corpus, and
  statistical method.

## Required Commands

- `make inventory` validates repository and package manifests.
- `make check` runs the available aggregate repository contract when the
  change's assurance tier or a release boundary requires it.
- `make ci` runs repository, specification, cohesion, and aggregate checks.
- Local and CI invocations of the same selected gate MUST use the same scripts
  and thresholds.
- Missing tools, services, packages, profiles, or reports required by a
  selected gate MUST fail that gate.
- NilAway is advisory; its findings MUST remain visible and tracked against a
  no-regression baseline.

## Evidence Validity And Reuse

- Ordinary Tier A-C verification is attributable through the source revision,
  selected command, and local or CI result. It MUST NOT require bespoke
  fingerprints, persisted checkpoints, or per-module evidence artifacts.
- Expensive evidence MAY be reused when its complete behavior-affecting inputs
  can be identified. Reuse MUST retain the original result and execution
  revision and MUST NOT imply that the gate executed again.
- A machine-verifiable input fingerprint is REQUIRED only for reused expensive
  evidence and applicable Tier D trust boundaries. It MUST cover the inputs
  that can affect that result, including source, tests, fixtures, manifests,
  owned dependencies, gate code and configuration, pinned tools, required
  services, and behavior-affecting environment inputs.
- History-only or unrelated changes MUST NOT invalidate reusable evidence when
  its complete input fingerprint is unchanged. Only affected gates, modules,
  packages, and reverse dependants need to rerun.
- Long-running expensive matrices SHOULD checkpoint independently valid units
  atomically when doing so materially reduces recovery cost. Execution and
  environment identity are REQUIRED only where needed to validate reuse or an
  applicable Tier D boundary.
- Evidence MUST NOT be reused when its behavior-affecting input identity cannot
  be proven; the affected gate must run instead.
- Mutation evidence reuse MUST match the exact behavior-affecting verifier
  identity, including the upstream source checksum, semantic patches, enabled
  operators, coverage contract, and invocation policy. A tool version string
  alone MUST NOT authorize reuse. Executable hashes MUST remain recorded for
  traceability, but platform-specific binary bytes MUST NOT replace the
  portable semantic identity used for content-equivalent reuse.
- If reusable expensive evidence is invalidated solely because `HEAD` changed,
  tooling SHOULD correct the evidence model instead of launching an unchanged
  repository-wide rerun.

## CI And Workflows

- `.github/workflows/ci.yml` is the only owned GitHub Actions workflow.
- Package-local workflows MUST NOT be added.
- Actions and external tools MUST be pinned to immutable versions.
- Every selected module MUST have an attributable result. A persisted evidence
  artifact is REQUIRED only for reused expensive evidence or an applicable
  Tier D trust boundary.
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
  adoption guidance, tradeoffs, security notes, FAQ, and release notes.
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

- Classify each change by its actual assurance tier and run the narrowest gates
  that prove its observable contract and material risks.
- Run release, composition, and clean-consumer checks only at the applicable
  public delivery boundary.
- One complete independent review is the default for a meaningful public
  contract or release batch; additional reviews require a distinct named risk.
- Re-run affected gates after the final source, test, dependency, documentation,
  workflow, or generated-file change.
- Report exact selected commands and results. A skipped, blocked, stale, or
  warning-only selected gate is not a pass.
