# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected package's docs.
2. Run the affected package's deterministic Go tests as described below.
3. Identify owned dependencies and reverse dependants in `modules.json`.
4. Preserve unrelated work and generated/corpus provenance.

## Changes

Keep commits focused and conventional. Update every affected changelog with
the behavior and migration impact. Public API changes require compatibility
evidence and documentation. Specification behavior requires a decision record,
fixture coverage, and interoperability evidence.

New direct dependencies and dependency updates must follow the
[dependency governance policy](AGENTS.md#dependencies-and-supply-chain). Package-local
update bots are forbidden; the root policy owns every module and action update.

Specification-backed changes must follow the
[specification governance contract](AGENTS.md#design), update
the affected stable decision entries, and complete the Specification Decisions
section of the pull request template. An unresolved interpretation or stale
source pin is release-blocking; peer behavior cannot silently select policy.

Selected full/release mutation gates must finish with zero surviving viable
mutants; mutation is not required for an ordinary local or pull-request check.

Do not add package-local workflows, permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Use the checksum-pinned public Tools v2.4.0 release declared in `.golib.yaml`.
Download its released `golib` binary rather than building the verifier from source.

Use Go 1.27.2 for the producer; the public module minimum remains Go 1.27.0.
An isolated checkout needs no sibling repositories or coordination goal files.
Start with deterministic tests and one explicit active fuzz target when its
input boundary changes:

```bash
GOWORK=off go test ./...
GOWORK=off go test . -run '^$' -fuzz '^FuzzCompilePlanDeterminism$' -fuzztime=2s -timeout=2m
```

The first command includes fuzz seeds/corpus, not active fuzzing. Untagged Go
tests do not qualify PostgreSQL: its tests require the `integration` tag and
start the digest-pinned reference containers. The Kubernetes lifecycle suite
is a separate explicitly selected platform boundary, not an ordinary campaign.

Install the verified public `golib` release declared in `.golib.yaml` for
repository checks; CI verifies the same release without building it from source:

```bash
make check
make ci
make specification-check
```

Bounded local checks retain the catalog's PostgreSQL service requirement and
`integration` test tag; they are not database- or Docker-free.
`make ci` also checks repository/cohesion structure and the offline decision
register. The selected root race runs once with a two-minute timeout and checks
concurrent transaction callback admission, fail-closed unknown outcomes, and
draining before attempt cancellation. The existing root `TestMain` leak
assertion still applies. Base-free checks run deterministic fuzz seeds/corpus;
changed-source CI selects active fuzz smoke.

Optional one-package local mutation:

```bash
golib mutation --module . --package memory
```

Select the original full campaign and release boundary explicitly:

```bash
make check-full
make specification-check-online
golib api check
golib release dry-run --module .
```

Full checks retain all nine native fuzz targets at 10000x/20m each,
documentation examples, benchmarks and original module gates. The full Make
route retains repository/cohesion and online specification verification.
Release rehearsal can run locally; it is not restricted to CI. Report
unavailable or failing commands and never describe bounded checks alone as
release qualification.

## Adding A Module

Follow [repository structure policy](AGENTS.md#repository-structure). New modules
require an explicit purpose, ownership boundary, dependency review, package
catalog entry, full quality gates, documentation, changelog, license, security
policy, compatibility plan, and release dry-run.

## Source Documentation

Install the checksum-pinned public verifier declared in `.golib.yaml`.
`make source-docs-check` validates objective Go comment, marker, link and
generated-producer structure; `make check`, `make check-full` and `make ci`
require it before their existing checks. Shared CI runs the same strict
contract and includes its result in Required. This is structural enforcement,
not a substitute for source-truth review or behavioral verification.
