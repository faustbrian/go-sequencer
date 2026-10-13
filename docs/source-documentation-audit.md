# Source documentation audit

[Documentation index](README.md)

## Scope and initial contract plan

Tier A source documentation only from `f5f4d9bf4b4d1f3125735af36dbcf0c581be8d2a`.
Owner: this repository documentation worker; coordinator owns publication and CI.
Reproduce tracked coverage with `git ls-files "*.go"`, module boundaries with
`git ls-files "*go.mod"` and assurance entry points with
`rg "^func (Test|Fuzz|Benchmark|Example)" --glob "*_test.go"`.

All 84 Go files, one /v2 module and 17 source directories are in scope, including
26 implementation/helper files, 58 assurance files and deprecated facade packages.
The native checker reports 226 missing comments, chiefly public fields/interfaces.

Trace compiled plan ordering/fingerprints, operation registry and callback effects,
attempt/idempotency/lease/fencing transitions, transactional storage ownership and
crash/unknown outcomes, fleet start/drain/cancel, resource/text admission and HTTP
administration/queue/provider seams before writing guarantees. Direct consumers
include memory/PostgreSQL stores, four adapters and four retained facade variants,
scheduler, migrations bridge, HTTP handler, test helpers and existing examples.

Selected checks are native formatting and exact AST/token/literal/directive identity,
17 package renders, relevant native docs/example recipe, source/localdocs/offline
specification structure, links, pinned secret scans and ordinary complete diff review.
API mode is apidiff (api/v2.txt); no full-prose API snapshot is configured. No runtime,
dependency, fixture, pinned authority, decision/history or workflow changes planned.
No unchanged database/Kubernetes, race/leak, fuzz, benchmark, mutation or release
campaign is part of this source-only pass. Go runs wait for a serialized grant.

## Complete tracked coverage

| Go file | Initial gaps | Review boundary |
| --- | --- | --- |
| `adapters/idempotency/adapter.go` | 3 | package/API and private invariant source |
| `adapters/idempotency/adapter_test.go` | 0 | TestAdapterBoundsDetachedCleanupAfterCallerCancellation; TestNewWithCleanupTimeoutRejectsUnboundedDurations; further cases/helpers reviewed |
| `adapters/idempotency/example_test.go` | 0 | ExampleNew |
| `adapters/lease/adapter.go` | 6 | package/API and private invariant source |
| `adapters/lease/adapter_test.go` | 0 | TestAdapterBoundsDetachedReleaseAfterCallerCancellation; TestNewWithCleanupTimeoutRejectsUnboundedDurations; further cases/helpers reviewed |
| `adapters/lease/example_test.go` | 0 | ExampleNew |
| `adapters/queue/adapter.go` | 13 | package/API and private invariant source |
| `adapters/queue/adapter_test.go` | 0 | TestDispatcherPublishesIdentityOnlyMessage; TestChannelBoundDispatcherAndWorkerRejectCrossChannelMessages; further cases/helpers reviewed |
| `adapters/queue/checksum_security_test.go` | 0 | TestQueueChecksumCanonicalGrammar |
| `adapters/queue/example_test.go` | 0 | ExampleNewDispatcher |
| `adapters/queue/fuzz_test.go` | 0 | FuzzQueueMessageJSONValidation |
| `adapters/retry/adapter.go` | 1 | package/API and private invariant source |
| `adapters/retry/adapter_test.go` | 0 | TestClassifierMapsSequencerRetryability; TestAdapterUsesExternalBoundedPolicy; further cases/helpers reviewed |
| `adapters/retry/example_test.go` | 0 | ExampleNew |
| `approval_security_test.go` | 0 | TestOversizedApprovalKeepsSafeBlockedAudit; TestApprovalRejectsControlOnlyAttributionBeforeHandler; further cases/helpers reviewed |
| `audit_security_test.go` | 0 | TestDirectMemoryAuditTextIsRedactedAndAttributable; TestRunnerObserverClassifiesContextCancellation |
| `benchmark_test.go` | 0 | BenchmarkCompilePlan; BenchmarkMemoryBoundedHistory; further cases/helpers reviewed |
| `budget.go` | 0 | package/API and private invariant source |
| `budget_test.go` | 0 | TestExecutionBudgetIsFiniteNilSafeAndConcurrent; TestRetryAndConcurrentHedgesShareOneAmplificationBudget |
| `crash_recovery_test.go` | 0 | TestRunnerCrashAfterLocalTransactionCommitLeavesUnknownDurableOutcome |
| `example_test.go` | 0 | ExampleRunner |
| `fleet.go` | 4 | package/API and private invariant source |
| `fleet_internal_test.go` | 0 | TestFleetRetainsRenewalFailureArrivingWhileStoreErrorIsReported; TestAcceptedClaimRenewsBeforeHandlerAdmission; further cases/helpers reviewed |
| `fleet_model_test.go` | 0 | TestFleetReplicasClaimLeaderlesslyWithoutDuplicateCompletion |
| `fleet_recovery_model_test.go` | 0 | TestFleetRecoveryStateModelExploresCrashAndTakeoverTraces |
| `fleet_resilience_race_test.go` | 0 | TestLeaseRenewalCompletionRecoveryAndTakeoverRaceKeepsOneOwner; TestResetCompletionRaceNeverLeavesOldOwnershipWritable; further cases/helpers reviewed |
| `fleet_test.go` | 0 | TestFleetStopsAcceptingBeforeCancelingOwnedAttempts; TestFleetRenewsAcceptedAttemptsWithoutRenewingCompletedLease; further cases/helpers reviewed |
| `fuzz_test.go` | 0 | FuzzCompilePlanDeterminism; FuzzOperationDefinitionBoundaries; further cases/helpers reviewed |
| `goidempotency/adapter.go` | 3 | package/API and private invariant source |
| `goidempotency/facade_test.go` | 0 | TestFacadeDelegatesToCanonicalAdapter |
| `golease/adapter.go` | 6 | package/API and private invariant source |
| `golease/facade_test.go` | 0 | TestFacadeDelegatesToCanonicalAdapter |
| `goqueue/adapter.go` | 13 | package/API and private invariant source |
| `goqueue/facade_test.go` | 0 | TestFacadeDelegatesToCanonicalAdapter |
| `goretry/adapter.go` | 1 | package/API and private invariant source |
| `goretry/facade_test.go` | 0 | TestFacadeDelegatesToCanonicalAdapter |
| `internal/audittext/audittext.go` | 0 | package/API and private invariant source |
| `internal/audittext/audittext_test.go` | 0 | TestPrepareBoundsAndCleansAttribution |
| `internal/owneridentity/owneridentity.go` | 0 | package/API and private invariant source |
| `internal/owneridentity/owneridentity_test.go` | 0 | TestIdentityIsPreservedOrRejected |
| `kubernetes_lifecycle_helper_test.go` | 0 | TestKubernetesLifecycleHelper |
| `leak_test.go` | 0 | TestMain |
| `lease_keeper.go` | 0 | package/API and private invariant source |
| `lease_latency_security_test.go` | 0 | TestRunnerRetainsStoreAndLeaseFailureDuringMarkRunning; TestRunnerRetainsStoreAndLeaseFailureDuringBudgetSettlement; further cases/helpers reviewed |
| `lifecycle_admission_security_test.go` | 0 | TestConcurrentTransactionCallbackAdmissionIsExactlyOnce; TestUnsafeClaimOwnersHaveNoEffects; further cases/helpers reviewed |
| `memory/crash_failover_test.go` | 0 | TestStoreReplicaRaceAuthorizesExactlyOneClaim; TestStoreCrashFailoverFencesEveryStaleOwnerTransition |
| `memory/edge_test.go` | 0 | TestStoreValidationInspectionAndResetEdges; TestStoreDeferredRetryResetAndDefensiveCopies; further cases/helpers reviewed |
| `memory/lease_queue.go` | 0 | package/API and private invariant source |
| `memory/lease_queue_test.go` | 0 | TestLeaseQueueUsesStrictDeterministicExpiryAndIdentityOrder |
| `memory/store.go` | 0 | package/API and private invariant source |
| `memory/store_internal_test.go` | 0 | TestStoreInternalOrderingAndMissingLatest; TestStoreRejectsOwnershipAndRetryCounterOverflow |
| `memory/store_test.go` | 0 | TestStoreClaimsExactlyOnceAndEnforcesOwnership; TestStoreConcurrentClaimHasSingleWinner; further cases/helpers reviewed |
| `migrations/bridge.go` | 2 | package/API and private invariant source |
| `migrations/bridge_test.go` | 0 | TestBridgeAssertsPrerequisiteWithoutOwningHistory; TestBridgeValidationAndReaderFailure |
| `operation.go` | 38 | package/API and private invariant source |
| `operation_test.go` | 0 | TestNewOperationValidatesAndFreezesMetadata; TestNewOperationRequiresCryptographicSHA256Checksum; further cases/helpers reviewed |
| `plan.go` | 2 | package/API and private invariant source |
| `plan_test.go` | 0 | TestCompilePlanUsesDeterministicTopologicalOrder; TestCompilePlanOrderIsInvariantAcrossEquivalentInputPermutations; further cases/helpers reviewed |
| `postgres/audit_security_integration_test.go` | 0 | integration-only attribution/identity helper called by PostgreSQL suite |
| `postgres/audit_security_internal_test.go` | 0 | TestPostgresClaimAndStartPreserveOwnerParameters; TestDirectPostgresAuditParametersAreRedactedAndAttributable; further cases/helpers reviewed |
| `postgres/benchmark_integration_test.go` | 0 | BenchmarkPostgresStore |
| `postgres/fuzz_test.go` | 0 | FuzzPersistedAttemptStateErrorAndOutput; FuzzPersistedDependencyRefs; further cases/helpers reviewed |
| `postgres/integration_test.go` | 0 | TestPostgresStoreFailsClosedAndRecoversAfterServerRestart; TestPostgresStorePromotesSynchronousStandbyAndReconnects; further cases/helpers reviewed |
| `postgres/migrations.go` | 0 | package/API and private invariant source |
| `postgres/migrations_test.go` | 0 | TestMigrationsExposeVersionedDurableLedger; TestMigrationsExposePinnedDependencyExpansion; further cases/helpers reviewed |
| `postgres/store.go` | 0 | package/API and private invariant source |
| `postgres/store_internal_test.go` | 0 | TestIntegerConversionsRejectInvalidLedgerValues; TestStoreImmediateDatabaseFailures; further cases/helpers reviewed |
| `property_test.go` | 0 | TestPlanPropertyDependenciesAlwaysPrecedeDependents |
| `runner.go` | 34 | package/API and private invariant source |
| `runner_internal_test.go` | 0 | TestDurableStateErrorPreservesTerminalClassification; TestRunnerChannelAndDurableRecordPredicatesAreExact; further cases/helpers reviewed |
| `runner_test.go` | 0 | TestRunnerExecutesPlanInOrderAndReportsDurableResults; TestRunnerTreatsPersistedOneTimeSkipAsComplete; further cases/helpers reviewed |
| `scheduler/adapter.go` | 4 | package/API and private invariant source |
| `scheduler/adapter_test.go` | 0 | TestAdapterSchedulesExplicitEligibility; TestAdapterRejectsInvalidDependenciesAndRequests |
| `sequencehttp/audit_security_test.go` | 0 | TestAdministrativeHTTPAuditUsesDefensiveStoreSanitization |
| `sequencehttp/fuzz_test.go` | 0 | FuzzAdministrativeCommands |
| `sequencehttp/handler.go` | 9 | package/API and private invariant source |
| `sequencehttp/handler_test.go` | 0 | TestHandlerRequiresApplicationAuthorization; TestHandlerBindsResetActorToAuthorizedPrincipal; further cases/helpers reviewed |
| `sequencehttp/inspection_security_test.go` | 0 | TestInspectionPreservesExactPreencodedBoundary |
| `sequencertest/helpers.go` | 10 | package/API and private invariant source |
| `sequencertest/helpers_test.go` | 0 | TestClockAndOperationAreDeterministic; TestFaultStoreInjectsCrashBoundary; further cases/helpers reviewed |
| `state.go` | 0 | package/API and private invariant source |
| `state_test.go` | 0 | TestStateTransitionsAreExplicit; TestEveryStateHasStableText |
| `store.go` | 77 | package/API and private invariant source |
| `store_test.go` | 0 | TestClassifiedErrorsPreserveCause; TestReconcileResolutionTextIsStable; further cases/helpers reviewed |

## Investigated constraints and remaining questions

- Identity is caller-declared, canonical 71-byte SHA-256 text. Validation checks
  grammar and drift, not handler source, authentication or review provenance.
  Operation versions are supplied, not allocated; lifetime claim/fencing counters
  increment at store admission. Memory fails before uint/uint64 wrap; PostgreSQL
  is limited by signed bigint and provider errors. Exact candidates are necessary
  for rolling registries; zero/empty fields retain broader legacy selection.
- Metadata slices and compensation references are copied; callback interfaces,
  stores, clocks, pools and providers remain borrowed. There is no callback cloning
  or typed-nil detection. A plan permits concurrent metadata reads, not unsafe
  collaborator mutation. Constructors start no workers. Memory requires New,
  serializes with a mutex and checks contexts before the lock, not during waits.
- Plans use sorted-ID depth-first dependency traversal. Dependencies must match
  exact version/digest and reach Succeeded or Skipped; AllowedFailure does not
  satisfy them. This is not globally smallest-ready-node ordering. Operation
  timeouts share approval/condition/transaction/handler admission and callbacks;
  durable retries are immediate eligibility, not an external backoff scheduler.
- An unresponsive callback is indeterminate and retains its slot until return.
  Fleet closes admission first; no forced Go preemption exists. Observer workers
  are asynchronous, bounded to 16 pending events, drop overflow and have no flush
  or join API. Borrowed providers must honor context deadlines: Run joins renewal
  workers and cannot hard-bound shutdown when a store ignores cancellation.
  Fleet Run is one-shot; accepted contexts do not inherit Run context values.
- Lease fencing protects ledger writes, not arbitrary external effects. Application
  transactions are separate from claim/completion; crash after effect commit is
  unknown. Recovery batches at most 32 leases, records Indeterminate and permits
  automatic eligibility only under declared application idempotency. Reset and
  exact reconciliation differ; replay epochs do not erase lifetime attempts.
  Compensation generation guards and append-only history remain unchanged.
- Memory uses caller instants and independent copied inspection metadata;
  PostgreSQL claims/renewal/completion/reset use server time and whole-millisecond
  leases (positive submillisecond durations are rejected). PostgreSQL inspection
  represents absent times as Unix epoch and empty history for missing identities;
  memory uses Go zero time and ErrNotFound. Recovery/reconciliation keep their
  existing provider-specific time rules; contexts are not a preemption mechanism.
- Idempotency terminal updates run only after a returned callback result: a panic
  skips Complete/Fail even if an outer runner recovers it. Lease release is
  deferred even during panic but no renewal or TTL-forced callback stop exists.
  Both cleanup deadlines need provider cooperation. Queue messages are identity
  data, not authenticated commands; fresh Dispatch calls create new DeliveryID.
  Safe settlement/redelivery needs the application's durable Executor semantics.
  Retry adapter limits reservations, not policy duration or scheduling.
- Persistence redacts known patterns and sensitive key names with explicit byte
  bounds, not arbitrary secrets. Output encoded-size validation follows bounded
  field preparation. Returned wrapped errors and Report causes can still contain
  sensitive text, unlike persisted runner classifications and observer events.
- HTTP excludes slash-containing operation IDs despite the root grammar, bounds
  reset/reconcile bodies to 8 KiB and inspection to 1 MiB, runs controllers
  synchronously, trusts their pre-encoded bytes and uses application authorization.
  Authentication, CSRF, transport/body deadlines and controller durability remain
  application-owned. No exception/panic boundary is installed around controllers.
- Embedded migrations are forward-only and application-run; maintained reversible
  wording was corrected. Seven SQL migration inputs, four frozen goal requirement
  pins and api/v2.txt remain untouched. No external protocol/specification authority
  or generated Go exists in this module. Internal heap/index, sanitizer/identity,
  transaction callback admission and lease keeper invariants were traced in source.

Unknown application-specific rationale remains outside the implementation:
which source bytes deployments review/hash, how providers enforce external effect
idempotency/fencing and durable admission, and which application audit retention
or process termination policy is required. Neither source nor tests establish
those deployment guarantees. They are caller obligations, not invented promises
or runtime repairs in this pass. A context-ignoring custom store and adapter panic
terminal-update gap are explicitly retained limitations, not fixed here.

## Variants, assurance and native commands

All 58 assurance files were reviewed for entry points, assertions, helper ownership,
fixtures and retained failure boundaries. Nine fuzz targets cover plans, operation
bounds, sanitizer, mixed claims, queue commands, persisted records/references and
HTTP administration. Root benchmarks cover graph/history/claim/recovery/fleet,
queue settlement and retry/idempotency; PostgreSQL benchmarks need integration.
Five existing examples cover Runner and four canonical adapters. Existing TestMain
uses goleak in the native example recipe; no separate leak campaign is selected.
The three PostgreSQL integration files and Kubernetes helper preserve build tags;
no production platform variant or generator directive is present. Four deprecated
facades retain replacement paths and the 180-day/two-stable-minor retention rule;
they keep distinct named types and bridge values rather than aliases.

Native API mode is apidiff, not full go-doc prose: api/v2.txt is preserved.
The configured docs operation is `make -f verification/package.mk docs`, whose
script checks local links/required files and runs
`GOWORK=off go test ./... -run '^Example' -count=1`. Go 1.27.2 formatting,
all-84 AST/token/literal/directive identity and 17 native package renders are the
bounded source proof; no unchanged integration/race/fuzz/benchmark/mutation or
release campaigns run. Structural source checking does not prove semantics.
Final results are reported by the worker/coordinator, not frozen into API history.

## Objective Enforcement

The checksum-pinned public Tools v2.4.0 verifier now supplies
`make source-docs-check` as a prerequisite of the existing local and CI Make
routes. Ordinary shared CI selects strict source-documentation checking and
requires its result. Native prerequisites, operation budgets, security and
release controls are retained. Hosted exact-head evidence is recorded by the
coordination ledger; configuration alone does not establish green delivery.

Continue with [API](api.md).
