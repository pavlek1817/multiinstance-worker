# MultiInstanceWorker

Provider-agnostic building blocks for running background workers across multiple
instances of an application: leader election for singleton workloads, instance
discovery, and even splitting of a set of named workloads across live instances.

The core library defines the coordination *logic* and the storage *contracts* it needs,
and stays free of any backing-store dependency. The store itself comes from either a
provider package (`MultiInstanceWorker.Redis` today) or your own implementation of three
small interfaces.

## Packages

| Package | What it is | Depends on |
| --- | --- | --- |
| `MultiInstanceWorker` | The engine: runners, coordinator, assigners, DI extensions. | `MultiInstanceWorker.Abstractions` |
| `MultiInstanceWorker.Abstractions` | The contracts only (`ILeaseManager`, `IInstanceRegistry`, `IWorkloadStatusStore`, ...). | nothing |
| `MultiInstanceWorker.Redis` | Redis implementations of the three storage contracts, behind `AddMultiInstanceWorkerRedis`. | `MultiInstanceWorker.Abstractions`, StackExchange.Redis |

The engine and the Redis provider don't reference each other - they only share the contracts.

## Pieces

- `IInstanceIdentityProvider` / `ProcessInstanceIdentityProvider` — a unique id for
  the current process (`machine:pid:guid`), used to prove lease/heartbeat ownership.
- `IInstanceRegistry` — tracks which instances are alive, and whether they're
  draining. A consumer implements this against its own store (heartbeat write with
  TTL, list active *non-draining* instances - each an `ActiveInstance` with its
  `InstanceId` and a write-once `JoinedAtUtc` - mark draining, remove on shutdown).
- `ILeaseManager` — grants ownership of a single named workload to exactly one
  instance at a time. A consumer implements this against its own store, and must
  guarantee mutual exclusion on renewal (e.g. a short arbitration lock around a
  read-then-write of the lease record).
- `LeasedWorkerRunner` / `LeasedWorkerHostedService` — drives a workload's lifecycle:
  acquire-or-renew the lease every `renewInterval`, start the workload when owned,
  stop it when the lease is lost, and release the lease when told to stop. When its
  `stoppingToken` is cancelled (a coordinator reassigned this workload elsewhere, or
  the whole host is shutting down — the runner treats both identically) or
  `IInstanceRegistry.IsDraining` is already true (an operator drained the instance
  externally), it stops taking new work but lets an in-flight worker finish on its
  own — up to a `drainTimeout` ceiling — instead of cancelling it immediately, so
  in-flight work hands over cleanly rather than getting dropped. It never calls
  `IInstanceRegistry.BeginDrainAsync` itself: marking the whole instance draining in
  the backing store is a deliberate, external act (e.g. a `/drain` endpoint or a
  preStop hook), never inferred from a token cancelling — see
  [Draining an instance](#draining-an-instance) below. `LeasedWorkerRunner` is the
  internal state machine; `LeasedWorkerHostedService` (the public entry point) adapts
  it to the hosted-service lifecycle.
- `IWorkloadAssigner` — decides which of a set of named workloads belong to *this*
  instance, given the active instances, so a coordinator knows what to attempt
  locally. It's a liveness/efficiency decision, not the safety one — `ILeaseManager`
  still enforces exclusive ownership underneath whatever it picks. Two implementations
  ship today:
  - `BalancedNamedWorkloadAssigner` — deterministically slices the workloads evenly
    across the active instance ids (sorted, even split, remainder to the earliest
    instances).
  - `PrimaryNodeWorkloadAssigner` — active/passive instead of spread out: every
    workload goes to whichever active instance joined earliest (`ActiveInstance.JoinedAtUtc`,
    not id sort order - a later-joining instance can never sort ahead of an
    already-running incumbent just because its id happens to sort earlier).
    Failover falls out for free: once the primary's heartbeat expires and it drops
    out of the active set, whichever survivor joined earliest picks up everything
    on the next reconcile tick.
- `WorkloadCoordinatorHostedService<TWorkload>` — the sharded-workload counterpart
  to `LeasedWorkerHostedService`: one hosted service that heartbeats, re-evaluates
  its `IWorkloadAssigner`'s assignment every tick, and reconciles one
  `LeasedWorkerRunner` per assigned workload — starting runners for newly assigned
  workloads and stopping ones that dropped out of the assignment. A workload that
  drops out of the assignment — whether because this instance is draining or because
  a plain rebalance moved it elsewhere — gets the same `drainTimeout`-bounded "let it
  finish" treatment either way; neither ever marks the *instance* itself as draining
  in the backing store — that stays an explicit, external act regardless of cause.
  The coordinator's own tick loop never blocks waiting on this: a still-finishing
  runner is left alone and cleaned up on a later tick, so this instance's own
  heartbeat keeps going the whole time.
  Workload construction and execution stay entirely in the `executeAsync` delegate
  you supply — this class knows nothing about what a workload actually does.
- `IWorkloadStatusStore` — tracks each workload's lifecycle state (`WorkloadStatus`:
  `Active` / `Transferring` / `Inactive`) across the fleet. `LeasedWorkerRunner` writes
  `Active` (refreshed every `renewInterval` — the workload's own heartbeat) while it
  owns the lease and runs, `Transferring` for the whole drain wind-down (TTL-bounded by
  `drainTimeout`, so a crashed instance can never block a handover longer than it could
  ever legitimately keep finishing work), and `Inactive` once it releases. A coordinator
  reads it before starting a newly assigned workload and defers starting one still
  `Transferring` off another instance — avoiding a runner that would just poll for a
  lease it can't get yet. This is a liveness/efficiency layer on top of `ILeaseManager`,
  not a substitute for it: the lease is still what actually prevents a double-run if a
  status record is stale. Each record also carries a write-once `CreatedAtUtc`, and
  `GetAllAsync()` reads every currently live workload fleet-wide without the caller
  needing to already know the keys — this is what the sample's `/diagnostics` endpoint
  is built on, so a workload doesn't need to record anything of its own to show up there.
- `IDrainableService` — an optional `RequestDrain()` contract a workload can implement
  to receive its own cooperative stop signal, instead of depending on `IInstanceRegistry`
  just to poll `IsDraining`. Pass the workload as `drainable` to `LeasedWorkerHostedService`
  (or resolve it per-workload via `WorkloadCoordinatorHostedService<TWorkload>`'s
  `drainableSelector`) and `RequestDrain()` is called the moment that runner enters drain
  mode — well before `drainTimeout` would force a cancellation. It's still optional: a
  workload can instead observe `IInstanceRegistry.IsDraining` itself, or just rely on the
  cancellation token plus `drainTimeout`.
- `LeaderElectionConfig` — timing knobs (lease TTL / renew interval, heartbeat TTL /
  interval, drain timeout) with validation that renewal intervals stay safely inside
  their TTLs.
- `AddLeasedWorker` / `AddWorkloadCoordinator<TWorkload>` — `IServiceCollection`
  extensions that register the two hosted services above without hand-writing their
  constructor wiring (see Usage sketch below). Both default
  `IInstanceIdentityProvider` to `ProcessInstanceIdentityProvider` (and
  `AddWorkloadCoordinator` defaults `IWorkloadAssigner` to `BalancedNamedWorkloadAssigner`)
  via `TryAdd`, so a consumer's own registration always wins if they need something else.
  `ILeaseManager`, `IInstanceRegistry`, and `IWorkloadStatusStore` are never defaulted —
  register those with a provider package (`AddMultiInstanceWorkerRedis`) or against your own store.

## Draining an instance

Marking a whole instance draining (`IInstanceRegistry.BeginDrainAsync`) is always a deliberate,
external act — nothing in this library calls it on your behalf, no matter why a workload's runner
stops. A consumer wires it up as its own endpoint or lifecycle hook, e.g.:

```csharp
app.MapPost("/drain", async (IInstanceRegistry instanceRegistry, CancellationToken ct) =>
{
    await instanceRegistry.BeginDrainAsync(ct);
    return Results.Accepted();
});
```

...called from wherever your orchestrator can reach before it tears the instance down — a
Kubernetes `preStop` hook hitting that endpoint is the common case. Once called, `GetActiveInstancesAsync`
excludes the instance fleet-wide, so no new workload gets assigned to it, while every workload
already running there keeps finishing on its own (up to `drainTimeout`) via the exact same
graceful wind-down a plain per-workload reassignment goes through.

A bare shutdown (SIGTERM, `stoppingToken` cancelled) with no prior `/drain` call still winds every
workload down gracefully — `LeasedWorkerRunner` doesn't need `BeginDrainAsync` to have been called
to do that — it just won't proactively exclude the instance from new assignments; that only happens
once its heartbeat naturally expires. If you want the fleet to stop routing new work to an instance
the moment it starts shutting down, call the drain endpoint (or `BeginDrainAsync` directly) as the
first step of your shutdown sequence, before the host actually stops.

## Usage sketch

```csharp
// The backing store: the MultiInstanceWorker.Redis package...
services.AddMultiInstanceWorkerRedis(options =>
{
    options.ConnectionString = "localhost:6379";
    options.KeyPrefix = "my-app";
    options.InstanceHeartbeatTtl = TimeSpan.FromSeconds(15); // same value as your InstanceHeartbeatTtlMs
});

// ...or your own adapters, for any other store:
// services.AddSingleton<ILeaseManager, YourLeaseManager>();
// services.AddSingleton<IInstanceRegistry, YourInstanceRegistry>();
// services.AddSingleton<IWorkloadStatusStore, YourWorkloadStatusStore>();

services.AddLeasedWorker(
    workloadKey: "singleton:some-background-job",
    displayName: "Some background job",
    executeAsync: (sp, ct) => sp.GetRequiredService<SomeBackgroundJob>().ExecuteAsync(ct),
    configFactory: sp => sp.GetRequiredService<IOptions<YourWorkerTimingOptions>>().Value,
    drainableSelector: sp => sp.GetRequiredService<SomeBackgroundJob>());
```

Have `SomeBackgroundJob` implement `IDrainableService` and check its own flag at safe boundaries
(e.g. between units of work), rather than relying solely on `drainTimeout` — that ceiling exists to
force a stop if the worker doesn't cooperate, not as the primary drain signal. This keeps the
workload itself free of any dependency on `IInstanceRegistry`; it only needs to know "wrap up now".

For a set of *sharded* (not just singleton) workloads, use `AddWorkloadCoordinator<TWorkload>`
instead of one `AddLeasedWorker` call per workload:

```csharp
services.AddWorkloadCoordinator(
    YourWorkloadCatalog.All,
    keySelector: w => w.Key,
    displayNameSelector: w => w.DisplayName,
    executeAsync: (sp, workload, ct) => sp.GetRequiredService<YourWorkloadRunner>().ExecuteAsync(workload, ct),
    configFactory: sp => sp.GetRequiredService<IOptions<YourWorkerTimingOptions>>().Value,
    drainableSelector: (sp, workload) => sp.GetRequiredService<YourWorkloadRunner>().GetDrainable(workload));
    // workloadAssigner: new PrimaryNodeWorkloadAssigner(), // opt into active/passive instead of balanced
```

`YourWorkload` is entirely your own type — the coordinator only needs a key and a display name out
of it. `configFactory` receives the `IServiceProvider` (rather than a plain value) specifically so
it can come from the options pattern - `IOptions<T>` isn't resolvable until the container is built,
which is after these `Add*` calls run. See `docs/leader-election-flow.md` for the full lifecycle.

Constructing `LeasedWorkerHostedService`/`WorkloadCoordinatorHostedService<TWorkload>` directly
(as `AddLeasedWorker`/`AddWorkloadCoordinator<TWorkload>` do internally) still works if you need
finer control than the extensions give you.

## Project layout

```
src/MultiInstanceWorker/                the engine
src/MultiInstanceWorker.Abstractions/   the contracts shared by the engine and providers
src/MultiInstanceWorker.Redis/          the Redis provider
samples/MultiInstanceWorker.Sample.Api/ a runnable ASP.NET Core sample, Redis-backed (see below)
test/MultiInstanceWorker.Tests/         unit tests (mocked ILeaseManager/IInstanceRegistry/IWorkloadStatusStore)
test/MultiInstanceWorker.FunctionalTests/ functional tests against a real Redis (see below)
docs/leader-election-flow.md            lifecycle diagram and write-up
```

## Sample app: two instances, six jobs, real Redis

`samples/MultiInstanceWorker.Sample.Api` is a minimal-API app that wires this library up to
**Redis** with a single `AddMultiInstanceWorkerRedis` call (the `MultiInstanceWorker.Redis`
package) and runs six named jobs (`JobCatalog`) as sharded workloads under two
`WorkloadCoordinatorHostedService<TWorkload>`s, each with its own `IWorkloadAssigner` strategy,
instead of leaving each job to race for its own lease:

- `JobCatalog.Balanced` (`order-cleanup`, `inventory-sync`, `email-digest`, `cache-warmup`) —
  spread evenly across the active instances via `BalancedNamedWorkloadAssigner` (the default).
- `JobCatalog.Primary` (`billing-reconciliation`, `nightly-report`) — active/passive via
  `PrimaryNodeWorkloadAssigner`, all owned by whichever instance joined earliest.

Run two instances against the same Redis and watch the balanced jobs settle one-per-instance,
while the primary jobs both land on whichever instance joined first:

```
dotnet run --project samples/MultiInstanceWorker.Sample.Api --launch-profile InstanceA
dotnet run --project samples/MultiInstanceWorker.Sample.Api --launch-profile InstanceB
```

Both profiles default to `localhost:6379`; point `Redis:ConnectionString` at whatever Redis you
have running (`docker run --rm -p 6379:6379 redis:7.4` works). Each instance exposes:

- `GET /diagnostics` — this instance's id and drain state, plus every workload's status
  (`Active`/`Transferring`/`Inactive`), current owner, and `CreatedAtUtc` - read straight from
  `IWorkloadStatusStore` via `GetAllAsync()`, so it's the same fleet-wide view whichever instance
  you ask.
- `GET /instances` — the active (non-draining) instance ids this instance currently sees.
- `POST /drain` — manually triggers this instance's drain, without stopping its host, to see the
  other instance take over both jobs.

## Functional tests

`test/MultiInstanceWorker.FunctionalTests` starts a real Redis via
[Testcontainers](https://dotnet.testcontainers.org/) (Docker required) and exercises:

- `RedisLeaseManagerTests` — mutual exclusion, renewal, owner-checked release, and TTL-based
  failover when an owner stops renewing without releasing (a crash) - the actual proof that Redis
  is doing the locking, against real Redis commands rather than a mock.
- `RedisInstanceRegistryTests` — heartbeat visibility, TTL expiry, and the draining exclusion.
- `RedisServiceCollectionExtensionsTests` — what `AddMultiInstanceWorkerRedis` registers, both
  ways it can get its Redis connection, options validation, and that a consumer's own adapter
  registration wins.
- `TwoInstanceApiTests` — hosts two real instances of the sample API in-process
  (`WebApplicationFactory`) against one shared Redis and asserts: both jobs converge to exactly
  one owner, neither job is ever double-owned, and draining one instance hands both jobs to the
  other.

## Backing-store providers

The engine package stays free of any specific backing-store dependency (no StackExchange.Redis, no
SQL driver). Stores are supplied the way EF Core does providers: a separate package per store, each
shipping its own `ILeaseManager`/`IInstanceRegistry`/`IWorkloadStatusStore` implementation behind
one registration method, and each referencing only `MultiInstanceWorker.Abstractions` - never the
engine. `MultiInstanceWorker.Redis` is the first; another store (`MultiInstanceWorker.Postgres`,
...) would follow the same shape. See [its README](src/MultiInstanceWorker.Redis/README.md) for
its options.

One thing a provider can't take from the engine is `LeaderElectionConfig`, so the Redis provider
has its own `RedisWorkerOptions.InstanceHeartbeatTtl`. It must match
`LeaderElectionConfig.InstanceHeartbeatTtlMs` - set both from the same source, as the sample's
`Program.cs` does.

## Origin / migration note

This logic was extracted from a game-engine coordinator built for a crash-game
service. That consuming codebase currently still has its own copy of these types,
implemented directly against a Redis-backed caching/locking package
(`IRedisCaching`, `IDistributedLockManager`) rather than against the abstractions
here. Wiring that service up to this package means:

1. Reference this package from the consumer (local `ProjectReference` during
   development, or a `PackageReference` once this is published to a feed).
2. Write thin adapters implementing `ILeaseManager`, `IInstanceRegistry`, and
   `IWorkloadStatusStore` against whatever store the consumer already uses (its
   existing Redis-backed `RedisLeaseManager` / `RedisInstanceRegistry` are a
   near-literal starting point for the first two — the abstraction shapes are
   unchanged from what those already do, including the `IsDraining` /
   `BeginDrainAsync` members added for the draining/downsizing case; see
   `MultiInstanceWorker.Redis`'s `RedisWorkloadStatusStore` for the third) - or, if
   plain StackExchange.Redis is acceptable there, use `MultiInstanceWorker.Redis` as is.
3. Delete the consumer's local copies of `IInstanceIdentityProvider`,
   `ProcessInstanceIdentityProvider`, `IRedisLeaseManager` → `ILeaseManager`,
   `LeasedWorkerRunner`, `LeasedWorkerHostedService`, `BalancedNamedWorkloadAssigner`,
   `LeaderElectionConfig`, and `IDrainableService`, and update `using`s to this
   package's namespace (`MultiInstanceWorker`) instead of the consumer's own.
4. Replace the consumer's own coordinator with an `AddWorkloadCoordinator<TWorkload>` call,
   supplying its workload descriptor type, key/display-name selectors, and an
   `executeAsync` delegate that constructs and runs the actual workload (e.g. a game
   engine per workload). That construction logic is domain-specific and stays in the
   consumer — only the heartbeat/assign/reconcile loop moves into this library.
