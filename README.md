# MultiInstanceWorker

Provider-agnostic building blocks for running background workers across multiple
instances of an application: leader election for singleton workloads, instance
discovery, and even splitting of a set of named workloads across live instances.

This library defines the coordination *logic* and the storage *contracts* it needs.
It does not ship a Redis, SQL, or any other backing-store implementation — a
consuming application supplies that by implementing two small interfaces.

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
  stop it when the lease is lost, and release the lease on shutdown. On shutdown (or
  whenever `IInstanceRegistry.IsDraining` becomes true for any other reason, e.g. an
  operator-triggered drain ahead of downsizing) it stops taking new work but lets an
  in-flight worker finish on its own — up to a `drainTimeout` ceiling — instead of
  cancelling it immediately, so in-flight work hands over cleanly rather than getting
  dropped. `LeasedWorkerRunner` is the internal state machine; `LeasedWorkerHostedService`
  (the public entry point) adapts it to the hosted-service lifecycle.
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
  drops out while this instance is draining is left to finish on its own (same
  `drainTimeout`-bounded handling as full shutdown); one that drops out from a plain
  rebalance while the instance is healthy is stopped immediately. Workload
  construction and execution stay entirely in the `executeAsync` delegate you supply
  — this class knows nothing about what a workload actually does.
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
  `ILeaseManager` and `IInstanceRegistry` are never defaulted — register those against
  your own store before calling either extension.

## Usage sketch

```csharp
services.AddSingleton<ILeaseManager, YourLeaseManager>();       // your store adapter
services.AddSingleton<IInstanceRegistry, YourInstanceRegistry>(); // your store adapter

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
src/MultiInstanceWorker/                the library
samples/MultiInstanceWorker.Sample.Api/ a runnable ASP.NET Core sample, Redis-backed (see below)
test/MultiInstanceWorker.Tests/         unit tests (mocked ILeaseManager/IInstanceRegistry)
test/MultiInstanceWorker.FunctionalTests/ functional tests against a real Redis (see below)
docs/leader-election-flow.md            lifecycle diagram and write-up
```

## Sample app: two instances, two jobs, real Redis

`samples/MultiInstanceWorker.Sample.Api` is a minimal-API app that wires this library up to a
**Redis-backed `ILeaseManager`/`IInstanceRegistry`** (`samples/.../Redis/RedisLeaseManager.cs`,
`RedisInstanceRegistry.cs`) and runs two named jobs (`order-cleanup`, `inventory-sync`) as
sharded workloads under one `WorkloadCoordinatorHostedService<TWorkload>`, which splits them
across the active instances via `BalancedNamedWorkloadAssigner` instead of leaving each job to
race for its own lease. That Redis adapter deliberately lives in the sample, not in this package —
see [Why no `MultiInstanceWorker.Redis` package (yet)](#why-no-multiinstanceworkerredis-package-yet)
below.

Run two instances against the same Redis and watch the two jobs settle one-per-instance:

```
dotnet run --project samples/MultiInstanceWorker.Sample.Api --launch-profile InstanceA
dotnet run --project samples/MultiInstanceWorker.Sample.Api --launch-profile InstanceB
```

Both profiles default to `localhost:6379`; point `Redis:ConnectionString` at whatever Redis you
have running (`docker run --rm -p 6379:6379 redis:7.4` works). Each instance exposes:

- `GET /diagnostics` — this instance's id and drain state, plus each job's owner instance id,
  total tick count, and last tick time - read straight from Redis, so it's the same fleet-wide
  view whichever instance you ask.
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
- `TwoInstanceApiTests` — hosts two real instances of the sample API in-process
  (`WebApplicationFactory`) against one shared Redis and asserts: both jobs converge to exactly
  one owner, neither job is ever double-owned, and draining one instance hands both jobs to the
  other.

## Why no `MultiInstanceWorker.Redis` package (yet)

This package stays free of any specific backing-store dependency (no StackExchange.Redis, no SQL
driver) — the sample's Redis adapter is a real, tested implementation of the two interfaces, but
it's intentionally scoped to `samples/`, not published as part of this library. The natural next
step, if/when this needs to support more than "copy the sample's Redis classes into your app", is
something closer to how EF Core does providers: separate packages
(`MultiInstanceWorker.Redis`, `MultiInstanceWorker.Postgres`, ...) each shipping their own
`ILeaseManager`/`IInstanceRegistry` implementation behind a `.UseRedis(...)`-style extension
method, rather than baking any one store into the core package. The sample here is what such a
provider package would eventually wrap.

## Origin / migration note

This logic was extracted from a game-engine coordinator built for a crash-game
service. That consuming codebase currently still has its own copy of these types,
implemented directly against a Redis-backed caching/locking package
(`IRedisCaching`, `IDistributedLockManager`) rather than against the abstractions
here. Wiring that service up to this package means:

1. Reference this package from the consumer (local `ProjectReference` during
   development, or a `PackageReference` once this is published to a feed).
2. Write thin adapters implementing `ILeaseManager` and `IInstanceRegistry` against
   whatever store the consumer already uses (its existing Redis-backed
   `RedisLeaseManager` / `RedisInstanceRegistry` are a near-literal starting point —
   the abstraction shapes are unchanged from what those already do, including the
   `IsDraining` / `BeginDrainAsync` members added for the draining/downsizing case).
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
