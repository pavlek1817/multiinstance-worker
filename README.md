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
  TTL, list active *non-draining* ids, mark draining, remove on shutdown).
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
  dropped.
- `BalancedNamedWorkloadAssigner` — deterministically slices a set of named
  workloads across the active instance ids (sorted, even split, remainder to the
  earliest instances), so a coordinator can decide which workloads *this* instance
  should run.
- `LeaderElectionConfig` — timing knobs (lease TTL / renew interval, heartbeat TTL /
  interval) with validation that renewal intervals stay safely inside their TTLs.

## Usage sketch

```csharp
services.AddSingleton<IInstanceIdentityProvider, ProcessInstanceIdentityProvider>();
services.AddSingleton<ILeaseManager, YourLeaseManager>();       // your store adapter
services.AddSingleton<IInstanceRegistry, YourInstanceRegistry>(); // your store adapter
services.AddSingleton<BalancedNamedWorkloadAssigner>();

services.AddSingleton<IHostedService>(sp => new LeasedWorkerHostedService(
    sp.GetRequiredService<ILogger<LeasedWorkerHostedService>>(),
    sp.GetRequiredService<ILeaseManager>(),
    sp.GetRequiredService<IInstanceIdentityProvider>(),
    sp.GetRequiredService<IInstanceRegistry>(),
    workloadKey: "singleton:some-background-job",
    displayName: "Some background job",
    leaseTtl: TimeSpan.FromSeconds(30),
    renewInterval: TimeSpan.FromSeconds(10),
    drainTimeout: TimeSpan.FromSeconds(60),
    executeAsync: ct => sp.GetRequiredService<SomeBackgroundJob>().ExecuteAsync(ct)));
```

Your `executeAsync` callback should itself check `IInstanceRegistry.IsDraining` at safe boundaries
(e.g. between units of work) and return once it's true, rather than relying solely on
`drainTimeout` — that ceiling exists to force a stop if the worker doesn't cooperate, not as
the primary drain signal.

For a set of *sharded* (not just singleton) workloads, write a coordinator hosted
service that on each tick: calls `IInstanceRegistry.HeartbeatAsync`, reads
`GetActiveInstanceIdsAsync`, asks `BalancedNamedWorkloadAssigner` which of your
workloads belong to this instance, and starts/stops one `LeasedWorkerRunner` per
assigned workload. See `docs/leader-election-flow.md` for the full lifecycle.

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
`RedisInstanceRegistry.cs`) and runs two independent singleton jobs (`order-cleanup`,
`inventory-sync`) via `LeasedWorkerHostedService`. That Redis adapter deliberately lives in the
sample, not in this package — see [Why no `MultiInstanceWorker.Redis` package (yet)](#why-no-multiinstanceworkerredis-package-yet)
below.

Run two instances against the same Redis and watch them contend for the same two leases:

```
dotnet run --project samples/MultiInstanceWorker.Sample.Api --launch-profile InstanceA
dotnet run --project samples/MultiInstanceWorker.Sample.Api --launch-profile InstanceB
```

Both profiles default to `localhost:6379`; point `Redis:ConnectionString` at whatever Redis you
have running (`docker run --rm -p 6379:6379 redis:7.4` works). Each instance exposes:

- `GET /diagnostics` — this instance's id, drain state, and per-job tick counts/ownership.
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
   and `LeaderElectionConfig`, and update `using`s to this package's namespace
   (`MultiInstanceWorker`) instead of the consumer's own.
4. Keep the consumer's game-engine-specific coordinator (workload descriptors,
   the hosted service that constructs the actual game engine per workload) in the
   consumer — that piece is domain-specific and stays out of this library.
