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
- `IInstanceRegistry` — tracks which instances are alive. A consumer implements this
  against its own store (heartbeat write with TTL, list active ids, remove on shutdown).
- `ILeaseManager` — grants ownership of a single named workload to exactly one
  instance at a time. A consumer implements this against its own store, and must
  guarantee mutual exclusion on renewal (e.g. a short arbitration lock around a
  read-then-write of the lease record).
- `LeasedWorkerRunner` / `LeasedWorkerHostedService` — drives a workload's lifecycle:
  acquire-or-renew the lease every `renewInterval`, start the workload when owned,
  stop it when the lease is lost, and release the lease on shutdown.
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
    workloadKey: "singleton:some-background-job",
    displayName: "Some background job",
    leaseTtl: TimeSpan.FromSeconds(30),
    renewInterval: TimeSpan.FromSeconds(10),
    executeAsync: ct => sp.GetRequiredService<SomeBackgroundJob>().ExecuteAsync(ct)));
```

For a set of *sharded* (not just singleton) workloads, write a coordinator hosted
service that on each tick: calls `IInstanceRegistry.HeartbeatAsync`, reads
`GetActiveInstanceIdsAsync`, asks `BalancedNamedWorkloadAssigner` which of your
workloads belong to this instance, and starts/stops one `LeasedWorkerRunner` per
assigned workload. See `docs/leader-election-flow.md` for the full lifecycle.

## Project layout

```
src/MultiInstanceWorker/        the library
test/MultiInstanceWorker.Tests/ NUnit tests
docs/leader-election-flow.md    lifecycle diagram and write-up
```

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
   the abstraction shapes are unchanged from what those already do).
3. Delete the consumer's local copies of `IInstanceIdentityProvider`,
   `ProcessInstanceIdentityProvider`, `IRedisLeaseManager` → `ILeaseManager`,
   `LeasedWorkerRunner`, `LeasedWorkerHostedService`, `BalancedNamedWorkloadAssigner`,
   and `LeaderElectionConfig`, and update `using`s to this package's namespace
   (`MultiInstanceWorker`) instead of the consumer's own.
4. Keep the consumer's game-engine-specific coordinator (workload descriptors,
   the hosted service that constructs the actual game engine per workload) in the
   consumer — that piece is domain-specific and stays out of this library.
