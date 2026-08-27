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
- `WorkloadCoordinatorHostedService<TWorkload>` — the sharded-workload counterpart
  to `LeasedWorkerHostedService`: one hosted service that heartbeats, re-evaluates
  `BalancedNamedWorkloadAssigner`'s assignment every tick, and reconciles one
  `LeasedWorkerRunner` per assigned workload — starting runners for newly assigned
  workloads and stopping ones that dropped out of the assignment. A workload that
  drops out while this instance is draining is left to finish on its own (same
  `drainTimeout`-bounded handling as full shutdown); one that drops out from a plain
  rebalance while the instance is healthy is stopped immediately. Workload
  construction and execution stay entirely in the `executeAsync` delegate you supply
  — this class knows nothing about what a workload actually does.
- `IDrainableService` — an optional `RequestDrain()` contract a workload can implement
  to receive its own cooperative stop signal, instead of depending on `IInstanceRegistry`
  just to poll `IsDraining`. Pass the workload as `drainable` to `LeasedWorkerRunner` /
  `LeasedWorkerHostedService` (or resolve it per-workload via
  `WorkloadCoordinatorHostedService<TWorkload>`'s `drainableSelector`) and `RequestDrain()`
  is called the moment that runner enters drain mode — well before `drainTimeout` would
  force a cancellation. It's still optional: a workload can instead observe
  `IInstanceRegistry.IsDraining` itself, or just rely on the cancellation token plus
  `drainTimeout`.
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
    executeAsync: ct => sp.GetRequiredService<SomeBackgroundJob>().ExecuteAsync(ct),
    drainable: sp.GetRequiredService<SomeBackgroundJob>()));
```

Have `SomeBackgroundJob` implement `IDrainableService` and check its own flag at safe boundaries
(e.g. between units of work), rather than relying solely on `drainTimeout` — that ceiling exists to
force a stop if the worker doesn't cooperate, not as the primary drain signal. This keeps the
workload itself free of any dependency on `IInstanceRegistry`; it only needs to know "wrap up now".

For a set of *sharded* (not just singleton) workloads, use
`WorkloadCoordinatorHostedService<TWorkload>` instead of one `LeasedWorkerHostedService`
per workload:

```csharp
services.AddSingleton<BalancedNamedWorkloadAssigner>();
services.AddSingleton<IReadOnlyCollection<YourWorkload>>(sp => YourWorkloadCatalog.All);

services.AddSingleton<IHostedService>(sp => new WorkloadCoordinatorHostedService<YourWorkload>(
    sp.GetRequiredService<ILogger<WorkloadCoordinatorHostedService<YourWorkload>>>(),
    sp.GetRequiredService<ILoggerFactory>(),
    sp.GetRequiredService<IInstanceRegistry>(),
    sp.GetRequiredService<IInstanceIdentityProvider>(),
    sp.GetRequiredService<ILeaseManager>(),
    sp.GetRequiredService<BalancedNamedWorkloadAssigner>(),
    sp.GetRequiredService<IReadOnlyCollection<YourWorkload>>(),
    keySelector: w => w.Key,
    displayNameSelector: w => w.DisplayName,
    executeAsync: (workload, ct) => sp.GetRequiredService<YourWorkloadRunner>().ExecuteAsync(workload, ct),
    leaseTtl: TimeSpan.FromSeconds(30),
    renewInterval: TimeSpan.FromSeconds(10),
    drainTimeout: TimeSpan.FromSeconds(60),
    heartbeatInterval: TimeSpan.FromSeconds(5),
    drainableSelector: workload => sp.GetRequiredService<YourWorkloadRunner>().GetDrainable(workload)));
```

`YourWorkload` is entirely your own type — the coordinator only needs a key and a display name out
of it.

## Why no `MultiInstanceWorker.Redis` package (yet)

This package stays free of any specific backing-store dependency (no StackExchange.Redis, no SQL
driver) - you supply `ILeaseManager` and `IInstanceRegistry` against whatever store you already
use. The project repository's sample app includes a real, tested Redis implementation of both
interfaces you can use as a starting point.

## More

Full docs, a runnable Redis-backed sample, and the project's test suite live in the source
repository: <https://github.com/pavlek1817/multiinstance-worker>.
