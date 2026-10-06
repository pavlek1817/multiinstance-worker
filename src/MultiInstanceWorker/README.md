# MultiInstanceWorker

Provider-agnostic building blocks for running background workers across multiple
instances of an application: leader election for singleton workloads, instance
discovery, and even splitting of a set of named workloads across live instances.

This library defines the coordination *logic*; the storage *contracts* it needs live in
`MultiInstanceWorker.Abstractions` (referenced automatically). It ships no backing-store
implementation itself — add the `MultiInstanceWorker.Redis` package for a ready-made Redis
one, or implement three small interfaces against your own store.

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
- `IWorkloadStatusStore` — tracks each workload's lifecycle state (`WorkloadStatus`:
  `Active` / `Transferring` / `Inactive`) across the fleet, each write TTL-bounded.
  `LeasedWorkerRunner` writes it automatically as a workload moves through its
  lifecycle; a coordinator reads it before starting a newly assigned workload to
  avoid starting a runner for one still finishing up elsewhere. `GetAllAsync()`
  reads every currently live record fleet-wide - handy for a diagnostics endpoint.
- `LeasedWorkerRunner` / `LeasedWorkerHostedService` — drives a workload's lifecycle:
  acquire-or-renew the lease every `renewInterval`, start the workload when owned,
  stop it when the lease is lost, and release it when told to stop. Whether it's
  told to stop because a coordinator reassigned it elsewhere or because the whole
  host is shutting down makes no difference to the runner - either way it stops
  taking new work but lets an in-flight worker finish on its own, up to a
  `drainTimeout` ceiling, instead of cancelling it immediately. It never marks the
  whole instance draining itself - see "Draining an instance" below.
  `LeasedWorkerRunner` is the internal state machine; `LeasedWorkerHostedService`
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
  workloads and stopping ones that dropped out of the assignment, whether reassigned
  elsewhere or the instance itself is draining - same graceful treatment either way.
  Workload construction and execution stay entirely in the `executeAsync` delegate
  you supply — this class knows nothing about what a workload actually does.
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

Marking a whole instance draining (`IInstanceRegistry.BeginDrainAsync`) is always a
deliberate, external act — nothing in this library calls it on your behalf, no matter
why a workload's runner stops. Wire it up as your own endpoint or lifecycle hook (a
Kubernetes `preStop` hook hitting it is the common case). Once called,
`GetActiveInstancesAsync` excludes the instance fleet-wide, so no new workload gets
assigned to it, while every workload already running there keeps finishing on its own
(up to `drainTimeout`). A bare shutdown with no prior drain call still winds every
workload down gracefully — it just won't proactively exclude the instance from new
assignments until its heartbeat naturally expires.

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
which is after these `Add*` calls run.

Constructing `LeasedWorkerHostedService`/`WorkloadCoordinatorHostedService<TWorkload>` directly
(as `AddLeasedWorker`/`AddWorkloadCoordinator<TWorkload>` do internally) still works if you need
finer control than the extensions give you.

## Backing-store providers

This package stays free of any specific backing-store dependency (no StackExchange.Redis, no SQL
driver). `ILeaseManager`, `IInstanceRegistry`, and `IWorkloadStatusStore` come from a separate
provider package - `MultiInstanceWorker.Redis` today - or from your own implementation against
whatever store you already use. Providers reference only `MultiInstanceWorker.Abstractions`, never
this package.

## More

Full docs, a runnable Redis-backed sample, and the project's test suite live in the source
repository: <https://github.com/pavlek1817/multiinstance-worker>.
