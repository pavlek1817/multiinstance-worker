# Leader election flow

This library keeps singleton (or evenly sharded) background work running on
exactly the right instances at a time. It uses two storage-backed concerns,
each defined here as an interface a consumer implements against its own store:

- an **instance registry** (`IInstanceRegistry`) that records which application
  processes are alive
- a **lease manager** (`ILeaseManager`) that grants one instance ownership of a
  named workload

## Main lifecycle

```mermaid
flowchart TD
    A[Application host starts] --> B[ProcessInstanceIdentityProvider creates InstanceId]
    B --> C[Coordinator and leased hosted services start]
    C --> D[HeartbeatAsync writes the instance record to the store]
    D --> E[GetActiveInstanceIdsAsync reads live instances]
    E --> F[BalancedNamedWorkloadAssigner chooses local workloads]
    F --> G{Is this workload assigned here?}
    G -- No --> D
    G -- Yes --> H[LeasedWorkerRunner starts]
    H --> I[TryAcquireOrRenewAsync]
    I --> J{Lease owned by this instance?}
    J -- No --> K[Wait for next renew interval]
    K --> I
    J -- Yes --> L[Start or keep running the worker task]
    L --> M{Draining or worker finished?}
    M -- No --> L
    M -- Finished --> N[Clean up worker state and continue]
    M -- Draining --> O[Finish current cycle, then stop taking new work]
    O --> P[ReleaseIfOwnedAsync]
    P --> Q[Lease removed only if owner still matches]
```

## What each part does

### Instance identity
`ProcessInstanceIdentityProvider` gives each process a unique id. That id is used everywhere the instance needs to prove ownership.

### Instance registry
A consumer's `IInstanceRegistry` implementation stores a heartbeat record with a TTL. A coordinator uses that list to know which instances are still active. (A Redis-backed example would write a keyed record with TTL and read back a secondary index of live keys; any store that supports expiring records and a membership query works.)

### Lease manager
A consumer's `ILeaseManager` implementation protects each named workload with mutual exclusion around a lease record — for example a short arbitration lock plus a read-then-write of the lease record. A lease can be renewed by the same owner, but not stolen by another owner before it expires.

### Leased worker runner
`LeasedWorkerRunner` is the loop that actually runs a workload. It:

1. tries to acquire or renew the lease
2. starts the worker only when the lease is owned
3. switches into drain mode on shutdown (or whenever `IInstanceRegistry.IsDraining` becomes true for any other reason), calling `RequestDrain()` on the workload's `IDrainableService` (if it supplied one) the moment that happens
4. lets the current cycle finish, then stops taking new work
5. releases the lease during shutdown

### Coordinator
For workloads beyond a single singleton, `WorkloadCoordinatorHostedService<TWorkload>` keeps
heartbeats alive, reads active instances, uses `BalancedNamedWorkloadAssigner` to decide which
workloads belong to this instance, and reconciles one `LeasedWorkerRunner` per assigned workload —
starting runners for newly assigned workloads and stopping ones that dropped out. Each runner gets
its own `CancellationTokenSource`, independent of the coordinator's host token, and is driven with
`LeasedWorkerRunner.RunAsync(stoppingToken, shutdownToken)`: the per-runner token as `stoppingToken`
(cancelled when the coordinator removes just that runner) and the host token as `shutdownToken`
(cancelled when the whole instance shuts down) — this is what lets one workload being reassigned
stay a purely local event instead of draining the whole instance.

A workload dropping out of this instance's assignment is handled based on *why* the instance's own
state changed, not why the workload left:
- instance draining, runner still running → left alone this tick, same as full shutdown
- instance draining, runner already finished → cleaned up without cancelling
- instance not draining (a plain rebalance) → stopped immediately, scoped to that one runner

Workload construction and execution stay entirely in the `executeAsync` delegate a consumer
supplies (it knows how to construct and run each workload) — that piece is domain-specific and
lives in the consuming application, not in this library.

### Draining
When an instance begins draining — because its host is shutting down, or because something else flips `IInstanceRegistry.IsDraining` ahead of a planned downsize — the backing store should mark that instance's record so other nodes stop assigning new workloads to it (see `GetActiveInstanceIdsAsync`, which excludes draining instances). Each `LeasedWorkerRunner` for that instance keeps its current worker running until it reaches a safe boundary and exits on its own, up to a `drainTimeout` ceiling; only once that ceiling is hit does the runner force-cancel the worker. This is what lets an instance hand its in-flight work over cleanly instead of dropping it when the cluster is downsized.

A workload doesn't have to depend on `IInstanceRegistry` to find out it should wrap up. If it
implements `IDrainableService`, the runner calls `RequestDrain()` on it the instant drain mode is
entered — well ahead of `drainTimeout` — so the workload only needs to know "stop after the current
unit of work", not anything about leases or instance registries.

## Result

At runtime, the cluster converges on one active owner per singleton workload. If an instance dies, its heartbeat expires, the workload is reassigned, and the new owner starts the worker after taking the lease.
