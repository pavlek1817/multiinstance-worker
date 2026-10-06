# MultiInstanceWorker.Abstractions

The contracts shared by [`MultiInstanceWorker`](https://www.nuget.org/packages/MultiInstanceWorker)
(the engine) and its backing-store providers such as
[`MultiInstanceWorker.Redis`](https://www.nuget.org/packages/MultiInstanceWorker.Redis). It has no
dependencies of its own.

You normally don't reference this package directly - it comes in transitively with either of the
two above. Reference it on its own only when writing a provider for another store, which means
implementing:

- `ILeaseManager` — grants ownership of a single named workload to exactly one instance at a time.
- `IInstanceRegistry` — tracks which instances are alive, and whether they're draining.
- `IWorkloadStatusStore` — tracks each workload's lifecycle state (`WorkloadStatus`) fleet-wide.

It also holds `IInstanceIdentityProvider`, `IWorkloadAssigner`, `IDrainableService`, and the
`ActiveInstance` / `WorkloadStatusRecord` types those interfaces exchange. Everything lives in the
`MultiInstanceWorker` namespace.

Full docs: <https://github.com/pavlek1817/multiinstance-worker>.
