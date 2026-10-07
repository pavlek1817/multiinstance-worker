namespace MultiInstanceWorker;

/// <summary>
/// Provides a graceful-drain signal for services that must stop at a safe boundary.
/// </summary>
/// <remarks>
/// This is an optional contract: a workload passed as the <c>drainable</c> argument to
/// <see cref="LeasedWorkerRunner"/> / <see cref="LeasedWorkerHostedService"/> (directly), or
/// resolved per-workload by <see cref="WorkloadCoordinatorHostedService{TWorkload}"/>'s
/// <c>drainableSelector</c>, has <see cref="RequestDrain"/> called on it the moment that runner
/// enters drain mode — well before the <c>drainTimeout</c> ceiling would force a cancellation.
/// Implementing it decouples a workload from the library's own <see cref="IInstanceRegistry"/>:
/// the workload only needs to know "wrap up now", not poll a leader-election-specific interface to
/// find out. It's still optional — a workload can instead observe <see cref="IInstanceRegistry.IsDraining"/>
/// itself, or just rely on the cancellation token plus <c>drainTimeout</c>.
/// </remarks>
public interface IDrainableService
{
    /// <summary>
    /// Requests a graceful stop after the current unit of work.
    /// </summary>
    void RequestDrain();
}
