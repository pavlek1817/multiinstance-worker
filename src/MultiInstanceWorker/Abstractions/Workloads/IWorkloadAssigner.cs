namespace MultiInstanceWorker;

/// <summary>
/// Decides which of a set of named workloads belong to this instance, given the currently active
/// instances.
/// </summary>
/// <remarks>
/// Used by <see cref="WorkloadCoordinatorHostedService{TWorkload}"/> to decide what to attempt
/// locally. This is a liveness/efficiency mechanism, not a safety one: two instances computing
/// slightly different assignments for a brief window (e.g. around a heartbeat TTL boundary) is
/// tolerated, because the actual mutual exclusion is still enforced by <see cref="ILeaseManager"/>
/// underneath each assigned workload's <see cref="LeasedWorkerRunner"/>. An implementation should
/// therefore be a pure, deterministic function of its inputs - no side effects, no calls back into
/// the backing store - so every instance reaches the same answer from the same active-instance view.
/// </remarks>
public interface IWorkloadAssigner
{
    /// <summary>
    /// Returns the slice of <paramref name="workloads"/> assigned to <paramref name="currentInstanceId"/>.
    /// </summary>
    /// <typeparam name="TWorkload">The consumer-defined workload descriptor type.</typeparam>
    /// <param name="workloads">The full catalog of workloads to divide up.</param>
    /// <param name="keySelector">Extracts each workload's unique, stable key.</param>
    /// <param name="activeInstances">The instances currently considered alive and non-draining.</param>
    /// <param name="currentInstanceId">The instance id to compute the assignment for.</param>
    IReadOnlyCollection<TWorkload> GetAssignedWorkloads<TWorkload>(
        IEnumerable<TWorkload> workloads,
        Func<TWorkload, string> keySelector,
        IEnumerable<ActiveInstance> activeInstances,
        string currentInstanceId);
}
