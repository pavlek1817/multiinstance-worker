namespace MultiInstanceWorker;

/// <summary>
/// Assigns every workload to a single active/passive primary instance, rather than splitting them
/// across the fleet.
/// </summary>
/// <remarks>
/// The primary is whichever active instance has the earliest <see cref="ActiveInstance.JoinedAtUtc"/> -
/// seniority, not id sort order - the same "reduce the active set to one instance" idea
/// <see cref="BalancedNamedWorkloadAssigner"/>'s slicing is built on, just picking one instance
/// instead of dividing among all of them. Sorting by join time rather than by instance id is what
/// keeps the primary stable: a newly-joined instance always has a later timestamp than the
/// incumbent, so it can never sort ahead of - and steal primary from - an instance that's been
/// running the whole time, the way an essentially-random id (e.g. one ending in a GUID) could.
/// Because it's still a pure function of the active-instance set, failover falls out for free -
/// once the primary's heartbeat expires and it drops out of that set, whichever surviving instance
/// joined earliest picks up every workload on its very next reconcile tick, without any extra
/// election step. Useful for workloads that must run on exactly one node at a time by design (e.g.
/// talking to a license- or connection-limited external system) rather than ones that benefit from
/// being spread out.
/// </remarks>
public sealed class PrimaryNodeWorkloadAssigner : IWorkloadAssigner
{
    /// <inheritdoc/>
    public IReadOnlyCollection<TWorkload> GetAssignedWorkloads<TWorkload>(
        IEnumerable<TWorkload> workloads,
        Func<TWorkload, string> keySelector,
        IEnumerable<ActiveInstance> activeInstances,
        string currentInstanceId)
    {
        var primary = activeInstances
            .DistinctBy(x => x.InstanceId)
            .OrderBy(x => x.JoinedAtUtc)
            .ThenBy(x => x.InstanceId)
            .FirstOrDefault();

        if (primary is null || primary.InstanceId != currentInstanceId)
        {
            return Array.Empty<TWorkload>();
        }

        return workloads
            .OrderBy(keySelector)
            .ToArray();
    }
}
