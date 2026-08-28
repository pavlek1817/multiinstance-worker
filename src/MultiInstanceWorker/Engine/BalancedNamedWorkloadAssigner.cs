namespace MultiInstanceWorker;

/// <summary>
/// Splits a set of named workloads evenly across the active instances, by sort order.
/// </summary>
/// <remarks>
/// Instances are ordered by <see cref="ActiveInstance.JoinedAtUtc"/> (then by instance id as a
/// tie-breaker), not by instance id alone - the same seniority ordering
/// <see cref="PrimaryNodeWorkloadAssigner"/> uses. This keeps each instance's slice of the
/// workload stable as the fleet scales up or down: a newly-joined instance is always appended
/// after the existing ones instead of potentially sorting ahead of them and reshuffling who owns
/// which slice.
/// </remarks>
public sealed class BalancedNamedWorkloadAssigner : IWorkloadAssigner
{
    /// <inheritdoc/>
    public IReadOnlyCollection<TWorkload> GetAssignedWorkloads<TWorkload>(
        IEnumerable<TWorkload> workloads,
        Func<TWorkload, string> keySelector,
        IEnumerable<ActiveInstance> activeInstances,
        string currentInstanceId)
    {
        var orderedInstances = activeInstances
            .DistinctBy(x => x.InstanceId)
            .OrderBy(x => x.JoinedAtUtc)
            .ThenBy(x => x.InstanceId)
            .Select(x => x.InstanceId)
            .ToArray();

        var currentIndex = Array.IndexOf(orderedInstances, currentInstanceId);
        if (currentIndex < 0)
        {
            return Array.Empty<TWorkload>();
        }

        var orderedWorkloads = workloads
            .OrderBy(keySelector)
            .ToArray();

        if (orderedWorkloads.Length == 0)
        {
            return Array.Empty<TWorkload>();
        }

        var baseWorkloadCount = orderedWorkloads.Length / orderedInstances.Length;
        var remainder = orderedWorkloads.Length % orderedInstances.Length;
        var assignedCount = baseWorkloadCount + (currentIndex < remainder ? 1 : 0);
        var startIndex = (currentIndex * baseWorkloadCount) + Math.Min(currentIndex, remainder);

        return orderedWorkloads
            .Skip(startIndex)
            .Take(assignedCount)
            .ToArray();
    }
}
