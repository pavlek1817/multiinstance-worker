namespace MultiInstanceWorker;

/// <summary>
/// Splits a set of named workloads evenly across the active instances, by sort order.
/// </summary>
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
            .Select(x => x.InstanceId)
            .Distinct()
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToArray();

        var currentIndex = Array.IndexOf(orderedInstances, currentInstanceId);
        if (currentIndex < 0)
        {
            return Array.Empty<TWorkload>();
        }

        var orderedWorkloads = workloads
            .OrderBy(keySelector, StringComparer.Ordinal)
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
