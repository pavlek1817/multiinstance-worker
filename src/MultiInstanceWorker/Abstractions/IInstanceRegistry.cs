namespace MultiInstanceWorker;

/// <summary>
/// Tracks which application instances are still alive for workload assignment.
/// </summary>
/// <remarks>
/// Implementations back this with whatever shared store the host has available
/// (Redis, a SQL table, etc.) as long as heartbeat records expire on their own
/// when an instance stops renewing them.
/// </remarks>
public interface IInstanceRegistry
{
    /// <summary>Returns the current instance identity.</summary>
    string InstanceId { get; }

    /// <summary>Gets a value indicating whether this instance is currently draining.</summary>
    bool IsDraining { get; }

    /// <summary>Refreshes the current instance heartbeat record in the backing store.</summary>
    Task HeartbeatAsync(CancellationToken ct);

    /// <summary>Marks the current instance as draining and persists that state to the backing store.</summary>
    Task BeginDrainAsync(CancellationToken ct);

    /// <summary>
    /// Returns the set of active, non-draining instances currently visible to the coordinator,
    /// each with the time it first joined the registry.
    /// </summary>
    Task<IReadOnlyCollection<ActiveInstance>> GetActiveInstancesAsync(CancellationToken ct);

    /// <summary>Removes the current instance from the active-instance index.</summary>
    Task RemoveCurrentAsync(CancellationToken ct);
}
