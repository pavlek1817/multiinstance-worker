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

    /// <summary>Refreshes the current instance heartbeat record in the backing store.</summary>
    Task HeartbeatAsync(CancellationToken ct);

    /// <summary>Returns the set of active instance ids currently visible to the coordinator.</summary>
    Task<string[]> GetActiveInstanceIdsAsync(CancellationToken ct);

    /// <summary>Removes the current instance from the active-instance index.</summary>
    Task RemoveCurrentAsync(CancellationToken ct);
}
