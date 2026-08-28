namespace MultiInstanceWorker;

/// <summary>
/// Configures how leader election and instance heartbeats are timed.
/// </summary>
/// <remarks>
/// The lease TTL and renewal interval control singleton workload ownership.
/// The heartbeat TTL and interval control how long an instance remains visible
/// in the active-instance index used by the coordinator.
/// </remarks>
public record LeaderElectionConfig
{
    /// <summary>How long a workload lease remains valid before another instance can take it.</summary>
    public int LeaseTtlMs { get; init; }

    /// <summary>How often the current owner tries to renew its workload lease.</summary>
    public int LeaseRenewIntervalMs { get; init; }

    /// <summary>How long an instance heartbeat remains visible in the backing store.</summary>
    public int InstanceHeartbeatTtlMs { get; init; }

    /// <summary>How often the coordinator refreshes the instance heartbeat.</summary>
    public int InstanceHeartbeatIntervalMs { get; init; }

    /// <summary>
    /// How long a draining (or, under a coordinator, reassigned-away) workload is allowed to
    /// finish on its own before it is force-cancelled.
    /// </summary>
    public int DrainTimeoutMs { get; init; }

    /// <summary>
    /// Validates that all timing values are positive and that every renewal interval
    /// is shorter than the TTL it is meant to sustain.
    /// </summary>
    public virtual void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.LeaseTtlMs, 0, nameof(this.LeaseTtlMs));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.LeaseRenewIntervalMs, 0, nameof(this.LeaseRenewIntervalMs));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(
            this.InstanceHeartbeatTtlMs,
            0,
            nameof(this.InstanceHeartbeatTtlMs));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(
            this.InstanceHeartbeatIntervalMs,
            0,
            nameof(this.InstanceHeartbeatIntervalMs));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.DrainTimeoutMs, 0, nameof(this.DrainTimeoutMs));

        if (this.LeaseRenewIntervalMs >= this.LeaseTtlMs)
        {
            throw new ArgumentOutOfRangeException(
                nameof(this.LeaseRenewIntervalMs),
                "Lease renew interval must be shorter than the lease TTL.");
        }

        if (this.InstanceHeartbeatIntervalMs >= this.InstanceHeartbeatTtlMs)
        {
            throw new ArgumentOutOfRangeException(
                nameof(this.InstanceHeartbeatIntervalMs),
                "Instance heartbeat interval must be shorter than the heartbeat TTL.");
        }
    }
}
