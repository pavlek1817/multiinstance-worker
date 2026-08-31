namespace MultiInstanceWorker;

/// <summary>Point-in-time lifecycle state for one workload, as recorded by an <see cref="IWorkloadStatusStore"/>.</summary>
public sealed record WorkloadStatusRecord
{
    /// <summary>The workload's key, as passed to <see cref="IWorkloadStatusStore.SetStatusAsync"/>.</summary>
    required public string WorkloadKey { get; init; }

    /// <summary>The workload's current lifecycle state.</summary>
    required public WorkloadStatus Status { get; init; }

    /// <summary>The instance id that wrote this status.</summary>
    required public string OwnerInstanceId { get; init; }

    /// <summary>
    /// When this record expires if not refreshed. A store must never return an already-expired
    /// record from <see cref="IWorkloadStatusStore.GetStatusesAsync"/> - this is only exposed for
    /// diagnostics.
    /// </summary>
    required public DateTimeOffset ExpiresAtUtc { get; init; }
}
