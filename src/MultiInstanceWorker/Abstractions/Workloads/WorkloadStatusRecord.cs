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

    /// <summary>
    /// When this workload's status was first recorded - written once and never refreshed by
    /// subsequent writes, the same write-once idea as <see cref="ActiveInstance.JoinedAtUtc"/>. A
    /// gap where the record fully expires and later gets written again starts a new value here,
    /// same as a restarted instance gets a new <c>JoinedAtUtc</c>.
    /// </summary>
    required public DateTimeOffset CreatedAtUtc { get; init; }
}
