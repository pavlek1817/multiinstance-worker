namespace MultiInstanceWorker;

/// <summary>
/// The lifecycle state of one workload, as tracked by <see cref="IWorkloadStatusStore"/>.
/// </summary>
public enum WorkloadStatus
{
    /// <summary>The workload is actively running on <see cref="WorkloadStatusRecord.OwnerInstanceId"/>.</summary>
    Active,

    /// <summary>
    /// The workload is winding down on <see cref="WorkloadStatusRecord.OwnerInstanceId"/> - either
    /// reassigned to another instance or the instance itself is draining - and must not be started
    /// anywhere else until it clears, whether by finishing naturally or by this status expiring.
    /// </summary>
    Transferring,

    /// <summary>The workload is not currently running anywhere.</summary>
    Inactive,
}
