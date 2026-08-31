namespace MultiInstanceWorker;

/// <summary>
/// Tracks each workload's lifecycle state (<see cref="WorkloadStatus"/>) across the fleet, so a
/// coordinator can tell a workload that is actively finishing up elsewhere - mid-handover, whether
/// from a plain reassignment or an instance drain - apart from one that has genuinely gone quiet.
/// </summary>
/// <remarks>
/// Implementations back this with whatever shared store the host has available (Redis, a SQL table,
/// etc.), same as <see cref="ILeaseManager"/> and <see cref="IInstanceRegistry"/>. Every write carries
/// its own TTL, and <see cref="GetStatusesAsync"/> must silently omit an expired record rather than
/// return it - this is what keeps a crashed writer from leaving <see cref="WorkloadStatus.Transferring"/>
/// stuck forever: the worst a stale record can do is block a handover for its own TTL, never
/// indefinitely.
/// <para/>
/// This is a liveness/efficiency mechanism layered on top of <see cref="ILeaseManager"/>, not a
/// replacement for it: <see cref="WorkloadCoordinatorHostedService{TWorkload}"/> uses it to avoid
/// starting a runner that would just poll for a lease it cannot get yet, but the lease itself remains
/// the actual mutual-exclusion guarantee underneath - two instances briefly disagreeing about a
/// workload's status (e.g. around a TTL boundary) is tolerated the same way a stale
/// <see cref="IWorkloadAssigner"/> view is.
/// </remarks>
public interface IWorkloadStatusStore
{
    /// <summary>
    /// Writes (or overwrites) the current status for <paramref name="workloadKey"/>, owned by
    /// <paramref name="ownerInstanceId"/>, valid for <paramref name="ttl"/>.
    /// </summary>
    Task SetStatusAsync(string workloadKey, WorkloadStatus status, string ownerInstanceId, TimeSpan ttl, CancellationToken ct);

    /// <summary>
    /// Reads the current, unexpired status for each of <paramref name="workloadKeys"/>, in parallel.
    /// A key with no live record - never written, or its TTL lapsed - is simply absent from the
    /// result rather than represented as an explicit <see cref="WorkloadStatus.Inactive"/> entry.
    /// </summary>
    Task<IReadOnlyDictionary<string, WorkloadStatusRecord>> GetStatusesAsync(IEnumerable<string> workloadKeys, CancellationToken ct);
}
