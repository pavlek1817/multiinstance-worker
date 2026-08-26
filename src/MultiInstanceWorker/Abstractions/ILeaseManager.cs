namespace MultiInstanceWorker;

/// <summary>
/// Coordinates ownership of named singleton workloads through a shared backing store.
/// </summary>
/// <remarks>
/// Implementations back this with whatever shared store the host has available
/// (Redis, a SQL table, etc.), and must guarantee that at most one owner can hold
/// a given lease at a time, e.g. by serializing renewals with a short-lived
/// arbitration lock around a read-then-write of the lease record.
/// </remarks>
public interface ILeaseManager
{
    /// <summary>
    /// Tries to acquire the lease, or renews it when the same instance already owns it.
    /// </summary>
    Task<bool> TryAcquireOrRenewAsync(string leaseName, string ownerId, TimeSpan leaseTtl, CancellationToken ct);

    /// <summary>
    /// Removes the lease only when the supplied owner still matches the stored owner.
    /// </summary>
    Task ReleaseIfOwnedAsync(string leaseName, string ownerId, CancellationToken ct);
}
