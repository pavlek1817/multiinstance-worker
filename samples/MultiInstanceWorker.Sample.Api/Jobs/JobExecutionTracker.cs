using MultiInstanceWorker.Sample.Api.Redis;

namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// Reads and writes each job's live execution state through <see cref="RedisJobExecutionStore"/>,
/// so <c>/diagnostics</c> shows the same fleet-wide view - current owner, total ticks, last tick
/// time - no matter which instance answers the request, instead of each instance only knowing
/// about jobs it happens to run itself.
/// </summary>
public sealed class JobExecutionTracker(RedisJobExecutionStore store, IInstanceIdentityProvider instanceIdentityProvider)
{
    /// <summary>Records one tick for <paramref name="jobName"/> on this instance and returns the job's new total tick count.</summary>
    public Task<long> RecordTickAsync(string jobName, CancellationToken ct) =>
        store.RecordTickAsync(jobName, instanceIdentityProvider.InstanceId, ct);

    public async Task<IReadOnlyCollection<JobSnapshot>> SnapshotAllAsync(IEnumerable<string> jobNames, CancellationToken ct)
    {
        var stats = await store.GetAllAsync(jobNames, ct);

        return stats
            .Select(s => new JobSnapshot(
                s.JobName,
                s.OwnerInstanceId,
                isRunningHere: s.OwnerInstanceId == instanceIdentityProvider.InstanceId,
                s.TickCount,
                s.LastTickAtUtc))
            .ToArray();
    }
}

/// <summary>Point-in-time view of one job's execution state, for <c>/diagnostics</c>.</summary>
public sealed class JobSnapshot(string name, string? ownerInstanceId, bool isRunningHere, long tickCount, DateTimeOffset? lastTickAtUtc)
{
    public string Name { get; } = name;

    // The instance id Redis last saw tick this job, or null if it has never ticked.
    public string? OwnerInstanceId { get; } = ownerInstanceId;

    // Whether the instance answering this request is the one currently ticking this job.
    public bool IsRunningHere { get; } = isRunningHere;

    public long TickCount { get; } = tickCount;

    public DateTimeOffset? LastTickAtUtc { get; } = lastTickAtUtc;
}
