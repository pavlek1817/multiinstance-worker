namespace MultiInstanceWorker;

/// <summary>
/// Optional: records and reports a job's live execution state (who's currently ticking it, how
/// many ticks, when last) so it can be surfaced fleet-wide - e.g. from a diagnostics endpoint -
/// regardless of which instance answers.
/// </summary>
/// <remarks>
/// Nothing in this package reads or writes through this interface - like <see cref="IWorkloadAssigner"/>
/// and <see cref="IDrainableService"/>, it stays outside the "the coordinator knows nothing about
/// what a workload actually does" boundary: this package has no opinion on what a "tick" means for
/// any given workload, or when one happens. A consumer's own <c>executeAsync</c> delegate resolves
/// this from DI and calls it on its own terms - see the sample app's <c>RedisJobExecutionStore</c>
/// for a real implementation.
/// </remarks>
public interface IJobExecutionStore
{
    /// <summary>
    /// Records one tick for <paramref name="jobKey"/>, owned by <paramref name="ownerInstanceId"/>,
    /// and returns its new total tick count.
    /// </summary>
    Task<long> RecordTickAsync(string jobKey, string ownerInstanceId, CancellationToken ct);

    /// <summary>Reads the current execution stats for each of <paramref name="jobKeys"/>, in parallel.</summary>
    Task<IReadOnlyCollection<JobExecutionStats>> GetAllAsync(IEnumerable<string> jobKeys, CancellationToken ct);
}
