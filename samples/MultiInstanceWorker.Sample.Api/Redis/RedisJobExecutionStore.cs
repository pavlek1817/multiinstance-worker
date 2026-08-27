using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MultiInstanceWorker.Sample.Api.Redis;

/// <summary>
/// A Redis-backed record of each job's live execution state: which instance ticked it most
/// recently, how many ticks it has recorded in total, and when. Unlike
/// <see cref="RedisLeaseManager"/>'s lease keys (which exist purely to arbitrate ownership), this
/// data exists purely to be read: it's what lets <c>/diagnostics</c> on either instance show the
/// same fleet-wide picture of a job, instead of each instance only knowing about the jobs it
/// happens to be running itself.
/// </summary>
/// <remarks>
/// Same rationale as the other Redis adapters in this sample: kept out of the core package because
/// it depends on StackExchange.Redis directly. Not part of the leader-election mechanism either -
/// the lease is still the only thing that actually decides ownership; this just makes that
/// decision, and the work done under it, visible.
/// </remarks>
public sealed class RedisJobExecutionStore(
    IConnectionMultiplexer connectionMultiplexer,
    IOptions<RedisOptions> options,
    TimeSpan statsTtl)
{
    // KEYS[1] = stats hash key, ARGV[1] = owner instance id, ARGV[2] = tick timestamp (unix ms).
    // Stamps the current owner/tick time, bumps the tick count, and refreshes the key's expiry in
    // one round trip (PEXPIRE alongside the write, same idea as the lease TTL) so a job nobody
    // ticks anymore eventually drops out of /diagnostics instead of showing a stale owner forever.
    private static readonly LuaScript RecordTickScript = LuaScript.Prepare(
        """
        redis.call('HSET', @statsKey, 'owner', @ownerId, 'lastTickAtUtc', @tickAtUtc)
        local tickCount = redis.call('HINCRBY', @statsKey, 'tickCount', 1)
        redis.call('PEXPIRE', @statsKey, @ttlMs)
        return tickCount
        """);

    private readonly RedisOptions options = options.Value;

    /// <summary>Records one tick for <paramref name="jobName"/> and returns the job's new total tick count.</summary>
    public async Task<long> RecordTickAsync(string jobName, string ownerInstanceId, CancellationToken ct)
    {
        var db = connectionMultiplexer.GetDatabase();
        var result = await db.ScriptEvaluateAsync(
            RecordTickScript,
            new
            {
                statsKey = this.buildStatsKey(jobName),
                ownerId = ownerInstanceId,
                tickAtUtc = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                ttlMs = (long)statsTtl.TotalMilliseconds,
            }).WaitAsync(ct);

        return (long)result;
    }

    /// <summary>Reads the current stats for each of <paramref name="jobNames"/>, in parallel.</summary>
    public async Task<IReadOnlyCollection<JobExecutionStats>> GetAllAsync(IEnumerable<string> jobNames, CancellationToken ct)
    {
        var db = connectionMultiplexer.GetDatabase();
        var snapshots = await Task.WhenAll(jobNames.Select(jobName => this.getOneAsync(db, jobName, ct)));
        return snapshots;
    }

    private async Task<JobExecutionStats> getOneAsync(IDatabase db, string jobName, CancellationToken ct)
    {
        var entries = await db.HashGetAllAsync(this.buildStatsKey(jobName)).WaitAsync(ct);
        if (entries.Length == 0)
        {
            // No tick has ever been recorded for this job (or its stats expired) - nobody's
            // currently claimed it as far as this store is concerned.
            return new JobExecutionStats(jobName, ownerInstanceId: null, tickCount: 0, lastTickAtUtc: null);
        }

        var hash = entries.ToDictionary(e => (string)e.Name!, e => e.Value, StringComparer.Ordinal);
        return new JobExecutionStats(
            jobName,
            ownerInstanceId: (string?)hash["owner"],
            tickCount: (long)hash["tickCount"],
            lastTickAtUtc: DateTimeOffset.FromUnixTimeMilliseconds((long)hash["lastTickAtUtc"]));
    }

    private RedisKey buildStatsKey(string jobName) => $"{this.options.KeyPrefix}:job:{jobName}:stats";
}

/// <summary>Point-in-time execution state for one job, as currently recorded in Redis.</summary>
public sealed class JobExecutionStats(string jobName, string? ownerInstanceId, long tickCount, DateTimeOffset? lastTickAtUtc)
{
    public string JobName { get; } = jobName;

    public string? OwnerInstanceId { get; } = ownerInstanceId;

    public long TickCount { get; } = tickCount;

    public DateTimeOffset? LastTickAtUtc { get; } = lastTickAtUtc;
}
