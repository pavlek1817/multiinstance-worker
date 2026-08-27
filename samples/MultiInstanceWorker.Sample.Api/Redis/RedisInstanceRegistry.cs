using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MultiInstanceWorker.Sample.Api.Redis;

/// <summary>
/// A Redis-backed <see cref="IInstanceRegistry"/>. Heartbeats live in a sorted set keyed by
/// instance id with the heartbeat timestamp as score; draining instances are tracked in a
/// separate set so they can be excluded from <see cref="GetActiveInstanceIdsAsync"/> without
/// losing their heartbeat.
/// </summary>
/// <remarks>
/// Same rationale as <see cref="RedisLeaseManager"/>: a real, StackExchange.Redis-backed adapter
/// that lives in this sample app, not in the core package.
/// </remarks>
public sealed class RedisInstanceRegistry : IInstanceRegistry
{
    private readonly IConnectionMultiplexer connectionMultiplexer;
    private readonly RedisOptions options;
    private readonly TimeSpan heartbeatTtl;

    public RedisInstanceRegistry(
        IConnectionMultiplexer connectionMultiplexer,
        IOptions<RedisOptions> options,
        IInstanceIdentityProvider instanceIdentityProvider,
        TimeSpan heartbeatTtl)
    {
        this.connectionMultiplexer = connectionMultiplexer;
        this.options = options.Value;
        this.heartbeatTtl = heartbeatTtl;
        this.InstanceId = instanceIdentityProvider.InstanceId;
    }

    public string InstanceId { get; }

    public bool IsDraining { get; private set; }

    private RedisKey heartbeatsKey => $"{this.options.KeyPrefix}:instances:heartbeats";

    private RedisKey drainingKey => $"{this.options.KeyPrefix}:instances:draining";

    public async Task HeartbeatAsync(CancellationToken ct)
    {
        var db = this.connectionMultiplexer.GetDatabase();
        await db.SortedSetAddAsync(this.heartbeatsKey, this.InstanceId, nowUnixMs()).WaitAsync(ct);
    }

    public async Task BeginDrainAsync(CancellationToken ct)
    {
        this.IsDraining = true;

        var db = this.connectionMultiplexer.GetDatabase();
        await db.SetAddAsync(this.drainingKey, this.InstanceId).WaitAsync(ct);
    }

    public async Task<string[]> GetActiveInstanceIdsAsync(CancellationToken ct)
    {
        var db = this.connectionMultiplexer.GetDatabase();
        var minScore = nowUnixMs() - this.heartbeatTtl.TotalMilliseconds;

        // Best-effort cleanup of heartbeats nobody renewed in time. Not required for
        // correctness below (the score-range read already excludes them) but keeps the
        // sorted set from growing forever across instance restarts.
        await db.SortedSetRemoveRangeByScoreAsync(this.heartbeatsKey, double.NegativeInfinity, minScore - 1).WaitAsync(ct);

        var activeTask = db.SortedSetRangeByScoreAsync(this.heartbeatsKey, minScore, double.PositiveInfinity);
        var drainingTask = db.SetMembersAsync(this.drainingKey);
        await Task.WhenAll(activeTask, drainingTask).WaitAsync(ct);

        var draining = new HashSet<string>(drainingTask.Result.Select(x => (string)x!), StringComparer.Ordinal);

        return activeTask.Result
            .Select(x => (string)x!)
            .Where(id => !draining.Contains(id))
            .ToArray();
    }

    public async Task RemoveCurrentAsync(CancellationToken ct)
    {
        var db = this.connectionMultiplexer.GetDatabase();
        await Task.WhenAll(
            db.SortedSetRemoveAsync(this.heartbeatsKey, this.InstanceId),
            db.SetRemoveAsync(this.drainingKey, this.InstanceId)).WaitAsync(ct);
    }

    private static double nowUnixMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
