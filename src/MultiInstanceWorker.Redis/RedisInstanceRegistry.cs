using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MultiInstanceWorker.Redis;

/// <summary>
/// A Redis-backed <see cref="IInstanceRegistry"/>. Heartbeats live in a sorted set keyed by
/// instance id with the heartbeat timestamp as score; draining instances are tracked in a
/// separate set so they can be excluded from <see cref="GetActiveInstancesAsync"/> without
/// losing their heartbeat. A separate hash records when each instance was first seen, since the
/// heartbeat score itself is overwritten on every renewal and so can't double as a join time.
/// </summary>
/// <remarks>
/// The heartbeat TTL comes from <see cref="RedisWorkerOptions.InstanceHeartbeatTtl"/> rather than
/// the engine package's <c>LeaderElectionConfig</c>, so this package depends only on the shared
/// contracts and never on the engine itself.
/// </remarks>
public sealed class RedisInstanceRegistry : IInstanceRegistry
{
    private readonly IConnectionMultiplexer connectionMultiplexer;
    private readonly RedisWorkerOptions options;
    private readonly TimeSpan heartbeatTtl;

    public RedisInstanceRegistry(
        IConnectionMultiplexer connectionMultiplexer,
        IOptions<RedisWorkerOptions> options,
        IInstanceIdentityProvider instanceIdentityProvider)
    {
        this.connectionMultiplexer = connectionMultiplexer;
        this.options = options.Value;
        this.heartbeatTtl = this.options.InstanceHeartbeatTtl;
        this.InstanceId = instanceIdentityProvider.InstanceId;
    }

    public string InstanceId { get; }

    public bool IsDraining { get; private set; }

    private RedisKey heartbeatsKey => $"{this.options.KeyPrefix}:instances:heartbeats";

    private RedisKey drainingKey => $"{this.options.KeyPrefix}:instances:draining";

    private RedisKey joinedAtKey => $"{this.options.KeyPrefix}:instances:joined-at";

    public async Task HeartbeatAsync(CancellationToken ct)
    {
        var db = this.connectionMultiplexer.GetDatabase();

        // The HashSetAsync's NX means only the first heartbeat for this instance id ever sets its
        // join time - every renewal after that is a no-op there, unlike the heartbeat score below.
        // It's stored as an integer millisecond timestamp (not the sorted set's double score) so
        // it reads back as a plain long, with no double-formatting round-trip to worry about.
        await Task.WhenAll(
            db.SortedSetAddAsync(this.heartbeatsKey, this.InstanceId, nowUnixMs()),
            db.HashSetAsync(
                this.joinedAtKey,
                this.InstanceId,
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                When.NotExists)).WaitAsync(ct);
    }

    public async Task BeginDrainAsync(CancellationToken ct)
    {
        this.IsDraining = true;

        var db = this.connectionMultiplexer.GetDatabase();
        await db.SetAddAsync(this.drainingKey, this.InstanceId).WaitAsync(ct);
    }

    public async Task<IReadOnlyCollection<ActiveInstance>> GetActiveInstancesAsync(CancellationToken ct)
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

        var activeIds = activeTask.Result
            .Select(x => (string)x!)
            .Where(id => !draining.Contains(id))
            .ToArray();

        if (activeIds.Length == 0)
        {
            return Array.Empty<ActiveInstance>();
        }

        var joinedAtValues = await db.HashGetAsync(this.joinedAtKey, activeIds.Select(id => (RedisValue)id).ToArray()).WaitAsync(ct);

        return activeIds
            .Select((id, index) => new ActiveInstance
            {
                InstanceId = id,

                // Defensive fallback only: every id here comes from the heartbeat set, and
                // HeartbeatAsync always writes joined-at alongside the heartbeat itself, so this
                // should never actually be missing.
                JoinedAtUtc = joinedAtValues[index].IsNull
                    ? DateTimeOffset.UtcNow
                    : DateTimeOffset.FromUnixTimeMilliseconds((long)joinedAtValues[index]),
            })
            .ToArray();
    }

    public async Task RemoveCurrentAsync(CancellationToken ct)
    {
        var db = this.connectionMultiplexer.GetDatabase();
        await Task.WhenAll(
            db.SortedSetRemoveAsync(this.heartbeatsKey, this.InstanceId),
            db.SetRemoveAsync(this.drainingKey, this.InstanceId),
            db.HashDeleteAsync(this.joinedAtKey, this.InstanceId)).WaitAsync(ct);
    }

    private static double nowUnixMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
