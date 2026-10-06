using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MultiInstanceWorker.Redis;

/// <summary>
/// A Redis-backed <see cref="IWorkloadStatusStore"/>. Each workload becomes a Redis hash (status,
/// owner, expiry, and a write-once creation time) with a key expiry (<c>PX</c>) as the record's
/// TTL - so an unrenewed status (e.g. this instance crashed mid-drain) disappears from Redis on its
/// own, the same way <see cref="RedisLeaseManager"/>'s leases do. A secondary sorted-set index
/// (member = workload key, score = expiry) is what lets <see cref="GetAllAsync"/> enumerate every
/// live workload without already knowing their keys, mirroring how <see cref="RedisInstanceRegistry"/>
/// indexes live instances rather than scanning key patterns.
/// </summary>
public sealed class RedisWorkloadStatusStore(IConnectionMultiplexer connectionMultiplexer, IOptions<RedisWorkerOptions> options)
    : IWorkloadStatusStore
{
    // KEYS[1] = status hash key, KEYS[2] = index key.
    // ARGV[1] = status, ARGV[2] = owner id, ARGV[3] = expiresAtUnixMs (also the index score),
    // ARGV[4] = nowUnixMs, ARGV[5] = ttlMs, ARGV[6] = workload key.
    // HSETNX only ever sets createdAtUtc the first time this hash is written after not existing
    // (fresh key, or one that fully expired away since) - every later write leaves it untouched.
    private static readonly LuaScript SetStatusScript = LuaScript.Prepare(
        """
        redis.call('HSET', @statusKey, 'status', @status, 'owner', @ownerId, 'expiresAtUtc', @expiresAtUnixMs)
        redis.call('HSETNX', @statusKey, 'createdAtUtc', @nowUnixMs)
        redis.call('PEXPIRE', @statusKey, @ttlMs)
        redis.call('ZADD', @indexKey, @expiresAtUnixMs, @workloadKey)
        return 1
        """);

    private readonly RedisWorkerOptions options = options.Value;

    private RedisKey indexKey => $"{this.options.KeyPrefix}:workload-status:index";

    public async Task SetStatusAsync(string workloadKey, WorkloadStatus status, string ownerInstanceId, TimeSpan ttl, CancellationToken ct)
    {
        var db = connectionMultiplexer.GetDatabase();
        var expiresAtUnixMs = DateTimeOffset.UtcNow.Add(ttl).ToUnixTimeMilliseconds();

        await db.ScriptEvaluateAsync(
            SetStatusScript,
            new
            {
                statusKey = (RedisKey)this.buildStatusKey(workloadKey),
                indexKey = this.indexKey,
                status = status.ToString(),
                ownerId = ownerInstanceId,
                expiresAtUnixMs,
                nowUnixMs = nowUnixMs(),
                ttlMs = (long)ttl.TotalMilliseconds,
                workloadKey,
            }).WaitAsync(ct);
    }

    public async Task<IReadOnlyDictionary<string, WorkloadStatusRecord>> GetStatusesAsync(IEnumerable<string> workloadKeys, CancellationToken ct)
    {
        var keys = workloadKeys.ToArray();
        if (keys.Length == 0)
        {
            return new Dictionary<string, WorkloadStatusRecord>();
        }

        var db = connectionMultiplexer.GetDatabase();
        var records = await Task.WhenAll(keys.Select(key => this.readOneAsync(db, key, ct)));

        var result = new Dictionary<string, WorkloadStatusRecord>(keys.Length, StringComparer.Ordinal);
        for (var i = 0; i < keys.Length; i++)
        {
            if (records[i] is { } record)
            {
                result[keys[i]] = record;
            }
        }

        return result;
    }

    public async Task<IReadOnlyCollection<WorkloadStatusRecord>> GetAllAsync(CancellationToken ct)
    {
        var db = connectionMultiplexer.GetDatabase();
        var now = nowUnixMs();

        // Best-effort cleanup of index entries whose hash has already expired in Redis - mirrors
        // RedisInstanceRegistry.GetActiveInstancesAsync pruning its own heartbeat set.
        await db.SortedSetRemoveRangeByScoreAsync(this.indexKey, double.NegativeInfinity, now - 1).WaitAsync(ct);

        var liveKeys = await db.SortedSetRangeByScoreAsync(this.indexKey, now, double.PositiveInfinity).WaitAsync(ct);
        if (liveKeys.Length == 0)
        {
            return Array.Empty<WorkloadStatusRecord>();
        }

        var records = await Task.WhenAll(liveKeys.Select(key => this.readOneAsync(db, (string)key!, ct)));
        return records.Where(record => record is not null).Select(record => record!).ToArray();
    }

    private static WorkloadStatusRecord? parseRecord(string workloadKey, HashEntry[] entries)
    {
        if (entries.Length == 0)
        {
            return null;
        }

        var hash = entries.ToDictionary(e => (string)e.Name!, e => e.Value, StringComparer.Ordinal);
        if (!hash.TryGetValue("status", out var statusValue)
            || !Enum.TryParse<WorkloadStatus>((string)statusValue!, out var status)
            || !hash.TryGetValue("owner", out var ownerValue)
            || !hash.TryGetValue("expiresAtUtc", out var expiresValue)
            || !long.TryParse((string)expiresValue!, out var expiresAtUnixMs)
            || !hash.TryGetValue("createdAtUtc", out var createdValue)
            || !long.TryParse((string)createdValue!, out var createdAtUnixMs))
        {
            return null;
        }

        var expiresAtUtc = DateTimeOffset.FromUnixTimeMilliseconds(expiresAtUnixMs);

        // Defensive: Redis's own PX expiry is what actually removes a stale key, but honoring the
        // embedded expiry too keeps this store's contract - never return an expired record - true
        // even under clock skew, or for an index entry that hasn't been pruned yet.
        if (expiresAtUtc <= DateTimeOffset.UtcNow)
        {
            return null;
        }

        return new WorkloadStatusRecord
        {
            WorkloadKey = workloadKey,
            Status = status,
            OwnerInstanceId = (string)ownerValue!,
            ExpiresAtUtc = expiresAtUtc,
            CreatedAtUtc = DateTimeOffset.FromUnixTimeMilliseconds(createdAtUnixMs),
        };
    }

    private static long nowUnixMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private async Task<WorkloadStatusRecord?> readOneAsync(IDatabase db, string workloadKey, CancellationToken ct)
    {
        var entries = await db.HashGetAllAsync(this.buildStatusKey(workloadKey)).WaitAsync(ct);
        return parseRecord(workloadKey, entries);
    }

    private string buildStatusKey(string workloadKey) => $"{this.options.KeyPrefix}:workload-status:{workloadKey}";
}
