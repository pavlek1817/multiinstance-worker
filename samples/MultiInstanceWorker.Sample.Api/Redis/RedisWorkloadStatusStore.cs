using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MultiInstanceWorker.Sample.Api.Redis;

/// <summary>
/// A Redis-backed <see cref="IWorkloadStatusStore"/>. Each workload becomes a single string key
/// packing status, expiry, and owner into one value, with a Redis key expiry (<c>PX</c>) as the
/// record's TTL - so an unrenewed status (e.g. this instance crashed mid-drain) disappears from
/// Redis on its own, the same way <see cref="RedisLeaseManager"/>'s leases do.
/// </summary>
/// <remarks>
/// Same rationale as <see cref="RedisLeaseManager"/> and <see cref="RedisInstanceRegistry"/>: a
/// real, StackExchange.Redis-backed adapter that lives in this sample app, not in the core package.
/// </remarks>
public sealed class RedisWorkloadStatusStore(IConnectionMultiplexer connectionMultiplexer, IOptions<RedisOptions> options)
    : IWorkloadStatusStore
{
    private readonly RedisOptions options = options.Value;

    public async Task SetStatusAsync(string workloadKey, WorkloadStatus status, string ownerInstanceId, TimeSpan ttl, CancellationToken ct)
    {
        var db = connectionMultiplexer.GetDatabase();
        var expiresAtUnixMs = DateTimeOffset.UtcNow.Add(ttl).ToUnixTimeMilliseconds();

        // Packed as "{status}|{expiresAtUnixMs}|{ownerInstanceId}", owner last so it can safely
        // contain '|' itself - only the first two fields are ever split off.
        var value = $"{status}|{expiresAtUnixMs}|{ownerInstanceId}";

        await db.StringSetAsync(this.buildStatusKey(workloadKey), value, ttl).WaitAsync(ct);
    }

    public async Task<IReadOnlyDictionary<string, WorkloadStatusRecord>> GetStatusesAsync(IEnumerable<string> workloadKeys, CancellationToken ct)
    {
        var keys = workloadKeys.ToArray();
        if (keys.Length == 0)
        {
            return new Dictionary<string, WorkloadStatusRecord>();
        }

        var db = connectionMultiplexer.GetDatabase();
        var redisKeys = keys.Select(key => (RedisKey)this.buildStatusKey(key)).ToArray();
        var values = await db.StringGetAsync(redisKeys).WaitAsync(ct);

        var result = new Dictionary<string, WorkloadStatusRecord>(keys.Length, StringComparer.Ordinal);
        var now = DateTimeOffset.UtcNow;

        for (var i = 0; i < keys.Length; i++)
        {
            if (values[i].IsNull)
            {
                continue;
            }

            var parts = ((string)values[i]!).Split('|', 3);
            if (parts.Length != 3
                || !Enum.TryParse<WorkloadStatus>(parts[0], out var status)
                || !long.TryParse(parts[1], out var expiresAtUnixMs))
            {
                continue;
            }

            var expiresAtUtc = DateTimeOffset.FromUnixTimeMilliseconds(expiresAtUnixMs);

            // Defensive: Redis's own PX expiry is what actually removes a stale key, but honoring
            // the embedded expiry too keeps this store's contract - never return an expired record -
            // true even under clock skew between the writer and whatever reads it back.
            if (expiresAtUtc <= now)
            {
                continue;
            }

            result[keys[i]] = new WorkloadStatusRecord
            {
                WorkloadKey = keys[i],
                Status = status,
                OwnerInstanceId = parts[2],
                ExpiresAtUtc = expiresAtUtc,
            };
        }

        return result;
    }

    private string buildStatusKey(string workloadKey) => $"{this.options.KeyPrefix}:workload-status:{workloadKey}";
}
