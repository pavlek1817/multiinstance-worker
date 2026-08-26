using Microsoft.Extensions.Options;
using MultiInstanceWorker;
using StackExchange.Redis;

namespace MultiInstanceWorker.Sample.Api.Redis;

/// <summary>
/// A Redis-backed <see cref="ILeaseManager"/>. Every named workload becomes a single string key
/// holding the owning instance id, with a Redis key expiry (<c>PX</c>) as the lease TTL.
/// </summary>
/// <remarks>
/// This is the kind of adapter the core <c>MultiInstanceWorker</c> package deliberately does not
/// ship: it depends on StackExchange.Redis directly, which is why it lives here in the sample
/// application rather than in the reusable library. Acquire/renew and release are each a single
/// Lua script, so the read-then-write is atomic on the Redis side without a separate arbitration
/// lock, as <see cref="ILeaseManager"/>'s contract requires.
/// </remarks>
public sealed class RedisLeaseManager(IConnectionMultiplexer connectionMultiplexer, IOptions<RedisOptions> options)
    : ILeaseManager
{
    // KEYS[1] = lease key, ARGV[1] = owner id, ARGV[2] = ttl in milliseconds.
    // Acquires the lease when unowned, or renews it when this owner already holds it.
    private static readonly LuaScript AcquireOrRenewScript = LuaScript.Prepare(
        """
        local current = redis.call('GET', @leaseKey)
        if current == false or current == @ownerId then
          redis.call('SET', @leaseKey, @ownerId, 'PX', @ttlMs)
          return 1
        end
        return 0
        """);

    // KEYS[1] = lease key, ARGV[1] = owner id.
    // Deletes the lease only if the caller still owns it, so a stale caller can never
    // clobber a lease some other instance has since acquired.
    private static readonly LuaScript ReleaseIfOwnedScript = LuaScript.Prepare(
        """
        local current = redis.call('GET', @leaseKey)
        if current == @ownerId then
          redis.call('DEL', @leaseKey)
        end
        return 0
        """);

    private readonly RedisOptions options = options.Value;

    public async Task<bool> TryAcquireOrRenewAsync(string leaseName, string ownerId, TimeSpan leaseTtl, CancellationToken ct)
    {
        var db = connectionMultiplexer.GetDatabase();
        var result = await db.ScriptEvaluateAsync(
            AcquireOrRenewScript,
            new
            {
                leaseKey = (RedisKey)this.buildLeaseKey(leaseName),
                ownerId,
                ttlMs = (long)leaseTtl.TotalMilliseconds,
            }).WaitAsync(ct);

        return (long)result == 1;
    }

    public async Task ReleaseIfOwnedAsync(string leaseName, string ownerId, CancellationToken ct)
    {
        var db = connectionMultiplexer.GetDatabase();
        await db.ScriptEvaluateAsync(
            ReleaseIfOwnedScript,
            new
            {
                leaseKey = (RedisKey)this.buildLeaseKey(leaseName),
                ownerId,
            }).WaitAsync(ct);
    }

    private string buildLeaseKey(string leaseName) => $"{this.options.KeyPrefix}:lease:{leaseName}";
}
