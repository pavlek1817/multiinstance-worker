namespace MultiInstanceWorker.Redis;

/// <summary>
/// Settings for the Redis-backed <see cref="ILeaseManager"/>, <see cref="IInstanceRegistry"/>, and
/// <see cref="IWorkloadStatusStore"/> implementations.
/// </summary>
public sealed class RedisWorkerOptions
{
    public const string SectionName = "Redis";

    /// <summary>
    /// The StackExchange.Redis connection string (e.g. "localhost:6379"). When set, the adapters
    /// use their own private connection built from it. Leave it unset to reuse the
    /// <c>IConnectionMultiplexer</c> the application already has registered.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Prefix applied to every key the adapters write, so they can share a Redis instance safely.</summary>
    public string KeyPrefix { get; set; } = "multiinstance-worker";

    /// <summary>
    /// How long an instance stays in the active set after its last heartbeat. Has no default on
    /// purpose: it must match the heartbeat TTL the workers themselves are timed against (the
    /// engine package's <c>LeaderElectionConfig.InstanceHeartbeatTtlMs</c>), and be longer than the
    /// interval they heartbeat at, so set it from that same source.
    /// </summary>
    public TimeSpan InstanceHeartbeatTtl { get; set; }
}
