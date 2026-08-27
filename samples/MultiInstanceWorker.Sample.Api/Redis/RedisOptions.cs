namespace MultiInstanceWorker.Sample.Api.Redis;

/// <summary>
/// Connection settings for the sample's Redis-backed <see cref="ILeaseManager"/> and
/// <see cref="IInstanceRegistry"/> implementations.
/// </summary>
public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    /// <summary>The StackExchange.Redis connection string (e.g. "localhost:6379").</summary>
    public string ConnectionString { get; init; } = "localhost:6379";

    /// <summary>Prefix applied to every key this sample writes, so it can share a Redis instance safely.</summary>
    public string KeyPrefix { get; init; } = "mi-sample";
}
