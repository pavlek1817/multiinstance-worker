using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MultiInstanceWorker.Redis;

/// <summary>
/// <see cref="IServiceCollection"/> extensions that register the Redis-backed
/// <see cref="ILeaseManager"/>, <see cref="IInstanceRegistry"/>, and <see cref="IWorkloadStatusStore"/>,
/// so a consumer doesn't have to write those three adapters itself.
/// </summary>
/// <remarks>
/// All three are registered with <c>TryAdd</c>, so a consumer's own registration - made before or
/// after calling these - always wins. <see cref="IInstanceIdentityProvider"/> is not registered
/// here: this package has no implementation of it. The engine package's <c>AddLeasedWorker</c> /
/// <c>AddWorkloadCoordinator</c> default it; an application using this package without the engine
/// registers its own.
/// </remarks>
public static class RedisServiceCollectionExtensions
{
    /// <summary>Registers the Redis adapters, configuring <see cref="RedisWorkerOptions"/> in code.</summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configure">Sets the connection, key prefix, and instance heartbeat TTL.</param>
    public static IServiceCollection AddMultiInstanceWorkerRedis(
        this IServiceCollection services,
        Action<RedisWorkerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        return addAdapters(services);
    }

    /// <summary>
    /// Registers the Redis adapters, binding <see cref="RedisWorkerOptions"/> from a configuration
    /// section (e.g. <c>configuration.GetSection(RedisWorkerOptions.SectionName)</c>).
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="configuration">The section holding <see cref="RedisWorkerOptions"/>' properties.</param>
    /// <param name="configure">
    /// Optional: adjusts the bound options afterwards - typically to set
    /// <see cref="RedisWorkerOptions.InstanceHeartbeatTtl"/> from wherever the application keeps its
    /// worker timing, rather than repeating it in the Redis section.
    /// </param>
    public static IServiceCollection AddMultiInstanceWorkerRedis(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<RedisWorkerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<RedisWorkerOptions>(configuration);
        if (configure is not null)
        {
            services.Configure(configure);
        }

        return addAdapters(services);
    }

    private static IServiceCollection addAdapters(IServiceCollection services)
    {
        services
            .AddOptions<RedisWorkerOptions>()
            .Validate(
                options => !string.IsNullOrWhiteSpace(options.KeyPrefix),
                $"{nameof(RedisWorkerOptions)}.{nameof(RedisWorkerOptions.KeyPrefix)} must not be empty.")
            .Validate(
                options => options.InstanceHeartbeatTtl > TimeSpan.Zero,
                $"{nameof(RedisWorkerOptions)}.{nameof(RedisWorkerOptions.InstanceHeartbeatTtl)} must be set to a positive duration.")
            .ValidateOnStart();

        services.TryAddSingleton<RedisConnectionProvider>();

        // Factories rather than plain TryAddSingleton<TService, TImplementation>(): the adapters take
        // an IConnectionMultiplexer, and it has to be this package's own (see RedisConnectionProvider),
        // not necessarily the one the application registered.
        services.TryAddSingleton<ILeaseManager>(sp => new RedisLeaseManager(
            sp.GetRequiredService<RedisConnectionProvider>().Connection,
            sp.GetRequiredService<IOptions<RedisWorkerOptions>>()));

        services.TryAddSingleton<IInstanceRegistry>(sp => new RedisInstanceRegistry(
            sp.GetRequiredService<RedisConnectionProvider>().Connection,
            sp.GetRequiredService<IOptions<RedisWorkerOptions>>(),
            sp.GetRequiredService<IInstanceIdentityProvider>()));

        services.TryAddSingleton<IWorkloadStatusStore>(sp => new RedisWorkloadStatusStore(
            sp.GetRequiredService<RedisConnectionProvider>().Connection,
            sp.GetRequiredService<IOptions<RedisWorkerOptions>>()));

        return services;
    }
}
