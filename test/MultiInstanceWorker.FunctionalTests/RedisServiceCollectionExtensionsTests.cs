using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MultiInstanceWorker.Redis;
using StackExchange.Redis;

namespace MultiInstanceWorker.FunctionalTests;

/// <summary>
/// Exercises <see cref="RedisServiceCollectionExtensions.AddMultiInstanceWorkerRedis(IServiceCollection, Action{RedisWorkerOptions})"/>
/// end to end: what it registers, where its Redis connection comes from, and that the adapters it
/// hands out really do reach the Redis instance they were pointed at.
/// </summary>
internal sealed class RedisServiceCollectionExtensionsTests
{
    private static readonly TimeSpan HeartbeatTtl = TimeSpan.FromSeconds(30);

    [Test]
    public async Task AddMultiInstanceWorkerRedis_WithAConnectionString_RegistersWorkingAdaptersOnItsOwnConnection()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IInstanceIdentityProvider>(new FixedInstanceIdentityProvider("instance-a"));
        services.AddMultiInstanceWorkerRedis(options =>
        {
            options.ConnectionString = RedisTestFixture.ConnectionString;
            options.KeyPrefix = newKeyPrefix();
            options.InstanceHeartbeatTtl = HeartbeatTtl;
        });

        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<ILeaseManager>().Should().BeOfType<RedisLeaseManager>();
        provider.GetRequiredService<IWorkloadStatusStore>().Should().BeOfType<RedisWorkloadStatusStore>();
        var registry = provider.GetRequiredService<IInstanceRegistry>();
        registry.Should().BeOfType<RedisInstanceRegistry>();

        await registry.HeartbeatAsync(CancellationToken.None);
        var active = await registry.GetActiveInstancesAsync(CancellationToken.None);
        active.Select(x => x.InstanceId).Should().BeEquivalentTo(new[] { "instance-a" });

        // Its own connection stays private - it must not show up as the application's multiplexer.
        provider.GetService<IConnectionMultiplexer>().Should().BeNull();
    }

    [Test]
    public async Task AddMultiInstanceWorkerRedis_WithoutAConnectionString_UsesTheApplicationsOwnMultiplexerAndLeavesItOpen()
    {
        using var applicationMultiplexer = ConnectionMultiplexer.Connect(RedisTestFixture.ConnectionString);

        var services = new ServiceCollection();
        services.AddSingleton<IConnectionMultiplexer>(applicationMultiplexer);
        services.AddMultiInstanceWorkerRedis(options =>
        {
            options.KeyPrefix = newKeyPrefix();
            options.InstanceHeartbeatTtl = HeartbeatTtl;
        });

        await using (var provider = services.BuildServiceProvider())
        {
            var leaseManager = provider.GetRequiredService<ILeaseManager>();
            var acquired = await leaseManager.TryAcquireOrRenewAsync("workload", "instance-a", TimeSpan.FromSeconds(5), CancellationToken.None);
            acquired.Should().BeTrue();
        }

        // The application owns that multiplexer, so disposing the container must not close it.
        applicationMultiplexer.IsConnected.Should().BeTrue();
    }

    [Test]
    public void AddMultiInstanceWorkerRedis_WithNeitherAConnectionStringNorAMultiplexer_FailsWithAClearMessage()
    {
        var services = new ServiceCollection();
        services.AddMultiInstanceWorkerRedis(options => options.InstanceHeartbeatTtl = HeartbeatTtl);

        using var provider = services.BuildServiceProvider();

        var resolve = () => provider.GetRequiredService<ILeaseManager>();

        resolve.Should().Throw<InvalidOperationException>().WithMessage("*ConnectionString*");
    }

    [Test]
    public void AddMultiInstanceWorkerRedis_WithoutAnInstanceHeartbeatTtl_FailsOptionsValidation()
    {
        var services = new ServiceCollection();
        services.AddMultiInstanceWorkerRedis(options => options.ConnectionString = RedisTestFixture.ConnectionString);

        using var provider = services.BuildServiceProvider();

        var resolve = () => provider.GetRequiredService<IOptions<RedisWorkerOptions>>().Value;

        resolve.Should().Throw<OptionsValidationException>().WithMessage("*InstanceHeartbeatTtl*");
    }

    [Test]
    public void AddMultiInstanceWorkerRedis_FromAConfigurationSection_BindsItAndThenAppliesTheConfigureCallback()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Redis:ConnectionString"] = "redis.example:6379",
                ["Redis:KeyPrefix"] = "from-config",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddMultiInstanceWorkerRedis(
            configuration.GetSection(RedisWorkerOptions.SectionName),
            options => options.InstanceHeartbeatTtl = HeartbeatTtl);

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<RedisWorkerOptions>>().Value;

        options.ConnectionString.Should().Be("redis.example:6379");
        options.KeyPrefix.Should().Be("from-config");
        options.InstanceHeartbeatTtl.Should().Be(HeartbeatTtl);
    }

    [Test]
    public void AddMultiInstanceWorkerRedis_LeavesAConsumersOwnAdapterRegistrationInPlace()
    {
        var consumerLeaseManager = new StubLeaseManager();

        var services = new ServiceCollection();
        services.AddSingleton<ILeaseManager>(consumerLeaseManager);
        services.AddMultiInstanceWorkerRedis(options =>
        {
            options.ConnectionString = RedisTestFixture.ConnectionString;
            options.InstanceHeartbeatTtl = HeartbeatTtl;
        });

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<ILeaseManager>().Should().BeSameAs(consumerLeaseManager);
    }

    private static string newKeyPrefix() => $"test-{Guid.NewGuid():N}";

    private sealed class FixedInstanceIdentityProvider(string instanceId)
        : IInstanceIdentityProvider
    {
        public string InstanceId { get; } = instanceId;
    }

    private sealed class StubLeaseManager : ILeaseManager
    {
        public Task<bool> TryAcquireOrRenewAsync(string leaseName, string ownerId, TimeSpan leaseTtl, CancellationToken ct) =>
            Task.FromResult(true);

        public Task ReleaseIfOwnedAsync(string leaseName, string ownerId, CancellationToken ct) => Task.CompletedTask;
    }
}
