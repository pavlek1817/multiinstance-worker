using FluentAssertions;
using Microsoft.Extensions.Options;
using MultiInstanceWorker;
using MultiInstanceWorker.Sample.Api.Redis;
using StackExchange.Redis;

namespace MultiInstanceWorker.FunctionalTests;

/// <summary>Exercises <see cref="RedisInstanceRegistry"/> directly against a real Redis instance.</summary>
internal sealed class RedisInstanceRegistryTests
{
    private IConnectionMultiplexer connectionMultiplexer = null!;
    private RedisOptions options = null!;

    [SetUp]
    public void SetUp()
    {
        this.connectionMultiplexer = ConnectionMultiplexer.Connect(RedisTestFixture.ConnectionString);
        this.options = new RedisOptions { KeyPrefix = $"test-{Guid.NewGuid():N}" };
    }

    [TearDown]
    public void TearDown() => this.connectionMultiplexer.Dispose();

    [Test]
    public async Task GetActiveInstanceIdsAsync_IncludesEveryInstanceThatHasHeartbeatedRecently()
    {
        var registryA = this.buildRegistry("instance-a", TimeSpan.FromSeconds(30));
        var registryB = this.buildRegistry("instance-b", TimeSpan.FromSeconds(30));

        await registryA.HeartbeatAsync(CancellationToken.None);
        await registryB.HeartbeatAsync(CancellationToken.None);

        var active = await registryA.GetActiveInstanceIdsAsync(CancellationToken.None);

        active.Should().BeEquivalentTo(new[] { "instance-a", "instance-b" });
    }

    [Test]
    public async Task GetActiveInstanceIdsAsync_ExcludesAnInstanceThatHasNotHeartbeatedWithinTheTtl()
    {
        // The staleness window is the TTL of whichever registry does the querying, not a
        // per-instance value - it's read from the shared heartbeat set, not from each member.
        var heartbeatTtl = TimeSpan.FromMilliseconds(300);
        var registryA = this.buildRegistry("instance-a", heartbeatTtl);
        var registryB = this.buildRegistry("instance-b", heartbeatTtl);

        await registryA.HeartbeatAsync(CancellationToken.None);
        await registryB.HeartbeatAsync(CancellationToken.None);

        // instance-a stops heartbeating (e.g. it crashed); instance-b keeps renewing.
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        await registryB.HeartbeatAsync(CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        var active = await registryB.GetActiveInstanceIdsAsync(CancellationToken.None);

        active.Should().BeEquivalentTo(new[] { "instance-b" });
    }

    [Test]
    public async Task GetActiveInstanceIdsAsync_ExcludesADrainingInstanceEvenThoughItIsStillHeartbeating()
    {
        var registryA = this.buildRegistry("instance-a", TimeSpan.FromSeconds(30));
        var registryB = this.buildRegistry("instance-b", TimeSpan.FromSeconds(30));

        await registryA.HeartbeatAsync(CancellationToken.None);
        await registryB.HeartbeatAsync(CancellationToken.None);
        await registryA.BeginDrainAsync(CancellationToken.None);

        var active = await registryB.GetActiveInstanceIdsAsync(CancellationToken.None);

        active.Should().BeEquivalentTo(new[] { "instance-b" });
    }

    [Test]
    public async Task RemoveCurrentAsync_RemovesTheInstanceEvenIfItWasDraining()
    {
        var registryA = this.buildRegistry("instance-a", TimeSpan.FromSeconds(30));
        var registryB = this.buildRegistry("instance-b", TimeSpan.FromSeconds(30));

        await registryA.HeartbeatAsync(CancellationToken.None);
        await registryB.HeartbeatAsync(CancellationToken.None);
        await registryA.BeginDrainAsync(CancellationToken.None);
        await registryA.RemoveCurrentAsync(CancellationToken.None);

        var active = await registryB.GetActiveInstanceIdsAsync(CancellationToken.None);

        active.Should().BeEquivalentTo(new[] { "instance-b" });
    }

    private RedisInstanceRegistry buildRegistry(string instanceId, TimeSpan heartbeatTtl) => new (
        this.connectionMultiplexer,
        Options.Create(this.options),
        new FixedInstanceIdentityProvider(instanceId),
        heartbeatTtl);

    private sealed class FixedInstanceIdentityProvider(string instanceId)
        : IInstanceIdentityProvider
    {
        public string InstanceId { get; } = instanceId;
    }
}
