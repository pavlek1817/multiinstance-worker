using FluentAssertions;
using Microsoft.Extensions.Options;
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
    public async Task GetActiveInstancesAsync_IncludesEveryInstanceThatHasHeartbeatedRecently()
    {
        var registryA = this.buildRegistry("instance-a", TimeSpan.FromSeconds(30));
        var registryB = this.buildRegistry("instance-b", TimeSpan.FromSeconds(30));

        await registryA.HeartbeatAsync(CancellationToken.None);
        await registryB.HeartbeatAsync(CancellationToken.None);

        var active = await registryA.GetActiveInstancesAsync(CancellationToken.None);

        active.Select(x => x.InstanceId).Should().BeEquivalentTo(new[] { "instance-a", "instance-b" });
    }

    [Test]
    public async Task GetActiveInstancesAsync_ExcludesAnInstanceThatHasNotHeartbeatedWithinTheTtl()
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

        var active = await registryB.GetActiveInstancesAsync(CancellationToken.None);

        active.Select(x => x.InstanceId).Should().BeEquivalentTo(new[] { "instance-b" });
    }

    [Test]
    public async Task GetActiveInstancesAsync_ExcludesADrainingInstanceEvenThoughItIsStillHeartbeating()
    {
        var registryA = this.buildRegistry("instance-a", TimeSpan.FromSeconds(30));
        var registryB = this.buildRegistry("instance-b", TimeSpan.FromSeconds(30));

        await registryA.HeartbeatAsync(CancellationToken.None);
        await registryB.HeartbeatAsync(CancellationToken.None);
        await registryA.BeginDrainAsync(CancellationToken.None);

        var active = await registryB.GetActiveInstancesAsync(CancellationToken.None);

        active.Select(x => x.InstanceId).Should().BeEquivalentTo(new[] { "instance-b" });
    }

    [Test]
    public async Task GetActiveInstancesAsync_JoinedAtUtcStaysFixedAcrossRepeatedHeartbeats()
    {
        // JoinedAtUtc must be write-once - a live instance renewing its heartbeat every
        // renewInterval must not look like it "rejoined" on every tick, or PrimaryNodeWorkloadAssigner
        // could never converge on a stable primary.
        var registry = this.buildRegistry("instance-a", TimeSpan.FromSeconds(30));

        await registry.HeartbeatAsync(CancellationToken.None);
        var firstJoinedAt = (await registry.GetActiveInstancesAsync(CancellationToken.None))
            .Single(x => x.InstanceId == "instance-a").JoinedAtUtc;

        await Task.Delay(TimeSpan.FromMilliseconds(50));
        await registry.HeartbeatAsync(CancellationToken.None);
        var secondJoinedAt = (await registry.GetActiveInstancesAsync(CancellationToken.None))
            .Single(x => x.InstanceId == "instance-a").JoinedAtUtc;

        secondJoinedAt.Should().Be(firstJoinedAt);
    }

    [Test]
    public async Task GetActiveInstancesAsync_ReportsAnEarlierJoinedAtForAnInstanceThatHeartbeatedFirst()
    {
        var registryA = this.buildRegistry("instance-a", TimeSpan.FromSeconds(30));
        var registryB = this.buildRegistry("instance-b", TimeSpan.FromSeconds(30));

        await registryA.HeartbeatAsync(CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(50));
        await registryB.HeartbeatAsync(CancellationToken.None);

        var active = await registryA.GetActiveInstancesAsync(CancellationToken.None);
        var joinedAtA = active.Single(x => x.InstanceId == "instance-a").JoinedAtUtc;
        var joinedAtB = active.Single(x => x.InstanceId == "instance-b").JoinedAtUtc;

        joinedAtA.Should().BeBefore(joinedAtB);
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

        var active = await registryB.GetActiveInstancesAsync(CancellationToken.None);

        active.Select(x => x.InstanceId).Should().BeEquivalentTo(new[] { "instance-b" });
    }

    private RedisInstanceRegistry buildRegistry(string instanceId, TimeSpan heartbeatTtl) => new (
        this.connectionMultiplexer,
        Options.Create(this.options),
        new FixedInstanceIdentityProvider(instanceId),
        Options.Create(new LeaderElectionConfig { InstanceHeartbeatTtlMs = (int)heartbeatTtl.TotalMilliseconds }));

    private sealed class FixedInstanceIdentityProvider(string instanceId)
        : IInstanceIdentityProvider
    {
        public string InstanceId { get; } = instanceId;
    }
}
