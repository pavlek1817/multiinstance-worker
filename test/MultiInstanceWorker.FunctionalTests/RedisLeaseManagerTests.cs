using FluentAssertions;
using Microsoft.Extensions.Options;
using MultiInstanceWorker.Redis;
using StackExchange.Redis;

namespace MultiInstanceWorker.FunctionalTests;

/// <summary>
/// Exercises <see cref="RedisLeaseManager"/> directly against a real Redis instance. This is the
/// actual proof that "Redis is the locking mechanism": mutual exclusion, renewal, owner-checked
/// release, and TTL-based failover when an owner stops renewing without releasing (i.e. it
/// crashed) - all backed by real Redis commands rather than a mock.
/// </summary>
internal sealed class RedisLeaseManagerTests
{
    private const string LeaseName = "workload";

    private IConnectionMultiplexer connectionMultiplexer = null!;
    private RedisLeaseManager leaseManager = null!;

    [SetUp]
    public void SetUp()
    {
        this.connectionMultiplexer = ConnectionMultiplexer.Connect(RedisTestFixture.ConnectionString);
        this.leaseManager = new RedisLeaseManager(
            this.connectionMultiplexer,
            Options.Create(new RedisWorkerOptions { KeyPrefix = $"test-{Guid.NewGuid():N}" }));
    }

    [TearDown]
    public void TearDown() => this.connectionMultiplexer.Dispose();

    [Test]
    public async Task TryAcquireOrRenewAsync_WhenUnowned_GrantsTheLeaseToTheCaller()
    {
        var acquired = await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-a", TimeSpan.FromSeconds(5), CancellationToken.None);

        acquired.Should().BeTrue();
    }

    [Test]
    public async Task TryAcquireOrRenewAsync_WhenOwnedByAnotherInstance_IsRefused()
    {
        await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-a", TimeSpan.FromSeconds(5), CancellationToken.None);

        var acquiredByB = await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-b", TimeSpan.FromSeconds(5), CancellationToken.None);

        acquiredByB.Should().BeFalse();
    }

    [Test]
    public async Task TryAcquireOrRenewAsync_WhenCalledAgainByTheOwner_RenewsInsteadOfFailing()
    {
        var ttl = TimeSpan.FromMilliseconds(300);
        await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-a", ttl, CancellationToken.None);

        // Keep renewing for longer than the original TTL alone would have survived.
        for (var i = 0; i < 5; i++)
        {
            await Task.Delay(150);
            var renewed = await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-a", ttl, CancellationToken.None);
            renewed.Should().BeTrue();
        }

        // Since instance-a kept renewing, instance-b must still be refused.
        var acquiredByB = await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-b", ttl, CancellationToken.None);
        acquiredByB.Should().BeFalse();
    }

    [Test]
    public async Task TryAcquireOrRenewAsync_AfterOwnerStopsRenewingAndTtlExpires_AllowsFailoverToAnotherInstance()
    {
        // instance-a "crashes": it acquires the lease and then never renews or releases it again.
        await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-a", TimeSpan.FromMilliseconds(300), CancellationToken.None);

        await Task.Delay(TimeSpan.FromMilliseconds(600));

        var acquiredByB = await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-b", TimeSpan.FromSeconds(5), CancellationToken.None);

        acquiredByB.Should().BeTrue("Redis should expire instance-a's key once its TTL elapses with no renewal");
    }

    [Test]
    public async Task ReleaseIfOwnedAsync_WhenCallerIsTheOwner_RemovesTheLeaseImmediately()
    {
        await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-a", TimeSpan.FromSeconds(30), CancellationToken.None);

        await this.leaseManager.ReleaseIfOwnedAsync(LeaseName, "instance-a", CancellationToken.None);

        var acquiredByB = await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-b", TimeSpan.FromSeconds(5), CancellationToken.None);
        acquiredByB.Should().BeTrue();
    }

    [Test]
    public async Task ReleaseIfOwnedAsync_WhenCallerIsNotTheOwner_LeavesTheLeaseInPlace()
    {
        await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-a", TimeSpan.FromSeconds(30), CancellationToken.None);

        // instance-b never owned the lease - releasing must be a no-op, not steal it from instance-a.
        await this.leaseManager.ReleaseIfOwnedAsync(LeaseName, "instance-b", CancellationToken.None);

        var acquiredByB = await this.leaseManager.TryAcquireOrRenewAsync(LeaseName, "instance-b", TimeSpan.FromSeconds(5), CancellationToken.None);
        acquiredByB.Should().BeFalse();
    }
}
