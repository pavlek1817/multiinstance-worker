using FluentAssertions;

namespace MultiInstanceWorker.Tests;

internal class LeaderElectionConfigTests
{
    [Test]
    public void ValidCase_ShouldNotThrow()
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlMs = 30000,
            LeaseRenewIntervalMs = 10000,
            InstanceHeartbeatTtlMs = 30000,
            InstanceHeartbeatIntervalMs = 10000,
            DrainTimeoutMs = 30000,
        };

        var act = () => config.Validate();

        act.Should().NotThrow();
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void LeaseTtlMsLessThanOrEqualZeroCase_ShouldThrow(int invalidLeaseTtlMs)
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlMs = invalidLeaseTtlMs,
            LeaseRenewIntervalMs = 10000,
            InstanceHeartbeatTtlMs = 30000,
            InstanceHeartbeatIntervalMs = 10000,
            DrainTimeoutMs = 30000,
        };

        var act = () => config.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void LeaseRenewIntervalMsGreaterThanOrEqualLeaseTtlCase_ShouldThrow()
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlMs = 30000,
            LeaseRenewIntervalMs = 30000,
            InstanceHeartbeatTtlMs = 30000,
            InstanceHeartbeatIntervalMs = 10000,
            DrainTimeoutMs = 30000,
        };

        var act = () => config.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void InstanceHeartbeatIntervalMsGreaterThanOrEqualInstanceHeartbeatTtlCase_ShouldThrow()
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlMs = 30000,
            LeaseRenewIntervalMs = 10000,
            InstanceHeartbeatTtlMs = 30000,
            InstanceHeartbeatIntervalMs = 30000,
            DrainTimeoutMs = 30000,
        };

        var act = () => config.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void DrainTimeoutMsLessThanOrEqualZeroCase_ShouldThrow(int invalidDrainTimeoutMs)
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlMs = 30000,
            LeaseRenewIntervalMs = 10000,
            InstanceHeartbeatTtlMs = 30000,
            InstanceHeartbeatIntervalMs = 10000,
            DrainTimeoutMs = invalidDrainTimeoutMs,
        };

        var act = () => config.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
