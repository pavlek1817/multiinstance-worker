using FluentAssertions;

namespace MultiInstanceWorker.Tests;

internal class LeaderElectionConfigTests
{
    [Test]
    public void ValidCase_ShouldNotThrow()
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlSeconds = 30,
            LeaseRenewIntervalMs = 10000,
            InstanceHeartbeatTtlSeconds = 30,
            InstanceHeartbeatIntervalMs = 10000,
        };

        var act = () => config.Validate();

        act.Should().NotThrow();
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void LeaseTtlSecondsLessThanOrEqualZeroCase_ShouldThrow(int invalidLeaseTtlSeconds)
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlSeconds = invalidLeaseTtlSeconds,
            LeaseRenewIntervalMs = 10000,
            InstanceHeartbeatTtlSeconds = 30,
            InstanceHeartbeatIntervalMs = 10000,
        };

        var act = () => config.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void LeaseRenewIntervalMsGreaterThanOrEqualLeaseTtlCase_ShouldThrow()
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlSeconds = 30,
            LeaseRenewIntervalMs = 30000,
            InstanceHeartbeatTtlSeconds = 30,
            InstanceHeartbeatIntervalMs = 10000,
        };

        var act = () => config.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void InstanceHeartbeatIntervalMsGreaterThanOrEqualInstanceHeartbeatTtlCase_ShouldThrow()
    {
        var config = new LeaderElectionConfig
        {
            LeaseTtlSeconds = 30,
            LeaseRenewIntervalMs = 10000,
            InstanceHeartbeatTtlSeconds = 30,
            InstanceHeartbeatIntervalMs = 30000,
        };

        var act = () => config.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}
