using FluentAssertions;

namespace MultiInstanceWorker.Tests;

internal class BalancedNamedWorkloadAssignerTests
{
    [Test]
    public void FourWorkloadsAndTwoInstancesCase_ShouldAssignEvenSliceOrderedByJoinTime()
    {
        var workloads = new[]
        {
            new Workload("alpha"),
            new Workload("beta"),
            new Workload("gamma"),
            new Workload("delta"),
        };

        // instance-b joined first, despite sorting after instance-a alphabetically, so it should
        // still get the first slice.
        var instances = new[]
        {
            new ActiveInstance { InstanceId = "instance-b", JoinedAtUtc = DateTimeOffset.UnixEpoch },
            new ActiveInstance { InstanceId = "instance-a", JoinedAtUtc = DateTimeOffset.UnixEpoch.AddMinutes(1) },
        };

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            instances,
            currentInstanceId: "instance-b");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    [Test]
    public void RemainderCase_ShouldAssignExtraWorkloadToEarlierJoinedInstance()
    {
        var workloads = new[]
        {
            new Workload("alpha"),
            new Workload("beta"),
            new Workload("gamma"),
        };

        var instances = new[]
        {
            new ActiveInstance { InstanceId = "instance-a", JoinedAtUtc = DateTimeOffset.UnixEpoch },
            new ActiveInstance { InstanceId = "instance-b", JoinedAtUtc = DateTimeOffset.UnixEpoch.AddMinutes(1) },
        };

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            instances,
            currentInstanceId: "instance-a");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    [Test]
    public void MissingInstanceCase_ShouldAssignNoWorkloads()
    {
        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            new[] { new Workload("alpha") },
            workload => workload.Key,
            new[] { new ActiveInstance { InstanceId = "instance-a", JoinedAtUtc = DateTimeOffset.UnixEpoch } },
            currentInstanceId: "instance-b");

        result.Should().BeEmpty();
    }

    [Test]
    public void InstancesOutOfIdOrder_ShouldBeSlicedByJoinTimeNotInstanceId()
    {
        var workloads = new[]
        {
            new Workload("alpha"),
            new Workload("beta"),
            new Workload("gamma"),
            new Workload("delta"),
        };

        // instance-z joined first despite sorting after instance-a alphabetically, so it should
        // still get the first slice.
        var instances = new[]
        {
            new ActiveInstance { InstanceId = "instance-z", JoinedAtUtc = DateTimeOffset.UnixEpoch },
            new ActiveInstance { InstanceId = "instance-a", JoinedAtUtc = DateTimeOffset.UnixEpoch.AddMinutes(1) },
        };

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            instances,
            currentInstanceId: "instance-z");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    [Test]
    public void SameJoinTime_ShouldBreakTiesByInstanceId()
    {
        var workloads = new[]
        {
            new Workload("alpha"),
            new Workload("beta"),
            new Workload("gamma"),
            new Workload("delta"),
        };

        // Every instance here joins at the same fixed instant, so instance id (the tie-breaker)
        // is what actually drives ordering.
        var instances = activeInstances("instance-b", "instance-a");

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            instances,
            currentInstanceId: "instance-a");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    private static ActiveInstance[] activeInstances(params string[] ids)
        => ids.Select(id => new ActiveInstance { InstanceId = id, JoinedAtUtc = DateTimeOffset.UnixEpoch }).ToArray();

    private sealed class Workload(string key)
    {
        public string Key { get; } = key;
    }
}
