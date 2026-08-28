using FluentAssertions;

namespace MultiInstanceWorker.Tests;

internal class BalancedNamedWorkloadAssignerTests
{
    [Test]
    public void FourWorkloadsAndTwoInstancesCase_ShouldAssignEvenSlice()
    {
        var workloads = new[]
        {
            new Workload("alpha"),
            new Workload("beta"),
            new Workload("gamma"),
            new Workload("delta"),
        };

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances("instance-b", "instance-a"),
            currentInstanceId: "instance-a");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    [Test]
    public void RemainderCase_ShouldAssignExtraWorkloadToEarlierSortedInstance()
    {
        var workloads = new[]
        {
            new Workload("alpha"),
            new Workload("beta"),
            new Workload("gamma"),
        };

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances("instance-a", "instance-b"),
            currentInstanceId: "instance-a");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    [Test]
    public void MissingInstanceCase_ShouldAssignNoWorkloads()
    {
        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            new[] { new Workload("alpha") },
            workload => workload.Key,
            activeInstances("instance-a"),
            currentInstanceId: "instance-b");

        result.Should().BeEmpty();
    }

    // Join time doesn't affect BalancedNamedWorkloadAssigner (only PrimaryNodeWorkloadAssigner
    // cares about it), so every instance here gets the same arbitrary, fixed value.
    private static ActiveInstance[] activeInstances(params string[] ids)
        => ids.Select(id => new ActiveInstance { InstanceId = id, JoinedAtUtc = DateTimeOffset.UnixEpoch }).ToArray();

    private sealed class Workload(string key)
    {
        public string Key { get; } = key;
    }
}
