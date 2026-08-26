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

        var activeInstances = new[] { "instance-b", "instance-a" };

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances,
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

        var activeInstances = new[] { "instance-a", "instance-b" };

        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances,
            currentInstanceId: "instance-a");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    [Test]
    public void MissingInstanceCase_ShouldAssignNoWorkloads()
    {
        var result = new BalancedNamedWorkloadAssigner().GetAssignedWorkloads(
            new[] { new Workload("alpha") },
            workload => workload.Key,
            new[] { "instance-a" },
            currentInstanceId: "instance-b");

        result.Should().BeEmpty();
    }

    private sealed class Workload(string key)
    {
        public string Key { get; } = key;
    }
}
