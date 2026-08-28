using FluentAssertions;

namespace MultiInstanceWorker.Tests;

internal class PrimaryNodeWorkloadAssignerTests
{
    private static readonly DateTimeOffset Earlier = new (2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly DateTimeOffset Later = Earlier.AddMinutes(5);

    [Test]
    public void CurrentInstanceJoinedEarliestCase_ShouldAssignEveryWorkload()
    {
        var workloads = new[] { new Workload("beta"), new Workload("alpha") };
        var activeInstances = new[]
        {
            new ActiveInstance { InstanceId = "instance-a", JoinedAtUtc = Earlier },
            new ActiveInstance { InstanceId = "instance-b", JoinedAtUtc = Later },
        };

        var result = new PrimaryNodeWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances,
            currentInstanceId: "instance-a");

        result.Select(x => x.Key).Should().Equal("alpha", "beta");
    }

    [Test]
    public void CurrentInstanceDidNotJoinEarliestCase_ShouldAssignNoWorkloads()
    {
        var workloads = new[] { new Workload("alpha") };
        var activeInstances = new[]
        {
            new ActiveInstance { InstanceId = "instance-a", JoinedAtUtc = Earlier },
            new ActiveInstance { InstanceId = "instance-b", JoinedAtUtc = Later },
        };

        var result = new PrimaryNodeWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances,
            currentInstanceId: "instance-b");

        result.Should().BeEmpty();
    }

    [Test]
    public void NoActiveInstancesCase_ShouldAssignNoWorkloads()
    {
        var result = new PrimaryNodeWorkloadAssigner().GetAssignedWorkloads(
            new[] { new Workload("alpha") },
            workload => workload.Key,
            Array.Empty<ActiveInstance>(),
            currentInstanceId: "instance-a");

        result.Should().BeEmpty();
    }

    [Test]
    public void PrimaryFailsOverCase_ShouldReassignEveryWorkloadToNextEarliestJoinedInstance()
    {
        var workloads = new[] { new Workload("alpha") };

        // "instance-a" was primary (joined earliest) and has since dropped out of the active set
        // (e.g. it crashed). Of the survivors, "instance-b" joined before "instance-c".
        var activeInstancesAfterFailover = new[]
        {
            new ActiveInstance { InstanceId = "instance-c", JoinedAtUtc = Later },
            new ActiveInstance { InstanceId = "instance-b", JoinedAtUtc = Earlier },
        };

        var result = new PrimaryNodeWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstancesAfterFailover,
            currentInstanceId: "instance-b");

        result.Select(x => x.Key).Should().Equal("alpha");
    }

    [Test]
    public void NewInstanceJoinsWithAnAlphabeticallyEarlierIdCase_ShouldNotStealPrimaryFromTheIncumbent()
    {
        // The whole point of sorting by join time instead of instance id: "instance-aaa" joins
        // after the incumbent and would sort first alphabetically, but must not take over from
        // "instance-z" merely because its id happens to be earlier in the alphabet.
        var workloads = new[] { new Workload("alpha") };
        var activeInstances = new[]
        {
            new ActiveInstance { InstanceId = "instance-z", JoinedAtUtc = Earlier },
            new ActiveInstance { InstanceId = "instance-aaa", JoinedAtUtc = Later },
        };

        var result = new PrimaryNodeWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances,
            currentInstanceId: "instance-z");

        result.Select(x => x.Key).Should().Equal("alpha");
    }

    [Test]
    public void TiedJoinTimeCase_ShouldBreakTieByInstanceIdOrdinal()
    {
        var workloads = new[] { new Workload("alpha") };
        var activeInstances = new[]
        {
            new ActiveInstance { InstanceId = "instance-b", JoinedAtUtc = Earlier },
            new ActiveInstance { InstanceId = "instance-a", JoinedAtUtc = Earlier },
        };

        var result = new PrimaryNodeWorkloadAssigner().GetAssignedWorkloads(
            workloads,
            workload => workload.Key,
            activeInstances,
            currentInstanceId: "instance-a");

        result.Select(x => x.Key).Should().Equal("alpha");
    }

    private sealed class Workload(string key)
    {
        public string Key { get; } = key;
    }
}
