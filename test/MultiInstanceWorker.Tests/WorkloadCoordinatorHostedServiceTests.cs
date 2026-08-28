using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace MultiInstanceWorker.Tests;

internal class WorkloadCoordinatorHostedServiceTests
{
    private static readonly TimeSpan LeaseTtl = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan RenewInterval = TimeSpan.FromMilliseconds(10);

    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromMilliseconds(20);

    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(2);

    private Mock<ILeaseManager> mockedLeaseManager = null!;

    private FakeInstanceRegistry instanceRegistry = null!;

    [SetUp]
    public void SetUp()
    {
        this.mockedLeaseManager = new Mock<ILeaseManager>();
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync(It.IsAny<string>(), "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        this.instanceRegistry = new FakeInstanceRegistry("instance-a") { ActiveInstanceIds = new[] { "instance-a" } };
    }

    [Test]
    public async Task BalancedAssignment_ShouldStartOneRunnerPerAssignedWorkload()
    {
        var startedA = new TaskCompletionSource();
        var startedB = new TaskCompletionSource();
        var workloads = new List<TestWorkload> { new ("workload-a"), new ("workload-b") };

        var coordinator = this.buildCoordinator(
            workloads,
            (workload, ct) =>
            {
                (workload.Key == "workload-a" ? startedA : startedB).TrySetResult();
                return Task.Delay(Timeout.Infinite, ct);
            });

        await coordinator.StartAsync(CancellationToken.None);
        try
        {
            await Task.WhenAll(
                startedA.Task.WaitAsync(TimeSpan.FromSeconds(5)),
                startedB.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await coordinator.StopAsync(stopCts.Token);
        }
    }

    [Test]
    public async Task WorkloadReassignedAway_WhileInstanceNotDraining_ShouldStopRunnerImmediately()
    {
        var startedA = new TaskCompletionSource();
        var startedB = new TaskCompletionSource();
        var bCancelled = new TaskCompletionSource();
        var workloads = new List<TestWorkload> { new ("workload-a"), new ("workload-b") };

        var coordinator = this.buildCoordinator(
            workloads,
            async (workload, ct) =>
            {
                (workload.Key == "workload-a" ? startedA : startedB).TrySetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException) when (workload.Key == "workload-b")
                {
                    bCancelled.TrySetResult();
                    throw;
                }
            });

        await coordinator.StartAsync(CancellationToken.None);
        try
        {
            // Both workloads land on the only active instance to start with.
            await Task.WhenAll(
                startedA.Task.WaitAsync(TimeSpan.FromSeconds(5)),
                startedB.Task.WaitAsync(TimeSpan.FromSeconds(5)));

            // A second instance joins: with two workloads split across two instances,
            // "workload-b" now belongs to "instance-b" and drops out of this instance's assignment.
            this.instanceRegistry.ActiveInstanceIds = new[] { "instance-a", "instance-b" };

            await bCancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));

            this.instanceRegistry.IsDraining.Should().BeFalse(
                "a plain rebalance away from a healthy instance must not drain the whole instance");
        }
        finally
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await coordinator.StopAsync(stopCts.Token);
        }
    }

    [Test]
    public async Task WorkloadReassignedAway_WhileInstanceDraining_ShouldLetItFinishThenCleanUp()
    {
        var started = new TaskCompletionSource();
        var allowFinish = new TaskCompletionSource();
        var wasCancelled = false;
        var workloads = new List<TestWorkload> { new ("workload-a") };

        var coordinator = this.buildCoordinator(
            workloads,
            async (_, ct) =>
            {
                started.TrySetResult();
                try
                {
                    await allowFinish.Task.WaitAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    wasCancelled = true;
                    throw;
                }
            });

        await coordinator.StartAsync(CancellationToken.None);
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // Instance starts draining: the fake registry excludes a draining instance from its
            // own active-instance list too (matching the real Redis-backed implementations), so
            // the workload drops out of this instance's assignment on the next tick.
            this.instanceRegistry.IsDraining = true;

            // Give the coordinator several reconcile ticks worth of time; the runner must be left
            // alone rather than force-stopped just because it's no longer "assigned".
            await Task.Delay(TimeSpan.FromMilliseconds(150));
            wasCancelled.Should().BeFalse("a workload dropped during drain should be left to finish on its own");
            allowFinish.Task.IsCompleted.Should().BeFalse("the worker hasn't been told to finish yet");

            allowFinish.SetResult();

            await waitUntilAsync(
                () => this.mockedLeaseManager.Invocations.Any(i => i.Method.Name == nameof(ILeaseManager.ReleaseIfOwnedAsync)),
                TimeSpan.FromSeconds(5));

            wasCancelled.Should().BeFalse("the workload finished naturally and was never cancelled");
        }
        finally
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await coordinator.StopAsync(stopCts.Token);
        }
    }

    [Test]
    public async Task InstanceDraining_ShouldCallRequestDrainOnTheAssignedWorkloadsDrainable()
    {
        var started = new TaskCompletionSource();
        var drainable = new FakeDrainableService();
        var workloads = new List<TestWorkload> { new ("workload-a") { Drainable = drainable } };

        var coordinator = this.buildCoordinator(
            workloads,
            async (_, ct) =>
            {
                started.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                }
            },
            drainableSelector: w => w.Drainable);

        await coordinator.StartAsync(CancellationToken.None);
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

            this.instanceRegistry.IsDraining = true;

            // RequestDrain should fire as soon as the coordinator/runner notices the drain, well
            // before drainTimeout would force a cancellation.
            await waitUntilAsync(() => drainable.DrainRequested, TimeSpan.FromSeconds(5));
        }
        finally
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await coordinator.StopAsync(stopCts.Token);
        }
    }

    private static async Task waitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(5, CancellationToken.None);
        }
    }

    private WorkloadCoordinatorHostedService<TestWorkload> buildCoordinator(
        IReadOnlyCollection<TestWorkload> workloads,
        Func<TestWorkload, CancellationToken, Task> executeAsync,
        Func<TestWorkload, IDrainableService?>? drainableSelector = null)
        => new (
            NullLogger<WorkloadCoordinatorHostedService<TestWorkload>>.Instance,
            NullLoggerFactory.Instance,
            this.instanceRegistry,
            new FixedInstanceIdentityProvider("instance-a"),
            this.mockedLeaseManager.Object,
            new BalancedNamedWorkloadAssigner(),
            workloads,
            keySelector: w => w.Key,
            displayNameSelector: w => w.Key,
            executeAsync: executeAsync,
            leaseTtl: LeaseTtl,
            renewInterval: RenewInterval,
            drainTimeout: DrainTimeout,
            heartbeatInterval: HeartbeatInterval,
            drainableSelector: drainableSelector);

    private sealed class TestWorkload(string key)
    {
        public string Key { get; } = key;

        public IDrainableService? Drainable { get; init; }
    }

    private sealed class FakeDrainableService : IDrainableService
    {
        public bool DrainRequested { get; private set; }

        public void RequestDrain() => this.DrainRequested = true;
    }

    private sealed class FixedInstanceIdentityProvider(string instanceId)
        : IInstanceIdentityProvider
    {
        public string InstanceId { get; } = instanceId;
    }

    private sealed class FakeInstanceRegistry(string instanceId)
        : IInstanceRegistry
    {
        public string InstanceId { get; } = instanceId;

        public bool IsDraining { get; set; }

        public string[] ActiveInstanceIds { get; set; } = Array.Empty<string>();

        public Task HeartbeatAsync(CancellationToken ct) => Task.CompletedTask;

        public Task BeginDrainAsync(CancellationToken ct)
        {
            this.IsDraining = true;
            return Task.CompletedTask;
        }

        // Mirrors the real Redis-backed registries: a draining instance excludes itself from its
        // own view of the active-instance set, same as it would be excluded from every other
        // instance's view.
        public Task<IReadOnlyCollection<ActiveInstance>> GetActiveInstancesAsync(CancellationToken ct)
        {
            var ids = this.IsDraining ? Array.Empty<string>() : this.ActiveInstanceIds;

            // These tests only exercise BalancedNamedWorkloadAssigner, which ignores JoinedAtUtc,
            // so a fixed value is fine here - there's nothing to assert about join order.
            IReadOnlyCollection<ActiveInstance> activeInstances = ids
                .Select(id => new ActiveInstance { InstanceId = id, JoinedAtUtc = DateTimeOffset.UnixEpoch })
                .ToArray();

            return Task.FromResult(activeInstances);
        }

        public Task RemoveCurrentAsync(CancellationToken ct) => Task.CompletedTask;
    }
}
