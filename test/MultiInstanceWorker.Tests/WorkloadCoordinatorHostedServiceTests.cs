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

    private FakeWorkloadStatusStore workloadStatusStore = null!;

    [SetUp]
    public void SetUp()
    {
        this.mockedLeaseManager = new Mock<ILeaseManager>();
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync(It.IsAny<string>(), "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        this.instanceRegistry = new FakeInstanceRegistry("instance-a") { ActiveInstanceIds = new[] { "instance-a" } };
        this.workloadStatusStore = new FakeWorkloadStatusStore();
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
    public async Task WorkloadReassignedAway_WhileInstanceNotDraining_ShouldLetItFinishNaturallyWithoutMarkingInstanceDraining()
    {
        var startedA = new TaskCompletionSource();
        var startedB = new TaskCompletionSource();
        var allowBToFinish = new TaskCompletionSource();
        var bExecutions = 0;
        var workloads = new List<TestWorkload> { new ("workload-a"), new ("workload-b") };

        var coordinator = this.buildCoordinator(
            workloads,
            async (workload, ct) =>
            {
                if (workload.Key == "workload-a")
                {
                    startedA.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return;
                }

                Interlocked.Increment(ref bExecutions);
                startedB.TrySetResult();

                // Deliberately does NOT observe ct - mirrors a workload (e.g. a game round) that
                // runs its current cycle to natural completion regardless of a reassignment signal,
                // only stopping cooperatively once it's actually done.
                await allowBToFinish.Task;
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

            // Give the coordinator several reconcile ticks worth of time; the workload must be left
            // running rather than force-stopped just because it's no longer assigned here.
            await Task.Delay(TimeSpan.FromMilliseconds(150));
            allowBToFinish.Task.IsCompleted.Should().BeFalse("the worker hasn't been told to finish yet");
            this.instanceRegistry.IsDraining.Should().BeFalse(
                "a plain rebalance away from a healthy instance must not drain the whole instance");

            // Lease renewal must keep succeeding throughout, or another instance could acquire the
            // same lease while this one is still finishing - a double-run hazard.
            this.mockedLeaseManager.Invocations
                .Count(i => i.Method.Name == nameof(ILeaseManager.TryAcquireOrRenewAsync) && (string)i.Arguments[0] == "workload-b")
                .Should().BeGreaterThan(1, "the lease must still be renewed while the reassigned workload winds down");

            allowBToFinish.SetResult();

            await waitUntilAsync(
                () => this.mockedLeaseManager.Invocations.Any(i =>
                    i.Method.Name == nameof(ILeaseManager.ReleaseIfOwnedAsync) && (string)i.Arguments[0] == "workload-b"),
                TimeSpan.FromSeconds(5));

            this.instanceRegistry.IsDraining.Should().BeFalse();

            // Give the coordinator a couple more ticks to prove it doesn't restart the workload on
            // this instance now that it's finished (it belongs to "instance-b" going forward).
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            bExecutions.Should().Be(1, "the reassigned workload must not be restarted on this instance once it finishes");
        }
        finally
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await coordinator.StopAsync(stopCts.Token);
        }
    }

    [Test]
    public async Task WorkloadReassignedAway_ShouldForceStopAfterDrainTimeout_WhenItNeverFinishesNaturally()
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
            await Task.WhenAll(
                startedA.Task.WaitAsync(TimeSpan.FromSeconds(5)),
                startedB.Task.WaitAsync(TimeSpan.FromSeconds(5)));

            this.instanceRegistry.ActiveInstanceIds = new[] { "instance-a", "instance-b" };

            // Not force-stopped immediately - it gets a chance to finish on its own first.
            var stoppedEarly = await Task.WhenAny(bCancelled.Task, Task.Delay(TimeSpan.FromMilliseconds(300))) == bCancelled.Task;
            stoppedEarly.Should().BeFalse("a reassigned workload must not be cancelled immediately");

            // It never cooperates (ignores the reassignment entirely), so the drainTimeout ceiling
            // must eventually force it to stop anyway.
            await bCancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));

            this.instanceRegistry.IsDraining.Should().BeFalse(
                "forcing a stuck reassigned workload to stop still must not drain the whole instance");
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

    [Test]
    public async Task WorkloadTransferring_ShouldNotStartOnTheNewInstance_UntilItFinishesOnTheOldOne()
    {
        // Reproduces the handover scenario directly: workload-a and workload-b both start on
        // instance-a; instance-b then joins and the balanced assigner moves workload-b onto it, but
        // workload-b needs time to finish its current unit of work on instance-a first. It must not
        // start on instance-b until it actually does.
        var leaseManager = new FakeLeaseManager();
        var statusStore = new FakeWorkloadStatusStore();
        var registryA = new FakeInstanceRegistry("instance-a") { ActiveInstanceIds = new[] { "instance-a" } };
        var registryB = new FakeInstanceRegistry("instance-b") { ActiveInstanceIds = new[] { "instance-a" } };
        var workloads = new List<TestWorkload> { new ("workload-a"), new ("workload-b") };

        var startedAOnA = new TaskCompletionSource();
        var startedBOnA = new TaskCompletionSource();
        var allowBToFinishOnA = new TaskCompletionSource();
        var startedBOnB = new TaskCompletionSource();
        var bStartCountOnB = 0;

        WorkloadCoordinatorHostedService<TestWorkload> buildCoordinatorFor(FakeInstanceRegistry registry, Func<TestWorkload, CancellationToken, Task> executeAsync)
            => new (
                NullLogger<WorkloadCoordinatorHostedService<TestWorkload>>.Instance,
                NullLoggerFactory.Instance,
                registry,
                new FixedInstanceIdentityProvider(registry.InstanceId),
                leaseManager,
                statusStore,
                new BalancedNamedWorkloadAssigner(),
                workloads,
                keySelector: w => w.Key,
                displayNameSelector: w => w.Key,
                executeAsync: executeAsync,
                leaseTtl: LeaseTtl,
                renewInterval: RenewInterval,
                drainTimeout: DrainTimeout,
                heartbeatInterval: HeartbeatInterval);

        var coordinatorA = buildCoordinatorFor(registryA, async (workload, ct) =>
        {
            if (workload.Key == "workload-a")
            {
                startedAOnA.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return;
            }

            startedBOnA.TrySetResult();

            // Deliberately does not observe ct - mirrors a workload finishing its current unit of
            // work naturally rather than stopping the instant it's reassigned.
            await allowBToFinishOnA.Task;
        });

        var coordinatorB = buildCoordinatorFor(registryB, (workload, ct) =>
        {
            if (workload.Key == "workload-b")
            {
                Interlocked.Increment(ref bStartCountOnB);
                startedBOnB.TrySetResult();
            }

            return Task.Delay(Timeout.Infinite, ct);
        });

        await coordinatorA.StartAsync(CancellationToken.None);
        try
        {
            await Task.WhenAll(
                startedAOnA.Task.WaitAsync(TimeSpan.FromSeconds(5)),
                startedBOnA.Task.WaitAsync(TimeSpan.FromSeconds(5)));

            // instance-b joins: the balanced assigner now wants workload-b on instance-b.
            registryA.ActiveInstanceIds = new[] { "instance-a", "instance-b" };
            registryB.ActiveInstanceIds = new[] { "instance-a", "instance-b" };

            await coordinatorB.StartAsync(CancellationToken.None);
            try
            {
                // Give instance-b's coordinator several reconcile ticks: workload-b is still
                // Transferring off instance-a (finishing its unit of work there), so it must not
                // start on instance-b yet - not "start and immediately fail to get the lease", but
                // not started at all.
                await Task.Delay(TimeSpan.FromMilliseconds(200));
                startedBOnB.Task.IsCompleted.Should().BeFalse(
                    "workload-b is still transferring off instance-a and must not be started on instance-b yet");

                allowBToFinishOnA.SetResult();

                // Once instance-a's runner actually finishes and releases, instance-b should pick it up.
                await startedBOnB.Task.WaitAsync(TimeSpan.FromSeconds(5));
                bStartCountOnB.Should().Be(1);
            }
            finally
            {
                using var stopBCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await coordinatorB.StopAsync(stopBCts.Token);
            }
        }
        finally
        {
            using var stopACts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await coordinatorA.StopAsync(stopACts.Token);
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
            this.workloadStatusStore,
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

    /// <summary>In-memory <see cref="IWorkloadStatusStore"/>, shareable across multiple coordinators the way a real backing store would be.</summary>
    private sealed class FakeWorkloadStatusStore : IWorkloadStatusStore
    {
        private readonly Dictionary<string, WorkloadStatusRecord> records = new (StringComparer.Ordinal);

        public Task SetStatusAsync(string workloadKey, WorkloadStatus status, string ownerInstanceId, TimeSpan ttl, CancellationToken ct)
        {
            var now = DateTimeOffset.UtcNow;

            lock (this.records)
            {
                // Write-once, same as the real store: preserved across writes while the record
                // hasn't lapsed, reset if it had already expired away.
                var createdAtUtc = this.records.TryGetValue(workloadKey, out var existing) && existing.ExpiresAtUtc > now
                    ? existing.CreatedAtUtc
                    : now;

                this.records[workloadKey] = new WorkloadStatusRecord
                {
                    WorkloadKey = workloadKey,
                    Status = status,
                    OwnerInstanceId = ownerInstanceId,
                    ExpiresAtUtc = now + ttl,
                    CreatedAtUtc = createdAtUtc,
                };
            }

            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, WorkloadStatusRecord>> GetStatusesAsync(IEnumerable<string> workloadKeys, CancellationToken ct)
        {
            lock (this.records)
            {
                var now = DateTimeOffset.UtcNow;
                IReadOnlyDictionary<string, WorkloadStatusRecord> result = workloadKeys
                    .Where(key => this.records.TryGetValue(key, out var record) && record.ExpiresAtUtc > now)
                    .ToDictionary(key => key, key => this.records[key], StringComparer.Ordinal);

                return Task.FromResult(result);
            }
        }

        public Task<IReadOnlyCollection<WorkloadStatusRecord>> GetAllAsync(CancellationToken ct)
        {
            lock (this.records)
            {
                var now = DateTimeOffset.UtcNow;
                IReadOnlyCollection<WorkloadStatusRecord> result = this.records.Values
                    .Where(record => record.ExpiresAtUtc > now)
                    .ToArray();

                return Task.FromResult(result);
            }
        }
    }

    /// <summary>
    /// In-memory <see cref="ILeaseManager"/> with real single-owner semantics (unlike the Moq mocks
    /// used elsewhere in this file, which just return a fixed answer) - needed for
    /// <see cref="WorkloadTransferring_ShouldNotStartOnTheNewInstance_UntilItFinishesOnTheOldOne"/>,
    /// which spans two real coordinators and so needs the lease itself to actually arbitrate them.
    /// </summary>
    private sealed class FakeLeaseManager : ILeaseManager
    {
        private readonly Dictionary<string, (string OwnerId, DateTimeOffset ExpiresAtUtc)> leases = new (StringComparer.Ordinal);

        public Task<bool> TryAcquireOrRenewAsync(string leaseName, string ownerId, TimeSpan leaseTtl, CancellationToken ct)
        {
            lock (this.leases)
            {
                var now = DateTimeOffset.UtcNow;
                if (!this.leases.TryGetValue(leaseName, out var existing) || existing.OwnerId == ownerId || existing.ExpiresAtUtc <= now)
                {
                    this.leases[leaseName] = (ownerId, now + leaseTtl);
                    return Task.FromResult(true);
                }

                return Task.FromResult(false);
            }
        }

        public Task ReleaseIfOwnedAsync(string leaseName, string ownerId, CancellationToken ct)
        {
            lock (this.leases)
            {
                if (this.leases.TryGetValue(leaseName, out var existing) && existing.OwnerId == ownerId)
                {
                    this.leases.Remove(leaseName);
                }
            }

            return Task.CompletedTask;
        }
    }
}
