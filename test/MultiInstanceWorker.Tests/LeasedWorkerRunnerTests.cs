using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace MultiInstanceWorker.Tests;

internal class LeasedWorkerRunnerTests
{
    private static readonly TimeSpan LeaseTtl = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan RenewInterval = TimeSpan.FromMilliseconds(10);

    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(5);

    private Mock<ILeaseManager> mockedLeaseManager = null!;

    private FakeInstanceRegistry instanceRegistry = null!;

    private FakeWorkloadStatusStore workloadStatusStore = null!;

    [SetUp]
    public void SetUp()
    {
        this.mockedLeaseManager = new Mock<ILeaseManager>();
        this.instanceRegistry = new FakeInstanceRegistry("instance-a");
        this.workloadStatusStore = new FakeWorkloadStatusStore();
    }

    [Test]
    public async Task LeaseNeverOwnedCase_ShouldNotStartWorker()
    {
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var workerStarted = false;
        var runner = this.buildRunner(_ =>
        {
            workerStarted = true;
            return Task.CompletedTask;
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await runner.RunAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(5));

        workerStarted.Should().BeFalse();
    }

    [Test]
    public async Task LeaseLostCase_ShouldCancelRunningWorker()
    {
        var ownsLease = true;
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => ownsLease);

        var workerStarted = new TaskCompletionSource();
        var workerCancelled = new TaskCompletionSource();
        var runner = this.buildRunner(async ct =>
        {
            workerStarted.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                workerCancelled.TrySetResult();
                throw;
            }
        });

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        ownsLease = false;
        await workerCancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task ShutdownWhileWorkerRunningCase_ShouldLetWorkerFinishOnItsOwnBeforeReleasingLease()
    {
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var workerStarted = new TaskCompletionSource();
        var allowWorkerToFinish = new TaskCompletionSource();
        var runner = this.buildRunner(async _ =>
        {
            workerStarted.TrySetResult();
            await allowWorkerToFinish.Task;
        });

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        // Drain mode should be entered immediately (visible via the Transferring status write),
        // but the runner must not tear down the in-flight worker just because it was told to stop.
        await waitUntilAsync(
            () => this.workloadStatusStore.Writes.Any(w => w.Status == WorkloadStatus.Transferring),
            TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromMilliseconds(100));
        runTask.IsCompleted.Should().BeFalse("the worker is still finishing its own work and hasn't hit the drain timeout");
        this.instanceRegistry.IsDraining.Should().BeFalse(
            "the runner must never mark the instance draining itself - only an explicit call to BeginDrainAsync does that");

        allowWorkerToFinish.SetResult();
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        this.mockedLeaseManager.Verify(
            x => x.ReleaseIfOwnedAsync("workload", "instance-a", It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Test]
    public async Task DrainTimeoutExceededCase_ShouldForciblyStopWorker()
    {
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var workerStarted = new TaskCompletionSource();
        var runner = this.buildRunner(
            ct =>
            {
                workerStarted.TrySetResult();
                return Task.Delay(Timeout.Infinite, ct);
            },
            drainTimeout: TimeSpan.FromMilliseconds(50));

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        // The worker never finishes on its own, so once the drain timeout elapses the
        // runner must cancel it forcibly instead of waiting forever.
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        this.mockedLeaseManager.Verify(
            x => x.ReleaseIfOwnedAsync("workload", "instance-a", It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Test]
    public async Task InstanceShutdown_WithDrainableWorkload_ShouldCallRequestDrainImmediately()
    {
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var workerStarted = new TaskCompletionSource();
        var drainable = new FakeDrainableService();

        // drainTimeout is short and irrelevant to what this test actually checks (RequestDrain
        // firing immediately): the worker only observes cancellation via the runner's own
        // force-cancel at the drainTimeout boundary, so leaving this at the 5s default raced
        // runTask's completion against this test's own 5s WaitAsync budget below with no real margin.
        var runner = this.buildRunner(
            async ct =>
            {
                workerStarted.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                }
            },
            drainTimeout: TimeSpan.FromMilliseconds(50),
            drainable: drainable);

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        // RequestDrain should fire as soon as drain mode is entered - well before drainTimeout -
        // so a cooperative workload gets the maximum possible time to wrap up on its own.
        await waitUntilAsync(() => drainable.DrainRequested, TimeSpan.FromSeconds(5));

        await runTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task AlreadyDrainingInstanceCase_ShouldExitWithoutAcquiringLease()
    {
        this.instanceRegistry.IsDraining = true;

        var runner = this.buildRunner(_ => Task.CompletedTask);

        await runner.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        this.mockedLeaseManager.Verify(
            x => x.TryAcquireOrRenewAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Test]
    public async Task RunnerStopping_ShouldNeverMarkInstanceDraining_OnlyAnExplicitDrainCallDoesThat()
    {
        // The runner treats "reassigned" and "host shutting down" identically, and never calls
        // BeginDrainAsync itself either way - that is an external act (e.g. a /drain endpoint).
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var runner = this.buildRunner(_ => Task.CompletedTask);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await runner.RunAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(5));

        this.instanceRegistry.IsDraining.Should().BeFalse(
            "the runner must never mark the instance draining itself - only an explicit call to BeginDrainAsync does that");
    }

    [Test]
    public async Task WorkloadReassignment_WhileWorkerRunning_ShouldLetWorkerFinishNaturallyBeforeReleasingLease()
    {
        var renewCount = 0;
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                Interlocked.Increment(ref renewCount);
                return true;
            });

        var workerStarted = new TaskCompletionSource();
        var allowWorkerToFinish = new TaskCompletionSource();
        var drainable = new FakeDrainableService();
        var runner = this.buildRunner(
            async _ =>
            {
                workerStarted.TrySetResult();
                await allowWorkerToFinish.Task;
            },
            drainable: drainable);

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var renewCountAtReassignment = renewCount;
        await cts.CancelAsync();

        // Reassignment should signal the cooperative workload immediately...
        await waitUntilAsync(() => drainable.DrainRequested, TimeSpan.FromSeconds(5));

        // ...but must not tear down the in-flight worker, nor mark the whole instance draining -
        // only this one workload was reassigned, the instance itself is still healthy.
        await Task.Delay(TimeSpan.FromMilliseconds(100));
        runTask.IsCompleted.Should().BeFalse("the worker is still finishing its own work and hasn't hit the drain timeout");
        this.instanceRegistry.IsDraining.Should().BeFalse(
            "cancelling the per-runner stoppingToken (workload reassignment) must not drain the instance");

        // The lease must keep being renewed throughout, or another instance could acquire it while
        // this one is still finishing up - a double-run hazard.
        renewCount.Should().BeGreaterThan(renewCountAtReassignment, "the lease must still be renewed while the worker winds down");

        allowWorkerToFinish.SetResult();
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        this.instanceRegistry.IsDraining.Should().BeFalse();
        this.mockedLeaseManager.Verify(
            x => x.ReleaseIfOwnedAsync("workload", "instance-a", It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Test]
    public async Task WorkloadReassignment_WhenWorkerNeverFinishes_ShouldForciblyStopAfterDrainTimeout()
    {
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var workerStarted = new TaskCompletionSource();
        var runner = this.buildRunner(
            ct =>
            {
                workerStarted.TrySetResult();
                return Task.Delay(Timeout.Infinite, ct);
            },
            drainTimeout: TimeSpan.FromMilliseconds(50));

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        // The worker never finishes on its own, so once the drain timeout elapses the runner must
        // cancel it forcibly instead of waiting forever - same ceiling as a real drain.
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        this.instanceRegistry.IsDraining.Should().BeFalse(
            "forcing a stuck reassigned workload to stop still must not drain the whole instance");
        this.mockedLeaseManager.Verify(
            x => x.ReleaseIfOwnedAsync("workload", "instance-a", It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Test]
    public async Task WorkloadStatus_ShouldReflectLifecycle_ActiveThenTransferringThenInactive()
    {
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var workerStarted = new TaskCompletionSource();
        var allowWorkerToFinish = new TaskCompletionSource();
        var runner = this.buildRunner(async _ =>
        {
            workerStarted.TrySetResult();
            await allowWorkerToFinish.Task;
        });

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Refreshed every renewInterval while owned and running - this is the workload's own heartbeat.
        await waitUntilAsync(
            () => this.workloadStatusStore.Writes.Any(w => w.Status == WorkloadStatus.Active && w.OwnerInstanceId == "instance-a"),
            TimeSpan.FromSeconds(5));

        await cts.CancelAsync();

        // Reassignment moves it into Transferring before the worker has actually finished.
        await waitUntilAsync(
            () => this.workloadStatusStore.Writes.Any(w => w.Status == WorkloadStatus.Transferring && w.OwnerInstanceId == "instance-a"),
            TimeSpan.FromSeconds(5));

        allowWorkerToFinish.SetResult();
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        this.workloadStatusStore.Writes.Last().Status.Should().Be(
            WorkloadStatus.Inactive, "the runner writes a terminal Inactive status once it actually releases the lease");
    }

    [Test]
    public async Task WorkloadTransferFromInstanceA_ToInstanceB_ShouldWaitForWorkToFinishOnAFirst()
    {
        // Two real runners for the SAME workload, one per instance, sharing a lease manager with
        // genuine single-owner semantics (unlike the canned Mock<ILeaseManager> used elsewhere in
        // this file) - this is what actually stops instance B from running the workload
        // concurrently with instance A, independent of any coordinator-level gating.
        var leaseManager = new FakeLeaseManager();
        var statusStore = new FakeWorkloadStatusStore();

        // Short and irrelevant to the scenario itself (A's "work" finishes voluntarily via
        // allowAToFinish, well inside it) - only matters for cleanup below, where B's own worker
        // (Task.Delay(Timeout.Infinite, ...)) never finishes on its own and would otherwise make
        // teardown wait out the full drainTimeout once cancelled.
        var cleanupDrainTimeout = TimeSpan.FromMilliseconds(200);

        var startedOnA = new TaskCompletionSource();
        var allowAToFinish = new TaskCompletionSource();
        var aExecutions = 0;
        var runnerA = new LeasedWorkerRunner(
            NullLogger.Instance,
            leaseManager,
            new FixedInstanceIdentityProvider("instance-a"),
            new FakeInstanceRegistry("instance-a"),
            statusStore,
            workloadKey: "workload",
            displayName: "test workload",
            leaseTtl: LeaseTtl,
            renewInterval: RenewInterval,
            drainTimeout: cleanupDrainTimeout,
            executeAsync: async _ =>
            {
                Interlocked.Increment(ref aExecutions);
                startedOnA.TrySetResult();

                // Deliberately does not observe the cancellation token - mirrors work that must run
                // to natural completion (e.g. "needs 15 seconds to finish") rather than stopping the
                // instant it's told to.
                await allowAToFinish.Task;
            });

        var startedOnB = new TaskCompletionSource();
        var bExecutions = 0;
        var runnerB = new LeasedWorkerRunner(
            NullLogger.Instance,
            leaseManager,
            new FixedInstanceIdentityProvider("instance-b"),
            new FakeInstanceRegistry("instance-b"),
            statusStore,
            workloadKey: "workload",
            displayName: "test workload",
            leaseTtl: LeaseTtl,
            renewInterval: RenewInterval,
            drainTimeout: cleanupDrainTimeout,
            executeAsync: ct =>
            {
                Interlocked.Increment(ref bExecutions);
                startedOnB.TrySetResult();
                return Task.Delay(Timeout.Infinite, ct);
            });

        using var ctsA = new CancellationTokenSource();
        var runTaskA = runnerA.RunAsync(ctsA.Token);

        // instance-a picks it up first.
        await startedOnA.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using var ctsB = new CancellationTokenSource();
        var runTaskB = runnerB.RunAsync(ctsB.Token);
        try
        {
            // instance-b is already polling for the same workload (e.g. the assigner moved it
            // there), but must not be able to start it while instance-a still owns the lease.
            var startedEarly = await Task.WhenAny(startedOnB.Task, Task.Delay(TimeSpan.FromMilliseconds(200))) == startedOnB.Task;
            startedEarly.Should().BeFalse("instance-b must not start the workload while instance-a still owns the lease");

            // instance-a is reassigned/drained mid-work - it keeps the lease renewed and lets the
            // work finish on its own rather than tearing it down.
            await ctsA.CancelAsync();

            await Task.Delay(TimeSpan.FromMilliseconds(200));
            startedOnB.Task.IsCompleted.Should().BeFalse("instance-a's work is still finishing; instance-b must keep waiting");

            // Only once the work actually finishes on instance-a does instance-b get the lease.
            allowAToFinish.SetResult();
            await startedOnB.Task.WaitAsync(TimeSpan.FromSeconds(5));

            aExecutions.Should().Be(1, "the work must run exactly once on instance-a, never restarted");
            bExecutions.Should().Be(1, "the work must start on instance-b exactly once, only after instance-a released it");
        }
        finally
        {
            await ctsB.CancelAsync();
            await runTaskA.WaitAsync(TimeSpan.FromSeconds(5));
            await runTaskB.WaitAsync(TimeSpan.FromSeconds(5));
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

    private LeasedWorkerRunner buildRunner(
        Func<CancellationToken, Task> executeAsync,
        TimeSpan? drainTimeout = null,
        IDrainableService? drainable = null)
        => new (
            NullLogger.Instance,
            this.mockedLeaseManager.Object,
            new FixedInstanceIdentityProvider("instance-a"),
            this.instanceRegistry,
            this.workloadStatusStore,
            workloadKey: "workload",
            displayName: "test workload",
            leaseTtl: LeaseTtl,
            renewInterval: RenewInterval,
            drainTimeout: drainTimeout ?? DrainTimeout,
            executeAsync: executeAsync,
            drainable: drainable);

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

        public Task HeartbeatAsync(CancellationToken ct) => Task.CompletedTask;

        public Task BeginDrainAsync(CancellationToken ct)
        {
            this.IsDraining = true;
            return Task.CompletedTask;
        }

        public Task<IReadOnlyCollection<ActiveInstance>> GetActiveInstancesAsync(CancellationToken ct)
            => Task.FromResult<IReadOnlyCollection<ActiveInstance>>(Array.Empty<ActiveInstance>());

        public Task RemoveCurrentAsync(CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class FakeDrainableService : IDrainableService
    {
        public bool DrainRequested { get; private set; }

        public void RequestDrain() => this.DrainRequested = true;
    }

    /// <summary>
    /// In-memory <see cref="ILeaseManager"/> with real single-owner semantics (unlike the Moq mocks
    /// used elsewhere in this file, which just return a fixed answer regardless of caller) - needed
    /// for <see cref="WorkloadTransferFromInstanceA_ToInstanceB_ShouldWaitForWorkToFinishOnAFirst"/>,
    /// which spans two real runners and so needs the lease itself to actually arbitrate them.
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

    /// <summary>In-memory <see cref="IWorkloadStatusStore"/> - records every write so tests can assert on the sequence of statuses a runner reports.</summary>
    private sealed class FakeWorkloadStatusStore : IWorkloadStatusStore
    {
        private readonly Dictionary<string, WorkloadStatusRecord> records = new (StringComparer.Ordinal);

        public List<WorkloadStatusRecord> Writes { get; } = new ();

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

                var record = new WorkloadStatusRecord
                {
                    WorkloadKey = workloadKey,
                    Status = status,
                    OwnerInstanceId = ownerInstanceId,
                    ExpiresAtUtc = now + ttl,
                    CreatedAtUtc = createdAtUtc,
                };

                this.records[workloadKey] = record;
                this.Writes.Add(record);
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
}
