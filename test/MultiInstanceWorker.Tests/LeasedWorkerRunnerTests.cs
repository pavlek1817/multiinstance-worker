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

    [SetUp]
    public void SetUp()
    {
        this.mockedLeaseManager = new Mock<ILeaseManager>();
        this.instanceRegistry = new FakeInstanceRegistry("instance-a");
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
        await runner.RunAsync(cts.Token, shutdownToken: cts.Token).WaitAsync(TimeSpan.FromSeconds(5));

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
        var runTask = runner.RunAsync(cts.Token, shutdownToken: cts.Token);

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
        var runTask = runner.RunAsync(cts.Token, shutdownToken: cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();

        // The instance should be marked draining immediately, but the runner must not
        // tear down the in-flight worker just because shutdown was requested.
        await waitUntilAsync(() => this.instanceRegistry.IsDraining, TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromMilliseconds(100));
        runTask.IsCompleted.Should().BeFalse("the worker is still finishing its own work and hasn't hit the drain timeout");

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
        var runTask = runner.RunAsync(cts.Token, shutdownToken: cts.Token);

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
            drainable: drainable);

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token, shutdownToken: cts.Token);

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

        await runner.RunAsync(CancellationToken.None, shutdownToken: CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        this.mockedLeaseManager.Verify(
            x => x.TryAcquireOrRenewAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Test]
    public async Task WorkloadReassignment_WhenStoppingTokenCancelled_ShouldNotCallBeginDrainAsync()
    {
        // stoppingToken is the per-runner token a coordinator cancels on reassignment; it must
        // not be conflated with the instance-level shutdownToken.
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var runner = this.buildRunner(_ => Task.CompletedTask);

        using var stoppingCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        using var shutdownCts = new CancellationTokenSource();

        await runner.RunAsync(stoppingCts.Token, shutdownToken: shutdownCts.Token).WaitAsync(TimeSpan.FromSeconds(5));

        this.instanceRegistry.IsDraining.Should().BeFalse(
            "cancelling the per-runner stoppingToken (workload reassignment) must not drain the instance");
    }

    [Test]
    public async Task InstanceShutdown_WhenShutdownTokenCancelled_ShouldCallBeginDrainAsync()
    {
        // shutdownToken is the host-level token cancelled during instance shutdown; only that one
        // should flip the instance into drain mode.
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var runner = this.buildRunner(_ => Task.CompletedTask);

        using var stoppingCts = new CancellationTokenSource();
        using var shutdownCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        var runTask = runner.RunAsync(stoppingCts.Token, shutdownToken: shutdownCts.Token);

        await waitUntilAsync(() => this.instanceRegistry.IsDraining, TimeSpan.FromSeconds(5));
        await stoppingCts.CancelAsync();
        await runTask.WaitAsync(TimeSpan.FromSeconds(5));

        this.instanceRegistry.IsDraining.Should().BeTrue(
            "cancelling shutdownToken (instance shutdown) must drain the instance");
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
}
