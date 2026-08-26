using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace MultiInstanceWorker.Tests;

internal class LeasedWorkerRunnerTests
{
    private static readonly TimeSpan LeaseTtl = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan RenewInterval = TimeSpan.FromMilliseconds(10);

    private Mock<ILeaseManager> mockedLeaseManager = null!;

    [SetUp]
    public void SetUp()
    {
        this.mockedLeaseManager = new Mock<ILeaseManager>();
    }

    [Test]
    public async Task LeaseOwnedCase_ShouldStartWorkerAndReleaseLeaseOnShutdown()
    {
        this.mockedLeaseManager
            .Setup(x => x.TryAcquireOrRenewAsync("workload", "instance-a", LeaseTtl, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var workerStarted = new TaskCompletionSource();
        var runner = this.buildRunner(ct =>
        {
            workerStarted.TrySetResult();
            return Task.Delay(Timeout.Infinite, ct);
        });

        using var cts = new CancellationTokenSource();
        var runTask = runner.RunAsync(cts.Token);

        await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();
        await awaitShutdownAsync(runTask, cts);

        this.mockedLeaseManager.Verify(
            x => x.ReleaseIfOwnedAsync("workload", "instance-a", It.IsAny<CancellationToken>()),
            Times.Once);
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
        await awaitShutdownAsync(runner.RunAsync(cts.Token), cts);

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
        await awaitShutdownAsync(runTask, cts);
    }

    /// <summary>
    /// Awaits shutdown of the runner, tolerating the <see cref="OperationCanceledException"/>
    /// that <see cref="LeasedWorkerRunner.RunAsync"/> can throw when cancellation is observed
    /// mid-delay, matching how real callers (e.g. a hosted-service loop) await it.
    /// </summary>
    private static async Task awaitShutdownAsync(Task runTask, CancellationTokenSource cts)
    {
        try
        {
            await runTask;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
        }
    }

    private LeasedWorkerRunner buildRunner(Func<CancellationToken, Task> executeAsync)
        => new (
            NullLogger.Instance,
            this.mockedLeaseManager.Object,
            new FixedInstanceIdentityProvider("instance-a"),
            workloadKey: "workload",
            displayName: "test workload",
            leaseTtl: LeaseTtl,
            renewInterval: RenewInterval,
            executeAsync: executeAsync);

    private sealed class FixedInstanceIdentityProvider(string instanceId)
        : IInstanceIdentityProvider
    {
        public string InstanceId { get; } = instanceId;
    }
}
