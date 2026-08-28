using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Adapts <see cref="LeasedWorkerRunner"/> to the hosted-service lifecycle.
/// </summary>
/// <remarks>
/// When used as a standalone <see cref="BackgroundService"/> (i.e. without a coordinator),
/// there is no distinction between "this runner is being stopped" and "the whole instance is
/// shutting down". Both roles are covered by the single <c>stoppingToken</c> supplied by the
/// host, which is forwarded to <see cref="LeasedWorkerRunner.RunAsync"/> as both
/// <c>stoppingToken</c> and <c>shutdownToken</c>.
/// </remarks>
/// <param name="logger">Logger for this runner's start/stop/lease-acquisition events.</param>
/// <param name="leaseManager">Grants ownership of this workload's lease to exactly one instance at a time.</param>
/// <param name="instanceIdentityProvider">This process's unique instance id, used to prove lease ownership.</param>
/// <param name="instanceRegistry">Reports whether this instance is draining, and is written to when drain mode begins.</param>
/// <param name="workloadKey">The workload's unique, stable lease key.</param>
/// <param name="displayName">The workload's human-readable name, for logs.</param>
/// <param name="leaseTtl">How long the lease remains valid before another instance can take it over.</param>
/// <param name="renewInterval">How often the owning instance renews the lease.</param>
/// <param name="drainTimeout">How long a draining workload is allowed to finish on its own before it is force-cancelled.</param>
/// <param name="executeAsync">Runs the workload for as long as this instance owns the lease.</param>
/// <param name="drainable">
/// Optional: if the workload implements <see cref="IDrainableService"/>, pass it here to have
/// <see cref="IDrainableService.RequestDrain"/> called the moment this runner enters drain mode.
/// </param>
public sealed class LeasedWorkerHostedService(
    ILogger<LeasedWorkerHostedService> logger,
    ILeaseManager leaseManager,
    IInstanceIdentityProvider instanceIdentityProvider,
    IInstanceRegistry instanceRegistry,
    string workloadKey,
    string displayName,
    TimeSpan leaseTtl,
    TimeSpan renewInterval,
    TimeSpan drainTimeout,
    Func<CancellationToken, Task> executeAsync,
    IDrainableService? drainable = null)
    : BackgroundService
{
    private readonly LeasedWorkerRunner runner = new (
        logger,
        leaseManager,
        instanceIdentityProvider,
        instanceRegistry,
        workloadKey,
        displayName,
        leaseTtl,
        renewInterval,
        drainTimeout,
        executeAsync,
        drainable);

    /// <inheritdoc/>
    /// <remarks>
    /// Passes <paramref name="stoppingToken"/> as both the per-runner stopping token and the
    /// instance-level shutdown token because there is no coordinator differentiating the two here.
    /// </remarks>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => this.runner.RunAsync(stoppingToken, shutdownToken: stoppingToken);
}
