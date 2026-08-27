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
