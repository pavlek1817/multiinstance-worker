using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Adapts <see cref="LeasedWorkerRunner"/> to the hosted-service lifecycle.
/// </summary>
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
    Func<CancellationToken, Task> executeAsync)
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
        executeAsync);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => this.runner.RunAsync(stoppingToken);
}
