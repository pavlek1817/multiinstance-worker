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
    string workloadKey,
    string displayName,
    TimeSpan leaseTtl,
    TimeSpan renewInterval,
    Func<CancellationToken, Task> executeAsync)
    : BackgroundService
{
    private readonly LeasedWorkerRunner runner = new (
        logger,
        leaseManager,
        instanceIdentityProvider,
        workloadKey,
        displayName,
        leaseTtl,
        renewInterval,
        executeAsync);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => this.runner.RunAsync(stoppingToken);
}
