using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Adapts <see cref="LeasedWorkerRunner"/> to the hosted-service lifecycle.
/// </summary>
/// <remarks>
/// The host's own <c>stoppingToken</c> is forwarded straight to <see cref="LeasedWorkerRunner.RunAsync"/>
/// unchanged - the runner treats "this workload was reassigned" and "the whole host is shutting
/// down" identically, so there is nothing here to differentiate. Marking the instance draining in
/// the backing store (<see cref="IInstanceRegistry.BeginDrainAsync"/>) is not this class's job
/// either - that is a deliberate, external act (e.g. a <c>/drain</c> endpoint or a preStop hook), not
/// something inferred from this token cancelling.
/// </remarks>
/// <param name="logger">Logger for this runner's start/stop/lease-acquisition events.</param>
/// <param name="leaseManager">Grants ownership of this workload's lease to exactly one instance at a time.</param>
/// <param name="instanceIdentityProvider">This process's unique instance id, used to prove lease ownership.</param>
/// <param name="instanceRegistry">Reports whether this instance is draining, and is written to when drain mode begins.</param>
/// <param name="workloadStatusStore">
/// Records this workload's <see cref="WorkloadStatus"/> (Active/Transferring/Inactive) as it moves
/// through its lifecycle, so other instances can tell it apart from one that has genuinely gone quiet.
/// </param>
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
    IWorkloadStatusStore workloadStatusStore,
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
        workloadStatusStore,
        workloadKey,
        displayName,
        leaseTtl,
        renewInterval,
        drainTimeout,
        executeAsync,
        drainable);

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => this.runner.RunAsync(stoppingToken);
}
