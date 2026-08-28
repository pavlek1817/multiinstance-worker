using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Runs a workload only while this instance owns the associated lease.
/// </summary>
/// <remarks>
/// The runner keeps renewing the lease, starts the workload on acquisition,
/// stops it on lease loss, and releases the lease when the host shuts down.
/// On shutdown (or when <see cref="IInstanceRegistry.IsDraining"/> becomes true for
/// any other reason, e.g. an operator-triggered drain ahead of downsizing) the runner
/// stops taking new work but lets an in-flight worker finish on its own, up to
/// <c>drainTimeout</c>, instead of cancelling it immediately. If the workload passed as
/// <paramref name="drainable"/> implements <see cref="IDrainableService"/>, its
/// <see cref="IDrainableService.RequestDrain"/> is invoked the moment drain mode is entered, so a
/// cooperative workload can wrap up on its own well before <c>drainTimeout</c> would force it.
/// </remarks>
internal sealed class LeasedWorkerRunner(
    ILogger logger,
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
{
    /// <summary>
    /// Drives the leader-election loop for the workload.
    /// </summary>
    /// <param name="stoppingToken">
    /// Signals that <em>this runner</em> should stop (e.g. workload reassigned by a coordinator).
    /// Cancelling this token exits the loop cleanly but does <b>not</b> mark the whole instance as
    /// draining — that is the responsibility of <paramref name="shutdownToken"/>.
    /// </param>
    /// <param name="shutdownToken">
    /// Signals that the <em>whole instance</em> is shutting down (e.g. preStop hook or SIGTERM).
    /// Cancelling this token enters drain mode and calls
    /// <see cref="IInstanceRegistry.BeginDrainAsync"/> to mark the instance in the backing store.
    /// For hosted services where there is no per-runner coordinator, pass the same token as both
    /// <paramref name="stoppingToken"/> and <paramref name="shutdownToken"/>.
    /// </param>
    public async Task RunAsync(CancellationToken stoppingToken, CancellationToken shutdownToken)
    {
        Task? workerTask = null;
        CancellationTokenSource? workerTokenSource = null;
        DateTimeOffset? drainStartedAtUtc = null;
        var draining = instanceRegistry.IsDraining;

        // Shared by both places below that transition into drain mode, so the cooperative push
        // signal (RequestDrain) and the backing-store write (BeginDrainAsync) can never drift apart.
        async Task enterDrainModeAsync()
        {
            draining = true;
            drainStartedAtUtc = DateTimeOffset.UtcNow;
            drainable?.RequestDrain();
            await instanceRegistry.BeginDrainAsync(CancellationToken.None);
        }

        LeasedWorkerRunnerLog.RunnerStarting(logger, displayName, instanceIdentityProvider.InstanceId);

        try
        {
            // This loop is the leader-election state machine for the workload.
            while (true)
            {
                // Only enter drain mode when the whole instance is shutting down or a drain was
                // requested externally. A plain stoppingToken cancellation means this runner was
                // removed by a coordinator (workload reassignment) — that must NOT mark the
                // entire instance as draining in the backing store.
                if (!draining && (shutdownToken.IsCancellationRequested || instanceRegistry.IsDraining))
                {
                    await enterDrainModeAsync();
                }

                if (draining && workerTask is null)
                {
                    break;
                }

                bool ownsLease;
                try
                {
                    // Renew the lease first; only the current owner is allowed to keep the worker running.
                    ownsLease = await leaseManager.TryAcquireOrRenewAsync(
                        workloadKey,
                        instanceIdentityProvider.InstanceId,
                        leaseTtl,
                        draining ? CancellationToken.None : stoppingToken);
                }
                catch (OperationCanceledException) when (!draining && stoppingToken.IsCancellationRequested)
                {
                    // stoppingToken was cancelled — workload was reassigned by the coordinator.
                    // Exit the loop cleanly without entering drain mode or touching the instance registry.
                    break;
                }

                if (ownsLease && workerTask is null)
                {
                    LeasedWorkerRunnerLog.LeaseAcquired(logger, displayName);
                    workerTokenSource = new CancellationTokenSource();
                    workerTask = Task.Run(() => executeAsync(workerTokenSource.Token), CancellationToken.None);
                }
                else if (!ownsLease && workerTask is not null)
                {
                    LeasedWorkerRunnerLog.LeaseLost(logger, displayName);
                    await stopWorkerAsync(workerTask, workerTokenSource);
                    workerTask = null;
                    workerTokenSource = null;

                    if (draining)
                    {
                        break;
                    }
                }

                // A completed worker is treated as a terminal state and gets cleaned up immediately.
                if (workerTask?.IsCompleted == true)
                {
                    await workerTask;
                    workerTokenSource?.Dispose();
                    workerTask = null;
                    workerTokenSource = null;

                    if (draining)
                    {
                        break;
                    }
                }

                if (draining)
                {
                    if (workerTask is null)
                    {
                        break;
                    }

                    var drainElapsed = DateTimeOffset.UtcNow - drainStartedAtUtc.GetValueOrDefault(DateTimeOffset.UtcNow);
                    if (drainElapsed >= drainTimeout)
                    {
                        await stopWorkerAsync(workerTask, workerTokenSource);
                        break;
                    }

                    await Task.WhenAny(workerTask, Task.Delay(renewInterval, CancellationToken.None));
                    continue;
                }

                try
                {
                    await Task.Delay(renewInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (!draining && shutdownToken.IsCancellationRequested)
                {
                    // The whole instance is shutting down — enter drain mode. Checked before the
                    // stoppingToken filter below so that a standalone hosted service (which passes
                    // the same token as both stoppingToken and shutdownToken, per
                    // LeasedWorkerHostedService) still drains: when a single cancellation trips
                    // both filters at once, the shutdown/drain outcome must win.
                    await enterDrainModeAsync();
                }
                catch (OperationCanceledException) when (!draining && stoppingToken.IsCancellationRequested)
                {
                    // stoppingToken fired during the delay — workload was reassigned.
                    // Exit cleanly; do not enter drain mode or call BeginDrainAsync.
                    break;
                }
            }
        }
        finally
        {
            // Shutdown is a best-effort cleanup path; stop the worker before releasing the lease.
            if (workerTask is not null)
            {
                if (!draining || !workerTask.IsCompleted)
                {
                    await stopWorkerAsync(workerTask, workerTokenSource);
                }
            }
            else
            {
                workerTokenSource?.Dispose();
            }

            await leaseManager.ReleaseIfOwnedAsync(workloadKey, instanceIdentityProvider.InstanceId, CancellationToken.None);

            LeasedWorkerRunnerLog.RunnerStopped(logger, displayName);
        }
    }

    /// <summary>Stops the worker task, tolerating cooperative cancellation.</summary>
    private static async Task stopWorkerAsync(Task workerTask, CancellationTokenSource? workerTokenSource)
    {
        if (workerTokenSource is null)
        {
            await workerTask;
            return;
        }

        workerTokenSource.Cancel();

        try
        {
            await workerTask;
        }
        catch (OperationCanceledException) when (workerTokenSource.IsCancellationRequested)
        {
        }
        finally
        {
            workerTokenSource.Dispose();
        }
    }
}
