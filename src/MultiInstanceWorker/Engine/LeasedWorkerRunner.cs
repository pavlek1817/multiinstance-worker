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
/// <c>drainTimeout</c>, instead of cancelling it immediately.
/// </remarks>
public sealed class LeasedWorkerRunner(
    ILogger logger,
    ILeaseManager leaseManager,
    IInstanceIdentityProvider instanceIdentityProvider,
    IInstanceRegistry instanceRegistry,
    string workloadKey,
    string displayName,
    TimeSpan leaseTtl,
    TimeSpan renewInterval,
    TimeSpan drainTimeout,
    Func<CancellationToken, Task> executeAsync)
{
    /// <summary>
    /// Drives the leader-election loop for the workload.
    /// </summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        Task? workerTask = null;
        CancellationTokenSource? workerTokenSource = null;
        DateTimeOffset? drainStartedAtUtc = null;
        var draining = instanceRegistry.IsDraining;

        LeasedWorkerRunnerLog.RunnerStarting(logger, displayName, instanceIdentityProvider.InstanceId);

        try
        {
            // This loop is the leader-election state machine for the workload.
            while (true)
            {
                if (!draining && (stoppingToken.IsCancellationRequested || instanceRegistry.IsDraining))
                {
                    draining = true;
                    drainStartedAtUtc = DateTimeOffset.UtcNow;
                    await instanceRegistry.BeginDrainAsync(CancellationToken.None);
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
                    draining = true;
                    drainStartedAtUtc = DateTimeOffset.UtcNow;
                    await instanceRegistry.BeginDrainAsync(CancellationToken.None);
                    continue;
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
                catch (OperationCanceledException) when (!draining && stoppingToken.IsCancellationRequested)
                {
                    draining = true;
                    drainStartedAtUtc = DateTimeOffset.UtcNow;
                    await instanceRegistry.BeginDrainAsync(CancellationToken.None);
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
