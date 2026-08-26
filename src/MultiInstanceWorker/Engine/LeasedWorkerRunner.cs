using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Runs a workload only while this instance owns the associated lease.
/// </summary>
/// <remarks>
/// The runner keeps renewing the lease, starts the workload on acquisition,
/// stops it on lease loss, and releases the lease when the host shuts down.
/// </remarks>
public sealed class LeasedWorkerRunner(
    ILogger logger,
    ILeaseManager leaseManager,
    IInstanceIdentityProvider instanceIdentityProvider,
    string workloadKey,
    string displayName,
    TimeSpan leaseTtl,
    TimeSpan renewInterval,
    Func<CancellationToken, Task> executeAsync)
{
    /// <summary>
    /// Drives the leader-election loop for the workload.
    /// </summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        Task? workerTask = null;
        CancellationTokenSource? workerTokenSource = null;

        LeasedWorkerRunnerLog.RunnerStarting(logger, displayName, instanceIdentityProvider.InstanceId);

        try
        {
            // This loop is the leader-election state machine for the workload.
            while (!stoppingToken.IsCancellationRequested)
            {
                // Renew the lease first; only the current owner is allowed to keep the worker running.
                var ownsLease = await leaseManager.TryAcquireOrRenewAsync(
                    workloadKey,
                    instanceIdentityProvider.InstanceId,
                    leaseTtl,
                    stoppingToken);

                if (ownsLease && workerTask is null)
                {
                    LeasedWorkerRunnerLog.LeaseAcquired(logger, displayName);
                    workerTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    workerTask = Task.Run(() => executeAsync(workerTokenSource.Token), CancellationToken.None);
                }
                else if (!ownsLease && workerTask is not null)
                {
                    LeasedWorkerRunnerLog.LeaseLost(logger, displayName);
                    await stopWorkerAsync(workerTask, workerTokenSource);
                    workerTask = null;
                    workerTokenSource = null;
                }

                // A completed worker is treated as a terminal state and gets cleaned up immediately.
                if (workerTask?.IsCompleted == true)
                {
                    await workerTask;
                    workerTokenSource?.Dispose();
                    workerTask = null;
                    workerTokenSource = null;
                }

                await Task.Delay(renewInterval, stoppingToken);
            }
        }
        finally
        {
            // Shutdown is a best-effort cleanup path; stop the worker before releasing the lease.
            if (workerTask is not null)
            {
                await stopWorkerAsync(workerTask, workerTokenSource);
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
