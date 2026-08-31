using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Runs a workload only while this instance owns the associated lease.
/// </summary>
/// <remarks>
/// The runner keeps renewing the lease, starts the workload on acquisition, stops it on lease loss,
/// and releases the lease when it stops. It reacts identically to <em>why</em> it's stopping —
/// <c>stoppingToken</c> cancelled (a coordinator reassigned this workload elsewhere, or the whole
/// host is shutting down — the runner has no way to tell those apart, and does not need to) or
/// <see cref="IInstanceRegistry.IsDraining"/> already true (an operator drained the instance
/// externally) — either way it stops taking new work but lets an in-flight worker finish on its own,
/// up to <c>drainTimeout</c>, instead of cancelling it immediately; the lease is kept renewed
/// throughout so nothing else can acquire it out from under the still-finishing worker. If the
/// workload passed as <paramref name="drainable"/> implements <see cref="IDrainableService"/>, its
/// <see cref="IDrainableService.RequestDrain"/> is invoked the moment drain mode is entered, so a
/// cooperative workload can wrap up on its own well before <c>drainTimeout</c> would force it.
/// <para/>
/// This runner never calls <see cref="IInstanceRegistry.BeginDrainAsync"/> itself — marking the
/// whole instance draining in the backing store is a deliberate, external act (e.g. a <c>/drain</c>
/// endpoint hit by an operator or a preStop hook), not something inferred from a token cancelling.
/// Conflating the two would mean a plain per-workload reassignment could accidentally exclude a
/// perfectly healthy instance from every future assignment, fleet-wide.
/// <para/>
/// Throughout, this runner also keeps <paramref name="workloadStatusStore"/> up to date with the
/// workload's <see cref="WorkloadStatus"/>: <see cref="WorkloadStatus.Active"/> while owned and
/// running (refreshed every <c>renewInterval</c>, doubling as the workload's own heartbeat),
/// <see cref="WorkloadStatus.Transferring"/> for the entire drain wind-down (TTL-bounded by
/// <c>drainTimeout</c>, so a crashed instance can never block a handover longer than it could ever
/// legitimately keep finishing work), and <see cref="WorkloadStatus.Inactive"/> once the lease is
/// released. A coordinator elsewhere uses this to avoid starting a runner for a workload it can see
/// is still <see cref="WorkloadStatus.Transferring"/> off another instance.
/// </remarks>
internal sealed class LeasedWorkerRunner(
    ILogger logger,
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
{
    /// <summary>
    /// Drives the leader-election loop for the workload.
    /// </summary>
    /// <param name="stoppingToken">
    /// Signals that this runner should stop — a coordinator reassigned this workload elsewhere, or
    /// the whole host is shutting down; the runner treats both identically. Cancelling this token
    /// winds the loop down gracefully, including the <c>drainTimeout</c>-bounded wait for an
    /// in-flight worker. It never marks the whole instance as draining in the backing store; only an
    /// explicit external call to <see cref="IInstanceRegistry.BeginDrainAsync"/> does that.
    /// </param>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        Task? workerTask = null;
        CancellationTokenSource? workerTokenSource = null;
        DateTimeOffset? drainStartedAtUtc = null;
        var draining = instanceRegistry.IsDraining;

        // Shared by every place below that transitions into drain mode, so the cooperative push
        // signal (RequestDrain) and the WorkloadStatus write can never drift apart.
        async Task enterDrainModeAsync()
        {
            draining = true;
            drainStartedAtUtc = DateTimeOffset.UtcNow;
            // Call the drain function on actual workload
            drainable?.RequestDrain();

            // Transferring's TTL is drainTimeout - the same hard ceiling this runner force-cancels
            // at below - so a status write that's never followed up (this instance crashes mid-drain)
            // can never block a handover for longer than this runner itself was ever allowed to hang
            // onto the work.
            await workloadStatusStore.SetStatusAsync(
                workloadKey, WorkloadStatus.Transferring, instanceIdentityProvider.InstanceId, drainTimeout, CancellationToken.None);
        }

        LeasedWorkerRunnerLog.RunnerStarting(logger, displayName, instanceIdentityProvider.InstanceId);

        try
        {
            // This loop is the leader-election state machine for the workload.
            while (true)
            {
                // Either this runner's own token was cancelled (reassignment or host shutdown - it
                // makes no difference here) or the instance was independently marked draining
                // elsewhere (e.g. an operator's /drain endpoint). Same graceful wind-down either way.
                if (!draining && (stoppingToken.IsCancellationRequested || instanceRegistry.IsDraining))
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
                    // Defensive: stoppingToken flipped to cancelled between the top-of-loop check
                    // and this call. Same graceful wind-down as above.
                    await enterDrainModeAsync();
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

                // Refreshed every renewInterval alongside the lease itself, for as long as this
                // instance genuinely owns the lease and the worker is running - this is what lets a
                // coordinator elsewhere treat WorkloadStatus.Active as the workload's own heartbeat.
                // CancellationToken.None, not stoppingToken: stoppingToken can flip to cancelled
                // between the top-of-loop drain check and here, and this write - unlike the lease
                // renewal above - has no surrounding catch to turn that into a graceful transition.
                if (ownsLease && workerTask is not null && !draining)
                {
                    await workloadStatusStore.SetStatusAsync(
                        workloadKey, WorkloadStatus.Active, instanceIdentityProvider.InstanceId, leaseTtl, CancellationToken.None);
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
                    // stoppingToken fired during the delay - enter the same graceful wind-down as above.
                    await enterDrainModeAsync();
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

            // Explicit terminal write rather than just letting Active/Transferring lapse, so a
            // handover unblocks the instant this runner actually stops instead of waiting out
            // whichever TTL that last-written status still had left.
            await workloadStatusStore.SetStatusAsync(
                workloadKey, WorkloadStatus.Inactive, instanceIdentityProvider.InstanceId, leaseTtl, CancellationToken.None);

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
