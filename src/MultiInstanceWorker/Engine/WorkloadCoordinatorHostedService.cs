using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Coordinates ownership of a set of named workloads across instances and starts leased runners
/// for whichever of them this instance owns.
/// </summary>
/// <remarks>
/// This is the generic sharded-workload counterpart to <see cref="LeasedWorkerHostedService"/>
/// (which handles a single singleton workload). On each tick it renews this instance's heartbeat,
/// asks the <see cref="IWorkloadAssigner"/> which of <paramref name="workloads"/> belong here, and
/// reconciles local <see cref="LeasedWorkerRunner"/> instances against that assignment — starting
/// runners for newly assigned workloads and stopping ones that are no longer owned.
/// <para/>
/// Workload construction and execution stay entirely in <paramref name="executeAsync"/>, supplied
/// by the consumer — this class knows nothing about what a workload actually does.
/// </remarks>
/// <typeparam name="TWorkload">The consumer-defined workload descriptor type.</typeparam>
/// <param name="logger">Logger for this coordinator's own start/stop/assignment events.</param>
/// <param name="loggerFactory">
/// Used to create a logger for each per-workload <see cref="LeasedWorkerRunner"/> this coordinator
/// starts, since those aren't resolved from DI individually.
/// </param>
/// <param name="instanceRegistry">Tracks this instance's heartbeat, drain state, and the active-instance set the assigner divides workloads across.</param>
/// <param name="instanceIdentityProvider">This process's unique instance id, used to prove lease/heartbeat ownership.</param>
/// <param name="leaseManager">Grants each assigned workload's lease to exactly one instance at a time.</param>
/// <param name="workloadStatusStore">
/// Tracks each workload's <see cref="WorkloadStatus"/> across the fleet. Written by each local
/// <see cref="LeasedWorkerRunner"/> as it moves through its lifecycle, and read here before starting
/// a newly assigned workload: one still <see cref="WorkloadStatus.Transferring"/> off another
/// instance is left for a later tick instead of starting a runner that would just poll for a lease
/// it cannot get yet. The lease itself, not this, remains the actual safety mechanism.
/// </param>
/// <param name="workloadAssigner">
/// Decides which of <paramref name="workloads"/> belong to this instance on each tick (e.g.
/// <see cref="BalancedNamedWorkloadAssigner"/> or <see cref="PrimaryNodeWorkloadAssigner"/>). This
/// is a liveness/efficiency decision, not the safety mechanism - <paramref name="leaseManager"/>
/// still enforces exclusive ownership underneath it.
/// </param>
/// <param name="workloads">The full, static catalog of workloads this coordinator divides up across instances.</param>
/// <param name="keySelector">Extracts each workload's unique, stable key (used as its lease key and runner identity).</param>
/// <param name="displayNameSelector">Extracts each workload's human-readable name, for logs.</param>
/// <param name="executeAsync">Runs one assigned workload for as long as this instance owns its lease.</param>
/// <param name="leaseTtl">How long a workload's lease remains valid before another instance can take it over.</param>
/// <param name="renewInterval">How often the owning instance renews each workload's lease.</param>
/// <param name="drainTimeout">
/// How long a draining or reassigned-away workload is allowed to finish on its own before this
/// coordinator force-cancels it.
/// </param>
/// <param name="heartbeatInterval">How often this coordinator refreshes the instance heartbeat and re-evaluates the assignment.</param>
/// <param name="drainableSelector">
/// Optional: resolves the <see cref="IDrainableService"/> for a workload, if it has one, so it
/// receives <see cref="IDrainableService.RequestDrain"/> the moment its runner enters drain mode.
/// </param>
public sealed class WorkloadCoordinatorHostedService<TWorkload>(
    ILogger<WorkloadCoordinatorHostedService<TWorkload>> logger,
    ILoggerFactory loggerFactory,
    IInstanceRegistry instanceRegistry,
    IInstanceIdentityProvider instanceIdentityProvider,
    ILeaseManager leaseManager,
    IWorkloadStatusStore workloadStatusStore,
    IWorkloadAssigner workloadAssigner,
    IReadOnlyCollection<TWorkload> workloads,
    Func<TWorkload, string> keySelector,
    Func<TWorkload, string> displayNameSelector,
    Func<TWorkload, CancellationToken, Task> executeAsync,
    TimeSpan leaseTtl,
    TimeSpan renewInterval,
    TimeSpan drainTimeout,
    TimeSpan heartbeatInterval,
    Func<TWorkload, IDrainableService?>? drainableSelector = null)
    : BackgroundService
{
    private readonly Dictionary<string, RunnerState> activeRunners = new ();

    /// <summary>
    /// Refreshes the instance heartbeat, recomputes workload ownership, and reconciles local runners.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        WorkloadCoordinatorLog.CoordinatorStarting(logger, instanceIdentityProvider.InstanceId);

        try
        {
            // The coordinator repeats the heartbeat/assign/reconcile cycle for as long as the host is alive.
            while (!stoppingToken.IsCancellationRequested)
            {
                await instanceRegistry.HeartbeatAsync(stoppingToken);

                var activeInstances = await instanceRegistry.GetActiveInstancesAsync(stoppingToken);
                var assignedWorkloads = workloadAssigner.GetAssignedWorkloads(
                    workloads,
                    keySelector,
                    activeInstances,
                    instanceRegistry.InstanceId);

                await this.reconcileRunnersAsync(assignedWorkloads, stoppingToken);
                await this.throwIfAnyRunnerFaultedAsync();

                await Task.Delay(heartbeatInterval, stoppingToken);
            }
        }
        finally
        {
            // Marking the instance draining in the backing store is never this coordinator's call to
            // make - that is a deliberate, external act (e.g. a /drain endpoint or a preStop hook),
            // not something inferred from stoppingToken cancelling. isDraining here only decides
            // whether to wait for runners to finish gracefully or force-stop them.
            var isDraining = stoppingToken.IsCancellationRequested || instanceRegistry.IsDraining;
            if (isDraining)
            {
                await this.waitForAllAsync();
            }
            else
            {
                await this.stopAllAsync();
            }

            await instanceRegistry.RemoveCurrentAsync(CancellationToken.None);

            WorkloadCoordinatorLog.CoordinatorStopped(logger, instanceIdentityProvider.InstanceId);
        }
    }

    /// <summary>
    /// Starts runners for newly assigned workloads and stops runners that are no longer owned.
    /// </summary>
    /// <remarks>
    /// A workload can drop out of this instance's assignment either because it was reassigned
    /// elsewhere (a plain rebalance) or because this instance itself started draining — either way
    /// it gets the same graceful "let it finish" treatment: <see cref="LeasedWorkerRunner"/> treats
    /// a plain reassignment the same as a drain internally (finishes the in-flight worker on its
    /// own, up to its <c>drainTimeout</c>, without marking the whole instance draining). Removal
    /// itself is fire-and-forget from here — this method must never block waiting for a removed
    /// runner to finish, since that would stall this instance's own heartbeat/reconcile loop (and
    /// so risk the rest of the fleet seeing a perfectly healthy instance as dead) for as long as the
    /// removed workload takes to wind down. A runner that already finished is cleaned up
    /// immediately; one still running is left alone and picked up on a later tick.
    /// <para/>
    /// Starting is additionally gated on <see cref="WorkloadStatus"/>: a newly assigned workload
    /// whose last-known status is still <see cref="WorkloadStatus.Transferring"/> off some other
    /// instance is left unstarted this tick rather than spun up as a runner doomed to poll for a
    /// lease it cannot get yet. This is a cheap, observable short-circuit on top of the lease, not a
    /// substitute for it — the lease is still what actually prevents a double-run if the status
    /// store is stale.
    /// </remarks>
    private async Task reconcileRunnersAsync(
        IReadOnlyCollection<TWorkload> assignedWorkloads,
        CancellationToken stoppingToken)
    {
        var assignedByKey = assignedWorkloads.ToDictionary(keySelector);

        var workloadToRemoveKeys = this.activeRunners.Keys
            .Where(key => !assignedByKey.ContainsKey(key))
            .ToArray();

        foreach (var removedKey in workloadToRemoveKeys)
        {
            if (!this.activeRunners.TryGetValue(removedKey, out var state))
            {
                continue;
            }

            if (state.ExecutionTask.IsCompleted)
            {
                await this.removeCompletedRunnerAsync(removedKey);
                continue;
            }

            // Cancel() is idempotent, so it's safe to call again on every tick a removed workload
            // is still winding down - only log the first time so a long wind-down doesn't spam.
            if (!state.CancellationTokenSource.IsCancellationRequested)
            {
                WorkloadCoordinatorLog.WorkloadRemoved(logger, removedKey);
                state.CancellationTokenSource.Cancel();
            }
        }

        var keysNeedingStart = assignedByKey.Keys
            .Where(key => !this.activeRunners.ContainsKey(key))
            .ToArray();

        // Batched once per tick, not per workload - GetStatusesAsync already reads its keys in
        // parallel, and there's no need for a round trip per newly assigned workload.
        var statuses = keysNeedingStart.Length == 0
            ? new Dictionary<string, WorkloadStatusRecord>()
            : await workloadStatusStore.GetStatusesAsync(keysNeedingStart, stoppingToken);

        foreach (var workload in assignedByKey.Values)
        {
            var key = keySelector(workload);
            if (this.activeRunners.ContainsKey(key))
            {
                continue;
            }

            if (statuses.TryGetValue(key, out var status)
                && status.Status == WorkloadStatus.Transferring
                && !string.Equals(status.OwnerInstanceId, instanceRegistry.InstanceId, StringComparison.Ordinal))
            {
                // Still finishing up on another instance - starting a runner here now would just
                // poll for a lease it can't get yet. Leave it for a later tick; the lease itself
                // (not this check) is what actually prevents a double-run if this status is stale.
                WorkloadCoordinatorLog.WorkloadTransferPending(logger, key, status.OwnerInstanceId);
                continue;
            }

            WorkloadCoordinatorLog.WorkloadAssigned(logger, key);

            // Linked to the host's stoppingToken: cancelling either fires the one token the runner
            // watches, and the runner treats both causes identically (see LeasedWorkerRunner's own
            // remarks) - reassignment cancels just this runner's own source directly (below, in the
            // removal loop), a real host shutdown cancels every linked source at once via the parent.
            var runnerTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var runner = new LeasedWorkerRunner(
                loggerFactory.CreateLogger<WorkloadCoordinatorHostedService<TWorkload>>(),
                leaseManager,
                instanceIdentityProvider,
                instanceRegistry,
                workloadStatusStore,
                workloadKey: key,
                displayName: displayNameSelector(workload),
                leaseTtl: leaseTtl,
                renewInterval: renewInterval,
                drainTimeout: drainTimeout,
                executeAsync: ct => executeAsync(workload, ct),
                drainable: drainableSelector?.Invoke(workload));

            this.activeRunners[key] = new RunnerState(
                workload,
                runnerTokenSource,
                Task.Run(() => runner.RunAsync(runnerTokenSource.Token), CancellationToken.None));
        }
    }

    /// <summary>Waits for any active runner to finish so faults surface immediately.</summary>
    private async Task throwIfAnyRunnerFaultedAsync()
    {
        foreach (var runner in this.activeRunners.Values)
        {
            if (runner.ExecutionTask.IsCompleted)
            {
                await runner.ExecutionTask;
            }
        }
    }

    /// <summary>Stops and removes every active runner during shutdown.</summary>
    private async Task stopAllAsync()
    {
        var keys = this.activeRunners.Keys.ToArray();
        foreach (var key in keys)
        {
            await this.stopRunnerAsync(key);
        }
    }

    /// <summary>Waits for all active runners to finish without cancelling them.</summary>
    private async Task waitForAllAsync()
    {
        var keys = this.activeRunners.Keys.ToArray();
        foreach (var key in keys)
        {
            if (this.activeRunners.TryGetValue(key, out var state))
            {
                await state.ExecutionTask;
                state.CancellationTokenSource.Dispose();
                this.activeRunners.Remove(key);
            }
        }
    }

    /// <summary>Stops one workload runner and disposes its cancellation source.</summary>
    private async Task stopRunnerAsync(string workloadKey)
    {
        if (!this.activeRunners.Remove(workloadKey, out var state))
        {
            return;
        }

        WorkloadCoordinatorLog.WorkloadRemoved(logger, workloadKey);
        state.CancellationTokenSource.Cancel();

        try
        {
            await state.ExecutionTask;
        }
        catch (OperationCanceledException) when (state.CancellationTokenSource.IsCancellationRequested)
        {
        }
        finally
        {
            state.CancellationTokenSource.Dispose();
        }
    }

    /// <summary>Removes a runner that already completed naturally.</summary>
    private async Task removeCompletedRunnerAsync(string workloadKey)
    {
        if (!this.activeRunners.Remove(workloadKey, out var state))
        {
            return;
        }

        await state.ExecutionTask;
        state.CancellationTokenSource.Dispose();
    }

    private sealed class RunnerState(
        TWorkload workload,
        CancellationTokenSource cancellationTokenSource,
        Task executionTask)
    {
        public TWorkload Workload { get; } = workload;

        public CancellationTokenSource CancellationTokenSource { get; } = cancellationTokenSource;

        public Task ExecutionTask { get; } = executionTask;
    }
}
