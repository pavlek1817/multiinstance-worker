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

                await this.reconcileRunnersAsync(assignedWorkloads, stoppingToken, instanceRegistry.IsDraining);
                await this.throwIfAnyRunnerFaultedAsync();

                await Task.Delay(heartbeatInterval, stoppingToken);
            }
        }
        finally
        {
            var isDraining = stoppingToken.IsCancellationRequested || instanceRegistry.IsDraining;
            if (isDraining)
            {
                await instanceRegistry.BeginDrainAsync(CancellationToken.None);
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
    private async Task reconcileRunnersAsync(
        IReadOnlyCollection<TWorkload> assignedWorkloads,
        CancellationToken stoppingToken,
        bool isDraining)
    {
        var assignedByKey = assignedWorkloads.ToDictionary(keySelector);

        var removedKeys = this.activeRunners.Keys
            .Where(key => !assignedByKey.ContainsKey(key))
            .ToArray();

        foreach (var removedKey in removedKeys)
        {
            // A workload can drop out of this instance's assignment either because it was
            // reassigned elsewhere (a plain rebalance) or because this instance itself started
            // draining. Only the latter gets the graceful "let it finish" treatment — a plain
            // rebalance while this instance is healthy stops the runner immediately.
            if (isDraining && this.activeRunners.TryGetValue(removedKey, out var state) && !state.ExecutionTask.IsCompleted)
            {
                continue;
            }

            if (isDraining && this.activeRunners.TryGetValue(removedKey, out var completedState) && completedState.ExecutionTask.IsCompleted)
            {
                await this.removeCompletedRunnerAsync(removedKey);
                continue;
            }

            await this.stopRunnerAsync(removedKey);
        }

        foreach (var workload in assignedByKey.Values)
        {
            var key = keySelector(workload);
            if (this.activeRunners.ContainsKey(key))
            {
                continue;
            }

            WorkloadCoordinatorLog.WorkloadAssigned(logger, key);

            // Use an independent CTS — NOT linked to the host stoppingToken.
            // This lets the coordinator cancel only this runner (e.g. workload reassignment)
            // without triggering instance-level drain logic inside the runner.
            // The host stoppingToken is passed separately as shutdownToken so the runner can
            // distinguish "I was reassigned" from "the whole instance is shutting down."
            var runnerTokenSource = new CancellationTokenSource();
            var runner = new LeasedWorkerRunner(
                loggerFactory.CreateLogger<WorkloadCoordinatorHostedService<TWorkload>>(),
                leaseManager,
                instanceIdentityProvider,
                instanceRegistry,
                workloadKey: key,
                displayName: displayNameSelector(workload),
                leaseTtl: leaseTtl,
                renewInterval: renewInterval,
                drainTimeout: drainTimeout,
                executeAsync: ct => executeAsync(workload, ct),
                drainable: drainableSelector?.Invoke(workload));

            // Pass the per-runner token as stoppingToken (cancelled on reassignment)
            // and the host stoppingToken as shutdownToken (cancelled on instance shutdown).
            this.activeRunners[key] = new RunnerState(
                workload,
                runnerTokenSource,
                Task.Run(() => runner.RunAsync(runnerTokenSource.Token, shutdownToken: stoppingToken), CancellationToken.None));
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
