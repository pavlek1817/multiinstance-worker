namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// The workload body handed to <see cref="WorkloadCoordinatorHostedService{TWorkload}"/> for each
/// of the sample's two jobs. It doesn't do anything domain-specific - it just ticks on an interval
/// and records that it did, so <c>/diagnostics</c> can show which instance is currently running it.
/// </summary>
/// <remarks>
/// Deliberately doesn't depend on <see cref="IInstanceRegistry"/> - that's a leader-election
/// concern, not something a workload should need to know about just to find out it should wrap
/// up. Instead it implements <see cref="IDrainableService"/>, and whoever registers this job as a
/// workload (see <c>Program.cs</c>) also passes it as the coordinator's <c>drainableSelector</c>
/// result, so <see cref="RequestDrain"/> gets called for it automatically when its runner enters
/// drain mode.
/// </remarks>
public sealed class SampleWorkerJob(
    string jobName,
    JobExecutionTracker tracker,
    IInstanceIdentityProvider instanceIdentityProvider,
    ILogger logger)
    : IDrainableService
{
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(250);

    private volatile bool draining;

    /// <inheritdoc/>
    /// <remarks>
    /// One-way, same as the runner's own drain flag: once a runner starts draining it never goes
    /// back to normal operation, so there's no need for this job to un-drain either.
    /// </remarks>
    public void RequestDrain() => this.draining = true;

    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // The runner calls RequestDrain() the moment it enters drain mode; checking that
                // flag here (between ticks) is what lets this job wrap up on its own well before
                // the runner's drainTimeout would force it via cancellation.
                if (this.draining)
                {
                    SampleWorkerJobLog.ObservedDraining(logger, jobName);
                    break;
                }

                // Written straight through to Redis (owner, tick count, last tick time) so
                // /diagnostics on either instance sees the same fleet-wide state for this job,
                // not just what happened to run locally.
                var tickCount = await tracker.RecordTickAsync(jobName, ct);
                SampleWorkerJobLog.Tick(logger, jobName, tickCount, instanceIdentityProvider.InstanceId);

                await Task.Delay(TickInterval, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
    }
}
