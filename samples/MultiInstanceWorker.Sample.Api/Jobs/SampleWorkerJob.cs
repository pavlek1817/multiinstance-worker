using MultiInstanceWorker;

namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// The workload body handed to <see cref="LeasedWorkerHostedService"/> for each of the sample's
/// two jobs. It doesn't do anything domain-specific - it just ticks on an interval and records
/// that it did, so <c>/diagnostics</c> can show which instance is currently running it.
/// </summary>
public static class SampleWorkerJob
{
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(250);

    public static async Task RunAsync(
        string jobName,
        JobExecutionTracker tracker,
        IInstanceRegistry instanceRegistry,
        ILogger logger,
        CancellationToken ct)
    {
        tracker.MarkStarted(jobName);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Best practice from the README: a cooperative workload checks IsDraining itself
                // at a safe boundary (here, between ticks) and stops on its own, rather than
                // relying solely on the runner's drainTimeout to force it.
                if (instanceRegistry.IsDraining)
                {
                    SampleWorkerJobLog.ObservedDraining(logger, jobName);
                    break;
                }

                tracker.RecordTick(jobName);
                var snapshot = tracker.Snapshot(jobName);
                SampleWorkerJobLog.Tick(logger, jobName, snapshot.TickCount, instanceRegistry.InstanceId);

                await Task.Delay(TickInterval, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        finally
        {
            tracker.MarkStopped(jobName);
        }
    }
}
