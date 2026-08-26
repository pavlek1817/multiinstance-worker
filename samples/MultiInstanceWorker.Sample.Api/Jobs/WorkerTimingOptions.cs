using MultiInstanceWorker;

namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// <see cref="LeaderElectionConfig"/> plus the drain-timeout ceiling that
/// <see cref="LeasedWorkerHostedService"/> also needs, so the sample can bind all of its worker
/// timing knobs from one "WorkerTiming" configuration section.
/// </summary>
public sealed record WorkerTimingOptions : LeaderElectionConfig
{
    public const string SectionName = "WorkerTiming";

    /// <summary>How long a draining runner lets its worker finish on its own before force-cancelling it.</summary>
    public int DrainTimeoutSeconds { get; init; }

    public override void Validate()
    {
        base.Validate();

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.DrainTimeoutSeconds, 0, nameof(this.DrainTimeoutSeconds));
    }
}
