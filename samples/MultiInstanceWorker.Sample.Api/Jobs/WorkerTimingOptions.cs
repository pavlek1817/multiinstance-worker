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

    /// <summary>
    /// How long a job's Redis-backed execution stats (owner, tick count, last tick time) survive
    /// without a tick refreshing them, before they expire - so <c>/diagnostics</c> stops showing a
    /// job as owned by an instance that isn't actually running it anymore. Refreshed on every tick,
    /// so this only matters once a job stops ticking for good.
    /// </summary>
    public int JobStatsTtlSeconds { get; init; }

    public override void Validate()
    {
        base.Validate();

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.DrainTimeoutSeconds, 0, nameof(this.DrainTimeoutSeconds));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.JobStatsTtlSeconds, 0, nameof(this.JobStatsTtlSeconds));
    }
}
