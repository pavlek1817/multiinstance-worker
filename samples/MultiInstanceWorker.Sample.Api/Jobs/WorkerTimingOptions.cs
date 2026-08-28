namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// <see cref="LeaderElectionConfig"/> (which already covers lease/heartbeat/drain timing) plus the
/// one extra knob this sample needs, so it can bind all of its worker timing from one
/// "WorkerTiming" configuration section.
/// </summary>
public sealed record WorkerTimingOptions : LeaderElectionConfig
{
    public const string SectionName = "WorkerTiming";

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

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.JobStatsTtlSeconds, 0, nameof(this.JobStatsTtlSeconds));
    }
}
