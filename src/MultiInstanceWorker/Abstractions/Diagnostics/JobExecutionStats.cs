namespace MultiInstanceWorker;

/// <summary>Point-in-time execution state for one job, as recorded by an <see cref="IJobExecutionStore"/>.</summary>
public sealed record JobExecutionStats
{
    /// <summary>The job's key, as passed to <see cref="IJobExecutionStore.RecordTickAsync"/>.</summary>
    required public string JobKey { get; init; }

    /// <summary>The instance id that recorded the most recent tick, or <see langword="null"/> if none has ever been recorded.</summary>
    public string? OwnerInstanceId { get; init; }

    /// <summary>The total number of ticks recorded for this job.</summary>
    public long TickCount { get; init; }

    /// <summary>When the most recent tick was recorded, or <see langword="null"/> if none has ever been recorded.</summary>
    public DateTimeOffset? LastTickAtUtc { get; init; }
}
