using System.Collections.Concurrent;

namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// In-memory record of what each worker job is doing on this instance, purely so the
/// <c>/diagnostics</c> endpoint has something to report. Not part of the leader-election
/// mechanism itself - the Redis lease is the only thing that actually decides ownership.
/// </summary>
public sealed class JobExecutionTracker
{
    private readonly ConcurrentDictionary<string, State> states = new (StringComparer.Ordinal);

    public void MarkStarted(string jobName) => this.states.AddOrUpdate(
        jobName,
        _ => new State(true, 0, null),
        (_, existing) => new State(true, existing.TickCount, existing.LastTickAtUtc));

    public void MarkStopped(string jobName) => this.states.AddOrUpdate(
        jobName,
        _ => new State(false, 0, null),
        (_, existing) => new State(false, existing.TickCount, existing.LastTickAtUtc));

    public void RecordTick(string jobName) => this.states.AddOrUpdate(
        jobName,
        _ => new State(true, 1, DateTimeOffset.UtcNow),
        (_, existing) => new State(true, existing.TickCount + 1, DateTimeOffset.UtcNow));

    public JobSnapshot Snapshot(string jobName) => this.states.TryGetValue(jobName, out var state)
        ? new JobSnapshot(jobName, state.IsRunningHere, state.TickCount, state.LastTickAtUtc)
        : new JobSnapshot(jobName, false, 0, null);

    public IReadOnlyCollection<JobSnapshot> SnapshotAll() => this.states
        .Select(kvp => new JobSnapshot(kvp.Key, kvp.Value.IsRunningHere, kvp.Value.TickCount, kvp.Value.LastTickAtUtc))
        .ToArray();

    private sealed class State(bool isRunningHere, long tickCount, DateTimeOffset? lastTickAtUtc)
    {
        public bool IsRunningHere { get; } = isRunningHere;

        public long TickCount { get; } = tickCount;

        public DateTimeOffset? LastTickAtUtc { get; } = lastTickAtUtc;
    }
}

/// <summary>Point-in-time view of one job's execution state on this instance, for <c>/diagnostics</c>.</summary>
public sealed class JobSnapshot(string name, bool isRunningHere, long tickCount, DateTimeOffset? lastTickAtUtc)
{
    public string Name { get; } = name;

    public bool IsRunningHere { get; } = isRunningHere;

    public long TickCount { get; } = tickCount;

    public DateTimeOffset? LastTickAtUtc { get; } = lastTickAtUtc;
}
