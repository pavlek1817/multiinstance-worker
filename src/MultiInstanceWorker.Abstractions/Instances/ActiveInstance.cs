namespace MultiInstanceWorker;

/// <summary>
/// One instance currently visible to <see cref="IInstanceRegistry.GetActiveInstancesAsync"/>.
/// </summary>
public sealed record ActiveInstance
{
    /// <summary>The instance's unique id.</summary>
    required public string InstanceId { get; init; }

    /// <summary>
    /// When this instance was first seen by the registry - written once and never refreshed by
    /// subsequent heartbeats, unlike the heartbeat record itself. Lets an <see cref="IWorkloadAssigner"/>
    /// (e.g. <see cref="PrimaryNodeWorkloadAssigner"/>) pick a stable, seniority-ordered instance
    /// instead of one that depends on how instance ids happen to sort.
    /// </summary>
    required public DateTimeOffset JoinedAtUtc { get; init; }
}
