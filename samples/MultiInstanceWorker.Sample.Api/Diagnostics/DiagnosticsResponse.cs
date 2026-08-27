using MultiInstanceWorker.Sample.Api.Jobs;

namespace MultiInstanceWorker.Sample.Api.Diagnostics;

/// <summary>What this instance currently knows about itself, for the <c>/diagnostics</c> endpoint.</summary>
public sealed class DiagnosticsResponse(string instanceId, bool isDraining, IReadOnlyCollection<JobSnapshot> jobs)
{
    public string InstanceId { get; } = instanceId;

    public bool IsDraining { get; } = isDraining;

    public IReadOnlyCollection<JobSnapshot> Jobs { get; } = jobs;
}
