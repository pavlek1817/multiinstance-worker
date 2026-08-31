namespace MultiInstanceWorker.Sample.Api.Diagnostics;

/// <summary>
/// What this instance currently knows about itself, for the <c>/diagnostics</c> endpoint - plus
/// every workload's status and current owner, pulled straight from <see cref="IWorkloadStatusStore"/>,
/// so this is the same fleet-wide picture whichever instance answers the request.
/// </summary>
public sealed class DiagnosticsResponse(string instanceId, bool isDraining, IReadOnlyCollection<WorkloadStatusRecord> workloads)
{
    public string InstanceId { get; } = instanceId;

    public bool IsDraining { get; } = isDraining;

    public IReadOnlyCollection<WorkloadStatusRecord> Workloads { get; } = workloads;
}
