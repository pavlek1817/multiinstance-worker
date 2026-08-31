using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Source-generated log messages for <see cref="WorkloadCoordinatorHostedService{TWorkload}"/>.
/// </summary>
internal static partial class WorkloadCoordinatorLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Workload coordinator starting on instance {InstanceId}.")]
    public static partial void CoordinatorStarting(ILogger logger, string instanceId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Workload coordinator stopped on instance {InstanceId}.")]
    public static partial void CoordinatorStopped(ILogger logger, string instanceId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Assigning workload {WorkloadKey} to this instance.")]
    public static partial void WorkloadAssigned(ILogger logger, string workloadKey);

    [LoggerMessage(Level = LogLevel.Information, Message = "Removing workload {WorkloadKey} from this instance.")]
    public static partial void WorkloadRemoved(ILogger logger, string workloadKey);

    [LoggerMessage(Level = LogLevel.Information, Message = "Deferring start of workload {WorkloadKey}: still transferring off instance {OwnerInstanceId}.")]
    public static partial void WorkloadTransferPending(ILogger logger, string workloadKey, string ownerInstanceId);
}
