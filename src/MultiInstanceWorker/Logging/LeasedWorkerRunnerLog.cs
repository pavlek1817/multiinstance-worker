using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// Source-generated log messages for <see cref="LeasedWorkerRunner"/>.
/// </summary>
internal static partial class LeasedWorkerRunnerLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Leader-election runner starting for {DisplayName} on instance {InstanceId}.")]
    public static partial void RunnerStarting(ILogger logger, string displayName, string instanceId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Lease acquired for {DisplayName}. Starting worker.")]
    public static partial void LeaseAcquired(ILogger logger, string displayName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Lease lost for {DisplayName}. Stopping worker.")]
    public static partial void LeaseLost(ILogger logger, string displayName);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leader-election runner stopped for {DisplayName}.")]
    public static partial void RunnerStopped(ILogger logger, string displayName);
}
