namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>Source-generated log messages for <see cref="SampleWorkerJob"/>.</summary>
internal static partial class SampleWorkerJobLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "{JobName} observed draining and is stopping itself.")]
    public static partial void ObservedDraining(ILogger logger, string jobName);

    [LoggerMessage(Level = LogLevel.Information, Message = "{JobName} ticked on instance {InstanceId}.")]
    public static partial void Tick(ILogger logger, string jobName, string instanceId);
}
