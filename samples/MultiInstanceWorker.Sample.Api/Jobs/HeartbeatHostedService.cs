using Microsoft.Extensions.Options;

namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// The "coordinator" piece the README says a consumer has to write itself: keeps this
/// instance's heartbeat alive in the <see cref="IInstanceRegistry"/> while the host is up, and
/// removes it on shutdown.
/// </summary>
public sealed class HeartbeatHostedService(
    IInstanceRegistry instanceRegistry,
    IOptions<WorkerTimingOptions> timingOptions,
    ILogger<HeartbeatHostedService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromMilliseconds(timingOptions.Value.InstanceHeartbeatIntervalMs);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await instanceRegistry.HeartbeatAsync(stoppingToken);
                await Task.Delay(interval, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            try
            {
                await instanceRegistry.RemoveCurrentAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to remove this instance from the registry on shutdown.");
            }
        }
    }
}
