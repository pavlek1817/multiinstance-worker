using Testcontainers.Redis;

namespace MultiInstanceWorker.FunctionalTests;

/// <summary>
/// Starts one real Redis container (via Testcontainers) for the whole functional test run, and
/// tears it down when the run finishes. Individual tests isolate themselves from each other with
/// a unique key prefix rather than by spinning up a fresh container per test.
/// </summary>
[SetUpFixture]
public sealed class RedisTestFixture
{
    private static RedisContainer? container;

    public static string ConnectionString => container?.GetConnectionString()
        ?? throw new InvalidOperationException("The Redis test container has not been started yet.");

    [OneTimeSetUp]
    public async Task StartContainerAsync()
    {
        container = new RedisBuilder("redis:7.4").Build();
        await container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task StopContainerAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync();
        }
    }
}
