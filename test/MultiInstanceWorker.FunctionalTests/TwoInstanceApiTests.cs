using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace MultiInstanceWorker.FunctionalTests;

/// <summary>
/// Runs two real instances of the sample API - each hosting the same two worker jobs - against
/// one shared Redis, and proves the thing this whole exercise is about: the Redis lease keeps
/// exactly one instance running each job at a time, and the other instance takes over once the
/// owner stops.
/// </summary>
internal sealed class TwoInstanceApiTests
{
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(15);

    private static readonly string[] JobNames = { "order-cleanup", "inventory-sync" };

    private static readonly JsonSerializerOptions JsonOptions = new (JsonSerializerDefaults.Web);

    private SampleApiFactory instanceA = null!;
    private SampleApiFactory instanceB = null!;
    private HttpClient clientA = null!;
    private HttpClient clientB = null!;

    [SetUp]
    public void SetUp()
    {
        var keyPrefix = $"test-{Guid.NewGuid():N}";
        this.instanceA = new SampleApiFactory(keyPrefix);
        this.instanceB = new SampleApiFactory(keyPrefix);
        this.clientA = this.instanceA.CreateClient();
        this.clientB = this.instanceB.CreateClient();
    }

    [TearDown]
    public async Task TearDown()
    {
        this.clientA.Dispose();
        this.clientB.Dispose();
        await this.instanceA.DisposeAsync();
        await this.instanceB.DisposeAsync();
    }

    [Test]
    public async Task BothJobs_EventuallyGetExactlyOneOwnerAcrossTheTwoInstances()
    {
        foreach (var jobName in JobNames)
        {
            await this.waitUntilAsync(async () => await this.countOwnersAsync(jobName) == 1);
        }
    }

    [Test]
    public async Task NeitherJob_IsEverOwnedByBothInstancesAtOnce()
    {
        // Sample over a window instead of taking one snapshot - a one-off race would still show
        // up as a moment where both instances believe they own the same job.
        var deadline = DateTimeOffset.UtcNow.Add(PollTimeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            foreach (var jobName in JobNames)
            {
                var owners = await this.countOwnersAsync(jobName);
                owners.Should().BeLessThanOrEqualTo(1, $"the Redis lease for '{jobName}' must never be held by both instances at once");
            }

            await Task.Delay(50);
        }
    }

    [Test]
    public async Task WhenTheOwningInstanceDrains_TheOtherInstanceTakesOverBothJobs()
    {
        // Let ownership settle first: every job must have converged to exactly one owner before
        // draining one of the instances.
        foreach (var jobName in JobNames)
        {
            await this.waitUntilAsync(async () => await this.countOwnersAsync(jobName) == 1);
        }

        var aOwnsOrderCleanup = await isOwnerAsync(this.clientA, "order-cleanup");
        var (drainingClient, survivingClient) = aOwnsOrderCleanup ? (this.clientA, this.clientB) : (this.clientB, this.clientA);

        using var drainResponse = await drainingClient.PostAsync("/drain", content: null);
        drainResponse.EnsureSuccessStatusCode();

        await this.waitUntilAsync(async () =>
        {
            var diagnostics = await getDiagnosticsAsync(survivingClient);
            return JobNames.All(jobName => diagnostics.Jobs.SingleOrDefault(j => j.Name == jobName)?.IsRunningHere == true);
        });
    }

    private static async Task<DiagnosticsDto> getDiagnosticsAsync(HttpClient client)
    {
        using var response = await client.GetAsync("/diagnostics");
        response.EnsureSuccessStatusCode();
        var diagnostics = await response.Content.ReadFromJsonAsync<DiagnosticsDto>(JsonOptions);
        return diagnostics ?? throw new InvalidOperationException("/diagnostics returned an empty body.");
    }

    private static async Task<bool> isOwnerAsync(HttpClient client, string jobName)
    {
        var diagnostics = await getDiagnosticsAsync(client);
        return diagnostics.Jobs.SingleOrDefault(j => j.Name == jobName)?.IsRunningHere ?? false;
    }

    private async Task<int> countOwnersAsync(string jobName)
    {
        var ownsOnA = await isOwnerAsync(this.clientA, jobName);
        var ownsOnB = await isOwnerAsync(this.clientB, jobName);
        return (ownsOnA ? 1 : 0) + (ownsOnB ? 1 : 0);
    }

    private async Task waitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTimeOffset.UtcNow.Add(PollTimeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(100);
        }

        Assert.Fail($"Condition was not met within {PollTimeout}.");
    }

    /// <summary>Deserialization contract for the sample's <c>/diagnostics</c> endpoint - deliberately
    /// separate from the sample's own DTO types, since a functional test should treat the HTTP API
    /// as an external contract rather than share the server's exact types.</summary>
    private sealed class DiagnosticsDto
    {
        [JsonPropertyName("instanceId")]
        public string InstanceId { get; set; } = string.Empty;

        [JsonPropertyName("isDraining")]
        public bool IsDraining { get; set; }

        [JsonPropertyName("jobs")]
        public List<JobSnapshotDto> Jobs { get; set; } = new ();
    }

    private sealed class JobSnapshotDto
    {
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        [JsonPropertyName("isRunningHere")]
        public bool IsRunningHere { get; set; }

        [JsonPropertyName("tickCount")]
        public long TickCount { get; set; }
    }

    /// <summary>
    /// Hosts one instance of the sample API in-process, pointed at the shared Redis test
    /// container and a caller-supplied key prefix so two factories in the same test contend for
    /// the same lease/heartbeat keys (simulating two real deployed instances) while different
    /// tests stay isolated from each other.
    /// </summary>
    private sealed class SampleApiFactory(string keyPrefix)
        : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, configBuilder) => configBuilder.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Redis:ConnectionString"] = RedisTestFixture.ConnectionString,
                ["Redis:KeyPrefix"] = keyPrefix,

                // Fast timing so ownership, renewal, and drain all converge quickly in tests.
                ["WorkerTiming:LeaseTtlSeconds"] = "2",
                ["WorkerTiming:LeaseRenewIntervalMs"] = "200",
                ["WorkerTiming:InstanceHeartbeatTtlSeconds"] = "2",
                ["WorkerTiming:InstanceHeartbeatIntervalMs"] = "200",
                ["WorkerTiming:DrainTimeoutSeconds"] = "5",

                // The per-tick job logging is useful when debugging a run by hand, but at
                // 250ms/tick across two instances and multiple tests it drowns out everything
                // else in a test run.
                ["Logging:LogLevel:MultiInstanceWorker"] = "Warning",
            }));
        }
    }
}
