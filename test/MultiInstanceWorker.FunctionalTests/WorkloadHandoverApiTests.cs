using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MultiInstanceWorker.Sample.Api.Redis;
using StackExchange.Redis;

namespace MultiInstanceWorker.FunctionalTests;

/// <summary>
/// Reproduces the scenario this whole handover mechanism exists for: one instance running two
/// workloads, a second instance joining, and one of the two workloads needing to move across - but
/// only once its current, uninterruptible unit of work actually finishes. Uses its own pair of
/// workloads (not <c>JobCatalog</c>'s) registered directly on the test's own
/// <see cref="WebApplicationFactory{TEntryPoint}"/>, specifically so one of them can take several
/// real seconds per unit of work without slowing down every other functional test that boots the
/// sample app. Observes ownership straight through <see cref="IWorkloadStatusStore"/> - the same
/// store <c>LeasedWorkerRunner</c> writes to on its own, so nothing in this test's workload bodies
/// needs to record anything itself.
/// </summary>
internal sealed class WorkloadHandoverApiTests
{
    private const string FastWorkloadKey = "handover-a";

    private const string SlowWorkloadKey = "handover-b";

    private static readonly TimeSpan SlowUnitOfWork = TimeSpan.FromSeconds(3);

    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(15);

    private static readonly JsonSerializerOptions JsonOptions = new (JsonSerializerDefaults.Web);

    private string keyPrefix = null!;
    private IConnectionMultiplexer connectionMultiplexer = null!;
    private RedisWorkloadStatusStore statusStore = null!;
    private HandoverApiFactory? instanceA;
    private HandoverApiFactory? instanceB;
    private HttpClient? clientA;
    private HttpClient? clientB;

    [SetUp]
    public void SetUp()
    {
        this.keyPrefix = $"test-{Guid.NewGuid():N}";
        this.connectionMultiplexer = ConnectionMultiplexer.Connect(RedisTestFixture.ConnectionString);

        // Reads only - the running instances' own LeasedWorkerRunner writes through its DI-registered
        // IWorkloadStatusStore; this is just how the test observes that same Redis-backed state from
        // outside the app.
        this.statusStore = new RedisWorkloadStatusStore(
            this.connectionMultiplexer,
            Options.Create(new RedisOptions { KeyPrefix = this.keyPrefix }));
    }

    [TearDown]
    public async Task TearDown()
    {
        this.clientA?.Dispose();
        this.clientB?.Dispose();

        if (this.instanceA is not null)
        {
            await this.instanceA.DisposeAsync();
        }

        if (this.instanceB is not null)
        {
            await this.instanceB.DisposeAsync();
        }

        this.connectionMultiplexer.Dispose();
    }

    [Test]
    public async Task SlowWorkloadReassignedToNewInstance_FinishesOnTheOldInstanceBeforeStartingOnTheNew()
    {
        // Only instance-a is up: with a single active instance, the balanced assigner puts both
        // workloads on it.
        this.instanceA = new HandoverApiFactory(this.keyPrefix);
        this.clientA = this.instanceA.CreateClient();
        var instanceAId = await getInstanceIdAsync(this.clientA);

        await this.waitUntilAsync(async () => await this.ownerAsync(FastWorkloadKey) == instanceAId);
        await this.waitUntilAsync(async () => await this.ownerAsync(SlowWorkloadKey) == instanceAId);

        // instance-b joins, sharing the same Redis and key prefix - the balanced assigner now wants
        // handover-b (sorted after handover-a) on instance-b, while handover-a stays put.
        this.instanceB = new HandoverApiFactory(this.keyPrefix);
        this.clientB = this.instanceB.CreateClient();
        var instanceBId = await getInstanceIdAsync(this.clientB);

        // Give the reassignment plenty of time to be noticed - well inside the slow workload's own
        // 3-second unit of work - but it must still be finishing up on instance-a, not yet started
        // on instance-b: the coordinator on instance-b must not even start a runner for it while its
        // WorkloadStatus is still Transferring off instance-a.
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        (await this.ownerAsync(SlowWorkloadKey)).Should().Be(
            instanceAId, "handover-b's current unit of work must finish on instance-a before instance-b starts it");

        // Only once it actually finishes does instance-b pick it up.
        await this.waitUntilAsync(async () => await this.ownerAsync(SlowWorkloadKey) == instanceBId);

        // handover-a was never part of the reassignment - it must have stayed on instance-a the
        // whole time, undisturbed by handover-b's handover.
        (await this.ownerAsync(FastWorkloadKey)).Should().Be(instanceAId);
    }

    private static async Task<string> getInstanceIdAsync(HttpClient client)
    {
        using var response = await client.GetAsync("/health");
        response.EnsureSuccessStatusCode();
        var health = await response.Content.ReadFromJsonAsync<HealthDto>(JsonOptions);
        return health?.InstanceId ?? throw new InvalidOperationException("/health returned an empty body.");
    }

    // Deliberately NOT passed `ct` for the slow workload - a single unit of work that must run to
    // completion once started, mirroring a job that can't be safely chopped up mid-flight. This is
    // what gives the test a reliable window in which to observe "reassigned, but still finishing
    // here". LeasedWorkerRunner keeps the lease renewed and the WorkloadStatus at Transferring
    // throughout, regardless of what this workload body does.
    private static Task runHandoverWorkloadAsync(IServiceProvider sp, string workloadKey, CancellationToken ct) =>
        workloadKey == SlowWorkloadKey
            ? Task.Delay(SlowUnitOfWork)
            : Task.Delay(Timeout.Infinite, ct);

    private async Task<string?> ownerAsync(string workloadKey)
    {
        var statuses = await this.statusStore.GetStatusesAsync(new[] { workloadKey }, CancellationToken.None);
        return statuses.TryGetValue(workloadKey, out var record) ? record.OwnerInstanceId : null;
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

    /// <summary>Deserialization contract for the sample's <c>/health</c> endpoint.</summary>
    private sealed class HealthDto
    {
        [JsonPropertyName("instanceId")]
        public string InstanceId { get; set; } = string.Empty;
    }

    /// <summary>
    /// Hosts one instance of the sample API, same as <see cref="TwoInstanceApiTests"/>'s own
    /// factory, plus one extra <c>WorkloadCoordinatorHostedService</c> registered directly here -
    /// not in <c>Program.cs</c> - for this test's own two workloads (<see cref="FastWorkloadKey"/>,
    /// <see cref="SlowWorkloadKey"/>). Keeping it scoped to just this factory, rather than adding it
    /// to the sample app itself, means no other functional test pays for the slow workload's
    /// multi-second unit of work or the extra time its graceful shutdown takes.
    /// </summary>
    private sealed class HandoverApiFactory(string keyPrefix)
        : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, configBuilder) => configBuilder.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Redis:ConnectionString"] = RedisTestFixture.ConnectionString,
                ["Redis:KeyPrefix"] = keyPrefix,

                // Fast enough that the reassignment itself is noticed quickly, but DrainTimeoutMs
                // stays comfortably above SlowUnitOfWork so the slow workload always finishes on its
                // own rather than being force-cancelled.
                ["WorkerTiming:LeaseTtlMs"] = "1000",
                ["WorkerTiming:LeaseRenewIntervalMs"] = "100",
                ["WorkerTiming:InstanceHeartbeatTtlMs"] = "1000",
                ["WorkerTiming:InstanceHeartbeatIntervalMs"] = "100",
                ["WorkerTiming:DrainTimeoutMs"] = "5000",
                ["Logging:LogLevel:MultiInstanceWorker"] = "Warning",
            }));

            // AddSingleton<IHostedService>(factory) internally, same as Program.cs's own
            // AddWorkloadCoordinator calls, so this appends a third coordinator rather than
            // replacing either of theirs - and resolves ILeaseManager/IInstanceRegistry/
            // IWorkloadStatusStore/IInstanceIdentityProvider from the exact same DI container
            // Program.cs already wired up to Redis, so this exercises the real gating mechanism.
            builder.ConfigureServices(services => services.AddWorkloadCoordinator(
                new[] { FastWorkloadKey, SlowWorkloadKey },
                keySelector: key => key,
                displayNameSelector: key => key,
                executeAsync: runHandoverWorkloadAsync,
                configFactory: sp => sp.GetRequiredService<IOptions<LeaderElectionConfig>>().Value));
        }
    }
}
