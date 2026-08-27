using Microsoft.Extensions.Options;
using MultiInstanceWorker;
using MultiInstanceWorker.Sample.Api.Diagnostics;
using MultiInstanceWorker.Sample.Api.Jobs;
using MultiInstanceWorker.Sample.Api.Redis;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddOptions<RedisOptions>()
    .Bind(builder.Configuration.GetSection(RedisOptions.SectionName));

builder.Services
    .AddOptions<WorkerTimingOptions>()
    .Bind(builder.Configuration.GetSection(WorkerTimingOptions.SectionName))
    .Validate(
        options =>
        {
            options.Validate();
            return true;
        },
        "Invalid WorkerTiming configuration.");

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var redisOptions = sp.GetRequiredService<IOptions<RedisOptions>>().Value;
    return ConnectionMultiplexer.Connect(redisOptions.ConnectionString);
});

// The core package is provider-agnostic - it does not know about Redis. These two adapters are
// what a consuming application supplies itself, per the README.
builder.Services.AddSingleton<IInstanceIdentityProvider, ProcessInstanceIdentityProvider>();
builder.Services.AddSingleton<ILeaseManager, RedisLeaseManager>();
builder.Services.AddSingleton<IInstanceRegistry>(sp => new RedisInstanceRegistry(
    sp.GetRequiredService<IConnectionMultiplexer>(),
    sp.GetRequiredService<IOptions<RedisOptions>>(),
    sp.GetRequiredService<IInstanceIdentityProvider>(),
    TimeSpan.FromSeconds(sp.GetRequiredService<IOptions<WorkerTimingOptions>>().Value.InstanceHeartbeatTtlSeconds)));

builder.Services.AddSingleton<JobExecutionTracker>();
builder.Services.AddHostedService<HeartbeatHostedService>();

foreach (var job in JobCatalog.All)
{
    // Not AddHostedService(factory) here: it (like the parameterless AddHostedService<T>())
    // de-duplicates by implementation type via TryAddEnumerable, so a second registration of
    // the same LeasedWorkerHostedService type - one per job - would be silently dropped.
    // AddSingleton<IHostedService>(factory) always appends, which is what multiple jobs need.
    builder.Services.AddSingleton<IHostedService>(sp =>
    {
        var timing = sp.GetRequiredService<IOptions<WorkerTimingOptions>>().Value;

        // One instance per job, reused for the app's lifetime: it's both the workload
        // (executeAsync) and the runner's cooperative drain target (drainable), so RequestDrain
        // and the ticking loop share the same drain flag.
        var sampleJob = new SampleWorkerJob(
            job.Name,
            sp.GetRequiredService<JobExecutionTracker>(),
            sp.GetRequiredService<IInstanceIdentityProvider>(),
            sp.GetRequiredService<ILogger<LeasedWorkerHostedService>>());

        return new LeasedWorkerHostedService(
            sp.GetRequiredService<ILogger<LeasedWorkerHostedService>>(),
            sp.GetRequiredService<ILeaseManager>(),
            sp.GetRequiredService<IInstanceIdentityProvider>(),
            sp.GetRequiredService<IInstanceRegistry>(),
            workloadKey: job.WorkloadKey,
            displayName: job.DisplayName,
            leaseTtl: TimeSpan.FromSeconds(timing.LeaseTtlSeconds),
            renewInterval: TimeSpan.FromMilliseconds(timing.LeaseRenewIntervalMs),
            drainTimeout: TimeSpan.FromSeconds(timing.DrainTimeoutSeconds),
            executeAsync: sampleJob.RunAsync,
            drainable: sampleJob);
    });
}

var app = builder.Build();

app.MapGet("/health", (IInstanceIdentityProvider identity) => Results.Ok(new { instanceId = identity.InstanceId }));

app.MapGet("/diagnostics", (IInstanceRegistry instanceRegistry, JobExecutionTracker tracker) =>
    Results.Ok(new DiagnosticsResponse(instanceRegistry.InstanceId, instanceRegistry.IsDraining, tracker.SnapshotAll())));

app.MapGet("/instances", async (IInstanceRegistry instanceRegistry, CancellationToken ct) =>
    Results.Ok(await instanceRegistry.GetActiveInstanceIdsAsync(ct)));

// Lets a test (or an operator) trigger this instance's drain without tearing down the whole
// host, demonstrating the "operator-triggered drain ahead of downsizing" case from the README.
app.MapPost("/drain", async (IInstanceRegistry instanceRegistry, CancellationToken ct) =>
{
    await instanceRegistry.BeginDrainAsync(ct);
    return Results.Accepted();
});

app.Run();
