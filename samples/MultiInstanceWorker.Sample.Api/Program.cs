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

builder.Services.AddSingleton(sp => new RedisJobExecutionStore(
    sp.GetRequiredService<IConnectionMultiplexer>(),
    sp.GetRequiredService<IOptions<RedisOptions>>(),
    TimeSpan.FromSeconds(sp.GetRequiredService<IOptions<WorkerTimingOptions>>().Value.JobStatsTtlSeconds)));
builder.Services.AddSingleton<JobExecutionTracker>();
builder.Services.AddSingleton<BalancedNamedWorkloadAssigner>();

// A single coordinator hosted service replaces one LeasedWorkerHostedService per job: instead of
// each job racing independently for its own lease (whichever instance gets there first keeps it
// forever, so nothing stops one instance from ending up with every job), the coordinator
// heartbeats, asks BalancedNamedWorkloadAssigner for this instance's even-split slice of
// JobCatalog.All on every tick, and reconciles local LeasedWorkerRunners to match. It also
// subsumes what the sample's old standalone HeartbeatHostedService did (heartbeat + remove-on-
// shutdown), so that service is no longer registered.
builder.Services.AddSingleton<IHostedService>(sp =>
{
    var timing = sp.GetRequiredService<IOptions<WorkerTimingOptions>>().Value;
    var tracker = sp.GetRequiredService<JobExecutionTracker>();
    var identity = sp.GetRequiredService<IInstanceIdentityProvider>();
    var jobLogger = sp.GetRequiredService<ILogger<SampleWorkerJob>>();

    // One SampleWorkerJob per catalog entry, reused for the app's lifetime: each is both the
    // workload (executeAsync) and the coordinator's cooperative drain target (drainable) for its
    // job, so RequestDrain and the ticking loop always share the same drain flag even as the
    // coordinator starts and stops the runner across reassignments.
    var jobsByWorkloadKey = JobCatalog.All.ToDictionary(
        job => job.WorkloadKey,
        job => new SampleWorkerJob(job.Name, tracker, identity, jobLogger));

    return new WorkloadCoordinatorHostedService<JobCatalog.JobDefinition>(
        sp.GetRequiredService<ILogger<WorkloadCoordinatorHostedService<JobCatalog.JobDefinition>>>(),
        sp.GetRequiredService<ILoggerFactory>(),
        sp.GetRequiredService<IInstanceRegistry>(),
        identity,
        sp.GetRequiredService<ILeaseManager>(),
        sp.GetRequiredService<BalancedNamedWorkloadAssigner>(),
        JobCatalog.All,
        keySelector: job => job.WorkloadKey,
        displayNameSelector: job => job.DisplayName,
        executeAsync: (job, ct) => jobsByWorkloadKey[job.WorkloadKey].RunAsync(ct),
        leaseTtl: TimeSpan.FromSeconds(timing.LeaseTtlSeconds),
        renewInterval: TimeSpan.FromMilliseconds(timing.LeaseRenewIntervalMs),
        drainTimeout: TimeSpan.FromSeconds(timing.DrainTimeoutSeconds),
        heartbeatInterval: TimeSpan.FromMilliseconds(timing.InstanceHeartbeatIntervalMs),
        drainableSelector: job => jobsByWorkloadKey[job.WorkloadKey]);
});

var app = builder.Build();

app.MapGet("/health", (IInstanceIdentityProvider identity) => Results.Ok(new { instanceId = identity.InstanceId }));

app.MapGet("/diagnostics", async (IInstanceRegistry instanceRegistry, JobExecutionTracker tracker, CancellationToken ct) =>
{
    // Pulled straight from Redis, keyed by JobCatalog.All rather than "whatever this instance has
    // touched" - so this shows the same fleet-wide picture (current owner, total ticks) whichever
    // instance answers, not just the jobs this instance happens to be running.
    var jobs = await tracker.SnapshotAllAsync(JobCatalog.All.Select(job => job.Name), ct);
    return Results.Ok(new DiagnosticsResponse(instanceRegistry.InstanceId, instanceRegistry.IsDraining, jobs));
});

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
