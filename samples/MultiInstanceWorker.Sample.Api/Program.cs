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

// RedisInstanceRegistry only needs the lease/heartbeat/drain timing, so it depends on the core
// LeaderElectionConfig rather than the sample-specific WorkerTimingOptions - bound from the same
// "WorkerTiming" section so both option types stay in sync.
builder.Services
    .AddOptions<LeaderElectionConfig>()
    .Bind(builder.Configuration.GetSection(WorkerTimingOptions.SectionName));

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var redisOptions = sp.GetRequiredService<IOptions<RedisOptions>>().Value;
    return ConnectionMultiplexer.Connect(redisOptions.ConnectionString);
});

// The core package is provider-agnostic - it does not know about Redis. These two adapters are
// what a consuming application supplies itself, per the README. (IInstanceIdentityProvider isn't
// listed here: AddWorkloadCoordinator below defaults it to the stock ProcessInstanceIdentityProvider
// via TryAdd, and nothing in this sample needs a custom one.)
builder.Services.AddSingleton<ILeaseManager, RedisLeaseManager>();
builder.Services.AddSingleton<IInstanceRegistry, RedisInstanceRegistry>();

builder.Services.AddSingleton<IJobExecutionStore>(sp => new RedisJobExecutionStore(
    sp.GetRequiredService<IConnectionMultiplexer>(),
    sp.GetRequiredService<IOptions<RedisOptions>>(),
    TimeSpan.FromSeconds(sp.GetRequiredService<IOptions<WorkerTimingOptions>>().Value.JobStatsTtlSeconds)));
builder.Services.AddSingleton<JobExecutionTracker>();

// One SampleWorkerJob per catalog entry, reused for the app's lifetime: each is both the workload
// (executeAsync) and the coordinator's cooperative drain target (drainable) for its job, so
// RequestDrain and the ticking loop always share the same drain flag even as the coordinator
// starts and stops the runner across reassignments. Registered as a singleton so every
// AddWorkloadCoordinator delegate call below resolves the same cached dictionary.
builder.Services.AddSingleton<IReadOnlyDictionary<string, SampleWorkerJob>>(sp =>
{
    var tracker = sp.GetRequiredService<JobExecutionTracker>();
    var identity = sp.GetRequiredService<IInstanceIdentityProvider>();
    var jobLogger = sp.GetRequiredService<ILogger<SampleWorkerJob>>();

    return JobCatalog.All.ToDictionary(
        job => job.WorkloadKey,
        job => new SampleWorkerJob(job.Name, tracker, identity, jobLogger));
});

// Two coordinator hosted services - one per JobCatalog group - replace one LeasedWorkerHostedService
// per job: instead of each job racing independently for its own lease (whichever instance gets
// there first keeps it forever, so nothing stops one instance from ending up with every job), each
// coordinator heartbeats, asks its own IWorkloadAssigner for this instance's slice of its group on
// every tick, and reconciles local LeasedWorkerRunners to match. Both also subsume what the
// sample's old standalone HeartbeatHostedService did (heartbeat + remove-on-shutdown), so that
// service is no longer registered.
//
// JobCatalog.Balanced uses the default IWorkloadAssigner (BalancedNamedWorkloadAssigner - spread
// evenly across live instances); JobCatalog.Primary passes PrimaryNodeWorkloadAssigner explicitly
// to run active/passive instead, all on whichever instance joined earliest. Registering
// AddWorkloadCoordinator twice is safe - each call appends its own IHostedService rather than
// replacing the other, and the two groups' WorkloadKeys never collide.
builder.Services.AddWorkloadCoordinator(
    JobCatalog.Balanced,
    keySelector: job => job.WorkloadKey,
    displayNameSelector: job => job.DisplayName,
    executeAsync: (sp, job, ct) => sp.GetRequiredService<IReadOnlyDictionary<string, SampleWorkerJob>>()[job.WorkloadKey].RunAsync(ct),
    configFactory: sp => sp.GetRequiredService<IOptions<WorkerTimingOptions>>().Value,
    drainableSelector: (sp, job) => sp.GetRequiredService<IReadOnlyDictionary<string, SampleWorkerJob>>()[job.WorkloadKey]);

builder.Services.AddWorkloadCoordinator(
    JobCatalog.Primary,
    keySelector: job => job.WorkloadKey,
    displayNameSelector: job => job.DisplayName,
    executeAsync: (sp, job, ct) => sp.GetRequiredService<IReadOnlyDictionary<string, SampleWorkerJob>>()[job.WorkloadKey].RunAsync(ct),
    configFactory: sp => sp.GetRequiredService<IOptions<WorkerTimingOptions>>().Value,
    drainableSelector: (sp, job) => sp.GetRequiredService<IReadOnlyDictionary<string, SampleWorkerJob>>()[job.WorkloadKey],
    workloadAssigner: new PrimaryNodeWorkloadAssigner());

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
{
    var activeInstances = await instanceRegistry.GetActiveInstancesAsync(ct);
    return Results.Ok(activeInstances.Select(x => x.InstanceId));
});

// Lets a test (or an operator) trigger this instance's drain without tearing down the whole
// host, demonstrating the "operator-triggered drain ahead of downsizing" case from the README.
app.MapPost("/drain", async (IInstanceRegistry instanceRegistry, CancellationToken ct) =>
{
    await instanceRegistry.BeginDrainAsync(ct);
    return Results.Accepted();
});

app.Run();
