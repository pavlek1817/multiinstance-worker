using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using MultiInstanceWorker;
using MultiInstanceWorker.Redis;
using MultiInstanceWorker.Sample.Api.Diagnostics;
using MultiInstanceWorker.Sample.Api.Jobs;

var builder = WebApplication.CreateBuilder(args);

// Enums (WorkloadStatus, in /diagnostics) as strings rather than numbers - readable without the
// caller having to know the enum's underlying values.
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

builder.Services
    .AddOptions<LeaderElectionConfig>()
    .Bind(builder.Configuration.GetSection("WorkerTiming"))
    .Validate(
        options =>
        {
            options.Validate();
            return true;
        },
        "Invalid WorkerTiming configuration.");

// The core package is provider-agnostic - it does not know about Redis. MultiInstanceWorker.Redis
// supplies the ILeaseManager/IInstanceRegistry/IWorkloadStatusStore it needs, connecting with the
// "Redis" section's ConnectionString. (IInstanceIdentityProvider isn't registered here:
// AddWorkloadCoordinator below defaults it to the stock ProcessInstanceIdentityProvider via TryAdd,
// and nothing in this sample needs a custom one.)
builder.Services.AddMultiInstanceWorkerRedis(builder.Configuration.GetSection(RedisWorkerOptions.SectionName));

// The Redis package doesn't know LeaderElectionConfig (it shares only contracts with the core
// package), so the heartbeat TTL its instance registry expires instances by is handed over here,
// from the same WorkerTiming section the coordinators below are timed by - one source, no drift.
builder.Services
    .AddOptions<RedisWorkerOptions>()
    .Configure<IOptions<LeaderElectionConfig>>((redis, timing) =>
        redis.InstanceHeartbeatTtl = TimeSpan.FromMilliseconds(timing.Value.InstanceHeartbeatTtlMs));

// One SampleWorkerJob per catalog entry, reused for the app's lifetime: each is both the workload
// (executeAsync) and the coordinator's cooperative drain target (drainable) for its job, so
// RequestDrain and the ticking loop always share the same drain flag even as the coordinator
// starts and stops the runner across reassignments. Registered as a singleton so every
// AddWorkloadCoordinator delegate call below resolves the same cached dictionary.
builder.Services.AddSingleton<IReadOnlyDictionary<string, SampleWorkerJob>>(sp =>
{
    var identity = sp.GetRequiredService<IInstanceIdentityProvider>();
    var jobLogger = sp.GetRequiredService<ILogger<SampleWorkerJob>>();

    return JobCatalog.All.ToDictionary(
        job => job.WorkloadKey,
        job => new SampleWorkerJob(job.Name, identity, jobLogger));
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
    configFactory: sp => sp.GetRequiredService<IOptions<LeaderElectionConfig>>().Value,
    drainableSelector: (sp, job) => sp.GetRequiredService<IReadOnlyDictionary<string, SampleWorkerJob>>()[job.WorkloadKey]);

builder.Services.AddWorkloadCoordinator(
    JobCatalog.Primary,
    keySelector: job => job.WorkloadKey,
    displayNameSelector: job => job.DisplayName,
    executeAsync: (sp, job, ct) => sp.GetRequiredService<IReadOnlyDictionary<string, SampleWorkerJob>>()[job.WorkloadKey].RunAsync(ct),
    configFactory: sp => sp.GetRequiredService<IOptions<LeaderElectionConfig>>().Value,
    drainableSelector: (sp, job) => sp.GetRequiredService<IReadOnlyDictionary<string, SampleWorkerJob>>()[job.WorkloadKey],
    workloadAssigner: new PrimaryNodeWorkloadAssigner());

var app = builder.Build();

app.MapGet("/health", (IInstanceIdentityProvider identity) => Results.Ok(new { instanceId = identity.InstanceId }));

app.MapGet("/diagnostics", async (IInstanceRegistry instanceRegistry, IWorkloadStatusStore workloadStatusStore, CancellationToken ct) =>
{
    // Pulled straight from Redis via GetAllAsync (not filtered to JobCatalog.All) - the same
    // fleet-wide picture (status, current owner) whichever instance answers, not just the
    // workloads this instance happens to be running.
    var workloads = await workloadStatusStore.GetAllAsync(ct);
    return Results.Ok(new DiagnosticsResponse(instanceRegistry.InstanceId, instanceRegistry.IsDraining, workloads));
});

app.MapGet("/instances", async (IInstanceRegistry instanceRegistry, CancellationToken ct) =>
{
    var activeInstances = await instanceRegistry.GetActiveInstancesAsync(ct);
    return Results.Ok(activeInstances.Select(x => x.InstanceId));
});

// Lets a test (or an operator - e.g. a Kubernetes preStop hook, called just before SIGTERM) trigger
// this instance's drain without tearing down the whole host, demonstrating the "operator-triggered
// drain ahead of downsizing" case from the README. This is the ONLY place in this sample that calls
// BeginDrainAsync: neither LeasedWorkerRunner nor WorkloadCoordinatorHostedService ever call it
// themselves - marking the whole instance draining fleet-wide is a deliberate, external act, not
// something the library infers from a cancellation token. A bare SIGTERM without a preceding call
// here still winds every workload down gracefully (same drainTimeout-bounded treatment), it just
// won't proactively exclude this instance from new assignments - that only happens once its
// heartbeat naturally expires.
app.MapPost("/drain", async (IInstanceRegistry instanceRegistry, CancellationToken ct) =>
{
    await instanceRegistry.BeginDrainAsync(ct);
    return Results.Accepted();
});

app.Run();
