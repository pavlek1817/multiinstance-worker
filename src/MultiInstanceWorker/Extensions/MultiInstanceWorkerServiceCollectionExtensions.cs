using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MultiInstanceWorker;

/// <summary>
/// <see cref="IServiceCollection"/> extensions that wire up <see cref="LeasedWorkerHostedService"/>
/// and <see cref="WorkloadCoordinatorHostedService{TWorkload}"/> without a consumer needing to
/// hand-write the <c>sp.GetRequiredService&lt;T&gt;()</c> chain each one's constructor needs.
/// </summary>
/// <remarks>
/// These extensions only wire up what the core package can supply a sensible default for
/// (<see cref="IInstanceIdentityProvider"/> via <see cref="ProcessInstanceIdentityProvider"/>, and
/// for <see cref="AddWorkloadCoordinator{TWorkload}"/>, <see cref="IWorkloadAssigner"/> via
/// <see cref="BalancedNamedWorkloadAssigner"/>) using <c>TryAdd</c>, so a consumer's own
/// registration - made before or after calling these - always wins. <see cref="ILeaseManager"/>,
/// <see cref="IInstanceRegistry"/>, and <see cref="IWorkloadStatusStore"/> are never defaulted: this
/// package is provider-agnostic and has no backing-store implementation to offer, so a consumer must
/// register those against its own store before calling either extension.
/// </remarks>
public static class MultiInstanceWorkerServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="LeasedWorkerHostedService"/> for a single singleton workload.
    /// </summary>
    /// <remarks>
    /// Safe to call more than once for different workloads: each call appends its own
    /// <see cref="IHostedService"/> registration rather than replacing a previous one.
    /// </remarks>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="workloadKey">The workload's unique, stable lease key.</param>
    /// <param name="displayName">The workload's human-readable name, for logs.</param>
    /// <param name="executeAsync">Runs the workload for as long as this instance owns its lease.</param>
    /// <param name="configFactory">
    /// Resolves the lease/heartbeat/drain timing for this workload from <see cref="IServiceProvider"/>
    /// - typically <c>sp =&gt; sp.GetRequiredService&lt;IOptions&lt;YourOptions&gt;&gt;().Value</c>,
    /// so it can come from the framework's options pattern rather than a value computed before the
    /// container is built. <see cref="LeaderElectionConfig.InstanceHeartbeatTtlMs"/> and
    /// <see cref="LeaderElectionConfig.InstanceHeartbeatIntervalMs"/> are not used by a standalone
    /// <see cref="LeasedWorkerHostedService"/> (only <see cref="IInstanceRegistry"/>'s own
    /// implementation cares about those), but the same config type is accepted here so a consumer
    /// can bind one configuration section for every workload.
    /// </param>
    /// <param name="drainableSelector">
    /// Optional: resolves the workload's <see cref="IDrainableService"/>, if it has one, so it
    /// receives <see cref="IDrainableService.RequestDrain"/> the moment this runner enters drain mode.
    /// </param>
    public static IServiceCollection AddLeasedWorker(
        this IServiceCollection services,
        string workloadKey,
        string displayName,
        Func<IServiceProvider, CancellationToken, Task> executeAsync,
        Func<IServiceProvider, LeaderElectionConfig> configFactory,
        Func<IServiceProvider, IDrainableService?>? drainableSelector = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(workloadKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(displayName);
        ArgumentNullException.ThrowIfNull(executeAsync);
        ArgumentNullException.ThrowIfNull(configFactory);

        services.TryAddSingleton<IInstanceIdentityProvider, ProcessInstanceIdentityProvider>();

        // Not AddHostedService<LeasedWorkerHostedService>(): that de-duplicates by implementation
        // type via TryAddEnumerable, so a second AddLeasedWorker call (a second workload) would be
        // silently dropped. AddSingleton<IHostedService>(factory) always appends.
        services.AddSingleton<IHostedService>(sp =>
        {
            var config = configFactory(sp);
            return new LeasedWorkerHostedService(
                sp.GetRequiredService<ILogger<LeasedWorkerHostedService>>(),
                sp.GetRequiredService<ILeaseManager>(),
                sp.GetRequiredService<IInstanceIdentityProvider>(),
                sp.GetRequiredService<IInstanceRegistry>(),
                sp.GetRequiredService<IWorkloadStatusStore>(),
                workloadKey: workloadKey,
                displayName: displayName,
                leaseTtl: TimeSpan.FromMilliseconds(config.LeaseTtlMs),
                renewInterval: TimeSpan.FromMilliseconds(config.LeaseRenewIntervalMs),
                drainTimeout: TimeSpan.FromMilliseconds(config.DrainTimeoutMs),
                executeAsync: ct => executeAsync(sp, ct),
                drainable: drainableSelector?.Invoke(sp));
        });

        return services;
    }

    /// <summary>
    /// Registers a <see cref="WorkloadCoordinatorHostedService{TWorkload}"/> for a set of sharded
    /// workloads.
    /// </summary>
    /// <remarks>
    /// Safe to call more than once for different <typeparamref name="TWorkload"/> types: each call
    /// appends its own <see cref="IHostedService"/> registration rather than replacing a previous one.
    /// </remarks>
    /// <typeparam name="TWorkload">The consumer-defined workload descriptor type.</typeparam>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="workloads">The full, static catalog of workloads to divide up across instances.</param>
    /// <param name="keySelector">Extracts each workload's unique, stable key (used as its lease key and runner identity).</param>
    /// <param name="displayNameSelector">Extracts each workload's human-readable name, for logs.</param>
    /// <param name="executeAsync">Runs one assigned workload for as long as this instance owns its lease.</param>
    /// <param name="configFactory">
    /// Resolves the lease/heartbeat/drain timing shared by every workload this coordinator manages,
    /// from <see cref="IServiceProvider"/> - typically
    /// <c>sp =&gt; sp.GetRequiredService&lt;IOptions&lt;YourOptions&gt;&gt;().Value</c>, so it can
    /// come from the framework's options pattern rather than a value computed before the container
    /// is built.
    /// </param>
    /// <param name="drainableSelector">
    /// Optional: resolves the <see cref="IDrainableService"/> for a workload, if it has one, so it
    /// receives <see cref="IDrainableService.RequestDrain"/> the moment its runner enters drain mode.
    /// </param>
    /// <param name="workloadAssigner">
    /// Optional: the assignment strategy to use (e.g. <see cref="BalancedNamedWorkloadAssigner"/>
    /// or <see cref="PrimaryNodeWorkloadAssigner"/>). Defaults to whatever <see cref="IWorkloadAssigner"/>
    /// is registered in <paramref name="services"/>, falling back to
    /// <see cref="BalancedNamedWorkloadAssigner"/> if none is.
    /// </param>
    public static IServiceCollection AddWorkloadCoordinator<TWorkload>(
        this IServiceCollection services,
        IReadOnlyCollection<TWorkload> workloads,
        Func<TWorkload, string> keySelector,
        Func<TWorkload, string> displayNameSelector,
        Func<IServiceProvider, TWorkload, CancellationToken, Task> executeAsync,
        Func<IServiceProvider, LeaderElectionConfig> configFactory,
        Func<IServiceProvider, TWorkload, IDrainableService?>? drainableSelector = null,
        IWorkloadAssigner? workloadAssigner = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(workloads);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(displayNameSelector);
        ArgumentNullException.ThrowIfNull(executeAsync);
        ArgumentNullException.ThrowIfNull(configFactory);

        services.TryAddSingleton<IInstanceIdentityProvider, ProcessInstanceIdentityProvider>();
        services.TryAddSingleton<IWorkloadAssigner, BalancedNamedWorkloadAssigner>();

        // Not AddHostedService<WorkloadCoordinatorHostedService<TWorkload>>(): that de-duplicates by
        // implementation type via TryAddEnumerable, so a second AddWorkloadCoordinator<TOther> call
        // would be silently dropped whenever TOther happens to close the same generic type.
        // AddSingleton<IHostedService>(factory) always appends.
        services.AddSingleton<IHostedService>(sp =>
        {
            var config = configFactory(sp);
            return new WorkloadCoordinatorHostedService<TWorkload>(
                sp.GetRequiredService<ILogger<WorkloadCoordinatorHostedService<TWorkload>>>(),
                sp.GetRequiredService<ILoggerFactory>(),
                sp.GetRequiredService<IInstanceRegistry>(),
                sp.GetRequiredService<IInstanceIdentityProvider>(),
                sp.GetRequiredService<ILeaseManager>(),
                sp.GetRequiredService<IWorkloadStatusStore>(),
                workloadAssigner ?? sp.GetRequiredService<IWorkloadAssigner>(),
                workloads,
                keySelector: keySelector,
                displayNameSelector: displayNameSelector,
                executeAsync: (workload, ct) => executeAsync(sp, workload, ct),
                leaseTtl: TimeSpan.FromMilliseconds(config.LeaseTtlMs),
                renewInterval: TimeSpan.FromMilliseconds(config.LeaseRenewIntervalMs),
                drainTimeout: TimeSpan.FromMilliseconds(config.DrainTimeoutMs),
                heartbeatInterval: TimeSpan.FromMilliseconds(config.InstanceHeartbeatIntervalMs),
                drainableSelector: drainableSelector is null ? null : workload => drainableSelector(sp, workload));
        });

        return services;
    }
}
