namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>
/// The demo worker jobs this sample runs, each leased independently through Redis. Split into two
/// groups so <c>Program.cs</c> can register two <c>WorkloadCoordinatorHostedService</c>s with
/// different <see cref="IWorkloadAssigner"/> strategies: <see cref="Balanced"/> jobs are spread
/// evenly across the live instances, while <see cref="Primary"/> jobs are all owned by a single
/// active/passive instance.
/// </summary>
public static class JobCatalog
{
    public static readonly JobDefinition OrderCleanup = new ("order-cleanup", "Order cleanup job");

    public static readonly JobDefinition InventorySync = new ("inventory-sync", "Inventory sync job");

    public static readonly JobDefinition EmailDigest = new ("email-digest", "Email digest job");

    public static readonly JobDefinition CacheWarmup = new ("cache-warmup", "Cache warmup job");

    public static readonly JobDefinition BillingReconciliation = new ("billing-reconciliation", "Billing reconciliation job");

    public static readonly JobDefinition NightlyReport = new ("nightly-report", "Nightly report job");

    /// <summary>Jobs assigned via <c>BalancedNamedWorkloadAssigner</c> - spread evenly across the live instances.</summary>
    public static IReadOnlyCollection<JobDefinition> Balanced { get; } =
        new[] { OrderCleanup, InventorySync, EmailDigest, CacheWarmup };

    /// <summary>
    /// Jobs assigned via <c>PrimaryNodeWorkloadAssigner</c> - active/passive, all owned by
    /// whichever instance joined earliest until it drops out and a survivor takes over.
    /// </summary>
    public static IReadOnlyCollection<JobDefinition> Primary { get; } =
        new[] { BillingReconciliation, NightlyReport };

    public static IReadOnlyCollection<JobDefinition> All { get; } = Balanced.Concat(Primary).ToArray();

    public sealed class JobDefinition(string name, string displayName)
    {
        public string Name { get; } = name;

        public string DisplayName { get; } = displayName;

        /// <summary>The Redis lease name for this job - distinct from <see cref="Name"/> so the
        /// two concerns (what the lease key looks like vs. what the job is called in
        /// diagnostics/logs) can diverge later without it being a breaking rename.</summary>
        public string WorkloadKey => $"job:{this.Name}";
    }
}
