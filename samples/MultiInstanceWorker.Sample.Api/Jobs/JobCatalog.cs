namespace MultiInstanceWorker.Sample.Api.Jobs;

/// <summary>The two demo worker jobs this sample runs, each leased independently through Redis.</summary>
public static class JobCatalog
{
    public static readonly JobDefinition OrderCleanup = new ("order-cleanup", "Order cleanup job");

    public static readonly JobDefinition InventorySync = new ("inventory-sync", "Inventory sync job");

    public static IReadOnlyCollection<JobDefinition> All { get; } = new[] { OrderCleanup, InventorySync };

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
