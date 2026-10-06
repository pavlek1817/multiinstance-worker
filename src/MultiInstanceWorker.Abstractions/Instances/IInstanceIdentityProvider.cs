namespace MultiInstanceWorker;

/// <summary>
/// Provides the unique identity used by this process for leader election.
/// </summary>
public interface IInstanceIdentityProvider
{
    /// <summary>Gets the unique id that represents the current process instance.</summary>
    string InstanceId { get; }
}
