namespace MultiInstanceWorker;

/// <summary>
/// Generates a process-scoped instance id for lease ownership and heartbeats.
/// </summary>
public sealed class ProcessInstanceIdentityProvider : IInstanceIdentityProvider
{
    public ProcessInstanceIdentityProvider()
    {
        // The GUID keeps ids unique even when machine name and process id repeat.
        this.InstanceId = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.CreateVersion7()}";
    }

    public string InstanceId { get; }
}
