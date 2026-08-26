using FluentAssertions;

namespace MultiInstanceWorker.Tests;

internal class ProcessInstanceIdentityProviderTests
{
    [Test]
    public void ConstructCase_ShouldIncludeMachineNameAndProcessId()
    {
        var provider = new ProcessInstanceIdentityProvider();

        provider.InstanceId.Should().StartWith($"{Environment.MachineName}:{Environment.ProcessId}:");
    }

    [Test]
    public void ConstructCase_ShouldProduceDistinctIdsAcrossInstances()
    {
        var first = new ProcessInstanceIdentityProvider();
        var second = new ProcessInstanceIdentityProvider();

        first.InstanceId.Should().NotBe(second.InstanceId);
    }
}
