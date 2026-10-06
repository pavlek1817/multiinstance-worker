using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MultiInstanceWorker.Redis;

/// <summary>
/// Resolves the <see cref="IConnectionMultiplexer"/> the Redis adapters talk through: a private one
/// built from <see cref="RedisWorkerOptions.ConnectionString"/> when that is set, otherwise whichever
/// <see cref="IConnectionMultiplexer"/> the application itself registered.
/// </summary>
/// <remarks>
/// A connection built here is never registered as <see cref="IConnectionMultiplexer"/> in the
/// container - an application that also uses Redis for something else must keep getting its own
/// multiplexer from DI, not this one. It is disposed with the container; one borrowed from the
/// application is left alone, since the application owns it.
/// </remarks>
internal sealed class RedisConnectionProvider : IDisposable
{
    private readonly Lazy<IConnectionMultiplexer> connection;
    private readonly bool ownsConnection;

    public RedisConnectionProvider(IServiceProvider serviceProvider, IOptions<RedisWorkerOptions> options)
    {
        var connectionString = options.Value.ConnectionString;
        this.ownsConnection = !string.IsNullOrWhiteSpace(connectionString);

        this.connection = new Lazy<IConnectionMultiplexer>(() => this.ownsConnection
            ? ConnectionMultiplexer.Connect(connectionString!)
            : serviceProvider.GetService<IConnectionMultiplexer>()
                ?? throw new InvalidOperationException(
                    $"No Redis connection is available: set {nameof(RedisWorkerOptions)}.{nameof(RedisWorkerOptions.ConnectionString)}, "
                    + $"or register an {nameof(IConnectionMultiplexer)} in the service collection."));
    }

    public IConnectionMultiplexer Connection => this.connection.Value;

    public void Dispose()
    {
        if (this.ownsConnection && this.connection.IsValueCreated)
        {
            this.connection.Value.Dispose();
        }
    }
}
