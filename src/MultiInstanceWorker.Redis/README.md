# MultiInstanceWorker.Redis

Redis (StackExchange.Redis) implementations of the three storage contracts
[`MultiInstanceWorker`](https://www.nuget.org/packages/MultiInstanceWorker) needs, so you don't have
to write them yourself:

- `RedisLeaseManager` (`ILeaseManager`) — one string key per workload holding the owner's instance
  id, with the key expiry as the lease TTL. Acquire/renew and release are single Lua scripts, so
  they're atomic.
- `RedisInstanceRegistry` (`IInstanceRegistry`) — heartbeats in a sorted set, draining instances in
  a set, write-once join times in a hash.
- `RedisWorkloadStatusStore` (`IWorkloadStatusStore`) — one hash per workload with a key expiry,
  plus a sorted-set index so every live workload can be listed.

This package depends only on `MultiInstanceWorker.Abstractions` (the contracts) - not on the engine
package.

## Usage

```csharp
services.AddMultiInstanceWorkerRedis(options =>
{
    options.ConnectionString = "localhost:6379";
    options.KeyPrefix = "my-app";
    options.InstanceHeartbeatTtl = TimeSpan.FromSeconds(15);
});

services.AddWorkloadCoordinator(/* ... from the MultiInstanceWorker package ... */);
```

Or bind the options from configuration, and set anything else in the optional callback:

```csharp
services.AddMultiInstanceWorkerRedis(
    configuration.GetSection(RedisWorkerOptions.SectionName), // "Redis"
    options => options.InstanceHeartbeatTtl = TimeSpan.FromSeconds(15));
```

All three adapters are registered with `TryAdd`, so your own registration of any of them wins.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `ConnectionString` | none | When set, the adapters use their own private connection, which is not registered as `IConnectionMultiplexer`. When unset, they use the `IConnectionMultiplexer` you registered yourself. |
| `KeyPrefix` | `multiinstance-worker` | Prefix for every key written. Instances must share it to see each other. |
| `InstanceHeartbeatTtl` | none (required) | How long an instance stays active after its last heartbeat. |

`InstanceHeartbeatTtl` has no default on purpose: it must be the same value your workers are timed
against (`LeaderElectionConfig.InstanceHeartbeatTtlMs` in the engine package). Set both from one
source - for example:

```csharp
services
    .AddOptions<RedisWorkerOptions>()
    .Configure<IOptions<LeaderElectionConfig>>((redis, timing) =>
        redis.InstanceHeartbeatTtl = TimeSpan.FromMilliseconds(timing.Value.InstanceHeartbeatTtlMs));
```

## Using it without the engine package

`RedisInstanceRegistry` needs an `IInstanceIdentityProvider`. The engine's `AddLeasedWorker` /
`AddWorkloadCoordinator` register a default one; if you use this package on its own, register
your own implementation.

Full docs: <https://github.com/pavlek1817/multiinstance-worker>.
