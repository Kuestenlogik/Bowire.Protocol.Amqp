// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Testcontainers.RabbitMq;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests.Integration;

/// <summary>
/// Spins up a RabbitMQ container with the Management plugin enabled —
/// this is the canonical 0.9.1 + management-API surface the plugin's
/// discovery path needs. Image pinned to <c>:3-management</c> to keep
/// the integration suite stable against upstream major bumps; refresh
/// in lockstep with the live samples / docs.
/// </summary>
/// <remarks>
/// Test classes wear <c>IClassFixture&lt;RabbitMqContainerFixture&gt;</c>
/// and read <see cref="AmqpUrl"/> + <see cref="ManagementPort"/> off
/// the fixture instead of hard-coding ports. The Testcontainers helper
/// picks ephemeral host ports so parallel suite runs (or a leftover
/// broker from a crashed previous run) don't fight over 5672.
/// Tests using this fixture carry <c>[Trait("Category", "Docker")]</c>
/// so CI can skip them via <c>--filter "Category!=Docker"</c> on
/// hosts without a Docker daemon.
/// </remarks>
public sealed class RabbitMqContainerFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:3-management")
        // Testcontainers.RabbitMq only exposes 5672 (AMQP wire) by
        // default; the management plugin we need for discovery lives
        // on 15672 and must be opened explicitly + bound to an
        // ephemeral host port via assignRandomHostPort: true.
        .WithPortBinding(15672, assignRandomHostPort: true)
        .Build();

    /// <summary>The full <c>amqp://...</c> URL the plugin can use as serverUrl.</summary>
    public string AmqpUrl { get; private set; } = string.Empty;

    /// <summary>The HTTP port the Management plugin is listening on.</summary>
    public int ManagementPort { get; private set; }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        AmqpUrl = _container.GetConnectionString();
        // RabbitMqBuilder exposes 15672 via GetMappedPublicPort.
        ManagementPort = _container.GetMappedPublicPort(15672);
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}
