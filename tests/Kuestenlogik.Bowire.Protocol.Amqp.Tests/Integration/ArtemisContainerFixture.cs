// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests.Integration;

/// <summary>
/// A live ActiveMQ Artemis broker for the AMQP 1.0 side, with the
/// addresses the discovery tests expect already created.
/// </summary>
/// <remarks>
/// <para>
/// Testcontainers has no Artemis module, so this is the generic builder
/// against the Apache image. The wire port (5672) is mapped to an
/// ephemeral host port; nothing else is needed — Artemis answers
/// management requests over that same connection, which is the whole
/// point of the path under test. The console on 8161 stays shut.
/// </para>
/// <para>
/// Topology is created with the broker's own CLI after start rather than
/// baked into a broker.xml: one multicast address with two queues (the
/// case where a bare "receive" is not enough) and one anycast queue
/// whose name is its address (the case where it is). Tests using this
/// fixture carry <c>[Trait("Category", "Docker")]</c>.
/// </para>
/// </remarks>
public sealed class ArtemisContainerFixture : IAsyncLifetime
{
    internal const string User = "bowire";
    internal const string Password = "bowire";

    private readonly IContainer _container = new ContainerBuilder("apache/activemq-artemis:2.42.0")
        .WithEnvironment("ARTEMIS_USER", User)
        .WithEnvironment("ARTEMIS_PASSWORD", Password)
        // AMQP acceptor. 61616 (core), 8161 (console) and the rest stay
        // unmapped: this suite speaks 1.0 and nothing else.
        .WithPortBinding(5672, assignRandomHostPort: true)
        // AMQ221007 is the broker saying it is serving; the console
        // lines that follow are a different subsystem and can lag.
        .WithWaitStrategy(Wait.ForUnixContainer()
            .UntilMessageIsLogged("AMQ221007"))
        .Build();

    /// <summary>The <c>amqp1://…</c> URL the plugin dials.</summary>
    public string Amqp10Url { get; private set; } = string.Empty;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        Amqp10Url = $"amqp1://{User}:{Password}@127.0.0.1:{_container.GetMappedPublicPort(5672)}";

        await CreateQueueAsync("harbor.cranes", "harbor", multicast: true, autoCreateAddress: true);
        await CreateQueueAsync("harbor.gates", "harbor", multicast: true, autoCreateAddress: false);
        await CreateQueueAsync("orders", "orders", multicast: false, autoCreateAddress: true);
    }

    private async Task CreateQueueAsync(string name, string address, bool multicast, bool autoCreateAddress)
    {
        string[] command =
        [
            "/var/lib/artemis-instance/bin/artemis", "queue", "create",
            "--name", name, "--address", address,
            multicast ? "--multicast" : "--anycast",
            "--durable", "--preserve-on-no-consumers",
            "--user", User, "--password", Password, "--silent",
            .. autoCreateAddress ? new[] { "--auto-create-address" } : [],
        ];
        var result = await _container.ExecAsync(command);
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"artemis queue create {name} failed ({result.ExitCode}): {result.Stderr}{result.Stdout}");
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}
