// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests.Integration;

/// <summary>
/// AMQP 1.0 discovery against Microsoft's Service Bus emulator: what the
/// namespace says about itself over its ATOM management feed, and the
/// services the plugin builds out of that answer.
/// </summary>
/// <remarks>
/// The cloud service is a paid resource and is not in CI. The emulator
/// serves the same feed from the same paths, so everything up to the
/// signature is proved here; the SAS vector stays in the unit suite,
/// because the emulator accepts any <c>Authorization</c> header it is
/// handed. See <see cref="ServiceBusEmulatorFixture"/>.
/// </remarks>
[Trait("Category", "Docker")]
// A second trait because this one is not priced like the others: the
// emulator needs SQL Server beside it, which is a 2.3 GB pull where
// RabbitMQ and Artemis are under 600 MB. `--filter-not-trait
// "Broker=ServiceBusEmulator"` drops it without dropping Docker.
[Trait("Broker", "ServiceBusEmulator")]
public sealed class ServiceBusDiscoveryE2ETests : IClassFixture<ServiceBusEmulatorFixture>
{
    private readonly ServiceBusEmulatorFixture _emulator;
    private readonly BowireAmqpProtocol _protocol = new();

    public ServiceBusDiscoveryE2ETests(ServiceBusEmulatorFixture emulator) => _emulator = emulator;

    [Fact]
    public async Task Discovery_finds_the_namespaces_queues_and_topics()
    {
        var services = await _protocol.DiscoverAsync(
            _emulator.ServiceBusUrl, showInternalServices: false, ct: TestContext.Current.CancellationToken);

        // Not the generic Broker service: that is the fallback, and this
        // namespace answered.
        Assert.DoesNotContain(services, s => s.Name == BowireAmqpProtocol.BrokerServiceName);
        Assert.Equal(
            ["harbor.gates", "orders", "situation"],
            services.Select(s => s.Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task A_queue_gets_the_bare_pair_of_methods()
    {
        var services = await _protocol.DiscoverAsync(
            _emulator.ServiceBusUrl, showInternalServices: false, ct: TestContext.Current.CancellationToken);

        var orders = services.Single(s => s.Name == "orders");
        Assert.Equal(["receive", "send"], orders.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
        Assert.Equal("Service Bus queue", orders.Description);
    }

    [Fact]
    public async Task A_topic_gets_a_receive_per_subscription()
    {
        var services = await _protocol.DiscoverAsync(
            _emulator.ServiceBusUrl, showInternalServices: false, ct: TestContext.Current.CancellationToken);

        // There is no single subscription a bare "receive" could mean, so
        // each one is named.
        var situation = services.Single(s => s.Name == "situation");
        Assert.Equal(
            ["receive:archive", "receive:ops", "send"],
            situation.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
        Assert.Equal("Service Bus topic (2 subscriptions)", situation.Description);
    }

    [Fact]
    public async Task A_subscriptions_receive_addresses_it_the_way_service_bus_spells_it()
    {
        var services = await _protocol.DiscoverAsync(
            _emulator.ServiceBusUrl, showInternalServices: false, ct: TestContext.Current.CancellationToken);
        var situation = services.Single(s => s.Name == "situation");

        Assert.True(AmqpEndpoint.TryParse(_emulator.ServiceBusUrl, out var endpoint));
        var address = _protocol.ResolveV10Address(endpoint, situation.Name, "receive:ops", metadata: null);

        Assert.Equal("situation/Subscriptions/ops", address);
    }

    [Fact]
    public async Task Discovery_turned_off_leaves_the_generic_broker_service()
    {
        var services = await _protocol.DiscoverAsync(
            _emulator.ServiceBusUrl.Replace(
                "_amqp10Discovery=servicebus", "_amqp10Discovery=none", StringComparison.Ordinal),
            showInternalServices: false, ct: TestContext.Current.CancellationToken);

        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, Assert.Single(services).Name);
    }

    [Fact]
    public async Task An_endpoint_told_it_is_artemis_does_not_read_the_feed()
    {
        // The operator's answer about which broker this is stands even
        // when it is wrong: nothing reads the ATOM feed, the Artemis
        // management address answers nothing, and the generic service is
        // what comes back.
        var services = await _protocol.DiscoverAsync(
            _emulator.ServiceBusUrl.Replace(
                "_amqp10Discovery=servicebus", "_amqp10Discovery=artemis", StringComparison.Ordinal),
            showInternalServices: false, ct: TestContext.Current.CancellationToken);

        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, Assert.Single(services).Name);
    }

    [Fact]
    public async Task Without_the_management_port_there_is_nothing_to_read()
    {
        // The regression this fixture exists for: the base URI used to be
        // hard-coded to https://<host>/, which against an emulator is a
        // closed port. Dropping _mgmtPort reproduces exactly that, and
        // discovery degrades to the generic service rather than failing
        // the connection.
        var withoutPort = System.Text.RegularExpressions.Regex.Replace(
            _emulator.ServiceBusUrl, @"&_mgmtPort=\d+", "");

        var services = await _protocol.DiscoverAsync(
            withoutPort, showInternalServices: false, ct: TestContext.Current.CancellationToken);

        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, Assert.Single(services).Name);
    }
}
