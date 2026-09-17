// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests.Integration;

/// <summary>
/// AMQP 1.0 discovery against a live ActiveMQ Artemis broker: what it
/// says about itself over its own management address, and whether a
/// message sent to a discovered service arrives on the queue the
/// discovered method names.
/// </summary>
/// <remarks>
/// Marked <c>[Trait("Category", "Docker")]</c> so a run without a Docker
/// daemon opts out via <c>dotnet test --filter "Category!=Docker"</c>,
/// exactly as the 0.9.1 suite does.
/// </remarks>
[Trait("Category", "Docker")]
public sealed class ArtemisDiscoveryE2ETests : IClassFixture<ArtemisContainerFixture>
{
    private readonly ArtemisContainerFixture _artemis;
    private readonly BowireAmqpProtocol _protocol = new();

    public ArtemisDiscoveryE2ETests(ArtemisContainerFixture artemis) => _artemis = artemis;

    private static Dictionary<string, string> ShortReceive() =>
        new(StringComparer.Ordinal) { ["receiveTimeoutSeconds"] = "5" };

    [Fact]
    public async Task Discovery_finds_the_brokers_addresses_and_their_queues()
    {
        var services = await _protocol.DiscoverAsync(
            _artemis.Amqp10Url, showInternalServices: false, ct: TestContext.Current.CancellationToken);

        // The fixture's topology, plus the two addresses every Artemis
        // ships with. Not the generic Broker service: that is the
        // fallback, and this broker answered.
        Assert.DoesNotContain(services, s => s.Name == BowireAmqpProtocol.BrokerServiceName);
        Assert.Contains(services, s => s.Name == "harbor");
        Assert.Contains(services, s => s.Name == "orders");
        Assert.Contains(services, s => s.Name == "DLQ");
        Assert.All(services, s => Assert.Contains("Artemis address", s.Description ?? "", StringComparison.Ordinal));

        // A multicast address with two queues: a receive per queue, no
        // bare one — there is no single queue it could mean.
        var harbor = services.Single(s => s.Name == "harbor");
        Assert.Equal(
            ["receive:harbor.cranes", "receive:harbor.gates", "send"],
            harbor.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
        Assert.Contains("MULTICAST", harbor.Description, StringComparison.Ordinal);

        // An anycast queue named after its address is the bare receive.
        var orders = services.Single(s => s.Name == "orders");
        Assert.Equal(["receive", "send"], orders.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task The_brokers_own_plumbing_is_hidden_unless_asked_for()
    {
        var hidden = await _protocol.DiscoverAsync(
            _artemis.Amqp10Url, showInternalServices: false, ct: TestContext.Current.CancellationToken);
        var shown = await _protocol.DiscoverAsync(
            _artemis.Amqp10Url, showInternalServices: true, ct: TestContext.Current.CancellationToken);

        Assert.DoesNotContain(hidden, s => s.Name.StartsWith("activemq.", StringComparison.Ordinal));
        Assert.DoesNotContain(hidden, s => s.Name.StartsWith("$sys.", StringComparison.Ordinal));
        Assert.Contains(shown, s => s.Name.StartsWith("activemq.", StringComparison.Ordinal));
        Assert.True(shown.Count > hidden.Count);
    }

    [Fact]
    public async Task A_multicast_address_fans_out_to_every_discovered_queue()
    {
        var services = await _protocol.DiscoverAsync(
            _artemis.Amqp10Url, showInternalServices: false, ct: TestContext.Current.CancellationToken);
        var harbor = services.Single(s => s.Name == "harbor");

        var sent = await _protocol.InvokeAsync(
            _artemis.Amqp10Url, "harbor", BowireAmqpProtocol.SendMethodName,
            ["""{"crane":7,"state":"lifting"}"""],
            showInternalServices: false, metadata: ShortReceive(),
            ct: TestContext.Current.CancellationToken);
        Assert.Equal("OK", sent.Status);

        // Both queues bound to the address get it, and each receive
        // reports the fully-qualified queue it read from.
        foreach (var method in harbor.Methods.Where(m => m.ServerStreaming).Select(m => m.Name))
        {
            var frame = await FirstFrameAsync("harbor", method);
            using var doc = JsonDocument.Parse(frame);
            var queue = method["receive:".Length..];
            Assert.Equal($"harbor::{queue}", doc.RootElement.GetProperty("address").GetString());
            Assert.Equal(7, doc.RootElement.GetProperty("value").GetProperty("crane").GetInt32());
        }
    }

    [Fact]
    public async Task An_anycast_queue_round_trips_through_its_bare_methods()
    {
        var sent = await _protocol.InvokeAsync(
            _artemis.Amqp10Url, "orders", BowireAmqpProtocol.SendMethodName,
            ["""{"order":42}"""],
            showInternalServices: false, metadata: ShortReceive(),
            ct: TestContext.Current.CancellationToken);
        Assert.Equal("OK", sent.Status);

        var frame = await FirstFrameAsync("orders", BowireAmqpProtocol.ReceiveMethodName);
        using var doc = JsonDocument.Parse(frame);
        Assert.Equal("orders", doc.RootElement.GetProperty("address").GetString());
        Assert.Equal(42, doc.RootElement.GetProperty("value").GetProperty("order").GetInt32());
    }

    [Fact]
    public async Task Discovery_turned_off_leaves_the_generic_broker_service()
    {
        var services = await _protocol.DiscoverAsync(
            _artemis.Amqp10Url + "?_amqp10Discovery=none",
            showInternalServices: false, ct: TestContext.Current.CancellationToken);

        var broker = Assert.Single(services);
        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, broker.Name);

        // And it still works: the address travels in the metadata, the
        // way it did before the broker could be asked anything.
        var sent = await _protocol.InvokeAsync(
            _artemis.Amqp10Url, BowireAmqpProtocol.BrokerServiceName, BowireAmqpProtocol.SendMethodName,
            ["""{"direct":true}"""],
            showInternalServices: false,
            metadata: new Dictionary<string, string>(StringComparer.Ordinal) { ["address"] = "orders" },
            ct: TestContext.Current.CancellationToken);
        Assert.Equal("OK", sent.Status);
    }

    [Fact]
    public async Task An_endpoint_told_it_is_service_bus_does_not_ask_artemis()
    {
        // The operator's answer about which broker this is stands even
        // when it is wrong: nothing reads the Artemis management address,
        // and the generic service is what comes back.
        var services = await _protocol.DiscoverAsync(
            _artemis.Amqp10Url + "?_amqp10Discovery=servicebus",
            showInternalServices: false, ct: TestContext.Current.CancellationToken);

        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, Assert.Single(services).Name);
    }

    /// <summary>The first frame of a receive stream, or a failure if none arrives.</summary>
    private async Task<string> FirstFrameAsync(string service, string method)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        await foreach (var frame in _protocol.InvokeStreamAsync(
            _artemis.Amqp10Url, service, method, [], showInternalServices: false,
            metadata: ShortReceive(), ct: cts.Token))
        {
            return frame;
        }
        Assert.Fail($"{service}/{method} yielded no frame");
        return string.Empty;
    }
}
