// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

/// <summary>
/// Shape-pinning for the AMQP 1.0 discovery path. The 1.0 wire has no
/// broker-side schema concept — the plugin returns a synthetic
/// <c>Broker</c> service with <c>send</c> + <c>receive</c> methods so
/// the workbench's sidebar has something to render. These tests pin
/// that synthetic contract.
/// </summary>
public sealed class AmqpV10DiscoverShapeTests
{
    [Fact]
    public async Task DiscoverAsync_v10_returns_single_broker_service()
    {
        var protocol = new BowireAmqpProtocol();
        var services = await protocol.DiscoverAsync(
            "amqp1://broker:5672",
            showInternalServices: false,
            ct: TestContext.Current.CancellationToken);

        var broker = Assert.Single(services);
        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, broker.Name);
        Assert.Equal("amqp", broker.Package);
    }

    [Fact]
    public async Task DiscoverAsync_v10_exposes_send_and_receive_on_broker()
    {
        var protocol = new BowireAmqpProtocol();
        var services = await protocol.DiscoverAsync(
            "amqp1://broker", showInternalServices: false,
            ct: TestContext.Current.CancellationToken);

        var broker = services.Single();
        Assert.Contains(broker.Methods, m => m.Name == BowireAmqpProtocol.SendMethodName);
        Assert.Contains(broker.Methods, m => m.Name == BowireAmqpProtocol.ReceiveMethodName);
    }

    [Fact]
    public async Task DiscoverAsync_v10_send_is_unary_receive_is_server_streaming()
    {
        var protocol = new BowireAmqpProtocol();
        var services = await protocol.DiscoverAsync(
            "amqps1://broker", showInternalServices: false,
            ct: TestContext.Current.CancellationToken);

        var broker = services.Single();
        var send = broker.Methods.Single(m => m.Name == BowireAmqpProtocol.SendMethodName);
        var receive = broker.Methods.Single(m => m.Name == BowireAmqpProtocol.ReceiveMethodName);

        Assert.False(send.ClientStreaming);
        Assert.False(send.ServerStreaming);
        Assert.False(receive.ClientStreaming);
        Assert.True(receive.ServerStreaming);
    }
}
