// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

/// <summary>
/// Coverage for the AMQP plugin's deterministic surface — identity,
/// settings, channel contract, URL parsing, argument validation.
/// Broker round-trips (RabbitMQ.Client → real RabbitMQ, AMQPNetLite →
/// real Service Bus / Artemis) are integration tests; they live in a
/// later pass when Testcontainers wiring lands.
/// </summary>
public sealed class BowireAmqpProtocolTests
{
    [Fact]
    public void Identity_pins_id_name_and_icon()
    {
        var protocol = new BowireAmqpProtocol();

        Assert.Equal("amqp", protocol.Id);
        Assert.Equal("AMQP", protocol.Name);
        Assert.Contains("<svg", protocol.IconSvg, StringComparison.Ordinal);
    }

    [Fact]
    public void Settings_expose_managementApiPort_default_15672()
    {
        var protocol = new BowireAmqpProtocol();
        var port = protocol.Settings.SingleOrDefault(s => s.Key == "managementApiPort");

        Assert.NotNull(port);
        Assert.Equal(15672, port!.DefaultValue);
    }

    [Fact]
    public void Settings_expose_receiveTimeoutSeconds_default_30()
    {
        var protocol = new BowireAmqpProtocol();
        var t = protocol.Settings.SingleOrDefault(s => s.Key == "receiveTimeoutSeconds");

        Assert.NotNull(t);
        Assert.Equal(30, t!.DefaultValue);
    }

    [Fact]
    public async Task OpenChannelAsync_returns_null_AMQP_uses_streaming_not_channels()
    {
        // AMQP doesn't map onto Bowire's WebSocket-style channel surface;
        // the plugin routes streaming via InvokeStreamAsync. Returning
        // null here is the contract that lets the workbench fall through
        // to streaming UI instead of opening a phantom channel.
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        var channel = await protocol.OpenChannelAsync(
            "amqp://localhost:5672", "exchange", "receive", showInternalServices: false, ct: ct);
        Assert.Null(channel);
    }

    [Fact]
    public async Task DiscoverAsync_rejects_invalid_scheme()
    {
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<ArgumentException>(
            () => protocol.DiscoverAsync("http://localhost:5672", showInternalServices: false, ct: ct));
    }

    [Fact]
    public async Task InvokeAsync_rejects_invalid_scheme()
    {
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<ArgumentException>(
            () => protocol.InvokeAsync(
                "kafka://localhost:9092", "exchange", "send",
                jsonMessages: ["{}"], showInternalServices: false, ct: ct));
    }

    [Fact]
    public async Task InvokeStreamAsync_rejects_invalid_scheme()
    {
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in protocol.InvokeStreamAsync(
                "not-a-url", "q", "receive", jsonMessages: [],
                showInternalServices: false, ct: ct).ConfigureAwait(false))
            {
            }
        }).ConfigureAwait(false);
    }
}
