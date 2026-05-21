// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

/// <summary>
/// Skeleton-release coverage: the surface of <see cref="BowireAmqpProtocol"/>
/// is fixed so embedded hosts and the AsyncAPI binding resolver can pin
/// against it. Tests assert that identity / settings / icon shape look
/// the way the 0.1.x release advertises, and that every IBowireProtocol
/// method explicitly fails with <see cref="NotImplementedException"/> so
/// nobody mistakes a silent no-op for a working implementation. Real
/// behavioural tests (broker round-trips via Testcontainers) land
/// alongside the 0.2.x discovery + invocation implementation.
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
    public async Task DiscoverAsync_throws_NotImplementedException_in_skeleton()
    {
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<NotImplementedException>(
            () => protocol.DiscoverAsync("amqp://localhost:5672", showInternalServices: false, ct: ct));
    }

    [Fact]
    public async Task InvokeAsync_throws_NotImplementedException_in_skeleton()
    {
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<NotImplementedException>(
            () => protocol.InvokeAsync(
                "amqp://localhost:5672", "exchange", "send",
                jsonMessages: ["{}"], showInternalServices: false, ct: ct));
    }

    [Fact]
    public async Task InvokeStreamAsync_throws_NotImplementedException_in_skeleton()
    {
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<NotImplementedException>(async () =>
        {
            await foreach (var _ in protocol.InvokeStreamAsync(
                "amqp://localhost:5672", "exchange", "receive",
                jsonMessages: [], showInternalServices: false, ct: ct).ConfigureAwait(false))
            {
                // Should never produce a value — the iterator throws
                // before yielding anything.
            }
        }).ConfigureAwait(false);
    }

    [Fact]
    public async Task OpenChannelAsync_returns_null_AMQP_uses_streaming_not_channels()
    {
        // AMQP doesn't map onto Bowire's WebSocket-style channel surface;
        // the plugin will route streaming via InvokeStreamAsync. Returning
        // null here is the contract that lets the workbench fall through
        // to streaming UI instead of opening a phantom channel.
        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        var channel = await protocol.OpenChannelAsync(
            "amqp://localhost:5672", "exchange", "receive", showInternalServices: false, ct: ct);
        Assert.Null(channel);
    }
}
