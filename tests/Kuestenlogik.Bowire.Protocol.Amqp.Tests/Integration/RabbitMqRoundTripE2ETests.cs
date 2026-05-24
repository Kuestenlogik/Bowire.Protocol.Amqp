// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests.Integration;

/// <summary>
/// End-to-end checks for the AMQP 0.9.1 plugin against a real RabbitMQ
/// container. Walks the same paths the audit flagged as untested:
/// discovery hits the management API and surfaces declared
/// exchanges + queues; InvokeAsync publishes a real message; the
/// streaming consume pipeline yields the same payload back via the
/// structured envelope.
/// </summary>
/// <remarks>
/// Marked <c>[Trait("Category", "Docker")]</c> so CI / dev runs
/// without a Docker daemon can opt out via
/// <c>dotnet test --filter "Category!=Docker"</c>. The Bowire CI
/// matrix runs both the Docker and non-Docker passes; locally,
/// developers without Docker still get every unit test.
/// </remarks>
[Trait("Category", "Docker")]
public sealed class RabbitMqRoundTripE2ETests : IClassFixture<RabbitMqContainerFixture>
{
    private readonly RabbitMqContainerFixture _broker;

    public RabbitMqRoundTripE2ETests(RabbitMqContainerFixture broker)
    {
        _broker = broker;
    }

    [Fact]
    public async Task DiscoverAsync_finds_declared_exchange_and_queue()
    {
        // Declare a topic exchange + bound queue, then ask the plugin
        // to discover. The management API picks up both as soon as
        // the channel call returns.
        await DeclareHarborTopologyAsync(TestContext.Current.CancellationToken);

        var protocol = new BowireAmqpProtocol();
        var ct = TestContext.Current.CancellationToken;
        var serverUrl = WithMgmtPort(_broker.AmqpUrl, _broker.ManagementPort);

        var services = await protocol.DiscoverAsync(serverUrl, showInternalServices: false, ct);

        Assert.Contains(services, s => s.Name == "harbor");
        Assert.Contains(services, s => s.Name == "harbor.cranes");
    }

    [Fact]
    public async Task Publish_then_consume_round_trips_through_structured_envelope()
    {
        var ct = TestContext.Current.CancellationToken;
        await DeclareHarborTopologyAsync(ct);
        var protocol = new BowireAmqpProtocol();
        var serverUrl = WithMgmtPort(_broker.AmqpUrl, _broker.ManagementPort);

        // Start the consume side first — open the stream, push a
        // message in, then read the first frame back. Both calls share
        // the same broker container so the round-trip is fully wired
        // through the plugin's invoke + receive paths (not RabbitMQ
        // .Client direct).
        var streamCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        streamCts.CancelAfter(TimeSpan.FromSeconds(15));

        var streamTask = ReadFirstFrameAsync(
            protocol, serverUrl, queue: "harbor.cranes", streamCts.Token);

        // Tiny delay so the consumer is bound before we publish; the
        // mgmt API doesn't expose a "consumer ready" hook, and
        // RabbitMQ delivers to the bound queue regardless, but the
        // streaming pipeline needs the consumer in place to surface
        // the message to the IAsyncEnumerable.
        await Task.Delay(TimeSpan.FromMilliseconds(250), ct);

        var publish = await protocol.InvokeAsync(
            serverUrl, service: "harbor", method: "send:cranes.crane-42",
            jsonMessages: ["""{"crane":"crane-42","status":"online"}"""],
            showInternalServices: false,
            metadata: new Dictionary<string, string>
            {
                ["contentType"] = "application/json",
                ["messageId"] = "evt-1",
            },
            ct: ct);
        Assert.Equal("OK", publish.Status);

        var firstFrame = await streamTask;
        using var doc = JsonDocument.Parse(firstFrame);
        var root = doc.RootElement;

        Assert.Equal("harbor", root.GetProperty("exchange").GetString());
        Assert.Equal("cranes.crane-42", root.GetProperty("routingKey").GetString());
        Assert.Equal("application/json", root.GetProperty("contentType").GetString());
        Assert.Equal("evt-1", root.GetProperty("messageId").GetString());
        Assert.Equal("json", root.GetProperty("encoding").GetString());

        var value = root.GetProperty("value");
        Assert.Equal("crane-42", value.GetProperty("crane").GetString());
        Assert.Equal("online", value.GetProperty("status").GetString());
    }

    private async Task DeclareHarborTopologyAsync(CancellationToken ct)
    {
        // Direct RabbitMQ.Client setup — gives the plugin's discovery
        // path real exchanges + bindings to find without going through
        // the workbench UI.
        var factory = new ConnectionFactory { Uri = new Uri(_broker.AmqpUrl) };
        await using var conn = await factory.CreateConnectionAsync(ct);
        await using var ch = await conn.CreateChannelAsync(cancellationToken: ct);

        await ch.ExchangeDeclareAsync("harbor", ExchangeType.Topic, durable: true, autoDelete: false, arguments: null, noWait: false, cancellationToken: ct);
        await ch.QueueDeclareAsync("harbor.cranes", durable: true, exclusive: false, autoDelete: false, arguments: null, noWait: false, cancellationToken: ct);
        await ch.QueueBindAsync("harbor.cranes", "harbor", "cranes.*", arguments: null, noWait: false, cancellationToken: ct);
    }

    private static async Task<string> ReadFirstFrameAsync(
        BowireAmqpProtocol protocol, string serverUrl, string queue, CancellationToken ct)
    {
        await foreach (var frame in protocol.InvokeStreamAsync(
            serverUrl, queue, "receive", jsonMessages: [],
            showInternalServices: false, ct: ct).ConfigureAwait(false))
        {
            return frame;
        }
        throw new InvalidOperationException("Stream completed before any frame arrived.");
    }

    private static string WithMgmtPort(string baseUrl, int mgmtPort)
    {
        // Testcontainers maps 15672 to an ephemeral host port; the
        // plugin reads it via the URL-query override.
        var sep = baseUrl.Contains('?', StringComparison.Ordinal) ? '&' : '?';
        return $"{baseUrl}{sep}_mgmtPort={mgmtPort.ToString(System.Globalization.CultureInfo.InvariantCulture)}&_receiveTimeout=8";
    }
}
