// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using Kuestenlogik.Bowire.Mocking;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests.Integration;

/// <summary>
/// End-to-end coverage for <see cref="AmqpMockEmitter"/>. Stands up a
/// real RabbitMQ broker, declares a queue, points the emitter at a
/// hand-built recording, and verifies that <c>StartAsync</c> →
/// <c>RunAsync</c> → <c>EmitAsync</c> actually publishes the captured
/// payloads.
/// </summary>
/// <remarks>
/// Same Docker-gated trait as the protocol's own round-trip suite —
/// non-Docker runs skip via <c>--filter "Category!=Docker"</c>.
/// </remarks>
[Trait("Category", "Docker")]
public sealed class AmqpMockEmitterE2ETests : IClassFixture<RabbitMqContainerFixture>
{
    private readonly RabbitMqContainerFixture _broker;

    private static readonly string[] ExpectedHellos = ["hello-1", "hello-2", "hello-3"];

    public AmqpMockEmitterE2ETests(RabbitMqContainerFixture broker)
    {
        _broker = broker;
    }

    [Fact]
    public async Task StartAsync_ReplaysRecording_Steps_OnDefaultExchange()
    {
        var ct = TestContext.Current.CancellationToken;
        // Declare an exclusive queue, bind nothing — default-exchange
        // routes by routing-key=queue-name. The emitter publishes
        // step.Method="send:<queueName>" → message lands here.
        var queueName = "bowire-mock-" + Guid.NewGuid().ToString("N")[..8];
        var factory = new ConnectionFactory { Uri = new Uri(_broker.AmqpUrl) };

        await using var conn = await factory.CreateConnectionAsync(ct);
        await using var channel = await conn.CreateChannelAsync(cancellationToken: ct);
        await channel.QueueDeclareAsync(queueName, durable: false, exclusive: false,
            autoDelete: true, arguments: null, cancellationToken: ct);

        var received = new List<string>();
        var consumer = new AsyncEventingBasicConsumer(channel);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += (_, ea) =>
        {
            received.Add(Encoding.UTF8.GetString(ea.Body.Span));
            if (received.Count >= 3) tcs.TrySetResult(true);
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer, cancellationToken: ct);

        // Recording: 3 sequential publishes via the default exchange,
        // routed by queueName. Speed=10x so the test stays under a
        // second wall-time even with the captured spacing.
        var baseTime = DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeMilliseconds();
        var recording = new BowireRecording
        {
            Steps =
            {
                MakeStep("s1", baseTime, queueName, "hello-1", _broker.AmqpUrl),
                MakeStep("s2", (baseTime + 20), queueName, "hello-2", _broker.AmqpUrl),
                MakeStep("s3", (baseTime + 40), queueName, "hello-3", _broker.AmqpUrl),
            },
        };

        await using var emitter = new AmqpMockEmitter();
        await emitter.StartAsync(
            recording,
            new MockEmitterOptions { ReplaySpeed = 10.0, Loop = false },
            NullLogger.Instance,
            ct);

        // Wait up to 5 s for the three messages.
        var got = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(5), ct));
        Assert.True(got == tcs.Task, $"received {received.Count}/3 messages within 5s");

        Assert.Equal(ExpectedHellos, received);
    }

    [Fact]
    public async Task StartAsync_HonoursRoutingKeyFromMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        // 'routingKey' metadata wins over a "send:..." suffix on
        // step.Method. Default exchange + queue-name binding-key picks
        // it up.
        var queueName = "bowire-mock-meta-" + Guid.NewGuid().ToString("N")[..8];
        var factory = new ConnectionFactory { Uri = new Uri(_broker.AmqpUrl) };

        await using var conn = await factory.CreateConnectionAsync(ct);
        await using var channel = await conn.CreateChannelAsync(cancellationToken: ct);
        await channel.QueueDeclareAsync(queueName, durable: false, exclusive: false,
            autoDelete: true, arguments: null, cancellationToken: ct);

        string? received = null;
        var consumer = new AsyncEventingBasicConsumer(channel);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += (_, ea) =>
        {
            received = Encoding.UTF8.GetString(ea.Body.Span);
            tcs.TrySetResult(true);
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer, cancellationToken: ct);

        var recording = new BowireRecording
        {
            Steps =
            {
                new BowireRecordingStep
                {
                    Id = "s",
                    Protocol = "amqp",
                    ServerUrl = _broker.AmqpUrl,
                    Service = "(default)",
                    Method = "send:wrong-key",
                    Body = "via-metadata",
                    Metadata = new Dictionary<string, string> { ["routingKey"] = queueName },
                    CapturedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                },
            },
        };

        await using var emitter = new AmqpMockEmitter();
        await emitter.StartAsync(
            recording,
            new MockEmitterOptions { ReplaySpeed = 10.0 },
            NullLogger.Instance,
            ct);

        var got = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(5), ct));
        Assert.True(got == tcs.Task, "publish via metadata routingKey didn't arrive");
        Assert.Equal("via-metadata", received);
    }

    [Fact]
    public async Task StartAsync_BinaryPayload_RoundTripsByteForByte()
    {
        var ct = TestContext.Current.CancellationToken;
        // Non-UTF-8 bytes — verifies the ResponseBinary-first decoding
        // path actually publishes raw bytes, not a re-encoded string.
        var queueName = "bowire-mock-bin-" + Guid.NewGuid().ToString("N")[..8];
        var factory = new ConnectionFactory { Uri = new Uri(_broker.AmqpUrl) };

        await using var conn = await factory.CreateConnectionAsync(ct);
        await using var channel = await conn.CreateChannelAsync(cancellationToken: ct);
        await channel.QueueDeclareAsync(queueName, durable: false, exclusive: false,
            autoDelete: true, arguments: null, cancellationToken: ct);

        byte[]? received = null;
        var consumer = new AsyncEventingBasicConsumer(channel);
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        consumer.ReceivedAsync += (_, ea) =>
        {
            received = ea.Body.ToArray();
            tcs.TrySetResult(true);
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer, cancellationToken: ct);

        var payload = new byte[] { 0x00, 0xFF, 0x10, 0xC2, 0xA0, 0xDE, 0xAD, 0xBE, 0xEF };
        var recording = new BowireRecording
        {
            Steps =
            {
                new BowireRecordingStep
                {
                    Id = "bin",
                    Protocol = "amqp",
                    ServerUrl = _broker.AmqpUrl,
                    Service = "(default)",
                    Method = "send:" + queueName,
                    ResponseBinary = Convert.ToBase64String(payload),
                    CapturedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                },
            },
        };

        await using var emitter = new AmqpMockEmitter();
        await emitter.StartAsync(
            recording,
            new MockEmitterOptions { ReplaySpeed = 10.0 },
            NullLogger.Instance,
            ct);

        var got = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(5), ct));
        Assert.True(got == tcs.Task);
        Assert.Equal(payload, received);
    }

    private static BowireRecordingStep MakeStep(
        string id, long capturedAt, string queueName, string body, string brokerUrl)
        => new()
        {
            Id = id,
            Protocol = "amqp",
            ServerUrl = brokerUrl,
            Service = "(default)",
            Method = "send:" + queueName,
            Body = body,
            CapturedAt = capturedAt,
        };
}
