// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Mocking;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>
/// Plugs into Bowire's mock server via the
/// <see cref="IBowireMockEmitter"/> extension point. When a recording
/// contains steps tagged <c>protocol: "amqp"</c>, the emitter opens a
/// single connection + channel against the broker derived from the
/// first step's <see cref="BowireRecordingStep.ServerUrl"/> and
/// re-publishes each step's captured payload at the original cadence
/// derived from <see cref="BowireRecordingStep.CapturedAt"/>.
/// </summary>
/// <remarks>
/// <para>
/// Routing convention (matches the live plugin's invoke shape):
/// </para>
/// <list type="bullet">
///   <item><c>service</c> is the exchange name. The literal
///   <c>"(default)"</c> resolves to the unnamed default exchange.</item>
///   <item><c>method</c> is <c>"send"</c> or <c>"send:&lt;routingKey&gt;"</c>.
///   When a bare <c>"send"</c> is paired with a <c>"routingKey"</c>
///   metadata entry the metadata wins; otherwise the colon-suffix
///   form gives the key.</item>
/// </list>
/// <para>
/// Payload precedence is the same as the Kafka / DIS / UDP emitters:
/// <see cref="BowireRecordingStep.ResponseBinary"/> (base64) wins so
/// non-text payloads round-trip byte-for-byte; UTF-8 of
/// <see cref="BowireRecordingStep.Body"/> is the fallback.
/// </para>
/// <para>
/// Scope: AMQP 0.9.1 only. The 1.0 wire's mock-emit counterpart is
/// a follow-up — recordings tagged <c>amqp</c> with a
/// <c>amqp1://</c> server URL log a one-time warning and are
/// skipped. The two stacks differ enough (AMQPNetLite vs
/// RabbitMQ.Client) that a clean 1.0 emitter belongs in its own
/// class once there's demand.
/// </para>
/// </remarks>
public sealed class AmqpMockEmitter : IBowireMockEmitter
{
    private IConnection? _connection;
    private IChannel? _channel;
    private CancellationTokenSource? _cts;
    private Task? _schedulerTask;
    private bool _disposed;

    /// <inheritdoc />
    public string Id => "amqp";

    /// <inheritdoc />
    public bool CanEmit(BowireRecording recording)
    {
        ArgumentNullException.ThrowIfNull(recording);
        return recording.Steps.Any(IsAmqp091Step);
    }

    /// <inheritdoc />
    public async Task StartAsync(
        BowireRecording recording,
        MockEmitterOptions options,
        ILogger logger,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(recording);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        var steps = recording.Steps.Where(IsAmqp091Step).ToList();
        if (steps.Count == 0) return;

        // Skip 1.0 steps loudly — the integration story for 1.0 mock-
        // emit lands in a follow-up; today it's a no-op so a mixed
        // recording doesn't accidentally bind a wrong-wire connection.
        var ignored = recording.Steps.Count(IsAmqp10Step);
        if (ignored > 0)
        {
            logger.LogWarning(
                "amqp-emitter skipping {Count} step(s) tagged amqp via amqp1:// — 1.0 mock-emit lands in a later release.",
                ignored);
        }

        if (!AmqpEndpoint.TryParse(steps[0].ServerUrl ?? "amqp://localhost:5672/", out var endpoint))
        {
            logger.LogWarning(
                "amqp-emitter could not parse step.ServerUrl ('{Url}'); defaulting to amqp://localhost:5672/.",
                steps[0].ServerUrl);
            // The default URL is a compile-time constant — TryParse against
            // it cannot fail at runtime, so the discard is intentional.
            _ = AmqpEndpoint.TryParse("amqp://localhost:5672/", out endpoint);
        }

        var factory = BuildFactory(endpoint!);
        _connection = await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
        _channel = await _connection.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _schedulerTask = Task.Run(() => RunAsync(steps, options, logger, _cts.Token), _cts.Token);

        logger.LogInformation(
            "amqp-emitter publishing → {Host}:{Port}{Vhost} (steps={Count})",
            endpoint!.Host, endpoint.Port, endpoint.AddressOrVhost, steps.Count);
    }

    private static bool IsAmqp091Step(BowireRecordingStep s) =>
        string.Equals(s.Protocol, "amqp", StringComparison.OrdinalIgnoreCase) &&
        (s.ServerUrl is null || s.ServerUrl.StartsWith("amqp://", StringComparison.OrdinalIgnoreCase)
                              || s.ServerUrl.StartsWith("amqps://", StringComparison.OrdinalIgnoreCase));

    private static bool IsAmqp10Step(BowireRecordingStep s) =>
        string.Equals(s.Protocol, "amqp", StringComparison.OrdinalIgnoreCase) &&
        s.ServerUrl is not null &&
        (s.ServerUrl.StartsWith("amqp1://", StringComparison.OrdinalIgnoreCase)
         || s.ServerUrl.StartsWith("amqps1://", StringComparison.OrdinalIgnoreCase));

    private static ConnectionFactory BuildFactory(AmqpEndpoint endpoint) => new()
    {
        HostName = endpoint.Host,
        Port = endpoint.Port,
        VirtualHost = string.IsNullOrEmpty(endpoint.AddressOrVhost) ? "/" : endpoint.AddressOrVhost,
        UserName = endpoint.UserName ?? "guest",
        Password = endpoint.Password ?? "guest",
        Ssl = new SslOption { Enabled = endpoint.UseTls, ServerName = endpoint.Host },
    };

    private async Task RunAsync(
        List<BowireRecordingStep> steps,
        MockEmitterOptions options,
        ILogger logger,
        CancellationToken ct)
    {
        if (_channel is null) return;

        var baseCapturedAt = steps[0].CapturedAt;
        var speed = options.ReplaySpeed;

        do
        {
            var scheduleStartTicks = Environment.TickCount64;

            foreach (var step in steps)
            {
                ct.ThrowIfCancellationRequested();

                if (speed > 0)
                {
                    var targetOffsetMs = (long)((step.CapturedAt - baseCapturedAt) / speed);
                    var elapsed = Environment.TickCount64 - scheduleStartTicks;
                    var waitMs = targetOffsetMs - elapsed;
                    if (waitMs > 0)
                    {
                        try { await Task.Delay(TimeSpan.FromMilliseconds(waitMs), ct); }
                        catch (OperationCanceledException) { return; }
                    }
                }

                await EmitAsync(step, logger, ct);
            }
        }
        while (options.Loop && !ct.IsCancellationRequested);
    }

    private async Task EmitAsync(BowireRecordingStep step, ILogger logger, CancellationToken ct)
    {
        var payload = DecodePayload(step, logger);
        if (payload is null) return;

        var exchange = string.Equals(step.Service, "(default)", StringComparison.Ordinal)
            ? string.Empty
            : step.Service ?? string.Empty;

        // Routing key resolution mirrors InvokeV091: metadata wins, then
        // the "send:key" colon suffix, then bare → empty string (broker
        // routes purely on exchange type / bindings).
        var routingKey = step.Metadata?.TryGetValue("routingKey", out var rk) == true && !string.IsNullOrEmpty(rk)
            ? rk
            : ExtractRoutingKeyFromMethod(step.Method) ?? string.Empty;

        var props = new BasicProperties { ContentType = "application/json" };
        if (step.Metadata is not null)
        {
            if (step.Metadata.TryGetValue("contentType", out var c) && !string.IsNullOrEmpty(c)) props.ContentType = c;
            if (step.Metadata.TryGetValue("messageId", out var mid) && !string.IsNullOrEmpty(mid)) props.MessageId = mid;
            if (step.Metadata.TryGetValue("correlationId", out var cid) && !string.IsNullOrEmpty(cid)) props.CorrelationId = cid;
        }

        try
        {
            await _channel!.BasicPublishAsync(
                exchange: exchange,
                routingKey: routingKey,
                mandatory: false,
                basicProperties: props,
                body: payload,
                cancellationToken: ct).ConfigureAwait(false);
            var payloadLength = payload.Length;
            logger.LogInformation(
                "amqp-emit(step={StepId}, exchange={Exchange}, routingKey={Key}, bytes={Bytes})",
                step.Id, exchange, routingKey, payloadLength);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "amqp-emitter publish failed for step '{StepId}' on exchange '{Exchange}'; scheduler continues.",
                step.Id, exchange);
        }
    }

    private static string? ExtractRoutingKeyFromMethod(string? method)
    {
        if (string.IsNullOrEmpty(method)) return null;
        const string sendPrefix = BowireAmqpProtocol.SendMethodName + ":";
        return method.StartsWith(sendPrefix, StringComparison.Ordinal)
            ? method[sendPrefix.Length..]
            : null;
    }

    /// <summary>
    /// Decode the step's payload to bytes. Same precedence as the Kafka,
    /// DIS, and UDP emitters: <see cref="BowireRecordingStep.ResponseBinary"/>
    /// (base64) wins; <see cref="BowireRecordingStep.Body"/> is
    /// UTF-8-encoded as a fallback.
    /// </summary>
    internal static byte[]? DecodePayload(BowireRecordingStep step, ILogger logger)
    {
        if (!string.IsNullOrEmpty(step.ResponseBinary))
        {
            try
            {
                return Convert.FromBase64String(step.ResponseBinary);
            }
            catch (FormatException ex)
            {
                logger.LogWarning(ex,
                    "amqp-emitter step '{StepId}' ResponseBinary is not valid base64 — falling back to Body.",
                    step.Id);
            }
        }
        if (!string.IsNullOrEmpty(step.Body))
        {
            return System.Text.Encoding.UTF8.GetBytes(step.Body);
        }
        logger.LogWarning(
            "amqp-emitter step '{StepId}' has no payload (neither ResponseBinary nor Body) — skipping.",
            step.Id);
        return null;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_cts is not null)
        {
            try { await _cts.CancelAsync().ConfigureAwait(false); }
            catch (ObjectDisposedException) { }
        }
        if (_schedulerTask is not null)
        {
            try { await _schedulerTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch (Exception) { /* swallow — disposal must not throw */ }
        }
        if (_channel is not null) await _channel.DisposeAsync().ConfigureAwait(false);
        if (_connection is not null) await _connection.DisposeAsync().ConfigureAwait(false);
        _cts?.Dispose();
    }
}
