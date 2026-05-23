// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Amqp;
using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Models;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>
/// Bowire protocol plugin for AMQP — covers both AMQP 0.9.1 (RabbitMQ,
/// ActiveMQ Classic) and AMQP 1.0 (Azure Service Bus, ActiveMQ Artemis)
/// through a single plugin id. The URL scheme picks the wire stack:
/// <list type="bullet">
///   <item><c>amqp://host:5672/vhost</c> → AMQP 0.9.1 via RabbitMQ.Client</item>
///   <item><c>amqps://host:5671/vhost</c> → AMQP 0.9.1 with TLS</item>
///   <item><c>amqp1://host:5672</c> → AMQP 1.0 via AMQPNetLite</item>
///   <item><c>amqps1://host:5671</c> → AMQP 1.0 with TLS</item>
/// </list>
/// Same dispatch model as the gRPC plugin's Native / gRPC-Web split: one
/// plugin id, one NuGet package, multiple wire variants gated on the URL
/// scheme. AsyncAPI's <c>bindings.amqp</c> and <c>bindings.amqp1</c> route
/// at this single plugin.
/// </summary>
public sealed class BowireAmqpProtocol : IBowireProtocol
{
    /// <summary>Synthetic service name used by the 1.0 wire (no broker schema).</summary>
    public const string BrokerServiceName = "Broker";

    /// <summary>Method name of the unary publish operation (0.9.1) / send (1.0).</summary>
    public const string SendMethodName = "send";

    /// <summary>Method name of the streaming consume / receive operation.</summary>
    public const string ReceiveMethodName = "receive";

    /// <inheritdoc />
    public string Name => "AMQP";

    /// <inheritdoc />
    public string Id => "amqp";

    /// <inheritdoc />
    public string IconSvg => """<svg viewBox="0 0 24 24" fill="none" stroke="#818cf8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" width="16" height="16" aria-hidden="true"><rect x="3" y="7" width="14" height="10" rx="1"/><path d="M17 10l4 2-4 2"/><path d="M7 4v3"/><path d="M13 4v3"/></svg>""";

    /// <inheritdoc />
    public IReadOnlyList<BowirePluginSetting> Settings =>
    [
        new("managementApiPort", "Management API port",
            "RabbitMQ Management HTTP API port for discovery (0.9.1 only). Defaults to 15672.",
            "number", 15672),
        new("discoveryTimeoutSeconds", "Discovery timeout",
            "Max seconds to wait on broker / management-API responses during discovery.",
            "number", 5),
        new("receiveTimeoutSeconds", "Receive timeout",
            "Max seconds the streaming consume/receive operation waits between frames before tearing down.",
            "number", 30),
    ];

    /// <inheritdoc />
    public async Task<List<BowireServiceInfo>> DiscoverAsync(
        string serverUrl, bool showInternalServices, CancellationToken ct = default)
    {
        var endpoint = ParseOrThrow(serverUrl);

        return endpoint.Wire switch
        {
            AmqpWire.V091 => await DiscoverV091Async(endpoint, showInternalServices, ct).ConfigureAwait(false),
            AmqpWire.V10 => DiscoverV10(),
            _ => throw new InvalidOperationException($"Unknown AMQP wire: {endpoint.Wire}"),
        };
    }

    /// <inheritdoc />
    public async Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default)
    {
        var endpoint = ParseOrThrow(serverUrl);
        var sw = Stopwatch.StartNew();
        var payloadJson = jsonMessages.Count == 0 ? "{}" : jsonMessages[0];
        var bodyBytes = Encoding.UTF8.GetBytes(payloadJson);

        return endpoint.Wire switch
        {
            AmqpWire.V091 => await InvokeV091Async(endpoint, service, method, bodyBytes, metadata, sw, ct).ConfigureAwait(false),
            AmqpWire.V10 => await InvokeV10Async(endpoint, service, method, bodyBytes, metadata, sw, ct).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unknown AMQP wire: {endpoint.Wire}"),
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> InvokeStreamAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var endpoint = ParseOrThrow(serverUrl);
        // method names other than "receive" are still allowed — callers
        // may carry custom verbs from a recording or AsyncAPI op.
        var receiveTimeout = ReadIntMeta(metadata, "receiveTimeoutSeconds", 30);

        if (endpoint.Wire == AmqpWire.V091)
        {
            await foreach (var frame in ConsumeV091Async(endpoint, service, method, metadata, receiveTimeout, ct).ConfigureAwait(false))
                yield return frame;
            yield break;
        }

        await foreach (var frame in ReceiveV10Async(endpoint, service, method, metadata, receiveTimeout, ct).ConfigureAwait(false))
            yield return frame;
    }

    /// <inheritdoc />
    public Task<IBowireChannel?> OpenChannelAsync(
        string serverUrl, string service, string method,
        bool showInternalServices, Dictionary<string, string>? metadata = null,
        CancellationToken ct = default)
    {
        // AMQP doesn't natively map onto Bowire's bidi-channel surface
        // (channels there model WebSocket-style frame streams). Both wire
        // variants route streaming work through InvokeStreamAsync above;
        // OpenChannelAsync stays null so the workbench falls through to
        // streaming UI instead of opening a phantom channel.
        return Task.FromResult<IBowireChannel?>(null);
    }

    // -------------------------------------------------------------------
    // 0.9.1 — RabbitMQ.Client implementation
    // -------------------------------------------------------------------

    private static async Task<List<BowireServiceInfo>> DiscoverV091Async(
        AmqpEndpoint endpoint, bool showInternalServices, CancellationToken ct)
    {
        // Management plugin lives on a separate HTTP port from the AMQP
        // wire port. Default 15672; can be overridden via "managementApiPort"
        // in the plugin settings (read by the workbench at the host
        // boundary — at this layer we use the convention default).
        var mgmt = new RabbitMqManagement(endpoint, managementPort: 15672, timeout: TimeSpan.FromSeconds(5));
        try
        {
            var exchanges = await mgmt.GetExchangesAsync(ct).ConfigureAwait(false);
            var bindings = await mgmt.GetBindingsAsync(ct).ConfigureAwait(false);

            var services = new List<BowireServiceInfo>();
            foreach (var ex in exchanges)
            {
                // Skip the unnamed default exchange and the AMQP-reserved
                // amq.* family unless showInternalServices is on. Those
                // exist on every broker and would dominate the sidebar.
                if (string.IsNullOrEmpty(ex.Name) && !showInternalServices) continue;
                if (!showInternalServices && ex.Name.StartsWith("amq.", StringComparison.Ordinal)) continue;

                var displayName = string.IsNullOrEmpty(ex.Name) ? "(default)" : ex.Name;
                var keys = bindings
                    .Where(b => string.Equals(b.Source, ex.Name, StringComparison.Ordinal))
                    .Select(b => b.RoutingKey)
                    .Where(k => !string.IsNullOrEmpty(k))
                    .Distinct(StringComparer.Ordinal)
                    .ToList();

                var methods = new List<BowireMethodInfo>
                {
                    // Every exchange supports unconstrained publish — routing-key
                    // comes from metadata or method name at invoke time.
                    BuildSendMethod(SendMethodName, ex.Name),
                };
                // Plus one publish-with-routing-key per known binding, so the
                // UI shows the actual addressable surface, not just "send".
                foreach (var key in keys)
                {
                    methods.Add(BuildSendMethod($"{SendMethodName}:{key}", ex.Name));
                }
                services.Add(new BowireServiceInfo(displayName, "amqp", methods));
            }
            return services;
        }
        finally
        {
            mgmt.Dispose();
        }
    }

    private static async Task<InvokeResult> InvokeV091Async(
        AmqpEndpoint endpoint, string service, string method, byte[] body,
        Dictionary<string, string>? metadata, Stopwatch sw, CancellationToken ct)
    {
        var factory = BuildRabbitFactory(endpoint);
        await using var conn = await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
        await using var channel = await conn.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);

        // service == exchange. method may be "send" (bare) or "send:routing-key"
        // (from a discovered binding). The routing-key in metadata wins so the
        // UI can override the discovered key for a one-off publish.
        var exchange = service == "(default)" ? string.Empty : service;
        var routingKey = ReadStringMeta(metadata, "routingKey")
            ?? ExtractRoutingKeyFromMethod(method)
            ?? string.Empty;

        var props = new BasicProperties();
        if (ReadStringMeta(metadata, "contentType") is { } ct1) props.ContentType = ct1;
        else props.ContentType = "application/json";
        if (ReadStringMeta(metadata, "messageId") is { } mid) props.MessageId = mid;
        if (ReadStringMeta(metadata, "correlationId") is { } cid) props.CorrelationId = cid;
        if (ReadIntMeta(metadata, "deliveryMode", -1) is int dm and (1 or 2))
            props.DeliveryMode = (DeliveryModes)dm;
        if (ReadStringMeta(metadata, "expiration") is { } exp) props.Expiration = exp;
        if (ReadBoolMeta(metadata, "mandatory", false) is bool mandatory && mandatory)
        {
            // mandatory=true is passed to BasicPublishAsync directly below;
            // there's no Properties bit for it.
        }
        var mandatoryFlag = ReadBoolMeta(metadata, "mandatory", false);

        await channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: mandatoryFlag,
            basicProperties: props,
            body: body,
            cancellationToken: ct).ConfigureAwait(false);

        sw.Stop();
        return new InvokeResult(
            Response: $"{{\"published\":true,\"exchange\":\"{exchange}\",\"routingKey\":\"{routingKey}\",\"bytes\":{body.Length}}}",
            DurationMs: sw.ElapsedMilliseconds,
            Status: "OK",
            Metadata: new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["wire"] = "amqp-0-9-1",
                ["exchange"] = exchange,
                ["routingKey"] = routingKey,
                ["payloadBytes"] = body.Length.ToString(System.Globalization.CultureInfo.InvariantCulture),
            });
    }

    private static async IAsyncEnumerable<string> ConsumeV091Async(
        AmqpEndpoint endpoint, string service, string method,
        Dictionary<string, string>? metadata, int receiveTimeoutSeconds,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // For a streaming consume the "service" carries the queue name.
        // method is usually "receive" but is not enforced.
        var queue = ReadStringMeta(metadata, "queue") ?? service;
        var autoAck = ReadBoolMeta(metadata, "autoAck", true);

        var factory = BuildRabbitFactory(endpoint);
        await using var conn = await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
        await using var channel = await conn.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);

        var queueFrames = new System.Threading.Channels.UnboundedChannelOptions { SingleReader = true, SingleWriter = false };
        var pipe = System.Threading.Channels.Channel.CreateUnbounded<string>(queueFrames);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var json = TryDecodeJson(ea.Body.Span, ea.BasicProperties?.ContentType)
                ?? $"\"{Convert.ToBase64String(ea.Body.ToArray())}\"";
            await pipe.Writer.WriteAsync(json, ct).ConfigureAwait(false);
        };

        await channel.BasicConsumeAsync(queue: queue, autoAck: autoAck, consumer: consumer, cancellationToken: ct).ConfigureAwait(false);

        var idleCutoff = TimeSpan.FromSeconds(receiveTimeoutSeconds);
        while (!ct.IsCancellationRequested)
        {
            using var idleCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            idleCts.CancelAfter(idleCutoff);
            string frame;
            try
            {
                frame = await pipe.Reader.ReadAsync(idleCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                // Idle timeout reached — tear down so callers see a finite
                // stream instead of a hang. The receive timeout is the
                // contract the workbench shows in the streaming UI.
                yield break;
            }
            yield return frame;
        }
    }

    private static RabbitMQ.Client.ConnectionFactory BuildRabbitFactory(AmqpEndpoint endpoint) => new()
    {
        HostName = endpoint.Host,
        Port = endpoint.Port,
        VirtualHost = string.IsNullOrEmpty(endpoint.AddressOrVhost) ? "/" : endpoint.AddressOrVhost,
        UserName = endpoint.UserName ?? "guest",
        Password = endpoint.Password ?? "guest",
        Ssl = new SslOption { Enabled = endpoint.UseTls, ServerName = endpoint.Host },
    };

    // -------------------------------------------------------------------
    // 1.0 — AMQPNetLite implementation
    // -------------------------------------------------------------------

    private static List<BowireServiceInfo> DiscoverV10()
    {
        // AMQP 1.0 has no broker-side schema concept — there is no way to
        // enumerate addresses from the wire. Return the synthetic Broker
        // service so the workbench has something to anchor send/receive
        // against; the actual target address comes from metadata at
        // invoke time.
        return new List<BowireServiceInfo>
        {
            new(BrokerServiceName, "amqp", new List<BowireMethodInfo>
            {
                BuildSendMethod(SendMethodName, BrokerServiceName),
                BuildReceiveMethod(ReceiveMethodName, BrokerServiceName),
            }),
        };
    }

    private static BowireMethodInfo BuildSendMethod(string name, string service) => new(
        Name: name,
        FullName: $"amqp/{service}/{name}",
        ClientStreaming: false,
        ServerStreaming: false,
        InputType: new BowireMessageInfo("AmqpMessage", "amqp.Message", []),
        OutputType: new BowireMessageInfo("AmqpPublishAck", "amqp.PublishAck", []),
        MethodType: "Unary")
    {
        Summary = $"Publish to {service}",
        Description = "Publish a single AMQP message. Metadata keys override routing-key / address / content-type / message-id / correlation-id / delivery-mode / expiration / mandatory.",
    };

    private static BowireMethodInfo BuildReceiveMethod(string name, string service) => new(
        Name: name,
        FullName: $"amqp/{service}/{name}",
        ClientStreaming: false,
        ServerStreaming: true,
        InputType: new BowireMessageInfo("AmqpReceiveRequest", "amqp.ReceiveRequest", []),
        OutputType: new BowireMessageInfo("AmqpMessage", "amqp.Message", []),
        MethodType: "ServerStreaming")
    {
        Summary = $"Receive from {service}",
        Description = "Stream messages from the queue (0.9.1) or address (1.0). Metadata keys override queue / address / autoAck / receiveTimeoutSeconds.",
    };

    private static async Task<InvokeResult> InvokeV10Async(
        AmqpEndpoint endpoint, string service, string method, byte[] body,
        Dictionary<string, string>? metadata, Stopwatch sw, CancellationToken ct)
    {
        var address = ReadStringMeta(metadata, "address")
            ?? (string.IsNullOrEmpty(endpoint.AddressOrVhost) ? service : endpoint.AddressOrVhost);

        var connection = await Connection.Factory.CreateAsync(BuildAmqp10Address(endpoint)).ConfigureAwait(false);
        try
        {
            var session = new Session(connection);
            var sender = new SenderLink(session, $"bowire-sender-{Guid.NewGuid():N}", address);
            try
            {
                using var message = new Message(body)
                {
                    Properties = new global::Amqp.Framing.Properties
                    {
                        ContentType = ReadStringMeta(metadata, "contentType") ?? "application/json",
                        MessageId = ReadStringMeta(metadata, "messageId") ?? Guid.NewGuid().ToString("N"),
                        CorrelationId = ReadStringMeta(metadata, "correlationId"),
                    },
                };
                // SendAsync doesn't take a CancellationToken in AMQPNetLite 2.x;
                // we bound it via WaitAsync so a CT cancellation tears the link
                // down instead of hanging the request.
                await sender.SendAsync(message).WaitAsync(ct).ConfigureAwait(false);
                sw.Stop();
                return new InvokeResult(
                    Response: $"{{\"sent\":true,\"address\":\"{address}\",\"bytes\":{body.Length}}}",
                    DurationMs: sw.ElapsedMilliseconds,
                    Status: "OK",
                    Metadata: new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["wire"] = "amqp-1-0",
                        ["address"] = address,
                        ["payloadBytes"] = body.Length.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    });
            }
            finally
            {
                await sender.CloseAsync().ConfigureAwait(false);
                await session.CloseAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            await connection.CloseAsync().ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<string> ReceiveV10Async(
        AmqpEndpoint endpoint, string service, string method,
        Dictionary<string, string>? metadata, int receiveTimeoutSeconds,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var address = ReadStringMeta(metadata, "address")
            ?? (string.IsNullOrEmpty(endpoint.AddressOrVhost) ? service : endpoint.AddressOrVhost);

        var connection = await Connection.Factory.CreateAsync(BuildAmqp10Address(endpoint)).ConfigureAwait(false);
        Session? session = null;
        ReceiverLink? receiver = null;
        try
        {
            session = new Session(connection);
            receiver = new ReceiverLink(session, $"bowire-receiver-{Guid.NewGuid():N}", address);
            var idleCutoff = TimeSpan.FromSeconds(receiveTimeoutSeconds);
            while (!ct.IsCancellationRequested)
            {
                var message = await receiver.ReceiveAsync(idleCutoff).WaitAsync(ct).ConfigureAwait(false);
                if (message == null)
                {
                    // Idle timeout — finite stream contract, same as 0.9.1 consume.
                    yield break;
                }
                receiver.Accept(message);
                var bodyBytes = message.Body switch
                {
                    byte[] b => b,
                    string s => Encoding.UTF8.GetBytes(s),
                    _ => Encoding.UTF8.GetBytes(message.Body?.ToString() ?? string.Empty),
                };
                var json = TryDecodeJson(bodyBytes, message.Properties?.ContentType)
                    ?? $"\"{Convert.ToBase64String(bodyBytes)}\"";
                yield return json;
            }
        }
        finally
        {
            if (receiver != null) await receiver.CloseAsync().ConfigureAwait(false);
            if (session != null) await session.CloseAsync().ConfigureAwait(false);
            await connection.CloseAsync().ConfigureAwait(false);
        }
    }

    private static global::Amqp.Address BuildAmqp10Address(AmqpEndpoint endpoint)
    {
        // AMQPNetLite's Address(string url) constructor takes amqp:// or
        // amqps:// — the "amqp1" / "amqps1" Bowire-prefix has already done
        // its job (picking this wire) and must be translated back to the
        // standard scheme before the library sees it.
        var scheme = endpoint.UseTls ? "amqps" : "amqp";
        return new global::Amqp.Address(
            host: endpoint.Host,
            port: endpoint.Port,
            user: endpoint.UserName,
            password: endpoint.Password,
            path: string.IsNullOrEmpty(endpoint.AddressOrVhost) ? "/" : "/" + endpoint.AddressOrVhost,
            scheme: scheme);
    }

    // -------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------

    private static AmqpEndpoint ParseOrThrow(string serverUrl)
    {
        if (!AmqpEndpoint.TryParse(serverUrl, out var endpoint))
            throw new ArgumentException(
                $"Not a valid AMQP server URL: '{serverUrl}'. " +
                "Expected one of amqp:// amqps:// amqp1:// amqps1://", nameof(serverUrl));
        return endpoint;
    }

    private static string? ExtractRoutingKeyFromMethod(string method)
    {
        // "send:my.routing.key" → "my.routing.key". Bare "send" → null.
        const string sendPrefix = SendMethodName + ":";
        return method.StartsWith(sendPrefix, StringComparison.Ordinal)
            ? method[sendPrefix.Length..]
            : null;
    }

    private static string? ReadStringMeta(Dictionary<string, string>? metadata, string key)
        => metadata is not null && metadata.TryGetValue(key, out var v) && !string.IsNullOrEmpty(v) ? v : null;

    private static int ReadIntMeta(Dictionary<string, string>? metadata, string key, int @default)
        => metadata is not null && metadata.TryGetValue(key, out var v) && int.TryParse(v, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var i) ? i : @default;

    private static bool ReadBoolMeta(Dictionary<string, string>? metadata, string key, bool @default)
        => metadata is not null && metadata.TryGetValue(key, out var v) && bool.TryParse(v, out var b) ? b : @default;

    private static string? TryDecodeJson(ReadOnlySpan<byte> body, string? contentType)
    {
        // Treat content-type that names JSON as a hint that the bytes are
        // already JSON-safe to inline. Other content types (binary, octet-
        // stream) fall back to base64 at the caller — we don't second-guess.
        if (string.IsNullOrEmpty(contentType) || !contentType.Contains("json", StringComparison.OrdinalIgnoreCase))
            return null;
        try
        {
            // Validate the bytes parse as JSON; if they do, return the raw
            // string. If they don't, return null so the caller base64s.
            var text = Encoding.UTF8.GetString(body);
            using var _ = System.Text.Json.JsonDocument.Parse(text);
            return text;
        }
        catch
        {
            return null;
        }
    }
}
