// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Amqp;
using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Models;
using Kuestenlogik.Bowire.Plugins;
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

    // Plugin-wide defaults. Mirrored as the DefaultValue on the
    // corresponding BowirePluginSetting entry below so the workbench's
    // settings dialog and the runtime stay in lockstep. Per-connection
    // overrides ride in on the URL query (?_mgmtPort=… &c.) — see
    // AmqpEndpoint.ParseSettingsQuery.
    internal const int DefaultManagementPort = 15672;
    internal const int DefaultDiscoveryTimeoutSeconds = 5;
    internal const int DefaultReceiveTimeoutSeconds = 30;
    internal const string Amqp10DiscoverySettingKey = "amqp10Discovery";

    /// <summary>
    /// Resolved in <see cref="Initialize"/>; null when the host
    /// registered none — the CLI's case, and every embedded host that
    /// never adopted workspace settings. The declared defaults then
    /// stand, which is what a plugin nobody configured should do.
    /// </summary>
    private IBowirePluginSettings? _settings;

    /// <summary>
    /// One client for every Service Bus namespace this process
    /// discovers. HttpClient is built to be shared; a new one per
    /// discovery would burn a socket per refresh.
    /// </summary>
    private static readonly HttpClient s_managementHttp = new();

    /// <inheritdoc />
    public void Initialize(IServiceProvider? serviceProvider)
        => _settings = serviceProvider?.GetService(typeof(IBowirePluginSettings)) as IBowirePluginSettings;

    /// <inheritdoc />
    public string Name => "AMQP";

    /// <inheritdoc />
    public string Id => "amqp";

    /// <inheritdoc />
    /// <remarks>
    /// Official AMQP brand mark from amqp.org — the geometric symbol
    /// only, no wordmark. Paths copied from the SVG the Bowire
    /// marketing site ships at /assets/images/protocols/amqp-mark.svg,
    /// cropped to a tight 24x24 viewBox so the mark fills the
    /// sidebar's icon slot at any size and matches what the user
    /// sees on the protocol card / downloads tile. The four
    /// hard-coded brand colours (#a2b0d9 / #002585 / #000000 /
    /// #caccce) are part of the mark and don't follow the workbench
    /// theme — same convention every other protocol icon uses for
    /// vendor-defined brand marks (MQTT's purple, Kafka's
    /// hex-cluster, etc.).
    /// </remarks>
    public string IconSvg => """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><g transform="translate(-38.00395,-184.20569)"><path style="fill:#a2b0d9" d="m 46.625336,184.20569 h 6.456585 v 14.97479 l -6.456585,-6.44351 z"/><path style="fill:#002585" d="m 46.626352,192.72959 6.461507,6.45658 H 38.00395 v -6.45658 z"/><path d="m 38.048463,184.21303 5.959091,5.98243 h -5.959091 z"/><path style="fill:#caccce" d="m 55.318237,184.20569 h 6.456585 v 23.72775 l -6.456585,-6.44351 z"/><path style="fill:#caccce" d="m 38.049298,184.21436 h 2.967838 2.988511 v 2.97153 3.00989 z"/><path d="m 55.316758,201.4825 6.460929,6.45658 h -23.76618 v -6.45658 z"/></g></svg>""";

    /// <inheritdoc />
    public IReadOnlyList<BowirePluginSetting> Settings =>
    [
        new("managementApiPort", "Management API port",
            $"RabbitMQ Management HTTP API port for discovery (0.9.1 only). Per-connection override: `?_mgmtPort=<n>` in the URL. Default {DefaultManagementPort}.",
            "number", DefaultManagementPort),
        new("discoveryTimeoutSeconds", "Discovery timeout",
            $"Max seconds to wait on broker / management-API responses during discovery. Per-connection override: `?_discoveryTimeout=<n>`. Default {DefaultDiscoveryTimeoutSeconds}.",
            "number", DefaultDiscoveryTimeoutSeconds),
        new("receiveTimeoutSeconds", "Receive timeout",
            $"Max seconds the streaming consume/receive operation waits between frames before tearing down. Per-connection override: `?_receiveTimeout=<n>` in the URL or `receiveTimeoutSeconds` in metadata. Default {DefaultReceiveTimeoutSeconds}.",
            "number", DefaultReceiveTimeoutSeconds),
        new(Amqp10DiscoverySettingKey, "AMQP 1.0 discovery",
            "Which broker's management surface to ask when discovering an `amqp1://` endpoint. AMQP 1.0 has no discovery of its own, but Artemis answers management requests over the AMQP connection itself and Service Bus serves an ATOM feed signed with the key already in the URL. `auto` picks Service Bus for a `*.servicebus.*` host and tries Artemis otherwise, falling back to the generic Broker service when neither answers. Per-connection override: `?_amqp10Discovery=<value>`.",
            "select", "auto",
            [
                new BowirePluginSettingOption("auto", "Auto — by host, then probe"),
                new BowirePluginSettingOption("artemis", "ActiveMQ Artemis"),
                new BowirePluginSettingOption("servicebus", "Azure Service Bus"),
                new BowirePluginSettingOption("none", "None — generic Broker service"),
            ]),
    ];

    /// <inheritdoc />
    public async Task<List<BowireServiceInfo>> DiscoverAsync(
        string serverUrl, bool showInternalServices, CancellationToken ct = default)
    {
        var endpoint = ParseOrThrow(serverUrl);

        return endpoint.Wire switch
        {
            AmqpWire.V091 => await DiscoverV091Async(endpoint, showInternalServices, ct).ConfigureAwait(false),
            AmqpWire.V10 => await DiscoverV10Async(endpoint, showInternalServices, ct).ConfigureAwait(false),
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
        // Resolution order for receiveTimeoutSeconds:
        //   1. metadata key (per-invoke override, e.g. from the workbench)
        //   2. URL-query override (?_receiveTimeout=N, picked up at parse)
        //   3. plugin default (DefaultReceiveTimeoutSeconds = 30 s)
        var receiveTimeout = ReadIntMeta(metadata, "receiveTimeoutSeconds",
            endpoint.ReceiveTimeoutSeconds ?? DefaultReceiveTimeoutSeconds);

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
        // wire port. Default DefaultManagementPort (15672); overridable
        // per connection via `?_mgmtPort=<n>` on the URL (picked up by
        // AmqpEndpoint.TryParse). The IBowireProtocol.DiscoverAsync
        // contract carries no metadata bag, so URL-query is the only
        // path a caller has to tweak discovery-time settings.
        var mgmtPort = endpoint.ManagementPort ?? DefaultManagementPort;
        var discoveryTimeout = TimeSpan.FromSeconds(
            endpoint.DiscoveryTimeoutSeconds ?? DefaultDiscoveryTimeoutSeconds);
        var mgmt = new RabbitMqManagement(endpoint, managementPort: mgmtPort, timeout: discoveryTimeout);
        try
        {
            var exchanges = await mgmt.GetExchangesAsync(ct).ConfigureAwait(false);
            var bindings = await mgmt.GetBindingsAsync(ct).ConfigureAwait(false);
            var queues = await mgmt.GetQueuesAsync(ct).ConfigureAwait(false);

            var services = new List<BowireServiceInfo>();

            // -------- Exchanges → publish (send) endpoints --------
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

            // -------- Queues → consume (receive) endpoints --------
            // The consumer surface lives on queues, not exchanges. Without
            // this, the workbench could only publish into AMQP and never
            // subscribe to anything — the half of the story most users
            // come for.
            foreach (var q in queues)
            {
                // RabbitMQ creates an `amq.gen-*` queue per anonymous
                // subscriber; those are noise in the sidebar unless the
                // operator opted in via showInternalServices.
                if (!showInternalServices && q.Name.StartsWith("amq.", StringComparison.Ordinal)) continue;

                services.Add(new BowireServiceInfo(q.Name, "amqp", new List<BowireMethodInfo>
                {
                    BuildReceiveMethod(ReceiveMethodName, q.Name),
                }));
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
        // Apply mTLS + SASL markers and strip them from metadata before
        // any of the headers below pick it up — the markers are
        // Bowire-private and must not leak as broker properties.
        metadata = AmqpSecurityConfig.ApplyV091(factory, metadata);
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

        // `mandatory` rides on BasicPublishAsync directly (no Properties
        // bit for it) — the broker returns the message via basic.return
        // when no queue is bound. Useful for surfacing "publish to a
        // dead routing key" cases as visible failures.
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
        metadata = AmqpSecurityConfig.ApplyV091(factory, metadata);
        await using var conn = await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
        await using var channel = await conn.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);

        var queueFrames = new System.Threading.Channels.UnboundedChannelOptions { SingleReader = true, SingleWriter = false };
        var pipe = System.Threading.Channels.Channel.CreateUnbounded<string>(queueFrames);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var envelope = BuildV091Envelope(ea);
            await pipe.Writer.WriteAsync(envelope, ct).ConfigureAwait(false);
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

    /// <summary>
    /// AMQP 1.0 discovery. The spec has none — but the two brokers Bowire
    /// meets most each answer a question about themselves, and neither
    /// needs a credential the connection does not already carry:
    /// Artemis takes management requests over the AMQP connection itself,
    /// Service Bus serves an ATOM feed signed with the shared-access key
    /// that is already in the URL. Which one to ask comes from the
    /// <c>amqp10Discovery</c> setting (per-connection:
    /// <c>?_amqp10Discovery=…</c>); <c>auto</c> reads the host and probes.
    /// Whatever fails to answer falls back to the generic
    /// <see cref="BrokerServiceName"/> service, so a broker that is
    /// neither — Solace, Qpid, a bespoke 1.0 endpoint — behaves exactly
    /// as it did before.
    /// </summary>
    private async Task<List<BowireServiceInfo>> DiscoverV10Async(
        AmqpEndpoint endpoint, bool showInternalServices, CancellationToken ct)
    {
        var requested = endpoint.Flavour
            ?? Amqp10Flavours.Parse(_settings?.GetValue(Id, Amqp10DiscoverySettingKey));
        if (requested == Amqp10Flavour.None) return SyntheticBrokerServices();

        var timeout = TimeSpan.FromSeconds(
            endpoint.DiscoveryTimeoutSeconds ?? DefaultDiscoveryTimeoutSeconds);
        var flavour = Amqp10Flavours.Resolve(requested, endpoint.Host);

        if (flavour == Amqp10Flavour.ServiceBus)
        {
            var entities = await ServiceBusManagement
                .ListEntitiesAsync(s_managementHttp, endpoint, timeout, ct).ConfigureAwait(false);
            if (entities is not null) return BuildServiceBusServices(entities, showInternalServices);
            // An explicit `servicebus` that could not be read is the
            // operator's answer about which broker this is, so there is
            // nothing else to try; `auto` on a servicebus host is the
            // same conclusion by a different route.
            return SyntheticBrokerServices();
        }

        var addresses = await TryListArtemisAddressesAsync(endpoint, timeout, ct).ConfigureAwait(false);
        if (addresses is not null) return BuildArtemisServices(addresses, showInternalServices);
        return SyntheticBrokerServices();
    }

    /// <summary>
    /// Ask the broker's management address, on a connection of its own.
    /// Null when the endpoint is not Artemis — no address, no answer, or
    /// a connection that could not be opened at all (a broker that is
    /// down is not a broker whose shape we can guess).
    /// </summary>
    private static async Task<List<ArtemisAddress>?> TryListArtemisAddressesAsync(
        AmqpEndpoint endpoint, TimeSpan timeout, CancellationToken ct)
    {
        Connection? connection = null;
        try
        {
            var factory = new global::Amqp.ConnectionFactory();
            connection = await factory.CreateAsync(BuildAmqp10Address(endpoint))
                .WaitAsync(timeout, ct).ConfigureAwait(false);
            return await ArtemisManagement.ListAddressesAsync(connection, timeout, ct).ConfigureAwait(false);
        }
        catch (AmqpException)
        {
            return null;
        }
        catch (System.Net.Sockets.SocketException)
        {
            return null;
        }
        catch (TimeoutException)
        {
            return null;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return null;
        }
        finally
        {
            if (connection is not null)
            {
                try { await connection.CloseAsync().ConfigureAwait(false); }
                catch (AmqpException) { /* closing a broken connection */ }
            }
        }
    }

    /// <summary>The generic surface: what every 1.0 endpoint had before, and what an unrecognised one still has.</summary>
    private static List<BowireServiceInfo> SyntheticBrokerServices() =>
    [
        new(BrokerServiceName, "amqp",
        [
            BuildSendMethod(SendMethodName, BrokerServiceName),
            BuildReceiveMethod(ReceiveMethodName, BrokerServiceName),
        ])
        {
            Description = "AMQP 1.0 endpoint without a readable broker surface — send and receive against an address you name (metadata key `address`, or the URL path).",
        },
    ];

    /// <summary>
    /// Artemis' addresses as services. An address is where a message is
    /// sent; a queue bound to it is where one is received, addressed in
    /// Artemis' fully-qualified form (<c>address::queue</c>) so a
    /// multicast address with several subscriptions can be told apart.
    /// The queue whose name is its address' — the ordinary anycast case —
    /// is also reachable as the bare <c>receive</c>.
    /// </summary>
    internal static List<BowireServiceInfo> BuildArtemisServices(
        List<ArtemisAddress> addresses, bool showInternalServices)
    {
        var services = new List<BowireServiceInfo>();
        foreach (var address in addresses.OrderBy(a => a.Name, StringComparer.Ordinal))
        {
            // Internal plumbing (`$sys.*`, activemq.notifications) and the
            // temporary addresses a dynamic reply queue leaves behind —
            // including the one this discovery just made — are noise on a
            // sidebar. Same rule the 0.9.1 path applies to `amq.*`.
            if (!showInternalServices && (address.Internal || address.Temporary || IsArtemisInternal(address.Name)))
                continue;

            var methods = new List<BowireMethodInfo> { BuildSendMethod(SendMethodName, address.Name) };
            foreach (var queue in address.Queues.OrderBy(q => q.Name, StringComparer.Ordinal))
            {
                if (!showInternalServices && (queue.Internal || queue.Temporary)) continue;
                methods.Add(string.Equals(queue.Name, address.Name, StringComparison.Ordinal)
                    ? BuildReceiveMethod(ReceiveMethodName, address.Name)
                    : BuildReceiveMethod($"{ReceiveMethodName}:{queue.Name}", address.Name));
            }
            // An address with no queue can still be sent to — a multicast
            // address nobody subscribes to yet accepts and drops.
            if (methods.Count == 1)
                methods.Add(BuildReceiveMethod(ReceiveMethodName, address.Name));

            // No Source: the host's discovery probe stamps it with the
            // plugin id for every service it collects. Where a service
            // came from is in its description, which survives and is
            // what the sidebar shows.
            services.Add(new BowireServiceInfo(address.Name, "amqp", methods)
            {
                Description = DescribeArtemisAddress(address),
            });
        }
        return services.Count > 0 ? services : SyntheticBrokerServices();
    }

    private static string DescribeArtemisAddress(ArtemisAddress address)
    {
        var routing = address.RoutingTypes.Count > 0
            ? string.Join("/", address.RoutingTypes)
            : "ANYCAST";
        var queues = address.Queues.Count switch
        {
            0 => "no queues",
            1 => "1 queue",
            var n => $"{n} queues",
        };
        return $"Artemis address ({routing}, {queues})";
    }

    // Artemis' own plumbing. `$sys.*` is the broker's internal namespace,
    // `activemq.*` its notification and management addresses — including
    // the one discovery just spoke to.
    private static bool IsArtemisInternal(string name)
        => name.StartsWith("$sys.", StringComparison.Ordinal)
        || name.StartsWith("activemq.", StringComparison.Ordinal);

    /// <summary>
    /// A Service Bus namespace as services: a queue is a service with
    /// send + receive, a topic is a service with send and one receive per
    /// subscription (addressed <c>topic/Subscriptions/name</c>, which is
    /// what the AMQP link wants).
    /// </summary>
    internal static List<BowireServiceInfo> BuildServiceBusServices(
        List<ServiceBusEntity> entities, bool showInternalServices)
    {
        var services = new List<BowireServiceInfo>();
        foreach (var entity in entities.OrderBy(e => e.Name, StringComparer.Ordinal))
        {
            if (!showInternalServices && entity.Name.StartsWith('$')) continue;

            var methods = new List<BowireMethodInfo> { BuildSendMethod(SendMethodName, entity.Name) };
            if (entity.Kind == ServiceBusEntityKind.Queue)
            {
                methods.Add(BuildReceiveMethod(ReceiveMethodName, entity.Name));
            }
            else
            {
                foreach (var subscription in entity.Subscriptions.OrderBy(s => s, StringComparer.Ordinal))
                    methods.Add(BuildReceiveMethod($"{ReceiveMethodName}:{subscription}", entity.Name));
            }

            services.Add(new BowireServiceInfo(entity.Name, "amqp", methods)
            {
                Description = entity.Kind == ServiceBusEntityKind.Queue
                    ? "Service Bus queue"
                    : $"Service Bus topic ({(entity.Subscriptions.Count == 1 ? "1 subscription" : entity.Subscriptions.Count + " subscriptions")})",
            });
        }
        return services.Count > 0 ? services : SyntheticBrokerServices();
    }

    /// <summary>
    /// The address a 1.0 send or receive works against. The method's
    /// suffix names a queue (Artemis) or a subscription (Service Bus) on
    /// the service's address; metadata still wins over both, and an
    /// endpoint whose URL carries a path keeps using it when the service
    /// is the synthetic broker.
    /// </summary>
    internal static string ResolveV10Address(
        AmqpEndpoint endpoint, string service, string method, Dictionary<string, string>? metadata)
    {
        if (ReadStringMeta(metadata, "address") is { } explicitAddress) return explicitAddress;

        var suffix = ExtractReceiveSuffix(method);
        if (suffix is not null && !string.Equals(service, BrokerServiceName, StringComparison.Ordinal))
        {
            // Service Bus wants the subscription path; Artemis wants the
            // fully-qualified queue name. A subscription name cannot
            // contain '/', and an Artemis queue name cannot contain
            // "::", so the two shapes stay apart — but the endpoint's
            // flavour is what actually decides, and the host is the only
            // part of it discovery and invoke both see.
            return Amqp10Flavours.IsServiceBusHost(endpoint.Host)
                ? $"{service}/Subscriptions/{suffix}"
                : $"{service}::{suffix}";
        }

        if (!string.Equals(service, BrokerServiceName, StringComparison.Ordinal)) return service;
        return string.IsNullOrEmpty(endpoint.AddressOrVhost) ? service : endpoint.AddressOrVhost;
    }

    /// <summary>"receive:orders" → "orders". Bare "receive", or any other method, → null.</summary>
    private static string? ExtractReceiveSuffix(string method)
    {
        const string receivePrefix = ReceiveMethodName + ":";
        return method.StartsWith(receivePrefix, StringComparison.Ordinal)
            ? method[receivePrefix.Length..]
            : null;
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
        var address = ResolveV10Address(endpoint, service, method, metadata);

        var amqpFactory = new global::Amqp.ConnectionFactory();
        metadata = AmqpSecurityConfig.ApplyV10(amqpFactory, metadata);
        var connection = await amqpFactory.CreateAsync(BuildAmqp10Address(endpoint)).ConfigureAwait(false);
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
        var address = ResolveV10Address(endpoint, service, method, metadata);

        var amqpFactory = new global::Amqp.ConnectionFactory();
        metadata = AmqpSecurityConfig.ApplyV10(amqpFactory, metadata);
        var connection = await amqpFactory.CreateAsync(BuildAmqp10Address(endpoint)).ConfigureAwait(false);
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
                yield return BuildV10Envelope(message, bodyBytes, address);
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

    /// <summary>
    /// Serialise a 0.9.1 delivery into the Bowire receive envelope. Mirrors
    /// the Kafka plugin's pattern: a JSON object that surfaces the
    /// transport-level metadata the broker hands us (exchange / routingKey /
    /// deliveryTag / contentType / messageId / correlationId / timestamp),
    /// plus the decoded body under either <c>value</c> (when the bytes parse
    /// as JSON per the content-type) or <c>bytes</c> (base64, when they
    /// don't). The <c>encoding</c> field tells the workbench which branch it
    /// was — so the body-viewer picks the right renderer without sniffing.
    /// </summary>
    private static string BuildV091Envelope(BasicDeliverEventArgs ea)
    {
        var contentType = ea.BasicProperties?.ContentType;
        var bodySpan = ea.Body.Span;
        var inlineJson = TryDecodeJson(bodySpan, contentType);

        using var stream = new System.IO.MemoryStream();
        using (var writer = new System.Text.Json.Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteString("exchange", ea.Exchange ?? string.Empty);
            writer.WriteString("routingKey", ea.RoutingKey ?? string.Empty);
            writer.WriteNumber("deliveryTag", ea.DeliveryTag);
            writer.WriteBoolean("redelivered", ea.Redelivered);
            if (!string.IsNullOrEmpty(contentType))
                writer.WriteString("contentType", contentType);
            if (!string.IsNullOrEmpty(ea.BasicProperties?.MessageId))
                writer.WriteString("messageId", ea.BasicProperties.MessageId);
            if (!string.IsNullOrEmpty(ea.BasicProperties?.CorrelationId))
                writer.WriteString("correlationId", ea.BasicProperties.CorrelationId);
            if (ea.BasicProperties?.IsTimestampPresent() == true)
                writer.WriteNumber("timestamp", ea.BasicProperties.Timestamp.UnixTime);
            if (inlineJson is not null)
            {
                writer.WriteString("encoding", "json");
                writer.WritePropertyName("value");
                // Re-emit the JSON token tree so the consumer sees parsed
                // JSON rather than a string-escaped blob.
                using var doc = System.Text.Json.JsonDocument.Parse(inlineJson);
                doc.RootElement.WriteTo(writer);
            }
            else
            {
                writer.WriteString("encoding", "base64");
                writer.WriteString("bytes", Convert.ToBase64String(bodySpan.ToArray()));
            }
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    /// <summary>
    /// 1.0 counterpart to <see cref="BuildV091Envelope"/>. The 1.0 wire has
    /// no exchange/routing-key concept — the address replaces both — and
    /// no native deliveryTag (we count locally). Everything else mirrors
    /// the 0.9.1 shape so the workbench's body-viewer doesn't need a
    /// wire-aware branch.
    /// </summary>
    private static string BuildV10Envelope(global::Amqp.Message message, byte[] bodyBytes, string address)
    {
        var contentType = message.Properties?.ContentType;
        var inlineJson = TryDecodeJson(bodyBytes, contentType);

        using var stream = new System.IO.MemoryStream();
        using (var writer = new System.Text.Json.Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteString("address", address);
            if (!string.IsNullOrEmpty(contentType))
                writer.WriteString("contentType", contentType);
            if (!string.IsNullOrEmpty(message.Properties?.MessageId))
                writer.WriteString("messageId", message.Properties.MessageId);
            if (!string.IsNullOrEmpty(message.Properties?.CorrelationId))
                writer.WriteString("correlationId", message.Properties.CorrelationId);
            if (message.Properties?.CreationTime is { } created && created != default)
                writer.WriteNumber("timestamp", new DateTimeOffset(created, TimeSpan.Zero).ToUnixTimeSeconds());
            if (inlineJson is not null)
            {
                writer.WriteString("encoding", "json");
                writer.WritePropertyName("value");
                using var doc = System.Text.Json.JsonDocument.Parse(inlineJson);
                doc.RootElement.WriteTo(writer);
            }
            else
            {
                writer.WriteString("encoding", "base64");
                writer.WriteString("bytes", Convert.ToBase64String(bodyBytes));
            }
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

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
