// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Runtime.CompilerServices;
using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Models;

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
/// Same dispatch model as the gRPC plugin's Native / gRPC-Web split:
/// one plugin id, one NuGet package, multiple wire variants gated on
/// the URL scheme — keeps the protocol picker simple while letting
/// AsyncAPI route both <c>bindings.amqp</c> and <c>bindings.amqp1</c>
/// at this single plugin.
/// </summary>
/// <remarks>
/// <para>
/// <strong>Status:</strong> 0.1.x skeleton. Discovery, invocation, and
/// streaming throw <see cref="NotImplementedException"/>. The shape of
/// the public surface (id, name, settings, icon, the
/// <see cref="IBowireProtocol"/> method set) is fixed in this release so
/// embedded hosts can reference the plugin and `IAsyncApiBindingResolver`
/// in the main Bowire repo can target it once the implementation lands.
/// </para>
/// <para>
/// <strong>Planned discovery</strong> (subsequent release):
/// <list type="bullet">
///   <item>0.9.1: RabbitMQ Management HTTP API (<c>/api/exchanges</c>,
///     <c>/api/queues</c>) — one Bowire service per exchange, one
///     method per binding. Fallback: flat list of well-known exchanges
///     when Management plugin is absent.</item>
///   <item>1.0: no broker-side schema concept — synthetic <c>Broker</c>
///     service with <c>Send</c> + <c>Receive</c> methods that take the
///     target address as metadata.</item>
/// </list>
/// </para>
/// <para>
/// <strong>Planned invocation</strong>:
/// <list type="bullet">
///   <item>0.9.1: <c>basic.publish</c> with exchange + routing-key +
///     properties (delivery mode, expiration, headers) from the
///     metadata bag — matches the AsyncAPI 0.9.1 binding spec.</item>
///   <item>1.0: <c>Session.Send</c> against the target address.</item>
/// </list>
/// </para>
/// </remarks>
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
    // Stylised AMQP envelope: outer queue rectangle, two arrows
    // (publish + consume), pinned to a muted indigo so it sits next
    // to the existing protocol icons without colliding with Kafka's
    // blue or MQTT's purple.
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
    ];

    /// <inheritdoc />
    public Task<List<BowireServiceInfo>> DiscoverAsync(
        string serverUrl, bool showInternalServices, CancellationToken ct = default)
    {
        // Skeleton release: the surface is wired up so AsyncAPI's
        // binding lookup and the protocol registry can pin against it,
        // but the wire-side discovery (Management HTTP API for 0.9.1,
        // synthetic Broker service for 1.0) lands in the next release.
        throw new NotImplementedException(
            "Bowire.Protocol.Amqp 0.1.x is a skeleton release — discovery " +
            "lands in 0.2.x. Track progress at " +
            "https://github.com/Kuestenlogik/Bowire/blob/main/ROADMAP.md");
    }

    /// <inheritdoc />
    public Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default)
    {
        throw new NotImplementedException(
            "Bowire.Protocol.Amqp 0.1.x is a skeleton release — invocation " +
            "(basic.publish for 0.9.1, Session.Send for 1.0) lands in 0.2.x.");
    }

    /// <inheritdoc />
#pragma warning disable CS1998 // No-op stream stub until 0.2.x.
    public async IAsyncEnumerable<string> InvokeStreamAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Streaming consume / receive lands in 0.2.x. Yielding here
        // would be wrong (silent no-op); throw so callers see the
        // skeleton state clearly.
        throw new NotImplementedException(
            "Bowire.Protocol.Amqp 0.1.x is a skeleton release — streaming " +
            "consume (0.9.1) / receive (1.0) lands in 0.2.x.");
#pragma warning disable CS0162 // Unreachable kept so the IAsyncEnumerable contract still compiles.
        yield break;
#pragma warning restore CS0162
    }
#pragma warning restore CS1998

    /// <inheritdoc />
    public Task<IBowireChannel?> OpenChannelAsync(
        string serverUrl, string service, string method,
        bool showInternalServices, Dictionary<string, string>? metadata = null,
        CancellationToken ct = default)
    {
        // AMQP doesn't natively map onto Bowire's bidi-channel surface
        // (channels there model WebSocket-style frame streams). Both
        // wire variants will route their streaming work through
        // InvokeStreamAsync above; OpenChannelAsync stays null.
        return Task.FromResult<IBowireChannel?>(null);
    }
}
