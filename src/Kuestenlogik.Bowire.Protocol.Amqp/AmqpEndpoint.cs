// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics.CodeAnalysis;
using System.Net;

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>
/// Wire variant of an AMQP endpoint. Picked by the URL scheme — the
/// plugin runs a single discover/invoke surface and routes internally
/// based on this value (same pattern the gRPC plugin uses to pick
/// native HTTP/2 vs. gRPC-Web).
/// </summary>
public enum AmqpWire
{
    /// <summary>AMQP 0.9.1 — RabbitMQ, ActiveMQ Classic. Wire stack: <c>RabbitMQ.Client</c>.</summary>
    V091,
    /// <summary>AMQP 1.0 — Azure Service Bus, ActiveMQ Artemis. Wire stack: <c>AMQPNetLite</c>.</summary>
    V10,
}

/// <summary>
/// Parsed AMQP endpoint URL. Holds the wire variant + connection coordinates
/// + the AMQP-flavour-specific tail (vhost for 0.9.1, address for 1.0) so
/// callers can hand the right surface to <c>RabbitMQ.Client</c> or
/// <c>AMQPNetLite</c> without re-parsing the URL.
/// </summary>
/// <remarks>
/// Scheme matrix:
/// <list type="bullet">
///   <item><c>amqp://[user:pass@]host[:port][/vhost]</c> → 0.9.1, TLS off, default port 5672</item>
///   <item><c>amqps://[user:pass@]host[:port][/vhost]</c> → 0.9.1, TLS on, default port 5671</item>
///   <item><c>amqp1://[user:pass@]host[:port][/address]</c> → 1.0, TLS off, default port 5672</item>
///   <item><c>amqps1://[user:pass@]host[:port][/address]</c> → 1.0, TLS on, default port 5671</item>
/// </list>
/// The 0.9.1 path defaults the vhost to <c>"/"</c> per RabbitMQ convention
/// when none is in the URL. The 1.0 path leaves <see cref="AddressOrVhost"/>
/// empty for callers to fill from <c>service</c>/<c>method</c> later.
/// </remarks>
public sealed record AmqpEndpoint(
    AmqpWire Wire,
    string Host,
    int Port,
    bool UseTls,
    string? UserName,
    string? Password,
    string AddressOrVhost,
    // Per-connection overrides for the plugin-wide settings. Picked up
    // from the URL query string at parse time:
    //   amqp://host:5672/vhost?_mgmtPort=15673&_discoveryTimeout=10&_receiveTimeout=60
    // null means "fall through to the plugin defaults declared on
    // BowireAmqpProtocol.Settings". Discovery has no metadata channel
    // (the IBowireProtocol.DiscoverAsync contract is metadata-less), so
    // the URL is the only place a caller can override these on a
    // per-connection basis.
    int? ManagementPort = null,
    int? DiscoveryTimeoutSeconds = null,
    int? ReceiveTimeoutSeconds = null)
{
    /// <summary>
    /// Try to parse a Bowire-style AMQP server URL. Returns <c>false</c> on
    /// an unrecognised scheme, blank host, or out-of-range port. Failure is
    /// non-throwing so callers can fall through to a clean
    /// <see cref="ArgumentException"/> with the offending URL embedded.
    /// </summary>
    public static bool TryParse(
        string serverUrl,
        [NotNullWhen(true)] out AmqpEndpoint? endpoint)
    {
        endpoint = null;
        if (string.IsNullOrWhiteSpace(serverUrl)) return false;

        // We accept the four custom schemes (amqp/amqps for 0.9.1, amqp1/
        // amqps1 for 1.0). Uri.TryCreate happily parses them all as long as
        // the host is non-empty.
        if (!Uri.TryCreate(serverUrl, UriKind.Absolute, out var uri)) return false;

        AmqpWire wire;
        bool tls;
        int defaultPort;
        switch (uri.Scheme.ToLowerInvariant())
        {
            case "amqp":   wire = AmqpWire.V091; tls = false; defaultPort = 5672; break;
            case "amqps":  wire = AmqpWire.V091; tls = true;  defaultPort = 5671; break;
            case "amqp1":  wire = AmqpWire.V10;  tls = false; defaultPort = 5672; break;
            case "amqps1": wire = AmqpWire.V10;  tls = true;  defaultPort = 5671; break;
            default: return false;
        }
        if (string.IsNullOrEmpty(uri.Host)) return false;

        var port = uri.IsDefaultPort ? defaultPort : uri.Port;
        if (port <= 0 || port > 65535) return false;

        string? user = null, pass = null;
        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var split = uri.UserInfo.Split(':', 2);
            user = WebUtility.UrlDecode(split[0]);
            if (split.Length == 2) pass = WebUtility.UrlDecode(split[1]);
        }

        // AbsolutePath includes a leading '/'. For 0.9.1 the vhost is the
        // path tail (RabbitMQ convention: empty path → vhost "/"). For 1.0
        // the path is left to the caller's service/method composition; if
        // the URL carries one we still ship it through so a callsite can
        // pick it up as a default address.
        var tail = string.IsNullOrEmpty(uri.AbsolutePath) || uri.AbsolutePath == "/"
            ? (wire == AmqpWire.V091 ? "/" : string.Empty)
            : WebUtility.UrlDecode(uri.AbsolutePath.TrimStart('/'));

        // Per-connection setting overrides via URL query. Three keys; each
        // optional. Unknown keys are ignored (forward-compat: an older
        // plugin reading a URL that an newer workbench wrote with extra
        // tweaks should keep working). Invalid integer values fall back
        // to null → plugin defaults.
        var (mgmtPort, discoveryTimeout, receiveTimeout) = ParseSettingsQuery(uri.Query);

        endpoint = new AmqpEndpoint(
            Wire: wire,
            Host: uri.Host,
            Port: port,
            UseTls: tls,
            UserName: user,
            Password: pass,
            AddressOrVhost: tail,
            ManagementPort: mgmtPort,
            DiscoveryTimeoutSeconds: discoveryTimeout,
            ReceiveTimeoutSeconds: receiveTimeout);
        return true;
    }

    private static (int? Mgmt, int? DiscoveryTimeout, int? ReceiveTimeout) ParseSettingsQuery(string query)
    {
        if (string.IsNullOrEmpty(query) || query == "?")
            return (null, null, null);

        int? mgmt = null, discovery = null, receive = null;
        // Hand-roll instead of HttpUtility / QueryHelpers to stay free of
        // a System.Web / Microsoft.AspNetCore reference at plugin scope.
        foreach (var raw in query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = raw.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var key = WebUtility.UrlDecode(raw[..eq]);
            var val = WebUtility.UrlDecode(raw[(eq + 1)..]);

            // Underscore-prefixed names are Bowire-private settings keys;
            // they don't collide with broker-native query params (RabbitMQ
            // uses e.g. heartbeat, connection_timeout — bare names).
            switch (key)
            {
                case "_mgmtPort":
                    if (int.TryParse(val, out var p) && p is > 0 and <= 65535) mgmt = p;
                    break;
                case "_discoveryTimeout":
                    if (int.TryParse(val, out var d) && d > 0) discovery = d;
                    break;
                case "_receiveTimeout":
                    if (int.TryParse(val, out var r) && r > 0) receive = r;
                    break;
            }
        }
        return (mgmt, discovery, receive);
    }
}
