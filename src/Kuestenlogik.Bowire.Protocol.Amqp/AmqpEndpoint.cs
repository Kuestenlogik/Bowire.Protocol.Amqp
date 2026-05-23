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
    string AddressOrVhost)
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

        endpoint = new AmqpEndpoint(
            Wire: wire,
            Host: uri.Host,
            Port: port,
            UseTls: tls,
            UserName: user,
            Password: pass,
            AddressOrVhost: tail);
        return true;
    }
}
