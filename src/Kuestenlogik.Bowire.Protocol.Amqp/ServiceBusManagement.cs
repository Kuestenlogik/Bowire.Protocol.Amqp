// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>One Service Bus entity: a queue, a topic, or a subscription on a topic.</summary>
internal sealed record ServiceBusEntity(string Name, ServiceBusEntityKind Kind, List<string> Subscriptions);

internal enum ServiceBusEntityKind
{
    Queue,
    Topic,
}

/// <summary>
/// Azure Service Bus' management surface: the namespace's ATOM feed over
/// HTTPS, signed with the same shared-access key the AMQP connection
/// carries in its user info.
/// </summary>
/// <remarks>
/// <para>
/// A Service Bus namespace is reached as
/// <c>amqps1://&lt;keyName&gt;:&lt;key&gt;@&lt;ns&gt;.servicebus.windows.net</c> — the
/// shape of a connection string, split the way every AMQP URL is. The
/// management feed wants the same pair as a SAS token in the
/// <c>Authorization</c> header, so discovery needs no second credential:
/// <c>GET /$Resources/queues</c> and <c>/$Resources/topics</c> list the
/// namespace, <c>GET /&lt;topic&gt;/Subscriptions</c> lists a topic's
/// subscriptions.
/// </para>
/// <para>
/// The ATOM feed is the long-standing management API (the one the older
/// SDKs' <c>NamespaceManager</c> used); the ARM control plane would need
/// an Entra token, a subscription id and a resource group, which is a
/// different credential than the one in the URL and belongs to a
/// different consent conversation. Anything the key cannot read comes
/// back 401 and discovery falls back to the bare broker service rather
/// than failing the connection.
/// </para>
/// </remarks>
internal static class ServiceBusManagement
{
    /// <summary>The management API version the ATOM feed is pinned to.</summary>
    internal const string ApiVersion = "2021-05";

    private static readonly XNamespace Atom = "http://www.w3.org/2005/Atom";

    /// <summary>
    /// The namespace's queues and topics (with each topic's
    /// subscriptions), or null when the endpoint carries no shared-access
    /// key or the feed refused it.
    /// </summary>
    public static async Task<List<ServiceBusEntity>?> ListEntitiesAsync(
        HttpClient http, AmqpEndpoint endpoint, TimeSpan timeout, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(http);
        ArgumentNullException.ThrowIfNull(endpoint);
        if (string.IsNullOrEmpty(endpoint.UserName) || string.IsNullOrEmpty(endpoint.Password)) return null;

        var baseUri = ManagementBaseUri(endpoint);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        try
        {
            var queues = await ListAsync(http, baseUri, "$Resources/queues", endpoint, cts.Token).ConfigureAwait(false);
            if (queues is null) return null;
            var topics = await ListAsync(http, baseUri, "$Resources/topics", endpoint, cts.Token).ConfigureAwait(false) ?? [];

            var entities = queues.Select(q => new ServiceBusEntity(q, ServiceBusEntityKind.Queue, [])).ToList();
            foreach (var topic in topics)
            {
                var subs = await ListAsync(http, baseUri, $"{Uri.EscapeDataString(topic)}/Subscriptions", endpoint, cts.Token).ConfigureAwait(false) ?? [];
                entities.Add(new ServiceBusEntity(topic, ServiceBusEntityKind.Topic, subs));
            }
            return entities;
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return null;
        }
    }

    /// <summary>
    /// Where the namespace's management feed lives.
    /// </summary>
    /// <remarks>
    /// A real namespace is <c>https://&lt;ns&gt;.servicebus.windows.net/</c>
    /// on the implicit port, which is what an <c>amqps1://</c> endpoint
    /// with no management port produces — the shape this method was
    /// hard-coded to before.
    /// <para>
    /// The official emulator is the reason it is not hard-coded any more.
    /// It serves the same feed over plain HTTP on its own port (5300 by
    /// default), which no part of the AMQP URL implies: the wire port is
    /// 5672 and the scheme is <c>amqp1://</c>. Both fall out of keys this
    /// plugin already has — <c>_mgmtPort</c> names the management port
    /// exactly as it does for RabbitMQ on the 0.9.1 side, and TLS on the
    /// connection picks the scheme. So
    /// <c>amqp1://user:key@127.0.0.1:5672?_amqp10Discovery=servicebus&amp;_mgmtPort=5300</c>
    /// discovers against the emulator, and nothing about a live namespace
    /// changes.
    /// </para>
    /// <para>
    /// The emulator does not validate the SAS header — a bogus token gets
    /// a 200 — so pointing at it proves the feed shape and the service
    /// composition, not the signature. That part stays covered by the
    /// computed vector in <c>Amqp10DiscoveryTests</c>.
    /// </para>
    /// </remarks>
    internal static Uri ManagementBaseUri(AmqpEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        var builder = new UriBuilder(endpoint.UseTls ? Uri.UriSchemeHttps : Uri.UriSchemeHttp, endpoint.Host)
        {
            Path = "/",
        };
        // Left at -1 the builder emits the scheme's default port, i.e.
        // nothing — which is what a live namespace wants.
        if (endpoint.ManagementPort is { } port) builder.Port = port;
        return builder.Uri;
    }

    /// <summary>The entity names in one ATOM feed, or null when the request was refused.</summary>
    private static async Task<List<string>?> ListAsync(
        HttpClient http, Uri baseUri, string path, AmqpEndpoint endpoint, CancellationToken ct)
    {
        var uri = new Uri(baseUri, $"{path}?api-version={ApiVersion}");
        using var request = new HttpRequestMessage(HttpMethod.Get, uri);
        // The token's scope is the URL it is sent to, without the query.
        var token = CreateSasToken(new Uri(baseUri, path).AbsoluteUri, endpoint.UserName!, endpoint.Password!, TimeSpan.FromMinutes(5));
        request.Headers.TryAddWithoutValidation("Authorization", token);

        using var response = await http.SendAsync(request, ct).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode) return null;
        var xml = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        return ParseFeed(xml);
    }

    /// <summary>
    /// The entity names out of an ATOM feed. Each entry's title is the
    /// entity's name; a topic's subscription feed names the subscription.
    /// An empty namespace answers with a feed that has no entries, which
    /// is a list of none — not a failure.
    /// </summary>
    internal static List<string> ParseFeed(string atomXml)
    {
        var names = new List<string>();
        if (string.IsNullOrWhiteSpace(atomXml)) return names;
        XDocument doc;
        try { doc = XDocument.Parse(atomXml); }
        catch (System.Xml.XmlException) { return names; }

        foreach (var entry in doc.Descendants(Atom + "entry"))
        {
            var title = entry.Element(Atom + "title")?.Value;
            if (!string.IsNullOrWhiteSpace(title)) names.Add(title.Trim());
        }
        return names;
    }

    /// <summary>
    /// A shared-access signature for <paramref name="resourceUri"/>, the
    /// header value included: <c>SharedAccessSignature sr=…&amp;sig=…&amp;se=…&amp;skn=…</c>,
    /// where the signature is HMAC-SHA256 over the URL-encoded resource
    /// and the expiry, separated by a newline.
    /// </summary>
    internal static string CreateSasToken(string resourceUri, string keyName, string key, TimeSpan validFor)
    {
        ArgumentException.ThrowIfNullOrEmpty(resourceUri);
        ArgumentException.ThrowIfNullOrEmpty(keyName);
        ArgumentException.ThrowIfNullOrEmpty(key);

        var expiry = DateTimeOffset.UtcNow.Add(validFor).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        // Lower-cased, escapes included: what the service signs against,
        // and what HttpUtility.UrlEncode would have produced without
        // pulling System.Web in at plugin scope. Entity names address
        // case-insensitively, so the case carries no meaning here.
        var encodedResource = Uri.EscapeDataString(resourceUri).ToLowerInvariant();
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(key));
        var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(encodedResource + "\n" + expiry)));
        return "SharedAccessSignature sr=" + encodedResource
            + "&sig=" + Uri.EscapeDataString(signature)
            + "&se=" + expiry
            + "&skn=" + Uri.EscapeDataString(keyName);
    }
}
