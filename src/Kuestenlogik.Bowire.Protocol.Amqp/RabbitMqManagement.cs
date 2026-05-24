// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Bowire.Protocol.Amqp;

/// <summary>
/// Thin client for the RabbitMQ Management HTTP API used by AMQP 0.9.1
/// discovery. The Management plugin is bundled with the RabbitMQ Docker
/// image and almost every production RabbitMQ install, so this is the
/// realistic discovery path for the 0.9.1 wire.
/// </summary>
/// <remarks>
/// Endpoints touched:
/// <list type="bullet">
///   <item><c>GET /api/exchanges/{vhost}</c> → one row per exchange.</item>
///   <item><c>GET /api/bindings/{vhost}</c> → one row per binding (exchange↔queue or exchange↔exchange).</item>
/// </list>
/// Defaults: port <c>15672</c>, basic auth <c>guest:guest</c> (RabbitMQ's
/// out-of-box dev credentials — overridden via the endpoint's user/pass).
/// HTTPS is opt-in via the <c>useTls</c> ctor flag; the management API
/// listens on plain HTTP by default on the same dev installs.
/// </remarks>
internal sealed class RabbitMqManagement : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly HttpClient _http;
    private readonly string _vhost;
    private readonly bool _ownsHttp;

    /// <summary>
    /// Build a Management client against the AMQP endpoint. Maps the
    /// endpoint's broker host + management port + credentials onto an
    /// <see cref="HttpClient"/>. The <paramref name="managementPort"/>
    /// overrides the AMQP port (5672/5671) because the management API
    /// listens on a separate port (15672 by default).
    /// </summary>
    public RabbitMqManagement(AmqpEndpoint endpoint, int managementPort, TimeSpan timeout)
    {
        var scheme = endpoint.UseTls ? "https" : "http";
        _http = new HttpClient
        {
            BaseAddress = new Uri($"{scheme}://{endpoint.Host}:{managementPort}/"),
            Timeout = timeout,
        };
        var user = endpoint.UserName ?? "guest";
        var pass = endpoint.Password ?? "guest";
        var creds = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{pass}"));
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", creds);
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        _vhost = string.IsNullOrEmpty(endpoint.AddressOrVhost) ? "/" : endpoint.AddressOrVhost;
        _ownsHttp = true;
    }

    /// <summary>Test-seam ctor — accepts a pre-built <see cref="HttpClient"/>
    /// and an explicit vhost. Used by tests to point at an in-memory mock
    /// or a stubbed HTTP transport without spinning up a real broker.</summary>
    internal RabbitMqManagement(HttpClient http, string vhost)
    {
        _http = http;
        _vhost = string.IsNullOrEmpty(vhost) ? "/" : vhost;
        _ownsHttp = false;
    }

    /// <summary>
    /// List all exchanges declared on the configured vhost. Includes the
    /// nameless default exchange (returned as <c>""</c> by the API);
    /// callers decide whether to surface it (under "default" / "(AMQP
    /// default)") or filter it out.
    /// </summary>
    public async Task<IReadOnlyList<ExchangeInfo>> GetExchangesAsync(CancellationToken ct)
    {
        var path = $"api/exchanges/{Uri.EscapeDataString(_vhost)}";
        var list = await _http.GetFromJsonAsync<List<ExchangeInfo>>(path, JsonOptions, ct).ConfigureAwait(false);
        return list ?? new List<ExchangeInfo>();
    }

    /// <summary>
    /// List every binding on the configured vhost. Each row carries the
    /// source exchange + destination (exchange or queue) + routing-key —
    /// the routing-key is what surfaces as the Bowire "method" on a
    /// discovered exchange-service.
    /// </summary>
    public async Task<IReadOnlyList<BindingInfo>> GetBindingsAsync(CancellationToken ct)
    {
        var path = $"api/bindings/{Uri.EscapeDataString(_vhost)}";
        var list = await _http.GetFromJsonAsync<List<BindingInfo>>(path, JsonOptions, ct).ConfigureAwait(false);
        return list ?? new List<BindingInfo>();
    }

    /// <summary>
    /// List every queue declared on the configured vhost. Queues are the
    /// consumer-facing surface — Bowire's discovery turns each one into a
    /// service entry with a streaming <c>receive</c> method so the
    /// workbench can subscribe to it without the operator hand-rolling a
    /// queue name in metadata.
    /// </summary>
    public async Task<IReadOnlyList<QueueInfo>> GetQueuesAsync(CancellationToken ct)
    {
        var path = $"api/queues/{Uri.EscapeDataString(_vhost)}";
        var list = await _http.GetFromJsonAsync<List<QueueInfo>>(path, JsonOptions, ct).ConfigureAwait(false);
        return list ?? new List<QueueInfo>();
    }

    public void Dispose()
    {
        if (_ownsHttp) _http.Dispose();
    }

    /// <summary>One row from <c>/api/exchanges</c>. Bare-bones — we only
    /// need the name + type for Bowire's service-tree rendering.</summary>
    public sealed record ExchangeInfo(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("type")] string Type,
        [property: JsonPropertyName("durable")] bool Durable);

    /// <summary>One row from <c>/api/bindings</c>. <c>source</c> is the
    /// exchange the binding leaves; <c>destination_type</c> is "queue"
    /// or "exchange"; <c>routing_key</c> is the key clients publish with.</summary>
    public sealed record BindingInfo(
        [property: JsonPropertyName("source")] string Source,
        [property: JsonPropertyName("destination")] string Destination,
        [property: JsonPropertyName("destination_type")] string DestinationType,
        [property: JsonPropertyName("routing_key")] string RoutingKey);

    /// <summary>One row from <c>/api/queues</c>. Bare-bones; we only need
    /// the name + durability for Bowire's service-tree rendering. The
    /// management API exposes plenty of runtime state (message counts,
    /// consumer counts, memory) that the workbench doesn't need at the
    /// discovery stage.</summary>
    public sealed record QueueInfo(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("durable")] bool Durable);
}
