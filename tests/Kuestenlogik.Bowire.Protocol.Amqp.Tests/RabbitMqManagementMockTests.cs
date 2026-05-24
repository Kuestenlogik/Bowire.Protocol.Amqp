// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Net;
using System.Net.Http.Headers;
using System.Text;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

/// <summary>
/// In-process wire-shape coverage for the AMQP 0.9.1 discovery path. Uses
/// the test-seam ctor on <see cref="RabbitMqManagement"/> with a stubbed
/// HttpMessageHandler so the assertions stay deterministic and broker-
/// free — broker round-trips live behind a Docker trait in the
/// integration suite. Together with <see cref="AmqpV10DiscoverShapeTests"/>
/// this gives the plugin actual coverage of its discovery shape (vs.
/// the older tests, which only covered argument validation).
/// </summary>
public sealed class RabbitMqManagementMockTests
{
    [Fact]
    public async Task GetExchangesAsync_parses_management_api_response()
    {
        // Body shape lifted from a real `GET /api/exchanges/%2F` response
        // on a fresh RabbitMQ 3.13 image. amq.* exchanges are always
        // returned by the API; filtering happens at the plugin layer.
        const string body = """
            [
              {"name":"","type":"direct","durable":true},
              {"name":"amq.direct","type":"direct","durable":true},
              {"name":"harbor","type":"topic","durable":true}
            ]
            """;
        using var handler = new StubHandler(body, "/api/exchanges/%2F");
        using var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://broker:15672/"),
        };
        using var mgmt = new RabbitMqManagement(http, vhost: "/");

        var exchanges = await mgmt.GetExchangesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, exchanges.Count);
        Assert.Contains(exchanges, e => e.Name == "harbor" && e.Type == "topic");
        Assert.Contains(exchanges, e => e.Name == "amq.direct");
        Assert.Contains(exchanges, e => string.IsNullOrEmpty(e.Name) && e.Type == "direct");
    }

    [Fact]
    public async Task GetBindingsAsync_parses_management_api_response()
    {
        const string body = """
            [
              {"source":"harbor","destination":"harbor.cranes","destination_type":"queue","routing_key":"cranes.*"},
              {"source":"harbor","destination":"harbor.berths","destination_type":"queue","routing_key":"berths.*"}
            ]
            """;
        using var handler = new StubHandler(body, "/api/bindings/%2F");
        using var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://broker:15672/"),
        };
        using var mgmt = new RabbitMqManagement(http, vhost: "/");

        var bindings = await mgmt.GetBindingsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, bindings.Count);
        Assert.All(bindings, b => Assert.Equal("harbor", b.Source));
        Assert.Contains(bindings, b => b.RoutingKey == "cranes.*" && b.Destination == "harbor.cranes");
    }

    [Fact]
    public async Task GetQueuesAsync_parses_management_api_response()
    {
        const string body = """
            [
              {"name":"harbor.cranes","durable":true},
              {"name":"harbor.berths","durable":true},
              {"name":"amq.gen-anonsubscriber","durable":false}
            ]
            """;
        using var handler = new StubHandler(body, "/api/queues/%2F");
        using var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://broker:15672/"),
        };
        using var mgmt = new RabbitMqManagement(http, vhost: "/");

        var queues = await mgmt.GetQueuesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, queues.Count);
        Assert.Contains(queues, q => q.Name == "harbor.cranes" && q.Durable);
        Assert.Contains(queues, q => q.Name == "amq.gen-anonsubscriber" && !q.Durable);
    }

    [Fact]
    public async Task GetQueuesAsync_url_encodes_non_root_vhost()
    {
        // A vhost like "my/vhost" must round-trip through %2F encoding;
        // the management API rejects bare slashes in path segments.
        using var handler = new StubHandler("[]", expectedPath: "/api/queues/my%2Fvhost");
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://broker:15672/") };
        using var mgmt = new RabbitMqManagement(http, vhost: "my/vhost");

        var queues = await mgmt.GetQueuesAsync(TestContext.Current.CancellationToken);

        Assert.Empty(queues);
        Assert.True(handler.PathMatched, $"Expected request to /api/queues/my%2Fvhost but got {handler.LastPath}");
    }

    /// <summary>
    /// Minimal HttpMessageHandler stub — returns a canned 200 JSON body
    /// for a single expected path, asserts the path matches what the
    /// management client built.
    /// </summary>
    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly string _body;
        private readonly string _expectedPath;
        public string? LastPath { get; private set; }
        public bool PathMatched => string.Equals(LastPath, _expectedPath, StringComparison.Ordinal);

        public StubHandler(string body, string expectedPath)
        {
            _body = body;
            _expectedPath = expectedPath;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastPath = request.RequestUri?.AbsolutePath;
            var resp = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(_body, Encoding.UTF8, "application/json"),
            };
            return Task.FromResult(resp);
        }
    }
}
