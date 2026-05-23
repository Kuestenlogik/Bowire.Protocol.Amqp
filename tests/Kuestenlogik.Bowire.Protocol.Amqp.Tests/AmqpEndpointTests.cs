// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

public sealed class AmqpEndpointTests
{
    [Theory]
    [InlineData("amqp://broker:5672", AmqpWire.V091, "broker", 5672, false)]
    [InlineData("amqps://broker:5671", AmqpWire.V091, "broker", 5671, true)]
    [InlineData("amqp1://broker:5672", AmqpWire.V10, "broker", 5672, false)]
    [InlineData("amqps1://broker:5671", AmqpWire.V10, "broker", 5671, true)]
    public void Picks_wire_variant_and_tls_from_scheme(string url, AmqpWire wire, string host, int port, bool tls)
    {
        Assert.True(AmqpEndpoint.TryParse(url, out var ep));
        Assert.Equal(wire, ep!.Wire);
        Assert.Equal(host, ep.Host);
        Assert.Equal(port, ep.Port);
        Assert.Equal(tls, ep.UseTls);
    }

    [Theory]
    [InlineData("amqp://broker", 5672)]
    [InlineData("amqps://broker", 5671)]
    [InlineData("amqp1://broker", 5672)]
    [InlineData("amqps1://broker", 5671)]
    public void Default_port_matches_scheme_when_omitted(string url, int expectedPort)
    {
        Assert.True(AmqpEndpoint.TryParse(url, out var ep));
        Assert.Equal(expectedPort, ep!.Port);
    }

    [Fact]
    public void Parses_user_password()
    {
        Assert.True(AmqpEndpoint.TryParse("amqp://alice:s3cret@broker:5672/myvhost", out var ep));
        Assert.Equal("alice", ep!.UserName);
        Assert.Equal("s3cret", ep.Password);
        Assert.Equal("myvhost", ep.AddressOrVhost);
    }

    [Fact]
    public void Vhost_defaults_to_slash_for_0_9_1_when_path_empty()
    {
        Assert.True(AmqpEndpoint.TryParse("amqp://broker:5672", out var ep));
        Assert.Equal("/", ep!.AddressOrVhost);
    }

    [Fact]
    public void Path_left_empty_for_1_0_when_url_has_no_path()
    {
        Assert.True(AmqpEndpoint.TryParse("amqp1://broker:5672", out var ep));
        Assert.Equal(string.Empty, ep!.AddressOrVhost);
    }

    [Fact]
    public void Url_decodes_percent_escapes_in_user_pass_and_path()
    {
        Assert.True(AmqpEndpoint.TryParse("amqp://us%40er:p%2Fass@broker/my%2Fvhost", out var ep));
        Assert.Equal("us@er", ep!.UserName);
        Assert.Equal("p/ass", ep.Password);
        Assert.Equal("my/vhost", ep.AddressOrVhost);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not-a-url")]
    [InlineData("http://broker:5672")]
    [InlineData("kafka://broker:9092")]
    [InlineData("amqp://")]
    public void Rejects_invalid_or_unrelated_urls(string url)
    {
        Assert.False(AmqpEndpoint.TryParse(url, out var ep));
        Assert.Null(ep);
    }
}
