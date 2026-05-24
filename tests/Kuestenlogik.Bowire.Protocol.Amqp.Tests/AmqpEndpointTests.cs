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
    internal void Picks_wire_variant_and_tls_from_scheme(string url, AmqpWire wire, string host, int port, bool tls)
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

    // -------- Settings overrides via URL query --------

    [Fact]
    public void Bare_url_leaves_setting_overrides_null()
    {
        // No query → all three override fields remain null, plugin defaults apply.
        Assert.True(AmqpEndpoint.TryParse("amqp://broker:5672/", out var ep));
        Assert.Null(ep!.ManagementPort);
        Assert.Null(ep.DiscoveryTimeoutSeconds);
        Assert.Null(ep.ReceiveTimeoutSeconds);
    }

    [Fact]
    public void Picks_up_all_three_setting_overrides_from_query()
    {
        Assert.True(AmqpEndpoint.TryParse(
            "amqp://broker:5672/?_mgmtPort=15673&_discoveryTimeout=10&_receiveTimeout=60",
            out var ep));
        Assert.Equal(15673, ep!.ManagementPort);
        Assert.Equal(10, ep.DiscoveryTimeoutSeconds);
        Assert.Equal(60, ep.ReceiveTimeoutSeconds);
    }

    [Fact]
    public void Setting_overrides_work_on_1_0_scheme_too()
    {
        // _receiveTimeout is the only setting that affects 1.0 streaming;
        // the others stay parsable even though 1.0 ignores them — the
        // parser doesn't know which wire will actually consume them.
        Assert.True(AmqpEndpoint.TryParse(
            "amqp1://broker/?_receiveTimeout=120",
            out var ep));
        Assert.Equal(AmqpWire.V10, ep!.Wire);
        Assert.Equal(120, ep.ReceiveTimeoutSeconds);
    }

    [Theory]
    [InlineData("?_mgmtPort=0")]
    [InlineData("?_mgmtPort=65536")]
    [InlineData("?_mgmtPort=-1")]
    [InlineData("?_mgmtPort=not-a-number")]
    [InlineData("?_discoveryTimeout=0")]
    [InlineData("?_discoveryTimeout=-5")]
    [InlineData("?_receiveTimeout=0")]
    public void Invalid_override_values_fall_back_to_null(string query)
    {
        Assert.True(AmqpEndpoint.TryParse($"amqp://broker:5672/{query}", out var ep));
        // Whichever field got the bad value stays null — caller falls
        // back to the plugin's DefaultXxx constant.
        Assert.True(
            ep!.ManagementPort is null &&
            ep.DiscoveryTimeoutSeconds is null &&
            ep.ReceiveTimeoutSeconds is null);
    }

    [Fact]
    public void Unknown_query_keys_are_ignored_forward_compat()
    {
        // A newer workbench shipping additional Bowire-private settings
        // keys mustn't blow up an older plugin's parser.
        Assert.True(AmqpEndpoint.TryParse(
            "amqp://broker:5672/?_mgmtPort=15673&_futureFlag=yes&heartbeat=30",
            out var ep));
        Assert.Equal(15673, ep!.ManagementPort);
    }
}
