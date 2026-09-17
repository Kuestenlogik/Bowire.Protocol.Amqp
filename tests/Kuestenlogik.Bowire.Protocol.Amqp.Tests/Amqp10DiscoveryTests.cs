// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Models;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests;

/// <summary>
/// AMQP 1.0 discovery beyond the synthetic Broker service. The wire has
/// no discovery of its own; Artemis answers management requests over the
/// AMQP connection, Service Bus serves an ATOM feed signed with the key
/// already in the URL. These pin the parsing of both answers, the shape
/// they turn into, and the fallbacks — the live brokers are the
/// Docker-tagged integration suite's business.
/// </summary>
public sealed class Amqp10DiscoveryTests
{
    // ---- which broker ----

    // The flavour enum is a plugin internal, so it cannot ride in a
    // [Theory]'s signature — a public test method may not take one. The
    // expectation travels as its name instead.
    [Theory]
    [InlineData("artemis", nameof(Amqp10Flavour.Artemis))]
    [InlineData("ARTEMIS", nameof(Amqp10Flavour.Artemis))]
    [InlineData("servicebus", nameof(Amqp10Flavour.ServiceBus))]
    [InlineData("service-bus", nameof(Amqp10Flavour.ServiceBus))]
    [InlineData("azure", nameof(Amqp10Flavour.ServiceBus))]
    [InlineData("none", nameof(Amqp10Flavour.None))]
    [InlineData("broker", nameof(Amqp10Flavour.None))]
    [InlineData("auto", nameof(Amqp10Flavour.Auto))]
    [InlineData("", nameof(Amqp10Flavour.Auto))]
    [InlineData(null, nameof(Amqp10Flavour.Auto))]
    [InlineData("rabbitmq", nameof(Amqp10Flavour.Auto))]
    public void Flavour_parses_the_setting_value(string? value, string expected)
        => Assert.Equal(expected, Amqp10Flavours.Parse(value).ToString());

    [Theory]
    [InlineData("contoso.servicebus.windows.net", true)]
    [InlineData("CONTOSO.SERVICEBUS.WINDOWS.NET", true)]
    [InlineData("gov.servicebus.usgovcloudapi.net", true)]
    [InlineData("cn.servicebus.chinacloudapi.cn", true)]
    [InlineData("localhost", false)]
    [InlineData("broker.internal", false)]
    // Not the namespace's own domain — a look-alike must not be taken for one.
    [InlineData("servicebus.windows.net.attacker.example", false)]
    [InlineData("", false)]
    public void ServiceBus_is_recognised_by_its_host(string host, bool expected)
        => Assert.Equal(expected, Amqp10Flavours.IsServiceBusHost(host));

    [Fact]
    public void Auto_reads_the_host_and_an_explicit_choice_stands()
    {
        Assert.Equal(Amqp10Flavour.ServiceBus, Amqp10Flavours.Resolve(Amqp10Flavour.Auto, "ns.servicebus.windows.net"));
        Assert.Equal(Amqp10Flavour.Artemis, Amqp10Flavours.Resolve(Amqp10Flavour.Auto, "localhost"));
        // An operator who said Artemis gets Artemis, host notwithstanding.
        Assert.Equal(Amqp10Flavour.Artemis, Amqp10Flavours.Resolve(Amqp10Flavour.Artemis, "ns.servicebus.windows.net"));
        Assert.Equal(Amqp10Flavour.None, Amqp10Flavours.Resolve(Amqp10Flavour.None, "localhost"));
    }

    [Theory]
    [InlineData("amqp1://host:5672?_amqp10Discovery=artemis", nameof(Amqp10Flavour.Artemis))]
    [InlineData("amqp1://host:5672?_amqp10Discovery=none", nameof(Amqp10Flavour.None))]
    [InlineData("amqps1://host?_receiveTimeout=60&_amqp10Discovery=servicebus", nameof(Amqp10Flavour.ServiceBus))]
    public void The_url_can_override_the_flavour(string url, string expected)
    {
        Assert.True(AmqpEndpoint.TryParse(url, out var endpoint));
        Assert.Equal(expected, endpoint.Flavour?.ToString());
    }

    [Fact]
    public void A_url_without_the_override_leaves_the_setting_to_decide()
    {
        Assert.True(AmqpEndpoint.TryParse("amqp1://host:5672", out var endpoint));
        Assert.Null(endpoint.Flavour);
    }

    // ---- Artemis' management answers ----

    // Captured from Artemis 2.44's listAddresses / listQueues: every
    // attribute a string, the data behind a `data` key, a count beside it.
    private const string ListAddressesJson = """
        {"data":[
          {"id":"12","name":"$sys.mqtt.sessions","routingTypes":"[\"ANYCAST\"]","queueCount":"1","internal":"true","temporary":"false","autoCreated":"true"},
          {"id":"2","name":"DLQ","routingTypes":"[\"ANYCAST\"]","queueCount":"1","internal":"false","temporary":"false","autoCreated":"false"},
          {"id":"20","name":"harbor","routingTypes":"[\"MULTICAST\"]","queueCount":"2","internal":"false","temporary":"false","autoCreated":"false"},
          {"id":"0","name":"5a5fb158-373a-49ba-9871-d92c367be169","routingTypes":"[\"ANYCAST\"]","queueCount":"1","internal":"false","temporary":"true","autoCreated":"true"}
        ],"count":4}
        """;

    private const string ListQueuesJson = """
        {"data":[
          {"id":"13","name":"$sys.mqtt.sessions","address":"$sys.mqtt.sessions","routingType":"ANYCAST","temporary":"false","internalQueue":"true","messageCount":"0","consumerCount":"0"},
          {"id":"3","name":"DLQ","address":"DLQ","routingType":"ANYCAST","temporary":"false","internalQueue":"false","messageCount":"4","consumerCount":"0"},
          {"id":"21","name":"harbor.cranes","address":"harbor","routingType":"MULTICAST","temporary":"false","internalQueue":"false","messageCount":"7","consumerCount":"1"},
          {"id":"22","name":"harbor.gates","address":"harbor","routingType":"MULTICAST","temporary":"false","internalQueue":"false","messageCount":"0","consumerCount":"0"}
        ],"count":4}
        """;

    [Fact]
    public void Artemis_addresses_and_queues_parse_into_the_broker_shape()
    {
        var addresses = ArtemisManagement.Compose(
            ArtemisManagement.ParseAddresses(ListAddressesJson),
            ArtemisManagement.ParseQueues(ListQueuesJson));

        var harbor = addresses.Single(a => a.Name == "harbor");
        Assert.Equal(["MULTICAST"], harbor.RoutingTypes);
        Assert.False(harbor.Internal);
        Assert.Equal(["harbor.cranes", "harbor.gates"], harbor.Queues.Select(q => q.Name).Order(StringComparer.Ordinal));
        Assert.Equal(7, harbor.Queues.Single(q => q.Name == "harbor.cranes").MessageCount);
        Assert.Equal(1, harbor.Queues.Single(q => q.Name == "harbor.cranes").ConsumerCount);

        Assert.True(addresses.Single(a => a.Name == "$sys.mqtt.sessions").Internal);
        Assert.True(addresses.Single(a => a.Name.StartsWith("5a5fb158", StringComparison.Ordinal)).Temporary);
    }

    [Fact]
    public void A_queue_whose_address_the_list_did_not_carry_gets_one_of_its_own()
    {
        // listQueues can name an address listAddresses' page did not
        // reach. The queue is still reachable, so it becomes a service.
        var addresses = ArtemisManagement.Compose(
            ArtemisManagement.ParseAddresses("""{"data":[],"count":0}"""),
            ArtemisManagement.ParseQueues("""
                {"data":[{"name":"late","address":"late.address","routingType":"ANYCAST","temporary":"false","internalQueue":"false","messageCount":"0","consumerCount":"0"}],"count":1}
                """));
        var address = Assert.Single(addresses);
        Assert.Equal("late.address", address.Name);
        Assert.Equal("late", Assert.Single(address.Queues).Name);
    }

    [Fact]
    public void Artemis_services_send_to_the_address_and_receive_from_each_queue()
    {
        var addresses = ArtemisManagement.Compose(
            ArtemisManagement.ParseAddresses(ListAddressesJson),
            ArtemisManagement.ParseQueues(ListQueuesJson));

        var services = BowireAmqpProtocol.BuildArtemisServices(addresses, showInternalServices: false);

        // The broker's own plumbing and the temporary reply address that
        // discovery itself just left behind stay out.
        Assert.Equal(["DLQ", "harbor"], services.Select(s => s.Name).Order(StringComparer.Ordinal));

        var harbor = services.Single(s => s.Name == "harbor");
        Assert.Equal("Artemis address (MULTICAST, 2 queues)", harbor.Description);
        Assert.Equal(
            ["receive:harbor.cranes", "receive:harbor.gates", "send"],
            harbor.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
        Assert.True(harbor.Methods.Single(m => m.Name == "receive:harbor.cranes").ServerStreaming);
        Assert.False(harbor.Methods.Single(m => m.Name == "send").ServerStreaming);

        // A queue named after its address is the bare `receive`.
        var dlq = services.Single(s => s.Name == "DLQ");
        Assert.Equal(["receive", "send"], dlq.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Artemis_internals_are_shown_when_asked_for()
    {
        var addresses = ArtemisManagement.Compose(
            ArtemisManagement.ParseAddresses(ListAddressesJson),
            ArtemisManagement.ParseQueues(ListQueuesJson));

        var services = BowireAmqpProtocol.BuildArtemisServices(addresses, showInternalServices: true);
        Assert.Contains(services, s => s.Name == "$sys.mqtt.sessions");
        Assert.Contains(services, s => s.Name.StartsWith("5a5fb158", StringComparison.Ordinal));
    }

    [Fact]
    public void An_address_with_no_queue_can_still_be_sent_to()
    {
        var addresses = ArtemisManagement.ParseAddresses("""
            {"data":[{"name":"notify","routingTypes":"[\"MULTICAST\"]","queueCount":"0","internal":"false","temporary":"false","autoCreated":"false"}],"count":1}
            """);
        var service = Assert.Single(BowireAmqpProtocol.BuildArtemisServices(addresses, showInternalServices: false));
        Assert.Equal("Artemis address (MULTICAST, no queues)", service.Description);
        Assert.Equal(["receive", "send"], service.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void A_broker_that_answers_with_nothing_keeps_the_generic_service()
    {
        var services = BowireAmqpProtocol.BuildArtemisServices([], showInternalServices: false);
        var broker = Assert.Single(services);
        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, broker.Name);
    }

    // ---- Service Bus' management answers ----

    private const string QueueFeedXml = """
        <?xml version="1.0" encoding="utf-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <title type="text">Queues</title>
          <entry>
            <title type="text">orders</title>
            <content type="application/xml"><QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><MaxDeliveryCount>10</MaxDeliveryCount></QueueDescription></content>
          </entry>
          <entry>
            <title type="text">orders-dead</title>
          </entry>
        </feed>
        """;

    [Fact]
    public void The_atom_feed_yields_the_entity_names()
        => Assert.Equal(["orders", "orders-dead"], ServiceBusManagement.ParseFeed(QueueFeedXml));

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not xml at all <")]
    public void A_feed_that_is_not_one_yields_nothing_rather_than_throwing(string xml)
        => Assert.Empty(ServiceBusManagement.ParseFeed(xml));

    [Fact]
    public void An_empty_namespace_is_a_list_of_none()
        => Assert.Empty(ServiceBusManagement.ParseFeed("""<feed xmlns="http://www.w3.org/2005/Atom"><title>Queues</title></feed>"""));

    [Fact]
    public void Service_bus_entities_become_queues_with_receive_and_topics_with_a_receive_per_subscription()
    {
        var services = BowireAmqpProtocol.BuildServiceBusServices(
        [
            new ServiceBusEntity("orders", ServiceBusEntityKind.Queue, []),
            new ServiceBusEntity("events", ServiceBusEntityKind.Topic, ["audit", "billing"]),
        ], showInternalServices: false);

        var orders = services.Single(s => s.Name == "orders");
        Assert.Equal("Service Bus queue", orders.Description);
        Assert.Equal(["receive", "send"], orders.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));

        var events = services.Single(s => s.Name == "events");
        Assert.Equal("Service Bus topic (2 subscriptions)", events.Description);
        Assert.Equal(
            ["receive:audit", "receive:billing", "send"],
            events.Methods.Select(m => m.Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void A_shared_access_signature_signs_the_resource_and_the_expiry()
    {
        // Vector computed independently: HMAC-SHA256 over the lower-cased
        // URL-encoded resource, a newline, and the expiry.
        var token = ServiceBusManagement.CreateSasToken(
            "https://demo.servicebus.windows.net/$Resources/queues",
            "RootManageSharedAccessKey",
            "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcd=",
            TimeSpan.FromMinutes(5));

        Assert.StartsWith("SharedAccessSignature ", token, StringComparison.Ordinal);
        Assert.Contains("sr=https%3a%2f%2fdemo.servicebus.windows.net%2f%24resources%2fqueues", token, StringComparison.Ordinal);
        Assert.Contains("&skn=RootManageSharedAccessKey", token, StringComparison.Ordinal);

        var expiry = long.Parse(token[(token.IndexOf("&se=", StringComparison.Ordinal) + 4)..token.IndexOf("&skn=", StringComparison.Ordinal)], System.Globalization.CultureInfo.InvariantCulture);
        Assert.InRange(expiry, DateTimeOffset.UtcNow.AddMinutes(4).ToUnixTimeSeconds(), DateTimeOffset.UtcNow.AddMinutes(6).ToUnixTimeSeconds());

        // The signature is reproducible for a fixed expiry: sign the same
        // string this token's own expiry produced and expect its sig.
        const string encoded = "https%3a%2f%2fdemo.servicebus.windows.net%2f%24resources%2fqueues";
        using var hmac = new System.Security.Cryptography.HMACSHA256(
            System.Text.Encoding.UTF8.GetBytes("AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcd="));
        var expected = Uri.EscapeDataString(Convert.ToBase64String(
            hmac.ComputeHash(System.Text.Encoding.UTF8.GetBytes(encoded + "\n" + expiry.ToString(System.Globalization.CultureInfo.InvariantCulture)))));
        Assert.Contains("&sig=" + expected, token, StringComparison.Ordinal);
    }

    [Fact]
    public void A_fixed_expiry_signs_to_a_known_value()
    {
        // The same inputs, the same signature — an independent
        // computation of the vector this implementation must produce.
        const string encoded = "https%3a%2f%2fdemo.servicebus.windows.net%2f%24resources%2fqueues";
        using var hmac = new System.Security.Cryptography.HMACSHA256(
            System.Text.Encoding.UTF8.GetBytes("AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcd="));
        var sig = Convert.ToBase64String(hmac.ComputeHash(System.Text.Encoding.UTF8.GetBytes(encoded + "\n1900000000")));
        Assert.Equal("1cZBVM6gvySko9TifbPi9ZS9FYsRVp6jPu5yN1q1eQs=", sig);
    }

    // ---- what a method sends to ----

    [Fact]
    public void A_method_suffix_names_the_queue_on_artemis_and_the_subscription_on_service_bus()
    {
        Assert.True(AmqpEndpoint.TryParse("amqp1://artemis:artemis@localhost:5672", out var artemis));
        Assert.Equal("harbor::harbor.cranes",
            new BowireAmqpProtocol().ResolveV10Address(artemis, "harbor", "receive:harbor.cranes", null));
        Assert.Equal("harbor", new BowireAmqpProtocol().ResolveV10Address(artemis, "harbor", "send", null));
        Assert.Equal("orders", new BowireAmqpProtocol().ResolveV10Address(artemis, "orders", "receive", null));

        Assert.True(AmqpEndpoint.TryParse("amqps1://key:secret@ns.servicebus.windows.net", out var sb));
        Assert.Equal("events/Subscriptions/audit",
            new BowireAmqpProtocol().ResolveV10Address(sb, "events", "receive:audit", null));
        Assert.Equal("events", new BowireAmqpProtocol().ResolveV10Address(sb, "events", "send", null));
    }

    [Fact]
    public void The_address_metadata_still_wins_and_the_generic_service_keeps_the_url_path()
    {
        Assert.True(AmqpEndpoint.TryParse("amqp1://localhost:5672/inbox", out var endpoint));
        Assert.Equal("elsewhere", new BowireAmqpProtocol().ResolveV10Address(
            endpoint, "harbor", "receive:harbor.cranes",
            new Dictionary<string, string> { ["address"] = "elsewhere" }));
        Assert.Equal("inbox", new BowireAmqpProtocol().ResolveV10Address(
            endpoint, BowireAmqpProtocol.BrokerServiceName, BowireAmqpProtocol.SendMethodName, null));
    }

    // ---- the fallbacks ----

    [Fact]
    public async Task An_endpoint_nothing_answers_on_falls_back_to_the_generic_broker()
    {
        // Port 1 has no broker. Discovery must come back with the
        // synthetic service rather than throwing at the workbench.
        var protocol = new BowireAmqpProtocol();
        var services = await protocol.DiscoverAsync(
            "amqp1://127.0.0.1:1?_discoveryTimeout=1",
            showInternalServices: false,
            ct: TestContext.Current.CancellationToken);

        var broker = Assert.Single(services);
        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, broker.Name);
        Assert.Contains("send", broker.Methods.Select(m => m.Name));
    }

    [Fact]
    public async Task Discovery_turned_off_asks_no_broker_anything()
    {
        // `none` must not open a connection at all — an unreachable
        // endpoint answers instantly rather than after the timeout.
        var protocol = new BowireAmqpProtocol();
        var started = System.Diagnostics.Stopwatch.StartNew();
        var services = await protocol.DiscoverAsync(
            "amqp1://127.0.0.1:1?_amqp10Discovery=none&_discoveryTimeout=30",
            showInternalServices: false,
            ct: TestContext.Current.CancellationToken);
        started.Stop();

        Assert.Equal(BowireAmqpProtocol.BrokerServiceName, Assert.Single(services).Name);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(5), $"took {started.Elapsed}");
    }

    [Fact]
    public void The_plugin_declares_the_discovery_setting_with_its_four_choices()
    {
        var setting = new BowireAmqpProtocol().Settings
            .Single(s => s.Key == BowireAmqpProtocol.Amqp10DiscoverySettingKey);
        Assert.Equal("select", setting.Type);
        Assert.Equal("auto", setting.DefaultValue);
        Assert.Equal(["artemis", "auto", "none", "servicebus"],
            (setting.Options ?? []).Select(o => o.Value).Order(StringComparer.Ordinal));
    }

    // ---- Where the management feed lives -------------------------------

    [Fact]
    public void A_live_namespace_is_https_on_the_implicit_port()
    {
        Assert.True(AmqpEndpoint.TryParse(
            "amqps1://key:secret@contoso.servicebus.windows.net", out var endpoint));

        Assert.Equal("https://contoso.servicebus.windows.net/", ServiceBusManagement.ManagementBaseUri(endpoint).AbsoluteUri);
    }

    [Fact]
    public void The_wire_port_does_not_leak_into_the_management_url()
    {
        // 5671 is where AMQP is, not where the feed is.
        Assert.True(AmqpEndpoint.TryParse(
            "amqps1://key:secret@contoso.servicebus.windows.net:5671", out var endpoint));

        Assert.Equal("https://contoso.servicebus.windows.net/", ServiceBusManagement.ManagementBaseUri(endpoint).AbsoluteUri);
    }

    [Fact]
    public void An_emulator_is_plain_http_on_the_port_it_was_given()
    {
        // The official emulator serves the same feed over HTTP on 5300,
        // which no part of the AMQP URL implies. Both halves come from
        // keys the plugin already has.
        Assert.True(AmqpEndpoint.TryParse(
            "amqp1://key:secret@127.0.0.1:5672?_amqp10Discovery=servicebus&_mgmtPort=5300", out var endpoint));

        Assert.Equal("http://127.0.0.1:5300/", ServiceBusManagement.ManagementBaseUri(endpoint).AbsoluteUri);
    }

    [Fact]
    public void A_management_port_on_a_tls_endpoint_stays_https()
    {
        Assert.True(AmqpEndpoint.TryParse(
            "amqps1://key:secret@gateway.internal:5671?_mgmtPort=8443", out var endpoint));

        Assert.Equal("https://gateway.internal:8443/", ServiceBusManagement.ManagementBaseUri(endpoint).AbsoluteUri);
    }
}
