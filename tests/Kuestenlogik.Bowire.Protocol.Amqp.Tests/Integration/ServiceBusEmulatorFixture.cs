// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace Kuestenlogik.Bowire.Protocol.Amqp.Tests.Integration;

/// <summary>
/// Microsoft's official Azure Service Bus emulator, with the topology the
/// discovery tests expect declared in its config file.
/// </summary>
/// <remarks>
/// <para>
/// A live Service Bus namespace is a paid Azure resource and cannot be in
/// CI, which left the Service Bus half of AMQP 1.0 discovery proved only
/// against captured feeds. The emulator closes most of that gap for free:
/// it serves the same ATOM management feed the cloud service does, from
/// the same <c>$Resources/queues</c>, <c>$Resources/topics</c> and
/// <c>&lt;topic&gt;/Subscriptions</c> paths.
/// </para>
/// <para>
/// What it does not prove: the emulator ignores the <c>Authorization</c>
/// header — a bogus SAS gets a 200 — so the signature stays covered by
/// the independently computed vector in <c>Amqp10DiscoveryTests</c>. And
/// it serves management over plain HTTP on 5300 rather than HTTPS on the
/// implicit port, which is exactly why
/// <see cref="ServiceBusManagement.ManagementBaseUri"/> derives the base
/// URI instead of hard-coding it.
/// </para>
/// <para>
/// Two containers on a private network: the emulator keeps its state in
/// SQL Server and will not start without one. Tests using this fixture
/// carry <c>[Trait("Category", "Docker")]</c>.
/// </para>
/// </remarks>
public sealed class ServiceBusEmulatorFixture : IAsyncLifetime
{
    /// <summary>The emulator's fixed credentials — the same pair its own docs print.</summary>
    internal const string KeyName = "RootManageSharedAccessKey";
    internal const string Key = "SAS_KEY_VALUE";

    private const string SqlPassword = "Str0ng!Passw0rd";
    private const string SqlAlias = "sbsql";
    private const int AmqpPort = 5672;
    private const int ManagementPort = 5300;

    /// <summary>
    /// The entities the emulator is told to create. Two queues and a
    /// topic with two subscriptions: enough to tell a queue's methods
    /// from a topic's, and to make a topic's per-subscription receive
    /// distinguishable from a single one.
    /// </summary>
    private const string ConfigJson = """
    {
      "UserConfig": {
        "Namespaces": [
          {
            "Name": "sbemulatorns",
            "Queues": [
              {
                "Name": "orders",
                "Properties": {
                  "DeadLetteringOnMessageExpiration": false,
                  "DefaultMessageTimeToLive": "PT1H",
                  "DuplicateDetectionHistoryTimeWindow": "PT20S",
                  "ForwardDeadLetteredMessagesTo": "",
                  "ForwardTo": "",
                  "LockDuration": "PT1M",
                  "MaxDeliveryCount": 3,
                  "RequiresDuplicateDetection": false,
                  "RequiresSession": false
                }
              },
              {
                "Name": "harbor.gates",
                "Properties": {
                  "DeadLetteringOnMessageExpiration": false,
                  "DefaultMessageTimeToLive": "PT1H",
                  "DuplicateDetectionHistoryTimeWindow": "PT20S",
                  "ForwardDeadLetteredMessagesTo": "",
                  "ForwardTo": "",
                  "LockDuration": "PT1M",
                  "MaxDeliveryCount": 3,
                  "RequiresDuplicateDetection": false,
                  "RequiresSession": false
                }
              }
            ],
            "Topics": [
              {
                "Name": "situation",
                "Properties": {
                  "DefaultMessageTimeToLive": "PT1H",
                  "DuplicateDetectionHistoryTimeWindow": "PT20S",
                  "RequiresDuplicateDetection": false
                },
                "Subscriptions": [
                  {
                    "Name": "ops",
                    "Properties": {
                      "DeadLetteringOnMessageExpiration": false,
                      "DefaultMessageTimeToLive": "PT1H",
                      "LockDuration": "PT1M",
                      "MaxDeliveryCount": 3,
                      "ForwardDeadLetteredMessagesTo": "",
                      "ForwardTo": "",
                      "RequiresSession": false
                    }
                  },
                  {
                    "Name": "archive",
                    "Properties": {
                      "DeadLetteringOnMessageExpiration": false,
                      "DefaultMessageTimeToLive": "PT1H",
                      "LockDuration": "PT1M",
                      "MaxDeliveryCount": 3,
                      "ForwardDeadLetteredMessagesTo": "",
                      "ForwardTo": "",
                      "RequiresSession": false
                    }
                  }
                ]
              }
            ]
          }
        ],
        "Logging": { "Type": "File" }
      }
    }
    """;

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _sql;
    private readonly IContainer _emulator;

    public ServiceBusEmulatorFixture()
    {
        _sql = new ContainerBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithNetwork(_network)
            .WithNetworkAliases(SqlAlias)
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithEnvironment("MSSQL_SA_PASSWORD", SqlPassword)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilMessageIsLogged("SQL Server is now ready for client connections"))
            .Build();

        _emulator = new ContainerBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithNetwork(_network)
            // Accepting the EULA for the emulator and for SQL Server on
            // Linux is what the image requires to start at all; both are
            // Microsoft's published development images.
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithEnvironment("MSSQL_SA_PASSWORD", SqlPassword)
            .WithEnvironment("SQL_SERVER", SqlAlias)
            .WithResourceMapping(
                Encoding.UTF8.GetBytes(ConfigJson),
                "/ServiceBus_Emulator/ConfigFiles/Config.json")
            .WithPortBinding(AmqpPort, assignRandomHostPort: true)
            .WithPortBinding(ManagementPort, assignRandomHostPort: true)
            // The emulator's own "up" line, printed after it has created
            // the entities from the config — the earlier lifetime lines
            // land before the feed would answer.
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilMessageIsLogged("Emulator Service is Successfully Up"))
            .Build();
    }

    /// <summary>
    /// The <c>amqp1://…</c> URL the plugin dials, carrying the two
    /// per-connection keys an emulator needs: the flavour, because
    /// <c>127.0.0.1</c> is not a <c>*.servicebus.*</c> host that auto
    /// would recognise, and the management port, because 5300 is not
    /// implied by the wire port.
    /// </summary>
    public string ServiceBusUrl { get; private set; } = string.Empty;

    /// <summary>The management feed's base URL, for asserting on it directly.</summary>
    public string ManagementBaseUrl { get; private set; } = string.Empty;

    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync();
        await _sql.StartAsync();
        await _emulator.StartAsync();

        var amqp = _emulator.GetMappedPublicPort(AmqpPort);
        var management = _emulator.GetMappedPublicPort(ManagementPort);
        ServiceBusUrl =
            $"amqp1://{KeyName}:{Key}@127.0.0.1:{amqp}?_amqp10Discovery=servicebus&_mgmtPort={management}";
        ManagementBaseUrl = $"http://127.0.0.1:{management}/";
    }

    public async ValueTask DisposeAsync()
    {
        await _emulator.DisposeAsync();
        await _sql.DisposeAsync();
        await _network.DisposeAsync();
    }
}
