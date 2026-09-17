// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

// Combined AMQP sample for Bowire. AMQP has no pure-.NET embeddable broker,
// so this sample points at an *external* RabbitMQ (docker-compose.yml
// alongside) while telling both stories from one project:
//
//   * Embedded — the workbench is mounted at /bowire and the bundled
//     amqp-catalogue.json seeds the Sources rail with both brokers; two
//     resilient background publishers keep a live surface on each wire:
//     0.9.1 declares a `harbor` topic exchange with bound queues and
//     emits crane telemetry once a second, 1.0 does the same against an
//     Artemis `harbor` address whose two queues the broker's own
//     management surface then reports to discovery.
//   * Separate — point an external workbench or
//     `bowire --url amqp://bowire:bowire@localhost:5672` (0.9.1) /
//     `bowire --url amqp1://bowire:bowire@localhost:5673` (1.0) at the
//     same brokers.
//
// The publisher is resilient: if the broker isn't up yet the host +
// workbench still start and it keeps retrying, re-declaring topology on
// reconnect.
//
// Run:
//   docker compose up                                             # broker
//   dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Amqp.Sample
//   → open http://localhost:5195/bowire

using System.Text.Json;
// AMQPNetLite — the 1.0 publisher. Aliased rather than imported: both
// libraries call their entry point ConnectionFactory, and this file
// drives one of each.
using Amqp10 = Amqp;
using Kuestenlogik.Bowire;            // AddBowire, MapBowire
using Kuestenlogik.Bowire.Sources;    // AddBowireCatalogue
using RabbitMQ.Client;

// Force the AMQP plugin assembly to load before AddBowire's reflection
// scan runs — the Kuestenlogik.Bowire 2.2.x contract scans loaded
// assemblies, so without an explicit type reference the plugin DLL
// wouldn't be loaded in time for discovery.
_ = typeof(global::Kuestenlogik.Bowire.Protocol.Amqp.BowireAmqpProtocol);

const string BrokerUrl = "amqp://bowire:bowire@localhost:5672";
const string ExchangeName = "harbor";
// The 1.0 broker. AMQPNetLite takes the standard scheme; `amqp1://` is
// Bowire's own prefix for picking the wire and never reaches a library.
const string Broker10Url = "amqp://bowire:bowire@localhost:5673";
const string Address10 = "harbor";

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://localhost:5195");

builder.Services.AddBowire();
builder.Services.AddBowireCatalogue(builder.Configuration);

var app = builder.Build();

// ---- Resilient publisher: crane telemetry once a second on `harbor` ----
_ = Task.Run(async () =>
{
    var ct = app.Lifetime.ApplicationStopping;
    var factory = new ConnectionFactory
    {
        Uri = new Uri(BrokerUrl),          // parses user/pass/host/port/vhost
        AutomaticRecoveryEnabled = true,
    };
    var seq = 0;
    while (!ct.IsCancellationRequested)
    {
        try
        {
            await using var conn = await factory.CreateConnectionAsync(ct);
            await using var channel = await conn.CreateChannelAsync(cancellationToken: ct);

            // The exact surface Bowire discovers via the management API:
            // a topic exchange + two explicitly-keyed bound queues.
            await channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic,
                durable: true, autoDelete: false, cancellationToken: ct);
            await channel.QueueDeclareAsync("harbor.cranes",
                durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
            await channel.QueueDeclareAsync("harbor.gates",
                durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
            await channel.QueueBindAsync("harbor.cranes", ExchangeName, "crane.telemetry", cancellationToken: ct);
            await channel.QueueBindAsync("harbor.gates", ExchangeName, "gate.telemetry", cancellationToken: ct);

            app.Logger.LogInformation(
                "AMQP publisher connected to {Url}; emitting crane telemetry on '{Exchange}'.",
                BrokerUrl, ExchangeName);

            while (!ct.IsCancellationRequested)
            {
                seq++;
                var body = JsonSerializer.SerializeToUtf8Bytes(new
                {
                    craneId = $"crane-{(seq % 5) + 1:00}",
                    loadTonnes = Math.Round((seq % 41) * 0.9, 1),
                    boomAngleDeg = Math.Round((seq * 7 % 91) * 1.0, 1),
                    seq,
                    ts = DateTimeOffset.UtcNow,
                });
                var props = new BasicProperties
                {
                    ContentType = "application/json",
                    MessageId = Guid.NewGuid().ToString("N"),
                };
                await channel.BasicPublishAsync(
                    exchange: ExchangeName, routingKey: "crane.telemetry",
                    mandatory: false, basicProperties: props, body: body, cancellationToken: ct);
                await Task.Delay(1000, ct);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Broker not up yet (no `docker compose up`) or connection dropped
            // — keep host + workbench alive; topology is re-declared on
            // reconnect. The `when` filter keeps CA1031 satisfied.
            app.Logger.LogDebug(ex, "AMQP publish failed (broker down?) — retrying in 2s");
            try { await Task.Delay(2000, ct); } catch (OperationCanceledException) { break; }
        }
    }
});

// ---- Resilient 1.0 publisher: the same telemetry on Artemis ----
// Two queues on one multicast address, which is the case AMQP 1.0
// discovery exists for: the address is where a message is sent, each
// queue is a separate `receive:<queue>` the workbench can subscribe to.
// Artemis creates both on first send (auto-create is on by default),
// so the demo needs no broker.xml.
_ = Task.Run(async () =>
{
    var ct = app.Lifetime.ApplicationStopping;
    var seq = 0;
    while (!ct.IsCancellationRequested)
    {
        Amqp10.Connection? connection = null;
        try
        {
            connection = await new Amqp10.ConnectionFactory().CreateAsync(new Amqp10.Address(Broker10Url));
            var session = new Amqp10.Session(connection);
            // A sender per queue: Artemis binds a multicast queue when
            // something asks for it, and a sender to `address::queue`
            // is such an ask. Afterwards the two show up under the
            // `harbor` address in discovery.
            var cranes = new Amqp10.SenderLink(session, "sample-cranes", $"{Address10}::{Address10}.cranes");
            var gates = new Amqp10.SenderLink(session, "sample-gates", $"{Address10}::{Address10}.gates");
            var fanout = new Amqp10.SenderLink(session, "sample-fanout", Address10);
            app.Logger.LogInformation(
                "AMQP 1.0 publisher connected to {Url}; emitting crane telemetry on '{Address}'.",
                Broker10Url, Address10);

            while (!ct.IsCancellationRequested)
            {
                seq++;
                using var message = new Amqp10.Message(JsonSerializer.SerializeToUtf8Bytes(new
                {
                    craneId = $"crane-{(seq % 5) + 1:00}",
                    loadTonnes = Math.Round((seq % 41) * 0.9, 1),
                    wire = "amqp-1-0",
                    seq,
                    ts = DateTimeOffset.UtcNow,
                }))
                {
                    Properties = new Amqp10.Framing.Properties
                    {
                        ContentType = "application/json",
                        MessageId = Guid.NewGuid().ToString("N"),
                    },
                };
                // To the address, so both queues get a copy — which is
                // what a subscriber on either `receive:` sees.
                await fanout.SendAsync(message);
                await Task.Delay(1000, ct);
            }
            _ = cranes; _ = gates;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Same contract as the 0.9.1 publisher: the host and the
            // workbench stay up whether or not a broker is running.
            app.Logger.LogDebug(ex, "AMQP 1.0 publish failed (broker down?) — retrying in 2s");
            try { await Task.Delay(2000, ct); } catch (OperationCanceledException) { break; }
        }
        finally
        {
            if (connection is not null)
            {
                try { await connection.CloseAsync(); } catch (Amqp10.AmqpException) { /* already gone */ }
            }
        }
    }
});

app.MapBowire("/bowire");
app.MapGet("/", () => Results.Redirect("/bowire"));
await app.RunAsync();
