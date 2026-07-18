// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

// Combined AMQP sample for Bowire. AMQP has no pure-.NET embeddable broker,
// so this sample points at an *external* RabbitMQ (docker-compose.yml
// alongside) while telling both stories from one project:
//
//   * Embedded — the workbench is mounted at /bowire and the bundled
//     amqp-catalogue.json seeds the Sources rail with the broker; a
//     resilient background publisher declares a `harbor` topic exchange +
//     bound queues and emits crane telemetry once a second so discovery
//     has a live surface.
//   * Separate — point an external workbench or
//     `bowire --url amqp://bowire:bowire@localhost:5672` at the same broker.
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

app.MapBowire("/bowire");
app.MapGet("/", () => Results.Redirect("/bowire"));
await app.RunAsync();
