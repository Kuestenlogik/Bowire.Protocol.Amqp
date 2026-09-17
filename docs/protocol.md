---
title: AMQP
summary: 'AMQP 0.9.1 + 1.0 in one sibling plugin. RabbitMQ via RabbitMQ.Client, ActiveMQ / Solace / Azure Service Bus via AMQPNetLite. Wire picked by URL scheme.'
---

<!--
  This file is the source of truth for this plugin's page on https://bowire.io.

  Bowire's docs build fetches it into its own docs/protocols/amqp.md and
  links it from the protocols navigation. Bowire keeps the fetched result
  committed so its site still builds when this repository is unreachable —
  that copy is overwritten on every build, so edit this file, never that one.

  The page lives here because the behaviour it describes lives here. While it
  sat in the Bowire repository, a claim and the code making it true could only
  be fixed in two separate commits, and for at least one setting they drifted
  for weeks.
-->

# AMQP Protocol

`Kuestenlogik.Bowire.Protocol.Amqp` (sibling repo) covers **both** major AMQP wires in one plugin id — the actual wire is picked from the URL scheme:

| URL scheme | Wire | Library | Discovery |
|------------|------|---------|-----------|
| `amqp://` / `amqps://` | AMQP **0.9.1** | RabbitMQ.Client | RabbitMQ Management HTTP API |
| `amqp1://` / `amqps1://` | AMQP **1.0** | AMQPNetLite | Artemis management over AMQP; Service Bus ATOM feed; synthetic `Broker` otherwise |

**Package:** `Kuestenlogik.Bowire.Protocol.Amqp` (sibling repo, not bundled with the CLI)

## Setup

```bash
bowire plugin install Kuestenlogik.Bowire.Protocol.Amqp
```

### Standalone — AMQP 0.9.1 (RabbitMQ)

```bash
bowire --url amqp://localhost:5672
```

### Standalone — AMQP 1.0 (ActiveMQ / Solace / Azure SB)

```bash
bowire --url amqp1://localhost:5672
```

### Embedded

```csharp
app.MapBowire(options =>
{
    options.ServerUrls.Add("amqp://localhost:5672");
});
```

## Discovery

**AMQP 0.9.1** — `AmqpDiscovery` hits the RabbitMQ Management HTTP API (`/api/queues`, `/api/exchanges`) on port 15672 by default. Each queue surfaces as a service with `publish` (Unary) and `consume` (ServerStreaming); each exchange surfaces as a service with `publish` per binding key.

**AMQP 1.0** — the spec defines no discovery, but the two brokers Bowire meets most each answer a question about themselves, and neither needs a credential the connection does not already carry.

- **ActiveMQ Artemis** answers management requests over the AMQP connection itself: a message to `activemq.management` naming a resource and an operation, replied to on a temporary queue. Discovery asks it for `listAddresses` and `listQueues`. Each address becomes a service with `send`; each queue bound to it becomes a `receive:<queue>` on that service, addressed in Artemis' fully-qualified form (`address::queue`) so a multicast address with several subscriptions can be told apart. A queue named after its address — the ordinary anycast case — is the bare `receive`. The broker's own plumbing (`$sys.*`, `activemq.*`, temporary addresses) is hidden unless `showInternalServices` is on.
- **Azure Service Bus** serves the namespace's ATOM management feed over HTTPS, signed with the shared-access key already in the URL (`amqps1://<keyName>:<key>@<ns>.servicebus.windows.net`). Queues become services with `send` + `receive`; topics become services with `send` and one `receive:<subscription>` per subscription, addressed `topic/Subscriptions/name`.

  The feed's location is derived, not assumed: `https://<host>/` for a live namespace, and `http://<host>:<port>/` when the connection is plaintext and `?_mgmtPort=` names a port. That second form is what reaches [Microsoft's Service Bus emulator](https://learn.microsoft.com/azure/service-bus-messaging/overview-emulator), which serves the same feed over HTTP on 5300 — a port no part of the AMQP URL implies. An emulator also needs `?_amqp10Discovery=servicebus`, because `auto` recognises Service Bus by hostname and `127.0.0.1` is not one:

  ```
  amqp1://RootManageSharedAccessKey:SAS_KEY_VALUE@127.0.0.1:5672?_amqp10Discovery=servicebus&_mgmtPort=5300
  ```
- **Anything else** — Solace, Qpid, a bespoke 1.0 endpoint — keeps the synthetic `Broker` service with generic `send` + `receive`, and the target address rides on the `address` metadata key or the URL path, exactly as before.

Which one to ask comes from the `amqp10Discovery` setting (per-connection: `?_amqp10Discovery=…`). `auto`, the default, reads the host — a `*.servicebus.*` name is Service Bus — and otherwise tries Artemis. Nothing answering is not an error: discovery falls back to the synthetic service rather than failing the connection, so a broker that is neither behaves as it always did.

## Invocation

Consume returns a structured envelope per message:

```json
{
  "exchange": "...",
  "routingKey": "...",
  "contentType": "application/json",
  "messageId": "...",
  "encoding": "...",
  "payload": "..."
}
```

## Security

Both wires honour the shared `__bowireMtls__` + `__bowireAmqpSasl__` marker keys for client cert + SASL auth respectively.

## Settings

- `managementApiPort` (number, default `15672`) — RabbitMQ Management API port; per-URL override via `?_mgmtPort=…`
- `discoveryTimeoutSeconds` (number, default `5`) — also bounds the Artemis management round-trip and the Service Bus feed
- `amqp10Discovery` (select, default `auto`) — `auto` / `artemis` / `servicebus` / `none`; which broker's management surface an `amqp1://` endpoint is asked. Per-connection override: `?_amqp10Discovery=<value>`
- `receiveTimeoutSeconds` (number, default `30`)

## Mock replay

`AmqpMockEmitter` re-publishes recorded messages on a `bowire mock` server. Recordings with `protocol: "amqp"` step kind get routed here, honouring loop / replay-speed.

## Coverage

First sibling plugin to clear stable. Live Testcontainers integration suites under `[Trait("Category","Docker")]` cover all three brokers: RabbitMQ for 0.9.1 (protocol + mock-emitter publish loop), ActiveMQ Artemis for 1.0 (management discovery, multicast fan-out to each discovered queue, anycast round-trip), and Microsoft's Service Bus emulator for the Service Bus flavour (the ATOM feed, a queue's methods, a topic's per-subscription methods, and the address each one resolves to).

What the emulator does not prove is the SAS signature — it accepts any `Authorization` header it is handed — so that stays covered by an independently computed vector in the unit suite. A live namespace is a paid resource and is not in CI.

The emulator needs SQL Server beside it, a 2.3 GB pull where the other two images are under 600 MB, so it carries a second trait: `dotnet test --filter-not-trait "Broker=ServiceBusEmulator"` drops it without dropping the rest of the Docker suite.

See: [Recording](../features/recording.md), [Mock Server](../features/mock-server.md).
