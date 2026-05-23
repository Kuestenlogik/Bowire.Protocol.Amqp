# Kuestenlogik.Bowire.Protocol.Amqp

[![NuGet](https://img.shields.io/nuget/v/Kuestenlogik.Bowire.Protocol.Amqp.svg)](https://www.nuget.org/packages/Kuestenlogik.Bowire.Protocol.Amqp)

Bowire plugin for AMQP — both **AMQP 0.9.1** (RabbitMQ, ActiveMQ Classic) and **AMQP 1.0** (Azure Service Bus, ActiveMQ Artemis) through a single plugin id (`amqp`). The wire variant is selected from the URL scheme.

> **Status: 0.2.x — functional.** Discovery, unary invocation, and streaming
> are wired through for both wires. AMQP 0.9.1 uses RabbitMQ.Client against
> the broker + the Management HTTP API for discovery. AMQP 1.0 uses
> AMQPNetLite with a synthetic `Broker` service (the wire has no schema
> concept). Integration tests against live brokers land in a later pass.

## URL schemes

```
amqp://host:5672/<vhost>      # AMQP 0.9.1, RabbitMQ default
amqps://host:5671/<vhost>     # AMQP 0.9.1 with TLS
amqp1://host:5672             # AMQP 1.0 (no vhost concept)
amqps1://host:5671            # AMQP 1.0 with TLS
```

The URL scheme picks the wire stack (`RabbitMQ.Client` vs `AMQPNetLite`)
the same way `grpcweb@` flips the gRPC plugin between HTTP/2 native and
gRPC-Web. One plugin, one package, two wires.

## How it discovers + invokes

- **0.9.1 discovery** uses the RabbitMQ Management HTTP API
  (`GET /api/exchanges/{vhost}`, `GET /api/bindings/{vhost}`). Every
  exchange becomes a Bowire service; every routing key bound to it
  becomes a `send:<key>` method on that service alongside the
  unconstrained `send`. The unnamed default exchange and the
  reserved `amq.*` family are hidden unless `showInternalServices`
  is on.
- **1.0 discovery** is schema-less by spec — the plugin exposes a
  synthetic `Broker` service with `send` (unary) and `receive`
  (server-streaming). The target address comes from the
  `address` metadata key (or the URL path) at invoke time.
- **Invocation**:
  - 0.9.1: `basic.publish` on a fresh channel. Metadata maps onto
    `routingKey` / `contentType` / `messageId` / `correlationId` /
    `deliveryMode` (1=non-persistent, 2=persistent) / `expiration`
    / `mandatory`. The unnamed default exchange is reachable as the
    service name `(default)`.
  - 1.0: `Session.Send` over a fresh `SenderLink` against the
    resolved address. Same metadata vocabulary (where applicable).
- **Streaming**:
  - 0.9.1: opens an `AsyncEventingBasicConsumer` on the queue named
    by the `queue` metadata key (defaults to the service name).
    Yields messages until the configured `receiveTimeoutSeconds`
    elapses with no new frame (default 30s), then closes cleanly.
  - 1.0: opens a `ReceiverLink` against the target address. Same
    timeout shape, same finite-stream contract.

Metadata keys are common across both wires where the concept exists
(content-type, message-id, correlation-id). Wire-specific keys
(`routingKey`, `mandatory`, `deliveryMode` for 0.9.1; `address` for
1.0) are documented in the inline XML comments on
`BowireAmqpProtocol`.

## Install

```bash
dotnet add package Kuestenlogik.Bowire.Protocol.Amqp
```

Embedded mode picks the plugin up via `AddBowire()` plugin scanning, no manual
registration required. Standalone CLI: `bowire plugin install Kuestenlogik.Bowire.Protocol.Amqp`.

## AsyncAPI bindings

The AsyncAPI plugin (`Kuestenlogik.Bowire.AsyncApi`) routes
`bindings.amqp` and `bindings.amqp1` declarations through this
plugin via the registry-driven binding lookup. The Phase C resolver
in the main repo lands alongside `Bowire 1.6.0`.

## License

Apache 2.0. See [LICENSE](LICENSE).
