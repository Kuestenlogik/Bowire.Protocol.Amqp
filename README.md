# Kuestenlogik.Bowire.Protocol.Amqp

[![CI](https://img.shields.io/github/actions/workflow/status/Kuestenlogik/Bowire.Protocol.Amqp/ci.yml?branch=main&label=CI)](https://github.com/Kuestenlogik/Bowire.Protocol.Amqp/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Amqp/branch/main/graph/badge.svg)](https://codecov.io/gh/Kuestenlogik/Bowire.Protocol.Amqp)
[![NuGet](https://img.shields.io/nuget/v/Kuestenlogik.Bowire.Protocol.Amqp.svg)](https://www.nuget.org/packages/Kuestenlogik.Bowire.Protocol.Amqp)
[![License](https://img.shields.io/github/license/Kuestenlogik/Bowire.Protocol.Amqp)](https://github.com/Kuestenlogik/Bowire.Protocol.Amqp/blob/main/LICENSE)
[![Bowire](https://img.shields.io/badge/Bowire-%E2%89%A5%201.5.0%2C%20%3C%202.0-006B9F)](https://github.com/Kuestenlogik/Bowire/blob/main/docs/architecture/compatibility.md)

Bowire plugin for AMQP — both **AMQP 0.9.1** (RabbitMQ, ActiveMQ Classic) and **AMQP 1.0** (Azure Service Bus, ActiveMQ Artemis) through a single plugin id (`amqp`). The wire variant is selected from the URL scheme.

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
plugin via the registry-driven binding lookup — shipped in Bowire
1.5.x as Phase C of the AsyncAPI roll-out.

## Tests

Unit tests run on every CI build with no broker required. The
integration suite under `tests/.../Integration` spins up real
RabbitMQ (0.9.1) and ActiveMQ Artemis (1.0) containers via
Testcontainers and round-trips publish + consume against each. The
integration tests carry `[Trait("Category", "Docker")]` so a local
`dotnet test --filter "Category!=Docker"` skips them on hosts
without a Docker daemon — CI runs them.

## Sample

A self-contained sample lives in the central
[`Bowire.Samples`](https://github.com/Kuestenlogik/Bowire.Samples) repo
under
[`src/Kuestenlogik.Bowire.Samples.Amqp`](https://github.com/Kuestenlogik/Bowire.Samples/tree/main/src/Kuestenlogik.Bowire.Samples.Amqp)
— `docker compose -f compose.yaml up` brings up RabbitMQ with the
management plugin, the ASP.NET host declares a `harbor` topic
exchange + bound queues and emits crane telemetry on a timer, and
Bowire pointed at `amqp://localhost:5672` discovers the surface and
streams the events live.

## License

Apache 2.0. See [LICENSE](LICENSE).
