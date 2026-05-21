# Kuestenlogik.Bowire.Protocol.Amqp

[![NuGet](https://img.shields.io/nuget/v/Kuestenlogik.Bowire.Protocol.Amqp.svg)](https://www.nuget.org/packages/Kuestenlogik.Bowire.Protocol.Amqp)

Bowire plugin for AMQP — both **AMQP 0.9.1** (RabbitMQ, ActiveMQ Classic) and **AMQP 1.0** (Azure Service Bus, ActiveMQ Artemis) through a single plugin id (`amqp`). The wire variant is selected from the URL scheme.

> **Status: 0.1.x skeleton.** Discovery, invocation, and streaming are stubbed
> with `NotImplementedException`. This release pins the plugin's surface area
> (`IBowireProtocol` shape, URL scheme, package id, dependency set) so
> embedded hosts can already reference it; functional implementation lands
> alongside `Bowire 1.6.0` in subsequent releases.

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

## Planned discovery + invocation

- **0.9.1 discovery** via RabbitMQ's Management HTTP API (`/api/exchanges`, `/api/queues`). Each exchange becomes a Bowire service, each binding becomes a method. Falls back to a flat list of well-known exchanges (`amq.direct`, `amq.topic`, `amq.fanout`) when the Management plugin isn't installed.
- **1.0 discovery** is schema-less by spec — the plugin exposes a synthetic `Broker` service with a `Send` and `Receive` method that accept a target address as metadata.
- **Invocation**:
  - 0.9.1: `basic.publish` with the AsyncAPI-bound routing key / exchange / message properties (delivery mode, expiration, headers).
  - 1.0: `Session.Send` against the configured target address.
- **Streaming**:
  - 0.9.1: open a consumer on a temporary auto-delete queue bound to the requested exchange + routing key, yield messages until the channel is closed.
  - 1.0: open a `ReceiverLink` against the target, same yield-loop shape.

## Install

```bash
dotnet add package Kuestenlogik.Bowire.Protocol.Amqp
```

Embedded mode picks the plugin up via `AddBowire()` plugin scanning, no manual
registration required. Standalone CLI: `bowire plugin install Kuestenlogik.Bowire.Protocol.Amqp`.

## AsyncAPI bindings

Once functional, the AsyncAPI plugin (`Kuestenlogik.Bowire.AsyncApi`) will
route `bindings.amqp` and `bindings.amqp1` declarations through this plugin
without further wiring — the registry-driven binding lookup is already in
place upstream.

## License

Apache 2.0. See [LICENSE](LICENSE).
