# Kuestenlogik.Bowire.Protocol.Amqp.Sample

An AMQP sample that points at **external** brokers (AMQP has no
.NET-embeddable broker), demonstrating **both** ways Bowire meets an AMQP
broker and **both** wires, from one project:

- **Embedded** — the workbench is mounted at `/bowire`, the bundled
  `amqp-catalogue.json` seeds the Sources rail with both brokers, and two
  resilient background publishers keep a live surface on each wire:
  - **0.9.1 (RabbitMQ)** declares a `harbor` topic exchange with bound
    queues (`harbor.cranes`, `harbor.gates`) and emits crane telemetry
    once a second.
  - **1.0 (Artemis)** sends the same telemetry to a `harbor` address with
    two queues bound to it, which the broker's own management surface
    then reports to discovery.
- **Separate** — point an external workbench or the CLI at either broker.

The publishers are resilient: if a broker isn't up yet the host +
workbench still start and they keep retrying, re-declaring topology on
reconnect.

## Run

```pwsh
docker compose up            # RabbitMQ + management plugin on :5672 / :15672
dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Amqp.Sample
```

- Embedded workbench: <http://localhost:5195/bowire> — the broker is
  already in the Sources rail. Discovery (via the management API) surfaces
  the `harbor` exchange as a `send` service and the queues as streaming
  `receive` services; subscribe to `harbor.cranes` to watch the live crane
  telemetry.
- As a separate target:

  ```pwsh
  bowire --url amqp://bowire:bowire@localhost:5672    # 0.9.1
  bowire --url amqp1://bowire:bowire@localhost:5673   # 1.0
  ```

## Notes

- The **management-plugin image** (`rabbitmq:4-management-alpine`, not
  plain `rabbitmq`) is required — the plugin discovers exchanges/queues via
  the management HTTP API on `:15672`.
- The compose file provisions a dedicated `bowire`/`bowire` user (not the
  default `guest`): `guest` is loopback-only, and a Docker-mapped port
  makes the connection look remote, so `guest` would be refused. The
  credentials are embedded in the catalogue URL deliberately — this is a
  localhost dev sample.
- **Artemis needs no second port.** AMQP 1.0 has no discovery of its own,
  but Artemis answers management requests over the AMQP connection itself
  (`activemq.management`), so `:5673` is the only port the plugin uses —
  the console on `:8162` is there for a human. Discovery reports the
  `harbor` address with a `receive:` per bound queue; subscribe to
  `receive:harbor.cranes` to watch the same telemetry arrive over 1.0.
  Set the `amqp10Discovery` plugin setting (or
  `?_amqp10Discovery=none` on the URL) to `none` to see what an
  unrecognised 1.0 broker looks like: one generic `Broker` service.
