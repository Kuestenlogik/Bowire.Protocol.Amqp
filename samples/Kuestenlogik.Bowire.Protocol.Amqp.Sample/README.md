# Kuestenlogik.Bowire.Protocol.Amqp.Sample

An AMQP sample that points at an **external** RabbitMQ broker (AMQP has no
.NET-embeddable broker), demonstrating **both** ways Bowire meets an AMQP
broker, from one project:

- **Embedded** — the workbench is mounted at `/bowire`, the bundled
  `amqp-catalogue.json` seeds the Sources rail with the broker, and a
  resilient background publisher declares a `harbor` topic exchange with
  bound queues (`harbor.cranes`, `harbor.gates`) and emits crane telemetry
  once a second, so discovery has a live surface.
- **Separate** — point an external workbench or the CLI at the same
  broker.

The publisher is resilient: if the broker isn't up yet the host +
workbench still start and it keeps retrying, re-declaring topology on
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
  bowire --url amqp://bowire:bowire@localhost:5672
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
