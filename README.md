# Spring Boot Outbox Starter

[![CI](https://github.com/KHolodilin/spring-boot-outbox-starter/actions/workflows/ci.yml/badge.svg)](https://github.com/KHolodilin/spring-boot-outbox-starter/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/KHolodilin/spring-boot-outbox-starter/branch/main/graph/badge.svg)](https://codecov.io/gh/KHolodilin/spring-boot-outbox-starter)
[![Maven Central](https://img.shields.io/maven-central/v/com.kholodilin/spring-boot-outbox-starter.svg?label=maven-central)](https://central.sonatype.com/artifact/com.kholodilin/spring-boot-outbox-starter)
[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Transactional Outbox Spring Boot starter for Java 21 / Spring Boot 4.1 with **multi-channel** pipelines, PostgreSQL as source of truth, and memory or Redis wake-up queues.

## Modules

| Module | Description |
|--------|-------------|
| `outbox-core` | Model, SPI, fluent API, channel registry, publisher/recovery workers |
| `outbox-persistence-jdbc` | JDBC `OutboxStore`, partitioned DDL, schema create/validate |
| `outbox-queue-memory` | In-process dispatch queue (default) |
| `outbox-queue-redis` | Shared Redis wake-up queue (fail-open) |
| `spring-boot-outbox-starter` | Auto-configuration, Micrometer, health |
| `outbox-demo-kafka` | Single-channel demo → Kafka |
| `outbox-demo-rest` | Dual-channel demo (`payments` + `webhooks`) |

## Architecture

```mermaid
flowchart LR
    subgraph ms [Microservice]
      TX["@Transactional service"]
      OS[OutboxService]
      subgraph chOrders [channel orders]
        S1[OutboxStore]
        Q1[DispatchQueue]
        W1[PublisherWorker]
        R1[RecoveryWorker]
        K[OutboxSink]
      end
    end
    PG[(PostgreSQL)]
    TX --> OS --> S1 --> PG
    S1 --> Q1 --> W1 --> K
    R1 --> S1
    R1 --> Q1
```

Each **channel** is an isolated pipeline: `table → queue → publisher worker → OutboxSink`.  
If `outbox.channels` is empty, an implicit channel `default` (table `outbox_events`) is used.

## Quick start

```xml
<dependency>
  <groupId>com.kholodilin</groupId>
  <artifactId>spring-boot-outbox-starter</artifactId>
  <version>0.1.1</version>
</dependency>
```

```java
@Component
public class KafkaOrdersSink implements OutboxSink {
    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        // deliver batch; do not update outbox tables
        return new OutboxPublishResult.AllSucceeded();
    }
}
```

```java
@Transactional
public void createOrder(Order order) {
    // business writes...
    outboxService
            .eventType("ORDER_CREATED")
            .aggregateId(String.valueOf(order.id()))
            .partitionKey(order.customerId())
            .payload(order)
            .append();
}
```

## Fluent API (multi-channel)

```java
outboxService
    .channel("orders")
    .eventType("ORDER_CREATED")
    .aggregateId(String.valueOf(orderId))
    .partitionKey(customerId)
    .payload(payload)
    .append();

outboxService
    .channel("notifications")
    .eventType("ORDER_EMAIL_REQUESTED")
    .aggregateId(String.valueOf(orderId))
    .partitionKey(customerId)
    .payload(emailPayload)
    .append();
```

Bind sinks with `@OutboxChannelSink("orders")`. A single unqualified `OutboxSink` bean maps to channel `default`.

## Configuration

```yaml
outbox:
  enabled: true
  instance-id: ${HOSTNAME:local}
  defaults:
    persistence:
      schema:
        # create  — run DDL at startup (dev/demo)
        # validate — fail fast if the table is missing/incompatible (production)
        # none — do nothing; you own migrations
        mode: create
    queue:
      # memory — in-process queue (default; one JVM only)
      # redis  — shared wake-up queue (requires StringRedisTemplate)
      # auto   — redis if StringRedisTemplate is present, otherwise memory
      type: memory
      capacity: 10000
      batch-size: 250
      batch-wait: 50ms
      usage-threshold: 0.8
      redis:
        key-prefix: "outbox:"   # used when type is redis (or auto→redis)
    publisher:
      enabled: true
      lease-duration: 30s
      max-retries: 5
    recovery:
      enabled: true
      interval: 10s
      batch-size: 500
  channels:
    orders:
      persistence:
        table-name: outbox_events_orders
    notifications:
      persistence:
        table-name: outbox_events_notifications
      queue:
        type: redis
```

### Queue type: `auto`

When `outbox.*.queue.type` is `auto`, the starter picks the implementation at startup:

| Condition | Resolved type |
|-----------|----------------|
| `StringRedisTemplate` bean is in the context | `redis` |
| otherwise | `memory` |

Use `auto` when the same app sometimes runs with Redis and sometimes without. Prefer an explicit `memory` or `redis` in production so misconfiguration fails loudly (`redis` without `StringRedisTemplate` → startup failure).

## Schema

Table-per-channel, partitioned by `status` (ACTIVE `< 100`, ARCHIVE `≥ 100`). Canonical DDL: `outbox-persistence-jdbc` resource `outbox-schema.sql`.

## Metrics & health

Event-level metrics carry tags `channel` and `eventType`:

- `outbox_enqueue_total`, `outbox_dequeue_total`
- `outbox_publish_total{result}`, `outbox_publish_seconds`
- `outbox_recovery_total`
- gauges `outbox_queue_size{channel}`, `outbox_queue_pressure{channel}`

Health indicator name: `outbox` (per-channel pressure / publisher enabled).

## Demo apps

- **outbox-demo-kafka** — default channel → Kafka topic `payments.events`
- **outbox-demo-rest** — `payments` (stub sink) + `webhooks` (REST) isolation

## Relationship to idempotency-starter

Outbox does **not** include HTTP idempotency. Compose with [`spring-boot-idempotency-starter`](https://github.com/KHolodilin/spring-boot-idempotency-starter) inside the same `@Transactional` boundary when needed.

## Requirements

- Java 21+
- Spring Boot 4.x (Jackson 3)
- PostgreSQL 13+
- An application-provided `OutboxSink` bean (per channel when `publisher.enabled=true`)

## Build

```bash
mvn clean verify     # integration tests require a running Docker daemon (Testcontainers)
```

The build enforces code format (Spotless / Palantir Java Format — run `mvn spotless:apply`
to fix), environment constraints (Maven Enforcer), javadoc validity and a minimum of
85% line coverage per library module (JaCoCo; the HTML report lands in
`<module>/target/site/jacoco/index.html`).

## Releasing

Push a tag — CI publishes signed artifacts to Maven Central and creates a GitHub Release:

```bash
git tag v0.1.1
git push origin v0.1.1
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
