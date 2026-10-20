# fleet-events-kafka

Event-driven Mule application for mining haul truck telemetry. Trucks report position,
engine, tyre, payload and fuel readings to `fleet.telemetry`; this app validates each
reading, drops duplicates, enriches it against the truck's rated payload and the site's
thresholds, and routes it:

| Outcome                         | Topic                 |
|---------------------------------|-----------------------|
| one or more thresholds breached | `fleet.alerts`        |
| everything else                 | `fleet.metrics`       |
| cannot be parsed or validated, or keeps failing | `fleet.telemetry.dlq` |

Mule 4.9 LTS, Java 17, Anypoint Connector for Apache Kafka 4.x.

## The event

```json
{
  "eventId": "6f1c2c1e-3a77-4f51-9a0e-0b9a3c1d2e4f",
  "eventType": "TRUCK_TELEMETRY",
  "truckId": "HT-0417",
  "model": "CAT_793F",
  "siteId": "PILBARA-WEST",
  "occurredAt": "2026-10-20T03:14:07.512Z",
  "position": { "lat": -22.6931, "lon": 118.2264, "speedKph": 38.2 },
  "engine": { "coolantTempC": 96.5, "oilPressureKpa": 410, "rpm": 1650 },
  "tyres": [ { "position": "FL", "pressureKpa": 690, "tempC": 64 } ],
  "payloadTonnes": 221.4,
  "fuelLevelPct": 54.0
}
```

The record key is the truck id. Producers may send an `x-correlation-id` header.

## How it works

**Consumption and commits.** `telemetry-consumer` listens with `ackMode="MANUAL"` and calls
`kafka:commit` only once a record has been dealt with: published to alerts or metrics,
skipped as a duplicate, or parked on the dead-letter topic. An error that propagates out of
the flow leaves the offset uncommitted, so the record is consumed again. The listener's
redelivery policy (`kafka.consumer.maxRedeliveries`) caps that; after the last attempt the
record arrives as `MULE:REDELIVERY_EXHAUSTED` and is dead-lettered.

**Validation.** `dw/modules/Telemetry.dwl` parses the raw bytes and returns every problem
with the event (required fields, truck id format, ISO-8601 timestamp, numeric ranges for
position, engine, payload, fuel and tyres). A record that is not JSON raises
`FLEET:UNPARSEABLE_EVENT`; one with problems raises `FLEET:INVALID_EVENT` with all of them in
the description. Both are poison and go straight to the DLQ; retrying cannot fix them.

**De-duplication.** Before enriching, the flow checks a persistent `os:object-store`
(`telemetry-dedup-store`, TTL from `dedup.ttlMinutes`) for the `eventId`. A hit is logged,
committed and skipped. The `eventId` is stored only after the publish succeeds; storing it
earlier would make a record whose publish failed look like a duplicate when it is
redelivered. The store is persistent because the commonest duplicate is a record redelivered
after a restart or rebalance. The check is per event id, not a distributed lock: two
partitions racing on the same id could both publish, which is acceptable for at-least-once
telemetry and is rare because the truck id key keeps a truck's events on one partition.

**Enrichment and routing.** `enrich` adds the rated payload for the model, payload
utilisation, a list of breaches and an overall severity (`NORMAL`, `WARNING`, `CRITICAL`).
Thresholds are properties (`fleet.thresholds.*`), so a site can tune them without a code
change:

| Breach              | Rule                                              | Severity |
|---------------------|---------------------------------------------------|----------|
| `ENGINE_OVERHEAT`   | coolant above `coolantTempMaxC`                   | CRITICAL |
| `LOW_OIL_PRESSURE`  | below `oilPressureMinKpa` with the engine above 500 rpm | CRITICAL |
| `TYRE_PRESSURE_LOW` | any tyre below `tyrePressureMinKpa`               | WARNING  |
| `TYRE_OVERHEAT`     | any tyre above `tyreTempMaxC`                     | WARNING  |
| `OVERLOAD`          | utilisation above `overloadPct`                   | WARNING  |
| `OVERSPEED`         | above `speedMaxKph`                               | WARNING  |
| `LOW_FUEL`          | below `fuelMinPct`                                | WARNING  |

A Choice router sends anything with breaches to `fleet.alerts` and the rest to
`fleet.metrics`, keyed by truck id so each truck's events stay ordered.

**Dead-letter topic.** The original bytes are republished unchanged so the record can be
replayed once the cause is fixed. The error travels in headers: `x-error-type`,
`x-error-description`, `x-original-topic`, `x-original-partition`, `x-original-offset`,
`x-consumer-group`, `x-failed-at` and `x-correlation-id`. If the DLQ publish itself fails,
nothing is committed and the record comes back later.

**Correlation.** Each record is processed inside `tracing:with-correlation-id`, using the
producer's `x-correlation-id` header if present, otherwise the `eventId`. The same id is set
on outbound alert and metric records and on DLQ records, and `tracing:set-logging-variable`
adds the truck id, so every log line carries both.

**Configuration.** Bootstrap servers, consumer group, topic names, redelivery limit, dedup
TTL and thresholds all come from `config/config-<env>.yaml`. Broker credentials (SASL/SCRAM)
come from `secure-config-<env>.yaml`.

## Project layout

Flows never talk to Kafka or the object store themselves: every publish, commit and store
operation sits in an adapter sub-flow with a stated input and output, and only the listener
source stays in the flow.

```
src/main/mule/
  config/global.xml                            properties and secure properties
  config/connectors.xml                        Kafka consumer and producer configs
  config/object-stores.xml                     dedup store
  flows/telemetry-consumer.xml                 listener, record context, correlation, commit; process-telemetry-event
  flows/telemetry-processing.xml               parse and validate, route to alerts or metrics
  adapters/kafka-publisher-adapter.xml         publish-telemetry-alert, publish-telemetry-metric, publish-dead-letter
  adapters/kafka-consumer-adapter.xml          commit-offset
  adapters/telemetry-dedup-store-adapter.xml   is-event-published, remember-published-event
  errors/telemetry-error-handler.xml           dead-letter and commit, or leave for redelivery
src/main/resources/
  config/                                      config-<env>.yaml and secure-config-<env>.yaml for local, dev and test
  dw/modules/                                  Telemetry (validation, enrichment, messages, headers), Errors
  dw/mappings/                                 record-context, enrich-telemetry
src/test/munit/                                flows/, adapters/, errors/ and dataweave/ suites
src/test/resources/fixtures/                   telemetry records, good and bad
```

## Running locally

1. A Kafka broker with SASL/SCRAM-SHA-512 enabled and a user for the app, for example:

   ```
   kafka-configs.sh --bootstrap-server localhost:9092 --alter \
     --add-config 'SCRAM-SHA-512=[password=<password>]' \
     --entity-type users --entity-name fleet-events
   ```

   and the four topics: `fleet.telemetry`, `fleet.alerts`, `fleet.metrics`,
   `fleet.telemetry.dlq`.
2. Encrypt the username and password with the Secure Properties Tool and your own key, and
   put them in `src/main/resources/config/secure-config-local.yaml`.
3. Run from Anypoint Studio with `-Dmule.env=local -Dmule.key=<key>`.
4. Publish a reading with any producer, keyed by truck id, for example the contents of
   `src/test/resources/fixtures/telemetry/overheating.json`, and watch `fleet.alerts`.

For a TLS listener (SASL_SSL), add a `tls:context` with the cluster's trust store to both
connections in `config/connectors.xml`.

## Tests

The suites mirror the source folders. The listener is never started in MUnit: each test
builds the event the Kafka source would deliver (raw bytes plus record attributes such as
`consumerCommitKey`) and calls the consumer flow. Every `kafka:publish` and `kafka:commit` is
mocked inside its adapter by `doc:name` and counted, so the adapters run in every flow test.

- `flows/telemetry-consumer-test-suite`: normal reading to metrics and committed; breach to
  alerts; producer correlation id adopted; duplicate skipped but committed; published id
  stored in the dedup store.
- `errors/telemetry-error-handler-test-suite`: a failed publish is neither stored nor
  committed; invalid and unparseable records are dead-lettered with the right headers and
  committed.
- `adapters/telemetry-dedup-store-adapter-test-suite`: the dedup check and the stored entry,
  against the real test object store.
- `dataweave/telemetry-rules-test-suite`: validation and enrichment functions on their own.

Run them from Studio or with `mvn clean test`.
