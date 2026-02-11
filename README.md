# Numaflow MQTT Source

A user-defined source for Numaflow that connects to an MQTT broker and receives
messages from the specified topic.

## Usage

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: example-pipeline
spec:
  interStepBufferServiceName: example-isbsvc
  vertices:
    - name: ingest
      source:
        udsource:
          container:
            image: playerdata/numaflow-mqtt-source:latest

            env:
              - name: RUST_LOG
                value: debug

              - name: MQTT_HOST
                value: test.mosquitto.org
              - name: MQTT_PORT
                value: "1883"

              - name: MQTT_TOPIC
                value: numaflow-mqtt-source/#

    - name: out
      sink:
        log: {}

  edges:
    - from: ingest
      to: blackhole

  watermark:
    idleSource:
      incrementBy: 2s
      stepInterval: 3s
      threshold: 5s
```

## Testing

Unit and integration tests:

```bash
cargo test
```

Integration tests in `tests/mqtt_integration.rs` use an embedded **rumqttd** broker.
They verify that the source connects, receives messages, and that `ack` and `nack`
behave correctly (ack sends PUBACK so the broker does not redeliver; nack drops
from pending without PUBACK).
