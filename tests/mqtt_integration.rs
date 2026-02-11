use numaflow::source::{Message, SourceReadRequest, Sourcer};
use numaflow_mqtt_source::{MqttSource, PacketID};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const BROKER_PORT: u16 = 18833;

fn rumqttd_config_toml(port: u16) -> String {
    format!(
        r#"
id = 0

[router]
max_connections = 100
max_outgoing_packet_count = 200
max_segment_size = 104857600
max_segment_count = 10
initialized_filters = ["factory/data/#"]

[v4.1]
name = "v4-1"
listen = "127.0.0.1:{port}"
next_connection_delay_ms = 1

[v4.1.connections]
connection_timeout_ms = 60000
max_payload_size = 20480
max_inflight_count = 100
dynamic_filters = true
"#
    )
}

fn start_broker(port: u16) {
    let config_toml = rumqttd_config_toml(port);
    let config: rumqttd::Config = toml::from_str(&config_toml).expect("valid rumqttd config");

    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(config);
        broker.start().expect("broker started");
    });

    // Allow broker to bind and accept
    std::thread::sleep(Duration::from_millis(500));
}

async fn publish_message(port: u16, topic: &str, payload: &[u8]) {
    let mut mqttoptions = MqttOptions::new("test-publisher", "127.0.0.1", port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client
        .publish(topic, QoS::AtLeastOnce, false, payload)
        .await
        .expect("publish");

    // Poll event loop until broker acknowledges the publish
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), eventloop.poll()).await {
            Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => break,
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => panic!("publish event loop error: {}", e),
            Err(_) => continue,
        }
    }
}

async fn read_messages(
    source: Arc<numaflow_mqtt_source::MqttSource>,
    max: usize,
    timeout: Duration,
) -> Result<Vec<Message>, tokio::time::error::Elapsed> {
    let (tx, mut rx) = mpsc::channel::<Message>(max);

    let request = SourceReadRequest {
        count: max,
        timeout,
    };

    tokio::time::timeout(timeout, source.read(request, tx)).await?;

    let mut received = Vec::new();

    rx.recv_many(&mut received, max).await;

    Ok(received)
}

#[tokio::test]
async fn source_connects_and_receives_messages() {
    let _ = env_logger::try_init();

    start_broker(BROKER_PORT);

    let mut mqttoptions = MqttOptions::new("numaflow-reader-test", "127.0.0.1", BROKER_PORT);
    mqttoptions.set_keep_alive(Duration::from_secs(10));
    mqttoptions.set_clean_session(false);

    let source = MqttSource::start(mqttoptions, "factory/data/sensor1".to_string());
    let source = Arc::new(source);

    // Wait for connection and subscription
    tokio::time::sleep(Duration::from_millis(800)).await;

    let topic = "factory/data/sensor1";
    let payload = b"hello from integration test";
    publish_message(BROKER_PORT, topic, payload).await;

    let messages = read_messages(Arc::clone(&source), 1, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(messages.len(), 1, "expected one message");
    let msg = &messages[0];
    assert_eq!(msg.value.as_slice(), payload);
    assert_eq!(msg.keys, vec![topic]);

    assert_eq!(
        msg.headers.get("mqtt-topic").map(String::as_str),
        Some(topic),
        "mqtt-topic header must match published topic"
    );
}

#[tokio::test]
async fn ack_removes_message_from_broker_flow() {
    let _ = env_logger::try_init();

    start_broker(BROKER_PORT + 1);

    let mut mqttoptions = MqttOptions::new("numaflow-ack-test", "127.0.0.1", BROKER_PORT + 1);
    mqttoptions.set_keep_alive(Duration::from_secs(10));
    mqttoptions.set_clean_session(false);

    let source = MqttSource::start(mqttoptions, "factory/data/ack-test".to_string());
    let source = Arc::new(source);

    tokio::time::sleep(Duration::from_millis(800)).await;

    let topic = "factory/data/ack-test";
    let payload = b"ack me";
    publish_message(BROKER_PORT + 1, topic, payload).await;

    let mut messages = read_messages(Arc::clone(&source), 1, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(messages.len(), 1);

    let message = messages.pop().unwrap();
    let packet_id = PacketID::try_from(message.offset).unwrap();

    source.ack(vec![packet_id.into()]).await;

    // After ack, the message should not be redelivered. A subsequent read with no new publish
    // should not return the same message again (we might get nothing or only future messages).
    tokio::time::sleep(Duration::from_millis(200)).await;

    // We should not see the same message again (no redelivery after ack)
    assert!(
        read_messages(Arc::clone(&source), 1, Duration::from_millis(100))
            .await
            .is_err(),
    );
}

#[tokio::test]
async fn nack_removes_from_pending_without_puback() {
    let _ = env_logger::try_init();

    start_broker(BROKER_PORT + 2);

    let mut mqttoptions = MqttOptions::new("numaflow-nack-test", "127.0.0.1", BROKER_PORT + 2);
    mqttoptions.set_keep_alive(Duration::from_secs(10));
    mqttoptions.set_clean_session(false);

    let source = MqttSource::start(mqttoptions, "factory/data/nack-test".to_string());
    let source = Arc::new(source);

    tokio::time::sleep(Duration::from_millis(800)).await;

    let topic = "factory/data/nack-test";
    let payload = b"nack me";
    publish_message(BROKER_PORT + 2, topic, payload).await;

    let mut messages = read_messages(Arc::clone(&source), 1, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(messages.len(), 1);

    let message = messages.pop().unwrap();
    let packet_id = PacketID::try_from(message.offset).unwrap();

    source.nack(vec![packet_id.clone().into()]).await;

    // Nack should complete without panic. The broker did not receive PUBACK, so it may redeliver.
    // We only verify that nack runs and that calling ack with the same offset later does not crash
    // (ack for unknown pkid is a no-op in our implementation).
    source.ack(vec![packet_id.into()]).await;
}
