use numaflow::source::{Message, SourceReadRequest, Sourcer};
use numaflow_mqtt_source::{MqttSource, PacketID};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
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

[v5.1]
name = "v5-1"
listen = "127.0.0.1:{port}"
next_connection_delay_ms = 1

[v5.1.connections]
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

    std::thread::sleep(Duration::from_millis(500));
}

async fn publish_message(port: u16, topic: &str, payload: &[u8]) {
    let mut mqttoptions = MqttOptions::new("test-publisher", "127.0.0.1", port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client
        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
        .await
        .expect("publish");

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
    mqttoptions.set_clean_start(false);

    let source = MqttSource::start(mqttoptions, "factory/data/sensor1".to_string(), None);
    let source = Arc::new(source);

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
    mqttoptions.set_clean_start(false);

    let source = MqttSource::start(mqttoptions, "factory/data/ack-test".to_string(), None);
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

    tokio::time::sleep(Duration::from_millis(200)).await;

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
    mqttoptions.set_clean_start(false);

    let source = MqttSource::start(mqttoptions, "factory/data/nack-test".to_string(), None);
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

    source.ack(vec![packet_id.into()]).await;
}

#[tokio::test]
async fn shared_subscription_no_duplicate_delivery() {
    let _ = env_logger::try_init();

    start_broker(BROKER_PORT + 3);

    let port = BROKER_PORT + 3;
    let topic = "factory/data/shared-test";
    let share_group = "numaflow-workers".to_string();

    let mut opts_a = MqttOptions::new("numaflow-shared-a", "127.0.0.1", port);
    opts_a.set_keep_alive(Duration::from_secs(10));
    opts_a.set_clean_start(false);

    let mut opts_b = MqttOptions::new("numaflow-shared-b", "127.0.0.1", port);
    opts_b.set_keep_alive(Duration::from_secs(10));
    opts_b.set_clean_start(false);

    let source_a = Arc::new(MqttSource::start(
        opts_a,
        topic.to_string(),
        Some(share_group.clone()),
    ));
    let source_b = Arc::new(MqttSource::start(
        opts_b,
        topic.to_string(),
        Some(share_group.clone()),
    ));

    tokio::time::sleep(Duration::from_millis(800)).await;

    let message_count = 10;
    for i in 0..message_count {
        publish_message(port, topic, format!("msg-{}", i).as_bytes()).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let (tx_a, mut rx_a) = mpsc::channel::<Message>(message_count);
    let (tx_b, mut rx_b) = mpsc::channel::<Message>(message_count);

    let source_a_clone = Arc::clone(&source_a);
    let source_b_clone = Arc::clone(&source_b);

    let (_, _) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(3),
            source_a_clone.read(
                SourceReadRequest {
                    count: message_count,
                    timeout: Duration::from_secs(3)
                },
                tx_a,
            ),
        ),
        tokio::time::timeout(
            Duration::from_secs(3),
            source_b_clone.read(
                SourceReadRequest {
                    count: message_count,
                    timeout: Duration::from_secs(3)
                },
                tx_b,
            ),
        ),
    );

    let mut from_a = Vec::new();
    let mut from_b = Vec::new();
    rx_a.recv_many(&mut from_a, message_count).await;
    rx_b.recv_many(&mut from_b, message_count).await;

    let total = from_a.len() + from_b.len();

    assert_eq!(
        total, message_count,
        "shared subscription must deliver each message exactly once (got {} total, expected {})",
        total, message_count
    );

    let mut all_payloads: Vec<Vec<u8>> = from_a
        .iter()
        .chain(from_b.iter())
        .map(|m| m.value.clone())
        .collect();
    all_payloads.sort();
    all_payloads.dedup();
    assert_eq!(
        all_payloads.len(),
        message_count,
        "no duplicate messages should be delivered"
    );
}
