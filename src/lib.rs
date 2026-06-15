mod packet_id;

use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
use rumqttc::mqttbytes::v4::Publish;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, Receiver};

pub use crate::packet_id::PacketID;

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 2166136261;
    for &byte in bytes {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

/// Returns which partition bucket a topic falls into given a total partition count.
/// Exposed so tests and tooling can verify routing without duplicating the hash logic.
pub fn partition_bucket(topic: &str, total: u32) -> u32 {
    fnv1a(topic.as_bytes()) % total
}

/// Controls which partition range this source instance handles.
///
/// Messages whose topic hashes outside `[min, max)` are immediately ACKed to the
/// broker and discarded — they will be handled by the other canary slot.
/// With `total=1` (the default when env vars are absent) every message passes through.
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    pub total: u32,
    pub min: u32,
    pub max: u32,
}

impl PartitionConfig {
    /// Reads `MQTT_PARTITION_TOTAL`, `MQTT_PARTITION_MIN`, `MQTT_PARTITION_MAX` from the
    /// environment. Returns `None` (no filtering) if `MQTT_PARTITION_TOTAL` is absent.
    /// Panics on startup if the values are present but invalid.
    pub fn from_env() -> Option<Self> {
        let Ok(total_str) = std::env::var("MQTT_PARTITION_TOTAL") else {
            return None;
        };
        let total: u32 = total_str
            .parse()
            .expect("MQTT_PARTITION_TOTAL must be a valid u32");
        assert!(total > 0, "MQTT_PARTITION_TOTAL must be greater than 0");
        let min: u32 = std::env::var("MQTT_PARTITION_MIN")
            .expect("MQTT_PARTITION_MIN required when MQTT_PARTITION_TOTAL is set")
            .parse()
            .expect("MQTT_PARTITION_MIN must be a valid u32");
        let max: u32 = std::env::var("MQTT_PARTITION_MAX")
            .expect("MQTT_PARTITION_MAX required when MQTT_PARTITION_TOTAL is set")
            .parse()
            .expect("MQTT_PARTITION_MAX must be a valid u32");
        Some(Self { total, min, max })
    }

    fn contains(&self, topic: &str) -> bool {
        let bucket = fnv1a(topic.as_bytes()) % self.total;
        bucket >= self.min && bucket < self.max
    }
}

#[derive(Debug, Clone)]
struct InflightMessage {
    topic: String,
    payload: Vec<u8>,
    pkid: PacketID,
}

pub struct MqttSource {
    messages: Arc<Mutex<Receiver<InflightMessage>>>,
    client: AsyncClient,
    pending_publishes: Arc<Mutex<HashMap<PacketID, Publish>>>,
}

#[tonic::async_trait]
impl Sourcer for MqttSource {
    async fn read(
        &self,
        request: SourceReadRequest,
        transmitter: tokio::sync::mpsc::Sender<Message>,
    ) {
        let mut receiver = self.messages.lock().await;
        let mut messages = Vec::new();

        receiver.recv_many(&mut messages, request.count).await;

        for mqtt_msg in messages {
            let offset: Offset = mqtt_msg.pkid.into();

            let headers: HashMap<String, String> =
                [("mqtt-topic".to_string(), mqtt_msg.topic.clone())]
                    .into_iter()
                    .collect();

            let numa_msg = Message {
                value: mqtt_msg.payload,
                event_time: chrono::Utc::now(),
                offset,
                keys: vec![mqtt_msg.topic],
                headers,
                user_metadata: None,
            };

            if let Err(e) = transmitter.send(numa_msg).await {
                log::error!("Failed to send to Numaflow: {:?}", e);
                break;
            }
        }
    }

    async fn ack(&self, offsets: Vec<Offset>) {
        for offset in offsets {
            let pkid = PacketID::try_from(offset).unwrap();
            let publish = self.pending_publishes.lock().await.remove(&pkid);

            let Some(ref p) = publish else {
                log::warn!("Ack for unknown pkid: {:?}", pkid);
                continue;
            };

            match self.client.ack(p).await {
                Ok(_) => log::debug!("Acked MQTT pkid: {:?}", pkid),
                Err(e) => log::error!("Failed to ack MQTT pkid {:?}: {:?}", pkid, e),
            }
        }
    }

    async fn nack(&self, offsets: Vec<Offset>) {
        for offset in offsets {
            let pkid = PacketID::try_from(offset).unwrap();
            self.pending_publishes.lock().await.remove(&pkid);
            log::debug!("Nacked MQTT pkid: {:?} (removed from pending)", pkid);
        }
    }

    async fn pending(&self) -> Option<usize> {
        let pending = Some(self.pending_publishes.lock().await.len());
        log::debug!("Pending: {:?}", pending);
        pending
    }

    async fn partitions(&self) -> Option<Vec<i32>> {
        Some(vec![0])
    }
}

impl MqttSource {
    fn new(
        rx: Receiver<InflightMessage>,
        pending_publishes: Arc<Mutex<HashMap<PacketID, Publish>>>,
        client: AsyncClient,
    ) -> Self {
        Self {
            messages: Arc::new(Mutex::new(rx)),
            pending_publishes,
            client,
        }
    }

    pub fn start(
        mut mqttoptions: MqttOptions,
        topic: String,
        partition: Option<PartitionConfig>,
    ) -> MqttSource {
        let (tx, rx) = mpsc::channel::<InflightMessage>(1000);
        let pending_publishes = Arc::new(Mutex::new(HashMap::new()));

        mqttoptions.set_manual_acks(true);

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        let source = Self::new(rx, pending_publishes.clone(), client.clone());

        tokio::spawn(async move {
            if let Err(e) = client.subscribe(topic, QoS::AtLeastOnce).await {
                log::error!("Failed to subscribe: {:?}", e);
                return;
            }

            loop {
                match eventloop.poll().await {
                    Ok(notification) => match notification {
                        Event::Incoming(Packet::Publish(publish)) => {
                            let pkid: PacketID = publish.pkid.into();
                            let topic = publish.topic.clone();
                            let payload = publish.payload.to_vec();

                            log::debug!("Received MQTT message on topic '{}'", topic);

                            if let Some(ref filter) = partition
                                && !filter.contains(&topic)
                            {
                                log::debug!(
                                    "Filtered topic '{}': bucket {} not in [{}, {})",
                                    topic,
                                    partition_bucket(&topic, filter.total),
                                    filter.min,
                                    filter.max,
                                );
                                if let Err(e) = client.ack(&publish).await {
                                    log::error!(
                                        "Failed to ack filtered message pkid {:?}: {:?}",
                                        pkid,
                                        e
                                    );
                                }
                                continue;
                            }

                            pending_publishes.lock().await.insert(pkid.clone(), publish);

                            let msg = InflightMessage {
                                topic,
                                payload,
                                pkid,
                            };

                            tx.send(msg).await.unwrap();
                        }
                        Event::Incoming(Packet::ConnAck(_)) => {
                            log::info!("MQTT Connected");
                        }
                        e => {
                            log::debug!("MQTT event: {:?}", e);
                        }
                    },
                    Err(e) => {
                        log::error!("MQTT connection error: {:?}. Exiting.", e);
                        std::process::exit(1);
                    }
                }
            }
        });

        source
    }
}
