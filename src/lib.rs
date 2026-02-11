mod packet_id;

use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
use rumqttc::mqttbytes::v4::Publish;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, Receiver};

pub use crate::packet_id::PacketID;

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
        // Remove from pending without sending PUBACK so the broker can redeliver.
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

    pub fn start(mut mqttoptions: MqttOptions, topic: String) -> MqttSource {
        let (tx, rx) = mpsc::channel::<InflightMessage>(1000);
        let pending_publishes = Arc::new(Mutex::new(HashMap::new()));

        // Disable auto-ack.
        // This ensures the broker keeps the message in flight until *we* manually ack it.
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
                    Ok(notification) => {
                        match notification {
                            Event::Incoming(Packet::Publish(publish)) => {
                                let pkid: PacketID = publish.pkid.into();
                                let topic = publish.topic.clone();
                                let payload = publish.payload.to_vec();

                                log::debug!("Received MQTT message: {:?}", publish);

                                // Store Publish for later ack (rumqttc requires &Publish for ack)
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
                        }
                    }
                    Err(e) => {
                        log::error!("MQTT connection error: {:?}. Retrying...", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        source
    }
}
