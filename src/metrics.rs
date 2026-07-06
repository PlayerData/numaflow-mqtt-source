use prometheus::{CounterVec, TextEncoder, register_counter_vec};
use std::sync::OnceLock;

static BYTES_SENT: OnceLock<CounterVec> = OnceLock::new();
static MESSAGES_SENT: OnceLock<CounterVec> = OnceLock::new();

pub fn bytes_sent() -> &'static CounterVec {
    BYTES_SENT.get_or_init(|| {
        register_counter_vec!(
            "mqtt_bytes_sent_total",
            "Total payload bytes forwarded to Numaflow per MQTT topic",
            &["topic"]
        )
        .unwrap()
    })
}

pub fn messages_sent() -> &'static CounterVec {
    MESSAGES_SENT.get_or_init(|| {
        register_counter_vec!(
            "mqtt_messages_sent_total",
            "Total messages forwarded to Numaflow per MQTT topic",
            &["topic"]
        )
        .unwrap()
    })
}

pub fn gather() -> String {
    let encoder = TextEncoder::new();
    let mut output = String::new();
    encoder
        .encode_utf8(&prometheus::gather(), &mut output)
        .unwrap();
    output
}
