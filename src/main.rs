use anyhow::Result;
use numaflow_mqtt_source::MqttSource;
use rumqttc::{MqttOptions, TlsConfiguration, Transport};
use std::fs;
use std::time::Duration;

fn get_transport() -> Transport {
    let mqtt_ca_cert = std::env::var("MQTT_CA_CERT");
    let mqtt_client_cert = std::env::var("MQTT_CLIENT_CERT");
    let mqtt_client_key = std::env::var("MQTT_CLIENT_KEY");

    let (Ok(ca_cert), Ok(client_cert), Ok(client_key)) =
        (mqtt_ca_cert, mqtt_client_cert, mqtt_client_key)
    else {
        return Transport::Tcp;
    };

    let ca_pem = fs::read(ca_cert).unwrap_or_default();
    let client_cert_pem = fs::read(client_cert).unwrap_or_default();
    let client_key_pem = fs::read(client_key).unwrap_or_default();

    Transport::Tls(TlsConfiguration::Simple {
        ca: ca_pem,
        alpn: None,
        client_auth: Some((client_cert_pem, client_key_pem)),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let id = std::env::var("NUMAFLOW_POD").unwrap();

    let mqtt_host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
    let mqtt_port = std::env::var("MQTT_PORT")
        .unwrap_or_else(|_| "1883".to_string())
        .parse::<u16>()
        .unwrap();

    let mqtt_topic = std::env::var("MQTT_TOPIC").unwrap();

    let transport = get_transport();

    let mut mqttoptions = MqttOptions::new(id, mqtt_host, mqtt_port);

    mqttoptions.set_transport(transport);
    mqttoptions.set_keep_alive(Duration::from_secs(10));

    log::info!("MQTT options: {:?}", mqttoptions);

    let source = MqttSource::start(mqttoptions, mqtt_topic);

    log::info!("Starting Numaflow MQTT source");

    numaflow::source::Server::new(source)
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}
