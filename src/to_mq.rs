use eyre::Result;
use fluvio::metadata::topic::TopicSpec;
use fluvio::{Fluvio, FluvioConfig, Offset, RecordKey};
use futures_util::StreamExt;
use log::debug;

const PARTITIONS: u32 = 1;
const REPLICAS: u32 = 1;
pub struct Mq {
    fluvio: Fluvio,
}

impl Mq {
    /// Connect to a Fluvio cluster.
    pub async fn new(address: &str) -> Result<Self> {
        //let mut cfg = FluvioConfig::new(address);
        //cfg.use_spu_local_address = true;
        let cfg = FluvioConfig::new(address);
        debug!("cfg: {:#?}", cfg);
        let fluvio = Fluvio::connect_with_config(&cfg)
            .await
            .map_err(|e| eyre::eyre!("Failed to connect to Fluvio: {}", e))?;
        Ok(Self { fluvio })
    }

    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let admin = self.fluvio.admin().await;
        let topics = admin
            .all::<TopicSpec>()
            .await
            .expect("Failed to list topics");
        let topic_names = topics
            .iter()
            .map(|topic| topic.name.clone())
            .collect::<Vec<String>>();
        Ok(topic_names)
    }

    pub async fn create_topic(&self, topic_name: &str) -> Result<()> {
        let admin = self.fluvio.admin().await;
        let topic_spec = TopicSpec::new_computed(PARTITIONS, REPLICAS, None);
        admin
            .create(topic_name.to_string(), false, topic_spec)
            .await
            .map_err(|e| eyre::eyre!("Failed to create topic: {}", e))?;
        Ok(())
    }

    pub async fn produce_record(&self, topic_name: &str, record: &str) -> Result<()> {
        let producer = self
            .fluvio
            .topic_producer(topic_name)
            .await
            .map_err(|e| eyre::eyre!("Failed to create producer: {}", e))?;
        producer
            .send(RecordKey::NULL, record)
            .await
            .map_err(|e| eyre::eyre!("Failed to send record: {}", e))?;
        producer
            .flush()
            .await
            .map_err(|e| eyre::eyre!("Failed to flush producer: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::AppConfig;
    use chrono::Local;
    use log::debug;

    #[tokio::test]
    async fn test_mq() -> Result<()> {
        let app_config = AppConfig::new().unwrap();
        let _ = app_config.init_log().unwrap();
        debug!("app_config: {:#?}", app_config);

        let mq = Mq::new(&app_config.mq.broker_url)
            .await
            .expect("Failed to connect to Fluvio. Is it running locally?");

        mq.create_topic("test-topic").await?;
        let topics = mq.list_topics().await?;
        debug!("topics: {:#?}", topics);

        let msg = format!("Hello World! - Time is {}", Local::now().to_rfc2822());
        mq.produce_record("test-topic", &msg).await?;

        Ok(())
    }
}
