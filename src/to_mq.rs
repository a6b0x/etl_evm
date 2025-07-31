use eyre::Result;
use fluvio::{Fluvio, FluvioConfig, Offset, RecordKey};
use futures_util::StreamExt;

pub struct Mq {
    fluvio: Fluvio,
}

impl Mq {
    /// Connect to a Fluvio cluster.
    pub async fn new(address: &str) -> Result<Self> {
        let cfg = FluvioConfig::new(address);
        let fluvio = Fluvio::connect_with_config(&cfg)
            .await
            .map_err(|e| eyre::eyre!("Failed to connect to Fluvio: {}", e))?;
        Ok(Self { fluvio })
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use fluvio::metadata::topic::TopicSpec;

    #[tokio::test]
    async fn test_mq() -> Result<()> {
        let mq = Mq::new("localhost:9003")
            .await
            .expect("Failed to connect to Fluvio. Is it running locally?");

        let admin = mq.fluvio.admin().await;
        let topics = admin.all::<TopicSpec>().await.expect("Failed to list topics");
        let topic_names = topics.iter().map(|topic| topic.name.clone()).collect::<Vec<String>>();
        println!("Topics:\n  - {}", topic_names.join("\n  - "));
        Ok(())
    }
}
