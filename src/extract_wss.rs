use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::eth::Header;
use eyre::Result;
use futures_util::StreamExt;

pub struct EthBlockSubscriber {
    pub provider: DynProvider,
}

impl EthBlockSubscriber {
    pub async fn new(url: &str) -> Result<Self> {
        let ws = WsConnect::new(url);
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        Ok(Self {
            provider: provider.erased(),
        })
    }

    pub async fn subscribe_block_header(&self) -> Result<impl StreamExt<Item = Header>> {
        let sub = self.provider.subscribe_blocks().await?;
        Ok(sub.into_stream())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::AppConfig;
    use log::info;

    #[tokio::test]
    async fn test_eth_block_subscriber() -> Result<()> {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let rpc_url = "wss://ethereum-rpc.publicnode.com";
        let subscriber = EthBlockSubscriber::new(rpc_url).await?;
        let mut block_srteam = subscriber.subscribe_block_header().await?;

        let mut receive_count = 0;
        while let Some(header) = block_srteam.next().await {
            info!("Received block {:#?}  ", header);

            receive_count += 1;
            if receive_count >= 3 {
                info!("Reached the limit of {:?} , exiting loop.", receive_count);
                break;
            }
            info!(
                "block number {:?} total received {:?} ",
                header.number, receive_count
            );
        }
        Ok(())
    }
}
