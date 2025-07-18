use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::eth::Block;
use alloy::rpc::types::eth::Header;
use eyre::Result;
use futures_util::StreamExt;
pub struct EvmBlock {
    pub provider: DynProvider,
}

impl EvmBlock {
    pub async fn new(url: &str) -> Result<Self> {
        if url.starts_with("ws://") || url.starts_with("wss://") {
            let ws_connect = WsConnect::new(url);
            let ws_provider = ProviderBuilder::new().connect_ws(ws_connect).await?;
            Ok(Self {
                provider: ws_provider.erased(),
            })
        } else {
            let http_url = url.parse()?;
            let http_provider = ProviderBuilder::new().connect_http(http_url);
            Ok(Self {
                provider: http_provider.erased(),
            })
        }
    }

    pub async fn subscribe_block_header(&self) -> Result<impl StreamExt<Item = Header>> {
        let sub = self.provider.subscribe_blocks().await?;
        Ok(sub.into_stream())
    }

    pub async fn get_latest_block_number(&self) -> Result<(u64)> {
        let latest_block_number = self.provider.get_block_number().await?;
        Ok(latest_block_number)
    }

    pub async fn get_block_by_number(&self, block_number: u64) -> Result<Option<Block>> {
        let block_raw_data = self
            .provider
            .get_block_by_number(block_number.into())
            .full()
            .await
            .unwrap();
        Ok(block_raw_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::AppConfig;
    use futures_util::future::ok;
    use log::info;

    #[tokio::test]
    async fn test_evm_block_ws() -> Result<()> {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let subscriber = EvmBlock::new(&app_config.eth.ws_url).await?;
        let mut block_srteam = subscriber.subscribe_block_header().await?;
        let mut receive_count = 0;
        while let Some(header) = block_srteam.next().await {
            info!("Received block {:#?}  ", header);
            receive_count += 1;
            if receive_count >= 3 {
                info!(
                    "Reached the limit of {:?} block number {:?} exiting loop.",
                    receive_count, header.number
                );
                break;
            }
            info!(
                "block number {:?} total received {:?} ",
                header.number, receive_count
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_evm_block_http() -> Result<()> {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let evm_block = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let new_block_number = evm_block.get_latest_block_number().await.unwrap();
        info!("get_latest_block_number : {:?}", new_block_number);
        let new_block_data = evm_block
            .get_block_by_number(new_block_number)
            .await
            .unwrap();
        let new_block_header = new_block_data.unwrap();
        info!(
            "get_block_data Block.header: {:#?}",
            new_block_header.header
        );
        info!(
            "get_block_data Block.first_transaction: {:#?}",
            new_block_header.transactions.first_transaction()
        );
        Ok(())
    }
}
