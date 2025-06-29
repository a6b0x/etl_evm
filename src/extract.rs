use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::Block;
use eyre::Result;

pub struct RpcClient {
    provider: Box<dyn Provider>, // 使用动态分发
}

impl RpcClient {
    pub fn new(url: &str) -> Result<Self> {
        let rpc_url = url.parse()?;
        let provider: Box<dyn Provider> = Box::new(ProviderBuilder::new().connect_http(rpc_url));
        Ok(Self { provider })
    }

    pub async fn get_new_block_number(&self) -> Result<(u64)> {
        let latest_block = self.provider.get_block_number().await?;
        Ok(latest_block)
    }

    pub async fn get_block_data(&self, block_number: u64) -> Result<Option<Block>> {
        let block_data = self
            .provider
            .get_block_by_number(block_number.into())
            .full()
            .await
            .unwrap();
        Ok(block_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::AppConfig;
    use log::info;

    #[tokio::test]
    async fn test_rpc_client() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();
        let new_block_number = rpc_client.get_new_block_number().await.unwrap();
        info!("get_new_block_number : {:?}", new_block_number);

        let new_block_data = rpc_client.get_block_data(new_block_number).await.unwrap();
        info!("get_block_data Block.header: {:#?}", new_block_data.unwrap().header);
    }
}
