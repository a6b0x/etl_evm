use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::Block;
use alloy::sol;
use alloy::primitives::{Address,address};
use eyre::Result;
use log::info;

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    UniswapV2Factory,
     "data/UniswapV2Factory.json"
);

pub struct RpcClient {
    provider: Box<dyn Provider>, 
}

impl RpcClient {
    pub fn new(url: &str) -> Result<Self> {
        let rpc_url = url.parse()?;
        let provider= ProviderBuilder::new().connect_http(rpc_url);
        Ok(Self { provider: Box::new(provider) })
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
    pub async fn get_uniswap_v2_all_pairs_length(&self) -> Result<u128> {
        let rpc_url = "https://reth-ethereum.ithaca.xyz/rpc".parse()?;
        let provider = ProviderBuilder::new().connect_http(rpc_url);
        const UNISWAP_V2_FACTORY_ADDR: Address = address!("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
        let contract = UniswapV2Factory::new(UNISWAP_V2_FACTORY_ADDR, provider);
        let all_pairs_length = contract.allPairsLength().call().await?;
        info!("all_pairs_length: {:?}", all_pairs_length);
        let all_pairs_length_u128: u128 = all_pairs_length.try_into().unwrap();
        Ok(all_pairs_length_u128)
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
    #[tokio::test]
    async fn test_get_uniswap_v2_all_pairs_length() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();
        let res = rpc_client.get_uniswap_v2_all_pairs_length().await.unwrap();
        info!("get_uniswap_v2_all_pairs_length: {:?}", res);
    }
}
