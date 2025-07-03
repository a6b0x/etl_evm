use alloy::primitives::{Address, Uint, address, keccak256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{Filter, Log, eth::Block};
use alloy::sol;
use eyre::Result;

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    UniswapV2Factory,
    "data/UniswapV2Factory.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    UniswapV2Pair,
    "data/UniswapV2Pair.json"
);

pub struct RpcClient {
    provider: DynProvider,
}

impl RpcClient {
    pub fn new(url: &str) -> Result<Self> {
        let rpc_url = url.parse()?;
        let provider = ProviderBuilder::new().connect_http(rpc_url);
        let dyn_provider = provider.erased();
        Ok(Self {
            provider: dyn_provider,
        })
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
        const UNISWAP_V2_FACTORY_ADDR: Address =
            address!("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
        let contract = UniswapV2Factory::new(UNISWAP_V2_FACTORY_ADDR, self.provider.clone());
        let all_pairs_length = contract.allPairsLength().call().await?;
        let all_pairs_length_u128: u128 = all_pairs_length.try_into().unwrap();
        Ok(all_pairs_length_u128)
    }

    pub async fn get_uniswap_v2_pair_created_events(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        const UNISWAP_V2_FACTORY_ADDR: Address =
            address!("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
        let event_signature = keccak256(b"PairCreated(address,address,address,uint256)");

        let filter = Filter::new()
            .event_signature(event_signature)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.provider.get_logs(&filter).await?;
        Ok(logs)
    }

    pub async fn get_pair_address(&self, pair_index_uint: Uint<256, 4>) -> Result<Address> {
        const UNISWAP_V2_FACTORY_ADDR: Address =
            address!("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
        let factory_contract =
            UniswapV2Factory::new(UNISWAP_V2_FACTORY_ADDR, self.provider.clone());
        let pair_address = factory_contract
            .allPairs(pair_index_uint)
            .call()
            .await?;
        Ok(pair_address)
    }

    pub async fn get_pair_liquidity_events(
        &self,
        pair_address: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<(Vec<Log>, Vec<Log>, Vec<Log>)> {
        let mint_event_signature = keccak256(b"Mint(address,uint256,uint256)");
        let burn_event_signature = keccak256(b"Burn(address,uint256,uint256,address)");
        let swap_event_signature = keccak256(b"Swap(address,uint256,uint256,uint256,uint256,address)");

        let mint_filter = Filter::new()
            .event_signature(mint_event_signature)
            .address(pair_address)
            .from_block(from_block)
            .to_block(to_block);
        let burn_filter = Filter::new()
            .event_signature(burn_event_signature)
            .address(pair_address)
            .from_block(from_block)
            .to_block(to_block);
        let swap_filter = Filter::new()
            .event_signature(swap_event_signature)
            .address(pair_address)
            .from_block(from_block)
            .to_block(to_block);

        let mint_logs = self.provider.get_logs(&mint_filter).await?;
        let burn_logs = self.provider.get_logs(&burn_filter).await?;
        let swap_logs = self.provider.get_logs(&swap_filter).await?;

        Ok((mint_logs, burn_logs, swap_logs))
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
        let new_block_header = new_block_data.unwrap();
        info!(
            "get_block_data Block.header: {:#?}",
            new_block_header.header
        );
        info!(
            "get_block_data Block.first_transaction: {:#?}",
            new_block_header.transactions.first_transaction()
        );
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
    #[tokio::test]
    async fn test_get_uniswap_v2_pair_created_events() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();

        let from_block = 22770510;
        let to_block = 22770512;
        let events = rpc_client
            .get_uniswap_v2_pair_created_events(from_block, to_block)
            .await
            .unwrap();

        info!(
            "Number of PairCreated events in block range {} - {}: {}",
            from_block,
            to_block,
            events.len()
        );
        //info!("test_get_uniswap_v2_pair_created_events: {:#?}", events.first());
        info!("test_get_uniswap_v2_pair_created_events: {:#?}", events);
    }
    #[tokio::test]
    async fn test_get_latest_pair_address() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();

        let all_pairs_length = rpc_client.get_uniswap_v2_all_pairs_length().await.unwrap();
        info!("get_uniswap_v2_all_pairs_length: {:?}", all_pairs_length);

        let latest_pair_index = all_pairs_length.saturating_sub(1);
        let latest_pair_index_uint: Uint<256, 4> = Uint::from(latest_pair_index);

        let latest_pair_address = rpc_client
            .get_pair_address(latest_pair_index_uint)
            .await
            .unwrap();
        info!("Latest Uniswap V2 pair address: {:?}", latest_pair_address);
    }

    #[tokio::test]
    async fn test_get_latest_pair_liquidity_events() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();

        let pair_address_str = "0xaAF2fe003BB967EB7C35A391A2401e966bdB7F95";
        let from_block = 22828657;
        let to_block = 22828661;

        let pair_address = pair_address_str.parse().unwrap();
        let (mint_logs, burn_logs, swap_logs) = rpc_client
            .get_pair_liquidity_events(pair_address, from_block, to_block)
            .await
            .unwrap();
        info!("mint_logs: {:#?}", mint_logs);
        info!("burn_logs: {:#?}", burn_logs);
        info!("swap_logs: {:#?}", swap_logs);
    }
}
