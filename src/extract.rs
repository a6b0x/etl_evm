use alloy::primitives::{Address, Uint, address, keccak256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{Filter, Log, eth::Block};
use alloy::sol;
use eyre::Result;

pub struct RpcClient {
    pub provider: DynProvider,
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

    pub fn provider(&self) -> &DynProvider {
        &self.provider
    }
}

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    UniswapV2Router,
    "data/UniswapV2Router02.json"
);

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
pub struct UniswapV2 {
    pub provider: DynProvider,
    pub router_caller: UniswapV2Router::UniswapV2RouterInstance<DynProvider>,
    pub factory_caller: UniswapV2Factory::UniswapV2FactoryInstance<DynProvider>,
}

impl UniswapV2 {
    pub async fn new(fprovider: DynProvider, router_address: Address) -> Self {
        let router_contract = UniswapV2Router::new(router_address, fprovider.clone());

        let factory_address = router_contract.factory().call().await.unwrap();
        let factory_contract = UniswapV2Factory::new(factory_address, fprovider.clone());

        Self {
            provider: fprovider,
            router_caller: router_contract,
            factory_caller: factory_contract,
        }
    }

    pub async fn get_pairs_length(&self) -> Result<u128> {
        let all_pairs_length = self.factory_caller.allPairsLength().call().await?;
        let all_pairs_length_u128: u128 = all_pairs_length.try_into().unwrap();
        Ok(all_pairs_length_u128)
    }

    pub async fn get_pair_address(&self, pair_index_uint: Uint<256, 4>) -> Result<Address> {
        let pair_address = self.factory_caller.allPairs(pair_index_uint).call().await?;
        Ok(pair_address)
    }

    pub async fn get_pair_created(&self, from_block: u64, to_block: u64) -> Result<Vec<Log>> {
        //const UNISWAP_V2_FACTORY_ADDR: Address =
        //    address!("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
        let event_signature = keccak256(b"PairCreated(address,address,address,uint256)");

        let filter = Filter::new()
            .event_signature(event_signature)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.provider.get_logs(&filter).await?;
        Ok(logs)
    }

    pub async fn get_pair_liquidity(
        &self,
        pair_address: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<(Vec<Log>, Vec<Log>, Vec<Log>)> {
        let mint_event_signature = keccak256(b"Mint(address,uint256,uint256)");
        let burn_event_signature = keccak256(b"Burn(address,uint256,uint256,address)");
        let swap_event_signature =
            keccak256(b"Swap(address,uint256,uint256,uint256,uint256,address)");

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
    async fn test_uniswap_v2() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level : {:?}", log_level);
        info!("app_config.eth: {:#?}", app_config.eth);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();

        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(rpc_client.provider.clone(), router_addr).await;
        info!(
            "uniswap_v2 factory_caller: {:#?}",
            uniswap_v2.factory_caller
        );

        let pair_length = uniswap_v2.get_pairs_length().await.unwrap();
        info!("get_pairs_length: {:?}", pair_length);

        let latest_pair_index = pair_length.saturating_sub(1);
        let latest_pair_index_uint: Uint<256, 4> = Uint::from(latest_pair_index);
        let latest_pair_address = uniswap_v2
            .get_pair_address(latest_pair_index_uint)
            .await
            .unwrap();
        info!("get_pair_address(latest): {:?}", latest_pair_address);

        let from_block = 22770510;
        let to_block = 22770512;
        let pair_created_events = uniswap_v2
            .get_pair_created(from_block, to_block)
            .await
            .unwrap();

        info!(
            "Number of PairCreated events in block range {} - {}: {}",
            from_block,
            to_block,
            pair_created_events.len()
        );
        //info!("test_get_uniswap_v2_pair_created_events: {:#?}", events.first());
        info!("get_pair_created: {:#?}", pair_created_events);

        let pair_address = address!("0xaAF2fe003BB967EB7C35A391A2401e966bdB7F95");
        let from_block1 = 22828657;
        let to_block1 = 22828661;

        let (mint_logs, burn_logs, swap_logs) = uniswap_v2
            .get_pair_liquidity(pair_address, from_block1, to_block1)
            .await
            .unwrap();
        info!(
            "get_pair_liquidity pair_address:{} mint_logs: {:#?} burn_logs: {:#?} swap_logs: {:#?}",
            pair_address,
            mint_logs, burn_logs, swap_logs
        );
    }
}
