use alloy::primitives::{Address, Uint, address, keccak256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{Filter, Log, eth::Block};
use alloy::sol;
use eyre::Result;

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

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    ERC20Token,
    "data/ERC20.json" 
);

pub struct UniswapV2 {
    pub rpc_client: DynProvider,
    pub router_caller: UniswapV2Router::UniswapV2RouterInstance<DynProvider>,
    pub factory_caller: UniswapV2Factory::UniswapV2FactoryInstance<DynProvider>,
}

impl UniswapV2 {
    pub async fn new(rpc_client: DynProvider, router_address: Address) -> Self {
        let router_contract = UniswapV2Router::new(router_address, rpc_client.clone());

        let factory_address = router_contract.factory().call().await.unwrap();
        let factory_contract = UniswapV2Factory::new(factory_address, rpc_client.clone());

        Self {
            rpc_client,
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

        let logs = self.rpc_client.get_logs(&filter).await?;
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

        let mint_logs = self.rpc_client.get_logs(&mint_filter).await?;
        let burn_logs = self.rpc_client.get_logs(&burn_filter).await?;
        let swap_logs = self.rpc_client.get_logs(&swap_filter).await?;

        Ok((mint_logs, burn_logs, swap_logs))
    }

    pub async fn get_token_symbol(&self, token_address: Address) -> Result<String> {
        let token_contract = ERC20Token::new(token_address, self.rpc_client.clone());
        let token_symbol = token_contract.symbol().call().await?;

        Ok(token_symbol)
    }

    pub async fn get_tokens_address(&self, pair_address: Address) -> Result<(Address, Address)> {
        let token_contract = UniswapV2Pair::new(pair_address, self.rpc_client.clone());
        let token0_address = token_contract.token0().call().await?;
        let token1_address= token_contract.token1().call().await?;

        Ok((token0_address, token1_address))
    }

    pub async fn get_token_decimals(&self, token_address: Address) -> Result<u8> {
        let token_contract = ERC20Token::new(token_address, self.rpc_client.clone());
        let token_decimals = token_contract.decimals().call().await?;

        Ok(token_decimals)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::AppConfig;
    use log::info;
    use crate::extract_block::EvmBlock;

    #[tokio::test]
    async fn test_uniswap_v2() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let evm_block = EvmBlock::new(&app_config.eth.http_url).await.unwrap();

        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(evm_block.rpc_client.clone(), router_addr).await;
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

        let token_addr = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let token_name = uniswap_v2.get_token_symbol(token_addr).await.unwrap();
        info!("get_token_symbol: {:?}", token_name);
    }
}
