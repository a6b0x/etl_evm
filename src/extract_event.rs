use alloy::primitives::{address, keccak256, Address, Uint};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::{eth::Block, Filter, Log};
use alloy::sol;
use alloy::sol_types::{SolEvent, SolValue};
use eyre::Result;
use futures_util::future::ok;
use futures_util::StreamExt;
use std::collections::HashMap;

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    UniswapV2Router,
    "data/UniswapV2Router02.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    #[derive(Debug)]
    UniswapV2Factory,
    "data/UniswapV2Factory.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    #[derive(Debug)]
    UniswapV2Pair,
    "data/UniswapV2Pair.json"
);

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    ERC20Token,
    "data/ERC20.json"
);

sol! {
    #[sol(rpc)]
    contract UniswapV2Pool {
        function allPairsLength() external view returns (uint);
    }
}

sol!(
    #[allow(missing_docs)]
    #[sol(rpc)]
    UniswapV2PairList,
    "data/UniswapV2PairList.json"
);

pub struct UniswapV2 {
    pub provider: DynProvider,
    pub router_caller: UniswapV2Router::UniswapV2RouterInstance<DynProvider>,
    pub factory_caller: UniswapV2Factory::UniswapV2FactoryInstance<DynProvider>,
}
#[derive(Debug)]
pub struct UniswapV2Tokens {
    pub provider: DynProvider,
    pub pair_caller: UniswapV2Pair::UniswapV2PairInstance<DynProvider>,
    pub pair_address: Address,
    pub token0_caller: ERC20Token::ERC20TokenInstance<DynProvider>,
    pub token1_caller: ERC20Token::ERC20TokenInstance<DynProvider>,
    pub token0_address: Address,
    pub token0_decimals: u8,
    pub token0_symbol: String,
    pub token1_address: Address,
    pub token1_decimals: u8,
    pub token1_symbol: String,
    pub block_number: Option<u64>,
}

impl UniswapV2 {
    pub async fn new(provider: DynProvider, router_address: Address) -> Self {
        let router_contract = UniswapV2Router::new(router_address, provider.clone());

        let factory_address = router_contract.factory().call().await.unwrap();
        let factory_contract = UniswapV2Factory::new(factory_address, provider.clone());

        Self {
            provider,
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
        // let filter = self
        //     .factory_caller
        //     .PairCreated_filter()
        //     .topic3(pair_address);
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

    pub async fn get_token_pair(&self, token_a: Address, token_b: Address) -> Result<Address> {
        let (token0, token1) = if token_a < token_b {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        };

        let pair_address = self.factory_caller.getPair(token0, token1).call().await?;

        if pair_address == Address::ZERO {
            return Err(eyre::eyre!("Pair not found for given tokens"));
        }

        Ok(pair_address)
    }
    pub async fn get_token_first_block(
        &self,
        token0_address: Address,
        token1_address: Address,
        from_block: u64,
        to_block: u64,
    ) -> Result<(u64, u64)> {
        let filter = self
            .factory_caller
            .PairCreated_filter()
            .topic1(token0_address)
            .topic2(token1_address)
            .from_block(from_block)
            .to_block(to_block);

        let logs = filter.query().await?;
        let (_, log) = logs
            .into_iter()
            .next()
            .ok_or_else(|| eyre::eyre!("PairCreated log not found"))?;
        let block_number = log
            .block_number
            .ok_or_else(|| eyre::eyre!("Missing block number in log"))?;
        let block_timestamp = log
            .block_timestamp
            .ok_or_else(|| eyre::eyre!("Missing timestamp in log"))?;

        Ok((block_number, block_timestamp))
    }

    pub async fn get_all_pair_len(&self, token_address: Address) -> Result<u128> {
        let token_caller = UniswapV2Pool::new(token_address, self.provider.clone());
        let balance = token_caller.allPairsLength().call().await?;
        Ok(balance
            .try_into()
            .map_err(|e| eyre::eyre!("Conversion error: {}", e))?)
    }

    pub async fn get_pair_list(&self, from_index: u64, list_size: usize) -> Result<Vec<Address>> {
        let deployer = UniswapV2PairList::deploy_builder(
            self.provider.clone(),
            Uint::from(from_index),
            Uint::from(list_size as u64),
            *self.factory_caller.address(),
        );
        let res = deployer
            .call_raw()
            .await
            .map_err(|e| eyre::eyre!("Failed to call UniswapV2PairList: {}", e))?;

        let res_data = <Vec<Address> as SolValue>::abi_decode(&res)?;
        Ok(res_data)
    }
}

impl UniswapV2Tokens {
    pub async fn new(pair_address: Address, provider: DynProvider) -> Result<Self> {
        let pair_caller = UniswapV2Pair::new(pair_address, provider.clone());
        let token0_address = pair_caller.token0().call().await?;
        let token1_address = pair_caller.token1().call().await?;

        let token0_caller = ERC20Token::new(token0_address, provider.clone());
        let token0_decimals = token0_caller.decimals().call().await?;
        let token1_caller = ERC20Token::new(token1_address, provider.clone());
        let token1_decimals = token1_caller.decimals().call().await?;

        let token0_symbol = token0_caller.symbol().call().await?;
        let token1_symbol = token1_caller.symbol().call().await?;

        Ok(Self {
            provider,
            pair_caller,
            pair_address,
            token0_caller,
            token0_address,
            token0_decimals,
            token0_symbol,
            token1_caller,
            token1_address,
            token1_decimals,
            token1_symbol,
            block_number: None,
        })
    }

    pub async fn get_price(&self) -> Result<(f64, f64, u32)> {
        let reserves = self.pair_caller.getReserves().call().await?;
        let reserve0 = reserves._reserve0.to::<u128>();
        let reserve1 = reserves._reserve1.to::<u128>();
        let block_timestamp = reserves._blockTimestampLast;

        if reserve0 == 0 || reserve1 == 0 {
            return Err(eyre::eyre!("Insufficient reserves"));
        }

        let price0 = (reserve1 as f64 / 10f64.powi(self.token1_decimals as i32))
            / (reserve0 as f64 / 10f64.powi(self.token0_decimals as i32));

        let price1 = 1.0 / price0;

        Ok((price0, price1, block_timestamp))
    }

    pub async fn subscribe_swap_event(&self) -> Result<impl StreamExt<Item = Log>> {
        let swap_event_signature =
            keccak256(b"Swap(address,uint256,uint256,uint256,uint256,address)");
        let filter = Filter::new()
            .event_signature(swap_event_signature)
            .address(self.pair_address);
        let sub = self.provider.subscribe_logs(&filter).await?;
        Ok(sub.into_stream())
    }

    pub async fn get_swap_event(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<(UniswapV2Pair::Swap, Log)>> {
        let filter = self
            .pair_caller
            .Swap_filter()
            .address(self.pair_address)
            .from_block(from_block)
            .to_block(to_block);

        filter
            .query()
            .await
            .map_err(|e| eyre::eyre!("get_swap_event error: {}", e))
    }

    pub async fn get_burn_event(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<(UniswapV2Pair::Burn, Log)>> {
        let filter = self
            .pair_caller
            .Burn_filter()
            .address(self.pair_address)
            .from_block(from_block)
            .to_block(to_block);

        filter
            .query()
            .await
            .map_err(|e| eyre::eyre!("get_burn_event error: {}", e))
    }

    pub async fn get_mint_event(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<(UniswapV2Pair::Mint, Log)>> {
        let filter = self
            .pair_caller
            .Mint_filter()
            .address(self.pair_address)
            .from_block(from_block)
            .to_block(to_block);

        filter
            .query()
            .await
            .map_err(|e| eyre::eyre!("get_mint_event error: {}", e))
    }

    pub async fn get_all_event(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<HashMap<String, Vec<Log>>> {
        // let mint_event_signature = keccak256(b"Mint(address,uint256,uint256)");
        // let burn_event_signature = keccak256(b"Burn(address,uint256,uint256,address)");
        // let swap_event_signature =
        //     keccak256(b"Swap(address,uint256,uint256,uint256,uint256,address)");
        let mint_event_signature = UniswapV2Pair::Mint::SIGNATURE_HASH;
        let burn_event_signature = UniswapV2Pair::Burn::SIGNATURE_HASH;
        let swap_event_signature = UniswapV2Pair::Swap::SIGNATURE_HASH;
        let filter = Filter::new()
            .event_signature(vec![
                mint_event_signature.into(),
                burn_event_signature.into(),
                swap_event_signature.into(),
            ])
            .address(self.pair_address)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self.provider.get_logs(&filter).await?;
        let mut topic_log = HashMap::<String, Vec<Log>>::new();
        for log in logs {
            if log.topics().len() < 1 {
                continue;
            }
            let event_signature = log.topics()[0];
            let event_name = match event_signature {
                sig if sig == mint_event_signature => "Mint",
                sig if sig == burn_event_signature => "Burn",
                sig if sig == swap_event_signature => "Swap",
                _ => continue,
            };
            topic_log
                .entry(event_name.to_string())
                .or_default()
                .push(log);
        }
        Ok(topic_log)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract_block::EvmBlock;
    use crate::init::AppConfig;
    use log::info;

    #[tokio::test]
    async fn test_uniswap_v2() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let evm_block = EvmBlock::new(&app_config.eth.http_url).await.unwrap();

        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(evm_block.provider.clone(), router_addr).await;
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

        let pair_list = uniswap_v2.get_pair_list(400000, 10).await.unwrap();
        info!("get_pair_list: {:?}", pair_list);

    }

    #[tokio::test]
    async fn test_uniswap_v2_tokens() -> Result<()> {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let evm_block = EvmBlock::new(&app_config.eth.ws_url).await?;
        let pair_address = address!("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
        let uniswap_v2_tokens = UniswapV2Tokens::new(pair_address, evm_block.provider.clone())
            .await
            .unwrap();
        info!("uniswap_v2_tokens: {:#?}", uniswap_v2_tokens);

        let from_block = 10008555;
        let to_block = 10008566;

        let swap_even_log = uniswap_v2_tokens
            .get_swap_event(from_block, to_block)
            .await?;
        info!("get_swap_event: {:#?}", swap_even_log);

        let burn_even_log = uniswap_v2_tokens
            .get_burn_event(from_block, to_block)
            .await?;
        info!("get_burn_event: {:#?}", burn_even_log);

        let mint_even_log = uniswap_v2_tokens
            .get_mint_event(from_block, to_block)
            .await?;
        info!("get_mint_event: {:#?}", mint_even_log);

        let all_event_log = uniswap_v2_tokens
            .get_all_event(from_block, to_block)
            .await?;
        info!("get_all_event: {:#?}", all_event_log);

        // let mut stream = uniswap_v2_tokens.subscribe_swap_event().await?;
        // while let Some(log) = stream.next().await {
        //     info!("Received log: {:#?}", log);
        // }
        Ok(())
    }
}
