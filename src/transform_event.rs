use alloy::primitives::Address;
use alloy::rpc::types::eth::Log;
use chrono::{DateTime, Utc};
use eyre::Result;

#[derive(Debug, serde::Serialize)]
pub struct PairCreatedEvent {
    pub event_type: String,
    pub function_signature: String,
    pub token0_address: Address,
    pub token1_address: Address,
    pub block_number: u64,
    pub transaction_hash: String,
    pub factory_address: Address,
    pub pair_address: Address,
    #[serde(serialize_with = "serialize_timestamp")]
    pub block_timestamp: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct MintEvent {
    pub event_type: String,
    pub function_signature: String,
    pub caller_address: Address,
    pub pair_address: Address,
    pub token0_amount: u128,
    pub token1_amount: u128,
    pub block_number: u64,
    pub transaction_hash: String,
    #[serde(serialize_with = "serialize_timestamp")]
    pub block_timestamp: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct BurnEvent {
    pub event_type: String,
    pub function_signature: String,
    pub caller_address: Address,
    pub pair_address: Address,
    pub address: Address,
    pub token0_amount: u128,
    pub token1_amount: u128,
    pub block_number: u64,
    pub transaction_hash: String,
    #[serde(serialize_with = "serialize_timestamp")]
    pub block_timestamp: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct SwapEvent {
    pub event_type: String,
    pub function_signature: String,
    pub caller_address: Address,
    pub pair_address: Address,
    pub receiver_address: Address,
    pub token0_amount: u128,
    pub token1_amount: u128,
    pub token0_amounts: f64,
    pub token1_amounts: f64,
    pub token0_token1: f64,
    pub token1_token0: f64,
    pub block_number: u64,
    pub transaction_hash: String,
    #[serde(serialize_with = "serialize_timestamp")]
    pub block_timestamp: u64,
}

fn serialize_timestamp<S>(timestamp: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let timestamp = DateTime::<Utc>::from_timestamp(*timestamp as i64, 0).unwrap();
    let formatted = timestamp.format("%Y-%m-%d %H:%M:%S");
    serializer.collect_str(&formatted)
}
impl PairCreatedEvent {
    pub fn to_influx_line(&self) -> String {
        format!(
                "create_event,transaction_hash={},event_type={},factory_address={},pair_adress={},token0={},token1={} block_number={} {}",
                self.transaction_hash,
                self.event_type,
                self.factory_address,
                self.pair_address,
                self.token0_address,
                self.token1_address,
                self.block_number,
                self.block_timestamp
            )
    }
}

impl MintEvent {
    pub fn to_influx_line(&self) -> String {
        format!(
                "mint_event,transaction_hash={},event_type={},caller_address={},pair_address={} amount0={},amount1={},block_number={} {}",
                self.transaction_hash,
                self.event_type,
                self.caller_address,
                self.pair_address,
                self.token0_amount,
                self.token1_amount,
                self.block_number,
                self.block_timestamp
            )
    }
}

impl BurnEvent {
    pub fn to_influx_line(&self) -> String {
        format!(
                "burn_event,transaction_hash={},event_type={},caller_address={},pair_address={} amount0={},amount1={},block_number={} {}",
                self.transaction_hash,
                self.event_type,
                self.caller_address,
                self.pair_address,
                self.token0_amount,
                self.token1_amount,
                self.block_number,
                self.block_timestamp
            )
    }
}

impl SwapEvent {
    pub fn to_influx_line(&self) -> String {
        format!("swap_event,pair_address={},caller_address={},receiver_address={},transaction_hash={} \
                token0_amount={},token1_amount={},token0_amounts={},token1_amounts={},token0_token1={},token1_token0={},block_number={} {}",
                self.pair_address,
                self.caller_address,
                self.receiver_address,
                self.transaction_hash,
                self.token0_amount,
                self.token1_amount,
                self.token0_amounts,
                self.token1_amounts,
                self.token0_token1,
                self.token1_token0,
                self.block_number,
                self.block_timestamp
            )
    }
}

pub fn transform_pair_created_event(logs: &[Log]) -> Result<Vec<PairCreatedEvent>> {
    let mut events = Vec::new();
    for log in logs {
        if log.topics().len() < 3 {
            return Err(eyre::eyre!("Invalid PairCreated event log topics length"));
        }
        let function_signature = log.topics()[0].to_string();
        let token0_address = Address::from_slice(&log.topics()[1][12..32]);
        let token1_address = Address::from_slice(&log.topics()[2][12..32]);
        let block_number = log.block_number.unwrap();
        let transaction_hash = log.transaction_hash.unwrap().to_string();
        let factory_address = log.address();
        let pair_address = Address::from_slice(&log.data().data[12..32]);
        let block_timestamp = log.block_timestamp.unwrap();

        events.push(PairCreatedEvent {
            event_type: "PairCreated".to_string(),
            function_signature,
            token0_address,
            token1_address,
            block_number,
            transaction_hash,
            factory_address,
            pair_address,
            block_timestamp,
        });
    }
    Ok(events)
}

pub fn transform_mint_event(logs: &[Log]) -> Result<Vec<MintEvent>> {
    let mut events = Vec::new();
    for log in logs {
        if log.topics().len() < 2 {
            return Err(eyre::eyre!("Invalid Mint event log topics length"));
        }
        let function_signature = log.topics()[0].to_string();
        let caller_address = Address::from_slice(&log.topics()[1][12..32]);
        let pair_address = log.address();

        let log_data = log.data().data.clone();
        if log_data.len() < 48 {
            return Err(eyre::eyre!(
                "Mint event log data length is less than 48 bytes"
            ));
        }
        let token0_amount = u128::from_be_bytes(log_data[16..32].try_into().unwrap());
        let token1_amount = u128::from_be_bytes(log_data[48..64].try_into().unwrap());

        let block_number = log.block_number.unwrap();
        let transaction_hash = log.transaction_hash.unwrap().to_string();
        let block_timestamp = log.block_timestamp.unwrap();

        events.push(MintEvent {
            event_type: "Mint".to_string(),
            function_signature,
            caller_address,
            pair_address,
            token0_amount,
            token1_amount,
            block_number,
            transaction_hash,
            block_timestamp,
        });
    }
    Ok(events)
}

pub fn transform_burn_event(logs: &[Log]) -> Result<Vec<BurnEvent>> {
    let mut events = Vec::new();
    for log in logs {
        if log.topics().len() < 3 {
            return Err(eyre::eyre!("Invalid Burn event log topics length"));
        }
        let function_signature = log.topics()[0].to_string();
        let caller_address = Address::from_slice(&log.topics()[1][12..32]);
        let address = Address::from_slice(&log.topics()[2][12..32]);
        let pair_address = log.address();

        let log_data = log.data().data.clone();
        if log_data.len() < 64 {
            return Err(eyre::eyre!(
                "Burn event log data length is less than 64 bytes"
            ));
        }
        let token0_amount = u128::from_be_bytes(log_data[16..32].try_into().unwrap());
        let token1_amount = u128::from_be_bytes(log_data[48..64].try_into().unwrap());

        let block_number = log.block_number.unwrap();
        let transaction_hash = log.transaction_hash.unwrap().to_string();
        let block_timestamp = log.block_timestamp.unwrap();

        events.push(BurnEvent {
            event_type: "Burn".to_string(),
            function_signature,
            caller_address,
            pair_address,
            address,
            token0_amount,
            token1_amount,
            block_number,
            transaction_hash,
            block_timestamp,
        });
    }
    Ok(events)
}

pub fn transform_swap_event(
    logs: &[Log],
    token0_decimals: u8,
    token1_decimals: u8,
) -> Result<Vec<SwapEvent>> {
    let mut events = Vec::new();

    for log in logs {
        if log.topics().len() < 3 {
            return Err(eyre::eyre!("Invalid Swap event log topics length"));
        }
        let function_signature = log.topics()[0].to_string();
        let caller_address = Address::from_slice(&log.topics()[1][12..32]);
        let receiver_address = Address::from_slice(&log.topics()[2][12..32]);
        let pair_address = log.address();

        let log_data = log.data().data.clone();
        if log_data.len() < 128 {
            return Err(eyre::eyre!(
                "Swap event log data length is less than 128 bytes"
            ));
        }
        let amount0_in = u128::from_be_bytes(log_data[16..32].try_into().unwrap());
        let amount1_in = u128::from_be_bytes(log_data[48..64].try_into().unwrap());
        let amount0_out = u128::from_be_bytes(log_data[80..96].try_into().unwrap());
        let amount1_out = u128::from_be_bytes(log_data[112..128].try_into().unwrap());
        let token0_amount = amount0_in + amount0_out;
        let token1_amount = amount1_in + amount1_out;
        let token0_amounts = token0_amount as f64 / 10f64.powi(token0_decimals as i32);
        let token1_amounts = token1_amount as f64 / 10f64.powi(token1_decimals as i32);
        let token0_token1 = token0_amounts / token1_amounts;
        let token1_token0 = token1_amounts / token0_amounts;

        let block_number = log.block_number.unwrap();
        let transaction_hash = log.transaction_hash.unwrap().to_string();
        let block_timestamp = log.block_timestamp.unwrap();

        events.push(SwapEvent {
            event_type: "Swap".to_string(),
            function_signature,
            caller_address,
            pair_address,
            receiver_address,
            token0_amount,
            token1_amount,
            token0_amounts,
            token1_amounts,
            token0_token1,
            token1_token0,
            block_number,
            transaction_hash,
            block_timestamp,
        });
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        extract_block::EvmBlock,
        extract_event::{UniswapV2, UniswapV2Tokens},
        init::AppConfig,
    };
    use alloy::primitives::address;
    use chrono::{DateTime, Local, Utc};
    use log::info;

    #[tokio::test]
    async fn test_transform_pair() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let evm_block = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(evm_block.provider.clone(), router_addr).await;
        info!(
            "uniswap_v2 factory_caller: {:#?}",
            uniswap_v2.factory_caller
        );

        let from_block = 22770510;
        let to_block = 22770512;
        let pair_created_events = uniswap_v2
            .get_pair_created(from_block, to_block)
            .await
            .unwrap();
        info!("pair_created_events: {:#?}", pair_created_events);
        let transformed_events = transform_pair_created_event(&pair_created_events).unwrap();
        info!("transformed_events: {:#?}", transformed_events);
    }

    #[tokio::test]
    async fn test_transform_pair_event() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let evm_block = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(evm_block.provider.clone(), router_addr).await;
        info!(
            "uniswap_v2 factory_caller: {:#?}",
            uniswap_v2.factory_caller
        );

        let weth_usdc_pair = address!("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
        let uniswap_v2_tokens = UniswapV2Tokens::new(weth_usdc_pair, evm_block.provider)
            .await
            .unwrap();
        info!("uniswap_v2_tokens: {:#?}", uniswap_v2_tokens);

        let (price0, price1, timestamp) = uniswap_v2_tokens.get_price().await.unwrap();
        let date_time = DateTime::<Utc>::from_timestamp(timestamp as i64, 0).unwrap();
        let local_date_time = date_time.with_timezone(&Local);
        info!("price0: {:?} ", price0);
        info!("price1: {:?} ", price1);
        info!("local_date_time: {:?} ", local_date_time);

        let weth_addr = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let botto_addr = address!("0x9DFAD1b7102D46b1b197b90095B5c4E9f5845BBA");
        let pair_addr = uniswap_v2
            .get_token_pair(weth_addr, botto_addr)
            .await
            .unwrap();
        info!("get_token_pair: {:?}", pair_addr);

        let token0_addr = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let token1_addr = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let from_block = 10008350;
        let to_block = 10008360;
        let (first_block, block_timestamp) = uniswap_v2
            .get_token_first_block(token0_addr, token1_addr, from_block, to_block)
            .await
            .unwrap();
        info!("first_block: {:?} ", first_block);

        let from_block1 = 22921717;
        let to_block1 = 22921721;
        let all_event = uniswap_v2_tokens
            .get_all_event(from_block1, to_block1)
            .await
            .unwrap();

        let mint_events = transform_mint_event(all_event.get("Mint").unwrap_or(&vec![])).unwrap();
        let burn_events = transform_burn_event(all_event.get("Burn").unwrap_or(&vec![])).unwrap();
        let swap_events = transform_swap_event(
            all_event.get("Swap").unwrap_or(&vec![]),
            uniswap_v2_tokens.token0_decimals,
            uniswap_v2_tokens.token1_decimals,
        )
        .unwrap();

        info!("All mint events: {:#?}", mint_events);
        info!("All burn events: {:#?}", burn_events);
        info!("All swap events: {:#?}", swap_events);
    }
}
