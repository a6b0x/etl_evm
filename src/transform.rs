use alloy::primitives::Address;
use alloy::rpc::types::eth::{Block, Log};
use chrono::{DateTime, Local, Utc};
use env_logger::fmt::Timestamp;
use eyre::{ContextCompat, Result};

#[derive(Debug)]
pub struct BlockV1 {
    pub block_number: u64,
    pub tx_count: usize,
    pub miner: String,
    pub date_time: DateTime<Local>,
}

pub fn transform_block(block: &Block) -> Result<BlockV1> {
    let block_number = block.header.number;
    let tx_count = block.transactions.len();
    let miner = block.header.beneficiary.to_string();

    let date_time = DateTime::<Utc>::from_timestamp(block.header.timestamp as i64, 0)
        .context("Failed to convert timestamp to DateTime")?;
    let local_date_time = date_time.with_timezone(&Local);

    Ok(BlockV1 {
        block_number,
        tx_count,
        miner,
        date_time: local_date_time,
    })
}
#[derive(Debug)]
pub struct PairCreatedEvent {
    pub fun_signature: String,
    pub token0: Address,
    pub token1: Address,
    pub block_number: u64,
    pub tx_hash: String,
    pub factory_address: Address,
    pub pair_address: Address,
    pub timestamp: u64,
}

pub fn transform_pair_created_event(logs: &[Log]) -> Result<Vec<PairCreatedEvent>> {
    let mut events = Vec::new();
    for log in logs {
        if log.topics().len() < 3 {
            return Err(eyre::eyre!("Invalid PairCreated event log topics length"));
        }
        let fun_signature = log.topics()[0].to_string();
        let token0 = Address::from_slice(&log.topics()[1][12..32]);
        let token1 = Address::from_slice(&log.topics()[2][12..32]);
        let block_number = log.block_number.unwrap();
        let tx_hash = log.transaction_hash.unwrap().to_string();
        let factory_address = log.address();
        let pair_address = Address::from_slice(&log.data().data[12..32]);
        let timestamp = log.block_timestamp.unwrap();

        events.push(PairCreatedEvent {
            fun_signature,
            token0,
            token1,
            block_number,
            tx_hash,
            factory_address,
            pair_address,
            timestamp,
        });
    }
    Ok(events)
}

#[derive(Debug)]
pub struct MintEvent {
    pub fun_signature: String,
    pub sender: Address,
    pub amount0: u128,
    pub amount1: u128,
    pub block_number: u64,
    pub tx_hash: String,
    pub timestamp: u64,
}

pub fn transform_mint_event(logs: &[Log]) -> Result<Vec<MintEvent>> {
    let mut events = Vec::new();
    for log in logs {
        if log.topics().len() < 2 {
            return Err(eyre::eyre!("Invalid Mint event log topics length"));
        }
        let fun_signature = log.topics()[0].to_string();
        let sender = Address::from_slice(&log.topics()[1][12..32]);

        let log_data = log.data().data.clone();
        if log_data.len() < 48 {
            return Err(eyre::eyre!(
                "Mint event log data length is less than 48 bytes"
            ));
        }
        let amount0 = u128::from_be_bytes(log_data[16..32].try_into().unwrap());
        let amount1 = u128::from_be_bytes(log_data[48..64].try_into().unwrap());

        let block_number = log.block_number.unwrap();
        let tx_hash = log.transaction_hash.unwrap().to_string();
        let timestamp = log.block_timestamp.unwrap();

        events.push(MintEvent {
            fun_signature,
            sender,
            amount0,
            amount1,
            block_number,
            tx_hash,
            timestamp,
        });
    }
    Ok(events)
}

#[derive(Debug)]
pub struct BurnEvent {
    pub fun_signature: String,
    pub sender: Address,
    pub address: Address,
    pub amount0: u128,
    pub amount1: u128,
    pub block_number: u64,
    pub tx_hash: String,
    pub timestamp: u64,
}

pub fn transform_burn_event(logs: &[Log]) -> Result<Vec<BurnEvent>> {
    let mut events = Vec::new();
    for log in logs {
        if log.topics().len() < 3 {
            return Err(eyre::eyre!("Invalid Burn event log topics length"));
        }
        let fun_signature = log.topics()[0].to_string();
        let sender = Address::from_slice(&log.topics()[1][12..32]);
        let address = Address::from_slice(&log.topics()[2][12..32]);

        let log_data = log.data().data.clone();
        if log_data.len() < 64 {
            return Err(eyre::eyre!(
                "Burn event log data length is less than 64 bytes"
            ));
        }
        let amount0 = u128::from_be_bytes(log_data[16..32].try_into().unwrap());
        let amount1 = u128::from_be_bytes(log_data[48..64].try_into().unwrap());

        let block_number = log.block_number.unwrap();
        let tx_hash = log.transaction_hash.unwrap().to_string();
        let timestamp = log.block_timestamp.unwrap();

        events.push(BurnEvent {
            fun_signature,
            sender,
            address,
            amount0,
            amount1,
            block_number,
            tx_hash,
            timestamp,
        });
    }
    Ok(events)
}

#[derive(Debug)]
pub struct SwapEvent {
    pub fun_signature: String,
    pub sender: Address,
    pub address: Address,
    pub amount0_in: u128,
    pub amount1_in: u128,
    pub amount0_out: u128,
    pub amount1_out: u128,
    pub block_number: u64,
    pub tx_hash: String,
    pub timestamp: u64,
}

pub fn transform_swap_event(logs: &[Log]) -> Result<Vec<SwapEvent>> {
    let mut events = Vec::new();
    for log in logs {
        if log.topics().len() < 3 {
            return Err(eyre::eyre!("Invalid Swap event log topics length"));
        }
        let fun_signature = log.topics()[0].to_string();
        let sender = Address::from_slice(&log.topics()[1][12..32]);
        let address = Address::from_slice(&log.topics()[2][12..32]);

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

        let block_number = log.block_number.unwrap();
        let tx_hash = log.transaction_hash.unwrap().to_string();
        let timestamp = log.block_timestamp.unwrap();

        events.push(SwapEvent {
            fun_signature,
            sender,
            address,
            amount0_in,
            amount1_in,
            amount0_out,
            amount1_out,
            block_number,
            tx_hash,
            timestamp,
        });
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{extract::RpcClient, extract::UniswapV2, init::AppConfig};
    use alloy::primitives::address;
    use log::info;

    #[tokio::test]
    async fn test_transform_block() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();
        let new_block_number = rpc_client.get_new_block_number().await.unwrap();
        info!("get_new_block_number: {:?}", new_block_number);

        let new_block_data = rpc_client.get_block_data(new_block_number).await.unwrap();
        if let Some(block) = new_block_data.as_ref() {
            //info!("get_block_data Block.Header: {:#?}", block.header);
            info!(
                "get_block_data Block.Header number: {:?} timestamp: {:?} transactions.len: {:?} beneficiary: {:?}",
                block.header.number,
                block.header.timestamp,
                block.transactions.len(),
                block.header.beneficiary
            );
            let transformed_block = transform_block(block).unwrap();
            info!("transformed_block: {:#?}", transformed_block);
            info!(
                "get_block_data Block.FirstTransaction: {:#?}",
                block.transactions.first_transaction()
            );
        }
    }
    #[tokio::test]
    async fn test_transform_pair_created_event() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();
        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(rpc_client.provider.clone(), router_addr).await;
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
    async fn test_transform_mbs_event() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();
        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(rpc_client.provider.clone(), router_addr).await;
        info!(
            "uniswap_v2 factory_caller: {:#?}",
            uniswap_v2.factory_caller
        );

        let pair_address = address!("0xaAF2fe003BB967EB7C35A391A2401e966bdB7F95");
        let from_block1 = 22828657;
        let to_block1 = 22828661;
        let (mint_logs, burn_logs, swap_logs) = uniswap_v2
            .get_pair_liquidity(pair_address, from_block1, to_block1)
            .await
            .unwrap();
        info!(
            "get_pair_liquidity pair_address: {} 
            mint_logs: {:#?} burn_logs: {:#?} swap_logs: {:#?}",
            pair_address, mint_logs, burn_logs, swap_logs
        );

        let mint_events = transform_mint_event(&mint_logs).unwrap();
        info!("mint_events: {:#?}", mint_events);

        let burn_events = transform_burn_event(&burn_logs).unwrap();
        info!("burn_events: {:#?}", burn_events);

        let swap_events = transform_swap_event(&swap_logs).unwrap();
        info!("swap_events: {:#?}", swap_events);
    }
}
