use csv::Writer;
use eyre::{Context, Result};
use serde::Serialize;
use std::fs::File;
pub struct CsvFile {
    writer: Writer<File>,
}
#[derive(Debug, Serialize)]
pub struct TableBlock {
    pub block_number: u64,
    pub tx_count: usize,
    pub miner: String,
    pub date_time: String,
}
#[derive(Debug, Serialize)]
pub struct UniswapV2Event {
    pub block_time: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub event_type: String,
    pub from_address: String,
    pub to_address: String,
    pub token0: String,
    pub token1: String,
    pub amount0: String,
    pub amount1: String,
}

impl CsvFile {
    pub fn new(filename: &str) -> Result<Self> {
        //let file = File::create(filename).context("Failed to create file")?;
        let file = File::options()
            .create(true)
            .append(true)
            .open(filename)
            .context("Failed to open file")?;
        let mut writer = Writer::from_writer(file);
        //writer.write_record(&["block_number","tx_count","miner","date_time"])
        //    .context("Failed to write record header")?;
        Ok(Self { writer })
    }

    pub fn write_block(&mut self, block: &TableBlock) -> Result<()> {
        self.writer
            .serialize(block)
            .context("Failed to write block data")
    }

    pub fn write_event(&mut self, events: &[UniswapV2Event]) -> Result<()> {
        for event in events {
            self.writer
                .serialize(event)
                .context("Failed to write event data")?;
        }
        Ok(())
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use crate::{
        extract::RpcClient, extract::UniswapV2, init::AppConfig, transform::transform_block,
        transform::transform_burn_event, transform::transform_mint_event,
        transform::transform_pair_created_event, transform::transform_swap_event,
    };
    use alloy::primitives::address;
    use eyre::{Ok, Result};
    use log::info;
    use std::io::Write;

    #[test]
    fn test_load() {
        let mut csv_file = CsvFile::new("data/test.csv").unwrap();
        let block = TableBlock {
            block_number: 1,
            tx_count: 1,
            miner: "0x1234567890123456789012345678901234567890".to_string(),
            date_time: "2023-01-01 00:00:00".to_string(),
        };
        csv_file.write_block(&block).unwrap();
    }

    #[tokio::test]
    async fn test_load_block() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();
        let new_block_number = rpc_client.get_new_block_number().await.unwrap();
        info!("get_new_block_number: {:?}", new_block_number);

        let mut csv_file = CsvFile::new("data/block.csv").unwrap();

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

            let block = TableBlock {
                block_number: transformed_block.block_number,
                tx_count: transformed_block.tx_count,
                miner: transformed_block.miner,
                date_time: transformed_block.date_time.to_string(),
            };
            csv_file.write_block(&block).unwrap();
        }
    }

    #[tokio::test]
    async fn test_load_event() {
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

        let from_block = 22828657;
        let to_block = 22828661;
        let pair_created_events = uniswap_v2
            .get_pair_created(from_block, to_block)
            .await
            .unwrap();
        info!("pair_created_events: {:#?}", pair_created_events);

        let pair_created_event = transform_pair_created_event(&pair_created_events).unwrap();
        info!("pair_created_event: {:#?}", pair_created_event);

        let mut csv_file = CsvFile::new("data/event.csv").unwrap();
        let pair_created_events = pair_created_event
            .iter()
            .map(|event| UniswapV2Event {
                block_time: event.date_time.to_string(),
                block_number: event.block_number,
                transaction_hash: event.tx_hash.clone(),
                event_type: "PairCreated".to_string(),
                from_address: event.factory_address.to_string(),
                to_address: event.pair_address.to_string(),
                token0: event.token0.to_string(),
                token1: event.token1.to_string(),
                amount0: 0.to_string(),
                amount1: 0.to_string(),
            })
            .collect::<Vec<_>>();
        csv_file.write_event(&pair_created_events).unwrap();

        let pair_address = address!("0xaAF2fe003BB967EB7C35A391A2401e966bdB7F95");
        let (mint_logs, burn_logs, swap_logs) = uniswap_v2
            .get_pair_liquidity(pair_address, from_block, to_block)
            .await
            .unwrap();
        info!(
            "get_pair_liquidity pair_address: {} 
            mint_logs: {:#?} burn_logs: {:#?} swap_logs: {:#?}",
            pair_address, mint_logs, burn_logs, swap_logs
        );

        let mint_events = transform_mint_event(&mint_logs).unwrap();
        info!("mint_events: {:#?}", mint_events);
        let mint_uniswap_events = mint_events
            .iter()
            .map(|event| UniswapV2Event {
                block_time: event.date_time.to_string(),
                block_number: event.block_number,
                transaction_hash: event.tx_hash.clone(),
                event_type: "Mint".to_string(),
                from_address: event.sender.to_string(),
                to_address:pair_address.to_string(),
                token0: "null".to_string(),
                token1: "null".to_string(),
                amount0: event.amount0.to_string(),
                amount1: event.amount1.to_string(),
            })
            .collect::<Vec<_>>();
        csv_file.write_event(&mint_uniswap_events).unwrap();

        let burn_events = transform_burn_event(&burn_logs).unwrap();
        info!("burn_events: {:#?}", burn_events);
        let burn_uniswap_events = burn_events
            .iter()
            .map(|event| UniswapV2Event {
                block_time: event.date_time.to_string(),
                block_number: event.block_number,
                transaction_hash: event.tx_hash.clone(),
                event_type: "Burn".to_string(),
                from_address: event.sender.to_string(),
                to_address: event.address.to_string(),
                token0: "null".to_string(),
                token1: "null".to_string(),
                amount0: event.amount0.to_string(),
                amount1: event.amount1.to_string(),
            })
            .collect::<Vec<_>>();
        csv_file.write_event(&burn_uniswap_events).unwrap();

        let swap_events = transform_swap_event(&swap_logs).unwrap();
        info!("swap_events: {:#?}", swap_events);
        let swap_uniswap_events = swap_events
            .iter()
            .map(|event| {
                let total_amount0 = event.amount0_in + event.amount0_out;
                let total_amount1 = event.amount1_in + event.amount1_out;
                UniswapV2Event {
                    block_time: event.date_time.to_string(),
                    block_number: event.block_number,
                    transaction_hash: event.tx_hash.clone(),
                    event_type: "Swap".to_string(),
                    from_address: event.sender.to_string(),
                    to_address: event.address.to_string(),
                    token0: "null".to_string(),
                    token1: "null".to_string(),
                    amount0: total_amount0.to_string(),
                    amount1: total_amount1.to_string(),
                }
            })
            .collect::<Vec<_>>();
        csv_file.write_event(&swap_uniswap_events).unwrap();
    }
}
