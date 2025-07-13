use csv::Writer;
use eyre::{Context, Ok, Result};
use serde::Serialize;
use std::fs::File;

pub struct CsvFile {
    writer: Writer<File>,
}

#[derive(Debug, Serialize)]
pub struct UniswapV2Pair {
    pub block_time: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub event_type: String,
    pub factory_address: String,
    pub pair_adress: String,
    pub token0: String,
    pub token1: String,
}

#[derive(Debug, Serialize)]
pub struct UniswapV2PairEvent {
    pub block_time: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub event_type: String,
    pub route_address: String,
    pub pair_address: String,
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

    pub fn write_pair(&mut self, events: &[UniswapV2Pair]) -> Result<()> {
        for event in events {
            self.writer
                .serialize(event)
                .context("Failed to write event data")?;
        }
        Ok(())
    }
    pub fn write_pair_event(&mut self, events: &[UniswapV2PairEvent]) -> Result<()> {
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
        extract::RpcClient, extract::UniswapV2, init::AppConfig,
        transform::transform_burn_event, transform::transform_mint_event,
        transform::transform_pair_created_event, transform::transform_swap_event,
    };
    use alloy::primitives::{address, Address};
    use chrono::{DateTime, Local, Utc};
    use eyre::{ContextCompat, Result};
    use log::info;
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    use reqwest::Client;

    #[tokio::test]
    async fn test_load_uniswapv2() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let rpc_client = RpcClient::new(&app_config.eth.http_url).unwrap();
        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(rpc_client.provider.clone(), router_addr).await;
        info!(
            "uniswap_v2 factory_caller: {:#?}",
            uniswap_v2.factory_caller
        );

        let from_block = 22828657;
        let to_block = 22828691;
        //let to_block = 22829000;
        let pair_created_events = uniswap_v2
            .get_pair_created(from_block, to_block)
            .await
            .unwrap();
        info!("pair_created_events: {:#?}", pair_created_events);

        let pair_created_event = transform_pair_created_event(&pair_created_events).unwrap();
        info!("pair_created_event: {:#?}", pair_created_event);

        let mut csv_file = CsvFile::new("data/pair.csv").unwrap();
        let pair_created_event1 = pair_created_event
            .iter()
            .map(|event| {
                let date_time = DateTime::<Utc>::from_timestamp(event.timestamp as i64, 0).unwrap();
                let local_date_time = date_time.with_timezone(&Local);
                UniswapV2Pair {
                    block_time: local_date_time.to_string(),
                    block_number: event.block_number,
                    transaction_hash: event.tx_hash.clone(),
                    event_type: "PairCreated".to_string(),
                    factory_address: event.factory_address.to_string(),
                    pair_adress: event.pair_address.to_string(),
                    token0: event.token0.to_string(),
                    token1: event.token1.to_string(),
                }
            })
            .collect::<Vec<_>>();
        csv_file.write_pair(&pair_created_event1).unwrap();

        let mut csv_file1 = CsvFile::new("data/pair_event.csv").unwrap();
        let mut pair_events: Vec<UniswapV2PairEvent> = Vec::new();
        for event in pair_created_event {
            let pair_address = event.pair_address;
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
                .map(|event| {
                    let date_time =
                        DateTime::<Utc>::from_timestamp(event.timestamp as i64, 0).unwrap();
                    let local_date_time = date_time.with_timezone(&Local);
                    UniswapV2PairEvent {
                        block_time: local_date_time.to_string(),
                        block_number: event.block_number,
                        transaction_hash: event.tx_hash.clone(),
                        event_type: "Mint".to_string(),
                        route_address: event.sender.to_string(),
                        pair_address: pair_address.to_string(),
                        amount0: event.amount0.to_string(),
                        amount1: event.amount1.to_string(),
                    }
                })
                .collect::<Vec<_>>();
            pair_events.extend(mint_uniswap_events);

            let burn_events = transform_burn_event(&burn_logs).unwrap();
            info!("burn_events: {:#?}", burn_events);
            let burn_uniswap_events = burn_events
                .iter()
                .map(|event| {
                    let date_time =
                        DateTime::<Utc>::from_timestamp(event.timestamp as i64, 0).unwrap();
                    let local_date_time = date_time.with_timezone(&Local);
                    UniswapV2PairEvent {
                        block_time: local_date_time.to_string(),
                        block_number: event.block_number,
                        transaction_hash: event.tx_hash.clone(),
                        event_type: "Burn".to_string(),
                        route_address: event.sender.to_string(),
                        pair_address: pair_address.to_string(),
                        amount0: event.amount0.to_string(),
                        amount1: event.amount1.to_string(),
                    }
                })
                .collect::<Vec<_>>();
            pair_events.extend(burn_uniswap_events);

            let swap_events = transform_swap_event(&swap_logs).unwrap();
            info!("swap_events: {:#?}", swap_events);
            let swap_uniswap_events = swap_events
                .iter()
                .map(|event| {
                    let total_amount0 = event.amount0_in + event.amount0_out;
                    let total_amount1 = event.amount1_in + event.amount1_out;
                    let date_time =
                        DateTime::<Utc>::from_timestamp(event.timestamp as i64, 0).unwrap();
                    let local_date_time = date_time.with_timezone(&Local);
                    UniswapV2PairEvent {
                        block_time: local_date_time.to_string(),
                        block_number: event.block_number,
                        transaction_hash: event.tx_hash.clone(),
                        event_type: "Swap".to_string(),
                        route_address: event.sender.to_string(),
                        pair_address: pair_address.to_string(),
                        amount0: total_amount0.to_string(),
                        amount1: total_amount1.to_string(),
                    }
                })
                .collect::<Vec<_>>();
            pair_events.extend(swap_uniswap_events);
        }
        csv_file1.write_pair_event(&pair_events).unwrap();
    }
}
