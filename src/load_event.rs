use crate::transform_event::{BurnEvent, MintEvent, PairCreatedEvent, SwapEvent};
use csv::Writer;
use eyre::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use std::fs::File;

pub struct PairsTableTsdb {
    pub influx_client: Client,
}

pub struct PairsTableFile {
    csv_writer: Writer<File>,
}

impl PairsTableTsdb {
    pub fn new(auth_token: &str) -> Self {
        let mut headers = HeaderMap::new();
        let auth_header_name =
            HeaderName::from_bytes(b"Authorization").expect("Invalid header name");
        let auth_header_value =
            HeaderValue::from_str(&format!("Bearer {}", auth_token)).expect("Invalid header value");
        headers.insert(auth_header_name, auth_header_value);

        Self {
            influx_client: Client::builder().default_headers(headers).build().unwrap(),
        }
    }

    pub async fn query(
        &self,
        url: &str,
        database: &str,
        sql: &str,
    ) -> Result<String, reqwest::Error> {
        let params = [("db", database), ("q", sql)];
        let response = self
            .influx_client
            .get(url)
            .query(&params)
            .send()
            .await?
            .text()
            .await?;
        Ok(response)
    }

    pub async fn write(&self, url: &str, data: &str) -> Result<String, reqwest::Error> {
        let response = self
            .influx_client
            .post(url)
            .body(data.to_string())
            .send()
            .await?
            .text()
            .await?;
        Ok(response)
    }
}

impl PairsTableFile {
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
        Ok(Self { csv_writer: writer })
    }

    pub fn write_pair_created_event(&mut self, events: &[PairCreatedEvent]) -> Result<()> {
        for event in events {
            self.csv_writer
                .serialize(event)
                .context("Failed to write event data")?;
        }
        Ok(())
    }
    pub fn write_mint_event(&mut self, events: &[MintEvent]) -> Result<()> {
        for event in events {
            self.csv_writer
                .serialize(event)
                .context("Failed to write event data")?;
        }
        Ok(())
    }
    pub fn write_burn_event(&mut self, events: &[BurnEvent]) -> Result<()> {
        for event in events {
            self.csv_writer
                .serialize(event)
                .context("Failed to write event data")?;
        }
        Ok(())
    }
    pub fn write_swap_event(&mut self, events: &[SwapEvent]) -> Result<()> {
        for event in events {
            self.csv_writer
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
        extract_block::EvmBlock,
        extract_event::{UniswapV2, UniswapV2Tokens},
        init::AppConfig,
        transform_event::{
            transform_burn_event, transform_mint_event, transform_pair_created_event,
            transform_swap_event, BurnEvent, MintEvent, SwapEvent,
        },
    };
    use alloy::primitives::Address;
    use std::str::FromStr;
    
    use eyre::{Ok, Result};
    use futures_util::StreamExt;
    use log::info;

    #[tokio::test]
    async fn test_pair_table() {
        let app_config = AppConfig::new().unwrap();
        info!("app_config: {:#?}", app_config);

        let _tsdb = PairsTableTsdb::new(&app_config.tsdb.auth_token);

        let evm_block = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let router_addr = Address::from_str("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D").unwrap();
        let uniswap_v2 = UniswapV2::new(evm_block.provider.clone(), router_addr).await;

        let from_block = 22828657;
        let to_block = 22828691; 

        let pair_created_event = uniswap_v2
            .get_pair_created(from_block, to_block)
            .await
            .unwrap();
        let pair_created_event1 = transform_pair_created_event(&pair_created_event).unwrap();
        info!("pair_created_event1: {:#?}", pair_created_event1);

        let mut pair_created_event_influx_data = String::new();
        for event in pair_created_event1.iter() {
            let line = event.to_influx_line();
            pair_created_event_influx_data.push_str(&line);
            pair_created_event_influx_data.push('\n');
        }
        pair_created_event_influx_data = pair_created_event_influx_data
            .trim_end_matches('\n')
            .to_string();
        info!(
            "pair_created_event_influx_data: {:#?}",
            pair_created_event_influx_data
        );

        let mut csv_file0 = PairsTableFile::new("data/univ2_create_event.csv").unwrap();
        csv_file0
            .write_pair_created_event(&pair_created_event1)
            .unwrap();

        let mut mint_event_temp: Vec<MintEvent> = Vec::new();
        let mut burn_event_temp: Vec<BurnEvent> = Vec::new();
        let mut swap_event_temp: Vec<SwapEvent> = Vec::new();
        for event in pair_created_event1 {
            let pair_address = event.pair_address;

            let uniswap_v2_tokens = UniswapV2Tokens::new(pair_address, evm_block.provider.clone())
                .await
                .unwrap();
            info!("uniswap_v2_tokens: {:#?}", uniswap_v2_tokens);

            let log3 = uniswap_v2_tokens
                .get_all_event(from_block, to_block)
                .await
                .unwrap();

            if let Some(mint_event_log) = log3.get("Mint") {
                let mint_event = transform_mint_event(mint_event_log).unwrap();
                mint_event_temp.extend(mint_event);
            }
            if let Some(burn_event_log) = log3.get("Burn") {
                let burn_event = transform_burn_event(burn_event_log).unwrap();
                burn_event_temp.extend(burn_event);
            }
            if let Some(swap_event_log) = log3.get("Swap") {
                let swap_event = transform_swap_event(
                    swap_event_log,
                    uniswap_v2_tokens.token0_decimals,
                    uniswap_v2_tokens.token1_decimals,
                ).unwrap();
                swap_event_temp.extend(swap_event);
            }
        }

        let mut csv_file1 = PairsTableFile::new("data/univ2_mint_event.csv").unwrap();
        csv_file1.write_mint_event(&mint_event_temp).unwrap();
        let mut csv_file2 = PairsTableFile::new("data/univ2_burn_event.csv").unwrap();
        csv_file2.write_burn_event(&burn_event_temp).unwrap();
        let mut csv_file3 = PairsTableFile::new("data/univ2_swap_event.csv").unwrap();
        csv_file3.write_swap_event(&swap_event_temp).unwrap();

    }

    #[tokio::test]
    async fn test_load_swap_event() -> Result<()> {
        let app_config = AppConfig::new().unwrap();
        let _ = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let evm_block = EvmBlock::new(&app_config.eth.ws_url).await.unwrap();
        let weth_usdc_pair = Address::from_str("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc").unwrap();
        let uniswap_v2_tokens = UniswapV2Tokens::new(weth_usdc_pair, evm_block.provider)
            .await
            .unwrap();
        info!("uniswap_v2_tokens: {:#?}", uniswap_v2_tokens);

        let tsdb = PairsTableTsdb::new(&app_config.tsdb.auth_token);

        let mut stream = uniswap_v2_tokens.subscribe_swap_event().await?;
        while let Some(log) = stream.next().await {
            let swap_event = transform_swap_event(
                &[log],
                uniswap_v2_tokens.token0_decimals,
                uniswap_v2_tokens.token1_decimals,
            )
            .unwrap();
            info!("swap_event: {:#?}", swap_event);

            let swap_event_influxdata = swap_event
                .iter()
                .map(|e| e.to_influx_line())
                .collect::<Vec<_>>()
                .join("\n")
                .trim_end_matches('\n')
                .to_string();
            info!("swap_event_influxdata: {:#?}", swap_event_influxdata);

            let response_swap_events = tsdb
                .write(&app_config.tsdb.write_url, &swap_event_influxdata)
                .await
                .unwrap();
            info!("response_swap_events: {:#?}", response_swap_events);
        }

        Ok(())
    }
}