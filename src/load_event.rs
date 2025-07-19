use crate::transform_event::{BurnEvent, MintEvent, PairCreatedEvent, SwapEvent};
use csv::Writer;
use eyre::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use std::fs::File;
pub struct PairsTableTsdb {
    pub Influx_client: Client,
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
            Influx_client: Client::builder().default_headers(headers).build().unwrap(),
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
            .Influx_client
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
            .Influx_client
            .post(url)
            .body(data.to_string())
            .send()
            .await?
            .text()
            .await?;
        Ok(response)
    }
}

pub struct PairsTableFile {
    csv_writer: Writer<File>,
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
        extract_event::{ERC20Token::new, UniswapV2, UniswapV2Pair::mintCall, UniswapV2Tokens},
        init::AppConfig,
        transform_event::{
            transform_burn_event, transform_mint_event, transform_pair_created_event,
            transform_swap_event, BurnEvent, MintEvent, PairCreatedEvent, SwapEvent,
        },
    };
    use alloy::primitives::{address, Address};
    use chrono::{DateTime, Local, TimeZone, Utc};
    use eyre::{Ok, Result};
    use futures_util::StreamExt;
    use log::info;

    #[tokio::test]
    async fn test_pair_table() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let auth_token = "apiv3_di3lJBckgHFT2cJc5VLkKwsWsVEwI3XZsefjifwwLNR8kruGfhazhZ3tGBvIPZIquaFlbnqHJgTDdaLUFgIzrw";
        let url = "http://tsdb:8181/api/v3/query_sql";
        let database = "evm";
        let url1 = "http://tsdb:8181/api/v3/write_lp?db=evm_uniswap_v2";
        let tsdb = PairsTableTsdb::new(auth_token);

        let evm_block = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let router_addr = address!("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
        let uniswap_v2 = UniswapV2::new(evm_block.provider.clone(), router_addr).await;
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
        let pair_created_event = transform_pair_created_event(&pair_created_events).unwrap();
        info!("pair_created_event: {:#?}", pair_created_event);
        let mut pair_created_event_influx_data = String::new();
        for event in pair_created_event.iter() {
            let line = format!(
                "event_create,transaction_hash={},event_type={},factory_address={},pair_adress={},token0={},token1={} block_number={} {}\n",
                event.transaction_hash,
                "PairCreated".to_string(),
                event.factory_address,
                event.pair_address,
                event.token0_address,
                event.token1_address,
                event.block_number,
                event.block_timestamp
            );
            pair_created_event_influx_data.push_str(&line);
        }
        if let Some('\n') = pair_created_event_influx_data.chars().last() {
            pair_created_event_influx_data.pop();
        }
        info!(
            "pair_created_event_influx_data: {:#?}",
            pair_created_event_influx_data
        );
        let response_pair_created_event = tsdb
            .write(url1, &pair_created_event_influx_data)
            .await
            .unwrap();
        info!(
            "response_pair_created_event: {:#?}",
            response_pair_created_event
        );
        let mut csv_file0 = PairsTableFile::new("data/event_create.csv").unwrap();
        csv_file0
            .write_pair_created_event(&pair_created_event)
            .unwrap();

        let mut mint_events_temp: Vec<MintEvent> = Vec::new();
        let mut burn_events_temp: Vec<BurnEvent> = Vec::new();
        let mut swap_events_temp: Vec<SwapEvent> = Vec::new();
        for event in pair_created_event {
            let pair_address = event.pair_address;
            let (mint_logs, burn_logs, swap_logs) = uniswap_v2
                .get_pair_liquidity(pair_address, from_block, to_block)
                .await
                .unwrap();

            let mint_event = transform_mint_event(&mint_logs).unwrap();
            mint_events_temp.extend(mint_event);

            let burn_event = transform_burn_event(&burn_logs).unwrap();
            burn_events_temp.extend(burn_event);

            // let swap_event = transform_swap_event(&swap_logs).unwrap();
            // swap_events_temp.extend(swap_event);
        }

        info!("mint_events: {:#?}", mint_events_temp);
        let mint_events_influxdata = mint_events_temp
            .iter()
            .map(|event| {
                format!(
                    "event_mint,transaction_hash={},event_type={},caller_address={},pair_address={} amount0={},amount1={},block_number={} {}",
                    event.transaction_hash,
                    event.event_type,
                    event.caller_address,
                    event.pair_address,
                    event.token0_amount,
                    event.token1_amount,
                    event.block_number,
                    event.block_timestamp
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim_end_matches('\n')
            .to_string();
        info!("mint_events_influxdata: {:#?}", mint_events_influxdata);
        let response_mint_events = tsdb.write(url1, &mint_events_influxdata).await.unwrap();
        info!("response_mint_events: {:#?}", response_mint_events);
        let mut csv_file1 = PairsTableFile::new("data/event_mint.csv").unwrap();
        csv_file1.write_mint_event(&mint_events_temp).unwrap();

        info!("burn_events: {:#?}", burn_events_temp);
        let burn_events_influxdata = burn_events_temp
            .iter()
            .map(|event| {
                format!(
                    "event_burn,transaction_hash={},event_type={},caller_address={},pair_address={} amount0={},amount1={},block_number={} {}",
                    event.transaction_hash,
                    event.event_type,
                    event.caller_address,
                    event.pair_address,
                    event.token0_amount,
                    event.token1_amount,
                    event.block_number,
                    event.block_timestamp
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            .trim_end_matches('\n')
            .to_string();
        info!("influxdata2: {:#?}", burn_events_influxdata);
        let response_burn_events = tsdb.write(url1, &burn_events_influxdata).await.unwrap();
        info!("response_burn_events: {:#?}", response_burn_events);
        let mut csv_file2 = PairsTableFile::new("data/event_burn.csv").unwrap();
        csv_file2.write_burn_event(&burn_events_temp).unwrap();

        // info!("swap_events: {:#?}", swap_events_temp);
        // let swap_events_influxdata = swap_events_temp
        //     .iter()
        //     .map(|event| {
        //         format!(
        //             "event_swap,transaction_hash={},event_type={},caller_address={},pair_address={} amount0={},amount1={},block_number={} {}",
        //             event.transaction_hash,
        //             event.event_type,
        //             event.caller_address,
        //             event.pair_address,
        //             event.token0_amount,
        //             event.token1_amount,
        //             event.block_number,
        //             event.block_timestamp
        //         )
        //     })
        //     .collect::<Vec<_>>()
        //     .join("\n")
        //     .trim_end_matches('\n')
        //     .to_string();
        // info!("swap_events_influxdata: {:#?}", swap_events_influxdata);
        // let response_swap_events = tsdb
        //     .write(url1, database, &swap_events_influxdata)
        //     .await
        //     .unwrap();
        // info!("response_swap_events: {:#?}", response_swap_events);
        // let mut csv_file3 = PairsTableFile::new("data/event_swap.csv").unwrap();
        // csv_file3.write_swap_event(&swap_events_temp).unwrap();
    }

    #[tokio::test]
    async fn test_load_swap_event() -> Result<()> {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let evm_block = EvmBlock::new(&app_config.eth.ws_url).await.unwrap();
        let weth_usdc_pair = address!("0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc");
        let uniswap_v2_tokens = UniswapV2Tokens::new(weth_usdc_pair, evm_block.provider)
            .await
            .unwrap();
        info!("uniswap_v2_tokens: {:#?}", uniswap_v2_tokens);

        let tsdb = PairsTableTsdb::new(&app_config.tsdb.auth_token);

        let mut stream = uniswap_v2_tokens.subscribe_swap_event().await?;
        while let Some(log) = stream.next().await {
            //info!("Received log: {:#?}", log);
            let swap_event = transform_swap_event(
                &[log],
                uniswap_v2_tokens.token0_decimals,
                uniswap_v2_tokens.token1_decimals,
            )
            .unwrap();
            info!("swap_event: {:#?}", swap_event);

            let swap_event_influxdata = swap_event
            .iter()
            .map(|e| {
            format!("swap_event1,pair_address={},caller_address={},receiver_address={},transaction_hash={} \
                token0_amount={},token1_amount={},token0_amounts={},token1_amounts={},token0_token1={},token1_token0={},block_number={} {}",
                e.pair_address,
                e.caller_address,
                e.receiver_address,
                e.transaction_hash,
                e.token0_amount,
                e.token1_amount,
                e.token0_amounts,
                e.token1_amounts,
                e.token0_token1,
                e.token1_token0,
                e.block_number,
                e.block_timestamp
            ) })
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
