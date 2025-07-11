use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use serde::Serialize;

pub struct Tsdb {
    pub httpclient: Client,
}

#[derive(Debug, Serialize)]
pub struct TableUniswapV2Pair {
    pub transaction_hash: String,
    pub event_type: String,
    pub factory_address: String,
    pub pair_adress: String,
    pub token0: String,
    pub token1: String,
    pub block_number: u64,
    pub block_timestamp: i64,
}

#[derive(Debug, Serialize)]
pub struct TableUniswapV2PairEvent {
    pub transaction_hash: String,
    pub event_type: String,
    pub route_address: String,
    pub pair_address: String,
    pub amount0:u128,
    pub amount1:u128,
    pub block_number: u64,
    pub block_timestamp: u64,
}

impl Tsdb {
    pub fn new(url: &str, auth_token: &str) -> Self {
        let mut headers = HeaderMap::new();
        let auth_header_name =
            HeaderName::from_bytes(b"Authorization").expect("Invalid header name");
        let auth_header_value =
            HeaderValue::from_str(&format!("Bearer {}", auth_token)).expect("Invalid header value");
        headers.insert(auth_header_name, auth_header_value);

        Self {
            httpclient: Client::builder().default_headers(headers).build().unwrap(),
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
            .httpclient
            .get(url)
            .query(&params)
            .send()
            .await?
            .text()
            .await?;
        Ok(response)
    }

    pub async fn write(
        &self,
        url: &str,
        database: &str,
        data: &str,
    ) -> Result<String, reqwest::Error> {
        let response = self
            .httpclient
            .post(url)
            .body(data.to_string())
            .send()
            .await?
            .text()
            .await?;
        Ok(response)
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
    use alloy::primitives::{address, Address};
    use chrono::{TimeZone, Utc,DateTime, Local,};
    use eyre::{Ok, Result};
    use log::info;

    #[tokio::test]
    async fn test_Tsdb() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let auth_token = "apiv3_di3lJBckgHFT2cJc5VLkKwsWsVEwI3XZsefjifwwLNR8kruGfhazhZ3tGBvIPZIquaFlbnqHJgTDdaLUFgIzrw";
        let url = "http://tsdb:8181/api/v3/query_sql";
        let database = "evm";
        let url1 = "http://tsdb:8181/api/v3/write_lp?db=evm";
        let tsdb = Tsdb::new(url, auth_token);

        //let uniswapv2_event = "uniswap_v2,transaction_hash=0xa4420e7baf138cd9789f649404be0f0bef6247d01aa36a3dead309ebe51bfb75 block_number=22828657,token=3 1735545602";
        let table_uniswapv2_event = TableUniswapV2Pair {
            block_timestamp: Utc::now().timestamp(),
            block_number: 22828657,
            transaction_hash: "0xa4420e7baf138cd9789f649404be0f0bef6247d01aa36a3dead309ebe51bfb75"
                .to_string(),
            event_type: "PairCreated".to_string(),
            factory_address: "0x5c69bee701ef814a2b6a3edd4b1652cb9d5366149".to_string(),
            pair_adress: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc".to_string(),
            token0: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599".to_string(),
            token1: "0x514910771af9ca656af840dff83e8264ecf986ca".to_string(),
        };
        let uniswapv2_event = format!(
            "uniswap_v2_,transaction_hash={},event_type={},factory_address={},pair_adress={},token0={},token1={} block_number={} {}",
            table_uniswapv2_event.transaction_hash,
            table_uniswapv2_event.event_type,
            table_uniswapv2_event.factory_address,
            table_uniswapv2_event.pair_adress,
            table_uniswapv2_event.token0,
            table_uniswapv2_event.token1,
            table_uniswapv2_event.block_number,
            table_uniswapv2_event.block_timestamp
        );

        let response = tsdb.write(url1, database, &uniswapv2_event).await.unwrap();
        info!("response: {:#?}", response);

        let sql = "SELECT * FROM uniswap_v2_ ORDER BY TIME DESC LIMIT 1";
        let response = tsdb.query(url, database, sql).await.unwrap();
        info!("response: {:#?}", response);
    }

    #[tokio::test]
    async fn test_Tsdb1() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config.log_level: {:?}", log_level);

        let auth_token = "apiv3_di3lJBckgHFT2cJc5VLkKwsWsVEwI3XZsefjifwwLNR8kruGfhazhZ3tGBvIPZIquaFlbnqHJgTDdaLUFgIzrw";
        let url = "http://tsdb:8181/api/v3/query_sql";
        let database = "evm";
        let url1 = "http://tsdb:8181/api/v3/write_lp?db=evm";
        let tsdb = Tsdb::new(url, auth_token);

        let rpc_client = RpcClient::new(&app_config.eth.rpc_url).unwrap();
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
                let mut influx_data = String::new();
        for event in pair_created_event.iter() {
            let line = format!(
                "uniswap_v2_pair,transaction_hash={},event_type={},factory_address={},pair_adress={},token0={},token1={} block_number={} {}\n",
                event.tx_hash,
                "PairCreated".to_string(),
                event.factory_address,
                event.pair_address,
                event.token0,
                event.token1,
                event.block_number,
                event.timestamp
            );
            influx_data.push_str(&line);
        }
        if let Some('\n') = influx_data.chars().last() {
            influx_data.pop();
        }
        info!("influx_data: {:#?}", influx_data);

        let mut pair_events: Vec<TableUniswapV2PairEvent> = Vec::new();
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
                    TableUniswapV2PairEvent {
                        block_timestamp: event.timestamp,
                        block_number: event.block_number,
                        transaction_hash: event.tx_hash.clone(),
                        event_type: "Mint".to_string(),
                        route_address: event.sender.to_string(),
                        pair_address: pair_address.to_string(),
                        amount0: event.amount0,
                        amount1: event.amount1,
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
                    TableUniswapV2PairEvent {
                        block_timestamp: event.timestamp,
                        block_number: event.block_number,
                        transaction_hash: event.tx_hash.clone(),
                        event_type: "Burn".to_string(),
                        route_address: event.sender.to_string(),
                        pair_address: pair_address.to_string(),
                        amount0: event.amount0,
                        amount1: event.amount1,
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
                    TableUniswapV2PairEvent {
                        block_timestamp: event.timestamp,
                        block_number: event.block_number,
                        transaction_hash: event.tx_hash.clone(),
                        event_type: "Swap".to_string(),
                        route_address: event.sender.to_string(),
                        pair_address: pair_address.to_string(),
                        amount0: total_amount0,
                        amount1: total_amount1,
                    }
                })
                .collect::<Vec<_>>();
            pair_events.extend(swap_uniswap_events);
        }

        let influxdata1 = pair_events
            .iter()
            .map(|event| {
                format!(
                    "uniswap_v2_pair_event,transaction_hash={},event_type={},route_address={},pair_address={} amount0={},amount1={},block_number={} {}",
                    event.transaction_hash,
                    event.event_type,
                    event.route_address,
                    event.pair_address,
                    event.amount0,
                    event.amount1,
                    event.block_number,
                    event.block_timestamp
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let influxdata1 = influxdata1.trim_end_matches('\n').to_string();
        info!("influxdata1: {:#?}", influxdata1);
        

        let response = tsdb.write(url1, database, &influxdata1).await.unwrap();
        info!("response: {:#?}", response);

        let sql = "SELECT * FROM uniswap_v2_pair_event ORDER BY TIME DESC LIMIT 1";
        let response = tsdb.query(url, database, sql).await.unwrap();
        info!("response: {:#?}", response);
    }
}
