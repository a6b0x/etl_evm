use crate::transform_block::BlockTemp;
use csv::Writer;
use eyre::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use serde::Serialize;
use std::fs::File;

#[derive(Debug, Serialize)]
pub struct BlockTable {
    pub block_number: u64,
    pub tx_count: usize,
    pub miner: String,
    pub date_time: String,
}

pub struct BlockTableFile {
    csv: Writer<File>,
}

impl BlockTableFile {
    pub fn new(filename: &str) -> Result<Self> {
        //let file = File::create(filename).context("Failed to create file")?;
        let file = File::options()
            .create(true)
            .append(true)
            .open(filename)
            .context("Failed to open file")?;
        let writer = Writer::from_writer(file);
        //writer.write_record(&["block_number","tx_count","miner","date_time"])
        //    .context("Failed to write record header")?;
        Ok(Self { csv: writer })
    }

    pub fn write_block(&mut self, block: &BlockTable) -> Result<()> {
        self.csv
            .serialize(block)
            .context("Failed to write block data")
    }
}

pub struct BlockTableTsdb {
    infux_client: Client,
}

impl BlockTableTsdb {
    pub fn new(auth_token: &str) -> Self {
        let mut headers = HeaderMap::new();
        let auth_header_name =
            HeaderName::from_bytes(b"Authorization").expect("Invalid header name");
        let auth_header_value =
            HeaderValue::from_str(&format!("Bearer {}", auth_token)).expect("Invalid header value");
        headers.insert(auth_header_name, auth_header_value);

        Self {
            infux_client: Client::builder().default_headers(headers).build().unwrap(),
        }
    }

    pub async fn write_block(
        &self,
        url: &str,
        block: &BlockTemp,
    ) -> Result<String, reqwest::Error> {
        let influx_data = format!(
            "block,miner={} transactions_len={},block_number={} {}",
            block.miner, block.transactions_len, block.block_number, block.timestamp
        );
        let response = self
            .infux_client
            .post(url)
            .body(influx_data)
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
    use crate::{extract_block::EvmBlock, init::AppConfig, transform_block::transform_block};
    use chrono::{DateTime, Local, Utc};
    
    use log::info;

    #[test]
    fn test_load() {
        let mut csv_file = BlockTableFile::new("data/test.csv").unwrap();
        let block = BlockTable {
            block_number: 1,
            tx_count: 1,
            miner: "0x1234567890123456789012345678901234567890".to_string(),
            date_time: "2023-01-01 00:00:00".to_string(),
        };
        csv_file.write_block(&block).unwrap();
    }

    #[tokio::test]
    async fn test_block_table_file() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let rpc_client = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let new_block_number = rpc_client.get_latest_block_number().await.unwrap();
        info!("get_latest_block_number: {:?}", new_block_number);

        let mut csv_file = BlockTableFile::new("data/block.csv").unwrap();

        let new_block_data = rpc_client
            .get_block_by_number(new_block_number)
            .await
            .unwrap();
        if let Some(block) = new_block_data.as_ref() {
            info!("get_block_data Block.Header: {:#?}", block.header);
            let transformed_block = transform_block(block).unwrap();
            info!("transformed_block: {:#?}", transformed_block);

            let date_time =
                DateTime::<Utc>::from_timestamp(transformed_block.timestamp as i64, 0).unwrap();
            let local_date_time = date_time.with_timezone(&Local);

            let block = BlockTable {
                block_number: transformed_block.block_number,
                tx_count: transformed_block.transactions_len,
                miner: transformed_block.miner,
                date_time: local_date_time.to_string(),
            };
            csv_file.write_block(&block).unwrap();
        }
    }

    #[tokio::test]
    async fn test_block_table_tsdb() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let rpc_client = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let new_block_number = rpc_client.get_latest_block_number().await.unwrap();
        info!("get_latest_block_number: {:?}", new_block_number);


        let auth_token = "apiv3_di3lJBckgHFT2cJc5VLkKwsWsVEwI3XZsefjifwwLNR8kruGfhazhZ3tGBvIPZIquaFlbnqHJgTDdaLUFgIzrw";
        let write_url = "http://tsdb:8181/api/v3/write_lp?db=evm";
        let tsdb_client = BlockTableTsdb::new(auth_token);

        let new_block_data = rpc_client
            .get_block_by_number(new_block_number)
            .await
            .unwrap();
        if let Some(block) = new_block_data.as_ref() {
            info!("get_block_data Block.Header: {:#?}", block.header);
            let transformed_block = transform_block(block).unwrap();
            info!("transformed_block: {:#?}", transformed_block);   
            let response = tsdb_client.write_block(write_url, &transformed_block).await.unwrap();
            info!("write_block: {:?}", response);
        }
    }
}
