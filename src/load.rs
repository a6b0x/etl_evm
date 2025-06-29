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

impl CsvFile {
    pub fn new(filename: &str) -> Result<Self> {
        let file = File::create(filename).context("Failed to create file")?;
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
}

#[cfg(test)]

mod tests {
    use super::*;
    use crate::{extract::RpcClient, init::AppConfig, transform::transform_block};
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
}
