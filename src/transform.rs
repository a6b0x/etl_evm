use alloy::rpc::types::eth::Block;
use chrono::{DateTime, Local, Utc};
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{extract::RpcClient, init::AppConfig};
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
            info!("get_block_data Block.transactions: {:#?}", block.transactions.first_transaction());
        }
    }
}
