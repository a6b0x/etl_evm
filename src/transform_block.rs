use alloy::rpc::types::eth::Block;
use eyre::Result;

#[derive(Debug)]
pub struct BlockTemp {
    pub block_number: u64,
    pub transactions_len: usize,
    pub miner: String,
    pub timestamp: u64,
}

pub fn transform_block(block: &Block) -> Result<BlockTemp> {
    let block_number = block.header.number;
    let transactions_len = block.transactions.len();
    let miner = block.header.beneficiary.to_string();
    let timestamp = block.header.timestamp;
    Ok(BlockTemp {
        block_number,
        transactions_len,
        miner,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{extract_block::EvmBlock,init::AppConfig};
    use log::info;

    #[tokio::test]
    async fn test_transform_block() {
        let app_config = AppConfig::new().unwrap();
        let log_level = app_config.init_log().unwrap();
        info!("app_config: {:#?}", app_config);

        let rpc_client = EvmBlock::new(&app_config.eth.http_url).await.unwrap();
        let new_block_number = rpc_client.get_latest_block_number().await.unwrap();
        info!("get_latest_block_number: {:?}", new_block_number);

        let new_block_data = rpc_client.get_block_by_number(new_block_number).await.unwrap();
        if let Some(block) = new_block_data.as_ref() {
            info!("get_block_by_number Block.Header: {:#?}", block.header);
            let transformed_block = transform_block(block).unwrap();
            info!("transformed_block: {:#?}", transformed_block);
        }
    }
}