use alloy::providers::{Provider, ProviderBuilder};
use eyre::Result;
use log::info;
use crate::init::init_config;

pub async fn get_blockdata() -> Result<()> {
    let rpc_url = "https://reth-ethereum.ithaca.xyz/rpc".parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    // Get latest block number.
    let latest_block = provider.get_block_number().await?;
    info!("get_blockdata result: {:?}", latest_block);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_blockdata(){
        let config_path = "data/etl.toml";
        let config = init_config(config_path);
        info!("Loaded configuration: {:?}", config);

        let result = get_blockdata().await;
        info!("get_blockdata result: {:?}", result);
    }
}
