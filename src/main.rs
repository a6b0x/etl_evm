use eyre::{Context, Result};
use log::{info, LevelFilter};

use crate::init::init_config;
use crate::extract::rpc::get_blockdata;

mod init;
mod extract;

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = "data/etl.toml";

    let config = init_config(config_path);
    println!("Loaded configuration: {:?}", config);
    
    let result = get_blockdata().await;
    info!("get_blockdata result: {:?}", result);
    
    Ok(())
}
