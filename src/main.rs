use eyre::{Context, Result};
use log::{info, LevelFilter};

mod init;
mod extract;
mod transform;
mod load;
mod load_tsdb;
mod extract_block;
mod transform_block;
mod load_block;

#[tokio::main]
async fn main() -> Result<()> {
    Ok(())
}
