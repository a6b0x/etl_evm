use eyre::{Context, Result};
use log::{info, LevelFilter};

mod init;
mod extract;
mod transform;
mod load;
mod load_tsdb;
mod extract_wss;

#[tokio::main]
async fn main() -> Result<()> {
    Ok(())
}
