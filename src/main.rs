use eyre::{Context, Result};
use log::{info, LevelFilter};

mod init;
mod extract_block;
mod transform_block;
mod load_block;
mod extract_event;
mod transform_event;
mod load_event;

#[tokio::main]
async fn main() -> Result<()> {
    Ok(())
}
